"""The eight steps, each printing what it did and what the numbers mean."""

from __future__ import annotations

import time
from argparse import Namespace
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import report
from .backtest import run_backtest
from .baseline import add_baselines
from .data import PARAM_KEYS, generate_sales, read_params, save_sales
from .features import FEATURES, SERIES, add_features, add_series_mean, fill_calendar
from .forecast import forecast_next_days, save_forecast
from .i18n import t
from .leak import LEAK_WINDOW, run_leak_experiment
from .metrics import bias, improvement, mape_nonzero, wape, wape_by_series
from .models import fit_gbt, per_series_weekday_mean
from .split import VALID_DAYS, last_date, time_split, validation_cutoff

# Results computed from sales.parquet; removed when the data is rebuilt for other options.
STALE_OUTPUTS = ("forecast.csv", "forecast.parquet")

STAGES = ("generate", "features", "baseline", "train", "leak", "per-series", "backtest", "forecast")


class Run:
    """Holds the Spark session, options and cached DataFrames shared by the steps."""

    def __init__(self, spark: SparkSession, args: Namespace, lang: str) -> None:
        self.spark = spark
        self.args = args
        self.lang = lang
        self.out = Path(args.out)
        self._filled: DataFrame | None = None
        self._feat0: DataFrame | None = None
        self._scored: DataFrame | None = None
        self._cached: list[DataFrame] = []  # every .cache() we make, released in release()

    # ----- output helpers -------------------------------------------------
    def say(self, key: str, **kw: object) -> None:
        print(t(self.lang, key, **kw))

    def title(self, number: int, key: str) -> None:
        print()
        print(t(self.lang, "step", n=number, title=t(self.lang, key)))

    def table(self, header_keys: list[str], rows: list[list[str]]) -> None:
        print(report.format_table([t(self.lang, k) for k in header_keys], rows))

    # ----- cache bookkeeping ----------------------------------------------
    def _keep(self, df: DataFrame) -> DataFrame:
        """Cache ``df`` and remember it so release() can free it."""
        df = df.cache()
        self._cached.append(df)
        return df

    def release(self) -> None:
        """Free every cached DataFrame of this run.

        Spark's cache is shared by the whole session and matched by query plan, so a stale
        cache of ``read.parquet(out/sales.parquet)`` would be reused even after the file is
        rewritten with other options. Releasing on regenerate and at the end prevents that.
        """
        for df in self._cached:
            df.unpersist()
        self._cached.clear()
        self._filled = self._feat0 = self._scored = None

    # ----- shared data ----------------------------------------------------
    @property
    def sales_path(self) -> Path:
        return self.out / "sales.parquet"

    def wanted_params(self) -> dict:
        """The data options given on this command line (defaults included)."""
        return {k: getattr(self.args, k) for k in PARAM_KEYS}

    @staticmethod
    def _describe(params: dict | None) -> str:
        if params is None:
            return "?"
        return " ".join(f"--{k} {params[k]}" for k in PARAM_KEYS)

    def ensure_sales(self) -> None:
        """Make sure out/sales.parquet exists *and* was made with this run's options."""
        wanted = self.wanted_params()
        if not self.sales_path.exists():
            self.say("auto_generate", path=self.sales_path)
            self.generate(announce=False)
            return
        saved = read_params(self.out)
        if saved == wanted:
            return  # 같은 옵션으로 만든 데이터 -> 그대로 재사용
        # 옵션이 다르거나(예: --stores 2), 옵션 기록이 없어 확인할 수 없으면 다시 만든다.
        # 옛 report.json·forecast 파일은 다른 데이터에서 나온 결과라 함께 지운다.
        self.say("regen_data", path=self.sales_path, old=self._describe(saved), new=self._describe(wanted))
        report.reset_report(self.out)
        for name in STALE_OUTPUTS:
            (self.out / name).unlink(missing_ok=True)
        self.generate(announce=False)

    @property
    def filled(self) -> DataFrame:
        """Sales with one row per store x item x day (missing days = 0)."""
        if self._filled is None:
            self.ensure_sales()
            raw = self.spark.read.parquet(str(self.sales_path))
            self._filled = self._keep(fill_calendar(raw))
        return self._filled

    @property
    def feat0(self) -> DataFrame:
        """Features without the per-series mean (that one depends on the cutoff)."""
        if self._feat0 is None:
            self._feat0 = self._keep(add_features(self.filled))
        return self._feat0

    @property
    def last(self):
        return last_date(self.filled)

    @property
    def cutoff(self):
        return validation_cutoff(self.last, VALID_DAYS)

    def split(self) -> tuple[DataFrame, DataFrame]:
        return time_split(add_series_mean(self.feat0, self.cutoff), self.cutoff, VALID_DAYS)

    def scored(self) -> DataFrame:
        """Validation rows with baseline columns and the GBT ``prediction`` (trained once)."""
        if self._scored is None:
            train, valid = self.split()
            self._scored = self._keep(add_baselines(fit_gbt(train, FEATURES).predict(valid)))
        return self._scored

    def _scores(self, df: DataFrame, pred: str) -> dict:
        mape, excluded = mape_nonzero(df, pred=pred)
        return {"wape": wape(df, pred=pred), "bias": bias(df, pred=pred), "mape": mape, "mape_excluded": excluded}

    # ----- steps ----------------------------------------------------------
    def generate(self, announce: bool = True) -> dict:
        a = self.args
        if announce:
            self.title(1, "t_generate")
        pdf = generate_sales(a.stores, a.items, a.days, a.seed)
        path = save_sales(pdf, self.out, params=self.wanted_params())
        full = a.stores * a.items * a.days
        info = {"stores": a.stores, "items": a.items, "days": a.days, "seed": a.seed,
                "rows": len(pdf), "possible_rows": full, "synthetic": True}
        if announce:
            self.say("gen_synth", seed=a.seed)
            self.say("gen_rows", stores=a.stores, items=a.items, days=a.days, full=full,
                     rows=len(pdf), missing=full - len(pdf))
            self.say("gen_saved", path=path)
        report.update_report(self.out, "data", info)
        self.release()  # 옛 데이터로 만든 캐시를 버린다
        return info

    def features(self) -> dict:
        self.title(2, "t_features")
        after = self.filled.count()  # also generates the data if it is missing
        before =self.spark.read.parquet(str(self.sales_path)).count()
        self.say("feat_fill", before=before, after=after, added=after - before)
        self.say("feat_why")
        self.say("feat_table")
        cols = ["date", "sales", "lag_7", "lag_14", "lag_28", "rmean_7", "rmean_28", "dow", "is_holiday"]
        head = (self.feat0.filter((F.col("store") == 0) & (F.col("item") == 0))
                .orderBy("date").select(*cols).limit(5).collect())
        print(report.format_table(cols, [
            [str(r["date"]), r["sales"], r["lag_7"], r["lag_14"], r["lag_28"],
             f"{r['rmean_7']:.1f}", f"{r['rmean_28']:.1f}", r["dow"], r["is_holiday"]] for r in head]))
        print(t(self.lang, "feat_list", cols=", ".join(FEATURES)))
        info = {"rows_before_fill": before, "rows_after_fill": after, "features": FEATURES}
        report.update_report(self.out, "features", info)
        return info

    def baseline(self) -> dict:
        self.title(3, "t_baseline")
        _, valid = self.split()
        base = self._keep(add_baselines(valid))
        naive = self._scores(base, "seasonal_naive")
        four = self._scores(base, "weekday_mean_4w")
        self.say("base_period", first=self.cutoff, last=self.last, days=VALID_DAYS)
        self.table(["c_method", "c_wape", "c_bias", "c_mape"], [
            [t(self.lang, "m_naive"), f"{naive['wape']:.4f}", f"{naive['bias']:+.4f}", f"{naive['mape']:.4f}"],
            [t(self.lang, "m_4w"), f"{four['wape']:.4f}", f"{four['bias']:+.4f}", f"{four['mape']:.4f}"],
        ])
        self.say("base_note_mape", excluded=naive["mape_excluded"])
        self.say("base_read", wape=naive["wape"], per100=naive["wape"] * 100)
        info = {"seasonal_naive": naive, "weekday_mean_4w": four,
                "validation": {"first": str(self.cutoff), "last": str(self.last), "days": VALID_DAYS}}
        report.update_report(self.out, "baseline", info)
        return info

    def train(self) -> dict:
        self.title(4, "t_train")
        scored = self.scored()
        naive = self._scores(scored, "seasonal_naive")
        gbt = self._scores(scored, "prediction")
        impr = improvement(naive["wape"], gbt["wape"])
        self.table(["c_method", "c_wape", "c_bias", "c_mape"], [
            [t(self.lang, "m_naive"), f"{naive['wape']:.4f}", f"{naive['bias']:+.4f}", f"{naive['mape']:.4f}"],
            [t(self.lang, "m_gbt"), f"{gbt['wape']:.4f}", f"{gbt['bias']:+.4f}", f"{gbt['mape']:.4f}"],
        ])
        self.say("train_impr", model=gbt["wape"], base=naive["wape"], impr=impr)
        self.say("train_read", per100=(naive["wape"] - gbt["wape"]) * 100, pct=impr * 100)
        meaning = "bias_even" if abs(gbt["bias"]) < 0.005 else ("bias_under" if gbt["bias"] < 0 else "bias_over")
        self.say("train_bias", bias=gbt["bias"], meaning=t(self.lang, meaning))
        info = {**gbt, "improvement": impr, "seasonal_naive_wape": naive["wape"], "features": FEATURES}
        report.update_report(self.out, "gbt", info)
        return info

    def leak(self) -> dict:
        self.title(5, "t_leak")
        res = run_leak_experiment(self.filled, self.cutoff, VALID_DAYS)
        lo, hi = LEAK_WINDOW
        self.table(["c_method", "c_wape"], [
            [t(self.lang, "leak_honest"), f"{res['honest_wape']:.4f}"],
            [t(self.lang, "leak_leaky", lo=lo, hi=hi), f"{res['leaky_window_wape']:.4f}"],
            [t(self.lang, "leak_random"), f"{res['random_split_wape']:.4f}"],
        ])
        self.say("leak_warn", pct=res["leak_gain"] * 100)
        self.say("leak_random_note")
        report.update_report(self.out, "leak", res)
        return res

    def per_series(self) -> dict:
        self.title(6, "t_per_series")
        scored = self.scored()
        series = self._keep(per_series_weekday_mean(self.filled, self.cutoff, VALID_DAYS))
        naive = wape(scored, pred="seasonal_naive")
        gbt = wape(scored)
        ps = wape(series)
        self.say("per_compare", days=VALID_DAYS)
        self.table(["c_method", "c_wape"], [
            [t(self.lang, "m_naive"), f"{naive:.4f}"],
            [t(self.lang, "m_gbt"), f"{gbt:.4f}"],
            [t(self.lang, "m_series"), f"{ps:.4f}"],
        ])
        worst = wape_by_series(scored, "prediction", 5)
        self.say("per_top")
        self.table(["c_store", "c_item", "c_wape", "c_sales"],
                   [[w["store"], w["item"], f"{w['wape']:.4f}", w["total_sales"]] for w in worst])
        self.say("per_note")
        info = {"wape": ps, "bias": bias(series), "rows": series.count(), "series_count": series.select(*SERIES).distinct().count(),
                "global_gbt_wape": gbt, "worst_series": worst}
        report.update_report(self.out, "per_series", info)
        return info

    def backtest(self) -> dict:
        self.title(7, "t_backtest")
        res = run_backtest(self.feat0, self.last)
        self.table(["c_origin", "c_baseline", "c_model", "c_impr"],
                   [[r["origin"], f"{r['baseline_wape']:.4f}", f"{r['gbt_wape']:.4f}", f"{r['improvement']:+.3f}"]
                    for r in res["rounds"]])
        self.say("bt_mean", base=res["mean_baseline_wape"], gbt=res["mean_gbt_wape"], wins=res["wins"])
        self.say("bt_read")
        report.update_report(self.out, "backtest", res)
        return res

    def forecast(self) -> dict:
        self.title(8, "t_forecast")
        table = forecast_next_days(self.filled)
        csv_path, parquet_path = save_forecast(table, self.out)
        series = len(table[["store", "item"]].drop_duplicates())
        self.say("fc_saved", csv=csv_path, parquet=parquet_path, rows=len(table), series=series)
        self.say("fc_head")
        self.table(["c_store", "c_item", "c_date", "c_forecast"],
                   [[r.store, r.item, str(r.date), f"{r.forecast:.2f}"] for r in table.head(7).itertuples()])
        info = {"csv": str(csv_path), "parquet": str(parquet_path), "rows": len(table), "series": series}
        report.update_report(self.out, "forecast", info)
        return info

    def run_all(self) -> None:
        start = time.time()
        report.reset_report(self.out)
        self.generate()
        self.features()
        self.baseline()
        self.train()
        self.leak()
        self.per_series()
        self.backtest()
        self.forecast()
        print()
        self.say("all_done", sec=time.time() - start, out=self.out)
