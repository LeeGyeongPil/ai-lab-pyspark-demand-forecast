"""Rolling-origin backtest: forecast 7 days, move the origin 7 days forward, repeat."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame

from .baseline import add_baselines
from .features import FEATURES, add_series_mean
from .metrics import improvement, wape
from .models import fit_gbt
from .split import BACKTEST_ROUNDS, HORIZON, backtest_origins, time_split


def run_backtest(feat: DataFrame, last: date, rounds: int = BACKTEST_ROUNDS, horizon: int = HORIZON) -> dict:
    """``feat`` has no ``series_mean`` yet: it is recomputed per origin from training days only."""
    results = []
    for origin in backtest_origins(last, rounds, horizon):
        with_mean = add_series_mean(feat, origin)
        train, valid = time_split(with_mean, origin, horizon)
        scored = add_baselines(fit_gbt(train, FEATURES).predict(valid))
        base = wape(scored, pred="seasonal_naive")
        gbt = wape(scored)
        results.append(
            {
                "origin": origin.isoformat(),
                "baseline_wape": base,
                "gbt_wape": gbt,
                "improvement": improvement(base, gbt),
            }
        )
    count = len(results)
    return {
        "rounds": results,
        "mean_baseline_wape": sum(r["baseline_wape"] for r in results) / count,
        "mean_gbt_wape": sum(r["gbt_wape"] for r in results) / count,
        "wins": sum(1 for r in results if r["gbt_wape"] < r["baseline_wape"]),
    }
