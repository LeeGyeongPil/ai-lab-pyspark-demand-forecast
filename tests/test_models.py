from datetime import timedelta

from pyspark.sql import functions as F

from demand_forecast.backtest import run_backtest
from demand_forecast.features import add_features
from demand_forecast.forecast import forecast_next_days, save_forecast
from demand_forecast.models import per_series_weekday_mean
from demand_forecast.split import VALID_DAYS, backtest_origins, last_date, validation_cutoff


def test_per_series_backtest_forecast_shapes(spark, small_filled, tmp_path):
    series = small_filled.select("store", "item").distinct().count()
    last = last_date(small_filled)
    cutoff = validation_cutoff(last)

    # per-series model: one row per series and validation day
    ps = per_series_weekday_mean(small_filled, cutoff, VALID_DAYS)
    assert ps.count() == series * VALID_DAYS
    assert ps.filter(F.col("prediction") < 0).count() == 0

    # rolling backtest: 4 rounds, each with baseline and model WAPE
    res = run_backtest(add_features(small_filled), last)
    assert len(res["rounds"]) == 4
    assert [r["origin"] for r in res["rounds"]] == [d.isoformat() for d in backtest_origins(last)]
    for r in res["rounds"]:
        assert r["baseline_wape"] > 0 and r["gbt_wape"] > 0

    # 7-day forecast table
    table = forecast_next_days(small_filled)
    assert len(table) == series * 7
    expected = {last + timedelta(days=k) for k in range(1, 8)}
    assert set(table["date"]) == expected
    assert (table["forecast"] >= 0).all()
    csv_path, parquet_path = save_forecast(table, tmp_path)
    assert csv_path.exists() and parquet_path.exists()
