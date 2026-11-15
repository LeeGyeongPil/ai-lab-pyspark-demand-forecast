from datetime import timedelta

import pandas as pd
from pyspark.sql import functions as F

from demand_forecast.data import generate_sales, spark_dayofweek
from demand_forecast.features import FEATURES, add_features, add_series_mean, fill_calendar


def test_generated_data_has_weekend_effect_and_missing_rows():
    pdf = generate_sales(stores=2, items=5, days=120, seed=42)  # item 4 is a slow seller
    assert len(pdf) < 2 * 5 * 120  # some zero-sale days are not recorded
    pdf["is_weekend"] = [spark_dayofweek(pd.Timestamp(d)) in (1, 7) for d in pdf["date"]]
    means = pdf.groupby("is_weekend")["sales"].mean()
    assert means[True] > means[False]  # Spark numbering: 1 = Sunday, 7 = Saturday


def test_fill_calendar_makes_lag7_exactly_seven_days(spark, small_pdf):
    # Delete rows in the middle of every series (first and last day stay).
    all_dates = sorted(small_pdf["date"].unique())
    gone = {all_dates[40], all_dates[41], all_dates[77]}
    pdf = small_pdf[~small_pdf["date"].isin(gone)]
    filled = fill_calendar(spark.createDataFrame(pdf))
    n_days = len(all_dates)

    per_series = filled.groupBy("store", "item").count().collect()
    assert all(r["count"] == n_days for r in per_series)

    out = add_features(filled, keep_warmup=True).toPandas()
    sales = {(r.store, r.item, r.date): r.sales for r in out.itertuples()}
    checked = 0
    for r in out.itertuples():
        if pd.isna(r.lag_7):
            continue
        assert r.lag_7 == sales[(r.store, r.item, r.date - timedelta(days=7))]
        checked += 1
    assert checked > 0
    # The deleted days really came back as zeros.
    filled_pd = filled.toPandas()
    assert (filled_pd[filled_pd["date"].isin(gone)]["sales"] == 0).all()


def test_features_use_only_values_at_least_7_days_old(spark, small_filled):
    target = sorted(r["date"] for r in small_filled.select("date").distinct().collect())[100]
    cutoff = target - timedelta(days=30)  # series_mean only sees days before the cutoff

    def feature_row(df):
        feat = add_series_mean(add_features(df), cutoff)
        row = feat.filter((F.col("store") == 0) & (F.col("item") == 0) & (F.col("date") == target)).first()
        return {c: row[c] for c in FEATURES}

    before = feature_row(small_filled)
    recent = (F.col("store") == 0) & (F.col("item") == 0) & (
        (F.col("date") > F.lit(target - timedelta(days=7))) & (F.col("date") <= F.lit(target))
    )  # d-6 .. d
    changed = small_filled.withColumn("sales", F.when(recent, F.lit(99999)).otherwise(F.col("sales")))
    assert changed.filter(recent).count() == 7
    assert feature_row(changed) == before
