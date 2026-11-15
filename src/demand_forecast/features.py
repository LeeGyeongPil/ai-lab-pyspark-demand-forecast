"""Calendar filling and leak-free window features (every feature is at least 7 days old)."""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .data import HOLIDAYS_MD

SERIES = ["store", "item"]
HORIZON = 7  # we predict 7 days ahead, so no feature may be newer than 7 days
WARMUP_DAYS = 34  # the longest window reaches back 34 days

# Columns the global model sees.
FEATURES = [
    "dow", "month", "is_holiday",
    "lag_7", "lag_14", "lag_28", "rmean_7", "rmean_28", "series_mean",
]


def _day_range(spark, first: date, last: date) -> DataFrame:
    return spark.range(1).select(F.explode(F.sequence(F.lit(first), F.lit(last))).alias("date"))


def fill_calendar(df: DataFrame) -> DataFrame:
    """Give every store x item one row per day; days with no record become sales = 0.

    Without this, ``lag(7)`` means "7 rows back", not "7 days back".
    """
    bounds = df.agg(F.min("date").alias("lo"), F.max("date").alias("hi")).first()
    days = _day_range(df.sparkSession, bounds["lo"], bounds["hi"])
    grid = df.select(*SERIES).distinct().crossJoin(days)
    return (
        grid.join(df.select(*SERIES, "date", "sales"), SERIES + ["date"], "left")
        .fillna({"sales": 0})
        .withColumn("sales", F.col("sales").cast("long"))
    )


def extend_calendar(filled: DataFrame, horizon: int = HORIZON) -> DataFrame:
    """Append ``horizon`` future days per series with unknown (null) sales."""
    last = filled.agg(F.max("date")).first()[0]
    days = _day_range(filled.sparkSession, last + timedelta(days=1), last + timedelta(days=horizon))
    future = (
        filled.select(*SERIES).distinct().crossJoin(days)
        .withColumn("sales", F.lit(None).cast("long"))
    )
    return filled.unionByName(future.select(*filled.columns))


def add_features(df: DataFrame, keep_warmup: bool = False) -> DataFrame:
    """Add weekday/holiday columns and lag / rolling-mean features from the past only.

    Rolling windows end at -7 (not -1) so that day d never sees sales from d-6..d.
    """
    w = Window.partitionBy(*SERIES).orderBy("date")
    out = (
        df.withColumn("dow", F.dayofweek("date"))  # Spark: 1 = Sunday ... 7 = Saturday
        .withColumn("month", F.month("date"))
        .withColumn("is_holiday", F.date_format("date", "MM-dd").isin(*HOLIDAYS_MD).cast("int"))
        .withColumn("lag_7", F.lag("sales", 7).over(w))
        .withColumn("lag_14", F.lag("sales", 14).over(w))
        .withColumn("lag_21", F.lag("sales", 21).over(w))
        .withColumn("lag_28", F.lag("sales", 28).over(w))
        .withColumn("rmean_7", F.avg("sales").over(w.rowsBetween(-13, -7)))
        .withColumn("rmean_28", F.avg("sales").over(w.rowsBetween(-34, -7)))
    )
    if keep_warmup:
        return out
    first = df.agg(F.min("date")).first()[0]
    return out.filter(F.col("date") >= F.lit(first + timedelta(days=WARMUP_DAYS)))


def add_series_mean(feat: DataFrame, cutoff: date) -> DataFrame:
    """Mean sales per store x item computed from days *before* ``cutoff`` only."""
    means = (
        feat.filter((F.col("date") < F.lit(cutoff)) & F.col("sales").isNotNull())
        .groupBy(*SERIES)
        .agg(F.avg("sales").alias("series_mean"))
    )
    return feat.join(means, SERIES, "left").fillna({"series_mean": 0.0})
