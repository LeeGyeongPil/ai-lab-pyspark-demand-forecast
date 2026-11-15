"""Baseline forecasts a model has to beat."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_baselines(df: DataFrame) -> DataFrame:
    """Add two columns.

    * ``seasonal_naive``: same weekday last week (``lag_7``).
    * ``weekday_mean_4w``: mean of the same weekday over the last 4 weeks.
    """
    return df.withColumn("seasonal_naive", F.col("lag_7").cast("double")).withColumn(
        "weekday_mean_4w",
        (F.col("lag_7") + F.col("lag_14") + F.col("lag_21") + F.col("lag_28")) / 4.0,
    )
