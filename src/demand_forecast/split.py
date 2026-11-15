"""Time-based split and rolling-origin dates."""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

VALID_DAYS = 28
HORIZON = 7
BACKTEST_ROUNDS = 4


def last_date(df: DataFrame) -> date:
    return df.filter(F.col("sales").isNotNull()).agg(F.max("date")).first()[0]


def validation_cutoff(last: date, days: int = VALID_DAYS) -> date:
    """First validation day: the last ``days`` days are held out."""
    return last - timedelta(days=days - 1)


def time_split(feat: DataFrame, cutoff: date, days: int = VALID_DAYS) -> tuple[DataFrame, DataFrame]:
    """Train on days before ``cutoff``; validate on the next ``days`` days."""
    end = cutoff + timedelta(days=days)
    labeled = feat.filter(F.col("sales").isNotNull())
    train = labeled.filter(F.col("date") < F.lit(cutoff))
    valid = labeled.filter((F.col("date") >= F.lit(cutoff)) & (F.col("date") < F.lit(end)))
    return train, valid


def backtest_origins(last: date, rounds: int = BACKTEST_ROUNDS, horizon: int = HORIZON) -> list[date]:
    """Forecast origins, oldest first: each covers ``horizon`` days, the last one ends at ``last``."""
    return [last - timedelta(days=horizon * k - 1) for k in range(rounds, 0, -1)]
