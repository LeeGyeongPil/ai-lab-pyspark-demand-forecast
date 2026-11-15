"""Leakage experiment: what a "too good" validation score looks like."""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .features import FEATURES, SERIES, add_features, add_series_mean
from .metrics import wape
from .models import fit_gbt
from .split import time_split

LEAK_WINDOW = (-1, 0)  # rowsBetween(-1, 0): yesterday and TODAY (today is the answer we predict)
LEAK_COLUMN = "leaky_mean"


def add_leaky_mean(filled: DataFrame, window: tuple[int, int] = LEAK_WINDOW) -> DataFrame:
    """A moving average that includes today (the target). At forecast time today is unknown, so it cannot exist."""
    w = Window.partitionBy(*SERIES).orderBy("date").rowsBetween(*window)
    return filled.withColumn(LEAK_COLUMN, F.avg("sales").over(w))


def run_leak_experiment(
    filled: DataFrame, cutoff: date, days: int, window: tuple[int, int] = LEAK_WINDOW
) -> dict:
    """Return WAPE for honest features, a leaky window that includes today, and a random split."""
    feat = add_series_mean(add_features(add_leaky_mean(filled, window)), cutoff)
    train, valid = time_split(feat, cutoff, days)
    honest = wape(fit_gbt(train, FEATURES).predict(valid))
    leaky = wape(fit_gbt(train, FEATURES + [LEAK_COLUMN]).predict(valid))

    # Random split: rows from the whole year are mixed, so "future" days end up in training.
    everything = feat.filter(F.col("sales").isNotNull())
    tr, te = everything.randomSplit([0.8, 0.2], seed=1)
    random_split = wape(fit_gbt(tr, FEATURES).predict(te))
    return {
        "window": list(window),
        "honest_wape": honest,
        "leaky_window_wape": leaky,
        "random_split_wape": random_split,
        "leak_gain": 1.0 - leaky / honest,
    }
