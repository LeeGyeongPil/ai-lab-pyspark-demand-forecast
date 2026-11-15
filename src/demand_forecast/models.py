"""Global GBT model and a per-series model trained with applyInPandas."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressionModel, GBTRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .features import FEATURES


@dataclass
class GbtModel:
    """A fitted GBT plus the assembler that builds its feature vector."""

    assembler: VectorAssembler
    model: GBTRegressionModel
    columns: list[str]

    def predict(self, df: DataFrame) -> DataFrame:
        """Add a ``prediction`` column (never below 0: you cannot sell negative units)."""
        scored = self.model.transform(self.assembler.transform(df.dropna(subset=self.columns)))
        return scored.withColumn("prediction", F.greatest(F.col("prediction"), F.lit(0.0))).drop("features")


def fit_gbt(train: DataFrame, columns: list[str] | None = None, max_iter: int = 30) -> GbtModel:
    """Train one model for all stores and items."""
    columns = list(columns or FEATURES)
    assembler = VectorAssembler(inputCols=columns, outputCol="features", handleInvalid="error")
    data = assembler.transform(train.filter(F.col("sales").isNotNull()).dropna(subset=columns))
    model = GBTRegressor(labelCol="sales", maxIter=max_iter, maxDepth=5, seed=7).fit(data)
    return GbtModel(assembler, model, columns)


def per_series_weekday_mean(filled: DataFrame, cutoff: date, days: int, weeks: int = 8) -> DataFrame:
    """One tiny model per store x item, run in parallel with ``applyInPandas``.

    Prediction for day d = mean of the same weekday over the last ``weeks`` weeks that are at
    least 7 days old (d-7, d-14, ...), the same 7-day-ahead rule the global model follows.
    Returns rows ``store, item, date, sales, prediction`` for ``cutoff .. cutoff + days - 1``.
    """
    end = cutoff + timedelta(days=days)

    def fit_one_series(p: pd.DataFrame) -> pd.DataFrame:
        p = p.sort_values("date").reset_index(drop=True)  # filled calendar: one row per day
        sales = p["sales"].astype("float64")
        shifted = pd.concat([sales.shift(7 * k) for k in range(1, weeks + 1)], axis=1)
        out = pd.DataFrame(
            {
                "store": p["store"],
                "item": p["item"],
                "date": p["date"],
                "sales": p["sales"],
                "prediction": shifted.mean(axis=1).fillna(0.0),
            }
        )
        keep = (out["date"] >= cutoff) & (out["date"] < end)
        return out[keep]

    return filled.filter(F.col("sales").isNotNull()).groupBy("store", "item").applyInPandas(
        fit_one_series,
        schema="store long, item long, date date, sales long, prediction double",
    )
