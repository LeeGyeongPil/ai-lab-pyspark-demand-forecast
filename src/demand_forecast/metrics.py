"""Forecast error metrics computed on Spark DataFrames."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _totals(df: DataFrame, actual: str, pred: str) -> tuple[float, float, float]:
    row = df.agg(
        F.sum(F.abs(F.col(actual) - F.col(pred))).alias("abs_err"),
        F.sum(F.col(pred) - F.col(actual)).alias("signed"),
        F.sum(F.col(actual)).alias("total"),
    ).first()
    total = row["total"]
    if total is None or total <= 0:
        raise ValueError(
            "WAPE needs a positive total of actual sales, but the total is 0 (or the data is empty). "
            "Check that the validation period contains sales."
        )
    return float(row["abs_err"]), float(row["signed"]), float(total)


def wape(df: DataFrame, actual: str = "sales", pred: str = "prediction") -> float:
    """Weighted absolute percentage error: sum|actual - pred| / sum(actual)."""
    abs_err, _, total = _totals(df, actual, pred)
    return abs_err / total


def bias(df: DataFrame, actual: str = "sales", pred: str = "prediction") -> float:
    """Sum(pred - actual) / sum(actual). Negative = under-forecast (stock-outs)."""
    _, signed, total = _totals(df, actual, pred)
    return signed / total


def mape_nonzero(df: DataFrame, actual: str = "sales", pred: str = "prediction") -> tuple[float, int]:
    """MAPE over rows where actual > 0, plus how many zero rows were left out."""
    nonzero = df.filter(F.col(actual) != 0)
    excluded = df.count() - nonzero.count()
    row = nonzero.agg(F.avg(F.abs(F.col(actual) - F.col(pred)) / F.abs(F.col(actual))).alias("m")).first()
    if row["m"] is None:
        raise ValueError("MAPE needs at least one row with non-zero actual sales.")
    return float(row["m"]), excluded


def improvement(baseline_wape: float, model_wape: float) -> float:
    """1 - model/baseline: share of the baseline's error that the model removes."""
    return 1.0 - model_wape / baseline_wape


def wape_by_series(df: DataFrame, pred: str = "prediction", top: int = 5) -> list[dict]:
    """The ``top`` store x item pairs with the largest WAPE."""
    rows = (
        df.groupBy("store", "item")
        .agg(
            (F.sum(F.abs(F.col("sales") - F.col(pred))) / F.sum("sales")).alias("wape"),
            F.sum("sales").alias("total"),
        )
        .filter(F.col("total") > 0)
        .orderBy(F.col("wape").desc())
        .limit(top)
        .collect()
    )
    return [
        {"store": r["store"], "item": r["item"], "wape": float(r["wape"]), "total_sales": int(r["total"])}
        for r in rows
    ]
