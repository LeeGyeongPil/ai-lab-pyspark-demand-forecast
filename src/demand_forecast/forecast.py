"""The final product: a 7-day forecast table for every store x item."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .features import FEATURES, HORIZON, add_features, add_series_mean, extend_calendar
from .models import fit_gbt
from .split import last_date


def forecast_next_days(filled: DataFrame, horizon: int = HORIZON) -> pd.DataFrame:
    """Train on all known days, then predict the ``horizon`` days after the last date.

    Future rows carry null sales; no feature touches them because every window ends 7 days back.
    """
    last = last_date(filled)
    extended = extend_calendar(filled, horizon)
    feat = add_features(extended)
    known = add_series_mean(feat, last + timedelta(days=1))
    model = fit_gbt(known.filter(F.col("sales").isNotNull()), FEATURES)
    future = known.filter(F.col("date") > F.lit(last))
    out = (
        model.predict(future)
        .select("store", "item", "date", F.round("prediction", 2).alias("forecast"))
        .orderBy("store", "item", "date")
        .toPandas()
    )
    return out


def save_forecast(table: pd.DataFrame, out_dir: str | Path) -> tuple[Path, Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    csv_path, parquet_path = out / "forecast.csv", out / "forecast.parquet"
    table.to_csv(csv_path, index=False)
    table.to_parquet(parquet_path, index=False)
    return csv_path, parquet_path
