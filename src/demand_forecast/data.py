"""Synthetic daily sales: store x item x date, with weekday/holiday effects and missing rows."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

START = date(2025, 1, 1)
# Fixed holidays as "MM-DD" so no extra package is needed.
HOLIDAYS_MD = ("01-01", "03-01", "05-05", "10-03", "12-25")


def spark_dayofweek(ts: pd.Timestamp) -> int:
    """Spark numbering: 1 = Sunday ... 7 = Saturday (pandas uses 0 = Monday ... 6 = Sunday)."""
    return (ts.dayofweek + 1) % 7 + 1


def generate_sales(stores: int = 5, items: int = 10, days: int = 365, seed: int = 42) -> pd.DataFrame:
    """Return recorded sales rows. Zero-sale days are often *not recorded* (missing rows)."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range(START, periods=days, freq="D")
    weekend = np.array([spark_dayofweek(d) in (1, 7) for d in dates])  # Sunday or Saturday
    holiday = np.array([d.strftime("%m-%d") in HOLIDAYS_MD for d in dates])
    trend = 1 + 0.001 * np.arange(days)
    frames = []
    for s in range(stores):
        for i in range(items):
            base = rng.uniform(5, 50)
            if i % 5 == 4:  # a slow seller: many zero days (this is what breaks MAPE)
                base *= 0.12
            mult = 1.0 + 0.4 * weekend + 0.8 * holiday
            sales = rng.poisson(base * mult * trend).astype("int64")
            keep = (sales > 0) | (rng.random(days) > 0.7)  # 70% of zero days go unrecorded
            frames.append(
                pd.DataFrame(
                    {"store": s, "item": i, "date": dates.date, "sales": sales}
                )[keep]
            )
    return pd.concat(frames, ignore_index=True)


# sales.parquet 옆에 "어떤 옵션으로 만든 데이터인지"를 적어 두는 파일.
# 다음 단계가 다른 --stores/--items/--days/--seed 로 실행되면 이 값과 비교해 다시 생성한다.
PARAMS_FILE = "sales.params.json"
PARAM_KEYS = ("stores", "items", "days", "seed")


def params_path(out_dir: str | Path) -> Path:
    return Path(out_dir) / PARAMS_FILE


def read_params(out_dir: str | Path) -> dict | None:
    """Options used to make the saved sales.parquet, or None if unknown (no or broken file)."""
    path = params_path(out_dir)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or set(data) != set(PARAM_KEYS):
        return None
    return {k: data[k] for k in PARAM_KEYS}


def save_sales(pdf: pd.DataFrame, out_dir: str | Path, params: dict | None = None) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "sales.parquet"
    meta = params_path(out)
    # 먼저 옛 옵션 기록을 지운다: parquet 쓰기가 중간에 실패해도 옛 기록이 새 파일을 보증하지 않게.
    meta.unlink(missing_ok=True)
    pdf.to_parquet(path, index=False)
    if params is not None:
        meta.write_text(json.dumps({k: params[k] for k in PARAM_KEYS}) + "\n", encoding="utf-8")
    return path
