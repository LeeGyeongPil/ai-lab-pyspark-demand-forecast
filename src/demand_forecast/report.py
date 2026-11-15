"""Plain-text tables and the accumulating ``report.json``."""

from __future__ import annotations

import json
import unicodedata
from pathlib import Path


def display_width(text: str) -> int:
    """Terminal width: Korean characters take two columns."""
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1 for ch in text)


def _pad(text: str, width: int) -> str:
    return text + " " * (width - display_width(text))


def format_table(headers: list[str], rows: list[list[str]]) -> str:
    """Fixed-width table with a rule under the header."""
    widths = [max(display_width(str(c)) for c in col) for col in zip(headers, *rows, strict=True)]
    lines = ["  ".join(_pad(str(h), w) for h, w in zip(headers, widths, strict=True))]
    lines.append("  ".join("-" * w for w in widths))
    for row in rows:
        lines.append("  ".join(_pad(str(c), w) for c, w in zip(row, widths, strict=True)))
    return "\n".join(lines)


def report_path(out_dir: str | Path) -> Path:
    return Path(out_dir) / "report.json"


def reset_report(out_dir: str | Path) -> None:
    path = report_path(out_dir)
    if path.exists():
        path.unlink()


def update_report(out_dir: str | Path, section: str, data: dict) -> Path:
    """Merge one section into ``out/report.json`` (other sections are kept)."""
    path = report_path(out_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    current = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    current[section] = data
    path.write_text(json.dumps(current, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    return path
