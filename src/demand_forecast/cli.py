"""Command line entry point: ``demand-forecast <step> [options]``."""

from __future__ import annotations

import argparse
import sys

from . import sparkenv
from .i18n import LANGS, resolve_lang, t

COMMANDS = {
    "generate": "help_generate",
    "features": "help_features",
    "baseline": "help_baseline",
    "train": "help_train",
    "leak": "help_leak",
    "per-series": "help_per_series",
    "backtest": "help_backtest",
    "forecast": "help_forecast",
    "all": "help_all",
}
MIN_DAYS = 120


def _scan_lang(argv: list[str]) -> str | None:
    """Find ``--lang`` before argparse runs, so that --help is already in that language."""
    for i, arg in enumerate(argv):
        if arg == "--lang" and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith("--lang="):
            return arg.split("=", 1)[1]
    return None


def build_parser(lang: str) -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--stores", type=int, default=5, help=t(lang, "help_stores"))
    common.add_argument("--items", type=int, default=10, help=t(lang, "help_items"))
    common.add_argument("--days", type=int, default=365, help=t(lang, "help_days"))
    common.add_argument("--seed", type=int, default=42, help=t(lang, "help_seed"))
    common.add_argument("--out", default="out", help=t(lang, "help_out"))
    common.add_argument("--lang", choices=LANGS, default=None, help=t(lang, "help_lang"))
    common.add_argument("--tmp-dir", default=None, help=t(lang, "help_tmp"))

    parser = argparse.ArgumentParser(prog="demand-forecast", description=t(lang, "help_desc"))
    # --lang is also accepted before the step name (e.g. `demand-forecast --lang ko --help`).
    parser.add_argument("--lang", dest="top_lang", choices=LANGS, default=None, help=t(lang, "help_lang"))
    sub = parser.add_subparsers(dest="command", metavar="<step>")
    for name, help_key in COMMANDS.items():
        sub.add_parser(name, parents=[common], help=t(lang, help_key), description=t(lang, help_key))
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else list(argv)
    lang = resolve_lang(_scan_lang(argv))
    parser = build_parser(lang)
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 0
    lang = resolve_lang(args.lang or args.top_lang)

    if args.stores < 1 or args.items < 1:
        print(t(lang, "err_args", detail=t(lang, "err_positive")), file=sys.stderr)
        return 2
    if args.days < MIN_DAYS:
        print(t(lang, "err_args", detail=t(lang, "err_min_days", days=args.days)), file=sys.stderr)
        return 2

    java_home = sparkenv.find_java()
    if java_home is None:
        print(t(lang, "err_java"), file=sys.stderr)
        return 2
    tmp = sparkenv.prepare_env(tmp_dir=args.tmp_dir, java_home=java_home)

    # Imported late so that --help works without starting anything heavy.
    from pyspark.sql import SparkSession

    from .pipeline import Run

    had_session = SparkSession.getActiveSession() is not None
    try:
        spark = sparkenv.get_spark(tmp)
    except sparkenv.SparkStartError as exc:
        print(t(lang, "err_spark", detail=exc), file=sys.stderr)
        return 2
    run = None
    try:
        run = Run(spark, args, lang)
        if args.command == "all":
            run.run_all()
        else:
            getattr(run, args.command.replace("-", "_"))()
    finally:
        if run is not None:
            run.release()  # cached DataFrames belong to this run only
        if not had_session:  # do not stop a session that someone else (e.g. a test) owns
            spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
