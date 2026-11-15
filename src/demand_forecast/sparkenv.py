"""Spark environment: find Java 17+, point temp folders at a writable place, start a session."""

from __future__ import annotations

import glob
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Iterable, MutableMapping
from pathlib import Path

MIN_JAVA = 17

# Where Java usually lives (checked in this order after JAVA_HOME).
DEFAULT_CANDIDATES = (
    "/opt/homebrew/opt/openjdk@21",
    "/opt/homebrew/opt/openjdk@17",
    "/opt/homebrew/opt/openjdk",
    "/usr/local/opt/openjdk@21",
    "/usr/local/opt/openjdk@17",
    "/usr/local/opt/openjdk",
)


class SparkStartError(RuntimeError):
    """Raised when the Spark JVM could not be started (message explains how to fix it)."""


def java_major_version(java_home: str | Path) -> int | None:
    """Return the major Java version of ``<java_home>/bin/java`` or None if it cannot be run."""
    exe = Path(java_home) / "bin" / "java"
    if not exe.is_file():
        return None
    try:
        proc = subprocess.run(
            [str(exe), "-version"], capture_output=True, text=True, timeout=20, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return None
    match = re.search(r'version "(\d+)(?:\.(\d+))?', proc.stderr + proc.stdout)
    if not match:
        return None
    major = int(match.group(1))
    # Old style "1.8.0_x" means Java 8.
    return int(match.group(2)) if major == 1 and match.group(2) else major


def _default_candidates() -> list[str]:
    return list(DEFAULT_CANDIDATES) + sorted(glob.glob("/usr/lib/jvm/*"), reverse=True)


def find_java(
    candidates: Iterable[str] | None = None,
    env: MutableMapping[str, str] | None = None,
) -> str | None:
    """Find a Java home with version >= 17.

    Order: ``JAVA_HOME``, candidate folders, then ``java`` on ``PATH``.
    Pass ``candidates=[]`` and ``env={}`` to simulate a machine without Java.
    """
    env = os.environ if env is None else env
    ordered: list[str] = []
    if env.get("JAVA_HOME"):
        ordered.append(env["JAVA_HOME"])
    ordered.extend(_default_candidates() if candidates is None else list(candidates))
    on_path = shutil.which("java", path=env.get("PATH", ""))
    if on_path:
        ordered.append(str(Path(on_path).resolve().parent.parent))
    for home in ordered:
        # Homebrew keeps the real JDK one level down.
        for root in (home, str(Path(home) / "libexec" / "openjdk.jdk" / "Contents" / "Home")):
            version = java_major_version(root)
            if version is not None and version >= MIN_JAVA:
                return root
    return None


def resolve_tmp_dir(tmp_dir: str | Path | None = None, env: MutableMapping[str, str] | None = None) -> Path:
    """Spark/Java temp folder: argument > DEMAND_FORECAST_TMP > ``./.spark-tmp``."""
    env = os.environ if env is None else env
    chosen = tmp_dir or env.get("DEMAND_FORECAST_TMP") or Path.cwd() / ".spark-tmp"
    return Path(chosen).expanduser().resolve()


def prepare_env(
    env: MutableMapping[str, str] | None = None,
    tmp_dir: str | Path | None = None,
    java_home: str | None = None,
) -> Path:
    """Set environment variables Spark needs *before* the JVM starts; return the temp folder.

    Some sandboxes forbid writing to the default Java temp folder, which only shows up
    as a vague JAVA_GATEWAY_EXITED error. Pointing the folders at a writable place avoids it.
    """
    env = os.environ if env is None else env
    tmp = resolve_tmp_dir(tmp_dir, env)
    tmp.mkdir(parents=True, exist_ok=True)
    env["SPARK_LOCAL_DIRS"] = str(tmp)
    option = f"-Djava.io.tmpdir={tmp}"
    existing = env.get("JAVA_TOOL_OPTIONS", "")
    if option not in existing:
        env["JAVA_TOOL_OPTIONS"] = f"{existing} {option}".strip()
    if java_home:
        env["JAVA_HOME"] = java_home
    env.setdefault("PYSPARK_PYTHON", sys.executable)  # workers use the same interpreter
    return tmp


def get_spark(tmp: Path):
    """Create (or reuse) a local SparkSession with small, quiet settings."""
    from pyspark.sql import SparkSession

    try:
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("demand-forecast")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.local.dir", str(tmp))
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as exc:  # PySparkRuntimeError(JAVA_GATEWAY_EXITED) and friends
        raise SparkStartError(
            f"Could not start Spark ({type(exc).__name__}). Check that JAVA_HOME points to Java "
            f"{MIN_JAVA}+ and that the temp folder is writable: {tmp} "
            "(change it with --tmp-dir or DEMAND_FORECAST_TMP)."
        ) from exc
    spark.sparkContext.setLogLevel("ERROR")
    return spark
