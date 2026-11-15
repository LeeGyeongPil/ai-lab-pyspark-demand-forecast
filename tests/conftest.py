"""Shared fixtures: one SparkSession for the whole run and a small synthetic dataset."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from demand_forecast import sparkenv
from demand_forecast.data import generate_sales
from demand_forecast.features import fill_calendar

# Spark temp files go inside the repo so that restricted sandboxes can write them.
os.environ.setdefault("DEMAND_FORECAST_TMP", str(Path(__file__).resolve().parents[1] / ".spark-tmp"))


@pytest.fixture(scope="session")
def spark():
    java_home = sparkenv.find_java()
    assert java_home, "Java 17+ is required to run the tests (set JAVA_HOME)"
    tmp = sparkenv.prepare_env(java_home=java_home)
    session = sparkenv.get_spark(tmp)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def small_pdf():
    """2 stores x 3 items x 120 days."""
    return generate_sales(stores=2, items=3, days=120, seed=42)


@pytest.fixture(scope="session")
def small_filled(spark, small_pdf):
    return fill_calendar(spark.createDataFrame(small_pdf)).cache()


@pytest.fixture(scope="session")
def small_out(tmp_path_factory):
    return tmp_path_factory.mktemp("out")
