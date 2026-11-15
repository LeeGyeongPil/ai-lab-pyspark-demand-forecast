import json

from demand_forecast import cli


def test_all_steps_write_outputs_and_report(spark, tmp_path, capsys):
    out = tmp_path / "out"
    code = cli.main(["all", "--lang", "en", "--stores", "2", "--items", "3", "--days", "120", "--out", str(out)])
    assert code == 0
    for name in ("sales.parquet", "forecast.csv", "forecast.parquet", "report.json"):
        assert (out / name).exists(), name
    report = json.loads((out / "report.json").read_text(encoding="utf-8"))
    for key in ("baseline", "gbt", "per_series", "leak", "backtest"):
        assert key in report
    assert "seasonal_naive" in report["baseline"] and "wape" in report["gbt"]
    assert len(report["backtest"]["rounds"]) == 4
    text = capsys.readouterr().out
    assert "synthetic practice data" in text
    assert "improvement over seasonal naive" in text


def test_tmp_dir_option_and_korean_features_line(spark, tmp_path, capsys):
    """Regression: --tmp-dir used to crash (wrong prepare_env argument order)."""
    out = tmp_path / "out"
    code = cli.main(["features", "--lang", "ko", "--stores", "2", "--items", "2", "--days", "120",
                     "--out", str(out), "--tmp-dir", str(tmp_path / "spark-tmp")])
    assert code == 0
    text = capsys.readouterr().out
    assert "특성 열:" in text
    assert "features:" not in text.replace("features: 빠진", "")


def _baseline_wape(out):
    report = json.loads((out / "report.json").read_text(encoding="utf-8"))
    return report["baseline"]["seasonal_naive"]["wape"], report


def test_changed_data_options_regenerate_existing_sales(spark, tmp_path, capsys):
    """Regression: an existing out/sales.parquet used to be reused, silently ignoring
    --stores/--items/--days/--seed (README "Try changing things" 3 showed the wrong number)."""
    from demand_forecast.data import read_params

    out = tmp_path / "out"
    small = ["--lang", "en", "--out", str(out), "--days", "120"]

    # 1) data made with 2 stores x 2 items
    assert cli.main(["generate", *small, "--stores", "2", "--items", "2"]) == 0
    assert read_params(out) == {"stores": 2, "items": 2, "days": 120, "seed": 42}
    capsys.readouterr()

    # 2) a later step with other options must rebuild the data, say so, and use the new options
    assert cli.main(["baseline", *small, "--stores", "2", "--items", "3"]) == 0
    text = capsys.readouterr().out
    assert "generating it again" in text and "--items 2" in text and "--items 3" in text
    assert read_params(out) == {"stores": 2, "items": 3, "days": 120, "seed": 42}
    wape_42, report = _baseline_wape(out)
    assert report["data"]["items"] == 3
    assert spark.read.parquet(str(out / "sales.parquet")).select("store", "item").distinct().count() == 6

    # 3) same options again -> reused, no regeneration message, same number
    assert cli.main(["baseline", *small, "--stores", "2", "--items", "3"]) == 0
    assert "generating it again" not in capsys.readouterr().out
    assert _baseline_wape(out)[0] == wape_42

    # 4) only --seed changes -> new data, different score; stale results from the old data are removed
    (out / "forecast.csv").write_text("stale\n", encoding="utf-8")
    assert cli.main(["baseline", *small, "--stores", "2", "--items", "3", "--seed", "7"]) == 0
    assert "generating it again" in capsys.readouterr().out
    wape_7, report = _baseline_wape(out)
    assert report["data"]["seed"] == 7 and wape_7 != wape_42
    assert set(report) == {"data", "baseline"}
    assert not (out / "forecast.csv").exists()

    # 5) sales.parquet without an options record (older version) -> cannot be trusted, rebuilt
    (out / "sales.params.json").unlink()
    assert cli.main(["baseline", *small, "--stores", "2", "--items", "3", "--lang", "ko"]) == 0
    assert "다시 생성합니다" in capsys.readouterr().out
    assert read_params(out)["seed"] == 42
    assert _baseline_wape(out)[0] == wape_42
