from pathlib import Path

from demand_forecast import cli, sparkenv


def test_missing_java_message_and_tmpdir(monkeypatch, capsys, tmp_path):
    # No Java anywhere: no candidates, no JAVA_HOME, empty PATH.
    assert sparkenv.find_java(candidates=[], env={}) is None

    # The CLI explains how to fix it and exits with code 2 (no Java stack trace).
    monkeypatch.setattr(sparkenv, "find_java", lambda *a, **k: None)
    code = cli.main(["features", "--lang", "en", "--out", str(tmp_path)])
    assert code == 2
    assert "Java 17+ not found" in capsys.readouterr().err

    # prepare_env points Spark and Java temp folders at the chosen folder.
    env: dict[str, str] = {}
    tmp = tmp_path / ".spark-tmp"
    result = sparkenv.prepare_env(env=env, tmp_dir=tmp)
    assert result == tmp.resolve() and tmp.is_dir()
    assert env["SPARK_LOCAL_DIRS"] == str(tmp.resolve())
    assert f"-Djava.io.tmpdir={tmp.resolve()}" in env["JAVA_TOOL_OPTIONS"]
    sparkenv.prepare_env(env=env, tmp_dir=tmp)  # calling twice does not duplicate the option
    assert env["JAVA_TOOL_OPTIONS"].count("java.io.tmpdir") == 1


def test_default_tmp_dir_is_spark_tmp_in_cwd(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    assert sparkenv.resolve_tmp_dir(env={}) == Path(tmp_path).resolve() / ".spark-tmp"
    assert sparkenv.resolve_tmp_dir(env={"DEMAND_FORECAST_TMP": str(tmp_path / "x")}) == (tmp_path / "x").resolve()


def test_java_version_rejects_missing_home(tmp_path):
    assert sparkenv.java_major_version(tmp_path) is None
    assert sparkenv.find_java(candidates=[str(tmp_path)], env={"PATH": ""}) is None


def test_invalid_options_exit_2(capsys):
    assert cli.main(["features", "--lang", "en", "--days", "50"]) == 2
    assert "at least 120" in capsys.readouterr().err
    assert cli.main(["features", "--lang", "en", "--stores", "0"]) == 2
