import pytest

from demand_forecast.metrics import bias, improvement, mape_nonzero, wape, wape_by_series


def _df(spark, rows):
    return spark.createDataFrame(rows, "store long, item long, sales long, prediction double")


def test_wape_bias_mape_known_values(spark):
    # actual 10, 0, 20, 30  / predicted 12, 3, 18, 30
    df = _df(spark, [(0, 0, 10, 12.0), (0, 0, 0, 3.0), (0, 0, 20, 18.0), (0, 0, 30, 30.0)])
    # |errors| = 2 + 3 + 2 + 0 = 7, total actual = 60
    assert wape(df) == pytest.approx(7 / 60, abs=1e-4)
    # signed errors = +2 +3 -2 + 0 = +3
    assert bias(df) == pytest.approx(3 / 60, abs=1e-4)
    # MAPE ignores the zero row: (0.2 + 0.1 + 0.0) / 3
    value, excluded = mape_nonzero(df)
    assert value == pytest.approx(0.1, abs=1e-4)
    assert excluded == 1


def test_zero_total_raises_clear_error(spark):
    df = _df(spark, [(0, 0, 0, 1.0), (0, 0, 0, 2.0)])
    with pytest.raises(ValueError, match="positive total"):
        wape(df)
    with pytest.raises(ValueError, match="non-zero"):
        mape_nonzero(df)


def test_improvement_and_worst_series(spark):
    assert improvement(0.2, 0.15) == pytest.approx(0.25)
    df = _df(spark, [(0, 0, 10, 10.0), (0, 1, 10, 20.0), (1, 0, 10, 12.0)])
    worst = wape_by_series(df, top=2)
    assert [(w["store"], w["item"]) for w in worst] == [(0, 1), (1, 0)]
