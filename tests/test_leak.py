from demand_forecast.leak import run_leak_experiment
from demand_forecast.split import VALID_DAYS, last_date, validation_cutoff


def test_leaky_window_inflates_score(small_filled):
    """A window that includes today makes the validation score look much better."""
    cutoff = validation_cutoff(last_date(small_filled), VALID_DAYS)
    res = run_leak_experiment(small_filled, cutoff, VALID_DAYS)
    assert res["leaky_window_wape"] <= res["honest_wape"] * 0.85
    assert res["random_split_wape"] > 0  # reported for reference, not asserted against honest
