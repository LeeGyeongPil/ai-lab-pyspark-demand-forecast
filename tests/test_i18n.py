import re

import pytest

from demand_forecast import cli
from demand_forecast.i18n import MESSAGES, resolve_lang

HANGUL = re.compile("[가-힣]")
SMALL = ["--stores", "2", "--items", "3", "--days", "120"]


def test_lang_selection_and_keys_match(spark, small_out, capsys):
    # same message keys in both languages
    assert set(MESSAGES["en"]) == set(MESSAGES["ko"])

    # priority: --lang > DEMAND_FORECAST_LANG > LC_ALL > LC_MESSAGES > LANG; not Korean -> English
    assert resolve_lang("ko", {"DEMAND_FORECAST_LANG": "en", "LANG": "en_US.UTF-8"}) == "ko"
    assert resolve_lang(None, {"DEMAND_FORECAST_LANG": "ko", "LANG": "en_US.UTF-8"}) == "ko"
    assert resolve_lang(None, {"DEMAND_FORECAST_LANG": "en", "LC_ALL": "ko_KR.UTF-8"}) == "en"
    assert resolve_lang(None, {"LC_ALL": "ko_KR.UTF-8", "LANG": "en_US.UTF-8"}) == "ko"
    assert resolve_lang(None, {"LC_MESSAGES": "ko_KR.UTF-8", "LANG": "en_US.UTF-8"}) == "ko"
    assert resolve_lang(None, {"LANG": "ko_KR.UTF-8"}) == "ko"
    assert resolve_lang(None, {"LANG": "fr_FR.UTF-8"}) == "en"
    assert resolve_lang(None, {}) == "en"

    # --help is translated
    for lang, expect_hangul in (("ko", True), ("en", False)):
        with pytest.raises(SystemExit) as exc:
            cli.main(["--lang", lang, "--help"])
        assert exc.value.code == 0
        assert bool(HANGUL.search(capsys.readouterr().out)) is expect_hangul

    # a real step prints in the chosen language (same numbers)
    outputs = {}
    for lang in ("en", "ko"):
        assert cli.main(["baseline", "--lang", lang, "--out", str(small_out), *SMALL]) == 0
        outputs[lang] = capsys.readouterr().out
    assert HANGUL.search(outputs["ko"]) and not HANGUL.search(outputs["en"])
    assert "seasonal naive" in outputs["en"]
    number = re.search(r"0\.\d{4}", outputs["en"]).group(0)
    assert number in outputs["ko"]


def test_leak_warning_is_printed_in_both_languages(spark, small_out, capsys):
    assert cli.main(["leak", "--lang", "en", "--out", str(small_out), *SMALL]) == 0
    assert "information you cannot have in real use" in capsys.readouterr().out
    assert cli.main(["leak", "--lang", "ko", "--out", str(small_out), *SMALL]) == 0
    assert "운영에서는 쓸 수 없는 정보" in capsys.readouterr().out
