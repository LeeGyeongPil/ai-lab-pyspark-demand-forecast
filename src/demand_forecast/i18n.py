"""English / Korean messages and the language-selection rule.

Order: ``--lang`` > ``DEMAND_FORECAST_LANG`` > ``LC_ALL`` / ``LC_MESSAGES`` / ``LANG``.
Anything that is not Korean falls back to English.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

LANGS = ("en", "ko")
ENV_VAR = "DEMAND_FORECAST_LANG"


def _to_lang(value: str) -> str:
    return "ko" if value.strip().lower().startswith("ko") else "en"


def resolve_lang(cli_lang: str | None = None, env: Mapping[str, str] | None = None) -> str:
    """Return ``"en"`` or ``"ko"`` following the priority rule above."""
    env = os.environ if env is None else env
    if cli_lang in LANGS:
        return str(cli_lang)
    if env.get(ENV_VAR):
        return _to_lang(env[ENV_VAR])
    for name in ("LC_ALL", "LC_MESSAGES", "LANG"):
        if env.get(name):
            return _to_lang(env[name])
    return "en"


MESSAGES: dict[str, dict[str, str]] = {
    "en": {
        # --help
        "help_desc": "Learn demand forecasting with PySpark on synthetic store x item daily sales.",
        "help_stores": "number of stores (default: %(default)s)",
        "help_items": "number of items per store (default: %(default)s)",
        "help_days": "number of days of history, at least 120 (default: %(default)s)",
        "help_seed": "random seed for the synthetic data (default: %(default)s)",
        "help_out": "output folder (default: %(default)s)",
        "help_lang": "output language: en or ko (default: env DEMAND_FORECAST_LANG, then LANG)",
        "help_tmp": "Spark/Java temp folder (default: env DEMAND_FORECAST_TMP, then ./.spark-tmp)",
        "help_generate": "step 1: create synthetic daily sales (out/sales.parquet)",
        "help_features": "step 2: fill missing days and build lag / rolling / calendar features",
        "help_baseline": "step 3: score the baselines (same weekday last week)",
        "help_train": "step 4: train the global GBT model and compare with the baseline",
        "help_leak": "step 5: leakage experiment (a feature that peeks at today)",
        "help_per_series": "step 6: one small model per store x item (applyInPandas)",
        "help_backtest": "step 7: rolling-origin backtest, 4 rounds of 7 days",
        "help_forecast": "step 8: forecast the next 7 days (out/forecast.csv)",
        "help_all": "run all 8 steps",
        # errors
        "err_java": (
            "Java 17+ not found. Install it (macOS: brew install openjdk@21; Ubuntu: apt install "
            "openjdk-21-jre-headless) and set JAVA_HOME to it, then run again."
        ),
        "err_spark": "{detail}",
        "err_args": "Invalid option: {detail}",
        "err_min_days": "--days must be at least 120 (got {days}); the features need 34 warm-up days.",
        "err_positive": "--stores and --items must be 1 or more.",
        # stages
        "step": "[{n}/8] {title}",
        "t_generate": "generate: synthetic sales data",
        "t_features": "features: calendar fill + lag / rolling / calendar features",
        "feat_list": "feature columns: {cols}",
        "t_baseline": "baseline: the forecasts a model has to beat",
        "t_train": "train: global GBT model vs baseline",
        "t_leak": "leak: what happens when a feature peeks at today",
        "t_per_series": "per-series: one small model per store x item",
        "t_backtest": "backtest: rolling origin, 4 rounds x 7 days",
        "t_forecast": "forecast: the next 7 days",
        "auto_generate": "(no {path} found - generating it first)",
        "regen_data": "(the existing {path} was made with different options [{old}] - generating it again with [{new}]; old results (report.json, forecast files) are cleared)",
        "gen_synth": "This is synthetic practice data created from seed {seed} - the numbers show the method, not a real store.",
        "gen_rows": "{stores} stores x {items} items x {days} days = {full} possible rows; {rows} recorded, {missing} days missing (days with zero sales are often simply not written down).",
        "gen_saved": "saved {path}",
        "feat_fill": "calendar fill: {before} rows -> {after} rows ({added} missing days filled with sales = 0)",
        "feat_why": "Every feature is at least 7 days old, because we predict 7 days ahead: on the forecast day nothing newer than 7 days ago is known yet.",
        "feat_table": "first rows of store 0 / item 0 (after the first 34 warm-up days):",
        "base_period": "validation period: {first} .. {last} ({days} days, each day predicted 7 days ahead)",
        "base_note_mape": "MAPE is shown only as a side note: {excluded} rows with 0 sales are left out because dividing by 0 is undefined.",
        "base_read": "Reading: WAPE {wape:.2f} means about {per100:.0f} units wrong per 100 units sold.",
        "train_impr": "improvement over seasonal naive = 1 - {model:.4f} / {base:.4f} = {impr:.3f}",
        "train_read": "Reading: the model gets about {per100:.0f} fewer units wrong per 100 sold, i.e. {pct:.0f}% less forecast error than 'same as last week'.",
        "train_bias": "bias {bias:+.3f}: {meaning}",
        "bias_under": "slightly under-forecasting (stock-out risk is a bit larger)",
        "bias_over": "slightly over-forecasting (leftover stock risk is a bit larger)",
        "bias_even": "almost balanced",
        "leak_honest": "honest features (everything >= 7 days old)",
        "leak_leaky": "+ moving average that includes today, rowsBetween({lo}, {hi})",
        "leak_random": "honest features, random split (for reference)",
        "leak_warn": "WARNING: the second score looks {pct:.0f}% better, but that feature uses today's real sales - information you cannot have in real use. A good-looking validation score can be fake.",
        "leak_random_note": "Random split mixes days from the whole year (future days end up in training) and scores a different set of rows, so it is not directly comparable; here the difference is small because every feature is a past value, but it is a risk once a leaky feature is added.",
        "per_compare": "same {days} validation days, WAPE:",
        "per_top": "5 store x item pairs with the largest error (look at these first):",
        "per_note": "applyInPandas loads one whole series into memory per task - fine for 365 days, risky for huge series.",
        "bt_mean": "mean over 4 rounds: baseline {base:.4f}, GBT {gbt:.4f}; GBT won {wins} of 4 rounds",
        "bt_read": "Reading: a single validation window can be lucky; winning in every round means the gain is not luck.",
        "fc_saved": "saved {csv} and {parquet} ({rows} rows = {series} series x 7 days)",
        "fc_head": "first rows (forecast = units to expect that day):",
        "all_done": "done in {sec:.0f}s. Outputs are in {out}/ (sales.parquet, forecast.csv, forecast.parquet, report.json).",
        # table headers
        "c_method": "method",
        "c_wape": "WAPE",
        "c_bias": "bias",
        "c_mape": "MAPE(non-0)",
        "c_store": "store",
        "c_item": "item",
        "c_sales": "total sales",
        "c_origin": "origin (first day)",
        "c_baseline": "baseline WAPE",
        "c_model": "GBT WAPE",
        "c_impr": "improvement",
        "c_date": "date",
        "c_forecast": "forecast",
        "m_naive": "seasonal naive (same weekday last week)",
        "m_4w": "mean of same weekday, last 4 weeks",
        "m_gbt": "GBT global model",
        "m_series": "per-series weekday mean (8 weeks)",
    },
    "ko": {
        "help_desc": "합성 매장 x 상품 일별 판매 데이터로 PySpark 수요 예측을 배우는 데모.",
        "help_stores": "매장 수 (기본값: %(default)s)",
        "help_items": "매장당 상품 수 (기본값: %(default)s)",
        "help_days": "기록 일수, 최소 120 (기본값: %(default)s)",
        "help_seed": "합성 데이터 난수 시드 (기본값: %(default)s)",
        "help_out": "출력 폴더 (기본값: %(default)s)",
        "help_lang": "출력 언어: en 또는 ko (기본값: 환경변수 DEMAND_FORECAST_LANG, 다음 LANG)",
        "help_tmp": "Spark/Java 임시 폴더 (기본값: 환경변수 DEMAND_FORECAST_TMP, 다음 ./.spark-tmp)",
        "help_generate": "1단계: 합성 일별 판매 데이터 생성 (out/sales.parquet)",
        "help_features": "2단계: 빠진 날짜 채우기, 시차·이동평균·달력 특성 만들기",
        "help_baseline": "3단계: 기준 예측(지난주 같은 요일) 점수 매기기",
        "help_train": "4단계: 전역 GBT 모델 학습 후 기준 예측과 비교",
        "help_leak": "5단계: 누수 실험 (오늘 값을 훔쳐보는 특성)",
        "help_per_series": "6단계: 매장 x 상품마다 작은 모델 (applyInPandas)",
        "help_backtest": "7단계: 롤링 원점 백테스트, 7일씩 4회",
        "help_forecast": "8단계: 다음 7일 예측 (out/forecast.csv)",
        "help_all": "8단계 전체 실행",
        "err_java": (
            "Java 17 이상을 찾을 수 없습니다 (Java 17+ not found). 설치하고(macOS: brew install openjdk@21, "
            "Ubuntu: apt install openjdk-21-jre-headless) JAVA_HOME 을 지정한 뒤 다시 실행하세요."
        ),
        "err_spark": "{detail}",
        "err_args": "잘못된 옵션: {detail}",
        "err_min_days": "--days 는 120 이상이어야 합니다 (입력값 {days}). 특성에 준비 기간 34일이 필요합니다.",
        "err_positive": "--stores 와 --items 는 1 이상이어야 합니다.",
        "step": "[{n}/8] {title}",
        "t_generate": "generate: 합성 판매 데이터 만들기",
        "t_features": "features: 빠진 날짜 채우기 + 시차·이동평균·달력 특성",
        "feat_list": "특성 열: {cols}",
        "t_baseline": "baseline: 모델이 이겨야 하는 기준 예측",
        "t_train": "train: 전역 GBT 모델 vs 기준 예측",
        "t_leak": "leak: 특성이 오늘 값을 훔쳐보면 생기는 일",
        "t_per_series": "per-series: 매장 x 상품마다 작은 모델",
        "t_backtest": "backtest: 롤링 원점, 7일씩 4회",
        "t_forecast": "forecast: 다음 7일 예측",
        "auto_generate": "({path} 가 없어 먼저 생성합니다)",
        "regen_data": "(기존 {path} 는 다른 옵션 [{old}] 으로 만든 데이터라 [{new}] 로 다시 생성합니다. 옛 결과(report.json, forecast 파일)는 지웁니다)",
        "gen_synth": "시드 {seed} 로 만든 합성 연습 데이터입니다 - 숫자는 방법을 보여 줄 뿐 실제 매장이 아닙니다.",
        "gen_rows": "{stores}개 매장 x {items}개 상품 x {days}일 = 가능한 행 {full}개 중 {rows}개만 기록됨, 빠진 날 {missing}개 (판매 0인 날은 흔히 기록하지 않습니다).",
        "gen_saved": "저장: {path}",
        "feat_fill": "달력 채우기: {before}행 -> {after}행 (빠진 {added}일을 판매 0 으로 채움)",
        "feat_why": "모든 특성은 7일 이상 지난 값만 씁니다. 7일 앞을 예측하므로 예측하는 날에는 7일보다 최근 값을 아직 모르기 때문입니다.",
        "feat_table": "0번 매장 / 0번 상품의 앞부분 (준비 기간 34일 이후):",
        "base_period": "검증 기간: {first} .. {last} ({days}일, 매일 7일 앞을 예측)",
        "base_note_mape": "MAPE 는 참고용입니다: 판매 0인 {excluded}행은 0으로 나눌 수 없어 뺐습니다.",
        "base_read": "해석: WAPE 가 {wape:.2f} 이면 판매 100개당 약 {per100:.0f}개를 틀린다는 뜻입니다.",
        "train_impr": "계절성 나이브 대비 개선율 = 1 - {model:.4f} / {base:.4f} = {impr:.3f}",
        "train_read": "해석: 판매 100개당 틀리는 양이 약 {per100:.0f}개 줄어, '지난주와 같다'는 예측보다 오차가 {pct:.0f}% 적습니다.",
        "train_bias": "편향 {bias:+.3f}: {meaning}",
        "bias_under": "아주 약간 적게 예측 (품절 위험이 조금 더 큼)",
        "bias_over": "아주 약간 많게 예측 (재고가 남을 위험이 조금 더 큼)",
        "bias_even": "거의 균형",
        "leak_honest": "정직한 특성 (모두 7일 이상 지난 값)",
        "leak_leaky": "+ 오늘을 포함한 이동평균, rowsBetween({lo}, {hi})",
        "leak_random": "정직한 특성 + 무작위 분할 (참고용)",
        "leak_warn": "경고: 두 번째 점수가 {pct:.0f}% 더 좋아 보이지만, 그 특성은 오늘의 실제 판매량을 씁니다. 실제 운영에서는 쓸 수 없는 정보입니다. 검증 점수가 좋아 보여도 가짜일 수 있습니다.",
        "leak_random_note": "무작위 분할은 1년 전체의 날짜를 섞어(미래 날짜가 학습에 들어감) 다른 행들을 채점하므로 바로 비교할 수 없습니다. 이 데이터에서는 모든 특성이 과거 값이라 차이가 작지만, 누수 특성이 섞이면 위험해집니다.",
        "per_compare": "같은 검증 {days}일, WAPE:",
        "per_top": "오차가 가장 큰 매장 x 상품 5개 (먼저 살펴볼 곳):",
        "per_note": "applyInPandas 는 시리즈 하나 전체를 작업마다 메모리에 올립니다 - 365일은 괜찮지만 아주 긴 시리즈는 위험합니다.",
        "bt_mean": "4회 평균: 기준 {base:.4f}, GBT {gbt:.4f}; 4회 중 GBT가 {wins}회 이김",
        "bt_read": "해석: 검증 구간 한 번은 운이 좋을 수 있습니다. 매 회차 이기면 개선이 운이 아니라는 뜻입니다.",
        "fc_saved": "저장: {csv}, {parquet} ({rows}행 = 시리즈 {series}개 x 7일)",
        "fc_head": "앞부분 (forecast = 그날 예상 판매 개수):",
        "all_done": "{sec:.0f}초 만에 끝. 결과는 {out}/ 에 있습니다 (sales.parquet, forecast.csv, forecast.parquet, report.json).",
        "c_method": "방법",
        "c_wape": "WAPE",
        "c_bias": "편향",
        "c_mape": "MAPE(0 제외)",
        "c_store": "매장",
        "c_item": "상품",
        "c_sales": "총 판매",
        "c_origin": "기준일(첫날)",
        "c_baseline": "기준 WAPE",
        "c_model": "GBT WAPE",
        "c_impr": "개선율",
        "c_date": "날짜",
        "c_forecast": "예측",
        "m_naive": "계절성 나이브 (지난주 같은 요일)",
        "m_4w": "최근 4주 같은 요일 평균",
        "m_gbt": "GBT 전역 모델",
        "m_series": "시리즈별 요일 평균 (8주)",
    },
}


def t(lang: str, key: str, **kwargs: object) -> str:
    """Look up ``key`` in ``lang`` and fill in ``{placeholders}``."""
    return MESSAGES[lang][key].format(**kwargs) if kwargs else MESSAGES[lang][key]
