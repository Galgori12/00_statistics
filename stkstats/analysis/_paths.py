"""공통 경로: analysis 패키지 어디서나 동일한 프로젝트 루트 사용."""
from pathlib import Path

# stkstats/analysis/_paths.py -> suhoTrade (프로젝트 루트)
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # analysis -> stkstats -> suhoTrade

EVENTS_2025 = BASE_DIR / "stkstats/data/raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet"
MINUTE_DIR = BASE_DIR / "stkstats/data/raw/minute_ohlc_t1"
DAILY_DIR = BASE_DIR / "stkstats/data/raw/daily_ohlc"
DERIVED_DIR = BASE_DIR / "stkstats/data/derived"
