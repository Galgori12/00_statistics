import pandas as pd
from pathlib import Path

from stkstats.analysis._common import load_events, load_parquet, save_parquet

BASE = Path("stkstats/data")
EVENTS_IN = BASE / "raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet"
DAILY_DIR = BASE / "raw/daily_ohlc"
EVENTS_OUT = BASE / "raw/archive/upper_limit_events_cleaned_2025_minute_ok_with_limit_close.parquet"

# 가능한 날짜/종가 컬럼 후보들 (여기서 자동 탐색)
DATE_CANDS = ["일자", "date", "dt", "base_dt", "trd_dt", "trade_dt"]
CLOSE_CANDS = ["close", "close_pric", "lst_pric", "종가", "stck_clpr", "cur_prc"]

def pick_col(cols, cands):
    s = set(cols)
    for c in cands:
        if c in s:
            return c
    return None

def normalize_yyyymmdd(x) -> str:
    # 20250102, "2025-01-02", datetime 등 → "YYYYMMDD"
    if pd.isna(x):
        return None
    if isinstance(x, (pd.Timestamp,)):
        return x.strftime("%Y%m%d")
    s = str(x).strip()
    if not s:
        return None
    # 숫자형
    if s.isdigit():
        if len(s) == 8:
            return s
        # 20250102000000 같은 경우 앞 8자리
        if len(s) > 8:
            return s[:8]
    # "YYYY-MM-DD"
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y%m%d")
    except Exception:
        return None

def load_daily_for_stock(stk_cd: str):
    """
    daily_ohlc 안에서 해당 종목 파일을 최대한 찾아서 로드.
    지원: {stk_cd}.parquet / {stk_cd}.csv / glob
    """
    if not DAILY_DIR.exists():
        raise FileNotFoundError(f"DAILY_DIR not found: {DAILY_DIR}")

    # 가장 흔한 케이스 먼저
    candidates = [
        DAILY_DIR / f"{stk_cd}.parquet",
        DAILY_DIR / f"{stk_cd}.pq",
        DAILY_DIR / f"{stk_cd}.csv",
    ]

    # 없으면 glob로 찾기
    if not any(p.exists() for p in candidates):
        globs = list(DAILY_DIR.glob(f"*{stk_cd}*.parquet")) + list(DAILY_DIR.glob(f"*{stk_cd}*.csv"))
        candidates += globs

    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        return None, None

    try:
        if path.suffix.lower() in [".parquet", ".pq"]:
            df = load_parquet(path)
        else:
            df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return None, None

    if df is None or df.empty:
        return None, None

    date_col = pick_col(df.columns, DATE_CANDS)
    close_col = pick_col(df.columns, CLOSE_CANDS)

    if date_col is None or close_col is None:
        return None, None

    # 날짜 정규화
    df = df.copy()
    df["__dt"] = df[date_col].apply(normalize_yyyymmdd)
    df = df.dropna(subset=["__dt"])

    # 종가 숫자화
    df["__close"] = pd.to_numeric(df[close_col], errors="coerce")
    df = df.dropna(subset=["__close"])

    # (dt -> close) 맵
    m = dict(zip(df["__dt"].astype(str), df["__close"].astype(float)))
    return m, path

def main():
    if not EVENTS_IN.exists():
        raise FileNotFoundError(f"EVENTS_IN not found: {EVENTS_IN}")

    df = load_events(EVENTS_IN)
    print("[DBG] events rows:", len(df))
    print("[DBG] events cols:", list(df.columns))

    # limit_dt / t1_dt 정규화
    df["limit_dt"] = df["limit_dt"].apply(normalize_yyyymmdd)
    df["t1_dt"] = df["t1_dt"].apply(normalize_yyyymmdd)

    # 캐시 (stk_cd별 daily 로드 1번만)
    cache = {}
    cache_path = {}

    limit_close_vals = []
    missing = 0

    for i, r in df.iterrows():
        stk_cd = str(r["stk_cd"])
        limit_dt = r["limit_dt"]

        if stk_cd not in cache:
            m, p = load_daily_for_stock(stk_cd)
            cache[stk_cd] = m
            cache_path[stk_cd] = p

        m = cache[stk_cd]
        if m is None:
            limit_close_vals.append(None)
            missing += 1
            continue

        v = m.get(str(limit_dt))
        if v is None:
            limit_close_vals.append(None)
            missing += 1
        else:
            limit_close_vals.append(float(v))

    df["limit_close"] = limit_close_vals

    total = len(df)
    matched = total - missing
    print(f"[DBG] limit_close match: {matched}/{total} ({matched/total*100:.2f}%)")

    # gap_true 생성: (t1_open - limit_close) / limit_close
    df["t1_open"] = pd.to_numeric(df["t1_open"], errors="coerce")
    df["gap_true"] = (df["t1_open"] - df["limit_close"]) / df["limit_close"]

    # 저장
    EVENTS_OUT.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, EVENTS_OUT)
    print(f"[DONE] saved: {EVENTS_OUT}")

    # 샘플 출력
    print("\n=== SAMPLE ===")
    print(df[["stk_cd", "limit_dt", "limit_close", "t1_dt", "t1_open", "gap_true"]].head(10).to_string(index=False))

if __name__ == "__main__":
    main()