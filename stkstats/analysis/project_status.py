from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd

from stkstats.analysis._common import find_minute_path as _find_minute_path, load_events, load_parquet, resolve_daily_path as _resolve_daily_path


BASE_DIR = Path(__file__).resolve().parents[2]

EVENTS_PATH = BASE_DIR / "stkstats/data/raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet"
MINUTE_DIR = BASE_DIR / "stkstats/data/raw/minute_ohlc_t1"
DAILY_DIR = BASE_DIR / "stkstats/data/raw/daily_ohlc"
DERIVED_DIR = BASE_DIR / "stkstats/data/derived"

BEST_PER_BIN_CSV = DERIVED_DIR / "grid_by_gap_bin_minutes_best_per_bin_2025.csv"
SUMMARY_BY_BIN_CSV = DERIVED_DIR / "grid_by_gap_bin_minutes_summary_2025.csv"

# gap bins (ratio)
GAP_BINS = [-1, 0, 0.03, 0.07, 0.10, 0.15, 0.20, 0.25, 1]
GAP_LABELS = ["<0", "0~3", "3~7", "7~10", "10~15", "15~20", "20~25", "25+"]


def norm_yyyymmdd_any(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def year_of(dt_yyyymmdd: str) -> str:
    return dt_yyyymmdd[:4] if dt_yyyymmdd and len(dt_yyyymmdd) >= 4 else ""


def find_minute_path(stk_cd: str, t1_dt: str) -> Optional[Path]:
    return _find_minute_path(MINUTE_DIR, stk_cd, t1_dt)


def resolve_daily_path(stk_cd: str, limit_dt: str) -> Optional[Path]:
    p = _resolve_daily_path(DAILY_DIR, stk_cd, limit_dt)
    if p is not None:
        return p
    # 4) fallback: search any subdir (slow but OK for status)
    cand = sorted(DAILY_DIR.rglob(f"*{stk_cd}*.parquet"))
    return cand[0] if cand else None


def detect_daily_schema(df: pd.DataFrame) -> Tuple[str, str]:
    # 날짜 컬럼 후보
    for c in ["dt", "일자", "date", "base_dt", "trd_dt", "trade_dt", "trd_dd", "trading_date"]:
        if c in df.columns:
            date_col = c
            break
    else:
        raise RuntimeError(f"[DAILY] date_col not found: {df.columns.tolist()}")

    # 종가 컬럼 후보
    for c in ["cur_prc", "종가", "close", "close_pric", "cls_prc", "lst_pric", "close_price"]:
        if c in df.columns:
            close_col = c
            break
    else:
        raise RuntimeError(f"[DAILY] close_col not found: {df.columns.tolist()}")

    return date_col, close_col


def load_limit_close_one(stk_cd: str, limit_dt: str, cache: Dict[str, Tuple[pd.DataFrame, str, str, pd.Series]]) -> Optional[float]:
    p = resolve_daily_path(stk_cd, limit_dt)
    if p is None or not p.exists():
        return None

    key = str(p.resolve())
    if key not in cache:
        df = load_parquet(p)
        date_col, close_col = detect_daily_schema(df)
        dt_norm = df[date_col].map(norm_yyyymmdd_any)
        cache[key] = (df, date_col, close_col, dt_norm)

    df, date_col, close_col, dt_norm = cache[key]
    target = norm_yyyymmdd_any(limit_dt)
    hit = df.loc[dt_norm == target]
    if hit.empty:
        return None

    v = pd.to_numeric(hit.iloc[0][close_col], errors="coerce")
    if pd.isna(v):
        return None
    return float(abs(v))


def print_header(title: str):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def main():
    print_header("PROJECT STATUS (suhoTrade / stkstats)")

    print(f"[BASE_DIR]   {BASE_DIR}")
    print(f"[EVENTS]     {EVENTS_PATH}  exists={EVENTS_PATH.exists()}")
    print(f"[MINUTE_DIR] {MINUTE_DIR}  exists={MINUTE_DIR.exists()}")
    print(f"[DAILY_DIR]  {DAILY_DIR}  exists={DAILY_DIR.exists()}")
    print(f"[DERIVED]    {DERIVED_DIR}  exists={DERIVED_DIR.exists()}")

    # ---- events ----
    if not EVENTS_PATH.exists():
        print("\n[ERR] EVENTS_PATH not found. stop.")
        return

    events = load_events(EVENTS_PATH)
    print_header("1) EVENTS FILE")
    print(f"rows={len(events)}")
    print(f"cols={events.columns.tolist()}")

    # normalize
    events["stk_cd"] = events["stk_cd"].astype(str).str.zfill(6)
    events["limit_dt"] = events["limit_dt"].map(norm_yyyymmdd_any)
    events["t1_dt"] = events["t1_dt"].map(norm_yyyymmdd_any)

    # ---- minute existence ----
    print_header("2) MINUTE FILE COVERAGE")
    ex = events.apply(lambda r: find_minute_path(r["stk_cd"], r["t1_dt"]) is not None, axis=1)
    ex_cnt = int(ex.sum())
    print(f"minute exists: {ex_cnt} / {len(events)}  ({ex_cnt/len(events):.2%})")

    miss_ex = events.loc[~ex, ["stk_cd", "t1_dt"]].head(20).to_dict("records")
    if miss_ex:
        print("missing examples (first 20):")
        for m in miss_ex:
            print(" ", m)

    # ---- daily limit_close attach rate (sample + ratio) ----
    print_header("3) DAILY LIMIT_CLOSE MATCH RATE")
    cache: Dict[str, Tuple[pd.DataFrame, str, str, pd.Series]] = {}
    # 샘플 30개만 먼저
    sample_n = min(30, len(events))
    sample = events.head(sample_n).copy()
    sample["limit_close"] = sample.apply(lambda r: load_limit_close_one(r["stk_cd"], r["limit_dt"], cache), axis=1)
    ok_sample = int(sample["limit_close"].notna().sum())
    print(f"sample match: {ok_sample} / {sample_n}")

    if ok_sample == 0:
        # daily 파일 패턴 보여주기
        print("\n[HINT] daily files sample (up to 30):")
        for p in list(DAILY_DIR.rglob("*.parquet"))[:30]:
            try:
                rel = p.relative_to(DAILY_DIR)
            except Exception:
                rel = p
            print(" ", rel)

        # 어떤 종목에서 daily가 아예 안 잡히는지 5개
        print("\n[HINT] resolve_daily_path samples:")
        for r in sample[["stk_cd", "limit_dt"]].head(5).to_dict("records"):
            p = resolve_daily_path(r["stk_cd"], r["limit_dt"])
            print(" ", r, "->", p)

    # 전체 비율(시간 조금 더 걸려도 status에서 한 번)
    all_lc = []
    for _, r in events.iterrows():
        all_lc.append(load_limit_close_one(r["stk_cd"], r["limit_dt"], cache))
    lc_series = pd.Series(all_lc)
    ok_all = int(lc_series.notna().sum())
    print(f"\nfull match: {ok_all} / {len(events)}  ({ok_all/len(events):.2%})")

    # ---- gap bin distribution (if possible) ----
    print_header("4) GAP BIN DISTRIBUTION (if limit_close available)")
    t1_open = pd.to_numeric(events.get("t1_open", pd.Series([None] * len(events))), errors="coerce")
    limit_close = lc_series
    gap = (t1_open - limit_close) / limit_close
    gap_bin = pd.cut(gap, bins=GAP_BINS, labels=GAP_LABELS)
    cnt = gap_bin.value_counts(dropna=False).sort_index()
    print(cnt.to_string())

    # ---- derived outputs ----
    print_header("5) DERIVED OUTPUT FILES (recent-ish)")
    if DERIVED_DIR.exists():
        outs = sorted(DERIVED_DIR.glob("*.*"), key=lambda p: p.stat().st_mtime, reverse=True)[:25]
        for p in outs:
            print(f"- {p.name}  ({p.stat().st_size/1024:.1f} KB)")
    else:
        print("[WARN] derived dir missing")

    # ---- show BEST_PER_BIN if exists ----
    print_header("6) BEST PER GAP BIN (from derived csv)")
    if BEST_PER_BIN_CSV.exists():
        best = pd.read_csv(BEST_PER_BIN_CSV)
        print(f"[FOUND] {BEST_PER_BIN_CSV}")
        print(best.to_string(index=False))
    else:
        print(f"[MISS] {BEST_PER_BIN_CSV}")

    print_header("DONE")


if __name__ == "__main__":
    main()