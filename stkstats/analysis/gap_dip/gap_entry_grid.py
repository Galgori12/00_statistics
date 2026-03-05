# stkstats/analysis/gap_dip/gap_entry_grid.py
from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd

from stkstats.analysis._common import find_minute_path, load_events, load_parquet, save_parquet


# =========================
# Config
# =========================
BASE_DIR = Path(__file__).resolve().parents[3]  # suhoTrade (from analysis/gap_dip/)

EVENTS_PATH = BASE_DIR / "stkstats/data/raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet"
DAILY_DIR   = BASE_DIR / "stkstats/data/raw/daily_ohlc"
MINUTE_DIR  = BASE_DIR / "stkstats/data/raw/minute_ohlc_t1"

OUT_DETAIL  = BASE_DIR / "stkstats/data/derived/gap_entry_grid_detail_2025.parquet"
OUT_SUMMARY = BASE_DIR / "stkstats/data/derived/gap_entry_grid_summary_2025.csv"

ENTRY_K_LIST = [0.97, 0.96, 0.95]
TP_K = 1.07
SL_K = 0.96

# gap bins (ratio)
GAP_BINS = [-1, 0, 0.03, 0.07, 0.10, 0.15, 0.20, 0.25, 1]
GAP_LABELS = ["<0", "0~3", "3~7", "7~10", "10~15", "15~20", "20~25", "25+"]

# When BOTH and TP/SL hit in the same minute candle
AMBIG_SAME_MIN = "AMBIGUOUS_SAME_MIN"


# =========================
# Helpers
# =========================
def _to_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


def _choose_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


@dataclass
class DailySchema:
    date_col: str
    close_col: str


def detect_daily_schema(df: pd.DataFrame) -> DailySchema:
    date_col = _choose_col(df, ["일자", "dt", "date", "trd_dt", "base_dt"])
    close_col = _choose_col(df, ["cur_prc", "종가", "close", "close_pric", "cls_prc"])
    if not date_col or not close_col:
        raise RuntimeError(
            f"[DAILY] cannot detect schema. columns={df.columns.tolist()}"
        )
    return DailySchema(date_col=date_col, close_col=close_col)


@dataclass
class MinuteSchema:
    tm_col: str
    high_col: str
    low_col: str
    cur_col: str


def detect_minute_schema(df: pd.DataFrame) -> MinuteSchema:
    tm_col = _choose_col(df, ["cntr_tm", "tm", "time", "dtm", "stck_cntg_hour"])
    high_col = _choose_col(df, ["high_pric", "high", "고가"])
    low_col = _choose_col(df, ["low_pric", "low", "저가"])
    cur_col = _choose_col(df, ["cur_prc", "close", "종가"])
    if not tm_col or not high_col or not low_col or not cur_col:
        raise RuntimeError(
            f"[MINUTE] cannot detect schema. columns={df.columns.tolist()}"
        )
    return MinuteSchema(tm_col=tm_col, high_col=high_col, low_col=low_col, cur_col=cur_col)


def load_daily_close(
    stk_cd: str,
    limit_dt: str,
    cache: Dict[str, Tuple[pd.DataFrame, DailySchema]],
) -> Optional[float]:
    p = find_daily_path(stk_cd, limit_dt)
    if p is None:
        return None

    key = str(p.resolve())
    if key not in cache:
        df = load_parquet(p)
        schema = detect_daily_schema(df)
        cache[key] = (df, schema)

    df, schema = cache[key]

    target = str(limit_dt)  # 이미 8자리 문자열로 들어옴
    dt_series = df[schema.date_col].astype(str)
    hit = df.loc[dt_series == target]

    if hit.empty:
        return None

    v = _to_float(hit.iloc[0][schema.close_col])
    return abs(v) if v is not None else None


def load_minute_df(stk_cd: str, t1_dt: str) -> Optional[pd.DataFrame]:
    p = find_minute_path(MINUTE_DIR, stk_cd, t1_dt)
    if p is None:
        return None
    return load_parquet(p)


def resolve_both_with_minutes(
    mdf: pd.DataFrame,
    entry: float,
    tp: float,
    sl: float,
) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    """
    분봉으로 BOTH(=TP/SL 모두 일중 히트) 케이스 순서를 판정
    반환: (order, entry_tm, tp_tm, sl_tm)
      - order: "TP_FIRST" | "SL_FIRST" | AMBIGUOUS_SAME_MIN | "NONE"
    """
    schema = detect_minute_schema(mdf)

    df = mdf.copy()

    # 숫자화 + cur_prc 음수 처리(일부 API)
    for c in [schema.high_col, schema.low_col, schema.cur_col]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df[schema.cur_col] = df[schema.cur_col].abs()

    # 시간 오름차순 정렬 (저장 자체가 역순일 수 있음)
    df = df.sort_values(schema.tm_col, ascending=True)

    entry_tm = None
    tp_tm = None
    sl_tm = None

    entered = False
    for _, r in df.iterrows():
        tm = str(r[schema.tm_col])

        low = r[schema.low_col]
        high = r[schema.high_col]

        if pd.isna(low) or pd.isna(high):
            continue

        if not entered:
            if low <= entry:
                entered = True
                entry_tm = tm
            else:
                continue

        # after entry: check tp/sl hits
        hit_tp = high >= tp
        hit_sl = low <= sl

        if hit_tp and hit_sl:
            # same minute ambiguity
            tp_tm = tm
            sl_tm = tm
            return (AMBIG_SAME_MIN, entry_tm, tp_tm, sl_tm)

        if hit_tp and tp_tm is None:
            tp_tm = tm
            return ("TP_FIRST", entry_tm, tp_tm, sl_tm)

        if hit_sl and sl_tm is None:
            sl_tm = tm
            return ("SL_FIRST", entry_tm, tp_tm, sl_tm)

    return ("NONE", entry_tm, tp_tm, sl_tm)


def calc_ev(tp_cnt: float, sl_cnt: float, trades: float, tp_ret=0.07, sl_ret=-0.04) -> float:
    if trades <= 0:
        return float("nan")
    return (tp_cnt * tp_ret + sl_cnt * sl_ret) / trades

def year_from_yyyymmdd(dt8: str) -> str:
    s = str(dt8)
    return s[:4]

def find_daily_path(stk_cd: str, limit_dt: str) -> Optional[Path]:
    """
    daily_ohlc 파일 구조가 여러 가지일 수 있어서 전부 탐색.
    우선순위:
      1) daily_ohlc/{stk_cd}.parquet
      2) daily_ohlc/by_year/{year}/{stk_cd}.parquet
      3) daily_ohlc/by_year/{stk_cd}.parquet
      4) daily_ohlc/by_year/{year}.parquet  (연도 통합 파일인 경우)
    """
    y = year_from_yyyymmdd(limit_dt)
    candidates = [
        DAILY_DIR / f"{stk_cd}.parquet",
        DAILY_DIR / "by_year" / y / f"{stk_cd}.parquet",
        DAILY_DIR / "by_year" / f"{stk_cd}.parquet",
        DAILY_DIR / "by_year" / f"{y}.parquet",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


# =========================
# Main
# =========================
def main():
    print("[DBG] daily_dir:", DAILY_DIR.resolve())
    print("[DBG] sample daily file exists 005930:", (DAILY_DIR / "005930.parquet").exists())
    print("[DBG] sample by_year exists:", (DAILY_DIR / "by_year").exists())
    events = load_events(EVENTS_PATH)

    # Basic required columns
    need = ["stk_cd", "limit_dt", "t1_dt", "t1_open", "t1_high", "t1_low"]
    missing = [c for c in need if c not in events.columns]
    if missing:
        raise RuntimeError(f"[EVENTS] missing columns: {missing}. columns={events.columns.tolist()}")

    # Normalize types
    events["stk_cd"] = events["stk_cd"].astype(str).str.zfill(6)
    events["limit_dt"] = events["limit_dt"].astype(str)
    events["t1_dt"] = events["t1_dt"].astype(str)

    for c in ["t1_open", "t1_high", "t1_low"]:
        events[c] = pd.to_numeric(events[c], errors="coerce")

    # Attach limit_close from daily_ohlc/{stk_cd}.parquet
    daily_cache: Dict[str, Tuple[pd.DataFrame, DailySchema]] = {}
    limit_closes = []
    for _, r in events.iterrows():
        lc = load_daily_close(r["stk_cd"], r["limit_dt"], daily_cache)
        limit_closes.append(lc)
    events["limit_close"] = limit_closes

    # Drop rows without limit_close (cannot compute gap)
    before = len(events)
    events = events.dropna(subset=["limit_close"]).copy()
    after = len(events)
    print(f"[DBG] attached limit_close: kept {after}/{before} rows (dropped {before-after} missing limit_close)")

    # Gap
    events["gap"] = (events["t1_open"] - events["limit_close"]) / events["limit_close"]
    events["gap_bin"] = pd.cut(events["gap"], bins=GAP_BINS, labels=GAP_LABELS)

    # Run grid simulation
    rows = []
    for entry_k in ENTRY_K_LIST:
        for _, r in events.iterrows():
            stk_cd = r["stk_cd"]
            t1_dt = r["t1_dt"]

            t1_open = r["t1_open"]
            t1_high = r["t1_high"]
            t1_low = r["t1_low"]

            if pd.isna(t1_open) or pd.isna(t1_high) or pd.isna(t1_low):
                continue

            entry = float(t1_open) * float(entry_k)
            tp = entry * TP_K
            sl = entry * SL_K

            # daily pre-check
            if float(t1_low) > entry:
                result = "NO_ENTRY"
                order = None
                entry_tm = tp_tm = sl_tm = None
            else:
                hit_tp = float(t1_high) >= tp
                hit_sl = float(t1_low) <= sl

                if hit_tp and not hit_sl:
                    result = "TP"
                    order = "TP_ONLY"
                    entry_tm = tp_tm = sl_tm = None
                elif hit_sl and not hit_tp:
                    result = "SL"
                    order = "SL_ONLY"
                    entry_tm = tp_tm = sl_tm = None
                elif (not hit_tp) and (not hit_sl):
                    result = "NONE_AFTER_ENTRY"
                    order = "NONE"
                    entry_tm = tp_tm = sl_tm = None
                else:
                    # BOTH -> resolve with minutes
                    mdf = load_minute_df(stk_cd, t1_dt)
                    if mdf is None or len(mdf) == 0:
                        result = "AMBIG_NO_MINUTE"
                        order = "NO_MINUTE"
                        entry_tm = tp_tm = sl_tm = None
                    else:
                        ord2, entry_tm, tp_tm, sl_tm = resolve_both_with_minutes(mdf, entry, tp, sl)
                        order = ord2
                        if ord2 == "TP_FIRST":
                            result = "TP"
                        elif ord2 == "SL_FIRST":
                            result = "SL"
                        elif ord2 == AMBIG_SAME_MIN:
                            result = AMBIG_SAME_MIN
                        else:
                            result = "NONE_AFTER_ENTRY"

            rows.append(
                {
                    "stk_cd": stk_cd,
                    "limit_dt": r["limit_dt"],
                    "t1_dt": t1_dt,
                    "gap": r["gap"],
                    "gap_bin": r["gap_bin"],
                    "entry_k": entry_k,
                    "tp_k": TP_K,
                    "sl_k": SL_K,
                    "entry": entry,
                    "tp": tp,
                    "sl": sl,
                    "result": result,
                    "order": order,
                    "entry_tm": entry_tm,
                    "tp_tm": tp_tm,
                    "sl_tm": sl_tm,
                }
            )

        print(f"[DBG] done entry_k={entry_k}")

    detail = pd.DataFrame(rows)
    if detail.empty:
        raise RuntimeError("[ERR] detail result empty. check inputs/paths.")

    # Summary: gap_bin x entry_k
    # trades = TP + SL (only executed outcomes) OR include NONE_AFTER_ENTRY? -> here, TRADES means entered trades
    # We'll define:
    #   ENTERED = result != NO_ENTRY
    #   TRADES  = ENTERED count
    #   TP/SL   = counts
    #   EV uses TP=+7%, SL=-4% on entered trades
    detail["entered"] = detail["result"].ne("NO_ENTRY")

    g = detail[detail["entered"]].groupby(["gap_bin", "entry_k"])["result"].value_counts().unstack().fillna(0)

    # Ensure columns exist
    for c in ["TP", "SL"]:
        if c not in g.columns:
            g[c] = 0.0

    g["TRADES"] = g.sum(axis=1)
    g["WINRATE"] = g["TP"] / g["TRADES"]
    g["EV"] = [
        calc_ev(tp_cnt=row["TP"], sl_cnt=row["SL"], trades=row["TRADES"])
        for _, row in g.iterrows()
    ]

    summary = g.reset_index().sort_values(["gap_bin", "entry_k"])

    # Print a compact view
    print("\n=== GAP x ENTRY GRID SUMMARY (entered trades only) ===")
    print(summary[["gap_bin", "entry_k", "TRADES", "TP", "SL", "WINRATE", "EV"]].to_string(index=False))

    # Save outputs
    OUT_DETAIL.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(detail, OUT_DETAIL)
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")

    print(f"\n[DONE] saved detail:  {OUT_DETAIL}")
    print(f"[DONE] saved summary: {OUT_SUMMARY}")


if __name__ == "__main__":
    main()