from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd

from stkstats.analysis._common import load_events, load_parquet, load_minute_df, save_parquet


# =========================
# Paths / Config
# =========================
BASE_DIR = Path(__file__).resolve().parents[3]  # suhoTrade/ (from analysis/entry_tp_sl/)
EVENTS_PATH = BASE_DIR / "stkstats/data/raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet"
DAILY_DIR = BASE_DIR / "stkstats/data/raw/daily_ohlc"
MINUTE_DIR = BASE_DIR / "stkstats/data/raw/minute_ohlc_t1"

OUT_DIR = BASE_DIR / "stkstats/data/derived"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_SUMMARY = OUT_DIR / "tp_sl_grid_minutes_summary_2025.csv"
OUT_TOP_TRADES = OUT_DIR / "tp_sl_grid_minutes_top_examples_2025.parquet"

# Grid (원하는대로 늘려도 됨)
ENTRY_K_LIST = [0.95]  # 지금 결론이 -5% 우세라 우선 고정
TP_LIST = [0.05, 0.06, 0.07, 0.08, 0.09, 0.10]
SL_LIST = [0.03, 0.04, 0.05, 0.06]

# Gap filters (ratio)
GAP_FILTERS = [
    ("NO_FILTER", None),
    ("gap<25", 0.25),
    ("gap<20", 0.20),
    ("gap<15", 0.15),
]

AMBIG_SAME_MIN = "AMBIGUOUS_SAME_MIN"


# =========================
# Utils
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


def norm_yyyymmdd(x) -> str:
    if x is None:
        return ""
    s = str(x)
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8]


@dataclass
class DailySchema:
    date_col: str
    close_col: str


def detect_daily_schema(df: pd.DataFrame) -> DailySchema:
    date_col = _choose_col(df, ["dt", "일자", "date", "base_dt", "trd_dt"])
    close_col = _choose_col(df, ["cur_prc", "종가", "close", "close_pric", "cls_prc", "lst_pric"])
    if not date_col or not close_col:
        raise RuntimeError(f"[DAILY] cannot detect schema. columns={df.columns.tolist()}")
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
        raise RuntimeError(f"[MINUTE] cannot detect schema. columns={df.columns.tolist()}")
    return MinuteSchema(tm_col=tm_col, high_col=high_col, low_col=low_col, cur_col=cur_col)


def find_daily_path(stk_cd: str) -> Optional[Path]:
    p = DAILY_DIR / f"{stk_cd}.parquet"
    return p if p.exists() else None


def load_daily_close(
    stk_cd: str,
    limit_dt: str,
    cache: Dict[str, Tuple[pd.DataFrame, DailySchema]],
) -> Optional[float]:
    p = find_daily_path(stk_cd)
    if p is None:
        return None

    key = str(p.resolve())
    if key not in cache:
        df = load_parquet(p)
        schema = detect_daily_schema(df)
        cache[key] = (df, schema)

    df, schema = cache[key]
    target = norm_yyyymmdd(limit_dt)
    dt_series = df[schema.date_col].map(norm_yyyymmdd)
    hit = df.loc[dt_series == target]
    if hit.empty:
        return None

    v = _to_float(hit.iloc[0][schema.close_col])
    return abs(v) if v is not None else None


def load_minute_df(stk_cd: str, t1_dt: str, cache: Dict[Tuple[str, str], Optional[pd.DataFrame]]) -> Optional[pd.DataFrame]:
    key = (stk_cd, t1_dt)
    if key in cache:
        return cache[key]

    df = load_minute_df(MINUTE_DIR, stk_cd, t1_dt, cache=cache)
    return df


def simulate_with_minutes(
    mdf: pd.DataFrame,
    entry: float,
    tp_price: float,
    sl_price: float,
) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    schema = detect_minute_schema(mdf)
    df = mdf.copy()

    for c in [schema.high_col, schema.low_col, schema.cur_col]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df[schema.cur_col] = df[schema.cur_col].abs()

    df = df.sort_values(schema.tm_col, ascending=True)

    entered = False
    entry_tm = None

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

        hit_tp = high >= tp_price
        hit_sl = low <= sl_price

        if hit_tp and hit_sl:
            return (AMBIG_SAME_MIN, entry_tm, tm, AMBIG_SAME_MIN)
        if hit_tp:
            return ("TP", entry_tm, tm, "TP_FIRST")
        if hit_sl:
            return ("SL", entry_tm, tm, "SL_FIRST")

    if not entered:
        return ("NO_ENTRY", None, None, None)
    return ("NONE_AFTER_ENTRY", entry_tm, None, "NONE")


def calc_ev(tp_cnt: float, sl_cnt: float, trades: float, tp_ret: float, sl_ret: float) -> float:
    if trades <= 0:
        return float("nan")
    return (tp_cnt * tp_ret + sl_cnt * sl_ret) / trades


def main():
    events = load_events(EVENTS_PATH)

    need = ["stk_cd", "limit_dt", "t1_dt", "t1_open"]
    miss = [c for c in need if c not in events.columns]
    if miss:
        raise RuntimeError(f"[EVENTS] missing columns: {miss}")

    events["stk_cd"] = events["stk_cd"].astype(str).str.zfill(6)
    events["limit_dt"] = events["limit_dt"].map(norm_yyyymmdd)
    events["t1_dt"] = events["t1_dt"].map(norm_yyyymmdd)
    events["t1_open"] = pd.to_numeric(events["t1_open"], errors="coerce")

    daily_cache: Dict[str, Tuple[pd.DataFrame, DailySchema]] = {}
    limit_close = []
    for _, r in events.iterrows():
        limit_close.append(load_daily_close(r["stk_cd"], r["limit_dt"], daily_cache))
    events["limit_close"] = limit_close
    events = events.dropna(subset=["limit_close", "t1_open"]).copy()
    events["gap"] = (events["t1_open"] - events["limit_close"]) / events["limit_close"]

    minute_cache: Dict[Tuple[str, str], Optional[pd.DataFrame]] = {}
    results_rows = []
    top_examples = []

    for entry_k in ENTRY_K_LIST:
        for tp in TP_LIST:
            for sl in SL_LIST:
                for filt_name, filt_thr in GAP_FILTERS:
                    if filt_thr is None:
                        sub = events
                    else:
                        sub = events[events["gap"] < float(filt_thr)]

                    tp_cnt = sl_cnt = ambig_cnt = none_cnt = no_entry_cnt = no_minute_cnt = 0
                    trades_entered = 0
                    examples_this = []

                    for _, r in sub.iterrows():
                        stk_cd = r["stk_cd"]
                        t1_dt = r["t1_dt"]
                        t1_open = float(r["t1_open"])

                        entry = t1_open * float(entry_k)
                        tp_price = entry * (1.0 + float(tp))
                        sl_price = entry * (1.0 - float(sl))

                        mdf = load_minute_df(stk_cd, t1_dt, minute_cache)
                        if mdf is None or len(mdf) == 0:
                            no_minute_cnt += 1
                            continue

                        res, entry_tm, exit_tm, order = simulate_with_minutes(mdf, entry, tp_price, sl_price)

                        if res == "NO_ENTRY":
                            no_entry_cnt += 1
                            continue

                        trades_entered += 1

                        if res == "TP":
                            tp_cnt += 1
                        elif res == "SL":
                            sl_cnt += 1
                        elif res == AMBIG_SAME_MIN:
                            ambig_cnt += 1
                        else:
                            none_cnt += 1

                        if len(examples_this) < 5 and res in ("TP", "SL", AMBIG_SAME_MIN):
                            examples_this.append({
                                "filter": filt_name,
                                "entry_k": entry_k,
                                "tp": tp,
                                "sl": sl,
                                "stk_cd": stk_cd,
                                "limit_dt": r["limit_dt"],
                                "t1_dt": t1_dt,
                                "gap": float(r["gap"]),
                                "entry": entry,
                                "tp_price": tp_price,
                                "sl_price": sl_price,
                                "result": res,
                                "order": order,
                                "entry_tm": entry_tm,
                                "exit_tm": exit_tm,
                            })

                    winrate = (tp_cnt / trades_entered) if trades_entered else 0.0
                    ev = calc_ev(tp_cnt, sl_cnt, trades_entered, tp_ret=tp, sl_ret=-sl)

                    results_rows.append({
                        "filter": filt_name,
                        "entry_k": entry_k,
                        "tp": tp,
                        "sl": sl,
                        "entered_trades": trades_entered,
                        "tp_cnt": tp_cnt,
                        "sl_cnt": sl_cnt,
                        "winrate": winrate,
                        "EV": ev,
                        "ambig_same_min": ambig_cnt,
                        "none_after_entry": none_cnt,
                        "no_entry": no_entry_cnt,
                        "no_minute_skipped": no_minute_cnt,
                    })

                    top_examples.extend(examples_this)
                    print(f"[DBG] done filter={filt_name} entry={entry_k} tp={tp} sl={sl} trades={trades_entered} EV={ev:.6f}")

    res = pd.DataFrame(results_rows)
    res = res.sort_values("EV", ascending=False)

    print("\n=== TOP 30 (minute-based) ===")
    print(res.head(30).to_string(index=False))

    res.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    print(f"\n[DONE] saved summary: {OUT_SUMMARY}")

    if top_examples:
        exdf = pd.DataFrame(top_examples)
        save_parquet(exdf, OUT_TOP_TRADES)
        print(f"[DONE] saved examples: {OUT_TOP_TRADES}")


if __name__ == "__main__":
    main()
