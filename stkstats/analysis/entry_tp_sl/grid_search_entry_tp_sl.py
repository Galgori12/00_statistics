from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd

from stkstats.analysis._common import find_minute_path as _find_minute_path, load_events, load_parquet, load_minute_df as _load_minute_df


# =========================
# Paths / Config
# =========================
BASE_DIR = Path(__file__).resolve().parents[3]  # suhoTrade/ (from analysis/entry_tp_sl/)
EVENTS_PATH = BASE_DIR / "stkstats/data/raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet"
DAILY_DIR = BASE_DIR / "stkstats/data/raw/daily_ohlc"
MINUTE_DIR = BASE_DIR / "stkstats/data/raw/minute_ohlc_t1"

OUT_DIR = BASE_DIR / "stkstats/data/derived"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_SUMMARY = OUT_DIR / "grid_entry_tp_sl_minutes_summary_2025.csv"
OUT_DEBUG = OUT_DIR / "grid_entry_tp_sl_minutes_debug_2025.txt"

# ===== Grid =====
ENTRY_K_LIST = [0.94, 0.95, 0.96, 0.97]
TP_LIST = [0.05, 0.06, 0.07, 0.08, 0.09, 0.10]
SL_LIST = [0.03, 0.04, 0.05, 0.06]

GAP_FILTERS = [
    ("NO_FILTER", None),
    ("gap<25", 0.25),
    ("gap<20", 0.20),
    ("gap<15", 0.15),
]

MIN_ENTERED_TRADES = 1
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


def load_daily_close(stk_cd: str, limit_dt: str, cache: Dict[str, Tuple[pd.DataFrame, DailySchema]]) -> Optional[float]:
    p = DAILY_DIR / f"{stk_cd}.parquet"
    if not p.exists():
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


def find_minute_path(stk_cd: str, t1_dt: str) -> Optional[Path]:
    return _find_minute_path(MINUTE_DIR, stk_cd, t1_dt)


def load_minute_df(stk_cd: str, t1_dt: str, cache: Dict[Tuple[str, str], Optional[pd.DataFrame]]) -> Optional[pd.DataFrame]:
    return _load_minute_df(MINUTE_DIR, stk_cd, t1_dt, cache=cache)


def minute_eod_close(mdf: pd.DataFrame) -> Optional[float]:
    """분봉 마지막 체결가(또는 close 대용)"""
    sch = detect_minute_schema(mdf)
    df = mdf.copy()
    df[sch.tm_col] = df[sch.tm_col].astype(str)
    df[sch.cur_col] = pd.to_numeric(df[sch.cur_col], errors="coerce").abs()
    df = df.dropna(subset=[sch.cur_col])
    if df.empty:
        return None
    df = df.sort_values(sch.tm_col, ascending=True)
    return float(df.iloc[-1][sch.cur_col])


def simulate_with_minutes(mdf: pd.DataFrame, entry: float, tp_price: float, sl_price: float) -> str:
    sch = detect_minute_schema(mdf)
    df = mdf.copy()

    for c in [sch.high_col, sch.low_col, sch.cur_col]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # ✅ 음수 방어
    df[sch.cur_col] = df[sch.cur_col].abs()
    df[sch.high_col] = df[sch.high_col].abs()
    df[sch.low_col] = df[sch.low_col].abs()

    df = df.sort_values(sch.tm_col, ascending=True)

    entered = False
    for _, r in df.iterrows():
        low = r[sch.low_col]
        high = r[sch.high_col]
        if pd.isna(low) or pd.isna(high):
            continue

        if not entered:
            if low <= entry:
                entered = True
            else:
                continue

        hit_tp = high >= tp_price
        hit_sl = low <= sl_price

        if hit_tp and hit_sl:
            return AMBIG_SAME_MIN
        if hit_tp:
            return "TP"
        if hit_sl:
            return "SL"

    if not entered:
        return "NO_ENTRY"
    return "NONE_AFTER_ENTRY"


def write_debug(lines: List[str]) -> None:
    OUT_DEBUG.write_text("\n".join(lines), encoding="utf-8-sig")
    print(f"[DBG] wrote: {OUT_DEBUG}")


def main():
    dbg: List[str] = []
    dbg.append(f"[DBG] BASE_DIR={BASE_DIR}")
    dbg.append(f"[DBG] EVENTS_PATH exists={EVENTS_PATH.exists()} {EVENTS_PATH}")
    dbg.append(f"[DBG] DAILY_DIR exists={DAILY_DIR.exists()} {DAILY_DIR}")
    dbg.append(f"[DBG] MINUTE_DIR exists={MINUTE_DIR.exists()} {MINUTE_DIR}")

    events = load_events(EVENTS_PATH)
    dbg.append(f"[DBG] events raw rows={len(events)} cols={events.columns.tolist()}")

    need = ["stk_cd", "limit_dt", "t1_dt", "t1_open"]
    miss = [c for c in need if c not in events.columns]
    if miss:
        raise RuntimeError(f"[EVENTS] missing columns: {miss}")

    events["stk_cd"] = events["stk_cd"].astype(str).str.zfill(6)
    events["limit_dt"] = events["limit_dt"].map(norm_yyyymmdd)
    events["t1_dt"] = events["t1_dt"].map(norm_yyyymmdd)
    events["t1_open"] = pd.to_numeric(events["t1_open"], errors="coerce")

    # t1_close는 있으면 쓰고, 아니면 분봉 EOD로 대체
    if "t1_close" in events.columns:
        events["t1_close_num"] = pd.to_numeric(events["t1_close"], errors="coerce")
    else:
        events["t1_close_num"] = float("nan")
    dbg.append(f"[DBG] t1_close_num NaN ratio={events['t1_close_num'].isna().mean():.4f}")

    # limit_close attach (가능한 만큼만)
    daily_cache: Dict[str, Tuple[pd.DataFrame, DailySchema]] = {}
    lc = []
    for _, r in events.iterrows():
        lc.append(load_daily_close(r["stk_cd"], r["limit_dt"], daily_cache))
    events["limit_close"] = lc
    dbg.append(f"[DBG] limit_close NaN ratio={events['limit_close'].isna().mean():.4f}")

    # ✅ 최소한 t1_open만 있으면 진행 (여기서 0행 되는 걸 방지)
    before = len(events)
    events = events.dropna(subset=["t1_open"]).copy()
    dbg.append(f"[DBG] events kept after dropna(t1_open)={len(events)}/{before}")

    # gap은 limit_close 있는 행만 계산
    events["gap"] = (events["t1_open"] - events["limit_close"]) / events["limit_close"]

    # minute exists count
    ex_cnt = 0
    for _, r in events.iterrows():
        p = find_minute_path(r["stk_cd"], r["t1_dt"])
        if p is not None and p.exists():
            ex_cnt += 1
    dbg.append(f"[DBG] minute exists count={ex_cnt}/{len(events)}")

    minute_cache: Dict[Tuple[str, str], Optional[pd.DataFrame]] = {}

    rows = []
    total_jobs = len(ENTRY_K_LIST) * len(TP_LIST) * len(SL_LIST) * len(GAP_FILTERS)
    job_i = 0

    for entry_k in ENTRY_K_LIST:
        for tp in TP_LIST:
            for sl in SL_LIST:
                for filt_name, filt_thr in GAP_FILTERS:
                    job_i += 1

                    if filt_thr is None:
                        sub = events
                    else:
                        # gap 필터는 gap이 NaN이면 제외 (limit_close 없는 행)
                        sub = events.dropna(subset=["gap"])
                        sub = sub[sub["gap"] < float(filt_thr)]

                    entered = 0
                    tp_cnt = 0
                    sl_cnt = 0
                    ambig_cnt = 0
                    none_cnt = 0
                    no_entry_cnt = 0
                    no_minute_cnt = 0

                    rets: List[float] = []

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

                        res = simulate_with_minutes(mdf, entry, tp_price, sl_price)

                        if res == "NO_ENTRY":
                            no_entry_cnt += 1
                            continue

                        entered += 1

                        if res == "TP":
                            tp_cnt += 1
                            rets.append(float(tp))
                        elif res == "SL":
                            sl_cnt += 1
                            rets.append(-float(sl))
                        elif res == AMBIG_SAME_MIN:
                            ambig_cnt += 1
                            rets.append(0.0)
                        else:
                            none_cnt += 1
                            # EOD close: t1_close_num 있으면 사용, 아니면 minute 마지막가 사용
                            t1_close = r["t1_close_num"]
                            if pd.isna(t1_close):
                                eod = minute_eod_close(mdf)
                                if eod is None:
                                    rets.append(0.0)
                                else:
                                    rets.append((float(eod) - entry) / entry)
                            else:
                                rets.append((float(t1_close) - entry) / entry)

                    ev = (sum(rets) / len(rets)) if rets else float("nan")
                    winrate = (tp_cnt / entered) if entered else 0.0

                    rows.append({
                        "filter": filt_name,
                        "entry_k": entry_k,
                        "tp": tp,
                        "sl": sl,
                        "entered_trades": entered,
                        "tp_cnt": tp_cnt,
                        "sl_cnt": sl_cnt,
                        "winrate": winrate,
                        "EV": ev,
                        "ambig_same_min": ambig_cnt,
                        "none_after_entry": none_cnt,
                        "no_entry": no_entry_cnt,
                        "no_minute_skipped": no_minute_cnt,
                    })

                    if job_i % 50 == 0 or job_i == total_jobs:
                        dbg.append(f"[DBG] progress {job_i}/{total_jobs}")

    res = pd.DataFrame(rows)
    if res.empty:
        dbg.append("[ERR] res empty")
        write_debug(dbg)
        print("[ERR] res empty. check debug txt.")
        return

    dbg.append("[DBG] entered_trades describe:\n" + res["entered_trades"].describe().to_string())
    dbg.append(f"[DBG] max_entered={res['entered_trades'].max()}")
    dbg.append(f"[DBG] max_EV={res['EV'].max()}")
    write_debug(dbg)

    # save
    res.sort_values(["filter", "entry_k", "tp", "sl"]).to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    print(f"[DONE] saved summary: {OUT_SUMMARY}")

    # print tops
    print("\n=== TOP 40 (no min-trades filter) ===")
    print(res.sort_values("EV", ascending=False).head(40).to_string(index=False))

    res["pass_min_trades"] = res["entered_trades"] >= MIN_ENTERED_TRADES
    top = res[res["pass_min_trades"]].sort_values("EV", ascending=False).head(40)
    print(f"\n=== TOP 40 (entered_trades >= {MIN_ENTERED_TRADES}) ===")
    print(top.to_string(index=False))


if __name__ == "__main__":
    main()