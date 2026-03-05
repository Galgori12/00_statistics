"""
arm_delay=3분 정밀 분석:
- entry 이후 3분 동안의 최저가(min_low)로 '지연구간 최대 역행(drawdown)' 계산
- drawdown_pct = (min_low - entry) / entry  (음수)
- 지연구간 내 SL(entry*0.96) 하회 여부(=슬 뚫었다가 살아난 케이스) 체크
- outcome(TP/SL/NONE_AFTER_ENTRY/NO_ENTRY/AMBIG) 별 분포도 출력

실행:
  python -m stkstats.analysis.analyze_arm_delay_dd

입력:
  both_out : stkstats/data/derived/both_resolved_minutes_entry97_tp107_sl096_2025_04_12.parquet
  minute_dir: stkstats/data/raw/minute_ohlc_t1/{stk_cd}/{t1_dt}.parquet

출력(저장):
  stkstats/data/derived/armdelay3_drawdown_2025.parquet
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from stkstats.analysis._common import find_minute_path, load_events, load_minute_df, save_parquet


# ===== PATHS =====
BOTH_OUT = Path("stkstats/data/derived/both_resolved_minutes_entry97_tp107_sl096_2025_04_12.parquet")
MINUTE_DIR = Path("stkstats/data/raw/minute_ohlc_t1")
OUT_PATH = Path("stkstats/data/derived/armdelay3_drawdown_2025.parquet")

# ===== STRATEGY =====
ENTRY_K = 0.97
TP_K = 1.07
SL_K = 0.96

ARM_DELAY_MIN = 3  # 정밀분석 대상


@dataclass
class SimResult:
    outcome: str
    entry_tm: Optional[str]
    exit_tm: Optional[str]
    minutes_to_exit: Optional[int]
    dd_min_low_delay: Optional[float]
    dd_pct_delay: Optional[float]
    sl_breach_during_delay: Optional[bool]


def _load_minute(stk_cd: str, t1_dt: str) -> Tuple[Optional[pd.DataFrame], str]:
    p = find_minute_path(MINUTE_DIR, stk_cd, t1_dt)
    if p is None:
        return None, "MINUTE_MISSING"

    df = pd.read_parquet(p)
    if df is None or df.empty:
        return None, "EMPTY_MINUTE"

    need = {"cntr_tm", "high_pric", "low_pric"}
    if not need.issubset(df.columns):
        return None, "BAD_MINUTE_COLS"

    df = df.copy()
    df["cntr_tm"] = df["cntr_tm"].astype(str)
    df["high_pric"] = pd.to_numeric(df["high_pric"], errors="coerce").abs()
    df["low_pric"] = pd.to_numeric(df["low_pric"], errors="coerce").abs()
    df = df.dropna(subset=["cntr_tm", "high_pric", "low_pric"])
    if df.empty:
        return None, "EMPTY_MINUTE"

    df = df.sort_values("cntr_tm", ascending=True).reset_index(drop=True)
    return df, "OK"


def _to_dt(s: str) -> pd.Timestamp:
    return pd.to_datetime(s, format="%Y%m%d%H%M%S", errors="raise")


def _mins_between(t1: str, t2: str) -> Optional[int]:
    try:
        return int((_to_dt(t2) - _to_dt(t1)).total_seconds() // 60)
    except Exception:
        return None


def simulate_with_dd(mdf: pd.DataFrame, entry: float, tp: float, sl: float, arm_delay_min: int) -> SimResult:
    # entry: low <= entry 최초
    hit = mdf.index[mdf["low_pric"] <= entry]
    if len(hit) == 0:
        return SimResult("NO_ENTRY", None, None, None, None, None, None)

    entry_i = int(hit[0])
    entry_tm = str(mdf.loc[entry_i, "cntr_tm"])
    entry_dt = _to_dt(entry_tm)

    # --- delay 구간 drawdown 계산 (entry시점 포함, entry 후 arm_delay_min 분까지) ---
    dd_window_end_dt = entry_dt + pd.Timedelta(minutes=arm_delay_min)
    # <= end (포함)로 잡음
    window_mask = mdf["cntr_tm"].map(_to_dt) <= dd_window_end_dt
    window = mdf.iloc[entry_i:][window_mask.iloc[entry_i:].values]

    if window.empty:
        dd_min_low = float(mdf.loc[entry_i, "low_pric"])
    else:
        dd_min_low = float(window["low_pric"].min())

    dd_pct = float((dd_min_low - entry) / entry)
    sl_breach = bool(dd_min_low <= sl)

    # --- 본 시뮬: TP 즉시 유효, SL은 arm_delay 이후부터 유효 ---
    for j in range(entry_i, len(mdf)):
        tm = str(mdf.loc[j, "cntr_tm"])
        cur_dt = _to_dt(tm)
        mins_since_entry = int((cur_dt - entry_dt).total_seconds() // 60)

        tp_hit = float(mdf.loc[j, "high_pric"]) >= tp
        sl_hit = (mins_since_entry >= arm_delay_min) and (float(mdf.loc[j, "low_pric"]) <= sl)

        if tp_hit and sl_hit:
            return SimResult("AMBIGUOUS_SAME_MIN", entry_tm, tm, _mins_between(entry_tm, tm), dd_min_low, dd_pct, sl_breach)
        if tp_hit:
            return SimResult("TP", entry_tm, tm, _mins_between(entry_tm, tm), dd_min_low, dd_pct, sl_breach)
        if sl_hit:
            return SimResult("SL", entry_tm, tm, _mins_between(entry_tm, tm), dd_min_low, dd_pct, sl_breach)

    return SimResult("NONE_AFTER_ENTRY", entry_tm, None, None, dd_min_low, dd_pct, sl_breach)


def _bin_dd(dd_pct_series: pd.Series) -> pd.Series:
    # dd_pct는 음수. 보기 좋게 "하락폭"으로 bin.
    dd = (-dd_pct_series).clip(lower=0)
    bins = [-np.inf, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.07, 0.10, np.inf]
    labels = ["<=0.5%", "0.5~1%", "1~2%", "2~3%", "3~4%", "4~5%", "5~7%", "7~10%", ">10%"]
    return pd.cut(dd, bins=bins, labels=labels)


def main():
    if not BOTH_OUT.exists():
        raise FileNotFoundError(f"both_out not found: {BOTH_OUT}")

    df = load_events(BOTH_OUT)
    for c in ["stk_cd", "t1_dt", "t1_open"]:
        if c not in df.columns:
            raise RuntimeError(f"Missing column: {c}")

    df["stk_cd"] = df["stk_cd"].astype(str).str.zfill(6)
    df["t1_dt"] = df["t1_dt"].astype(str)
    df["t1_open"] = pd.to_numeric(df["t1_open"], errors="coerce").abs()

    rows = []
    total = len(df)

    for i, r in enumerate(df.itertuples(index=False), start=1):
        stk_cd = r.stk_cd
        t1_dt = r.t1_dt
        t1_open = float(r.t1_open)

        entry = t1_open * ENTRY_K
        tp = entry * TP_K
        sl = entry * SL_K

        mdf, st = _load_minute(stk_cd, t1_dt)
        if st != "OK":
            sr = SimResult(st, None, None, None, None, None, None)
        else:
            sr = simulate_with_dd(mdf, entry=entry, tp=tp, sl=sl, arm_delay_min=ARM_DELAY_MIN)

        rows.append(
            {
                "stk_cd": stk_cd,
                "t1_dt": t1_dt,
                "t1_open": t1_open,
                "entry": entry,
                "tp": tp,
                "sl": sl,
                "arm_delay_min": ARM_DELAY_MIN,
                "outcome": sr.outcome,
                "entry_tm": sr.entry_tm,
                "exit_tm": sr.exit_tm,
                "minutes_to_exit": sr.minutes_to_exit,
                "dd_min_low_delay": sr.dd_min_low_delay,
                "dd_pct_delay": sr.dd_pct_delay,
                "sl_breach_during_delay": sr.sl_breach_during_delay,
            }
        )

        if i % 200 == 0 or i == total:
            print(f"[PROG] {i}/{total}")

    out = pd.DataFrame(rows)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(out, OUT_PATH)
    print(f"\n[DONE] saved: {OUT_PATH} rows={len(out)}")

    # ===== SUMMARY =====
    ok = out[out["outcome"].isin(["TP", "SL", "NONE_AFTER_ENTRY", "NO_ENTRY", "AMBIGUOUS_SAME_MIN"])].copy()
    trade = out[out["outcome"].isin(["TP", "SL"])].copy()

    print("\n=== OUTCOME COUNTS (arm_delay=3) ===")
    print(out["outcome"].value_counts())

    if not trade.empty:
        print("\n=== WINRATE (TP/(TP+SL)) ===")
        winrate = trade["outcome"].eq("TP").mean()
        print(f"TRADES={len(trade)} TP={trade['outcome'].eq('TP').sum()} SL={trade['outcome'].eq('SL').sum()} WINRATE={winrate*100:.2f}%")

    # drawdown stats (entry 발생한 케이스만)
    entry_ok = out[out["outcome"] != "NO_ENTRY"].copy()
    entry_ok = entry_ok[entry_ok["dd_pct_delay"].notna()].copy()

    if entry_ok.empty:
        print("\n[WARN] No drawdown stats available.")
        return

    print("\n=== DRAW DOWN DURING DELAY (dd_pct_delay, negative) ===")
    print(entry_ok["dd_pct_delay"].describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]))

    # bin
    entry_ok["dd_bin"] = _bin_dd(entry_ok["dd_pct_delay"])
    print("\n=== DELAY DRAWDOWN BIN RATIO (all entries) ===")
    print((entry_ok["dd_bin"].value_counts(normalize=True).sort_index() * 100).round(2))

    # SL breach during delay ratio
    print("\n=== SL BREACH DURING DELAY RATIO (min_low within delay <= SL) ===")
    print((entry_ok["sl_breach_during_delay"].value_counts(normalize=True) * 100).round(2))

    # by outcome
    print("\n=== DELAY DRAWDOWN BIN RATIO BY OUTCOME (TP vs SL) ===")
    for k in ["TP", "SL"]:
        sub = entry_ok[entry_ok["outcome"] == k].copy()
        if sub.empty:
            continue
        sub["dd_bin"] = _bin_dd(sub["dd_pct_delay"])
        print(f"\n[{k}] n={len(sub)}")
        print((sub["dd_bin"].value_counts(normalize=True).sort_index() * 100).round(2))
        print("SL breach during delay (%):")
        print((sub["sl_breach_during_delay"].value_counts(normalize=True) * 100).round(2))


if __name__ == "__main__":
    main()