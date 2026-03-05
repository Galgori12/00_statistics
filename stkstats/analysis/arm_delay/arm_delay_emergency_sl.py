"""
arm_delay + emergency SL(비상손절) 분석

- entry = t1_open * 0.97
- TP    = entry * 1.07
- SL    = entry * 0.96  (정규 손절: arm_delay 이후부터 유효)
- emergency SL = entry * (1 - EMERGENCY_DD)  (항상 즉시 유효)

실행 예:
  python -m stkstats.analysis.analyze_arm_delay_with_emergency_sl --arm 3 --emg 0.06
  python -m stkstats.analysis.analyze_arm_delay_with_emergency_sl --arm 3 --emg 0.07
"""

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
from typing import Optional, List, Dict
import argparse

import pandas as pd

from stkstats.analysis._common import find_minute_path, load_events, load_parquet


BOTH_OUT = Path("stkstats/data/derived/both_resolved_minutes_entry97_tp107_sl096_2025_04_12.parquet")
MINUTE_DIR = Path("stkstats/data/raw/minute_ohlc_t1")

ENTRY_K = 0.97
TP_K = 1.07
SL_K = 0.96


@dataclass
class SimResult:
    outcome: str  # TP / SL / EMG_SL / NONE_AFTER_ENTRY / NO_ENTRY / AMBIGUOUS_SAME_MIN / MINUTE_MISSING / BAD_MINUTE / EMPTY_MINUTE
    entry_tm: Optional[str]
    exit_tm: Optional[str]
    minutes_to_exit: Optional[int]


def _load_minute(stk_cd: str, t1_dt: str) -> Optional[pd.DataFrame]:
    p = find_minute_path(MINUTE_DIR, stk_cd, t1_dt)
    if p is None:
        return None
    df = load_parquet(p)
    if df is None or df.empty:
        return pd.DataFrame()

    need = {"cntr_tm", "high_pric", "low_pric"}
    if not need.issubset(df.columns):
        df2 = pd.DataFrame()
        df2.attrs["bad_cols"] = True
        return df2

    df = df.copy()
    df["cntr_tm"] = df["cntr_tm"].astype(str)
    df["high_pric"] = pd.to_numeric(df["high_pric"], errors="coerce").abs()
    df["low_pric"] = pd.to_numeric(df["low_pric"], errors="coerce").abs()
    df = df.dropna(subset=["cntr_tm", "high_pric", "low_pric"])
    if df.empty:
        return pd.DataFrame()

    df = df.sort_values("cntr_tm", ascending=True).reset_index(drop=True)
    return df


def _mins_between(t1: str, t2: str) -> Optional[int]:
    try:
        a = pd.to_datetime(t1, format="%Y%m%d%H%M%S", errors="raise")
        b = pd.to_datetime(t2, format="%Y%m%d%H%M%S", errors="raise")
        return int((b - a).total_seconds() // 60)
    except Exception:
        return None


def simulate_one(mdf: pd.DataFrame, entry: float, tp: float, sl: float, arm_delay_min: int, emg_dd: Optional[float]) -> SimResult:
    """
    emg_dd: 예) 0.06이면 entry*(1-0.06) 아래로 low가 가면 즉시 EMG_SL
            None이면 비상손절 없음
    """
    # entry 시점
    hit_entry = mdf.index[mdf["low_pric"] <= entry]
    if len(hit_entry) == 0:
        return SimResult("NO_ENTRY", None, None, None)

    entry_i = int(hit_entry[0])
    entry_tm = str(mdf.loc[entry_i, "cntr_tm"])
    entry_dt = pd.to_datetime(entry_tm, format="%Y%m%d%H%M%S", errors="coerce")

    emg_sl = None
    if emg_dd is not None:
        emg_sl = entry * (1.0 - float(emg_dd))

    for j in range(entry_i, len(mdf)):
        tm = str(mdf.loc[j, "cntr_tm"])
        cur_dt = pd.to_datetime(tm, format="%Y%m%d%H%M%S", errors="coerce")
        mins_since_entry = int((cur_dt - entry_dt).total_seconds() // 60) if (pd.notna(cur_dt) and pd.notna(entry_dt)) else (j - entry_i)

        hi = float(mdf.loc[j, "high_pric"])
        lo = float(mdf.loc[j, "low_pric"])

        tp_hit = hi >= tp

        emg_hit = (emg_sl is not None) and (lo <= emg_sl)

        sl_armed = mins_since_entry >= arm_delay_min
        sl_hit = sl_armed and (lo <= sl)

        # 같은 분에 여러 조건 동시에 -> 모호 처리
        if tp_hit and (sl_hit or emg_hit):
            return SimResult("AMBIGUOUS_SAME_MIN", entry_tm, tm, _mins_between(entry_tm, tm))

        if tp_hit:
            return SimResult("TP", entry_tm, tm, _mins_between(entry_tm, tm))

        if emg_hit:
            return SimResult("EMG_SL", entry_tm, tm, _mins_between(entry_tm, tm))

        if sl_hit:
            return SimResult("SL", entry_tm, tm, _mins_between(entry_tm, tm))

    return SimResult("NONE_AFTER_ENTRY", entry_tm, None, None)


def summarize(outcomes: List[str]) -> Dict[str, float]:
    s = pd.Series(outcomes)
    counts = s.value_counts(dropna=False).to_dict()

    # 손익판단: TP만 승, SL/EMG_SL은 패
    win = counts.get("TP", 0)
    loss = counts.get("SL", 0) + counts.get("EMG_SL", 0)

    trades = win + loss
    winrate = (win / trades) if trades else 0.0

    return {
        "N": len(outcomes),
        "TRADES": trades,
        "TP": win,
        "SL": counts.get("SL", 0),
        "EMG_SL": counts.get("EMG_SL", 0),
        "LOSS(SL+EMG)": loss,
        "WINRATE_%": round(winrate * 100, 2),
        "NO_ENTRY": counts.get("NO_ENTRY", 0),
        "NONE_AFTER_ENTRY": counts.get("NONE_AFTER_ENTRY", 0),
        "AMBIG": counts.get("AMBIGUOUS_SAME_MIN", 0),
        "MINUTE_MISSING": counts.get("MINUTE_MISSING", 0),
        "BAD_MINUTE": counts.get("BAD_MINUTE", 0),
        "EMPTY_MINUTE": counts.get("EMPTY_MINUTE", 0),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arm", type=int, default=3, help="SL arm delay minutes")
    ap.add_argument("--emg", type=float, default=None, help="emergency dd (e.g. 0.06 => -6%). omit for none")
    args = ap.parse_args()

    if not BOTH_OUT.exists():
        raise FileNotFoundError(f"both_out not found: {BOTH_OUT}")

    df = load_events(BOTH_OUT)
    df["stk_cd"] = df["stk_cd"].astype(str).str.zfill(6)
    df["t1_dt"] = df["t1_dt"].astype(str)
    df["t1_open"] = pd.to_numeric(df["t1_open"], errors="coerce").abs()

    outcomes: List[str] = []
    total = len(df)

    for i, r in enumerate(df.itertuples(index=False), start=1):
        stk_cd = r.stk_cd
        t1_dt = r.t1_dt
        t1_open = float(r.t1_open)

        entry = t1_open * ENTRY_K
        tp = entry * TP_K
        sl = entry * SL_K

        mdf = _load_minute(stk_cd, t1_dt)
        if mdf is None:
            outcomes.append("MINUTE_MISSING")
            continue
        if getattr(mdf, "attrs", {}).get("bad_cols", False):
            outcomes.append("BAD_MINUTE")
            continue
        if mdf.empty:
            outcomes.append("EMPTY_MINUTE")
            continue

        sr = simulate_one(mdf, entry=entry, tp=tp, sl=sl, arm_delay_min=args.arm, emg_dd=args.emg)
        outcomes.append(sr.outcome)

        if i % 200 == 0 or i == total:
            print(f"[PROG] {i}/{total}")

    summ = summarize(outcomes)
    print("\n=== ARM DELAY + EMERGENCY SL SUMMARY ===")
    print(f"ARM_DELAY_MIN = {args.arm}")
    print(f"EMERGENCY_DD  = {args.emg}  (None이면 비상손절 없음)")
    for k in ["N", "TRADES", "TP", "SL", "EMG_SL", "LOSS(SL+EMG)", "WINRATE_%", "NO_ENTRY", "NONE_AFTER_ENTRY", "AMBIG"]:
        print(f"{k:>16}: {summ[k]}")


if __name__ == "__main__":
    main()