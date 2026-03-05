"""
12조합 매트릭스: entry_k x tp_k (arm_delay=3, sl_k=0.96 고정)

- entry = t1_open * entry_k
- tp    = entry * tp_k
- sl    = entry * 0.96
- arm_delay=3분: entry 후 3분 지나야 SL 유효 (TP는 즉시 유효)

출력:
- 각 조합별 TP/SL/TRADES/WINRATE/EV(%) + NO_ENTRY/NONE/AMBIG
- EV(%)는 TP/SL 트레이드 기준:  EV = p_win*(tp_k-1) - p_loss*(1-0.96)

실행:
  python -m stkstats.analysis.analyze_entry_tp_grid
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, Dict, List

import pandas as pd
import numpy as np

from stkstats.analysis._common import find_minute_path, load_events, load_parquet


BOTH_OUT = Path("stkstats/data/derived/both_resolved_minutes_entry97_tp107_sl096_2025_04_12.parquet")
MINUTE_DIR = Path("stkstats/data/raw/minute_ohlc_t1")

ARM_DELAY_MIN = 3
SL_K = 0.96

ENTRY_K_LIST = [0.97, 0.96, 0.95]
TP_K_LIST = [1.07, 1.08, 1.09, 1.10]


@dataclass
class SimResult:
    outcome: str  # TP / SL / NO_ENTRY / NONE_AFTER_ENTRY / AMBIGUOUS_SAME_MIN / MINUTE_MISSING / BAD_MINUTE / EMPTY_MINUTE


def load_minute(stk_cd: str, t1_dt: str) -> Tuple[Optional[pd.DataFrame], str]:
    p = find_minute_path(MINUTE_DIR, stk_cd, t1_dt)
    if p is None:
        return None, "MINUTE_MISSING"

    df = load_parquet(p)
    if df is None or df.empty:
        return None, "EMPTY_MINUTE"

    need = {"cntr_tm", "high_pric", "low_pric"}
    if not need.issubset(df.columns):
        return None, "BAD_MINUTE"

    df = df.copy()
    df["cntr_tm"] = df["cntr_tm"].astype(str)
    df["high_pric"] = pd.to_numeric(df["high_pric"], errors="coerce").abs()
    df["low_pric"] = pd.to_numeric(df["low_pric"], errors="coerce").abs()
    df = df.dropna(subset=["cntr_tm", "high_pric", "low_pric"])
    if df.empty:
        return None, "EMPTY_MINUTE"

    df = df.sort_values("cntr_tm", ascending=True).reset_index(drop=True)
    return df, "OK"


def simulate(mdf: pd.DataFrame, t1_open: float, entry_k: float, tp_k: float) -> SimResult:
    entry = t1_open * entry_k
    tp = entry * tp_k
    sl = entry * SL_K

    # entry: low <= entry 최초
    hit = mdf.index[mdf["low_pric"] <= entry]
    if len(hit) == 0:
        return SimResult("NO_ENTRY")

    entry_i = int(hit[0])
    entry_tm = mdf.loc[entry_i, "cntr_tm"]
    entry_dt = pd.to_datetime(entry_tm, format="%Y%m%d%H%M%S", errors="coerce")

    # entry 이후 순회
    for j in range(entry_i, len(mdf)):
        tm = mdf.loc[j, "cntr_tm"]
        cur_dt = pd.to_datetime(tm, format="%Y%m%d%H%M%S", errors="coerce")

        if pd.notna(entry_dt) and pd.notna(cur_dt):
            mins_since_entry = int((cur_dt - entry_dt).total_seconds() // 60)
        else:
            # fallback (대부분 1분봉이라 근사 가능)
            mins_since_entry = j - entry_i

        hi = float(mdf.loc[j, "high_pric"])
        lo = float(mdf.loc[j, "low_pric"])

        tp_hit = hi >= tp
        sl_hit = (mins_since_entry >= ARM_DELAY_MIN) and (lo <= sl)

        if tp_hit and sl_hit:
            return SimResult("AMBIGUOUS_SAME_MIN")
        if tp_hit:
            return SimResult("TP")
        if sl_hit:
            return SimResult("SL")

    return SimResult("NONE_AFTER_ENTRY")


def summarize(outcomes: List[str], tp_k: float) -> Dict[str, float]:
    s = pd.Series(outcomes)
    c = s.value_counts(dropna=False).to_dict()

    tp = c.get("TP", 0)
    sl = c.get("SL", 0)
    trades = tp + sl
    winrate = (tp / trades) if trades else 0.0

    # EV in "return units" (e.g. 0.01=1%)
    win_gain = (tp_k - 1.0)
    loss = (1.0 - SL_K)
    ev = winrate * win_gain - (1.0 - winrate) * loss if trades else 0.0

    return {
        "TP": tp,
        "SL": sl,
        "TRADES": trades,
        "WINRATE_%": round(winrate * 100, 2),
        "EV_%": round(ev * 100, 3),
        "NO_ENTRY": c.get("NO_ENTRY", 0),
        "NONE": c.get("NONE_AFTER_ENTRY", 0),
        "AMBIG": c.get("AMBIGUOUS_SAME_MIN", 0),
        "MINUTE_MISSING": c.get("MINUTE_MISSING", 0),
        "BAD_MINUTE": c.get("BAD_MINUTE", 0),
        "EMPTY_MINUTE": c.get("EMPTY_MINUTE", 0),
    }


def main():
    if not BOTH_OUT.exists():
        raise FileNotFoundError(f"both_out not found: {BOTH_OUT}")

    df = load_events(BOTH_OUT)
    for col in ["stk_cd", "t1_dt", "t1_open"]:
        if col not in df.columns:
            raise RuntimeError(f"Missing column in both_out: {col}")

    df["stk_cd"] = df["stk_cd"].astype(str).str.zfill(6)
    df["t1_dt"] = df["t1_dt"].astype(str)
    df["t1_open"] = pd.to_numeric(df["t1_open"], errors="coerce").abs()

    combos = [(ek, tk) for ek in ENTRY_K_LIST for tk in TP_K_LIST]
    outcomes_map: Dict[Tuple[float, float], List[str]] = {(ek, tk): [] for ek, tk in combos}

    total = len(df)

    # minute 캐시 (같은 이벤트에서 12번 재로딩 방지)
    for i, r in enumerate(df.itertuples(index=False), start=1):
        stk_cd = r.stk_cd
        t1_dt = r.t1_dt
        t1_open = float(r.t1_open)

        mdf, st = load_minute(stk_cd, t1_dt)
        if st != "OK":
            for ek, tk in combos:
                outcomes_map[(ek, tk)].append(st)
            continue

        for ek, tk in combos:
            sr = simulate(mdf, t1_open=t1_open, entry_k=ek, tp_k=tk)
            outcomes_map[(ek, tk)].append(sr.outcome)

        if i % 200 == 0 or i == total:
            print(f"[PROG] {i}/{total}")

    rows = []
    for ek, tk in combos:
        summ = summarize(outcomes_map[(ek, tk)], tp_k=tk)
        summ["ENTRY_K"] = ek
        summ["TP_K"] = tk
        rows.append(summ)

    out = pd.DataFrame(rows)
    out = out.sort_values(["EV_%", "WINRATE_%"], ascending=[False, False]).reset_index(drop=True)

    # 보기 좋게 출력
    cols = ["ENTRY_K", "TP_K", "TRADES", "TP", "SL", "WINRATE_%", "EV_%", "NO_ENTRY", "NONE", "AMBIG"]
    print("\n=== ENTRY x TP GRID (arm_delay=3, SL=0.96) ===")
    print(out[cols].to_string(index=False))

    # 상위 3개만 추가로 강조
    print("\n=== TOP 3 by EV_% ===")
    print(out[cols].head(3).to_string(index=False))


if __name__ == "__main__":
    main()