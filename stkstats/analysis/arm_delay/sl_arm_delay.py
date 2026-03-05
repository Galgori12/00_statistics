"""
SL Arm Delay(손절 활성 지연) 시뮬레이션

- entry = t1_open * 0.97
- TP    = entry * 1.07
- SL    = entry * 0.96

핵심:
- 진입 후 arm_delay 분 동안은 SL 조건(low<=SL)을 무시
- TP는 즉시 유효(arm_delay 영향 없음)

실행:
  python -m stkstats.analysis.analyze_sl_arm_delay

입력:
  both_out: stkstats/data/derived/both_resolved_minutes_entry97_tp107_sl096_2025_04_12.parquet
  minute:   stkstats/data/raw/minute_ohlc_t1/{stk_cd}/{t1_dt}.parquet
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
import numpy as np

from stkstats.analysis._common import find_minute_path, load_events, load_parquet


# ====== PATHS (너 환경 기준) ======
BOTH_OUT = Path("stkstats/data/derived/both_resolved_minutes_entry97_tp107_sl096_2025_04_12.parquet")
MINUTE_DIR = Path("stkstats/data/raw/minute_ohlc_t1")


# ====== STRATEGY PARAMS ======
ENTRY_K = 0.97
TP_K = 1.07
SL_K = 0.96

# 테스트할 arm delay(분)
ARM_DELAYS = [0, 1, 3, 5]


@dataclass
class SimResult:
    outcome: str  # TP / SL / NONE_AFTER_ENTRY / NO_ENTRY / AMBIGUOUS_SAME_MIN / MINUTE_MISSING / BAD_MINUTE / EMPTY_MINUTE
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
        # 신호를 위해 특별 컬럼 삽입
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

    # 원본이 역순인 경우 많아서 오름차순 정렬
    df = df.sort_values("cntr_tm", ascending=True).reset_index(drop=True)
    return df


def _mins_between(t1: str, t2: str) -> Optional[int]:
    try:
        a = pd.to_datetime(t1, format="%Y%m%d%H%M%S", errors="raise")
        b = pd.to_datetime(t2, format="%Y%m%d%H%M%S", errors="raise")
        return int((b - a).total_seconds() // 60)
    except Exception:
        return None


def simulate_one(mdf: pd.DataFrame, entry: float, tp: float, sl: float, arm_delay_min: int) -> SimResult:
    # entry 시점: low <= entry 최초
    hit_entry = mdf.index[mdf["low_pric"] <= entry]
    if len(hit_entry) == 0:
        return SimResult("NO_ENTRY", None, None, None)

    entry_i = int(hit_entry[0])
    entry_tm = str(mdf.loc[entry_i, "cntr_tm"])

    # entry_i부터 순회
    for j in range(entry_i, len(mdf)):
        tm = str(mdf.loc[j, "cntr_tm"])
        mins_since_entry = j - entry_i  # 분봉이 1분 단위로 저장된다는 가정 (너 데이터가 1분봉이었음)

        tp_hit = float(mdf.loc[j, "high_pric"]) >= tp
        sl_armed = mins_since_entry >= arm_delay_min
        sl_hit = sl_armed and (float(mdf.loc[j, "low_pric"]) <= sl)

        # 같은 분에 TP/SL 동시 히트
        if tp_hit and sl_hit:
            return SimResult("AMBIGUOUS_SAME_MIN", entry_tm, tm, _mins_between(entry_tm, tm))

        if tp_hit:
            return SimResult("TP", entry_tm, tm, _mins_between(entry_tm, tm))

        if sl_hit:
            return SimResult("SL", entry_tm, tm, _mins_between(entry_tm, tm))

    return SimResult("NONE_AFTER_ENTRY", entry_tm, None, None)


def summarize(outcomes: List[str]) -> Dict[str, float]:
    s = pd.Series(outcomes)
    counts = s.value_counts(dropna=False).to_dict()

    win = counts.get("TP", 0)
    loss = counts.get("SL", 0)
    trades = win + loss
    winrate = (win / trades) if trades else 0.0

    return {
        "N": len(outcomes),
        "TP": win,
        "SL": loss,
        "TRADES(TP+SL)": trades,
        "WINRATE_%": round(winrate * 100, 2),
        "NO_ENTRY": counts.get("NO_ENTRY", 0),
        "NONE_AFTER_ENTRY": counts.get("NONE_AFTER_ENTRY", 0),
        "AMBIG": counts.get("AMBIGUOUS_SAME_MIN", 0),
        "MINUTE_MISSING": counts.get("MINUTE_MISSING", 0),
        "BAD_MINUTE": counts.get("BAD_MINUTE", 0),
        "EMPTY_MINUTE": counts.get("EMPTY_MINUTE", 0),
    }


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

    # 결과 저장용
    outcomes_by_delay: Dict[int, List[str]] = {d: [] for d in ARM_DELAYS}

    total = len(df)
    missing_minute = 0

    for i, r in enumerate(df.itertuples(index=False), start=1):
        stk_cd = r.stk_cd
        t1_dt = r.t1_dt
        t1_open = float(r.t1_open)

        entry = t1_open * ENTRY_K
        tp = entry * TP_K
        sl = entry * SL_K

        mdf = _load_minute(stk_cd, t1_dt)

        # minute 파일 관련 처리
        if mdf is None:
            missing_minute += 1
            for d in ARM_DELAYS:
                outcomes_by_delay[d].append("MINUTE_MISSING")
            continue

        if getattr(mdf, "attrs", {}).get("bad_cols", False):
            for d in ARM_DELAYS:
                outcomes_by_delay[d].append("BAD_MINUTE")
            continue

        if mdf.empty:
            for d in ARM_DELAYS:
                outcomes_by_delay[d].append("EMPTY_MINUTE")
            continue

        # delay별 시뮬
        for d in ARM_DELAYS:
            sr = simulate_one(mdf, entry=entry, tp=tp, sl=sl, arm_delay_min=d)
            outcomes_by_delay[d].append(sr.outcome)

        if i % 100 == 0 or i == total:
            print(f"[PROG] {i}/{total}")

    print("\n=== SL ARM DELAY SUMMARY (TP vs SL winrate) ===")
    rows = []
    for d in ARM_DELAYS:
        summ = summarize(outcomes_by_delay[d])
        summ["ARM_DELAY_MIN"] = d
        rows.append(summ)

    out = pd.DataFrame(rows).set_index("ARM_DELAY_MIN")
    # 보기 좋게 컬럼 순서
    cols = ["N", "TRADES(TP+SL)", "TP", "SL", "WINRATE_%", "NO_ENTRY", "NONE_AFTER_ENTRY", "AMBIG", "MINUTE_MISSING", "BAD_MINUTE", "EMPTY_MINUTE"]
    out = out[cols]
    print(out.to_string())

    print("\nNOTE: WINRATE_%는 TP/(TP+SL)로 계산 (NO_ENTRY/NONE/AMBIG 제외).")


if __name__ == "__main__":
    main()