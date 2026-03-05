"""
쿨다운 진입 분석

기존 전략:
entry = t1_open * 0.97
TP = entry * 1.07
SL = entry * 0.96

변형:
entry 터치 시각이 09:00~09:05 사이면 진입 무효 처리

실행:
python -m stkstats.analysis.analyze_cooldown_entry
"""

from pathlib import Path
import pandas as pd
import numpy as np

from stkstats.analysis._common import find_minute_path, load_events, load_parquet

BOTH_OUT = Path("stkstats/data/derived/both_resolved_minutes_entry97_tp107_sl096_2025_04_12.parquet")
MINUTE_DIR = Path("stkstats/data/raw/minute_ohlc_t1")

ENTRY_K = 0.97
TP_K = 1.07
SL_K = 0.96

COOLDOWN_MIN = 5  # 🔥 여기만 바꾸면 됨 (5분 쿨다운)


def get_entry_time(minute_df, entry):
    hit = minute_df[minute_df["low_pric"] <= entry]
    if hit.empty:
        return None
    return hit.iloc[0]["cntr_tm"]


def minute_to_minutes_from_open(cntr_tm):
    # 20250723153500 -> datetime
    dt = pd.to_datetime(cntr_tm, format="%Y%m%d%H%M%S")
    market_open = dt.replace(hour=9, minute=0, second=0)
    return int((dt - market_open).total_seconds() // 60)


def main():
    df = load_events(BOTH_OUT)

    df = df.copy()
    df["stk_cd"] = df["stk_cd"].astype(str).str.zfill(6)
    df["t1_dt"] = df["t1_dt"].astype(str)
    df["t1_open"] = pd.to_numeric(df["t1_open"], errors="coerce").abs()

    results = []

    for r in df.itertuples(index=False):
        stk_cd = r.stk_cd
        t1_dt = r.t1_dt
        t1_open = r.t1_open

        entry = t1_open * ENTRY_K

        minute_path = find_minute_path(MINUTE_DIR, stk_cd, t1_dt)
        if minute_path is None:
            continue

        mdf = load_parquet(minute_path)
        if mdf.empty:
            continue

        mdf["low_pric"] = pd.to_numeric(mdf["low_pric"], errors="coerce").abs()
        mdf["high_pric"] = pd.to_numeric(mdf["high_pric"], errors="coerce").abs()
        mdf["cntr_tm"] = mdf["cntr_tm"].astype(str)
        mdf = mdf.sort_values("cntr_tm")

        entry_tm = get_entry_time(mdf, entry)
        if entry_tm is None:
            results.append("NO_ENTRY")
            continue

        mins_from_open = minute_to_minutes_from_open(entry_tm)

        # 🔥 쿨다운 적용
        if mins_from_open < COOLDOWN_MIN:
            results.append("SKIP_COOLDOWN")
        else:
            results.append(r.result)

    df["cooldown_result"] = results

    # NO_ENTRY + SKIP 제외하고 재계산
    valid = df[~df["cooldown_result"].isin(["NO_ENTRY", "SKIP_COOLDOWN"])]

    win = valid["cooldown_result"].isin(["TP_ONLY", "TP_FIRST"]).sum()
    loss = valid["cooldown_result"].isin(["SL_ONLY", "SL_FIRST"]).sum()

    total = win + loss
    winrate = win / total if total > 0 else 0

    print("\n=== COOLDOWN RESULT ===")
    print(f"Cooldown minutes: {COOLDOWN_MIN}")
    print(f"Trades: {total}")
    print(f"Win: {win}")
    print(f"Loss: {loss}")
    print(f"Winrate: {round(winrate*100,2)}%")

    print("\nOriginal Winrate was about 32.7%")

if __name__ == "__main__":
    main()