from pathlib import Path
import pandas as pd
import numpy as np

from stkstats.analysis._common import find_minute_path, load_parquet

MIN_DIR = Path("stkstats/data/raw/minute_ohlc_t1")
EVENTS = Path("stkstats/data/raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet")
OUT = Path("stkstats/data/derived/dip_time_ev_heatmap_2025.csv")

TP = 0.05
SL = 0.015


def load_minute(stk_cd, t1_dt):
    p = find_minute_path(MIN_DIR, stk_cd, t1_dt)
    if p is None:
        return None
    df = load_parquet(p)
    df["cntr_tm"] = df["cntr_tm"].astype(str)
    df = df.sort_values("cntr_tm")
    df["high_pric"] = df["high_pric"].abs()
    df["low_pric"] = df["low_pric"].abs()
    return df


def first_dip(df, t1_open):
    lows = df["low_pric"].values
    times = df["cntr_tm"].values

    for i in range(len(df)):
        if lows[i] < t1_open:
            dip = (t1_open - lows[i]) / t1_open
            tm = times[i][-6:]
            return i, dip, tm
    return None, None, None


def simulate(df, entry_idx, entry_price):
    tp = entry_price * (1 + TP)
    sl = entry_price * (1 - SL)

    highs = df["high_pric"].values
    lows = df["low_pric"].values

    for i in range(entry_idx + 1, len(df)):
        hit_tp = highs[i] >= tp
        hit_sl = lows[i] <= sl

        if hit_tp and hit_sl:
            return "AMBIG"
        if hit_tp:
            return "TP"
        if hit_sl:
            return "SL"

    return "NONE"


def time_bin(tm):
    t = int(tm[2:4]) * 60 + int(tm[4:6])

    if t <= 1:
        return "09:00-09:01"
    if t <= 2:
        return "09:01-09:02"
    if t <= 3:
        return "09:02-09:03"
    if t <= 5:
        return "09:03-09:05"
    return "09:05+"


def dip_bin(d):
    if d < 0.01:
        return "0-1%"
    if d < 0.02:
        return "1-2%"
    if d < 0.03:
        return "2-3%"
    return "3%+"


def main():
    events = load_parquet(EVENTS)

    rows = []

    for _, ev in events.iterrows():
        stk_cd = str(ev["stk_cd"]).zfill(6)
        t1_dt = str(ev["t1_dt"])
        t1_open = ev["t1_open"]

        df = load_minute(stk_cd, t1_dt)
        if df is None:
            continue

        idx, dip, tm = first_dip(df, t1_open)
        if idx is None:
            continue

        if dip > 0.03:
            continue

        res = simulate(df, idx, df["low_pric"].iloc[idx])

        rows.append({
            "dip": dip,
            "time": tm,
            "dip_bin": dip_bin(dip),
            "time_bin": time_bin(tm),
            "result": res
        })

    df = pd.DataFrame(rows)

    def ev(g):
        tp = (g["result"] == "TP").sum()
        sl = (g["result"] == "SL").sum()
        n = len(g)
        return (tp * TP - sl * SL) / n

    grp = df.groupby(["time_bin", "dip_bin"], as_index=False)

    summary = grp.agg(
        trades=("result", "size"),
        TP=("result", lambda s: (s == "TP").sum()),
        SL=("result", lambda s: (s == "SL").sum()),
    )

    summary["EV"] = (summary["TP"] * TP - summary["SL"] * SL) / summary["trades"]

    summary.to_csv(OUT, index=False)
    print(summary.sort_values("EV", ascending=False).head(20))


if __name__ == "__main__":
    main()