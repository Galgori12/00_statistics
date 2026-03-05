import pandas as pd
from pathlib import Path

from stkstats.analysis._common import load_parquet

BASE = Path("stkstats/data")
EVENTS = BASE / "raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet"
MINUTE_DIR = BASE / "raw/minute_ohlc_t1"

df_events = load_parquet(EVENTS)

rows = []

for _, ev in df_events.iterrows():

    stk_cd = ev["stk_cd"]
    t1_dt = ev["t1_dt"]
    t1_open = ev["t1_open"]

    path = MINUTE_DIR / stk_cd / f"{t1_dt}.parquet"
    if not path.exists():
        continue

    df = load_parquet(path)

    df = df.sort_values("cntr_tm")

    df["time"] = pd.to_datetime(df["cntr_tm"])

    dip_rows = df[df["low_pric"] < t1_open]

    if len(dip_rows) == 0:
        continue

    dip_row = dip_rows.iloc[0]

    first_low = dip_row["low_pric"]

    dip_pct = (t1_open - first_low) / t1_open

    if dip_pct > 0.03:
        continue

    dip_time = dip_row["time"]

    after = df[df["cntr_tm"] >= dip_row["cntr_tm"]]

    max_high = after["high_pric"].max()

    upside = (max_high - first_low) / first_low

    rows.append({
        "dip_time": dip_time,
        "dip_pct": dip_pct,
        "upside": upside
    })

df = pd.DataFrame(rows)

df["minute"] = df["dip_time"].dt.strftime("%H:%M")


def time_bin(m):
    if m <= "09:03":
        return "09:00~09:03"
    elif m <= "09:05":
        return "09:03~09:05"
    elif m <= "09:10":
        return "09:05~09:10"
    else:
        return "09:10~"


df["time_bin"] = df["minute"].apply(time_bin)

summary = df.groupby("time_bin").agg(
    trades=("upside", "count"),
    avg_upside=("upside", "mean"),
    med_upside=("upside", "median")
).reset_index()

print(summary)