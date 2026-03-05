import pandas as pd
from pathlib import Path

from stkstats.analysis._common import load_parquet

BASE = Path("stkstats/data")
EVENTS = BASE / "raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet"
MINUTE_DIR = BASE / "raw/minute_ohlc_t1"

MIN_DIP = 0.005   # 0.5%  (여기만 바꿔가며 테스트)
MAX_DIP = 0.03

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

    # dip 계산
    df["dip_pct"] = (t1_open - df["low_pric"]) / t1_open

    # 의미 있는 dip
    dips = df[(df["dip_pct"] >= MIN_DIP) & (df["dip_pct"] <= MAX_DIP)]

    if len(dips) == 0:
        continue

    dip_row = dips.iloc[0]

    first_low = dip_row["low_pric"]
    dip_pct = dip_row["dip_pct"]

    after = df[df["cntr_tm"] >= dip_row["cntr_tm"]]

    max_high = after["high_pric"].max()

    upside = (max_high - first_low) / first_low

    rows.append({
        "dip_pct": dip_pct,
        "upside": upside,
        "dip_time": dip_row["time"]
    })

df = pd.DataFrame(rows)

print("trades:", len(df))
print("avg_upside:", df["upside"].mean())
print("median_upside:", df["upside"].median())