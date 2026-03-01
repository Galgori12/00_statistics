from pathlib import Path
import pandas as pd
from stkstats.collectors.kiwoom_client import KiwoomClient
from stkstats.utils.io import load_parquet, save_parquet

RAW_DIR = Path("statistics/06_data/raw")
DAILY_DIR = RAW_DIR / "daily_ohlc"
EVENTS_PATH = RAW_DIR / "upper_limit_events_2023_2025.parquet"
OUT_DIR = RAW_DIR / "minute_ohlc_t1"

def find_next_trading_day(daily_df: pd.DataFrame, dt: str) -> str | None:
    d = daily_df.sort_values("dt")
    idx = d.index[d["dt"] == dt]
    if len(idx) == 0:
        return None
    pos = list(d.index).index(idx[0])
    if pos + 1 >= len(d):
        return None
    return str(d.iloc[pos + 1]["dt"])

def normalize_minute_rows(rows):
    return pd.DataFrame(rows)

def main():
    client = KiwoomClient()
    events = load_parquet(EVENTS_PATH)

    # (종목, dt) 루프
    for _, e in events.iterrows():
        stk_cd = str(e["stk_cd"]).strip()
        dt = str(e["dt"]).strip()

        daily_path = DAILY_DIR / f"{stk_cd}.parquet"
        if not daily_path.exists():
            continue
        daily = load_parquet(daily_path)
        if "dt" not in daily.columns:
            continue

        next_dt = find_next_trading_day(daily, dt)
        if not next_dt:
            continue

        out_path = OUT_DIR / stk_cd / f"{next_dt}.parquet"
        if out_path.exists():
            continue

        # ka10080 base_dt에 next_dt 넣고 1분봉( tic_scope=1 ) 요청
        rows = client.fetch_minute_all(stk_cd=stk_cd, base_dt=next_dt, tic_scope="1", upd_stkpc_tp="1")
        df = normalize_minute_rows(rows)
        df["stk_cd"] = stk_cd
        df["base_dt"] = next_dt
        save_parquet(df, out_path)
        print(f"[OK] minute saved: {stk_cd} {next_dt} rows={len(df)}")

if __name__ == "__main__":
    main()