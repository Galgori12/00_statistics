from pathlib import Path
import pandas as pd
from stkstats.collectors.kiwoom_client import KiwoomClient
from stkstats.utils.io import save_parquet

RAW_DIR = Path("stkstats/data/raw")
STOCK_LIST = RAW_DIR / "stock_list.csv"
OUT_DIR = RAW_DIR / "daily_ohlc"

def normalize_daily_rows(rows):
    # 키움 응답은 문자열일 가능성 높음 → 숫자 변환은 raw에선 최소한만
    df = pd.DataFrame(rows)
    # 표준 컬럼명 확인: dt, open_pric, high_pric, low_pric, cur_prc, trde_prica, trde_qty, pred_pre_sig ...
    return df

def main():
    client = KiwoomClient()
    stocks = pd.read_csv(STOCK_LIST, dtype=str)
    print("DEBUG stock_list path =", STOCK_LIST)
    print("DEBUG stocks rows =", len(stocks), "cols =", list(stocks.columns))

    base_dt = "20251231"  # 2025 마지막 영업일이 아닐 수 있지만, base_dt는 기준일이므로 충분히 큰 날짜로
    for i, row in stocks.iterrows():
        stk_cd = row["stk_cd"].strip()
        out_path = OUT_DIR / f"{stk_cd}.parquet"
        if out_path.exists():
            continue  # 재실행 시 스킵

        rows = client.fetch_daily_all(stk_cd=stk_cd, base_dt=base_dt, upd_stkpc_tp="1")
        df = normalize_daily_rows(rows)
        save_parquet(df, out_path)
        print(f"[OK] daily saved: {stk_cd} rows={len(df)}")

if __name__ == "__main__":
    main()