from pathlib import Path
import pandas as pd

RAW_DIR = Path("stkstats/data/raw")
DAILY_DIR = RAW_DIR / "daily_ohlc"
OUT_PATH = RAW_DIR / "upper_limit_events_2023_2025.parquet"


START_DATE = "20230101"
END_DATE = "20251231"


def main():
    all_events = []

    files = list(DAILY_DIR.glob("*.parquet"))
    print(f"총 종목 수: {len(files)}")

    for fp in files:
        stk_cd = fp.stem
        df = pd.read_parquet(fp)

        if df.empty:
            continue

        df = df.sort_values("dt")

        # 기간 필터
        df = df[(df["dt"] >= START_DATE) & (df["dt"] <= END_DATE)]

        if df.empty:
            continue

        # 상한가 조건
        limit_df = df[df["pred_pre_sig"] == "1"]

        if limit_df.empty:
            continue

        # 다음날 데이터 붙이기
        df = df.reset_index(drop=True)

        for idx in limit_df.index:
            if idx + 1 >= len(df):
                continue

            today = df.loc[idx]
            next_day = df.loc[idx + 1]

            event = {
                "stk_cd": stk_cd,
                "limit_dt": today["dt"],
                "limit_trde_prica": today["trde_prica"],
                "t1_dt": next_day["dt"],
                "t1_open": next_day["open_pric"],
                "t1_high": next_day["high_pric"],
                "t1_low": next_day["low_pric"],
                "t1_close": next_day["cur_prc"],
            }

            all_events.append(event)

    if not all_events:
        print("상한가 이벤트 없음")
        return

    out_df = pd.DataFrame(all_events)
    out_df.to_parquet(OUT_PATH, index=False)

    print(f"저장 완료: {OUT_PATH}")
    print(f"총 이벤트 수: {len(out_df)}")


if __name__ == "__main__":
    main()