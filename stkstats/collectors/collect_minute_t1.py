# stkstats/collectors/collect_minute_t1.py
from __future__ import annotations

from pathlib import Path

import pandas as pd

from stkstats.collectors.kiwoom_client import KiwoomClient
from stkstats.utils.io import load_parquet, save_parquet

# ✅ 입력: 2025-04~12로 필터된 minute_ok 이벤트 파일
EVENTS_PATH = "stkstats/data/raw/archive/upper_limit_events_cleaned_minute_ok_2025_04_12.parquet"

RAW = Path("stkstats/data/raw")
OUT_DIR = RAW / "minute_ohlc_t1"

def normalize_minute_rows(rows):
    """
    ka10080 응답을 DataFrame으로 정리
    """
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # 숫자 컬럼 정리
    num_cols = ["cur_prc", "trde_qty", "open_pric", "high_pric", "low_pric", "pred_pre"]
    for c in num_cols:
        if c in df.columns:
            df[c] = (
                df[c]
                .astype(str)
                .str.replace(",", "", regex=False)
                .astype(float)
            )

    # 체결시간 정리
    if "cntr_tm" in df.columns:
        df["cntr_tm"] = df["cntr_tm"].astype(str).str.strip()

    return df


def _pick_first(row, keys):
    for k in keys:
        if k in row and str(row[k]).strip() not in ("", "nan", "None", ""):
            return str(row[k]).strip()
    return None


def main():
    client = KiwoomClient(sleep_sec=1.2, max_retries=8)
    events = load_parquet(EVENTS_PATH)
    total = len(events)

    # (종목, t1_dt) 루프
    for i, (_, e) in enumerate(events.iterrows(), start=1):
        stk_cd = _pick_first(e, ["stk_cd", "code", "종목코드"])
        dt = _pick_first(e, ["t1_dt", "dt", "base_dt", "limit_dt", "일자", "date"])

        if not stk_cd or not dt:
            print(f"[WARN] missing stk_cd/dt. keys={list(e.index)}")
            continue

        # YYYYMMDD 정규화
        dt = dt.replace("-", "").replace(".", "").replace("/", "")
        if len(dt) != 8 or not dt.isdigit():
            print(f"[WARN] bad dt format. stk_cd={stk_cd} dt={dt}")
            continue

        out_path = OUT_DIR / stk_cd / f"{dt}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if out_path.exists():
            continue

        # ✅ 하루치 1분봉만 가져오기
        rows = client.fetch_minute_one_day(
            stk_cd=stk_cd,
            base_dt=dt,
            tic_scope="1",        # 1분
            upd_stkpc_tp="1",     # 수정주가
        )

        df = normalize_minute_rows(rows)
        if df is None or len(df) == 0:
            print(f"[WARN] empty minute. stk_cd={stk_cd} dt={dt}")
            continue

        # ✅ 안전: 하루치만 남기기 (연속조회가 과거까지 내려간 경우 방어)
        if "cntr_tm" in df.columns:
            df["cntr_tm"] = df["cntr_tm"].astype(str).str.strip()
            df = df[df["cntr_tm"].str.startswith(dt)].copy()
            df = df.sort_values("cntr_tm").drop_duplicates(subset=["cntr_tm"], keep="last")

        df["stk_cd"] = stk_cd
        df["base_dt"] = dt

        save_parquet(df, out_path)
        print(f"[OK] ({i}/{total}) minute saved: {stk_cd} {dt} rows={len(df)}")


if __name__ == "__main__":
    main()