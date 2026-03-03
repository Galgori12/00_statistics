from __future__ import annotations

from pathlib import Path
import pandas as pd

from stkstats.utils.io import load_parquet, save_parquet

EVENTS_PATH = "stkstats/data/raw/archive/upper_limit_events_cleaned_minute_ok_2025_04_12.parquet"
DAILY_BY_YEAR = Path("stkstats/data/raw/daily_ohlc/by_year")
OUT_DIR = Path("stkstats/data/derived/daily_after_t1_120d")
OUT_ALL = Path("stkstats/data/derived/daily_after_t1_120d.parquet")

N_TRADING_DAYS = 120  # ≈ 6 months


def _load_daily_for_stk(stk_cd: str) -> pd.DataFrame:
    # 2023~2025 연도 파일을 이어붙임 (없는 연도는 스킵)
    dfs = []
    for y in ("2023", "2024", "2025"):
        p = DAILY_BY_YEAR / y / f"{stk_cd}.parquet"
        if p.exists():
            dfy = pd.read_parquet(p)
            dfs.append(dfy)
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)

    # 표준화
    if "dt" not in df.columns:
        return pd.DataFrame()
    df["dt"] = df["dt"].astype(str).str.strip().str[:8]

    # 정렬 + dt 중복 제거
    df = df.sort_values("dt").drop_duplicates(subset=["dt"], keep="last")
    return df


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    events = load_parquet(EVENTS_PATH).copy()
    # 필수 컬럼 정리
    events["stk_cd"] = events["stk_cd"].astype(str).str.zfill(6)
    events["t1_dt"] = events["t1_dt"].astype(str).str[:8]
    if "limit_dt" in events.columns:
        events["limit_dt"] = events["limit_dt"].astype(str).str[:8]
    else:
        events["limit_dt"] = ""

    all_rows = []

    total = len(events)
    for i, (_, e) in enumerate(events.iterrows(), start=1):
        stk_cd = e["stk_cd"]
        t1_dt = e["t1_dt"]
        limit_dt = e.get("limit_dt", "")

        daily = _load_daily_for_stk(stk_cd)
        if daily.empty:
            print(f"[WARN] ({i}/{total}) no daily found for stk_cd={stk_cd}")
            continue

        # t1_dt 이후(포함)부터 N 거래일 slice
        sub = daily[daily["dt"] >= t1_dt].copy()
        if sub.empty:
            print(f"[WARN] ({i}/{total}) no daily after t1_dt. stk_cd={stk_cd} t1_dt={t1_dt}")
            continue

        sub = sub.head(N_TRADING_DAYS).copy()

        # 필요한 컬럼만(없으면 생성)
        keep = [
            "dt",
            "open_pric", "high_pric", "low_pric", "cur_prc",
            "trde_qty", "trde_prica",
            "pred_pre", "pred_pre_sig",
        ]
        for c in keep:
            if c not in sub.columns:
                sub[c] = pd.NA
        sub = sub[keep]

        # 메타 컬럼
        sub["stk_cd"] = stk_cd
        sub["t1_dt"] = t1_dt
        sub["limit_dt"] = limit_dt

        # 이벤트별 저장 (분봉과 동일 구조)
        out_path = OUT_DIR / stk_cd / f"{t1_dt}.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        save_parquet(sub, out_path)

        all_rows.append(sub)
        if i % 50 == 0:
            print(f"[OK] built {i}/{total}")

    if all_rows:
        out_all = pd.concat(all_rows, ignore_index=True)
        save_parquet(out_all, OUT_ALL)
        print(f"[DONE] saved merged: {OUT_ALL} rows={len(out_all)}")
    else:
        print("[DONE] nothing built")


if __name__ == "__main__":
    main()