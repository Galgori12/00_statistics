"""
SL_ONLY bounce 분석 (진입 후 SL 도달 전 반등폭)

실행:
  python -m stkstats.analysis.analyze_sl_only_bounce

입력(커밋 7dfe2fc 기준):
  both_out : stkstats/data/derived/both_resolved_minutes_entry97_tp107_sl096_2025.parquet
  minute_dir: stkstats/data/raw/minute_ohlc_t1/{stk_cd}/{t1_dt}.parquet

출력:
  stkstats/data/derived/sl_only_bounce_2025.parquet
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from stkstats.analysis._common import find_minute_path, load_events, load_parquet, save_parquet


# ====== PATHS (커밋 기준 그대로) ======
BOTH_OUT = Path("stkstats/data/derived/both_resolved_minutes_entry97_tp107_sl096_2025_04_12.parquet")
MINUTE_DIR = Path("stkstats/data/raw/minute_ohlc_t1")
OUT_PATH = Path("stkstats/data/derived/sl_only_bounce_2025.parquet")


# ====== STRATEGY PARAMS ======
ENTRY_K = 0.97
TP_K = 1.07
SL_K = 0.96


@dataclass
class BounceResult:
    bounce_pct: Optional[float]
    entry_tm: Optional[str]
    sl_tm: Optional[str]
    minutes_to_sl: Optional[int]
    max_high: Optional[float]
    entry: float
    sl: float
    status: str  # OK / NO_ENTRY_TOUCH / NO_SL_TOUCH / MINUTE_MISSING / BAD_MINUTE_COLS / EMPTY_MINUTE


def _to_numeric_series(s: pd.Series) -> pd.Series:
    # cur_prc가 음수로 오는 케이스가 있어 abs 처리 (이미지/로그 기반)
    out = pd.to_numeric(s, errors="coerce")
    return out


def load_minute(stk_cd: str, t1_dt: str) -> Tuple[Optional[pd.DataFrame], str]:
    """
    minute 파일 로드 + 컬럼 정리
    반환: (df or None, status)
    """
    p = find_minute_path(MINUTE_DIR, stk_cd, t1_dt)
    if p is None:
        return None, "MINUTE_MISSING"

    try:
        df = load_parquet(p)
    except Exception:
        return None, "MINUTE_MISSING"

    if df is None or df.empty:
        return None, "EMPTY_MINUTE"

    # 필수 컬럼 확인
    need = {"cntr_tm", "high_pric", "low_pric"}
    if not need.issubset(df.columns):
        return None, "BAD_MINUTE_COLS"

    # 숫자형 변환
    df = df.copy()
    df["high_pric"] = _to_numeric_series(df["high_pric"]).abs()
    df["low_pric"] = _to_numeric_series(df["low_pric"]).abs()

    # cntr_tm 정리 (문자열 유지, 정렬은 문자열로도 가능하지만 안전하게)
    df["cntr_tm"] = df["cntr_tm"].astype(str)

    # 시간 오름차순 (원본은 역순인 경우가 많음)
    df = df.sort_values("cntr_tm", ascending=True).reset_index(drop=True)

    # high/low NaN 제거
    df = df.dropna(subset=["cntr_tm", "high_pric", "low_pric"])
    if df.empty:
        return None, "EMPTY_MINUTE"

    return df, "OK"


def analyze_sl_only_bounce(minute_df: pd.DataFrame, entry: float, sl: float) -> BounceResult:
    """
    entry 터치 이후 ~ sl 터치 시점까지 최고가(max_high)로 bounce_pct 계산
    bounce_pct = (max_high - entry) / entry
    """
    # entry 터치: low <= entry 최초 시점
    entry_pos = minute_df.index[minute_df["low_pric"] <= entry]
    if len(entry_pos) == 0:
        return BounceResult(
            bounce_pct=None,
            entry_tm=None,
            sl_tm=None,
            minutes_to_sl=None,
            max_high=None,
            entry=entry,
            sl=sl,
            status="NO_ENTRY_TOUCH",
        )
    entry_i = int(entry_pos[0])
    entry_tm = str(minute_df.loc[entry_i, "cntr_tm"])

    # sl 터치: entry 이후 low <= sl 최초 시점
    after = minute_df.iloc[entry_i:]
    sl_pos = after.index[after["low_pric"] <= sl]
    if len(sl_pos) == 0:
        return BounceResult(
            bounce_pct=None,
            entry_tm=entry_tm,
            sl_tm=None,
            minutes_to_sl=None,
            max_high=None,
            entry=entry,
            sl=sl,
            status="NO_SL_TOUCH",
        )
    sl_i = int(sl_pos[0])
    sl_tm = str(minute_df.loc[sl_i, "cntr_tm"])

    window = minute_df.iloc[entry_i : sl_i + 1]
    max_high = float(window["high_pric"].max())
    bounce_pct = float((max_high - entry) / entry)

    # minutes_to_sl 계산 (YYYYMMDDHHMMSS 문자열에서 분 단위 차이)
    minutes_to_sl = None
    try:
        t_entry = pd.to_datetime(entry_tm, format="%Y%m%d%H%M%S", errors="raise")
        t_sl = pd.to_datetime(sl_tm, format="%Y%m%d%H%M%S", errors="raise")
        minutes_to_sl = int((t_sl - t_entry).total_seconds() // 60)
    except Exception:
        minutes_to_sl = None

    return BounceResult(
        bounce_pct=bounce_pct,
        entry_tm=entry_tm,
        sl_tm=sl_tm,
        minutes_to_sl=minutes_to_sl,
        max_high=max_high,
        entry=entry,
        sl=sl,
        status="OK",
    )


def main():
    if not BOTH_OUT.exists():
        raise FileNotFoundError(f"both_out not found: {BOTH_OUT}")

    df = load_events(BOTH_OUT)

    # 기대 컬럼 체크
    for c in ["stk_cd", "t1_dt", "t1_open", "result"]:
        if c not in df.columns:
            raise RuntimeError(f"Missing column in both_out: {c}")

    # SL_ONLY만
    df_sl = df[df["result"] == "SL_ONLY"].copy()
    if df_sl.empty:
        print("[INFO] No SL_ONLY rows found.")
        return

    # 타입 정리
    df_sl["stk_cd"] = df_sl["stk_cd"].astype(str).str.zfill(6)
    df_sl["t1_dt"] = df_sl["t1_dt"].astype(str)
    df_sl["t1_open"] = pd.to_numeric(df_sl["t1_open"], errors="coerce").abs()

    rows = []
    total = len(df_sl)
    ok_cnt = 0

    for i, r in enumerate(df_sl.itertuples(index=False), start=1):
        stk_cd = getattr(r, "stk_cd")
        t1_dt = getattr(r, "t1_dt")
        t1_open = float(getattr(r, "t1_open"))

        entry = t1_open * ENTRY_K
        sl = entry * SL_K

        mdf, st = load_minute(stk_cd, t1_dt)
        if st != "OK":
            br = BounceResult(
                bounce_pct=None,
                entry_tm=None,
                sl_tm=None,
                minutes_to_sl=None,
                max_high=None,
                entry=entry,
                sl=sl,
                status=st,
            )
        else:
            br = analyze_sl_only_bounce(mdf, entry=entry, sl=sl)

        d = {
            "stk_cd": stk_cd,
            "t1_dt": t1_dt,
            "t1_open": t1_open,
            "entry": br.entry,
            "sl": br.sl,
            "bounce_pct": br.bounce_pct,
            "max_high": br.max_high,
            "entry_tm": br.entry_tm,
            "sl_tm": br.sl_tm,
            "minutes_to_sl": br.minutes_to_sl,
            "status": br.status,
        }
        rows.append(d)

        if br.status == "OK":
            ok_cnt += 1

        if i % 50 == 0 or i == total:
            print(f"[PROG] {i}/{total} processed | OK={ok_cnt}")

    out_df = pd.DataFrame(rows)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(out_df, OUT_PATH)
    print(f"[DONE] saved: {OUT_PATH} rows={len(out_df)} OK={ok_cnt}")

    # ====== QUICK STATS ======
    ok = out_df[out_df["status"] == "OK"].copy()
    if ok.empty:
        print("[WARN] No OK rows to summarize.")
        return

    # bounce_pct 분포 요약
    print("\n=== BOUNCE_PCT (SL_ONLY, OK only) ===")
    print(ok["bounce_pct"].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9, 0.95]))

    # 구간별 비중 (필터 후보로 바로 쓰기 좋게)
    bins = [-np.inf, 0.005, 0.01, 0.015, 0.02, 0.03, np.inf]  # 0.5%,1%,1.5%,2%,3%
    labels = ["<=0.5%", "0.5~1%", "1~1.5%", "1.5~2%", "2~3%", ">3%"]
    ok["bounce_bin"] = pd.cut(ok["bounce_pct"], bins=bins, labels=labels)
    print("\n=== BOUNCE BIN COUNTS ===")
    print(ok["bounce_bin"].value_counts(dropna=False).sort_index())

    print("\n=== BOUNCE BIN RATIO ===")
    print((ok["bounce_bin"].value_counts(normalize=True, dropna=False).sort_index() * 100).round(2))

    # 참고: SL까지 걸린 시간(분) 분포
    if ok["minutes_to_sl"].notna().any():
        print("\n=== MINUTES_TO_SL (OK only) ===")
        print(ok["minutes_to_sl"].describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.9, 0.95]))


if __name__ == "__main__":
    main()
