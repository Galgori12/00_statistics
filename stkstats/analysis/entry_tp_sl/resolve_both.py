"""
분봉 기준 진입/TP/SL 순서 판정 (entry 97%, TP 107%, SL 96%).
실행: python -m stkstats.analysis.entry_tp_sl.resolve_both
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd

from stkstats.analysis._common import load_events, load_parquet, save_parquet


# 입력 (너가 이미 확정한 1068 이벤트)
EVENTS_PATH = "stkstats/data/raw/archive/upper_limit_events_cleaned_minute_ok_2025_04_12.parquet"

# 분봉 저장 위치 (이미 수집 완료)
MIN_DIR = Path("stkstats/data/raw/minute_ohlc_t1")

# 일봉 저장 위치 (t1_open 없을 때 보조로 씀)
DAILY_BY_YEAR = Path("stkstats/data/raw/daily_ohlc/by_year")

# 출력
OUT_PATH = "stkstats/data/derived/both_resolved_minutes_entry97_tp107_sl096_2025_04_12.parquet"

ENTRY_MULT = 0.97
TP_MULT = 1.07
SL_MULT = 0.96

# rows 너무 적으면(단기과열/거래정지 등) 품질 낮음 표기용
MIN_ROWS_OK = 200


def _zfill6(x) -> str:
    return str(x).strip().zfill(6)


def _yyyymmdd(x) -> str:
    s = str(x).strip()
    return s.replace("-", "").replace(".", "").replace("/", "")[:8]


def _load_t1_open_from_daily(stk_cd: str, t1_dt: str) -> float | None:
    # t1_dt 연도 파일 우선, 없으면 주변 연도도 체크
    years = [t1_dt[:4], "2025", "2024", "2023"]
    for y in years:
        p = DAILY_BY_YEAR / str(y) / f"{stk_cd}.parquet"
        if not p.exists():
            continue
        df = load_parquet(p)
        if "dt" not in df.columns:
            continue
        df["dt"] = df["dt"].astype(str).str[:8]
        row = df[df["dt"] == t1_dt]
        if len(row) == 0:
            continue
        if "open_pric" in row.columns:
            v = row["open_pric"].iloc[0]
            try:
                return float(v)
            except Exception:
                return None
    return None


def _to_float_series(df: pd.DataFrame, col: str) -> pd.Series:
    s = df[col]
    # 이미 numeric이면 그대로
    if pd.api.types.is_numeric_dtype(s):
        return s.astype(float)
    # 문자열 숫자 처리
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False), errors="coerce")


def resolve_one_day(min_df: pd.DataFrame, t1_open: float) -> dict:
    """
    1분봉으로 진입/TP/SL 순서 판정.
    - entry: low <= t1_open*0.97
    - tp: high >= entry*1.07
    - sl: low <= entry*0.96
    - 같은 분봉에서 tp/sl 동시 충족 -> AMBIGUOUS_SAME_MIN
    """
    if min_df is None or len(min_df) == 0:
        return {"result": "NO_MINUTE", "entry_tm": None, "tp_tm": None, "sl_tm": None}

    # 컬럼 방어
    for c in ["cntr_tm", "high_pric", "low_pric"]:
        if c not in min_df.columns:
            return {"result": "BAD_COLS", "entry_tm": None, "tp_tm": None, "sl_tm": None}

    df = min_df.copy()
    df["cntr_tm"] = df["cntr_tm"].astype(str).str.strip()

    # 숫자화
    df["high_pric"] = _to_float_series(df, "high_pric").abs()
    df["low_pric"] = _to_float_series(df, "low_pric").abs()

    # 시간 정렬(오름차순)
    df = df.sort_values("cntr_tm")

    entry = float(t1_open) * ENTRY_MULT
    tp = entry * TP_MULT
    sl = entry * SL_MULT

    # 진입 시점(처음 low<=entry)
    entry_hits = df[df["low_pric"] <= entry]
    if entry_hits.empty:
        return {"result": "NO_ENTRY", "entry_tm": None, "tp_tm": None, "sl_tm": None}

    entry_tm = entry_hits["cntr_tm"].iloc[0]

    # 진입 이후 구간
    after = df[df["cntr_tm"] >= entry_tm].copy()

    tp_hits = after[after["high_pric"] >= tp]
    sl_hits = after[after["low_pric"] <= sl]

    tp_tm = tp_hits["cntr_tm"].iloc[0] if not tp_hits.empty else None
    sl_tm = sl_hits["cntr_tm"].iloc[0] if not sl_hits.empty else None

    if tp_tm is None and sl_tm is None:
        return {"result": "NONE_AFTER_ENTRY", "entry_tm": entry_tm, "tp_tm": None, "sl_tm": None}

    if tp_tm is not None and sl_tm is None:
        return {"result": "TP_ONLY", "entry_tm": entry_tm, "tp_tm": tp_tm, "sl_tm": None}

    if tp_tm is None and sl_tm is not None:
        return {"result": "SL_ONLY", "entry_tm": entry_tm, "tp_tm": None, "sl_tm": sl_tm}

    # 둘 다 있는 경우: 먼저 발생한 쪽
    if tp_tm < sl_tm:
        return {"result": "TP_FIRST", "entry_tm": entry_tm, "tp_tm": tp_tm, "sl_tm": sl_tm}
    if sl_tm < tp_tm:
        return {"result": "SL_FIRST", "entry_tm": entry_tm, "tp_tm": tp_tm, "sl_tm": sl_tm}
    return {"result": "AMBIGUOUS_SAME_MIN", "entry_tm": entry_tm, "tp_tm": tp_tm, "sl_tm": sl_tm}


def main():
    out_dir = Path(OUT_PATH).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    events = load_events(EVENTS_PATH)
    events["stk_cd"] = events["stk_cd"].astype(str).apply(_zfill6)
    # minute_ok 파일은 보통 t1_dt가 있음
    if "t1_dt" not in events.columns:
        raise RuntimeError("EVENTS_PATH에 t1_dt 컬럼이 없습니다.")
    events["t1_dt"] = events["t1_dt"].astype(str).apply(_yyyymmdd)

    total = len(events)
    rows = []

    for i, (_, e) in enumerate(events.iterrows(), start=1):
        stk_cd = e["stk_cd"]
        t1_dt = e["t1_dt"]

        # t1_open 확보 (이벤트에 있으면 우선)
        t1_open = None
        for k in ["t1_open", "open_pric", "t1_open_pric"]:
            if k in e.index and str(e[k]).strip() not in ("", "nan", "None"):
                try:
                    t1_open = float(e[k])
                except Exception:
                    t1_open = None
                break
        if t1_open is None:
            t1_open = _load_t1_open_from_daily(stk_cd, t1_dt)

        if t1_open is None or pd.isna(t1_open) or t1_open <= 0:
            rows.append({
                "stk_cd": stk_cd, "t1_dt": t1_dt,
                "result": "NO_T1_OPEN", "entry_tm": None, "tp_tm": None, "sl_tm": None,
                "rows": 0, "data_quality": "NO_T1_OPEN",
            })
            continue

        mp = MIN_DIR / stk_cd / f"{t1_dt}.parquet"
        if not mp.exists():
            rows.append({
                "stk_cd": stk_cd, "t1_dt": t1_dt,
                "result": "NO_MINUTE_FILE", "entry_tm": None, "tp_tm": None, "sl_tm": None,
                "rows": 0, "data_quality": "NO_MINUTE_FILE",
            })
            continue

        mdf = load_parquet(mp)
        # 혹시 여러 날짜 섞였으면 t1_dt로 필터
        if "cntr_tm" in mdf.columns:
            mdf["cntr_tm"] = mdf["cntr_tm"].astype(str).str.strip()
            mdf = mdf[mdf["cntr_tm"].str.startswith(t1_dt)].copy()

        info = resolve_one_day(mdf, t1_open=float(t1_open))
        nrows = int(len(mdf)) if mdf is not None else 0
        dq = "OK" if nrows >= MIN_ROWS_OK else "LOW_ROWS"

        rows.append({
            "stk_cd": stk_cd,
            "t1_dt": t1_dt,
            "t1_open": float(t1_open),
            "entry": float(t1_open) * ENTRY_MULT,
            "tp": float(t1_open) * ENTRY_MULT * TP_MULT,
            "sl": float(t1_open) * ENTRY_MULT * SL_MULT,
            "result": info["result"],
            "entry_tm": info["entry_tm"],
            "tp_tm": info["tp_tm"],
            "sl_tm": info["sl_tm"],
            "rows": nrows,
            "data_quality": dq,
        })

        if i % 50 == 0:
            print(f"[OK] processed {i}/{total}")

    out = pd.DataFrame(rows)
    save_parquet(out, OUT_PATH)

    # 요약 출력
    print("\n=== RESULT COUNTS ===")
    print(out["result"].value_counts(dropna=False))
    print("\n=== DATA_QUALITY ===")
    print(out["data_quality"].value_counts(dropna=False))
    print(f"\n[DONE] saved: {OUT_PATH} rows={len(out)}")


if __name__ == "__main__":
    main()
