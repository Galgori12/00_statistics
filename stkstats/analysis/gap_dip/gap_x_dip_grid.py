import math
import pandas as pd
from pathlib import Path

from stkstats.analysis._common import find_minute_path, load_events, load_parquet

# =============================================================================
# Paths (schema.md 기준)
# =============================================================================
BASE = Path("stkstats/data")
EVENTS = BASE / "raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet"
MINUTE_DIR = BASE / "raw/minute_ohlc_t1"

# =============================================================================
# Params (원하면 여기만 바꿔서 실험)
# =============================================================================
# first dip 범위 (0~8%)
MAX_DIP = 0.08

# 오프닝 3분 안쪽만 쓸지 (네 통계에서 93%가 09:00~09:03)
OPENING_ONLY = True
OPENING_CUTOFF = "09:03"  # HH:MM

# TP/SL (entry 대비)
TP = 0.05
SL = 0.015

# Dip bins: 0~8%를 1% 단위
DIP_BINS = [0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08]
DIP_LABELS = ["0~1", "1~2", "2~3", "3~4", "4~5", "5~6", "6~7", "7~8"]

# Gap bins (너 로그 분포랑 동일)
GAP_BINS = [-math.inf, 0, 0.03, 0.07, 0.10, 0.15, 0.20, 0.25, math.inf]
GAP_LABELS = ["<0", "0~3", "3~7", "7~10", "10~15", "15~20", "20~25", "25+"]

# 저장 파일명
OUT = BASE / "derived" / f"gap_x_dip_grid_tp{int(TP*100)}_sl{int(SL*1000)}_opening{int(OPENING_ONLY)}.csv"


# =============================================================================
# Helpers
# =============================================================================
def ensure_price_cols(df: pd.DataFrame) -> pd.DataFrame:
    """가격 컬럼 숫자화 + abs 처리(키움 cur_prc 음수 이슈 포함)"""
    for c in ["open_pric", "high_pric", "low_pric", "cur_prc"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").abs()
    for c in ["trde_qty", "acc_trde_qty"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def parse_time(df: pd.DataFrame) -> pd.DataFrame:
    """cntr_tm(YYYYMMDDHHMMSS) -> dt, hhmm"""
    df["dt"] = pd.to_datetime(df["cntr_tm"], errors="coerce")
    df["hhmm"] = df["dt"].dt.strftime("%H:%M")
    return df


def compute_gap_if_missing(df_events: pd.DataFrame) -> pd.DataFrame:
    """gap 컬럼이 없으면 자동 생성.
    1순위: limit_close 사용 (정확)
    2순위: limit_trde_prica 사용 (근사)
    """
    if "gap" in df_events.columns:
        return df_events

    # 숫자형 변환
    df_events["t1_open"] = pd.to_numeric(df_events["t1_open"], errors="coerce")

    if "limit_close" in df_events.columns:
        df_events["limit_close"] = pd.to_numeric(df_events["limit_close"], errors="coerce")
        df_events["gap"] = (df_events["t1_open"] - df_events["limit_close"]) / df_events["limit_close"]
        print("[DBG] gap computed from limit_close")
    elif "limit_trde_prica" in df_events.columns:
        df_events["limit_trde_prica"] = pd.to_numeric(df_events["limit_trde_prica"], errors="coerce")
        df_events["gap"] = (df_events["t1_open"] - df_events["limit_trde_prica"]) / df_events["limit_trde_prica"]
        print("[WARN] gap computed from limit_trde_prica (proxy). Prefer limit_close if available.")
    else:
        raise KeyError("gap missing and cannot compute it: need limit_close or limit_trde_prica")

    return df_events


def find_first_dip(df: pd.DataFrame, t1_open: float):
    """시간 오름차순 df에서 low < open 첫 행을 찾아 dip_pct/low/time 리턴"""
    df["dip_pct"] = (t1_open - df["low_pric"]) / t1_open
    dips = df[df["low_pric"] < t1_open]
    if dips.empty:
        return None
    r = dips.iloc[0]
    return r


def judge_tp_sl(df_after: pd.DataFrame, entry_price: float, tp: float, sl: float) -> str:
    """분봉 순서대로 TP/SL 먼저 맞는지 판별 (같은 분봉이면 AMBIGUOUS)"""
    tp_price = entry_price * (1 + tp)
    sl_price = entry_price * (1 - sl)

    for _, r in df_after.iterrows():
        hit_tp = (r["high_pric"] >= tp_price)
        hit_sl = (r["low_pric"] <= sl_price)

        if hit_tp and hit_sl:
            return "AMBIGUOUS_SAME_MIN"
        if hit_tp:
            return "TP"
        if hit_sl:
            return "SL"

    return "NONE_AFTER_ENTRY"


# =============================================================================
# Main
# =============================================================================
def main():
    if not EVENTS.exists():
        raise FileNotFoundError(f"EVENTS not found: {EVENTS}")
    if not MINUTE_DIR.exists():
        raise FileNotFoundError(f"MINUTE_DIR not found: {MINUTE_DIR}")

    df_events = load_events(EVENTS)
    print("[DBG] events cols:", list(df_events.columns))
    print("[DBG] events rows:", len(df_events))

    # gap 없으면 생성
    df_events = compute_gap_if_missing(df_events)

    # 결측 정리
    df_events = df_events.dropna(subset=["gap", "stk_cd", "t1_dt", "t1_open"])

    # gap_bin
    df_events["gap_bin"] = pd.cut(
        df_events["gap"],
        bins=GAP_BINS,
        labels=GAP_LABELS,
        right=True,
        include_lowest=True,
    )

    rows = []
    skipped_no_minute = 0
    skipped_bad_minute = 0
    skipped_missing_cols = 0
    skipped_no_dip = 0
    skipped_over_maxdip = 0
    skipped_opening = 0
    skipped_bad_time = 0

    need_cols = {"cntr_tm", "high_pric", "low_pric"}  # open_pric 없어도 dip/TP/SL 계산 가능

    for _, ev in df_events.iterrows():
        stk_cd = str(ev["stk_cd"])
        t1_dt = str(ev["t1_dt"])
        t1_open = float(ev["t1_open"])
        gap_bin = ev["gap_bin"]

        path = find_minute_path(MINUTE_DIR, stk_cd, t1_dt)
        if path is None:
            skipped_no_minute += 1
            continue

        try:
            dfm = load_parquet(path)
        except Exception:
            skipped_bad_minute += 1
            continue

        # 컬럼 체크
        if not need_cols.issubset(dfm.columns):
            skipped_missing_cols += 1
            continue

        dfm = ensure_price_cols(dfm)
        dfm = dfm.sort_values("cntr_tm")  # ✅ 핵심: 시간 오름차순
        dfm = parse_time(dfm)

        if dfm["dt"].isna().all():
            skipped_bad_time += 1
            continue

        dip_row = find_first_dip(dfm, t1_open)
        if dip_row is None:
            skipped_no_dip += 1
            continue

        dip_pct = float(dip_row["dip_pct"])
        if dip_pct > MAX_DIP:
            skipped_over_maxdip += 1
            continue

        dip_hhmm = str(dip_row["hhmm"])
        if OPENING_ONLY and dip_hhmm > OPENING_CUTOFF:
            skipped_opening += 1
            continue

        dip_bin = pd.cut(
            pd.Series([dip_pct]),
            bins=DIP_BINS,
            labels=DIP_LABELS,
            right=True,
            include_lowest=True,
        ).iloc[0]

        # entry = dip_row의 low
        entry_price = float(dip_row["low_pric"])
        entry_tm = dip_row["cntr_tm"]

        df_after = dfm[dfm["cntr_tm"] >= entry_tm]

        outcome = judge_tp_sl(df_after, entry_price, TP, SL)

        rows.append({
            "stk_cd": stk_cd,
            "t1_dt": t1_dt,
            "gap": float(ev["gap"]),
            "gap_bin": str(gap_bin),
            "dip_pct": dip_pct,
            "dip_bin": str(dip_bin),
            "dip_time": dip_hhmm,
            "outcome": outcome,
        })

    df = pd.DataFrame(rows)

    print("\n=== COUNTS ===")
    print("events:", len(df_events))
    print("rows(with minute & dip):", len(df))
    print("skipped_no_minute:", skipped_no_minute)
    print("skipped_bad_minute:", skipped_bad_minute)
    print("skipped_missing_cols:", skipped_missing_cols)
    print("skipped_no_dip:", skipped_no_dip)
    print("skipped_over_maxdip:", skipped_over_maxdip)
    print("skipped_opening:", skipped_opening)
    print("skipped_bad_time:", skipped_bad_time)

    if df.empty:
        print("\n[ERR] no rows. check minute files / columns / filters.")
        return

    print("\n=== OUTCOME DISTRIBUTION ===")
    print(df["outcome"].value_counts())

    # 집계
    g = df.groupby(["gap_bin", "dip_bin"], dropna=False)

    summary = g.agg(
        trades=("outcome", "count"),
        tp_cnt=("outcome", lambda x: (x == "TP").sum()),
        sl_cnt=("outcome", lambda x: (x == "SL").sum()),
        ambig=("outcome", lambda x: (x == "AMBIGUOUS_SAME_MIN").sum()),
        none_after=("outcome", lambda x: (x == "NONE_AFTER_ENTRY").sum()),
    ).reset_index()

    # winrate/EV는 TP+SL 기준(ambig/none 제외)
    den = (summary["tp_cnt"] + summary["sl_cnt"])
    summary["winrate"] = (summary["tp_cnt"] / den).where(den > 0, 0.0)
    summary["EV"] = ((summary["tp_cnt"] * TP) - (summary["sl_cnt"] * SL)) / den.where(den > 0, 1)

    # 보기 좋게 정렬
    summary = summary.sort_values(["gap_bin", "dip_bin"])

    # 저장
    OUT.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("\n=== GAP x DIP GRID (TP/SL based) ===")
    print(summary.to_string(index=False))

    print(f"\n[DONE] saved: {OUT}")


if __name__ == "__main__":
    main()