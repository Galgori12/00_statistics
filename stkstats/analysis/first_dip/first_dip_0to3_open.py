from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple, Dict, List

import pandas as pd

from stkstats.analysis._common import find_minute_path, load_events, load_parquet, save_parquet


# =========================
# Paths
# =========================
BASE_DIR = Path(__file__).resolve().parents[3]  # suhoTrade (from analysis/first_dip/)
EVENTS_PATH = BASE_DIR / "stkstats/data/raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet"
MINUTE_DIR = BASE_DIR / "stkstats/data/raw/minute_ohlc_t1"
OUT_DIR = BASE_DIR / "stkstats/data/derived"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_DETAIL = OUT_DIR / "first_dip_open_0to3_detail_2025.parquet"
OUT_SUMMARY = OUT_DIR / "first_dip_open_0to3_summary_2025.csv"
OUT_GRID = OUT_DIR / "first_dip_open_0to3_tp_sl_grid_2025.csv"


# =========================
# Config you can tweak
# =========================

# "첫 눌림"을 이 구간으로만 볼 거야: 0% ~ -3%
# bin은 (0~-1), (-1~-2), (-2~-3)
DIP_BINS = [0.00, 0.01, 0.02, 0.03]
DIP_LABELS = ["0~1", "1~2", "2~3"]  # (%) 단위로 해석

# 손절 짧게 / 익절 길게
TP_LIST = [0.03, 0.05, 0.07, 0.10, 0.12]
SL_LIST = [0.005, 0.01, 0.015, 0.02]  # 0.5% ~ 2%

AMBIG = "AMBIGUOUS_SAME_MIN"


# =========================
# Helpers
# =========================
def norm_yyyymmdd(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _find_minute_path(stk_cd: str, t1_dt: str) -> Optional[Path]:
    return find_minute_path(MINUTE_DIR, stk_cd, t1_dt)


def detect_cols(df: pd.DataFrame) -> Tuple[str, str, str, str]:
    # time
    for c in ["cntr_tm", "tm", "time", "dtm", "stck_cntg_hour"]:
        if c in df.columns:
            tm = c
            break
    else:
        raise RuntimeError(f"no time col: {df.columns.tolist()}")

    # high/low/cur
    for c in ["high_pric", "high", "고가"]:
        if c in df.columns:
            hi = c
            break
    else:
        raise RuntimeError("no high col")

    for c in ["low_pric", "low", "저가"]:
        if c in df.columns:
            lo = c
            break
    else:
        raise RuntimeError("no low col")

    for c in ["cur_prc", "close", "종가"]:
        if c in df.columns:
            cur = c
            break
    else:
        raise RuntimeError("no cur col")

    return tm, hi, lo, cur


def prepare_minute_df(mdf: pd.DataFrame) -> pd.DataFrame:
    tm, hi, lo, cur = detect_cols(mdf)
    df = mdf.copy()
    df[tm] = df[tm].astype(str)
    for c in [hi, lo, cur]:
        df[c] = pd.to_numeric(df[c], errors="coerce").abs()
    df = df.dropna(subset=[hi, lo])
    df = df.sort_values(tm, ascending=True)
    return df


def first_dip_info(df: pd.DataFrame, t1_open: float) -> Optional[Dict]:
    """
    "첫 눌림" 정의(실용적으로):
    - 시가보다 낮아지는 첫 순간(첫 low < open)이 발생한 분봉을 "첫 눌림 분봉"으로 보고
    - 그 분봉의 low 기준 눌림폭 dip = (open - low)/open
    - dip이 0~3% 구간이면 채택
    """
    tm, hi, lo, cur = detect_cols(df)

    for _, r in df.iterrows():
        low = float(r[lo])
        if low < t1_open:  # 첫 눌림 발생
            dip = (t1_open - low) / t1_open
            return {
                "first_dip_tm": r[tm],
                "first_dip_low": low,
                "first_dip_pct": dip,  # 0.0123 = 1.23%
            }
    return None


def simulate_tp_sl_after_entry(df: pd.DataFrame, entry: float, tp: float, sl: float) -> str:
    tm, hi, lo, cur = detect_cols(df)
    tp_price = entry * (1.0 + tp)
    sl_price = entry * (1.0 - sl)

    entered = False
    for _, r in df.iterrows():
        low = float(r[lo])
        high = float(r[hi])

        if not entered:
            # entry는 "첫 눌림 low"로 정의했으니, 그 시점 이후부터 보게끔
            # 하지만 단순화를 위해 low<=entry이면 진입으로 간주
            if low <= entry:
                entered = True
            else:
                continue

        hit_tp = high >= tp_price
        hit_sl = low <= sl_price

        if hit_tp and hit_sl:
            return AMBIG
        if hit_tp:
            return "TP"
        if hit_sl:
            return "SL"

    if not entered:
        return "NO_ENTRY"
    return "NONE_AFTER_ENTRY"


def eod_close(df: pd.DataFrame) -> Optional[float]:
    tm, hi, lo, cur = detect_cols(df)
    s = pd.to_numeric(df[cur], errors="coerce").abs()
    s = s.dropna()
    if s.empty:
        return None
    return float(s.iloc[-1])


# =========================
# Main
# =========================
def main():
    ev = load_events(EVENTS_PATH)
    ev["stk_cd"] = ev["stk_cd"].astype(str).str.zfill(6)
    ev["t1_dt"] = ev["t1_dt"].map(norm_yyyymmdd)
    ev["t1_open"] = pd.to_numeric(ev["t1_open"], errors="coerce")
    ev["t1_close"] = pd.to_numeric(ev["t1_close"], errors="coerce") if "t1_close" in ev.columns else float("nan")
    ev = ev.dropna(subset=["t1_open"])

    rows = []
    minute_ok = 0

    for _, r in ev.iterrows():
        stk = r["stk_cd"]
        t1_dt = r["t1_dt"]
        t1_open = float(r["t1_open"])
        t1_close = r["t1_close"]

        p = _find_minute_path(stk, t1_dt)
        if p is None or not p.exists():
            continue

        mdf = load_parquet(p)
        df = prepare_minute_df(mdf)
        if df.empty:
            continue

        minute_ok += 1

        info = first_dip_info(df, t1_open)
        if info is None:
            # 시가 아래로 한 번도 안 내려간 날 (first dip 없음)
            continue

        dip = float(info["first_dip_pct"])
        if not (0.0 <= dip <= 0.03):
            continue

        # dip_bin
        dip_bin = pd.cut([dip], bins=DIP_BINS, labels=DIP_LABELS, include_lowest=True)[0]

        entry = float(info["first_dip_low"])  # "첫 눌림 low"를 entry로 두고 이후 상승여력 측정

        # 이후 상승여력(최대 상승률): entry 이후의 고가 기준
        tm, hi, lo, cur = detect_cols(df)
        # entry가 찍힌 분봉 이후만 보고 싶으면: first_dip_tm 이후 slice
        after = df[df[tm] >= info["first_dip_tm"]].copy()
        max_high = float(after[hi].max()) if not after.empty else float(df[hi].max())
        upside_from_entry = (max_high - entry) / entry

        # 종가 수익률(일봉 종가 있으면 그걸 사용, 없으면 분봉 마지막가)
        if pd.isna(t1_close):
            eod = eod_close(df)
            close_ret = 0.0 if eod is None else (eod - entry) / entry
        else:
            close_ret = (float(t1_close) - entry) / entry

        rows.append({
            "stk_cd": stk,
            "t1_dt": t1_dt,
            "t1_open": t1_open,
            "first_dip_tm": info["first_dip_tm"],
            "first_dip_low": entry,
            "first_dip_pct": dip,         # 0.0123
            "dip_bin": str(dip_bin),      # "0~1" etc
            "max_high_after": max_high,
            "upside_from_entry": upside_from_entry,  # 최대상승률
            "close_ret_from_entry": close_ret,       # 종가수익률
        })

    detail = pd.DataFrame(rows)
    print(f"[DBG] minute readable events: {minute_ok}")
    print(f"[DBG] first-dip in 0~-3% rows: {len(detail)}")

    if detail.empty:
        raise RuntimeError("[ERR] detail empty. check minute paths / definition.")

    save_parquet(detail, OUT_DETAIL)
    print(f"[DONE] saved detail: {OUT_DETAIL}")

    # -------------------------
    # 1) 상승여력 평균(구간별)
    # -------------------------
    summary = detail.groupby("dip_bin").agg(
        rows=("stk_cd", "count"),
        avg_first_dip_pct=("first_dip_pct", "mean"),
        avg_upside=("upside_from_entry", "mean"),
        med_upside=("upside_from_entry", "median"),
        avg_close_ret=("close_ret_from_entry", "mean"),
        med_close_ret=("close_ret_from_entry", "median"),
    ).reset_index()

    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    print(f"[DONE] saved summary: {OUT_SUMMARY}")
    print("\n=== FIRST DIP (0~-3%) UPSIDE SUMMARY ===")
    print(summary.to_string(index=False))

    # -------------------------
    # 2) TP/SL grid (손절 짧게 / 익절 길게)
    #    entry = first_dip_low 기준
    # -------------------------
    grid_rows = []
    # 미리 분봉 캐시(성능)
    minute_cache: Dict[Tuple[str, str], pd.DataFrame] = {}

    for tp in TP_LIST:
        for sl in SL_LIST:
            tp_cnt = 0
            sl_cnt = 0
            amb = 0
            none = 0
            rets: List[float] = []

            for _, r in detail.iterrows():
                stk = r["stk_cd"]
                t1_dt = r["t1_dt"]
                entry = float(r["first_dip_low"])

                key = (stk, t1_dt)
                if key not in minute_cache:
                    p = _find_minute_path(stk, t1_dt)
                    if p is None or not p.exists():
                        continue
                    df = prepare_minute_df(load_parquet(p))
                    minute_cache[key] = df
                df = minute_cache[key]

                res = simulate_tp_sl_after_entry(df, entry, tp, sl)
                if res == "TP":
                    tp_cnt += 1
                    rets.append(tp)
                elif res == "SL":
                    sl_cnt += 1
                    rets.append(-sl)
                elif res == AMBIG:
                    amb += 1
                    rets.append(0.0)
                else:
                    # NONE_AFTER_ENTRY는 EOD로
                    none += 1
                    eod = eod_close(df)
                    rets.append(0.0 if eod is None else (eod - entry) / entry)

            trades = tp_cnt + sl_cnt + amb + none
            winrate = (tp_cnt / trades) if trades else 0.0
            ev = (sum(rets) / len(rets)) if rets else float("nan")

            grid_rows.append({
                "tp": tp,
                "sl": sl,
                "trades": trades,
                "tp_cnt": tp_cnt,
                "sl_cnt": sl_cnt,
                "ambig": amb,
                "none_after_entry": none,
                "winrate": winrate,
                "EV": ev,
            })

    grid = pd.DataFrame(grid_rows).sort_values("EV", ascending=False)
    grid.to_csv(OUT_GRID, index=False, encoding="utf-8-sig")
    print(f"\n[DONE] saved tp/sl grid: {OUT_GRID}")
    print("\n=== TOP 20 TP/SL (first dip 0~-3%) ===")
    print(grid.head(20).to_string(index=False))


if __name__ == "__main__":
    main()