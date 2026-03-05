import math
import pandas as pd
from pathlib import Path

from stkstats.analysis._common import find_minute_path, load_events, load_parquet

BASE = Path("stkstats/data")
EVENTS = BASE / "raw/archive/upper_limit_events_cleaned_2025_minute_ok_with_limit_close.parquet"
MINUTE_DIR = BASE / "raw/minute_ohlc_t1"

# ===== Params =====
MAX_DIP = 0.08
OPENING_ONLY = True
OPENING_CUTOFF = "09:03"

TP = 0.05
SL = 0.015

DIP_BINS = [0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08]
DIP_LABELS = ["0~1", "1~2", "2~3", "3~4", "4~5", "5~6", "6~7", "7~8"]

GAP_BINS = [-math.inf, 0, 0.03, 0.07, 0.10, 0.15, 0.20, 0.25, math.inf]
GAP_LABELS = ["<0", "0~3", "3~7", "7~10", "10~15", "15~20", "20~25", "25+"]

OUT = BASE / "derived" / f"gap_x_dip_TRUEGAP_tp{int(TP*100)}_sl{int(SL*1000)}_opening{int(OPENING_ONLY)}.csv"

def ensure_price_cols(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["open_pric", "high_pric", "low_pric", "cur_prc"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").abs()
    return df

def parse_time(df: pd.DataFrame) -> pd.DataFrame:
    df["dt"] = pd.to_datetime(df["cntr_tm"], errors="coerce")
    df["hhmm"] = df["dt"].dt.strftime("%H:%M")
    return df

def find_first_dip(df: pd.DataFrame, t1_open: float):
    df["dip_pct"] = (t1_open - df["low_pric"]) / t1_open
    dips = df[df["low_pric"] < t1_open]
    if dips.empty:
        return None
    return dips.iloc[0]

def judge_tp_sl(df_after: pd.DataFrame, entry_price: float, tp: float, sl: float) -> str:
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

def main():
    if not EVENTS.exists():
        raise FileNotFoundError(f"EVENTS not found: {EVENTS}")

    df_events = load_events(EVENTS)
    print("[DBG] events cols:", list(df_events.columns))
    print("[DBG] events rows:", len(df_events))

    # 진짜 gap 사용
    if "gap_true" not in df_events.columns:
        raise KeyError("gap_true not found. Run attach_limit_close_and_gap_true_2025 first.")

    df_events = df_events.dropna(subset=["gap_true", "t1_open", "stk_cd", "t1_dt"])

    df_events["gap_bin"] = pd.cut(
        df_events["gap_true"],
        bins=GAP_BINS,
        labels=GAP_LABELS,
        right=True,
        include_lowest=True
    )

    rows = []
    skipped_no_minute = 0
    skipped_no_dip = 0
    skipped_over_maxdip = 0
    skipped_opening = 0

    need_cols = {"cntr_tm", "high_pric", "low_pric"}

    for _, ev in df_events.iterrows():
        stk_cd = str(ev["stk_cd"])
        t1_dt = str(ev["t1_dt"])
        t1_open = float(ev["t1_open"])
        gap_bin = ev["gap_bin"]

        path = find_minute_path(MINUTE_DIR, stk_cd, t1_dt)
        if path is None:
            skipped_no_minute += 1
            continue

        dfm = load_parquet(path)
        if not need_cols.issubset(dfm.columns):
            continue

        dfm = ensure_price_cols(dfm)
        dfm = dfm.sort_values("cntr_tm")
        dfm = parse_time(dfm)

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
            include_lowest=True
        ).iloc[0]

        entry_price = float(dip_row["low_pric"])
        entry_tm = dip_row["cntr_tm"]
        df_after = dfm[dfm["cntr_tm"] >= entry_tm]

        outcome = judge_tp_sl(df_after, entry_price, TP, SL)

        # PnL for EV_all
        if outcome == "TP":
            pnl = TP
        elif outcome == "SL":
            pnl = -SL
        else:
            pnl = 0.0  # AMBIG/NONE은 0 처리(보수적). 필요하면 제외 버전도 따로 계산 가능.

        rows.append({
            "gap_bin": str(gap_bin),
            "dip_bin": str(dip_bin),
            "outcome": outcome,
            "pnl": pnl
        })

    df = pd.DataFrame(rows)

    print("\n=== COUNTS ===")
    print("events:", len(df_events))
    print("rows(with minute & dip):", len(df))
    print("skipped_no_minute:", skipped_no_minute)
    print("skipped_no_dip:", skipped_no_dip)
    print("skipped_over_maxdip:", skipped_over_maxdip)
    print("skipped_opening:", skipped_opening)

    print("\n=== OUTCOME DISTRIBUTION ===")
    print(df["outcome"].value_counts())

    g = df.groupby(["gap_bin", "dip_bin"], dropna=False)

    summary = g.agg(
        trades=("outcome", "count"),
        tp_cnt=("outcome", lambda x: (x == "TP").sum()),
        sl_cnt=("outcome", lambda x: (x == "SL").sum()),
        ambig=("outcome", lambda x: (x == "AMBIGUOUS_SAME_MIN").sum()),
        none_after=("outcome", lambda x: (x == "NONE_AFTER_ENTRY").sum()),
        EV_all=("pnl", "mean"),  # ✅ 전체 평균 PnL
    ).reset_index()

    den = (summary["tp_cnt"] + summary["sl_cnt"])
    summary["winrate_tp_sl"] = (summary["tp_cnt"] / den).where(den > 0, 0.0)
    summary["EV_tp_sl_only"] = ((summary["tp_cnt"] * TP) - (summary["sl_cnt"] * SL)) / den.where(den > 0, 1)

    summary = summary.sort_values(["gap_bin", "dip_bin"])

    OUT.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT, index=False, encoding="utf-8-sig")

    print("\n=== GAP x DIP GRID (TRUE GAP) ===")
    print(summary.to_string(index=False))
    print(f"\n[DONE] saved: {OUT}")

if __name__ == "__main__":
    main()