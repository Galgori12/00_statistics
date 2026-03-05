# stkstats/analysis/gap_dip/gap_dip_ev_heatmap.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from stkstats.analysis._common import find_minute_path, load_events, load_parquet, save_parquet


RAW_MIN_DIR_DEFAULT = Path("stkstats/data/raw/minute_ohlc_t1")
OUT_DIR_DEFAULT = Path("stkstats/data/derived")


def _to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def _abs_price_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").abs()
    return df


def load_minute(stk_cd: str, t1_dt: str, min_dir: Path) -> Optional[pd.DataFrame]:
    p = find_minute_path(min_dir, stk_cd, t1_dt)
    if p is None:
        return None
    df = load_parquet(p)
    if df is None or len(df) == 0:
        return None

    # cntr_tm normalization
    if "cntr_tm" not in df.columns:
        for alt in ["tm", "time", "hhmm", "stck_cntg_hour", "dtm"]:
            if alt in df.columns:
                df["cntr_tm"] = df[alt]
                break
    if "cntr_tm" not in df.columns:
        return None

    df["cntr_tm"] = df["cntr_tm"].astype(str)
    df = _abs_price_cols(df, ["open_pric", "high_pric", "low_pric", "cur_prc"])
    df = df.sort_values("cntr_tm", ascending=True).reset_index(drop=True)

    need = {"high_pric", "low_pric"}
    if not need.issubset(set(df.columns)):
        return None

    return df


@dataclass
class EntryInfo:
    entry_px: float
    entry_idx: int
    dip_pct: float
    dip_tm: str  # HHMMSS


def find_first_dip_entry(
    df_min: pd.DataFrame,
    t1_open: float,
    max_dip_pct: float,
    latest_time: str,  # HHMMSS, e.g. "090300"
    min_dip_pct: float = 0.0,
) -> Optional[EntryInfo]:
    if not np.isfinite(t1_open) or t1_open <= 0:
        return None

    lows = df_min["low_pric"].to_numpy(dtype=float)
    times = df_min["cntr_tm"].astype(str).to_numpy()
    hhmmss = np.array([t[-6:] if len(t) >= 6 else t.zfill(6) for t in times])

    idxs = np.where(lows < t1_open)[0]
    if len(idxs) == 0:
        return None

    i = int(idxs[0])
    low = float(lows[i])
    dip = (t1_open - low) / t1_open

    if not np.isfinite(dip) or dip < 0:
        return None
    if dip < min_dip_pct:
        return None
    if dip > max_dip_pct:
        return None
    if hhmmss[i] > latest_time:
        return None

    return EntryInfo(entry_px=low, entry_idx=i, dip_pct=float(dip), dip_tm=str(hhmmss[i]))


def simulate_tp_sl_after_entry(
    df_min: pd.DataFrame,
    entry_idx: int,
    entry_px: float,
    tp_pct: float,
    sl_pct: float,
) -> str:
    """
    Returns one of {"TP","SL","AMBIG","NONE"}.
    """
    if not np.isfinite(entry_px) or entry_px <= 0:
        return "NONE"

    tp_px = entry_px * (1.0 + tp_pct)
    sl_px = entry_px * (1.0 - sl_pct)

    highs = df_min["high_pric"].to_numpy(dtype=float)
    lows = df_min["low_pric"].to_numpy(dtype=float)

    for j in range(entry_idx + 1, len(df_min)):
        hit_tp = highs[j] >= tp_px
        hit_sl = lows[j] <= sl_px
        if hit_tp and hit_sl:
            return "AMBIG"
        if hit_tp:
            return "TP"
        if hit_sl:
            return "SL"

    return "NONE"


def ensure_gap(events: pd.DataFrame) -> pd.DataFrame:
    """
    gap 우선순위:
      1) gap 컬럼이 있으면 그대로 사용
      2) gap_true 있으면 gap으로 복사
      3) limit_close 있으면 gap=(t1_open-limit_close)/limit_close
      4) (현재 파일 케이스) limit_trde_prica 있으면 gap=(t1_open-limit_trde_prica)/limit_trde_prica
    """
    ev = events.copy()

    if "gap" in ev.columns:
        return ev

    if "gap_true" in ev.columns:
        ev["gap"] = pd.to_numeric(ev["gap_true"], errors="coerce")
        return ev

    if "limit_close" in ev.columns and "t1_open" in ev.columns:
        t1_open = pd.to_numeric(ev["t1_open"], errors="coerce")
        limit_close = pd.to_numeric(ev["limit_close"], errors="coerce")
        ev["gap"] = (t1_open - limit_close) / limit_close
        return ev

    # ✅ fallback: use limit_trde_prica (upper limit price)
    if "limit_trde_prica" in ev.columns and "t1_open" in ev.columns:
        t1_open = pd.to_numeric(ev["t1_open"], errors="coerce")
        limit_px = pd.to_numeric(ev["limit_trde_prica"], errors="coerce")
        ev["gap"] = (t1_open - limit_px) / limit_px
        return ev

    raise RuntimeError(
        f"events must have 'gap' or ('t1_open' with 'limit_close' or 'limit_trde_prica'). "
        f"columns={list(ev.columns)[:60]}"
    )


def assign_bins(values: pd.Series, edges: List[float]) -> pd.Categorical:
    """
    edges in fraction units. ex: [-0.01,0,0.03,0.07,...]
    Returns category labels like '0.00~0.03'
    """
    bins = pd.cut(values, bins=edges, right=False, include_lowest=True)
    return bins


def evaluate_gap_dip_grid(
    events: pd.DataFrame,
    min_dir: Path,
    tp_pct: float,
    sl_pct: float,
    max_dip_pct: float,
    latest_time: str,
    min_dip_pct: float,
    gap_edges: List[float],
    dip_edges: List[float],
) -> pd.DataFrame:
    # prep
    events = ensure_gap(events)

    need = {"stk_cd", "t1_dt", "t1_open", "gap"}
    missing = need - set(events.columns)
    if missing:
        raise RuntimeError(f"events missing columns: {sorted(missing)}")

    # compute per-event entry + result once
    minute_cache: Dict[Tuple[str, str], Optional[pd.DataFrame]] = {}
    rows: List[Dict[str, Any]] = []

    for _, ev in events.iterrows():
        stk_cd = str(ev["stk_cd"]).zfill(6)
        t1_dt = str(ev["t1_dt"])
        t1_open = _to_float(ev["t1_open"])
        gap = _to_float(ev["gap"])

        key = (stk_cd, t1_dt)
        if key not in minute_cache:
            minute_cache[key] = load_minute(stk_cd, t1_dt, min_dir)
        df_min = minute_cache[key]
        if df_min is None:
            continue

        ent = find_first_dip_entry(
            df_min=df_min,
            t1_open=t1_open,
            max_dip_pct=max_dip_pct,
            latest_time=latest_time,
            min_dip_pct=min_dip_pct,
        )
        if ent is None:
            continue

        res = simulate_tp_sl_after_entry(
            df_min=df_min,
            entry_idx=ent.entry_idx,
            entry_px=ent.entry_px,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
        )

        rows.append(
            dict(
                stk_cd=stk_cd,
                t1_dt=t1_dt,
                t1_open=t1_open,
                gap=gap,
                dip=ent.dip_pct,
                dip_time=ent.dip_tm,
                result=res,
            )
        )

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No rows after filtering. Check max_dip/latest_time/min_dip and that minute files exist.")

    # bins
    df["gap_bin"] = assign_bins(df["gap"], gap_edges).astype(str)
    df["dip_bin"] = assign_bins(df["dip"], dip_edges).astype(str)

    # summarize
    def _ev_fixed(gr: pd.DataFrame) -> float:
        tp = (gr["result"] == "TP").sum()
        sl = (gr["result"] == "SL").sum()
        amb = (gr["result"] == "AMBIG").sum()
        none = (gr["result"] == "NONE").sum()
        trades = tp + sl + amb + none
        if trades == 0:
            return np.nan
        return (tp * tp_pct - sl * sl_pct) / trades

    out = (
        df.groupby(["gap_bin", "dip_bin"], dropna=False)
        .apply(
            lambda g: pd.Series(
                {
                    "trades": len(g),
                    "TP": int((g["result"] == "TP").sum()),
                    "SL": int((g["result"] == "SL").sum()),
                    "AMBIG": int((g["result"] == "AMBIG").sum()),
                    "NONE": int((g["result"] == "NONE").sum()),
                    "winrate_TP_over_TPSL": float(
                        (g["result"] == "TP").sum() / max(1, ((g["result"] == "TP").sum() + (g["result"] == "SL").sum()))
                    ),
                    "EV_fixed": float(_ev_fixed(g)),
                    "avg_gap": float(np.nanmean(g["gap"].to_numpy(dtype=float))),
                    "avg_dip": float(np.nanmean(g["dip"].to_numpy(dtype=float))),
                }
            )
        )
        .reset_index()
        .sort_values(["gap_bin", "dip_bin"])
    )

    return df, out


def save_pivots_and_heatmaps(summary: pd.DataFrame, out_dir: Path, prefix: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # pivot tables
    piv_ev = summary.pivot(index="gap_bin", columns="dip_bin", values="EV_fixed")
    piv_tr = summary.pivot(index="gap_bin", columns="dip_bin", values="trades")
    piv_wr = summary.pivot(index="gap_bin", columns="dip_bin", values="winrate_TP_over_TPSL")

    piv_ev.to_csv(out_dir / f"{prefix}_pivot_EV_fixed.csv", encoding="utf-8-sig")
    piv_tr.to_csv(out_dir / f"{prefix}_pivot_trades.csv", encoding="utf-8-sig")
    piv_wr.to_csv(out_dir / f"{prefix}_pivot_winrate.csv", encoding="utf-8-sig")

    # heatmap helper
    def _heatmap(piv: pd.DataFrame, title: str, fname: str, fmt: str):
        fig = plt.figure(figsize=(12, 6))
        ax = plt.gca()
        arr = piv.to_numpy()
        im = ax.imshow(arr, aspect="auto")

        ax.set_xticks(np.arange(piv.shape[1]))
        ax.set_xticklabels(list(piv.columns), rotation=45, ha="right", fontsize=9)
        ax.set_yticks(np.arange(piv.shape[0]))
        ax.set_yticklabels(list(piv.index), fontsize=9)

        ax.set_xlabel("dip_bin")
        ax.set_ylabel("gap_bin")
        ax.set_title(title)

        for i in range(arr.shape[0]):
            for j in range(arr.shape[1]):
                v = arr[i, j]
                if np.isfinite(v):
                    ax.text(j, i, format(v, fmt), ha="center", va="center", fontsize=8)

        plt.colorbar(im, ax=ax)
        plt.tight_layout()
        plt.savefig(out_dir / fname, dpi=160)
        plt.close(fig)

    _heatmap(piv_ev, f"{prefix} — EV_fixed", f"{prefix}_heatmap_EV_fixed.png", ".4f")
    _heatmap(piv_wr, f"{prefix} — winrate(TP/(TP+SL))", f"{prefix}_heatmap_winrate.png", ".2f")
    _heatmap(piv_tr, f"{prefix} — trades", f"{prefix}_heatmap_trades.png", ".0f")


def parse_edges(s: str) -> List[float]:
    """
    "0,0.03,0.07" -> [0.0, 0.03, 0.07]
    """
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return [float(p) for p in parts]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True, help="events parquet (must include stk_cd,t1_dt,t1_open and gap or limit_close)")
    ap.add_argument("--min_dir", default=str(RAW_MIN_DIR_DEFAULT))
    ap.add_argument("--out_dir", default=str(OUT_DIR_DEFAULT))

    ap.add_argument("--tp", type=float, default=0.05, help="TP percent (fraction). ex 0.05")
    ap.add_argument("--sl", type=float, default=0.015, help="SL percent (fraction). ex 0.015")

    ap.add_argument("--max_dip", type=float, default=0.03, help="max first dip (fraction). ex 0.03")
    ap.add_argument("--min_dip", type=float, default=0.0, help="min first dip (fraction). ex 0.01")
    ap.add_argument("--latest_time", default="090300", help="dip_time <= HHMMSS. ex 090300")

    # default bins (fraction units)
    # gap: <0, 0~3, 3~7, 7~10, 10~15, 15~20, 20~25, 25+
    ap.add_argument("--gap_edges", default="-0.50,0,0.03,0.07,0.10,0.15,0.20,0.25,0.80")
    # dip: 0~1, 1~2, 2~3, 3~4, 4~5, 5~8
    ap.add_argument("--dip_edges", default="0,0.01,0.02,0.03,0.04,0.05,0.08")

    ap.add_argument("--prefix", default="gap_x_dip_ev_2025", help="output file prefix")
    args = ap.parse_args()

    events = load_events(Path(args.events))
    min_dir = Path(args.min_dir)
    out_dir = Path(args.out_dir)

    gap_edges = parse_edges(args.gap_edges)
    dip_edges = parse_edges(args.dip_edges)

    detail, summary = evaluate_gap_dip_grid(
        events=events,
        min_dir=min_dir,
        tp_pct=float(args.tp),
        sl_pct=float(args.sl),
        max_dip_pct=float(args.max_dip),
        latest_time=str(args.latest_time),
        min_dip_pct=float(args.min_dip),
        gap_edges=gap_edges,
        dip_edges=dip_edges,
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    detail_path = out_dir / f"{args.prefix}_detail.parquet"
    summary_path = out_dir / f"{args.prefix}_summary.csv"

    save_parquet(detail, detail_path)
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")

    print(f"[OK] saved detail: {detail_path} rows={len(detail)}")
    print(f"[OK] saved summary: {summary_path} rows={len(summary)}")

    save_pivots_and_heatmaps(summary, out_dir, args.prefix)

    # show top bins by EV (with minimum trades filter)
    top = summary[summary["trades"] >= 10].sort_values("EV_fixed", ascending=False).head(15)
    print("\n=== TOP 15 bins (trades>=10) by EV_fixed ===")
    print(top[["gap_bin", "dip_bin", "trades", "TP", "SL", "AMBIG", "NONE", "winrate_TP_over_TPSL", "EV_fixed", "avg_gap", "avg_dip"]].to_string(index=False))


if __name__ == "__main__":
    main()