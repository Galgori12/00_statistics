# stkstats/analysis/first_dip/tp_sl_heatmap.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from stkstats.analysis._common import find_minute_path, load_events, load_parquet


RAW_MIN_DIR_DEFAULT = Path("stkstats/data/raw/minute_ohlc_t1")
OUT_DIR_DEFAULT = Path("stkstats/data/derived")
DEFAULT_TP_LIST = [0.03, 0.04, 0.05, 0.06, 0.07, 0.08]
DEFAULT_SL_LIST = [0.008, 0.01, 0.012, 0.015, 0.02]


def _to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def _abs_price_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            # Kiwoom minute data can contain negative prices (use abs)
            df[c] = pd.to_numeric(df[c], errors="coerce").abs()
    return df


def load_minute(stk_cd: str, t1_dt: str, min_dir: Path) -> Optional[pd.DataFrame]:
    p = find_minute_path(min_dir, stk_cd, t1_dt)
    if p is None:
        return None
    df = load_parquet(p)
    if df is None or len(df) == 0:
        return None

    # normalize time column
    if "cntr_tm" not in df.columns:
        # try common alternatives
        for alt in ["tm", "time", "hhmm", "stck_cntg_hour", "dtm"]:
            if alt in df.columns:
                df["cntr_tm"] = df[alt]
                break
    if "cntr_tm" not in df.columns:
        return None

    df["cntr_tm"] = df["cntr_tm"].astype(str)
    df = _abs_price_cols(df, ["open_pric", "high_pric", "low_pric", "cur_prc"])

    # sort ascending by time (data may be reverse)
    df = df.sort_values("cntr_tm", ascending=True).reset_index(drop=True)

    # basic sanity
    need = {"high_pric", "low_pric"}
    if not need.issubset(set(df.columns)):
        return None

    return df


@dataclass
class EntryInfo:
    entry_px: float
    entry_idx: int
    dip_pct: float
    dip_tm: str


def find_first_dip_entry(
    df_min: pd.DataFrame,
    t1_open: float,
    max_dip_pct: float,
    latest_time: str,  # e.g. "090300"
) -> Optional[EntryInfo]:
    """
    First Dip definition (snapshot):
      - minute sorted by cntr_tm ASC
      - first minute where low_pric < t1_open
      - dip_pct = (t1_open - low) / t1_open
      - filter: 0 <= dip <= max_dip_pct
      - filter: dip_time <= latest_time (HHMMSS)
    """
    if not np.isfinite(t1_open) or t1_open <= 0:
        return None

    lows = df_min["low_pric"].to_numpy(dtype=float)
    times = df_min["cntr_tm"].astype(str).to_numpy()

    # build HHMMSS from cntr_tm format like YYYYMMDDHHMMSS
    # if already HHMMSS, this still works by taking last 6
    hhmmss = np.array([t[-6:] if len(t) >= 6 else t.zfill(6) for t in times])

    # first dip: first low < t1_open
    idxs = np.where(lows < t1_open)[0]
    if len(idxs) == 0:
        return None

    i = int(idxs[0])
    low = float(lows[i])
    dip = (t1_open - low) / t1_open

    if dip < 0:
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
) -> Tuple[str, Optional[float]]:
    """
    Scan forward after entry minute:
      - TP hit if high >= entry*(1+tp)
      - SL hit if low <= entry*(1-sl)
      - If both in same minute => AMBIG
      - If none => NONE (return close_ret if possible)
    Returns: (result, close_ret)
      result in {"TP","SL","AMBIG","NONE"}
    """
    if not np.isfinite(entry_px) or entry_px <= 0:
        return "NONE", None

    tp_px = entry_px * (1.0 + tp_pct)
    sl_px = entry_px * (1.0 - sl_pct)

    highs = df_min["high_pric"].to_numpy(dtype=float)
    lows = df_min["low_pric"].to_numpy(dtype=float)

    # scan from entry_idx+1 onward (after entry)
    for j in range(entry_idx + 1, len(df_min)):
        hit_tp = highs[j] >= tp_px
        hit_sl = lows[j] <= sl_px
        if hit_tp and hit_sl:
            return "AMBIG", None
        if hit_tp:
            return "TP", None
        if hit_sl:
            return "SL", None

    # NONE: use last cur_prc or close proxy
    close_ret = None
    if "cur_prc" in df_min.columns:
        last_px = float(pd.to_numeric(df_min["cur_prc"].iloc[-1], errors="coerce"))
        if np.isfinite(last_px) and last_px > 0:
            close_ret = (last_px - entry_px) / entry_px
    return "NONE", close_ret


def evaluate_grid(
    events: pd.DataFrame,
    min_dir: Path,
    tp_list: List[float],
    sl_list: List[float],
    max_dip_pct: float,
    latest_time: str,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    # cache minute dfs per (stk_cd, t1_dt) to speed up grid
    minute_cache: Dict[Tuple[str, str], Optional[pd.DataFrame]] = {}

    # precompute entry per event once (entry depends on dip filters, not on TP/SL)
    entries: List[Optional[EntryInfo]] = []
    minutes: List[Optional[pd.DataFrame]] = []

    for _, ev in events.iterrows():
        stk_cd = str(ev["stk_cd"]).zfill(6)
        t1_dt = str(ev["t1_dt"])
        t1_open = _to_float(ev["t1_open"])

        key = (stk_cd, t1_dt)
        if key not in minute_cache:
            minute_cache[key] = load_minute(stk_cd, t1_dt, min_dir)
        df_min = minute_cache[key]
        if df_min is None:
            entries.append(None)
            minutes.append(None)
            continue

        ent = find_first_dip_entry(
            df_min=df_min,
            t1_open=t1_open,
            max_dip_pct=max_dip_pct,
            latest_time=latest_time,
        )
        entries.append(ent)
        minutes.append(df_min)

    n_events = len(events)
    n_entries = sum(1 for e in entries if e is not None)

    print(f"[INFO] events={n_events} | entries(found under dip/time filters)={n_entries}")

    for tp in tp_list:
        for sl in sl_list:
            tp_cnt = sl_cnt = ambig_cnt = none_cnt = 0
            close_rets: List[float] = []

            for ent, df_min in zip(entries, minutes):
                if ent is None or df_min is None:
                    continue
                res, close_ret = simulate_tp_sl_after_entry(
                    df_min=df_min,
                    entry_idx=ent.entry_idx,
                    entry_px=ent.entry_px,
                    tp_pct=tp,
                    sl_pct=sl,
                )
                if res == "TP":
                    tp_cnt += 1
                elif res == "SL":
                    sl_cnt += 1
                elif res == "AMBIG":
                    ambig_cnt += 1
                else:
                    none_cnt += 1
                    if close_ret is not None and np.isfinite(close_ret):
                        close_rets.append(float(close_ret))

            trades = tp_cnt + sl_cnt + ambig_cnt + none_cnt
            denom = (tp_cnt + sl_cnt)
            winrate = (tp_cnt / denom) if denom > 0 else np.nan

            # EV_fixed: treat TP as +tp, SL as -sl, ignore others (NONE/AMBIG) as 0
            ev_fixed = ((tp_cnt * tp) - (sl_cnt * sl)) / trades if trades > 0 else np.nan

            # EV_close: for NONE cases, use close_ret when available (fallback 0 otherwise)
            none_mean = float(np.mean(close_rets)) if len(close_rets) > 0 else 0.0
            ev_close = (
                (tp_cnt * tp) + (sl_cnt * (-sl)) + (none_cnt * none_mean)
            ) / trades if trades > 0 else np.nan

            rows.append(
                dict(
                    TP_pct=tp,
                    SL_pct=sl,
                    trades=trades,
                    TP=tp_cnt,
                    SL=sl_cnt,
                    AMBIG=ambig_cnt,
                    NONE=none_cnt,
                    winrate=winrate,
                    EV_fixed=ev_fixed,
                    EV_close=ev_close,
                )
            )

    return pd.DataFrame(rows)


def save_heatmap(df_grid: pd.DataFrame, out_dir: Path, value_col: str = "EV_fixed") -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    pivot = df_grid.pivot(index="SL_pct", columns="TP_pct", values=value_col).sort_index(ascending=True)
    csv_path = out_dir / f"tp_sl_grid_{value_col}.csv"
    pivot.to_csv(csv_path, encoding="utf-8-sig")
    print(f"[OK] saved pivot csv: {csv_path}")

    # Heatmap plot (matplotlib default colormap)
    fig = plt.figure(figsize=(10, 6))
    ax = plt.gca()
    im = ax.imshow(pivot.to_numpy(), aspect="auto")

    ax.set_xticks(np.arange(pivot.shape[1]))
    ax.set_xticklabels([f"{c*100:.1f}%" for c in pivot.columns], rotation=45, ha="right")

    ax.set_yticks(np.arange(pivot.shape[0]))
    ax.set_yticklabels([f"{r*100:.1f}%" for r in pivot.index])

    ax.set_xlabel("TP")
    ax.set_ylabel("SL")
    ax.set_title(f"TP×SL Heatmap ({value_col})")

    # annotate cells
    arr = pivot.to_numpy()
    for i in range(arr.shape[0]):
        for j in range(arr.shape[1]):
            v = arr[i, j]
            if np.isfinite(v):
                ax.text(j, i, f"{v*100:.2f}%", ha="center", va="center", fontsize=9)

    plt.colorbar(im, ax=ax)
    png_path = out_dir / f"tp_sl_heatmap_{value_col}.png"
    plt.tight_layout()
    plt.savefig(png_path, dpi=160)
    plt.close(fig)
    print(f"[OK] saved heatmap png: {png_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True, help="events parquet path (must include stk_cd,t1_dt,t1_open)")
    ap.add_argument("--min_dir", default=str(RAW_MIN_DIR_DEFAULT))
    ap.add_argument("--out_dir", default=str(OUT_DIR_DEFAULT))

    ap.add_argument("--max_dip", type=float, default=0.03, help="max first dip pct (default=0.03 => 3%)")
    ap.add_argument("--latest_time", default="090300", help="dip_time must be <= this HHMMSS (default=090300)")

    ap.add_argument("--tp_list", default=",".join(str(x) for x in DEFAULT_TP_LIST))
    ap.add_argument("--sl_list", default=",".join(str(x) for x in DEFAULT_SL_LIST))

    args = ap.parse_args()

    events_path = Path(args.events)
    min_dir = Path(args.min_dir)
    out_dir = Path(args.out_dir)

    events = load_events(events_path)

    # required cols
    need = {"stk_cd", "t1_dt", "t1_open"}
    missing = need - set(events.columns)
    if missing:
        raise RuntimeError(f"events missing columns: {sorted(missing)}")

    tp_list = [float(x) for x in args.tp_list.split(",") if x.strip()]
    sl_list = [float(x) for x in args.sl_list.split(",") if x.strip()]

    df_grid = evaluate_grid(
        events=events,
        min_dir=min_dir,
        tp_list=tp_list,
        sl_list=sl_list,
        max_dip_pct=float(args.max_dip),
        latest_time=str(args.latest_time),
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    grid_csv = out_dir / "tp_sl_grid_full.csv"
    df_grid.to_csv(grid_csv, index=False, encoding="utf-8-sig")
    print(f"[OK] saved full grid: {grid_csv}")

    # heatmaps
    save_heatmap(df_grid, out_dir, value_col="EV_fixed")
    save_heatmap(df_grid, out_dir, value_col="EV_close")

    # print top combos
    top = df_grid.sort_values("EV_fixed", ascending=False).head(10)
    print("\n=== TOP 10 by EV_fixed ===")
    print(top[["TP_pct", "SL_pct", "trades", "TP", "SL", "NONE", "AMBIG", "winrate", "EV_fixed", "EV_close"]].to_string(index=False))


if __name__ == "__main__":
    main()