from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd

from stkstats.analysis._common import find_minute_path as _find_minute_path, load_events, load_parquet, load_minute_df as _load_minute_df


# =========================
# Paths / Config
# =========================
BASE_DIR = Path(__file__).resolve().parents[3]  # suhoTrade/ (from analysis/entry_tp_sl/)
EVENTS_PATH = BASE_DIR / "stkstats/data/raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet"
DAILY_DIR = BASE_DIR / "stkstats/data/raw/daily_ohlc"
MINUTE_DIR = BASE_DIR / "stkstats/data/raw/minute_ohlc_t1"

OUT_DIR = BASE_DIR / "stkstats/data/derived"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_SUMMARY = OUT_DIR / "grid_by_gap_bin_minutes_summary_2025.csv"
OUT_BEST_PER_BIN = OUT_DIR / "grid_by_gap_bin_minutes_best_per_bin_2025.csv"
OUT_DEBUG = OUT_DIR / "grid_by_gap_bin_minutes_debug_2025.txt"

# ===== Grid =====
ENTRY_K_LIST = [0.94, 0.95, 0.96, 0.97]
TP_LIST = [0.05, 0.06, 0.07, 0.08, 0.09, 0.10]
SL_LIST = [0.03, 0.04, 0.05, 0.06]

# gap bins (ratio)
GAP_BINS = [-1, 0, 0.03, 0.07, 0.10, 0.15, 0.20, 0.25, 1]
GAP_LABELS = ["<0", "0~3", "3~7", "7~10", "10~15", "15~20", "20~25", "25+"]

MIN_ENTERED_TRADES_PER_BIN = 10
AMBIG_SAME_MIN = "AMBIGUOUS_SAME_MIN"


# =========================
# Utils
# =========================
def _to_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


def _choose_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def norm_yyyymmdd_any(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def year_of(dt_yyyymmdd: str) -> str:
    return dt_yyyymmdd[:4] if dt_yyyymmdd and len(dt_yyyymmdd) >= 4 else ""


@dataclass
class DailySchema:
    date_col: str
    close_col: str


def detect_daily_schema(df: pd.DataFrame) -> DailySchema:
    date_col = _choose_col(df, ["dt", "일자", "date", "base_dt", "trd_dt", "trade_dt", "trd_dd", "trading_date"])
    close_col = _choose_col(df, ["cur_prc", "종가", "close", "close_pric", "cls_prc", "lst_pric", "close_price"])
    if not date_col or not close_col:
        raise RuntimeError(f"[DAILY] cannot detect schema. columns={df.columns.tolist()}")
    return DailySchema(date_col=date_col, close_col=close_col)


@dataclass
class MinuteSchema:
    tm_col: str
    high_col: str
    low_col: str
    cur_col: str


def detect_minute_schema(df: pd.DataFrame) -> MinuteSchema:
    tm_col = _choose_col(df, ["cntr_tm", "tm", "time", "dtm", "stck_cntg_hour"])
    high_col = _choose_col(df, ["high_pric", "high", "고가"])
    low_col = _choose_col(df, ["low_pric", "low", "저가"])
    cur_col = _choose_col(df, ["cur_prc", "close", "종가"])
    if not tm_col or not high_col or not low_col or not cur_col:
        raise RuntimeError(f"[MINUTE] cannot detect schema. columns={df.columns.tolist()}")
    return MinuteSchema(tm_col=tm_col, high_col=high_col, low_col=low_col, cur_col=cur_col)


# =========================
# Daily file resolver
# =========================
def build_daily_index(daily_dir: Path, limit: int = 20000) -> Dict[str, Path]:
    """
    fallback용: daily_dir 아래 모든 parquet를 훑어서
    파일명에 포함된 6자리 종목코드를 key로 매핑 (첫 발견 우선)
    """
    idx: Dict[str, Path] = {}
    cnt = 0
    for p in daily_dir.rglob("*.parquet"):
        name = p.stem
        # name 안에 6자리 숫자 찾기
        # 가장 간단히: 모든 6자리 substring 후보 중 앞쪽부터
        for i in range(0, max(0, len(name) - 5)):
            sub = name[i:i+6]
            if sub.isdigit():
                if sub not in idx:
                    idx[sub] = p
                break
        cnt += 1
        if cnt >= limit:
            break
    return idx


def resolve_daily_path(stk_cd: str, limit_dt: str, daily_index: Optional[Dict[str, Path]]) -> Optional[Path]:
    # 1) daily_ohlc/{stk}.parquet
    p1 = DAILY_DIR / f"{stk_cd}.parquet"
    if p1.exists():
        return p1

    yyyy = year_of(limit_dt)

    # 2) daily_ohlc/by_year/{YYYY}/{stk}.parquet
    if yyyy:
        p2 = DAILY_DIR / "by_year" / yyyy / f"{stk_cd}.parquet"
        if p2.exists():
            return p2

        # 3) daily_ohlc/by_year/{YYYY}/*{stk}*.parquet
        d = DAILY_DIR / "by_year" / yyyy
        if d.exists():
            cand = sorted(d.glob(f"*{stk_cd}*.parquet"))
            if cand:
                return cand[0]

    # 4) fallback index
    if daily_index is not None and stk_cd in daily_index:
        p = daily_index[stk_cd]
        if p.exists():
            return p

    return None


def load_daily_close(
    stk_cd: str,
    limit_dt: str,
    cache: Dict[str, Tuple[pd.DataFrame, DailySchema, pd.Series]],
    daily_index: Optional[Dict[str, Path]],
) -> Optional[float]:
    p = resolve_daily_path(stk_cd, limit_dt, daily_index)
    if p is None:
        return None

    key = str(p.resolve())
    if key not in cache:
        df = load_parquet(p)
        schema = detect_daily_schema(df)
        dt_norm = df[schema.date_col].map(norm_yyyymmdd_any)
        cache[key] = (df, schema, dt_norm)

    df, schema, dt_norm = cache[key]
    target = norm_yyyymmdd_any(limit_dt)
    if target == "":
        return None

    hit = df.loc[dt_norm == target]
    if hit.empty:
        return None

    v = _to_float(hit.iloc[0][schema.close_col])
    return abs(v) if v is not None else None


# =========================
# Minute helpers
# =========================
def find_minute_path(stk_cd: str, t1_dt: str) -> Optional[Path]:
    return _find_minute_path(MINUTE_DIR, stk_cd, t1_dt)


def load_minute_df(stk_cd: str, t1_dt: str, cache: Dict[Tuple[str, str], Optional[pd.DataFrame]]) -> Optional[pd.DataFrame]:
    return _load_minute_df(MINUTE_DIR, stk_cd, t1_dt, cache=cache)


def minute_eod_close(mdf: pd.DataFrame) -> Optional[float]:
    sch = detect_minute_schema(mdf)
    df = mdf.copy()
    df[sch.tm_col] = df[sch.tm_col].astype(str)
    df[sch.cur_col] = pd.to_numeric(df[sch.cur_col], errors="coerce").abs()
    df = df.dropna(subset=[sch.cur_col])
    if df.empty:
        return None
    df = df.sort_values(sch.tm_col, ascending=True)
    return float(df.iloc[-1][sch.cur_col])


def simulate_with_minutes(mdf: pd.DataFrame, entry: float, tp_price: float, sl_price: float) -> str:
    sch = detect_minute_schema(mdf)
    df = mdf.copy()
    for c in [sch.high_col, sch.low_col, sch.cur_col]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df[sch.cur_col] = df[sch.cur_col].abs()
    df[sch.high_col] = df[sch.high_col].abs()
    df[sch.low_col] = df[sch.low_col].abs()
    df = df.sort_values(sch.tm_col, ascending=True)

    entered = False
    for _, r in df.iterrows():
        low = r[sch.low_col]
        high = r[sch.high_col]
        if pd.isna(low) or pd.isna(high):
            continue
        if not entered:
            if low <= entry:
                entered = True
            else:
                continue
        hit_tp = high >= tp_price
        hit_sl = low <= sl_price
        if hit_tp and hit_sl:
            return AMBIG_SAME_MIN
        if hit_tp:
            return "TP"
        if hit_sl:
            return "SL"

    if not entered:
        return "NO_ENTRY"
    return "NONE_AFTER_ENTRY"


def write_debug(lines: List[str]) -> None:
    OUT_DEBUG.write_text("\n".join(lines), encoding="utf-8-sig")
    print(f"[DBG] wrote: {OUT_DEBUG}")


def main():
    dbg: List[str] = []
    dbg.append(f"[DBG] BASE_DIR={BASE_DIR}")
    dbg.append(f"[DBG] EVENTS_PATH exists={EVENTS_PATH.exists()} {EVENTS_PATH}")
    dbg.append(f"[DBG] DAILY_DIR exists={DAILY_DIR.exists()} {DAILY_DIR}")
    dbg.append(f"[DBG] MINUTE_DIR exists={MINUTE_DIR.exists()} {MINUTE_DIR}")

    # daily 파일 패턴 보여주기 (샘플 20개)
    sample_daily = list(DAILY_DIR.rglob("*.parquet"))[:20]
    dbg.append(f"[DBG] daily parquet sample count={len(sample_daily)}")
    for p in sample_daily:
        dbg.append(f"[DBG] daily sample: {p.relative_to(DAILY_DIR)}")

    # fallback 인덱스 (필요할 때만)
    daily_index: Optional[Dict[str, Path]] = None

    events = load_events(EVENTS_PATH)
    dbg.append(f"[DBG] events raw rows={len(events)} cols={events.columns.tolist()}")

    events["stk_cd"] = events["stk_cd"].astype(str).str.zfill(6)
    events["limit_dt"] = events["limit_dt"].map(norm_yyyymmdd_any)
    events["t1_dt"] = events["t1_dt"].map(norm_yyyymmdd_any)
    events["t1_open"] = pd.to_numeric(events["t1_open"], errors="coerce")
    events["t1_close_num"] = pd.to_numeric(events["t1_close"], errors="coerce") if "t1_close" in events.columns else float("nan")

    dbg.append(f"[DBG] t1_close_num NaN ratio={events['t1_close_num'].isna().mean():.4f}")

    # daily attach
    daily_cache: Dict[str, Tuple[pd.DataFrame, DailySchema, pd.Series]] = {}
    lc = []
    for _, r in events.iterrows():
        v = load_daily_close(r["stk_cd"], r["limit_dt"], daily_cache, daily_index)
        lc.append(v)
    events["limit_close"] = lc
    nan_ratio = float(events["limit_close"].isna().mean())
    dbg.append(f"[DBG] limit_close NaN ratio={nan_ratio:.4f}")

    # 만약 still 100%면: fallback index 생성 후 한번 더 시도
    if nan_ratio > 0.99:
        dbg.append("[WARN] limit_close almost all NaN. Building daily_index via rglob...")
        daily_index = build_daily_index(DAILY_DIR)
        dbg.append(f"[DBG] daily_index size={len(daily_index)}")
        lc2 = []
        for _, r in events.iterrows():
            lc2.append(load_daily_close(r["stk_cd"], r["limit_dt"], daily_cache, daily_index))
        events["limit_close"] = lc2
        nan_ratio = float(events["limit_close"].isna().mean())
        dbg.append(f"[DBG] limit_close NaN ratio after index={nan_ratio:.4f}")

    events = events.dropna(subset=["t1_open"]).copy()

    events["gap"] = (events["t1_open"] - events["limit_close"]) / events["limit_close"]
    events["gap_bin"] = pd.cut(events["gap"], bins=GAP_BINS, labels=GAP_LABELS)

    # minute exists count
    ex_cnt = 0
    for _, r in events.iterrows():
        p = find_minute_path(r["stk_cd"], r["t1_dt"])
        if p is not None and p.exists():
            ex_cnt += 1
    dbg.append(f"[DBG] minute exists count={ex_cnt}/{len(events)}")

    minute_cache: Dict[Tuple[str, str], Optional[pd.DataFrame]] = {}
    rows = []

    for gb in GAP_LABELS:
        sub = events[events["gap_bin"] == gb].copy()
        if sub.empty:
            dbg.append(f"[DBG] gap_bin={gb}: 0 rows (skip)")
            continue

        for entry_k in ENTRY_K_LIST:
            for tp in TP_LIST:
                for sl in SL_LIST:
                    entered = 0
                    tp_cnt = 0
                    sl_cnt = 0
                    ambig_cnt = 0
                    none_cnt = 0
                    no_entry_cnt = 0
                    no_minute_cnt = 0
                    rets: List[float] = []

                    for _, r in sub.iterrows():
                        stk_cd = r["stk_cd"]
                        t1_dt = r["t1_dt"]
                        t1_open = float(r["t1_open"])

                        entry = t1_open * float(entry_k)
                        tp_price = entry * (1.0 + float(tp))
                        sl_price = entry * (1.0 - float(sl))

                        mdf = load_minute_df(stk_cd, t1_dt, minute_cache)
                        if mdf is None or len(mdf) == 0:
                            no_minute_cnt += 1
                            continue

                        res = simulate_with_minutes(mdf, entry, tp_price, sl_price)

                        if res == "NO_ENTRY":
                            no_entry_cnt += 1
                            continue

                        entered += 1

                        if res == "TP":
                            tp_cnt += 1
                            rets.append(float(tp))
                        elif res == "SL":
                            sl_cnt += 1
                            rets.append(-float(sl))
                        elif res == AMBIG_SAME_MIN:
                            ambig_cnt += 1
                            rets.append(0.0)
                        else:
                            none_cnt += 1
                            t1_close = r["t1_close_num"]
                            if pd.isna(t1_close):
                                eod = minute_eod_close(mdf)
                                rets.append(0.0 if eod is None else (float(eod) - entry) / entry)
                            else:
                                rets.append((float(t1_close) - entry) / entry)

                    winrate = (tp_cnt / entered) if entered else 0.0
                    ev = (sum(rets) / len(rets)) if rets else float("nan")

                    rows.append({
                        "gap_bin": gb,
                        "entry_k": entry_k,
                        "tp": tp,
                        "sl": sl,
                        "entered_trades": entered,
                        "tp_cnt": tp_cnt,
                        "sl_cnt": sl_cnt,
                        "winrate": winrate,
                        "EV": ev,
                        "ambig_same_min": ambig_cnt,
                        "none_after_entry": none_cnt,
                        "no_entry": no_entry_cnt,
                        "no_minute_skipped": no_minute_cnt,
                    })

        print(f"[DBG] done gap_bin={gb} rows={len(sub)}")

    res = pd.DataFrame(rows)
    if res.empty:
        dbg.append("[ERR] result empty. gap_bin all empty. limit_close attach still failing.")
        write_debug(dbg)
        print("[ERR] result empty. check debug txt.")
        return

    res.to_csv(OUT_SUMMARY, index=False, encoding="utf-8-sig")
    print(f"\n[DONE] saved summary: {OUT_SUMMARY}")

    ok = res[res["entered_trades"] >= MIN_ENTERED_TRADES_PER_BIN].copy()
    if ok.empty:
        dbg.append(f"[WARN] No rows satisfy entered_trades >= {MIN_ENTERED_TRADES_PER_BIN}.")
        write_debug(dbg)
        print(f"[WARN] No rows satisfy entered_trades >= {MIN_ENTERED_TRADES_PER_BIN}. Lower threshold.")
        return

    best = ok.sort_values(["gap_bin", "EV"], ascending=[True, False]).groupby("gap_bin").head(1)
    best.to_csv(OUT_BEST_PER_BIN, index=False, encoding="utf-8-sig")
    print(f"[DONE] saved best_per_bin: {OUT_BEST_PER_BIN}")

    print("\n=== BEST PER GAP_BIN (entered_trades >= %d) ===" % MIN_ENTERED_TRADES_PER_BIN)
    print(best.sort_values("gap_bin").to_string(index=False))

    dbg.append("[DBG] entered_trades describe:\n" + res["entered_trades"].describe().to_string())
    dbg.append(f"[DBG] max_entered={res['entered_trades'].max()}")
    dbg.append(f"[DBG] max_EV={res['EV'].max()}")
    write_debug(dbg)


if __name__ == "__main__":
    main()