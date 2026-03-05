"""
analysis 공통: parquet 로딩/저장, 분봉 경로·로딩.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Tuple, Union

import pandas as pd

from stkstats.utils.io import load_parquet as _load_parquet, save_parquet as _save_parquet


def _path(p: Union[Path, str]) -> Path:
    return Path(p) if isinstance(p, str) else p


def load_parquet(path: Union[Path, str]) -> pd.DataFrame:
    """parquet 파일 로드."""
    return _load_parquet(_path(path))


def load_events(path: Union[Path, str]) -> pd.DataFrame:
    """이벤트 parquet 로드 (복사본 반환, 수정 시 원본 영향 없음)."""
    return load_parquet(path).copy()


def save_parquet(df: pd.DataFrame, path: Union[Path, str]) -> None:
    """DataFrame을 parquet으로 저장 (index=False, 상위 디렉터리 자동 생성)."""
    _save_parquet(df, _path(path))


def find_minute_path(
    minute_dir: Union[Path, str],
    stk_cd: str,
    t1_dt: str,
) -> Optional[Path]:
    """
    분봉 parquet 경로 탐색.
    - minute_dir / stk_cd / {t1_dt}.parquet 우선
    - 없으면 minute_dir / stk_cd / *{t1_dt}*.parquet glob
    """
    d = _path(minute_dir) / str(stk_cd).strip().zfill(6)
    if not d.exists():
        return None
    p1 = d / f"{str(t1_dt).strip()[:8]}.parquet"
    if p1.exists():
        return p1
    cand = sorted(d.glob(f"*{str(t1_dt).strip()[:8]}*.parquet"))
    return cand[0] if cand else None


def load_minute_df(
    minute_dir: Union[Path, str],
    stk_cd: str,
    t1_dt: str,
    cache: Optional[Dict[Tuple[str, str], Optional[pd.DataFrame]]] = None,
) -> Optional[pd.DataFrame]:
    """
    (stk_cd, t1_dt) 분봉 DataFrame 로드.
    cache가 있으면 (stk_cd, t1_dt) 키로 캐시 사용/저장.
    """
    stk_cd = str(stk_cd).strip().zfill(6)
    t1_dt = str(t1_dt).strip().replace("-", "").replace(".", "")[:8]
    if cache is not None:
        key = (stk_cd, t1_dt)
        if key in cache:
            return cache[key]
    p = find_minute_path(minute_dir, stk_cd, t1_dt)
    if p is None:
        if cache is not None:
            cache[(stk_cd, t1_dt)] = None
        return None
    df = pd.read_parquet(p)
    if cache is not None:
        cache[(stk_cd, t1_dt)] = df
    return df


def resolve_daily_path(
    daily_dir: Union[Path, str],
    stk_cd: str,
    limit_dt: str,
    *,
    year_from_dt: Optional[str] = None,
) -> Optional[Path]:
    """
    일봉 parquet 경로 탐색.
    1) daily_dir / {stk_cd}.parquet
    2) daily_dir / by_year / {yyyy} / {stk_cd}.parquet
    3) daily_dir / by_year / {yyyy} / *{stk_cd}*.parquet
    """
    base = _path(daily_dir)
    stk_cd = str(stk_cd).strip().zfill(6)
    dt8 = str(limit_dt).strip().replace("-", "").replace(".", "")[:8]
    yyyy = year_from_dt if year_from_dt is not None else (dt8[:4] if len(dt8) >= 4 else "")

    p1 = base / f"{stk_cd}.parquet"
    if p1.exists():
        return p1
    if yyyy:
        p2 = base / "by_year" / yyyy / f"{stk_cd}.parquet"
        if p2.exists():
            return p2
        d = base / "by_year" / yyyy
        if d.exists():
            cand = sorted(d.glob(f"*{stk_cd}*.parquet"))
            if cand:
                return cand[0]
    return None
