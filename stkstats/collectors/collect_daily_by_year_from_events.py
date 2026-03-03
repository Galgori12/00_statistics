from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from auto_trade.api.auth import KiwoomAuth
from stkstats.utils.io import load_parquet, save_parquet


CHART_PATH = "/api/dostk/chart"

EVENTS_PATH = "stkstats/data/raw/core/upper_limit_events_2023_2025.parquet"
OUT_ROOT = Path("stkstats/data/raw/daily_ohlc/by_year")

YEARS = [2023, 2024, 2025]          # 필요시 추가
UPD_STKPC_TP = "1"                  # 수정주가 (0/1)
SLEEP_SEC = 1.2                     # 429 줄이기
MAX_RETRIES = 8
MAX_PAGES = 500                     # 연속조회 페이지 안전장치


class KiwoomDailyClient:
    def __init__(self, timeout: int = 30):
        auth = KiwoomAuth()
        self.base_url = auth.base_url.rstrip("/")
        auth.login()
        if not auth.access_token:
            raise RuntimeError("토큰 발급 실패")
        self.token = f"Bearer {auth.access_token}"
        self.timeout = timeout
        self.session = requests.Session()

    def _post(
        self,
        api_id: str,
        body: Dict[str, Any],
        cont_yn: Optional[str] = None,
        next_key: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        url = self.base_url + CHART_PATH
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": self.token,
            "api-id": api_id,
        }
        if cont_yn:
            headers["cont-yn"] = cont_yn
        if next_key:
            headers["next-key"] = next_key

        last_err: Optional[Exception] = None
        for i in range(MAX_RETRIES):
            try:
                resp = self.session.post(url, json=body, headers=headers, timeout=self.timeout)
                if resp.status_code == 429 or resp.status_code >= 500:
                    wait = SLEEP_SEC * (2 ** i)
                    print(f"[WARN] {resp.status_code} limit. wait {wait:.1f}s retry ({i+1}/{MAX_RETRIES})")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json(), dict(resp.headers)
            except Exception as ex:
                last_err = ex
                wait = SLEEP_SEC * (2 ** i)
                print(f"[WARN] request failed. wait {wait:.1f}s retry ({i+1}/{MAX_RETRIES}) err={ex}")
                time.sleep(wait)

        raise RuntimeError(f"daily request failed after retries. last_err={last_err}")

    @staticmethod
    def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = payload.get("stk_dt_pole_chart_qry")
        if isinstance(rows, list):
            return rows
        return []

    def fetch_daily_until(self, stk_cd: str, base_dt: str, stop_dt_inclusive: str) -> List[Dict[str, Any]]:
        """
        base_dt에서 시작해 과거로 내려가며 연속조회.
        rows의 dt가 stop_dt_inclusive보다 '더 과거'로 내려가면 중단.
        """
        body = {"stk_cd": stk_cd, "base_dt": base_dt, "upd_stkpc_tp": UPD_STKPC_TP}

        all_rows: List[Dict[str, Any]] = []
        cont_yn = None
        next_key = None

        for _ in range(MAX_PAGES):
            payload, headers = self._post("ka10081", body, cont_yn=cont_yn, next_key=next_key)
            rows = self._extract_rows(payload)
            if not rows:
                break

            all_rows.extend(rows)

            # stop condition: 이번 페이지의 "가장 과거 dt"가 stop_dt보다 더 과거면 중단
            dts = [str(r.get("dt", "")).strip() for r in rows if r.get("dt") is not None]
            dts = [d for d in dts if len(d) >= 8]
            if dts:
                min_dt = min(dts)[:8]
                if min_dt < stop_dt_inclusive:
                    break

            cont_yn = headers.get("cont-yn")
            next_key = headers.get("next-key")
            if cont_yn != "Y" or not next_key:
                break

            time.sleep(SLEEP_SEC)

        return all_rows


def normalize_daily_rows(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # 표준 컬럼
    if "dt" in df.columns:
        df["dt"] = df["dt"].astype(str).str.strip().str[:8]

    num_cols = ["cur_prc", "trde_qty", "trde_prica", "open_pric", "high_pric", "low_pric", "pred_pre", "trde_tern_rt"]
    for c in num_cols:
        if c in df.columns:
            df[c] = (
                df[c]
                .astype(str)
                .str.replace(",", "", regex=False)
                .replace({"": None, "nan": None, "None": None})
            )
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 정렬 + 중복 제거
    if "dt" in df.columns:
        df = df.sort_values("dt").drop_duplicates(subset=["dt"], keep="last")

    return df


def main():
    events = load_parquet(EVENTS_PATH)
    events["stk_cd"] = events["stk_cd"].astype(str).str.zfill(6)
    events["limit_dt"] = events["limit_dt"].astype(str).str[:8]

    client = KiwoomDailyClient()

    for year in YEARS:
        year_start = f"{year}0101"
        year_end = f"{year}1231"

        # 해당 연도에 상한가 이벤트가 있었던 종목만
        sub = events[(events["limit_dt"] >= year_start) & (events["limit_dt"] <= year_end)]
        uniq_stocks = sorted(sub["stk_cd"].unique().tolist())
        print(f"\n=== YEAR {year} stocks={len(uniq_stocks)} ===")

        out_dir = OUT_ROOT / str(year)
        out_dir.mkdir(parents=True, exist_ok=True)

        for i, stk_cd in enumerate(uniq_stocks, start=1):
            out_path = out_dir / f"{stk_cd}.parquet"
            if out_path.exists():
                continue

            # 해당 연도 전체를 덮기 위해 year_end를 base_dt로 두고 과거로 내려가며 year_start까지 수집
            rows = client.fetch_daily_until(stk_cd=stk_cd, base_dt=year_end, stop_dt_inclusive=year_start)
            df = normalize_daily_rows(rows)

            if df.empty or "dt" not in df.columns:
                print(f"[WARN] empty daily. ({i}/{len(uniq_stocks)}) stk_cd={stk_cd}")
                continue

            # 연도 필터링 (안전)
            df = df[(df["dt"] >= year_start) & (df["dt"] <= year_end)].copy()
            if df.empty:
                print(f"[WARN] no rows in year. ({i}/{len(uniq_stocks)}) stk_cd={stk_cd}")
                continue

            df["stk_cd"] = stk_cd
            save_parquet(df, out_path)
            print(f"[OK] ({i}/{len(uniq_stocks)}) saved daily: {year} {stk_cd} rows={len(df)}")


if __name__ == "__main__":
    main()