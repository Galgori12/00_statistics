# stkstats/collectors/kiwoom_client.py
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from auto_trade.api.auth import KiwoomAuth  # 기존 로그인 재사용


KIWOOM_CHART_PATH = "/api/dostk/chart"


class KiwoomClient:
    """
    Kiwoom REST client (ka10080).
    - 안정성: 429/5xx 재시도 + 지수 백오프
    - 연속조회: cont-yn / next-key 자동 처리
    - 하루치 제한: start_dt(YYYYMMDD)보다 과거 데이터가 나오면 즉시 중단
    """

    def __init__(self, timeout: int = 30, sleep_sec: float = 0.6, max_retries: int = 8):
        auth = KiwoomAuth()
        self.base_url = auth.base_url  # 토큰 발급과 동일 도메인 사용
        auth.login()
        if not auth.access_token:
            raise RuntimeError("토큰 발급 실패")
        self.token = f"Bearer {auth.access_token}"

        self.timeout = timeout
        self.sleep_sec = sleep_sec
        self.max_retries = max_retries
        self.session = requests.Session()

    def _post_chart(
        self,
        api_id: str,
        body: Dict[str, Any],
        cont_yn: Optional[str] = None,
        next_key: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        url = self.base_url.rstrip("/") + KIWOOM_CHART_PATH
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": self.token,
            "api-id": api_id,
        }
        if cont_yn:
            headers["cont-yn"] = cont_yn
        if next_key:
            headers["next-key"] = next_key

        # retry with exponential backoff on 429/5xx
        last_err: Optional[Exception] = None
        for i in range(self.max_retries):
            try:
                resp = self.session.post(url, json=body, headers=headers, timeout=self.timeout)
                if resp.status_code == 429 or resp.status_code >= 500:
                    wait = self.sleep_sec * (2 ** i)
                    print(f"[WARN] {resp.status_code} rate/server limit. wait {wait:.1f}s then retry... ({i+1}/{self.max_retries})")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                return data, dict(resp.headers)
            except Exception as ex:
                last_err = ex
                wait = self.sleep_sec * (2 ** i)
                print(f"[WARN] request failed. wait {wait:.1f}s then retry... ({i+1}/{self.max_retries}) err={ex}")
                time.sleep(wait)

        raise RuntimeError(f"chart request failed after retries. last_err={last_err}")

    @staticmethod
    def _extract_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        # response list key (docs) = stk_min_pole_chart_qry
        rows = payload.get("stk_min_pole_chart_qry")
        if rows is None:
            return []
        if isinstance(rows, list):
            return rows
        return []

    def fetch_minute_one_day(
        self,
        stk_cd: str,
        base_dt: str,
        tic_scope: str = "1",
        upd_stkpc_tp: str = "1",
        max_pages: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Fetch minute bars for a single day (base_dt YYYYMMDD).
        Uses continuation until:
          - cont-yn != 'Y'
          - or rows go earlier than base_dt (safety stop)
          - or max_pages reached
        """
        body = {
            "stk_cd": stk_cd,
            "tic_scope": str(tic_scope),
            "upd_stkpc_tp": str(upd_stkpc_tp),
            "base_dt": str(base_dt),
        }

        all_rows: List[Dict[str, Any]] = []
        cont_yn = None
        next_key = None

        for page in range(max_pages):
            payload, headers = self._post_chart("ka10080", body, cont_yn=cont_yn, next_key=next_key)
            rows = self._extract_rows(payload)
            if not rows:
                break

            all_rows.extend(rows)

            # safety stop: if we already see earlier dates than base_dt, stop.
            # cntr_tm expected like YYYYMMDDHHMMSS or YYYYMMDDHHMM
            # Kiwoom often returns newest->older
            try:
                min_tm = min(str(r.get("cntr_tm", "")).strip() for r in rows if r.get("cntr_tm") is not None)
            except ValueError:
                min_tm = ""
            if min_tm and len(min_tm) >= 8:
                day = min_tm[:8]
                if day < base_dt:
                    break

            cont_yn = headers.get("cont-yn")
            next_key = headers.get("next-key")

            if cont_yn != "Y" or not next_key:
                break

            time.sleep(self.sleep_sec)

        return all_rows