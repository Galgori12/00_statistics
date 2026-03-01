import os
import time
from typing import Dict, Any, List, Optional, Tuple

import requests

from auto_trade.api.auth import KiwoomAuth  # 기존 로그인 재사용


KIWOOM_CHART_PATH = "/api/dostk/chart"


class KiwoomClient:
    def __init__(self, token=None, timeout=30, sleep_sec=1.0):
        auth = KiwoomAuth()
        self.base_url = auth.base_url  # ✅ 토큰 발급과 동일 도메인 사용

        auth.login()
        if not auth.access_token:
            raise RuntimeError("토큰 발급 실패")
        self.token = f"Bearer {auth.access_token}"

        self.timeout = timeout
        self.sleep_sec = sleep_sec
        self.session = requests.Session()

    def _post_chart(
            self,
            api_id: str,
            body: Dict[str, Any],
            cont_yn: Optional[str] = None,
            next_key: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": self.token,
            "api-id": api_id,
        }
        if cont_yn:
            headers["cont-yn"] = cont_yn
        if next_key:
            headers["next-key"] = next_key

        url = f"{self.base_url}{KIWOOM_CHART_PATH}"

        # ✅ 429 대비 재시도 (지수 백오프)
        max_retries = 8
        backoff = 1.0  # seconds

        for attempt in range(max_retries):
            resp = self.session.post(url, headers=headers, json=body, timeout=self.timeout)

            if resp.status_code == 200:
                data = resp.json()
                out_headers = {
                    "cont-yn": resp.headers.get("cont-yn", ""),
                    "next-key": resp.headers.get("next-key", ""),
                }
                time.sleep(self.sleep_sec)  # 정상 호출 간 기본 텀
                return data, out_headers

            # 레이트리밋: 기다리고 재시도
            if resp.status_code == 429:
                wait = backoff * (2 ** attempt)
                print(f"[WARN] 429 rate limit. wait {wait:.1f}s then retry... ({attempt + 1}/{max_retries})")
                time.sleep(wait)
                continue

            # 그 외 오류는 바로 실패
            raise RuntimeError(f"[{api_id}] HTTP {resp.status_code}: {resp.text[:500]}")

        raise RuntimeError(f"[{api_id}] HTTP 429: too many requests after retries")

    def fetch_daily_all(self, stk_cd: str, base_dt: str, upd_stkpc_tp: str = "1") -> List[Dict[str, Any]]:
        """ka10081: 종목 1개 일봉 전체(연속조회)"""
        api_id = "ka10081"
        body = {"stk_cd": stk_cd, "base_dt": base_dt, "upd_stkpc_tp": upd_stkpc_tp}

        out: List[Dict[str, Any]] = []
        cont_yn, next_key = None, None

        while True:
            data, h = self._post_chart(api_id, body, cont_yn=cont_yn, next_key=next_key)

            rows = data.get("stk_dt_pole_chart_qry", [])
            if not isinstance(rows, list):
                rows = []
            out.extend(rows)

            if h.get("cont-yn") != "Y":
                break
            cont_yn, next_key = "Y", h.get("next-key")

        return out

    def fetch_minute_all(self, stk_cd: str, base_dt: str, tic_scope: str = "1", upd_stkpc_tp: str = "1") -> List[Dict[str, Any]]:
        """ka10080: 종목 1개 분봉 전체(연속조회)"""
        api_id = "ka10080"
        body = {"stk_cd": stk_cd, "tic_scope": tic_scope, "upd_stkpc_tp": upd_stkpc_tp, "base_dt": base_dt}

        out: List[Dict[str, Any]] = []
        cont_yn, next_key = None, None

        while True:
            data, h = self._post_chart(api_id, body, cont_yn=cont_yn, next_key=next_key)

            rows = data.get("stk_min_pole_chart_qry", [])
            if not isinstance(rows, list):
                rows = []
            out.extend(rows)

            if h.get("cont-yn") != "Y":
                break
            cont_yn, next_key = "Y", h.get("next-key")

        return out