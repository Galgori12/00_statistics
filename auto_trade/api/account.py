import requests
from auto_trade.config.config import get_base_url


class AccountAPI:
    def __init__(self, auth):
        self.base_url = get_base_url()
        self.token = auth.access_token

    def _post(self, api_id: str, body: dict | None = None):
        url = f"{self.base_url}/api/dostk/acnt"
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {self.token}",
            "api-id": api_id,
        }
        print("POST", api_id, body)

        resp = requests.post(url, headers=headers, json=(body or {}), timeout=10)
        # 에러일 때 원인 로그 보기 좋게
        try:
            data = resp.json()
        except Exception:
            resp.raise_for_status()
            raise
        if resp.status_code >= 400:
            raise RuntimeError(f"{api_id} HTTP {resp.status_code}: {data}")
        return data

    # ✅ ka00001: 계좌번호조회
    def get_account_numbers(self):
        data = self._post("ka00001", body={})
        # 문서상 body에 acctNo 가 옴 (String)
        acct = None
        if isinstance(data, dict):
            acct = data.get("acctNo") or data.get("acct_no") or data.get("acctno")
            # 어떤 응답은 data/output 같은 래핑이 있을 수 있어 방어
            if acct is None:
                for k in ("data", "output", "result", "res"):
                    if isinstance(data.get(k), dict):
                        acct = data[k].get("acctNo") or data[k].get("acct_no") or data[k].get("acctno")
                        if acct:
                            break

        # acct가 "12345678;87654321" 같은 형태일 수도 있어서 분해
        accounts = []
        if isinstance(acct, list):
            accounts = [str(x).strip() for x in acct if str(x).strip()]
        elif isinstance(acct, str):
            s = acct.strip()
            if s:
                # 구분자가 ; , 공백 등일 가능성 대비
                for sep in (";", ",", " "):
                    if sep in s:
                        parts = [p.strip() for p in s.split(sep)]
                        accounts = [p for p in parts if p]
                        break
                else:
                    accounts = [s]

        return accounts, data

    # ✅ kt00004: 계좌평가현황요청 (지금 계좌정보 표는 이걸로 채우면 됨)
    def get_account_evaluation(self, account_no: str | None = None, qry_tp="1", dmst_stex_tp="KRX"):
        body = {"qry_tp": qry_tp, "dmst_stex_tp": dmst_stex_tp}
        if account_no:
            body["acctNo"] = account_no
        return self._post("kt00004", body=body)

    # ✅ (옵션) kt00005: 체결잔고요청 (출금가능/주문가능 등 상세)
    def get_filled_balance(self, dmst_stex_tp="KRX"):
        body = {"dmst_stex_tp": dmst_stex_tp}
        return self._post("kt00005", body=body)