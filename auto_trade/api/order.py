import requests
from auto_trade.config.config import get_base_url


class OrderAPI:
    def __init__(self, auth, account_no):
        self.base_url = get_base_url()
        self.token = auth.access_token
        self.account_no = account_no

    def buy_market(self, stock_code, qty):
        url = f"{self.base_url}/api/dostk/ordr"

        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {self.token}",
            "api-id": "kt10000"
        }

        body = {
            "dmst_stex_tp": "KRX",
            "stk_cd": stock_code,
            "ord_qty": str(qty),
            "trde_tp": "3"
        }

        res = requests.post(url, headers=headers, json=body)
        return res.json()

    def sell_market(self, stock_code, qty):
        url = f"{self.base_url}/api/dostk/ordr"

        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {self.token}",
            "api-id": "kt10001"
        }

        body = {
            "dmst_stex_tp": "KRX",
            "stk_cd": stock_code,
            "ord_qty": str(qty),
            "trde_tp": "3"
        }

        res = requests.post(url, headers=headers, json=body)
        return res.json()