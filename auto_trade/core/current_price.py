import requests
from auto_trade.config import get_base_url


class MarketAPI:
    def __init__(self, auth):
        self.base_url = get_base_url()
        self.token = auth.access_token

    def get_current_price(self, stock_code):
        url = f"{self.base_url}/api/dostk/price"  # ← 여기 실제 URL로 바꿔야 함

        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {self.token}",
            "api-id": "ka10001"  # ← 실제 API ID로 교체 필요
        }

        body = {
            "stk_cd": stock_code
        }

        response = requests.post(url, headers=headers, json=body)
        data = response.json()

        print("현재가 조회 응답:", data)

        return data