import requests
from auto_trade.config.config import get_base_url, get_app_key, get_app_secret


class KiwoomAuth:
    def __init__(self):
        self.app_key = get_app_key()
        self.app_secret = get_app_secret()
        self.base_url = get_base_url()
        self.access_token = None

    def login(self):
        url = f"{self.base_url}/oauth2/token"

        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "api-id": "au10001"
        }

        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.app_secret
        }

        response = requests.post(url, json=body, headers=headers)
        data = response.json()

        if response.status_code == 200 and "token" in data:
            self.access_token = data["token"]
            print("✅ 로그인 성공")
        else:
            print("❌ 로그인 실패:", data)