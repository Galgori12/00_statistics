import os
from dotenv import load_dotenv

load_dotenv()

REAL_BASE_URL = "https://api.kiwoom.com"
MOCK_BASE_URL = "https://mockapi.kiwoom.com"


def get_mode():
    return os.getenv("MODE", "mock")  # 기본 mock


def get_base_url():
    return REAL_BASE_URL if get_mode() == "real" else MOCK_BASE_URL


def get_app_key():
    if get_mode() == "real":
        return os.getenv("REAL_APP_KEY")
    return os.getenv("MOCK_APP_KEY")


def get_app_secret():
    if get_mode() == "real":
        return os.getenv("REAL_APP_SECRET")
    return os.getenv("MOCK_APP_SECRET")