# tools/make_stock_master_kiwoom.py

from auto_trade.api.auth import KiwoomAuth
from auto_trade.api.market import MarketAPI

def main():
    auth = KiwoomAuth()   # ✅ 클래스 이름 정확히
    auth.login()

    if not auth.access_token:
        print("❌ 토큰 발급 실패로 종료")
        return

    m = MarketAPI(auth)
    m.update_stock_master_csv(mrkt_types=["0", "10"])  # 코스피, 코스닥

if __name__ == "__main__":
    main()