import time
from datetime import datetime
from auto_trade.api.market import MarketAPI
from auto_trade.api.order import OrderAPI


class AutoTrader:
    def __init__(self, auth, account_no):
        self.market = MarketAPI(auth)
        self.order = OrderAPI(auth, account_no)

    def wait_market_open(self):
        print("⏳ 장 시작 대기중...")
        while True:
            now = datetime.now().strftime("%H:%M")
            if now >= "09:00":
                print("🚀 장 시작!")
                break
            time.sleep(20)

    def run(self, stock_code):
        self.wait_market_open()

        # 1️⃣ 시장가 매수
        buy_result = self.order.buy_market(stock_code, 1)

        if buy_result.get("return_code") != 0:
            print("❌ 매수 실패:", buy_result)
            return

        print("✅ 매수 주문 완료")

        # 2️⃣ 매수가 확인 (현재가로 대체)
        time.sleep(2)
        data = self.market.get_stock_info(stock_code)
        buy_price = abs(int(data.get("cur_prc")))

        target_price = int(buy_price * 1.10)
        stop_price = int(buy_price * 0.95)

        print(f"🎯 익절가: {target_price}")
        print(f"🛑 손절가: {stop_price}")

        # 3️⃣ 감시 루프
        while True:
            data = self.market.get_stock_info(stock_code)
            current_price = abs(int(data.get("cur_prc")))

            print(f"📡 현재가: {current_price}")

            if current_price >= target_price:
                print("💰 익절 실행")
                print(self.order.sell_market(stock_code, 1))
                break

            if current_price <= stop_price:
                print("🛑 손절 실행")
                print(self.order.sell_market(stock_code, 1))
                break

            time.sleep(3)