import pandas as pd

# 설정
TP = 0.13
SL = -0.05

df = pd.read_excel("sample4.xlsx")

results = []
pnl = []

for _, r in df.iterrows():

    high = float(r["고가"]) / 100
    low = float(r["저가"]) / 100
    close = float(r["종가"]) / 100
    order = str(r["순서"]).strip()

    result = "NONE"
    trade_pnl = close

    # -------------------------
    # 시고저종
    # -------------------------
    if order == "시고저종":

        if high >= TP:
            result = "TP_FIRST"
            trade_pnl = TP

        elif low <= SL:
            result = "SL_ONLY"
            trade_pnl = SL

        else:
            result = "NONE"
            trade_pnl = close

    # -------------------------
    # 시저고종
    # -------------------------
    elif order == "시저고종":

        if low <= SL:
            result = "SL_FIRST"
            trade_pnl = SL

        elif high >= TP:
            result = "TP_ONLY"
            trade_pnl = TP

        else:
            result = "NONE"
            trade_pnl = close

    # -------------------------
    # first dip (정수)
    # -------------------------
    else:

        try:
            dip = float(order.replace("%", "")) / 100

            if dip <= SL:
                result = "SL_FIRST"
                trade_pnl = SL

            elif high >= TP:
                result = "TP_ONLY"
                trade_pnl = TP

            else:
                result = "NONE"
                trade_pnl = close

        except:
            result = "UNKNOWN"
            trade_pnl = 0

    results.append(result)
    pnl.append(trade_pnl)

df["result"] = results
df["pnl"] = pnl

# 결과 출력
print(df["result"].value_counts())

# EV 계산
ev = df["pnl"].mean()

print("총 트레이드:", len(df))
print("평균 기대값 EV:", round(ev * 100, 2), "%")

# 결과 저장
df.to_csv("excel_backtest_result.csv", index=False)