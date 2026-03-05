"""
TP/SL 그리드 분석 (gap_entry_grid_detail 기반).
실행: python -m stkstats.analysis.entry_tp_sl.tp_sl_grid
"""
import pandas as pd

from stkstats.analysis._common import load_parquet

DETAIL_PATH = "stkstats/data/derived/gap_entry_grid_detail_2025.parquet"

df = load_parquet(DETAIL_PATH)

# entry = -5% 전략만 사용
df = df[df["entry_k"] == 0.95].copy()

# gap 필터 (현재 최적)
df = df[df["gap"] < 0.25]

# TP / SL 후보
tp_list = [0.05, 0.06, 0.07, 0.08, 0.09, 0.10]
sl_list = [0.03, 0.04, 0.05, 0.06]

results = []

for tp in tp_list:
    for sl in sl_list:

        tp_price = df["entry"] * (1 + tp)
        sl_price = df["entry"] * (1 - sl)

        hit_tp = df["tp"] >= tp_price
        hit_sl = df["sl"] <= sl_price

        result = []

        for i in range(len(df)):

            if hit_tp.iloc[i] and not hit_sl.iloc[i]:
                result.append("TP")

            elif hit_sl.iloc[i] and not hit_tp.iloc[i]:
                result.append("SL")

            elif hit_tp.iloc[i] and hit_sl.iloc[i]:
                # BOTH → 기존 결과 사용
                result.append(df.iloc[i]["result"])

            else:
                result.append("NONE")

        r = pd.Series(result)

        trades = (r != "NONE").sum()

        tp_cnt = (r == "TP").sum()
        sl_cnt = (r == "SL").sum()

        winrate = tp_cnt / trades if trades else 0

        ev = ((tp_cnt * tp) - (sl_cnt * sl)) / trades if trades else 0

        results.append({
            "TP": tp,
            "SL": sl,
            "trades": trades,
            "winrate": winrate,
            "EV": ev
        })

res = pd.DataFrame(results)

print("\n=== TP/SL GRID RESULTS ===")

print(res.sort_values("EV", ascending=False).head(15))


if __name__ == "__main__":
    pass  # script runs on import above; or call main() if we refactor
