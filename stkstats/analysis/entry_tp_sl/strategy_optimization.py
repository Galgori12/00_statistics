"""
전략 최적화: GAP × ENTRY EV 매트릭스, gap filter 테스트.
실행: python -m stkstats.analysis.entry_tp_sl.strategy_optimization
"""
import pandas as pd

from stkstats.analysis._common import load_parquet

DETAIL_PATH = "stkstats/data/derived/gap_entry_grid_detail_2025.parquet"

df = load_parquet(DETAIL_PATH)

# 진입한 거래만 사용
df = df[df["result"] != "NO_ENTRY"].copy()

# TP/SL만 분석
df = df[df["result"].isin(["TP", "SL"])]

# 수익률
df["ret"] = df["result"].map({
    "TP": 0.07,
    "SL": -0.04
})

print("\n==============================")
print("1️⃣ GAP × ENTRY EV MATRIX")
print("==============================")

pivot = df.pivot_table(
    values="ret",
    index="gap_bin",
    columns="entry_k",
    aggfunc="mean"
)

print(pivot)

print("\n==============================")
print("2️⃣ ENTRY 전체 성능")
print("==============================")

entry_summary = df.groupby("entry_k").agg(
    trades=("ret", "count"),
    winrate=("result", lambda x: (x == "TP").mean()),
    EV=("ret", "mean")
)

print(entry_summary)

print("\n==============================")
print("3️⃣ GAP FILTER TEST")
print("==============================")

filters = [
    ("NO_FILTER", None),
    ("gap < 25", df["gap"] < 0.25),
    ("gap < 20", df["gap"] < 0.20),
    ("gap < 15", df["gap"] < 0.15),
    ("gap 3~20", (df["gap"] > 0.03) & (df["gap"] < 0.20))
]

results = []

for name, cond in filters:

    if cond is None:
        d = df
    else:
        d = df[cond]

    for entry in [0.95, 0.96, 0.97]:

        d2 = d[d["entry_k"] == entry]

        if len(d2) == 0:
            continue

        trades = len(d2)
        winrate = (d2["result"] == "TP").mean()
        ev = d2["ret"].mean()

        results.append({
            "filter": name,
            "entry": entry,
            "trades": trades,
            "winrate": winrate,
            "EV": ev
        })

opt = pd.DataFrame(results)

print(opt.sort_values("EV", ascending=False).head(10))

print("\n==============================")
print("4️⃣ TOP STRATEGY CANDIDATES")
print("==============================")

best = opt.sort_values("EV", ascending=False).head(5)

print(best)


if __name__ == "__main__":
    pass
