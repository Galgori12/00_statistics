# stkstats Strategy Definition

This document describes the trading strategy used for backtesting.

---

# Strategy Concept

The strategy targets stocks that hit an **upper price limit** and trades the next trading day based on a pullback entry.

Key idea

After a strong momentum event (limit up), a controlled dip can offer a high-probability rebound trade.

---

# Entry Rule

Entry price

entry = t1_open * 0.97

Meaning

Enter when price drops **3% below the next day open**.

Purpose

Avoid chasing the open and wait for a pullback.

---

# Take Profit

TP = entry * 1.07

Profit target

+7%

---

# Stop Loss

SL = entry * 0.96

Loss limit

-4%

---

# Trade Resolution Logic

Entry condition

low ≤ entry price

If entry occurs

Check which event happens first:

1. high ≥ TP → Take Profit
2. low ≤ SL → Stop Loss

If both occur

Minute data is used to determine the correct order.

---

# Minute Resolution Rule

1. Sort minute data by `cntr_tm` ascending
2. Detect first entry trigger
3. After entry:

   * check TP hit
   * check SL hit
4. First hit determines the result.

Possible outcomes

* TP_ONLY
* SL_ONLY
* TP_FIRST
* SL_FIRST
* NONE_AFTER_ENTRY
* AMBIGUOUS_SAME_MIN

---

# Key Metrics

| metric  | meaning                  |
| ------- | ------------------------ |
| TRADES  | total executed trades    |
| TP      | take profit count        |
| SL      | stop loss count          |
| WINRATE | TP / trades              |
| EV      | expected value per trade |

EV calculation

EV = (winrate × TP_return) + (lossrate × SL_return)

---

# Research Goals

1. Identify optimal entry conditions
2. Evaluate gap effects
3. Improve EV of the strategy
4. Reduce ambiguous trade outcomes
