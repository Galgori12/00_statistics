# stkstats Data Flow

This document describes the data pipeline used in the stkstats analysis project.

---

# 1. Raw Event Collection

Source data of upper limit events.

File

data/raw/upper_limit_events_2023_2025.parquet

Contains all detected upper limit events from 2023–2025.

Output columns (core)

* stk_cd
* limit_dt
* limit_trde_prica
* trde_qty
* stk_nm

Purpose

Base dataset for all strategy analysis.

---

# 2. Event Cleaning

Script

analysis/clean_upper_limit_events.py

Output

data/raw/upper_limit_events_cleaned.parquet

Filtering rules

* invalid trading days removed
* zero volume events removed

Result

3773 events → 3707 cleaned events

Purpose

Create a reliable dataset for next-day analysis.

---

# 3. Minute Data Availability Filter

Script

analysis/filter_events_by_minute.py

Output

data/raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet

Process

Check if minute data is available for the next trading day (t1_dt).

Purpose

Only events with available minute data are kept for intraday strategy testing.

---

# 4. Minute Data Collection

Script

collectors/collect_minute_t1.py

Output directory

data/raw/minute_ohlc_t1/{stk_cd}/{t1_dt}.parquet

Contents

Minute OHLC data for the day after the upper limit event.

Purpose

Resolve intraday price order for strategy simulation.

---

# 5. BOTH Case Resolution

Script

analysis/resolve_both_entry97_tp107_sl096.py

Output

data/derived/both_resolved_minutes_entry97_tp107_sl096_2025.parquet

Process

Cases where both TP and SL are hit during the day are resolved using minute data.

Resolution rule

1. entry triggered
2. first occurrence of TP or SL is detected
3. determine TP_FIRST or SL_FIRST

Purpose

Determine the true outcome of ambiguous cases.

---

# 6. Strategy Evaluation

Script

analysis/analyze_gap_ev_2025.py

Purpose

Calculate expected value (EV) and performance statistics.

Outputs

* win rate
* TP / SL counts
* EV%
* gap statistics

---

# Overall Pipeline

upper_limit_events
→ cleaned_events
→ minute_available_events
→ minute_ohlc_collection
→ both_case_resolution
→ strategy_analysis
