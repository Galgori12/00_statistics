# stkstats Data Schema

## 1. Upper Limit Events (Raw)

Source
`data/raw/upper_limit_events_2023_2025.parquet`

| column           | description |
| ---------------- | ----------- |
| stk_cd           | 종목코드        |
| limit_dt         | 상한가 발생일     |
| limit_trde_prica | 상한가 가격      |
| trde_qty         | 거래량         |
| stk_nm           | 종목명         |

---

## 2. Cleaned Upper Limit Events

Source
`data/raw/upper_limit_events_cleaned.parquet`

| column           | description |
| ---------------- | ----------- |
| stk_cd           | 종목코드        |
| limit_dt         | 상한가 발생일     |
| limit_trde_prica | 상한가 가격      |
| t1_dt            | 다음 거래일      |
| t1_open          | 다음날 시가      |
| t1_high          | 다음날 고가      |
| t1_low           | 다음날 저가      |
| t1_close         | 다음날 종가      |

---

## 3. Minute Available Events

Source
`data/raw/archive/upper_limit_events_cleaned_2025_minute_ok.parquet`

| column      | description |
| ----------- | ----------- |
| stk_cd      | 종목코드        |
| limit_dt    | 상한가 발생일     |
| t1_dt       | 다음 거래일      |
| t1_open     | 다음날 시가      |
| t1_high     | 다음날 고가      |
| t1_low      | 다음날 저가      |
| t1_close    | 다음날 종가      |
| limit_close | 상한가날 종가     |
| gap         | 갭률          |

---

## 4. Minute OHLC Data

Directory
`data/raw/minute_ohlc_t1/{stk_cd}/{t1_dt}.parquet`

| column       | description           |
| ------------ | --------------------- |
| cntr_tm      | 체결시간 (YYYYMMDDHHMMSS) |
| open_pric    | 시가                    |
| high_pric    | 고가                    |
| low_pric     | 저가                    |
| cur_prc      | 현재가 / 종가              |
| trde_qty     | 거래량                   |
| acc_trde_qty | 누적 거래량                |

Notes

* 데이터는 **시간 역순(desc)** 으로 저장될 수 있음
* 분석 시 **cntr_tm 기준 오름차순 정렬 필요**
* `cur_prc` 값이 **음수로 제공될 수 있음 → abs() 처리**

---

## 5. BOTH Resolution Results

Source
`data/derived/both_resolved_minutes_entry97_tp107_sl096_2025.parquet`

| column      | description         |
| ----------- | ------------------- |
| stk_cd      | 종목코드                |
| t1_dt       | 다음 거래일              |
| entry_price | 진입가격                |
| tp_price    | 익절가격                |
| sl_price    | 손절가격                |
| entry_time  | 진입시간                |
| exit_time   | 종료시간                |
| order       | TP_FIRST / SL_FIRST |
| result      | TP / SL             |

---

## Common Strategy Parameters

| parameter   | value                  |
| ----------- | ---------------------- |
| entry rule  | entry = t1_open * 0.97 |
| take profit | TP = entry * 1.07      |
| stop loss   | SL = entry * 0.96      |
