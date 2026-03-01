from pathlib import Path
import pandas as pd

# 입력/출력 경로
IN_PATH = Path("auto_trade/data/stock_master.csv")
OUT_PATH = Path("stkstats/data/raw/stock_list.csv")

def main():
    if not IN_PATH.exists():
        raise FileNotFoundError(f"입력 파일이 없습니다: {IN_PATH.resolve()}")

    # stock_master.csv는 "헤더 없음"으로 보이므로 header=None
    # 인코딩은 환경마다 달라서 utf-8-sig 먼저 시도, 실패하면 cp949로 재시도
    try:
        df = pd.read_csv(IN_PATH, header=None, dtype=str, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(IN_PATH, header=None, dtype=str, encoding="cp949")

    if df.shape[1] < 2:
        raise RuntimeError(f"컬럼이 2개 미만입니다. 현재 컬럼 수={df.shape[1]}")

    out = df.iloc[:, [0, 1]].copy()
    out.columns = ["stk_cd", "stk_nm"]

    # 공백 제거 + 종목코드 6자리 보장(앞 0 유지)
    out["stk_cd"] = out["stk_cd"].str.strip().str.zfill(6)
    out["stk_nm"] = out["stk_nm"].str.strip()

    # 비어있는 행 제거 + 중복 제거
    out = out.dropna()
    out = out[(out["stk_cd"] != "") & (out["stk_nm"] != "")]

    # 헤더/쓰레기 제거
    out["stk_cd"] = out["stk_cd"].str.strip()
    out = out[~out["stk_cd"].str.lower().isin(["00code", "code", "stk_cd", "종목코드"])]

    # 주식만: 6자리 숫자만 남김
    out = out[out["stk_cd"].str.fullmatch(r"\d{6}")]

    out = out.drop_duplicates(subset=["stk_cd"]).reset_index(drop=True)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    print(f"[OK] saved: {OUT_PATH} rows={len(out)}")
    print(out.head(10).to_string(index=False))

if __name__ == "__main__":
    main()