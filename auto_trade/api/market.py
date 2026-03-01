# api/market.py (MarketAPI 클래스 안에 추가 추천)
import csv
from pathlib import Path
import requests
from auto_trade.config.config import get_base_url

class MarketAPI:
    def __init__(self, auth):
        self.base_url = get_base_url()
        self.token = auth.access_token

    def get_stock_info(self, stock_code: str) -> dict:
        """
        ka10001: 종목 현재 시세(단건)
        반환 dict에 cur_prc(현재가), flu_rt(등락률), trde_qty(거래량) 등이 들어옴
        """
        url = f"{self.base_url}/api/dostk/stkinfo"

        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {self.token}",
            "api-id": "ka10001"
        }

        body = {"stk_cd": stock_code}

        resp = requests.post(url, headers=headers, json=body, timeout=20)
        resp.raise_for_status()
        return resp.json()

    def fetch_stock_list(self, mrkt_tp: str) -> list[dict]:
        """
        ka10099 종목정보 리스트
        mrkt_tp: "0"(코스피), "10"(코스닥), ... (문서 참고)
        return: [{"code":..., "name":..., "lastPrice":..., "marketName":...}, ...]
        """
        url = f"{self.base_url}/api/dostk/stkinfo"  # 문서에 나온 URL 그대로 사용

        cont_yn = "N"
        next_key = ""

        out: list[dict] = []

        while True:
            headers = {
                "Content-Type": "application/json;charset=UTF-8",
                "authorization": f"Bearer {self.token}",
                "api-id": "ka10099",
                "cont-yn": cont_yn,
                "next-key": next_key,
            }
            body = {"mrkt_tp": mrkt_tp}

            resp = requests.post(url, headers=headers, json=body, timeout=20)
            print("HTTP", resp.status_code, "len=", len(resp.text))
            resp.raise_for_status()

            data = resp.json()

            # 응답 구조가 문서처럼 { list: [...] } 일 때
            items = data.get("list", []) if isinstance(data, dict) else []
            for it in items:
                out.append({
                    "code": str(it.get("code", "")).strip(),
                    "name": str(it.get("name", "")).strip(),
                    "lastPrice": str(it.get("lastPrice", "")).strip(),   # 전일종가
                    "marketCode": str(it.get("marketCode", "")).strip(),
                    "marketName": str(it.get("marketName", "")).strip(),
                })

            # 연속조회 키는 "헤더"에 있을 수도 있고 "바디"에 있을 수도 있어서 둘 다 시도
            cont_yn = (
                resp.headers.get("cont-yn")
                or data.get("cont-yn")
                or data.get("cont_yn")
                or "N"
            )
            next_key = (
                resp.headers.get("next-key")
                or data.get("next-key")
                or data.get("next_key")
                or ""
            )

            if cont_yn != "Y":
                break

        # 혹시 빈 code 제거
        out = [x for x in out if x["code"]]
        return out

    def update_stock_master_csv(self, mrkt_types: list[str] = None):
        if mrkt_types is None:
            mrkt_types = ["0", "10"]

        all_rows: list[dict] = []

        for mt in mrkt_types:
            try:
                rows = self.fetch_stock_list(mt)
                print(f"✅ mrkt_tp={mt} rows={len(rows):,}")
                for r in rows:
                    r["mrkt_tp"] = mt
                all_rows.extend(rows)
            except Exception as e:
                print(f"❌ mrkt_tp={mt} 실패: {e}")
                # 여기서 다음 시장 계속 진행
                continue

        if not all_rows:
            raise RuntimeError("종목 리스트를 하나도 못 가져왔습니다.")

        # 중복 제거(코드 기준)
        uniq = {}
        for r in all_rows:
            code = (r.get("code") or "").strip()
            if code:
                uniq[code] = r
        final_rows = list(uniq.values())

        from pathlib import Path
        import csv

        out_dir = Path(__file__).resolve().parents[1] / "data"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "stock_master.csv"

        fieldnames = ["code", "name", "lastPrice", "marketCode", "marketName", "mrkt_tp"]
        with out_path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in sorted(final_rows, key=lambda x: (x.get("mrkt_tp", ""), x.get("code", ""))):
                w.writerow({k: r.get(k, "") for k in fieldnames})

        print(f"✅ saved: {out_path} ({len(final_rows):,} rows)")
        return out_path

def load_stock_master(csv_path: str | None = None) -> dict:
    """
    return: { "005930": {"name": "...", "lastPrice": 71000, ...}, ... }
    """
    if csv_path is None:
        csv_path = Path(__file__).resolve().parents[1] / "data" / "stock_master.csv"

    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"stock_master.csv 파일이 없습니다: {csv_path}")

    out = {}
    with csv_path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("code") or "").strip()
            if not code:
                continue

            name = (row.get("name") or "").strip()

            lp = (row.get("lastPrice") or "").replace(",", "").strip()
            try:
                last_price = abs(int(float(lp))) if lp else 0
            except:
                last_price = 0

            out[code] = {
                "name": name,
                "lastPrice": last_price,
                "marketCode": (row.get("marketCode") or "").strip(),
                "marketName": (row.get("marketName") or "").strip(),
                "mrkt_tp": (row.get("mrkt_tp") or "").strip(),
            }
    return out