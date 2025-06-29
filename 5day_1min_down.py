import os
import time
import datetime
import pandas as pd
import requests

from utils import KiwoomTR
from config import host

# 요청 간 최소 대기 시간 (초)
REQUEST_INTERVAL = 0.2


class KiwoomRestAPI:
    """REST API 기반의 간단한 래퍼"""

    def __init__(self):
        self.tr = KiwoomTR()

    def get_stock_codes(self):
        """모든 상장 종목 코드를 반환한다."""
        stock_list = self.tr.fn_ka10099(data={})
        codes = []
        for item in stock_list:
            code = item.get("stk_cd", "")
            codes.append(code.replace("_AL", ""))
        return codes

    def request_daily(self, code: str, days: int = 6) -> pd.DataFrame:
        """일봉 데이터를 조회한다."""
        params = {
            "stk_cd": f"{code}_AL",
            "qry_dt": datetime.datetime.now().strftime("%Y%m%d"),
            "indc_tp": "0",
        }
        dfs = []
        cont_yn = "N"
        next_key = ""
        for _ in range(10):
            df, has_next, next_key = self.tr.fn_ka10086(
                data=params,
                cont_yn=cont_yn,
                next_key=next_key,
            )
            dfs.append(df)
            if not has_next:
                break
            cont_yn = "Y"
            time.sleep(REQUEST_INTERVAL)
        if not dfs:
            return pd.DataFrame(columns=["date", "close"])
        all_df = pd.concat(dfs).reset_index(drop=True)
        all_df.rename(columns={"날짜": "date", "종가": "close"}, inplace=True)
        all_df["date"] = pd.to_datetime(all_df["date"], format="%Y%m%d")
        cutoff = datetime.datetime.now() - datetime.timedelta(days=days)
        return all_df[all_df["date"] >= cutoff][["date", "close"]]

    def request_minute(self, code: str, days: int = 5) -> pd.DataFrame:
        """1분봉 데이터를 조회한다. 실 서비스에서는 해당 REST 엔드포인트가 변경될 수 있다."""
        endpoint = "/api/dostk/minute-chart"
        url = host + endpoint
        start_dt = datetime.datetime.now() - datetime.timedelta(days=days)

        dfs = []
        cont_yn = "N"
        next_key = ""
        while True:
            headers = {
                "Content-Type": "application/json;charset=UTF-8",
                "authorization": f"Bearer {self.tr.token}",
                "cont-yn": cont_yn,
                "next-key": next_key,
                "api-id": "ka10080",
            }
            data = {
                "stk_cd": f"{code}_AL",
                "inq_time": datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
                "time_unit": "1",
            }
            resp = requests.post(url, headers=headers, json=data)
            resp.raise_for_status()
            res = resp.json().get("chart", [])
            if not res:
                break
            df = pd.DataFrame(res)
            # 예상 필드 매핑
            col_map = {
                "date": "date",
                "time": "time",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
            }
            df.rename(columns=col_map, inplace=True)
            df["datetime"] = pd.to_datetime(df["date"] + df["time"], format="%Y%m%d%H%M%S")
            df = df[["datetime", "open", "high", "low", "close", "volume"]]
            dfs.append(df)
            if resp.headers.get("cont-yn") != "Y":
                break
            next_key = resp.headers.get("next-key", "")
            cont_yn = "Y"
            if df["datetime"].min() <= start_dt:
                break
            time.sleep(REQUEST_INTERVAL)
        if not dfs:
            return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])
        all_df = pd.concat(dfs)
        return all_df[all_df["datetime"] >= start_dt].sort_values("datetime").reset_index(drop=True)


def fetch_selected_minute():
    save_dir = r"C:\\Users\\heo31\\PycharmProjects\\stock\\data_selected_minute"
    os.makedirs(save_dir, exist_ok=True)

    api = KiwoomRestAPI()

    codes = api.get_stock_codes()

    for idx, code in enumerate(codes, 1):
        print(f"[{idx}/{len(codes)}] {code} -> 일봉 확인 중...")
        try:
            df_daily = api.request_daily(code, days=6)
            df_daily["pct_change"] = df_daily["close"].pct_change()
            cnt = (df_daily["pct_change"] >= 0.15).sum()
            if cnt < 2:
                print(f"  {code} 조건 미달 ({cnt}일) - 스킵")
                continue
            print(f"  {code} 조건 충족 ({cnt}일) -> 1분봉 수집")
            df_min = api.request_minute(code, days=5)
            if df_min.empty:
                print(f"  {code} 1분봉 데이터 없음")
                continue
            filename = f"{code}_1m.csv"
            path = os.path.join(save_dir, filename)
            df_min.to_csv(path, index=False, encoding="utf-8-sig")
            print(f"  저장 완료: {path}")
        except Exception as e:
            print(f"  오류: {e} - 건너뜁니다.")
        time.sleep(REQUEST_INTERVAL)


if __name__ == "__main__":
    fetch_selected_minute()