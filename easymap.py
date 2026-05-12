# -*- coding: utf-8 -*-
"""
Easymap 爬蟲模組
目標：內政部地籍圖資 https://easymap.moi.gov.tw/Z10Web/
功能：輸入縣市/鄉鎮/段別/地號 → 取得 WGS84 座標（lat/lng）

破解流程：
  1. GET /Z10Web/ → 取得 session cookie
  2. POST /Z10Web/layout/setToken.jsp → 解析 HTML 取得 anti-bot token
  3. 後續所有 POST 都帶 struts.token.name=token 與 token=<值>
"""

import re
import logging
import requests

logger = logging.getLogger(__name__)

HOST = "https://easymap.moi.gov.tw"


class EasymapCrawler:
    """每個查詢建立一個新 session，避免 token 過期問題"""

    def __init__(self):
        self.session = requests.Session()
        # 設定 User-Agent 避免被擋
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
        })
        self.token = ""

    def init(self):
        """初始化：取得 session cookie + anti-bot token"""
        self.session.get(f"{HOST}/Z10Web/", timeout=10)
        res = self.session.post(f"{HOST}/Z10Web/layout/setToken.jsp", timeout=10)
        match = re.search(r'name="token"\s+value=["\']([^"\']+)["\']', res.text)
        if not match:
            raise RuntimeError("Easymap：無法取得 token，網站可能已變更結構")
        self.token = match.group(1)

    def _post(self, endpoint, params=None):
        """帶 token 的 POST 請求，回傳 JSON"""
        data = {"struts.token.name": "token", "token": self.token}
        if params:
            data.update(params)
        res = self.session.post(f"{HOST}/Z10Web/{endpoint}", data=data, timeout=10)
        return res.json()

    @staticmethod
    def _norm(s):
        """台 → 臺 正規化，讓「台東縣」和「臺東縣」都能查到"""
        return s.replace("台", "臺")

    def get_cities(self):
        """取得所有縣市清單，回傳 [{"id": ..., "name": ...}, ...]"""
        self.init()
        return self._post("City_json_getList")

    def get_towns(self, city_code):
        """取得指定縣市的鄉鎮清單"""
        self.init()
        return self._post("City_json_getTownList", {"cityCode": city_code})

    def get_sections(self, city_code, town_code):
        """取得指定鄉鎮的段別清單"""
        self.init()
        return self._post("City_json_getSectionList", {
            "cityCode": city_code,
            "townCode": town_code,
        })

    def locate(self, sect_no, office, land_no):
        """
        用段號 + 辦事處代碼 + 地號取得座標
        land_no 格式：8碼字串，主號 4 碼 + 子號 4 碼
        例：
          "0100-0021" → 主號 "0100" + 子號 "0021" → "01000021"
          "364"       → 主號 "0364" + 子號 "0000" → "03640000"
          "364-1"     → 主號 "0364" + 子號 "0001" → "03640001"
        回傳 {"lat": ..., "lng": ...} 或 None
        """
        self.init()
        # 地號分成主號/子號，各 zfill 到 4 碼後拼起來（修正：原本 zfill(8) 會把 "364" 變成 "00000364" 是錯的）
        raw = str(land_no).strip().replace(" ", "")
        if "-" in raw:
            main, sub = raw.split("-", 1)
        else:
            main, sub = raw, "0"
        # 主號/子號只保留數字
        main = "".join(ch for ch in main if ch.isdigit())
        sub  = "".join(ch for ch in sub  if ch.isdigit()) or "0"
        land_no_clean = main.zfill(4) + sub.zfill(4)
        res = self._post("Land_json_locate", {
            "sectNo": sect_no,
            "office": office,
            "landNo": land_no_clean,
        })
        logger.info("Easymap locate 回應 (landNo=%s): %s", land_no_clean, res)
        if res and res.get("X") and res.get("Y"):
            # Easymap 回傳 X=經度、Y=緯度（已是 WGS84，可直接用於 Leaflet）
            return {"lat": float(res["Y"]), "lng": float(res["X"])}
        return None
