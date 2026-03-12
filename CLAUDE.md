# 公司物件庫 — 專案規則

## 專案概述
房仲公司內部物件資料庫管理工具。整合 Google Sheets（主資料）+ Firestore（即時查詢）+ Word 物件總表（最新售價/狀態），提供物件搜尋、篩選、排序、買方配對、戰況版、物件總表比對審查等功能。

## 專案結構
```
real-estate-library/
├── app.py              # Flask 後端（所有 API + 前端 HTML 內嵌於此）
├── Dockerfile
└── requirements.txt

~/Projects/
├── export_word_table.py    # 本機 Word 解析器（→ 輸出 CSV + word_meta.json）
├── review_v2.py            # 本機物件總表比對審查工具（port 5100）
├── match_memory.json       # 人工確認記憶庫（review_v2 使用）
├── word_公寓.csv           # export_word_table.py 輸出
├── word_房屋.csv
├── word_農地.csv
├── word_建地.csv
└── word_meta.json          # Word 文件右上角日期（民國年/西元）
```

## Word 物件總表路徑
```
/Users/chenweiliang/Documents/日盛同步/物件總表/New物件總表--公司5張修改版-總價排序.doc
```

## 核心 API 端點
| 端點 | 方法 | 用途 |
|------|------|------|
| `/api/company-properties/search` | GET | 搜尋物件（keyword/category/area/agent/status/price/expiry） |
| `/api/company-properties/options` | GET | 回傳篩選選單（類別/地區/在線+其他經紀人） |
| `/api/buyers` | GET/POST | 買方需求管理 |
| `/api/buyers/match` | POST | 買方配對搜尋（模糊地區、面積、售價） |
| `/api/sheets/sync` | POST | Google Sheets → Firestore 全量同步 |
| `/api/word-snapshot/upload` | POST | 上傳 Word .doc（舊版） |
| `/api/word-snapshot/upload-csv` | POST | 上傳 export_word_table.py 產出的 CSV（推薦） |
| `/api/word-snapshot/meta` | GET/POST | 取得/儲存 Word 文件日期 |
| `/api/word-snapshot/status` | GET | 目前 snapshot 上傳資訊 |
| `/api/word-snapshot/prices` | GET | 售價字典（供前端卡片對比） |
| `/api/me` | GET | 目前登入者姓名/email（session） |
| `/api/war-board` | GET/POST/DELETE | 戰況版（斡旋中物件，存 Firestore） |

## Firestore 集合
- `company_properties`：主物件資料，欄位含 `案名`、`物件地址`、`物件類別`、`經紀人`、`售價(萬)`、`銷售中`（**布林 True/False**，或字串「已下架」/「已成交」）、`委託到期日`、`委託編號`、`資料序號`
- `word_snapshot/latest`：最新上傳的 Word/CSV snapshot，含 `prices`（售價字典）、`doc_date`（文件日期）
- `war_board`：戰況版個人記錄

## 銷售中欄位格式說明（重要）
Firestore 的 `銷售中` 欄位可能是：
- 布林 `True` → 銷售中
- 布林 `False` → 已下架或已成交
- 字串 `"已下架"` / `"已成交"` / `"銷售中"` → 舊格式資料

**判斷函數**（app.py 後端篩選時使用）：
```python
def _is_selling(r):
    v = r.get("銷售中")
    if v is True:  return True
    if v is False: return False
    s = str(v).strip()
    if s in ("True", "銷售中", "true", "1"): return True
    if s in ("False", "已下架", "已成交", "false", "0"): return False
    return True  # 無此欄位視為銷售中
```

## CSV 上傳比對邏輯（重要，已磨合）
上傳 CSV 更新 Firestore 時，採三層比對：

1. **委託號碼精確比對**（最可靠）→ Firestore 的 `委託編號` = CSV 的 `委託號碼`
2. **案名 + 特徵評分比對**（輔助）：
   - 售價差 <5% → +3分；差 >5% → -5分
   - 面積差 <10% → +2分；差 >10% → -3分
   - 有委託到期日 → +1分（較可能是現役物件）
   - 分數 ≥ 0 才更新；< 0 視為同名不同物件，跳過
3. **同名不同委託號碼** → 跳過（不更新銷售中）

**面積欄位對應**：
| CSV 類型 | CSV 面積欄 | Firestore 面積欄 |
|----------|------------|-----------------|
| 農地/建地 | `面積坪` | `地坪` |
| 房屋 | `地坪` | `地坪` |
| 公寓 | `室內坪` / `建坪` | `室內坪` / `建坪` |

農地：Word 中的「台分」已由 `export_word_table.py` 換算成坪（1分=293.4坪）存入 `面積坪`。

## 在線人員（ACTIVE_AGENTS）
```python
ACTIVE_AGENTS = ["張文澤", "陳威良", "雷文海", "歐芷妤", "許荺芯", "蔡秀芳", "李振迎"]
```
- 公司物件庫的「經紀人」篩選下拉：在線人員置頂，其他摺疊
- 預設登入者（從 session 抓）的物件先顯示

## 前端預設狀態（頁面載入時）
公司物件庫 tab 載入後，自動帶入：
- 狀態：銷售中
- 到期日篩選：委託中（未過期）
- 排序：到期日 近→遠
- 經紀人：登入者姓名（從 `/api/me` 取得）

## 到期日篩選與排序（前端重要邏輯）
- **有選到期日篩選時**：一次載入全部資料（`per_page=500` 遞迴翻頁），前端排序後不分頁顯示
- **無到期日篩選時**：正常分頁（每頁 20 筆）
- 民國日期解析：`XXX年M月D日` → 西元 `XXX+1911`年

## 物件總表日期（word_meta.json）
- `export_word_table.py` 解析 Word 時自動擷取文件右上角日期（格式：`115年3月2日`）
- 存入 `word_meta.json`：`{"minguo": "115年3月2日", "western": "2026-03-02", "parsed_at": "..."}`
- 上傳 CSV 時一併上傳此檔 → 存入 Firestore `word_snapshot/latest.doc_date`
- 公司物件庫頁面在上傳按鈕旁顯示此日期
- `review_v2.py` 在頁面頂部顯示此日期

## 部署
- **Cloud Run**：`gcloud run deploy real-estate-library --source . --region asia-east1 --allow-unauthenticated --clear-base-image`
- **本機**：不需要（直接用 Cloud Run）
- `review_v2.py` 和 `export_word_table.py` 是本機工具，不部署

## 本機工具操作流程
```
1. python3 export_word_table.py
   → 產出 word_公寓.csv、word_房屋.csv、word_農地.csv、word_建地.csv、word_meta.json

2. 在公司物件庫頁面「上傳解析 CSV」
   → 一次選取 4 個 CSV + word_meta.json（共 5 個檔案）

3. （可選）python3 review_v2.py
   → 開啟 http://localhost:5100 進行物件總表比對審查
```

## 合作習慣（已磨合的規則）
- **`export_word_table.py` 解析規則不能隨意改動**：已磨合多次，是最精確的版本，修改前必須充分確認影響範圍
- **Firestore 中文欄位名限制**：不能在 `.where()` 或 `.select()` 用中文欄位名（會報 Path not consumed 錯誤），必須全量 `.stream()` 後在 Python 端篩選
- **前端 onclick 字串傳值**：用單引號包字串，不用 `JSON.stringify()`（雙引號會截斷 HTML 屬性）
- **JS template literal 在 Python f-string 中**：反引號會被轉義成 `\``，改用字串拼接避免語法錯誤
- **銷售中欄位比對**：永遠用 `_is_selling()` 函數，不要用 `is not False` 或 `== True`（字串格式會出錯）
- **部署策略**：修改完成且語法確認 OK 後直接部署，不需再問使用者確認
