# real-estate-library（物件庫）

房仲 AI 工具平台 — 物件庫。新增、編輯、刪除物件，整合周邊調查總結與廣告產出；每用戶獨立，管理員可查看各用戶。

## 本地執行

```bash
cd /Users/chenweiliang/Projects/real-estate-library
cp .env.example .env   # 編輯 .env，FLASK_SECRET_KEY 需與 Portal 一致
pip install -r requirements.txt
python app.py
```

預設 port 5004，瀏覽 http://localhost:5004

## 環境變數

| 變數 | 說明 |
|------|------|
| FLASK_SECRET_KEY | 與 Portal、Survey、AD 相同，用於驗證 Portal 的 token |
| PORTAL_URL | 入口網址，登出或 token 失敗時導向 |
| ADMIN_EMAILS | 管理員信箱（逗號分隔），可查看各用戶物件 |
| GCS_BUCKET | 有設定則物件存 GCS，否則存本機 users/（Cloud Run 建議必設，否則無法持久化） |
| PORT | 本機 port，預設 5004 |
| FLASK_DEBUG | 非空則為開發模式 |

## GCS 設定與權限（Cloud Run 出現「無法載入物件列表」時）

1. **確認環境變數**：部署時會從 `~/Projects/.env` 讀取 `GCS_BUCKET` 並傳給 Cloud Run。若 .env 未設或為空，物件庫會無法讀寫 GCS。  
   在 `~/Projects/.env` 加上或確認：`GCS_BUCKET=real-estate-survey-data-0393195862`（與 Survey/Portal 同一個 bucket 即可）。

2. **服務帳號權限**：Cloud Run 使用的服務帳號必須對該 bucket 有讀寫權限。若 Survey 已能正常寫入同一 bucket，通常同一專案下物件庫也會用同一服務帳號；若只有物件庫報錯，可手動授權：
   ```bash
   gcloud run services describe real-estate-library --region asia-east1 --format="value(spec.template.spec.serviceAccountName)"
   gsutil iam ch serviceAccount:上述輸出的信箱:objectAdmin gs://real-estate-survey-data-0393195862
   ```
   若上述第一行輸出為空，則使用專案預設計算服務帳號（例如 `334765337861-compute@developer.gserviceaccount.com`）。

3. **重新部署**：改完 .env 或 IAM 後，執行 `./real-estate-survey/deploy-all.sh` 重新部署，使環境變數生效。

## 部署

- 一鍵部署（含 Portal、Survey、AD、本服務）：在 `~/Projects/` 執行  
  `./real-estate-survey/deploy-all.sh`
- 僅部署本服務：  
  `cd /Users/chenweiliang/Projects/real-estate-library && gcloud run deploy real-estate-library --source . --region asia-east1 --allow-unauthenticated --set-env-vars "FLASK_SECRET_KEY=...,PORTAL_URL=...,ADMIN_EMAILS=...,GCS_BUCKET=..."`

部署完成後，請在 **~/Projects/.env** 加上：

```
LIBRARY_URL=https://你的-real-estate-library-Cloud-Run-網址
FREE_LIBRARY_LIMIT=3
```

再重新部署 Portal（或執行 deploy-all.sh），入口才會出現「物件庫」並正確導向。
