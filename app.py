# -*- coding: utf-8 -*-
"""
房仲工具 — 物件庫（real-estate-library）
物件新增/刪除/編輯，並整合 Survey 環境總結與 AD 產出。每用戶獨立，管理員可查看各用戶。
"""

import os
import json
import re
# favicon 已更新為透明版 (static/favicon.png)
import base64
import uuid
import threading
import urllib.request
import urllib.error
import urllib.parse
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone, date, timedelta

from flask import Flask, request, session, redirect, jsonify, render_template_string
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature

# 先載入 .env，確保後續取環境變數時已包含 .env 中的值
try:
    from dotenv import load_dotenv
    _dir = os.path.dirname(os.path.abspath(__file__))
    for p in (os.path.join(_dir, ".env"), os.path.join(_dir, "..", ".env")):
        if os.path.isfile(p):
            load_dotenv(p, override=False)
            break
except Exception:
    pass

# Gemini 圖片辨識（直接呼叫，不經由 Portal 代理）
# 優先用新版 google.genai，fallback 到舊版 google.generativeai
_GEMINI_KEY = (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or "").strip()
SCREENSHOTONE_KEY = (os.environ.get("SCREENSHOTONE_KEY") or "").strip()
_genai = None
_GEMINI_OK = False
try:
    import google.genai as _genai_new
    _genai = _genai_new
    _GEMINI_OK = bool(_GEMINI_KEY)
    _GEMINI_SDK = "new"
except ImportError:
    try:
        import google.generativeai as _genai_old
        if _GEMINI_KEY:
            _genai_old.configure(api_key=_GEMINI_KEY)
        _genai = _genai_old
        _GEMINI_OK = bool(_GEMINI_KEY)
        _GEMINI_SDK = "old"
    except ImportError:
        _GEMINI_SDK = None

# Firestore（有環境就啟用，否則 None）
try:
    from google.cloud import firestore as _firestore
    _db = None  # 延遲初始化
except ImportError:
    _firestore = None
    _db = None

# SELLER → PEOPLE 整合層：所有準賣方資料來自 people + roles/seller
import seller_facade

def _server_ts():
    """回傳 Firestore SERVER_TIMESTAMP sentinel。"""
    if _firestore is None:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return _firestore.SERVER_TIMESTAMP


def _get_db():
    """取得 Firestore client（延遲初始化）"""
    global _db
    if _db is not None:
        return _db
    if _firestore is None:
        return None
    try:
        _db = _firestore.Client(project=os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT"))
        return _db
    except Exception as e:
        import logging
        logging.warning("Library: Firestore 初始化失敗，使用 GCS/本地 fallback: %s", e)
        return None

app = Flask(__name__)

# 跨工具回饋系統
from feedback_endpoint import bp as _feedback_bp
app.register_blueprint(_feedback_bp)
_secret = os.environ.get("FLASK_SECRET_KEY", "")
if not _secret and not os.environ.get("FLASK_DEBUG"):
    raise RuntimeError("FLASK_SECRET_KEY 未設定。生產環境必須設定此環境變數。")
app.secret_key = _secret or "dev-only-insecure-key"
# SameSite=None：Portal 跨站跳轉後瀏覽器才能正確帶 session cookie
app.config["SESSION_COOKIE_SAMESITE"] = "None"
app.config["SESSION_COOKIE_SECURE"] = True
# Session 持久化：關閉瀏覽器後仍保留 30 天（避免每次都要重新登入）
app.config["PERMANENT_SESSION_LIFETIME"] = 60 * 60 * 24 * 30  # 30 天（秒）

# ─── 開發模式：自動模擬登入 ───
@app.before_request
def auto_login_dev():
    """本地開發時，SKIP_AUTH=true 會自動模擬登入，跳過 Portal token 驗證"""
    if os.getenv('SKIP_AUTH'):
        session['user_email'] = 'dev@test.com'
        session['user_name'] = '開發測試'

PORTAL_URL = (os.environ.get("PORTAL_URL") or "").strip()
BUYER_URL  = (os.environ.get("BUYER_URL") or "").strip()
ADMIN_EMAILS = [e.strip() for e in (os.environ.get("ADMIN_EMAILS") or "").split(",") if e.strip()]
SERVICE_API_KEY = (os.environ.get("SERVICE_API_KEY") or "").strip()
TOKEN_SERIALIZER = URLSafeTimedSerializer(app.secret_key)
TOKEN_MAX_AGE = 300  # 5 分鐘，容忍 Cloud Run cold start

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")

# Gmail SMTP 設定（用於到期日通知）
GMAIL_SENDER   = os.environ.get("GMAIL_SENDER", "")    # 寄件人 Gmail 帳號
GMAIL_APP_PASS = os.environ.get("GMAIL_APP_PASS", "")  # Gmail 應用程式密碼（非登入密碼）
OBJECTS_GCS_PREFIX = (os.environ.get("OBJECTS_GCS_PREFIX") or "objects").strip().rstrip("/")
# 無 GCS 時用本地目錄；Cloud Run 唯讀檔案系統則改用 /tmp
_default_users = os.path.join(_APP_DIR, "users")
if GCS_BUCKET:
    USERS_DIR = _default_users
else:
    try:
        os.makedirs(_default_users, exist_ok=True)
        USERS_DIR = _default_users
    except (PermissionError, OSError):
        USERS_DIR = os.path.join("/tmp", "real-estate-library", "users")
        os.makedirs(USERS_DIR, exist_ok=True)
_gcs_client = None
_gcs_bucket = None

GENERAL_FEEDBACK_FILE = os.path.join(_APP_DIR, "general_feedback.json")

# 物件欄位（與 AD 辨識、Survey 等一致；未來可擴充）
PROPERTY_FIELDS = [
    ("project_name", "物件名稱", "text"),
    ("address", "地址", "text"),
    ("price", "總價（萬）", "number"),
    ("building_ping", "建物坪數", "number"),
    ("land_ping", "土地坪數", "number"),
    ("authority_ping", "權狀坪數", "number"),
    ("layout", "格局", "text"),
    ("floor", "樓層", "text"),
    ("age", "屋齡", "text"),
    ("parking", "車位", "text"),
    ("case_number", "案號", "text"),
    ("location_area", "區域", "text"),
]
EXTRA_FIELDS = [
    ("env_description", "環境說明", "textarea"),
    ("custom_title", "顯示標題", "text"),
    ("survey_summary", "周邊調查總結", "textarea"),
    ("survey_history_id", "Survey 歷史 ID", "text"),
]
AD_OUTPUTS_KEY = "ad_outputs"


def _safe_email(email):
    return email.replace("@", "_at_").replace(".", "_") if email else ""


def _is_admin(email):
    return email in ADMIN_EMAILS


def _get_gcs_bucket():
    global _gcs_client, _gcs_bucket
    if _gcs_bucket is None and GCS_BUCKET:
        from google.cloud import storage
        _gcs_client = storage.Client()
        _gcs_bucket = _gcs_client.bucket(GCS_BUCKET)
    return _gcs_bucket


def _gcs_read(path):
    b = _get_gcs_bucket()
    if not b:
        return None
    blob = b.blob(path)
    if not blob.exists():
        return None
    return blob.download_as_text(encoding="utf-8")


def _gcs_write(path, data_str):
    b = _get_gcs_bucket()
    if not b:
        return False
    blob = b.blob(path)
    blob.upload_from_string(data_str, content_type="application/json")
    return True


def _gcs_list_prefix(prefix):
    b = _get_gcs_bucket()
    if not b:
        return []
    blobs = list(b.list_blobs(prefix=prefix))
    return [blob.name for blob in blobs]


def _objects_dir(email):
    """本地/GCS fallback 目錄（Firestore 優先時不會用到）"""
    safe = _safe_email(email)
    if not safe:
        return None, None
    if GCS_BUCKET:
        return None, f"users/{safe}/{OBJECTS_GCS_PREFIX}"
    d = os.path.join(USERS_DIR, safe, "objects")
    os.makedirs(d, exist_ok=True)
    return d, None


def _list_user_ids(email):
    """列出用戶所有物件 ID。優先 Firestore，否則 GCS/本地。"""
    db = _get_db()
    if db and email:
        try:
            docs = db.collection("users").document(email).collection("objects").select([]).stream()
            return [doc.id for doc in docs]
        except Exception as e:
            import logging
            logging.warning("Library: Firestore 列出物件失敗: %s", e)

    local_dir, gcs_prefix = _objects_dir(email)
    if local_dir:
        if not os.path.isdir(local_dir):
            return []
        return [f.replace(".json", "") for f in os.listdir(local_dir) if f.endswith(".json")]
    if gcs_prefix:
        names = _gcs_list_prefix(gcs_prefix + "/")
        return [os.path.basename(n).replace(".json", "") for n in names if n.endswith(".json")]
    return []


def _load_object(email, obj_id):
    """讀取一筆物件。優先 Firestore，否則 GCS/本地。"""
    if not email or not obj_id:
        return None
    obj_id = os.path.basename(obj_id).replace("..", "")

    db = _get_db()
    if db:
        try:
            doc = db.collection("users").document(email).collection("objects").document(obj_id).get()
            if doc.exists:
                data = doc.to_dict()
                data.pop("_id", None)
                return data
        except Exception as e:
            import logging
            logging.warning("Library: Firestore 讀取物件失敗: %s", e)

    # Fallback：GCS / 本地
    safe = _safe_email(email)
    if not safe:
        return None
    local_dir, gcs_prefix = _objects_dir(email)
    if local_dir:
        fpath = os.path.join(local_dir, f"{obj_id}.json")
        if not os.path.isfile(fpath):
            return None
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    if gcs_prefix:
        raw = _gcs_read(f"{gcs_prefix}/{obj_id}.json")
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None
    return None


def _save_object(email, obj_id, data):
    """儲存物件。優先 Firestore，否則 GCS/本地。"""
    if not email or not obj_id:
        return False
    obj_id = os.path.basename(obj_id).replace("..", "")
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    data["owner_email"] = email
    if "id" not in data:
        data["id"] = obj_id

    db = _get_db()
    if db:
        try:
            db.collection("users").document(email).collection("objects").document(obj_id).set(data)
            return True
        except Exception as e:
            import logging
            logging.warning("Library: Firestore 儲存物件失敗，改用 GCS/本地: %s", e)

    # Fallback
    safe = _safe_email(email)
    if not safe:
        return False
    data_str = json.dumps(data, ensure_ascii=False, indent=2)
    local_dir, gcs_prefix = _objects_dir(email)
    if local_dir:
        fpath = os.path.join(local_dir, f"{obj_id}.json")
        tmp = fpath + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(data_str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, fpath)
        return True
    if gcs_prefix:
        return _gcs_write(f"{gcs_prefix}/{obj_id}.json", data_str)
    return False


def _delete_object(email, obj_id):
    """刪除物件。優先 Firestore，否則 GCS/本地。"""
    if not email or not obj_id:
        return False
    obj_id = os.path.basename(obj_id).replace("..", "")

    db = _get_db()
    if db:
        try:
            db.collection("users").document(email).collection("objects").document(obj_id).delete()
            return True
        except Exception as e:
            import logging
            logging.warning("Library: Firestore 刪除物件失敗，改用 GCS/本地: %s", e)

    # Fallback
    safe = _safe_email(email)
    if not safe:
        return False
    local_dir, gcs_prefix = _objects_dir(email)
    if local_dir:
        fpath = os.path.join(local_dir, f"{obj_id}.json")
        if os.path.isfile(fpath):
            try:
                os.remove(fpath)
                return True
            except Exception:
                return False
        return False
    if gcs_prefix:
        b = _get_gcs_bucket()
        if not b:
            return False
        blob = b.blob(f"{gcs_prefix}/{obj_id}.json")
        if blob.exists():
            blob.delete()
            return True
    return False


def _list_users_with_objects():
    """列出所有有物件的用戶（管理員用）。優先 Firestore，否則 GCS/本地。"""
    db = _get_db()
    if db:
        try:
            # 列出 users 集合的所有 document（每個 document 代表一個用戶）
            users = [doc.id for doc in db.collection("users").select([]).stream()]
            return sorted(users)
        except Exception as e:
            import logging
            logging.warning("Library: Firestore 列出用戶失敗，改用 GCS: %s", e)

    if GCS_BUCKET:
        blobs = list(_get_gcs_bucket().list_blobs(prefix="users/"))
        prefixes = set()
        for blob in blobs:
            parts = blob.name.split("/")
            if len(parts) >= 2:
                prefixes.add(parts[1])
        return sorted(prefixes)
    if not os.path.isdir(USERS_DIR):
        return []
    return sorted([d for d in os.listdir(USERS_DIR) if os.path.isdir(os.path.join(USERS_DIR, d))])


def _require_user():
    email = session.get("user_email")
    if not email:
        return None, ("請先登入", 401)
    return email, None


def _can_access(email, target_email, is_admin):
    return email == target_email or (is_admin and target_email)


# ══════════════════════════════════════════
#  組織（Org）功能
# ══════════════════════════════════════════

def _get_org_for_user(email):
    """
    查詢該 email 屬於哪個組織。
    回傳 org dict（含 org_id、name、role）；若不屬於任何組織則回傳 None。

    策略：直接查 users/{email} document 上的 org_id 欄位（O(1)，不需 collectionGroup）。
    若沒有 org_id 欄位則回 None（個人用戶）。
    org_id 在建立組織時由 Portal 後台寫入。
    """
    if not email:
        return None
    db = _get_db()
    if not db:
        return None
    try:
        # 快速查法：直接讀 users/{email} 文件的 org_id 欄位
        user_doc = db.collection("users").document(email).get()
        if not user_doc.exists:
            return None
        user_data = user_doc.to_dict() or {}
        org_id = user_data.get("org_id", "")
        if not org_id:
            return None
        # 查組織資料
        org_doc = db.collection("orgs").document(org_id).get()
        if not org_doc.exists:
            return None
        org_data = org_doc.to_dict() or {}
        # 查成員角色
        member_doc = db.collection("orgs").document(org_id).collection("members").document(email).get()
        role = "viewer"
        if member_doc.exists:
            role = member_doc.to_dict().get("role", "viewer")
        return {
            "org_id":      org_id,
            "name":        org_data.get("name", ""),
            "role":        role,
            "owner_email": org_data.get("owner_email", ""),
        }
    except Exception as e:
        import logging
        logging.warning("Library: _get_org_for_user 查詢失敗: %s", e)
    return None


def _get_user_role_in_org(org_id, email):
    """
    查詢某 email 在指定組織中的角色。
    回傳 'admin' / 'editor' / 'viewer'，或 None（非成員）。
    """
    if not org_id or not email:
        return None
    db = _get_db()
    if not db:
        return None
    try:
        doc = db.collection("orgs").document(org_id).collection("members").document(email).get()
        if doc.exists:
            return doc.to_dict().get("role", "viewer")
    except Exception as e:
        import logging
        logging.warning("Library: _get_user_role_in_org 查詢失敗: %s", e)
    return None


def _list_org_object_ids(org_id):
    """列出組織物件庫的所有物件 ID。"""
    db = _get_db()
    if not db or not org_id:
        return []
    try:
        docs = db.collection("orgs").document(org_id).collection("objects").select([]).stream()
        return [doc.id for doc in docs]
    except Exception as e:
        import logging
        logging.warning("Library: _list_org_object_ids 失敗: %s", e)
        return []


def _load_org_object(org_id, obj_id):
    """讀取組織物件庫的一筆物件。"""
    db = _get_db()
    if not db or not org_id or not obj_id:
        return None
    try:
        doc = db.collection("orgs").document(org_id).collection("objects").document(obj_id).get()
        if doc.exists:
            data = doc.to_dict()
            data.pop("_id", None)
            return data
    except Exception as e:
        import logging
        logging.warning("Library: _load_org_object 失敗: %s", e)
    return None


def _save_org_object(org_id, obj_id, data):
    """儲存組織物件庫的一筆物件。"""
    db = _get_db()
    if not db or not org_id or not obj_id:
        return False
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    if "id" not in data:
        data["id"] = obj_id
    try:
        db.collection("orgs").document(org_id).collection("objects").document(obj_id).set(data)
        return True
    except Exception as e:
        import logging
        logging.warning("Library: _save_org_object 失敗: %s", e)
        return False


def _delete_org_object(org_id, obj_id):
    """刪除組織物件庫的一筆物件。"""
    db = _get_db()
    if not db or not org_id or not obj_id:
        return False
    try:
        db.collection("orgs").document(org_id).collection("objects").document(obj_id).delete()
        return True
    except Exception as e:
        import logging
        logging.warning("Library: _delete_org_object 失敗: %s", e)
        return False


def _verify_service_key():
    """驗證 X-Service-Key 或 Authorization Bearer 與 SERVICE_API_KEY 一致（供 AD/Portal 後端呼叫）。"""
    if not SERVICE_API_KEY:
        return False
    import hmac
    key = request.headers.get("X-Service-Key") or ""
    if not key and request.headers.get("Authorization", "").startswith("Bearer "):
        key = request.headers.get("Authorization", "").replace("Bearer ", "", 1).strip()
    return hmac.compare_digest(key, SERVICE_API_KEY)



VALID_THEME_STYLES = ["navy", "forest", "amber", "minimal", "rose", "oled"]


# ── LOG 工具函式 ──
def log_event(event_type, user_id="", detail=None):
    """記錄業務事件，輸出至 Cloud Logging（Cloud Run stdout 自動收集）。"""
    print(json.dumps({
        "time": datetime.now(timezone.utc).isoformat(),
        "event": event_type,   # 事件名稱，例如 "library_search"
        "user": user_id,
        "detail": detail or {}
    }, ensure_ascii=False), flush=True)


@app.route("/api/client-log", methods=["POST"])
def api_client_log():
    """接收前端 JS 錯誤，記錄至 Cloud Logging。"""
    data = request.get_json(silent=True) or {}
    log_event("client_error", detail=data)
    return jsonify({"ok": True})


@app.route("/api/theme", methods=["GET"])
def api_theme_get():
    """讀取主題（與 Portal 共用 Firestore system_settings/theme），供跨工具同步。"""
    db = _get_db()
    style, mode = "navy", "system"
    if db:
        try:
            doc = db.collection("system_settings").document("theme").get()
            if doc.exists:
                d = doc.to_dict()
                style = d.get("style") or "navy"
                if d.get("mode") in ("dark", "light", "system"):
                    mode = d["mode"]
        except Exception:
            pass
    return jsonify({"style": style, "mode": mode})

@app.route("/api/theme", methods=["POST"])
def api_theme_set():
    email = session.get("user_email", "")
    if not email:
        return jsonify({"error": "請先登入"}), 401
    data = request.get_json(silent=True) or {}
    update = {}
    if "style" in data:
        if not _is_admin(email):
            return jsonify({"error": "無管理權限"}), 403
        style = data["style"]
        if style not in VALID_THEME_STYLES:
            return jsonify({"error": "無效風格"}), 400
        update["style"] = style
    if "mode" in data:
        mode = data["mode"]
        if mode in ("dark", "light", "system"):
            update["mode"] = mode
    if update:
        db = _get_db()
        if db:
            try:
                db.collection("system_settings").document("theme").set(update, merge=True)
            except Exception as e:
                return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})

@app.route("/health")
def health():
    return {"service": "real-estate-library", "status": "ok"}, 200


@app.route("/auth/portal-login", methods=["GET", "POST"])
def auth_portal_login():
    token = request.form.get("token") or request.args.get("token", "")
    if not token:
        return redirect(PORTAL_URL or "/")
    try:
        payload = TOKEN_SERIALIZER.loads(token, salt="portal-sso", max_age=TOKEN_MAX_AGE)
    except (SignatureExpired, BadSignature):
        return redirect(PORTAL_URL or "/")
    except Exception:
        return redirect(PORTAL_URL or "/")
    email = payload.get("email", "")
    if not email:
        return redirect(PORTAL_URL or "/")
    tab = request.args.get("tab") or request.form.get("tab", "")
    session["user_email"] = email
    session["user_name"] = payload.get("name", "")
    session["user_picture"] = payload.get("picture", "")
    session.permanent = True   # 讓 cookie 存活 30 天，不隨分頁關閉消失
    session.modified = True
    # 有指定分頁時，redirect 到 /?tab=xxx（同域 redirect，SameSite 不影響）
    # 無指定分頁時，直接 render 首頁（Set-Cookie 與 HTML 同一 response，最穩定）
    if tab:
        return redirect(f"/?tab={tab}")
    from flask import make_response
    resp = _render_app()
    if not hasattr(resp, 'headers'):
        resp = make_response(resp)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    session.clear()
    if request.headers.get("Accept", "").find("application/json") >= 0:
        return {"ok": True}, 200
    return redirect(PORTAL_URL or "/")


@app.route("/api/me", methods=["GET"])
def api_me():
    """回傳目前登入者基本資訊（含組織資訊）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    org = _get_org_for_user(email)
    return jsonify({
        "email":    email,
        "name":     session.get("user_name", ""),
        "picture":  session.get("user_picture", ""),
        "is_admin": _is_admin(email),
        # 組織資訊（若有）
        "org": org,  # None 或 {org_id, name, role, owner_email}
    })


@app.route("/api/objects", methods=["GET"])
def api_objects_list():
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    is_admin = _is_admin(email)

    # 判斷要顯示「個人庫」還是「組織庫」
    # ?mode=personal → 強制個人庫；?mode=org 或預設 → 若有組織則用組織庫
    mode = request.args.get("mode", "").strip()  # "personal" 或 "org" 或 ""
    org_info = _get_org_for_user(email)
    use_org = org_info and mode != "personal"

    if use_org:
        org_id = org_info["org_id"]
        ids = _list_org_object_ids(org_id)
        items = []
        for oid in sorted(ids, reverse=True):
            obj = _load_org_object(org_id, oid)
            if obj:
                items.append({
                    "id":           obj.get("id", oid),
                    "custom_title": obj.get("custom_title", ""),
                    "project_name": obj.get("project_name", ""),
                    "address":      obj.get("address", ""),
                    "created_at":   obj.get("created_at", ""),
                    "updated_at":   obj.get("updated_at", ""),
                    "owner_email":  obj.get("owner_email", ""),
                })
        return jsonify({
            "items":       items,
            "target_user": email,
            "is_admin":    is_admin,
            "org":         org_info,
            "mode":        "org",
        })

    # 個人庫邏輯（原有）
    target = request.args.get("user", "").strip() or email
    if not _can_access(email, target, is_admin):
        return jsonify({"error": "無權限查看該用戶的物件"}), 403
    try:
        ids = _list_user_ids(target)
    except Exception as e:
        import logging
        logging.exception("api_objects_list: _list_user_ids failed: %s", e)
        msg = "無法載入物件列表，請確認 GCS 已設定且服務有權限"
        try:
            from google.cloud.exceptions import Forbidden, NotFound
            if isinstance(e, Forbidden):
                msg = "GCS 權限不足（403），請確認服務帳號已授權 objectAdmin 於該 bucket"
            elif isinstance(e, NotFound):
                msg = "GCS bucket 不存在或路徑錯誤（404）"
        except ImportError:
            pass
        return jsonify({"error": msg}), 500
    items = []
    for oid in sorted(ids, reverse=True):
        obj = _load_object(target, oid)
        if obj:
            items.append({
                "id":           obj.get("id", oid),
                "custom_title": obj.get("custom_title", ""),
                "project_name": obj.get("project_name", ""),
                "address":      obj.get("address", ""),
                "created_at":   obj.get("created_at", ""),
                "updated_at":   obj.get("updated_at", ""),
                "owner_email":  obj.get("owner_email", target),
            })
    # 有帶搜尋關鍵字才記錄 library_search
    q = request.args.get("q", "").strip()
    if q:
        log_event("library_search", user_id=email, detail={"q": q[:50]})
    return jsonify({
        "items":       items,
        "target_user": target,
        "is_admin":    is_admin,
        "org":         org_info,  # 告訴前端此人有組織（雖然現在看個人庫）
        "mode":        "personal",
    })


@app.route("/api/users", methods=["GET"])
def api_users_list():
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email):
        return jsonify({"error": "僅管理員可查看"}), 403
    try:
        users = _list_users_with_objects()
    except Exception as e:
        import logging
        logging.exception("api_users_list: _list_users_with_objects failed: %s", e)
        msg = "無法載入用戶列表，請確認 GCS 已設定且服務有權限"
        try:
            from google.cloud.exceptions import Forbidden, NotFound
            if isinstance(e, Forbidden):
                msg = "GCS 權限不足（403），請確認服務帳號已授權 objectAdmin 於該 bucket"
            elif isinstance(e, NotFound):
                msg = "GCS bucket 不存在或路徑錯誤（404）"
        except ImportError:
            pass
        return jsonify({"error": msg}), 500
    return jsonify({"users": users})


@app.route("/api/objects", methods=["POST"])
def api_objects_create():
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    data = request.get_json() or {}
    log_event("library_object_create", user_id=email, detail={"title": (data.get("custom_title") or data.get("project_name") or "")[:50]})

    # 判斷要存入個人庫還是組織庫
    mode = data.get("_mode", "").strip()  # "personal" 或 "org"
    org_info = _get_org_for_user(email)
    use_org = org_info and mode != "personal"

    # 組織庫權限檢查：viewer 不能新增
    if use_org and org_info.get("role") == "viewer":
        return jsonify({"error": "你在此組織的權限為「只能查看」，無法新增物件"}), 403

    now = datetime.now()
    obj_id = now.strftime("%Y%m%d_%H%M%S")
    title = (data.get("custom_title") or data.get("project_name") or "未命名").strip()
    if title:
        safe_title = re.sub(r'[^\w\u4e00-\u9fff\-]', '_', title)[:24]
        obj_id = f"{obj_id}_{safe_title}"
    obj = {"id": obj_id, "created_at": now.isoformat(), "owner_email": email}
    for key, _label, _typ in PROPERTY_FIELDS + EXTRA_FIELDS:
        if key == "ad_outputs":
            continue
        if key in data:
            obj[key] = data[key]
        elif key == "custom_title":
            obj[key] = data.get("project_name", "") or ""
        else:
            obj[key] = obj.get(key, "")
    obj[AD_OUTPUTS_KEY] = data.get(AD_OUTPUTS_KEY, [])

    if use_org:
        org_id = org_info["org_id"]
        obj["org_id"] = org_id
        if _save_org_object(org_id, obj_id, obj):
            return jsonify({"ok": True, "id": obj_id, "object": obj, "mode": "org"}), 201
        return jsonify({"error": "儲存失敗"}), 500

    if _save_object(email, obj_id, obj):
        return jsonify({"ok": True, "id": obj_id, "object": obj, "mode": "personal"}), 201
    return jsonify({"error": "儲存失敗"}), 500


@app.route("/api/objects/<obj_id>", methods=["GET"])
def api_objects_get(obj_id):
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

    # 若有指定 org_id 參數，優先從組織庫讀取
    org_id_param = request.args.get("org_id", "").strip()
    if org_id_param:
        role = _get_user_role_in_org(org_id_param, email)
        if not role and not _is_admin(email):
            return jsonify({"error": "無組織存取權限"}), 403
        obj = _load_org_object(org_id_param, obj_id)
        if not obj:
            return jsonify({"error": "物件不存在"}), 404
        # 記錄查看組織庫物件
        log_event("library_view", user_id=email, detail={"obj_id": obj_id, "mode": "org"})
        return jsonify(obj)

    # 一般個人庫邏輯
    target = request.args.get("user", "").strip() or email
    if not _can_access(email, target, _is_admin(email)):
        return jsonify({"error": "無權限"}), 403
    obj = _load_object(target, obj_id)
    if not obj:
        return jsonify({"error": "物件不存在"}), 404
    # 記錄查看個人庫物件
    log_event("library_view", user_id=email, detail={"obj_id": obj_id, "mode": "personal"})
    return jsonify(obj)


@app.route("/api/objects/<obj_id>", methods=["PUT"])
def api_objects_update(obj_id):
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    data = request.get_json() or {}

    # 若有指定 org_id（body 或 query），優先更新組織庫
    org_id_param = (data.get("_org_id") or request.args.get("org_id", "")).strip()
    if org_id_param:
        role = _get_user_role_in_org(org_id_param, email)
        if not role and not _is_admin(email):
            return jsonify({"error": "無組織存取權限"}), 403
        if role == "viewer":
            return jsonify({"error": "你在此組織的權限為「只能查看」，無法編輯"}), 403
        obj = _load_org_object(org_id_param, obj_id)
        if not obj:
            return jsonify({"error": "物件不存在"}), 404
        for key, _label, _typ in PROPERTY_FIELDS + EXTRA_FIELDS:
            if key in data:
                obj[key] = data[key]
        if AD_OUTPUTS_KEY in data:
            obj[AD_OUTPUTS_KEY] = data[AD_OUTPUTS_KEY]
        if _save_org_object(org_id_param, obj_id, obj):
            return jsonify({"ok": True, "object": obj})
        return jsonify({"error": "儲存失敗"}), 500

    # 個人庫邏輯
    target = request.args.get("user", "").strip() or email
    if not _can_access(email, target, _is_admin(email)):
        return jsonify({"error": "無權限"}), 403
    obj = _load_object(target, obj_id)
    if not obj:
        return jsonify({"error": "物件不存在"}), 404
    for key, _label, _typ in PROPERTY_FIELDS + EXTRA_FIELDS:
        if key in data:
            obj[key] = data[key]
    if AD_OUTPUTS_KEY in data:
        obj[AD_OUTPUTS_KEY] = data[AD_OUTPUTS_KEY]
    if _save_object(target, obj_id, obj):
        return jsonify({"ok": True, "object": obj})
    return jsonify({"error": "儲存失敗"}), 500


@app.route("/api/objects/<obj_id>", methods=["DELETE"])
def api_objects_delete(obj_id):
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

    # 若有 org_id，從組織庫刪除
    org_id_param = request.args.get("org_id", "").strip()
    if org_id_param:
        role = _get_user_role_in_org(org_id_param, email)
        if not role and not _is_admin(email):
            return jsonify({"error": "無組織存取權限"}), 403
        if role not in ("admin",) and not _is_admin(email):
            return jsonify({"error": "僅組織管理員可刪除物件"}), 403
        if _delete_org_object(org_id_param, obj_id):
            return jsonify({"ok": True})
        return jsonify({"error": "刪除失敗或物件不存在"}), 404

    # 個人庫邏輯
    target = request.args.get("user", "").strip() or email
    if not _can_access(email, target, _is_admin(email)):
        return jsonify({"error": "無權限"}), 403
    if _delete_object(target, obj_id):
        return jsonify({"ok": True})
    return jsonify({"error": "刪除失敗或物件不存在"}), 404


# ──────────────────────────────────────────
#  組織成員管理 API
# ──────────────────────────────────────────

@app.route("/api/org/info", methods=["GET"])
def api_org_info():
    """回傳目前用戶的組織資訊（成員列表供管理員用）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    org_info = _get_org_for_user(email)
    if not org_info:
        return jsonify({"org": None})
    org_id = org_info["org_id"]
    db = _get_db()
    members = []
    if db:
        try:
            docs = db.collection("orgs").document(org_id).collection("members").stream()
            for doc in docs:
                d = doc.to_dict()
                members.append({
                    "email":     doc.id,
                    "role":      d.get("role", "viewer"),
                    "joined_at": d.get("joined_at", ""),
                })
        except Exception as e:
            import logging
            logging.warning("api_org_info: 讀取成員失敗: %s", e)
    return jsonify({"org": org_info, "members": members})


@app.route("/api/org/members", methods=["GET"])
def api_org_members_list():
    """列出組織成員（需為組織 admin 或系統管理員）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    org_info = _get_org_for_user(email)
    if not org_info:
        return jsonify({"error": "你不屬於任何組織"}), 404
    if org_info.get("role") != "admin" and not _is_admin(email):
        return jsonify({"error": "僅組織管理員可查看成員列表"}), 403
    org_id = org_info["org_id"]
    db = _get_db()
    members = []
    if db:
        try:
            docs = db.collection("orgs").document(org_id).collection("members").stream()
            for doc in docs:
                d = doc.to_dict()
                members.append({
                    "email":     doc.id,
                    "role":      d.get("role", "viewer"),
                    "joined_at": d.get("joined_at", ""),
                })
        except Exception as e:
            import logging
            logging.warning("api_org_members_list: 失敗: %s", e)
            return jsonify({"error": "讀取成員列表失敗"}), 500
    return jsonify({"org": org_info, "members": members})


@app.route("/api/org/members", methods=["POST"])
def api_org_members_add():
    """邀請成員加入組織。Body: { "email": "xxx@yyy.com", "role": "editor" }"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    org_info = _get_org_for_user(email)
    if not org_info:
        return jsonify({"error": "你不屬於任何組織"}), 404
    if org_info.get("role") != "admin" and not _is_admin(email):
        return jsonify({"error": "僅組織管理員可邀請成員"}), 403
    data = request.get_json() or {}
    target_email = (data.get("email") or "").strip().lower()
    role = (data.get("role") or "editor").strip()
    if not target_email or "@" not in target_email:
        return jsonify({"error": "請輸入有效的 email"}), 400
    if role not in ("admin", "editor", "viewer"):
        return jsonify({"error": "角色必須是 admin / editor / viewer"}), 400
    org_id = org_info["org_id"]
    db = _get_db()
    if not db:
        return jsonify({"error": "Firestore 未初始化"}), 500
    try:
        db.collection("orgs").document(org_id).collection("members").document(target_email).set({
            "email":     target_email,
            "role":      role,
            "joined_at": datetime.now(timezone.utc).isoformat(),
            "invited_by": email,
        })
        # 同步在 users/{target_email} 寫入 org_id，讓物件庫能快速查到組織（不需 collectionGroup）
        db.collection("users").document(target_email).set({"org_id": org_id}, merge=True)
        return jsonify({"ok": True, "email": target_email, "role": role})
    except Exception as e:
        import logging
        logging.warning("api_org_members_add: 失敗: %s", e)
        return jsonify({"error": "新增成員失敗"}), 500


@app.route("/api/org/members", methods=["DELETE"])
def api_org_members_remove():
    """移除組織成員。Body: { "email": "xxx@yyy.com" }"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    org_info = _get_org_for_user(email)
    if not org_info:
        return jsonify({"error": "你不屬於任何組織"}), 404
    if org_info.get("role") != "admin" and not _is_admin(email):
        return jsonify({"error": "僅組織管理員可移除成員"}), 403
    data = request.get_json() or {}
    target_email = (data.get("email") or "").strip().lower()
    if not target_email:
        return jsonify({"error": "缺少 email"}), 400
    # 不能移除自己（唯一管理員）
    if target_email == email:
        return jsonify({"error": "不能移除自己，請先轉讓管理員再離開"}), 400
    org_id = org_info["org_id"]
    db = _get_db()
    if not db:
        return jsonify({"error": "Firestore 未初始化"}), 500
    try:
        db.collection("orgs").document(org_id).collection("members").document(target_email).delete()
        # 清除 users/{target_email} 的 org_id 欄位
        try:
            from google.cloud.firestore import DELETE_FIELD
            db.collection("users").document(target_email).update({"org_id": DELETE_FIELD})
        except Exception:
            db.collection("users").document(target_email).set({"org_id": ""}, merge=True)
        return jsonify({"ok": True})
    except Exception as e:
        import logging
        logging.warning("api_org_members_remove: 失敗: %s", e)
        return jsonify({"error": "移除成員失敗"}), 500


@app.route("/api/org/members/role", methods=["PATCH"])
def api_org_members_update_role():
    """更新成員角色。Body: { "email": "xxx@yyy.com", "role": "viewer" }"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    org_info = _get_org_for_user(email)
    if not org_info:
        return jsonify({"error": "你不屬於任何組織"}), 404
    if org_info.get("role") != "admin" and not _is_admin(email):
        return jsonify({"error": "僅組織管理員可修改角色"}), 403
    data = request.get_json() or {}
    target_email = (data.get("email") or "").strip().lower()
    new_role = (data.get("role") or "").strip()
    if not target_email or new_role not in ("admin", "editor", "viewer"):
        return jsonify({"error": "缺少 email 或角色無效"}), 400
    org_id = org_info["org_id"]
    db = _get_db()
    if not db:
        return jsonify({"error": "Firestore 未初始化"}), 500
    try:
        db.collection("orgs").document(org_id).collection("members").document(target_email).set(
            {"role": new_role}, merge=True
        )
        return jsonify({"ok": True, "email": target_email, "role": new_role})
    except Exception as e:
        import logging
        logging.warning("api_org_members_update_role: 失敗: %s", e)
        return jsonify({"error": "更新角色失敗"}), 500


@app.route("/api/org/transfer-objects", methods=["POST"])
def api_org_transfer_objects():
    """
    把自己個人庫的物件複製到組織庫。
    Body: { "confirm": true }（防誤觸）
    """
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    org_info = _get_org_for_user(email)
    if not org_info:
        return jsonify({"error": "你不屬於任何組織"}), 404
    data = request.get_json() or {}
    if not data.get("confirm"):
        return jsonify({"error": "請傳入 confirm: true 以確認轉移"}), 400
    org_id = org_info["org_id"]
    ids = _list_user_ids(email)
    copied, failed = 0, 0
    for oid in ids:
        obj = _load_object(email, oid)
        if obj:
            obj["org_id"] = org_id
            if _save_org_object(org_id, oid, obj):
                copied += 1
            else:
                failed += 1
    return jsonify({"ok": True, "copied": copied, "failed": failed,
                    "message": f"已複製 {copied} 筆物件到組織庫，失敗 {failed} 筆"})


def _field_key_label():
    return [(k, l) for k, l, _ in PROPERTY_FIELDS + EXTRA_FIELDS]


# ── Sheets → Firestore 同步邏輯 ──

SHEET_ID = os.environ.get("PROPERTY_SHEET_ID", "1Gm9FYLgYcyQHhiLMD_bmABKXvl-bPDJQeN-46DUxyjU")
SHEET_NAME = os.environ.get("PROPERTY_SHEET_NAME", "主頁")
GCP_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT") or "gen-lang-client-0393195862"

# 數字欄位（自動轉 float）
_NUMERIC_FIELDS = {"地坪", "建坪", "管理費(元)", "委託價(萬)", "售價(萬)", "現有貸款(萬)", "成交金額(萬)"}

_sync_lock = threading.Lock()   # 避免同時多次同步
_sync_status = {"running": False, "last_run": None, "last_result": None}


def _get_sheets_service(timeout=30):
    """建立 Sheets API service（讀寫權限）。"""
    import google.auth
    from googleapiclient.discovery import build
    creds, _ = google.auth.default(scopes=[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/spreadsheets",
    ])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _sheets_read_all():
    """用 ADC 讀取整張 Sheets，回傳 (headers, data_rows)。"""
    service = _get_sheets_service()
    result = service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A1:AZ9999"
    ).execute()
    all_rows = result.get("values", [])
    if len(all_rows) < 4:
        return [], []

    headers = all_rows[1]   # 第2行是欄位名

    def is_header_row(row):
        return bool(row) and row[0].strip() == headers[0]

    # 第4行起為資料，過濾空行和重複標題行
    data_rows = [r for r in all_rows[3:] if any(c.strip() for c in r) and not is_header_row(r)]
    return headers, data_rows


# ─────────────────────────────────────────────────────────────
# 地號 → 座標反查（easymap 內政部地籍圖資）
# ─────────────────────────────────────────────────────────────
_easymap_cache = {"cities": None, "towns": {}, "sections": {}}  # in-memory cache

def _easymap_resolve(area, section, landno):
    """area=鄉/市/鎮（如「台東」「成功鎮」皆可）、section=段別（如「新田」「新田段」皆可）、
    landno=地號（如 0123-0004 或 123）。回傳 (lat, lng) 或 None。

    比對規則（重要 bug fix）：
    - area: easymap 回傳「台東市」，library 資料常寫「台東」（沒後綴）→ 兩邊都剝 市/鎮/鄉/區
    - section: easymap 回傳「新田段」，library 資料常寫「新田」（沒後綴）→ 兩邊都剝 段
    - section 可能有多段別（"中濱 掃別"）：取第一個
    - landno 可能有多地號（"364 359"）：取第一個
    - 台 ↔ 臺 一律正規化
    """
    if not area or not section or not landno:
        return None

    def _norm_area(s):
        # 剝 市/鎮/鄉/區 後綴 + 台→臺
        import re
        return re.sub(r"[市鎮鄉區]$", "", str(s or "").strip()).replace("台", "臺")

    def _norm_section_full(s):
        # 完整正規化：剝段/小段/空格，得到合併字串（含小段名）
        # 例：「中濱 掃別」→「中濱掃別」、「中濱段掃別小段」→「中濱掃別」 → 匹配
        s = str(s or "").split(",")[0].strip()
        return s.replace("小段", "").replace("段", "").replace(" ", "").replace("台", "臺")

    def _norm_section_first(s):
        # 只取第一段名（用於跨段物件 fallback）：「中濱 掃別」→「中濱」
        s = str(s or "").split(",")[0].strip()
        first = s.split(" ")[0]
        return first.rstrip("段").replace("台", "臺")

    try:
        from easymap import EasymapCrawler
        c = EasymapCrawler()
        # 一次初始化 cache：縣市清單
        if _easymap_cache["cities"] is None:
            _easymap_cache["cities"] = c.get_cities()
        # 找臺東縣
        city = next((x for x in _easymap_cache["cities"]
                     if (x.get("name") or "").replace("台", "臺") == "臺東縣"), None)
        if not city:
            return None
        city_code = city.get("id") or city.get("code")
        # 鄉鎮 cache（同縣市）
        if city_code not in _easymap_cache["towns"]:
            _easymap_cache["towns"][city_code] = c.get_towns(city_code)
        towns = _easymap_cache["towns"][city_code]
        area_norm = _norm_area(area)  # "台東" → "臺東"，"成功鎮" → "成功"
        town = next((x for x in towns if _norm_area(x.get("name")) == area_norm), None)
        if not town:
            return None
        town_code = town.get("id") or town.get("code")
        # 段別 cache
        sec_key = f"{city_code}|{town_code}"
        if sec_key not in _easymap_cache["sections"]:
            _easymap_cache["sections"][sec_key] = c.get_sections(city_code, town_code)
        sections = _easymap_cache["sections"][sec_key]
        # 段別比對：先用「完整正規化」（含小段名）→「中濱 掃別」匹配「中濱段掃別小段」
        # 失敗 fallback 到「只取第一段」→「中濱 掃別」匹配「中濱段」（跨段物件用第一個地號）
        sect_full = _norm_section_full(section)
        sec = next((s for s in sections if _norm_section_full(s.get("name")) == sect_full), None)
        if not sec:
            sect_first = _norm_section_first(section)
            # 找最短匹配（避免「中濱」誤匹「中濱段掃別小段」剝後「中濱掃別」——必須完全相等）
            sec = next((s for s in sections
                        if _norm_section_full(s.get("name")) == sect_first), None)
        if not sec:
            return None
        sect_no = sec.get("sectNo") or sec.get("id")
        office  = sec.get("office") or sec.get("officeCode") or ""
        # 地號正規化：取第一個（多地號就用第一個）、補零成 8 碼
        first_landno = str(landno).split(",")[0].split(";")[0].split(" ")[0].strip()
        coord = c.locate(sect_no, office, first_landno)
        if coord and "lat" in coord and "lng" in coord:
            return (coord["lat"], coord["lng"])
    except Exception as e:
        import logging
        logging.warning(f"easymap_resolve 失敗 area={area} section={section} landno={landno}: {e}")
    return None


def _sheets_write_coord_batch(seq_to_coord: dict):
    """把 seq → 'lat,lng' 字串寫回 SHEETS 的「座標」欄位（只動該欄、其他欄不碰）。"""
    if not seq_to_coord:
        return {"ok": True, "updated": 0}
    import logging
    log = logging.getLogger("sheets-coord")
    try:
        service = _get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!A1:AZ9999"
        ).execute()
        all_rows = result.get("values", [])
        if len(all_rows) < 4:
            return {"ok": False, "error": "Sheets 列數不足"}
        headers = all_rows[1]
        try:
            seq_col_idx   = headers.index("資料序號")
            coord_col_idx = headers.index("座標")
        except ValueError as e:
            return {"ok": False, "error": f"找不到欄位（請先在 SHEETS 加「座標」欄）：{e}"}
        def col_letter(idx):
            r = ""
            while idx >= 0:
                r = chr(idx % 26 + ord('A')) + r
                idx = idx // 26 - 1
            return r
        coord_col = col_letter(coord_col_idx)
        updates = []
        for i, row in enumerate(all_rows):
            if i < 3:
                continue
            seq_val = row[seq_col_idx].strip() if seq_col_idx < len(row) else ""
            if seq_val in seq_to_coord:
                updates.append({
                    "range": f"{SHEET_NAME}!{coord_col}{i+1}",
                    "values": [[seq_to_coord[seq_val]]],
                })
        if not updates:
            return {"ok": True, "updated": 0}
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "RAW", "data": updates},
        ).execute()
        log.info(f"sheets_write_coord：已更新 {len(updates)} 列")
        return {"ok": True, "updated": len(updates)}
    except Exception as e:
        log.exception("sheets_write_coord 失敗")
        return {"ok": False, "error": str(e)}


def _sheets_write_selling_status(seq_to_selling: dict):
    """
    把 Firestore 的「銷售中」值回寫到 Sheets，只動這一欄，其他欄位完全不碰。
    seq_to_selling: {資料序號(str): True/False, ...}
    """
    import logging
    log = logging.getLogger("sheets-writeback")
    if not seq_to_selling:
        return {"ok": True, "updated": 0}

    try:
        service = _get_sheets_service()
        # 讀取整張，只需要找 header 位置和 資料序號 欄
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1:AZ9999"
        ).execute()
        all_rows = result.get("values", [])
        if len(all_rows) < 4:
            return {"ok": False, "error": "Sheets 資料列數不足"}

        headers = all_rows[1]  # 第2行是 header

        # 找「資料序號」和「銷售中」的欄索引
        try:
            seq_col_idx     = headers.index("資料序號")
            selling_col_idx = headers.index("銷售中")
        except ValueError as e:
            return {"ok": False, "error": f"找不到欄位：{e}"}

        # 轉成 Sheets 欄字母（A=0, B=1, ...）
        def col_letter(idx):
            result = ""
            while idx >= 0:
                result = chr(idx % 26 + ord('A')) + result
                idx = idx // 26 - 1
            return result

        selling_col = col_letter(selling_col_idx)

        # 掃描資料列（第4行起 = all_rows index 3 起），找出需要更新的列
        # all_rows[0]=第1行, [1]=第2行(header), [2]=第3行, [3]=第4行...
        updates = []  # list of (sheets_row_number_1based, new_value)
        for i, row in enumerate(all_rows):
            if i < 3:  # 跳過前3行（標題/header/空行）
                continue
            seq_val = row[seq_col_idx].strip() if seq_col_idx < len(row) else ""
            if seq_val in seq_to_selling:
                new_val = "TRUE" if seq_to_selling[seq_val] else "FALSE"
                sheets_row = i + 1  # Sheets 列號從 1 開始
                updates.append({
                    "range": f"{SHEET_NAME}!{selling_col}{sheets_row}",
                    "values": [[new_val]]
                })

        if not updates:
            log.info("sheets_write_selling_status：無需更新的列")
            return {"ok": True, "updated": 0}

        # 用 batchUpdate 一次送出，不逐列打 API
        body = {
            "valueInputOption": "RAW",
            "data": updates
        }
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID, body=body
        ).execute()

        log.info(f"sheets_write_selling_status：已更新 {len(updates)} 列")
        return {"ok": True, "updated": len(updates)}

    except Exception as e:
        log.exception("sheets_write_selling_status 失敗")
        return {"ok": False, "error": str(e)}


@app.route("/api/sheets/writeback-selling", methods=["POST"])
def api_sheets_writeback_selling():
    """
    管理員專用：把 Firestore company_properties 所有物件的「銷售中」狀態
    一次全部回寫到 Google Sheets，只更新這一欄，其他欄位完全不碰。
    """
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    try:
        # 讀出所有物件的 資料序號 → 銷售中 映射
        seq_to_selling = {}
        for doc in db.collection("company_properties").stream():
            r = doc.to_dict()
            seq = doc.id  # doc ID = 資料序號
            if seq and seq.isdigit():
                seq_to_selling[seq] = _is_selling(r)
        if not seq_to_selling:
            return jsonify({"ok": False, "error": "Firestore 無資料"}), 400
        result = _sheets_write_selling_status(seq_to_selling)
        if not result.get("ok"):
            return jsonify({"ok": False, "error": result.get("error", "未知錯誤")}), 500

        # ───── 順便補座標：銷售中 + 委託期內 + 段別/地號齊全 + 座標空白 ─────
        # 用 easymap 反查座標、寫回 SHEETS「座標」欄位 + Firestore
        coord_targets = []
        for doc in db.collection("company_properties").stream():
            r = doc.to_dict()
            if not _is_selling(r) or not _is_within_delegation(r):
                continue
            if r.get("座標"):  # 已有座標
                continue
            area    = (r.get("鄉/市/鎮") or "").strip()
            section = (r.get("段別") or "").strip()
            landno  = (r.get("地號") or "").strip()
            seq     = doc.id
            if area and section and landno and seq and seq.isdigit():
                coord_targets.append((seq, area, section, landno))

        coord_updates = {}
        coord_failed = 0
        for seq, area, section, landno in coord_targets:
            coord = _easymap_resolve(area, section, landno)
            if coord:
                coord_str = f"{coord[0]:.6f},{coord[1]:.6f}"
                coord_updates[seq] = coord_str
                # 寫進 Firestore（用 merge 不影響其他欄位）
                db.collection("company_properties").document(seq).set(
                    {"座標": coord_str}, merge=True
                )
            else:
                coord_failed += 1

        coord_result = _sheets_write_coord_batch(coord_updates) if coord_updates else {"ok": True, "updated": 0}

        return jsonify({
            "ok": True,
            "selling": {"total": len(seq_to_selling), "updated": result["updated"]},
            "coord":   {"attempted": len(coord_targets),
                        "resolved":  len(coord_updates),
                        "failed":    coord_failed,
                        "sheets_updated": coord_result.get("updated", 0)},
            "message": (f"回寫銷售中 {result['updated']} 筆；"
                        f"補座標：嘗試 {len(coord_targets)} 筆、查到 {len(coord_updates)} 筆、寫回 SHEETS {coord_result.get('updated', 0)} 筆"),
        })
    except Exception as e:
        import logging
        logging.exception("writeback-selling 失敗")
        return jsonify({"error": str(e)}), 500


@app.route("/api/debug/sheets-headers", methods=["GET"])
def api_debug_sheets_headers():
    """管理員診斷用：讀取 Sheets header 行，確認欄位名稱是否正確。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員"}), 403
    try:
        service = _get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1:AZ20"
        ).execute()
        rows = result.get("values", [])
        headers = rows[1] if len(rows) > 1 else []
        return jsonify({
            "sheet_id": SHEET_ID,
            "sheet_name": SHEET_NAME,
            "total_rows_fetched": len(rows),
            "row1": rows[0] if rows else [],
            "row2_headers": headers,
            "row3": rows[2] if len(rows) > 2 else [],
            "row4": rows[3] if len(rows) > 3 else [],
            "has_seq_col": "資料序號" in headers,
            "has_selling_col": "銷售中" in headers,
            "seq_col_index": headers.index("資料序號") if "資料序號" in headers else -1,
            "selling_col_index": headers.index("銷售中") if "銷售中" in headers else -1,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _col_letter(idx):
    """0-based 欄位 index → 欄位字母（0→A, 25→Z, 26→AA …）"""
    idx += 1
    result = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        result = chr(65 + rem) + result
    return result


# ── ACCESS 比對欄位正規化 ──
_ACCESS_DATE_FIELDS = {"委託到期日", "完成日期", "到期日", "建照日期"}
_ACCESS_NUM_FIELDS  = {"地坪", "建坪", "室內坪", "管理費(元)",
                       "委託價(萬)", "售價(萬)", "現有貸款(萬)", "成交金額(萬)"}

def _access_norm_val(field, val):
    """正規化欄位值供 ACCESS 比對（去單位、統一日期/編號/布林格式）"""
    v = str(val).strip() if val is not None else ""
    if not v:
        return ""
    # 電話欄位：去非數字字元 + 去前置 0（963060220 vs 0963060220 視為相同）
    if field in ("行動電話1", "行動電話2", "室內電話1", "室內電話2", "電話"):
        digits = re.sub(r'\D', '', v)
        return digits.lstrip('0')
    # 竣工日期：兩邊常常一邊是「年/月」一邊是「年/月/日」造成假差異，只比對到「月」
    # 同時統一民國／西元（< 1911 視為民國，0102 = 民國 102 = 西元 2013）
    if field == "竣工日期":
        s = re.sub(r'[/\-\.\s]+$', '', v).strip()  # 去尾巴 /
        m = re.match(r'^(\d{1,4})\s*年\s*(\d{1,2})', s)
        if not m:
            m = re.match(r'^(\d{1,4})[/\-\.](\d{1,2})', s)
        if m:
            y, mo = int(m.group(1)), int(m.group(2))
            if y < 1911:
                y += 1911
            return f"{y}/{mo:02d}"
        return v
    # 委託編號 / 委託號碼：去 .0、空白；單一編號補零到 6 位（91839 vs 091839 視為相同）
    if field in ("委託編號", "委託號碼"):
        cleaned = re.sub(r'\.0$', '', v).strip()
        # 多編號用空白拆 → 各自補零 → 排序去重後組回
        tokens = [t for t in re.split(r'\s+', cleaned) if t]
        norm_tokens = []
        for t in tokens:
            if t.isdigit():
                norm_tokens.append(t.zfill(6))
            else:
                norm_tokens.append(t)
        return " ".join(sorted(set(norm_tokens)))
    # 銷售中：統一布林格式（True/False/銷售中/已下架/已成交/Yes/No）
    if field == "銷售中":
        s = v.lower()
        if s in ("true", "1", "yes", "y", "銷售中"):
            return "true"
        if s in ("false", "0", "no", "n", "已下架", "已成交"):
            return "false"
        return s
    if field in _ACCESS_NUM_FIELDS:
        # 去萬、坪、元、逗號等後轉 float 字串
        cleaned = re.sub(r'[萬坪元,,\s]', '', v)
        try:
            return str(round(float(cleaned), 2))
        except Exception:
            return v
    if field in _ACCESS_DATE_FIELDS:
        # 民國／西元 統一成西元 YYYY/MM/DD
        # 涵蓋：「113年1月15日」「102/4/20」「0102/4/20」「2024/04/20」等
        # 規則：4 位數但 y < 1911 也視為民國年（如 0102 = 民國 102 = 西元 2013）
        m = re.match(r'^(\d{1,4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})', v)
        if not m:
            m = re.match(r'^(\d{1,4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})', v)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if y < 1911:
                y += 1911
            return f"{y}/{mo:02d}/{d:02d}"
        return v
    # 一般字串：壓縮空白
    return re.sub(r'\s+', ' ', v)

# 物件類別關鍵字（用於判斷該物件「有」土地或建物的部分）
# 沿用 CATEGORY_GROUPS（app.py:2433）的設計，但 hardcode 在 helper 旁方便維護
_LAND_HINTS = ("農地", "建地", "土地", "林地", "農舍", "農建")
_BUILDING_HINTS = ("公寓", "套房", "華廈", "平房", "透天", "透住",
                   "別墅", "店住", "店面", "攤位", "辦公", "民宿",
                   "廠房", "廠辦", "住家", "住宅", "大樓")


def _has_land_part(cat):
    """類別裡是否含「土地」部分（含混合類別如『透天+農地』）。"""
    s = str(cat or "").strip()
    return bool(s) and any(t in s for t in _LAND_HINTS)


def _has_building_part(cat):
    """類別裡是否含「建物」部分（含混合類別如『透天+農地』）。"""
    s = str(cat or "").strip()
    return bool(s) and any(t in s for t in _BUILDING_HINTS)


def _is_land_category(cat):
    """向後相容：判斷是否含土地部分。"""
    return _has_land_part(cat)


def _dates_equivalent(a, b):
    """判斷兩個日期字串是否「民國/西元」等效。涵蓋幾種混雜情境：
    - '95/4/22' vs '2006/4/22'（民國 vs 西元，差 1911）
    - '1995/4/22' vs '95/4/22'（主頁 Sheets 自動加 19 前綴，實際是民國 95，後兩碼相等）
    - '0095/4/22' vs '95/4/22'（零填充）
    """
    def _to_tuple(v):
        s = str(v or "").strip()
        if not s:
            return None
        m = (re.match(r'^(\d{1,4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})', s) or
             re.match(r'^(\d{1,4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})', s))
        if not m:
            return None
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    ta, tb = _to_tuple(a), _to_tuple(b)
    if not ta or not tb:
        return False
    if ta[1] != tb[1] or ta[2] != tb[2]:
        return False  # 月日必須相同
    ya, yb = ta[0], tb[0]
    if ya == yb:
        return True
    # 民國 vs 西元（差 1911）
    if abs(ya - yb) == 1911:
        return True
    # 一邊「19XX」（被 Sheets 自動加 19）、另一邊「XX」民國 → 後兩碼相等
    if ya >= 1900 and ya < 2000 and yb < 1000 and (ya % 100) == (yb % 100):
        return True
    if yb >= 1900 and yb < 2000 and ya < 1000 and (yb % 100) == (ya % 100):
        return True
    return False


def _likely_different_property(od, nd, changed_fields):
    """判斷 ACCESS 比對命中的兩筆，是不是「同一不動產的不同物件」（不該套用為修改）。
    場景：同地號的舊屋拆掉重蓋新屋 / 屋主轉售後新屋主再委託 / 同地號老物件已成交、新物件新建。
    這些 case 雖然 hard_key 相同，但其實是兩個獨立的委託物件，不該被當「修改」直接覆蓋。
    """
    # 條件 A：所有權人變動 + 案名變動 + 售價差 > 50%
    owner_old = str(od.get("所有權人", "") or "").strip()
    owner_new = str(nd.get("所有權人", "") or "").strip()
    name_old  = str(od.get("案名", "") or "").strip()
    name_new  = str(nd.get("案名", "") or "").strip()
    price_old = _parse_price_num(od.get("售價(萬)"))
    price_new = _parse_price_num(nd.get("售價(萬)"))
    owner_changed = owner_old and owner_new and owner_old != owner_new
    name_changed  = name_old  and name_new  and name_old  != name_new
    big_price_diff = False
    try:
        if isinstance(price_old, (int, float)) and isinstance(price_new, (int, float)) and price_old > 0 and price_new > 0:
            ratio = abs(price_new - price_old) / max(price_old, price_new)
            big_price_diff = ratio > 0.5
    except Exception:
        pass
    if owner_changed and name_changed and big_price_diff:
        return True
    # 條件 B：差異欄位 ≥ 5（噪音欄位 SKIP_FIELDS 已被排除，剩下都是有意義的）
    if len(changed_fields) >= 5:
        return True
    return False


def _parse_date_smart(s):
    """強化日期解析：自動判斷民國/西元，處理 1995/4/22 = 民國 95 等情境。
    回傳 (西元年, 月, 日) tuple，無法解析回 None（注意與 _parse_date_for_compare 不同：這裡用 None 代表「無資料」便於過濾判斷）。
    """
    s = str(s or "").strip()
    if not s:
        return None
    m = (re.match(r'^(\d{1,4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})', s) or
         re.match(r'^(\d{1,4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})', s))
    if not m:
        return None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if y < 1911:
        # 民國年（含 4 位數但 < 1911 如 0102）
        y += 1911
    elif 1900 <= y < 2000:
        # 19XX 視為民國 XX（Sheets 自動加 19 前綴）→ 取後 2 碼當民國年
        y = (y % 100) + 1911
    return (y, mo, d)


def _parse_date_for_compare(s):
    """把日期字串轉成 (年, 月, 日) tuple 供比較。空值或無法解析 → (0,0,0)。
    用於同 hard_bldg 多筆時取委託日較新者。
    """
    s = str(s or "").strip()
    if not s:
        return (0, 0, 0)
    # 西元 YYYY/MM/DD 或 YYYY-MM-DD
    m = re.match(r'^(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})', s)
    if m:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    # 民國年 NNN年MM月DD日（含可能的空白）
    m2 = re.match(r'^(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})', s)
    if m2:
        y = int(m2.group(1))
        if y < 1000:
            y += 1911
        return (y, int(m2.group(2)), int(m2.group(3)))
    return (0, 0, 0)


def _normalize_landno(s):
    """地號正規化 + 拆分：去空白、去 .0 結尾、空白拆成多 token。
    例：'0835-0001 0835-0002' → ['0835-0001', '0835-0002']
    """
    raw = str(s or "").strip()
    if not raw:
        return []
    # 把全形空白、各種分隔符統一成空白，再拆
    cleaned = re.sub(r'[　,，、/／]+', ' ', raw)
    tokens = []
    for t in re.split(r'\s+', cleaned):
        t = re.sub(r'\.0$', '', t).strip()
        if t:
            tokens.append(t)
    return tokens


# 建號欄的「明確無建號」標記（預售屋、老屋未保存登記等）
# 助理在 Sheets 建號欄填這些值，等同告訴系統「這筆物件沒有建號是事實」：
# - 體檢不再提示「缺建號」
# - 比對時不會產生 hard_bldg key（避免誤配對）
_NO_BLDGNO_MARKERS = {
    "無", "無建號", "沒有", "沒有建號",
    "N/A", "n/a", "NA", "na", "N.A.",
    "—", "-", "─", "－",
    "未保存", "未保存登記", "未登記", "尚未登記", "保存登記中",
    "預售", "預售中", "預售屋",
    "待登記", "待保存", "待登",
}


def _normalize_bldgno(s):
    """建號正規化 + 拆分。
    若整個欄位是「無/N/A/預售/未保存」等標記 → 回傳空 list（明確無建號）。
    其他正常情況同地號邏輯（去空白、拆 token）。
    """
    raw = str(s or "").strip()
    if not raw or raw in _NO_BLDGNO_MARKERS:
        return []
    # 一般情況：拆 token 並過濾掉混在一起的標記
    cleaned = re.sub(r'[　,，、/／]+', ' ', raw)
    tokens = []
    for t in re.split(r'\s+', cleaned):
        t = re.sub(r'\.0$', '', t).strip()
        if t and t not in _NO_BLDGNO_MARKERS:
            tokens.append(t)
    return tokens


def _access_hard_keys(d):
    """產出此筆資料的所有「硬資料指紋」key tuple。

    讀取欄位：物件類別 / 鄉/市/鎮 / 段別 / 地號 / 建號

    若 鄉/市/鎮 + 段別 + 任一個地號 都有值：
      土地類 → ("hard_land", f"{鄉/市/鎮}|{段別}|{地號}")
      建物類 → 每個 (地號, 建號) 配對 → ("hard_bldg", f"{鄉/市/鎮}|{段別}|{地號}|{建號}")
      類別缺失 → 兩種都吐（permissive），讓比對更寬

    一筆資料可能產出 0~多個 key（多地號的農地會吐多個）。
    缺鄉鎮、段別、或地號的物件回傳空 list（fallback 到 _access_make_key）。
    """
    area = str(d.get("鄉/市/鎮", "") or "").strip()
    sect = str(d.get("段別", "") or "").strip()
    if not area or not sect:
        return []
    landnos = _normalize_landno(d.get("地號", ""))
    if not landnos:
        return []
    bldgnos = _normalize_bldgno(d.get("建號", ""))
    cat = str(d.get("物件類別", "") or "").strip()

    # 類別判斷：含土地部分 / 含建物部分 / 類別不認得（兩種都吐 permissive）
    has_land = _has_land_part(cat)
    has_bldg = _has_building_part(cat)
    if not has_land and not has_bldg:
        # 類別空白或不認得 → 兩種都吐
        has_land = True
        has_bldg = True

    keys = []
    # 建物 key 優先（最精確：地號+建號）
    if has_bldg and bldgnos:
        for ln in landnos:
            for bn in bldgnos:
                keys.append(("hard_bldg", f"{area}|{sect}|{ln}|{bn}"))
    # 土地 key：對每個地號各產一個（不需建號）
    if has_land:
        for ln in landnos:
            keys.append(("hard_land", f"{area}|{sect}|{ln}"))
    return keys


def _access_make_key(d):
    """建立比對主鍵（多層 fallback）：
    1. 硬資料指紋（鄉/市/鎮 + 段別 + 地號 + 建號）→ 永遠優先
    2. 委託編號 + 建號/地號（向後相容）
    3. 案名 + 地號 + 物件地址（最後 fallback）

    僅回傳第一個（單一 tuple，向後相容 _AC_CACHE / stringify）。
    多 key 索引交給 orig_index 端用 _access_hard_keys 自行展開。
    """
    # 1. 硬資料指紋
    hk = _access_hard_keys(d)
    if hk:
        return hk[0]
    # 2. 委託編號（保留現有邏輯）
    comm = re.sub(r'\.0$', '', str(d.get("委託編號", "") or "").strip())
    comm = re.sub(r'\s+', ' ', comm).strip()
    bno  = re.sub(r'\.0$', '', str(d.get("建號", "") or "").strip())
    dino = re.sub(r'\s+', '', str(d.get("地號", "") or "").strip())
    name = re.sub(r'\s+', '', str(d.get("案名", "") or "").strip())
    addr = re.sub(r'\s+', '', str(d.get("物件地址", "") or "").strip())
    if comm:
        detail = bno or dino
        return ("comm", f"{comm}|{detail}")
    # 3. fallback
    return ("name_addr", f"{name}|{dino}|{addr}")

def _access_row_to_dict(headers, row):
    """把一行資料轉成 {欄位名: 值} dict（按 header 名稱對應）"""
    d = {}
    for i, h in enumerate(headers):
        if h and h.strip():
            d[h.strip()] = row[i].strip() if i < len(row) else ""
    return d


# 伺服器端暫存完整比對資料（供 apply 使用，key = email）
_AC_CACHE = {}

# ── ACCESS 忽略規則 helpers ──

def _ac_ignore_rule_id(object_key_str, field, ignored_value):
    """產生忽略規則的 Firestore document ID（確保同一規則不重複建立）"""
    import hashlib
    raw = f"{object_key_str}|{field}|{ignored_value}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:20]

def _ac_load_ignore_rules():
    """從 Firestore 載入所有忽略規則，回傳 set of (object_key_str, field, ignored_value)"""
    db = _get_db()
    if db is None:
        return {}
    try:
        docs = db.collection("access_ignore_rules").stream()
        rules = {}
        for doc in docs:
            d = doc.to_dict()
            key = (d.get("object_key", ""), d.get("field", ""), d.get("ignored_value", ""))
            rules[key] = doc.id
        return rules  # {(obj_key, field, val): doc_id}
    except Exception:
        return {}

@app.route("/api/access-ignore-rules", methods=["GET"])
def api_access_ignore_rules_list():
    """列出所有忽略規則"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    try:
        docs = db.collection("access_ignore_rules").order_by("created_at", direction=_firestore.Query.DESCENDING).stream()
        rules = []
        for doc in docs:
            d = doc.to_dict()
            d["id"] = doc.id
            # Firestore timestamp → 字串
            if hasattr(d.get("created_at"), "isoformat"):
                d["created_at"] = d["created_at"].isoformat()
            rules.append(d)
        return jsonify({"ok": True, "rules": rules})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/access-ignore-rules", methods=["POST"])
def api_access_ignore_rules_add():
    """新增忽略規則"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    data = request.get_json(silent=True) or {}
    obj_key     = data.get("object_key", "").strip()
    display_name= data.get("display_name", "").strip()
    field       = data.get("field", "").strip()
    ign_val     = data.get("ignored_value", "").strip()
    if not obj_key or not field:
        return jsonify({"error": "缺少必要欄位"}), 400
    rule_id = _ac_ignore_rule_id(obj_key, field, ign_val)
    db.collection("access_ignore_rules").document(rule_id).set({
        "object_key":     obj_key,
        "display_name":   display_name,
        "field":          field,
        "ignored_value":  ign_val,
        "created_by":     email,
        "created_at":     _firestore.SERVER_TIMESTAMP,
    })
    return jsonify({"ok": True, "id": rule_id})

@app.route("/api/access-ignore-rules/<rule_id>", methods=["DELETE"])
def api_access_ignore_rules_delete(rule_id):
    """刪除（解鎖）忽略規則"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    db.collection("access_ignore_rules").document(rule_id).delete()
    return jsonify({"ok": True})

@app.route("/api/access-compare", methods=["POST"])
def api_access_compare():
    """
    比對「新貼入的 Access Sheets」與原始物件庫 Sheets 的差異。
    Input JSON: { new_sheet_id, new_sheet_name (選填，空白=自動取第一個分頁) }
    Output: { ok, added, modified, removed, compare_fields }
    """
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403

    data = request.get_json(silent=True) or {}
    raw_input      = (data.get("new_sheet_id") or "").strip()
    new_sheet_name = (data.get("new_sheet_name") or "").strip()
    # 日期過濾門檻（委託日 < 此日期的物件不參與比對，但保留委託日空白者）
    # 預設民國 102/1/1 = 西元 2013/01/01（使用者進公司日的同年）
    min_commit_date_raw = (data.get("min_commit_date") or "2013-01-01").strip()
    _min_date_match = re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$', min_commit_date_raw)
    if _min_date_match:
        min_commit_tuple = (int(_min_date_match.group(1)), int(_min_date_match.group(2)), int(_min_date_match.group(3)))
    else:
        min_commit_tuple = None  # 無效格式 → 不過濾
    if not raw_input:
        return jsonify({"error": "請提供新 Sheets 網址或 ID"}), 400

    # 自動從網址抓出 Sheets ID（支援 /spreadsheets/d/{ID}/... 格式）
    m = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', raw_input)
    new_sheet_id = m.group(1) if m else raw_input

    try:
        service = _get_sheets_service(timeout=30)

        # ── 載入忽略規則（全公司共用）──
        ignore_rules = _ac_load_ignore_rules()  # {(obj_key_str, field, val): doc_id}

        # ── 取兩邊 Sheets 的 sheet_gid（給前端組「📍 列 N」連結用）──
        orig_sheet_gid = 0
        new_sheet_gid  = 0
        try:
            orig_meta = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
            for s in orig_meta.get("sheets", []):
                if s["properties"]["title"] == SHEET_NAME:
                    orig_sheet_gid = s["properties"]["sheetId"]
                    break
            new_meta = service.spreadsheets().get(spreadsheetId=new_sheet_id).execute()
            for s in new_meta.get("sheets", []):
                title = s["properties"]["title"]
                if (new_sheet_name and title == new_sheet_name) or (not new_sheet_name):
                    new_sheet_gid = s["properties"]["sheetId"]
                    break
        except Exception:
            pass  # gid 抓不到不影響主流程，只是連結會 fallback 到 gid=0

        # ── 讀原始 Sheets（只取 A~AU 欄，跳過 AV+ 自訂欄）──
        orig_result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1:AU9999"
        ).execute(num_retries=0)
        orig_all = orig_result.get("values", [])
        if len(orig_all) < 4:
            return jsonify({"error": "原始 Sheets 資料不足（需至少4行）"}), 400

        orig_headers = [h.strip() for h in orig_all[1]]  # 第 2 行是 header

        def _is_hdr(row):
            return bool(row) and row[0].strip() == (orig_headers[0] if orig_headers else "")

        # 第 5 行起為資料（前4列為標題/輔助列）；同時記錄每行的 Sheets 列號（1-based）
        orig_data = []
        orig_row_numbers = []
        for i, row in enumerate(orig_all[4:]):
            if any(c.strip() for c in row) and not _is_hdr(row):
                orig_data.append(row)
                orig_row_numbers.append(5 + i)  # Sheets 第 5 行 = index 4 + 1-based

        # ── 讀新 Sheets ──
        if not new_sheet_name:
            # 自動取第一個分頁名稱
            meta = service.spreadsheets().get(spreadsheetId=new_sheet_id).execute(num_retries=0)
            new_sheet_name = meta["sheets"][0]["properties"]["title"]

        new_result = service.spreadsheets().values().get(
            spreadsheetId=new_sheet_id,
            range=f"{new_sheet_name}!A1:AU9999"
        ).execute(num_retries=0)
        new_all = new_result.get("values", [])
        if not new_all:
            return jsonify({"error": "新 Sheets 無資料"}), 400

        # 自動偵測 header 行（找含「委託編號」「案名」「資料序號」任一欄的行）
        new_header_idx = 0
        for i, row in enumerate(new_all[:6]):
            if any(c.strip() in ("委託編號", "案名", "資料序號") for c in row):
                new_header_idx = i
                break
        new_headers = [h.strip() for h in new_all[new_header_idx]]
        # 同時保留 row 編號（給前端「📍 ACCESS 列 N」連結用）
        new_data_with_rn = []
        for i, r in enumerate(new_all[new_header_idx + 1:]):
            if any(c.strip() for c in r):
                # 過濾重複的 header 行
                if r and r[0].strip() == new_headers[0]:
                    continue
                new_data_with_rn.append((new_header_idx + 2 + i, r))  # 1-based row number
        new_row_numbers = [rn for rn, _ in new_data_with_rn]
        new_data = [r for _, r in new_data_with_rn]

        # ── 委託日過濾（民國 102/1/1 預設）──
        # 委託日 < 過濾門檻 的物件直接跳過比對（保留委託日空白者）
        filtered_orig_count = 0
        filtered_new_count  = 0
        if min_commit_tuple:
            # 主頁過濾
            commit_col_orig = None
            for i, h in enumerate(orig_headers):
                if h == "委託日":
                    commit_col_orig = i; break
            if commit_col_orig is not None:
                kept_data, kept_rns = [], []
                for row, rn in zip(orig_data, orig_row_numbers):
                    v = row[commit_col_orig] if commit_col_orig < len(row) else ""
                    t = _parse_date_smart(v)
                    if t is None or t >= min_commit_tuple:
                        kept_data.append(row); kept_rns.append(rn)
                    else:
                        filtered_orig_count += 1
                orig_data = kept_data
                orig_row_numbers = kept_rns
            # ACCESS 過濾
            commit_col_new = None
            for i, h in enumerate(new_headers):
                if h == "委託日":
                    commit_col_new = i; break
            if commit_col_new is not None:
                kept_data, kept_rns = [], []
                for row, rn in zip(new_data, new_row_numbers):
                    v = row[commit_col_new] if commit_col_new < len(row) else ""
                    t = _parse_date_smart(v)
                    if t is None or t >= min_commit_tuple:
                        kept_data.append(row); kept_rns.append(rn)
                    else:
                        filtered_new_count += 1
                new_data = kept_data
                new_row_numbers = kept_rns

        # ── 決定比較欄位（取兩邊 header 的交集，排除資料序號）──
        orig_hdr_set = set(h for h in orig_headers if h)
        new_hdr_set  = set(h for h in new_headers  if h)
        _IMPORTANT = ["案名", "售價(萬)", "委託到期日", "物件地址",
                      "經紀人", "委託編號", "物件類別", "地坪", "建坪", "室內坪"]
        # 永遠免比對的欄位（噪音欄位，每次比對顯示但無實質業務意義）
        # - 格局：被 Sheets 格式化成日期（如 "2003/2/2" 實為 "3/2/2"），純格式噪音
        # - 銷售中：兩邊各自維護（主頁有「📤 回寫銷售中」專門同步），ACCESS 比對不該插一腳
        _SKIP_FIELDS = {"格局", "銷售中"}
        all_common = [h for h in orig_headers
                      if h and h in new_hdr_set and h != "資料序號" and h not in _SKIP_FIELDS]
        # 重要欄位前置，其餘依原始順序（_IMPORTANT 也過濾掉 _SKIP_FIELDS，雙重保險）
        compare_fields = [f for f in _IMPORTANT if f in orig_hdr_set and f in new_hdr_set and f not in _SKIP_FIELDS] + \
                         [f for f in all_common if f not in _IMPORTANT]

        # ── 建立原始 Sheets 多 key 索引 ──
        # 每個 entry 用 entry_id 唯一識別；同一 entry 可被多個 key 指到
        # （例如多地號的農地會吐多個 hard_land key 都指向同一 entry）
        all_entries = []          # 所有 orig entry 列表（依序）
        orig_index = {}            # key → entry（多 key 索引到同一 entry）
        orig_key_collision = []    # 記錄 key 衝突
        orig_no_hard_key_count = 0  # 沒有 hard_key 的 orig 筆數（資料品質指標）

        for i, row in enumerate(orig_data):
            d = _access_row_to_dict(orig_headers, row)
            entry_id = len(all_entries)
            entry = {
                "_id":        entry_id,
                "row_number": orig_row_numbers[i],
                "data":       d,
                "seq":        d.get("資料序號", "").strip(),
                "_key":       "",  # 第一個 key 字串，除錯用
            }
            # 收集所有 key：硬資料指紋（多個）+ 主 key（單一，可能與 hard 重複）
            hard_keys = _access_hard_keys(d)
            main_key  = _access_make_key(d)
            if not hard_keys:
                orig_no_hard_key_count += 1
            entry["_key"] = str(main_key)
            # 索引策略：有 hard_bldg 就不索引 hard_land
            # 原因：兩戶地號相同建號不同（同棟大樓不同戶）若都索引 hard_land 會跨戶誤配
            bldg_keys = [k for k in hard_keys if k[0] == "hard_bldg"]
            land_keys = [k for k in hard_keys if k[0] == "hard_land"]
            keys_for_index = bldg_keys if bldg_keys else land_keys
            if main_key not in keys_for_index:
                keys_for_index = keys_for_index + [main_key]
            # 同物件兩筆委託歷史（如：先建檔無委託約、後簽約再建檔）→ 保留委託日較新的當代表
            entry_date = _parse_date_for_compare(d.get("委託日", ""))
            entry["_date"] = entry_date  # 保留供新 ACCESS 端比較用
            # 去重索引
            seen_for_this_entry = set()
            for k in keys_for_index:
                if k in seen_for_this_entry:
                    continue
                seen_for_this_entry.add(k)
                if k in orig_index:
                    # 不同 entry 共用同 key → 衝突
                    if orig_index[k]["_id"] != entry_id:
                        existing = orig_index[k]
                        existing_date = existing.get("_date") or _parse_date_for_compare(existing["data"].get("委託日", ""))
                        if entry_date > existing_date:
                            # 本筆較新 → 替換
                            orig_index[k] = entry
                            orig_key_collision.append({
                                "key":         str(k),
                                "kept":        d.get("案名", "") + " (委託日 " + str(d.get("委託日","")) + ")",
                                "overwritten": existing["data"].get("案名", "") + " (委託日 " + str(existing["data"].get("委託日","")) + ")",
                                "_reason":     "保留委託日較新者",
                            })
                        else:
                            # 既有較新 → 不變，本筆當作衝突
                            orig_key_collision.append({
                                "key":         str(k),
                                "kept":        existing["data"].get("案名", "") + " (委託日 " + str(existing["data"].get("委託日","")) + ")",
                                "overwritten": d.get("案名", "") + " (委託日 " + str(d.get("委託日","")) + ")",
                                "_reason":     "保留委託日較新者",
                            })
                    continue
                orig_index[k] = entry
            all_entries.append(entry)

        # ── 新 ACCESS 端去重（通用版：對所有 hard_keys 各自比委託日）──
        # 場景：
        # (a) ACCESS 同物件兩筆（hard_bldg 重複）
        # (b) ACCESS 多地號合併新版 + 舊拆分版（hard_land 多 key 重疊）
        #     例：主頁合併新版（2026 委託，地號「695 699」）
        #         ACCESS 留著舊 A/B 拆分版（2025 委託，A=695、B=699）
        #         ACCESS 也有合併新版（2026 委託，地號「695 699」）
        #     → 合併新版的 hard_land(695) 和 hard_land(699) 都比 A/B 的委託日新
        #       → A、B 兩列整列跳過（所有 hard_key 都被新版搶走）
        new_dedup_skip_idx = set()
        new_dedup_count = 0
        # 第一遍：每個 hard_key 找出「委託日最新」的 row
        key_winner = {}  # hard_key → (idx, date_tuple)
        new_pre = []  # 預先 row → (nd, hard_keys, date) 避免重複計算
        for idx, new_row in enumerate(new_data):
            nd_pre = _access_row_to_dict(new_headers, new_row)
            nd_pre_hard = _access_hard_keys(nd_pre)
            nd_pre_date = _parse_date_for_compare(nd_pre.get("委託日", ""))
            new_pre.append((nd_pre, nd_pre_hard, nd_pre_date))
            for k in nd_pre_hard:
                if k not in key_winner or nd_pre_date > key_winner[k][1]:
                    key_winner[k] = (idx, nd_pre_date)
        # 第二遍：跳過條件 = 此 row 所有 hard_key 都被「**嚴格較新**」的 row 搶走
        # 重要：同日（無法判斷誰新誰舊，例如 預售屋 A/B 同日簽約）或自己無日期 → 不跳過
        for idx, (nd_pre, nd_pre_hard, nd_pre_date) in enumerate(new_pre):
            if not nd_pre_hard:
                continue  # 無 hard_key 的不參與這個去重（仍走 comm/name_addr）
            if nd_pre_date == (0, 0, 0):
                continue  # 自己沒委託日 → 無法判斷新舊，保留
            all_outdated = True
            for k in nd_pre_hard:
                wi, wd = key_winner.get(k, (idx, nd_pre_date))
                if wi == idx:
                    all_outdated = False; break  # 自己就是 winner
                if wd <= nd_pre_date:
                    all_outdated = False; break  # 沒被嚴格較新搶走（同日或更舊）
            if all_outdated:
                new_dedup_skip_idx.add(idx)
                new_dedup_count += 1

        # ── 比對 ──
        added_display    = []
        modified_display = []
        removed_display  = []
        added_full       = []
        modified_full    = []
        matched_entry_ids = set()  # 已被新 ACCESS 配對到的 entry id
        match_kind_counts = {       # 命中種類統計（含二次 fallback）
            "hard_bldg": 0, "hard_land": 0, "comm": 0, "name_addr": 0, "fallback": 0
        }

        for idx, new_row in enumerate(new_data):
            if idx in new_dedup_skip_idx:
                continue  # 同 hard_bldg 較舊版本，跳過
            nd = _access_row_to_dict(new_headers, new_row)
            nd_hard_keys = _access_hard_keys(nd)
            nd_main_key  = _access_make_key(nd)

            # 比對策略：有 hard_bldg 就只試 hard_bldg（不 fallback 到 hard_land 避免跨戶誤配）
            # 兩戶同地號不同建號的情況下，hard_land 一致會誤配到別戶
            nd_bldg_keys = [k for k in nd_hard_keys if k[0] == "hard_bldg"]
            nd_land_keys = [k for k in nd_hard_keys if k[0] == "hard_land"]
            nd_keys_to_try = nd_bldg_keys if nd_bldg_keys else nd_land_keys

            # 按優先序試 key：先 hard，再 main
            matched_entry = None
            matched_kind  = None
            matched_orig_k = None
            for k in nd_keys_to_try:
                if k in orig_index:
                    matched_entry = orig_index[k]
                    matched_kind  = k[0]  # "hard_land" / "hard_bldg"
                    matched_orig_k = k
                    break
            if matched_entry is None and nd_main_key in orig_index:
                # nd_main_key 可能是 hard_*、comm、或 name_addr
                matched_entry = orig_index[nd_main_key]
                matched_kind  = nd_main_key[0]
                matched_orig_k = nd_main_key

            # 檢查：此 orig entry 是否已被前面的 ACCESS row 配過
            # 若是 → 本筆當作「無法配對」走 added 流程
            # 場景：主頁一筆物件地號「695 699」，ACCESS 拆成 A(695) B(699) 兩筆
            #       第一筆配上主頁那 entry，第二筆同 entry 已被配過 → 列入新增
            if matched_entry is not None and matched_entry["_id"] in matched_entry_ids:
                matched_entry = None
                matched_kind  = None
                matched_orig_k = None

            if matched_entry is None:
                # 第一輪沒命中 → 暫進新增（可能會被二次配對救回）
                added_display.append({
                    "idx":          idx,
                    "display_name": nd.get("案名", "") or nd.get("委託編號", "（未知案名）"),
                    "price":        nd.get("售價(萬)", ""),
                    "agent":        nd.get("經紀人", ""),
                    "comm":         nd.get("委託編號", ""),
                    "_key":         str(nd_main_key),
                    "row_in_new":   new_row_numbers[idx] if idx < len(new_row_numbers) else None,
                })
                added_full.append({f: nd.get(f, "") for f in new_headers if f})
            else:
                matched_entry_ids.add(matched_entry["_id"])
                match_kind_counts[matched_kind] = match_kind_counts.get(matched_kind, 0) + 1
                od = matched_entry["data"]
                obj_key_str = str(matched_orig_k)
                changed_fields = []
                for field in compare_fields:
                    nv = _access_norm_val(field, nd.get(field, ""))
                    ov = _access_norm_val(field, od.get(field, ""))
                    if nv != ov:
                        # 日期欄位：嘗試民國/西元等效（1995 vs 95、95 vs 2006 等視為相同）
                        if field in _ACCESS_DATE_FIELDS or field == "竣工日期":
                            if _dates_equivalent(nd.get(field, ""), od.get(field, "")):
                                continue
                        raw_new = nd.get(field, "")
                        rule_key = (obj_key_str, field, raw_new.strip())
                        is_locked = rule_key in ignore_rules
                        changed_fields.append({
                            "field":   field,
                            "old":     od.get(field, ""),
                            "new":     raw_new,
                            "_locked": is_locked,
                            "_rule_id": ignore_rules.get(rule_key, ""),
                        })
                if changed_fields:
                    midx = len(modified_display)
                    modified_display.append({
                        "idx":           midx,
                        "row_in_orig":   matched_entry["row_number"],
                        "row_in_new":    new_row_numbers[idx] if idx < len(new_row_numbers) else None,
                        "seq":           matched_entry["seq"],
                        "display_name":  od.get("案名", "") or str(matched_orig_k),
                        "changed_fields": changed_fields,
                        "_key":          str(matched_orig_k) + "→" + matched_kind,
                        "_likely_different": _likely_different_property(od, nd, changed_fields),
                        "_old_name":     od.get("案名", ""),
                        "_old_owner":    od.get("所有權人", ""),
                        "_new_owner":    nd.get("所有權人", ""),
                    })
                    modified_full.append({
                        "row_in_orig": matched_entry["row_number"],
                        "new_data":    {f: nd.get(f, "") for f in new_headers if f},
                    })

        # ── 二次配對：未配到的新增項，用「案名+物件地址」再比一次 ──
        # 原因：新 Sheets 沒有 hard_key 也沒有委託編號的物件 → 第一輪走 name_addr，
        # 但原始 Sheets 同一物件若有 hard_key/comm key → 第一輪永遠配不到
        orig_name_addr_fallback = {}  # (正規化案名, 正規化地址) → entry
        for entry in all_entries:
            if entry["_id"] in matched_entry_ids:
                continue
            d = entry["data"]
            fname = re.sub(r'\s+', '', str(d.get("案名", "") or "").strip())
            faddr = re.sub(r'\s+', '', str(d.get("物件地址", "") or "").strip())
            if fname or faddr:
                fallback_key = (fname, faddr)
                if fallback_key not in orig_name_addr_fallback:
                    orig_name_addr_fallback[fallback_key] = entry

        # 對所有「新增」項目嘗試二次配對
        still_added_display = []
        still_added_full    = []
        for i, item in enumerate(added_display):
            nd = added_full[i] if i < len(added_full) else {}
            fname = re.sub(r'\s+', '', str(nd.get("案名", "") or "").strip())
            faddr = re.sub(r'\s+', '', str(nd.get("物件地址", "") or "").strip())
            fallback_key = (fname, faddr)
            if (fname or faddr) and fallback_key in orig_name_addr_fallback:
                # 二次配對成功，改為修改
                orig_entry = orig_name_addr_fallback[fallback_key]
                del orig_name_addr_fallback[fallback_key]  # 每個 entry 只配一次
                matched_entry_ids.add(orig_entry["_id"])
                match_kind_counts["fallback"] = match_kind_counts.get("fallback", 0) + 1
                od = orig_entry["data"]
                # 用 entry 的代表性 main_key 字串做為 ignore_rules 查詢的 key
                orig_main_key = _access_make_key(od)
                obj_key_str2 = str(orig_main_key)
                changed_fields = []
                for field in compare_fields:
                    nv = _access_norm_val(field, nd.get(field, ""))
                    ov = _access_norm_val(field, od.get(field, ""))
                    if nv != ov:
                        # 日期欄位：嘗試民國/西元等效
                        if field in _ACCESS_DATE_FIELDS or field == "竣工日期":
                            if _dates_equivalent(nd.get(field, ""), od.get(field, "")):
                                continue
                        raw_new = nd.get(field, "")
                        rule_key2 = (obj_key_str2, field, raw_new.strip())
                        is_locked2 = rule_key2 in ignore_rules
                        changed_fields.append({
                            "field":   field,
                            "old":     od.get(field, ""),
                            "new":     raw_new,
                            "_locked": is_locked2,
                            "_rule_id": ignore_rules.get(rule_key2, ""),
                        })
                if changed_fields:
                    # 從 added_display item 的 idx 取對應的 ACCESS row 編號
                    new_idx = item.get("idx") if isinstance(item, dict) else None
                    row_in_new = new_row_numbers[new_idx] if (new_idx is not None and new_idx < len(new_row_numbers)) else None
                    midx = len(modified_display)
                    modified_display.append({
                        "idx":            midx,
                        "row_in_orig":    orig_entry["row_number"],
                        "row_in_new":     row_in_new,
                        "seq":             orig_entry["seq"],
                        "display_name":    od.get("案名", "") or str(orig_main_key),
                        "changed_fields":  changed_fields,
                        "_key":            str(orig_main_key) + "→fallback",
                        "_likely_different": _likely_different_property(od, nd, changed_fields),
                        "_old_name":       od.get("案名", ""),
                        "_old_owner":      od.get("所有權人", ""),
                        "_new_owner":      nd.get("所有權人", ""),
                    })
                    modified_full.append({
                        "row_in_orig": orig_entry["row_number"],
                        "new_data":    {f: nd.get(f, "") for f in new_headers if f},
                    })
            else:
                still_added_display.append(item)
                if i < len(added_full):
                    still_added_full.append(added_full[i])
        added_display = still_added_display
        added_full    = still_added_full

        # 可能下架：原始 Sheets 有、ACCESS 沒有（用 entry id 判斷，不再用 key）
        for entry in all_entries:
            if entry["_id"] in matched_entry_ids:
                continue
            d = entry["data"]
            removed_display.append({
                "seq":          entry["seq"],
                "row_in_orig":  entry["row_number"],
                "display_name": d.get("案名", "") or entry["_key"],
                "comm":         d.get("委託編號", ""),
            })

        # ── 伺服器端暫存完整資料（供 apply 使用）──
        _AC_CACHE[email] = {
            "orig_headers":  orig_headers,
            "added_full":    added_full,
            "modified_full": modified_full,
        }

        # ── 診斷：取前 3 筆 key 供除錯 ──
        orig_sample = [str(k) for k in list(orig_index.keys())[:3]]
        new_sample  = []
        for new_row in new_data[:5]:
            nd = _access_row_to_dict(new_headers, new_row)
            new_sample.append(str(_access_make_key(nd)))

        # 輕量顯示資料（不含完整行，只有差異欄位），5000 筆以內不會凍結
        _DISP_LIMIT = 5000
        return jsonify({
            "ok":              True,
            "added":           added_display[:_DISP_LIMIT],
            "added_total":     len(added_display),
            "modified":        modified_display[:_DISP_LIMIT],
            "modified_total":  len(modified_display),
            "removed":         removed_display[:_DISP_LIMIT],
            "removed_total":   len(removed_display),
            "compare_fields":  compare_fields,
            # 兩邊 Sheets 的 ID + sheet_gid（給前端組「📍 列 N」連結用）
            "orig_sheet_id":   SHEET_ID,
            "orig_sheet_gid":  orig_sheet_gid,
            "new_sheet_id":    new_sheet_id,
            "new_sheet_gid":   new_sheet_gid,
            # 委託日過濾結果（讓前端知道濾掉了多少）
            "min_commit_date":    min_commit_date_raw if min_commit_tuple else "",
            "filtered_orig_count": filtered_orig_count,
            "filtered_new_count":  filtered_new_count,
            "_diag": {
                "orig_headers_sample":     orig_headers[:5],
                "new_headers_sample":      new_headers[:5],
                "orig_keys_sample":        orig_sample,
                "new_keys_sample":         new_sample,
                "new_header_idx":          new_header_idx,
                "orig_data_count":         len(orig_data),
                "new_data_count":          len(new_data),
                "key_collisions":          orig_key_collision[:20],  # 最多顯示20筆衝突
                "key_collision_count":     len(orig_key_collision),
                "match_kind_counts":       match_kind_counts,        # 各種 key 命中分佈
                "orig_no_hard_key_count":  orig_no_hard_key_count,   # 缺硬資料的 orig 筆數
                "new_dedup_count":         new_dedup_count,          # ACCESS 端同物件多筆委託歷史，去重的筆數
            },
        })

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "detail": traceback.format_exc()}), 500


@app.route("/api/access-apply", methods=["POST"])
def api_access_apply():
    """
    套用比對結果到原始 Sheets：
    - 修改：cell-by-cell 更新指定欄位（只動 A~AU，AV~CY 完全不碰）
    - 新增：追加到主頁末尾，自動分配資料序號
    Input JSON: {
      apply_modified: [{ row_in_orig (int), fields: {欄位名: 新值} }],
      apply_added:    [{ 欄位名: 值, … }]
    }
    Output: { ok, modified_count, added_count, message }
    """
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403

    data = request.get_json(silent=True) or {}
    # apply_modified: [{ idx (int), row_in_orig, changed_fields:[{field,new}] }]
    # apply_added:    [{ idx (int) }]  — 實際資料從 _AC_CACHE[email] 取
    apply_modified = data.get("apply_modified", [])
    apply_added    = data.get("apply_added", [])

    if not apply_modified and not apply_added:
        return jsonify({"ok": True, "modified_count": 0, "added_count": 0,
                        "message": "無變更需套用"})

    cache = _AC_CACHE.get(email)
    if not cache:
        return jsonify({"error": "比對資料已過期，請重新執行比對"}), 400

    try:
        service = _get_sheets_service(timeout=30)

        # 讀取原始 Sheets header 行，用名稱定位欄號（不用固定數字，防欄位順序改變出錯）
        hdr_result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A2:AU2"
        ).execute(num_retries=0)
        raw_headers = hdr_result.get("values", [[]])[0]
        headers = [h.strip() for h in raw_headers]
        header_to_col = {h: i for i, h in enumerate(headers) if h}

        # ── 修改：逐欄位建立 batchUpdate 請求 ──
        value_updates = []
        for item in apply_modified:
            row_num = item.get("row_in_orig")
            fields  = {f["field"]: f["new"] for f in item.get("changed_fields", [])}
            if not row_num or not fields:
                continue
            for field_name, new_val in fields.items():
                if field_name not in header_to_col:
                    continue  # 欄位不存在原始 Sheets，跳過
                col_idx    = header_to_col[field_name]
                col_letter = _col_letter(col_idx)
                value_updates.append({
                    "range":  f"{SHEET_NAME}!{col_letter}{row_num}",
                    "values": [[new_val]]
                })

        # ⚠️ 重要：必須先寫 modified、再做 added 的 insertDimension。
        # 原因：modified 用 row_in_orig（套用前的列號）；如果先 insert 會把所有資料下移，
        # 之後 modified 寫入時用舊 row_in_orig 會寫到「插入空白行」的錯誤位置（bug 已造成 40 列受損）。
        # 修法：先寫 modified（row_in_orig 有效），之後 insertDimension 會連同 modified 寫入的資料一起下移。

        # ── 批次更新修改的欄位（必須先做，row_in_orig 在 insert 前還有效）──
        modified_count = 0
        if value_updates:
            service.spreadsheets().values().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={
                    "valueInputOption": "USER_ENTERED",
                    "data": value_updates
                }
            ).execute()
            modified_count = len({item.get("row_in_orig") for item in apply_modified
                                   if item.get("row_in_orig")})

        # ── 新增：從 server cache 取完整資料，找最大序號後追加 ──
        added_count = 0
        added_full  = cache.get("added_full", [])
        apply_added_indices = [item.get("idx") for item in apply_added if item.get("idx") is not None]

        if apply_added_indices:
            orig_all = service.spreadsheets().values().get(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!A1:AU9999"
            ).execute(num_retries=0).get("values", [])

            orig_headers = [h.strip() for h in (orig_all[1] if len(orig_all) > 1 else headers)]
            seq_col_idx  = next((i for i, h in enumerate(orig_headers) if h == "資料序號"), -1)

            max_seq = 0
            if seq_col_idx >= 0:
                for row in orig_all[4:]:  # 第 5 列起為資料
                    if seq_col_idx < len(row):
                        try:
                            v = int(float(row[seq_col_idx].strip()))
                            if v > max_seq:
                                max_seq = v
                        except Exception:
                            pass

            new_seq = max_seq + 1
            new_rows = []
            for idx in apply_added_indices:
                if idx >= len(added_full):
                    continue
                new_item = added_full[idx]
                row_vals = []
                for h in orig_headers:
                    if h == "資料序號":
                        row_vals.append(str(new_seq))
                        new_seq += 1
                    else:
                        row_vals.append(new_item.get(h, ""))
                new_rows.append(row_vals)

            if new_rows:
                # ── 取得分頁的 sheetId（數字 GID），插入列需要用到 ──
                meta = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
                target_sheet_gid = None
                for s in meta.get("sheets", []):
                    if s["properties"]["title"] == SHEET_NAME:
                        target_sheet_gid = s["properties"]["sheetId"]
                        break
                if target_sheet_gid is None:
                    return jsonify({"error": f"找不到分頁 {SHEET_NAME}"}), 400

                # ── 在第 5 列前插入 N 列空白列（繼承下方資料列格式）──
                INSERT_ROW_IDX = 4  # 0-based，= Sheets 第 5 列前
                service.spreadsheets().batchUpdate(
                    spreadsheetId=SHEET_ID,
                    body={"requests": [{
                        "insertDimension": {
                            "range": {
                                "sheetId":    target_sheet_gid,
                                "dimension":  "ROWS",
                                "startIndex": INSERT_ROW_IDX,
                                "endIndex":   INSERT_ROW_IDX + len(new_rows)
                            },
                            "inheritFromBefore": False  # 繼承下方資料列格式
                        }
                    }]}
                ).execute()

                # ── 寫入資料到剛插入的空白列 ──
                start_row = INSERT_ROW_IDX + 1  # 1-based
                end_row   = INSERT_ROW_IDX + len(new_rows)
                service.spreadsheets().values().batchUpdate(
                    spreadsheetId=SHEET_ID,
                    body={
                        "valueInputOption": "USER_ENTERED",
                        "data": [{
                            "range":  f"{SHEET_NAME}!A{start_row}:AU{end_row}",
                            "values": new_rows
                        }]
                    }
                ).execute()
                added_count = len(new_rows)

        msg = f"已修改 {modified_count} 筆物件"
        if added_count > 0:
            msg += f"，新增 {added_count} 筆（請記得在 CR 欄向下複製公式）"

        return jsonify({
            "ok":             True,
            "modified_count": modified_count,
            "added_count":    added_count,
            "message":        msg
        })

    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "detail": traceback.format_exc()}), 500


def _audit_ack_id(firestore_doc_id, field):
    """產生「已檢查」的 doc id：每個 (firestore_doc_id, field) 一對一。
    這樣使用者可以「只確認某個欄位（如建號＝未保存登記）」而不影響其他缺欄位的提醒。
    """
    import hashlib
    raw = f"{firestore_doc_id}|{field}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()[:20]


@app.route("/api/access-data-audit", methods=["GET"])
def api_access_data_audit():
    """資料體檢：掃描銷售中物件，找出缺硬資料（鄉/市/鎮、段別、地號、建物缺建號）的物件。
    這些物件在 ACCESS 比對時會 fallback 到 name_addr 配對（精準度低），建議補齊。
    使用者按過「✓ 已檢查」的（存在 audit_acks collection）會自動隱藏。
    僅管理員。
    """
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403

    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503

    # 載入所有「已檢查」確認，建立 (doc_id, field) → True 的索引
    # 同時保留 ack_id → (doc_id, field) 反查表，前端管理用
    acked_pairs = set()  # set of (firestore_doc_id, field)
    try:
        for ack in db.collection("audit_acks").stream():
            r = ack.to_dict() or {}
            fid = r.get("firestore_doc_id", "")
            fld = r.get("field", "")
            if fid and fld:
                acked_pairs.add((fid, fld))
    except Exception:
        pass  # ack 載入失敗不影響主流程

    # 建立「資料序號 → 主頁 Sheets row」字典（供前端跳行用）
    # 主頁的「資料序號」欄在 column AV（96 欄 header 範圍中的第 48 欄）
    seq_to_main_row = {}
    main_sheet_gid = 0
    try:
        sheets_svc = _get_sheets_service(timeout=20)
        # 取 sheet gid
        sheets_meta = sheets_svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        for s in sheets_meta.get("sheets", []):
            if s["properties"]["title"] == SHEET_NAME:
                main_sheet_gid = s["properties"]["sheetId"]; break
        # 讀整張 sheet header（96 欄）找出資料序號欄
        full_hdr = sheets_svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!A2:CY2"
        ).execute().get("values", [[]])[0]
        seq_col_in_main = None
        for i, h in enumerate(full_hdr):
            if h.strip() == "資料序號":
                seq_col_in_main = i; break
        if seq_col_in_main is not None:
            # 用 column letter 算出範圍
            col_letter = ""
            n = seq_col_in_main
            while True:
                col_letter = chr(65 + n % 26) + col_letter
                n = n // 26 - 1
                if n < 0: break
            seq_result = sheets_svc.spreadsheets().values().get(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!{col_letter}5:{col_letter}9999"
            ).execute()
            seq_vals = seq_result.get("values", [])
            for i, row_v in enumerate(seq_vals, start=5):  # 1-based row
                v = (row_v[0] if row_v else "").strip()
                if v:
                    seq_to_main_row[v] = i
    except Exception:
        pass  # 找不到 row 不影響體檢，只是沒法跳行

    missing = []
    stats = {
        "missing_鄉/市/鎮":    0,
        "missing_段別":       0,
        "missing_地號":       0,
        "missing_建號（建物）": 0,
        "missing_類別異常":    0,  # 類別說建物但既沒地址也沒建號 → 類別可能填錯
    }
    total = 0
    hidden = 0  # 被「已檢查」隱藏的筆數

    try:
        for d in db.collection("company_properties").stream():
            rd = d.to_dict() or {}
            if not _is_selling(rd):
                continue
            total += 1
            cat      = str(rd.get("物件類別", "") or "").strip()
            area     = str(rd.get("鄉/市/鎮", "") or "").strip()
            sect     = str(rd.get("段別", "") or "").strip()
            land     = _normalize_landno(rd.get("地號", ""))
            bldg_raw = str(rd.get("建號", "") or "").strip()  # 助理填的原始值
            addr     = str(rd.get("物件地址", "") or "").strip()
            has_addr = bool(addr and len(addr) >= 3)
            缺欄位 = []
            if not area:
                缺欄位.append("鄉/市/鎮")
            if not sect:
                缺欄位.append("段別")
            if not land:
                缺欄位.append("地號")
            # 建號規則（依使用者經驗修正）：
            # - 純土地類（農地/建地/土地/林地）→ 不檢查（本來就沒建號）
            # - 建物類 + 有物件地址 + 沒建號 → 「缺建號」（建物地址都有了，建號應該也要有）
            # - 建物類 + 沒物件地址 + 沒建號 → 「類別異常」
            #   （沒地址沒建號還說自己是透天/別墅 → 反過來懷疑類別填錯，可能其實是建地）
            # - 助理填了任何值（包括「無」「預售」「N/A」等標記）→ 視為已處理，不提示
            if _has_building_part(cat) and not bldg_raw:
                if has_addr:
                    缺欄位.append("建號")
                else:
                    缺欄位.append("類別異常")
            if 缺欄位:
                # 過濾掉每個欄位的已檢查確認
                visible_fields = []
                hidden_fields  = []
                for f in 缺欄位:
                    if (d.id, f) in acked_pairs:
                        hidden_fields.append(f)
                    else:
                        visible_fields.append(f)
                if not visible_fields:
                    # 全部欄位都已確認 → 該筆不顯示
                    hidden += 1
                    continue
                # 累計統計（只統計顯示的欄位）
                for f in visible_fields:
                    if f == "建號":
                        key = "missing_建號（建物）"
                    elif f == "類別異常":
                        key = "missing_類別異常"
                    else:
                        key = f"missing_{f}"
                    if key in stats:
                        stats[key] += 1
                # 用資料序號（= Firestore doc_id）查主頁 Sheets 對應 row
                main_row = seq_to_main_row.get(str(d.id), None)
                if main_row is None:
                    # 試一下 rd.get("資料序號") 字串化版本
                    main_row = seq_to_main_row.get(str(rd.get("資料序號", "")).strip(), None)
                missing.append({
                    "doc_id":          d.id,
                    "案名":            rd.get("案名", ""),
                    "委託編號":        rd.get("委託編號", ""),
                    "物件類別":        cat,
                    "資料序號":        rd.get("資料序號", ""),
                    "經紀人":          rd.get("經紀人", ""),
                    "缺欄位":          visible_fields,
                    "已確認欄位":      hidden_fields,    # 同筆其他欄位已確認的清單（顯示用）
                    "ack_ids":         {f: _audit_ack_id(d.id, f) for f in visible_fields},
                    "row_in_main":     main_row,         # 主頁 Sheets 對應行（前端跳行用）
                })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "detail": traceback.format_exc()[:500]}), 500

    return jsonify({
        "ok":            True,
        "total_scanned": total,
        "missing_count": len(missing),
        "hidden_count":  hidden,           # 被「已檢查」隱藏的筆數
        "missing":       missing[:1000],   # 最多回 1000 筆，避免 payload 太大
        "stats":         stats,
        "main_sheet_id":  SHEET_ID,        # 主頁 Sheets ID（供前端組行號連結）
        "main_sheet_gid": main_sheet_gid,
    })


@app.route("/api/access-data-audit/ack", methods=["POST"])
def api_access_data_audit_ack():
    """標記某筆物件「某個欄位」為「已檢查」，下次體檢自動隱藏這個欄位的提醒。
    Body: { firestore_doc_id, field, note (optional), display_name (optional) }
    """
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403

    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503

    data = request.get_json(silent=True) or {}
    fid = str(data.get("firestore_doc_id", "") or "").strip()
    field = str(data.get("field", "") or "").strip()
    if not fid or not field:
        return jsonify({"error": "缺 firestore_doc_id 或 field"}), 400

    ack_id = _audit_ack_id(fid, field)
    from datetime import datetime as _dt
    db.collection("audit_acks").document(ack_id).set({
        "firestore_doc_id": fid,
        "field":            field,
        "display_name":     str(data.get("display_name", "") or ""),
        "note":             str(data.get("note", "") or ""),
        "acked_by":         email,
        "acked_at":         _dt.utcnow().isoformat(),
    })
    return jsonify({"ok": True, "ack_id": ack_id})


@app.route("/api/access-data-audit/acks", methods=["GET"])
def api_access_data_audit_acks_list():
    """列出所有已檢查確認（用於管理 UI）。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403

    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503

    items = []
    try:
        for d in db.collection("audit_acks").stream():
            rec = d.to_dict() or {}
            rec["id"] = d.id
            items.append(rec)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    items.sort(key=lambda x: x.get("acked_at", ""), reverse=True)
    return jsonify({"ok": True, "items": items, "total": len(items)})


@app.route("/api/access-data-audit/acks/<ack_id>", methods=["DELETE"])
def api_access_data_audit_acks_delete(ack_id):
    """解除「已檢查」（取消後該筆物件下次體檢會重新出現）。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403

    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503

    try:
        db.collection("audit_acks").document(ack_id).delete()
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True})


@app.route("/api/access-data-audit/delete-rows", methods=["POST"])
def api_access_data_audit_delete_rows():
    """從主頁 Sheets 批次刪除指定行（用於完全重複的清理）。
    Body: { row_numbers: [int, ...], confirm_token: "CONFIRM_DELETE_DUPLICATES" }
    安全限制：
    - 僅管理員
    - 一次最多 1000 列
    - 必須帶確認 token（前端 confirm 後才送出）
    - 從大到小刪除（避免 row 編號偏移）
    """
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403

    data = request.get_json(silent=True) or {}
    rows_to_delete = data.get("row_numbers", [])
    if data.get("confirm_token") != "CONFIRM_DELETE_DUPLICATES":
        return jsonify({"error": "缺確認 token（前端應先彈窗確認）"}), 400
    if not rows_to_delete:
        return jsonify({"error": "row_numbers 為空"}), 400
    if len(rows_to_delete) > 1000:
        return jsonify({"error": "一次最多 1000 列，請分批"}), 400

    try:
        service = _get_sheets_service(timeout=60)
        # 取主頁分頁的數字 sheetId
        meta = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        sheet_gid = None
        for s in meta.get("sheets", []):
            if s["properties"]["title"] == SHEET_NAME:
                sheet_gid = s["properties"]["sheetId"]
                break
        if sheet_gid is None:
            return jsonify({"error": f"找不到分頁 {SHEET_NAME}"}), 500

        # 從大到小排序，避免刪除後 row 編號偏移影響後續刪除
        sorted_rows = sorted(set(int(r) for r in rows_to_delete), reverse=True)
        # 排除 row < 5（前 4 列是表頭/輔助列，不該刪）
        sorted_rows = [r for r in sorted_rows if r >= 5]
        if not sorted_rows:
            return jsonify({"error": "row_numbers 都小於 5（保護表頭區域）"}), 400

        # 一個 batchUpdate 處理多個 deleteDimension
        requests_body = []
        for r in sorted_rows:
            requests_body.append({
                "deleteDimension": {
                    "range": {
                        "sheetId":     sheet_gid,
                        "dimension":   "ROWS",
                        "startIndex":  r - 1,  # 0-based
                        "endIndex":    r,
                    }
                }
            })

        service.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": requests_body}
        ).execute()

        return jsonify({
            "ok":            True,
            "deleted_count": len(sorted_rows),
            "deleted_rows":  sorted_rows[:50],  # 回傳前 50 筆做日誌
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "detail": traceback.format_exc()[:500]}), 500


@app.route("/api/access-clean-broken-rows", methods=["GET", "POST"])
def api_access_clean_broken_rows():
    """一次性清理 ACCESS 套用 bug 造成的破損列：
    - 條件：非空欄位 <= 2、銷售中欄有值、無案名
    - 從大到小刪除避免 row 偏移
    - 僅管理員。GET 預覽（不刪）、POST 實際執行（必須帶 confirm_token）
    """
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403

    try:
        service = _get_sheets_service(timeout=60)
        meta = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        sheet_gid = None
        for s in meta.get("sheets", []):
            if s["properties"]["title"] == SHEET_NAME:
                sheet_gid = s["properties"]["sheetId"]; break
        if sheet_gid is None:
            return jsonify({"error": f"找不到分頁 {SHEET_NAME}"}), 500

        res = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!A1:AU9999"
        ).execute()
        rows = res.get("values", [])
        if len(rows) < 5:
            return jsonify({"error": "資料不足"}), 400
        h = rows[1]
        name_idx  = h.index("案名") if "案名" in h else -1
        sell_idx  = h.index("銷售中") if "銷售中" in h else -1
        owner_idx = h.index("所有權人") if "所有權人" in h else -1
        broken_list = []
        for i, row in enumerate(rows[4:], start=5):
            if not row: continue
            non_empty = sum(1 for c in row if c.strip())
            if non_empty == 0: continue
            name_v  = (row[name_idx].strip()  if name_idx  >= 0 and name_idx  < len(row) else "")
            owner_v = (row[owner_idx].strip() if owner_idx >= 0 and owner_idx < len(row) else "")
            # broken 判定：無案名 + 無所有權人 + 非空 <= 5
            # 「有所有權人」的列保留（可能是案名漏填的真實物件）
            if not name_v and not owner_v and non_empty <= 5:
                broken_list.append(i)

        method = request.method
        if method == "GET":
            return jsonify({
                "ok": True,
                "preview_only": True,
                "broken_count": len(broken_list),
                "broken_rows":  broken_list,
                "hint":         "POST 同端點 + body {confirm_token:'CONFIRM_CLEAN_BROKEN'} 才會實際刪除"
            })

        data = request.get_json(silent=True) or {}
        if data.get("confirm_token") != "CONFIRM_CLEAN_BROKEN":
            return jsonify({"error": "缺確認 token"}), 400

        # 從大到小刪除
        requests_body = []
        for r in sorted(set(broken_list), reverse=True):
            if r < 5: continue  # 保護表頭
            requests_body.append({
                "deleteDimension": {
                    "range": {"sheetId": sheet_gid, "dimension": "ROWS",
                              "startIndex": r - 1, "endIndex": r}
                }
            })
        if requests_body:
            service.spreadsheets().batchUpdate(
                spreadsheetId=SHEET_ID, body={"requests": requests_body}
            ).execute()
        return jsonify({"ok": True, "deleted_count": len(requests_body), "deleted_rows": broken_list})
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "detail": traceback.format_exc()[:500]}), 500


@app.route("/api/access-data-audit/duplicates", methods=["GET"])
def api_access_data_audit_duplicates():
    """主頁 Sheets 重複物件偵測：
    - exact_dup_groups: 同 hard_key + 同委託編號 + 同委託日（一定要刪一份）
    - history_groups:   同 hard_key 但委託編號或委託日不同（同物件多次委託歷史，使用者決定）
    每筆提供 Sheets 行號連結，方便直接跳到 Sheets 編輯。
    """
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403

    try:
        service = _get_sheets_service(timeout=30)
        # 取主頁 sheet 的數字 gid（用於組行號連結 URL）
        meta = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        sheet_gid = 0
        for s in meta.get("sheets", []):
            if s["properties"]["title"] == SHEET_NAME:
                sheet_gid = s["properties"]["sheetId"]
                break

        # 讀主頁全部資料
        result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1:AU9999"
        ).execute()
        all_rows = result.get("values", [])
        if len(all_rows) < 5:
            return jsonify({"error": "主頁資料不足"}), 400

        headers = all_rows[1]  # header 在第 2 列
        # 從第 5 列起為資料
        data_rows = []
        for i, row in enumerate(all_rows[4:]):
            if any(c.strip() for c in row):
                data_rows.append((5 + i, row))

        # 對每筆物件計算 hard_keys，用代表性 key 歸群
        from collections import defaultdict
        key_groups = defaultdict(list)  # primary_key tuple → [item info]
        for rn, row in data_rows:
            d = _access_row_to_dict(headers, row)
            hks = _access_hard_keys(d)
            if not hks:
                continue
            # 代表性 key：hard_bldg 優先，否則 hard_land 第一個
            bldg_keys = [k for k in hks if k[0] == "hard_bldg"]
            land_keys = [k for k in hks if k[0] == "hard_land"]
            primary = bldg_keys[0] if bldg_keys else land_keys[0]
            key_groups[primary].append({
                "row":         rn,
                "name":        d.get("案名", ""),
                "comm":        str(d.get("委託編號", "") or "").strip(),
                "commit_date": str(d.get("委託日", "") or "").strip(),
                "seq":         str(d.get("資料序號", "") or "").strip(),
                "agent":       d.get("經紀人", ""),
                "category":    d.get("物件類別", ""),
                "price":       d.get("售價(萬)", ""),
                "expiry":      d.get("委託到期日", ""),
            })

        exact_dup_groups = []
        history_groups   = []
        for k, items in key_groups.items():
            if len(items) < 2:
                continue
            # 群內按 (委編, 委託日) 再分子群 — 同 hard_key 內同 (委編,日) 出現 ≥2 次的列就是完全重複；
            # 其他列（不同委編或不同日）才當歷史版本。
            # 修這個 bug：「皇家大樓一樓」3 列同 hard_key（14、15 完全重複；388 是舊版），
            # 之前整群被歸 history → 列 14/15 沒進完全重複區。
            from collections import defaultdict as _dd
            sub = _dd(list)
            for it in items:
                sub[(it["comm"], it["commit_date"])].append(it)

            exact_items_in_group = []  # 屬於完全重複的列（合併同 hard_key 內所有子群的「重複多餘列」）
            unique_reps          = []  # 每個 (委編,日) 子群挑一筆代表（給歷史版本用）

            for sub_key, sub_items in sub.items():
                if len(sub_items) >= 2:
                    # 完全重複子群：整組丟進 exact_dup_groups
                    exact_items_in_group.extend(sub_items)
                    # 留一筆當代表（保留列號最小者，留作歷史 representative）
                    sub_items_sorted = sorted(sub_items, key=lambda x: x["row"])
                    unique_reps.append(sub_items_sorted[0])
                else:
                    unique_reps.append(sub_items[0])

            if exact_items_in_group:
                # 同 hard_key 的完全重複另成一群（一次刪掉同群多餘列，前端可分群顯示）
                exact_dup_groups.append({
                    "key":   str(k),
                    "items": exact_items_in_group,
                })

            # 不同 (委編,日) 的列才是歷史版本，需要 ≥ 2 個不同 (委編,日) 才算歷史
            if len(unique_reps) >= 2:
                sorted_items = sorted(
                    unique_reps,
                    key=lambda x: _parse_date_for_compare(x["commit_date"]),
                    reverse=True
                )
                for i, it in enumerate(sorted_items):
                    it["is_latest"] = (i == 0)
                history_groups.append({
                    "key":   str(k),
                    "items": sorted_items,
                })

        return jsonify({
            "ok":               True,
            "sheet_id":         SHEET_ID,
            "sheet_gid":        sheet_gid,
            "exact_dup_groups": exact_dup_groups,
            "exact_dup_count":  sum(len(g["items"]) for g in exact_dup_groups),
            "history_groups":   history_groups,
            "history_count":    sum(len(g["items"]) for g in history_groups),
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "detail": traceback.format_exc()[:500]}), 500


def _parse_price_num(val):
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except Exception:
        return val


def _row_to_doc(headers, row):
    """把一行資料轉成 Firestore document dict。"""
    data = {}
    for i, h in enumerate(headers):
        if not h or not h.strip():
            continue
        val = row[i].strip() if i < len(row) else ""
        if not val:
            continue
        if h in _NUMERIC_FIELDS:
            data[h] = _parse_price_num(val)
        elif h == "銷售中":
            pass  # 銷售中由 Firestore 自行管理（Word 審查或手動回寫），不從 Sheets 覆蓋
        else:
            data[h] = val
    return data


def _do_sync(org_id=None):
    """執行同步（在背景執行緒中跑）。回傳結果 dict。
    org_id：寫入每筆文件的組織 ID，None 表示自動從管理員帳號查詢。
    """
    import logging
    log = logging.getLogger("sync-properties")
    started = datetime.now(timezone.utc).isoformat()

    try:
        log.info("開始同步 Sheets → Firestore")
        headers, data_rows = _sheets_read_all()
        log.info(f"讀到 {len(data_rows)} 筆資料")

        db = _get_db()
        if db is None:
            return {"ok": False, "error": "Firestore 未連線", "started": started}

        # 自動查詢管理員的 org_id（用於標記每筆資料的組織歸屬）
        if not org_id:
            for admin_email in ADMIN_EMAILS:
                org = _get_org_for_user(admin_email)
                if org:
                    org_id = org["org_id"]
                    log.info(f"同步使用組織 org_id={org_id}（來自 {admin_email}）")
                    break
            if not org_id:
                log.warning("管理員尚未加入任何組織，本次同步不寫入 org_id")

        col = db.collection("company_properties")

        # 委託日過濾門檻（民國 102/1/1 = 西元 2013/1/1）— 同步只處理此日後物件，省時
        # 對於既有 Firestore 中 < 2013/1/1 的舊物件：不更新、也不刪除（保留歷史）
        MIN_COMMIT = (2013, 1, 1)
        commit_col_idx = None
        for i, h in enumerate(headers):
            if h == "委託日":
                commit_col_idx = i
                break

        # 讀取既有 Firestore 文件 ID + 委託日（供 to_delete 階段判斷保留舊資料）
        existing_with_date = {}
        for doc in col.stream():
            rec = doc.to_dict() or {}
            existing_with_date[doc.id] = _parse_date_smart(rec.get("委託日", ""))
        existing_ids = set(existing_with_date.keys())

        written = skipped = deleted = skipped_old = preserved_old = 0
        seen_ids = set()
        # 明細追蹤（給 UI 顯示）：新增/刪除的案名 + 序號
        added_items   = []  # [{seq, 案名, 委託日, 經紀人}]
        deleted_items = []  # 同上

        # 先把 Firestore 既有物件的案名抓出來（待會刪除時顯示用）
        existing_names = {}  # doc_id → 案名
        for doc in col.stream():
            rec = doc.to_dict() or {}
            existing_names[doc.id] = rec.get("案名", "")

        for row in data_rows:
            d = _row_to_doc(headers, row)
            seq = str(d.get("資料序號", "")).strip()
            if not seq or not seq.isdigit():
                skipped += 1
                continue

            # 委託日過濾：< 2013/1/1 → 跳過寫入（省時），但放入 seen_ids 避免被誤刪
            # 委託日空白者保留處理
            if commit_col_idx is not None:
                commit_v = row[commit_col_idx] if commit_col_idx < len(row) else ""
                t = _parse_date_smart(commit_v)
                if t is not None and t < MIN_COMMIT:
                    skipped_old += 1
                    seen_ids.add(seq)
                    continue

            doc_id = seq
            is_new = doc_id not in existing_ids  # 寫入前判斷是新增還是更新
            seen_ids.add(doc_id)
            d["_synced_at"] = started
            # 標記組織歸屬，讓不同公司的資料互相隔離
            if org_id:
                d["org_id"] = org_id
            # merge=True：只更新有值的欄位，不覆蓋 Firestore 中沒在 Sheets 的欄位
            # 特別保護「銷售中」：由 Word 審查或手動回寫管理，不被 Sheets 覆蓋
            col.document(doc_id).set(d, merge=True)
            written += 1
            if is_new:
                # 新增明細（給 UI 顯示）
                added_items.append({
                    "seq":     seq,
                    "案名":     d.get("案名", ""),
                    "委託日":   d.get("委託日", ""),
                    "經紀人":   d.get("經紀人", ""),
                })
            if written % 200 == 0:
                log.info(f"進度：{written}/{len(data_rows)}")

        # 刪除 Firestore 中已不存在於 Sheets 的文件（避免髒資料）
        # 但 < 2013/1/1 的舊物件保留不刪（使用者「先用一段時間再說」原則）
        to_delete = existing_ids - seen_ids
        for doc_id in to_delete:
            ed = existing_with_date.get(doc_id)
            if ed is None or ed < MIN_COMMIT:
                preserved_old += 1
                continue
            deleted_items.append({
                "seq":  doc_id,
                "案名":  existing_names.get(doc_id, ""),
            })
            col.document(doc_id).delete()
            deleted += 1

        added_count   = len(added_items)
        updated_count = written - added_count
        result = {
            "ok": True,
            "written":        written,        # 新增 + 更新總計
            "added":          added_count,    # 新增（Firestore 之前沒有）
            "updated":        updated_count,  # 更新（已存在，欄位刷新）
            "skipped":        skipped,        # 跳過（資料序號異常）
            "skipped_old":    skipped_old,    # 跳過寫入（委託日 < 2013/1/1）
            "deleted":        deleted,        # 刪除（不在 Sheets 中）
            "preserved_old":  preserved_old,  # 保留不刪（既有舊物件）
            "added_items":    added_items[:50],   # 新增明細前 50 筆給 UI 顯示
            "deleted_items":  deleted_items[:50], # 刪除明細前 50 筆給 UI 顯示
            "started":  started,
            "finished": datetime.now(timezone.utc).isoformat()
        }
        log.info(f"同步完成：written={written}(added={added_count}/updated={updated_count}) deleted={deleted} skipped_old={skipped_old} preserved_old={preserved_old}")

        # 同步完成後，更新物件快速搜尋索引（存入 Firestore meta 文件）
        try:
            _rebuild_prop_index(db, col)
            log.info("物件搜尋索引更新完成")
        except Exception as ex:
            log.warning(f"索引更新失敗（不影響同步結果）: {ex}")

        # 通知 home-start 對外網站做全量同步（webhook 失敗不影響主流程）
        try:
            home_start_url = (os.environ.get("HOME_START_URL") or "").strip()
            service_key    = (os.environ.get("SERVICE_API_KEY") or "").strip()
            if home_start_url and service_key:
                import requests as _req
                _req.post(
                    f"{home_start_url.rstrip('/')}/api/sync/full",
                    headers={"X-Service-Key": service_key},
                    timeout=8,
                )
                log.info("已通知 home-start 全量同步")
        except Exception as ex:
            log.warning(f"通知 home-start 失敗（不影響本同步結果）: {ex}")

        return result

    except Exception as e:
        log.exception("同步失敗")
        return {"ok": False, "error": str(e), "started": started}


def _rebuild_prop_index(db, col=None):
    """
    重建物件快速搜尋索引，存入 Firestore meta/prop_index 文件。
    只含 id, 案名, 物件地址, 類別, 銷售中旗標，供 /api/prop-suggest 使用。
    銷售中的物件排在前面，搜尋時優先出現。
    """
    if col is None:
        db = _get_db()
        if db is None:
            return
        col = db.collection("company_properties")
    selling = []
    others  = []
    for d in col.stream():
        r = d.to_dict()
        name    = str(r.get("案名", "") or "").strip()
        address = str(r.get("物件地址", "") or "").strip()
        cat     = str(r.get("類別", "") or "").strip()
        owner   = str(r.get("所有權人", "") or "").strip()
        section = str(r.get("段別", "") or "").strip()
        landno  = str(r.get("地號", "") or "").strip()
        area    = str(r.get("鄉/市/鎮", "") or "").strip()
        if not name:
            continue
        s = r.get("銷售中")
        is_selling = (s is True or s == "銷售中")
        entry = {
            "id": d.id, "n": name, "a": address, "c": cat,
            "s": 1 if is_selling else 0,
            "o": owner, "sec": section, "lno": landno, "ar": area,
        }
        if is_selling:
            selling.append(entry)
        else:
            others.append(entry)
    # 銷售中排前，其餘排後
    index = selling + others
    # 存入 meta 集合（單一文件，最大 1MB；約 5000 筆 × 80 bytes ≈ 400KB，OK）
    db.collection("meta").document("prop_index").set({
        "data": json.dumps(index, ensure_ascii=False),
        "updated_at": datetime.now(timezone.utc).isoformat()
    })


@app.route("/api/sync-properties", methods=["POST"])
def api_sync_properties():
    """
    觸發 Sheets → Firestore 同步。
    - 管理員登入後可呼叫（前端按鈕）
    - Cloud Scheduler 用 X-Sync-Key header 驗證（不需登入）
    """
    # 驗證方式1：管理員 session
    is_admin_user = False
    email = session.get("user_email")
    if email and _is_admin(email):
        is_admin_user = True

    # 驗證方式2：Cloud Scheduler 傳來的 Sync Key
    sync_key = os.environ.get("SYNC_SECRET_KEY", "")
    req_key = request.headers.get("X-Sync-Key", "")
    is_scheduler = bool(sync_key and req_key and sync_key == req_key)

    if not is_admin_user and not is_scheduler:
        return jsonify({"error": "無權限"}), 403

    # 避免重複同步
    if _sync_status["running"]:
        return jsonify({"error": "同步正在執行中，請稍後再試"}), 429

    # 取得觸發者的 org_id（管理員登入時有 session，排程器則自動查詢）
    trigger_org_id = None
    if email:
        org = _get_org_for_user(email)
        if org:
            trigger_org_id = org["org_id"]

    # 背景執行（避免 Cloud Scheduler timeout）
    def run():
        _sync_status["running"] = True
        try:
            result = _do_sync(org_id=trigger_org_id)
            _sync_status["last_result"] = result
            _sync_status["last_run"] = result.get("finished") or result.get("started")
        finally:
            _sync_status["running"] = False

    t = threading.Thread(target=run, daemon=True)
    t.start()

    return jsonify({"ok": True, "message": "同步已在背景啟動，約需 1-2 分鐘完成"})


# ── 除錯用：比對 Sheets vs Firestore 的指定物件（管理員限定） ──
@app.route("/api/debug-sync/<seq_id>", methods=["GET"])
def api_debug_sync(seq_id):
    """讀取 Sheets 與 Firestore 中指定資料序號的物件，回傳比對結果。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email):
        return jsonify({"error": "無權限"}), 403

    result = {"seq_id": seq_id}

    # 1. 從 Sheets 讀取
    try:
        headers, data_rows = _sheets_read_all()
        sheets_doc = None
        for row in data_rows:
            d = _row_to_doc(headers, row)
            if str(d.get("資料序號", "")).strip() == str(seq_id):
                sheets_doc = d
                break
        result["sheets"] = sheets_doc or "找不到此資料序號"
        result["sheets_headers"] = headers  # 看欄位順序是否正確
    except Exception as e:
        result["sheets_error"] = str(e)

    # 2. 從 Firestore 讀取
    try:
        db = _get_db()
        doc = db.collection("company_properties").document(str(seq_id)).get()
        if doc.exists:
            result["firestore"] = doc.to_dict()
        else:
            result["firestore"] = "文件不存在"
    except Exception as e:
        result["firestore_error"] = str(e)

    return jsonify(result)


@app.route("/api/sync-properties/status", methods=["GET"])
def api_sync_properties_status():
    """查詢同步狀態（管理員用）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email):
        return jsonify({"error": "無權限"}), 403
    return jsonify({
        "running": _sync_status["running"],
        "last_run": _sync_status["last_run"],
        "last_result": _sync_status["last_result"]
    })


@app.route("/api/internal/scheduled-writeback", methods=["POST"])
def api_internal_scheduled_writeback():
    """
    Cloud Scheduler 定時觸發的銷售中回寫端點（不需 session，用 SERVICE_API_KEY 驗證）。
    設定方式：Cloud Scheduler → 每天 06:05 打此端點，Header 帶 X-Api-Key。
    在 scheduled-sync（06:00）之後執行，把 Firestore 銷售中狀態同步回 Sheets。
    """
    api_key = request.headers.get("X-Api-Key", "")
    if not api_key or api_key != SERVICE_API_KEY:
        return jsonify({"error": "unauthorized"}), 401

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    import threading
    def _run():
        try:
            seq_to_selling = {}
            for doc in db.collection("company_properties").stream():
                r = doc.to_dict()
                seq = doc.id
                if seq and seq.isdigit():
                    seq_to_selling[seq] = _is_selling(r)
            if seq_to_selling:
                _sheets_write_selling_status(seq_to_selling)
        except Exception:
            import logging
            logging.getLogger("scheduled-writeback").exception("回寫失敗")

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "銷售中回寫已啟動（背景執行）"})


@app.route("/api/internal/scheduled-sync", methods=["POST"])
def api_internal_scheduled_sync():
    """
    Cloud Scheduler 定時觸發的 Sheets 同步端點（不需 session，用 SERVICE_API_KEY 驗證）。
    設定方式：Cloud Scheduler → 每天 06:00 打此端點，Header 帶 X-Api-Key。
    """
    api_key = request.headers.get("X-Api-Key", "")
    if not api_key or api_key != SERVICE_API_KEY:
        return jsonify({"error": "unauthorized"}), 401

    with _sync_lock:
        if _sync_status["running"]:
            return jsonify({"ok": False, "message": "同步已在進行中"}), 409
        _sync_status["running"] = True

    import threading
    def _run():
        try:
            result = _do_sync()
            _sync_status["last_result"] = result
            _sync_status["last_run"] = datetime.now(timezone.utc).isoformat()
        finally:
            _sync_status["running"] = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "同步已啟動（背景執行）"})


# ── 物件類別大類對應表（不影響 Sheets 原始資料） ──
# 搜尋時選大類 → 自動展開成多個原始類別進行過濾
CATEGORY_GROUPS = {
    "住宅類": ["住家", "住宅", "套房", "華廈", "平房", "透天", "透住",
              "透天+農地", "透天 建地", "建地 住家", "建地+住家",
              "建地+平房", "平房+承租地"],
    "公寓類": ["公寓", "公寓/套房"],
    "別墅類": ["別墅", "別墅+建地", "別墅店住", "農地+別墅", "店面+別墅"],
    "店面/商用": ["店住", "店面", "店面 建地", "店面+建地", "攤位", "辦公大樓",
                "民宿", "廠房", "廠辦"],
    "農地類": ["農地", "農舍", "農建地", "農建", "農+建", "農地+建地",
              "建地+農地", "農地+農舍", "農地+住家", "農地/建地",
              "農地 資材室", "農地+別墅"],
    "建地類": ["建地", "建農地", "土地", "林地", "國有農地+建地",
              "建地+廠房", "農建", "農建地"],
}

# ── 地區簡寫 → 完整縣市鄉鎮名稱對應表 ──
# 排序原則：台東縣在最前，台東市第一；花蓮縣次之；其他縣市最後
AREA_DISPLAY = {
    # ── 台東縣（主要業務區） ──
    "台東":   "台東縣 台東市",
    "卑南":   "台東縣 卑南鄉",
    "鹿野":   "台東縣 鹿野鄉",
    "關山":   "台東縣 關山鎮",
    "池上":   "台東縣 池上鄉",
    "東河":   "台東縣 東河鄉",
    "成功":   "台東縣 成功鎮",
    "長濱":   "台東縣 長濱鄉",
    "太麻里": "台東縣 太麻里鄉",
    "大武":   "台東縣 大武鄉",
    "延平":   "台東縣 延平鄉",
    "海端":   "台東縣 海端鄉",
    "金峯":   "台東縣 金峰鄉",
    "金鋒":   "台東縣 金峰鄉",   # 同鄉不同寫法
    "獅子鄉": "屏東縣 獅子鄉",   # 屏東（接近台東）
    "綠島":   "台東縣 綠島鄉",
    # ── 花蓮縣 ──
    "花蓮":       "花蓮縣 花蓮市",
    "壽豐":       "花蓮縣 壽豐鄉",
    "光復":       "花蓮縣 光復鄉",
    "玉里":       "花蓮縣 玉里鎮",
    "富里":       "花蓮縣 富里鄉",
    "花蓮富里":   "花蓮縣 富里鄉",
    "鳳林":       "花蓮縣 鳳林鎮",
    "花蓮豐濱":   "花蓮縣 豐濱鄉",
    "花蓮縣.豐濱市": "花蓮縣 豐濱鄉",
    # ── 其他縣市 ──
    "台中大里區": "台中市 大里區",
    "台南":   "台南市",
    "彰化":   "彰化縣",
    "高雄":   "高雄市",
    "新營":   "台南市 新營區",
    "潮州":   "屏東縣 潮州鎮",
    "枋寮":   "屏東縣 枋寮鄉",
}

# 地區排序順序（台東縣優先、台東市最前；其他依縣市分組）
_AREA_SORT_ORDER = [
    "台東", "卑南", "鹿野", "關山", "池上", "東河", "成功", "長濱",
    "太麻里", "大武", "延平", "海端", "金峯", "金鋒", "綠島",
    "花蓮", "壽豐", "光復", "玉里", "富里", "花蓮富里", "鳳林",
    "花蓮豐濱", "花蓮縣.豐濱市",
    "獅子鄉", "潮州", "枋寮",
    "台中大里區", "台南", "新營", "彰化", "高雄",
]

def _area_sort_key(raw_area):
    """地區排序鍵：依 _AREA_SORT_ORDER 排序，不在表中的排最後"""
    try:
        return _AREA_SORT_ORDER.index(raw_area)
    except ValueError:
        return len(_AREA_SORT_ORDER)
# 反查：原始類別 → 大類名稱（不在表中的 → 自動歸「其他」）
_CAT_REVERSE = {}
for _grp, _cats in CATEGORY_GROUPS.items():
    for _c in _cats:
        _CAT_REVERSE[_c] = _grp

# 「其他」包含所有不在上述大類的原始類別（動態判斷，不需列舉）
_OTHER_GROUP = "其他"

# 公司目前在線人員（置頂顯示）
ACTIVE_AGENTS = ["張文澤", "陳威良", "雷文海", "歐芷妤", "許荺芯", "蔡秀芳", "李振迎"]


def _expand_category_group(name):
    """輸入大類名稱，回傳原始類別 list；若不是大類則回傳 [name]。
    「其他」特殊處理：回傳空 list（代表「不在任何大類」的類別）。
    """
    if name == _OTHER_GROUP:
        return []   # 由呼叫端特殊處理
    return CATEGORY_GROUPS.get(name, [name])


# ── 日期字串轉可排序格式（YYYY-MM-DD），支援 YYYY/M/D 不補零格式 ──
def _parse_date_key(date_str):
    """把各種日期格式轉成 YYYY-MM-DD 字串供排序，無效值給 '0000-00-00'。"""
    import re as _re
    s = str(date_str).strip()
    if not s or s == 'None':
        return '0000-00-00'
    # 格式：YYYY/M/D 或 YYYY-M-D 或 YYYY/MM/DD
    m = _re.match(r'(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})', s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    return '0000-00-00'


# ── 銷售狀態判斷輔助（模組層級，多個 API 共用） ──
def _is_selling(r):
    """判斷物件是否為銷售中。銷售中欄位可能是布林或字串，統一轉換。"""
    v = r.get("銷售中")
    if v is True:   return True
    if v is False:  return False
    s = str(v).strip()
    if s in ("True", "銷售中", "true", "1"): return True
    if s in ("False", "已下架", "已成交", "false", "0"): return False
    return True  # 無此欄位或其他值，視為銷售中


def _parse_expiry_yyyymmdd(r):
    """將物件的「委託到期日」解析為 YYYY-MM-DD 字串；無值或無法解析回傳 None。
    支援格式：民國「115年5月29日」、西元「2026/05/29」、「2026-05-29」。
    """
    exp = str(r.get("委託到期日") or "").strip()
    if not exp:
        return None
    import re as _re
    m = _re.match(r"(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日", exp)
    if m:
        yr = int(m.group(1)) + (1911 if int(m.group(1)) < 1000 else 0)
        return f"{yr:04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m2 = _re.match(r"(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})", exp)
    if m2:
        return f"{int(m2.group(1)):04d}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}"
    return None


def _is_within_delegation(r):
    """委託期間內：有「委託到期日」且日期 >= 今天（台灣時區）。"""
    expiry = _parse_expiry_yyyymmdd(r)
    if not expiry:
        return False
    from datetime import datetime, timezone, timedelta
    today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    return expiry >= today


def _notify_home_start(property_id, action="upsert"):
    """通知 home-start 物件變動（webhook）。失敗不影響主流程。
    action: upsert（新增/修改）/ unlist（下架）/ delete（刪除）。
    """
    home_start_url = (os.environ.get("HOME_START_URL") or "").strip()
    service_key    = (os.environ.get("SERVICE_API_KEY") or "").strip()
    if not home_start_url or not service_key or not property_id:
        return
    try:
        import requests as _req
        _req.post(
            f"{home_start_url.rstrip('/')}/api/sync/webhook",
            json={"property_id": str(property_id), "action": action},
            headers={"X-Service-Key": service_key, "Content-Type": "application/json"},
            timeout=5,
        )
    except Exception as _e:
        # webhook 失敗不影響本主流程，只記 log
        print(f"[home-start webhook] {_e}", flush=True)


def _public_property_payload(r):
    """組成對外公開的物件資訊（簡化欄位，不含經紀人/所有權人等內部資訊）。"""
    if not r:
        return None
    coord = (r.get("座標") or "").strip()
    lat = lng = None
    if coord and "," in coord:
        try:
            parts = coord.split(",")
            lat = float(parts[0].strip())
            lng = float(parts[1].strip())
        except (ValueError, IndexError):
            pass
    layout = ""
    rooms = baths = 0
    try:
        rooms = int(r.get("房數") or r.get("房") or 0)
    except (ValueError, TypeError):
        pass
    try:
        baths = int(r.get("衛數") or r.get("衛") or 0)
    except (ValueError, TypeError):
        pass
    if rooms or baths:
        layout = f"{rooms or '?'}房{baths or '?'}衛"
    return {
        "id":            r.get("id"),
        "source_id":     r.get("id"),
        "title":         (r.get("案名") or "").strip(),
        "address":       (r.get("物件地址") or "").strip(),
        "area":          (r.get("鄉/市/鎮") or r.get("地區") or "").strip(),
        "category":      (r.get("物件類別") or "").strip(),
        "price":         r.get("售價(萬)") or r.get("售價") or None,
        "building_ping": r.get("建坪") or None,
        "land_ping":     r.get("地坪") or None,
        "rooms":         rooms,
        "baths":         baths,
        "layout":        layout or (r.get("格局") or ""),
        "floor":         (r.get("樓層") or r.get("floor") or "").strip(),
        "age":           r.get("屋齡") or r.get("age") or None,
        "parking":       (r.get("車位") or "").strip(),
        "lat":           lat,
        "lng":           lng,
        "is_selling":    _is_selling(r),
    }


def _verify_service_key():
    """跨服務驗證：header X-Service-Key 等於環境變數 SERVICE_API_KEY。"""
    expected = (os.environ.get("SERVICE_API_KEY") or "").strip()
    got = (request.headers.get("X-Service-Key") or "").strip()
    return bool(expected) and got == expected


# ── 對外公開 API（給 home-start 同步用，需 X-Service-Key）──

@app.route("/api/admin/backfill-coords", methods=["POST"])
def api_admin_backfill_coords():
    """補座標 API（service-key 認證，給管理腳本/排程用）。

    動作：掃描銷售中 ∧ 有段別+地號 ∧ 座標空白的物件，呼叫 easymap 反查座標，
    **只寫進 Firestore**「座標」欄位（merge=True），不動 Sheets。

    與 /api/sheets/writeback-selling 內建的「順便補座標」差異：
    - 不檢查委託期內（多補一些，畢竟 library 自家地圖也想看）
    - 不寫回 Sheets（最低風險、不動主資料）
    - 可用 service key 自動觸發（不用人工登入按鈕）

    Query/Body：可加 ?limit=50 限制每次處理筆數（避免一次跑太久）
    回傳：{ ok, candidates, resolved, failed, written_firestore, details:[...] }
    """
    if not _verify_service_key():
        return jsonify({"error": "unauthorized"}), 401

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    try:
        limit = int(request.args.get("limit") or 0)
    except (TypeError, ValueError):
        limit = 0

    # 1) 找出待補座標的物件
    coord_targets = []  # [(seq, area, section, landno)]
    skipped_reasons = {"已有座標": 0, "非銷售中": 0, "缺段別/地號": 0, "id 非數字": 0}
    for doc in db.collection("company_properties").stream():
        r = doc.to_dict()
        seq = doc.id
        if not _is_selling(r):
            skipped_reasons["非銷售中"] += 1
            continue
        if r.get("座標"):
            skipped_reasons["已有座標"] += 1
            continue
        area    = (r.get("鄉/市/鎮") or "").strip()
        section = (r.get("段別") or "").strip()
        landno  = (r.get("地號") or "").strip()
        if not (area and section and landno):
            skipped_reasons["缺段別/地號"] += 1
            continue
        if not (seq and str(seq).isdigit()):
            skipped_reasons["id 非數字"] += 1
            continue
        coord_targets.append((seq, area, section, landno))

    if limit > 0:
        coord_targets = coord_targets[:limit]

    # 2) 逐筆呼叫 easymap 並寫進 Firestore
    resolved = 0
    failed = 0
    details = []
    for seq, area, section, landno in coord_targets:
        coord = _easymap_resolve(area, section, landno)
        if coord:
            coord_str = f"{coord[0]:.6f},{coord[1]:.6f}"
            try:
                db.collection("company_properties").document(seq).set(
                    {"座標": coord_str}, merge=True
                )
                resolved += 1
                details.append({"seq": seq, "ok": True, "coord": coord_str})
            except Exception as e:
                failed += 1
                details.append({"seq": seq, "ok": False, "error": f"firestore_write: {e}"})
        else:
            failed += 1
            details.append({
                "seq": seq, "ok": False,
                "area": area, "section": section, "landno": landno,
                "error": "easymap 反查失敗",
            })

    return jsonify({
        "ok": True,
        "candidates": len(coord_targets),
        "resolved": resolved,
        "failed": failed,
        "skipped": skipped_reasons,
        "details": details[:50],  # 只回前 50 筆細節避免回傳過大
    })


@app.route("/api/public/properties", methods=["GET"])
def api_public_properties():
    """回傳「銷售中 + 委託期間內」的物件（簡化欄位）。home-start 全量同步用。
    過期委託（無到期日或到期日 < 今天）不會被列入，確保對外網站只顯示能成交的物件。
    """
    if not _verify_service_key():
        return jsonify({"error": "unauthorized"}), 401
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    items = []
    try:
        for doc in db.collection("company_properties").stream():
            r = doc.to_dict()
            r["id"] = doc.id
            if not _is_selling(r):
                continue
            if not _is_within_delegation(r):
                continue  # 委託已到期或無到期日，不對外公開
            payload = _public_property_payload(r)
            if payload:
                items.append(payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    return jsonify({"items": items, "count": len(items)})


@app.route("/api/public/properties/<prop_id>", methods=["GET"])
def api_public_property_one(prop_id):
    """回傳單筆物件的簡化欄位。home-start webhook 同步單筆用。
    若已下架或委託過期，回傳 410 Gone，方便 home-start 知道要下架本地快取。
    """
    if not _verify_service_key():
        return jsonify({"error": "unauthorized"}), 401
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        doc = db.collection("company_properties").document(prop_id).get()
        if not doc.exists:
            return jsonify({"error": "not found"}), 404
        r = doc.to_dict()
        r["id"] = doc.id
        if not _is_selling(r) or not _is_within_delegation(r):
            return jsonify({"error": "gone"}), 410
        return jsonify(_public_property_payload(r))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── 地圖：取得有座標的銷售中物件 ──

@app.route("/api/map/properties", methods=["GET"])
def api_map_properties():
    """回傳銷售中且有座標的物件，支援類別/地區/經紀人篩選。
    Query params:
      cats   - 逗號分隔的物件類別（支援大類名稱，空白=全部）
      areas  - 逗號分隔的地區（空白=全部）
      agents - 逗號分隔的經紀人（空白=全部）
    """
    if not session.get("user_email"):
        return jsonify({"error": "請先登入"}), 401

    # 解析篩選參數
    cats_raw   = request.args.get("cats", "").strip()
    areas_raw  = request.args.get("areas", "").strip()
    agents_raw = request.args.get("agents", "").strip()

    # 展開大類 → 原始類別 set（空白 = 不篩選）
    filter_cats = set()
    filter_cats_other = False  # 是否包含「其他」大類
    if cats_raw:
        for c in cats_raw.split(","):
            c = c.strip()
            if not c: continue
            if c == _OTHER_GROUP:
                filter_cats_other = True
            else:
                filter_cats.update(_expand_category_group(c))

    filter_areas  = {a.strip() for a in areas_raw.split(",")  if a.strip()} if areas_raw  else set()
    filter_agents = {a.strip() for a in agents_raw.split(",") if a.strip()} if agents_raw else set()

    db = _get_db()
    results = []
    for doc in db.collection("company_properties").stream():
        r = doc.to_dict()
        r["id"] = doc.id
        # 只要銷售中 + 有座標
        if not _is_selling(r):
            continue
        coord = r.get("座標", "").strip()
        if not coord:
            continue
        parts = coord.split(",")
        if len(parts) != 2:
            continue
        try:
            lat = float(parts[0].strip())
            lng = float(parts[1].strip())
        except ValueError:
            continue

        # 篩選類別
        if filter_cats or filter_cats_other:
            cat = r.get("物件類別", "")
            in_known = cat in filter_cats
            in_other = filter_cats_other and _CAT_REVERSE.get(cat) is None
            if not in_known and not in_other:
                continue

        # 篩選地區
        if filter_areas:
            area = r.get("鄉/市/鎮", "") or r.get("地區", "") or ""
            if area not in filter_areas:
                continue

        # 篩選經紀人
        if filter_agents:
            agent = r.get("經紀人", "")
            if agent not in filter_agents:
                continue

        results.append({
            "id":       r["id"],
            "案名":     r.get("案名", ""),
            "物件地址": r.get("物件地址", ""),
            "物件類別": r.get("物件類別", ""),
            "售價":     r.get("售價(萬)", ""),
            "經紀人":   r.get("經紀人", ""),
            "地區":     r.get("鄉/市/鎮", "") or r.get("地區", ""),
            "lat":      lat,
            "lng":      lng,
        })
    return jsonify({"items": results})


# ── 地圖：篩選選項 API ──

@app.route("/api/map/options", methods=["GET"])
def api_map_options():
    """回傳地圖篩選用的類別/地區/經紀人選項（只統計有座標的銷售中物件）。"""
    if not session.get("user_email"):
        return jsonify({"error": "請先登入"}), 401
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        import re as _re
        raw_categories = set()
        areas = set()
        agents = set()
        for doc in db.collection("company_properties").stream():
            r = doc.to_dict()
            if not _is_selling(r):
                continue
            coord = r.get("座標", "").strip()
            if not coord or len(coord.split(",")) != 2:
                continue
            if r.get("物件類別"):
                raw_categories.add(r["物件類別"])
            area = r.get("鄉/市/鎮", "") or r.get("地區", "")
            if area:
                areas.add(area)
            raw_ag = str(r.get("經紀人", ""))
            parts = _re.split(r'[/．、,，\s]+', raw_ag)
            for ag in parts:
                ag = ag.strip()
                if ag and 2 <= len(ag) <= 4 and not _re.search(r'\d', ag):
                    matched = False
                    for known in ACTIVE_AGENTS:
                        if known in ag and len(ag) > len(known):
                            agents.add(known)
                            matched = True
                    if not matched:
                        agents.add(ag)

        # 大類整理
        all_known_cats = {c for cats in CATEGORY_GROUPS.values() for c in cats}
        display_categories = set(CATEGORY_GROUPS.keys())
        has_other = any(c not in all_known_cats for c in raw_categories)
        if has_other:
            display_categories.add(_OTHER_GROUP)
        group_order = list(CATEGORY_GROUPS.keys())
        def cat_sort_key(c):
            if c in group_order: return (0, group_order.index(c))
            if c == _OTHER_GROUP: return (2, c)
            return (1, c)

        sorted_raw_areas = sorted(areas, key=_area_sort_key)
        area_options = [{"value": a, "label": AREA_DISPLAY.get(a, a)} for a in sorted_raw_areas]
        active_found   = [a for a in ACTIVE_AGENTS if a in agents]
        inactive_found = sorted(agents - set(ACTIVE_AGENTS))

        return jsonify({
            "categories": sorted(display_categories, key=cat_sort_key),
            "areas":      area_options,
            "agents":     {"active": active_found, "inactive": inactive_found}
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── 地圖情境書籤 API ──

@app.route("/api/map-presets", methods=["GET"])
def api_map_presets_list():
    """列出目前登入者的地圖篩選情境。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    try:
        docs = db.collection("map_presets").where("created_by", "==", email).stream()
        items = []
        for d in docs:
            row = d.to_dict()
            row["id"] = d.id
            items.append(row)
        items.sort(key=lambda x: x.get("created_at", ""))
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/map-presets", methods=["POST"])
def api_map_presets_create():
    """新增或覆蓋地圖篩選情境（依 name 去重，同名則更新）。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    try:
        data = request.get_json(force=True) or {}
        name = str(data.get("name", "")).strip()
        if not name:
            return jsonify({"error": "請填寫情境名稱"}), 400
        params = data.get("params", {})
        now = datetime.now(timezone.utc).isoformat()
        existing = list(db.collection("map_presets")
                        .where("created_by", "==", email)
                        .where("name", "==", name)
                        .stream())
        if existing:
            db.collection("map_presets").document(existing[0].id).update(
                {"params": params, "updated_at": now})
            return jsonify({"id": existing[0].id, "updated": True})
        else:
            doc_ref = db.collection("map_presets").add({
                "name":       name,
                "params":     params,
                "created_by": email,
                "created_at": now,
                "updated_at": now,
            })
            return jsonify({"id": doc_ref[1].id, "created": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/map-presets/<preset_id>", methods=["DELETE"])
def api_map_presets_delete(preset_id):
    """刪除地圖篩選情境（只能刪自己的）。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    try:
        doc_ref = db.collection("map_presets").document(preset_id)
        doc = doc_ref.get()
        if not doc.exists:
            return jsonify({"error": "找不到此情境"}), 404
        if doc.to_dict().get("created_by") != email:
            return jsonify({"error": "無權刪除他人情境"}), 403
        doc_ref.delete()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── 公司物件庫搜尋 API（Firestore company_properties 集合） ──

@app.route("/api/company-properties/search", methods=["GET"])
def api_company_properties_search():
    """搜尋公司物件庫，支援多條件篩選。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

    # ── 組織存取控制 ──
    # 必須是管理員、或屬於某個組織，才可使用公司物件庫
    is_admin = _is_admin(email)
    org_info = _get_org_for_user(email)
    org_id = org_info["org_id"] if org_info else None

    if not is_admin and not org_id:
        return jsonify({
            "error": "您尚未加入任何組織，請聯絡管理員將您加入公司組織後，才能使用公司物件庫。",
            "need_org": True
        }), 403

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    # 取得查詢參數（category/area/agent 支援多選，以逗號分隔）
    keyword    = request.args.get("keyword", "").strip()
    categories = [c for c in request.args.get("category", "").split(",") if c.strip()]
    areas      = [a for a in request.args.get("area", "").split(",") if a.strip()]
    price_min  = request.args.get("price_min", "").strip()
    price_max  = request.args.get("price_max", "").strip()
    status     = request.args.get("status", "").strip()  # "selling"/"sold"/"delisted"/""
    agents     = [a for a in request.args.get("agent", "").split(",") if a.strip()]
    sort_by    = request.args.get("sort", "serial_desc").strip()  # 排序方式
    page       = max(1, int(request.args.get("page", 1)))
    per_page   = min(500, max(1, int(request.args.get("per_page", 20))))
    # 是否只看「目前在起家對外網站上架」（= 銷售中 + 委託期內）
    on_home_start_only = request.args.get("on_home_start", "").strip() == "1"
    # 委託日過濾門檻（預設不過濾，前端傳「2013-01-01」才會啟用）
    # 邏輯保留，但預設關閉，使用者「先用一段時間再說」（2026-05）
    min_commit_date_raw = request.args.get("min_commit_date", "").strip()
    _mcd_m = re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$', min_commit_date_raw)
    min_commit_tuple = (int(_mcd_m.group(1)), int(_mcd_m.group(2)), int(_mcd_m.group(3))) if _mcd_m else None

    try:
        col = db.collection("company_properties")
        query = col

        # 展開大類 → 原始類別 set（支援多選）
        all_known_cats = {c for cats in CATEGORY_GROUPS.values() for c in cats}
        has_other_group = _OTHER_GROUP in categories
        # 展開所有選取的大類成原始類別
        expanded_cats = set()
        for cat in categories:
            if cat == _OTHER_GROUP:
                continue
            for c in _expand_category_group(cat):
                expanded_cats.add(c)

        # 「鄉/市/鎮」欄位名含斜線，Firestore 會誤判為路徑，
        # 地區過濾統一在 Python 端處理

        # 全量讀取
        docs = list(query.stream())
        results = [{"id": d.id, **d.to_dict()} for d in docs]

        # ── 組織資料隔離 ──
        # 有 org_id 的用戶只看自己組織的資料（或尚未標記 org_id 的舊資料）
        # 管理員且無 org_id 時看全部（初始設定期間的 fallback）
        if org_id:
            results = [r for r in results if r.get("org_id") == org_id or not r.get("org_id")]

        # Python 端：類別過濾（支援多選 + 「其他」群組）
        if categories:
            def _cat_match(r):
                rc = r.get("物件類別")
                # 選了「其他」且物件不屬於任何大類
                if has_other_group and rc not in all_known_cats:
                    return True
                # 展開的原始類別命中
                if expanded_cats and rc in expanded_cats:
                    return True
                return False
            results = [r for r in results if _cat_match(r)]

        # Python 端：地區過濾（單選或多選均在此處理，因欄位名含斜線不能用 Firestore query）
        if areas:
            results = [r for r in results if r.get("鄉/市/鎮") in set(areas)]

        # Python 端：關鍵字
        if keyword:
            kw = keyword.lower()
            results = [r for r in results if
                       kw in str(r.get("案名", "")).lower() or
                       kw in str(r.get("物件地址", "")).lower() or
                       kw in str(r.get("委託編號", "")).lower() or
                       kw in str(r.get("所有權人", "")).lower()]

        # Python 端：售價區間
        if price_min:
            try:
                pmin = float(price_min)
                results = [r for r in results if _parse_price(r.get("售價(萬)")) is not None
                           and _parse_price(r.get("售價(萬)")) >= pmin]
            except Exception:
                pass
        if price_max:
            try:
                pmax = float(price_max)
                results = [r for r in results if _parse_price(r.get("售價(萬)")) is not None
                           and _parse_price(r.get("售價(萬)")) <= pmax]
            except Exception:
                pass

        # Python 端：狀態判斷使用模組層級的 _is_selling()

        if status == "selling":
            results = [r for r in results if _is_selling(r)]
        elif status == "sold":
            results = [r for r in results if not _is_selling(r) and r.get("成交日期")]
        elif status == "delisted":
            results = [r for r in results if not _is_selling(r) and not r.get("成交日期")]

        # Python 端：經紀人多選（包含比對，應對多人合寫情況）
        if agents:
            def _agent_match(r):
                raw = str(r.get("經紀人", ""))
                return any(ag in raw for ag in agents)
            results = [r for r in results if _agent_match(r)]

        # Python 端：只看「目前在起家對外網站上架」
        # 條件 = 銷售中 + 委託期內（與 home-start /api/public/properties 篩選邏輯一致）
        if on_home_start_only:
            results = [r for r in results if _is_selling(r) and _is_within_delegation(r)]

        # ── 委託日過濾（民國 102/1/1 預設，使用者進公司後的記錄才看）──
        # 委託日 < 門檻 的物件不顯示（保留空白者，避免誤殺）
        filtered_old_count = 0
        if min_commit_tuple:
            kept_results = []
            for r in results:
                t = _parse_date_smart(r.get("委託日", ""))
                if t is None or t >= min_commit_tuple:
                    kept_results.append(r)
                else:
                    filtered_old_count += 1
            results = kept_results

        # ── 同物件多次委託歷史去重：同 hard_key 多筆 → 取「委託日最新」當代表
        # 舊版本壓進 _history 欄位（前端「📜 委託歷史」按鈕展開）
        # 沒 hard_key 的物件保持獨立（不會被誤合併）
        from collections import defaultdict as _dd
        hk_groups = _dd(list)
        no_hk_results = []
        for r in results:
            hks = _access_hard_keys(r)
            if not hks:
                no_hk_results.append(r)
                continue
            bldg = [k for k in hks if k[0] == "hard_bldg"]
            land = [k for k in hks if k[0] == "hard_land"]
            primary = bldg[0] if bldg else land[0]
            hk_groups[primary].append(r)
        deduped = list(no_hk_results)
        for k, items in hk_groups.items():
            if len(items) == 1:
                deduped.append(items[0])
                continue
            sorted_items = sorted(
                items,
                key=lambda r: _parse_date_for_compare(str(r.get("委託日", "") or "")),
                reverse=True
            )
            latest = sorted_items[0]
            history = []
            for old in sorted_items[1:]:
                history.append({
                    "委託日":       str(old.get("委託日", "") or ""),
                    "委託編號":     str(old.get("委託編號", "") or ""),
                    "售價(萬)":    old.get("售價(萬)"),
                    "經紀人":       str(old.get("經紀人", "") or ""),
                    "委託到期日":   str(old.get("委託到期日", "") or ""),
                    "銷售中":       _is_selling(old),
                    "成交日期":     str(old.get("成交日期", "") or ""),
                    "成交金額(萬)": old.get("成交金額(萬)"),
                    "資料序號":     old.get("資料序號"),
                    "id":           old.get("id"),
                })
            latest["_history"] = history
            deduped.append(latest)
        results = deduped

        # 後端排序（依前端傳入的 sort 參數）
        def _parse_expiry_key(r):
            """將委託到期日解析為可比較的字串 YYYY-MM-DD，無值給 '9999-99-99'（排最後）"""
            exp = str(r.get("委託到期日") or "").strip()
            if not exp:
                return "9999-99-99"
            import re as _re
            m = _re.match(r"(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日", exp)
            if m:
                yr = int(m.group(1)) + (1911 if int(m.group(1)) < 1000 else 0)
                return f"{yr:04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            m2 = _re.match(r"(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})", exp)
            if m2:
                return f"{int(m2.group(1)):04d}-{int(m2.group(2)):02d}-{int(m2.group(3)):02d}"
            return "9999-99-99"

        if sort_by == "price_asc":
            results.sort(key=lambda r: float(r.get("售價(萬)") or 0))
        elif sort_by == "price_desc":
            results.sort(key=lambda r: float(r.get("售價(萬)") or 0), reverse=True)
        elif sort_by == "date_desc":
            results.sort(key=lambda r: _parse_date_key(str(r.get("委託日") or r.get("完成日") or "")), reverse=True)
        elif sort_by == "date_asc":
            results.sort(key=lambda r: _parse_date_key(str(r.get("委託日") or r.get("完成日") or "")))
        elif sort_by == "expiry_asc":
            results.sort(key=_parse_expiry_key)
        elif sort_by == "expiry_desc":
            results.sort(key=_parse_expiry_key, reverse=True)
        elif sort_by == "serial_asc":
            results.sort(key=lambda r: int(r.get("資料序號", 0) or 0))
        else:  # serial_desc（預設）
            results.sort(key=lambda r: -int(r.get("資料序號", 0) or 0))

        total = len(results)
        start = (page - 1) * per_page
        page_data = results[start:start + per_page]

        # 列表只回傳卡片需要的欄位（減少傳輸量）
        card_fields = {
            "id", "案名", "物件地址", "物件類別", "售價(萬)",
            "建坪", "地坪", "經紀人", "銷售中", "成交日期", "委託到期日",
            "資料序號", "鄉/市/鎮", "已加星", "舊案名", "原售價(萬)", "所有權人",
            "段別", "地號",   # FOUNDI 土地查詢用
            "_history"        # 同物件多次委託歷史（前端展示「📜 委託歷史」用）
        }
        slim = [{k: r[k] for k in card_fields if k in r} for r in page_data]
        # 補上 id、統一「銷售中」布林、加上「目前在起家上架」旗標
        # （與 home-start /api/public/properties 篩選邏輯一致：銷售中 + 委託期內）
        for orig, s in zip(page_data, slim):
            s["id"] = orig["id"]
            s["銷售中"] = _is_selling(orig)
            s["on_home_start"] = _is_selling(orig) and _is_within_delegation(orig)

        return jsonify({
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
            "items": slim,
            "filtered_old_count": filtered_old_count,  # 被「委託日門檻」過濾掉的舊資料筆數
            "min_commit_date":    min_commit_date_raw if min_commit_tuple else "",
            # 給前端組「在起家上看」連結用（webhook URL 同時也是公開站台 URL）
            "home_start_url": (os.environ.get("HOME_START_URL") or "").strip().rstrip("/"),
        })

    except Exception as e:
        import logging
        logging.exception("company-properties search 失敗")
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════
# Word 物件總表 Snapshot — 解析 & 上傳
# ══════════════════════════════════════════════

def _parse_word_prices(file_bytes):
    """
    解析 .doc 二進位，呼叫 export_word_table.py 完整解析邏輯，
    回傳 {normalized案名: {案名, 委託號碼, 售價萬}} 供售價對比使用。
    """
    import subprocess, tempfile, os as _os, sys as _sys

    # 把檔案寫到暫存
    with tempfile.NamedTemporaryFile(suffix='.doc', delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        # 取得純文字（textutil on macOS / antiword on Linux）
        r = subprocess.run(["textutil", "-convert", "txt", "-stdout", tmp_path],
                           capture_output=True, timeout=60)
        if r.returncode != 0 or not r.stdout.strip():
            r = subprocess.run(["antiword", tmp_path],
                               capture_output=True, timeout=60)
        text = r.stdout.decode("utf-8", errors="replace")
    except Exception as e:
        return None, f"文字擷取失敗：{e}"
    finally:
        _os.unlink(tmp_path)

    if not text.strip():
        return None, "無法從 Word 檔案擷取文字（可能是 .docx 格式，請另存為 .doc）"

    # 動態載入 export_word_table.py 的解析函數
    try:
        _proj = "/Users/chenweiliang/Projects"
        if _proj not in _sys.path:
            _sys.path.insert(0, _proj)
        import importlib
        ewt = importlib.import_module("export_word_table")
    except ImportError:
        # Cloud Run 上沒有本地腳本，改用內建精簡解析
        ewt = None

    results = {}

    def _norm(s):
        s = re.sub(r'\s+', '', str(s))
        s = re.sub(r'(?<!\d)\d{5,6}(?!\d)', '', s)
        return s.strip()

    if ewt:
        # 使用完整解析器，精度最高
        try:
            all_entries = []
            all_entries += ewt.parse_condo_section(text)
            for st in ["住家", "別墅", "店住"]:
                all_entries += ewt.parse_house_section(text, st)
            all_entries += ewt.parse_farm_entries(text)
            all_entries += ewt.parse_build_entries(text)

            # 計算「今年/明年」到期日的輔助函數
            from datetime import date as _date
            _today = _date.today()

            def _expand_expiry(raw):
                """把 Word 的 '月/日' 格式轉為完整日期字串 (YYYY/M/D)"""
                if not raw or not raw.strip():
                    return ""
                m = re.match(r'^(\d{1,2})/(\d{1,2})$', raw.strip())
                if not m:
                    return ""
                mo, dy = int(m.group(1)), int(m.group(2))
                try:
                    # 若今年這個日期已過，到期日為明年
                    cand = _date(_today.year, mo, dy)
                    if cand < _today:
                        cand = _date(_today.year + 1, mo, dy)
                    return cand.strftime("%Y/%m/%d")
                except ValueError:
                    return ""

            for e in all_entries:
                name = e.get("案名","").strip()
                price = e.get("售價萬","")
                comm  = str(e.get("委託號碼","") or "").zfill(6) if e.get("委託號碼") else ""
                expiry_raw = e.get("到期日","").strip()
                if not name or not price:
                    continue
                try:
                    price_f = float(str(price).replace(",",""))
                except Exception:
                    continue
                key = _norm(name)
                if not key:
                    continue
                expiry_full = _expand_expiry(expiry_raw)
                existing = results.get(key)
                # 保留委託號碼較大（較新）的
                if not existing or comm > existing.get("委託號碼",""):
                    results[key] = {
                        "案名": name,
                        "委託號碼": comm,
                        "售價萬": price_f,
                        "委託到期日": expiry_full,   # 完整日期（如 2026/05/29）
                    }
        except Exception as ex:
            return None, f"解析失敗：{ex}"
    else:
        # Cloud Run 精簡版：逐行掃描案名 + 售價（準確度較低但可用）
        def _parse_p(s):
            s = str(s).strip()
            m = re.search(r'([\d,\.]+)\s*億\s*([\d,\.]*)\s*萬', s)
            if m:
                try: return float(m.group(1).replace(',',''))*10000 + (float(m.group(2).replace(',','')) if m.group(2) else 0)
                except Exception: pass
            m = re.search(r'([\d,\.]+)\s*萬', s)
            if m:
                try: return float(m.group(1).replace(',',''))
                except Exception: pass
            return None

        _SKIP = re.compile(r'^[\d,\.]+\s*(分|坪|萬|億)|網路沒上|不上網|到期|押金|租金|編號|地址|格局|現況|樓層|座向|完成日|業務')
        lines = text.split('\n')
        current_name, current_comm = "", ""
        for raw in lines:
            line = raw.strip()
            if not line: continue
            cm = re.search(r'(?<!\d)(\d{5,6})(?!\d)', line)
            if cm: current_comm = cm.group(1).zfill(6)
            p = _parse_p(line)
            if p and p > 50 and current_name and not _SKIP.search(current_name):
                key = _norm(current_name)
                if key:
                    existing = results.get(key)
                    if not existing or current_comm > existing.get("委託號碼",""):
                        results[key] = {"案名": current_name, "委託號碼": current_comm, "售價萬": p}
            elif re.search(r'[\u4e00-\u9fff]', line) and not re.search(r'萬', line):
                name_c = re.sub(r'(?<!\d)\d{5,6}(?!\d)','',line).strip()
                if 2 <= len(name_c) <= 20 and not _SKIP.search(name_c):
                    current_name = name_c

    return results, None


@app.route("/api/word-snapshot/upload", methods=["POST"])
def api_word_snapshot_upload():
    """上傳 .doc 物件總表，解析後存入 Firestore word_snapshot 集合。僅管理員可用。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email):
        return jsonify({"error": "僅管理員可上傳"}), 403

    if 'file' not in request.files:
        return jsonify({"error": "請選擇 .doc 檔案"}), 400
    f = request.files['file']
    if not f.filename.lower().endswith('.doc'):
        return jsonify({"error": "僅支援 .doc 格式"}), 400

    file_bytes = f.read()
    if len(file_bytes) < 1000:
        return jsonify({"error": "檔案太小，可能不是有效的 Word 文件"}), 400

    # 解析售價
    price_map, parse_err = _parse_word_prices(file_bytes)
    if parse_err:
        return jsonify({"error": "解析失敗：" + parse_err}), 500

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    # ── 存入 Firestore word_snapshot 快照 ──
    now_str = datetime.now(timezone.utc).isoformat()
    doc_ref = db.collection("word_snapshot").document("latest")
    doc_ref.set({
        "uploaded_at": now_str,
        "uploaded_by": email,
        "filename":    f.filename,
        "count":       len(price_map),
        "prices":      price_map,   # {normalized案名: {案名, 委託號碼, 售價萬, 委託到期日}}
    })

    # ── 比對 Firestore company_properties，回寫銷售中與委託到期日 ──
    # Word 總表上的物件全部是「銷售中」；不在 Word 上的不動（可能已下架或資料不同步）
    updated_count = 0
    try:
        col = db.collection("company_properties")
        all_docs = list(col.stream())  # 中文欄位名不能用 select()，全量讀取後 Python 端篩

        def _norm_name(s):
            """正規化案名：去空白、去委託號碼"""
            s = re.sub(r'\s+', '', str(s))
            s = re.sub(r'(?<!\d)\d{5,6}(?!\d)', '', s)
            return s.strip()

        for doc in all_docs:
            dd = doc.to_dict()
            key = _norm_name(dd.get("案名", ""))
            if not key:
                continue
            match = price_map.get(key)
            if not match:
                continue

            # 需要更新的欄位
            updates = {}

            # 1. 銷售中：Word 上有 → 標為 True
            if dd.get("銷售中") is not True:
                updates["銷售中"] = True

            # 2. 委託到期日：Word 解析有值且 Firestore 無值或不同，才更新
            expiry = match.get("委託到期日", "")
            if expiry and dd.get("委託到期日", "") != expiry:
                updates["委託到期日"] = expiry

            if updates:
                col.document(doc.id).update(updates)
                updated_count += 1

    except Exception as ex:
        # 回寫失敗不影響快照本身
        import logging
        logging.getLogger("word-upload").warning(f"回寫 Firestore 失敗：{ex}")

    return jsonify({
        "ok": True,
        "uploaded_at": now_str,
        "count": len(price_map),
        "updated_firestore": updated_count,
        "message": f"解析完成，共 {len(price_map)} 筆物件，已更新 {updated_count} 筆 Firestore 資料（銷售中 + 委託到期日）"
    })


@app.route("/api/word-snapshot/upload-csv", methods=["POST"])
def api_word_snapshot_upload_csv():
    """
    上傳 export_word_table.py 產出的 CSV 檔（公寓/房屋/農地/建地），
    解析後寫回 Firestore：銷售中=True、委託到期日、售價萬。
    僅管理員可用。
    """
    import csv as _csv
    import io as _io
    import logging
    from datetime import date as _date

    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email):
        return jsonify({"error": "僅管理員可上傳"}), 403

    if 'file' not in request.files:
        return jsonify({"error": "請選擇 CSV 檔案"}), 400
    f = request.files['file']
    if not f.filename.lower().endswith('.csv'):
        return jsonify({"error": "僅支援 .csv 格式"}), 400

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    # 讀 CSV（去掉 BOM）
    raw = f.read().decode('utf-8-sig')
    reader = _csv.DictReader(_io.StringIO(raw))
    rows = list(reader)
    if not rows:
        return jsonify({"error": "CSV 內容為空"}), 400

    today = _date.today()

    def _parse_expiry(raw_str):
        """把各種到期日格式轉為 YYYY/MM/DD 字串"""
        s = str(raw_str).strip()
        if not s:
            return ""
        # 民國年格式：115年6月30日 / 109年3月31日
        m = re.match(r'^(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日$', s)
        if m:
            try:
                yr = int(m.group(1)) + 1911
                return f"{yr}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"
            except Exception:
                return ""
        # 短格式：月/日（如 12/16、7/31）
        m = re.match(r'^(\d{1,2})/(\d{1,2})$', s)
        if m:
            try:
                mo, dy = int(m.group(1)), int(m.group(2))
                cand = _date(today.year, mo, dy)
                if cand < today:
                    cand = _date(today.year + 1, mo, dy)
                return cand.strftime("%Y/%m/%d")
            except Exception:
                return ""
        return ""

    def _norm_name(s):
        """正規化案名：去空白、去委託號碼"""
        s = re.sub(r'\s+', '', str(s))
        s = re.sub(r'(?<!\d)\d{5,6}(?!\d)', '', s)
        return s.strip()

    def _parse_num(s):
        """解析數字，失敗回傳 None"""
        try:
            return float(str(s).replace(',', '').strip())
        except Exception:
            return None

    def _similar(a, b, tol=0.10):
        """兩個數字是否在容許誤差（預設 10%）內相近"""
        if a is None or b is None:
            return None  # 無法判斷
        if a == 0 and b == 0:
            return True
        if a == 0 or b == 0:
            return False
        return abs(a - b) / max(abs(a), abs(b)) <= tol

    # 建立兩個索引表：
    # csv_by_name: {正規化案名 → [資料列表]}（同名多筆，交由後續細比對）
    # csv_by_comm: {委託號碼 → 資料}（最精確，直接配對）
    csv_by_name = {}  # {norm_name: [payload, ...]}
    csv_by_comm = {}  # {委託號碼: payload}
    FEN_TO_PING = 293.4  # 1台分 = 293.4坪

    for row in rows:
        name = str(row.get('案名', '')).strip()
        if not name:
            continue
        comm = str(row.get('委託號碼', '') or '').strip()
        comm = comm.zfill(6) if comm.strip('0') else ''

        price  = _parse_num(row.get('售價萬', ''))
        expiry = _parse_expiry(row.get('到期日', ''))

        # 面積：農地/建地用「面積坪」，房屋用「地坪」，公寓用「室內坪」或「建坪」
        # CSV 有「面積坪」→ 農地/建地；有「地坪」→ 房屋；有「室內坪」→ 公寓
        area_csv = (_parse_num(row.get('面積坪'))
                    or _parse_num(row.get('地坪'))
                    or _parse_num(row.get('室內坪'))
                    or _parse_num(row.get('建坪')))

        key = _norm_name(name)
        if not key:
            continue

        payload = {
            '案名':      name,
            '委託號碼':  comm,
            '售價萬':    price,
            '面積坪':    area_csv,
            '委託到期日': expiry,
            '經紀人':    str(row.get('經紀人', '') or '').strip(),  # 經紀人：用於同名物件的區分
        }
        # 同名可能有多筆（不同委託），全部存進 list
        csv_by_name.setdefault(key, []).append(payload)
        if comm and comm != '000000':
            csv_by_comm[comm] = payload

    # 讀取 Firestore 並比對更新
    col = db.collection("company_properties")
    all_docs = list(col.stream())  # 中文欄位名不能用 select()，全量讀取後 Python 端篩

    updated = 0
    skipped = 0
    skipped_ambiguous = 0
    for doc in all_docs:
        dd = doc.to_dict()
        db_name = dd.get("案名", "")
        db_comm = str(dd.get("委託編號", "") or "").strip().zfill(6) if dd.get("委託編號") else ""
        db_seq  = int(dd.get("資料序號", 0) or 0)

        # Firestore 面積：農地/建地→地坪，房屋→地坪，公寓→室內坪/建坪
        db_area = (_parse_num(dd.get("地坪"))
                   or _parse_num(dd.get("室內坪"))
                   or _parse_num(dd.get("建坪")))
        db_price = _parse_num(dd.get("售價(萬)"))

        match = None
        name_changed = False
        match_by_comm = False

        # ── Step 1：委託號碼精確比對（最可靠）──
        if db_comm and db_comm != '000000':
            comm_match = csv_by_comm.get(db_comm)
            if comm_match:
                match = comm_match
                match_by_comm = True
                csv_name = match.get('案名', '')
                if csv_name and _norm_name(csv_name) != _norm_name(db_name):
                    name_changed = True

        # ── Step 2：案名比對 + 面積/售價輔助篩選 ──
        if not match:
            candidates = csv_by_name.get(_norm_name(db_name), [])
            if candidates:
                # 若只有一筆同名，直接用（但先確認委託號碼不衝突）
                best = None
                best_score = -1
                db_agent = str(dd.get("經紀人", "") or "").strip()  # Firestore 的經紀人
                for cand in candidates:
                    csv_comm  = cand.get('委託號碼', '')
                    csv_agent = str(cand.get('經紀人', '') or '').strip()

                    # 委託號碼都有值且不一樣 → 明確是不同物件，跳過
                    if (csv_comm and csv_comm != '000000'
                            and db_comm and db_comm != '000000'
                            and csv_comm != db_comm):
                        continue

                    # 計算相似度分數（越高越好）
                    score = 0

                    # ── 經紀人比對（最重要，解決同名不同人的問題）──
                    if db_agent and csv_agent:
                        if db_agent == csv_agent:
                            score += 5   # 經紀人完全吻合，強力加分
                        else:
                            score -= 8   # 經紀人明確不同，強力扣分（不應配對）

                    csv_price = cand.get('售價萬')
                    csv_area  = cand.get('面積坪')
                    price_sim = _similar(db_price, csv_price, tol=0.05)  # 售價 5% 容差
                    area_sim  = _similar(db_area,  csv_area,  tol=0.10)  # 面積 10% 容差

                    if price_sim is True:  score += 3   # 售價吻合加高分
                    if price_sim is False: score -= 5   # 售價明顯不同扣大分
                    if area_sim  is True:  score += 2   # 面積吻合加分
                    if area_sim  is False: score -= 3   # 面積明顯不同扣分
                    # 有委託到期日的優先（比舊資料更可能是現役）
                    if cand.get('委託到期日'): score += 1

                    if score > best_score:
                        best_score = score
                        best = cand

                if best is not None and best_score >= 0:
                    match = best
                elif best is not None and best_score < 0:
                    # 找到候選但特徵（經紀人/售價/面積）明顯不符 → 視為不同物件，跳過
                    skipped_ambiguous += 1
                    logging.getLogger("csv-upload").info(
                        f"[同名但特徵不符跳過] {db_name} | seq={db_seq}"
                        f" | FS經紀人={db_agent} CSV經紀人={best.get('經紀人')}"
                        f" | FS售價={db_price} 面積={db_area}"
                        f" | CSV售價={best.get('售價萬')} 面積={best.get('面積坪')}"
                    )
                    continue

        if not match:
            skipped += 1
            continue

        updates = {}
        # 1. 銷售中 → 標為布林 True（確保格式一致）
        if dd.get("銷售中") is not True:
            updates["銷售中"] = True
        # 2. 委託到期日 → 有值才更新
        expiry = match.get("委託到期日", "")
        if expiry and dd.get("委託到期日", "") != expiry:
            updates["委託到期日"] = expiry
        # 3. 售價 → CSV 有值且與 Firestore 不同才更新
        price = match.get("售價萬")
        if price is not None and dd.get("售價(萬)") != price:
            updates["售價(萬)"] = price
        # 4. 案名改動 → 舊案名存入「舊案名」欄（供前端顯示「原：xxx」備註）
        if name_changed:
            new_csv_name = match.get('案名', '')
            # 只有 Firestore 還沒記錄此「舊案名」時才更新，避免覆蓋更早的記錄
            if dd.get("舊案名", "") != db_name:
                updates["舊案名"] = db_name
            # 同步更新案名為 CSV 上的新案名
            if new_csv_name and dd.get("案名") != new_csv_name:
                updates["案名"] = new_csv_name

        if updates:
            col.document(doc.id).update(updates)
            updated += 1

    renamed_count = sum(1 for d in all_docs
                        if _norm_name(d.to_dict().get("案名","")) not in csv_by_name
                        and str(d.to_dict().get("委託編號","") or "").zfill(6) in csv_by_comm)

    return jsonify({
        "ok": True,
        "csv_rows": len(rows),
        "csv_matched": len(csv_by_name),
        "updated_firestore": updated,
        "skipped_ambiguous": skipped_ambiguous,
        "message": f"CSV {len(rows)} 筆 → 比對 {len(csv_by_name)} 筆 → 更新 Firestore {updated} 筆"
                   + (f"（{skipped_ambiguous} 筆同名不同委託，已跳過）" if skipped_ambiguous else "")
    })


@app.route("/api/word-review/analyze", methods=["POST"])
def api_word_review_analyze():
    """
    分析 export_word_table.py 產出的 CSV 與 Firestore 的配對結果，
    回傳高信心/中信心/衝突/未配對分組，但不寫入 Firestore。僅管理員。
    """
    import csv as _csv
    import io as _io
    from datetime import date as _date

    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email):
        return jsonify({"error": "僅管理員可使用"}), 403

    if 'file' not in request.files:
        return jsonify({"error": "請選擇 CSV 檔案"}), 400
    f = request.files['file']
    if not f.filename.lower().endswith('.csv'):
        return jsonify({"error": "僅支援 .csv 格式"}), 400

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    raw = f.read().decode('utf-8-sig')
    reader = _csv.DictReader(_io.StringIO(raw))
    rows = list(reader)
    if not rows:
        return jsonify({"error": "CSV 內容為空"}), 400

    today = _date.today()

    def _pe(s):
        """解析各種到期日格式 → YYYY/MM/DD"""
        s = str(s).strip()
        if not s:
            return ""
        m = re.match(r'^(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日$', s)
        if m:
            try:
                return f"{int(m.group(1))+1911}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"
            except Exception:
                return ""
        m = re.match(r'^(\d{1,2})/(\d{1,2})$', s)
        if m:
            try:
                mo, dy = int(m.group(1)), int(m.group(2))
                c = _date(today.year, mo, dy)
                if c < today:
                    c = _date(today.year + 1, mo, dy)
                return c.strftime("%Y/%m/%d")
            except Exception:
                return ""
        return ""

    def _nn(s):
        """正規化案名：去空白、去委託號碼"""
        s = re.sub(r'\s+', '', str(s))
        return re.sub(r'(?<!\d)\d{5,6}(?!\d)', '', s).strip()

    def _pn(s):
        """解析數字"""
        try:
            return float(str(s).replace(',', '').strip())
        except Exception:
            return None

    def _sm(a, b, tol=0.10):
        """兩數字是否在容差內相近"""
        if a is None or b is None:
            return None
        if a == 0 and b == 0:
            return True
        if a == 0 or b == 0:
            return False
        return abs(a - b) / max(abs(a), abs(b)) <= tol

    def _agent_score(ag_a, ag_b):
        """比較經紀人，支援空格或頓號分隔的多位承攬人（集合比對，順序無關）"""
        if not ag_a or not ag_b:
            return 0
        import re as _re
        def _split(s):
            return set(_re.split(r'[\s、,，]+', s.strip()))
        set_a = _split(ag_a)
        set_b = _split(ag_b)
        if set_a == set_b:  return 5   # 完全相同（含多人同時承攬）
        if set_a & set_b:   return 3   # 有交集（共同承攬部分相符）
        return -8                       # 完全不同人

    # 建立 CSV 索引
    csv_by_name, csv_by_comm, csv_by_seq, csv_by_addr = {}, {}, {}, {}
    for row in rows:
        name = str(row.get('案名', '')).strip()
        if not name:
            continue
        comm = str(row.get('委託號碼', '') or '').strip()
        comm = comm.zfill(6) if comm.strip('0') else ''
        price  = _pn(row.get('售價萬', ''))
        expiry = _pe(row.get('到期日', ''))
        area   = (_pn(row.get('面積坪')) or _pn(row.get('地坪'))
                  or _pn(row.get('室內坪')) or _pn(row.get('建坪')))
        addr   = re.sub(r'\s+', '', str(row.get('物件地址', '') or ''))
        key = _nn(name)
        if not key:
            continue
        p = {'案名': name, '委託號碼': comm, '售價萬': price,
             '面積坪': area, '委託到期日': expiry,
             '經紀人': str(row.get('經紀人', '') or '').strip(),
             '物件地址': addr}
        csv_by_name.setdefault(key, []).append(p)
        if comm and comm != '000000':
            csv_by_comm[comm] = p
        # 資料序號直接索引（export_word_table.py 已對應 Firestore 序號）
        seq = str(row.get('資料序號', '') or '').strip()
        if seq and seq.isdigit():
            csv_by_seq[seq] = p
        # 物件地址索引（公寓/房屋類精確命中）
        if addr and len(addr) >= 6:
            csv_by_addr.setdefault(addr, []).append(p)

    col  = db.collection("company_properties")
    # 委託日過濾（民國 102/1/1）跟 ACCESS 比對 / 同步一致 — 跳過舊物件加速比對
    MIN_COMMIT = (2013, 1, 1)
    docs = []
    review_filtered_old = 0
    for d in col.stream():
        rd = d.to_dict() or {}
        t = _parse_date_smart(rd.get("委託日", ""))
        if t is not None and t < MIN_COMMIT:
            review_filtered_old += 1
            continue
        docs.append(d)

    high, medium, conflict = [], [], []
    matched_comms = set()
    matched_names = set()  # 已配對的 CSV 正規案名

    for doc in docs:
        dd  = doc.to_dict()
        dbn = dd.get("案名", "")
        dbc = str(dd.get("委託編號", "") or "").strip().zfill(6) if dd.get("委託編號") else ""
        dbs = str(int(dd.get("資料序號", 0) or 0))  # Firestore 資料序號
        dba = (_pn(dd.get("地坪")) or _pn(dd.get("室內坪")) or _pn(dd.get("建坪")))
        dbp = _pn(dd.get("售價(萬)"))
        dbe = dd.get("委託到期日", "")
        dbg = str(dd.get("經紀人", "") or "").strip()
        dbaddr = re.sub(r'\s+', '', str(dd.get('物件地址', '') or ''))

        match, match_by, score, name_changed = None, "", 0, False

        # Step 0：資料序號精確命中（最優先，export_word_table.py 已對應好）
        if dbs and dbs != '0':
            cm = csv_by_seq.get(dbs)
            if cm:
                match = cm
                match_by = "資料序號"
                score = 10
                if cm.get('案名') and _nn(cm['案名']) != _nn(dbn):
                    name_changed = True

        # Step 1：委託號碼精確比對
        if not match and dbc and dbc != '000000':
            cm = csv_by_comm.get(dbc)
            if cm:
                match = cm
                match_by = "委託號碼"
                score = 10
                if cm.get('案名') and _nn(cm['案名']) != _nn(dbn):
                    name_changed = True
                matched_comms.add(dbc)

        # Step 1.5：物件地址精確命中（公寓/房屋類，地址是硬資料）
        if not match and dbaddr and len(dbaddr) >= 6:
            addr_cands = csv_by_addr.get(dbaddr, [])
            if addr_cands:
                match = addr_cands[0]
                match_by = "地址比對"
                score = 8

        # Step 2：案名 + 特徵評分比對
        if not match:
            candidates = csv_by_name.get(_nn(dbn), [])
            best, best_score = None, -999
            for cand in candidates:
                cc = cand.get('委託號碼', '')
                cg = str(cand.get('經紀人', '') or '').strip()
                # 委託號碼都有值且不同 → 明確是不同物件
                if (cc and cc != '000000' and dbc and dbc != '000000' and cc != dbc):
                    continue
                s = _agent_score(dbg, cg)  # 經紀人集合比對（支援多位承攬人）
                ps  = _sm(dbp, cand.get('售價萬'), 0.05)
                as_ = _sm(dba, cand.get('面積坪'), 0.10)
                if ps  is True:  s += 3
                if ps  is False: s -= 5
                if as_ is True:  s += 2
                if as_ is False: s -= 3
                if cand.get('委託到期日'): s += 1
                if s > best_score:
                    best_score = s
                    best = cand
            if best is not None:
                match = best
                score = best_score
                match_by = "案名比對"
                matched_names.add(_nn(best.get('案名', '')))

        if not match:
            continue

        item = {
            "doc_id":    doc.id,
            "db_name":   dbn,
            "db_seq":    dbs,
            "db_price":  dbp,
            "db_expiry": dbe,
            "db_agent":  dbg,
            "csv_name":   match.get('案名', ''),
            "csv_price":  match.get('售價萬'),
            "csv_expiry": match.get('委託到期日', ''),
            "csv_agent":  match.get('經紀人', ''),
            "csv_comm":   match.get('委託號碼', ''),
            "match_by":   match_by,
            "score":      score,
            "name_changed": name_changed,
        }
        # 信心分組：委託號碼/序號命中或高評分 → 高信心；但地址明顯不符則降中信心
        addr_mismatch = False
        if match_by in ("委託號碼", "資料序號", "物件地址"):
            da = item.get("db_addr", ""); ca = item.get("csv_addr", "")
            if da and ca and da != ca and ca not in da and da not in ca:
                addr_mismatch = True
                item["match_by"] = match_by + "（地址不符，請確認）"
        if (match_by in ("委託號碼", "資料序號", "物件地址") and not addr_mismatch) or score >= 3:
            high.append(item)
        elif score >= 0:
            medium.append(item)
        else:
            item["conflict_reason"] = f"同名但特徵衝突（分數 {score}，可能是不同物件或助理打錯）"
            conflict.append(item)

    # 找 CSV 裡找不到 Firestore 對應的物件（理論上不應存在，但打錯字時可能發生）
    unmatched = []
    for key, payloads in csv_by_name.items():
        for p in payloads:
            cc = p.get('委託號碼', '')
            if cc and cc in matched_comms:
                continue
            if _nn(p.get('案名', '')) in matched_names:
                continue
            unmatched.append({
                "csv_name":   p.get('案名', ''),
                "csv_price":  p.get('售價萬'),
                "csv_expiry": p.get('委託到期日', ''),
                "csv_agent":  p.get('經紀人', ''),
                "csv_comm":   cc,
                "reason": "Firestore 中找不到對應物件（可能案名差異大或助理打錯）",
            })

    return jsonify({
        "ok":       True,
        "csv_rows": len(rows),
        "high":     high,
        "medium":   medium,
        "conflict": conflict,
        "unmatched": unmatched,
    })


@app.route("/api/word-review/apply", methods=["POST"])
def api_word_review_apply():
    """
    套用使用者在審查介面確認的配對結果，寫入 Firestore。僅管理員。
    Body: {"items": [{"doc_id": "...", "price": ..., "expiry": "...",
                      "name_changed": bool, "old_name": "...", "new_name": "..."}]}
    """
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email):
        return jsonify({"error": "僅管理員可使用"}), 403

    data  = request.json or {}
    items = data.get("items", [])
    if not items:
        return jsonify({"error": "沒有要套用的項目"}), 400

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    col = db.collection("company_properties")
    updated = 0
    selling_writeback = {}  # {資料序號: True/False}，用於回寫 Sheets
    for it in items:
        did = it.get("doc_id")
        if not did:
            continue
        upd = {"銷售中": True}  # 確認在總表上 = 銷售中
        if it.get("expiry"):
            upd["委託到期日"] = it["expiry"]
        if it.get("price") is not None:
            # 售價有異動時：保留原售價供物件卡片顯示備註
            old_p = it.get("old_price")
            if old_p is not None and old_p != it["price"]:
                upd["原售價(萬)"] = old_p
            upd["售價(萬)"] = it["price"]
        if it.get("name_changed") and it.get("old_name") and it.get("new_name"):
            upd["舊案名"] = it["old_name"]   # 保留原案名供物件卡片顯示備註
            upd["案名"]  = it["new_name"]
        try:
            col.document(did).update(upd)
            updated += 1
            # 收集有資料序號的更新，供回寫 Sheets 用
            if did and did.isdigit():
                selling_writeback[did] = True  # apply-word-match 確認的都是銷售中
        except Exception:
            pass

    # 非同步回寫 Sheets「銷售中」欄（只改這一欄，不動其他欄位）
    if selling_writeback:
        import threading
        t = threading.Thread(
            target=_sheets_write_selling_status,
            args=(selling_writeback,),
            daemon=True
        )
        t.start()

    return jsonify({"ok": True, "updated": updated,
                    "message": f"已更新 {updated} 筆物件（銷售中、售價、到期日）"})


# ── AI（Gemini）重新配對問題組 ─────────────────────────────────────────────

@app.route("/api/word-review/ai-match", methods=["POST"])
def api_word_review_ai_match():
    """
    用 Gemini 對問題組（conflict + unmatched）重新配對。
    Input: { items: [{csv_name, csv_price, csv_agent, csv_comm, csv_expiry, ...}] }
    Output: { ok, results: [{idx, matched_doc_id, matched_db_name, matched_db_addr, confidence, reason}] }
    每筆 item 取候選物件 → 丟 Gemini → 拿配對建議。前端展示後由人工確認。
    """
    import difflib
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403
    if not _GEMINI_OK or not _genai:
        return jsonify({"error": "未設定 GOOGLE_API_KEY，無法使用 AI 配對"}), 503

    data = request.get_json(silent=True) or {}
    items = data.get("items", []) or []
    if not items:
        return jsonify({"ok": True, "results": []})
    if len(items) > 100:
        return jsonify({"error": "一次最多處理 100 筆，請分批"}), 400

    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503

    # 載入所有銷售中物件（一次，避免 N 次查詢）
    all_props = []
    for d in db.collection("company_properties").stream():
        rd = d.to_dict()
        if not _is_selling(rd):
            continue
        all_props.append({
            "doc_id":   d.id,
            "案名":     str(rd.get("案名", "") or ""),
            "物件地址": str(rd.get("物件地址", "") or ""),
            "物件類別": str(rd.get("物件類別", "") or ""),
            "經紀人":   str(rd.get("經紀人", "") or ""),
            "售價":     rd.get("售價(萬)") if rd.get("售價(萬)") is not None else rd.get("售價萬", ""),
            "委託編號": str(rd.get("委託編號", "") or ""),
            # 硬資料：面積（給 AI 用來辨別「不同物件」）— 不一致 > 20% 就一定不同
            "地坪":     str(rd.get("地坪", "") or ""),
            "建坪":     str(rd.get("建坪", "") or ""),
            "室內坪":   str(rd.get("室內坪", "") or ""),
        })

    def _split_agents(s):
        return set(x.strip() for x in re.split(r"[,/／、，\s]+", str(s or "")) if x.strip())

    def _name_similarity(a, b):
        """案名相似度 0-1（去空白後比較）"""
        a = re.sub(r"\s+", "", str(a or ""))
        b = re.sub(r"\s+", "", str(b or ""))
        if not a or not b: return 0.0
        return difflib.SequenceMatcher(None, a, b).ratio()

    results = []
    client = _genai.Client(api_key=_GEMINI_KEY)

    for idx, item in enumerate(items):
        csv_name  = str(item.get("csv_name", "") or "")
        csv_price = item.get("csv_price", "")
        csv_agent = item.get("csv_agent", "")
        csv_comm  = str(item.get("csv_comm", "") or "")
        csv_expiry= item.get("csv_expiry", "")
        # Word 端面積（給 AI 比對用）
        csv_land     = item.get("csv_land", "")
        csv_build    = item.get("csv_build", "")
        csv_interior = item.get("csv_interior", "")

        # 候選篩選（兩階段）：
        # 1. 經紀人有交集 → 加入候選池
        # 2. 若上一步 < 3 筆，補入案名相似度 > 0.4 的物件
        item_agents = _split_agents(csv_agent)
        cands = []
        for p in all_props:
            db_agents = _split_agents(p.get("經紀人", ""))
            if item_agents and db_agents and (item_agents & db_agents):
                cands.append((p, _name_similarity(csv_name, p["案名"])))
        if len(cands) < 3:
            for p in all_props:
                if p in [c[0] for c in cands]: continue
                sim = _name_similarity(csv_name, p["案名"])
                if sim >= 0.4:
                    cands.append((p, sim))
        # 依案名相似度排序，取前 10
        cands.sort(key=lambda x: x[1], reverse=True)
        cands = cands[:10]

        if not cands:
            results.append({
                "idx": idx,
                "matched_doc_id": None,
                "confidence": 0.0,
                "reason": "找不到任何候選物件（經紀人不重疊且案名差異過大）",
                "candidates_count": 0,
            })
            continue

        # 建 prompt（含面積：地坪/建坪/室內坪 — 面積差 > 20% 一定不是同物件）
        cand_list = "\n".join([
            f"  [{i+1}] doc_id={c[0]['doc_id']} | 案名：{c[0]['案名']} | 地址：{c[0]['物件地址']} | 類別：{c[0]['物件類別']} | 經紀人：{c[0]['經紀人']} | 售價：{c[0]['售價']}萬 | 委編：{c[0]['委託編號']} | 地坪：{c[0].get('地坪','')} | 建坪：{c[0].get('建坪','')} | 室內坪：{c[0].get('室內坪','')}"
            for i, c in enumerate(cands)
        ])
        prompt = (
            "你是房仲資料比對助理。請判斷以下「Word 物件」是否對應到「候選物件清單」中的某一筆。\n\n"
            "【Word 物件】\n"
            f"  案名：{csv_name}\n"
            f"  售價：{csv_price}萬\n"
            f"  經紀人：{csv_agent}\n"
            f"  委託編號：{csv_comm}\n"
            f"  委託到期日：{csv_expiry}\n"
            f"  地坪：{csv_land}　建坪：{csv_build}　室內坪：{csv_interior}\n\n"
            "【候選物件清單】\n"
            f"{cand_list}\n\n"
            "請輸出 JSON（嚴格符合此格式，不要多餘文字）：\n"
            '{ "matched_doc_id": "若找到對應的 doc_id 字串；找不到則填 null", '
            '"confidence": 0.0 到 1.0 的數字, '
            '"reason": "30字內的判斷理由（中文）" }\n\n'
            "判斷原則（按重要性排序）：\n"
            "★ 面積是不動產的硬資料 — 同一物件實體面積不會變。地坪/建坪/室內坪若兩邊都有值，差距 > 20% → 一定是不同物件 → 不配對 null。\n"
            "1. 面積吻合（差 ≤ 5%）+ 案名相似 + 經紀人有交集 + 售價接近 → 高信心 0.8-1.0\n"
            "2. 面積無資料可驗證、其他軟資料相似 → 中信心 0.4-0.7\n"
            "3. 面積差距 > 20%，或售價差很多、案名語意完全不同 → 不配對 null + 低信心\n"
            "4. 寧可不配對（null）也不要錯配 — 案名相似但面積不符是「同段別不同戶」常見情況。"
        )

        try:
            resp = client.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt,
                config=_genai.types.GenerateContentConfig(response_mime_type="application/json"),
            )
            parsed = json.loads(resp.text)
            matched_id = parsed.get("matched_doc_id")
            matched_p = next((c[0] for c in cands if c[0]["doc_id"] == matched_id), None) if matched_id else None
            results.append({
                "idx": idx,
                "matched_doc_id": matched_id if matched_p else None,
                "matched_db_name": matched_p["案名"] if matched_p else "",
                "matched_db_addr": matched_p["物件地址"] if matched_p else "",
                "matched_db_agent": matched_p["經紀人"] if matched_p else "",
                "matched_db_price": matched_p["售價"] if matched_p else "",
                "confidence": float(parsed.get("confidence", 0) or 0),
                "reason": str(parsed.get("reason", "") or "")[:60],
                "candidates_count": len(cands),
            })
        except Exception as e:
            results.append({
                "idx": idx,
                "matched_doc_id": None,
                "confidence": 0.0,
                "reason": f"AI 失敗：{str(e)[:50]}",
                "candidates_count": len(cands),
            })

    return jsonify({"ok": True, "results": results, "model": "gemini-2.0-flash"})


# ── 強行配對記憶 API ─────────────────────────────────────────────────────────

@app.route("/api/word-match-memory", methods=["GET"])
def api_word_match_memory_list():
    """取得所有強行配對記憶。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    docs = db.collection("word_match_memory").order_by("created_at").stream()
    items = []
    for d in docs:
        rec = d.to_dict()
        rec["_id"] = d.id
        items.append(rec)
    return jsonify({"ok": True, "items": items})


@app.route("/api/word-match-memory", methods=["POST"])
def api_word_match_memory_add():
    """新增一筆強行配對記憶。
    Body: {word_name, word_comm, db_seq, db_doc_id, memo}
    """
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    data = request.json or {}
    word_comm = str(data.get("word_comm", "") or "").strip()
    word_name = str(data.get("word_name", "") or "").strip()
    db_seq    = str(data.get("db_seq",   "") or "").strip()
    db_doc_id = str(data.get("db_doc_id","") or "").strip()
    if not (word_name and db_seq):
        return jsonify({"error": "缺少 word_name 或 db_seq"}), 400
    from datetime import datetime as _dt
    rec = {
        "word_name": word_name,
        "word_comm": word_comm,
        "db_seq":    db_seq,
        "db_doc_id": db_doc_id,
        "memo":      str(data.get("memo", "") or ""),
        "created_by": email,
        "created_at": _dt.utcnow().isoformat(),
    }
    doc_ref = db.collection("word_match_memory").document()
    doc_ref.set(rec)
    rec["_id"] = doc_ref.id
    return jsonify({"ok": True, "item": rec})


@app.route("/api/word-match-memory/<mem_id>", methods=["DELETE"])
def api_word_match_memory_delete(mem_id):
    """刪除一筆強行配對記憶。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可使用"}), 403
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    db.collection("word_match_memory").document(mem_id).delete()
    return jsonify({"ok": True})




@app.route("/api/word-review/upload-doc", methods=["POST"])
def api_word_review_upload_doc():
    """
    直接上傳 Word .doc 物件總表，雲端解析後與 Firestore 比對，
    回傳高信心/中信心/衝突/未配對分組，但不寫入 Firestore。僅管理員。
    """
    import tempfile, os
    from datetime import date as _date

    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email):
        return jsonify({"error": "僅管理員可使用"}), 403

    if 'file' not in request.files:
        return jsonify({"error": "請選擇 .doc 或 .docx 檔案"}), 400
    f = request.files['file']
    fname_lower = f.filename.lower()
    if not (fname_lower.endswith('.doc') or fname_lower.endswith('.docx')):
        return jsonify({"error": "僅支援 .doc / .docx 格式"}), 400

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    # 儲存到暫存檔（antiword / python-docx 需要實體路徑）
    suffix = '.docx' if fname_lower.endswith('.docx') else '.doc'
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    try:
        f.save(tmp.name)
        tmp.close()
        try:
            import word_parser
            parsed = word_parser.parse_doc(tmp.name)
        except RuntimeError as e:
            return jsonify({"error": f"Word 解析失敗：{e}"}), 500
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass

    doc_date = parsed.get("doc_date")

    # 合併四類物件為統一清單（格式與 CSV analyze 相同）
    all_entries = (parsed.get("condo") or []) + (parsed.get("house") or []) + \
                  (parsed.get("farm") or []) + (parsed.get("build") or [])

    today = _date.today()

    def _pe(s):
        s = str(s).strip()
        if not s:
            return ""
        m = re.match(r'^(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日$', s)
        if m:
            try:
                return f"{int(m.group(1))+1911}/{int(m.group(2)):02d}/{int(m.group(3)):02d}"
            except Exception:
                return ""
        m = re.match(r'^(\d{1,2})/(\d{1,2})$', s)
        if m:
            try:
                mo, dy = int(m.group(1)), int(m.group(2))
                c = _date(today.year, mo, dy)
                if c < today:
                    c = _date(today.year + 1, mo, dy)
                return c.strftime("%Y/%m/%d")
            except Exception:
                return ""
        return ""

    def _nn(s):
        s = re.sub(r'\s+', '', str(s))
        s = re.sub(r'[（(][^）)]*[）)]', '', s)  # 去除括號附註如(二筆)，避免同物件因標注不同而無法比對
        return re.sub(r'(?<!\d)\d{5,6}(?!\d)', '', s).strip()

    def _ca(s):
        """去除地址末尾附帶的民國年或委託號碼（Word解析時可能殘留）"""
        s = str(s)
        s = re.sub(r'\s+1\d\d\s*$', '', s)       # 末尾空格+民國年
        s = re.sub(r'\s+\d{5,6}\s*$', '', s)     # 末尾空格+委託號碼
        s = re.sub(r'(號)(1\d\d)\s*$', r'\1', s)  # 末尾無空格民國年
        s = re.sub(r'(號)(\d{5,6})\s*$', r'\1', s)  # 末尾無空格委託號
        return s.strip()

    def _pn(s):
        try:
            return float(str(s).replace(',', '').strip())
        except Exception:
            return None

    def _pn_sum(s):
        """解析可能含多地號的地坪字串，加總後回傳
        支援格式：
          單純數字：「1513.9」→ 1513.9
          加號分隔：「4820.86+2081.46」→ 6902.32
          空白分隔：「757.08 756.65」→ 1513.73（Firestore 多地號用空白隔開）
        """
        try:
            raw = str(s or '').replace(',', '').strip()
            # 先試直接轉數字（最常見，效率最高）
            return float(raw)
        except Exception:
            pass
        try:
            # 把加號和空白都當分隔符，拆開後加總
            parts = [float(x) for x in re.split(r'[\s+]+', str(s or '').replace(',', '').strip()) if x.strip()]
            if parts:
                return sum(parts)
        except Exception:
            pass
        return None

    def _effective_price(cand):
        """解析候選售價（萬）
        一般格式：直接解析數字
        單價格式「X/分」：地坪（坪）÷ 293.4 × X → 換算為實際售價（萬）
        （農地常見：Firestore 存單價如「150/分」，地坪可能是「4820.86+2081.46」多地號相加）
        """
        raw = str(cand.get('售價(萬)', '') or '').strip()
        # 一般數字格式（最常見）
        num = _pn(raw)
        if num is not None:
            return num
        # 單價格式：如「150/分」
        m = re.match(r'^(\d+(?:\.\d+)?)\s*/\s*分$', raw)
        if m:
            per_min = float(m.group(1))   # 每台分售價（萬）
            # 地坪可能是加法字串如「4820.86+2081.46」→ 先加總再換算
            地坪_total = _pn_sum(cand.get('地坪'))
            if 地坪_total and 地坪_total > 0:
                # 坪 ÷ 293.4 = 台分；台分 × per_min = 總售價（萬）
                return round(地坪_total / 293.4 * per_min, 1)
        return None

    def _sm(a, b, tol=0.10):
        if a is None or b is None:
            return None
        if a == 0 and b == 0:
            return True
        if a == 0 or b == 0:
            return False
        return abs(a - b) / max(abs(a), abs(b)) <= tol

    def _agent_score(ag_db, ag_csv):
        """比較經紀人（軟資料）：換手正常，不扣分；同人或有交集才加分"""
        if not ag_db or not ag_csv:
            return 0
        import re as _re
        def _split(s):
            return set(_re.split(r'[\s、,，]+', s.strip()))
        db_set  = _split(ag_db)
        csv_set = _split(ag_csv)
        if db_set == csv_set:   return 3   # 完全相同 +3
        if db_set & csv_set:    return 1   # 有交集 +1
        return 0                            # 完全不同 → 不扣分（換手正常）

    def _hard_area_score(w_row, d_row):
        """不動產硬資料：面積欄位精確比對（依類別調整容差）
        同一物件的實體面積不會因換手或改價而改變，是最可靠的比對基準。

        容差差異化（使用者經驗）：
          - 建物類（公寓/透天/別墅...）面積精確 → 2% 命中、20% 確定不同
          - 土地類（農地/建地/林地）常因 平方公尺↔坪↔台分 換算 + 助理估算誤差較大
            → 10% 命中、30% 才確定不同

        回傳 (score, has_hard_match, has_area_data)
          - has_hard_match：至少一組面積在容差內吻合
          - has_area_data ：至少一組面積兩邊都有值（不論是否吻合）
        """
        score = 0
        has_hard = False
        has_area_data = False
        # 判斷類別：用 Word 或 Firestore 任一邊的類別欄
        cat_w = str(w_row.get('物件類別', '') or '').strip()
        cat_d = str(d_row.get('物件類別', '') or '').strip()
        is_land = _has_land_part(cat_w) or _has_land_part(cat_d)
        # 土地類容差更寬（10% 命中、30% 才算衝突）
        tol_match    = 0.10 if is_land else 0.02
        tol_conflict = 0.30 if is_land else 0.20
        # (Word欄, Firestore欄, 命中加分) — 同一 Firestore 欄只比一次
        pairs = [
            ('地坪',   '地坪',   8),   # 房屋地坪
            ('面積坪', '地坪',   8),   # 農地/建地：Word面積坪 vs Firestore地坪（已換算成坪）
            ('建坪',   '建坪',   6),   # 公寓建坪
            ('室內坪', '室內坪', 5),   # 公寓室內坪
        ]
        checked_db = set()
        for wf, df, pts in pairs:
            if df in checked_db:
                continue
            wv = _pn(w_row.get(wf))
            dv = _pn_sum(d_row.get(df))  # Firestore 地坪可能是「757.08 756.65」多地號空白分隔
            if not wv or not dv:
                continue
            checked_db.add(df)
            has_area_data = True  # 兩邊都有面積資料 → 可以驗證
            if _sm(wv, dv, tol_match):           # 容差內：硬資料命中
                score += pts
                has_hard = True
            elif not _sm(wv, dv, tol_conflict):  # 超過衝突閾值：幾乎確定是不同物件
                score -= pts
        return score, has_hard, has_area_data

    # 從 Firestore 載入所有物件並建立索引（Word 是主體，Firestore 是查詢對象）
    col     = db.collection("company_properties")
    # 委託日過濾（民國 102/1/1）跟 ACCESS 比對 / 同步一致 — 跳過舊物件加速比對
    MIN_COMMIT = (2013, 1, 1)
    db_docs = []
    upload_filtered_old = 0
    for d in col.stream():
        rd = d.to_dict() or {}
        t = _parse_date_smart(rd.get("委託日", ""))
        if t is not None and t < MIN_COMMIT:
            upload_filtered_old += 1
            continue
        db_docs.append(d)

    db_by_comm = {}   # 委託編號 → Firestore doc dict
    db_by_name = {}   # 正規案名 → list of Firestore doc dict
    db_by_addr = {}   # 正規化地址 → Firestore doc dict（地址精確命中用）
    db_by_agent = {}  # 經紀人 → list of Firestore doc dict（案名前綴找不到時兜底用）
    for doc in db_docs:
        dd = doc.to_dict()
        dd['_doc_id'] = doc.id
        dbc = str(dd.get("委託編號", "") or "").strip()
        # Sheets 同步可能存為浮點數（如 91803.0）→ 轉整數字串
        try:
            if '.' in dbc:
                dbc = str(int(float(dbc)))
        except Exception:
            pass
        dbc = dbc.zfill(6) if dbc.strip('0') else ''
        if dbc and dbc != '000000':
            db_by_comm[dbc] = dd
        dbn = str(dd.get("案名", "") or "").strip()
        key = _nn(dbn)
        if key:
            db_by_name.setdefault(key, []).append(dd)
        # 地址索引（正規化去空白）— 同地址多筆時取「委託日最新」當代表
        dba = re.sub(r'\s+', '', str(dd.get('物件地址', '') or ''))
        if dba and len(dba) >= 6:
            existing = db_by_addr.get(dba)
            if existing is None:
                db_by_addr[dba] = dd
            else:
                old_dt = _parse_date_smart(existing.get("委託日", ""))
                new_dt = _parse_date_smart(dd.get("委託日", ""))
                # 新筆有委託日 且 (舊筆沒有 或 新筆較新) → 替換
                if new_dt is not None and (old_dt is None or new_dt > old_dt):
                    db_by_addr[dba] = dd
        # 經紀人索引（案名差異大時的兜底）
        dag = str(dd.get("經紀人", "") or "").strip()
        if dag:
            db_by_agent.setdefault(dag, []).append(dd)

    # Step 0：載入強行配對記憶，建立索引（委託號碼 → db_seq；案名 → db_seq）
    mem_by_comm = {}   # word_comm → memory record
    mem_by_name = {}   # _nn(word_name) → memory record
    try:
        mem_docs = db.collection("word_match_memory").stream()
        for md in mem_docs:
            mr = md.to_dict()
            mr["_mem_id"] = md.id
            wc = str(mr.get("word_comm", "") or "").strip()
            if wc and wc != '000000':
                mem_by_comm[wc] = mr
            wn = _nn(str(mr.get("word_name", "") or ""))
            if wn:
                mem_by_name[wn] = mr
    except Exception:
        pass  # 記憶載入失敗不影響主流程

    # db_by_seq：資料序號 → Firestore doc（供記憶配對使用）
    db_by_seq = {}
    for dd_val in list(db_by_comm.values()) + [v for lst in db_by_name.values() for v in lst]:
        seq = str(int(float(str(dd_val.get("資料序號", 0) or 0)))).strip()
        if seq and seq != '0':
            db_by_seq[seq] = dd_val

    # 以 Word 條目為主，逐一在 Firestore 找對應物件
    # Word 是目前在架物件的唯一真相；Firestore 是歷史全量（從未清除）
    high, medium, conflict, unmatched = [], [], [], []
    for row in all_entries:
        name = str(row.get('案名', '')).strip()
        if not name:
            continue
        comm = str(row.get('委託號碼', '') or '').strip()
        comm = comm.zfill(6) if comm.strip('0') else ''
        price  = _pn(row.get('售價萬', ''))
        expiry = _pe(row.get('到期日', ''))
        area   = (_pn(row.get('面積坪')) or _pn(row.get('地坪'))
                  or _pn(row.get('室內坪')) or _pn(row.get('建坪')))
        agent  = str(row.get('經紀人', '') or '').strip()
        key    = _nn(name)
        if not key:
            continue
        # 農地鄉鎮前綴補充：偵測案名起頭是否為台東鄉鎮名，建立去前綴版本
        # 優先用 CSV「區域」欄（如「成功（鎮）」），次用 hardcoded 鄉鎮清單（upload doc 路徑無 區域 欄）
        _TAITUNG_TOWNS = ["台東", "成功", "關山", "池上", "鹿野", "延平", "東河", "長濱",
                          "太麻里", "大武", "卑南", "海端", "金峰", "達仁", "綠島", "蘭嶼"]
        region_nn = _nn(str(row.get('區域', '') or ''))  # 括號內容由 _nn 自動去除
        if not (region_nn and len(region_nn) >= 2 and key.startswith(region_nn) and len(key) > len(region_nn)):
            # 區域欄為空時，從案名本身偵測鄉鎮前綴
            region_nn = next((t for t in _TAITUNG_TOWNS if key.startswith(t) and len(key) > len(t)), '')
        key_no_town = key[len(region_nn):] if region_nn else ''

        match, match_by, score, name_changed, best_has_hard = None, "", 0, False, False
        best_has_area = False  # 是否有面積資料可驗證（不論是否吻合）

        # 0. 強行配對記憶（優先級最高）
        mem = mem_by_comm.get(comm) or mem_by_name.get(key)
        if mem:
            mem_seq = str(mem.get("db_seq", "") or "").strip()
            mem_doc = db_by_seq.get(mem_seq)
            if mem_doc:
                match    = mem_doc
                match_by = f"強行記憶配對（序號 {mem_seq}）"
                score    = 99
                if mem_doc.get('案名') and _nn(str(mem_doc['案名'])) != _nn(name):
                    name_changed = True

        # 1. 先嘗試委託號碼精確比對
        if comm and comm != '000000':
            cm = db_by_comm.get(comm)
            if cm:
                match = cm
                match_by = "委託號碼"
                score = 10
                if cm.get('案名') and _nn(str(cm['案名'])) != _nn(name):
                    name_changed = True

        # 1.5. 地址精確命中（地址是硬資料，地址相同就是同物件）
        if not match:
            row_addr = re.sub(r'\s+', '', _ca(str(row.get('物件地址', '') or '')))
            if row_addr and len(row_addr) >= 6:
                cm = db_by_addr.get(row_addr)
                if not cm:
                    # 公寓1樓有時 Firestore 不寫「N樓」→ 去樓層後再試一次
                    stripped_addr = re.sub(r'\d+樓(之\d+)?$', '', row_addr).strip()
                    if stripped_addr != row_addr and len(stripped_addr) >= 6:
                        cm = db_by_addr.get(stripped_addr)
                if cm:
                    match = cm
                    match_by = "地址比對"
                    score = 8
                    if cm.get('案名') and _nn(str(cm['案名'])) != _nn(name):
                        name_changed = True

        # 2. 再嘗試案名 + 特徵評分比對（含硬資料面積比對）
        if not match:
            candidates = db_by_name.get(key, [])
            if key_no_town:
                # 補充去鄉鎮前綴的候選（如 Word「成功坪頂段海景農地」vs Firestore「坪頂段海景農地」）
                no_town_cands = [c for c in db_by_name.get(key_no_town, []) if c not in candidates]
                candidates = candidates + no_town_cands
            best, best_score, best_has_hard = None, -999, False
            best_has_area = False
            best_date = (0, 0, 0)
            for cand in candidates:
                cc_raw = str(cand.get('委託編號', '') or '').strip()
                try:
                    if '.' in cc_raw: cc_raw = str(int(float(cc_raw)))
                except Exception: pass
                # Firestore 委託編號可能含多個（空白分隔，如「051857 051719」），拆開後各自 zfill
                cc_parts = [x.zfill(6) if x.strip('0') else '' for x in cc_raw.split() if x.strip()]
                if not cc_parts:
                    cc_parts = ['']
                cc = cc_parts[0]  # 主要委託號（相容舊邏輯使用 cc 變數的地方）
                cg = str(cand.get('經紀人', '') or '').strip()
                # 兩邊都有委託號且清單中沒有任一吻合 → 不同物件，跳過
                if comm and comm != '000000':
                    valid_cc = [p for p in cc_parts if p and p != '000000']
                    if valid_cc and comm not in valid_cc:
                        continue
                # 硬資料：面積精確比對（地坪/建坪/室內坪/面積坪，2% 容差）
                area_sc, has_hard, has_area = _hard_area_score(row, cand)
                s = area_sc
                # 售價：輔助參考（正常波動不扣重分）
                dbp = _pn(cand.get('售價(萬)'))
                ps  = _sm(price, dbp, 0.05)
                if ps is True:                            s += 2
                elif ps is False and not _sm(price, dbp, 0.30): s -= 2
                # 經紀人：軟資料，換手正常 → 只加分不扣分
                s += _agent_score(cg, agent)
                if cand.get('委託到期日'): s += 1
                # 取最高分；分數相同時偏好「委託日較新」（Word 物件總表是現役清單，應配最新委託）
                cand_date = _parse_date_smart(cand.get('委託日', '')) or (0, 0, 0)
                if s > best_score or (s == best_score and cand_date > best_date):
                    best_score = s
                    best = cand
                    best_has_hard = has_hard
                    best_has_area = has_area
                    best_date = cand_date
            if best is not None:
                match = best
                score = best_score
                match_by = "硬資料比對（面積）" if best_has_hard else "案名比對"

        # 找不到對應 → 前綴模糊搜尋找近似候選，供人工比對
        if not match:
            near_miss, nm_score = None, -999
            csv_addr_nm = _ca(str(row.get('物件地址', '') or '')).strip()
            prefix = key[:min(len(key), 6)] if len(key) >= 4 else ''
            short_prefix = key[:3] if len(key) >= 3 else ''  # 3字備援前綴（如「四川路」）
            geo_prefix   = key[:2] if len(key) >= 2 else ''  # 2字地名前綴（如「都歷」）
            # 去鄉鎮前綴的補充搜尋前綴（如「坪頂段海景農地」的前6字）
            extra_prefix = key_no_town[:min(len(key_no_town), 6)] if key_no_town and len(key_no_town) >= 4 else ''
            extra_short  = key_no_town[:3] if key_no_town and len(key_no_town) >= 3 else ''
            if prefix or extra_prefix or geo_prefix:
                for db_key, db_cands in db_by_name.items():
                    if (db_key.startswith(prefix) or prefix in db_key
                            or (short_prefix and db_key.startswith(short_prefix))
                            or (extra_prefix and (db_key.startswith(extra_prefix) or extra_prefix in db_key))
                            or (extra_short and db_key.startswith(extra_short))
                            # 2字地名相同：處理小地名插入（如「都歷看海農地」vs「都歷豐田看海農地」）
                            or (geo_prefix and len(db_key) >= 4 and db_key[:2] == geo_prefix)):
                        for cand in db_cands:
                            area_sc, _, _ = _hard_area_score(row, cand)  # 面積也納入近似候選評分
                            s = area_sc
                            s += _agent_score(str(cand.get('經紀人','') or '').strip(), agent)
                            # 售價：相近加分，差距太大扣分（農地 3530萬 vs 建地 220萬 → 必須懲罰）
                            # 注意：農地 Firestore 可能存「150/分」單價格式，需換算後才能比對
                            cand_p = _effective_price(cand)
                            if _sm(price, cand_p, 0.10) is True:
                                s += 2
                            elif price and cand_p and price > 0 and cand_p > 0:
                                pdiff = abs(price - cand_p) / max(price, cand_p)
                                if pdiff > 1.50: s -= 5   # 差超過 150%（如 3530 vs 220）
                                elif pdiff > 0.50: s -= 3  # 差超過 50%
                            # 物件類別不符懲罰（土地類農地/建地 vs 建物類公寓/房屋）
                            cand_cat = str(cand.get('物件類別', '') or '').strip()
                            if cand_cat:
                                # 有「面積坪」欄位 = 農地或建地（土地類）；其他 = 公寓/房屋（建物類）
                                row_is_land = row.get('面積坪') is not None
                                cand_is_land = any(t in cand_cat for t in ('農地', '建地'))
                                if row_is_land != cand_is_land:
                                    s -= 4  # 土地類 vs 建物類，一定是不同物件
                            # 地址：硬資料，不同地址就是不同物件
                            cand_addr = str(cand.get('物件地址', '') or '').strip()
                            if csv_addr_nm and cand_addr:
                                if csv_addr_nm == cand_addr:
                                    s += 6  # 地址完全相符 → 高信心
                                elif csv_addr_nm in cand_addr or cand_addr in csv_addr_nm:
                                    # 子字串包含：Firestore 地址多了「台東縣」前綴、或 Word 多了「1樓」
                                    s += 2  # 地址部分相符
                                else:
                                    # 嘗試去樓層後再比對（如 Word 寫「1樓」Firestore 未寫）
                                    ca_stripped = re.sub(r'\d+樓(之\d+)?$', '', csv_addr_nm).strip()
                                    da_stripped = re.sub(r'\d+樓(之\d+)?$', '', cand_addr).strip()
                                    if ca_stripped and da_stripped and (ca_stripped == da_stripped
                                            or ca_stripped in da_stripped or da_stripped in ca_stripped):
                                        s += 2  # 去樓層後地址相符（樓層只是補充說明）
                                    else:
                                        s -= 8  # 地址確實不同（286號 vs 288號）→ 幾乎確定是不同物件
                            if s > nm_score:
                                nm_score = s
                                near_miss = cand
            # 案名前綴找不到候選 → 同經紀人兜底掃描（農地/建地案名差異大時）
            if near_miss is None and agent:
                fallback_cands = db_by_agent.get(agent, [])
                for cand in fallback_cands:
                    area_sc2, _, _ = _hard_area_score(row, cand)
                    s2 = area_sc2
                    s2 += _agent_score(str(cand.get('經紀人', '') or '').strip(), agent)
                    cand_p2 = _effective_price(cand)   # 支援「X/分」農地單價格式
                    if _sm(price, cand_p2, 0.10) is True:
                        s2 += 2
                    elif price and cand_p2 and price > 0 and cand_p2 > 0:
                        pdiff2 = abs(price - cand_p2) / max(price, cand_p2)
                        if pdiff2 > 1.50: s2 -= 5
                        elif pdiff2 > 0.50: s2 -= 3
                    # 物件類別不符懲罰
                    cand_cat2 = str(cand.get('物件類別', '') or '').strip()
                    if cand_cat2:
                        row_is_land2 = row.get('面積坪') is not None
                        cand_is_land2 = any(t in cand_cat2 for t in ('農地', '建地'))
                        if row_is_land2 != cand_is_land2:
                            s2 -= 4
                    cand_addr2 = str(cand.get('物件地址', '') or '').strip()
                    if csv_addr_nm and cand_addr2:
                        if csv_addr_nm == cand_addr2:
                            s2 += 6
                        elif csv_addr_nm in cand_addr2 or cand_addr2 in csv_addr_nm:
                            s2 += 2
                        else:
                            ca_s2 = re.sub(r'\d+樓(之\d+)?$', '', csv_addr_nm).strip()
                            da_s2 = re.sub(r'\d+樓(之\d+)?$', '', cand_addr2).strip()
                            if ca_s2 and da_s2 and (ca_s2 == da_s2 or ca_s2 in da_s2 or da_s2 in ca_s2):
                                s2 += 2
                            else:
                                s2 -= 4  # 兜底掃描中地址不符扣分輕一些（農地地號格式差異大）
                    if s2 > nm_score:
                        nm_score = s2
                        near_miss = cand
            # 地址不符時（分數太低）不顯示近似候選，避免誤導
            if near_miss is not None and nm_score < 0:
                near_miss = None
            # 近似候選分數很高（地址+經紀人+售價全吻合）→ 升信心，不留在問題區
            if near_miss is not None and nm_score >= 8:
                nm_name = str(near_miss.get('案名', '') or '')
                # 案名「規範化後相同」或「一邊是另一邊的前綴 + 差 ≤ 6 字」都視為同名
                # 業務常態：助理會在案名加 A7、B1 等戶號後綴；WORD/ACCESS 可能省略
                _na = _nn(nm_name); _nb = _nn(name)
                nm_name_match = bool(_na and _nb) and (
                    _na == _nb or
                    (_na.startswith(_nb) and len(_na) - len(_nb) <= 6) or
                    (_nb.startswith(_na) and len(_nb) - len(_na) <= 6)
                )
                # 案名相同／前綴變體 → 升高信心（主迴圈漏配，近似搜尋補救）
                # 案名差異大但其他資料吻合 → 升中信心，讓使用者確認
                match_reason = "近似搜尋補救（案名完全相符）" if nm_name_match else "近似候選升中信心"
                item_nm = {
                    "doc_id":    near_miss['_doc_id'],
                    "db_name":   nm_name,
                    "db_seq":    str(int(near_miss.get('資料序號', 0) or 0)),
                    "db_price":  _pn(near_miss.get('售價(萬)')),
                    "db_expiry": near_miss.get('委託到期日', ''),
                    "db_agent":  str(near_miss.get('經紀人', '') or '').strip(),
                    "db_addr":   str(near_miss.get('物件地址', '') or '').strip(),
                    "csv_name":  name, "csv_price": price, "csv_expiry": expiry,
                    "csv_agent": agent, "csv_comm": comm,
                    "csv_addr":  csv_addr_nm,
                    "csv_row":   str(row.get('流水號', '') or '').strip(),  # Word 內行號
                    "match_by":  match_reason,
                    "score":     nm_score,
                    "name_changed": not nm_name_match,
                    "db_land":   _pn(near_miss.get('地坪')),
                    "db_build":  _pn(near_miss.get('建坪')),
                    "db_interior": _pn(near_miss.get('室內坪')),
                    "csv_land":  _pn(row.get('地坪')) or _pn(row.get('面積坪')),
                    "csv_build": _pn(row.get('建坪')),
                    "csv_interior": _pn(row.get('室內坪')),
                }
                if nm_name_match:
                    high.append(item_nm)
                else:
                    medium.append(item_nm)
                continue
            um = {
                "csv_name":     name,
                "csv_price":    price,
                "csv_expiry":   expiry,
                "csv_agent":    agent,
                "csv_comm":     comm,
                "csv_addr":     _ca(str(row.get('物件地址', '') or '')).strip(),
                "csv_land":     _pn(row.get('地坪')) or _pn(row.get('面積坪')),
                "csv_build":    _pn(row.get('建坪')),
                "csv_interior": _pn(row.get('室內坪')),
            }
            if near_miss:
                um["nm_doc_id"]   = near_miss['_doc_id']
                um["nm_name"]     = str(near_miss.get('案名', '') or '')
                um["nm_price"]    = _pn(near_miss.get('售價(萬)'))
                um["nm_agent"]    = str(near_miss.get('經紀人', '') or '').strip()
                um["nm_seq"]      = str(near_miss.get('資料序號', '') or '')
                um["nm_expiry"]   = near_miss.get('委託到期日', '')
                um["nm_addr"]     = str(near_miss.get('物件地址', '') or '').strip()
                um["nm_land"]     = _pn(near_miss.get('地坪'))
                um["nm_build"]    = _pn(near_miss.get('建坪'))
                um["nm_interior"] = _pn(near_miss.get('室內坪'))
                um["nm_score"]    = nm_score
                um["nm_comm"]     = str(near_miss.get('委託編號', '') or '').strip()
            unmatched.append(um)
            continue

        dbn = str(match.get('案名', '') or '')
        dbs = int(match.get('資料序號', 0) or 0)
        dbp = _pn(match.get('售價(萬)'))
        dbe = match.get('委託到期日', '')
        dbg = str(match.get('經紀人', '') or '').strip()
        # 面積對照（供前端顯示診斷）
        db_land     = _pn(match.get('地坪'))
        db_build    = _pn(match.get('建坪'))
        db_interior = _pn(match.get('室內坪'))
        csv_land    = _pn(row.get('地坪')) or _pn(row.get('面積坪'))
        csv_build   = _pn(row.get('建坪'))
        csv_interior = _pn(row.get('室內坪'))
        item = {
            "doc_id":        match['_doc_id'],
            "db_name":       dbn,
            "db_seq":        dbs,
            "db_price":      dbp,
            "db_expiry":     dbe,
            "db_agent":      dbg,
            "db_addr":       str(match.get('物件地址', '') or '').strip(),
            "db_land":       db_land,
            "db_build":      db_build,
            "db_interior":   db_interior,
            "csv_name":      name,
            "csv_price":     price,
            "csv_expiry":    expiry,
            "csv_agent":     agent,
            "csv_comm":      comm,
            "csv_addr":      str(row.get('物件地址', '') or '').strip(),
            "csv_land":      csv_land,
            "csv_build":     csv_build,
            "csv_interior":  csv_interior,
            "csv_row":       str(row.get('流水號', '') or '').strip(),  # Word 物件總表內的行號
            "match_by":      match_by,
            "score":         score,
            "has_hard":      best_has_hard,
            "has_area":      best_has_area,
            "name_changed":  name_changed,
        }
        # 委託號碼/序號命中或高評分 → 高信心；但地址明顯不符則降中信心
        # 強行記憶配對（score=99）→ 永遠高信心，跳過地址檢查
        addr_mismatch = False
        if score != 99 and match_by in ("委託號碼", "資料序號", "物件地址"):
            da = item.get("db_addr", ""); ca = item.get("csv_addr", "")
            if da and ca and da != ca and ca not in da and da not in ca:
                # 去樓層後再比對：若只差「1樓」這類樓層資訊，不算地址不符
                da_s = re.sub(r'\d+樓(之\d+)?$', '', da).strip()
                ca_s = re.sub(r'\d+樓(之\d+)?$', '', ca).strip()
                floor_only_diff = (da_s and ca_s and (da_s == ca_s or da_s in ca_s or ca_s in da_s))
                if not floor_only_diff:
                    addr_mismatch = True
                    item["match_by"] = match_by + "（地址不符，請確認）"
        # 強配對（強行記憶 / 委編 / 資料序號 / 物件地址）→ 高信心
        if score == 99 or (match_by in ("委託號碼", "資料序號", "物件地址") and not addr_mismatch):
            high.append(item)
        # 「中信心定義」修正（使用者經驗：中信心應該是「硬資料命中、軟資料待確認」）：
        # 兩邊都有面積但沒一組落在 2% 容差 → 一律歸衝突，不該進高/中信心。
        # 這樣中信心 bucket 就只剩「硬資料 OK」或「沒面積可驗證」兩種。
        elif best_has_area and not best_has_hard:
            # 例：金樽沖浪特區農地(四) 847 坪 vs (一) 780.14 坪 → 差 7.89%，明顯不同物件
            item["conflict_reason"] = f"面積不符（兩邊都有但差 > 2% 容差，分數 {score}）"
            conflict.append(item)
        elif score >= 3:
            # score >= 3 通常代表 has_hard 命中（地坪 +8 / 建坪 +6）或其他強配對
            high.append(item)
        elif score >= 0:
            # 中信心：has_hard 命中（軟資料差異待確認），或無面積資料但軟資料相符
            medium.append(item)
        elif best_has_hard:
            # 面積命中但其他軟資料差距大（少見保險）
            item["conflict_reason"] = f"面積符合但其他差距大（分數 {score}）"
            conflict.append(item)
        else:
            # 真的無面積可驗證，僅軟資料不符 → 歸中信心，人工確認
            item["match_by"] = "案名比對（無面積驗證）"
            medium.append(item)

    return jsonify({
        "ok":       True,
        "csv_rows": len(all_entries),
        "high":     high,
        "medium":   medium,
        "conflict": conflict,
        "unmatched": unmatched,
        "doc_date": doc_date,
    })


@app.route("/api/word-snapshot/meta", methods=["POST", "GET"])
def api_word_snapshot_meta():
    """
    POST：前端上傳 word_meta.json 的內容（JSON body），
          儲存至 Firestore word_snapshot/latest 的 doc_date 欄位。
    GET：回傳目前儲存的物件總表日期。
    """
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    doc_ref = db.collection("word_snapshot").document("latest")

    if request.method == "GET":
        try:
            doc = doc_ref.get()
            if not doc.exists:
                return jsonify({"doc_date": None})
            d = doc.to_dict()
            return jsonify({"doc_date": d.get("doc_date")})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    # POST：儲存日期資料
    data = request.get_json(silent=True) or {}
    minguo  = data.get("minguo", "")
    western = data.get("western", "")
    if not minguo:
        return jsonify({"error": "缺少 minguo 欄位"}), 400

    try:
        doc_ref.set({
            "doc_date": {
                "minguo":    minguo,
                "western":   western,
                "saved_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "saved_by":  email,
            }
        }, merge=True)
        return jsonify({"ok": True, "minguo": minguo})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/word-snapshot/status", methods=["GET"])
def api_word_snapshot_status():
    """回傳目前 Word snapshot 的版本資訊。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

    db = _get_db()
    if db is None:
        return jsonify({"status": "no_db"}), 200

    try:
        doc = db.collection("word_snapshot").document("latest").get()
        if not doc.exists:
            return jsonify({"status": "none"})
        d = doc.to_dict()
        return jsonify({
            "status":      "ok",
            "uploaded_at": d.get("uploaded_at", ""),
            "filename":    d.get("filename", ""),
            "count":       d.get("count", 0),
        })
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/api/word-snapshot/prices", methods=["GET"])
def api_word_snapshot_prices():
    """回傳目前 snapshot 的售價字典，供前端卡片對比用。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

    db = _get_db()
    if db is None:
        return jsonify({}), 200

    try:
        doc = db.collection("word_snapshot").document("latest").get()
        if not doc.exists:
            return jsonify({})
        return jsonify(doc.to_dict().get("prices", {}))
    except Exception:
        return jsonify({})


@app.route("/api/company-properties/<prop_id>", methods=["GET"])
def api_company_property_get(prop_id):
    """取得單筆公司物件完整資料。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

    is_admin = _is_admin(email)
    org_info = _get_org_for_user(email)
    org_id = org_info["org_id"] if org_info else None
    if not is_admin and not org_id:
        return jsonify({"error": "您尚未加入任何組織，無法存取物件資料。", "need_org": True}), 403

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    try:
        doc = db.collection("company_properties").document(prop_id).get()
        if not doc.exists:
            return jsonify({"error": "找不到物件"}), 404

        data = {"id": doc.id, **doc.to_dict()}

        # 確認這筆資料屬於用戶的組織（有 org_id 的文件才做檢查）
        if org_id and data.get("org_id") and data.get("org_id") != org_id:
            return jsonify({"error": "無權存取此物件"}), 403

        # 只有管理員才能看敏感欄位
        if not _is_admin(email):
            sensitive = {"身份証字號", "室內電話1", "行動電話1",
                         "連絡人室內電話2", "連絡人行動電話2",
                         "買方電話", "買方生日", "賣方生日",
                         "買方姓名", "買方住址"}
            for k in sensitive:
                data.pop(k, None)

        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/company-properties/<prop_id>/star", methods=["POST"])
def api_company_property_star(prop_id):
    """切換物件的加星狀態，回傳新狀態。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    try:
        doc_ref = db.collection("company_properties").document(prop_id)
        doc = doc_ref.get()
        if not doc.exists:
            return jsonify({"error": "找不到物件"}), 404

        current = doc.to_dict().get("已加星", False)
        new_val = not bool(current)
        doc_ref.update({"已加星": new_val})
        # 通知 home-start 該物件變動（雖然 star 不顯示，但保持資料新鮮）
        _notify_home_start(prop_id, action="upsert")
        return jsonify({"starred": new_val})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/company-properties/<prop_id>/push-to-home-start", methods=["POST"])
def api_company_property_push_to_home_start(prop_id):
    """單筆推送該物件到起家對外網站（觸發 home-start 從 library 拉最新資料）。
    用於：使用者剛改完售價/物件資訊，不想等 daily cron、要立刻反映到對外網站。
    """
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    if not prop_id:
        return jsonify({"error": "缺少 property_id"}), 400
    home_start_url = (os.environ.get("HOME_START_URL") or "").strip()
    if not home_start_url:
        return jsonify({"error": "HOME_START_URL 環境變數未設定"}), 500
    # 同步呼叫 webhook（_notify_home_start 是 fire-and-forget；改成這裡直接 POST 看結果）
    try:
        import requests as _req
        service_key = (os.environ.get("SERVICE_API_KEY") or "").strip()
        r = _req.post(
            f"{home_start_url.rstrip('/')}/api/sync/webhook",
            json={"property_id": str(prop_id), "action": "upsert"},
            headers={"X-Service-Key": service_key, "Content-Type": "application/json"},
            timeout=8,
        )
        if r.status_code >= 400:
            return jsonify({"error": f"home-start 回應 {r.status_code}: {r.text[:200]}"}), 502
        return jsonify({"ok": True, "home_start_response": r.json() if r.text else None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ════════════════════════════════════════════════════════
# yes319 公司網頁 → home-start 同步（爬蟲 + 比對 + 推送）
# ════════════════════════════════════════════════════════

@app.route("/api/yes319/sync", methods=["POST"])
def api_yes319_sync():
    """從 yes319 公司網頁爬全部委託期內物件、比對 home-start、把特色/機能/屋齡/樓層推到 home-start。
    需要管理員或登入者觸發；Cloud Scheduler 也可用 X-Service-Key。
    參數：?dry_run=1 → 只比對不推送（看會配對幾筆）
    """
    # 認證：本機/admin session 或 service key（Cloud Scheduler 用）
    has_session_auth = False
    try:
        email, err = _require_user()
        if not err:
            has_session_auth = True
    except Exception:
        pass
    if not has_session_auth:
        # 退而其次：service key
        expected_key = (os.environ.get("SERVICE_API_KEY") or "").strip()
        sent_key = (request.headers.get("X-Service-Key") or "").strip()
        if not expected_key or sent_key != expected_key:
            return jsonify({"error": "unauthorized"}), 401

    home_start_url = (os.environ.get("HOME_START_URL") or "").strip()
    service_key = (os.environ.get("SERVICE_API_KEY") or "").strip()
    if not home_start_url or not service_key:
        return jsonify({"error": "HOME_START_URL 或 SERVICE_API_KEY 未設定"}), 500

    dry_run = (request.args.get("dry_run") or request.form.get("dry_run") or "").strip() in ("1", "true", "yes")

    try:
        import yes319_sync
        result = yes319_sync.run_full_sync(
            home_start_url=home_start_url,
            service_key=service_key,
            dry_run=dry_run,
        )
        return jsonify(result)
    except Exception as e:
        import logging
        logging.exception("yes319 sync 失敗")
        return jsonify({"error": str(e)}), 500


# ════════════════════════════════════════════════════════
# 物件事件歷史（home-start 等子服務的稽核 trail）
# ════════════════════════════════════════════════════════

@app.route("/api/property-events", methods=["POST"])
def api_property_events_create():
    """接收 home-start 等子服務 fire-and-forget 寫入的事件。需 service-key。"""
    expected = (os.environ.get("SERVICE_API_KEY") or "").strip()
    sent = (request.headers.get("X-Service-Key") or "").strip()
    if not expected or sent != expected:
        return jsonify({"error": "unauthorized"}), 401
    d = request.get_json(silent=True) or {}
    pid = str(d.get("property_id") or "").strip()
    event_type = str(d.get("event_type") or "").strip()
    if not pid or not event_type:
        return jsonify({"error": "需要 property_id 和 event_type"}), 400
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone(timedelta(hours=8))).isoformat()
        doc_ref = db.collection("property_events").document()
        doc_ref.set({
            "property_id": pid,
            "event_type":  event_type,
            "source":      d.get("source") or "unknown",
            "actor":       d.get("actor") or "",
            "payload":     d.get("payload") or {},
            "created_at":  now,
        })
        return jsonify({"ok": True, "id": doc_ref.id})
    except Exception as e:
        import logging
        logging.exception("property-events create 失敗")
        return jsonify({"error": str(e)}), 500


@app.route("/api/property-events", methods=["GET"])
def api_property_events_list():
    """查詢某物件的事件歷史。只有陳威良 / admin 看得到。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    pid = str(request.args.get("property_id") or "").strip()
    if not pid:
        return jsonify({"error": "需要 property_id"}), 400
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    # 能見性：只有陳威良 / 管理員看完整內容
    is_owner = (email == "a0911190009@gmail.com") or _is_admin(email)
    if not is_owner:
        return jsonify({"items": [], "hidden": True})
    try:
        docs = list(db.collection("property_events").where("property_id", "==", pid).stream())
        items = []
        for d in docs:
            dd = d.to_dict() or {}
            items.append({
                "id":         d.id,
                "event_type": dd.get("event_type", ""),
                "source":     dd.get("source", ""),
                "actor":      dd.get("actor", ""),
                "payload":    dd.get("payload", {}),
                "created_at": dd.get("created_at", ""),
            })
        items.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return jsonify({"items": items})
    except Exception as e:
        import logging
        logging.exception("property-events list 失敗")
        return jsonify({"error": str(e)}), 500


@app.route("/api/yes319/unlist-missing", methods=["POST"])
def api_yes319_unlist_missing():
    """home-start 有 yes319_objno 但 yes319 已不存在的物件 → 標記下架。
    ?dry_run=1 只回傳要下架的清單，不實際標記。
    """
    has_session_auth = False
    try:
        email, err = _require_user()
        if not err: has_session_auth = True
    except Exception:
        pass
    if not has_session_auth:
        expected_key = (os.environ.get("SERVICE_API_KEY") or "").strip()
        sent_key = (request.headers.get("X-Service-Key") or "").strip()
        if not expected_key or sent_key != expected_key:
            return jsonify({"error": "unauthorized"}), 401

    home_start_url = (os.environ.get("HOME_START_URL") or "").strip()
    service_key = (os.environ.get("SERVICE_API_KEY") or "").strip()
    if not home_start_url or not service_key:
        return jsonify({"error": "HOME_START_URL 或 SERVICE_API_KEY 未設定"}), 500
    dry_run = (request.args.get("dry_run") or "").strip() in ("1", "true", "yes")
    try:
        import yes319_sync
        return jsonify(yes319_sync.run_unlist_missing(home_start_url, service_key, dry_run=dry_run))
    except Exception as e:
        import logging
        logging.exception("yes319 unlist 失敗")
        return jsonify({"error": str(e)}), 500


@app.route("/api/yes319/create-missing-preview", methods=["GET", "POST"])
def api_yes319_create_missing_preview():
    """預覽 yes319 有但 home-start 沒有的物件清單（dry-run，不寫入）。"""
    has_session_auth = False
    try:
        email, err = _require_user()
        if not err: has_session_auth = True
    except Exception:
        pass
    if not has_session_auth:
        expected_key = (os.environ.get("SERVICE_API_KEY") or "").strip()
        sent_key = (request.headers.get("X-Service-Key") or "").strip()
        if not expected_key or sent_key != expected_key:
            return jsonify({"error": "unauthorized"}), 401
    home_start_url = (os.environ.get("HOME_START_URL") or "").strip()
    service_key = (os.environ.get("SERVICE_API_KEY") or "").strip()
    try:
        import yes319_sync
        return jsonify(yes319_sync.run_create_missing_dryrun(home_start_url, service_key))
    except Exception as e:
        import logging
        logging.exception("yes319 create preview 失敗")
        return jsonify({"error": str(e)}), 500


@app.route("/api/yes319/sync-all", methods=["POST"])
def api_yes319_sync_all():
    """一鍵全自動：文案同步 + 照片同步 + 下架預覽 + 缺漏預覽
    順序執行 4 個動作，回傳合併結果。下架/缺漏只 dry-run，使用者看完結果再決定。
    預計 10-15 分鐘（4 次 yes319 爬蟲）。
    """
    has_session_auth = False
    try:
        email, err = _require_user()
        if not err:
            has_session_auth = True
    except Exception:
        pass
    if not has_session_auth:
        expected_key = (os.environ.get("SERVICE_API_KEY") or "").strip()
        sent_key = (request.headers.get("X-Service-Key") or "").strip()
        if not expected_key or sent_key != expected_key:
            return jsonify({"error": "unauthorized"}), 401

    home_start_url = (os.environ.get("HOME_START_URL") or "").strip()
    service_key    = (os.environ.get("SERVICE_API_KEY") or "").strip()
    if not home_start_url or not service_key:
        return jsonify({"error": "HOME_START_URL 或 SERVICE_API_KEY 未設定"}), 500

    import logging as _log
    out = {"ok": True}
    try:
        import yes319_sync
        # 1. 文案同步（爬 yes319 → 比對 → 推送 features/amenities/age/floor）
        try:
            out["sync"] = yes319_sync.run_full_sync(home_start_url, service_key, dry_run=False)
        except Exception as e:
            _log.exception("sync-all: full_sync 失敗")
            out["sync"] = {"ok": False, "error": str(e)}
        # 2. 照片同步（對「有 yes319_objno 但無照片」的補照）
        try:
            out["photos"] = yes319_sync.run_photo_sync(home_start_url, service_key)
        except Exception as e:
            _log.exception("sync-all: photo_sync 失敗")
            out["photos"] = {"ok": False, "error": str(e)}
        # 3. 下架預覽（不實際下架，回傳待下架清單給使用者確認）
        try:
            out["unlist_preview"] = yes319_sync.run_unlist_missing(home_start_url, service_key, dry_run=True)
        except Exception as e:
            _log.exception("sync-all: unlist_preview 失敗")
            out["unlist_preview"] = {"ok": False, "error": str(e)}
        # 4. 缺漏預覽（yes319 有但 home-start 沒對應的）
        try:
            out["create_missing"] = yes319_sync.run_create_missing_dryrun(home_start_url, service_key)
        except Exception as e:
            _log.exception("sync-all: create_missing 失敗")
            out["create_missing"] = {"ok": False, "error": str(e)}
        return jsonify(out)
    except Exception as e:
        _log.exception("sync-all 失敗")
        return jsonify({"error": str(e)}), 500


@app.route("/api/yes319/sync-photos", methods=["POST"])
def api_yes319_sync_photos():
    """從 yes319 下載照片，補到 home-start 那些「有 yes319_objno 但無照片」的物件。"""
    has_session_auth = False
    try:
        email, err = _require_user()
        if not err:
            has_session_auth = True
    except Exception:
        pass
    if not has_session_auth:
        expected_key = (os.environ.get("SERVICE_API_KEY") or "").strip()
        sent_key = (request.headers.get("X-Service-Key") or "").strip()
        if not expected_key or sent_key != expected_key:
            return jsonify({"error": "unauthorized"}), 401

    home_start_url = (os.environ.get("HOME_START_URL") or "").strip()
    service_key = (os.environ.get("SERVICE_API_KEY") or "").strip()
    if not home_start_url or not service_key:
        return jsonify({"error": "HOME_START_URL 或 SERVICE_API_KEY 未設定"}), 500

    try:
        import yes319_sync
        result = yes319_sync.run_photo_sync(
            home_start_url=home_start_url,
            service_key=service_key,
        )
        return jsonify(result)
    except Exception as e:
        import logging
        logging.exception("yes319 photo sync 失敗")
        return jsonify({"error": str(e)}), 500


@app.route("/api/company-properties/<prop_id>/showings", methods=["GET"])
def api_company_property_showings(prop_id):
    """取得該物件的帶看紀錄（從 Buyer 服務共用的 showings collection 查詢）。
    管理員看全部，一般用戶只看自己建立的。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    try:
        col = db.collection("showings")
        # 依 prop_id 篩選（排序在 Python 端做，避免需要 Firestore 複合索引）
        docs = col.where("prop_id", "==", prop_id).stream()
        items = []
        for d in docs:
            row = d.to_dict()
            # 一般用戶只看自己建立的
            if not _is_admin(email) and row.get("created_by") != email:
                continue
            items.append({
                "id":          d.id,
                "buyer_name":  row.get("buyer_name", ""),
                "date":        row.get("date", ""),
                "reaction":    row.get("reaction", ""),
                "note":        row.get("note", ""),
                "created_by":  row.get("created_by", ""),
            })
        # 按日期降序排列（最新的在上面）
        items.sort(key=lambda x: x.get("date", ""), reverse=True)
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


## 物件搜尋索引記憶體快取（避免每次請求都打 Firestore）
_prop_index_cache = None          # list of {id, n, a, c}
_prop_index_cache_time = 0        # Unix timestamp


def _get_prop_index():
    """取得物件搜尋索引（優先記憶體快取，逾 5 分鐘重新從 Firestore 讀取）。"""
    import time
    global _prop_index_cache, _prop_index_cache_time
    now = time.time()
    if _prop_index_cache is not None and (now - _prop_index_cache_time) < 300:
        return _prop_index_cache
    db = _get_db()
    if db is None:
        return []
    try:
        doc = db.collection("meta").document("prop_index").get()
        if doc.exists:
            d = doc.to_dict()
            raw = d.get("data") or d.get("index") or "[]"
            _prop_index_cache = json.loads(raw)
            _prop_index_cache_time = now
            return _prop_index_cache
    except Exception:
        pass
    return _prop_index_cache or []


@app.route("/api/prop-suggest", methods=["GET"])
def api_prop_suggest():
    """
    公開 API（不需登入）：依關鍵字快速搜尋公司物件，供買方管理自動完成使用。
    從記憶體快取的索引搜尋，毫秒級回應。索引在每次 Sheets 同步後自動更新。
    回傳欄位：id, 案名, 地址, 類別。
    """
    kw = request.args.get("q", "").strip()
    if not kw:
        return jsonify({"items": []})
    kw_lower = kw.lower()
    index = _get_prop_index()
    results = []
    for item in index:
        name = item.get("n", "")
        addr = item.get("a", "")
        if kw_lower in name.lower() or kw_lower in addr.lower():
            results.append({
                "id":     item["id"],
                "案名":   name,
                "地址":   addr,
                "類別":   item.get("c", ""),
                "銷售中": item.get("s", 0) == 1,
                "所有權人": item.get("o", ""),
                "段別":   item.get("sec", ""),
                "地號":   item.get("lno", ""),
                "縣市鄉鎮": item.get("ar", ""),
            })
            if len(results) >= 10:
                break
    return jsonify({"items": results})


@app.route("/api/rebuild-prop-index", methods=["POST"])
def api_rebuild_prop_index():
    """重建物件搜尋索引（管理員用）。同步完成後自動觸發，也可手動呼叫。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email):
        return jsonify({"error": "僅管理員可用"}), 403
    try:
        global _prop_index_cache, _prop_index_cache_time
        _rebuild_prop_index(_get_db())
        _prop_index_cache = None  # 清快取，下次請求時重讀
        _prop_index_cache_time = 0
        # 重讀並回傳筆數
        index = _get_prop_index()
        return jsonify({"ok": True, "count": len(index)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/agent-emails", methods=["GET"])
def api_agent_emails_list():
    """列出所有經紀人 email 設定（僅管理員）。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可用"}), 403
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    try:
        docs = db.collection("agent_emails").stream()
        result = [{"id": d.id, **d.to_dict()} for d in docs]
        result.sort(key=lambda x: x.get("name", ""))
        return jsonify({"items": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/agent-emails", methods=["POST"])
def api_agent_emails_save():
    """新增或更新一筆經紀人 email（僅管理員）。body: {name, email}"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可用"}), 403
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    try:
        data = request.get_json(force=True) or {}
        name  = str(data.get("name", "")).strip()
        em    = str(data.get("email", "")).strip()
        if not name: return jsonify({"error": "請填寫經紀人姓名"}), 400
        db.collection("agent_emails").document(name).set({"name": name, "email": em})
        return jsonify({"ok": True, "name": name, "email": em})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/agent-emails/<name>", methods=["DELETE"])
def api_agent_emails_delete(name):
    """刪除一筆經紀人 email（僅管理員）。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    if not _is_admin(email): return jsonify({"error": "僅管理員可用"}), 403
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    try:
        db.collection("agent_emails").document(name).delete()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cp-presets", methods=["GET"])
def api_cp_presets_list():
    """列出目前登入者的所有篩選情境。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    try:
        docs = db.collection("cp_presets").where("created_by", "==", email).stream()
        items = []
        for d in docs:
            row = d.to_dict()
            row["id"] = d.id
            items.append(row)
        items.sort(key=lambda x: x.get("created_at", ""))
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cp-presets", methods=["POST"])
def api_cp_presets_create():
    """新增或覆蓋一個篩選情境（依 name 去重，同名則更新）。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    try:
        data = request.get_json(force=True) or {}
        name = str(data.get("name", "")).strip()
        if not name:
            return jsonify({"error": "請填寫情境名稱"}), 400
        params = data.get("params", {})  # 儲存篩選/排序參數
        now = datetime.now(timezone.utc).isoformat()
        # 查是否已有同名情境（同使用者）
        existing = list(db.collection("cp_presets")
                        .where("created_by", "==", email)
                        .where("name", "==", name)
                        .stream())
        if existing:
            doc_ref = db.collection("cp_presets").document(existing[0].id)
            doc_ref.update({"params": params, "updated_at": now})
            return jsonify({"id": existing[0].id, "updated": True})
        else:
            doc_ref = db.collection("cp_presets").add({
                "name":       name,
                "params":     params,
                "created_by": email,
                "created_at": now,
                "updated_at": now,
            })
            return jsonify({"id": doc_ref[1].id, "created": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cp-presets/<preset_id>", methods=["DELETE"])
def api_cp_presets_delete(preset_id):
    """刪除一個篩選情境（只能刪自己的）。"""
    email, err = _require_user()
    if err: return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None: return jsonify({"error": "Firestore 未連線"}), 503
    try:
        doc_ref = db.collection("cp_presets").document(preset_id)
        doc = doc_ref.get()
        if not doc.exists:
            return jsonify({"error": "找不到此情境"}), 404
        if doc.to_dict().get("created_by") != email:
            return jsonify({"error": "無權刪除他人情境"}), 403
        doc_ref.delete()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/notify-expiry", methods=["POST", "GET"])
def api_notify_expiry():
    """
    每日到期日通知：掃 Firestore，找委託到期日剛好等於今天 +30 或 +15 天的銷售中物件，
    依經紀人分組，查 Firestore agent_emails collection 取得 email，寄 Gmail 通知。
    由 Cloud Scheduler 每天早上 8 點觸發（GET 或 POST 皆可）。
    安全性：需要 X-Notify-Secret header 或 Admin session。
    """
    # 驗證：header secret 或 admin session
    secret = request.headers.get("X-Notify-Secret", "")
    notify_secret = os.environ.get("NOTIFY_SECRET", "")
    if notify_secret and secret != notify_secret:
        # 沒有 header secret 時，允許管理員從瀏覽器手動觸發
        email_s = session.get("user_email", "")
        if not _is_admin(email_s):
            return jsonify({"error": "未授權"}), 403

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    if not GMAIL_SENDER or not GMAIL_APP_PASS:
        return jsonify({"error": "Gmail 未設定（GMAIL_SENDER / GMAIL_APP_PASS）"}), 503

    try:
        today = date.today()
        target_days = {15, 30}  # 到期前幾天通知

        # 讀取 agent_emails collection：{經紀人名稱: email}
        agent_email_map = {}
        for doc in db.collection("agent_emails").stream():
            d = doc.to_dict()
            name = d.get("name", doc.id)
            em   = d.get("email", "")
            if name and em:
                agent_email_map[name] = em

        # 掃所有銷售中物件
        all_docs = list(db.collection("company_properties").stream())

        # 分組：{經紀人: [(物件名, 到期日, 剩餘天數), ...]}
        from collections import defaultdict
        agent_props = defaultdict(list)

        for doc in all_docs:
            d = doc.to_dict()
            if not _is_selling(d):
                continue
            exp_str = str(d.get("委託到期日", "") or "").strip()
            if not exp_str:
                continue

            # 解析到期日
            exp_date = None
            m = re.match(r'(\d+)[年/\-](\d{1,2})[月/\-](\d{1,2})', exp_str)
            if m:
                yr = int(m.group(1))
                if yr < 1000:
                    yr += 1911  # 民國轉西元
                try:
                    exp_date = date(yr, int(m.group(2)), int(m.group(3)))
                except Exception:
                    continue
            if exp_date is None:
                continue

            days_left = (exp_date - today).days
            if days_left not in target_days:
                continue

            agent = str(d.get("經紀人", "") or "").strip()
            name  = str(d.get("案名", "") or "（無案名）").strip()
            addr  = str(d.get("物件地址", "") or "").strip()
            price = d.get("售價(萬)", "")
            # 民國到期日顯示
            roc_yr = exp_date.year - 1911
            exp_label = f"{roc_yr}年{exp_date.month}月{exp_date.day}日"

            agent_props[agent].append({
                "案名": name, "地址": addr, "售價": price,
                "到期日": exp_label, "剩餘天數": days_left
            })

        if not agent_props:
            return jsonify({"message": "今日無到期通知", "sent": 0})

        # 寄信
        sent = 0
        errors = []
        context = ssl.create_default_context()

        for agent, props in agent_props.items():
            # 找對應 email（處理多人合寫如「陳威良 歐芷妤」）
            target_emails = []
            for ag_name, ag_email in agent_email_map.items():
                if ag_name in agent:
                    target_emails.append(ag_email)
            if not target_emails:
                errors.append(f"{agent}：找不到 email，跳過")
                continue

            # 組信件內容
            rows_30 = [p for p in props if p["剩餘天數"] == 30]
            rows_15 = [p for p in props if p["剩餘天數"] == 15]

            def _table(rows):
                lines = []
                for p in rows:
                    addr_str = f"　地址：{p['地址']}" if p['地址'] else ""
                    price_str = f"　售價：{p['售價']}萬" if p['售價'] else ""
                    lines.append(f"  • {p['案名']}（到期：{p['到期日']}）{addr_str}{price_str}")
                return "\n".join(lines)

            body_parts = []
            if rows_30:
                body_parts.append(f"【30 天後到期（{len(rows_30)} 筆）】\n{_table(rows_30)}")
            if rows_15:
                body_parts.append(f"【⚠️ 15 天後到期（{len(rows_15)} 筆）】\n{_table(rows_15)}")

            body = (
                f"您好 {agent}，\n\n"
                f"以下物件委託即將到期，請留意：\n\n"
                + "\n\n".join(body_parts)
                + "\n\n請儘早與屋主聯繫續約或更新委託狀態。\n\n— 日盛不動產物件系統"
            )

            subject = f"【委託到期提醒】{today.month}/{today.day} 共 {len(props)} 筆即將到期"

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"]    = GMAIL_SENDER
            msg["To"]      = ", ".join(target_emails)
            msg.attach(MIMEText(body, "plain", "utf-8"))

            try:
                with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
                    server.login(GMAIL_SENDER, GMAIL_APP_PASS)
                    server.sendmail(GMAIL_SENDER, target_emails, msg.as_string())
                sent += 1
            except Exception as e:
                errors.append(f"{agent}：寄信失敗 {e}")

        result = {"message": f"通知完成，共寄出 {sent} 封", "sent": sent}
        if errors:
            result["errors"] = errors
        return jsonify(result)

    except Exception as e:
        import logging
        logging.exception("notify-expiry 失敗")
        return jsonify({"error": str(e)}), 500


@app.route("/api/company-properties/options", methods=["GET"])
def api_company_properties_options():
    """回傳搜尋用的篩選選項（類別清單、地區清單、經紀人清單）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

    is_admin = _is_admin(email)
    org_info = _get_org_for_user(email)
    org_id = org_info["org_id"] if org_info else None
    if not is_admin and not org_id:
        return jsonify({"error": "您尚未加入任何組織，無法取得篩選選項。", "need_org": True}), 403

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    try:
        col = db.collection("company_properties")
        raw_categories = set()
        areas = set()
        agents = set()
        for doc in col.stream():
            d = doc.to_dict()
            # 只統計屬於自己組織的資料（or 尚未標記 org_id 的舊資料）
            if org_id and d.get("org_id") and d.get("org_id") != org_id:
                continue
            if d.get("物件類別"):
                raw_categories.add(d["物件類別"])
            if d.get("鄉/市/鎮"):
                areas.add(d["鄉/市/鎮"])
            # 拆分多人合寫（各種分隔符），取出個別姓名，排除委託編號
            import re as _re
            raw_ag = str(d.get("經紀人", ""))
            # 先用明確分隔符切割
            parts = _re.split(r'[/．、,，\s]+', raw_ag)
            for ag in parts:
                ag = ag.strip()
                # 排除含數字（委託編號）、超過4字（多半是合寫）、少於2字的
                if ag and 2 <= len(ag) <= 4 and not _re.search(r'\d', ag):
                    # 再用已知在線名單試拆（無分隔符的合寫）
                    matched = False
                    for known in ACTIVE_AGENTS:
                        if known in ag and len(ag) > len(known):
                            agents.add(known)
                            matched = True
                    if not matched:
                        agents.add(ag)

        # 把原始類別對應到大類
        # 有對應大類 → 顯示大類；不在任何大類 → 歸入「其他」
        all_known_cats = {c for cats in CATEGORY_GROUPS.values() for c in cats}
        display_categories = set(CATEGORY_GROUPS.keys())  # 固定顯示所有大類
        has_other = any(c not in all_known_cats for c in raw_categories)
        if has_other:
            display_categories.add(_OTHER_GROUP)

        # 大類固定順序，「其他」排最後
        group_order = list(CATEGORY_GROUPS.keys())
        def cat_sort_key(c):
            if c in group_order:
                return (0, group_order.index(c))
            if c == _OTHER_GROUP:
                return (2, c)
            return (1, c)

        # 地區：依排序表排序，並附上完整顯示名稱
        sorted_raw_areas = sorted(areas, key=_area_sort_key)
        area_options = [
            {"value": a, "label": AREA_DISPLAY.get(a, a)}
            for a in sorted_raw_areas
        ]

        # 經紀人：在線人員置頂，其他排後
        active_found   = [a for a in ACTIVE_AGENTS if a in agents]
        inactive_found = sorted(agents - set(ACTIVE_AGENTS))

        return jsonify({
            "categories": sorted(display_categories, key=cat_sort_key),
            "areas": area_options,   # [{value: 簡寫, label: 完整名稱}]
            "agents": {              # 分群，前端用 <optgroup> 呈現
                "active":   active_found,    # 在線人員（保持 ACTIVE_AGENTS 順序）
                "inactive": inactive_found   # 其他人員（字母排序）
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _parse_price(val):
    """把售價欄位轉為 float，失敗回傳 None。"""
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").strip())
    except Exception:
        return None


# ── Gemini 圖片辨識（直接呼叫） ──
_EXTRACT_SYSTEM = (
    "你是房產截圖分析專家。"
    "規則：輸出格式僅 JSON；語言繁體中文（台灣）；"
    "數值欄位（price、building_ping、land_ping、authority_ping）必須為純數字。"
    "請只回傳 JSON，不要 markdown 標記。"
)
_EXTRACT_PROMPT = (
    '請從圖片中擷取房產物件資訊，輸出以下 JSON 格式（若無資料則留空字串或 null）：\n'
    '{"project_name":"物件名稱","address":"完整地址","price":1800,"building_ping":10.5,'
    '"land_ping":15.2,"authority_ping":25.7,"layout":"3房2廳2衛","floor":"3樓/共5樓",'
    '"age":"5年","parking":"有","case_number":"A123456","location_area":"台北市"}\n'
    '注意：price、building_ping、land_ping、authority_ping 必須是純數字。'
    '請務必使用真實的物件名稱，不要輸出「物件名稱」這四個字。'
)


def _gemini_extract_image(raw_bytes, mime):
    """用 Gemini 辨識圖片，回傳 extracted dict。失敗拋 RuntimeError。"""
    if not _GEMINI_OK or not _genai:
        raise RuntimeError("未設定 GOOGLE_API_KEY，無法使用圖片辨識")
    prompt = _EXTRACT_SYSTEM + "\n\n" + _EXTRACT_PROMPT
    mime = mime or "image/jpeg"

    if _GEMINI_SDK == "new":
        # 新版 google.genai SDK
        client = _genai.Client(api_key=_GEMINI_KEY)
        image_part = _genai.types.Part.from_bytes(data=raw_bytes, mime_type=mime)
        resp = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=[prompt, image_part],
            config=_genai.types.GenerateContentConfig(response_mime_type="application/json"),
        )
        text = resp.text
    else:
        # 舊版 google.generativeai SDK
        image_part = _genai.types.Part.from_data(data=raw_bytes, mime_type=mime)
        model = _genai.GenerativeModel("gemini-2.0-flash")
        cfg = _genai.types.GenerationConfig(response_mime_type="application/json")
        resp = model.generate_content([prompt, image_part], generation_config=cfg)
        text = resp.text

    parsed = json.loads(text)
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
        parsed = parsed[0]
    return parsed


@app.route("/api/extract-from-image", methods=["POST"])
def api_extract_from_image():
    """圖片辨識：直接呼叫 Gemini，回傳 extracted 物件欄位。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    f = request.files.get("image")
    if not f or f.filename == "":
        return jsonify({"error": "請上傳或貼上圖片"}), 400
    raw = f.read()
    if not raw:
        return jsonify({"error": "圖片為空"}), 400
    try:
        extracted = _gemini_extract_image(raw, f.mimetype or "image/jpeg")
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503
    except Exception as e:
        return jsonify({"error": f"辨識失敗：{e}"}), 502
    return jsonify({"ok": True, "extracted": extracted})


# 儲存非同步截圖工作結果（記憶體，重啟即清空）
_screenshot_jobs: dict = {}


def _decode_punycode_url(url: str) -> str:
    """將 punycode 域名（xn--xxx）轉回 Unicode（中文域名），避免 Screenshotone 拒絕。"""
    try:
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname or ""
        # 若域名含 xn-- 段落才需要轉換
        if "xn--" in host:
            decoded_host = host.encode("ascii").decode("idna")
            # 重組 URL，替換 host 部分
            netloc = parsed.netloc.replace(host, decoded_host)
            url = urllib.parse.urlunparse(parsed._replace(netloc=netloc))
    except Exception:
        pass  # 轉換失敗就用原始 URL
    return url


def _run_screenshot_job(job_id: str, url: str):
    """背景執行截圖 + Gemini 辨識，結果存入 _screenshot_jobs。"""
    import requests as _req
    url = _decode_punycode_url(url)  # 確保域名是 Unicode 格式
    try:
        params = {
            "access_key": SCREENSHOTONE_KEY,
            "url": url,
            "format": "jpg",
            "image_quality": 85,
            "viewport_width": 1280,
            "viewport_height": 1800,   # 加高，截到更多內容
            "full_page": "true",        # 完整頁面截圖
            "block_ads": "true",
            "block_cookie_banners": "true",
            "ignore_host_errors": "true",
            "delay": 4,                 # 等待 JS 渲染（網頁動態內容需要時間）
            "timeout": 40,
        }
        resp = _req.get("https://api.screenshotone.com/take", params=params, timeout=35)
        if resp.status_code != 200:
            try:
                msg = resp.json().get("message", "截圖失敗")
            except Exception:
                msg = f"截圖服務回傳 {resp.status_code}"
            _screenshot_jobs[job_id] = {"done": True, "error": msg}
            return
        raw_bytes = resp.content
        if not raw_bytes:
            _screenshot_jobs[job_id] = {"done": True, "error": "截圖無內容"}
            return
    except Exception as e:
        _screenshot_jobs[job_id] = {"done": True, "error": f"截圖失敗：{e}"}
        return
    try:
        extracted = _gemini_extract_image(raw_bytes, "image/jpeg")
        # 把截圖 base64 也存入，前端 console 可用 img.src = 'data:image/jpeg;base64,...' 查看
        img_b64 = base64.b64encode(raw_bytes).decode()
        _screenshot_jobs[job_id] = {"done": True, "ok": True, "extracted": extracted, "debug_img": img_b64}
    except Exception as e:
        _screenshot_jobs[job_id] = {"done": True, "error": f"辨識失敗：{e}"}


@app.route("/api/extract-from-url", methods=["POST"])
def api_extract_from_url():
    """網址截圖辨識（非同步）：立即回傳 job_id，前端輪詢 /api/extract-from-url/poll/<job_id>。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    if not SCREENSHOTONE_KEY:
        return jsonify({"error": "未設定截圖服務 API Key"}), 503
    if not _GEMINI_OK:
        return jsonify({"error": "未設定 GOOGLE_API_KEY，無法辨識"}), 503
    data = request.get_json() or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "請提供網址"}), 400
    if not url.startswith("http://") and not url.startswith("https://"):
        return jsonify({"error": "網址須為 http:// 或 https://"}), 400
    job_id = str(uuid.uuid4())
    _screenshot_jobs[job_id] = {"done": False}
    t = threading.Thread(target=_run_screenshot_job, args=(job_id, url), daemon=True)
    t.start()
    return jsonify({"job_id": job_id})


@app.route("/api/extract-from-url/poll/<job_id>", methods=["GET"])
def api_extract_from_url_poll(job_id):
    """輪詢截圖辨識工作結果。"""
    _, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    job = _screenshot_jobs.get(job_id)
    if job is None:
        return jsonify({"error": "工作不存在或已過期"}), 404
    return jsonify(job)


# ── AD 歷史代理（資料在 AD 服務，保留代理） ──

def _portal_api_get(path, email):
    """後端代理 GET Portal API，回傳 (data_dict, status_code)。"""
    if not PORTAL_URL:
        return {"error": "未設定 PORTAL_URL"}, 503
    url = PORTAL_URL.rstrip("/") + path + ("?email=" + urllib.request.quote(email))
    req = urllib.request.Request(url, headers={"X-Service-Key": SERVICE_API_KEY})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode()), resp.status
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode()), e.code
        except Exception:
            return {"error": f"Portal 回應錯誤 {e.code}"}, e.code
    except Exception as e:
        return {"error": f"連線失敗：{e}"}, 502


def _portal_api_post_json(path, email, payload):
    """後端代理 POST JSON 至 Portal API，回傳 (data_dict, status_code)。"""
    if not PORTAL_URL:
        return {"error": "未設定 PORTAL_URL"}, 503
    url = PORTAL_URL.rstrip("/") + path + "?email=" + urllib.request.quote(email)
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"X-Service-Key": SERVICE_API_KEY, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode()), resp.status
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode()), e.code
        except Exception:
            return {"error": f"Portal 回應錯誤 {e.code}"}, e.code
    except Exception as e:
        return {"error": f"連線失敗：{e}"}, 502


@app.route("/api/proxy/ad-history-list", methods=["GET"])
def proxy_ad_history_list():
    """代理：取得 AD 歷史列表（AD 歷史資料在 AD 服務，保留代理）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    data, code = _portal_api_get("/api/properties/ad-history-list", email)
    return jsonify(data), code


@app.route("/api/proxy/import-from-ad-history", methods=["POST"])
def proxy_import_from_ad_history():
    """代理：從 AD 歷史匯入為物件（AD 歷史資料在 AD 服務，保留代理）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    data, code = _portal_api_post_json(
        "/api/properties/import-from-ad-history", email, request.get_json() or {}
    )
    return jsonify(data), code


@app.route("/api/objects/for-service-selling", methods=["GET"])
def api_objects_for_service_selling():
    """供 AD 以 X-Service-Key 列出銷售中的公司物件（欄位轉為英文名）。"""
    if not _verify_service_key():
        return jsonify({"error": "需要有效的 X-Service-Key"}), 401
    db = _get_db()
    if not db:
        return jsonify({"items": []})
    try:
        docs = db.collection("company_properties").stream()
        items = []
        for doc in docs:
            r = doc.to_dict()
            if not _is_selling(r):
                continue
            # 案名為空的記錄略過（避免廣告工具出現「未命名」）
            if not str(r.get("案名", "") or "").strip():
                continue
            # 建坪：公寓優先用「室內坪」，房屋用「建坪」
            building = r.get("建坪") or r.get("室內坪") or ""
            items.append({
                "id":            doc.id,
                "project_name":  str(r.get("案名", "") or ""),
                "address":       str(r.get("物件地址", "") or ""),
                "price":         r.get("售價(萬)", ""),
                "building_ping": building,
                "land_ping":     r.get("地坪", ""),
                "authority_ping": str(r.get("權狀坪數", "") or ""),
                "layout":        str(r.get("格局", "") or ""),
                "floor":         str(r.get("樓層", "") or r.get("樓別", "") or ""),
                "age":           str(r.get("屋齡", "") or ""),
                "parking":       str(r.get("車位", "") or ""),
                "case_number":   str(r.get("委託編號", "") or ""),
                "location_area": str(r.get("鄉/市/鎮", "") or ""),
            })
        items.sort(key=lambda x: x["project_name"])
        return jsonify({"items": items})
    except Exception as e:
        import logging
        logging.warning("Library: for-service-selling 失敗: %s", e)
        return jsonify({"items": [], "error": str(e)})


@app.route("/api/company-properties/expiring", methods=["GET"])
def api_company_properties_expiring():
    """晨報專用：以 X-Service-Key 取得 N 天內即將到期的委託物件。
    Query: email=xxx, days=7（預設 7 天）"""
    if not _verify_service_key():
        return jsonify({"error": "需要有效的 X-Service-Key"}), 401
    days = int(request.args.get("days", 7))
    db = _get_db()
    if not db:
        return jsonify({"items": []})
    try:
        from datetime import datetime, timedelta
        today_str = datetime.now().strftime("%Y-%m-%d")
        future_str = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
        docs = db.collection("company_properties").stream()
        items = []
        for doc in docs:
            r = doc.to_dict()
            # 只看銷售中的物件
            if not _is_selling(r):
                continue
            exp_date = _parse_expiry_key(r)
            # 到期日在今天～N天後之間
            if today_str <= exp_date <= future_str:
                items.append({
                    "案名": r.get("案名", ""),
                    "經紀人": r.get("經紀人", ""),
                    "委託到期日": r.get("委託到期日", ""),
                    "委託編號": r.get("委託編號", ""),
                    "物件地址": r.get("物件地址", ""),
                })
        # 按到期日排序（近→遠）
        items.sort(key=lambda x: _parse_expiry_key(x))
        return jsonify({"items": items})
    except Exception as e:
        import logging
        logging.warning("Library: expiring 查詢失敗: %s", e)
        return jsonify({"items": [], "error": str(e)})


# ══ 資料庫檢視 API（管理員限定）══

# company_properties 集合的欄位順序（與 Google Sheets 一致）
_COMPANY_PROP_COL_ORDER = [
    "資料序號", "委託編號", "委託日", "案名", "所有權人", "身份証字號",
    "室內電話1", "行動電話1", "通訊住址", "連絡人姓名", "連絡人與所有權人關係",
    "連絡人室內電話2", "連絡人行動電話2", "物件類別", "物件地址", "鄉/市/鎮",
    "段別", "地號", "建號", "座向", "竣工日期", "格局", "現況", "地坪", "建坪",
    "樓別", "管理費(元)", "車位", "委託價(萬)", "售價(萬)", "現有貸款(萬)",
    "債權人", "售屋原因", "委託到期日", "經紀人", "契變", "備註", "成交日期",
    "成交金額(萬)", "買方姓名", "買方電話", "買方住址", "備註1", "買方生日",
    "賣方生日", "欄位1", "銷售中", "備用",
]

@app.route("/api/firestore/collections")
def api_firestore_collections():
    """列出 Firestore 所有頂層集合名稱"""
    email = session.get("user_email")
    if not email or not _is_admin(email):
        return jsonify({"error": "僅管理員可用"}), 403
    db = _get_db()
    # 取得所有頂層集合
    cols = [c.id for c in db.collections()]
    cols.sort()
    return jsonify({"collections": cols})


@app.route("/api/firestore/browse")
def api_firestore_browse():
    """讀取指定集合的文件，以表格方式呈現"""
    email = session.get("user_email")
    if not email or not _is_admin(email):
        return jsonify({"error": "僅管理員可用"}), 403

    collection = request.args.get("collection", "").strip()
    if not collection:
        return jsonify({"error": "請指定集合名稱"}), 400

    keyword = request.args.get("keyword", "").strip()
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(200, max(10, int(request.args.get("per_page", 50))))

    db = _get_db()
    docs = []
    for doc in db.collection(collection).stream():
        d = doc.to_dict() or {}
        d["__doc_id__"] = doc.id
        docs.append(d)

    # 關鍵字搜尋：在所有欄位值中搜尋
    if keyword:
        kw = keyword.lower()
        filtered = []
        for d in docs:
            for v in d.values():
                if kw in str(v).lower():
                    filtered.append(d)
                    break
        docs = filtered

    total = len(docs)

    # 收集所有欄位名（動態）
    all_keys = set()
    for d in docs:
        all_keys.update(d.keys())
    all_keys.discard("__doc_id__")

    if collection == "company_properties":
        # company_properties：按照 Sheets 欄位順序，資料序號排第一
        # 先放已定義的欄位（依順序），再放其餘未知欄位（字母排序），最後放文件 ID
        ordered = [c for c in _COMPANY_PROP_COL_ORDER if c in all_keys]
        remaining = sorted(all_keys - set(_COMPANY_PROP_COL_ORDER))
        columns = ordered + remaining + ["__doc_id__"]
    else:
        # 其他集合：__doc_id__ 放最前面，其他字母排序
        columns = ["__doc_id__"] + sorted(all_keys)

    # 分頁
    start = (page - 1) * per_page
    end = start + per_page
    page_docs = docs[start:end]

    # 把值轉成字串方便前端顯示
    rows = []
    for d in page_docs:
        row = {}
        for col in columns:
            val = d.get(col, "")
            if isinstance(val, dict) or isinstance(val, list):
                row[col] = json.dumps(val, ensure_ascii=False, default=str)[:200]
            else:
                row[col] = str(val) if val is not None else ""
        rows.append(row)

    return jsonify({
        "collection": collection,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
        "columns": columns,
        "rows": rows,
    })


@app.route("/api/general-feedback", methods=["GET"])
def api_general_feedback_get():
    """列出所有通用反饋"""
    return jsonify(_load_general_feedback())


@app.route("/api/general-feedback", methods=["POST"])
def api_general_feedback():
    """通用反饋"""
    data = request.get_json() or {}
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"error": "請輸入意見內容"}), 400

    entries = _load_general_feedback()
    entries.append({
        "text": text,
        "category": data.get("category", ""),
        "created_at": datetime.now().isoformat(),
    })
    data_str = json.dumps(entries, ensure_ascii=False, indent=2)

    if GCS_BUCKET:
        _gcs_write("general_feedback.json", data_str)
    else:
        _atomic_write(GENERAL_FEEDBACK_FILE, data_str)

    return jsonify({"ok": True, "total": len(entries)})


# ══════════════════════════════════════════════════════════════════
# 準賣方管理 API
# Firestore 集合：seller_prospects（主資料）、seller_contacts（互動記事）
# 每筆資料以 created_by = email 區分個人資料
# ══════════════════════════════════════════════════════════════════

def _recalc_seller_last_contact(db, seller_id):
    """重新計算準賣方的最後追蹤時間（from seller_contacts）。"""
    try:
        contacts = list(db.collection("seller_contacts").where("seller_id", "==", seller_id).stream())
        if not contacts:
            db.collection("seller_prospects").document(seller_id).update({"last_contact_at": None})
            return None
        items = [c.to_dict() for c in contacts]
        last = max(items, key=lambda x: x.get("contact_at", ""))
        last_contact_at = last.get("contact_at")
        db.collection("seller_prospects").document(seller_id).update({"last_contact_at": last_contact_at})
        return last_contact_at
    except Exception:
        return None


@app.route("/api/sellers/sort-order", methods=["GET"])
def api_sellers_sort_order_get():
    """取得準賣方卡片排列順序。從 people.sort_order 推。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        items = seller_facade.list_sellers(db, email, _is_admin(email))
        order = [s["id"] for s in items if s.get("sort_order") is not None]
        return jsonify({"order": order})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/sort-order", methods=["PUT"])
def api_sellers_sort_order_put():
    """儲存準賣方卡片排列順序。寫到每筆 people.sort_order。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        data = request.get_json(force=True) or {}
        order = data.get("order", [])
        if not isinstance(order, list):
            return jsonify({"error": "order 格式不正確"}), 400
        for idx, pid in enumerate(order):
            if not pid:
                continue
            try:
                db.collection("people").document(pid).update({
                    "sort_order": (idx + 1) * 10,
                    "updated_at": _server_ts(),
                })
            except Exception:
                pass
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers", methods=["GET"])
def api_sellers_list():
    """取得準賣方清單。資料源：people（active_roles 含 'seller'）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        items = seller_facade.list_sellers(db, email, _is_admin(email))
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers", methods=["POST"])
def api_sellers_create():
    """新增準賣方 = 建 person + 建 roles/seller。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        data = request.get_json(silent=True) or {}
        person_id = seller_facade.create_seller(db, email, data, _server_ts)
        result = seller_facade.get_seller(db, person_id, email, _is_admin(email))
        return jsonify({"ok": True, "id": person_id, **(result or {})})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ↓ 舊版 create 函式 body 不再使用 ↓
def _api_sellers_create_legacy_unused():
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    data = request.get_json(silent=True) or {}
    name = str(data.get("name", "")).strip()
    if not name:
        return jsonify({"error": "請填寫屋主姓名"}), 400
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        doc = {
            "name":          name,
            "phone":         str(data.get("phone", "")).strip(),
            "address":       str(data.get("address", "")).strip(),
            "land_number":   str(data.get("land_number", "")).strip(),
            "category":      str(data.get("category", "")).strip(),
            "owner_price":   data.get("owner_price"),      # 屋主期望售價（萬）
            "suggest_price": data.get("suggest_price"),    # 房仲建議售價（萬）
            "source":        str(data.get("source", "")).strip(),
            "status":        data.get("status", "培養中"),
            "note":          str(data.get("note", "")).strip(),
            "last_contact_at": None,
            "created_by":    email,
            "created_at":    now,
            "updated_at":    now,
        }
        ref = db.collection("seller_prospects").document()
        ref.set(doc)
        return jsonify({"ok": True, "id": ref.id, **doc})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/<seller_id>", methods=["GET"])
def api_seller_get(seller_id):
    """取得單筆準賣方資料（從 people + roles/seller 拼）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        result = seller_facade.get_seller(db, seller_id, email, _is_admin(email))
        if result is None:
            return jsonify({"error": "找不到此準賣方"}), 404
        if result is False:
            return jsonify({"error": "無權限"}), 403
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/<seller_id>", methods=["PUT"])
def api_seller_update(seller_id):
    """更新準賣方資料：寫進 people 主檔 + roles/seller。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        data = request.get_json(silent=True) or {}
        result = seller_facade.update_seller(db, seller_id, email, _is_admin(email), data, _server_ts)
        if result is None:
            return jsonify({"error": "找不到此準賣方"}), 404
        if result is False:
            return jsonify({"error": "無權限"}), 403
        return jsonify({"ok": True, "id": seller_id, **(seller_facade.get_seller(db, seller_id, email, _is_admin(email)) or {})})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/<seller_id>", methods=["DELETE"])
def api_seller_delete(seller_id):
    """刪除準賣方。Query: mode=tag_only（撕標籤）或 mode=full（連人脈一起刪）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    mode = (request.args.get("mode") or "tag_only").strip()
    try:
        if mode == "full":
            result = seller_facade.soft_delete_person(db, seller_id, email, _is_admin(email), _server_ts)
        else:
            result = seller_facade.archive_seller(db, seller_id, email, _is_admin(email), _server_ts)
        if result is None:
            return jsonify({"error": "找不到此準賣方"}), 404
        if result is False:
            return jsonify({"error": "無權限"}), 403
        return jsonify({"ok": True, "mode": mode})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── 準賣方互動記事（讀寫 people/{pid}/contacts/）──

def _verify_seller_owner(db, person_id, email):
    """驗證 person 存在 + 權限。回傳 (person_dict, error_response)。"""
    ref = db.collection("people").document(person_id)
    snap = ref.get()
    if not snap.exists:
        return None, (jsonify({"error": "找不到此準賣方"}), 404)
    person = snap.to_dict() or {}
    if not _is_admin(email) and person.get("created_by") != email:
        return None, (jsonify({"error": "無權限"}), 403)
    return person, None


def _recalc_person_last_contact(db, person_id):
    """重新計算 last_contact_at（from people/{pid}/contacts）。"""
    try:
        contacts = list(db.collection("people").document(person_id).collection("contacts").stream())
        if not contacts:
            db.collection("people").document(person_id).update({"last_contact_at": None})
            return None
        def _ct(d):
            v = (d.to_dict() or {}).get("contact_at")
            return v.isoformat() if hasattr(v, "isoformat") else (v or "")
        latest = max(contacts, key=_ct)
        last = (latest.to_dict() or {}).get("contact_at")
        db.collection("people").document(person_id).update({"last_contact_at": last})
        return last
    except Exception:
        return None


@app.route("/api/sellers/<seller_id>/contacts", methods=["GET"])
def api_seller_contacts_list(seller_id):
    """取得準賣方互動記事（從 people/{pid}/contacts/）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        person, err_resp = _verify_seller_owner(db, seller_id, email)
        if err_resp:
            return err_resp
        items = []
        for d in db.collection("people").document(seller_id).collection("contacts").stream():
            data = d.to_dict() or {}
            ca = data.get("contact_at")
            items.append({
                "id": d.id,
                "seller_id": seller_id,
                "content": data.get("content"),
                "contact_at": ca.isoformat() if hasattr(ca, "isoformat") else ca,
                "created_by": data.get("created_by"),
            })
        items.sort(key=lambda x: x.get("contact_at") or "", reverse=True)
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/<seller_id>/contacts", methods=["POST"])
def api_seller_contact_create(seller_id):
    """新增互動記事 → people/{pid}/contacts/。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        person, err_resp = _verify_seller_owner(db, seller_id, email)
        if err_resp:
            return err_resp
        data = request.get_json(silent=True) or {}
        content = str(data.get("content", "")).strip()
        if not content:
            return jsonify({"error": "請填寫互動內容"}), 400
        contact_at = (data.get("contact_at") or "").strip()
        if not contact_at:
            contact_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c_ref = db.collection("people").document(seller_id).collection("contacts").document()
        c_ref.set({
            "content": content,
            "contact_at": contact_at,
            "via": "other",
            "created_by": email,
            "created_at": _server_ts(),
        })
        _recalc_person_last_contact(db, seller_id)
        return jsonify({"ok": True, "id": c_ref.id, "seller_id": seller_id,
                        "content": content, "contact_at": contact_at,
                        "created_by": email})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/<seller_id>/contacts/<contact_id>", methods=["PUT"])
def api_seller_contact_update(seller_id, contact_id):
    """修改互動記事。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        person, err_resp = _verify_seller_owner(db, seller_id, email)
        if err_resp:
            return err_resp
        c_ref = db.collection("people").document(seller_id).collection("contacts").document(contact_id)
        csnap = c_ref.get()
        if not csnap.exists:
            return jsonify({"error": "找不到此記事"}), 404
        cdata = csnap.to_dict() or {}
        if cdata.get("created_by") != email and not _is_admin(email):
            return jsonify({"error": "無權限"}), 403
        data = request.get_json(silent=True) or {}
        update = {
            "content": str(data.get("content", cdata.get("content", ""))).strip(),
            "contact_at": (data.get("contact_at") or cdata.get("contact_at", "")),
        }
        if hasattr(update["contact_at"], "strip"):
            update["contact_at"] = update["contact_at"].strip()
        c_ref.update(update)
        _recalc_person_last_contact(db, seller_id)
        return jsonify({"ok": True, "id": contact_id, **update})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/<seller_id>/contacts/<contact_id>", methods=["DELETE"])
def api_seller_contact_delete(seller_id, contact_id):
    """刪除互動記事。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        person, err_resp = _verify_seller_owner(db, seller_id, email)
        if err_resp:
            return err_resp
        c_ref = db.collection("people").document(seller_id).collection("contacts").document(contact_id)
        csnap = c_ref.get()
        if not csnap.exists:
            return jsonify({"error": "找不到此記事"}), 404
        cdata = csnap.to_dict() or {}
        if cdata.get("created_by") != email and not _is_admin(email):
            return jsonify({"error": "無權限"}), 403
        c_ref.delete()
        _recalc_person_last_contact(db, seller_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── 準賣方頭像上傳 ──

@app.route("/api/sellers/<seller_id>/avatar", methods=["POST"])
def api_seller_avatar_upload(seller_id):
    """上傳準賣方頭像。前端應已縮成 160x160 JPEG 並 base64，
    body: {avatar_b64: 'data:image/jpeg;base64,...'} 直接寫進 people.avatar_b64。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        person, err_resp = _verify_seller_owner(db, seller_id, email)
        if err_resp:
            return err_resp

        # 接受 JSON {avatar_b64: ...}（新版前端）或 multipart file（後備）
        avatar_b64 = None
        if request.is_json:
            data = request.get_json(silent=True) or {}
            avatar_b64 = data.get("avatar_b64")
        elif "file" in request.files:
            f = request.files["file"]
            ext = os.path.splitext((f.filename or "").lower())[1]
            mime_map = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
                        "webp": "image/webp", "gif": "image/gif"}
            mime = mime_map.get(ext.lstrip("."), "image/jpeg")
            raw = f.read()
            # 1MB 上限保護（Firestore 文件欄位限制）
            if len(raw) > 700 * 1024:
                return jsonify({"error": "圖片太大，請改用較小檔案或重整頁面（前端會自動縮圖）"}), 400
            avatar_b64 = "data:" + mime + ";base64," + base64.b64encode(raw).decode("ascii")

        if not avatar_b64:
            return jsonify({"error": "請提供 avatar_b64 或檔案"}), 400

        db.collection("people").document(seller_id).update({
            "avatar_b64": avatar_b64,
            "updated_at": _server_ts(),
        })
        return jsonify({"ok": True, "url": avatar_b64})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── 準賣方相關圖檔（讀寫 people/{pid}/files/）──

@app.route("/api/sellers/<seller_id>/files", methods=["POST"])
def api_seller_file_upload(seller_id):
    """上傳相關圖檔到 GCS，metadata 存入 people/{pid}/files/ 子集合。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    if "file" not in request.files:
        return jsonify({"error": "請選擇檔案"}), 400
    f = request.files["file"]
    ext = os.path.splitext(f.filename.lower())[1]
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".pdf"):
        return jsonify({"error": "僅支援 jpg/png/webp/gif/pdf"}), 400
    try:
        person, err_resp = _verify_seller_owner(db, seller_id, email)
        if err_resp:
            return err_resp

        raw = f.read()
        b = _get_gcs_bucket()
        if b is None:
            return jsonify({"error": "GCS 未設定"}), 503
        file_id = str(uuid.uuid4())[:8]
        safe_name = re.sub(r"[^\w.\-]", "_", f.filename)
        gcs_path = f"people-files/{email}/{seller_id}/{file_id}_{safe_name}"
        blob = b.blob(gcs_path)
        mime_map = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
                    "webp": "image/webp", "gif": "image/gif", "pdf": "application/pdf"}
        mime = mime_map.get(ext.lstrip("."), "application/octet-stream")
        blob.upload_from_string(raw, content_type=mime)
        blob.make_public()
        url = blob.public_url
        # 寫到 people/{pid}/files/{file_id}
        db.collection("people").document(seller_id).collection("files").document(file_id).set({
            "id": file_id,
            "filename": safe_name,
            "url": url,
            "gcs_path": gcs_path,
            "mime_type": mime,
            "uploaded_at": _server_ts(),
            "uploaded_by": email,
        })
        db.collection("people").document(seller_id).update({"updated_at": _server_ts()})
        return jsonify({"ok": True, "file_id": file_id, "name": safe_name,
                        "url": url, "gcs_path": gcs_path,
                        "uploaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/<seller_id>/files/<file_id>", methods=["DELETE"])
def api_seller_file_delete(seller_id, file_id):
    """刪除相關圖檔（GCS + people/{pid}/files/{file_id}）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        person, err_resp = _verify_seller_owner(db, seller_id, email)
        if err_resp:
            return err_resp
        f_ref = db.collection("people").document(seller_id).collection("files").document(file_id)
        fsnap = f_ref.get()
        if not fsnap.exists:
            return jsonify({"error": "找不到此檔案"}), 404
        gcs_path = (fsnap.to_dict() or {}).get("gcs_path")
        if gcs_path:
            b = _get_gcs_bucket()
            if b:
                try:
                    b.blob(gcs_path).delete()
                except Exception:
                    pass
        f_ref.delete()
        db.collection("people").document(seller_id).update({"updated_at": _server_ts()})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    email = session.get("user_email")
    if not email:
        return redirect(PORTAL_URL or "/") if PORTAL_URL else "<h1>請從入口登入</h1>"
    return _render_app()


def _load_general_feedback():
    """讀取通用反饋列表"""
    if GCS_BUCKET:
        try:
            content = _gcs_read("general_feedback.json")
            return json.loads(content) if content else []
        except:
            return []
    else:
        if os.path.exists(GENERAL_FEEDBACK_FILE):
            try:
                with open(GENERAL_FEEDBACK_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except:
                return []
        return []


def _atomic_write(fpath, data_str):
    """原子寫入：先寫 .tmp，fsync 後再 os.replace，讀取時永遠是完整檔案。"""
    os.makedirs(os.path.dirname(fpath), exist_ok=True)
    tmp = fpath + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data_str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, fpath)


def _render_app():
    name = session.get("user_name") or session.get("user_email") or "使用者"
    email = session.get("user_email", "")
    portal_link = PORTAL_URL or "#"
    is_admin = _is_admin(email)
    fields = _field_key_label()

    # 生成管理員用戶選擇列
    if is_admin:
        admin_bar = (
            '<div class="flex items-center gap-3 px-2 py-2 mb-3 rounded-xl text-sm" style="background:var(--bg-t);border:1px solid var(--bd);color:var(--txs);">'
            '<span>查看用戶：</span>'
            '<select id="userSelect" class="rounded-lg px-3 py-1 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">'
            '<option value="">載入中…</option>'
            '</select>'
            '</div>'
        )
    else:
        admin_bar = ""

    # 生成編輯表單的欄位 HTML
    wide_keys = ("env_description", "survey_summary", "address")
    textarea_keys = ("env_description", "survey_summary")
    fields_html_parts = []
    for key, label in fields:
        span_class = "sm:col-span-2" if key in wide_keys else ""
        div_class = f'<div class="{span_class}">' if span_class else "<div>"
        lbl = f'<label class="block text-xs mb-1" style="color:var(--txs);" for="f_{key}">{label}</label>'
        if key in textarea_keys:
            inp = (f'<textarea id="f_{key}" name="{key}" rows="3"'
                   f' class="w-full rounded-lg px-3 py-2 text-sm resize-none focus:outline-none"'
                   f' style="background:var(--bg-t);border:1px solid var(--bd);color:var(--tx);"'
                   f' placeholder="{label}"></textarea>')
        else:
            inp = (f'<input type="text" id="f_{key}" name="{key}"'
                   f' class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none"'
                   f' style="background:var(--bg-t);border:1px solid var(--bd);color:var(--tx);"'
                   f' placeholder="{label}">')
        fields_html_parts.append(f"{div_class}{lbl}{inp}</div>")
    fields_html = "\n        ".join(fields_html_parts)

    # 用 Python 字串替換，完全避免 Jinja2 誤解析 JS {} 語法
    html = OBJECTS_APP_HTML
    html = html.replace("__PORTAL_LINK__", portal_link)
    html = html.replace("__BUYER_URL_STR__", BUYER_URL)
    html = html.replace("__FIELDS_JSON__", json.dumps(fields, ensure_ascii=False))
    html = html.replace("__IS_ADMIN_JSON__", json.dumps(is_admin))
    html = html.replace("__ADMIN_BAR__", admin_bar)
    html = html.replace("__FIELDS_HTML__", fields_html)
    html = html.replace("__BUYER_URL__", json.dumps(BUYER_URL))
    # 加 Cache-Control 標頭，禁止瀏覽器快取動態 HTML 頁面
    from flask import make_response
    resp = make_response(html)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


OBJECTS_APP_HTML = """
<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <link rel="icon" type="image/png" href="/static/favicon.png">
  <link rel="apple-touch-icon" href="/static/logo.png">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>物件庫 - 房仲 AI 工具平台</title>
  <!-- 立刻清除 URL 中的 token，避免 Tailwind CDN 掃描到長字串造成 SyntaxError -->
  <script>if(location.pathname.indexOf('/auth/')>=0||location.search.indexOf('token=')>=0){history.replaceState(null,'','/');}</script>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    #toast-container{position:fixed;top:1rem;right:1rem;z-index:9999;display:flex;flex-direction:column;gap:.5rem;pointer-events:none}
    .toast-item{padding:.6rem 1rem;border-radius:.75rem;font-size:.875rem;font-weight:500;box-shadow:0 4px 16px rgba(0,0,0,.4);opacity:1;transition:opacity .3s;pointer-events:none}
    .toast-success{background:#059669;color:#fff}
    .toast-error{background:#dc2626;color:#fff}
    .toast-info{background:#2563eb;color:#fff}
    .toast-out{opacity:0}
    /* 複選下拉面板 */
    #cp-cat-panel,#cp-area-panel,#cp-agent-panel{scrollbar-width:thin;scrollbar-color:#475569 transparent}
    #cp-cat-panel::-webkit-scrollbar,#cp-area-panel::-webkit-scrollbar,#cp-agent-panel::-webkit-scrollbar{width:4px}
    #cp-cat-panel::-webkit-scrollbar-thumb,#cp-area-panel::-webkit-scrollbar-thumb,#cp-agent-panel::-webkit-scrollbar-thumb{background:#475569;border-radius:2px}
    /* 到期警示動畫 */
    @keyframes pulse-warn{0%,100%{opacity:1}50%{opacity:.6}}
    .animate-pulse{animation:pulse-warn 2s ease-in-out infinite}
    /* ── 通用意見反饋 ── */
    .gf-overlay {
      display: none;
      position: fixed;
      inset: 0;
      background: rgba(0,0,0,0.5);
      z-index: 2000;
      justify-content: center;
      align-items: center;
    }
    .gf-overlay.show { display: flex; }
    .gf-dialog {
      background: var(--card);
      border: 1px solid var(--input-border);
      border-radius: 12px;
      padding: 1.5rem;
      width: 90%;
      max-width: 420px;
      box-shadow: 0 8px 32px rgba(0,0,0,0.3);
    }
    .gf-dialog h3 { margin: 0 0 0.8rem; font-size: 1.1rem; }
    .gf-dialog select {
      width: 100%;
      padding: 0.4rem;
      margin-bottom: 0.6rem;
      font-size: 0.88rem;
      border: 1px solid var(--input-border);
      border-radius: 6px;
      background: var(--bg);
      color: var(--text);
    }
    .gf-dialog textarea {
      width: 100%;
      min-height: 80px;
      resize: vertical;
      font-size: 0.88rem;
      border: 1px solid var(--input-border);
      border-radius: 6px;
      background: var(--bg);
      color: var(--text);
      padding: 0.5rem;
      box-sizing: border-box;
      font-family: inherit;
    }
    .gf-dialog textarea:focus { outline: none; border-color: var(--accent); }
    .gf-dialog .gf-actions {
      display: flex;
      gap: 0.5rem;
      justify-content: flex-end;
      margin-top: 0.8rem;
    }
    .gf-dialog .gf-actions button {
      padding: 0.4rem 1rem;
      font-size: 0.85rem;
      border: 1px solid var(--input-border);
      border-radius: 6px;
      cursor: pointer;
      background: var(--bg);
      color: var(--text);
    }
    .gf-dialog .gf-actions button.primary {
      background: var(--accent);
      color: #fff;
      border-color: var(--accent);
    }
    .gf-dialog .gf-toast {
      margin-top: 0.5rem;
      font-size: 0.82rem;
      color: var(--ok);
    }
  </style>
  <style>
/* ══ 6 套主題 CSS 變數 ══ */
[data-theme="navy-dark"]{--bg-p:#0f172a;--bg-s:#1e293b;--bg-t:#293548;--bg-h:#334155;--bd:#334155;--bdl:#475569;--tx:#f1f5f9;--txs:#94a3b8;--txm:#64748b;--ac:#3b82f6;--ach:#2563eb;--act:#fff;--acs:rgba(59,130,246,0.15);--dg:#f87171;--dgb:rgba(239,68,68,0.15);--ok:#34d399;--warn:#fbbf24;--tg:#1d4ed8;--tgt:#bfdbfe;--sh:0 8px 32px rgba(0,0,0,0.5);}
[data-theme="navy-light"]{--bg-p:#f0f4f8;--bg-s:#fff;--bg-t:#f8fafc;--bg-h:#e2e8f0;--bd:#cbd5e1;--bdl:#e2e8f0;--tx:#0f172a;--txs:#475569;--txm:#94a3b8;--ac:#2563eb;--ach:#1d4ed8;--act:#fff;--acs:rgba(37,99,235,0.1);--dg:#dc2626;--dgb:rgba(220,38,38,0.08);--ok:#16a34a;--warn:#d97706;--tg:#dbeafe;--tgt:#1e40af;--sh:0 4px 16px rgba(0,0,0,0.1);}
[data-theme="forest-dark"]{--bg-p:#0a1a12;--bg-s:#132218;--bg-t:#1a3024;--bg-h:#1e3d2a;--bd:#1e3d2a;--bdl:#2d5a3e;--tx:#ecfdf5;--txs:#86efac;--txm:#4ade80;--ac:#22c55e;--ach:#16a34a;--act:#fff;--acs:rgba(34,197,94,0.15);--dg:#f87171;--dgb:rgba(239,68,68,0.15);--ok:#34d399;--warn:#fbbf24;--tg:#14532d;--tgt:#86efac;--sh:0 8px 32px rgba(0,0,0,0.6);}
[data-theme="forest-light"]{--bg-p:#f0fdf4;--bg-s:#fff;--bg-t:#f7fef9;--bg-h:#dcfce7;--bd:#bbf7d0;--bdl:#dcfce7;--tx:#14532d;--txs:#166534;--txm:#4ade80;--ac:#16a34a;--ach:#15803d;--act:#fff;--acs:rgba(22,163,74,0.1);--dg:#dc2626;--dgb:rgba(220,38,38,0.08);--ok:#16a34a;--warn:#d97706;--tg:#dcfce7;--tgt:#14532d;--sh:0 4px 16px rgba(0,80,40,0.1);}
[data-theme="amber-dark"]{--bg-p:#1a1208;--bg-s:#261a0c;--bg-t:#332210;--bg-h:#3d2b14;--bd:#3d2b14;--bdl:#5c3d1e;--tx:#fef3c7;--txs:#fcd34d;--txm:#d97706;--ac:#f59e0b;--ach:#d97706;--act:#1a1208;--acs:rgba(245,158,11,0.15);--dg:#f87171;--dgb:rgba(239,68,68,0.15);--ok:#34d399;--warn:#fbbf24;--tg:#78350f;--tgt:#fde68a;--sh:0 8px 32px rgba(0,0,0,0.6);}
[data-theme="amber-light"]{--bg-p:#fffbeb;--bg-s:#fff;--bg-t:#fefce8;--bg-h:#fef3c7;--bd:#fde68a;--bdl:#fef3c7;--tx:#451a03;--txs:#92400e;--txm:#b45309;--ac:#d97706;--ach:#b45309;--act:#fff;--acs:rgba(217,119,6,0.1);--dg:#dc2626;--dgb:rgba(220,38,38,0.08);--ok:#16a34a;--warn:#d97706;--tg:#fef3c7;--tgt:#78350f;--sh:0 4px 16px rgba(180,100,0,0.1);}
[data-theme="minimal-light"]{--bg-p:#f9fafb;--bg-s:#fff;--bg-t:#f3f4f6;--bg-h:#f3f4f6;--bd:#e5e7eb;--bdl:#f3f4f6;--tx:#111827;--txs:#6b7280;--txm:#9ca3af;--ac:#4f46e5;--ach:#4338ca;--act:#fff;--acs:rgba(79,70,229,0.08);--dg:#ef4444;--dgb:rgba(239,68,68,0.08);--ok:#10b981;--warn:#f59e0b;--tg:#ede9fe;--tgt:#4c1d95;--sh:0 1px 8px rgba(0,0,0,0.08);}
[data-theme="minimal-dark"]{--bg-p:#18181b;--bg-s:#27272a;--bg-t:#3f3f46;--bg-h:#3f3f46;--bd:#3f3f46;--bdl:#52525b;--tx:#fafafa;--txs:#a1a1aa;--txm:#71717a;--ac:#6366f1;--ach:#4f46e5;--act:#fff;--acs:rgba(99,102,241,0.15);--dg:#f87171;--dgb:rgba(239,68,68,0.15);--ok:#34d399;--warn:#fbbf24;--tg:#312e81;--tgt:#c7d2fe;--sh:0 8px 32px rgba(0,0,0,0.5);}
[data-theme="rose-light"]{--bg-p:#fff1f2;--bg-s:#fff;--bg-t:#fff1f2;--bg-h:#ffe4e6;--bd:#fecdd3;--bdl:#ffe4e6;--tx:#4c0519;--txs:#9f1239;--txm:#e11d48;--ac:#e11d48;--ach:#be123c;--act:#fff;--acs:rgba(225,29,72,0.08);--dg:#be123c;--dgb:rgba(190,18,60,0.08);--ok:#16a34a;--warn:#d97706;--tg:#ffe4e6;--tgt:#9f1239;--sh:0 4px 16px rgba(200,0,50,0.1);}
[data-theme="rose-dark"]{--bg-p:#1a0810;--bg-s:#2a0f1c;--bg-t:#3a1528;--bg-h:#4a1a32;--bd:#4a1a32;--bdl:#6b2545;--tx:#fff1f2;--txs:#fda4af;--txm:#fb7185;--ac:#fb7185;--ach:#f43f5e;--act:#fff;--acs:rgba(251,113,133,0.15);--dg:#f87171;--dgb:rgba(239,68,68,0.15);--ok:#34d399;--warn:#fbbf24;--tg:#881337;--tgt:#fda4af;--sh:0 8px 32px rgba(0,0,0,0.6);}
[data-theme="oled-dark"]{--bg-p:#000;--bg-s:#0a0a0a;--bg-t:#141414;--bg-h:#1f1f1f;--bd:#1f1f1f;--bdl:#2d2d2d;--tx:#fff;--txs:#a3a3a3;--txm:#525252;--ac:#fff;--ach:#e5e5e5;--act:#000;--acs:rgba(255,255,255,0.08);--dg:#f87171;--dgb:rgba(239,68,68,0.15);--ok:#34d399;--warn:#fbbf24;--tg:#1f1f1f;--tgt:#a3a3a3;--sh:0 8px 32px rgba(0,0,0,0.8);}
    /* ── 統一 Sidebar（80px icon-only，與 Portal 一致） ── */
    #app-sidebar{position:fixed;top:0;left:0;height:100%;width:80px;background:var(--bg-s);border-right:1px solid var(--bd);display:flex;flex-direction:column;z-index:300;transition:background 0.3s,border-color 0.3s;}
    #app-sidebar .sb-logo{display:flex;align-items:center;justify-content:center;padding:14px 0;border-bottom:1px solid var(--bd);}
    #app-sidebar .sb-logo img{height:48px;width:48px;object-fit:contain;border-radius:8px;}
    #app-sidebar .sb-logo span{display:none;}
    #app-sidebar nav{flex:1;padding:12px 0;display:flex;flex-direction:column;align-items:center;gap:4px;overflow-y:auto;overflow-x:hidden;min-height:0;}
    #app-sidebar nav a{width:60px;height:60px;min-width:60px;min-height:60px;display:flex;align-items:center;justify-content:center;border-radius:14px;color:var(--txs);text-decoration:none;transition:background 0.15s,color 0.15s;position:relative;}
    #app-sidebar nav a img{width:36px;height:36px;object-fit:contain;}
    #app-sidebar nav a .sb-nav-text{display:none;}
    #app-sidebar nav a:hover{background:var(--bg-h);}
    #app-sidebar nav a.active{background:var(--acs);}
    #app-sidebar nav a .sb-tooltip{position:absolute;left:calc(100% + 10px);top:50%;transform:translateY(-50%);background:var(--bg-s);color:var(--tx);border:1px solid var(--bd);border-radius:8px;padding:5px 10px;font-size:0.78rem;font-weight:600;white-space:nowrap;pointer-events:none;opacity:0;transition:opacity 0.15s;z-index:300;box-shadow:0 4px 12px rgba(0,0,0,.08);}
    #app-sidebar nav a:hover .sb-tooltip{opacity:1;}
    /* flyout 子選單（position:fixed 逃脫 overflow-x:hidden）*/
    .sb-fw{width:60px;height:60px;min-width:60px;min-height:60px;display:flex;align-items:center;justify-content:center;}
    .sb-fw.hidden{display:none!important;}
    .sb-fw>a{width:60px;height:60px;display:flex;align-items:center;justify-content:center;border-radius:14px;color:var(--txs);text-decoration:none;transition:background 0.15s;}
    .sb-fw>a img{width:36px;height:36px;object-fit:contain;}
    .sb-fw:hover>a{background:var(--bg-h);}
    .sb-flyout{position:fixed;background:var(--bg-s);border:1px solid var(--bd);border-radius:12px;padding:6px;min-width:150px;white-space:nowrap;pointer-events:none;opacity:0;transition:opacity 0.15s;z-index:9999;box-shadow:0 4px 20px rgba(0,0,0,.15);display:none;}
    .sb-flyout-title{font-size:0.7rem;font-weight:700;color:var(--txm);padding:4px 8px 6px;letter-spacing:.03em;}
    .sb-flyout-item{display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:8px;text-decoration:none;color:var(--tx);font-size:0.8rem;font-weight:500;transition:background 0.12s;}
    .sb-flyout-item:hover{background:var(--bg-h);}
    .sb-flyout-item .fi-dot{width:6px;height:6px;border-radius:50%;background:var(--txm);flex-shrink:0;}
    #app-sidebar .sb-user{padding:12px 0;border-top:1px solid var(--bd);display:flex;justify-content:center;}
    #app-sidebar .sb-user button{width:60px;height:60px;display:flex;align-items:center;justify-content:center;border-radius:14px;border:none;background:var(--bg-h);cursor:pointer;transition:background 0.15s;}
    #app-sidebar .sb-user button:hover{background:var(--bd);}
    #app-sidebar .sb-user .sb-hide{display:none;}
    /* 通用頭像容器 */
    .av-wrap{position:relative;flex-shrink:0;border-radius:50%;overflow:hidden;border:2px solid var(--bdl);}
    .av-wrap img{position:absolute;inset:0;width:100%;height:100%;border-radius:50%;object-fit:cover;}
    .av-wrap .av-fb{width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-weight:700;color:#fff;font-size:0.9rem;background:linear-gradient(135deg,var(--ac),var(--ach));}
    /* 手機 Header */
    #app-header{display:none;position:sticky;top:0;z-index:250;background:var(--bg-s);border-bottom:1px solid var(--bd);padding:10px 16px;align-items:center;justify-content:space-between;transition:background 0.3s;}
    #app-header .hd-logo{display:flex;align-items:center;gap:8px;font-weight:600;color:var(--tx);font-size:0.85rem;}
    /* Dropdown */
    #user-dropdown{position:fixed;z-index:500;width:220px;background:var(--bg-s);border:1px solid var(--bd);border-radius:14px;box-shadow:var(--sh);overflow:hidden;display:none;}
    #user-dropdown .dd-header{padding:12px 16px;border-bottom:1px solid var(--bd);background:var(--bg-p);}
    #user-dropdown .dd-header p{margin:0;font-size:0.85rem;font-weight:600;color:var(--tx);}
    #user-dropdown a,#user-dropdown button{display:flex;align-items:center;gap:10px;width:100%;padding:10px 16px;border:none;background:none;color:var(--txs);font-size:0.85rem;text-decoration:none;cursor:pointer;text-align:left;transition:background 0.15s;}
    #user-dropdown a:hover,#user-dropdown button:hover{background:var(--bg-h);color:var(--tx);}
    #user-dropdown .dd-danger{color:var(--dg);}
    #user-dropdown .dd-danger:hover{background:var(--dgb);}
    #user-dropdown .dd-divider{height:1px;background:var(--bd);margin:4px 0;}
    @media(min-width:768px){body{padding-left:calc(80px + 1.5rem);padding-right:1.5rem;}}
    @media(max-width:767px){ #app-sidebar{display:none;} #app-header{display:flex;}body{padding-left:1rem;padding-right:1rem;padding-bottom:72px;}}
    /* 手機底部 Tab Bar（統一 Portal 風格） */
    .app-tb-item{flex:1;display:flex;flex-direction:column;align-items:center;gap:2px;padding:4px 2px;color:var(--txm);font-size:0.62rem;text-decoration:none;transition:color 0.15s;border-top:2px solid transparent;}
    .app-tb-item:hover{color:var(--tx)!important;}
    .app-tb-active{color:var(--ac)!important;border-top-color:var(--ac)!important;}
    /* 舊 lib-mobile-tabbar 保留（向後相容），實際已換成 app-tab-bar */
    .lib-mobile-tabbar{display:none!important;}
    /* 外觀設定面板 */
    #theme-panel{position:fixed;top:0;right:0;bottom:0;width:288px;background:var(--bg-s);border-left:1px solid var(--bd);z-index:800;padding:20px;overflow-y:auto;box-shadow:var(--sh);transition:background 0.3s,border-color 0.3s;}
    .tp-style-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:16px;}
    .tp-style-card{border:2px solid var(--bd);border-radius:10px;padding:8px;cursor:pointer;transition:border-color 0.2s,transform 0.15s;position:relative;overflow:hidden;}
    .tp-style-card:hover{transform:scale(1.02);}
    .tp-style-card.selected{border-color:var(--ac);}
    .tp-style-card .preview{height:44px;border-radius:6px;margin-bottom:6px;display:flex;overflow:hidden;}
    .tp-style-card .preview .sb-strip{width:28%;height:100%;}
    .tp-style-card .preview .ct-strip{flex:1;height:100%;padding:3px;display:flex;flex-direction:column;gap:2px;}
    .tp-style-card .preview .ln{border-radius:2px;height:5px;}
    .tp-check{position:absolute;top:5px;right:5px;width:16px;height:16px;border-radius:50%;background:var(--ac);color:var(--act);font-size:9px;display:none;align-items:center;justify-content:center;}
    .tp-style-card.selected .tp-check{display:flex;}
    .tp-style-name{font-size:0.72rem;font-weight:600;color:var(--tx);margin-bottom:1px;}
    .tp-style-desc{font-size:0.62rem;color:var(--txm);}
    .tp-mode-row{display:flex;gap:5px;margin-bottom:14px;}
    .tp-mode-btn{flex:1;padding:7px 4px;border-radius:7px;border:1px solid var(--bd);background:none;color:var(--txs);font-size:0.74rem;cursor:pointer;transition:all 0.15s;}
    .tp-mode-btn.active{background:var(--ac);color:var(--act);border-color:var(--ac);}
    .tp-section{font-size:0.68rem;font-weight:600;color:var(--txm);text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;margin-top:14px;}
    .badge-role{background:var(--tg);color:var(--tgt);}
    /* points-pill — 與 Portal 一模一樣 */
    .points-pill{display:inline-flex;align-items:center;padding:0.2rem 0.6rem;border-radius:9999px;font-size:0.72rem;font-weight:600;white-space:nowrap;}
    .points-pill.admin{background:rgba(139,92,246,0.2);color:rgb(196,167,255);}
    .points-pill.sub{background:rgba(34,197,94,0.2);color:rgb(134,239,172);}
    .points-pill.points{background:var(--acs);color:var(--ac);}
    /* ── 覆蓋 Tailwind 主內容區硬編碼顏色 → CSS 變數（加 body 前綴提高權重） ── */
    body{background:var(--bg-p)!important;color:var(--tx)!important;overflow-x:hidden;}
    body [class*="min-h-screen"]{background:var(--bg-p)!important;color:var(--tx)!important;}
    /* 背景色覆蓋（含透明度變體：/60、/95 等） */
    body header,body header.sticky,body header[class*="sticky"]{background:var(--bg-s)!important;border-color:var(--bd)!important;}
    body [class*="bg-slate-9"],body [class*="bg-slate-95"]{background:var(--bg-s)!important;}
    body [class*="bg-slate-8"]{background:var(--bg-t)!important;}
    body [class*="bg-slate-7"],body [class*="bg-slate-6"]{background:var(--bg-h)!important;}
    body [class*="bg-white"]:not(button):not(a){background:var(--bg-s)!important;}
    /* 邊框色覆蓋 */
    body [class*="border-slate"],body [class*="divide-slate"]{border-color:var(--bd)!important;}
    /* 文字色覆蓋 */
    body [class*="text-slate-1"],body [class*="text-slate-2"],body [class*="text-white"]:not(button[class*="bg-blue"]):not(button[class*="bg-red"]){color:var(--tx)!important;}
    body [class*="text-slate-3"],body [class*="text-slate-4"],body [class*="text-slate-5"]{color:var(--txs)!important;}
    body [class*="text-slate-6"],body [class*="text-slate-7"]{color:var(--txm)!important;}
    body [class*="text-gray-"]{color:var(--txs)!important;}
    body [class*="hover\\:text-slate-2"]:hover,body [class*="hover\\:text-slate-1"]:hover{color:var(--tx)!important;}
    /* hover 背景覆蓋 */
    body [class*="hover\\:bg-slate-7"]:hover,body [class*="hover\\:bg-slate-6"]:hover{background:var(--bg-h)!important;}
    /* accent（藍色）→ 主題 accent 色 */
    body .tab-btn[class*="text-blue"]{color:var(--ac)!important;}
    /* ── 準賣方卡片 grid（比照買方列表風格）── */
    .btn-primary{background:var(--ac);color:var(--act);border-radius:.5rem;padding:.4rem 1rem;font-size:.85rem;font-weight:600;transition:background .15s;border:none;cursor:pointer;}
    .btn-primary:hover{background:var(--ach);}
    .card{background:var(--bg-s);border:1px solid var(--bd);border-radius:1rem;padding:1rem;transition:background 0.3s,border-color 0.3s;}
    .badge{display:inline-block;padding:2px 8px;border-radius:9999px;font-size:.7rem;font-weight:600;}
    .badge-blue{background:#1d4ed8;color:#bfdbfe;}
    .badge-green{background:#166534;color:#bbf7d0;}
    .badge-gray{background:#374151;color:#9ca3af;}
    .badge-amber{background:#92400e;color:#fde68a;}
    .badge-purple{background:#6b21a8;color:#e9d5ff;}
    .sl-col-btn{width:28px;height:28px;border-radius:6px;border:1px solid var(--bd);background:transparent;color:var(--txs);font-size:12px;font-weight:600;cursor:pointer;transition:all .15s;}
    .sl-col-btn:hover{border-color:var(--ac);color:var(--ac);}
    .sl-col-btn.active{background:var(--ac);color:var(--act);border-color:var(--ac);}
    #sl-list.drag-mode .card{cursor:grab;user-select:none;}
    #sl-list.drag-mode .card:active{cursor:grabbing;}
    .card.sl-drag-over{border:2px dashed var(--ac);opacity:0.7;}
    .card.sl-dragging{opacity:0.4;transform:scale(0.96);}
    .sl-avatar{width:44px;height:44px;border-radius:50%;object-fit:cover;flex-shrink:0;border:2px solid var(--bd);}
    .sl-avatar-ph{width:44px;height:44px;border-radius:50%;background:var(--ac);color:#fff;font-size:17px;font-weight:700;display:flex;align-items:center;justify-content:center;flex-shrink:0;}
    .color-dot{width:28px;height:28px;border-radius:50%;border:2px solid transparent;cursor:pointer;transition:transform .15s,border-color .15s;flex-shrink:0;}
    .color-dot:hover{transform:scale(1.15);border-color:var(--tx);}
    .color-dot.selected{border-color:var(--ac);box-shadow:0 0 0 2px var(--ac);}
    /* 準賣方 Modal */
    .modal-bg{position:fixed;inset:0;z-index:200;background:rgba(0,0,0,.6);backdrop-filter:blur(4px);display:flex;align-items:center;justify-content:center;padding:1rem;}
    .modal-box{background:var(--bg-s);border:1px solid var(--bd);border-radius:1.25rem;width:100%;max-width:520px;max-height:92vh;overflow-y:auto;box-shadow:0 8px 32px rgba(0,0,0,.3);}
    .modal-box::-webkit-scrollbar{width:6px;}
    .modal-box::-webkit-scrollbar-thumb{background:var(--bd);border-radius:3px;}
    body [class*="border-blue-4"],body [class*="border-blue-5"]{border-color:var(--ac)!important;}
    body [class*="bg-blue-6"],body [class*="bg-blue-5"]{background:var(--ach)!important;}
    body [class*="hover\\:bg-blue-5"]:hover,body [class*="hover\\:bg-blue-6"]:hover{background:var(--ac)!important;}
    body [class*="text-blue-4"],body [class*="text-blue-3"]{color:var(--ac)!important;}
    body [class*="focus\\:border-blue"]:focus{border-color:var(--ac)!important;}
    /* shadow / ring */
    body [class*="ring-slate"],body [class*="shadow-"]{box-shadow:var(--sh)!important;}
    /* input / select / textarea */
    body input,body select,body textarea{background:var(--bg-t)!important;color:var(--tx)!important;border-color:var(--bd)!important;}
    body input::placeholder,body textarea::placeholder{color:var(--txm)!important;}
    /* 捲軸 */
    #cp-cat-panel,#cp-area-panel,#cp-agent-panel{scrollbar-color:var(--bdl) transparent!important;}
    #cp-cat-panel::-webkit-scrollbar-thumb,#cp-area-panel::-webkit-scrollbar-thumb,#cp-agent-panel::-webkit-scrollbar-thumb{background:var(--bdl)!important;}
  </style>
  <!-- Leaflet.js（地圖功能使用） -->
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    /* 地圖標記 label 樣式 */
  </style>
<script>
/* 前端錯誤自動回報至 Cloud Logging */
window.onerror = function(msg, src, line, col, err) {
    fetch('/api/client-log', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({type: 'js_error', msg: msg, src: src, line: line, viewport: window.innerWidth + 'x' + window.innerHeight})}).catch(function(){});
};
window.addEventListener('unhandledrejection', function(e) {
    fetch('/api/client-log', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({type: 'promise_error', msg: String(e.reason), viewport: window.innerWidth + 'x' + window.innerHeight})}).catch(function(){});
});
</script>
</head>
<body data-theme="navy-dark" class="min-h-screen font-sans antialiased">

<!-- ── 外觀設定面板 ── -->
<div id="theme-panel" style="display:none;">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:4px;">
    <div style="font-size:0.95rem;font-weight:700;color:var(--tx);">🎨 外觀設定</div>
    <button onclick="document.getElementById('theme-panel').style.display='none'" style="background:none;border:none;color:var(--txm);cursor:pointer;font-size:1.2rem;line-height:1;">✕</button>
  </div>
  <div style="font-size:0.75rem;color:var(--txm);margin-bottom:14px;">管理員設定的風格，所有成員同步套用</div>
  <div class="tp-section">明暗模式（個人）</div>
  <div class="tp-mode-row" id="tp-mode-row">
    <button class="tp-mode-btn" id="tp-btn-dark" onclick="window._tpSetMode('dark')">🌙 深色</button>
    <button class="tp-mode-btn" id="tp-btn-light" onclick="window._tpSetMode('light')">☀️ 淺色</button>
    <button class="tp-mode-btn" id="tp-btn-system" onclick="window._tpSetMode('system')">🖥️ 系統</button>
  </div>
  <div id="tp-admin-only" style="display:none;">
    <div class="tp-section">色系風格（後台統一）</div>
    <div class="tp-style-grid" id="tp-style-grid">
      <div class="tp-style-card" id="tp-card-navy" onclick="window._tpAdminSetStyle('navy')">
        <div class="preview"><div class="sb-strip" style="background:#1e293b;"></div><div class="ct-strip" style="background:#0f172a;"><div class="ln" style="background:#334155;width:80%;"></div><div class="ln" style="background:#3b82f6;width:50%;"></div><div class="ln" style="background:#334155;width:65%;"></div></div></div>
        <div class="tp-check">✓</div><div class="tp-style-name">🌙 深夜藍</div><div class="tp-style-desc">穩重專業</div>
      </div>
      <div class="tp-style-card" id="tp-card-forest" onclick="window._tpAdminSetStyle('forest')">
        <div class="preview"><div class="sb-strip" style="background:#132218;"></div><div class="ct-strip" style="background:#0a1a12;"><div class="ln" style="background:#1e3d2a;width:80%;"></div><div class="ln" style="background:#22c55e;width:50%;"></div><div class="ln" style="background:#1e3d2a;width:65%;"></div></div></div>
        <div class="tp-check">✓</div><div class="tp-style-name">🌿 森林綠</div><div class="tp-style-desc">清新活力</div>
      </div>
      <div class="tp-style-card" id="tp-card-amber" onclick="window._tpAdminSetStyle('amber')">
        <div class="preview"><div class="sb-strip" style="background:#261a0c;"></div><div class="ct-strip" style="background:#1a1208;"><div class="ln" style="background:#3d2b14;width:80%;"></div><div class="ln" style="background:#f59e0b;width:50%;"></div><div class="ln" style="background:#3d2b14;width:65%;"></div></div></div>
        <div class="tp-check">✓</div><div class="tp-style-name">🌅 暖棕商務</div><div class="tp-style-desc">低調奢華</div>
      </div>
      <div class="tp-style-card" id="tp-card-minimal" onclick="window._tpAdminSetStyle('minimal')">
        <div class="preview"><div class="sb-strip" style="background:#fff;border-right:1px solid #e5e7eb;"></div><div class="ct-strip" style="background:#f9fafb;"><div class="ln" style="background:#e5e7eb;width:80%;"></div><div class="ln" style="background:#4f46e5;width:50%;"></div><div class="ln" style="background:#e5e7eb;width:65%;"></div></div></div>
        <div class="tp-check">✓</div><div class="tp-style-name">⬜ 純白簡約</div><div class="tp-style-desc">清晰易讀</div>
      </div>
      <div class="tp-style-card" id="tp-card-rose" onclick="window._tpAdminSetStyle('rose')">
        <div class="preview"><div class="sb-strip" style="background:#2a0f1c;"></div><div class="ct-strip" style="background:#1a0810;"><div class="ln" style="background:#4a1a32;width:80%;"></div><div class="ln" style="background:#fb7185;width:50%;"></div><div class="ln" style="background:#4a1a32;width:65%;"></div></div></div>
        <div class="tp-check">✓</div><div class="tp-style-name">🌸 玫瑰粉</div><div class="tp-style-desc">優雅浪漫</div>
      </div>
      <div class="tp-style-card" id="tp-card-oled" onclick="window._tpAdminSetStyle('oled')">
        <div class="preview"><div class="sb-strip" style="background:#0a0a0a;"></div><div class="ct-strip" style="background:#000;"><div class="ln" style="background:#1f1f1f;width:80%;"></div><div class="ln" style="background:#fff;width:50%;"></div><div class="ln" style="background:#1f1f1f;width:65%;"></div></div></div>
        <div class="tp-check">✓</div><div class="tp-style-name">🖤 OLED 黑</div><div class="tp-style-desc">省電護眼</div>
      </div>
    </div>
    <button onclick="window._tpSaveStyle()" style="width:100%;padding:9px;border-radius:8px;background:var(--ac);color:var(--act);border:none;cursor:pointer;font-size:0.85rem;font-weight:600;">💾 套用到所有工具</button>
    <div id="tp-save-msg" style="text-align:center;font-size:0.75rem;color:var(--ok);margin-top:6px;display:none;">✓ 已儲存！所有工具同步套用</div>
  </div>
  <div style="margin-top:14px;padding:10px;border-radius:8px;background:var(--bg-t);border:1px solid var(--bd);font-size:0.7rem;color:var(--txm);line-height:1.6;">
    💡 風格由管理員統一設定，明暗模式依個人裝置偏好儲存。
  </div>
</div>

<!-- ── 桌機左側 Sidebar（80px icon-only，與 Portal 一致） ── -->
<aside id="app-sidebar">
  <div class="sb-logo">
    <a id="sb-logo-link" href="javascript:void(0)" onclick="var el=document.getElementById('sb-portal-home');if(el&&el.href&&el.href!='javascript:void(0)')window.open(el.href,'tool-portal');else if(el&&el.getAttribute('data-href'))window.open(el.getAttribute('data-href'),'tool-portal');" title="回到工具首頁" style="display:flex;align-items:center;justify-content:center;">
      <img src="/static/logo.png" alt="U.P." onerror="this.style.display='none'" />
    </a>
    <span>物件庫</span>
  </div>
  <nav>
    <a href="__PORTAL_LINK__" target="tool-portal" id="sb-portal-home" class="hidden"><img src="/static/tool-reels.png" alt="" /><span class="sb-nav-text">工具首頁</span><span class="sb-tooltip">工具首頁</span></a>
    <div class="sb-fw hidden" id="sb-ad">
      <a href="javascript:void(0)" id="sb-ad-link" target="tool-post"><img src="/static/tool-ad.png" alt="" /><span class="sb-tooltip">廣告文案</span></a>
      <div class="sb-flyout"><div class="sb-flyout-title">廣告文案</div>
        <a id="sb-ad-campaigns" href="#" target="tool-post" class="sb-flyout-item"><span class="fi-dot"></span>廣告活動</a>
        <a id="sb-ad-photos" href="#" target="tool-post" class="sb-flyout-item"><span class="fi-dot"></span>物件照片</a>
        <a id="sb-ad-showcase" href="#" target="tool-post" class="sb-flyout-item"><span class="fi-dot"></span>展示頁</a>
      </div>
    </div>
    <a href="#" class="active"><img src="/static/tool-library.png" alt="" /><span class="sb-nav-text">物件庫</span><span class="sb-tooltip">物件庫</span></a>
    <div class="sb-fw hidden" id="sb-buyer">
      <a href="javascript:void(0)" id="sb-buyer-link" target="tool-buyer"><img src="/static/tool-buyer.png" alt="" /><span class="sb-tooltip">買方管理</span></a>
      <div class="sb-flyout"><div class="sb-flyout-title">買方管理</div>
        <a id="sb-buyer-buyers" href="#" target="tool-buyer" class="sb-flyout-item"><span class="fi-dot"></span>買方需求</a>
        <a id="sb-buyer-war" href="#" target="tool-buyer" class="sb-flyout-item"><span class="fi-dot"></span>戰況版</a>
        <a id="sb-buyer-showings" href="#" target="tool-buyer" class="sb-flyout-item"><span class="fi-dot"></span>帶看紀錄</a>
      </div>
    </div>
    <a href="javascript:void(0)" id="sb-survey" class="hidden"><img src="/static/tool-survey.png" alt="" /><span class="sb-nav-text">周邊調查</span><span class="sb-tooltip">周邊調查</span></a>
    <div class="sb-fw hidden" id="sb-calendar">
      <a href="javascript:void(0)" id="sb-calendar-link" target="tool-calendar"><img src="/static/tool-calendar.png" alt="" /><span class="sb-tooltip">業務行事曆</span></a>
      <div class="sb-flyout"><div class="sb-flyout-title">業務行事曆</div>
        <a id="sb-calendar-week" href="#" target="tool-calendar" class="sb-flyout-item"><span class="fi-dot"></span>週視圖</a>
        <a id="sb-calendar-month" href="#" target="tool-calendar" class="sb-flyout-item"><span class="fi-dot"></span>月視圖</a>
      </div>
    </div>
    <a href="javascript:void(0)" id="sb-notes" class="hidden"><img src="/static/tool-doc.png" alt="" /><span class="sb-nav-text">記事本</span><span class="sb-tooltip">記事本</span></a>
  </nav>
  <div class="sb-user">
    <!-- 桌機：只顯示頭像，文字隱藏 -->
    <button type="button" onclick="libToggleDropdown(event)">
      <div id="sb-avatar" class="av-wrap" style="width:36px;height:36px;flex-shrink:0;"><div class="av-fb">?</div></div>
      <div class="sb-hide" style="min-width:0;flex:1;">
        <div id="sb-name" style="font-size:0.82rem;font-weight:600;color:var(--tx);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"></div>
        <span id="sb-badge" class="points-pill points" style="margin-top:2px;">— 點</span>
      </div>
      <svg class="sb-hide" style="width:16px;height:16px;color:var(--txm);flex-shrink:0;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
    </button>
  </div>
</aside>

<!-- ── 手機頂部 Header ── -->
<header id="app-header">
  <div class="hd-logo">
    <span>📁 物件庫</span>
  </div>
  <div style="display:flex;align-items:center;gap:8px;">
    <div style="display:flex;align-items:center;gap:8px;cursor:pointer;" onclick="libToggleDropdown(event)">
      <span id="hd-badge" class="points-pill points">— 點</span>
      <div id="hd-avatar" class="av-wrap" style="width:34px;height:34px;"><div class="av-fb">?</div></div>
    </div>
  </div>
</header>

<!-- ── 使用者 Dropdown ── -->
<div id="user-dropdown">
  <div class="dd-header">
    <p id="dd-name">載入中…</p>
    <span id="dd-badge" class="points-pill points" style="margin-top:4px;">— 點</span>
  </div>
  <div style="padding:4px 0;">
    <a id="dd-plans" href="javascript:void(0)" class="hidden">⬆️ 升級方案</a>
    <a id="dd-account" href="javascript:void(0)" class="hidden">👤 帳號管理</a>
    <a id="dd-admin" href="javascript:void(0)" class="hidden">🛡️ 後台管理</a>
    <button onclick="libCloseDropdown();document.getElementById('theme-panel').style.display='block';" style="display:flex;align-items:center;gap:10px;width:100%;padding:10px 16px;border:none;background:none;color:var(--txs);font-size:0.85rem;cursor:pointer;text-align:left;transition:background 0.15s;" onmouseover="this.style.background='var(--bg-h)';this.style.color='var(--tx)'" onmouseout="this.style.background='none';this.style.color='var(--txs)'">🎨 外觀設定</button>
    <button onclick="libCloseDropdown();document.getElementById('gf-overlay').classList.add('show');" style="display:flex;align-items:center;gap:10px;width:100%;padding:10px 16px;border:none;background:none;color:var(--txs);font-size:0.85rem;cursor:pointer;text-align:left;transition:background 0.15s;" onmouseover="this.style.background='var(--bg-h)';this.style.color='var(--tx)'" onmouseout="this.style.background='none';this.style.color='var(--txs)'">💬 意見反饋</button>
  </div>
  <div class="dd-divider"></div>
  <div style="padding:4px 0;">
    <button class="dd-danger" onclick="libDoLogout()">🚪 登出</button>
  </div>
</div>
<div id="user-dropdown-backdrop" style="display:none;position:fixed;inset:0;z-index:499;" onclick="libCloseDropdown()"></div>

<!-- 更多選單遮罩 -->
<div id="more-menu-overlay" onclick="toggleMoreMenu()" style="display:none;position:fixed;inset:0;z-index:240;background:rgba(0,0,0,0.4);"></div>
<!-- 更多選單面板（從底部滑出，含周邊調查、行事曆、實價登錄） -->
<div id="more-menu" style="display:none;position:fixed;left:0;right:0;z-index:252;background:var(--bg-s);border-radius:20px 20px 0 0;border-top:1px solid var(--bd);padding:16px 16px 20px;padding-bottom:calc(20px + env(safe-area-inset-bottom));bottom:calc(64px + env(safe-area-inset-bottom));max-height:70vh;overflow-y:auto;transition:background 0.3s;">
  <div style="width:36px;height:4px;background:var(--bd);border-radius:2px;margin:0 auto 16px;"></div>
  <div style="font-size:0.72rem;font-weight:700;color:var(--txm);margin-bottom:12px;text-transform:uppercase;letter-spacing:0.06em;">更多工具</div>
  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;">
    <a id="more-survey" href="javascript:void(0)" onclick="closeMoreMenu()" style="display:flex;flex-direction:column;align-items:center;gap:4px;padding:10px 4px;border-radius:12px;background:var(--bg-t,#f5f5f7);text-decoration:none;color:var(--tx);">
      <img src="/static/tool-survey.png" alt="" style="width:42px;height:42px;object-fit:contain;" /><span style="font-size:0.65rem;font-weight:600;">周邊調查</span>
    </a>
    <a id="more-calendar" href="javascript:void(0)" onclick="closeMoreMenu()" style="display:flex;flex-direction:column;align-items:center;gap:4px;padding:10px 4px;border-radius:12px;background:var(--bg-t,#f5f5f7);text-decoration:none;color:var(--tx);">
      <img src="/static/tool-calendar.png" alt="" style="width:42px;height:42px;object-fit:contain;" /><span style="font-size:0.65rem;font-weight:600;">行事曆</span>
    </a>
    <a id="more-price" href="javascript:void(0)" onclick="closeMoreMenu()" style="display:flex;flex-direction:column;align-items:center;gap:4px;padding:10px 4px;border-radius:12px;background:var(--bg-t,#f5f5f7);text-decoration:none;color:var(--tx);">
      <img src="/static/tool-price.png" alt="" style="width:42px;height:42px;object-fit:contain;" /><span style="font-size:0.65rem;font-weight:600;">實價登錄</span>
    </a>
    <a id="more-notes" href="javascript:void(0)" onclick="closeMoreMenu()" style="display:flex;flex-direction:column;align-items:center;gap:4px;padding:10px 4px;border-radius:12px;background:var(--bg-t,#f5f5f7);text-decoration:none;color:var(--tx);">
      <img src="/static/tool-doc.png" alt="" style="width:42px;height:42px;object-fit:contain;" /><span style="font-size:0.65rem;font-weight:600;">記事本</span>
    </a>
  </div>
</div>

<!-- 底部 Tab Bar（統一 Portal 風格：首頁｜物件庫(active)｜廣告｜買方｜更多） -->
<nav id="app-tab-bar" style="display:none;position:fixed;bottom:0;left:0;right:0;z-index:250;background:var(--bg-s);backdrop-filter:blur(12px);border-top:1px solid var(--bd);padding-bottom:env(safe-area-inset-bottom);transition:background 0.3s;">
  <div style="display:flex;align-items:center;padding:6px 0 4px;">
    <a id="tb-home" href="__PORTAL_LINK__" target="tool-portal" class="app-tb-item">
      <img src="/static/tool-reels.png" alt="" style="width:36px;height:36px;object-fit:contain;" /><span>首頁</span>
    </a>
    <a href="#" class="app-tb-item app-tb-active">
      <img src="/static/tool-library.png" alt="" style="width:36px;height:36px;object-fit:contain;" /><span>物件庫</span>
    </a>
    <a id="tb-ad" href="javascript:void(0)" class="app-tb-item">
      <img src="/static/tool-ad.png" alt="" style="width:36px;height:36px;object-fit:contain;" /><span>廣告</span>
    </a>
    <a id="tb-buyer" href="javascript:void(0)" class="app-tb-item">
      <img src="/static/tool-buyer.png" alt="" style="width:36px;height:36px;object-fit:contain;" /><span>買方</span>
    </a>
    <button onclick="toggleMoreMenu()" class="app-tb-item" style="border:none;background:none;cursor:pointer;">
      <span style="font-size:1.4rem;line-height:1;">⋯</span><span>更多</span>
    </button>
  </div>
</nav>

<div id="toast-container"></div>

<!-- 頂部分頁列（移除舊的導覽 header，只保留分頁標籤） -->
<header class="sticky top-0 z-50 backdrop-blur shadow" style="background:var(--bg-s);border-bottom:1px solid var(--bd);">
  <!-- 分頁標籤 -->
  <div class="flex" style="border-top:1px solid var(--bd);">
    <button id="tab-company" onclick="switchTab('company')"
      class="tab-btn flex-1 py-2 text-sm font-medium border-b-2 transition" style="color:var(--ac);border-color:var(--ac);">
      🏢 公司物件庫
    </button>
    <!-- 地圖 tab：所有登入者皆可使用 -->
    <button id="tab-map" onclick="switchTab('map')"
      class="tab-btn flex-1 py-2 text-sm font-medium border-b-2 border-transparent transition" style="color:var(--txs);">
      🗺️ 地圖
    </button>
    <!-- 準賣方管理 tab：所有登入者皆可使用 -->
    <button id="tab-sellers" onclick="switchTab('sellers')"
      class="tab-btn flex-1 py-2 text-sm font-medium border-b-2 border-transparent transition" style="color:var(--txs);">
      🏠 準賣方
    </button>
    <!-- 資料庫檢視 tab：僅管理員看得到（由 JS 控制顯示） -->
    <button id="tab-dbview" onclick="switchTab('dbview')"
      class="tab-btn hidden flex-1 py-2 text-sm font-medium border-b-2 border-transparent transition" style="color:var(--txs);">
      📊 資料庫
    </button>
    <!-- 設定 tab：僅管理員看得到（由 JS 控制顯示） -->
    <button id="tab-settings" onclick="switchTab('settings')"
      class="tab-btn hidden flex-1 py-2 text-sm font-medium border-b-2 border-transparent transition" style="color:var(--txs);">
      ⚙️ 設定
    </button>
    <!-- 組織設定 tab：屬於組織的人才看得到（由 JS 控制顯示） -->
    <button id="tab-org" onclick="switchTab('org')"
      class="tab-btn hidden flex-1 py-2 text-sm font-medium border-b-2 border-transparent transition" style="color:var(--txs);">
      🏢 組織
    </button>
  </div>
</header>

<!-- ══ 公司物件庫分頁 ══ -->
<div id="pane-company" style="display:none" class="max-w-4xl mx-auto px-4 py-6">

  <!-- 搜尋條件列 -->
  <div class="rounded-2xl p-4 mb-4" style="background:var(--bg-t);border:1px solid var(--bd);">
    <!-- 第一列：關鍵字 + 售價 + 狀態 -->
    <div class="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-3">
      <input id="cp-keyword" type="text" placeholder="🔍 案名 / 地址 / 委託編號"
        class="col-span-2 rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);"
        onkeydown="if(event.key==='Enter')cpSearch()">
      <input id="cp-price-min" type="number" placeholder="最低售價（萬）"
        class="rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
      <input id="cp-price-max" type="number" placeholder="最高售價（萬）"
        class="rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
    </div>
    <!-- 第二列：狀態（單選）+ 複選下拉觸發器 -->
    <div class="flex flex-wrap gap-2 mb-3 items-center">
      <!-- 狀態（保留 select，不需複選） -->
      <select id="cp-status"
        class="rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        <option value="selling">銷售中</option>
        <option value="">全部狀態</option>
        <option value="sold">已成交</option>
        <option value="delisted">已下架</option>
      </select>
      <!-- 委託到期日篩選（前端過濾） -->
      <select id="cp-expiry"
        onchange="cpFetch()"
        class="rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        <option value="">全部到期日</option>
        <option value="active">委託中（未過期）</option>
        <option value="soon">即將到期（15天內）</option>
        <option value="expired">已過期</option>
        <option value="empty">未填到期日</option>
      </select>
      <!-- 在「起家」對外網站上架中（後端篩選：銷售中 + 委託期內） -->
      <label class="flex items-center gap-1.5 rounded-lg px-3 py-2 text-sm cursor-pointer transition" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);" title="只看目前公開在起家網站的物件">
        <input type="checkbox" id="cp-on-home-start" onchange="cpSearch()" style="cursor:pointer;">
        <span>🏠 在起家</span>
      </label>
      <!-- 類別複選按鈕 -->
      <div class="relative">
        <button id="cp-cat-btn" onclick="cpToggleDropdown('cat')"
          class="flex items-center gap-1 rounded-lg px-3 py-2 text-sm transition" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
          <span id="cp-cat-label">全部類別</span>
          <svg class="w-3 h-3" style="color:var(--txs);" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
        </button>
        <div id="cp-cat-panel" class="hidden absolute left-0 top-full mt-1 z-50 rounded-xl p-3 min-w-[180px] max-h-72 overflow-y-auto" style="background:var(--bg-s);border:1px solid var(--bd);box-shadow:var(--sh);">
          <div id="cp-cat-list" class="space-y-1"></div>
        </div>
      </div>
      <!-- 地區複選按鈕 -->
      <div class="relative">
        <button id="cp-area-btn" onclick="cpToggleDropdown('area')"
          class="flex items-center gap-1 rounded-lg px-3 py-2 text-sm transition" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
          <span id="cp-area-label">全部地區</span>
          <svg class="w-3 h-3" style="color:var(--txs);" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
        </button>
        <div id="cp-area-panel" class="hidden absolute left-0 top-full mt-1 z-50 rounded-xl p-3 min-w-[200px] max-h-72 overflow-y-auto" style="background:var(--bg-s);border:1px solid var(--bd);box-shadow:var(--sh);">
          <div id="cp-area-list" class="space-y-1"></div>
        </div>
      </div>
      <!-- 經紀人複選按鈕 -->
      <div class="relative">
        <button id="cp-agent-btn" onclick="cpToggleDropdown('agent')"
          class="flex items-center gap-1 rounded-lg px-3 py-2 text-sm transition" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
          <span id="cp-agent-label">全部經紀人</span>
          <svg class="w-3 h-3" style="color:var(--txs);" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
        </button>
        <div id="cp-agent-panel" class="hidden absolute left-0 top-full mt-1 z-50 rounded-xl p-3 min-w-[180px] max-h-72 overflow-y-auto" style="background:var(--bg-s);border:1px solid var(--bd);box-shadow:var(--sh);">
          <p class="text-xs mb-2" style="color:var(--txm);">── 在線人員 ──</p>
          <div id="cp-agent-active-list" class="space-y-1 mb-2"></div>
          <p class="text-xs mb-2" style="color:var(--txm);">── 其他 ──</p>
          <div id="cp-agent-inactive-list" class="space-y-1"></div>
        </div>
      </div>
    </div>
    <div class="flex gap-2 items-center flex-wrap">
      <button onclick="cpSearch()"
        class="px-5 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition">搜尋</button>
      <button onclick="cpReset()"
        class="px-4 py-2 rounded-lg text-sm transition" style="background:var(--bg-h);color:var(--txs);">重設</button>
      <!-- 加星篩選按鈕：點一下只看已加星，再點取消 -->
      <button id="cp-star-filter-btn" onclick="cpToggleStarFilter()"
        class="px-4 py-2 rounded-lg text-sm transition flex items-center gap-1" style="background:var(--bg-h);color:var(--txs);"
        title="只顯示已加星物件">
        <span id="cp-star-filter-icon">☆</span>
        <span id="cp-star-filter-label">追蹤中</span>
      </button>
      <div class="flex items-center gap-1 ml-2">
        <span class="text-xs" style="color:var(--txs);">排序：</span>
        <select id="cp-sort"
          onchange="cpSearch()"
          class="text-xs rounded-lg px-2 py-1.5 focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
          <option value="price_asc">售價 低→高</option>
          <option value="price_desc">售價 高→低</option>
          <option value="date_desc">委託日 新→舊</option>
          <option value="date_asc">委託日 舊→新</option>
          <option value="expiry_asc">到期日 近→遠</option>
          <option value="expiry_desc">到期日 遠→近</option>
          <option value="serial_asc">序號 小→大</option>
          <option value="serial_desc">序號 大→小</option>
        </select>
      </div>
      <!-- 情境書籤 -->
      <div class="flex items-center gap-1 ml-2 relative">
        <span class="text-xs" style="color:var(--txs);">情境：</span>
        <select id="cp-preset-select"
          onchange="cpApplyPreset()"
          class="text-xs rounded-lg px-2 py-1.5 focus:outline-none max-w-[140px]" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);"
          title="選擇已儲存的篩選情境">
          <option value="">— 選擇情境 —</option>
        </select>
        <!-- 刪除目前選中情境 -->
        <button id="cp-preset-delete-btn" onclick="cpDeletePreset()" title="刪除此情境"
          class="hidden text-red-400 hover:text-red-300 text-base leading-none px-1">×</button>
        <!-- 儲存目前篩選為情境 -->
        <button onclick="cpSavePreset()" title="將目前篩選/排序另存為情境"
          class="px-2 py-1.5 rounded-lg bg-purple-700 hover:bg-purple-600 text-purple-100 text-xs transition flex items-center gap-1">
          💾 儲存情境
        </button>
      </div>
    </div>
  </div>

  <!-- 管理員工具列（只有管理員看得到，預設收合） -->
  <div id="cp-sync-bar" class="hidden mb-3 rounded-xl px-4 py-2" style="background:var(--bg-t);border:1px solid var(--bd);">
    <!-- 摺疊標題列（永遠顯示） -->
    <div class="flex flex-wrap items-center gap-3">
      <button onclick="cpToggleSyncBar()" id="cp-sync-bar-toggle"
        class="px-3 py-1 rounded-lg text-xs font-semibold transition flex items-center gap-2"
        style="background:var(--bg-h);color:var(--tx);border:1px solid var(--bd);cursor:pointer;"
        title="展開／收合同步工具列（更新資料時才需要展開）">
        🛠️ 同步工具 <span id="cp-sync-bar-arrow" style="font-size:0.65rem;color:var(--txm);transition:transform 0.2s;">▶</span>
      </button>
      <span class="flex-1" style="font-size:0.75rem;color:var(--txs);">上次同步：<span id="cp-last-sync" style="color:var(--tx);">讀取中…</span></span>
    </div>

    <!-- 摺疊內容（預設隱藏，按 cpToggleSyncBar 切換） -->
    <div id="cp-sync-bar-content" class="hidden flex flex-wrap items-center gap-3" style="margin-top:10px;padding-top:10px;border-top:1px solid var(--bd);">
      <!-- 步驟 1：ACCESS 比對更新（紅 = 流程起點） -->
      <button onclick="openAccessCompareModal()"
        class="px-4 py-1.5 rounded-lg bg-rose-600 hover:bg-rose-500 text-white text-xs font-semibold transition"
        title="【步驟 1】把公司 Access 新資料貼到一張新 Google Sheets（接受網址或 ID），比對與主頁 Sheets 的差異（修改／新增／可能下架），選擇套用哪些變更回主頁 Sheets。完整流程順序：1.ACCESS比對 → 2.同步Sheets → 3.比對審查 → 4.回寫銷售中">
        📋 ACCESS比對
      </button>
      <!-- 步驟 2：同步 Sheets（橘） -->
      <button id="cp-sync-btn" onclick="cpTriggerSync()"
        class="px-4 py-1.5 rounded-lg bg-amber-600 hover:bg-amber-500 text-white text-xs font-semibold transition"
        title="【步驟 2】Google Sheets → Firestore 全量同步，網頁才查得到最新資料。完整流程順序：1.ACCESS比對 → 2.同步Sheets → 3.比對審查 → 4.回寫銷售中。⚠️ 必須在「比對審查」之前做，否則 Word 寫的銷售中會被舊 Sheets 覆蓋。">
        🔄 同步 Sheets
      </button>
      <!-- 步驟 3：比對審查（青綠）- 上傳 .doc/.docx 或 CSV → 審查配對 → 確認後寫入 -->
      <label class="flex items-center gap-1 px-4 py-1.5 rounded-lg bg-teal-700 hover:bg-teal-600 text-white text-xs font-semibold transition cursor-pointer"
        title="【步驟 3】上傳物件總表 Word（.doc / .docx 雲端解析）或本機 export_word_table.py 產出的 4 個 CSV + word_meta.json，把銷售中／到期日／售價寫入 Firestore。完整流程順序：1.ACCESS比對 → 2.同步Sheets → 3.比對審查 → 4.回寫銷售中">
        🔍 比對審查
        <input type="file" accept=".csv,.json,.doc,.docx" multiple class="hidden" onchange="cpOpenReview(this)">
      </label>
      <!-- 步驟 4：回寫銷售中 + 補座標 → Sheets（靛藍 = 流程終點） -->
      <button id="cp-writeback-btn" onclick="cpWritebackSelling()"
        class="px-4 py-1.5 rounded-lg bg-indigo-700 hover:bg-indigo-600 text-white text-xs font-semibold transition"
        title="【步驟 4】(1) 把 Firestore 「銷售中」狀態回寫到 Google Sheets；(2) 對銷售中+委託期內+有段別地號但無座標的物件，用 easymap 反查座標，寫回 SHEETS「座標」欄位+Firestore。每筆查 5-10 秒，銷售中物件多會跑 3-10 分鐘。">
        📤 回寫銷售中 + 補座標
      </button>
      <!-- 步驟 5：yes319 全自動（一鍵跑文案 + 照片 + 下架預覽 + 缺漏預覽） -->
      <button id="cp-yes319-btn" onclick="cpSyncYes319All()"
        class="px-4 py-1.5 rounded-lg bg-rose-700 hover:bg-rose-600 text-white text-xs font-semibold transition"
        title="一鍵全自動：(1) 爬 yes319 比對 home-start 推送特色/機能/屋齡/樓層　(2) 對沒照片的物件補 yes319 照片　(3) 列出待下架物件給你確認　(4) 列出 yes319 多出來的物件供查看。預計 10-15 分鐘。">
        🌐 同步 yes319（一鍵全自動）
      </button>
      <!-- 說明按鈕 -->
      <button onclick="document.getElementById('cp-sync-help-modal').style.display='flex'"
        class="px-3 py-1.5 rounded-lg text-xs font-semibold transition" style="background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);"
        title="查看按鈕說明與操作流程">
        ❓ 說明
      </button>
      <!-- 設定按鈕（管理員限定，開啟設定 Modal） -->
      <button onclick="openSettingsModal()"
        class="px-3 py-1.5 rounded-lg text-xs font-semibold transition" style="background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);"
        title="系統設定">
        ⚙️ 設定
      </button>
      <span id="cp-word-status" style="font-size:0.75rem;color:var(--txs);"></span>
      <!-- 物件總表日期標籤 -->
      <span id="cp-doc-date" style="font-size:0.75rem;color:var(--txm);margin-left:0.25rem;" title="物件總表更新日期"></span>
    </div>
  </div>

  <!-- 同步說明 Modal -->
  <div id="cp-sync-help-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:500;align-items:center;justify-content:center;"
    onclick="if(event.target===this)this.style.display='none'">
    <div style="background:var(--bg-s);border:1px solid var(--bd);border-radius:16px;padding:28px 32px;max-width:620px;width:92%;max-height:88vh;display:flex;flex-direction:column;box-shadow:var(--sh);position:relative;">
      <button onclick="document.getElementById('cp-sync-help-modal').style.display='none'"
        style="position:absolute;top:14px;right:18px;background:none;border:none;color:var(--txm);font-size:20px;cursor:pointer;line-height:1;">✕</button>
      <h2 style="color:var(--tx);font-size:16px;font-weight:700;margin:0 0 16px;">📋 按鈕說明與操作流程</h2>
      <div style="overflow-y:auto;flex:1;padding-right:4px;">

        <!-- 按鈕說明 -->
        <div style="margin-bottom:20px;">
          <p style="color:var(--txm);font-size:12px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;margin:0 0 10px;">按鈕功能</p>
          <div style="display:flex;flex-direction:column;gap:10px;">
            <div style="background:var(--bg-t);border:1px solid var(--bd);border-radius:10px;padding:12px 14px;">
              <p style="color:var(--ok);font-weight:700;margin:0 0 4px;font-size:13px;">🔄 同步 Sheets</p>
              <p style="color:var(--txs);font-size:12px;margin:0;">從 Google Sheets 把物件基本資料（案名、地址、類別、經紀人、售價等）同步到 Firestore 資料庫。這是物件資料的<strong style="color:var(--tx);">主要來源</strong>，Sheets 有新增/修改物件後要按此更新。</p>
              <p style="color:var(--txm);font-size:11px;margin:6px 0 0;">⏱ 資料量大時需等待 1～10 分鐘，同步中請勿重複點擊。</p>
            </div>
            <div style="background:var(--bg-t);border:1px solid var(--bd);border-radius:10px;padding:12px 14px;">
              <p style="color:var(--ac);font-weight:700;margin:0 0 4px;font-size:13px;">🔍 比對審查</p>
              <p style="color:var(--txs);font-size:12px;margin:0;">支援兩種上傳方式：（A）<strong>直接上傳 .doc 或 .docx</strong>（雲端自動解析，最方便）；（B）上傳由 <code style="background:var(--bg-p);padding:1px 5px;border-radius:4px;color:var(--ac);">export_word_table.py</code> 產出的 4 個 CSV + <code style="background:var(--bg-p);padding:1px 5px;border-radius:4px;color:var(--ac);">word_meta.json</code>（本機解析，最精確）。系統分析與 Firestore 的配對結果，分為<strong style="color:var(--ok);">高信心</strong>、<strong style="color:var(--warn);">中信心</strong>、問題三組，讓你逐一確認後寫入。</p>
              <p style="color:var(--txm);font-size:11px;margin:6px 0 0;">💡 接受副檔名：<code style="background:var(--bg-p);padding:1px 4px;border-radius:3px;">.doc</code> / <code style="background:var(--bg-p);padding:1px 4px;border-radius:3px;">.docx</code> / <code style="background:var(--bg-p);padding:1px 4px;border-radius:3px;">.csv</code> / <code style="background:var(--bg-p);padding:1px 4px;border-radius:3px;">.json</code>。CSV 方式一次可選 4 個 CSV + 1 個 word_meta.json。</p>
            </div>
            <div style="background:var(--bg-t);border:1px solid var(--bd);border-radius:10px;padding:12px 14px;">
              <p style="font-weight:700;margin:0 0 4px;font-size:13px;color:#a78bfa;">📋 ACCESS 比對更新</p>
              <p style="color:var(--txs);font-size:12px;margin:0;">把公司 Access 的最新資料（已整理到 Google Sheets）與主頁 Sheets 比對，找出差異後讓你選擇性套用。比對結果分三個 Tab：</p>
              <div style="margin:8px 0 0;display:flex;flex-direction:column;gap:4px;font-size:12px;">
                <div style="display:flex;gap:6px;"><span style="color:var(--warn);font-weight:700;white-space:nowrap;">✏️ 修改</span><span style="color:var(--txs);">兩邊都有、但欄位值不同的物件。每個差異欄位都可單獨勾選，只套用你想更新的欄位。</span></div>
                <div style="display:flex;gap:6px;"><span style="color:var(--ok);font-weight:700;white-space:nowrap;">＋ 新增</span><span style="color:var(--txs);">Access 有、主頁 Sheets 沒有的物件，套用後自動插入第 5 列（最上方資料列）並分配序號。</span></div>
                <div style="display:flex;gap:6px;"><span style="color:#f87171;font-weight:700;white-space:nowrap;">● 可能下架</span><span style="color:var(--txs);">主頁有、Access 沒有的物件，僅供參考，不會自動刪除，需人工判斷。</span></div>
              </div>
              <p style="color:var(--txm);font-size:11px;margin:8px 0 0;">💡 Access Sheets 已預設填入。換新表時可<strong>直接貼整段網址</strong>（系統會自動抓 ID），不必再手動剪 ID。比對約需 30～60 秒。</p>
            </div>
          </div>
        </div>

        <!-- 兩條資料來源說明 -->
        <div style="margin-bottom:20px;">
          <p style="color:var(--txm);font-size:12px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;margin:0 0 10px;">資料來源與分工</p>
          <div style="background:var(--bg-t);border:1px solid var(--bd);border-radius:10px;padding:14px 16px;display:flex;flex-direction:column;gap:8px;">
            <div style="display:flex;gap:10px;align-items:flex-start;">
              <span style="background:var(--warn);color:#000;border-radius:6px;padding:2px 7px;font-size:11px;font-weight:700;white-space:nowrap;">Sheets</span>
              <span style="color:var(--txs);font-size:12px;">物件<strong style="color:var(--tx);">基本資料</strong>（案名、地址、類別、經紀人）的主要來源。新增或修改物件後，在此頁按「🔄 同步 Sheets」更新 Firestore。</span>
            </div>
            <div style="border-top:1px solid var(--bd);"></div>
            <div style="display:flex;gap:10px;align-items:flex-start;">
              <span style="background:var(--ok);color:#fff;border-radius:6px;padding:2px 7px;font-size:11px;font-weight:700;white-space:nowrap;">物件總表</span>
              <span style="color:var(--txs);font-size:12px;"><strong style="color:var(--tx);">銷售中狀態、委託到期日、最新售價</strong>的來源。Sheets 不含這些資訊，需靠 Word 物件總表補充。由本機工具處理後寫入 Firestore，<strong style="color:var(--ac);">不需要回此頁上傳</strong>（除非跳過比對審查，直接上傳 CSV）。</span>
            </div>
          </div>

          <!-- 資料安全說明 -->
          <div style="background:var(--bg-t);border:1px solid var(--bd);border-radius:10px;padding:14px 16px;margin-top:10px;">
            <p style="color:var(--warn);font-size:12px;font-weight:700;margin:0 0 8px;">🛡️ 資料安全：CSV 上傳只會動哪些欄位？</p>
            <div style="display:flex;flex-direction:column;gap:6px;font-size:12px;">
              <div style="display:flex;gap:8px;align-items:flex-start;">
                <span style="color:var(--ok);font-weight:700;white-space:nowrap;">只會改</span>
                <span style="color:var(--txs);">銷售中狀態、委託到期日、售價（萬）— 這三個欄位</span>
              </div>
              <div style="display:flex;gap:8px;align-items:flex-start;">
                <span style="color:var(--dg);font-weight:700;white-space:nowrap;">不會動</span>
                <span style="color:var(--txs);">案名、地址、類別、經紀人、所有權人等基本資料</span>
              </div>
              <div style="border-top:1px solid var(--bd);padding-top:6px;display:flex;gap:8px;align-items:flex-start;">
                <span style="color:var(--warn);font-weight:700;white-space:nowrap;">配對邏輯</span>
                <span style="color:var(--txs);">同名物件會同時比對<strong style="color:var(--tx);">委託號碼 → 經紀人 → 售價 → 面積</strong>，層層過濾，避免同名不同人的物件互相污染。</span>
              </div>
              <div style="border-top:1px solid var(--bd);padding-top:6px;display:flex;gap:8px;align-items:flex-start;">
                <span style="color:var(--ac);font-weight:700;white-space:nowrap;">萬一誤改</span>
                <span style="color:var(--txs);">在此頁按「🔄 同步 Sheets」即可將售價還原回 Sheets 的原始值。Sheets 是最終原始依據，永遠可以用來救回資料。</span>
              </div>
            </div>
          </div>
        </div>

        <!-- 操作流程 -->
        <div style="margin-bottom:20px;">
          <p style="color:var(--txm);font-size:12px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;margin:0 0 10px;">操作流程</p>

          <!-- 按鈕順序總覽 -->
          <div style="background:linear-gradient(135deg,rgba(167,139,250,.12),rgba(96,165,250,.12));border:1px solid var(--bd);border-radius:10px;padding:14px 16px;margin-bottom:10px;">
            <p style="color:var(--tx);font-size:12px;font-weight:700;margin:0 0 10px;">🧭 四個按鈕的標準順序（全部都要做時）</p>
            <div style="display:flex;flex-direction:column;gap:6px;font-size:12px;">
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#e11d48;color:#fff;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;">1</span><span style="color:var(--txs);"><strong style="color:#e11d48;">📋 ACCESS比對</strong>　→　把 Access 新資料寫入主頁 Sheets（補新增物件、改欄位）</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#d97706;color:#fff;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;">2</span><span style="color:var(--txs);"><strong style="color:#d97706;">🔄 同步 Sheets</strong>　→　Sheets 最新內容寫入 Firestore（網頁才查得到）</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#0f766e;color:#fff;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;">3</span><span style="color:var(--txs);"><strong style="color:#0f766e;">🔍 比對審查</strong>　→　上傳 Word（.doc/.docx）或 CSV，把<em>銷售中／到期日／售價</em>寫入 Firestore</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#4338ca;color:#fff;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;">4</span><span style="color:var(--txs);"><strong style="color:#4338ca;">📤 回寫銷售中</strong>　→　Firestore 銷售中狀態回寫 Sheets（兩邊一致）</span></div>
            </div>
            <p style="color:var(--txm);font-size:11px;margin:10px 0 6px;font-weight:700;">為什麼是這個順序？</p>
            <ul style="color:var(--txs);font-size:11px;margin:0;padding-left:18px;line-height:1.7;">
              <li>1→2：ACCESS 改完 Sheets 後要再同步，否則網頁看到的還是舊資料</li>
              <li>2→3：先同步好基本資料，比對審查才有正確的物件可比對</li>
              <li>3→4：Word 的銷售中進 Firestore 後要回寫 Sheets，否則下次同步 Sheets 又會把銷售中打回舊狀態</li>
            </ul>
            <p style="color:var(--txm);font-size:11px;margin:10px 0 6px;font-weight:700;">不一定四個都要按：</p>
            <ul style="color:var(--txs);font-size:11px;margin:0;padding-left:18px;line-height:1.7;">
              <li>只有 Access 更新　→　只跑 <strong>1+2</strong></li>
              <li>只有 Word 物件總表更新　→　只跑 <strong>3+4</strong></li>
              <li>單純 Sheets 改了東西　→　只跑 <strong>2</strong></li>
            </ul>
            <p style="color:var(--warn);font-size:11px;margin:10px 0 0;">⚠️ <strong>重點</strong>：步驟 2「同步 Sheets」會用 Sheets 內容覆蓋 Firestore，所以一定要在步驟 3「比對審查」之前做，不然 Word 寫進 Firestore 的銷售中狀態會被舊 Sheets 蓋掉。</p>
          </div>

          <!-- 情境一 -->
          <div style="background:var(--bg-t);border:1px solid var(--bd);border-radius:10px;padding:14px 16px;margin-bottom:10px;">
            <p style="color:var(--warn);font-size:12px;font-weight:700;margin:0 0 10px;">📌 情境一：Sheets 有新增或修改物件</p>
            <div style="display:flex;flex-direction:column;gap:6px;">
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--warn);color:#000;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">1</span><span style="color:var(--txs);font-size:12px;">在此頁按「<strong style="color:var(--warn);">🔄 同步 Sheets</strong>」→ 等待完成（約 1～10 分鐘）</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--txs);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">✓</span><span style="color:var(--txm);font-size:12px;">基本資料更新完成。<em>銷售中狀態/到期日/售價</em>若需同步，請另跑物件總表流程（情境二）。</span></div>
            </div>
          </div>

          <!-- 情境二 -->
          <div style="background:var(--bg-t);border:1px solid var(--bd);border-radius:10px;padding:14px 16px;">
            <p style="color:var(--ac);font-size:12px;font-weight:700;margin:0 0 10px;">📌 情境二：公司發下新版物件總表 Word 檔</p>
            <p style="color:var(--txm);font-size:11px;margin:0 0 8px;">兩種方式擇一：</p>
            <p style="color:var(--tx);font-size:11px;font-weight:700;margin:0 0 4px;">方式 A：直接上傳 .doc / .docx（最方便）</p>
            <div style="display:flex;flex-direction:column;gap:6px;margin-bottom:10px;">
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--tx);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">1</span><span style="color:var(--txs);font-size:12px;">按「<strong style="color:var(--ac);">🔍 比對審查</strong>」，直接選取 Word 原檔（<code style="background:var(--bg-p);padding:1px 4px;border-radius:3px;">.doc</code> 或 <code style="background:var(--bg-p);padding:1px 4px;border-radius:3px;">.docx</code>）</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--tx);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">2</span><span style="color:var(--txs);font-size:12px;">雲端自動解析（用 word_parser.py），約需 30～60 秒</span></div>
            </div>
            <p style="color:var(--tx);font-size:11px;font-weight:700;margin:0 0 4px;">方式 B：本機處理成 CSV 再上傳（解析最精確）</p>
            <div style="display:flex;flex-direction:column;gap:6px;">
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--tx);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">1</span><span style="color:var(--txs);font-size:12px;">本機執行：<code style="background:var(--bg-p);padding:1px 6px;border-radius:4px;color:var(--ac);font-size:11px;">python3 export_word_table.py</code> → 產出 4 個 CSV + word_meta.json</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--tx);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">2</span><span style="color:var(--txs);font-size:12px;">回此頁按「<strong style="color:var(--ac);">🔍 比對審查</strong>」，一次選取 5 個檔案（4 CSV + 1 word_meta.json）</span></div>
            </div>
            <p style="color:var(--txm);font-size:11px;margin:8px 0 6px;">⬇ 兩種方式之後的步驟相同：</p>
            <div style="display:flex;flex-direction:column;gap:6px;">
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--tx);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">3</span><span style="color:var(--txs);font-size:12px;">審查介面顯示三組結果：✅ 高信心全選套用、⚠️ 中信心逐一確認、❓ 問題筆數供參考</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--ok);color:#fff;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">✓</span><span style="color:var(--ok);font-size:12px;"><strong>按「套用確認的配對」→ 直接寫入 Firestore，完成！</strong></span></div>
            </div>
          </div>

          <!-- 情境三 -->
          <div style="background:var(--bg-t);border:1px solid var(--bd);border-radius:10px;padding:14px 16px;margin-top:10px;">
            <p style="color:#a78bfa;font-size:12px;font-weight:700;margin:0 0 10px;">📌 情境三：Access 資料有更新，想同步回主頁 Sheets</p>
            <div style="display:flex;flex-direction:column;gap:7px;">
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--tx);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">1</span><span style="color:var(--txs);font-size:12px;">在此頁按「<strong style="color:#a78bfa;">📋 ACCESS比對</strong>」，預設已填入 Sheets，直接按「開始比對」；換新表時把整段<strong>網址</strong>貼上即可（也接受純 ID）</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--tx);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">2</span><span style="color:var(--txs);font-size:12px;">比對完成後，在「修改」Tab 用搜尋框找到想確認的物件，逐一取消不想套用的欄位勾選</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--tx);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">3</span><span style="color:var(--txs);font-size:12px;">若某個差異已知是 Access 打錯的，按欄位旁的「🔒鎖定」，下次比對自動預設不套用</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--ok);color:#fff;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">✓</span><span style="color:var(--ok);font-size:12px;"><strong>按「套用選取變更」→ 直接寫入主頁 Sheets，完成！</strong></span></div>
            </div>
          </div>

          <!-- 鎖定功能說明 -->
          <div style="background:var(--bg-t);border:1px solid var(--bd);border-radius:10px;padding:14px 16px;margin-top:10px;">
            <p style="color:var(--txm);font-size:12px;font-weight:700;margin:0 0 8px;">🔒 忽略規則（鎖定差異）</p>
            <div style="display:flex;flex-direction:column;gap:5px;font-size:12px;color:var(--txs);">
              <p style="margin:0;">若 Access 助理長期打錯某個欄位，你已知道不需要更新，可對該欄位按「🔒鎖定」。</p>
              <p style="margin:0;">下次比對時，被鎖定的差異會以灰色 🔒 顯示，預設不勾選，不影響其他欄位的套用。</p>
              <p style="margin:0;">鎖定規則是<strong style="color:var(--tx);">全公司共用</strong>，在 Modal 右上角按「🔒 忽略規則」可查看所有規則，並一鍵解鎖。</p>
            </div>
          </div>
        </div>

        <!-- 本機工具說明 -->
        <div>
          <p style="color:var(--txm);font-size:12px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;margin:0 0 10px;">本機工具說明</p>
          <div style="background:var(--bg-t);border:1px solid var(--bd);border-radius:10px;padding:14px 16px;display:flex;flex-direction:column;gap:10px;">
            <div>
              <p style="color:var(--ac);font-weight:600;font-size:12px;margin:0 0 3px;"><code style="background:var(--bg-p);padding:1px 5px;border-radius:4px;">export_word_table.py</code></p>
              <p style="color:var(--txs);font-size:12px;margin:0;">讀取 Word 物件總表，精確解析各類型（公寓/房屋/農地/建地）的欄位，輸出 CSV 檔。解析規則經過多次磨合，是目前最精確的版本。<br>路徑：<code style="background:var(--bg-p);padding:1px 5px;border-radius:4px;color:var(--ac);font-size:11px;">/Users/chenweiliang/Projects/export_word_table.py</code></p>
            </div>
            <div style="border-top:1px solid var(--bd);padding-top:10px;">
              <p style="color:#a78bfa;font-weight:600;font-size:12px;margin:0 0 3px;"><code style="background:var(--bg-p);padding:1px 5px;border-radius:4px;">review_v2.py</code>（舊版本機審查工具，已整合進雲端）</p>
              <p style="color:var(--txs);font-size:12px;margin:0;">本機版比對審查工具，功能已整合進此頁的「🔍 比對審查」按鈕。<br>若需要使用舊版（含記憶庫功能），路徑：<code style="background:var(--bg-p);padding:1px 5px;border-radius:4px;color:var(--ac);font-size:11px;">/Users/chenweiliang/Projects/review_v2.py</code></p>
            </div>
          </div>
        </div>

      </div>
    </div>
  </div>

  <!-- 設定 Modal（管理員限定，由 cp-sync-bar 的 ⚙️ 設定 按鈕開啟） -->
  <div id="settings-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:700;align-items:center;justify-content:center;"
    onclick="if(event.target===this)document.getElementById('settings-modal').style.display='none'">
    <div style="background:var(--bg-s);border:1px solid var(--bd);border-radius:16px;padding:0;width:92%;max-width:560px;max-height:88vh;display:flex;flex-direction:column;box-shadow:var(--sh);position:relative;overflow:hidden;">
      <!-- Header -->
      <div style="padding:16px 24px 12px;border-bottom:1px solid var(--bd);display:flex;align-items:center;justify-content:space-between;">
        <span style="font-size:15px;font-weight:700;color:var(--tx);">⚙️ 系統設定</span>
        <button onclick="document.getElementById('settings-modal').style.display='none'"
          style="background:none;border:none;color:var(--txm);font-size:20px;cursor:pointer;line-height:1;">✕</button>
      </div>
      <!-- 內容（可捲動） -->
      <div style="overflow-y:auto;flex:1;padding:20px 24px;display:flex;flex-direction:column;gap:20px;">

        <!-- 經紀人 Email 管理 -->
        <div class="rounded-2xl p-5" style="background:var(--bg-t);border:1px solid var(--bd);">
          <div class="flex items-center justify-between mb-4">
            <div>
              <h3 class="font-semibold" style="color:var(--tx);">📧 經紀人 Email 管理</h3>
              <p class="text-xs mt-0.5" style="color:var(--txs);">設定各經紀人的通知 Email，委託到期日通知時使用</p>
            </div>
            <button onclick="agentEmailOpenAdd()"
              class="px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold transition">
              ＋ 新增
            </button>
          </div>
          <!-- 新增/編輯表單（預設隱藏） -->
          <div id="agent-email-form" class="hidden rounded-xl p-4 mb-4" style="background:var(--bg-h);border:1px solid var(--bd);">
            <div class="grid grid-cols-2 gap-3 mb-3">
              <div>
                <label class="text-xs block mb-1" style="color:var(--txs);">經紀人姓名</label>
                <input id="agent-email-name" type="text" placeholder="如：陳威良"
                  class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-t);border:1px solid var(--bd);color:var(--tx);">
              </div>
              <div>
                <label class="text-xs block mb-1" style="color:var(--txs);">Email</label>
                <input id="agent-email-addr" type="email" placeholder="如：abc@gmail.com"
                  class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-t);border:1px solid var(--bd);color:var(--tx);">
              </div>
            </div>
            <div class="flex gap-2">
              <button onclick="agentEmailSave()"
                class="px-4 py-1.5 rounded-lg bg-green-600 hover:bg-green-500 text-white text-xs font-semibold transition">儲存</button>
              <button onclick="agentEmailCloseForm()"
                class="px-4 py-1.5 rounded-lg text-xs transition" style="background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);">取消</button>
            </div>
          </div>
          <!-- 列表 -->
          <div id="agent-email-list" class="space-y-2">
            <p class="text-sm text-center py-4" style="color:var(--txm);">載入中…</p>
          </div>
        </div>

        <!-- 物件搜尋索引 -->
        <div class="rounded-2xl p-5" style="background:var(--bg-t);border:1px solid var(--bd);">
          <h3 class="font-semibold mb-1" style="color:var(--tx);">🔍 物件搜尋索引</h3>
          <p class="text-xs mb-3" style="color:var(--txs);">買方管理輸入物件名稱時的自動完成資料來源。每次「立即同步 Sheets」後自動更新，也可手動重建。</p>
          <button onclick="rebuildPropIndex()"
            class="px-4 py-2 rounded-lg text-sm font-semibold transition" style="background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);">
            重建物件索引
          </button>
          <p id="prop-index-result" class="text-xs mt-2" style="color:var(--txs);"></p>
        </div>

        <!-- 到期通知測試 -->
        <div class="rounded-2xl p-5" style="background:var(--bg-t);border:1px solid var(--bd);">
          <h3 class="font-semibold mb-1" style="color:var(--tx);">🔔 到期通知測試</h3>
          <p class="text-xs mb-3" style="color:var(--txs);">手動觸發一次到期日通知，確認 Email 是否正常發送（每天早上 8 點自動執行）</p>
          <button onclick="triggerNotify()"
            class="px-4 py-2 rounded-lg bg-amber-600 hover:bg-amber-500 text-white text-sm font-semibold transition">
            立即執行通知
          </button>
          <p id="notify-result" class="text-xs mt-2" style="color:var(--txs);"></p>
        </div>

      </div>
    </div>
  </div>

  <!-- 物件總表比對審查 Modal（僅管理員，日盛房屋專用） -->
  <div id="cp-review-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:600;align-items:flex-start;justify-content:center;padding-top:32px;"
    onclick="if(event.target===this)cpCloseReview()">
    <div style="background:var(--bg-s);border:1px solid var(--bd);border-radius:16px;width:96%;max-width:800px;max-height:88vh;display:flex;flex-direction:column;box-shadow:var(--sh);position:relative;">
      <!-- Header -->
      <div style="padding:18px 24px 12px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:12px;">
        <span style="font-size:15px;font-weight:700;color:var(--tx);">🔍 物件總表比對審查</span>
        <span id="rv-subtitle" style="font-size:12px;color:var(--txs);"></span>
        <button onclick="cpCloseReview()"
          style="margin-left:auto;background:none;border:none;color:var(--txm);font-size:20px;cursor:pointer;line-height:1;">✕</button>
      </div>
      <!-- Loading -->
      <div id="rv-loading" style="padding:48px;text-align:center;color:var(--txs);font-size:14px;">
        <div style="font-size:28px;margin-bottom:12px;">⏳</div>
        <div id="rv-loading-text">分析中，請稍候…</div>
      </div>
      <!-- 結果區 -->
      <div id="rv-results" style="display:none;flex:1;overflow:hidden;flex-direction:column;">
        <!-- 強行配對記憶抽屜 -->
        <div id="rv-mem-drawer" style="border-bottom:1px solid var(--bd);background:var(--bg-t);">
          <div onclick="rvToggleMemDrawer()" style="padding:8px 24px;cursor:pointer;display:flex;align-items:center;gap:8px;user-select:none;">
            <span style="font-size:12px;font-weight:700;color:var(--txm);">🧠 強行配對記憶</span>
            <span id="rv-mem-badge" style="background:var(--bg-h);color:var(--txs);border-radius:9px;padding:1px 7px;font-size:11px;">0</span>
            <span id="rv-mem-arrow" style="margin-left:auto;color:var(--txs);font-size:11px;">▼ 展開</span>
          </div>
          <div id="rv-mem-body" style="display:none;padding:0 24px 10px;overflow-x:auto;">
            <table style="width:100%;border-collapse:collapse;font-size:11px;">
              <thead>
                <tr style="color:var(--txs);">
                  <th style="text-align:left;padding:3px 6px;border-bottom:1px solid var(--bd);">委託號</th>
                  <th style="text-align:left;padding:3px 6px;border-bottom:1px solid var(--bd);">Word 案名</th>
                  <th style="text-align:left;padding:3px 6px;border-bottom:1px solid var(--bd);">→ Firestore 序號</th>
                  <th style="text-align:left;padding:3px 6px;border-bottom:1px solid var(--bd);">備註</th>
                  <th style="padding:3px 6px;border-bottom:1px solid var(--bd);"></th>
                </tr>
              </thead>
              <tbody id="rv-mem-tbody"></tbody>
            </table>
          </div>
        </div>
        <!-- Tabs -->
        <div style="display:flex;gap:0;border-bottom:1px solid var(--bd);padding:0 24px;">
          <button id="rv-tab-high-btn" onclick="rvTab('high')"
            style="padding:10px 16px;font-size:12px;font-weight:600;border:none;background:none;border-bottom:2px solid var(--ac);color:var(--ac);cursor:pointer;">
            ✅ 高信心 <span id="rv-count-high" style="background:var(--ac);color:#fff;border-radius:9px;padding:1px 7px;margin-left:4px;">0</span>
          </button>
          <button id="rv-tab-medium-btn" onclick="rvTab('medium')"
            style="padding:10px 16px;font-size:12px;font-weight:600;border:none;background:none;border-bottom:2px solid transparent;color:var(--txm);cursor:pointer;">
            ⚠️ 中信心 <span id="rv-count-medium" style="background:var(--bg-h);color:var(--txs);border-radius:9px;padding:1px 7px;margin-left:4px;">0</span>
          </button>
          <button id="rv-tab-issues-btn" onclick="rvTab('issues')"
            style="padding:10px 16px;font-size:12px;font-weight:600;border:none;background:none;border-bottom:2px solid transparent;color:var(--txm);cursor:pointer;">
            ❓ 問題 <span id="rv-count-issues" style="background:var(--bg-h);color:var(--txs);border-radius:9px;padding:1px 7px;margin-left:4px;">0</span>
          </button>
        </div>
        <!-- Tab 內容 -->
        <div style="flex:1;overflow-y:auto;padding:16px 24px;">
          <!-- 高信心 -->
          <div id="rv-pane-high">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">
              <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--txs);cursor:pointer;">
                <input type="checkbox" id="rv-high-all" checked onchange="rvToggleAll(this)"> 全選／全消
              </label>
              <span style="font-size:12px;color:var(--txs);">以下物件配對信心高，預設全選。取消勾選即排除。</span>
            </div>
            <div id="rv-high-list" style="display:flex;flex-direction:column;gap:8px;"></div>
          </div>
          <!-- 中信心 -->
          <div id="rv-pane-medium" style="display:none;">
            <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:10px;">
              <p style="font-size:12px;color:var(--txs);margin:0;flex:1;min-width:240px;">以下物件配對有些不確定，請逐一確認。✅ 確認配對，❌ 跳過此筆。</p>
              <button id="rv-ai-medium-btn" onclick="rvRunAiMatchMedium()"
                style="padding:6px 12px;border-radius:8px;background:linear-gradient(135deg,#6366f1,#8b5cf6);color:#fff;border:none;font-size:12px;font-weight:700;cursor:pointer;"
                title="用 Gemini 驗證規則找到的中信心配對：AI 同意 → 強化信心；AI 不同意 → 提示重新檢視；AI 找到更好的 → 顯示替代配對">
                🤖 AI 驗證
              </button>
            </div>
            <div id="rv-ai-medium-result" style="display:none;margin-bottom:14px;padding:12px;background:linear-gradient(135deg,rgba(99,102,241,.08),rgba(139,92,246,.08));border:1px solid rgba(139,92,246,.3);border-radius:10px;"></div>
            <div id="rv-medium-list" style="display:flex;flex-direction:column;gap:8px;"></div>
          </div>
          <!-- 問題（衝突 + 未配對） -->
          <div id="rv-pane-issues" style="display:none;">
            <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:10px;">
              <p style="font-size:12px;color:var(--txs);margin:0;flex:1;min-width:240px;">以下 Word 條目在 Firestore 找不到對應或特徵衝突。可按右側按鈕讓 Gemini 嘗試配對。</p>
              <button id="rv-ai-match-btn" onclick="rvRunAiMatch()"
                style="padding:6px 12px;border-radius:8px;background:linear-gradient(135deg,#6366f1,#8b5cf6);color:#fff;border:none;font-size:12px;font-weight:700;cursor:pointer;"
                title="把問題組丟給 Gemini 2.0 Flash 重新配對，AI 會給出建議與信心分數，最終由你確認">
                🤖 用 Gemini 重新配對
              </button>
            </div>
            <!-- 同步 Sheets 提示 banner（找不到對應常因 Firestore 沒同步主 Sheets 新資料）-->
            <div style="margin-bottom:12px;padding:10px 12px;background:rgba(217,119,6,0.10);border:1px solid rgba(217,119,6,0.3);border-radius:8px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;font-size:12px;">
              <span style="color:#d97706;font-weight:700;">💡 找不到對應通常是因為：</span>
              <span style="color:var(--txs);flex:1;min-width:200px;">主 Sheets 有這筆物件，但 Firestore 還沒同步進來。可以先做一次「🔄 同步 Sheets」，再重新跑比對審查。</span>
              <button onclick="rvTriggerSyncFromReview()"
                style="padding:5px 12px;border-radius:6px;background:#d97706;color:#fff;border:none;font-size:12px;font-weight:700;cursor:pointer;flex-shrink:0;">
                🔄 立即同步 Sheets
              </button>
            </div>
            <div id="rv-ai-result" style="display:none;margin-bottom:14px;padding:12px;background:linear-gradient(135deg,rgba(99,102,241,.08),rgba(139,92,246,.08));border:1px solid rgba(139,92,246,.3);border-radius:10px;"></div>
            <div id="rv-issues-list" style="display:flex;flex-direction:column;gap:8px;"></div>
          </div>
        </div>
        <!-- Footer -->
        <div style="padding:14px 24px;border-top:1px solid var(--bd);display:flex;align-items:center;gap:12px;background:var(--bg-s);">
          <span id="rv-apply-count" style="font-size:12px;color:var(--txs);">已選 0 筆</span>
          <button onclick="cpApplyReview()"
            style="margin-left:auto;padding:8px 20px;border-radius:8px;background:var(--ac);color:#fff;border:none;font-size:13px;font-weight:700;cursor:pointer;">
            ✅ 套用確認的配對
          </button>
          <button onclick="cpCloseReview()"
            style="padding:8px 16px;border-radius:8px;background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);font-size:12px;cursor:pointer;">
            取消
          </button>
        </div>
      </div>
    </div>
  </div>

  <!-- 配對記憶確認 Modal（套用成功後若有手動確認的配對才彈出） -->
  <div id="rv-memory-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:700;align-items:center;justify-content:center;">
    <div style="background:var(--bg-s);border:1px solid var(--bd);border-radius:16px;width:96%;max-width:680px;max-height:88vh;display:flex;flex-direction:column;box-shadow:var(--sh);">
      <div style="padding:18px 22px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:10px;">
        <span style="font-size:18px;">🧠</span>
        <span style="font-size:15px;font-weight:700;color:var(--tx);">記住這些配對嗎？</span>
        <span style="margin-left:auto;font-size:11px;color:var(--txm);">下次同案名／同委託號的 Word 條目會自動配對到相同物件</span>
      </div>
      <div style="padding:12px 22px;flex:1;overflow-y:auto;">
        <p style="font-size:12px;color:var(--txs);margin:0 0 12px;">您剛剛<strong style="color:var(--tx);">手動確認</strong>了以下配對。勾選要記住的項目，下次跑「比對審查」時會自動配對，省去重複確認的時間。</p>
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;">
          <label style="display:flex;align-items:center;gap:6px;font-size:12px;color:var(--txs);cursor:pointer;">
            <input type="checkbox" id="rv-mem-all" checked onchange="rvMemToggleAll(this)"> 全選／全消
          </label>
          <span id="rv-mem-count" style="font-size:11px;color:var(--txm);">已選 0 筆</span>
        </div>
        <div id="rv-mem-list" style="display:flex;flex-direction:column;gap:6px;"></div>
      </div>
      <div style="padding:14px 22px;border-top:1px solid var(--bd);display:flex;align-items:center;gap:10px;background:var(--bg-t);">
        <button onclick="rvMemSkip()"
          style="padding:8px 16px;border-radius:8px;background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);font-size:12px;cursor:pointer;">
          ❌ 不記住，直接關閉
        </button>
        <button onclick="rvMemSave()"
          style="margin-left:auto;padding:8px 18px;border-radius:8px;background:var(--ok);color:#fff;border:none;font-size:13px;font-weight:700;cursor:pointer;">
          🧠 記住勾選的配對
        </button>
      </div>
    </div>
  </div>

  <!-- ACCESS 比對更新 Modal（管理員限定） -->
  <div id="ac-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:650;align-items:flex-start;justify-content:center;padding-top:28px;"
    onclick="if(event.target===this)document.getElementById('ac-modal').style.display='none'">
    <div style="background:var(--bg-s);border:1px solid var(--bd);border-radius:16px;width:96%;max-width:820px;max-height:90vh;display:flex;flex-direction:column;box-shadow:var(--sh);overflow:hidden;">

      <!-- Header -->
      <div style="padding:16px 22px 12px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:10px;flex-shrink:0;">
        <span style="font-size:15px;font-weight:700;color:var(--tx);">📋 ACCESS 比對更新</span>
        <span id="ac-subtitle" style="font-size:12px;color:var(--txs);"></span>
        <button onclick="openAuditModal()"
          style="margin-left:auto;font-size:12px;padding:4px 10px;border:1px solid var(--bd);border-radius:6px;background:var(--bg-t);color:var(--txm);cursor:pointer;"
          title="掃描銷售中物件，列出缺鄉/市/鎮、段別、地號、建號（建物）的物件。這些物件比對精準度低，建議補齊。">🩺 資料體檢</button>
        <button onclick="openDupModal()"
          style="margin-left:8px;font-size:12px;padding:4px 10px;border:1px solid var(--bd);border-radius:6px;background:var(--bg-t);color:var(--txm);cursor:pointer;"
          title="掃描主頁 Sheets 找重複物件：完全重複（同硬資料+同委託）一定要刪、歷史版本（同硬資料但委託不同）你決定">🧹 重複清理</button>
        <button onclick="openAcIgnoreModal()"
          style="margin-left:8px;font-size:12px;padding:4px 10px;border:1px solid var(--bd);border-radius:6px;background:var(--bg-t);color:var(--txm);cursor:pointer;">🔒 忽略規則</button>
        <button onclick="document.getElementById('ac-modal').style.display='none'"
          style="margin-left:8px;background:none;border:none;color:var(--txm);font-size:20px;cursor:pointer;line-height:1;">✕</button>
      </div>

      <!-- 輸入區 -->
      <div id="ac-input-section" style="padding:16px 22px;border-bottom:1px solid var(--bd);flex-shrink:0;">
        <p style="font-size:12px;color:var(--txs);margin:0 0 12px;">
          把公司最新 Access 資料（欄位與主頁相同的 A~AU 欄）貼到一張全新的 Google Sheets，再把該 Sheets 的<strong>網址或 ID</strong>貼到下方開始比對。
        </p>
        <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;">
          <div style="flex:1;min-width:220px;">
            <label style="font-size:11px;color:var(--txs);display:block;margin-bottom:4px;">新 Sheets 網址或 ID（必填）</label>
            <input id="ac-sheet-id" type="text" value="1_PE14LjVJ0M0Z2npklYHfmimfyVw98QLWK7cGq-oF7U"
              placeholder="貼上 Google Sheets 網址或 ID 都可以"
              style="width:100%;padding:7px 10px;border-radius:8px;border:1px solid var(--bd);background:var(--bg-t);color:var(--tx);font-size:12px;box-sizing:border-box;">
          </div>
          <div style="width:130px;">
            <label style="font-size:11px;color:var(--txs);display:block;margin-bottom:4px;">分頁名稱（空白=第一個）</label>
            <input id="ac-sheet-name" type="text" placeholder="工作表1"
              style="width:100%;padding:7px 10px;border-radius:8px;border:1px solid var(--bd);background:var(--bg-t);color:var(--tx);font-size:12px;box-sizing:border-box;">
          </div>
          <div style="width:160px;">
            <label style="font-size:11px;color:var(--txs);display:block;margin-bottom:4px;" title="只比對委託日 >= 此日期的物件，空白者也保留">📅 委託日門檻</label>
            <input id="ac-min-date" type="date" value="2013-01-01"
              style="width:100%;padding:7px 10px;border-radius:8px;border:1px solid var(--bd);background:var(--bg-t);color:var(--tx);font-size:12px;box-sizing:border-box;">
          </div>
          <button id="ac-compare-btn" onclick="accessRunCompare()"
            style="padding:8px 18px;border-radius:8px;background:var(--ac);color:#fff;border:none;font-size:13px;font-weight:700;cursor:pointer;white-space:nowrap;">
            開始比對
          </button>
        </div>
        <p style="margin:6px 0 0;font-size:10px;color:var(--txm);">💡 預設只比對民國 102/1/1（西元 2013/1/1）後的物件，省資源。委託日空白者保留。要比所有資料把日期改成 1900-01-01。</p>
        <div id="ac-loading" style="display:none;padding:10px 0;font-size:13px;color:var(--txs);">⏳ 比對中，請稍候…</div>
      </div>

      <!-- 比對結果區 -->
      <div id="ac-results" style="display:none;flex:1;overflow:hidden;flex-direction:column;">

        <!-- 分頁按鈕 -->
        <div style="display:flex;gap:0;border-bottom:1px solid var(--bd);padding:0 22px;flex-shrink:0;">
          <button id="ac-tab-mod-btn" onclick="acTab('mod')"
            style="padding:9px 14px;font-size:12px;font-weight:600;border:none;background:none;border-bottom:2px solid var(--ac);color:var(--ac);cursor:pointer;">
            ✏️ 修改 <span id="ac-cnt-mod" style="background:var(--ac);color:#fff;border-radius:9px;padding:1px 7px;margin-left:3px;">0</span>
          </button>
          <button id="ac-tab-add-btn" onclick="acTab('add')"
            style="padding:9px 14px;font-size:12px;font-weight:600;border:none;background:none;border-bottom:2px solid transparent;color:var(--txm);cursor:pointer;">
            ➕ 新增 <span id="ac-cnt-add" style="background:var(--bg-h);color:var(--txs);border-radius:9px;padding:1px 7px;margin-left:3px;">0</span>
          </button>
          <button id="ac-tab-rem-btn" onclick="acTab('rem')"
            style="padding:9px 14px;font-size:12px;font-weight:600;border:none;background:none;border-bottom:2px solid transparent;color:var(--txm);cursor:pointer;">
            🔴 可能下架 <span id="ac-cnt-rem" style="background:var(--bg-h);color:var(--txs);border-radius:9px;padding:1px 7px;margin-left:3px;">0</span>
          </button>
        </div>

        <!-- 分頁內容 -->
        <div style="flex:1;overflow-y:auto;padding:14px 22px;">

          <!-- 修改 Tab -->
          <div id="ac-pane-mod">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap;">
              <label style="display:flex;align-items:center;gap:5px;font-size:12px;color:var(--txs);cursor:pointer;">
                <input type="checkbox" id="ac-mod-all" checked onchange="acToggleAll('mod',this)"> 全選
              </label>
              <input id="ac-mod-filter" type="text" placeholder="🔍 搜尋案名…"
                oninput="acFilterMod(this.value)"
                style="flex:1;min-width:120px;max-width:220px;font-size:12px;padding:3px 8px;border:1px solid var(--bd);border-radius:6px;background:var(--bg);color:var(--tx);">
              <span id="ac-mod-filter-hint" style="font-size:11px;color:var(--txs);"></span>
              <span style="font-size:11px;color:var(--txs);">勾選要套用的修改項目</span>
            </div>
            <div id="ac-list-mod" style="display:flex;flex-direction:column;gap:8px;"></div>
          </div>

          <!-- 新增 Tab -->
          <div id="ac-pane-add" style="display:none;">
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">
              <label style="display:flex;align-items:center;gap:5px;font-size:12px;color:var(--txs);cursor:pointer;">
                <input type="checkbox" id="ac-add-all" checked onchange="acToggleAll('add',this)"> 全選
              </label>
              <span style="font-size:11px;color:var(--txs);">勾選要新增到主頁的物件（自動分配資料序號）</span>
            </div>
            <div id="ac-list-add" style="display:flex;flex-direction:column;gap:8px;"></div>
          </div>

          <!-- 可能下架 Tab（僅提示，不套用） -->
          <div id="ac-pane-rem" style="display:none;">
            <p style="font-size:12px;color:var(--txs);margin:0 0 10px;">⚠️ 以下物件在主頁有、但 Access 沒有。<strong>僅供提示，不會自動刪除。</strong>請自行確認是否已下架，再到 Sheets 手動更新「銷售中」欄。</p>
            <div id="ac-list-rem" style="display:flex;flex-direction:column;gap:6px;"></div>
          </div>
        </div>

        <!-- Footer -->
        <div style="padding:12px 22px;border-top:1px solid var(--bd);display:flex;align-items:center;gap:12px;background:var(--bg-s);flex-shrink:0;">
          <span id="ac-apply-count" style="font-size:12px;color:var(--txs);"></span>
          <button onclick="accessApply()"
            style="margin-left:auto;padding:8px 20px;border-radius:8px;background:var(--ok);color:#fff;border:none;font-size:13px;font-weight:700;cursor:pointer;">
            ✅ 套用選取變更
          </button>
          <button onclick="document.getElementById('ac-modal').style.display='none'"
            style="padding:8px 14px;border-radius:8px;background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);font-size:12px;cursor:pointer;">
            取消
          </button>
        </div>
      </div>
    </div>
  </div>

  <!-- ACCESS 忽略規則管理 Modal -->
  <div id="ac-ignore-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:700;align-items:flex-start;justify-content:center;padding-top:40px;"
    onclick="if(event.target===this)document.getElementById('ac-ignore-modal').style.display='none'">
    <div style="background:var(--bg);border-radius:16px;width:min(680px,96vw);max-height:80vh;display:flex;flex-direction:column;overflow:hidden;box-shadow:0 8px 40px rgba(0,0,0,.3);">
      <div style="padding:16px 22px 12px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:10px;flex-shrink:0;">
        <span style="font-size:15px;font-weight:700;color:var(--tx);">🔒 ACCESS 忽略規則管理</span>
        <button onclick="document.getElementById('ac-ignore-modal').style.display='none'"
          style="margin-left:auto;background:none;border:none;color:var(--txm);font-size:20px;cursor:pointer;line-height:1;">✕</button>
      </div>
      <div style="padding:12px 22px;flex:1;overflow-y:auto;">
        <p style="font-size:12px;color:var(--txs);margin:0 0 12px;">以下差異被標記為「忽略」，比對時會以 🔒 顯示、預設不套用。點「解鎖」可恢復正常比對。</p>
        <div id="ac-ignore-list" style="display:flex;flex-direction:column;gap:8px;">
          <p style="color:var(--txs);font-size:13px;">載入中…</p>
        </div>
      </div>
      <div style="padding:12px 22px;border-top:1px solid var(--bd);flex-shrink:0;text-align:right;">
        <button onclick="document.getElementById('ac-ignore-modal').style.display='none'"
          style="padding:7px 18px;border-radius:8px;background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);font-size:12px;cursor:pointer;">關閉</button>
      </div>
    </div>
  </div>

  <!-- 已檢查清單管理 Modal -->
  <div id="ac-audit-acks-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:710;align-items:center;justify-content:center;"
    onclick="if(event.target===this)document.getElementById('ac-audit-acks-modal').style.display='none'">
    <div style="background:var(--bg-s);border:1px solid var(--bd);border-radius:16px;width:96%;max-width:680px;max-height:85vh;display:flex;flex-direction:column;box-shadow:var(--sh);">
      <div style="padding:16px 22px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:10px;">
        <span style="font-size:16px;">📋</span>
        <span style="font-size:14px;font-weight:700;color:var(--tx);">已檢查確認清單</span>
        <span style="font-size:11px;color:var(--txm);">解除後該欄位下次體檢會重新出現</span>
        <button onclick="document.getElementById('ac-audit-acks-modal').style.display='none'"
          style="margin-left:auto;background:none;border:none;color:var(--txm);font-size:20px;cursor:pointer;line-height:1;">✕</button>
      </div>
      <div id="ac-audit-acks-list" style="padding:14px 22px;flex:1;overflow-y:auto;display:flex;flex-direction:column;gap:6px;"></div>
    </div>
  </div>

  <!-- 🩺 資料體檢 Modal（管理員限定） -->
  <div id="ac-audit-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:700;align-items:center;justify-content:center;"
    onclick="if(event.target===this)document.getElementById('ac-audit-modal').style.display='none'">
    <div style="background:var(--bg-s);border:1px solid var(--bd);border-radius:16px;width:96%;max-width:760px;max-height:88vh;display:flex;flex-direction:column;box-shadow:var(--sh);">
      <div style="padding:18px 22px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:10px;">
        <span style="font-size:18px;">🩺</span>
        <span style="font-size:15px;font-weight:700;color:var(--tx);">物件硬資料體檢</span>
        <span id="ac-audit-subtitle" style="font-size:11px;color:var(--txm);">掃描中…</span>
        <button onclick="document.getElementById('ac-audit-modal').style.display='none'"
          style="margin-left:auto;background:none;border:none;color:var(--txm);font-size:20px;cursor:pointer;line-height:1;">✕</button>
      </div>
      <div style="padding:14px 22px;border-bottom:1px solid var(--bd);background:var(--bg-t);">
        <p style="margin:0 0 8px;font-size:12px;color:var(--txs);line-height:1.6;">缺<strong style="color:var(--tx);">鄉/市/鎮、段別、地號（土地）或建號（建物）</strong>的物件，ACCESS 比對時無法用「硬資料指紋」精準配對，會 fallback 到案名+地址（精準度低，容易誤配）。</p>
        <p style="margin:0 0 6px;font-size:11px;color:var(--txm);">處理方式三選一（依你方便）：</p>
        <ul style="margin:0 0 6px;padding-left:18px;font-size:11px;color:var(--txs);line-height:1.7;">
          <li><strong style="color:var(--ok);">直接按欄位旁 ✓</strong>（最快，記在 Firestore，下次體檢自動隱藏）</li>
          <li>到主頁 Sheets 補真實的硬資料（例如查謄本後填上正確建號）</li>
          <li>到主頁 Sheets 該欄填「<code style="background:var(--bg-p);padding:1px 4px;border-radius:3px;">無</code>」、「<code style="background:var(--bg-p);padding:1px 4px;border-radius:3px;">預售</code>」、「<code style="background:var(--bg-p);padding:1px 4px;border-radius:3px;">N/A</code>」、「<code style="background:var(--bg-p);padding:1px 4px;border-radius:3px;">未保存</code>」（適合預售屋、老屋未保存登記）</li>
        </ul>
        <p style="margin:0;font-size:11px;color:var(--ac);">💡 已檢查的清單可從頂端「管理」連結維護或解除。</p>
      </div>
      <div style="padding:12px 22px;flex:1;overflow-y:auto;">
        <div id="ac-audit-stats" style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px;font-size:11px;"></div>
        <div id="ac-audit-list" style="display:flex;flex-direction:column;gap:6px;"></div>
      </div>
      <div style="padding:12px 22px;border-top:1px solid var(--bd);background:var(--bg-t);display:flex;align-items:center;gap:10px;">
        <span id="ac-audit-summary" style="font-size:11px;color:var(--txm);"></span>
        <button onclick="document.getElementById('ac-audit-modal').style.display='none'"
          style="margin-left:auto;padding:7px 18px;border-radius:8px;background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);font-size:12px;cursor:pointer;">關閉</button>
      </div>
    </div>
  </div>

  <!-- 🧹 重複清理 Modal（管理員限定） -->
  <div id="ac-dup-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:705;align-items:center;justify-content:center;"
    onclick="if(event.target===this)document.getElementById('ac-dup-modal').style.display='none'">
    <div style="background:var(--bg-s);border:1px solid var(--bd);border-radius:16px;width:96%;max-width:880px;max-height:88vh;display:flex;flex-direction:column;box-shadow:var(--sh);">
      <div style="padding:18px 22px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:10px;">
        <span style="font-size:18px;">🧹</span>
        <span style="font-size:15px;font-weight:700;color:var(--tx);">主頁重複物件清理</span>
        <span id="ac-dup-subtitle" style="font-size:11px;color:var(--txm);">掃描中…</span>
        <button onclick="document.getElementById('ac-dup-modal').style.display='none'"
          style="margin-left:auto;background:none;border:none;color:var(--txm);font-size:20px;cursor:pointer;line-height:1;">✕</button>
      </div>
      <!-- Tab 切換 -->
      <div style="display:flex;border-bottom:1px solid var(--bd);background:var(--bg-t);">
        <button id="ac-dup-tab-exact-btn" onclick="dupSwitchTab('exact')"
          style="flex:1;padding:12px 16px;font-size:13px;font-weight:700;border:none;background:none;border-bottom:3px solid var(--ac);color:var(--ac);cursor:pointer;">
          🔴 完全重複 <span id="ac-dup-cnt-exact" style="background:var(--bg-h);color:var(--txs);border-radius:9px;padding:1px 7px;margin-left:4px;font-size:11px;">0</span>
        </button>
        <button id="ac-dup-tab-history-btn" onclick="dupSwitchTab('history')"
          style="flex:1;padding:12px 16px;font-size:13px;font-weight:700;border:none;background:none;border-bottom:3px solid transparent;color:var(--txm);cursor:pointer;">
          🕓 歷史版本 <span id="ac-dup-cnt-history" style="background:var(--bg-h);color:var(--txs);border-radius:9px;padding:1px 7px;margin-left:4px;font-size:11px;">0</span>
        </button>
      </div>
      <!-- 提示文字 -->
      <div id="ac-dup-tab-hint" style="padding:10px 22px;background:rgba(239,68,68,0.06);border-bottom:1px solid var(--bd);font-size:11px;color:var(--txs);line-height:1.6;">
        <strong style="color:#dc2626;">完全重複</strong>：同物件被貼了兩次（同硬資料 + 同委託編號 + 同委託日）。<strong>一定要刪一份</strong>，不會有業務影響。
      </div>
      <div style="flex:1;overflow-y:auto;padding:14px 22px;" id="ac-dup-list-wrap">
        <p style="margin:0;font-size:12px;color:var(--txs);">⏳ 讀取中…</p>
      </div>
      <div style="padding:12px 22px;border-top:1px solid var(--bd);background:var(--bg-t);">
        <span id="ac-dup-summary" style="font-size:11px;color:var(--txm);"></span>
        <button onclick="document.getElementById('ac-dup-modal').style.display='none'"
          style="margin-left:8px;float:right;padding:7px 18px;border-radius:8px;background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);font-size:12px;cursor:pointer;">關閉</button>
      </div>
    </div>
  </div>

  <!-- 結果資訊列 -->
  <div id="cp-info" class="mb-3 hidden" style="font-size:0.875rem;color:var(--txs);">
    共 <span id="cp-total" class="font-bold" style="color:var(--tx);">0</span> 筆，第
    <span id="cp-page-num" class="font-bold" style="color:var(--tx);">1</span> /
    <span id="cp-total-pages" class="font-bold" style="color:var(--tx);">1</span> 頁
  </div>

  <!-- 結果列表 -->
  <div id="cp-list" class="space-y-2"></div>

  <!-- 分頁控制 -->
  <div id="cp-pagination" class="flex gap-2 justify-center mt-4 hidden">
    <button id="cp-prev" onclick="cpChangePage(-1)"
      class="transition disabled:opacity-40" style="padding:0.5rem 1rem;border-radius:0.5rem;background:var(--bg-h);color:var(--txs);font-size:0.875rem;border:1px solid var(--bd);cursor:pointer;">← 上一頁</button>
    <button id="cp-next" onclick="cpChangePage(1)"
      class="transition disabled:opacity-40" style="padding:0.5rem 1rem;border-radius:0.5rem;background:var(--bg-h);color:var(--txs);font-size:0.875rem;border:1px solid var(--bd);cursor:pointer;">下一頁 →</button>
  </div>

  <!-- 初始提示 -->
  <div id="cp-placeholder" class="text-center py-16" style="color:var(--txm);">
    <div class="text-5xl mb-3">🏢</div>
    <p class="text-lg font-medium" style="color:var(--txs);">公司物件庫</p>
    <p class="text-sm mt-1">輸入條件後按「搜尋」，或直接按搜尋顯示全部物件</p>
  </div>
</div>

<!-- ══ 設定分頁（僅管理員）══ -->
<!-- ══ 準賣方管理分頁 ══ -->
<div id="pane-sellers" class="mx-auto px-4 py-6" style="max-width:896px;transition:max-width 0.3s;">

  <div class="flex items-center justify-between mb-4">
    <h2 class="font-bold text-lg" style="color:var(--tx);">🏠 準賣方列表</h2>
    <button onclick="slOpenCreate()" class="btn-primary">＋ 新增準賣方</button>
  </div>

  <!-- 搜尋 + 篩選 -->
  <div class="flex gap-2 mb-2 flex-wrap">
    <input id="sl-keyword" type="text" placeholder="搜尋姓名、地址、地號…" oninput="slFilterRender()" class="flex-1 min-w-40">
    <select id="sl-status-filter" onchange="slFilterRender()" style="width:auto">
      <option value="">全部狀態</option>
      <option value="培養中">培養中</option>
      <option value="已報價">已報價</option>
      <option value="已簽委託">已簽委託</option>
      <option value="放棄">放棄</option>
    </select>
  </div>

  <!-- 欄數切換 -->
  <div class="flex items-center gap-2 mb-2 flex-wrap">
    <span class="text-xs" style="color:var(--txs);">欄數：</span>
    <div class="flex gap-1">
      <button class="sl-col-btn" data-col="1" onclick="slSetColumns(1)">1</button>
      <button class="sl-col-btn active" data-col="2" onclick="slSetColumns(2)">2</button>
      <button class="sl-col-btn" data-col="3" onclick="slSetColumns(3)">3</button>
      <button class="sl-col-btn" data-col="4" onclick="slSetColumns(4)">4</button>
      <button class="sl-col-btn" data-col="5" onclick="slSetColumns(5)">5</button>
    </div>
  </div>

  <!-- 準賣方列表 grid -->
  <div id="sl-list" style="display:grid;grid-template-columns:repeat(2,1fr);gap:12px;">
    <p class="text-center py-12" style="color:var(--txs);grid-column:1/-1;">載入中…</p>
  </div>
</div>

<!-- ══ 準賣方 新增/編輯 Modal ══ -->
<div id="sl-modal" class="modal-bg" style="display:none;" onclick="if(event.target===this)slModalClose()">
  <div class="modal-box" style="max-width:540px;">
    <div class="flex items-center justify-between px-5 pt-5 pb-3" style="border-bottom:1px solid var(--bd);">
      <h3 id="sl-modal-title" class="font-bold text-base" style="color:var(--tx);">新增準賣方</h3>
      <button onclick="slModalClose()" class="text-xl leading-none" style="color:var(--txs);">✕</button>
    </div>
    <div class="px-5 py-4">
      <!-- 頭像上傳（編輯模式才顯示） -->
      <div id="sl-avatar-section" style="display:none;" class="flex items-center gap-4 mb-4">
        <div id="sl-avatar-wrap" style="position:relative;width:64px;height:64px;flex-shrink:0;">
          <img id="sl-avatar-img" src="" alt=""
            style="width:64px;height:64px;border-radius:50%;object-fit:cover;background:var(--bg-h);border:2px solid var(--bd);display:none;">
          <div id="sl-avatar-placeholder" style="width:64px;height:64px;border-radius:50%;background:var(--ac);display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:700;color:#fff;"></div>
          <!-- 點擊觸發上傳 -->
          <label style="position:absolute;inset:0;border-radius:50%;cursor:pointer;display:flex;align-items:flex-end;justify-content:center;padding-bottom:4px;background:transparent;" title="點擊更換頭像">
            <span style="font-size:10px;background:rgba(0,0,0,.5);color:#fff;border-radius:4px;padding:1px 4px;">換圖</span>
            <input type="file" accept="image/*" style="display:none;" onchange="slAvatarUpload(this)">
          </label>
        </div>
        <div>
          <p class="text-xs" style="color:var(--txs);">點擊頭像可更換照片</p>
          <p class="text-xs" style="color:var(--txm);">支援 jpg / png / webp</p>
        </div>
      </div>

      <!-- 基本資料 -->
      <div class="grid grid-cols-2 gap-3 mb-3">
        <div>
          <label class="text-xs block mb-1 font-medium" style="color:var(--txs);">屋主姓名 <span style="color:var(--err);">*</span></label>
          <input id="sl-f-name" type="text" placeholder="如：王大明"
            class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        </div>
        <div>
          <label class="text-xs block mb-1 font-medium" style="color:var(--txs);">聯絡電話</label>
          <input id="sl-f-phone" type="tel" placeholder="09xx-xxxxxx"
            class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        </div>
      </div>
      <div class="grid grid-cols-2 gap-3 mb-3">
        <div>
          <label class="text-xs block mb-1 font-medium" style="color:var(--txs);">物件地址</label>
          <input id="sl-f-address" type="text" placeholder="如：台東市中山路1號"
            class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        </div>
        <div>
          <label class="text-xs block mb-1 font-medium" style="color:var(--txs);">地號</label>
          <input id="sl-f-land" type="text" placeholder="如：知本段123地號"
            class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        </div>
      </div>
      <div class="grid grid-cols-2 gap-3 mb-3">
        <div>
          <label class="text-xs block mb-1 font-medium" style="color:var(--txs);">物件類別</label>
          <select id="sl-f-category"
            class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
            <option value="">— 選擇類別 —</option>
            <option value="農地">農地</option>
            <option value="建地">建地</option>
            <option value="公寓">公寓</option>
            <option value="房屋">房屋</option>
            <option value="別墅">別墅</option>
            <option value="店住">店住</option>
          </select>
        </div>
        <div>
          <label class="text-xs block mb-1 font-medium" style="color:var(--txs);">案源來源</label>
          <select id="sl-f-source"
            class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
            <option value="">— 選擇來源 —</option>
            <option value="自行開發">自行開發</option>
            <option value="介紹">介紹</option>
            <option value="廣告">廣告</option>
            <option value="其他">其他</option>
          </select>
        </div>
      </div>
      <div class="grid grid-cols-2 gap-3 mb-3">
        <div>
          <label class="text-xs block mb-1 font-medium" style="color:var(--txs);">屋主期望售價（萬）</label>
          <input id="sl-f-owner-price" type="number" placeholder="如：800"
            class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        </div>
        <div>
          <label class="text-xs block mb-1 font-medium" style="color:var(--txs);">房仲建議售價（萬）</label>
          <input id="sl-f-suggest-price" type="number" placeholder="如：750"
            class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        </div>
      </div>
      <div class="mb-3">
        <label class="text-xs block mb-1 font-medium" style="color:var(--txs);">狀態</label>
        <select id="sl-f-status"
          class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
          <option value="培養中">培養中</option>
          <option value="已報價">已報價</option>
          <option value="已簽委託">已簽委託</option>
          <option value="放棄">放棄</option>
        </select>
      </div>
      <div class="mb-4">
        <label class="text-xs block mb-1 font-medium" style="color:var(--txs);">備註</label>
        <textarea id="sl-f-note" rows="3" placeholder="其他補充說明…"
          class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none resize-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);"></textarea>
      </div>

      <!-- 卡片顏色 -->
      <div class="mb-4">
        <label class="text-xs block mb-1 font-medium" style="color:var(--txs);">卡片顏色</label>
        <div id="sl-color-picker" class="flex gap-2 flex-wrap mt-1">
          <button type="button" class="color-dot selected" data-color="" title="預設" onclick="slPickColor('')" style="background:var(--bg-s);border:2px solid var(--bd);"></button>
          <button type="button" class="color-dot" data-color="#ffd6d6" title="淡玫瑰" onclick="slPickColor('#ffd6d6')" style="background:#ffd6d6;"></button>
          <button type="button" class="color-dot" data-color="#ffdfc8" title="淡桃" onclick="slPickColor('#ffdfc8')" style="background:#ffdfc8;"></button>
          <button type="button" class="color-dot" data-color="#fff3c4" title="淡黃" onclick="slPickColor('#fff3c4')" style="background:#fff3c4;"></button>
          <button type="button" class="color-dot" data-color="#d6f5d6" title="淡綠" onclick="slPickColor('#d6f5d6')" style="background:#d6f5d6;"></button>
          <button type="button" class="color-dot" data-color="#c8f0ec" title="淡薄荷" onclick="slPickColor('#c8f0ec')" style="background:#c8f0ec;"></button>
          <button type="button" class="color-dot" data-color="#c8e8f8" title="淡水藍" onclick="slPickColor('#c8e8f8')" style="background:#c8e8f8;"></button>
          <button type="button" class="color-dot" data-color="#d4d8f8" title="淡藍紫" onclick="slPickColor('#d4d8f8')" style="background:#d4d8f8;"></button>
          <button type="button" class="color-dot" data-color="#ead5f8" title="淡紫" onclick="slPickColor('#ead5f8')" style="background:#ead5f8;"></button>
          <button type="button" class="color-dot" data-color="#f8d5ec" title="淡粉紫" onclick="slPickColor('#f8d5ec')" style="background:#f8d5ec;"></button>
          <button type="button" class="color-dot" data-color="#ede0d4" title="奶茶" onclick="slPickColor('#ede0d4')" style="background:#ede0d4;"></button>
          <button type="button" class="color-dot" data-color="#e8e8e8" title="淡灰" onclick="slPickColor('#e8e8e8')" style="background:#e8e8e8;"></button>
        </div>
        <input type="hidden" id="sl-f-color" value="">
      </div>

      <!-- 互動記事區（編輯模式才顯示） -->
      <!-- 相關圖檔（編輯模式才顯示） -->
      <div id="sl-files-section" style="display:none;">
        <div style="border-top:1px solid var(--bd);margin-bottom:12px;padding-top:16px;">
          <div class="flex items-center justify-between mb-3">
            <span class="text-sm font-semibold" style="color:var(--tx);">🖼️ 相關圖檔</span>
            <label id="sl-files-upload-btn" class="px-3 py-1 rounded-lg text-xs font-semibold cursor-pointer text-white" style="background:var(--ac);">
              ＋ 上傳
              <input type="file" accept="image/*,.pdf" multiple style="display:none;" onchange="slFilesUpload(this)">
            </label>
          </div>
          <div id="sl-files-list" class="flex flex-wrap gap-2">
            <p class="text-xs" style="color:var(--txm);">尚無圖檔</p>
          </div>
        </div>
      </div>

      <div id="sl-contacts-section" style="display:none;">
        <div style="border-top:1px solid var(--bd);margin-bottom:12px;padding-top:16px;">
          <div class="flex items-center justify-between mb-3">
            <span class="text-sm font-semibold" style="color:var(--tx);">📞 互動記事</span>
          </div>
          <!-- 新增記事輸入框 -->
          <div class="rounded-xl p-3 mb-3" style="background:var(--bg-h);border:1px solid var(--bd);">
            <textarea id="sl-contact-input" rows="2" placeholder="記錄本次聯繫內容…"
              class="w-full rounded-lg px-2 py-1.5 text-sm focus:outline-none resize-none mb-2" style="background:var(--bg-t);border:1px solid var(--bd);color:var(--tx);"></textarea>
            <div class="flex items-center gap-2">
              <input id="sl-contact-date" type="datetime-local"
                class="rounded-lg px-2 py-1 text-xs focus:outline-none" style="background:var(--bg-t);border:1px solid var(--bd);color:var(--tx);">
              <button onclick="slContactAdd()" class="px-3 py-1 rounded-lg text-xs font-semibold text-white" style="background:var(--ac);">新增記事</button>
            </div>
          </div>
          <!-- 記事列表 -->
          <div id="sl-contact-list">
            <p class="text-xs text-center py-2" style="color:var(--txm);">載入中…</p>
          </div>
        </div>
      </div>

      <!-- 操作按鈕 -->
      <div class="flex gap-2 justify-end">
        <button id="sl-btn-delete" onclick="slDelete()" class="hidden px-4 py-2 rounded-lg text-sm font-semibold transition"
          style="background:var(--err,#ef4444);color:#fff;">刪除</button>
        <button onclick="slModalClose()" class="px-4 py-2 rounded-lg text-sm font-semibold transition"
          style="background:var(--bg-h);color:var(--tx);border:1px solid var(--bd);">取消</button>
        <button id="sl-btn-save" onclick="slSave()" class="px-4 py-2 rounded-lg text-sm font-semibold text-white transition"
          style="background:var(--ac);">儲存</button>
      </div>
    </div>
  </div>
</div>

<!-- ── 地圖分頁（flex 直向排列）── -->
<div id="pane-map" style="display:none;height:calc(100vh - 90px);flex-direction:column;">
  <!-- 篩選列（position:relative + z-index 確保下拉蓋住 Leaflet） -->
  <div id="map-filter-bar" style="padding:6px 12px;background:var(--bg-s);border-bottom:1px solid var(--bd);display:flex;flex-wrap:wrap;align-items:center;gap:6px;flex-shrink:0;position:relative;z-index:500;">
    <!-- 類別複選 -->
    <div class="relative">
      <button id="map-cat-btn" onclick="mapToggleDropdown('cat')"
        class="flex items-center gap-1 rounded-lg px-3 py-1.5 text-sm transition" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        <span id="map-cat-label">全部類別</span>
        <svg class="w-3 h-3" style="color:var(--txs);" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
      </button>
      <div id="map-cat-panel" class="hidden absolute left-0 top-full mt-1 rounded-xl p-3 min-w-[160px] max-h-60 overflow-y-auto" style="background:var(--bg-s);border:1px solid var(--bd);box-shadow:var(--sh);z-index:1000;">
        <div id="map-cat-list" class="space-y-1"></div>
      </div>
    </div>
    <!-- 地區複選 -->
    <div class="relative">
      <button id="map-area-btn" onclick="mapToggleDropdown('area')"
        class="flex items-center gap-1 rounded-lg px-3 py-1.5 text-sm transition" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        <span id="map-area-label">全部地區</span>
        <svg class="w-3 h-3" style="color:var(--txs);" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
      </button>
      <div id="map-area-panel" class="hidden absolute left-0 top-full mt-1 rounded-xl p-3 min-w-[180px] max-h-60 overflow-y-auto" style="background:var(--bg-s);border:1px solid var(--bd);box-shadow:var(--sh);z-index:1000;">
        <div id="map-area-list" class="space-y-1"></div>
      </div>
    </div>
    <!-- 經紀人複選 -->
    <div class="relative">
      <button id="map-agent-btn" onclick="mapToggleDropdown('agent')"
        class="flex items-center gap-1 rounded-lg px-3 py-1.5 text-sm transition" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        <span id="map-agent-label">全部經紀人</span>
        <svg class="w-3 h-3" style="color:var(--txs);" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
      </button>
      <div id="map-agent-panel" class="hidden absolute left-0 top-full mt-1 rounded-xl p-3 min-w-[160px] max-h-60 overflow-y-auto" style="background:var(--bg-s);border:1px solid var(--bd);box-shadow:var(--sh);z-index:1000;">
        <p class="text-xs mb-2" style="color:var(--txm);">── 在線人員 ──</p>
        <div id="map-agent-active-list" class="space-y-1 mb-2"></div>
        <p class="text-xs mb-2" style="color:var(--txm);">── 其他 ──</p>
        <div id="map-agent-inactive-list" class="space-y-1"></div>
      </div>
    </div>
    <!-- 套用 / 重設 -->
    <button onclick="mapApplyFilter()"
      class="px-4 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition">套用</button>
    <button onclick="mapResetFilter()"
      class="px-3 py-1.5 rounded-lg text-sm transition" style="background:var(--bg-h);color:var(--txs);">重設</button>
    <!-- 情境書籤（靠右） -->
    <div class="flex items-center gap-1 ml-auto">
      <span class="text-xs" style="color:var(--txs);">情境：</span>
      <select id="map-preset-select"
        onchange="mapApplyPreset()"
        class="text-xs rounded-lg px-2 py-1.5 focus:outline-none max-w-[130px]" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);"
        title="選擇已儲存的地圖篩選情境">
        <option value="">— 選擇情境 —</option>
      </select>
      <button id="map-preset-delete-btn" onclick="mapDeletePreset()" title="刪除此情境"
        class="hidden text-red-400 hover:text-red-300 text-base leading-none px-1">×</button>
      <button onclick="mapSavePreset()" title="將目前篩選另存為情境"
        class="px-2 py-1.5 rounded-lg bg-purple-700 hover:bg-purple-600 text-purple-100 text-xs transition flex items-center gap-1">
        💾 儲存情境
      </button>
    </div>
  </div>
  <!-- 統計列 + 圖釘切換 -->
  <div id="map-stat-bar" style="padding:4px 12px;font-size:0.8rem;color:var(--txs);background:var(--bg-s);border-bottom:1px solid var(--bd);display:flex;align-items:center;justify-content:space-between;height:32px;flex-shrink:0;">
    <span id="map-stat-text">載入中...</span>
    <button id="map-pin-toggle" onclick="mapTogglePinMode()"
      title="切換圖釘樣式"
      style="font-size:11px;font-weight:600;padding:3px 10px;border-radius:6px;border:1px solid var(--bd);background:var(--bg-h);color:var(--txs);cursor:pointer;flex-shrink:0;">
      🔵 圓點模式
    </button>
  </div>
  <!-- Leaflet 地圖容器（flex:1 填滿剩餘空間） -->
  <div id="map-container" style="flex:1;width:100%;min-height:0;"></div>
</div>

<!-- ══ 資料庫檢視分頁（管理員限定）══ -->
<div id="pane-dbview" style="display:none;max-width:95vw;" class="mx-auto px-4 py-6">
  <div class="flex items-center justify-between mb-4 flex-wrap gap-3">
    <h2 class="font-bold text-lg" style="color:var(--tx);">📊 Firestore 資料庫檢視</h2>
    <div class="flex items-center gap-2 flex-wrap">
      <!-- 集合選擇 -->
      <select id="dbv-collection"
        onchange="dbvLoadCollection()"
        class="rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        <option value="">選擇集合…</option>
      </select>
      <!-- 搜尋 -->
      <input id="dbv-keyword" type="text" placeholder="搜尋關鍵字…"
        class="rounded-lg px-3 py-2 text-sm focus:outline-none w-48" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);"
        onkeydown="if(event.key==='Enter')dbvLoadCollection()">
      <button onclick="dbvLoadCollection()"
        class="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition">搜尋</button>
      <!-- 每頁筆數 -->
      <select id="dbv-perpage"
        onchange="dbvLoadCollection()"
        class="rounded-lg px-2 py-2 text-sm focus:outline-none" style="background:var(--bg-h);border:1px solid var(--bd);color:var(--tx);">
        <option value="20">20 筆</option>
        <option value="50" selected>50 筆</option>
        <option value="100">100 筆</option>
        <option value="200">200 筆</option>
      </select>
    </div>
  </div>
  <!-- 統計資訊 -->
  <div id="dbv-info" class="text-sm mb-3" style="color:var(--txs);"></div>
  <!-- 表格容器（可水平捲動） -->
  <div class="rounded-xl overflow-hidden" style="border:1px solid var(--bd);">
    <div style="overflow-x:auto;">
      <table id="dbv-table" class="w-full text-sm" style="min-width:600px;">
        <thead id="dbv-thead"></thead>
        <tbody id="dbv-tbody"></tbody>
      </table>
    </div>
  </div>
  <!-- 分頁控制 -->
  <div id="dbv-pager" class="flex items-center justify-center gap-3 mt-4"></div>
</div>

<!-- ══ 組織設定分頁（屬於組織的人才看得到）══ -->
<div id="pane-org" style="display:none" class="max-w-2xl mx-auto px-4 py-6">
  <h2 class="font-bold text-lg mb-1" style="color:var(--tx);">🏢 組織設定</h2>
  <p id="org-panel-desc" class="text-sm mb-5" style="color:var(--txs);">載入中…</p>

  <!-- 成員列表 -->
  <div class="rounded-2xl p-5 mb-6" style="background:var(--bg-t);border:1px solid var(--bd);">
    <div class="flex items-center justify-between mb-4">
      <div>
        <h3 class="font-semibold" style="color:var(--tx);">👥 組織成員</h3>
        <p class="text-xs mt-0.5" style="color:var(--txs);">管理員可邀請成員並設定其操作權限</p>
      </div>
      <!-- 邀請按鈕只有 org admin 才看得到 -->
      <button id="btn-org-invite" onclick="orgInviteOpen()" class="hidden px-3 py-1.5 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-xs font-semibold transition">
        ＋ 邀請成員
      </button>
    </div>

    <!-- 邀請表單（預設隱藏） -->
    <div id="org-invite-form" class="hidden rounded-xl p-4 mb-4" style="background:var(--bg-h);border:1px solid var(--bd);">
      <div class="grid grid-cols-2 gap-3 mb-3">
        <div>
          <label class="text-xs block mb-1" style="color:var(--txs);">成員 Email</label>
          <input id="org-invite-email" type="email" placeholder="如：colleague@gmail.com"
            class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-t);border:1px solid var(--bd);color:var(--tx);">
        </div>
        <div>
          <label class="text-xs block mb-1" style="color:var(--txs);">角色</label>
          <select id="org-invite-role"
            class="w-full rounded-lg px-3 py-2 text-sm focus:outline-none" style="background:var(--bg-t);border:1px solid var(--bd);color:var(--tx);">
            <option value="editor">✏️ 編輯者（可新增/編輯）</option>
            <option value="viewer">👀 觀察者（只能查看）</option>
            <option value="admin">🛡️ 管理員（全部操作）</option>
          </select>
        </div>
      </div>
      <div class="flex gap-2">
        <button onclick="orgInviteSave()"
          class="px-4 py-1.5 rounded-lg bg-green-600 hover:bg-green-500 text-white text-xs font-semibold transition">邀請</button>
        <button onclick="orgInviteClose()"
          class="px-4 py-1.5 rounded-lg text-xs transition" style="background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);">取消</button>
      </div>
    </div>

    <!-- 成員列表 -->
    <div id="org-member-list" class="space-y-2">
      <p class="text-sm text-center py-4" style="color:var(--txm);">載入中…</p>
    </div>
  </div>

  <!-- 個人庫轉移到組織庫 -->
  <div id="org-transfer-section" class="hidden rounded-2xl p-5 mb-6" style="background:var(--bg-t);border:1px solid var(--bd);">
    <h3 class="font-semibold mb-1" style="color:var(--tx);">📦 轉移個人物件到組織庫</h3>
    <p class="text-xs mb-3" style="color:var(--txs);">把你個人庫的物件複製一份到組織共用庫。原始個人庫資料不受影響。</p>
    <button onclick="orgTransferObjects()"
      class="px-4 py-2 rounded-lg text-white text-sm font-semibold transition" style="background:#d97706;">
      📦 開始複製到組織庫
    </button>
    <p id="org-transfer-result" class="text-xs mt-2" style="color:var(--txs);"></p>
  </div>
</div>

<!-- 公司物件詳情 Modal -->
<!-- z-[700]：比 cp-review-modal (z-index:600) 高，避免在審查 modal 內點「序號 #N」開不出來 -->
<div id="cp-detail-modal" role="dialog" aria-modal="true"
  class="hidden fixed inset-0 z-[700] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm"
  onclick="if(event.target===this)closeCpDetail()">
  <div class="w-full max-w-2xl rounded-2xl flex flex-col max-h-[90vh]" style="background:var(--bg-s);border:1px solid var(--bd);box-shadow:var(--sh);"
    onclick="event.stopPropagation()">
    <div class="flex items-center justify-between px-6 py-4 shrink-0" style="border-bottom:1px solid var(--bd);">
      <h3 id="cp-detail-title" class="font-bold text-lg" style="color:var(--tx);">物件詳情</h3>
      <button onclick="closeCpDetail()" class="text-xl leading-none" style="background:none;border:none;color:var(--txs);cursor:pointer;">✕</button>
    </div>
    <div id="cp-detail-body" class="overflow-y-auto px-6 py-5 space-y-1 text-sm"></div>
    <div class="px-6 py-4 shrink-0" style="border-top:1px solid var(--bd);">
      <button onclick="closeCpDetail()" class="px-4 py-2 rounded-lg text-sm transition" style="background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);cursor:pointer;">關閉</button>
    </div>
  </div>
</div>

<!-- yes319 全自動同步結果 Modal（4 個區塊：文案 / 照片 / 下架預覽 / 缺漏預覽） -->
<div id="cp-yes319-result-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:650;align-items:flex-start;justify-content:center;padding-top:32px;"
  onclick="if(event.target===this)closeYes319ResultModal()">
  <div style="background:var(--bg-s);border:1px solid var(--bd);border-radius:16px;width:96%;max-width:760px;max-height:88vh;display:flex;flex-direction:column;box-shadow:var(--sh);">
    <div style="padding:16px 24px 12px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:10px;">
      <span style="font-size:15px;font-weight:700;color:var(--tx);">✅ yes319 同步完成</span>
      <span id="cp-yes319-result-subtitle" style="font-size:12px;color:var(--txs);"></span>
      <button onclick="closeYes319ResultModal()"
        style="margin-left:auto;background:none;border:none;color:var(--txm);font-size:20px;cursor:pointer;line-height:1;">✕</button>
    </div>
    <div id="cp-yes319-result-body" style="flex:1;overflow-y:auto;padding:16px 24px;"></div>
    <div style="padding:12px 24px;border-top:1px solid var(--bd);display:flex;justify-content:flex-end;gap:8px;">
      <button onclick="closeYes319ResultModal()"
        style="padding:7px 18px;border-radius:8px;background:var(--ac);color:#fff;border:none;font-size:13px;font-weight:700;cursor:pointer;">
        關閉
      </button>
    </div>
  </div>
</div>

<!-- 同步 Sheets 完成後的結果明細 Modal -->
<div id="cp-sync-result-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:650;align-items:flex-start;justify-content:center;padding-top:32px;"
  onclick="if(event.target===this)closeSyncResultModal()">
  <div style="background:var(--bg-s);border:1px solid var(--bd);border-radius:16px;width:96%;max-width:720px;max-height:88vh;display:flex;flex-direction:column;box-shadow:var(--sh);">
    <div style="padding:16px 24px 12px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:10px;">
      <span style="font-size:15px;font-weight:700;color:var(--tx);">✅ 同步完成</span>
      <span id="cp-sync-result-subtitle" style="font-size:12px;color:var(--txs);"></span>
      <button onclick="closeSyncResultModal()"
        style="margin-left:auto;background:none;border:none;color:var(--txm);font-size:20px;cursor:pointer;line-height:1;">✕</button>
    </div>
    <div id="cp-sync-result-body" style="flex:1;overflow-y:auto;padding:16px 24px;"></div>
    <div style="padding:12px 24px;border-top:1px solid var(--bd);display:flex;justify-content:flex-end;gap:8px;">
      <button onclick="closeSyncResultModal()"
        style="padding:7px 18px;border-radius:8px;background:var(--ac);color:#fff;border:none;font-size:13px;font-weight:700;cursor:pointer;">
        關閉
      </button>
    </div>
  </div>
</div>

<!-- new-prop-modal 已移除（我的物件功能移至廣告文案工具的「文案收藏」） -->

<script>
  const fields = __FIELDS_JSON__;
  const isAdmin   = __IS_ADMIN_JSON__;
  const BUYER_URL = __BUYER_URL__;

  // 管理員才顯示「資料庫檢視」tab（設定已改為 Modal，不顯示於分頁列）
  if (isAdmin) {
    var dbviewTab = document.getElementById('tab-dbview');
    if (dbviewTab) dbviewTab.classList.remove('hidden');
  }

  // ══ 組織（Org）功能 JS ══
  var _orgInfo = null;       // 目前使用者的 org 資訊（由 /api/me 填入）
  var _libMode = 'personal'; // 預設個人庫

  // 非同步查詢 org 資訊，不阻擋物件列表載入
  fetch('/api/me').then(function(r){ return r.json(); }).then(function(u) {
    if (u.error || !u.org) return;  // 沒有組織，什麼都不做
    _orgInfo = u.org;
    _libMode = 'org';
    // 顯示組織 tab
    var orgTab = document.getElementById('tab-org');
    if (orgTab) orgTab.classList.remove('hidden');
    // 顯示庫切換下拉
    var modeBar = document.getElementById('lib-mode-bar');
    if (modeBar) { modeBar.classList.remove('hidden'); modeBar.style.display = 'flex'; }
    // 更新 org 名稱和角色
    var orgName = document.getElementById('lib-mode-org-name');
    var roleBadge = document.getElementById('lib-mode-role-badge');
    if (orgName) orgName.textContent = u.org.name || '';
    var roleMap = { admin: '管理員', editor: '編輯者', viewer: '觀察者' };
    if (roleBadge) roleBadge.textContent = roleMap[u.org.role] || u.org.role;
    // 有組織就重新用 org 模式載入列表
    loadList();
  }).catch(function(){});

  // 切換個人庫 / 組織庫
  function libSwitchMode(mode) {
    _libMode = mode;
    loadObjects();
  }

  // 切換分頁（擴充原 switchTab 支援 org 分頁）
  var _origSwitchTab = typeof switchTab === 'function' ? switchTab : null;

  // ══ 組織成員管理 UI ══
  function orgLoadMembers() {
    var list = document.getElementById('org-member-list');
    if (!list) return;
    list.innerHTML = '<p class="text-sm text-center py-4" style="color:var(--txm);">載入中…</p>';
    fetch('/api/org/members')
      .then(function(r){ return r.json(); })
      .then(function(data) {
        if (data.error) {
          list.innerHTML = '<p style="color:var(--dg);font-size:0.875rem;">' + escapeHtml(data.error) + '</p>';
          return;
        }
        _orgInfo = data.org || _orgInfo;
        var members = data.members || [];
        // 更新面板說明
        var desc = document.getElementById('org-panel-desc');
        if (desc && data.org) {
          desc.textContent = '組織名稱：' + (data.org.name || '') + '　|　你的角色：' + ({ admin:'管理員', editor:'編輯者', viewer:'觀察者' }[data.org.role] || data.org.role);
        }
        // 顯示邀請按鈕（管理員才有）
        var inviteBtn = document.getElementById('btn-org-invite');
        if (inviteBtn && data.org && data.org.role === 'admin') inviteBtn.classList.remove('hidden');
        // 顯示轉移區塊（管理員才有）
        var transferSection = document.getElementById('org-transfer-section');
        if (transferSection && data.org && data.org.role === 'admin') transferSection.classList.remove('hidden');

        if (!members.length) {
          list.innerHTML = '<p class="text-sm text-center py-4" style="color:var(--txm);">尚無成員</p>';
          return;
        }
        var isOrgAdmin = data.org && data.org.role === 'admin';
        var html = '';
        var roleLabel = { admin: '🛡️ 管理員', editor: '✏️ 編輯者', viewer: '👀 觀察者' };
        members.forEach(function(m) {
          html += '<div class="flex items-center justify-between rounded-xl px-4 py-2.5" style="background:var(--bg-h);">';
          html += '<div>';
          html += '<span class="text-sm font-medium" style="color:var(--tx);">' + escapeHtml(m.email) + '</span>';
          html += '<span class="text-xs ml-3 px-2 py-0.5 rounded-full" style="background:var(--acs);color:var(--ac);">' + (roleLabel[m.role] || m.role) + '</span>';
          if (m.joined_at) html += '<span class="text-xs ml-2" style="color:var(--txm);">加入：' + escapeHtml(m.joined_at.slice(0,10)) + '</span>';
          html += '</div>';
          if (isOrgAdmin) {
            html += '<div class="flex gap-2">';
            // 角色下拉：用 data-email 傳遞，避免引號問題
            html += '<select data-email="' + escapeHtml(m.email) + '" onchange="orgChangeRole(this.dataset.email,this.value)" class="text-xs rounded px-2 py-1" style="background:var(--bg-t);border:1px solid var(--bd);color:var(--tx);">';
            ['admin','editor','viewer'].forEach(function(r) {
              html += '<option value="' + r + '"' + (m.role === r ? ' selected' : '') + '>' + (roleLabel[r]||r) + '</option>';
            });
            html += '</select>';
            html += '<button data-email="' + escapeHtml(m.email) + '" onclick="orgRemoveMember(this.dataset.email)" class="text-xs px-2 py-1 rounded transition" style="color:var(--dg);border:1px solid var(--bd);">移除</button>';
            html += '</div>';
          }
          html += '</div>';
        });
        list.innerHTML = html;
      })
      .catch(function(e) {
        if (list) list.innerHTML = '<p style="color:var(--dg);font-size:0.875rem;">載入失敗</p>';
      });
  }

  function orgInviteOpen() {
    var form = document.getElementById('org-invite-form');
    if (form) form.classList.remove('hidden');
    var emailEl = document.getElementById('org-invite-email');
    if (emailEl) emailEl.focus();
  }
  function orgInviteClose() {
    var form = document.getElementById('org-invite-form');
    if (form) form.classList.add('hidden');
    var emailEl = document.getElementById('org-invite-email');
    if (emailEl) emailEl.value = '';
  }
  function orgInviteSave() {
    var email = (document.getElementById('org-invite-email') || {}).value || '';
    var role  = (document.getElementById('org-invite-role') || {}).value || 'editor';
    if (!email || !email.includes('@')) { toast('請輸入有效的 Email', 'error'); return; }
    fetch('/api/org/members', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: email.trim().toLowerCase(), role: role })
    }).then(function(r){ return r.json(); }).then(function(d) {
      if (d.error) { toast('邀請失敗：' + d.error, 'error'); return; }
      toast('已邀請 ' + email, 'success');
      orgInviteClose();
      orgLoadMembers();
    }).catch(function(){ toast('邀請失敗，請重試', 'error'); });
  }
  function orgRemoveMember(targetEmail) {
    if (!confirm('確定要移除成員 ' + targetEmail + '？')) return;
    fetch('/api/org/members', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: targetEmail })
    }).then(function(r){ return r.json(); }).then(function(d) {
      if (d.error) { toast('移除失敗：' + d.error, 'error'); return; }
      toast('已移除 ' + targetEmail, 'success');
      orgLoadMembers();
    }).catch(function(){ toast('移除失敗，請重試', 'error'); });
  }
  function orgChangeRole(targetEmail, newRole) {
    fetch('/api/org/members/role', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email: targetEmail, role: newRole })
    }).then(function(r){ return r.json(); }).then(function(d) {
      if (d.error) { toast('更新角色失敗：' + d.error, 'error'); return; }
      toast('已更新角色', 'success');
    }).catch(function(){ toast('更新失敗，請重試', 'error'); });
  }
  function orgTransferObjects() {
    if (!confirm('確定要把你的個人物件庫複製到組織庫？（原資料不受影響）')) return;
    var resultEl = document.getElementById('org-transfer-result');
    if (resultEl) resultEl.textContent = '複製中…';
    fetch('/api/org/transfer-objects', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ confirm: true })
    }).then(function(r){ return r.json(); }).then(function(d) {
      if (d.error) { toast('複製失敗：' + d.error, 'error'); if (resultEl) resultEl.textContent = '失敗：' + d.error; return; }
      toast(d.message || '複製完成', 'success');
      if (resultEl) resultEl.textContent = d.message || '';
    }).catch(function(){ toast('複製失敗，請重試', 'error'); });
  }

  // ══ 經紀人 Email 管理 ══
  function agentEmailLoad() {
    var list = document.getElementById('agent-email-list');
    if (!list) return;
    list.innerHTML = '<p class="text-sm text-center py-4" style="color:var(--txm);">載入中…</p>';
    fetch('/api/agent-emails')
      .then(function(r){ return r.json(); })
      .then(function(data) {
        if (data.error) { list.innerHTML = '<p style="color:var(--dg);font-size:0.875rem;">' + escapeHtml(data.error) + '</p>'; return; }
        var items = data.items || [];
        if (!items.length) {
          list.innerHTML = '<p class="text-sm text-center py-4" style="color:var(--txm);">尚無設定，請點「＋ 新增」</p>';
          return;
        }
        var html = '';
        items.forEach(function(item) {
          html += '<div class="flex items-center justify-between rounded-xl px-4 py-2.5" style="background:var(--bg-h);">';
          html += '<div>';
          html += '<span class="text-sm font-medium" style="color:var(--tx);">' + escapeHtml(item.name) + '</span>';
          html += '<span class="text-xs ml-3" style="color:var(--txs);">' + (item.email ? escapeHtml(item.email) : '<em style="color:var(--txm);">未設定</em>') + '</span>';
          html += '</div>';
          html += '<div class="flex gap-2">';
          html += '<button class="ae-edit-btn text-xs px-2 py-1 rounded transition" style="color:var(--ac);border:1px solid var(--bd);" '
                + 'data-name="' + escapeHtml(item.name) + '" data-email="' + escapeHtml(item.email||'') + '">編輯</button>';
          html += '<button class="ae-del-btn text-xs px-2 py-1 rounded transition" style="color:var(--dg);border:1px solid var(--bd);" '
                + 'data-name="' + escapeHtml(item.name) + '">刪除</button>';
          html += '</div></div>';
        });
        list.innerHTML = html;
        // 事件委派
        list.querySelectorAll('.ae-edit-btn').forEach(function(btn) {
          btn.addEventListener('click', function() {
            document.getElementById('agent-email-name').value  = this.dataset.name;
            document.getElementById('agent-email-addr').value  = this.dataset.email;
            document.getElementById('agent-email-form').classList.remove('hidden');
          });
        });
        list.querySelectorAll('.ae-del-btn').forEach(function(btn) {
          btn.addEventListener('click', function() {
            var name = this.dataset.name;
            if (!confirm('確定刪除「' + name + '」的 Email 設定？')) return;
            fetch('/api/agent-emails/' + encodeURIComponent(name), {method: 'DELETE'})
              .then(function(r){ return r.json(); })
              .then(function(d) {
                if (d.ok) agentEmailLoad();
                else alert('刪除失敗：' + d.error);
              });
          });
        });
      })
      .catch(function(e) { list.innerHTML = '<p class="text-red-400 text-sm">載入失敗</p>'; });
  }

  function agentEmailOpenAdd() {
    document.getElementById('agent-email-name').value = '';
    document.getElementById('agent-email-addr').value = '';
    document.getElementById('agent-email-form').classList.remove('hidden');
    document.getElementById('agent-email-name').focus();
  }

  function agentEmailCloseForm() {
    document.getElementById('agent-email-form').classList.add('hidden');
  }

  function agentEmailSave() {
    var name  = document.getElementById('agent-email-name').value.trim();
    var email = document.getElementById('agent-email-addr').value.trim();
    if (!name) { alert('請填寫經紀人姓名'); return; }
    fetch('/api/agent-emails', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name: name, email: email})
    })
    .then(function(r){ return r.json(); })
    .then(function(d) {
      if (d.ok) { agentEmailCloseForm(); agentEmailLoad(); }
      else alert('儲存失敗：' + d.error);
    });
  }

  function rebuildPropIndex() {
    var btn = document.querySelector('[onclick="rebuildPropIndex()"]');
    var result = document.getElementById('prop-index-result');
    if (btn) { btn.disabled = true; btn.textContent = '⏳ 建立中...'; }
    if (result) result.textContent = '正在掃描全部物件，約需 30 秒…';
    fetch('/api/rebuild-prop-index', {method:'POST'})
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (btn) { btn.disabled = false; btn.textContent = '重建物件索引'; }
        if (result) result.textContent = d.ok ? ('✅ 完成，共 ' + d.count + ' 筆物件已建入索引') : ('❌ ' + d.error);
      })
      .catch(function(e) {
        if (btn) { btn.disabled = false; btn.textContent = '重建物件索引'; }
        if (result) result.textContent = '失敗：' + e;
      });
  }

  function triggerNotify() {
    var btn = document.querySelector('[onclick="triggerNotify()"]');
    var result = document.getElementById('notify-result');
    if (btn) { btn.disabled = true; btn.textContent = '⏳ 執行中...'; }
    if (result) result.textContent = '正在執行，請稍候...';
    fetch('/api/notify-expiry', {method: 'POST'})
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (btn) { btn.disabled = false; btn.textContent = '立即執行通知'; }
        if (result) result.textContent = d.message || d.error || JSON.stringify(d);
        if (d.errors && d.errors.length) result.textContent += '　警告：' + d.errors.join('、');
      })
      .catch(function(e) {
        if (btn) { btn.disabled = false; btn.textContent = '立即執行通知'; }
        if (result) result.textContent = '執行失敗：' + e;
      });
  }

  // ── Toast ──
  function toast(msg, type) {
    type = type || 'info';
    var c = document.getElementById('toast-container');
    var el = document.createElement('div');
    el.className = 'toast-item toast-' + type;
    el.textContent = msg;
    c.appendChild(el);
    setTimeout(function() { el.classList.add('toast-out'); setTimeout(function(){ el.remove(); }, 300); }, 2500);
  }

  function escapeHtml(s) { if (s == null) return ''; var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
  function targetUser() { return isAdmin && document.getElementById('userSelect') ? document.getElementById('userSelect').value : ''; }
  function apiUrl(path) {
    var params = [];
    var u = targetUser();
    if (u) params.push('user=' + encodeURIComponent(u));
    // 加入 mode 參數，支援組織庫 / 個人庫切換
    if (path === '/api/objects' && typeof _libMode !== 'undefined') {
      params.push('mode=' + encodeURIComponent(_libMode));
    }
    return path + (params.length ? '?' + params.join('&') : '');
  }

  // _currentOrgId（保留給組織庫使用，已移除我的物件相關函式）
  var _currentOrgId = null;

  // 我的物件相關函式已移至廣告文案工具（文案收藏）

  // ── URL 參數：自動切換到公司物件庫並定位到該物件 ──
  // 支援 ?prop_name=<案名>（直接搜尋，不需登入API）
  // 或    ?prop=<prop_id>（向後相容，需登入）
  (function() {
    var ps = new URLSearchParams(window.location.search);
    var propName = ps.get('prop_name');  // 買方管理傳來的案名
    var propId   = ps.get('prop');       // 舊版 prop_id 格式
    if (!propName && !propId) return;
    history.replaceState(null, '', window.location.pathname);  // 移除 URL 參數

    function _locateByName(name) {
      // 設旗標：告訴 cpLoadMe 不要預設設定 agent（避免時序競爭覆蓋搜尋結果）
      window._cpLocating = true;
      // 填入關鍵字（正確 ID 是 cp-keyword）
      var kwEl     = document.getElementById('cp-keyword');
      var statusEl = document.getElementById('cp-status');
      var expiryEl = document.getElementById('cp-expiry');
      var sortEl   = document.getElementById('cp-sort');
      if (kwEl)     kwEl.value     = name;
      if (statusEl) statusEl.value = '';   // 清除狀態篩選（顯示全部）
      if (expiryEl) expiryEl.value = '';   // 清除到期日篩選
      if (sortEl)   sortEl.value   = 'price_asc';
      // 清除多選篩選（類別、地區、經紀人）及其 checkbox DOM
      document.querySelectorAll('.cp-cat-cb,.cp-area-cb,.cp-agent-cb')
        .forEach(function(cb){ cb.checked = false; });
      _cpSelected = { cat: new Set(), area: new Set(), agent: new Set() };
      _cpUpdateLabel('agent'); _cpUpdateLabel('cat'); _cpUpdateLabel('area');
      window._cpSearched = false;
      // 呼叫 cpSearch 重建 _cpLastQuery 再 fetch
      cpSearch();
      toast('📍 已定位到「' + name + '」', 'info');
    }

    setTimeout(function() {
      switchTab('company');
      setTimeout(function() {
        if (propName) {
          // 直接用案名搜尋，不需 API 呼叫
          _locateByName(propName);
        } else {
          // 舊版：以 prop_id 查 API 再取得案名
          fetch('/api/company-properties/' + encodeURIComponent(propId))
            .then(function(r){ return r.json(); })
            .then(function(d) {
              if (d.error || !d['案名']) { toast('找不到物件 #' + propId, 'error'); return; }
              _locateByName(d['案名']);
            })
            .catch(function() { toast('定位物件失敗，請重試', 'error'); });
        }
      }, 500);
    }, 150);
  })();

  // 開啟設定 Modal（管理員限定）
  function openSettingsModal() {
    document.getElementById('settings-modal').style.display = 'flex';
    agentEmailLoad();  // 開啟時自動載入 Email 列表
  }

  // ══ 分頁切換 ══
  function switchTab(tab) {
    var paneCompanyEl  = document.getElementById('pane-company');
    var paneOrgEl      = document.getElementById('pane-org');
    var paneDbviewEl   = document.getElementById('pane-dbview');
    var paneSellersEl  = document.getElementById('pane-sellers');
    var paneMapEl      = document.getElementById('pane-map');

    // 全部隱藏（加 null check 防止任一元素不存在時崩潰）
    if (paneCompanyEl)  paneCompanyEl.style.display  = 'none';
    if (paneOrgEl)      paneOrgEl.style.display      = 'none';
    if (paneDbviewEl)   paneDbviewEl.style.display   = 'none';
    if (paneSellersEl)  paneSellersEl.style.display  = 'none';
    if (paneMapEl)      paneMapEl.style.display      = 'none';

    if (tab === 'company') {
      if (paneCompanyEl) paneCompanyEl.style.display = 'block';
    } else if (tab === 'buyers') {
      if (paneBuyersEl) paneBuyersEl.style.display = 'block';
    } else if (tab === 'war') {
      if (paneWarEl) paneWarEl.style.display = 'block';
    } else if (tab === 'settings') {
      // 設定已改為 Modal，直接開啟並停留在 company pane
      if (paneCompanyEl) paneCompanyEl.style.display = 'block';
      openSettingsModal();
      return;  // 不繼續更新 tab 按鈕樣式
    } else if (tab === 'org') {
      if (paneOrgEl) paneOrgEl.style.display = 'block';
      orgLoadMembers();  // 進入組織設定頁自動載入成員列表
    } else if (tab === 'dbview') {
      if (paneDbviewEl) paneDbviewEl.style.display = 'block';
      dbvInit();  // 進入資料庫檢視頁自動載入集合列表
    } else if (tab === 'sellers') {
      if (paneSellersEl) paneSellersEl.style.display = 'block';
      slLoad();  // 進入準賣方管理頁自動載入列表
    } else if (tab === 'map') {
      if (paneMapEl) paneMapEl.style.display = 'flex';  // flex 讓內部直向排列
      mapInit();  // 初始化地圖（第一次進入才建立，之後只重整資料）
      if (!window._mapOptionsLoaded) { window._mapOptionsLoaded = true; mapLoadOptions(); }
      if (!window._mapPresetsLoaded) { window._mapPresetsLoaded = true; mapLoadPresets(); }
    }

    // 分頁按鈕樣式
    document.querySelectorAll('.tab-btn').forEach(function(btn) {
      btn.style.color        = 'var(--txs)';
      btn.style.borderBottom = '2px solid transparent';
      btn.style.fontWeight   = '400';
    });
    var activeBtn = document.getElementById('tab-' + tab);
    if (activeBtn) {
      activeBtn.style.color        = 'var(--ac)';
      activeBtn.style.borderBottom = '2px solid var(--ac)';
      activeBtn.style.fontWeight   = '600';
    }

    // 切換到公司物件時：載入篩選選項 + 自動以登入者×銷售中搜尋 + 顯示管理員工具列
    if (tab === 'company') {
      if (!window._cpOptionsLoaded) { cpLoadOptions(); }
      if (!window._cpSearched) { window._cpSearched = true; cpLoadMe(); }
      if (!window._cpWordLoaded) { window._cpWordLoaded = true; cpLoadWordSnapshot(); }
      if (!window._cpPresetsLoaded) { window._cpPresetsLoaded = true; cpLoadPresets(); }
      if (isAdmin) {
        document.getElementById('cp-sync-bar').style.display = 'flex';
        cpLoadSyncStatus();
        cpLoadWordSnapshotStatus();
      }
    }
  }

  // ══ 資料庫檢視功能（管理員限定）══
  var _dbvInited = false;  // 只載入一次集合清單
  var _dbvPage = 1;        // 目前頁碼
  var _dbvSortCol = '';    // 排序欄位
  var _dbvSortAsc = true;  // 升冪

  // 初始化：載入集合清單
  function dbvInit() {
    if (_dbvInited) return;
    _dbvInited = true;
    fetch('/api/firestore/collections')
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.error) { toast(d.error, 'error'); return; }
        var sel = document.getElementById('dbv-collection');
        d.collections.forEach(function(name) {
          var opt = document.createElement('option');
          opt.value = name;
          opt.textContent = name;
          sel.appendChild(opt);
        });
      })
      .catch(function() { toast('載入集合清單失敗', 'error'); });
  }

  // 載入指定集合的資料
  function dbvLoadCollection(page) {
    var collection = document.getElementById('dbv-collection').value;
    if (!collection) { toast('請先選擇集合', 'info'); return; }
    _dbvPage = page || 1;
    _dbvSortCol = '';  // 重新載入時清除排序
    _dbvSortAsc = true;

    var keyword = (document.getElementById('dbv-keyword').value || '').trim();
    var perPage = document.getElementById('dbv-perpage').value || '50';

    var url = '/api/firestore/browse?collection=' + encodeURIComponent(collection)
      + '&page=' + _dbvPage
      + '&per_page=' + perPage;
    if (keyword) url += '&keyword=' + encodeURIComponent(keyword);

    document.getElementById('dbv-info').textContent = '載入中…';
    document.getElementById('dbv-thead').innerHTML = '';
    document.getElementById('dbv-tbody').innerHTML = '';
    document.getElementById('dbv-pager').innerHTML = '';

    fetch(url)
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.error) { toast(d.error, 'error'); document.getElementById('dbv-info').textContent = ''; return; }
        // 統計資訊
        var startNum = (d.page - 1) * d.per_page + 1;
        var endNum = Math.min(d.page * d.per_page, d.total);
        document.getElementById('dbv-info').textContent =
          '集合：' + d.collection + '　共 ' + d.total + ' 筆'
          + (d.total > 0 ? '　顯示第 ' + startNum + '~' + endNum + ' 筆' : '');

        // 存下資料供前端排序用
        window._dbvData = d;

        dbvRenderTable(d);
        dbvRenderPager(d);
      })
      .catch(function() { toast('載入資料失敗', 'error'); document.getElementById('dbv-info').textContent = ''; });
  }

  // 渲染表格
  function dbvRenderTable(d) {
    var thead = document.getElementById('dbv-thead');
    var tbody = document.getElementById('dbv-tbody');
    thead.innerHTML = '';
    tbody.innerHTML = '';

    if (!d.columns || d.columns.length === 0 || d.rows.length === 0) {
      tbody.innerHTML = '<tr><td class="px-4 py-8 text-center" style="color:var(--txs);">無資料</td></tr>';
      return;
    }

    // 表頭
    var headerRow = document.createElement('tr');
    headerRow.style.cssText = 'background:var(--bg-h);border-bottom:2px solid var(--bd);';
    d.columns.forEach(function(col) {
      var th = document.createElement('th');
      th.style.cssText = 'padding:8px 12px;text-align:left;font-size:12px;font-weight:600;color:var(--tx);white-space:nowrap;cursor:pointer;user-select:none;';
      // 顯示欄位名稱（__doc_id__ 顯示為「文件 ID」）
      var label = col === '__doc_id__' ? '文件 ID' : col;
      // 排序箭頭
      var arrow = '';
      if (_dbvSortCol === col) arrow = _dbvSortAsc ? ' ▲' : ' ▼';
      th.textContent = label + arrow;
      th.onclick = (function(c) {
        return function() { dbvSort(c); };
      })(col);
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    // 資料列
    var rows = d.rows;
    rows.forEach(function(row, idx) {
      var tr = document.createElement('tr');
      tr.style.cssText = 'border-bottom:1px solid var(--bd);transition:background 0.15s;';
      // 交替背景色
      if (idx % 2 === 1) tr.style.background = 'var(--bg-t)';
      tr.onmouseenter = function() { this.style.background = 'var(--bg-h)'; };
      tr.onmouseleave = function() { this.style.background = idx % 2 === 1 ? 'var(--bg-t)' : ''; };

      d.columns.forEach(function(col) {
        var td = document.createElement('td');
        td.style.cssText = 'padding:6px 12px;font-size:13px;color:var(--tx);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
        td.textContent = row[col] || '';
        td.title = row[col] || '';  // hover 顯示完整內容
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
  }

  // 前端排序（點表頭）
  function dbvSort(col) {
    if (!window._dbvData) return;
    // 同一欄位切換升降冪，不同欄位預設升冪
    if (_dbvSortCol === col) {
      _dbvSortAsc = !_dbvSortAsc;
    } else {
      _dbvSortCol = col;
      _dbvSortAsc = true;
    }
    // 排序資料列
    var rows = window._dbvData.rows.slice();  // 複製一份
    rows.sort(function(a, b) {
      var va = (a[col] || '').toString();
      var vb = (b[col] || '').toString();
      // 嘗試數字比較
      var na = parseFloat(va), nb = parseFloat(vb);
      if (!isNaN(na) && !isNaN(nb)) {
        return _dbvSortAsc ? na - nb : nb - na;
      }
      // 字串比較
      return _dbvSortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
    });
    var sorted = Object.assign({}, window._dbvData, { rows: rows });
    dbvRenderTable(sorted);
  }

  // 渲染分頁按鈕
  function dbvRenderPager(d) {
    var pager = document.getElementById('dbv-pager');
    pager.innerHTML = '';
    if (d.pages <= 1) return;

    var btnStyle = 'padding:6px 14px;border-radius:8px;font-size:13px;cursor:pointer;transition:all 0.15s;';

    // 上一頁
    if (d.page > 1) {
      var prev = document.createElement('button');
      prev.textContent = '← 上一頁';
      prev.style.cssText = btnStyle + 'background:var(--bg-h);color:var(--tx);border:1px solid var(--bd);';
      prev.onclick = function() { dbvLoadCollection(d.page - 1); };
      pager.appendChild(prev);
    }

    // 頁碼資訊
    var info = document.createElement('span');
    info.style.cssText = 'font-size:13px;color:var(--txs);';
    info.textContent = '第 ' + d.page + ' / ' + d.pages + ' 頁';
    pager.appendChild(info);

    // 下一頁
    if (d.page < d.pages) {
      var next = document.createElement('button');
      next.textContent = '下一頁 →';
      next.style.cssText = btnStyle + 'background:var(--bg-h);color:var(--tx);border:1px solid var(--bd);';
      next.onclick = function() { dbvLoadCollection(d.page + 1); };
      pager.appendChild(next);
    }
  }

  // ══ 公司物件搜尋 ══
  var _cpPage = 1;
  var _cpLastQuery = {};

  // ══ 複選狀態管理 ══
  var _cpSelected = { cat: new Set(), area: new Set(), agent: new Set() };
  var _cpOptionsData = {};  // 儲存 options 供重建 label 用

  // 開關複選面板，點外部關閉
  function cpToggleDropdown(type) {
    var panel = document.getElementById('cp-' + type + '-panel');
    var isHidden = panel.classList.contains('hidden');
    // 先關所有面板
    ['cat','area','agent'].forEach(function(t) {
      document.getElementById('cp-' + t + '-panel').classList.add('hidden');
    });
    if (isHidden) panel.classList.remove('hidden');
  }
  document.addEventListener('click', function(e) {
    ['cat','area','agent'].forEach(function(t) {
      var btn = document.getElementById('cp-' + t + '-btn');
      var panel = document.getElementById('cp-' + t + '-panel');
      if (btn && panel && !btn.contains(e.target) && !panel.contains(e.target)) {
        panel.classList.add('hidden');
      }
    });
  });

  // 建立勾選框項目
  function _cpMakeCheckbox(type, value, label) {
    var wrap = document.createElement('label');
    wrap.className = 'flex items-center gap-2 text-sm text-slate-200 cursor-pointer hover:text-white py-0.5';
    var cb = document.createElement('input');
    cb.type = 'checkbox'; cb.value = value;
    cb.className = 'w-3.5 h-3.5 rounded accent-blue-500';
    cb.checked = _cpSelected[type].has(value);
    cb.addEventListener('change', function() {
      if (this.checked) _cpSelected[type].add(value);
      else _cpSelected[type].delete(value);
      _cpUpdateLabel(type);
    });
    wrap.appendChild(cb);
    wrap.appendChild(document.createTextNode(label));
    return wrap;
  }

  // 更新按鈕標籤文字
  function _cpUpdateLabel(type) {
    var sel = _cpSelected[type];
    var labelEl = document.getElementById('cp-' + type + '-label');
    if (!sel.size) {
      var defaults = {cat:'全部類別', area:'全部地區', agent:'全部經紀人'};
      labelEl.textContent = defaults[type];
      labelEl.className = '';
    } else {
      var vals = Array.from(sel);
      // 地區要顯示完整名稱
      if (type === 'area' && _cpOptionsData.areas) {
        vals = vals.map(function(v) {
          var found = (_cpOptionsData.areas || []).find(function(a) { return a.value === v; });
          return found ? found.label.split(' ').pop() : v;  // 取最後一段（市/鄉/鎮）
        });
      }
      labelEl.textContent = vals.length <= 2 ? vals.join('、') : vals[0] + ' 等' + vals.length + '項';
      labelEl.className = 'text-blue-300 font-semibold';
    }
  }

  function cpLoadOptions() {
    fetch('/api/company-properties/options').then(r => r.json()).then(function(data) {
      if (data.error) return;
      window._cpOptionsLoaded = true;
      _cpOptionsData = data;

      // 類別複選面板
      var catList = document.getElementById('cp-cat-list');
      (data.categories || []).forEach(function(c) {
        catList.appendChild(_cpMakeCheckbox('cat', c, c));
      });

      // 地區複選面板
      var areaList = document.getElementById('cp-area-list');
      (data.areas || []).forEach(function(a) {
        var val = (typeof a === 'object') ? a.value : a;
        var lbl = (typeof a === 'object') ? a.label : a;
        areaList.appendChild(_cpMakeCheckbox('area', val, lbl));
      });

      // 經紀人複選面板（在線 + 其他分群）
      var agentData   = data.agents || {};
      var activeList  = Array.isArray(agentData) ? agentData : (agentData.active   || []);
      var inactList   = Array.isArray(agentData) ? []        : (agentData.inactive || []);
      var activePanel = document.getElementById('cp-agent-active-list');
      var inactPanel  = document.getElementById('cp-agent-inactive-list');
      activeList.forEach(function(a) { activePanel.appendChild(_cpMakeCheckbox('agent', a, a)); });
      inactList.forEach(function(a)  { inactPanel.appendChild(_cpMakeCheckbox('agent', a, a)); });
    });
  }

  // 從 session 預設帶入登入者姓名，並預設銷售中
  function cpLoadMe() {
    fetch('/api/me').then(r => r.json()).then(function(data) {
      if (data.error || !data.name) return;
      var name = data.name;
      // 等 options 載入完成後再勾選
      var tryCheck = function() {
        // 若目前正在定位特定物件（由 ?prop_name= 觸發），不要覆蓋篩選條件
        if (window._cpLocating) return;
        var panel = document.getElementById('cp-agent-active-list');
        var inact = document.getElementById('cp-agent-inactive-list');
        if (!panel) { setTimeout(tryCheck, 200); return; }
        // 找對應 checkbox 打勾
        var allCbs = panel.querySelectorAll('input[type=checkbox]');
        var found = false;
        allCbs.forEach(function(cb) {
          if (cb.value === name) { cb.checked = true; _cpSelected.agent.add(name); found = true; }
        });
        if (!found) {
          // 不在在線名單，找其他群
          inact.querySelectorAll('input[type=checkbox]').forEach(function(cb) {
            if (cb.value === name) { cb.checked = true; _cpSelected.agent.add(name); }
          });
        }
        _cpUpdateLabel('agent');
        // 預設到期日篩選「委託中（未過期）」+ 排序「到期日 近→遠」
        var expiryEl = document.getElementById('cp-expiry');
        if (expiryEl) expiryEl.value = 'active';
        var sortEl = document.getElementById('cp-sort');
        if (sortEl) sortEl.value = 'expiry_asc';
        cpSearch();  // 帶入姓名後自動搜尋
      };
      // options 可能還沒載入，稍等
      if (window._cpOptionsLoaded) tryCheck();
      else {
        var wait = setInterval(function() {
          if (window._cpOptionsLoaded) { clearInterval(wait); tryCheck(); }
        }, 150);
      }
    });
  }

  function cpSearch() {
    _cpPage = 1;
    var onHsCb = document.getElementById('cp-on-home-start');
    _cpLastQuery = {
      keyword:   document.getElementById('cp-keyword').value.trim(),
      category:  Array.from(_cpSelected.cat).join(','),
      area:      Array.from(_cpSelected.area).join(','),
      price_min: document.getElementById('cp-price-min').value,
      price_max: document.getElementById('cp-price-max').value,
      status:    document.getElementById('cp-status').value,
      agent:     Array.from(_cpSelected.agent).join(','),
      sort:      (document.getElementById('cp-sort') || {}).value || 'serial_desc',
      on_home_start: (onHsCb && onHsCb.checked) ? '1' : '',
    };
    cpFetch();
  }

  function cpReset() {
    document.getElementById('cp-keyword').value = '';
    document.getElementById('cp-price-min').value = '';
    document.getElementById('cp-price-max').value = '';
    document.getElementById('cp-status').value = 'selling';
    // 清除複選
    ['cat','area','agent'].forEach(function(t) {
      _cpSelected[t].clear();
      _cpUpdateLabel(t);
      var panels = ['cp-'+t+'-list','cp-'+t+'-active-list','cp-'+t+'-inactive-list'];
      panels.forEach(function(pid) {
        var el = document.getElementById(pid);
        if (el) el.querySelectorAll('input[type=checkbox]').forEach(function(cb){ cb.checked=false; });
      });
    });
    _cpPage = 1;
    _cpLastQuery = {};
    var sortEl = document.getElementById('cp-sort');
    if (sortEl) sortEl.value = 'expiry_asc';
    var expiryEl = document.getElementById('cp-expiry');
    if (expiryEl) expiryEl.value = 'active';
    // 重設星號篩選
    var starBtn = document.getElementById('cp-star-filter-btn');
    if (starBtn) {
      starBtn.dataset.active = '0';
      starBtn.classList.remove('bg-yellow-500/20', 'border-yellow-500', 'text-yellow-300');
      starBtn.classList.add('bg-slate-700', 'text-slate-300');
      document.getElementById('cp-star-filter-icon').textContent = '☆';
    }
    document.getElementById('cp-list').innerHTML = '';
    document.getElementById('cp-info').classList.add('hidden');
    document.getElementById('cp-pagination').classList.add('hidden');
    document.getElementById('cp-placeholder').classList.remove('hidden');
  }

  // ══════════════════════════════════════════
  //  情境書籤（cp-presets）
  // ══════════════════════════════════════════
  var _cpPresets = [];  // 快取目前使用者的情境清單

  // 載入情境清單並填入下拉選單
  function cpLoadPresets() {
    fetch('/api/cp-presets')
      .then(function(r){ return r.json(); })
      .then(function(d) {
        _cpPresets = d.items || [];
        var sel = document.getElementById('cp-preset-select');
        if (!sel) return;
        var current = sel.value;
        sel.innerHTML = '<option value="">— 選擇情境 —</option>'
          + _cpPresets.map(function(p) {
              return '<option value="' + escapeHtml(p.id) + '">' + escapeHtml(p.name) + '</option>';
            }).join('');
        // 恢復選中狀態
        if (current) sel.value = current;
        cpUpdatePresetDeleteBtn();
      })
      .catch(function(){});
  }

  // 套用選中的情境（自動填入篩選/排序並搜尋）
  function cpApplyPreset() {
    var sel = document.getElementById('cp-preset-select');
    cpUpdatePresetDeleteBtn();
    if (!sel || !sel.value) return;
    var preset = _cpPresets.find(function(p){ return p.id === sel.value; });
    if (!preset || !preset.params) return;
    var p = preset.params;
    // 填入關鍵字
    var kwEl = document.getElementById('cp-keyword');
    if (kwEl) kwEl.value = p.keyword || '';
    // 填入售價
    var pmn = document.getElementById('cp-price-min');
    var pmx = document.getElementById('cp-price-max');
    if (pmn) pmn.value = p.price_min || '';
    if (pmx) pmx.value = p.price_max || '';
    // 填入狀態
    var stEl = document.getElementById('cp-status');
    if (stEl) stEl.value = p.status || '';
    // 填入到期日篩選
    var expEl = document.getElementById('cp-expiry');
    if (expEl) expEl.value = p.expiry || '';
    // 填入排序
    var sortEl = document.getElementById('cp-sort');
    if (sortEl) sortEl.value = p.sort || 'serial_desc';
    // 填入多選：類別、地區、經紀人
    ['cat','area','agent'].forEach(function(t) {
      var vals = p['sel_' + t] ? p['sel_' + t].split(',').filter(Boolean) : [];
      _cpSelected[t] = new Set(vals);
      _cpUpdateLabel(t);
      // 同步 checkbox 狀態
      var panels = ['cp-'+t+'-list','cp-'+t+'-active-list','cp-'+t+'-inactive-list'];
      panels.forEach(function(pid) {
        var el = document.getElementById(pid);
        if (!el) return;
        el.querySelectorAll('input[type=checkbox]').forEach(function(cb) {
          cb.checked = vals.includes(cb.value);
        });
      });
    });
    // 星號篩選
    var starBtn = document.getElementById('cp-star-filter-btn');
    var starIcon = document.getElementById('cp-star-filter-icon');
    var starLabel = document.getElementById('cp-star-filter-label');
    if (starBtn) {
      var starActive = !!p.star_filter;
      starBtn.dataset.active = starActive ? '1' : '0';
      starBtn.classList.toggle('bg-amber-600', starActive);
      starBtn.classList.toggle('hover:bg-amber-500', starActive);
      starBtn.classList.toggle('bg-slate-700', !starActive);
      starBtn.classList.toggle('hover:bg-slate-600', !starActive);
      if (starIcon)  starIcon.textContent  = starActive ? '★' : '☆';
      if (starLabel) starLabel.textContent = starActive ? '追蹤中' : '追蹤中';
    }
    // 執行搜尋
    cpSearch();
    toast('✅ 已套用情境「' + preset.name + '」', 'info');
  }

  // 顯示/隱藏刪除按鈕（有選中情境才顯示）
  function cpUpdatePresetDeleteBtn() {
    var sel = document.getElementById('cp-preset-select');
    var btn = document.getElementById('cp-preset-delete-btn');
    if (!sel || !btn) return;
    btn.classList.toggle('hidden', !sel.value);
  }

  // 儲存目前篩選/排序為情境
  function cpSavePreset() {
    var name = prompt('請輸入情境名稱（同名會覆蓋）：');
    if (!name || !name.trim()) return;
    // 收集目前所有篩選/排序狀態
    var params = {
      keyword:   (document.getElementById('cp-keyword') || {}).value || '',
      price_min: (document.getElementById('cp-price-min') || {}).value || '',
      price_max: (document.getElementById('cp-price-max') || {}).value || '',
      status:    (document.getElementById('cp-status') || {}).value || '',
      expiry:    (document.getElementById('cp-expiry') || {}).value || '',
      sort:      (document.getElementById('cp-sort') || {}).value || 'serial_desc',
      sel_cat:   Array.from(_cpSelected.cat || []).join(','),
      sel_area:  Array.from(_cpSelected.area || []).join(','),
      sel_agent: Array.from(_cpSelected.agent || []).join(','),
      star_filter: document.getElementById('cp-star-filter-btn') && document.getElementById('cp-star-filter-btn').dataset.active === '1' ? 1 : 0,
    };
    fetch('/api/cp-presets', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ name: name.trim(), params: params }),
    })
    .then(function(r){ return r.json(); })
    .then(function(d) {
      if (d.error) { toast(d.error, 'error'); return; }
      toast('💾 情境「' + name.trim() + '」已儲存', 'success');
      cpLoadPresets();  // 重新載入選單
      // 選中剛儲存的情境
      setTimeout(function() {
        var sel = document.getElementById('cp-preset-select');
        if (sel && d.id) { sel.value = d.id; cpUpdatePresetDeleteBtn(); }
      }, 400);
    })
    .catch(function(){ toast('儲存失敗', 'error'); });
  }

  // 刪除選中的情境
  function cpDeletePreset() {
    var sel = document.getElementById('cp-preset-select');
    if (!sel || !sel.value) return;
    var preset = _cpPresets.find(function(p){ return p.id === sel.value; });
    if (!preset) return;
    if (!confirm('確定刪除情境「' + preset.name + '」？')) return;
    fetch('/api/cp-presets/' + encodeURIComponent(sel.value), { method: 'DELETE' })
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (d.error) { toast(d.error, 'error'); return; }
        toast('已刪除情境「' + preset.name + '」', 'info');
        sel.value = '';
        cpUpdatePresetDeleteBtn();
        cpLoadPresets();
      })
      .catch(function(){ toast('刪除失敗', 'error'); });
  }

  // 切換星號篩選模式
  function cpToggleStarFilter() {
    var btn = document.getElementById('cp-star-filter-btn');
    var icon = document.getElementById('cp-star-filter-icon');
    var isActive = btn.dataset.active === '1';
    if (isActive) {
      // 取消篩選
      btn.dataset.active = '0';
      btn.classList.remove('bg-yellow-500/20', 'border-yellow-500', 'text-yellow-300');
      btn.classList.add('bg-slate-700', 'text-slate-300');
      icon.textContent = '☆';
    } else {
      // 啟用篩選
      btn.dataset.active = '1';
      btn.classList.remove('bg-slate-700', 'text-slate-300');
      btn.classList.add('bg-yellow-500/20', 'border-yellow-500', 'text-yellow-300');
      icon.textContent = '★';
    }
    _cpPage = 1;
    cpFetch();
  }

  function cpChangePage(dir) {
    _cpPage = Math.max(1, _cpPage + dir);
    cpFetch();
    window.scrollTo(0, 0);
  }

  // ══ Word Snapshot 售價對比 ══
  var _cpWordPrices = {};   // {normalized案名: {案名, 委託號碼, 售價萬}}

  // 鄉市鎮簡稱 → FOUNDI 用的完整鄉市鎮名對照表
  var _AREA_MAP = {
    '台東':'台東市','台東市':'台東市',
    '卑南':'卑南鄉','太麻里':'太麻里鄉','大武':'大武鄉','金峯':'金峯鄉','金鋒':'金峯鄉',
    '達仁':'達仁鄉','蘭嶼':'蘭嶼鄉','綠島':'綠島鄉',
    '長濱':'長濱鄉','成功':'成功鎮','東河':'東河鄉','鹿野':'鹿野鄉',
    '關山':'關山鎮','池上':'池上鄉','延平':'延平鄉','海端':'海端鄉',
    '富里':'富里鄉',
    // 花蓮
    '光復':'光復鄉','壽豐':'壽豐鄉','玉里':'玉里鎮','鳳林':'鳳林鎮',
    '花蓮':'花蓮市','花蓮富里':'富里鄉','花蓮豐濱':'豐濱鄉',
  };

  // 建立 FOUNDI 查詢連結
  function _buildFoundiUrl(item) {
    var cat = item['物件類別'] || '';
    var area = item['鄉/市/鎮'] || '';
    var locality = _AREA_MAP[area] || '';
    // 縣市判斷：花蓮相關用花蓮縣，其餘用台東縣
    var city = (area.indexOf('花蓮') >= 0 || ['光復','壽豐','玉里','鳳林'].indexOf(area) >= 0)
               ? '花蓮縣' : '台東縣';

    // 土地類（農地/建地）→ 地號查詢
    if (cat === '農地' || cat === '建地' || cat === '農建地') {
      var section = item['段別'] || '';
      var landNo  = String(item['地號'] || '').trim().split(/[ \t,，]+/)[0]; // 多地號取第一個
      if (!section || !landNo || !locality) return '';
      // 地號拆主號/次號：998-13 → main=998, sub=13；6555 → main=6555, sub=0
      var parts = landNo.split('-');
      var main = parts[0].replace(/[^0-9]/g,'');
      var sub  = parts[1] ? parts[1].replace(/[^0-9]/g,'') : '0';
      if (!main) return '';
      var sectionName = section.replace(/段$/, '') + '段'; // 確保有「段」字
      return 'https://www.foundi.info/tool/land?location_type=land_address'
           + '&city=' + encodeURIComponent(city)
           + '&locality=' + encodeURIComponent(locality)
           + '&section_name=' + encodeURIComponent(sectionName)
           + '&main_key=' + encodeURIComponent(main)
           + '&sub_key=' + encodeURIComponent(sub);
    }

    // 房屋類（公寓/房屋/店住/辦公）→ 地址查詢
    var addr = String(item['物件地址'] || '').trim();
    if (!addr || !locality) return '';
    // 補全地址：若未含鄉鎮市名，加在前面
    var fullAddr = addr;
    if (addr.indexOf(locality) < 0) fullAddr = locality + addr;
    return 'https://www.foundi.info/tool/address'
         + '?city=' + encodeURIComponent(city)
         + '&locality=' + encodeURIComponent(locality)
         + '&road=' + encodeURIComponent(city + fullAddr);
  }

  // 正規化案名（和後端一致）
  function _normName(s) {
    return String(s || '').replace(/[ \t]+/g, '').replace(/(?<![0-9])[0-9]{5,6}(?![0-9])/g, '').trim();
  }

  // 載入目前 snapshot 的售價字典
  function cpLoadWordSnapshot() {
    fetch('/api/word-snapshot/prices').then(r => r.json()).then(function(data) {
      _cpWordPrices = data || {};
    }).catch(function() { _cpWordPrices = {}; });
  }

  // 顯示 Word snapshot 狀態（管理員）+ 物件總表日期
  function cpLoadWordSnapshotStatus() {
    fetch('/api/word-snapshot/status').then(r => r.json()).then(function(data) {
      var el = document.getElementById('cp-word-status');
      if (!el) return;
      if (data.status === 'none' || data.status === 'no_db') {
        el.textContent = '尚無物件總表';
      } else if (data.status === 'ok') {
        // 只存 doc_date 沒實際 snapshot 時，filename/count 是空的 → 不顯示空殼
        if (!data.filename && !data.count) {
          el.textContent = '';
        } else {
          var dt = data.uploaded_at ? new Date(data.uploaded_at).toLocaleDateString('zh-TW') : '';
          el.textContent = '總表：' + (data.filename || '(未命名)') + '（' + dt + '，' + (data.count||0) + '筆）';
        }
      }
    }).catch(function() {});

    // 同時顯示物件總表文件日期（右上角日期）
    fetch('/api/word-snapshot/meta').then(r => r.json()).then(function(data) {
      var el = document.getElementById('cp-doc-date');
      if (!el) return;
      var d = data.doc_date;
      if (d && d.minguo) {
        el.textContent = '📄 總表：' + d.minguo;
        el.title = '物件總表更新日期（Word 文件右上角）';
      } else {
        el.textContent = '';
      }
    }).catch(function() {});
  }

  // 上傳解析後的 CSV（export_word_table.py 產出），精確更新 Firestore
  // 支援一次多選多個 CSV + word_meta.json（自動偵測），依序上傳累計結果
  function cpUploadCsv(input) {
    if (!input.files || !input.files.length) return;
    var allFiles = Array.from(input.files);

    // 分流：.json 獨立處理（上傳物件總表日期），.csv 正常上傳
    var jsonFiles = allFiles.filter(function(f){ return f.name.toLowerCase().endsWith('.json'); });
    var files     = allFiles.filter(function(f){ return f.name.toLowerCase().endsWith('.csv'); });

    // 上傳 word_meta.json → /api/word-snapshot/meta
    jsonFiles.forEach(function(jf) {
      var reader = new FileReader();
      reader.onload = function(e) {
        try {
          var meta = JSON.parse(e.target.result);
          fetch('/api/word-snapshot/meta', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(meta),
          }).then(function(r){ return r.json(); }).then(function(d) {
            if (d.ok) {
              var el = document.getElementById('cp-doc-date');
              if (el) el.textContent = '📄 總表：' + d.minguo;
              toast('✅ 物件總表日期已更新：' + d.minguo, 'success');
            }
          }).catch(function(){});
        } catch(e) {}
      };
      reader.readAsText(jf, 'utf-8');
    });

    if (!files.length) { input.value = ''; return; }

    var el = document.getElementById('cp-word-status');
    var totalRows = 0, totalUpdated = 0, done = 0, errors = [];

    var _timer = null, _dots = 0;
    function _startProgress() {
      _timer = setInterval(function() {
        _dots = (_dots + 1) % 4;
        if (el) el.textContent = '上傳 ' + done + '/' + files.length + ' 個 CSV' + '…'.repeat(_dots + 1);
      }, 700);
    }
    function _stopProgress() { if (_timer) { clearInterval(_timer); _timer = null; } }

    if (el) el.textContent = '準備上傳 ' + files.length + ' 個 CSV…';
    _startProgress();

    // 逐一依序上傳（避免同時大量請求）
    function uploadNext(idx) {
      if (idx >= files.length) {
        // 全部完成
        _stopProgress();
        var msg = '✅ ' + files.length + ' 個 CSV，共 ' + totalRows + ' 筆 → 更新 Firestore ' + totalUpdated + ' 筆';
        if (errors.length) msg += '（' + errors.length + ' 個失敗：' + errors.join('、') + '）';
        toast(msg, errors.length ? 'warn' : 'success');
        if (el) el.textContent = msg;
        setTimeout(function(){ cpFetch(); }, 600);
        input.value = '';
        return;
      }
      var file = files[idx];
      var fd = new FormData();
      fd.append('file', file);
      fetch('/api/word-snapshot/upload-csv', { method: 'POST', body: fd })
        .then(r => r.json()).then(function(data) {
          done++;
          if (data.error) {
            errors.push(file.name);
          } else {
            totalRows    += (data.csv_rows || 0);
            totalUpdated += (data.updated_firestore || 0);
          }
          uploadNext(idx + 1);
        }).catch(function(e) {
          done++;
          errors.push(file.name);
          uploadNext(idx + 1);
        });
    }
    uploadNext(0);
  }

  // ── 物件總表比對審查（日盛房屋管理員專用）────────────────────────
  // 全域狀態：審查結果與使用者選擇
  var _rvData = { high: [], medium: [], conflict: [], unmatched: [] };
  var _rvConfirmed = {};   // {doc_id: {doc_id, price, expiry, name_changed, old_name, new_name}}
  var _rvMetaFiles = [];   // word_meta.json 暫存，等審查完再上傳

  // 入口：使用者選取檔案後觸發
  function cpOpenReview(input) {
    if (!input.files || !input.files.length) return;
    var allFiles = Array.from(input.files);
    var jsonFiles = allFiles.filter(function(f){ return f.name.toLowerCase().endsWith('.json'); });
    var csvFiles  = allFiles.filter(function(f){ return f.name.toLowerCase().endsWith('.csv'); });
    var docFiles  = allFiles.filter(function(f){ return f.name.toLowerCase().endsWith('.doc') || f.name.toLowerCase().endsWith('.docx'); });
    _rvMetaFiles = jsonFiles;

    // --- 路徑一：直接上傳 .doc（雲端解析） ---
    if (docFiles.length) {
      var modal = document.getElementById('cp-review-modal');
      modal.style.display = 'flex';
      document.getElementById('rv-loading').style.display = 'block';
      document.getElementById('rv-results').style.display = 'none';
      document.getElementById('rv-loading-text').textContent = '上傳並解析 Word 物件總表…';
      document.getElementById('rv-subtitle').textContent = '';
      _rvData = { high: [], medium: [], conflict: [], unmatched: [] };
      _rvConfirmed = {};

      var fd = new FormData();
      fd.append('file', docFiles[0]);   // 每次只傳一個 .doc
      fetch('/api/word-review/upload-doc', { method: 'POST', body: fd })
        .then(function(r){ return r.json(); })
        .then(function(d) {
          if (d.error) {
            toast('❌ ' + d.error, 'error');
            modal.style.display = 'none';
            input.value = '';
            return;
          }
          // 更新總表日期顯示（doc_date 是物件 {minguo, western, ...}，取 minguo）
          if (d.doc_date) {
            var el = document.getElementById('cp-doc-date');
            if (el) {
              var minguo = (typeof d.doc_date === 'string') ? d.doc_date : (d.doc_date.minguo || '');
              if (minguo) el.textContent = '📄 總表：' + minguo;
            }
          }
          // 合併分析結果
          _rvData.high      = d.high      || [];
          _rvData.medium    = d.medium    || [];
          _rvData.conflict  = d.conflict  || [];
          _rvData.unmatched = d.unmatched || [];
          _rvData.csv_rows  = d.csv_rows  || 0;
          _rvRender();
          input.value = '';
        })
        .catch(function(e){
          toast('❌ 上傳失敗，請稍後再試', 'error');
          modal.style.display = 'none';
          input.value = '';
        });
      return;
    }

    // --- 路徑二：只選了 json → 直接上傳 meta ---
    if (!csvFiles.length) {
      jsonFiles.forEach(function(jf){ _rvUploadMeta(jf); });
      input.value = '';
      return;
    }

    // --- 路徑三：CSV 逐一分析 ---
    var modal = document.getElementById('cp-review-modal');
    modal.style.display = 'flex';
    document.getElementById('rv-loading').style.display = 'block';
    document.getElementById('rv-results').style.display = 'none';
    document.getElementById('rv-loading-text').textContent = '分析中（0/' + csvFiles.length + '）…';
    document.getElementById('rv-subtitle').textContent = '';

    // 重置狀態
    _rvData = { high: [], medium: [], conflict: [], unmatched: [] };
    _rvConfirmed = {};

    // 逐一分析每個 CSV
    function analyzeNext(idx) {
      if (idx >= csvFiles.length) {
        _rvRender();
        input.value = '';
        return;
      }
      var fd = new FormData();
      fd.append('file', csvFiles[idx]);
      document.getElementById('rv-loading-text').textContent =
        '分析中（' + (idx+1) + '/' + csvFiles.length + '）：' + csvFiles[idx].name;
      fetch('/api/word-review/analyze', { method: 'POST', body: fd })
        .then(function(r){ return r.json(); })
        .then(function(d) {
          if (d.error) { toast('❌ ' + d.error, 'error'); }
          else {
            _rvData.high     = _rvData.high.concat(d.high     || []);
            _rvData.medium   = _rvData.medium.concat(d.medium   || []);
            _rvData.conflict = _rvData.conflict.concat(d.conflict || []);
            _rvData.unmatched = _rvData.unmatched.concat(d.unmatched || []);
          }
          analyzeNext(idx + 1);
        })
        .catch(function(){ toast('❌ 分析失敗：' + csvFiles[idx].name, 'error'); analyzeNext(idx+1); });
    }
    analyzeNext(0);
  }

  // 渲染審查結果
  function _rvRender() {
    var d = _rvData;
    var issueCount = d.conflict.length + d.unmatched.length;

    // 顯示結果區
    document.getElementById('rv-loading').style.display = 'none';
    document.getElementById('rv-results').style.display = 'flex';
    var totalWord = d.csv_rows || (d.high.length + d.medium.length + d.conflict.length + d.unmatched.length);
    var subtitle = 'Word 共 ' + totalWord + ' 筆｜高信心 ' + d.high.length + ' ／ 中信心 ' + d.medium.length + ' ／ 問題 ' + issueCount;
    // 未配對數量高（> 5 筆）→ subtitle 加紅色提示「強烈建議先同步 Sheets」
    var unmatchedCount = (d.unmatched || []).length;
    if (unmatchedCount > 5) {
      subtitle += ' ｜ <span style="color:#dc2626;font-weight:700;">⚠️ 未配對 ' + unmatchedCount
        + ' 筆，多半是主 Sheets 已有但 Firestore 沒同步，建議先 🔄 同步 Sheets</span>';
      document.getElementById('rv-subtitle').innerHTML = subtitle;
    } else {
      document.getElementById('rv-subtitle').textContent = subtitle;
    }

    // 更新 tab 數字
    document.getElementById('rv-count-high').textContent   = d.high.length;
    document.getElementById('rv-count-medium').textContent = d.medium.length;
    document.getElementById('rv-count-issues').textContent = issueCount;

    // ── 共用工具函數 ──────────────────────────────────────────────
    // 單一欄位行（空值不顯示）
    function fmtR(label, val) {
      if (val === null || val === undefined || val === '') return '';
      return '<div style="font-size:11px;color:var(--txs);margin-top:2px;"><span style="color:var(--txm);font-weight:600;">'
        + label + '：</span>' + val + '</div>';
    }
    // 售價顯示（有變動則標綠）
    function fmtPrice(dbP, csvP) {
      if (csvP === null || csvP === undefined) return '-';
      if (dbP !== null && dbP !== undefined && dbP !== csvP)
        return '<span style="color:var(--txm);text-decoration:line-through;">' + dbP + '</span>'
          + ' <strong style="color:var(--ok);">→ ' + csvP + '</strong> 萬';
      return csvP + ' 萬';
    }
    // 比對方式 → 人類可讀說明
    function fmtMatchReason(item, type) {
      var m = item.match_by || '', s = item.score;
      var icon = type === 'high' ? '✅' : (type === 'medium' ? '⚠️' : '⚡');
      if (m === '委託號碼')                      return icon + ' 委託號碼精確命中（最可靠）';
      if (m === '資料序號')                      return icon + ' 資料序號直接命中（最可靠）';
      if (m === '物件地址')                      return icon + ' 物件地址精確命中';
      if (m.indexOf('地址不符') >= 0)            return icon + ' ' + m.replace('（地址不符，請確認）','') + ' 命中，但地址不同，請確認是否同一物件（可能是門牌打錯或不同單位）';
      if (type === 'high') {
        if (m.indexOf('面積') >= 0) return icon + ' 面積硬資料吻合，評分 ' + s + ' 分';
        return icon + ' ' + m + '（評分 ' + s + ' 分）';
      }
      if (type === 'medium') {
        if (m === '案名比對（無面積驗證）')
          return icon + ' Firestore 無面積資料，無法以硬資料確認，僅靠案名比對（評分 ' + s + ' 分）';
        if (m === '近似候選升中信心')
          return icon + ' 案名略有不同，但地址／售價相符，請確認是否同一物件（評分 ' + s + ' 分）';
        if (m.indexOf('面積') >= 0)
          return icon + ' 面積有對應，但評分 ' + s + ' 分，未達高信心門檻（需 ≥3），請確認';
        return icon + ' ' + m + '，評分 ' + s + ' 分，未達高信心門檻';
      }
      // conflict
      return icon + ' 已確認面積硬資料，兩邊數字明顯不符，推斷非同一物件（' + (item.conflict_reason||'') + '）';
    }
    // 兩欄硬資料欄位（左：Word；右：Firestore）
    // 規則：只要任一方有某欄位，兩欄都顯示（沒值的那方顯示 -），確保左右對齊可比對
    function fmtHardCols(item, isRight) {
      var addr = isRight ? item.db_addr     : item.csv_addr;
      var land = isRight ? item.db_land     : item.csv_land;
      var bld  = isRight ? item.db_build    : item.csv_build;
      var inn  = isRight ? item.db_interior : item.csv_interior;
      // 數值差異 >2% 標橘（右欄才比較）
      function cmpNum(a, b) {
        if (a == null || b == null) return '';
        return (Math.abs(a - b) / Math.max(Math.abs(a), Math.abs(b)) > 0.02) ? 'color:var(--warn);font-weight:700;' : '';
      }
      // 地址不同時兩欄都標橘
      var da = item.db_addr || '', ca = item.csv_addr || '';
      var addrMismatch = da && ca && da !== ca && ca.indexOf(da) < 0 && da.indexOf(ca) < 0;
      var addrStyle = addrMismatch ? 'color:var(--warn);font-weight:700;' : '';
      var landStyle = isRight ? cmpNum(item.db_land,     item.csv_land)     : '';
      var bldStyle  = isRight ? cmpNum(item.db_build,    item.csv_build)    : '';
      var innStyle  = isRight ? cmpNum(item.db_interior, item.csv_interior) : '';
      // 任一方有值才顯示該列（沒值的那方顯示半透明 -）
      var showAddr = !!(item.db_addr || item.csv_addr);
      var showLand = item.db_land     != null || item.csv_land     != null;
      var showBld  = item.db_build    != null || item.csv_build    != null;
      var showInn  = item.db_interior != null || item.csv_interior != null;
      var dash = '<span style="opacity:0.4;">-</span>';
      function aVal(v) { return (v != null) ? v + ' 坪' : dash; }
      function aAddr(v) { return (v && v.trim()) ? v : dash; }
      return (showAddr ? '<div style="font-size:11px;color:var(--txs);margin-top:2px;' + addrStyle + '"><span style="color:var(--txm);font-weight:600;">地址：</span>' + aAddr(addr) + (addrMismatch && addr ? ' ⚠️' : '') + '</div>' : '')
        + (showLand ? '<div style="font-size:11px;color:var(--txs);margin-top:2px;' + landStyle + '"><span style="color:var(--txm);font-weight:600;">地坪：</span>' + aVal(land) + '</div>' : '')
        + (showBld  ? '<div style="font-size:11px;color:var(--txs);margin-top:2px;' + bldStyle  + '"><span style="color:var(--txm);font-weight:600;">建坪：</span>' + aVal(bld)  + '</div>' : '')
        + (showInn  ? '<div style="font-size:11px;color:var(--txs);margin-top:2px;' + innStyle  + '"><span style="color:var(--txm);font-weight:600;">室內坪：</span>' + aVal(inn) + '</div>' : '');
    }

    // ── 高信心卡片 ──────────────────────────────────────────────
    var highList = document.getElementById('rv-high-list');
    highList.innerHTML = '';
    d.high.forEach(function(item) {
      // 預設加入確認清單
      _rvConfirmed[item.doc_id] = {
        doc_id: item.doc_id,
        price:  item.csv_price,
        old_price: item.db_price,   // 原售價（用於寫入 原售價(萬) 備註）
        expiry: item.csv_expiry,
        name_changed: item.name_changed,
        old_name: item.name_changed ? item.db_name  : '',
        new_name: item.name_changed ? item.csv_name : '',
      };
      var nameLabel = item.name_changed
        ? '<span style="color:var(--warn);">📝 ' + item.db_name + ' → ' + item.csv_name + '</span>'
        : item.db_name;
      var leftCol = '<div style="flex:1;min-width:0;">'
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;text-transform:uppercase;display:flex;justify-content:space-between;align-items:center;gap:6px;">'
        + '<span>Word 物件總表</span>'
        + (item.csv_row ? '<button type="button" onclick="rvCopyName(this, ' + JSON.stringify(item.csv_name || '').replace(/"/g, '&quot;') + ')" title="點下複製案名（可貼到 Word 用 Cmd+F 搜尋）" style="background:var(--bg-h);color:var(--ac);padding:1px 7px;border-radius:9px;font-weight:700;border:none;cursor:pointer;font-family:inherit;">行 #' + item.csv_row + '</button>' : '')
        + '</div>'
        + '<div style="font-size:13px;color:var(--tx);font-weight:600;">' + (item.csv_name || item.db_name) + '</div>'
        + fmtR('售價', item.csv_price!=null ? item.csv_price+' 萬' : '')
        + fmtHardCols(item, false)
        + fmtR('委託號', item.csv_comm)
        + fmtR('到期', item.csv_expiry)
        + fmtR('經紀人', item.csv_agent)
        + '</div>';
      var rightCol = '<div style="flex:1;min-width:0;padding-left:12px;border-left:1px solid var(--bd);">'
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;display:flex;justify-content:space-between;align-items:center;gap:6px;">'
        + '<span>FIRESTORE 現有</span>'
        + (item.db_seq ? '<button type="button" onclick="cpOpenDetail(' + JSON.stringify(String(item.doc_id || '')).replace(/"/g, '&quot;') + ')" title="點下開啟物件詳情 modal" style="background:var(--bg-h);color:var(--ac);padding:1px 7px;border-radius:9px;font-weight:700;border:none;cursor:pointer;font-family:inherit;">序號 #' + item.db_seq + ' →</button>' : '')
        + '</div>'
        + '<div style="font-size:13px;color:var(--tx);font-weight:600;">' + nameLabel + '</div>'
        + fmtR('售價', fmtPrice(item.db_price, item.csv_price))
        + fmtHardCols(item, true)
        + fmtR('序號', item.db_seq ? String(item.db_seq) : '')
        + fmtR('到期', item.db_expiry)
        + fmtR('經紀人', item.db_agent)
        + '</div>';
      var div = document.createElement('div');
      div.style.cssText = 'border:1px solid var(--ok);border-radius:10px;padding:10px 14px;';
      div.innerHTML = '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">'
        + '<input type="checkbox" checked data-docid="' + item.doc_id + '" onchange="rvToggleHigh(this)" style="cursor:pointer;flex-shrink:0;">'
        + '<span style="font-size:11px;color:var(--ok);">' + fmtMatchReason(item, 'high') + '</span>'
        + '</div>'
        + '<div style="display:flex;gap:0;">' + leftCol + rightCol + '</div>';
      highList.appendChild(div);
    });

    // ── 中信心卡片 ──────────────────────────────────────────────
    var medList = document.getElementById('rv-medium-list');
    medList.innerHTML = '';
    d.medium.forEach(function(item, idx) {
      var medId = 'med-' + idx;
      var nameLabel = item.name_changed
        ? '<span style="color:var(--warn);">📝 ' + item.db_name + ' → ' + item.csv_name + '</span>'
        : (item.db_name || item.csv_name);
      var leftCol = '<div style="flex:1;min-width:0;">'
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;text-transform:uppercase;display:flex;justify-content:space-between;align-items:center;gap:6px;">'
        + '<span>Word 物件總表</span>'
        + (item.csv_row ? '<button type="button" onclick="rvCopyName(this, ' + JSON.stringify(item.csv_name || '').replace(/"/g, '&quot;') + ')" title="點下複製案名（可貼到 Word 用 Cmd+F 搜尋）" style="background:var(--bg-h);color:var(--ac);padding:1px 7px;border-radius:9px;font-weight:700;border:none;cursor:pointer;font-family:inherit;">行 #' + item.csv_row + '</button>' : '')
        + '</div>'
        + '<div style="font-size:13px;color:var(--tx);font-weight:600;">' + (item.csv_name || item.db_name) + '</div>'
        + fmtR('售價', item.csv_price!=null ? item.csv_price+' 萬' : '')
        + fmtHardCols(item, false)
        + fmtR('委託號', item.csv_comm)
        + fmtR('到期', item.csv_expiry)
        + fmtR('經紀人', item.csv_agent)
        + '</div>';
      var rightCol = '<div style="flex:1;min-width:0;padding-left:12px;border-left:1px solid var(--bd);">'
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;display:flex;justify-content:space-between;align-items:center;gap:6px;">'
        + '<span>FIRESTORE 現有</span>'
        + (item.db_seq ? '<button type="button" onclick="cpOpenDetail(' + JSON.stringify(String(item.doc_id || '')).replace(/"/g, '&quot;') + ')" title="點下開啟物件詳情 modal" style="background:var(--bg-h);color:var(--ac);padding:1px 7px;border-radius:9px;font-weight:700;border:none;cursor:pointer;font-family:inherit;">序號 #' + item.db_seq + ' →</button>' : '')
        + '</div>'
        + '<div style="font-size:13px;color:var(--tx);font-weight:600;">' + nameLabel + '</div>'
        + fmtR('售價', fmtPrice(item.db_price, item.csv_price))
        + fmtHardCols(item, true)
        + fmtR('序號', item.db_seq ? String(item.db_seq) : '')
        + fmtR('到期', item.db_expiry)
        + fmtR('經紀人', item.db_agent)
        + '</div>';
      var itemJson = JSON.stringify({
        doc_id: item.doc_id, price: item.csv_price, old_price: item.db_price,
        expiry: item.csv_expiry,
        name_changed: item.name_changed, old_name: item.db_name, new_name: item.csv_name,
        // 記憶用：套用成功後問使用者要不要記住這個 word→db 的配對，下次自動套用
        _mem_source: 'medium',
        _mem_word_name: item.csv_name || '',
        _mem_word_comm: item.csv_comm || '',
        _mem_db_seq: item.db_seq || ''
      }).replace(/"/g, '&quot;');
      var div = document.createElement('div');
      div.id = medId;
      div.dataset.csvName = item.csv_name || '';
      div.dataset.csvComm = item.csv_comm || '';
      div.style.cssText = 'border:1px solid var(--bd);border-radius:10px;padding:10px 14px;';
      div.innerHTML = '<div style="font-size:11px;color:var(--warn);margin-bottom:6px;">' + fmtMatchReason(item, 'medium') + '</div>'
        + '<div style="display:flex;gap:0;">' + leftCol + rightCol + '</div>'
        + '<div style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap;align-items:center;">'
        + '<button onclick="rvAcceptMedium(this)" data-docid="' + item.doc_id + '" data-item="' + itemJson + '"'
        + ' style="padding:4px 12px;border-radius:7px;background:var(--ok);color:#fff;border:none;font-size:12px;font-weight:700;cursor:pointer;">✅ 確認配對</button>'
        + '<button onclick="rvSkipMedium(this)" style="padding:4px 12px;border-radius:7px;background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);font-size:12px;cursor:pointer;">❌ 跳過</button>'
        + '</div>'
        + '<div class="rv-force-row" style="margin-top:6px;display:flex;align-items:center;gap:6px;">'
        + '<input class="rv-force-seq-input" placeholder="輸入 Firestore 序號（強行記憶）"'
        + ' style="flex:1;max-width:200px;padding:3px 7px;border:1px solid var(--bd);border-radius:5px;font-size:11px;" />'
        + '<button onclick="rvForceMatch(this)" data-cardid="' + medId + '"'
        + ' style="padding:4px 10px;border-radius:7px;background:#888;color:#fff;border:none;font-size:11px;cursor:pointer;">💾 強行記憶</button>'
        + '</div>';
      medList.appendChild(div);
    });

    // ── 問題清單（衝突 + 未配對）──────────────────────────────
    var issueList = document.getElementById('rv-issues-list');
    issueList.innerHTML = '';
    d.conflict.forEach(function(item, cidx) {
      var conflId = 'conf-' + cidx;
      var leftCol = '<div style="flex:1;min-width:0;">'
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;text-transform:uppercase;">Word 物件總表</div>'
        + '<div style="font-size:13px;color:var(--tx);font-weight:600;">' + item.csv_name + '</div>'
        + fmtR('售價', item.csv_price!=null ? item.csv_price+' 萬' : '')
        + fmtHardCols(item, false)
        + fmtR('委託號', item.csv_comm)
        + fmtR('到期', item.csv_expiry)
        + fmtR('經紀人', item.csv_agent)
        + '</div>';
      var rightCol = '<div style="flex:1;min-width:0;padding-left:12px;border-left:1px solid var(--bd);">'
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;display:flex;justify-content:space-between;align-items:center;gap:6px;">'
        + '<span>FIRESTORE 現有</span>'
        + (item.db_seq ? '<button type="button" onclick="cpOpenDetail(' + JSON.stringify(String(item.doc_id || '')).replace(/"/g, '&quot;') + ')" title="點下開啟物件詳情 modal" style="background:var(--bg-h);color:var(--ac);padding:1px 7px;border-radius:9px;font-weight:700;border:none;cursor:pointer;font-family:inherit;">序號 #' + item.db_seq + ' →</button>' : '')
        + '</div>'
        + '<div style="font-size:13px;color:var(--tx);font-weight:600;">' + item.db_name + '</div>'
        + fmtR('售價', item.db_price!=null ? item.db_price+' 萬' : '')
        + fmtHardCols(item, true)
        + fmtR('序號', item.db_seq ? String(item.db_seq) : '')
        + fmtR('到期', item.db_expiry)
        + fmtR('經紀人', item.db_agent)
        + '</div>';
      var div = document.createElement('div');
      div.id = conflId;
      div.dataset.csvName = item.csv_name || '';
      div.dataset.csvComm = item.csv_comm || '';
      div.style.cssText = 'background:var(--bg-t);border:1px solid var(--warn);border-radius:10px;padding:10px 14px;';
      div.innerHTML = '<div style="font-size:11px;font-weight:600;color:var(--warn);margin-bottom:6px;">'
        + fmtMatchReason(item, 'conflict') + '</div>'
        + '<div style="display:flex;gap:0;">' + leftCol + rightCol + '</div>'
        + '<div class="rv-force-row" style="margin-top:6px;display:flex;align-items:center;gap:6px;border-top:1px dashed var(--bd);padding-top:6px;">'
        + '<input class="rv-force-seq-input" placeholder="輸入 Firestore 序號（強行記憶配對）"'
        + ' style="flex:1;max-width:200px;padding:3px 7px;border:1px solid var(--bd);border-radius:5px;font-size:11px;" />'
        + '<button onclick="rvForceMatch(this)" data-cardid="' + conflId + '"'
        + ' style="padding:4px 10px;border-radius:7px;background:#888;color:#fff;border:none;font-size:11px;cursor:pointer;">💾 強行記憶</button>'
        + '</div>';
      issueList.appendChild(div);
    });
    d.unmatched.forEach(function(item, idx) {
      var cardId = 'unm-' + idx;
      var leftCol = '<div style="flex:1;min-width:0;">'
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;text-transform:uppercase;">Word 物件總表</div>'
        + '<div style="font-size:13px;color:var(--tx);font-weight:600;">' + item.csv_name + '</div>'
        + fmtR('售價', item.csv_price!=null ? item.csv_price+' 萬' : '')
        + fmtR('地址', item.csv_addr)
        + fmtR('地坪', item.csv_land!=null ? item.csv_land+' 坪' : '')
        + fmtR('建坪', item.csv_build!=null ? item.csv_build+' 坪' : '')
        + fmtR('室內坪', item.csv_interior!=null ? item.csv_interior+' 坪' : '')
        + fmtR('委託號', item.csv_comm)
        + fmtR('到期', item.csv_expiry)
        + fmtR('經紀人', item.csv_agent)
        + '</div>';
      var rightCol, buttons = '';
      if (item.nm_doc_id) {
        var nmItemJson = JSON.stringify({
          doc_id: item.nm_doc_id,
          price: item.csv_price, expiry: item.csv_expiry,
          name_changed: (item.nm_name !== item.csv_name),
          old_name: item.nm_name, new_name: item.csv_name,
          // 記憶用：套用成功後問使用者要不要記住此配對
          _mem_source: 'unmatched',
          _mem_word_name: item.csv_name || '',
          _mem_word_comm: item.csv_comm || '',
          _mem_db_seq: item.nm_seq || ''
        }).replace(/"/g, '&quot;');
        // 比對說明：逐欄檢視吻合狀況，幫助判斷是否為同一物件
        var reasonLines = [];
        if (item.nm_price != null && item.csv_price != null) {
          var pd = Math.abs(item.nm_price - item.csv_price) / (Math.max(Math.abs(item.nm_price), Math.abs(item.csv_price)) || 1);
          reasonLines.push({ok: pd < 0.05, txt: '售價 ' + item.csv_price + '萬' + (pd >= 0.05 ? '（DB:' + item.nm_price + '萬）' : '')});
        }
        var wA = item.csv_land != null ? item.csv_land : (item.csv_build != null ? item.csv_build : item.csv_interior);
        var dA = item.nm_land != null ? item.nm_land : (item.nm_build != null ? item.nm_build : item.nm_interior);
        if (wA != null && dA != null) {
          var ad = Math.abs(wA - dA) / (Math.max(Math.abs(wA), Math.abs(dA)) || 1);
          reasonLines.push({ok: ad < 0.02, txt: '面積 ' + wA + '坪' + (ad >= 0.02 ? '（DB:' + dA + '坪，差' + (ad*100).toFixed(1) + '%）' : ' ≈ ' + dA + '坪')});
        }
        if (item.nm_agent || item.csv_agent) {
          var agMatch = (item.nm_agent || '') === (item.csv_agent || '');
          reasonLines.push({ok: agMatch, txt: '經紀人 ' + (item.csv_agent||'—') + (agMatch ? '' : ' → ' + (item.nm_agent||'—'))});
        }
        var na = item.nm_addr || '', ca = item.csv_addr || '';
        if (na || ca) {
          if (!na || !ca) {
            reasonLines.push({ok: null, txt: '地址 Word:' + (ca||'無') + ' / DB:' + (na||'無')});
          } else if (na === ca || na.indexOf(ca) >= 0 || ca.indexOf(na) >= 0) {
            reasonLines.push({ok: true, txt: '地址相符'});
          } else {
            reasonLines.push({ok: false, txt: '地址不符 ' + ca + ' / ' + na});
          }
        }
        if (item.csv_comm) {
          var nmComm = item.nm_comm || '';
          reasonLines.push({ok: null, txt: '委託號 Word:' + item.csv_comm + (nmComm ? '  DB:' + nmComm : '（DB:無）')});
        }
        var reasonHtml = '';
        if (reasonLines.length) {
          reasonHtml = '<div style="margin-top:6px;padding:5px 7px;background:rgba(128,128,128,0.06);border-radius:6px;">'
            + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:2px;">比對說明</div>'
            + reasonLines.map(function(r){
                var icon = r.ok === true ? '✓' : r.ok === false ? '✗' : '—';
                var color = r.ok === true ? 'var(--ok)' : r.ok === false ? 'var(--err)' : 'var(--txm)';
                return '<div style="font-size:11px;color:' + color + ';margin-top:1px;">' + icon + ' ' + r.txt + '</div>';
              }).join('') + '</div>';
        }
        rightCol = '<div style="flex:1;min-width:0;padding-left:12px;border-left:1px solid var(--bd);">'
          + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;display:flex;justify-content:space-between;align-items:center;gap:6px;">'
          + '<span>FIRESTORE 近似候選（分數 ' + item.nm_score + '）</span>'
          + (item.nm_seq ? '<button type="button" onclick="cpOpenDetail(' + JSON.stringify(String(item.nm_doc_id || '')).replace(/"/g, '&quot;') + ')" title="點下開啟物件詳情 modal" style="background:var(--bg-h);color:var(--ac);padding:1px 7px;border-radius:9px;font-weight:700;border:none;cursor:pointer;font-family:inherit;">序號 #' + item.nm_seq + ' →</button>' : '')
          + '</div>'
          + '<div style="font-size:13px;color:var(--tx);font-weight:600;' + (item.nm_name!==item.csv_name?'color:var(--warn);':'') + '">' + item.nm_name + '</div>'
          + fmtR('售價', item.nm_price!=null ? item.nm_price+' 萬' : '')
          + fmtR('地址', item.nm_addr)
          + fmtR('地坪', item.nm_land!=null ? item.nm_land+' 坪' : '')
          + fmtR('建坪', item.nm_build!=null ? item.nm_build+' 坪' : '')
          + fmtR('室內坪', item.nm_interior!=null ? item.nm_interior+' 坪' : '')
          + fmtR('序號', item.nm_seq)
          + fmtR('到期', item.nm_expiry)
          + fmtR('經紀人', item.nm_agent)
          + reasonHtml
          + '</div>';
        buttons = '<div style="margin-top:8px;display:flex;gap:8px;">'
          + '<button onclick="rvAcceptUnmatched(this)" data-cardid="' + cardId + '"'
          + ' data-item="' + nmItemJson + '"'
          + ' style="padding:4px 12px;border-radius:7px;background:var(--ok);color:#fff;border:none;font-size:12px;font-weight:700;cursor:pointer;">✅ 是同一物件</button>'
          + '<button onclick="rvSkipUnmatched(this)" data-cardid="' + cardId + '"'
          + ' style="padding:4px 12px;border-radius:7px;background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);font-size:12px;cursor:pointer;">— 略過</button>'
          + '</div>';
      } else {
        rightCol = '<div style="flex:1;min-width:0;padding-left:12px;border-left:1px solid var(--bd);">'
          + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;">FIRESTORE</div>'
          + '<div style="font-size:12px;color:var(--txm);margin-top:8px;">❓ 未找到近似物件</div>'
          + '<div style="font-size:11px;color:var(--txs);margin-top:4px;">新物件，需先匯入 Sheets</div>'
          + '</div>';
      }
      // 強行記憶列（底部輸入框，讓使用者手動填入已知的 Firestore 序號）
      var forceRow = '<div class="rv-force-row" style="margin-top:8px;display:flex;align-items:center;gap:6px;border-top:1px dashed var(--bd);padding-top:6px;">'
        + '<input class="rv-force-seq-input" placeholder="輸入 Firestore 序號（強行記憶配對）"'
        + ' style="flex:1;max-width:200px;padding:3px 7px;border:1px solid var(--bd);border-radius:5px;font-size:11px;" />'
        + '<button onclick="rvForceMatch(this)" data-cardid="' + cardId + '"'
        + ' style="padding:4px 10px;border-radius:7px;background:#888;color:#fff;border:none;font-size:11px;cursor:pointer;">💾 強行記憶</button>'
        + '</div>';
      var div = document.createElement('div');
      div.id = cardId;
      div.dataset.csvName = item.csv_name || '';
      div.dataset.csvComm = item.csv_comm || '';
      div.style.cssText = 'border:1px solid var(--bd);border-radius:10px;padding:12px 14px;';
      div.innerHTML = '<div style="display:flex;gap:0;">' + leftCol + rightCol + '</div>' + buttons + forceRow;
      issueList.appendChild(div);
    });

    rvLoadMemories();   // 載入強行配對記憶抽屜
    rvTab('high');
    _rvUpdateCount();
  }

  // ── 強行配對記憶 ─────────────────────────────────────────────────────────

  // 展開/收起記憶抽屜
  function rvToggleMemDrawer() {
    var body = document.getElementById('rv-mem-body');
    var arrow = document.getElementById('rv-mem-arrow');
    if (!body) return;
    var open = body.style.display !== 'none';
    body.style.display = open ? 'none' : 'block';
    if (arrow) arrow.textContent = open ? '▼ 展開' : '▲ 收起';
  }

  // 載入並渲染所有強行配對記憶
  function rvLoadMemories() {
    fetch('/api/word-match-memory')
      .then(function(r){ return r.json(); })
      .then(function(data) {
        var mems = data.items || [];
        var badge = document.getElementById('rv-mem-badge');
        if (badge) badge.textContent = mems.length;
        var tbody = document.getElementById('rv-mem-tbody');
        if (!tbody) return;
        tbody.innerHTML = '';
        if (!mems.length) {
          tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--txs);padding:10px;">（尚無強行配對記憶）</td></tr>';
          return;
        }
        mems.forEach(function(m) {
          var tr = document.createElement('tr');
          tr.innerHTML = '<td style="padding:4px 6px;">' + escapeHtml(m.word_comm || '—') + '</td>'
            + '<td style="padding:4px 6px;">' + escapeHtml(m.word_name || '—') + '</td>'
            + '<td style="padding:4px 6px;font-weight:700;color:var(--ac);">序號 ' + escapeHtml(String(m.db_seq || '')) + '</td>'
            + '<td style="padding:4px 6px;color:var(--txs);">' + escapeHtml(m.memo || '') + '</td>'
            + '<td style="padding:4px 6px;"><button onclick="rvDeleteMemory(this)" data-memid="' + m._id + '"'
            + ' style="padding:2px 7px;border-radius:4px;background:var(--err,#e55);color:#fff;border:none;font-size:11px;cursor:pointer;">🗑 刪除</button></td>';
          tbody.appendChild(tr);
        });
      }).catch(function(){});
  }

  // 刪除一筆強行配對記憶
  function rvDeleteMemory(btn) {
    var memId = btn.dataset.memid;
    if (!confirm('確定刪除這筆強行記憶？刪除後下次上傳將不再自動配對。')) return;
    fetch('/api/word-match-memory/' + memId, { method: 'DELETE' })
      .then(function(r){ return r.json(); })
      .then(function(){ rvLoadMemories(); toast('已刪除強行記憶', 'success'); })
      .catch(function(){ toast('刪除失敗', 'error'); });
  }

  // 強行記憶配對：儲存記憶到 Firestore，下次上傳自動套用
  function rvForceMatch(btn) {
    var cardId = btn.dataset.cardid;
    var card = document.getElementById(cardId);
    var inp  = card ? card.querySelector('.rv-force-seq-input') : null;
    var seq  = inp ? inp.value.trim() : '';
    if (!seq) { toast('請輸入 Firestore 資料序號', 'warn'); return; }
    var wordName = card ? (card.dataset.csvName || '') : '';
    var wordComm = card ? (card.dataset.csvComm || '') : '';
    if (!wordName) { toast('找不到 Word 案名，無法記憶', 'warn'); return; }
    btn.disabled = true;
    btn.textContent = '儲存中…';
    fetch('/api/word-match-memory', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ word_name: wordName, word_comm: wordComm, db_seq: seq, memo: '' })
    }).then(function(r){ return r.json(); })
      .then(function(data) {
        btn.disabled = false;
        btn.textContent = '💾 強行記憶';
        if (data.error) { toast('❌ ' + data.error, 'error'); return; }
        toast('✅ 已記憶（序號 ' + seq + '），下次上傳自動配對', 'success');
        rvLoadMemories();
        // 更新卡片的強行記憶列，顯示已儲存提示
        if (card) {
          var forceRow = card.querySelector('.rv-force-row');
          if (forceRow) {
            forceRow.innerHTML = '<span style="font-size:11px;color:var(--ok,green);font-weight:600;">'
              + '✅ 已記憶：序號 ' + escapeHtml(seq) + '，下次上傳自動配對</span>';
          }
        }
      }).catch(function(){
        btn.disabled = false;
        btn.textContent = '💾 強行記憶';
        toast('❌ 儲存失敗', 'error');
      });
  }

  // 切換 Tab
  function rvTab(name) {
    ['high','medium','issues'].forEach(function(t) {
      document.getElementById('rv-pane-' + t).style.display = (t===name) ? 'block' : 'none';
      var btn = document.getElementById('rv-tab-' + t + '-btn');
      if (t===name) {
        btn.style.borderBottomColor = 'var(--ac)';
        btn.style.color = 'var(--ac)';
      } else {
        btn.style.borderBottomColor = 'transparent';
        btn.style.color = 'var(--txm)';
      }
    });
  }

  // 🔄 在審查 modal 內觸發「同步 Sheets」— 關閉審查 + 呼叫 cpTriggerSync。
  // 同步完成後使用者要自己重新跑比對審查（同步約 1-2 分鐘，期間不能保留現有審查結果）
  function rvTriggerSyncFromReview() {
    if (!confirm('將執行「🔄 同步 Sheets」（主 Sheets → Firestore，約 1-2 分鐘）。\\n\\n執行後請：\\n1. 等同步完成（右上角會 toast 提示）\\n2. 重新點「🔍 比對審查」上傳同一個 Word 檔再跑一次\\n\\n（目前審查結果會關閉，未確認的配對不會保留）\\n\\n確認執行？')) return;
    cpCloseReview();
    // 跳到頂部，讓使用者看得到主同步按鈕的狀態變化
    window.scrollTo({ top: 0, behavior: 'smooth' });
    cpTriggerSync();
  }

  // 🤖 用 Gemini 對問題組重新配對
  function rvRunAiMatch() {
    var items = (_rvData.conflict || []).concat(_rvData.unmatched || []);
    if (!items.length) { toast('沒有問題項目需要 AI 重新配對', 'info'); return; }
    if (items.length > 100) { toast('問題項目超過 100 筆，請先處理一部分', 'error'); return; }

    var btn = document.getElementById('rv-ai-match-btn');
    var resultDiv = document.getElementById('rv-ai-result');
    btn.disabled = true;
    btn.textContent = '🤖 AI 比對中…';
    resultDiv.style.display = 'block';
    resultDiv.innerHTML = '<p style="margin:0;font-size:12px;color:var(--txs);">⏳ Gemini 處理中（每筆約 1-2 秒），共 ' + items.length + ' 筆…</p>';

    fetch('/api/word-review/ai-match', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: items })
    })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      btn.disabled = false;
      btn.textContent = '🤖 重新跑一次';
      if (d.error) {
        resultDiv.innerHTML = '<p style="margin:0;font-size:12px;color:#f87171;">❌ ' + d.error + '</p>';
        return;
      }
      _rvAiResults = d.results || [];
      rvRenderAiResults(items, _rvAiResults);
    })
    .catch(function(e) {
      btn.disabled = false;
      btn.textContent = '🤖 重新跑一次';
      resultDiv.innerHTML = '<p style="margin:0;font-size:12px;color:#f87171;">❌ 失敗：' + e.message + '</p>';
    });
  }

  var _rvAiResults = [];
  var _rvAiMediumResults = [];

  // 🤖 用 Gemini 驗證中信心組（規則找到的配對是否正確）
  function rvRunAiMatchMedium() {
    var items = _rvData.medium || [];
    if (!items.length) { toast('沒有中信心項目需要 AI 驗證', 'info'); return; }
    if (items.length > 100) { toast('中信心項目超過 100 筆，請先處理一部分', 'error'); return; }

    var btn = document.getElementById('rv-ai-medium-btn');
    var resultDiv = document.getElementById('rv-ai-medium-result');
    btn.disabled = true;
    btn.textContent = '🤖 AI 驗證中…';
    resultDiv.style.display = 'block';
    resultDiv.innerHTML = '<p style="margin:0;font-size:12px;color:var(--txs);">⏳ Gemini 處理中（每筆約 1-2 秒），共 ' + items.length + ' 筆…</p>';

    fetch('/api/word-review/ai-match', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items: items })
    })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      btn.disabled = false;
      btn.textContent = '🤖 重新驗證';
      if (d.error) {
        resultDiv.innerHTML = '<p style="margin:0;font-size:12px;color:#f87171;">❌ ' + d.error + '</p>';
        return;
      }
      _rvAiMediumResults = d.results || [];
      rvRenderAiMediumResults(items, _rvAiMediumResults);
    })
    .catch(function(e) {
      btn.disabled = false;
      btn.textContent = '🤖 重新驗證';
      resultDiv.innerHTML = '<p style="margin:0;font-size:12px;color:#f87171;">❌ 失敗：' + e.message + '</p>';
    });
  }

  // 渲染中信心 AI 驗證結果（對比規則 vs AI）
  // 中信心：渲染 AI 驗證結果（注入到各卡片，頂端只放計數）
  function rvRenderAiMediumResults(items, results) {
    var div = document.getElementById('rv-ai-medium-result');
    var agree = 0, disagree = 0, no = 0;
    results.forEach(function(r, i) {
      var item = items[i] || {};
      var ruleId = item.doc_id || '';
      _rvInjectAiBadge('med-' + i, r, item, 'medium');
      if (!r.matched_doc_id || r.confidence < 0.4) no++;
      else if (r.matched_doc_id === ruleId) agree++;
      else disagree++;
    });

    div.innerHTML = '<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;font-size:12px;">'
      + '<span style="font-weight:700;color:#a78bfa;">🤖 Gemini 已標到各卡片</span>'
      + '<span style="color:var(--txs);">共 ' + results.length + ' 筆 ｜ ✅ AI 同意 ' + agree
      + ' ｜ ⚠️ AI 不同意 ' + disagree + ' ｜ ❌ AI 也找不到 ' + no + '</span>'
      + '<span style="color:var(--txm);font-size:10px;margin-left:auto;">模型：gemini-2.0-flash</span>'
      + '</div>';
  }

  // 把 AI 結果注入到對應卡片底部（badge 樣式）
  function _rvInjectAiBadge(cardId, r, item, mode) {
    var card = document.getElementById(cardId);
    if (!card) return;
    // 移除舊 badge（重新跑時更新）
    var old = card.querySelector('.rv-ai-badge');
    if (old) old.remove();

    var pct = Math.round((r.confidence || 0) * 100);
    var bg, color, label;

    if (mode === 'medium') {
      // 驗證模式：和規則建議比對
      var ruleId = item.doc_id;
      if (!r.matched_doc_id || r.confidence < 0.4) {
        bg = 'rgba(107,114,128,0.12)'; color = '#9ca3af';
        label = '🤖 AI 也找不到合適配對 — 建議跳過此筆';
      } else if (r.matched_doc_id === ruleId) {
        bg = 'rgba(34,197,94,0.12)'; color = '#16a34a';
        label = '🤖 AI 同意此配對（信心 ' + pct + '%）— 可放心套用';
      } else {
        bg = 'rgba(217,119,6,0.12)'; color = '#d97706';
        label = '🤖 AI 不同意 — 建議改配對：「' + (r.matched_db_name || '') + '」（信心 ' + pct + '%）';
      }
    } else {
      // 找配對模式：規則找不到，AI 嘗試
      if (!r.matched_doc_id || r.confidence < 0.4) {
        bg = 'rgba(107,114,128,0.12)'; color = '#9ca3af';
        label = '🤖 AI 找不到對應 — 可能是新物件';
      } else if (r.confidence >= 0.7) {
        bg = 'rgba(34,197,94,0.12)'; color = '#16a34a';
        label = '🤖 AI 建議配對：「' + (r.matched_db_name || '') + '」（信心 ' + pct + '%）';
      } else {
        bg = 'rgba(217,119,6,0.12)'; color = '#d97706';
        label = '🤖 AI 不太確定，可能是：「' + (r.matched_db_name || '') + '」（信心 ' + pct + '%）';
      }
    }

    var badge = document.createElement('div');
    badge.className = 'rv-ai-badge';
    badge.style.cssText = 'margin-top:8px;padding:7px 10px;border-radius:6px;background:' + bg + ';border-left:3px solid ' + color + ';font-size:11px;line-height:1.5;';
    var inner = '<div style="font-weight:700;color:' + color + ';">' + label + '</div>';
    if (r.matched_doc_id && r.matched_db_addr) {
      inner += '<div style="color:var(--txs);margin-top:2px;">📍 ' + r.matched_db_addr;
      if (r.matched_db_agent) inner += '　／ 經紀人：' + r.matched_db_agent;
      if (r.matched_db_price) inner += '　／ ' + r.matched_db_price + '萬';
      inner += '</div>';
    }
    if (r.reason) inner += '<div style="color:var(--txm);margin-top:2px;font-style:italic;">💬 ' + r.reason + '</div>';
    badge.innerHTML = inner;
    card.appendChild(badge);
  }

  // 問題組：渲染 AI 配對結果（注入到各卡片，頂端只放計數）
  function rvRenderAiResults(items, results) {
    var div = document.getElementById('rv-ai-result');
    var hi = 0, mid = 0, no = 0;
    var conflictLen = (_rvData.conflict || []).length;

    results.forEach(function(r, i) {
      var item = items[i] || {};
      // 決定卡片 id：前 conflictLen 筆是 conflict（id=conf-i），後面是 unmatched（id=unm-(i-conflictLen)）
      var cardId = (i < conflictLen) ? ('conf-' + i) : ('unm-' + (i - conflictLen));
      _rvInjectAiBadge(cardId, r, item, 'issues');
      if (r.matched_doc_id && r.confidence >= 0.7) hi++;
      else if (r.matched_doc_id && r.confidence >= 0.4) mid++;
      else no++;
    });

    div.innerHTML = '<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;font-size:12px;">'
      + '<span style="font-weight:700;color:#a78bfa;">🤖 Gemini 已標到各卡片</span>'
      + '<span style="color:var(--txs);">共 ' + results.length + ' 筆 ｜ ✅ 高信心 ' + hi
      + ' ｜ ⚠️ 不確定 ' + mid + ' ｜ ❌ 找不到 ' + no + '</span>'
      + '<span style="color:var(--txm);font-size:10px;margin-left:auto;">模型：gemini-2.0-flash</span>'
      + '</div>';
  }

  // 勾選/取消全選（高信心）
  function rvToggleAll(cb) {
    document.querySelectorAll('#rv-high-list input[type=checkbox]').forEach(function(el) {
      el.checked = cb.checked;
      var did = el.dataset.docid;
      if (cb.checked) {
        // 找回 data
        var item = _rvData.high.find(function(x){ return x.doc_id === did; });
        if (item) _rvConfirmed[did] = {doc_id:did, price:item.csv_price, old_price:item.db_price, expiry:item.csv_expiry, name_changed:item.name_changed, old_name:item.db_name, new_name:item.csv_name};
      } else {
        delete _rvConfirmed[did];
      }
    });
    _rvUpdateCount();
  }

  // 單一高信心勾選切換
  function rvToggleHigh(cb) {
    var did = cb.dataset.docid;
    if (cb.checked) {
      var item = _rvData.high.find(function(x){ return x.doc_id === did; });
      if (item) _rvConfirmed[did] = {doc_id:did, price:item.csv_price, old_price:item.db_price, expiry:item.csv_expiry, name_changed:item.name_changed, old_name:item.db_name, new_name:item.csv_name};
    } else {
      delete _rvConfirmed[did];
    }
    _rvUpdateCount();
  }

  // 中信心：確認一筆（再按一次取消，可回復）
  // 點 Word 行號徽章：複製案名到剪貼簿，方便去 Word 文件 Cmd+F 搜尋
  function rvCopyName(btn, name) {
    if (!name) { toast('沒有案名可複製', 'error'); return; }
    var done = function() {
      var orig = btn.innerHTML;
      btn.innerHTML = '✓ 已複製';
      setTimeout(function(){ btn.innerHTML = orig; }, 1500);
      toast('已複製「' + name + '」，可貼到 Word 用 Cmd+F 搜尋', 'info');
    };
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(name).then(done, function(){ done(); });
      } else {
        var ta = document.createElement('textarea');
        ta.value = name; document.body.appendChild(ta);
        ta.select(); document.execCommand('copy'); document.body.removeChild(ta);
        done();
      }
    } catch (e) { toast('複製失敗：' + e, 'error'); }
  }

  function rvAcceptMedium(btn) {
    var docId = btn.dataset.docid;
    if (btn.dataset.state === 'confirmed') {
      // 已確認 → 取消（回復原狀）
      delete _rvConfirmed[docId];
      btn.textContent = '✅ 確認配對';
      btn.dataset.state = '';
      btn.style.opacity = '';
      btn.style.background = 'var(--ok)';
      if (btn.nextElementSibling) btn.nextElementSibling.style.display = '';
      _rvUpdateCount();
      return;
    }
    // 確認
    try {
      var data = JSON.parse(btn.dataset.item);
      _rvConfirmed[docId] = data;
    } catch(e) {
      _rvConfirmed[docId] = {doc_id: docId};
    }
    btn.textContent = '↩️ 取消確認';
    btn.dataset.state = 'confirmed';
    btn.style.opacity = '0.85';
    btn.style.background = 'var(--bg-h)';
    btn.style.color = 'var(--ok)';
    if (btn.nextElementSibling) btn.nextElementSibling.style.display = 'none';
    _rvUpdateCount();
  }

  // 中信心：跳過一筆（再按一次取消，可回復）
  function rvSkipMedium(btn) {
    if (btn.dataset.state === 'skipped') {
      btn.textContent = '❌ 跳過';
      btn.dataset.state = '';
      btn.style.opacity = '';
      if (btn.previousElementSibling) btn.previousElementSibling.style.display = '';
      return;
    }
    btn.textContent = '↩️ 取消跳過';
    btn.dataset.state = 'skipped';
    btn.style.opacity = '0.7';
    if (btn.previousElementSibling) btn.previousElementSibling.style.display = 'none';
  }

  // 問題：確認近似候選是同一物件（再按一次取消，可回復）
  function rvAcceptUnmatched(btn) {
    var cardId = btn.getAttribute('data-cardid');
    var card = cardId ? document.getElementById(cardId) : null;
    if (btn.dataset.state === 'confirmed') {
      // 取消確認
      try {
        var raw0 = btn.getAttribute('data-item').replace(/&quot;/g, '"');
        var it0 = JSON.parse(raw0);
        delete _rvConfirmed[it0.doc_id];
      } catch(e) {}
      btn.textContent = '✅ 是同一物件';
      btn.dataset.state = '';
      btn.style.opacity = '';
      if (card) { card.style.opacity = ''; card.style.borderColor = ''; }
      if (btn.nextElementSibling) btn.nextElementSibling.style.display = '';
      _rvUpdateCount();
      return;
    }
    // 確認
    var raw = btn.getAttribute('data-item').replace(/&quot;/g, '"');
    var item = JSON.parse(raw);
    _rvConfirmed[item.doc_id] = item;
    _rvUpdateCount();
    if (card) { card.style.opacity = '0.5'; card.style.borderColor = 'var(--ok)'; }
    btn.textContent = '↩️ 取消確認';
    btn.dataset.state = 'confirmed';
    btn.style.opacity = '0.85';
    if (btn.nextElementSibling) btn.nextElementSibling.style.display = 'none';
  }

  // 問題：略過此筆（再按一次取消，可回復）
  function rvSkipUnmatched(btn) {
    var cardId = btn.getAttribute('data-cardid');
    var card = cardId ? document.getElementById(cardId) : null;
    if (btn.dataset.state === 'skipped') {
      btn.textContent = '— 略過';
      btn.dataset.state = '';
      btn.style.opacity = '';
      if (card) card.style.opacity = '';
      if (btn.previousElementSibling) btn.previousElementSibling.style.display = '';
      return;
    }
    if (card) card.style.opacity = '0.4';
    btn.textContent = '↩️ 取消跳過';
    btn.dataset.state = 'skipped';
    btn.style.opacity = '0.7';
    if (btn.previousElementSibling) btn.previousElementSibling.style.display = 'none';
  }

  // 更新底部「已選 N 筆」
  function _rvUpdateCount() {
    var n = Object.keys(_rvConfirmed).length;
    document.getElementById('rv-apply-count').textContent = '已選 ' + n + ' 筆';
  }

  // 確認退出審查 Modal（有已選配對時提示）
  function cpCloseReview() {
    var n = Object.keys(_rvConfirmed).length;
    if (n > 0 && !confirm('已選取 ' + n + ' 筆配對尚未套用，確定要離開？')) return;
    document.getElementById('cp-review-modal').style.display = 'none';
  }

  // 🧠 配對記憶：套用成功後若有手動確認項目，跳 Modal 問是否記住
  var _rvMemCandidates = [];
  function rvShowMemoryModal(items) {
    _rvMemCandidates = items;
    var listEl = document.getElementById('rv-mem-list');
    var html = '';
    items.forEach(function(it, i) {
      var srcLabel = it._mem_source === 'medium' ? '⚠️ 中信心' : '❓ 問題';
      var srcColor = it._mem_source === 'medium' ? 'var(--warn)' : '#a78bfa';
      html += '<label style="display:flex;align-items:flex-start;gap:8px;padding:8px 10px;border:1px solid var(--bd);border-radius:8px;background:var(--bg-t);cursor:pointer;">';
      html += '<input type="checkbox" class="rv-mem-cb" data-idx="' + i + '" checked onchange="_rvMemUpdateCount()" style="margin-top:3px;">';
      html += '<div style="flex:1;font-size:12px;">';
      html += '<div style="display:flex;align-items:center;gap:6px;margin-bottom:3px;">';
      html += '<span style="background:' + srcColor + ';color:#fff;padding:1px 6px;border-radius:4px;font-size:10px;font-weight:700;">' + srcLabel + '</span>';
      html += '<strong style="color:var(--tx);">' + (it._mem_word_name || '(無案名)') + '</strong>';
      if (it._mem_word_comm) html += '<span style="color:var(--txm);font-size:11px;">委託：' + it._mem_word_comm + '</span>';
      html += '</div>';
      html += '<div style="color:var(--txs);font-size:11px;padding-left:2px;">↳ Firestore 序號：<strong>' + it._mem_db_seq + '</strong>';
      if (it.old_name && it.old_name !== it._mem_word_name) {
        html += '　/ 物件原案名：' + it.old_name;
      }
      html += '</div></div></label>';
    });
    listEl.innerHTML = html;
    _rvMemUpdateCount();
    document.getElementById('rv-memory-modal').style.display = 'flex';
  }

  function _rvMemUpdateCount() {
    var n = document.querySelectorAll('#rv-mem-list .rv-mem-cb:checked').length;
    document.getElementById('rv-mem-count').textContent = '已選 ' + n + ' 筆';
  }

  function rvMemToggleAll(master) {
    document.querySelectorAll('#rv-mem-list .rv-mem-cb').forEach(function(cb){ cb.checked = master.checked; });
    _rvMemUpdateCount();
  }

  function rvMemSkip() {
    document.getElementById('rv-memory-modal').style.display = 'none';
    document.getElementById('cp-review-modal').style.display = 'none';
    _rvMemCandidates = [];
    setTimeout(function(){ cpFetch(); }, 600);
  }

  function rvMemSave() {
    var checkedIdx = Array.from(document.querySelectorAll('#rv-mem-list .rv-mem-cb:checked'))
      .map(function(cb){ return parseInt(cb.dataset.idx, 10); });
    if (!checkedIdx.length) {
      toast('未勾選任何項目，未記住任何配對', 'info');
      rvMemSkip();
      return;
    }
    var saveBtn = document.querySelector('#rv-memory-modal button[onclick="rvMemSave()"]');
    if (saveBtn) { saveBtn.disabled = true; saveBtn.textContent = '寫入中…'; }

    // 平行 POST 各筆配對到 /api/word-match-memory
    var promises = checkedIdx.map(function(i) {
      var it = _rvMemCandidates[i];
      return fetch('/api/word-match-memory', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          word_name: it._mem_word_name,
          word_comm: it._mem_word_comm,
          db_seq:    it._mem_db_seq,
          db_doc_id: it.doc_id || '',
          memo:      '從比對審查 ' + (it._mem_source === 'medium' ? '中信心' : '問題') + ' 確認',
        }),
      }).then(function(r){ return r.json(); });
    });
    Promise.all(promises).then(function(results) {
      var ok = results.filter(function(r){ return r.ok; }).length;
      var fail = results.length - ok;
      if (fail === 0) {
        toast('🧠 已記住 ' + ok + ' 筆配對，下次自動套用', 'success');
      } else {
        toast('🧠 記住 ' + ok + ' 筆，' + fail + ' 筆失敗', 'warn');
      }
      rvMemSkip();
    }).catch(function(){
      toast('❌ 記憶寫入失敗，請稍後再試', 'error');
      if (saveBtn) { saveBtn.disabled = false; saveBtn.textContent = '🧠 記住勾選的配對'; }
    });
  }

  // 套用確認的配對，寫入 Firestore
  function cpApplyReview() {
    var items = Object.values(_rvConfirmed);
    if (!items.length) { toast('請先勾選要套用的物件', 'warn'); return; }
    if (!confirm('確定要套用 ' + items.length + ' 筆配對結果，寫入 Firestore？')) return;

    var btn = document.querySelector('#cp-review-modal [onclick="cpApplyReview()"]');
    if (btn) { btn.disabled = true; btn.textContent = '寫入中…'; }

    fetch('/api/word-review/apply', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({items: items}),
    })
    .then(function(r){ return r.json(); })
    .then(function(d) {
      if (d.error) {
        toast('❌ ' + d.error, 'error');
        if (btn) { btn.disabled=false; btn.textContent='✅ 套用確認的配對'; }
        return;
      }
      toast('✅ 已更新 ' + d.updated + ' 筆物件（銷售中、售價、到期日）', 'success');
      // 上傳 word_meta.json（若有的話）
      _rvMetaFiles.forEach(function(jf){ _rvUploadMeta(jf); });
      _rvMetaFiles = [];
      // 收集手動確認的配對（中信心 + 問題組）→ 詢問是否記住
      var memCandidates = items.filter(function(it){
        return it && it._mem_source && it._mem_word_name && it._mem_db_seq;
      });
      if (memCandidates.length > 0) {
        // 顯示記憶確認 modal，使用者選完才關閉審查 modal
        rvShowMemoryModal(memCandidates);
      } else {
        document.getElementById('cp-review-modal').style.display = 'none';
        setTimeout(function(){ cpFetch(); }, 600);
      }
    })
    .catch(function(){ toast('❌ 套用失敗，請稍後再試', 'error'); });
  }

  // 上傳 word_meta.json（總表日期）
  function _rvUploadMeta(jf) {
    var reader = new FileReader();
    reader.onload = function(e) {
      try {
        var meta = JSON.parse(e.target.result);
        fetch('/api/word-snapshot/meta', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(meta),
        }).then(function(r){ return r.json(); }).then(function(d) {
          if (d.ok) {
            var el = document.getElementById('cp-doc-date');
            if (el) el.textContent = '📄 總表：' + d.minguo;
            toast('✅ 物件總表日期已更新：' + d.minguo, 'success');
          }
        }).catch(function(){});
      } catch(e) {}
    };
    reader.readAsText(jf, 'utf-8');
  }
  // ────────────────────────────────────────────────────────────────────

  // 📤 推送單筆物件到 home-start（觸發對外網站立刻拉最新資料）
  function cpPushToHomeStart(pid, btn) {
    if (!pid) return;
    var orig = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '⏳ 推送中…';
    fetch('/api/company-properties/' + encodeURIComponent(pid) + '/push-to-home-start', { method: 'POST' })
      .then(function(r){ return r.json(); })
      .then(function(d){
        if (d.ok) {
          btn.innerHTML = '✅ 已推送';
          setTimeout(function(){ btn.innerHTML = orig; btn.disabled = false; }, 2000);
        } else {
          alert('推送失敗：' + (d.error || '未知錯誤'));
          btn.innerHTML = orig; btn.disabled = false;
        }
      })
      .catch(function(e){
        alert('推送失敗：' + (e.message || e));
        btn.innerHTML = orig; btn.disabled = false;
      });
  }

  function cpFetch() {
    var list = document.getElementById('cp-list');
    list.innerHTML = '<p class="text-center py-8" style="color:var(--txs);">載入中…</p>';
    document.getElementById('cp-placeholder').classList.add('hidden');

    // 判斷是否需要全載（不分頁）：
    // 排序已由後端處理，只有「到期日篩選」或「星號篩選」需要全載（這兩個是前端過濾）
    var expiryFilterVal = (document.getElementById('cp-expiry') || {}).value || '';
    var starFilterActive = (document.getElementById('cp-star-filter-btn') || {}).dataset && document.getElementById('cp-star-filter-btn').dataset.active === '1';
    var needAllPages = !!expiryFilterVal || !!starFilterActive;

    var q = Object.assign({}, _cpLastQuery, { page: _cpPage });
    if (needAllPages) q.page = 1;   // 從第1頁開始
    var params = new URLSearchParams();
    Object.entries(q).forEach(function([k, v]) { if (v !== '') params.set(k, v); });

    // 一次撈全部的函數（大批量翻頁，每次 500 筆）
    function fetchAll(page, accumulated) {
      var p2 = new URLSearchParams(params);
      p2.set('page', page);
      p2.set('per_page', '500');  // 一次多拿，減少翻頁次數
      return fetch('/api/company-properties/search?' + p2.toString())
        .then(function(r) { return r.json(); })
        .then(function(data) {
          if (data.error) return Promise.reject(data);  // 傳整個 data object，保留 need_org 屬性
          var all = accumulated.concat(data.items || []);
          if (page < (data.pages || 1)) {
            return fetchAll(page + 1, all);
          }
          return { items: all, total: data.total, allLoaded: true, home_start_url: data.home_start_url };
        });
    }

    var fetchPromise = needAllPages
      ? fetchAll(1, [])
      : fetch('/api/company-properties/search?' + params.toString()).then(function(r){ return r.json(); });

    fetchPromise.then(function(data) {
        if (data.error) {
          if (data.need_org) {
            // 尚未加入組織 → 顯示友善說明，而非紅色錯誤
            list.innerHTML = '<div style="text-align:center;padding:40px 20px;color:var(--txm);">'
              + '<div style="font-size:2.5rem;margin-bottom:12px;">🏢</div>'
              + '<p style="font-size:1rem;font-weight:600;color:var(--tx);margin-bottom:8px;">公司物件庫需要加入組織才能使用</p>'
              + '<p style="font-size:0.875rem;">' + escapeHtml(data.error) + '</p>'
              + '</div>';
          } else {
            list.innerHTML = '<p class="text-red-400 text-center py-8">' + escapeHtml(data.error) + '</p>';
          }
          document.getElementById('cp-info').classList.add('hidden');
          document.getElementById('cp-pagination').classList.add('hidden');
          return;
        }
        var items = data.items || [];
        // home-start 公開站台 URL（給卡片組「在起家上看」連結用）
        var homeStartUrl = (data.home_start_url || '').replace(/\/$/, '');
        var hsLandCats = {'農地':1, '建地':1, '道路用地':1};
        if (!items.length) {
          list.innerHTML = '<p class="text-center py-10" style="color:var(--txm);">找不到符合條件的物件</p>';
          document.getElementById('cp-info').classList.add('hidden');
          document.getElementById('cp-pagination').classList.add('hidden');
          return;
        }

        // 前端：星號篩選（只顯示已加星物件）
        var starBtn = document.getElementById('cp-star-filter-btn');
        if (starBtn && starBtn.dataset.active === '1') {
          items = items.filter(function(item) { return !!item['已加星']; });
        }

        // 前端：委託到期日篩選
        var expiryFilter = (document.getElementById('cp-expiry') || {}).value || '';
        if (expiryFilter) {
          var today = new Date(); today.setHours(0,0,0,0);
          var soon15 = new Date(today); soon15.setDate(soon15.getDate() + 15);
          items = items.filter(function(item) {
            var expStr = item['委託到期日'] || '';
            if (!expStr) return expiryFilter === 'empty';
            // 解析民國日期「115年6月30日」或西元「2026/06/30」
            var expDate = null;
            var m = String(expStr).match(/([0-9]+)[ \t]*年[ \t]*([0-9]+)[ \t]*月[ \t]*([0-9]+)[ \t]*日/);
            if (m) {
              var yr = parseInt(m[1]) + (parseInt(m[1]) < 1000 ? 1911 : 0);
              expDate = new Date(yr, parseInt(m[2])-1, parseInt(m[3]));
            } else {
              var d = new Date(expStr);
              if (!isNaN(d)) expDate = d;
            }
            if (!expDate) return expiryFilter === 'empty';
            expDate.setHours(0,0,0,0);
            if (expiryFilter === 'active')  return expDate >= today;
            if (expiryFilter === 'soon')    return expDate >= today && expDate <= soon15;
            if (expiryFilter === 'expired') return expDate < today;
            return true;
          });
        }

        // 前端排序：後端已依 sort 參數排好，前端只需把「已加星」置頂（穩定排序，維持後端順序）
        items.sort(function(a, b) {
          var aStarred = a['已加星'] ? 0 : 1;
          var bStarred = b['已加星'] ? 0 : 1;
          return aStarred - bStarred;  // 相同時回傳 0，保留後端順序
        });

        // 更新資訊列與分頁
        var pg = document.getElementById('cp-pagination');
        if (needAllPages) {
          // 全部載入模式：顯示篩選後筆數，隱藏分頁按鈕
          document.getElementById('cp-total').textContent = items.length;
          document.getElementById('cp-page-num').textContent = 1;
          document.getElementById('cp-total-pages').textContent = 1;
          document.getElementById('cp-info').classList.remove('hidden');
          pg.classList.add('hidden');
        } else {
          document.getElementById('cp-total').textContent = data.total;
          document.getElementById('cp-page-num').textContent = data.page;
          document.getElementById('cp-total-pages').textContent = data.pages;
          document.getElementById('cp-info').classList.remove('hidden');
          pg.classList.remove('hidden');
          document.getElementById('cp-prev').disabled = data.page <= 1;
          document.getElementById('cp-next').disabled = data.page >= data.pages;
        }

        // 計算委託到期日剩餘天數
        function calcDaysLeft(dateStr) {
          if (!dateStr) return null;
          // 支援「115年6月30日」民國格式
          var m = String(dateStr).match(/([0-9]+)[ \t]*年[ \t]*([0-9]+)[ \t]*月[ \t]*([0-9]+)[ \t]*日/);
          var d;
          if (m) {
            var year = parseInt(m[1]) + (parseInt(m[1]) < 1000 ? 1911 : 0);
            d = new Date(year, parseInt(m[2])-1, parseInt(m[3]));
          } else {
            d = new Date(dateStr);
          }
          if (isNaN(d)) return null;
          var now = new Date(); now.setHours(0,0,0,0);
          return Math.round((d - now) / 86400000);
        }

        // 渲染列表
        var html = '';
        for (var i = 0; i < items.length; i++) {
          var item = items[i];
          // 統一轉布林（後端已處理，前端再做一層防護）
          var sellingRaw = item['銷售中'];
          var selling = (sellingRaw === true || sellingRaw === 'true' || sellingRaw === '銷售中' || sellingRaw === 'True' || sellingRaw === 1)
                        ? true
                        : (sellingRaw === false || sellingRaw === 'false' || sellingRaw === '已下架' || sellingRaw === '已成交' || sellingRaw === 'False' || sellingRaw === 0)
                        ? false : true;
          var dealDate = item['成交日期'] || '';
          var hasDeal = !!dealDate;
          var statusBadge;
          if (selling === false && hasDeal) {
            // 格式化成交日期為民國年（若為西元格式 2025/06/01 → 114年6月1日）
            var dealLabel = dealDate;
            var dm = String(dealDate).match(/([0-9]{4})[/\x2D]([0-9]{1,2})[/\x2D]([0-9]{1,2})/);
            if (dm) {
              var roc = parseInt(dm[1]) - 1911;
              dealLabel = roc + '年' + parseInt(dm[2]) + '月' + parseInt(dm[3]) + '日';
            }
            statusBadge = '<span style="font-size:0.75rem;background:var(--tg);color:var(--tgt);padding:0.125rem 0.5rem;border-radius:9999px;white-space:nowrap;">✅ 已成交：' + escapeHtml(dealLabel) + '</span>';
          } else if (selling === false && !hasDeal) {
            statusBadge = '<span style="font-size:0.75rem;background:var(--bg-h);color:var(--txs);padding:0.125rem 0.5rem;border-radius:9999px;">已下架</span>';
          } else {
            statusBadge = '<span style="font-size:0.75rem;background:var(--ok);color:#fff;padding:0.125rem 0.5rem;border-radius:9999px;">銷售中</span>';
          }
          // 售價對比：從 Word snapshot 找最新售價
          var dbPrice = item['售價(萬)'];
          var normKey = _normName(item['案名']);
          var wordHit = _cpWordPrices[normKey];
          // 也嘗試用委託號碼比對
          if (!wordHit && item['委託編號']) {
            for (var wk in _cpWordPrices) {
              if (_cpWordPrices[wk]['委託號碼'] === String(item['委託編號'])) {
                wordHit = _cpWordPrices[wk]; break;
              }
            }
          }
          var price;
          if (wordHit && wordHit['售價萬'] && String(wordHit['售價萬']) !== String(dbPrice)) {
            // 有新售價且不同 → 顯示對比（刪除線舊價 + 黃色新價）
            price = '<span style="text-decoration:line-through;color:var(--txm);font-size:0.75rem;">' + escapeHtml(String(dbPrice||'-')) + '萬</span>'
                  + ' <span style="color:var(--warn);font-weight:bold;">' + escapeHtml(String(wordHit['售價萬'])) + '萬</span>'
                  + '<span style="font-size:0.75rem;color:var(--warn);margin-left:0.125rem;">↑Word</span>';
          } else {
            price = dbPrice ? dbPrice + ' 萬' : '-';
          }
          var buildPing = item['建坪'] ? item['建坪'] + ' 坪' : (item['地坪'] ? item['地坪'] + ' 坪地' : '');
          var cat = item['物件類別'] ? '<span style="font-size:0.75rem;color:var(--warn);">' + escapeHtml(item['物件類別']) + '</span>' : '';
          var agent = item['經紀人'] ? '<span style="font-size:0.75rem;color:var(--txm);">' + escapeHtml(item['經紀人']) + '</span>' : '';
          var safeId = String(item.id).replace(/'/g, '');
          var name = escapeHtml(item['案名'] || '（無案名）');
          // 若有舊案名（案名曾改動），顯示「原案名：X」灰色小字
          var oldNameBadge = item['舊案名']
            ? '<span style="font-size:0.75rem;color:var(--txm);font-style:italic;">原案名：' + escapeHtml(item['舊案名']) + '</span>'
            : '';
          // 若有原售價（售價曾改動），顯示「原售價：X 萬」
          var oldPriceBadge = item['原售價(萬)']
            ? '<span style="font-size:0.75rem;color:var(--txm);font-style:italic;">原售價：' + escapeHtml(String(item['原售價(萬)'])) + ' 萬</span>'
            : '';
          // 地址顯示：有地址用地址；土地類（農地/建地）改顯示 縣市＋段別＋地號
          var addr;
          var _itemCat = item['物件類別'] || '';
          var _isLand = (_itemCat === '農地' || _itemCat === '建地' || _itemCat === '農建地');
          if (_isLand && !item['物件地址']) {
            var _locality = _AREA_MAP[item['鄉/市/鎮'] || ''] || item['鄉/市/鎮'] || '';
            var _section  = item['段別'] ? item['段別'].replace(/段$/, '') + '段' : '';
            var _landno   = item['地號'] || '';
            var _parts = [_locality, _section, _landno ? '地號 ' + _landno : ''].filter(Boolean);
            addr = escapeHtml(_parts.join(' ') || '-');
          } else {
            addr = escapeHtml(item['物件地址'] || '-');
          }

          // 委託到期日剩餘天數標示
          var expiryBadge = '';
          if (selling !== false) {  // 銷售中才顯示到期警示
            var daysLeft = calcDaysLeft(item['委託到期日']);
            if (daysLeft !== null) {
              if (daysLeft < 0) {
                expiryBadge = '<span style="font-size:0.75rem;background:var(--dgb);color:var(--dg);padding:0.125rem 0.5rem;border-radius:9999px;">⚠️ 已到期 ' + Math.abs(daysLeft) + '天</span>';
              } else if (daysLeft <= 15) {
                expiryBadge = '<span style="font-size:0.75rem;background:rgba(234,88,12,0.15);color:#f97316;padding:0.125rem 0.5rem;border-radius:9999px;" class="animate-pulse">⏰ 剩 ' + daysLeft + ' 天</span>';
              } else {
                expiryBadge = '<span style="font-size:0.75rem;color:var(--txm);">到期：剩' + daysLeft + '天</span>';
              }
            }
          }

          var starred = !!item['已加星'];
          var starIcon = starred ? '★' : '☆';
          // 星號按鈕樣式：已加星黃色，未加星灰色
          var starStyle = starred
            ? 'cp-star-btn;color:#facc15;'
            : 'cp-star-btn;color:var(--txm);';
          // 卡片邊框：已加星用黃色邊框，否則用主題邊框
          var cardBorderStyle = starred
            ? 'border:1px solid rgba(234,179,8,0.6);'
            : 'border:1px solid var(--bd);';

          html += '<div class="cp-card rounded-xl p-4 transition relative group" style="background:var(--bg-t);' + cardBorderStyle + '">';
          html += '<div class="flex items-start justify-between gap-2">';
          html += '<div class="min-w-0 cursor-pointer flex-1 cp-detail-btn" data-id="' + safeId + '">';
          html += '<p class="font-semibold truncate" style="color:var(--tx);">' + name + '</p>';
          if (oldNameBadge)  html += '<p class="truncate mt-0">' + oldNameBadge + '</p>';
          if (oldPriceBadge) html += '<p class="truncate mt-0">' + oldPriceBadge + '</p>';
          html += '<p class="truncate mt-0.5" style="font-size:0.75rem;color:var(--txs);">' + addr + '</p>';
          // 所有權人：只有管理員看得到
          if (isAdmin && item['所有權人']) {
            html += '<p class="truncate mt-0.5" style="font-size:0.75rem;color:var(--txm);">👤 ' + escapeHtml(item['所有權人']) + '</p>';
          }
          html += '</div>';
          // 右上角：星號按鈕 + 售價 + 狀態
          html += '<div class="shrink-0 text-right flex flex-col items-end gap-1">';
          html += '<button data-prop-id="' + safeId + '" class="cp-star-btn text-lg leading-none transition" style="color:' + (starred ? '#facc15' : 'var(--txm)') + ';background:none;border:none;cursor:pointer;" title="追蹤此物件">' + starIcon + '</button>';
          var priceHtml = (price.indexOf('<') >= 0) ? price : '<span style="font-weight:bold;color:var(--ac);font-size:0.875rem;">' + escapeHtml(price) + '</span>';
          html += '<p class="leading-tight" style="font-size:0.875rem;">' + priceHtml + '</p>' + statusBadge;
          html += '</div>';
          html += '</div>';
          html += '<div class="flex gap-3 mt-2 flex-wrap items-center justify-between">';
          html += '<div class="flex gap-3 flex-wrap items-center">' + cat;
          html += buildPing ? '<span style="font-size:0.75rem;color:var(--txs);">' + escapeHtml(buildPing) + '</span>' : '';
          html += agent + expiryBadge + '</div>';
          // 右下角：FOUNDI 連結 + 戰況按鈕
          html += '<div class="flex items-center gap-1">';
          // FOUNDI 連結
          var foundiUrl = _buildFoundiUrl(item);
          if (foundiUrl) {
            html += '<a href="' + foundiUrl + '" target="_blank" rel="noopener" '
                  + 'style="font-size:0.75rem;color:var(--ac);padding:0.125rem 0.5rem;border-radius:0.375rem;border:1px solid var(--bd);text-decoration:none;" '
                  + 'title="在 FOUNDI 查詢此物件" onclick="event.stopPropagation()">🔍 FOUNDI</a>';
          }
          // 帶看紀錄按鈕：跳轉到買方管理新增帶看
          if (BUYER_URL) {
            var showingUrl = BUYER_URL + '?action=showing&prop_id=' + encodeURIComponent(safeId)
                           + '&prop_name=' + encodeURIComponent(item['案名'] || '')
                           + '&prop_address=' + encodeURIComponent(item['物件地址'] || '');
            html += '<a href="' + showingUrl + '" target="_blank" rel="noopener" onclick="event.stopPropagation()" '
                  + 'style="font-size:0.75rem;color:var(--txs);padding:0.125rem 0.5rem;border-radius:0.375rem;border:1px solid var(--bd);text-decoration:none;" '
                  + 'title="記錄帶看">🗓</a>';
            // 帶看摘要展開按鈕（點擊後非同步載入）
            html += '<button class="cp-showing-toggle" '
                  + 'style="font-size:0.75rem;color:var(--txs);padding:0.125rem 0.5rem;border-radius:0.375rem;border:1px solid var(--bd);background:none;cursor:pointer;" '
                  + 'data-prop-id="' + safeId + '" data-loaded="0" title="查看曾帶看此物件的買方">👥 帶看</button>';
          }
          // 📜 委託歷史按鈕（同物件多次委託紀錄）
          var hist = item['_history'] || [];
          if (hist.length > 0) {
            html += '<button class="cp-history-toggle" '
                  + 'style="font-size:0.75rem;color:#a78bfa;padding:0.125rem 0.5rem;border-radius:0.375rem;border:1px solid rgba(167,139,250,0.4);background:rgba(167,139,250,0.08);cursor:pointer;" '
                  + 'data-prop-id="' + safeId + '" title="此物件過去的委託紀錄（' + hist.length + ' 筆）">📜 歷史 ' + hist.length + '</button>';
          }
          // 🏠 在起家上看（只在物件目前公開於 home-start 時顯示）
          if (item.on_home_start && homeStartUrl) {
            var hsPath = hsLandCats[item['物件類別']] ? '/land/' : '/property/';
            var hsLink = homeStartUrl + hsPath + safeId;
            html += '<a href="' + hsLink + '" target="_blank" rel="noopener" onclick="event.stopPropagation()" '
                  + 'style="font-size:0.75rem;color:#10b981;padding:0.125rem 0.5rem;border-radius:0.375rem;border:1px solid rgba(16,185,129,0.4);background:rgba(16,185,129,0.08);text-decoration:none;" '
                  + 'title="在起家對外網站開啟此物件">🏠 在起家</a>';
          }
          // 📤 推送到起家（立刻把 Library 最新資料推到對外網站，不用等 daily cron）
          html += '<button class="cp-push-home-btn" data-prop-id="' + safeId + '" '
                + 'style="font-size:0.75rem;color:var(--ac);padding:0.125rem 0.5rem;border-radius:0.375rem;border:1px solid var(--bd);background:none;cursor:pointer;" '
                + 'title="立刻把最新售價/資訊推到起家對外網站（5 秒生效）">📤 推送</button>';
          html += '</div>';
          // 帶看摘要區（預設摺疊）
          if (BUYER_URL) {
            html += '<div id="showing-panel-' + safeId + '" class="hidden mt-2 pt-2" style="border-top:1px solid var(--bd);">'
                  + '<p style="font-size:0.75rem;color:var(--txm);text-align:center;padding:0.5rem 0;">載入中…</p></div>';
          }
          // 委託歷史展開 panel（預設摺疊）
          if (hist.length > 0) {
            var histHtml = '<div id="history-panel-' + safeId + '" class="hidden mt-2 pt-2" style="border-top:1px solid var(--bd);">'
                         + '<p style="font-size:0.7rem;color:#a78bfa;font-weight:700;margin:0 0 6px;">📜 此物件過去的委託紀錄（' + hist.length + ' 筆，由新到舊）</p>';
            hist.forEach(function(h){
              var hSelling = h['銷售中'];
              var hBadge;
              if (hSelling === false && h['成交日期']) {
                hBadge = '<span style="background:var(--tg);color:var(--tgt);padding:1px 6px;border-radius:4px;font-size:9px;font-weight:700;">已成交 ' + escapeHtml(h['成交日期']) + '</span>';
              } else if (hSelling === false) {
                hBadge = '<span style="background:var(--bg-h);color:var(--txs);padding:1px 6px;border-radius:4px;font-size:9px;font-weight:700;">已下架</span>';
              } else {
                hBadge = '<span style="background:var(--ok);color:#fff;padding:1px 6px;border-radius:4px;font-size:9px;font-weight:700;">銷售中</span>';
              }
              histHtml += '<div style="display:flex;flex-wrap:wrap;align-items:center;gap:6px;padding:5px 8px;font-size:11px;background:var(--bg-s);border:1px solid var(--bd);border-radius:6px;margin-bottom:4px;">';
              histHtml += hBadge;
              if (h['委託日'])     histHtml += '<span style="color:var(--tx);">📅 委託 ' + escapeHtml(h['委託日']) + '</span>';
              if (h['委託到期日']) histHtml += '<span style="color:var(--txm);">到期 ' + escapeHtml(h['委託到期日']) + '</span>';
              if (h['委託編號'])   histHtml += '<span style="color:var(--txm);">編號 ' + escapeHtml(h['委託編號']) + '</span>';
              if (h['售價(萬)'] != null && h['售價(萬)'] !== '') histHtml += '<span style="color:var(--ac);font-weight:700;">' + escapeHtml(String(h['售價(萬)'])) + '萬</span>';
              if (h['成交金額(萬)'] != null && h['成交金額(萬)'] !== '') histHtml += '<span style="color:var(--tgt);">成交 ' + escapeHtml(String(h['成交金額(萬)'])) + '萬</span>';
              if (h['經紀人'])     histHtml += '<span style="color:var(--txm);">' + escapeHtml(h['經紀人']) + '</span>';
              if (h['資料序號'])   histHtml += '<span style="color:var(--txm);font-size:10px;">序號 ' + escapeHtml(String(h['資料序號'])) + '</span>';
              histHtml += '</div>';
            });
            histHtml += '</div>';
            html += histHtml;
          }
          html += '</div></div>';
        }
        list.innerHTML = html;
        // 星號按鈕事件委派
        list.querySelectorAll('.cp-star-btn').forEach(function(btn) {
          btn.addEventListener('click', function(e) {
            e.stopPropagation();
            var pid = this.dataset.propId;
            fetch('/api/company-properties/' + pid + '/star', {method: 'POST'})
              .then(function(r){ return r.json(); })
              .then(function(data) {
                if (data.starred !== undefined) {
                  // 更新圖示與樣式（不重新整理整頁）
                  btn.textContent = data.starred ? '★' : '☆';
                  btn.style.color = data.starred ? '#facc15' : 'var(--txm)';
                  // 更新卡片邊框
                  var card = btn.closest('.cp-card');
                  if (card) {
                    card.style.border = data.starred ? '1px solid rgba(234,179,8,0.6)' : '1px solid var(--bd)';
                  }
                  // 若目前是星號篩選模式，從列表移除取消追蹤的卡片
                  if (!data.starred && document.getElementById('cp-star-filter-btn').dataset.active === '1') {
                    var cardEl = btn.closest('[class*="rounded-xl"]');
                    if (cardEl) cardEl.remove();
                  }
                }
              });
          });
        });
        // 委託歷史展開按鈕事件委派
        list.querySelectorAll('.cp-history-toggle').forEach(function(btn) {
          btn.addEventListener('click', function(e) {
            e.stopPropagation();
            var pid = this.dataset.propId;
            var panel = document.getElementById('history-panel-' + pid);
            if (!panel) return;
            panel.classList.toggle('hidden');
          });
        });
        // 帶看摘要展開按鈕事件委派
        list.querySelectorAll('.cp-showing-toggle').forEach(function(btn) {
          btn.addEventListener('click', function(e) {
            e.stopPropagation();
            var pid   = this.dataset.propId;
            var panel = document.getElementById('showing-panel-' + pid);
            if (!panel) return;
            // 切換顯示/隱藏
            var isHidden = panel.classList.contains('hidden');
            panel.classList.toggle('hidden', !isHidden);
            if (!isHidden) return;  // 收起時不重新載入
            // 呼叫 Library 自己的代理 API（避免跨域問題）
            panel.innerHTML = '<p class="text-xs text-center py-2" style="color:var(--txm);">載入中…</p>';
            var reactionIcon = {'有興趣':'👍','普通':'😐','不喜歡':'👎'};
            fetch('/api/company-properties/' + encodeURIComponent(pid) + '/showings')
              .then(function(r){ return r.json(); })
              .then(function(d) {
                var items = d.items || [];
                // 新增帶看連結（若有設定 BUYER_URL）
                var addLink = BUYER_URL
                  ? '<a href="' + BUYER_URL + '?action=showing&prop_id=' + encodeURIComponent(pid) + '" target="_blank" '
                    + 'onclick="event.stopPropagation()" '
                    + 'class="block text-center text-xs text-blue-400 hover:text-blue-300 underline py-1">＋ 新增帶看</a>'
                  : '';
                // 展開後更新按鈕文字，顯示帶看筆數
                var toggleBtn = document.querySelector('.cp-showing-toggle[data-prop-id="' + pid + '"]');
                if (toggleBtn) toggleBtn.innerHTML = '👥 帶看 ' + items.length;
                if (!items.length) {
                  panel.innerHTML = '<p class="text-xs text-center py-2" style="color:var(--txs);">尚無帶看紀錄</p>' + addLink;
                  return;
                }
                var html = '<div class="space-y-1.5">';
                items.forEach(function(s) {
                  var icon = reactionIcon[s.reaction] || '•';
                  html += '<div class="text-xs leading-snug" style="color:var(--txs);">'
                        + '<span class="mr-1">' + icon + '</span>'
                        + '<span class="font-medium" style="color:var(--tx);">' + escapeHtml(s.buyer_name) + '</span>'
                        + '<span class="ml-2" style="color:var(--txm);">' + escapeHtml(s.date) + '</span>'
                        + (s.note ? '<span class="block pl-4 italic truncate" style="color:var(--txm);">' + escapeHtml(s.note) + '</span>' : '')
                        + '</div>';
                });
                html += '</div>' + addLink;
                panel.innerHTML = html;
              })
              .catch(function() {
                panel.innerHTML = '<p class="text-xs text-red-400 text-center py-2">載入失敗</p>';
              });
          });
        });
        // 物件卡片點擊開啟詳情事件委派
        list.querySelectorAll('.cp-detail-btn').forEach(function(el) {
          el.addEventListener('click', function() {
            cpOpenDetail(this.dataset.id);
          });
        });
        // 📤 推送到起家：事件委派
        list.querySelectorAll('.cp-push-home-btn').forEach(function(btn) {
          btn.addEventListener('click', function(e) {
            e.stopPropagation();
            cpPushToHomeStart(this.dataset.propId, this);
          });
        });
    }).catch(function(e) {
      // fetchAll reject 時傳的是 data object（含 error 和 need_org），一般網路錯誤是字串
      if (e && e.need_org) {
        list.innerHTML = '<div style="text-align:center;padding:40px 20px;color:var(--txm);">'
          + '<div style="font-size:2.5rem;margin-bottom:12px;">🏢</div>'
          + '<p style="font-size:1rem;font-weight:600;color:var(--tx);margin-bottom:8px;">公司物件庫需要加入組織才能使用</p>'
          + '<p style="font-size:0.875rem;">' + escapeHtml(e.error || '') + '</p>'
          + '</div>';
        document.getElementById('cp-info').classList.add('hidden');
        document.getElementById('cp-pagination').classList.add('hidden');
      } else {
        list.innerHTML = '<p class="text-red-400 text-center py-8">載入失敗：' + escapeHtml(String(e && e.error ? e.error : e)) + '</p>';
      }
    });
  }

  function cpOpenDetail(id) {
    fetch('/api/company-properties/' + encodeURIComponent(id)).then(r => r.json()).then(function(data) {
      if (data.error) { toast(data.error, 'error'); return; }

      document.getElementById('cp-detail-title').textContent = data['案名'] || '物件詳情';

      var PHONE_KEYS = new Set(['行動電話1','室內電話1','連絡人行動電話2','連絡人室內電話2']);

      // 格式化欄位值（URL/電話/純文字）
      function fmtVal(key, val) {
        var valStr = String(val);
        if (valStr.startsWith('http'))
          return '<a href="' + escapeHtml(valStr) + '" target="_blank" class="text-blue-400 underline hover:text-blue-300 break-all">開啟連結</a>';
        if (PHONE_KEYS.has(key))
          return '<a href="tel:' + escapeHtml(valStr.replace(/[^0-9+]/g,'')) + '" class="text-green-400 underline hover:text-green-300">' + escapeHtml(valStr) + '</a>';
        // 所有權人 → 點連結到人脈管理（沒有就建立）
        if (key === '所有權人') {
          var params = new URLSearchParams({
            name: valStr,
            prop: data['id'] || '',
            phone: data['行動電話1'] || '',
            contact: data['連絡人姓名'] || '',
            address: data['物件地址'] || '',
            category: data['物件類別'] || '',
            price: data['售價(萬)'] || '',
            case: data['案名'] || '',
          });
          var url = 'https://real-estate-people-334765337861.asia-east1.run.app/find-or-create?' + params.toString();
          return '<a href="' + escapeHtml(url) + '" target="_blank" rel="noopener" class="text-blue-400 underline hover:text-blue-300 break-all" title="跳到人脈管理（沒有的話可建立）">👤 ' + escapeHtml(valStr) + ' ↗</a>';
        }
        return '<span class="break-all">' + escapeHtml(valStr) + '</span>';
      }

      // 渲染單一欄位列
      function row(label, key, val) {
        var v = (val !== undefined) ? val : data[key];
        if (v == null || v === '') return '';
        return '<div class="flex gap-2 py-1.5 last:border-0" style="border-bottom:1px solid var(--bd);">'
          + '<span class="w-20 shrink-0 text-xs pt-0.5" style="color:var(--txm);">' + escapeHtml(label) + '</span>'
          + '<span class="text-sm flex-1" style="color:var(--tx);">' + fmtVal(key, v) + '</span></div>';
      }

      // 渲染分組區塊
      function section(icon, title, rows) {
        var inner = rows.join('');
        if (!inner) return '';
        return '<div class="mb-3 rounded-xl overflow-hidden" style="background:var(--bg-t);border:1px solid var(--bd);">'
          + '<div class="flex items-center gap-2 px-4 py-2" style="background:var(--bg-h);border-bottom:1px solid var(--bd);">'
          + '<span class="text-base">' + icon + '</span>'
          + '<span class="text-xs font-semibold tracking-wide" style="color:var(--txs);">' + title + '</span></div>'
          + '<div class="px-4 py-1">' + inner + '</div></div>';
      }

      // 狀態徽章
      var isSelling = data['銷售中'] !== false && data['銷售中'] !== '已下架' && data['銷售中'] !== '已成交';
      var statusBadge = isSelling
        ? '<span class="inline-block text-xs px-2 py-0.5 rounded-full" style="background:var(--ok);color:#fff;">銷售中</span>'
        : (data['成交日期']
            ? '<span class="inline-block text-xs px-2 py-0.5 rounded-full" style="background:var(--tg);color:var(--tgt);">已成交</span>'
            : '<span class="inline-block text-xs px-2 py-0.5 rounded-full" style="background:var(--bg-h);color:var(--txs);">已下架</span>');

      var html = '';

      // ── 狀態橫幅 ──
      html += '<div class="flex items-center gap-3 mb-3 px-1">'
        + statusBadge
        + (data['物件類別'] ? '<span class="text-xs px-2 py-0.5 rounded-full" style="color:var(--txs);background:var(--bg-h);">' + escapeHtml(data['物件類別']) + '</span>' : '')
        + (data['售價(萬)'] ? '<span class="text-amber-300 font-bold text-base ml-auto">' + escapeHtml(String(data['售價(萬)'])) + ' 萬</span>' : '')
        + '</div>';

      // ── 委託資訊 ──
      html += section('📋', '委託資訊', [
        row('委託編號', '委託編號'),
        row('委託日',   '委託日'),
        row('到期日',   '委託到期日'),
        row('經紀人',   '經紀人'),
        row('委託價',   '委託價(萬)', data['委託價(萬)'] ? data['委託價(萬)'] + ' 萬' : null),
        row('契變',     '契變'),
        row('成交日期', '成交日期'),
        row('成交金額', '成交金額(萬)', data['成交金額(萬)'] ? data['成交金額(萬)'] + ' 萬' : null),
      ]);

      // ── 物件基本 ──
      html += section('🏠', '物件基本', [
        row('所有權人', '所有權人'),
        row('現況',     '現況'),
        row('售屋原因', '售屋原因'),
        row('段別',     '段別'),
        row('地號',     '地號'),
        row('建號',     '建號'),
      ]);

      // ── 位置 ──
      html += section('📍', '位置', [
        row('地址',     '物件地址'),
        row('鄉鎮市',   '鄉/市/鎮'),
        row('Google地圖','GOOGLE地圖'),
        row('座標',     '座標'),
      ]);

      // ── 坪數 & 建物 ──
      html += section('📐', '坪數 & 建物', [
        row('地坪',     '地坪'),
        row('建坪',     '建坪'),
        row('樓別',     '樓別'),
        row('朝向',     '座向'),
        row('施工日期', '竣工日期'),
        row('格局',     '格局'),
        row('管理費',   '管理費(元)'),
        row('車位',     '車位'),
      ]);

      // ── 價格 & 貸款 ──
      html += section('💰', '價格 & 貸款', [
        row('售價',     '售價(萬)', data['售價(萬)'] ? data['售價(萬)'] + ' 萬' : null),
        row('現有貸款', '現有貸款(萬)', data['現有貸款(萬)'] ? data['現有貸款(萬)'] + ' 萬' : null),
        row('債權人',   '債權人'),
      ]);

      // ── 聯絡資訊 ──
      html += section('👤', '聯絡資訊', [
        row('連絡人',       '連絡人姓名'),
        row('與業主關係',   '連絡人與所有權人關係'),
        row('行動電話',     '行動電話1'),
        row('室內電話',     '室內電話1'),
        row('連絡人行動',   '連絡人行動電話2'),
        row('連絡人室內',   '連絡人室內電話2'),
      ]);

      // ── 備註 ──
      if (data['備註']) {
        html += '<div class="mb-3 bg-slate-800/60 rounded-xl border border-slate-700/60 overflow-hidden">'
          + '<div class="flex items-center gap-2 px-4 py-2 bg-slate-700/40 border-b border-slate-700/60">'
          + '<span class="text-base">📝</span><span class="text-xs font-semibold text-slate-300">備註</span></div>'
          + '<p class="px-4 py-3 text-sm text-slate-300 whitespace-pre-wrap">' + escapeHtml(data['備註']) + '</p></div>';
      }

      // ── 系統資訊（折疊，預設隱藏） ──
      html += '<details class="mb-1">'
        + '<summary class="text-xs text-slate-600 cursor-pointer hover:text-slate-400 px-1 py-1">⚙️ 系統資訊</summary>'
        + '<div class="mt-1 bg-slate-800/40 rounded-lg border border-slate-700/40 px-4 py-1">'
        + row('資料序號', '資料序號')
        + '</div></details>';

      // ── 起家操作歷史（從 home-start 等子服務寫入的事件） ──
      html += '<div id="cp-detail-events" class="mt-3"></div>';

      document.getElementById('cp-detail-body').innerHTML = html || '<p class="text-slate-500">無資料</p>';
      document.getElementById('cp-detail-modal').classList.remove('hidden');

      // 非同步載入事件歷史（避免拖慢主 modal 開啟）
      fetch('/api/property-events?property_id=' + encodeURIComponent(id))
        .then(function(r){ return r.json(); })
        .then(function(ed){
          var box = document.getElementById('cp-detail-events');
          if (!box) return;
          if (ed.hidden) return;  // 非管理員看不到
          if (!ed.items || !ed.items.length) return;
          var EVT_LABEL = {
            'offshelf':     {icon:'🚫', label:'親自下架',   color:'#F5613A'},
            'onshelf':      {icon:'✅', label:'重新上架',   color:'#1BA896'},
            'price_change': {icon:'💰', label:'手動改價',   color:'#F5613A'},
            'price_reset':  {icon:'↩️', label:'解除價格覆寫', color:'#5E6E82'},
            'meta_update':  {icon:'📝', label:'文案/標籤 修改', color:'#1A5DBF'},
          };
          var rows = ed.items.map(function(e){
            var c = EVT_LABEL[e.event_type] || {icon:'📌', label:e.event_type, color:'#5E6E82'};
            var when = e.created_at ? e.created_at.slice(0, 16).replace('T', ' ') : '';
            var detail = '';
            if (e.event_type === 'price_change' && e.payload) {
              detail = '<span style="color:#9AA5B4;"> ' + (e.payload.old_price || '?') + ' 萬 → </span><span style="color:#F5613A;font-weight:700;">' + (e.payload.new_price || '?') + ' 萬</span>';
            } else if (e.event_type === 'meta_update' && e.payload && e.payload.updated) {
              detail = '<span style="color:#9AA5B4;">（' + (e.payload.updated || []).join(', ') + '）</span>';
            }
            return '<div style="display:flex;gap:8px;align-items:flex-start;padding:6px 0;border-bottom:1px solid rgba(148,163,184,0.15);font-size:12px;">'
              + '<span style="font-size:14px;">' + c.icon + '</span>'
              + '<span style="color:' + c.color + ';font-weight:700;min-width:80px;">' + c.label + '</span>'
              + '<span style="flex:1;color:#cbd5e1;">' + detail + '</span>'
              + '<span style="color:#94a3b8;font-size:11px;white-space:nowrap;">' + when + '</span>'
              + '</div>';
          }).join('');
          box.innerHTML = '<div class="bg-slate-800/60 rounded-xl border border-slate-700/60 overflow-hidden">'
            + '<div class="flex items-center gap-2 px-4 py-2 bg-slate-700/40 border-b border-slate-700/60">'
            + '<span class="text-base">📜</span><span class="text-xs font-semibold text-slate-300">起家操作歷史（' + ed.items.length + ' 筆）</span></div>'
            + '<div style="padding:8px 16px;">' + rows + '</div></div>';
        })
        .catch(function(){ /* 無事件就靜默 */ });
    });
  }

  function closeCpDetail() {
    document.getElementById('cp-detail-modal').classList.add('hidden');
  }

  // ══ 同步功能（管理員） ══
  function cpLoadSyncStatus() {
    fetch('/api/sync-properties/status').then(function(r){ return r.json(); }).then(function(d) {
      if (d.error) return;
      var el = document.getElementById('cp-last-sync');
      if (d.running) {
        el.textContent = '\u540c\u6b65\u4e2d\uff0c\u8acb\u7a0d\u5019…';
        setTimeout(cpLoadSyncStatus, 3000);
      } else if (d.last_run) {
        var dt = new Date(d.last_run);
        var r = d.last_result || {};
        if (r.ok === false) {
          el.textContent = dt.toLocaleString('zh-TW') + ' ❌ 失敗：' + (r.error || '未知錯誤');
          el.style.color = '#f87171';
        } else {
          el.textContent = dt.toLocaleString('zh-TW') + '\uff08\u5beb\u5165 ' + (r.written||0) + ' \u7b46\uff0c\u522a\u9664 ' + (r.deleted||0) + ' \u7b46\uff09';
          el.style.color = '';
        }
      } else {
        el.textContent = '\u5c1a\u672a\u540c\u6b65\u904e';
      }
    }).catch(function(){});
  }

  // 同步工具列展開／收合（使用者只在更新資料時才需要展開，預設收合）
  function cpToggleSyncBar() {
    var content = document.getElementById('cp-sync-bar-content');
    var arrow = document.getElementById('cp-sync-bar-arrow');
    if (!content || !arrow) return;
    var willExpand = content.classList.contains('hidden');
    if (willExpand) {
      content.classList.remove('hidden');
      arrow.textContent = '▼';
      try { localStorage.setItem('cp-sync-bar-expanded', '1'); } catch(e) {}
    } else {
      content.classList.add('hidden');
      arrow.textContent = '▶';
      try { localStorage.setItem('cp-sync-bar-expanded', '0'); } catch(e) {}
    }
  }
  // 載入時還原上次摺疊狀態（預設收合）
  document.addEventListener('DOMContentLoaded', function(){
    try {
      if (localStorage.getItem('cp-sync-bar-expanded') === '1') {
        cpToggleSyncBar();
      }
    } catch(e) {}
  });

  // 同步結果明細 modal — 顯示新增/更新/刪除統計 + 案名列表
  // 接收 _do_sync 的回傳：{ ok, written, added, updated, skipped_old, deleted, preserved_old, added_items, deleted_items, ... }
  function showSyncResultModal(r) {
    var modal    = document.getElementById('cp-sync-result-modal');
    var subtitle = document.getElementById('cp-sync-result-subtitle');
    var body     = document.getElementById('cp-sync-result-body');
    if (!modal || !subtitle || !body) {
      toast('同步完成！', 'success');
      return;
    }
    var added         = r.added || 0;
    var updated       = r.updated || 0;
    var deleted       = r.deleted || 0;
    var skippedOld    = r.skipped_old || 0;
    var preservedOld  = r.preserved_old || 0;
    var addedItems    = r.added_items || [];
    var deletedItems  = r.deleted_items || [];

    // 副標題：一行摘要
    subtitle.textContent = '新增 ' + added + ' ／ 更新 ' + updated + ' ／ 刪除 ' + deleted;

    // 統計徽章列
    function badge(label, count, color) {
      return '<span style="display:inline-flex;align-items:center;gap:4px;background:var(--bg-t);border:1px solid var(--bd);border-radius:8px;padding:6px 12px;font-size:12px;color:' + color + ';">'
        + label + ' <strong style="font-size:14px;">' + count + '</strong></span>';
    }
    var stats = '<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:14px;">'
      + badge('🆕 新增', added, added > 0 ? '#16a34a' : 'var(--txm)')
      + badge('🔄 更新', updated, updated > 0 ? 'var(--ac)' : 'var(--txm)')
      + badge('🗑️ 刪除', deleted, deleted > 0 ? '#dc2626' : 'var(--txm)')
      + badge('⏩ 跳過舊資料（< 2013/1/1）', skippedOld, 'var(--txs)')
      + badge('🛡️ 保留舊 Firestore 物件', preservedOld, 'var(--txs)')
      + '</div>';

    // 新增明細
    function renderList(title, items, color, emptyMsg) {
      if (!items.length) {
        return '<div style="margin-bottom:14px;padding:10px 12px;background:var(--bg-t);border-radius:8px;font-size:12px;color:var(--txs);">'
          + '<strong style="color:' + color + ';">' + title + '</strong>：' + emptyMsg + '</div>';
      }
      var rows = items.map(function(it){
        var seq = it.seq || '';
        var nm  = it.案名 || '(無案名)';
        var dt  = it.委託日 || '';
        var ag  = it.經紀人 || '';
        return '<div style="padding:5px 8px;border-bottom:1px solid var(--bd);font-size:12px;display:flex;gap:8px;flex-wrap:wrap;">'
          + '<span style="color:var(--txm);font-weight:700;min-width:50px;">#' + escapeHtml(seq) + '</span>'
          + '<span style="color:var(--tx);flex:1;min-width:120px;">' + escapeHtml(nm) + '</span>'
          + (dt ? '<span style="color:var(--txs);">' + escapeHtml(dt) + '</span>' : '')
          + (ag ? '<span style="color:var(--txs);">' + escapeHtml(ag) + '</span>' : '')
          + '</div>';
      }).join('');
      return '<div style="margin-bottom:14px;border:1px solid var(--bd);border-radius:8px;overflow:hidden;">'
        + '<div style="padding:8px 12px;background:var(--bg-t);font-size:12px;font-weight:700;color:' + color + ';">'
        + title + '（' + items.length + ' 筆，僅顯示前 50 筆）</div>'
        + '<div style="max-height:240px;overflow-y:auto;">' + rows + '</div></div>';
    }

    body.innerHTML = stats
      + renderList('🆕 新增到 Firestore', addedItems, '#16a34a', '無新增')
      + renderList('🗑️ 從 Firestore 刪除（已不在 Sheets）', deletedItems, '#dc2626', '無刪除');

    modal.style.display = 'flex';
  }

  function closeSyncResultModal() {
    var modal = document.getElementById('cp-sync-result-modal');
    if (modal) modal.style.display = 'none';
  }

  function cpTriggerSync() {
    var btn = document.getElementById('cp-sync-btn');
    btn.disabled = true;
    btn.textContent = '\u540c\u6b65\u4e2d\uff0c\u8acb\u7a0d\u5019…';
    document.getElementById('cp-last-sync').textContent = '\u624b\u52d5\u540c\u6b65\u4e2d\u2026';
    fetch('/api/sync-properties', { method: 'POST' }).then(function(r){ return r.json(); }).then(function(d) {
      if (d.error) { toast(d.error, 'error'); btn.disabled=false; btn.textContent='\u7acb\u5373\u540c\u6b65 Sheets'; return; }
      toast('\u540c\u6b65\u5df2\u555f\u52d5\uff0c\u7d04 1-2 \u5206\u9418\u5f8c\u5b8c\u6210', 'info');
      // 每3秒輪詢狀態
      var poll = setInterval(function() {
        fetch('/api/sync-properties/status').then(function(r){ return r.json(); }).then(function(s) {
          if (!s.running) {
            clearInterval(poll);
            btn.disabled = false;
            btn.textContent = '\u7acb\u5373\u540c\u6b65 Sheets';
            cpLoadSyncStatus();
            var r = s.last_result || {};
            if (r.ok === false) {
              // 同步失敗，顯示錯誤訊息
              toast('同步失敗：' + (r.error || '未知錯誤'), 'error');
            } else {
              window._cpSearched = false;
              cpSearch();
              // 顯示同步結果明細 modal（新增/更新/刪除 + 案名列表）
              showSyncResultModal(r);
            }
          }
        });
      }, 3000);
    }).catch(function(e){ toast('\u547c\u53eb\u5931\u6557: ' + e, 'error'); btn.disabled=false; });
  }

  // yes319 一鍵全自動：文案 + 照片 + 下架預覽 + 缺漏預覽
  function cpSyncYes319All() {
    var btn = document.getElementById('cp-yes319-btn');
    if (!confirm('一鍵全自動同步 yes319（預計 10-15 分鐘，期間請勿關閉頁面）：\\n\\n1. 爬 yes319 比對 home-start，推送特色/機能/屋齡/樓層\\n2. 對沒照片的物件補 yes319 照片（最多 15 張/筆）\\n3. 列出 yes319 已撤離的物件（你看完後可確認下架）\\n4. 列出 yes319 有但 home-start 沒對應的物件（供查看）\\n\\n確認執行？')) return;
    btn.disabled = true;
    var orig = btn.innerHTML;
    btn.innerHTML = '⏳ 同步中…請稍候 10-15 分鐘';
    toast('yes319 全自動同步開始，預計 10-15 分鐘…', 'info');
    fetch('/api/yes319/sync-all', { method: 'POST' })
      .then(function(r){ return r.json(); })
      .then(function(d){
        btn.innerHTML = orig; btn.disabled = false;
        if (d.error) { toast('同步失敗：' + d.error, 'error'); return; }
        showYes319ResultModal(d);
      })
      .catch(function(e){
        btn.innerHTML = orig; btn.disabled = false;
        toast('呼叫失敗：' + e, 'error');
      });
  }

  // yes319 同步結果 modal — 4 區塊：文案 / 照片 / 下架預覽 / 缺漏預覽
  function showYes319ResultModal(d) {
    var modal    = document.getElementById('cp-yes319-result-modal');
    var subtitle = document.getElementById('cp-yes319-result-subtitle');
    var body     = document.getElementById('cp-yes319-result-body');
    if (!modal || !body) { alert('同步完成'); return; }

    var sync   = d.sync || {};
    var photos = d.photos || {};
    var unlist = d.unlist_preview || {};
    var miss   = d.create_missing || {};
    var toUnlist = unlist.to_unlist || [];
    var missing  = miss.missing || [];

    subtitle.textContent = '文案 ' + (sync.pushed_ok || 0)
      + ' ／ 照片 ' + (photos.total_uploaded || 0) + ' 張'
      + ' ／ 待下架 ' + toUnlist.length
      + ' ／ 缺漏 ' + missing.length;

    // 區塊樣式統一
    function section(emoji, title, color, content) {
      return '<div style="margin-bottom:14px;border:1px solid var(--bd);border-radius:8px;overflow:hidden;">'
        + '<div style="padding:8px 12px;background:var(--bg-t);font-size:13px;font-weight:700;color:' + color + ';">'
        + emoji + ' ' + title + '</div>'
        + '<div style="padding:10px 12px;font-size:12px;color:var(--tx);">' + content + '</div></div>';
    }

    // 1. 文案同步
    var syncContent;
    if (sync.error) {
      syncContent = '<span style="color:#dc2626;">❌ ' + escapeHtml(sync.error) + '</span>';
    } else {
      syncContent = '爬取 yes319 <b>' + (sync.yes319_crawled || 0) + '</b> 筆（失敗 ' + (sync.yes319_fails || 0) + '）'
        + '　／　high-start 物件 <b>' + (sync.home_start_total || 0) + '</b> 筆<br>'
        + '高信心配對 <b style="color:#16a34a;">' + (sync.matched || 0) + '</b> 對 '
        + '／ 需人工確認 <b style="color:#d97706;">' + (sync.suspect || 0) + '</b> 對 '
        + '／ 無對應 <b style="color:var(--txs);">' + (sync.unmatched || 0) + '</b> 筆<br>'
        + '✅ 推送成功 <b style="color:#16a34a;">' + (sync.pushed_ok || 0) + '</b> 筆（失敗 ' + (sync.pushed_fail || 0) + '）'
        + '　耗時 ' + (sync.elapsed_sec || 0) + ' 秒';
    }

    // 2. 照片同步
    var photosContent;
    if (photos.error) {
      photosContent = '<span style="color:#dc2626;">❌ ' + escapeHtml(photos.error) + '</span>';
    } else {
      photosContent = '處理物件 <b>' + (photos.targets || 0) + '</b> 筆<br>'
        + '✅ 上傳 <b style="color:#16a34a;">' + (photos.total_uploaded || 0) + '</b> 張'
        + '（失敗 ' + (photos.total_failed || 0) + '）　耗時 ' + (photos.elapsed_sec || 0) + ' 秒';
    }

    // 3. 待下架預覽 + 確認按鈕
    var unlistContent;
    if (unlist.error) {
      unlistContent = '<span style="color:#dc2626;">❌ ' + escapeHtml(unlist.error) + '</span>';
    } else if (toUnlist.length === 0) {
      unlistContent = '<span style="color:var(--ok);">✅ home-start 物件都還在 yes319 上，沒有需要下架的</span>';
    } else {
      var unlistList = toUnlist.map(function(x){
        return '<div style="padding:4px 0;border-bottom:1px solid var(--bd);">'
          + '<span style="color:var(--txm);font-weight:700;">#' + escapeHtml(String(x.id)) + '</span>'
          + ' <span style="color:var(--txs);">[' + escapeHtml(String(x.objno || '')) + ']</span>'
          + ' ' + escapeHtml(x.title || '(無案名)')
          + (x.price ? ' <span style="color:var(--txs);">' + x.price + '萬</span>' : '')
          + '</div>';
      }).join('');
      unlistContent = '<div style="margin-bottom:8px;color:#dc2626;font-weight:700;">⚠️ 找到 ' + toUnlist.length + ' 筆 yes319 已撤離的物件</div>'
        + '<div style="max-height:200px;overflow-y:auto;margin-bottom:10px;border:1px solid var(--bd);border-radius:6px;padding:6px 10px;">' + unlistList + '</div>'
        + '<button id="cp-yes319-confirm-unlist-btn" onclick="cpYes319ConfirmUnlist(' + toUnlist.length + ')"'
        + ' style="padding:6px 14px;border-radius:6px;background:#dc2626;color:#fff;border:none;font-size:12px;font-weight:700;cursor:pointer;">'
        + '🚫 確認下架這 ' + toUnlist.length + ' 筆</button>'
        + ' <span style="color:var(--txs);font-size:11px;margin-left:8px;">（不下架的話 buyer 端會誤以為還在賣）</span>';
    }

    // 4. 缺漏預覽
    var missContent;
    if (miss.error) {
      missContent = '<span style="color:#dc2626;">❌ ' + escapeHtml(miss.error) + '</span>';
    } else if (missing.length === 0) {
      missContent = '<span style="color:var(--ok);">✅ yes319 物件全部都已對應到 home-start</span>';
    } else {
      var missList = missing.slice(0, 50).map(function(x){
        return '<div style="padding:4px 0;border-bottom:1px solid var(--bd);">'
          + '<span style="color:var(--txm);font-weight:700;">[' + escapeHtml(String(x.objno || '')) + ']</span>'
          + ' <span style="color:var(--txs);">' + escapeHtml(x.kind || '') + '</span>'
          + ' ' + escapeHtml(x.title || '(無案名)')
          + (x.price_wan ? ' <span style="color:var(--txs);">' + x.price_wan + '萬</span>' : '')
          + (x.address ? ' <span style="color:var(--txs);">' + escapeHtml(x.address) + '</span>' : '')
          + ' <span style="color:var(--txs);">' + (x.n_photos || 0) + '照</span>'
          + (x.has_features ? ' <span style="color:#16a34a;">✓特色</span>' : '')
          + '</div>';
      }).join('');
      missContent = '<div style="margin-bottom:8px;">yes319 有 ' + (miss.yes319_total || 0)
        + ' 筆、home-start 有 ' + (miss.home_start_total || 0) + ' 筆<br>'
        + '<span style="color:#d97706;font-weight:700;">需新增到 home-start：' + missing.length + ' 筆</span>'
        + (missing.length > 50 ? '（僅顯示前 50 筆）' : '') + '</div>'
        + '<div style="max-height:240px;overflow-y:auto;border:1px solid var(--bd);border-radius:6px;padding:6px 10px;">' + missList + '</div>'
        + '<div style="color:var(--txs);font-size:11px;margin-top:6px;">（dry-run 預覽，未實際寫入。需匯入請聯絡開發者）</div>';
    }

    body.innerHTML =
      section('🌐', '1. 文案同步（特色/機能/屋齡/樓層）', 'var(--ac)', syncContent)
      + section('📷', '2. 照片同步', '#ec4899', photosContent)
      + section('🚫', '3. 待下架預覽（yes319 已撤離的物件）', '#dc2626', unlistContent)
      + section('👁', '4. 缺漏預覽（yes319 有但 home-start 沒）', '#d97706', missContent);

    modal.style.display = 'flex';
  }

  function closeYes319ResultModal() {
    var modal = document.getElementById('cp-yes319-result-modal');
    if (modal) modal.style.display = 'none';
  }

  // 確認下架按鈕：實際呼叫 unlist-missing（非 dry-run）
  function cpYes319ConfirmUnlist(expectedCount) {
    if (!confirm('確定要把這 ' + expectedCount + ' 筆 yes319 已撤離的物件全部下架嗎？\\n（會把 is_selling 設為 False，物件不會消失但 buyer 端不會列出）')) return;
    var btn = document.getElementById('cp-yes319-confirm-unlist-btn');
    if (btn) { btn.disabled = true; btn.innerHTML = '⏳ 下架中…'; }
    fetch('/api/yes319/unlist-missing', { method: 'POST' })
      .then(function(r){ return r.json(); })
      .then(function(d){
        if (d.error) { toast('下架失敗：' + d.error, 'error'); return; }
        toast('✅ 完成：下架 ' + (d.unlisted_ok || 0) + ' 筆（失敗 ' + (d.unlisted_fail || 0) + '）', 'success');
        if (btn) {
          btn.innerHTML = '✅ 已下架 ' + (d.unlisted_ok || 0) + ' 筆';
          btn.style.background = '#16a34a';
        }
      })
      .catch(function(e){
        if (btn) { btn.disabled = false; btn.innerHTML = '🚫 確認下架這 ' + expectedCount + ' 筆'; }
        toast('下架呼叫失敗：' + e, 'error');
      });
  }

  // 一鍵回寫 Firestore 銷售中 → Google Sheets + 補上架物件座標
  function cpWritebackSelling() {
    if (!confirm('確定執行？\\n(1) 把 Firestore「銷售中」狀態回寫到 Google Sheets\\n(2) 對銷售中+委託期內+有段別地號的物件，用 easymap 反查座標、寫回 SHEETS「座標」欄位\\n\\n預計 3-10 分鐘（看銷售中物件數量）')) return;
    var btn = document.getElementById('cp-writeback-btn');
    btn.disabled = true;
    btn.textContent = '⏳ 回寫中…';
    fetch('/api/sheets/writeback-selling', { method: 'POST' })
      .then(function(r){ return r.json(); })
      .then(function(d) {
        btn.disabled = false;
        btn.textContent = '📤 回寫銷售中 + 補座標';
        if (d.ok) {
          var msg = d.message || '完成';
          if (d.coord) {
            msg += '\\n座標：嘗試 ' + d.coord.attempted
                 + ' 筆、查到 ' + d.coord.resolved
                 + '（失敗 ' + d.coord.failed + '）'
                 + '、寫回 SHEETS ' + d.coord.sheets_updated + ' 筆';
          }
          alert('✅ ' + msg);
        } else {
          toast('❌ 失敗：' + (d.error || '未知錯誤'), 'error');
        }
      })
      .catch(function(e){
        btn.disabled = false;
        btn.textContent = '📤 回寫銷售中 + 補座標';
        toast('❌ 呼叫失敗：' + e, 'error');
      });
  }

  // ════════════════════════════════════════════════════════════════
  // ACCESS 比對更新
  // ════════════════════════════════════════════════════════════════

  // 全域比對結果暫存（供 accessApply 使用）
  var _acData = { added: [], modified: [], removed: [], compare_fields: [] };

  // 開啟 Modal
  function openAccessCompareModal() {
    document.getElementById('ac-modal').style.display = 'flex';
    document.getElementById('ac-results').style.display = 'none';
    document.getElementById('ac-loading').style.display = 'none';
    document.getElementById('ac-subtitle').textContent = '';
    document.getElementById('ac-compare-btn').disabled = false;
    document.getElementById('ac-compare-btn').textContent = '開始比對';
  }

  // 切換分頁
  function acTab(tab) {
    ['mod','add','rem'].forEach(function(t) {
      var pane = document.getElementById('ac-pane-' + t);
      var btn  = document.getElementById('ac-tab-' + t + '-btn');
      if (pane) pane.style.display = (t === tab) ? '' : 'none';
      if (btn) {
        btn.style.borderBottomColor = (t === tab) ? 'var(--ac)' : 'transparent';
        btn.style.color = (t === tab) ? 'var(--ac)' : 'var(--txm)';
      }
    });
    _acUpdateApplyCount();
  }

  // 全選 / 取消全選
  function acToggleAll(type, cb) {
    document.querySelectorAll('#ac-list-' + type + ' input[type=checkbox]')
      .forEach(function(el) { el.checked = cb.checked; });
    _acUpdateApplyCount();
  }

  // 更新「套用 X 筆」計數（只算卡片層的 checkbox，不算欄位層）
  function _acUpdateApplyCount() {
    var modCnt = document.querySelectorAll('#ac-list-mod .ac-mod-cb:checked').length;
    var addCnt = document.querySelectorAll('#ac-list-add .ac-add-cb:checked').length;
    var total  = modCnt + addCnt;
    var el = document.getElementById('ac-apply-count');
    if (el) el.textContent = '已勾選：修改 ' + modCnt + ' 筆、新增 ' + addCnt + ' 筆（共 ' + total + ' 筆）';
  }

  // 開始比對
  function accessRunCompare() {
    var sheetId   = (document.getElementById('ac-sheet-id').value || '').trim();
    var sheetName = (document.getElementById('ac-sheet-name').value || '').trim();
    var minDate   = (document.getElementById('ac-min-date').value || '').trim();  // YYYY-MM-DD
    if (!sheetId) { toast('請輸入新 Sheets 網址或 ID', 'error'); return; }

    var btn = document.getElementById('ac-compare-btn');
    btn.disabled = true;
    btn.textContent = '比對中…';
    document.getElementById('ac-loading').style.display = '';
    document.getElementById('ac-results').style.display = 'none';

    var _acAbort = new AbortController();
    var _acTimer = setTimeout(function() { _acAbort.abort(); }, 60000);  // 60 秒 timeout

    fetch('/api/access-compare', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ new_sheet_id: sheetId, new_sheet_name: sheetName, min_commit_date: minDate }),
      signal: _acAbort.signal
    })
    .then(function(r) { clearTimeout(_acTimer); return r.json(); })
    .then(function(d) {
      btn.disabled = false;
      btn.textContent = '開始比對';
      document.getElementById('ac-loading').style.display = 'none';
      if (!d.ok) {
        toast('❌ 比對失敗：' + (d.error || '未知錯誤'), 'error');
        return;
      }
      _acData = d;
      _acRenderResults(d);
    })
    .catch(function(e) {
      clearTimeout(_acTimer);
      btn.disabled = false;
      btn.textContent = '開始比對';
      document.getElementById('ac-loading').style.display = 'none';
      var msg = e.name === 'AbortError' ? '比對逾時（60秒），請確認新 Sheets 已共用給服務帳戶，再重試' : ('呼叫失敗：' + e);
      toast('❌ ' + msg, 'error');
    });
  }

  // 渲染比對結果
  // 鎖定某個欄位差異（不套用，記住此選擇）
  function acLockField(cardIdx, fieldIdx, btn) {
    var item = _acData.modified[cardIdx];
    if (!item) return;
    var f = item.changed_fields[fieldIdx];
    if (!f) return;
    var obj_key = item._key || '';
    fetch('/api/access-ignore-rules', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        object_key:    obj_key,
        display_name:  item.display_name,
        field:         f.field,
        ignored_value: (f.new || '').trim(),
      })
    }).then(function(r) {
      if (!r.ok) return r.text().then(function(t) { throw new Error('HTTP ' + r.status + ': ' + t.substring(0,200)); });
      return r.json();
    }).then(function(d) {
      if (d.ok) {
        toast('已鎖定「' + f.field + '」，下次比對自動忽略', 'ok');
        f._locked  = true;
        f._rule_id = d.id;
        // 重新渲染整個修改列表
        _acRenderResults(_acData);
      } else {
        toast('鎖定失敗：' + (d.error || ''), 'error');
      }
    }).catch(function(e) { toast('鎖定失敗：' + e.message, 'error'); });
  }

  // 解鎖某個忽略規則
  function acUnlockRule(ruleId, btn) {
    if (!ruleId) return;
    fetch('/api/access-ignore-rules/' + encodeURIComponent(ruleId), { method: 'DELETE' })
    .then(function(r) { return r.json(); }).then(function(d) {
      if (d.ok) {
        toast('已解鎖，重新比對後生效', 'ok');
        // 從 _acData 中清除該規則標記，重新渲染
        if (_acData && _acData.modified) {
          _acData.modified.forEach(function(item) {
            (item.changed_fields || []).forEach(function(f) {
              if (f._rule_id === ruleId) { f._locked = false; f._rule_id = ''; }
            });
          });
          _acRenderResults(_acData);
        }
        // 若管理 Modal 開著，重新載入列表
        if (document.getElementById('ac-ignore-modal').style.display === 'flex') {
          _acLoadIgnoreList();
        }
      } else {
        toast('解鎖失敗：' + (d.error || ''), 'error');
      }
    }).catch(function(e) { toast('解鎖失敗：' + e.message, 'error'); });
  }

  // 🩺 開啟資料體檢 Modal
  function openAuditModal() {
    document.getElementById('ac-audit-modal').style.display = 'flex';
    document.getElementById('ac-audit-subtitle').textContent = '掃描中（5000+ 筆需 5-15 秒）…';
    document.getElementById('ac-audit-stats').innerHTML = '';
    document.getElementById('ac-audit-list').innerHTML = '<p style="margin:0;font-size:12px;color:var(--txs);">⏳ 讀取 Firestore…</p>';
    document.getElementById('ac-audit-summary').textContent = '';
    _loadAuditData();
  }

  function _loadAuditData() {
    fetch('/api/access-data-audit')
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (d.error) {
          document.getElementById('ac-audit-list').innerHTML = '<p style="margin:0;font-size:12px;color:#f87171;">❌ ' + d.error + '</p>';
          document.getElementById('ac-audit-subtitle').textContent = '失敗';
          return;
        }
        var subtitle = '掃了 ' + d.total_scanned + ' 筆銷售中物件，發現 ' + d.missing_count + ' 筆需要補資料';
        if (d.hidden_count > 0) {
          subtitle += '（已隱藏 ' + d.hidden_count + ' 筆<a href="javascript:void(0)" onclick="openAuditAcksModal()" style="color:var(--ac);text-decoration:underline;margin-left:4px;">管理</a>）';
        }
        document.getElementById('ac-audit-subtitle').innerHTML = subtitle;

        // 統計徽章
        var statsHtml = '';
        var icons = {'missing_鄉/市/鎮':'🏙', 'missing_段別':'📍', 'missing_地號':'🆔', 'missing_建號（建物）':'🏢', 'missing_類別異常':'❓'};
        var statsLabel = {'missing_類別異常':'類別異常（建物卻無地址無建號）'};
        Object.keys(d.stats).forEach(function(k){
          var label = statsLabel[k] || k.replace('missing_', '缺');
          var icon = icons[k] || '⚠️';
          var count = d.stats[k];
          // 類別異常用紅色，其他用橘色
          var defaultColor = (k === 'missing_類別異常') ? '#dc2626' : '#d97706';
          var color = count > 0 ? defaultColor : 'var(--txm)';
          statsHtml += '<span style="background:var(--bg-t);border:1px solid var(--bd);border-radius:6px;padding:4px 10px;color:' + color + ';">' + icon + ' ' + label + '：<strong>' + count + '</strong></span>';
        });
        document.getElementById('ac-audit-stats').innerHTML = statsHtml;

        // 列表（按缺欄位數排序，多的在前）
        var items = d.missing.slice().sort(function(a,b){ return b.缺欄位.length - a.缺欄位.length; });
        if (!items.length) {
          document.getElementById('ac-audit-list').innerHTML = '<p style="margin:0;padding:20px;font-size:13px;color:var(--ok);text-align:center;">✅ 太好了，所有銷售中物件硬資料都齊全！</p>';
        } else {
          var html = '';
          items.forEach(function(it){
            // 每個缺欄位徽章旁加 ✓ 按鈕（單獨確認該欄位）
            // 「類別異常」用紅色 + 不同前綴文案（不是缺欄位，是類別跟硬資料矛盾）
            var fieldLabelMap = {'類別異常':'⚠️ 類別異常（建物卻沒地址沒建號）'};
            var fields = it.缺欄位.map(function(f){
              var ackId = (it.ack_ids || {})[f] || '';
              var dispName = (it.案名 || '').replace(/'/g, '');
              var isCatIssue = (f === '類別異常');
              var bg = isCatIssue ? 'rgba(239,68,68,0.15)' : 'rgba(217,119,6,0.15)';
              var color = isCatIssue ? '#dc2626' : '#d97706';
              var label = fieldLabelMap[f] || ('缺 ' + f);
              var title = isCatIssue
                ? '此欄位確認過（例如：類別其實是建地，已改）'
                : '這個欄位確認過（例如：建號是未保存登記）';
              return '<span class="audit-field-tag" data-fid="' + it.doc_id + '" data-field="' + f + '" data-name="' + dispName + '"'
                   + ' style="display:inline-flex;align-items:center;gap:4px;background:' + bg + ';color:' + color + ';padding:2px 6px;border-radius:4px;font-size:10px;font-weight:700;">'
                   + label
                   + '<button onclick="auditAckField(this)" title="' + title + '"'
                   + ' style="background:transparent;border:none;color:#16a34a;cursor:pointer;font-size:11px;padding:0 2px;line-height:1;font-weight:700;">✓</button>'
                   + '</span>';
            }).join(' ');
            // 同筆其他欄位已確認的提示（如果有）
            var ackedHint = '';
            if (it.已確認欄位 && it.已確認欄位.length) {
              ackedHint = '<span style="color:var(--txm);font-size:10px;margin-left:6px;">（' + it.已確認欄位.map(function(f){return '✓'+f;}).join(' ') + '）</span>';
            }
            // 主頁 Sheets 跳行連結
            var rowLink = '';
            if (it.row_in_main && d.main_sheet_id) {
              var url = 'https://docs.google.com/spreadsheets/d/' + d.main_sheet_id + '/edit#gid=' + (d.main_sheet_gid || 0) + '&range=A' + it.row_in_main;
              rowLink = '<a href="' + url + '" target="_blank" rel="noopener" '
                      + 'style="display:inline-flex;align-items:center;gap:2px;padding:2px 7px;border-radius:5px;background:rgba(245,158,11,0.12);color:#d97706;border:1px solid rgba(245,158,11,0.4);font-size:10px;font-weight:700;text-decoration:none;" '
                      + 'title="開主頁 Sheets 跳到此列">📍 主頁 ' + it.row_in_main + '</a>';
            }
            html += '<div style="border:1px solid var(--bd);border-radius:8px;padding:8px 12px;background:var(--bg-s);">';
            html += '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:12px;">';
            html += '<strong style="color:var(--tx);">' + (it.案名 || '(無案名)') + '</strong>';
            if (rowLink) html += rowLink;
            if (it.物件類別) html += '<span style="color:var(--txm);font-size:11px;">[' + it.物件類別 + ']</span>';
            if (it.資料序號) html += '<span style="color:var(--txm);font-size:11px;">序號 ' + it.資料序號 + '</span>';
            if (it.委託編號) html += '<span style="color:var(--txm);font-size:11px;">委託 ' + it.委託編號 + '</span>';
            if (it.經紀人) html += '<span style="color:var(--txm);font-size:11px;">' + it.經紀人 + '</span>';
            html += '</div>';
            html += '<div style="margin-top:4px;display:flex;gap:4px;flex-wrap:wrap;align-items:center;">' + fields + ackedHint + '</div>';
            html += '</div>';
          });
          document.getElementById('ac-audit-list').innerHTML = html;
        }

        // 底部摘要
        var summary = '共 ' + d.missing_count + ' / ' + d.total_scanned + ' 筆需要補資料';
        if (d.missing_count > d.missing.length) summary += '（顯示前 ' + d.missing.length + ' 筆）';
        if (d.hidden_count) summary += '｜已隱藏 ' + d.hidden_count + ' 筆';
        document.getElementById('ac-audit-summary').textContent = summary;
      })
      .catch(function(e){
        document.getElementById('ac-audit-list').innerHTML = '<p style="margin:0;font-size:12px;color:#f87171;">❌ 失敗：' + e.message + '</p>';
        document.getElementById('ac-audit-subtitle').textContent = '失敗';
      });
  }

  // 確認單一欄位（如：這筆建號是未保存登記）
  function auditAckField(btn) {
    var tag = btn.closest('.audit-field-tag');
    if (!tag) return;
    var fid = tag.dataset.fid;
    var field = tag.dataset.field;
    var name = tag.dataset.name || '';
    fetch('/api/access-data-audit/ack', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({firestore_doc_id: fid, field: field, display_name: name})
    })
    .then(function(r){ return r.json(); })
    .then(function(d){
      if (d.ok) {
        toast('✓ 已確認「' + name + '」的「' + field + '」欄位', 'success');
        // 重新載入列表（同筆物件的其他缺欄位仍會顯示，本欄位的徽章會消失）
        _loadAuditData();
      } else {
        toast('❌ ' + (d.error || '失敗'), 'error');
      }
    })
    .catch(function(e){ toast('❌ ' + e.message, 'error'); });
  }

  // 🧹 開啟主頁重複清理 Modal
  var _dupData = null;  // 緩存 API 結果，切 Tab 不重打 API
  function openDupModal() {
    document.getElementById('ac-dup-modal').style.display = 'flex';
    document.getElementById('ac-dup-subtitle').textContent = '掃描中（5000+ 筆需 5-15 秒）…';
    document.getElementById('ac-dup-list-wrap').innerHTML = '<p style="margin:0;font-size:12px;color:var(--txs);">⏳ 讀取主頁 Sheets…</p>';
    document.getElementById('ac-dup-cnt-exact').textContent = '…';
    document.getElementById('ac-dup-cnt-history').textContent = '…';
    document.getElementById('ac-dup-summary').textContent = '';
    fetch('/api/access-data-audit/duplicates')
      .then(function(r){ return r.json(); })
      .then(function(d){
        if (d.error) {
          document.getElementById('ac-dup-list-wrap').innerHTML = '<p style="margin:0;font-size:12px;color:#f87171;">❌ ' + d.error + '</p>';
          document.getElementById('ac-dup-subtitle').textContent = '失敗';
          return;
        }
        _dupData = d;
        document.getElementById('ac-dup-subtitle').textContent =
          '完全重複 ' + d.exact_dup_groups.length + ' 組（' + d.exact_dup_count + ' 列）｜歷史版本 ' + d.history_groups.length + ' 組（' + d.history_count + ' 列）';
        document.getElementById('ac-dup-cnt-exact').textContent = d.exact_dup_count;
        document.getElementById('ac-dup-cnt-history').textContent = d.history_count;
        dupSwitchTab('exact');
      })
      .catch(function(e){
        document.getElementById('ac-dup-list-wrap').innerHTML = '<p style="margin:0;font-size:12px;color:#f87171;">❌ ' + e.message + '</p>';
        document.getElementById('ac-dup-subtitle').textContent = '失敗';
      });
  }

  // 🗑️ 一鍵批次刪除完全重複（每組保留第一筆）
  function dupBulkDelete() {
    if (!_dupData || !_dupData.exact_dup_groups || !_dupData.exact_dup_groups.length) {
      toast('沒有完全重複的群組可刪除', 'info');
      return;
    }
    // 收集要刪的列：每組除了第一筆外其他都刪
    var rowsToDelete = [];
    _dupData.exact_dup_groups.forEach(function(g){
      g.items.slice(1).forEach(function(it){
        rowsToDelete.push(it.row);
      });
    });
    if (!rowsToDelete.length) {
      toast('沒有列需要刪除', 'info');
      return;
    }
    // 強制確認 dialog（用 \\n 讓 Python 輸出實際反斜線+n 給 JS，pitfalls #47）
    var msg = '確定要從主頁 Sheets 刪除 ' + rowsToDelete.length + ' 列嗎？\\n\\n'
            + '這些都是「完全重複」的列（同硬資料 + 同委託編號 + 同委託日），'
            + '系統會每組保留第一筆，刪除其餘。\\n\\n'
            + '⚠️ 操作會直接寫入 Google Sheets。\\n'
            + '✅ 仍可在 Sheets 用「檔案 → 版本記錄」找回。\\n\\n'
            + '繼續？';
    if (!confirm(msg)) return;

    var btn = document.getElementById('dup-bulk-btn');
    if (btn) { btn.disabled = true; btn.textContent = '刪除中…'; }

    fetch('/api/access-data-audit/delete-rows', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({
        row_numbers:    rowsToDelete,
        confirm_token:  'CONFIRM_DELETE_DUPLICATES'
      })
    })
    .then(function(r){ return r.json(); })
    .then(function(d){
      if (d.error) {
        toast('❌ ' + d.error, 'error');
        if (btn) { btn.disabled = false; btn.textContent = '🗑️ 一鍵清理 ' + rowsToDelete.length + ' 列'; }
        return;
      }
      toast('✅ 已從主頁 Sheets 刪除 ' + d.deleted_count + ' 列。重新掃描中…', 'success');
      // 重新跑 duplicates API 看新狀況
      setTimeout(function(){ openDupModal(); }, 800);
    })
    .catch(function(e){
      toast('❌ 失敗：' + e.message, 'error');
      if (btn) { btn.disabled = false; btn.textContent = '🗑️ 一鍵清理 ' + rowsToDelete.length + ' 列'; }
    });
  }

  function dupSwitchTab(tab) {
    var btnE = document.getElementById('ac-dup-tab-exact-btn');
    var btnH = document.getElementById('ac-dup-tab-history-btn');
    if (tab === 'exact') {
      btnE.style.borderBottomColor = 'var(--ac)'; btnE.style.color = 'var(--ac)';
      btnH.style.borderBottomColor = 'transparent'; btnH.style.color = 'var(--txm)';
      document.getElementById('ac-dup-tab-hint').innerHTML =
        '<strong style="color:#dc2626;">完全重複</strong>：同物件被貼了兩次（同硬資料 + 同委託編號 + 同委託日）。<strong>一定要刪一份</strong>，不會有業務影響（兩筆完全相同，留一筆即可）。';
    } else {
      btnH.style.borderBottomColor = 'var(--ac)'; btnH.style.color = 'var(--ac)';
      btnE.style.borderBottomColor = 'transparent'; btnE.style.color = 'var(--txm)';
      document.getElementById('ac-dup-tab-hint').innerHTML =
        '<strong style="color:#d97706;">歷史版本</strong>：同物件多次委託紀錄（委託編號或委託日不同）。<strong>主頁 Sheets 不需要刪</strong>（保留歷史紀錄，包括 ACCESS 拷貝那邊也是）。<br>系統會在 LIBRARY 公司物件庫顯示時自動過濾舊版，只顯示 ✅ 最新那筆。本清單僅供你檢視。';
    }
    _dupRender(tab);
  }

  function _dupRender(tab) {
    if (!_dupData) return;
    var wrap = document.getElementById('ac-dup-list-wrap');
    var groups = (tab === 'exact') ? _dupData.exact_dup_groups : _dupData.history_groups;
    if (!groups.length) {
      var msg = (tab === 'exact') ? '✅ 沒有完全重複的物件，主頁很乾淨！' : '✅ 沒有同物件多版本，主頁很乾淨！';
      wrap.innerHTML = '<p style="margin:0;padding:30px;text-align:center;font-size:13px;color:var(--ok);">' + msg + '</p>';
      document.getElementById('ac-dup-summary').textContent = '';
      return;
    }
    var sid = _dupData.sheet_id;
    var gid = _dupData.sheet_gid;
    var html = '';
    // 「完全重複」Tab：頂部加「一鍵清理」按鈕（每組保留第一筆，刪除其餘）
    if (tab === 'exact') {
      // 計算總共可刪除的列數（每組 N 筆 → 刪 N-1）
      var totalDeletable = groups.reduce(function(a, g){ return a + (g.items.length - 1); }, 0);
      html += '<div style="background:rgba(239,68,68,0.08);border:1px solid rgba(239,68,68,0.3);border-radius:10px;padding:12px 14px;margin-bottom:12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;">';
      html += '<span style="font-size:13px;font-weight:700;color:#dc2626;">💡 一鍵清理</span>';
      html += '<span style="font-size:11px;color:var(--txs);flex:1;">每組保留第一筆，刪除其餘 <strong style="color:#dc2626;">' + totalDeletable + '</strong> 列。完全重複（同硬資料+同委託+同日）刪除不會有業務影響。</span>';
      html += '<button onclick="dupBulkDelete()" id="dup-bulk-btn"'
            + ' style="padding:8px 14px;border-radius:8px;background:#dc2626;color:#fff;border:none;font-size:12px;font-weight:700;cursor:pointer;">🗑️ 一鍵清理 ' + totalDeletable + ' 列</button>';
      html += '</div>';
    }
    groups.forEach(function(g, gi){
      html += '<div style="border:1px solid var(--bd);border-radius:10px;padding:10px 12px;margin-bottom:10px;background:var(--bg-s);">';
      html += '<div style="font-size:11px;color:var(--txm);margin-bottom:6px;font-family:monospace;">🔑 ' + g.key + '</div>';
      g.items.forEach(function(it){
        var rowUrl = 'https://docs.google.com/spreadsheets/d/' + sid + '/edit#gid=' + gid + '&range=A' + it.row;
        var badge = '';
        if (tab === 'history') {
          if (it.is_latest) {
            badge = '<span style="background:rgba(34,197,94,0.2);color:#16a34a;padding:1px 7px;border-radius:5px;font-size:10px;font-weight:700;margin-right:6px;">✅ 最新（建議保留）</span>';
          } else {
            badge = '<span style="background:rgba(217,119,6,0.15);color:#d97706;padding:1px 7px;border-radius:5px;font-size:10px;font-weight:700;margin-right:6px;">🕓 歷史</span>';
          }
        } else {
          badge = '<span style="background:rgba(239,68,68,0.15);color:#dc2626;padding:1px 7px;border-radius:5px;font-size:10px;font-weight:700;margin-right:6px;">🔴 重複</span>';
        }
        html += '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;font-size:12px;padding:5px 0;border-top:1px dashed var(--bd);">';
        html += badge;
        html += '<a href="' + rowUrl + '" target="_blank" style="color:var(--ac);text-decoration:none;font-weight:700;" title="開啟主頁 Sheets 跳到此列">📍 列 ' + it.row + '</a>';
        html += '<strong style="color:var(--tx);">' + (it.name || '(無案名)') + '</strong>';
        if (it.category) html += '<span style="color:var(--txm);font-size:11px;">[' + it.category + ']</span>';
        if (it.seq) html += '<span style="color:var(--txm);font-size:11px;">序號 ' + it.seq + '</span>';
        if (it.comm) html += '<span style="color:var(--txm);font-size:11px;">委託 ' + it.comm + '</span>';
        if (it.commit_date) html += '<span style="color:var(--txm);font-size:11px;">委託日 ' + it.commit_date + '</span>';
        if (it.agent) html += '<span style="color:var(--txm);font-size:11px;">' + it.agent + '</span>';
        html += '</div>';
      });
      html += '</div>';
    });
    wrap.innerHTML = html;
    var totalRows = groups.reduce(function(a, g){ return a + g.items.length; }, 0);
    document.getElementById('ac-dup-summary').textContent = '本 Tab 共 ' + groups.length + ' 組、' + totalRows + ' 列。點「📍 列 N」連結會開啟 Sheets 並跳到該列。';
  }

  // 開啟「已檢查清單」管理 Modal
  function openAuditAcksModal() {
    document.getElementById('ac-audit-acks-modal').style.display = 'flex';
    var listEl = document.getElementById('ac-audit-acks-list');
    listEl.innerHTML = '<p style="margin:0;font-size:12px;color:var(--txs);">⏳ 載入中…</p>';
    fetch('/api/access-data-audit/acks')
      .then(function(r){ return r.json(); })
      .then(function(d){
        if (d.error) {
          listEl.innerHTML = '<p style="margin:0;font-size:12px;color:#f87171;">❌ ' + d.error + '</p>';
          return;
        }
        if (!d.items.length) {
          listEl.innerHTML = '<p style="margin:0;padding:20px;text-align:center;color:var(--txm);font-size:12px;">（尚無已檢查確認）</p>';
          return;
        }
        var html = '';
        d.items.forEach(function(r){
          var dt = r.acked_at ? new Date(r.acked_at).toLocaleDateString('zh-TW') : '';
          html += '<div style="display:flex;align-items:center;gap:8px;padding:8px 10px;border:1px solid var(--bd);border-radius:8px;background:var(--bg-t);font-size:12px;">';
          html += '<strong style="color:var(--tx);">' + (r.display_name || '(無案名)') + '</strong>';
          html += '<span style="background:rgba(34,197,94,0.15);color:#16a34a;padding:1px 6px;border-radius:4px;font-size:10px;font-weight:700;">已確認 ' + r.field + '</span>';
          html += '<span style="margin-left:auto;color:var(--txm);font-size:10px;">' + dt + ' / ' + (r.acked_by || '').split('@')[0] + '</span>';
          html += '<button data-id="' + r.id + '" onclick="auditUnack(this)"'
                + ' style="padding:3px 8px;border-radius:5px;background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);font-size:11px;cursor:pointer;">🔓 解除</button>';
          html += '</div>';
        });
        listEl.innerHTML = html;
      })
      .catch(function(e){ listEl.innerHTML = '<p style="color:#f87171;">❌ ' + e.message + '</p>'; });
  }

  // 解除某筆「已檢查」（下次體檢會重新出現）
  function auditUnack(btn) {
    var ackId = btn.dataset.id;
    if (!confirm('解除後該筆物件下次體檢會再出現，確定？')) return;
    fetch('/api/access-data-audit/acks/' + ackId, {method:'DELETE'})
      .then(function(r){ return r.json(); })
      .then(function(d){
        if (d.ok) {
          toast('🔓 已解除', 'success');
          openAuditAcksModal();   // 重載
          _loadAuditData();        // 主清單也重整
        } else {
          toast('❌ ' + (d.error || '失敗'), 'error');
        }
      })
      .catch(function(e){ toast('❌ ' + e.message, 'error'); });
  }

  // 開啟忽略規則管理 Modal
  function openAcIgnoreModal() {
    document.getElementById('ac-ignore-modal').style.display = 'flex';
    _acLoadIgnoreList();
  }

  // 載入並渲染忽略規則列表
  function _acLoadIgnoreList() {
    var list = document.getElementById('ac-ignore-list');
    list.innerHTML = '<p style="color:var(--txs);font-size:13px;">載入中…</p>';
    fetch('/api/access-ignore-rules').then(function(r) { return r.json(); }).then(function(d) {
      if (!d.ok) { list.innerHTML = '<p style="color:#f87171;">載入失敗</p>'; return; }
      if (d.rules.length === 0) {
        list.innerHTML = '<p style="color:var(--txs);font-size:13px;">目前沒有忽略規則。</p>';
        return;
      }
      list.innerHTML = d.rules.map(function(r) {
        return '<div class="ac-ignore-item" style="border:1px solid var(--bd);border-radius:8px;padding:10px 14px;background:var(--bg-t);display:flex;align-items:center;gap:10px;flex-wrap:wrap;">' +
          '<div style="flex:1;min-width:0;">' +
          '<div style="font-size:13px;font-weight:600;color:var(--tx);">' + _escHtml(r.display_name || r.object_key) + '</div>' +
          '<div style="font-size:11px;color:var(--txm);margin-top:3px;">欄位：<b>' + _escHtml(r.field) + '</b>　忽略的新值：<b>' + _escHtml(r.ignored_value || '（空）') + '</b></div>' +
          '<div style="font-size:10px;color:var(--txs);margin-top:2px;">建立者：' + _escHtml(r.created_by || '') + (r.created_at ? '　' + r.created_at.substring(0,10) : '') + '</div>' +
          '</div>' +
          '<button data-rule-id="' + _escHtml(r.id) + '" onclick="acUnlockRule(this.dataset.ruleId,this);this.parentElement.remove()" ' +
          'style="padding:5px 12px;border-radius:6px;border:1px solid #f87171;color:#f87171;background:none;font-size:12px;cursor:pointer;flex-shrink:0;">解鎖</button>' +
          '</div>';
      }).join('');
    }).catch(function() { list.innerHTML = '<p style="color:#f87171;">載入失敗</p>'; });
  }

  // 卡片勾選框切換：連帶勾/取消全部欄位
  function _acModCbChange(cb, cardIdx) {
    var checked = cb.checked;
    document.querySelectorAll('#ac-list-mod .ac-field-cb[data-card-idx="' + cardIdx + '"]').forEach(function(fcb) {
      fcb.checked = checked;
    });
  }

  // 欄位勾選框切換：若全部欄位都取消，連帶取消卡片；若有任一欄位勾選，連帶勾卡片
  function _acFieldCbChange(fcb, cardIdx) {
    var allFields = document.querySelectorAll('#ac-list-mod .ac-field-cb[data-card-idx="' + cardIdx + '"]');
    var anyChecked = Array.from(allFields).some(function(f) { return f.checked; });
    var cardCb = document.querySelector('#ac-list-mod .ac-mod-cb[data-idx="' + cardIdx + '"]');
    if (cardCb) cardCb.checked = anyChecked;
    _acUpdateApplyCount();
  }

  // 即時搜尋過濾修改卡片（用 display:none 隱藏不符合的）
  function acFilterMod(q) {
    var kw = q.trim().toLowerCase();
    var cards = document.querySelectorAll('#ac-list-mod [data-name]');
    var shown = 0;
    cards.forEach(function(card) {
      var name = (card.getAttribute('data-name') || '').toLowerCase();
      var match = !kw || name.indexOf(kw) !== -1;
      card.style.display = match ? '' : 'none';
      if (match) shown++;
    });
    var hint = document.getElementById('ac-mod-filter-hint');
    if (hint) hint.textContent = kw ? ('顯示 ' + shown + ' 筆') : '';
  }

  function _acRenderResults(d) {
    // 兩邊 Sheets 連結 base（給卡片內 📍 列 N 連結用）
    var origSid = d.orig_sheet_id || '';
    var origGid = d.orig_sheet_gid || 0;
    var newSid  = d.new_sheet_id  || '';
    var newGid  = d.new_sheet_gid || 0;
    function _origRowLink(rn) {
      if (!rn || !origSid) return '';
      var url = 'https://docs.google.com/spreadsheets/d/' + origSid + '/edit#gid=' + origGid + '&range=A' + rn;
      return '<a href="' + url + '" target="_blank" rel="noopener" '
           + 'style="display:inline-flex;align-items:center;gap:2px;padding:2px 7px;border-radius:5px;background:rgba(245,158,11,0.12);color:#d97706;border:1px solid rgba(245,158,11,0.4);font-size:10px;font-weight:700;text-decoration:none;margin-right:4px;" '
           + 'title="開主頁 Sheets 跳到此列">📍 主頁 ' + rn + '</a>';
    }
    function _newRowLink(rn) {
      if (!rn || !newSid) return '';
      var url = 'https://docs.google.com/spreadsheets/d/' + newSid + '/edit#gid=' + newGid + '&range=A' + rn;
      return '<a href="' + url + '" target="_blank" rel="noopener" '
           + 'style="display:inline-flex;align-items:center;gap:2px;padding:2px 7px;border-radius:5px;background:rgba(99,102,241,0.12);color:#6366f1;border:1px solid rgba(99,102,241,0.4);font-size:10px;font-weight:700;text-decoration:none;margin-right:4px;" '
           + 'title="開 ACCESS 拷貝 Sheets 跳到此列">📍 ACCESS ' + rn + '</a>';
    }

    // 更新 Tab 計數（顯示實際總筆數）
    var modTotal = d.modified_total || d.modified.length;
    var addTotal = d.added_total   || d.added.length;
    var remTotal = d.removed_total || d.removed.length;
    document.getElementById('ac-cnt-mod').textContent = modTotal;
    document.getElementById('ac-cnt-add').textContent = addTotal;
    document.getElementById('ac-cnt-rem').textContent = remTotal;

    var subtitle = '修改 ' + modTotal + ' ／ 新增 ' + addTotal + ' ／ 可能下架 ' + remTotal;
    if ((d.filtered_orig_count || 0) + (d.filtered_new_count || 0) > 0) {
      subtitle += ' ｜📅 已濾掉舊資料：主頁 ' + (d.filtered_orig_count || 0) + ' 筆 + ACCESS ' + (d.filtered_new_count || 0) + ' 筆';
    }
    document.getElementById('ac-subtitle').textContent = subtitle;

    // ── 修改列表（一次性 innerHTML，避免 O(n²) 凍結）──
    var modList = document.getElementById('ac-list-mod');
    if (d.modified.length === 0) {
      modList.innerHTML = '<p style="color:var(--txs);font-size:13px;">' + (modTotal === 0 ? '✅ 無修改差異' : '（套用時全部 ' + modTotal + ' 筆都會處理）') + '</p>';
    } else {
      var modHtml = d.modified.map(function(item, idx) {
        // _likely_different 卡片所有欄位也預設不勾（讓使用者主動勾才會套用）
        var likelyDiffPre = item._likely_different === true;
        var fieldDefaultChecked = likelyDiffPre ? '' : 'checked';
        // 每個欄位都有自己的勾選框；被鎖定的欄位顯示 🔒，不可勾選
        var fieldsHtml = item.changed_fields.map(function(f, fi) {
          if (f._locked) {
            // 🔒 鎖定欄位：灰色顯示 + 解鎖按鈕
            return '<div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-bottom:4px;opacity:0.5;">' +
              '<span style="font-size:11px;color:var(--txm);min-width:100px;">🔒 ' + _escHtml(f.field) + '</span>' +
              '<span style="font-size:12px;color:#f87171;text-decoration:line-through;">' + _escHtml(f.old || '（空）') + '</span>' +
              '<span style="font-size:11px;color:var(--txs);">→</span>' +
              '<span style="font-size:12px;color:var(--txm);">' + _escHtml(f.new || '（空）') + '</span>' +
              '<button data-rule-id="' + _escHtml(f._rule_id || '') + '" onclick="acUnlockRule(this.dataset.ruleId,this)" ' +
              'style="font-size:10px;padding:1px 6px;border:1px solid var(--bd);border-radius:4px;background:var(--bg);color:var(--txs);cursor:pointer;flex-shrink:0;">解鎖</button>' +
              '</div>';
          }
          return '<div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-bottom:4px;">' +
            '<label style="display:flex;align-items:center;gap:4px;cursor:pointer;min-width:100px;">' +
            '<input type="checkbox" class="ac-field-cb" data-card-idx="' + idx + '" data-field-idx="' + fi + '" ' + fieldDefaultChecked +
            ' onchange="_acFieldCbChange(this,' + idx + ')" style="flex-shrink:0;">' +
            '<span style="font-size:11px;color:var(--txm);">' + _escHtml(f.field) + '</span>' +
            '</label>' +
            '<span style="font-size:12px;color:#f87171;text-decoration:line-through;">' + _escHtml(f.old || '（空）') + '</span>' +
            '<span style="font-size:11px;color:var(--txs);">→</span>' +
            '<span style="font-size:12px;color:#4ade80;font-weight:600;">' + _escHtml(f.new || '（空）') + '</span>' +
            '<button onclick="acLockField(' + idx + ',' + fi + ',this)" ' +
            'title="鎖定此差異，下次比對自動忽略" ' +
            'style="font-size:10px;padding:1px 6px;border:1px solid var(--bd);border-radius:4px;background:var(--bg);color:var(--txs);cursor:pointer;flex-shrink:0;">🔒鎖定</button>' +
            '</div>';
        }).join('');
        // _likely_different = 同地號但所有權人/案名/售價差異大 → 可能是不同物件，預設不勾選 + 紅色警告
        var likelyDiff = item._likely_different === true;
        var checkedAttr = likelyDiff ? '' : 'checked';
        var cardBg = likelyDiff ? 'rgba(239,68,68,0.06)' : 'var(--bg-t)';
        var cardBorder = likelyDiff ? '1px solid rgba(239,68,68,0.4)' : '1px solid var(--bd)';
        var warnBanner = likelyDiff
          ? '<div style="background:rgba(239,68,68,0.15);color:#dc2626;padding:6px 10px;border-radius:6px;font-size:11px;font-weight:700;margin-bottom:6px;line-height:1.5;">⚠️ 可能是「同地號的不同物件」（同一塊地不同委託）— 已預設取消勾選，建議跳過或逐欄檢視<br>'
          + (item._old_owner || item._new_owner ? '<span style="font-weight:400;font-size:10px;color:var(--txs);">所有權人：' + _escHtml(item._old_owner || '(空)') + ' → ' + _escHtml(item._new_owner || '(空)') + '</span>' : '')
          + '</div>'
          : '';
        return '<div style="border:' + cardBorder + ';border-radius:8px;padding:10px 12px;background:' + cardBg + ';" data-name="' + _escHtml(item.display_name) + '">' +
          warnBanner +
          '<div style="display:flex;gap:8px;align-items:flex-start;">' +
          '<input type="checkbox" class="ac-mod-cb" data-idx="' + idx + '" ' + checkedAttr + ' onchange="_acModCbChange(this,' + idx + ');_acUpdateApplyCount()" style="margin-top:3px;flex-shrink:0;">' +
          '<div style="flex:1;">' +
          '<div style="font-size:13px;font-weight:600;color:var(--tx);margin-bottom:6px;display:flex;flex-wrap:wrap;align-items:center;gap:4px;">' +
          '<span>' + _escHtml(item.display_name) + '</span>' +
          (item.seq ? '<span style="font-size:11px;color:var(--txs);font-weight:400;">序號 ' + _escHtml(item.seq) + '</span>' : '') +
          _origRowLink(item.row_in_orig) +
          _newRowLink(item.row_in_new) +
          '</div>' + fieldsHtml +
          (item._key ? '<div style="font-size:10px;color:#888;margin-top:4px;word-break:break-all;">🔑 ' + _escHtml(item._key) + '</div>' : '') +
          '</div></div></div>';
      }).join('');
      if (modTotal > d.modified.length) modHtml += '<p style="color:#f87171;font-size:12px;margin-top:8px;">⚠️ 筆數超過顯示上限，僅顯示前 ' + d.modified.length + ' 筆。請用搜尋框確認後再套用，套用時全部 ' + modTotal + ' 筆都會處理。</p>';
      modList.innerHTML = modHtml;
    }

    // ── 新增列表 ──
    var addList = document.getElementById('ac-list-add');
    if (d.added.length === 0) {
      addList.innerHTML = '<p style="color:var(--txs);font-size:13px;">' + (addTotal === 0 ? '✅ 無新增物件' : '（套用時全部 ' + addTotal + ' 筆都會處理）') + '</p>';
    } else {
      addList.innerHTML = d.added.map(function(item, idx) {
        return '<div style="border:1px solid var(--bd);border-radius:8px;padding:10px 12px;background:var(--bg-t);">' +
          '<label style="display:flex;gap:8px;align-items:center;cursor:pointer;flex-wrap:wrap;">' +
          '<input type="checkbox" class="ac-add-cb" data-idx="' + idx + '" checked onchange="_acUpdateApplyCount()" style="flex-shrink:0;">' +
          '<div style="flex:1;">' +
          '<div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">' +
          '<span style="font-size:13px;font-weight:600;color:var(--tx);">' + _escHtml(item.display_name) + '</span>' +
          _newRowLink(item.row_in_new) +
          '</div>' +
          (item.price ? '<span style="font-size:11px;color:var(--txs);margin-right:8px;">售價 ' + _escHtml(item.price) + ' 萬</span>' : '') +
          (item.agent ? '<span style="font-size:11px;color:var(--txs);margin-right:8px;">經紀人 ' + _escHtml(item.agent) + '</span>' : '') +
          (item.comm  ? '<span style="font-size:11px;color:var(--txs);margin-right:8px;">委託 ' + _escHtml(item.comm) + '</span>' : '') +
          (item._key  ? '<div style="font-size:10px;color:#888;margin-top:3px;word-break:break-all;">🔑 ' + _escHtml(item._key) + '</div>' : '') +
          '</div></label></div>';
      }).join('') + (addTotal > d.added.length ? '<p style="color:#f87171;font-size:12px;margin-top:8px;">⚠️ 筆數超過顯示上限，僅顯示前 ' + d.added.length + ' 筆。請用搜尋框確認後再套用，套用時全部 ' + addTotal + ' 筆都會處理。</p>' : '');
    }

    // ── 可能下架列表（只顯示，不勾選）──
    var remList = document.getElementById('ac-list-rem');
    if (d.removed.length === 0) {
      remList.innerHTML = '<p style="color:var(--txs);font-size:13px;">' + (remTotal === 0 ? '✅ 無可能下架物件' : '（共 ' + remTotal + ' 筆，請人工確認）') + '</p>';
    } else {
      remList.innerHTML = d.removed.map(function(item) {
        return '<div style="padding:7px 10px;border-radius:6px;background:var(--bg-t);border:1px solid var(--bd);font-size:12px;color:var(--txm);display:flex;align-items:center;gap:6px;flex-wrap:wrap;">' +
          '<span>' + _escHtml(item.display_name) + '</span>' +
          _origRowLink(item.row_in_orig) +
          (item.comm ? '<span style="color:var(--txs);">委託 ' + _escHtml(item.comm) + '</span>' : '') +
          (item.seq  ? '<span style="color:var(--txs);">序號 ' + _escHtml(item.seq) + '</span>' : '') +
          '</div>';
      }).join('') + (remTotal > d.removed.length ? '<p style="color:var(--txs);font-size:12px;margin-top:8px;">⚠️ 僅顯示前 ' + d.removed.length + ' 筆，共 ' + remTotal + ' 筆</p>' : '');
    }

    // 顯示結果區，預設顯示修改 tab
    document.getElementById('ac-results').style.display = 'flex';
    acTab('mod');
    _acUpdateApplyCount();
  }

  // 套用選取的變更
  function accessApply() {
    // ── 收集修改項目 ──
    var applyModified = [];
    document.querySelectorAll('#ac-list-mod .ac-mod-cb:checked').forEach(function(cb) {
      var idx  = parseInt(cb.dataset.idx);
      var item = _acData.modified[idx];
      if (!item) return;
      // 只收集被勾選的欄位（使用者可能只勾部分欄位）
      var checkedFields = [];
      document.querySelectorAll('#ac-list-mod .ac-field-cb[data-card-idx="' + idx + '"]:checked').forEach(function(fcb) {
        var fi = parseInt(fcb.dataset.fieldIdx);
        if (item.changed_fields[fi]) checkedFields.push(item.changed_fields[fi]);
      });
      if (checkedFields.length === 0) return;  // 沒有任何欄位被勾選，跳過
      applyModified.push({
        idx: idx,
        row_in_orig: item.row_in_orig,
        changed_fields: checkedFields  // 只傳被勾選的欄位
      });
    });

    // ── 收集新增項目（只傳 idx，完整資料在 server cache）──
    var applyAdded = [];
    document.querySelectorAll('#ac-list-add .ac-add-cb:checked').forEach(function(cb) {
      var idx = parseInt(cb.dataset.idx);
      applyAdded.push({ idx: idx });
    });

    if (applyModified.length === 0 && applyAdded.length === 0) {
      toast('請先勾選要套用的項目', 'error');
      return;
    }

    var msg = '確定要套用 ' + applyModified.length + ' 筆修改';
    if (applyAdded.length > 0) msg += '＋新增 ' + applyAdded.length + ' 筆';
    msg += ' 到原始 Sheets 主頁？此操作會直接寫入 Google Sheets，無法復原。';
    if (!confirm(msg)) return;

    var btn = document.querySelector('#ac-modal button[onclick="accessApply()"]');
    if (btn) { btn.disabled = true; btn.textContent = '套用中…'; }

    fetch('/api/access-apply', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ apply_modified: applyModified, apply_added: applyAdded })
    })
    .then(function(r) { return r.json(); })
    .then(function(d) {
      if (btn) { btn.disabled = false; btn.textContent = '✅ 套用選取變更'; }
      if (d.ok) {
        toast('✅ ' + d.message, 'success');
        document.getElementById('ac-modal').style.display = 'none';
      } else {
        toast('❌ 套用失敗：' + (d.error || '未知錯誤'), 'error');
      }
    })
    .catch(function(e) {
      if (btn) { btn.disabled = false; btn.textContent = '✅ 套用選取變更'; }
      toast('❌ 呼叫失敗：' + e, 'error');
    });
  }

  // HTML 逸出（防 XSS）
  function _escHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  // ── Sidebar 使用者資訊初始化 ──
  (function() {
    var PORTAL_URL_JS = '__PORTAL_LINK__';
    var BUYER_URL_JS  = '__BUYER_URL_STR__';

    function _setAll(ids, val, prop) {
      ids.forEach(function(id) { var el = document.getElementById(id); if (el && val) el[prop] = val; });
    }
    // 設定頭像：有圖片顯示圖片，否則顯示名字首字縮寫
    function _setAvatar(ids, picUrl, name) {
      var initial = (name || '?').trim().charAt(0).toUpperCase();
      ids.forEach(function(id) {
        var wrap = document.getElementById(id);
        if (!wrap) return;
        if (picUrl) {
          wrap.innerHTML = '<img src="' + picUrl + '" referrerpolicy="no-referrer" alt="" /><div class="av-fb" style="display:none">' + initial + '</div>';
          var img = wrap.querySelector('img');
          img.onerror = function() { this.style.display='none'; wrap.querySelector('.av-fb').style.display='flex'; };
        } else {
          wrap.innerHTML = '<div class="av-fb">' + initial + '</div>';
        }
      });
    }

    // 從 session API 取得登入者資訊
    fetch('/api/me').then(function(r){ return r.json(); }).then(function(u) {
      if (u.error) return;
      _setAll(['sb-name', 'dd-name'], u.name || u.email, 'textContent');
      _setAvatar(['sb-avatar', 'hd-avatar'], u.picture || '', u.name || u.email);
      // 更新 points-pill badge（管理員/訂閱/點數，與 Portal 一致）
      var subActive = u.subscription_active;
      if (subActive === undefined && u.subscription_end) {
        try { subActive = new Date(u.subscription_end.replace('Z','').slice(0,19)).getTime() > Date.now(); } catch(e) { subActive = false; }
      }
      ['sb-badge', 'dd-badge', 'hd-badge'].forEach(function(id) {
        var el = document.getElementById(id); if (!el) return;
        el.classList.remove('admin','sub','points');
        if (u.is_admin) { el.classList.add('admin'); el.textContent = '管理員'; }
        else if (subActive) { el.classList.add('sub'); el.textContent = u.subscription_plan === 'yearly' ? '年訂閱' : '月訂閱'; }
        else { el.classList.add('points'); el.textContent = (u.points != null ? u.points : 0) + ' 點'; }
      });
      // Portal 連結
      if (PORTAL_URL_JS && PORTAL_URL_JS !== '#') {
        var plansUrl   = PORTAL_URL_JS.replace(/[/]$/, '') + '/plans';
        var accountUrl = PORTAL_URL_JS.replace(/[/]$/, '') + '/account';
        var adminUrl   = PORTAL_URL_JS.replace(/[/]$/, '') + '/admin';
        var sbPortalHome = document.getElementById('sb-portal-home');
        if (sbPortalHome) { sbPortalHome.href = PORTAL_URL_JS; sbPortalHome.classList.remove('hidden'); }
        var tbHome = document.getElementById('tb-home'); if (tbHome) tbHome.href = PORTAL_URL_JS;
        // Tab Bar 廣告和買方連結（透過 Portal /api/enter/ 跳轉）
        var portalBase = PORTAL_URL_JS.replace(/[/]$/, '');
        var tbAd = document.getElementById('tb-ad');
        if (tbAd) { tbAd.href = portalBase + '/api/enter/post'; tbAd.target = 'tool-post'; }
        // 更多選單連結（周邊調查、行事曆、實價登錄）
        var moreSurvey = document.getElementById('more-survey');
        if (moreSurvey) { moreSurvey.href = portalBase + '/api/enter/survey'; moreSurvey.target = 'tool-survey'; }
        var moreCalendar = document.getElementById('more-calendar');
        if (moreCalendar) { moreCalendar.href = portalBase + '/api/enter/calendar'; moreCalendar.target = 'tool-calendar'; }
        var morePrice = document.getElementById('more-price');
        if (morePrice) { morePrice.href = portalBase + '/api/enter/price'; morePrice.target = 'tool-price'; }
        // Sidebar 廣告（flyout）
        var sbAd = document.getElementById('sb-ad');
        if (sbAd) {
          sbAd.classList.remove('hidden');
          var _sbAdL = document.getElementById('sb-ad-link'); if (_sbAdL) _sbAdL.href = portalBase + '/api/enter/post';
          ['campaigns','photos','showcase'].forEach(function(t){ var e=document.getElementById('sb-ad-'+t); if(e) e.href=portalBase+'/api/enter/post?tab='+t; });
        }
        // Sidebar 周邊調查（tooltip）
        var sbSurvey = document.getElementById('sb-survey');
        if (sbSurvey) { sbSurvey.href = portalBase + '/api/enter/survey'; sbSurvey.target = 'tool-survey'; sbSurvey.classList.remove('hidden'); }
        // Sidebar 行事曆（flyout）
        var sbCalendar = document.getElementById('sb-calendar');
        if (sbCalendar) {
          sbCalendar.classList.remove('hidden');
          var _sbCalL = document.getElementById('sb-calendar-link'); if (_sbCalL) _sbCalL.href = portalBase + '/api/enter/calendar';
          ['week','month'].forEach(function(t){ var e=document.getElementById('sb-calendar-'+t); if(e) e.href=portalBase+'/api/enter/calendar?tab='+t; });
        }
        // Sidebar 記事本（tooltip）
        var sbNotes = document.getElementById('sb-notes');
        if (sbNotes) { sbNotes.href = portalBase + '/notes'; sbNotes.target = 'tool-portal'; sbNotes.classList.remove('hidden'); }
        var moreNotes = document.getElementById('more-notes');
        if (moreNotes) { moreNotes.href = portalBase + '/notes'; moreNotes.target = 'tool-portal'; }
        var ddPlans = document.getElementById('dd-plans');
        if (ddPlans) { ddPlans.href = plansUrl; ddPlans.classList.remove('hidden'); }
        var ddAccount = document.getElementById('dd-account');
        if (ddAccount) { ddAccount.href = accountUrl; ddAccount.classList.remove('hidden'); }
        if (u.is_admin) {
          var ddAdmin = document.getElementById('dd-admin');
          if (ddAdmin) { ddAdmin.href = adminUrl; ddAdmin.classList.remove('hidden'); }
        }
      }
      // 買方管理連結：透過 Portal /api/enter/buyer 跳轉，才能帶 token 自動登入
      var buyerPortalBase = PORTAL_URL_JS ? PORTAL_URL_JS.replace(/\/$/, '') : '';
      var buyerEnterUrl = buyerPortalBase ? buyerPortalBase + '/api/enter/buyer' : BUYER_URL_JS;
      if (buyerEnterUrl) {
        // Sidebar 買方管理（flyout）
        var sbBuyer = document.getElementById('sb-buyer');
        if (sbBuyer) {
          sbBuyer.classList.remove('hidden');
          var _sbBuyerL = document.getElementById('sb-buyer-link'); if (_sbBuyerL) _sbBuyerL.href = buyerEnterUrl;
          var _bp = buyerPortalBase || '';
          ['buyers','war','showings'].forEach(function(t){ var e=document.getElementById('sb-buyer-'+t); if(e) e.href=(_bp?_bp+'/api/enter/buyer?tab=':buyerEnterUrl+'?tab=')+t; });
        }
        var tbBuyer = document.getElementById('tb-buyer');
        if (tbBuyer) { tbBuyer.href = buyerEnterUrl; tbBuyer.target = 'tool-buyer'; tbBuyer.classList.remove('hidden'); }
      }
    }).catch(function(){});
  })();
  // ── Sidebar Flyout 定位 ──
  (function(){document.addEventListener('DOMContentLoaded',function(){document.querySelectorAll('.sb-fw .sb-flyout').forEach(function(flyout){var wrap=flyout.closest('.sb-fw');var _t=null;function show(){clearTimeout(_t);var r=wrap.getBoundingClientRect();flyout.style.top=(r.top+r.height/2)+'px';flyout.style.left=(r.right+6)+'px';flyout.style.transform='translateY(-50%)';flyout.style.display='block';requestAnimationFrame(function(){flyout.style.opacity='1';flyout.style.pointerEvents='auto';});}function hide(){_t=setTimeout(function(){flyout.style.opacity='0';flyout.style.pointerEvents='none';setTimeout(function(){if(flyout.style.opacity==='0')flyout.style.display='none';},160);},200);}wrap.addEventListener('mouseenter',show);wrap.addEventListener('mouseleave',hide);flyout.addEventListener('mouseenter',function(){clearTimeout(_t);});flyout.addEventListener('mouseleave',hide);});});})();

  function libToggleDropdown(e) {
    e.stopPropagation();
    var dd = document.getElementById('user-dropdown');
    var bd = document.getElementById('user-dropdown-backdrop');
    if (dd.style.display === 'block') { libCloseDropdown(); return; }
    var rect = e.currentTarget.getBoundingClientRect();
    var ddW = 220;
    var left = Math.max(8, rect.right - ddW);
    if (rect.top > window.innerHeight / 2) {
      dd.style.bottom = (window.innerHeight - rect.top + 8) + 'px'; dd.style.top = '';
    } else {
      dd.style.top = (rect.bottom + 8) + 'px'; dd.style.bottom = '';
    }
    dd.style.left = left + 'px';
    dd.style.display = 'block'; bd.style.display = 'block';
  }
  function libCloseDropdown() {
    document.getElementById('user-dropdown').style.display = 'none';
    document.getElementById('user-dropdown-backdrop').style.display = 'none';
  }
  function libDoLogout() {
    fetch('/auth/logout', {method:'POST'}).then(function(r){ return r.json(); }).then(function(d) {
      window.location.href = d.redirect || '__PORTAL_LINK__';
    }).catch(function(){ window.location.reload(); });
  }

  // ══════════════════════════════════════════════════
  // 準賣方管理 JS
  // ══════════════════════════════════════════════════
  var _slData    = [];      // 全部準賣方資料
  var _slCurrent = null;    // 目前編輯中的準賣方 id

  // 載入準賣方列表
  function slLoad() {
    // 同時載入準賣方列表 + 自訂排序
    Promise.all([
      fetch('/api/sellers').then(function(r){ return r.json(); }),
      fetch('/api/sellers/sort-order').then(function(r){ return r.json(); }).catch(function(){ return {order:[]}; })
    ]).then(function(results) {
      var d = results[0], sortRes = results[1];
      if (d.error) { toast('❌ ' + d.error, 'error'); return; }
      _slData = d.items || [];
      _slSavedOrder = sortRes.order || [];
      // 套用自訂排序（若有儲存過）
      if (_slSavedOrder.length) {
        var orderMap = {};
        _slSavedOrder.forEach(function(id, i){ orderMap[id] = i; });
        _slData.sort(function(a, b) {
          var ia = orderMap[a.id] != null ? orderMap[a.id] : 9999;
          var ib = orderMap[b.id] != null ? orderMap[b.id] : 9999;
          return ia - ib;
        });
      }
      slFilterRender();
    }).catch(function(){ toast('❌ 載入準賣方失敗', 'error'); });
  }

  // 依關鍵字 + 狀態篩選後重新渲染卡片列表
  function slFilterRender() {
    var kw     = (document.getElementById('sl-keyword') || {}).value || '';
    var status = (document.getElementById('sl-status-filter') || {}).value || '';
    kw = kw.toLowerCase();
    var filtered = _slData.filter(function(s) {
      var matchKw = !kw ||
        (s.name    || '').toLowerCase().includes(kw) ||
        (s.address || '').toLowerCase().includes(kw) ||
        (s.land_number || '').toLowerCase().includes(kw) ||
        (s.phone   || '').includes(kw);
      var matchStatus = !status || s.status === status;
      return matchKw && matchStatus;
    });
    slRenderList(filtered);
  }

  // 狀態 badge HTML（比照買方風格）
  function _slStatusBadge(status) {
    var map = { '培養中': 'badge-blue', '已報價': 'badge-amber', '已簽委託': 'badge-green', '放棄': 'badge-gray' };
    return '<span class="badge ' + (map[status] || 'badge-gray') + '">' + _esc(status || '') + '</span>';
  }

  // 渲染卡片列表（grid + .card 風格，比照買方列表）
  function slRenderList(items) {
    var el = document.getElementById('sl-list');
    if (!el) return;
    if (!items.length) {
      el.innerHTML = '<p class="text-center py-12 text-sm" style="color:var(--txs);grid-column:1/-1;">目前沒有符合的準賣方</p>';
      return;
    }
    el.innerHTML = items.map(function(s) {
      var lastContact = s.last_contact_at ? s.last_contact_at.substring(0, 10) : '';
      var priceInfo = '';
      if (s.owner_price)   priceInfo += '屋主：' + s.owner_price + '萬';
      if (s.suggest_price) priceInfo += (priceInfo ? '　' : '') + '建議：' + s.suggest_price + '萬';
      var location = [s.address, s.land_number].filter(Boolean).join(' / ');
      var cardStyle = s.card_color ? 'background:' + s.card_color + ';' : '';
      // 頭像：有圖用圖，否則顯示名字首字
      var avatarHtml = s.avatar_url
        ? '<img class="sl-avatar mr-3" src="' + _esc(s.avatar_url) + '" alt="">'
        : '<div class="sl-avatar-ph mr-3">' + _esc((s.name || '?')[0]) + '</div>';
      // 底部資訊列
      var parts = [];
      if (lastContact) parts.push('📅 追蹤 ' + lastContact);
      var bottomRow = parts.length ? '<p class="text-xs mt-1" style="color:var(--txm);">' + parts.join('　') + '</p>' : '';
      return '<div class="card hover:border-slate-500 transition cursor-pointer" data-sl-id="' + s.id + '" style="' + cardStyle + '" onclick="slOpenEdit(this.dataset.slId)">'
        + '<div class="flex items-start justify-between">'
          + avatarHtml
          + '<div class="flex-1 min-w-0">'
            + '<div class="flex items-center gap-2 flex-wrap mb-1">'
              + '<span class="font-semibold text-base" style="color:var(--tx);">' + _esc(s.name) + '</span>'
              + _slStatusBadge(s.status)
              + (s.phone ? '<span class="text-xs" style="color:var(--txs);">' + _esc(s.phone) + '</span>' : '')
            + '</div>'
            + '<div class="text-sm mb-1" style="color:var(--txs);">'
              + (s.category ? '🏷 ' + _esc(s.category) : '')
              + (s.source   ? (s.category ? '　' : '') + '📌 ' + _esc(s.source) : '')
            + '</div>'
            + (location  ? '<p class="text-xs mb-1 truncate" style="color:var(--txm);">📍 ' + _esc(location) + '</p>' : '')
            + (priceInfo ? '<p class="text-xs mb-1" style="color:var(--txm);">💰 ' + _esc(priceInfo) + '</p>' : '')
            + (s.note    ? '<p class="text-xs line-clamp-2" style="color:var(--txs);">' + _esc(s.note) + '</p>' : '')
            + bottomRow
          + '</div>'
          + '<div class="flex flex-col gap-1 ml-2 flex-shrink-0">'
            + '<button title="編輯" data-sl-id="' + s.id + '" style="width:30px;height:30px;border-radius:8px;border:1px solid var(--bd);background:transparent;display:flex;align-items:center;justify-content:center;cursor:pointer;" onclick="event.stopPropagation();slOpenEdit(this.dataset.slId)">'
            + '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color:var(--txs);"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>'
            + '</button>'
            + '<a title="在人脈管理查看" target="_blank" rel="noopener" href="https://real-estate-people-334765337861.asia-east1.run.app/people/' + s.id + '" style="width:30px;height:30px;border-radius:8px;border:1px solid var(--bd);background:transparent;display:flex;align-items:center;justify-content:center;cursor:pointer;text-decoration:none;" onclick="event.stopPropagation()">'
            + '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color:var(--ac);"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>'
            + '</a>'
            + '<button title="刪除" data-sl-id="' + s.id + '" style="width:30px;height:30px;border-radius:8px;border:1px solid var(--bd);background:transparent;display:flex;align-items:center;justify-content:center;cursor:pointer;" onclick="event.stopPropagation();slDelete(this.dataset.slId)">'
            + '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="color:var(--dg);"><polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6"/><path d="M14 11v6"/><path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/></svg>'
            + '</button>'
          + '</div>'
        + '</div>'
      + '</div>';
    }).join('');
    // 渲染完後綁定拖曳事件
    _slBindDragEvents();
  }

  // 欄數切換（記住偏好）
  function slSetColumns(n) {
    var list = document.getElementById('sl-list');
    list.style.gridTemplateColumns = 'repeat(' + n + ', 1fr)';
    var widthMap = {1: '640px', 2: '896px', 3: '1200px', 4: '1400px', 5: '100%'};
    document.getElementById('pane-sellers').style.maxWidth = widthMap[n] || '896px';
    document.querySelectorAll('.sl-col-btn').forEach(function(btn) {
      btn.classList.toggle('active', parseInt(btn.dataset.col) === n);
    });
    localStorage.setItem('sl_col_count', n);
  }
  // 頁面載入時讀取欄數偏好
  (function() {
    var saved = parseInt(localStorage.getItem('sl_col_count'));
    var n = (saved >= 1 && saved <= 5) ? saved : (window.innerWidth < 640 ? 1 : 2);
    setTimeout(function() { slSetColumns(n); }, 0);
  })();

  // ═══════════════════════════
  //  準賣方拖曳自由排列
  // ═══════════════════════════
  var _slDragSrc = null;
  var _slIsDragging = false;
  var _slTouchDragEl = null;
  var _slSavedOrder = [];

  function _slBindDragEvents() {
    var list = document.getElementById('sl-list');
    if (!list) return;
    list.classList.add('drag-mode');
    var cards = list.querySelectorAll('.card[data-sl-id]');
    cards.forEach(function(card) {
      // ── 桌面：HTML5 Drag & Drop ──
      card.setAttribute('draggable', 'true');
      card.ondragstart = function(e) {
        _slDragSrc = card; _slIsDragging = true;
        card.classList.add('sl-dragging');
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', card.dataset.slId);
      };
      card.ondragend = function() {
        card.classList.remove('sl-dragging');
        list.querySelectorAll('.sl-drag-over').forEach(function(el) { el.classList.remove('sl-drag-over'); });
        setTimeout(function() { _slIsDragging = false; }, 50);
      };
      card.ondragover = function(e) {
        e.preventDefault(); e.dataTransfer.dropEffect = 'move';
        if (card !== _slDragSrc) card.classList.add('sl-drag-over');
      };
      card.ondragleave = function() { card.classList.remove('sl-drag-over'); };
      card.ondrop = function(e) {
        e.preventDefault(); card.classList.remove('sl-drag-over');
        if (_slDragSrc && _slDragSrc !== card) {
          var all = Array.from(list.querySelectorAll('.card[data-sl-id]'));
          var si = all.indexOf(_slDragSrc), ti = all.indexOf(card);
          list.insertBefore(_slDragSrc, si < ti ? card.nextSibling : card);
          _slSaveDragOrder();
        }
      };
      // ── 手機：長壓觸控 ──
      var _tt = null;
      card.ontouchstart = function() {
        _tt = setTimeout(function() { _slTouchDragEl = card; card.classList.add('sl-dragging'); }, 400);
      };
      card.ontouchmove = function(e) {
        if (!_slTouchDragEl) { clearTimeout(_tt); return; }
        e.preventDefault();
        var t = e.touches[0];
        _slTouchDragEl.style.pointerEvents = 'none';
        var tgt = document.elementFromPoint(t.clientX, t.clientY);
        _slTouchDragEl.style.pointerEvents = '';
        list.querySelectorAll('.sl-drag-over').forEach(function(el) { el.classList.remove('sl-drag-over'); });
        if (tgt) { var tc = tgt.closest('.card[data-sl-id]'); if (tc && tc !== _slTouchDragEl) tc.classList.add('sl-drag-over'); }
      };
      card.ontouchend = function() {
        clearTimeout(_tt);
        if (!_slTouchDragEl) return;
        var ov = list.querySelector('.sl-drag-over');
        if (ov && ov !== _slTouchDragEl) {
          var all = Array.from(list.querySelectorAll('.card[data-sl-id]'));
          var si = all.indexOf(_slTouchDragEl), ti = all.indexOf(ov);
          list.insertBefore(_slTouchDragEl, si < ti ? ov.nextSibling : ov);
          _slSaveDragOrder();
        }
        _slTouchDragEl.classList.remove('sl-dragging');
        list.querySelectorAll('.sl-drag-over').forEach(function(el) { el.classList.remove('sl-drag-over'); });
        _slTouchDragEl = null;
      };
      // 拖曳後攔截 click，普通點擊正常開 modal
      card.addEventListener('click', function(e) {
        if (_slIsDragging) { e.stopImmediatePropagation(); e.preventDefault(); }
      }, true);
    });
  }

  function _slSaveDragOrder() {
    var list = document.getElementById('sl-list');
    var order = Array.from(list.querySelectorAll('.card[data-sl-id]')).map(function(el) { return el.dataset.slId; });
    fetch('/api/sellers/sort-order', {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({order: order})
    }).then(function(r) { return r.json(); }).then(function(d) {
      if (d.ok) { _slSavedOrder = order; }
      else { toast(d.error || '排列儲存失敗', 'error'); }
    }).catch(function() { toast('排列儲存失敗', 'error'); });
  }

  // HTML 跳脫（防止 XSS）
  function _esc(s) {
    return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  // 開啟新增 Modal
  function slOpenCreate() {
    _slCurrent = null;
    document.getElementById('sl-modal-title').textContent = '新增準賣方';
    document.getElementById('sl-f-name').value          = '';
    document.getElementById('sl-f-phone').value         = '';
    document.getElementById('sl-f-address').value       = '';
    document.getElementById('sl-f-land').value          = '';
    document.getElementById('sl-f-category').value      = '';
    document.getElementById('sl-f-source').value        = '';
    document.getElementById('sl-f-owner-price').value   = '';
    document.getElementById('sl-f-suggest-price').value = '';
    document.getElementById('sl-f-status').value        = '培養中';
    document.getElementById('sl-f-note').value          = '';
    slPickColor('');
    document.getElementById('sl-avatar-section').style.display  = 'none';
    document.getElementById('sl-files-section').style.display   = 'none';
    document.getElementById('sl-contacts-section').style.display = 'none';
    document.getElementById('sl-btn-delete').classList.add('hidden');
    document.getElementById('sl-modal').style.display = 'flex';
    document.getElementById('sl-f-name').focus();
  }

  // 開啟編輯 Modal
  function slOpenEdit(id) {
    var s = _slData.find(function(x){ return x.id === id; });
    if (!s) return;
    _slCurrent = id;
    document.getElementById('sl-modal-title').textContent = '編輯準賣方';
    document.getElementById('sl-f-name').value          = s.name || '';
    document.getElementById('sl-f-phone').value         = s.phone || '';
    document.getElementById('sl-f-address').value       = s.address || '';
    document.getElementById('sl-f-land').value          = s.land_number || '';
    document.getElementById('sl-f-category').value      = s.category || '';
    document.getElementById('sl-f-source').value        = s.source || '';
    document.getElementById('sl-f-owner-price').value   = s.owner_price != null ? s.owner_price : '';
    document.getElementById('sl-f-suggest-price').value = s.suggest_price != null ? s.suggest_price : '';
    document.getElementById('sl-f-status').value        = s.status || '培養中';
    document.getElementById('sl-f-note').value          = s.note || '';
    slPickColor(s.card_color || '');
    // 頭像區
    document.getElementById('sl-avatar-section').style.display = 'block';
    var imgEl = document.getElementById('sl-avatar-img');
    var phEl  = document.getElementById('sl-avatar-placeholder');
    if (s.avatar_url) {
      imgEl.src = s.avatar_url; imgEl.style.display = 'block';
      phEl.style.display = 'none';
    } else {
      imgEl.style.display = 'none';
      phEl.style.display  = 'flex';
      phEl.textContent    = (s.name || '?')[0];
    }
    document.getElementById('sl-files-section').style.display    = 'block';
    document.getElementById('sl-contacts-section').style.display = 'block';
    document.getElementById('sl-btn-delete').classList.remove('hidden');
    document.getElementById('sl-modal').style.display = 'flex';
    slFilesLoad(s);
    slContactsLoad(id);
  }

  // 關閉 Modal
  function slModalClose() {
    document.getElementById('sl-modal').style.display = 'none';
    _slCurrent = null;
  }

  // 卡片顏色選擇
  function slPickColor(color) {
    document.getElementById('sl-f-color').value = color;
    document.querySelectorAll('#sl-color-picker .color-dot').forEach(function(btn) {
      btn.classList.toggle('selected', btn.dataset.color === color);
    });
  }

  // 儲存（新增或更新）
  function slSave() {
    var name = document.getElementById('sl-f-name').value.trim();
    if (!name) { toast('請填寫屋主姓名', 'warn'); return; }
    var ownerP   = document.getElementById('sl-f-owner-price').value;
    var suggestP = document.getElementById('sl-f-suggest-price').value;
    var payload = {
      name:          name,
      phone:         document.getElementById('sl-f-phone').value.trim(),
      address:       document.getElementById('sl-f-address').value.trim(),
      land_number:   document.getElementById('sl-f-land').value.trim(),
      category:      document.getElementById('sl-f-category').value,
      source:        document.getElementById('sl-f-source').value,
      owner_price:   ownerP   !== '' ? parseFloat(ownerP)   : null,
      suggest_price: suggestP !== '' ? parseFloat(suggestP) : null,
      status:        document.getElementById('sl-f-status').value,
      note:          document.getElementById('sl-f-note').value.trim(),
      card_color:    document.getElementById('sl-f-color').value || '',
    };
    var url    = _slCurrent ? '/api/sellers/' + _slCurrent : '/api/sellers';
    var method = _slCurrent ? 'PUT' : 'POST';
    var btn = document.getElementById('sl-btn-save');
    btn.disabled = true; btn.textContent = '儲存中…';
    fetch(url, { method: method, headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) })
      .then(function(r){ return r.json(); })
      .then(function(d) {
        btn.disabled = false; btn.textContent = '儲存';
        if (d.error) { toast('❌ ' + d.error, 'error'); return; }
        toast(method === 'POST' ? '✅ 已新增準賣方' : '✅ 已更新', 'success');
        slModalClose();
        slLoad();
      })
      .catch(function(){ btn.disabled = false; btn.textContent = '儲存'; toast('❌ 儲存失敗', 'error'); });
  }

  // 刪除準賣方（兩階段詢問：撕去賣方標籤 vs 連人脈一起刪）
  function slDelete(id) {
    var targetId = id || _slCurrent;
    if (!targetId) return;
    var item = _slData.find(function(x){ return x.id === targetId; });
    var name = item ? item.name : '';
    if (!confirm('確定要刪除「' + (name || '此準賣方') + '」？')) return;
    var modeMsg = '請選擇刪除方式：\\n\\n'
                + '1 = 只撕去賣方標籤（人脈管理仍保留此人）\\n'
                + '2 = 連人脈管理也一起刪（可從人脈垃圾桶救回）\\n\\n'
                + '輸入 1 或 2：';
    var mode = prompt(modeMsg, '1');
    if (mode !== '1' && mode !== '2') return;
    var modeParam = (mode === '2') ? 'full' : 'tag_only';
    fetch('/api/sellers/' + targetId + '?mode=' + modeParam, { method: 'DELETE' })
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (d.error) { toast('❌ ' + d.error, 'error'); return; }
        toast(modeParam === 'full' ? '✅ 已從人脈刪除' : '✅ 已撕去賣方標籤（人脈仍保留）', 'success');
        if (_slCurrent) slModalClose();
        slLoad();
      })
      .catch(function(){ toast('❌ 刪除失敗', 'error'); });
  }

  // 載入互動記事列表
  function slContactsLoad(id) {
    var el = document.getElementById('sl-contact-list');
    if (!el) return;
    el.innerHTML = '<p class="text-xs text-center py-2" style="color:var(--txm);">載入中…</p>';
    fetch('/api/sellers/' + id + '/contacts')
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (d.error) { el.innerHTML = '<p class="text-xs" style="color:var(--err);">' + _esc(d.error) + '</p>'; return; }
        var items = d.items || [];
        if (!items.length) {
          el.innerHTML = '<p class="text-xs text-center py-2" style="color:var(--txm);">尚無互動記事</p>';
          return;
        }
        var html = '';
        items.forEach(function(c) {
          html += '<div class="rounded-lg p-3 mb-2" style="background:var(--bg-t);border:1px solid var(--bd);">' +
            '<div class="flex items-start justify-between gap-2">' +
              '<div class="flex-1">' +
                '<p class="text-xs mb-1" style="color:var(--txm);">🕐 ' + _esc((c.contact_at||'').substring(0,16).replace('T',' ')) + '</p>' +
                '<p class="text-sm" style="color:var(--tx);">' + _esc(c.content) + '</p>' +
              '</div>' +
              '<button data-sid="' + _slCurrent + '" data-cid="' + c.id + '" onclick="slContactDelete(this.dataset.sid,this.dataset.cid)" class="text-xs shrink-0" style="color:var(--txm);">✕</button>' +
            '</div>' +
          '</div>';
        });
        el.innerHTML = html;
      })
      .catch(function(){ el.innerHTML = '<p class="text-xs" style="color:var(--err);">載入失敗</p>'; });
  }

  // 新增互動記事
  function slContactAdd() {
    if (!_slCurrent) return;
    var content = (document.getElementById('sl-contact-input') || {}).value.trim();
    if (!content) { toast('請填寫互動內容', 'warn'); return; }
    var dateVal = (document.getElementById('sl-contact-date') || {}).value || '';
    var contact_at = dateVal ? dateVal.replace('T', ' ') + ':00' : '';
    fetch('/api/sellers/' + _slCurrent + '/contacts', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ content: content, contact_at: contact_at }),
    })
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (d.error) { toast('❌ ' + d.error, 'error'); return; }
        document.getElementById('sl-contact-input').value = '';
        document.getElementById('sl-contact-date').value  = '';
        toast('✅ 記事已新增', 'success');
        slContactsLoad(_slCurrent);
        // 更新本地 last_contact_at 顯示
        var s = _slData.find(function(x){ return x.id === _slCurrent; });
        if (s) { s.last_contact_at = d.contact_at || new Date().toISOString(); }
      })
      .catch(function(){ toast('❌ 新增失敗', 'error'); });
  }

  // 刪除互動記事
  function slContactDelete(sellerId, contactId) {
    if (!confirm('確定刪除此記事？')) return;
    fetch('/api/sellers/' + sellerId + '/contacts/' + contactId, { method: 'DELETE' })
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (d.error) { toast('❌ ' + d.error, 'error'); return; }
        toast('✅ 已刪除', 'success');
        slContactsLoad(sellerId);
      })
      .catch(function(){ toast('❌ 刪除失敗', 'error'); });
  }
  // 上傳頭像
  // 客戶端把圖片縮成 160x160 JPEG（仿 PEOPLE 工具寫法）
  function _slProcessAvatar(file) {
    return new Promise(function(resolve, reject) {
      var img = new Image();
      var reader = new FileReader();
      reader.onload = function() { img.src = reader.result; };
      reader.onerror = function() { reject(new Error('讀檔失敗')); };
      img.onload = function() {
        try {
          var size = 160;
          var canvas = document.createElement('canvas');
          canvas.width = size; canvas.height = size;
          var ctx = canvas.getContext('2d');
          var minSide = Math.min(img.width, img.height);
          var sx = (img.width - minSide) / 2;
          var sy = (img.height - minSide) / 2;
          ctx.drawImage(img, sx, sy, minSide, minSide, 0, 0, size, size);
          resolve(canvas.toDataURL('image/jpeg', 0.85));
        } catch (e) {
          reject(new Error('縮圖失敗：' + (e.message || e)));
        }
      };
      img.onerror = function() { reject(new Error('圖片解析失敗')); };
      reader.readAsDataURL(file);
    });
  }

  function slAvatarUpload(input) {
    if (!_slCurrent) { toast('❌ 請先儲存準賣方', 'error'); return; }
    if (!input || !input.files || !input.files[0]) return;
    var file = input.files[0];
    if (input.value !== undefined) input.value = '';
    if (!file.type || file.type.indexOf('image/') !== 0) {
      toast('❌ 請選圖片檔', 'error'); return;
    }
    toast('⏳ 處理頭像中…', 'info');
    _slProcessAvatar(file)
      .then(function(b64) {
        return fetch('/api/sellers/' + _slCurrent + '/avatar', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ avatar_b64: b64 }),
        }).then(function(r){ return r.json(); }).then(function(d) {
          if (d.error) throw new Error(d.error);
          var imgEl = document.getElementById('sl-avatar-img');
          var phEl  = document.getElementById('sl-avatar-placeholder');
          if (imgEl) { imgEl.src = b64; imgEl.style.display = 'block'; }
          if (phEl)  { phEl.style.display = 'none'; }
          var s = _slData.find(function(x){ return x.id === _slCurrent; });
          if (s) s.avatar_url = b64;
          toast('✅ 頭像已更新', 'success');
          slFilterRender();
        });
      })
      .catch(function(e) {
        console.error('[avatar]', e);
        toast('❌ 上傳失敗：' + (e.message || '未知錯誤'), 'error');
      });
  }

  // 載入相關圖檔（僅顯示圖片/PDF，過濾掉音訊等其他檔案）
  function slFilesLoad(s) {
    var el = document.getElementById('sl-files-list');
    if (!el) return;
    var allFiles = s.files || [];
    // 只留下圖片或 PDF（讀 mime_type 或 副檔名）
    var files = allFiles.filter(function(f) {
      var name = (f.name || '').toLowerCase();
      var mime = (f.mime_type || '').toLowerCase();
      if (mime.startsWith('image/') || mime === 'application/pdf') return true;
      return /\.(jpg|jpeg|png|webp|gif|pdf)$/i.test(name);
    });
    if (!files.length) {
      el.innerHTML = '<p class="text-xs" style="color:var(--txm);">尚無圖檔</p>';
      return;
    }
    // 用 DOM 操作取代字串拼接，完全避免引號跳脫問題
    el.innerHTML = '';
    files.forEach(function(f) {
      var isPdf = (f.name || '').toLowerCase().endsWith('.pdf');
      var wrap = document.createElement('div');
      wrap.style.cssText = 'position:relative;display:inline-block;';

      if (isPdf) {
        var pdfDiv = document.createElement('div');
        pdfDiv.style.cssText = 'width:64px;height:64px;background:var(--bg-h);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:22px;';
        pdfDiv.textContent = '📄';
        wrap.appendChild(pdfDiv);
      } else {
        var img = document.createElement('img');
        img.src = f.url;
        img.alt = '';
        img.style.cssText = 'width:64px;height:64px;object-fit:cover;border-radius:8px;cursor:pointer;';
        img.addEventListener('click', (function(url){ return function(){ window.open(url, '_blank'); }; })(f.url));
        wrap.appendChild(img);
      }

      var btn = document.createElement('button');
      btn.textContent = '✕';
      btn.style.cssText = 'position:absolute;top:-4px;right:-4px;width:18px;height:18px;border-radius:50%;background:rgba(0,0,0,.7);color:#fff;font-size:10px;line-height:18px;text-align:center;cursor:pointer;border:none;';
      btn.addEventListener('click', (function(fid){ return function(){ slFileDelete(fid); }; })(f.file_id));
      wrap.appendChild(btn);

      var cap = document.createElement('p');
      cap.className = 'text-xs truncate mt-1';
      cap.style.cssText = 'max-width:64px;color:var(--txm);';
      cap.textContent = f.name;
      wrap.appendChild(cap);

      el.appendChild(wrap);
    });
  }

  // 上傳相關圖檔（可多張）
  function slFilesUpload(input) {
    if (!_slCurrent || !input.files || !input.files.length) return;
    var files = Array.from(input.files);
    input.value = '';
    var done = 0;
    files.forEach(function(file) {
      var fd = new FormData();
      fd.append('file', file);
      fetch('/api/sellers/' + _slCurrent + '/files', { method: 'POST', body: fd })
        .then(function(r){ return r.json(); })
        .then(function(d) {
          done++;
          if (d.error) { toast('❌ ' + d.error, 'error'); return; }
          // 更新本地資料並重新渲染圖檔列表
          var s = _slData.find(function(x){ return x.id === _slCurrent; });
          if (s) {
            s.files = s.files || [];
            s.files.push({ file_id: d.file_id, name: d.name, url: d.url, gcs_path: d.gcs_path });
            slFilesLoad(s);
          }
          if (done === files.length) toast('✅ 圖檔上傳完成', 'success');
        })
        .catch(function(){ done++; toast('❌ 上傳失敗：' + file.name, 'error'); });
    });
  }

  // 刪除相關圖檔
  function slFileDelete(fileId) {
    if (!_slCurrent) return;
    if (!confirm('確定刪除此圖檔？')) return;
    fetch('/api/sellers/' + _slCurrent + '/files/' + fileId, { method: 'DELETE' })
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (d.error) { toast('❌ ' + d.error, 'error'); return; }
        var s = _slData.find(function(x){ return x.id === _slCurrent; });
        if (s) {
          s.files = (s.files || []).filter(function(f){ return f.file_id !== fileId; });
          slFilesLoad(s);
        }
        toast('✅ 已刪除', 'success');
      })
      .catch(function(){ toast('❌ 刪除失敗', 'error'); });
  }
  // ══ 準賣方管理 JS 結束 ══

  // ══ 地圖分頁 JS ══
  (function() {
    var _mapObj     = null;   // Leaflet Map 實例
    var _mapInited  = false;  // 是否已初始化過

    // 物件類別 → 地圖標記顏色
    var CAT_COLOR = {
      '農地': '#16a34a',   // 綠
      '建地': '#ea580c',   // 橘
      '公寓': '#2563eb',   // 藍
      '房屋': '#7c3aed',   // 紫
      '別墅': '#d97706',   // 金
      '店住': '#dc2626',   // 紅
    };
    var DEFAULT_COLOR = '#6b7280';  // 灰（未知類別）

    function _catColor(cat) {
      // 類別字串可能含「台東市公寓」→ 取最後兩字匹配
      for (var k in CAT_COLOR) {
        if (cat && cat.indexOf(k) !== -1) return CAT_COLOR[k];
      }
      return DEFAULT_COLOR;
    }

    // ── 篩選狀態 ──
    var _mapSel = { cat: new Set(), area: new Set(), agent: new Set() };

    // 更新篩選按鈕標籤
    function _mapUpdateLabel(type) {
      var labels = { cat: '全部類別', area: '全部地區', agent: '全部經紀人' };
      var el = document.getElementById('map-' + type + '-label');
      if (!el) return;
      var sel = _mapSel[type];
      if (sel.size === 0) {
        el.textContent = labels[type];
      } else if (sel.size === 1) {
        el.textContent = Array.from(sel)[0];
      } else {
        el.textContent = sel.size + ' 項已選';
      }
    }

    // 切換下拉面板（點按鈕開/關，點其他地方關閉）
    window.mapToggleDropdown = function(type) {
      var types = ['cat', 'area', 'agent'];
      types.forEach(function(t) {
        var panel = document.getElementById('map-' + t + '-panel');
        if (!panel) return;
        if (t === type) {
          panel.classList.toggle('hidden');
        } else {
          panel.classList.add('hidden');
        }
      });
    };

    // 點擊空白處關閉所有下拉
    document.addEventListener('click', function(e) {
      var types = ['cat', 'area', 'agent'];
      types.forEach(function(t) {
        var btn   = document.getElementById('map-' + t + '-btn');
        var panel = document.getElementById('map-' + t + '-panel');
        if (!btn || !panel) return;
        if (!btn.contains(e.target) && !panel.contains(e.target)) {
          panel.classList.add('hidden');
        }
      });
    });

    // checkbox 變更時更新 _mapSel
    window.mapOnCheck = function(type, value, checked) {
      if (checked) {
        _mapSel[type].add(value);
      } else {
        _mapSel[type].delete(value);
      }
      _mapUpdateLabel(type);
    };

    // 載入篩選選項（初次進入地圖分頁時呼叫）
    window.mapLoadOptions = function() {
      fetch('/api/map/options')
        .then(function(r){ return r.json(); })
        .then(function(d) {
          if (d.error) return;
          // 類別
          var catList = document.getElementById('map-cat-list');
          if (catList) {
            catList.innerHTML = (d.categories || []).map(function(c) {
              return '<label class="flex items-center gap-2 text-sm cursor-pointer py-0.5" style="color:var(--tx);">'
                + '<input type="checkbox" data-type="cat" value="' + escapeHtml(c) + '" onchange="mapOnCheck(this.dataset.type,this.value,this.checked)" class="rounded"> '
                + escapeHtml(c) + '</label>';
            }).join('');
          }
          // 地區
          var areaList = document.getElementById('map-area-list');
          if (areaList) {
            areaList.innerHTML = (d.areas || []).map(function(a) {
              return '<label class="flex items-center gap-2 text-sm cursor-pointer py-0.5" style="color:var(--tx);">'
                + '<input type="checkbox" data-type="area" value="' + escapeHtml(a.value) + '" onchange="mapOnCheck(this.dataset.type,this.value,this.checked)" class="rounded"> '
                + escapeHtml(a.label) + '</label>';
            }).join('');
          }
          // 經紀人（在線 + 其他）
          var activeList   = document.getElementById('map-agent-active-list');
          var inactiveList = document.getElementById('map-agent-inactive-list');
          var mkAgent = function(name) {
            return '<label class="flex items-center gap-2 text-sm cursor-pointer py-0.5" style="color:var(--tx);">'
              + '<input type="checkbox" data-type="agent" value="' + escapeHtml(name) + '" onchange="mapOnCheck(this.dataset.type,this.value,this.checked)" class="rounded"> '
              + escapeHtml(name) + '</label>';
          };
          if (activeList)   activeList.innerHTML   = (d.agents.active   || []).map(mkAgent).join('');
          if (inactiveList) inactiveList.innerHTML = (d.agents.inactive || []).map(mkAgent).join('');
        })
        .catch(function(){});
    };

    // 套用篩選（重新 fetch + 渲染）
    window.mapApplyFilter = function() {
      // 關閉所有下拉
      ['cat','area','agent'].forEach(function(t) {
        var p = document.getElementById('map-' + t + '-panel');
        if (p) p.classList.add('hidden');
      });
      mapLoad();
    };

    // 重設篩選
    window.mapResetFilter = function() {
      _mapSel = { cat: new Set(), area: new Set(), agent: new Set() };
      ['cat','area','agent'].forEach(function(t) {
        _mapUpdateLabel(t);
        var panels = ['map-' + t + '-list', 'map-' + t + '-active-list', 'map-' + t + '-inactive-list'];
        panels.forEach(function(pid) {
          var el = document.getElementById(pid);
          if (el) el.querySelectorAll('input[type=checkbox]').forEach(function(cb){ cb.checked = false; });
        });
      });
      // 清除情境選擇
      var sel = document.getElementById('map-preset-select');
      if (sel) sel.value = '';
      mapUpdatePresetDeleteBtn();
      mapLoad();
    };

    window.mapInit = function() {
      // 第一次才建立地圖實例
      if (!_mapInited) {
        _mapInited = true;
        // zoomControl:false 先關掉預設左上角縮放鍵，再手動加到右下角
        _mapObj = L.map('map-container', { zoomControl: false }).setView([22.750699, 121.177817], 13);

        // 定義底圖圖層（同 Survey 工具）
        var _osmLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
          maxZoom: 19, attribution: '&copy; OpenStreetMap contributors'
        });
        var _googleStreet = L.tileLayer('https://mt{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {
          maxZoom: 22, subdomains: '0123', attribution: '&copy; Google Maps'
        });
        var _googleSatellite = L.tileLayer('https://mt{s}.google.com/vt/lyrs=s&x={x}&y={y}&z={z}', {
          maxZoom: 22, subdomains: '0123', attribution: '&copy; Google Maps'
        });
        var _googleHybrid = L.tileLayer('https://mt{s}.google.com/vt/lyrs=y&x={x}&y={y}&z={z}', {
          maxZoom: 22, subdomains: '0123', attribution: '&copy; Google Maps'
        });
        _googleStreet.addTo(_mapObj);  // 預設 Google 街道

        // 圖層切換控制（右上角）
        L.control.layers({
          '🗺️ Google 街道':    _googleStreet,
          '🛰️ Google 衛星':    _googleSatellite,
          '🛰️ Google 衛星+路名': _googleHybrid,
          '🗺️ OpenStreetMap':  _osmLayer,
        }, null, { position: 'topright' }).addTo(_mapObj);

        L.control.zoom({ position: 'bottomright' }).addTo(_mapObj);
      }
      mapLoad();
    };

    var _mapItems   = [];        // 快取資料，切換模式時不重新 fetch
    var _mapDotMode = false;     // false = 箭頭標籤，true = 圓點

    // 根據目前模式建立 icon
    function _makeIcon(p) {
      var color = _catColor(p['物件類別']);
      if (_mapDotMode) {
        // 圓點模式：18px 實心圓 + drop-shadow
        return L.divIcon({
          className: '',
          html: '<div style="width:18px;height:18px;border-radius:50%;background:' + color + ';border:2px solid #fff;filter:drop-shadow(0 2px 4px rgba(0,0,0,0.45));"></div>',
          iconSize: [18, 18],
          iconAnchor: [9, 9]
        });
      } else {
        // 箭頭標籤模式
        var rawLabel = p['案名'] || p['物件地址'] || '未命名';
        var label = rawLabel.length > 14 ? rawLabel.slice(0, 13) + '…' : rawLabel;
        return L.divIcon({
          className: '',
          html: '<div style="display:inline-block;filter:drop-shadow(0 0 1px rgba(0,0,0,0.55)) drop-shadow(0 3px 5px rgba(0,0,0,0.35));">'
            + '<div style="background:' + color + ';color:#fff;font-size:12px;font-weight:600;padding:5px 10px 5px 16px;border-radius:0 8px 8px 0;white-space:nowrap;clip-path:polygon(14px 0%,100% 0%,100% 100%,14px 100%,0% 50%);">'
            + label + '</div></div>',
          iconSize: null,
          iconAnchor: [0, 16]
        });
      }
    }

    // 清除並重繪所有 marker（不重新 fetch）
    function _mapRenderMarkers() {
      _mapObj.eachLayer(function(layer) {
        if (layer instanceof L.Marker) _mapObj.removeLayer(layer);
      });
      _mapItems.forEach(function(p) {
        var rawLabel = p['案名'] || p['物件地址'] || '未命名';
        var price    = p['售價'] ? p['售價'] + ' 萬' : '—';
        var agent    = p['經紀人'] || '—';
        var cat      = p['物件類別'] || '—';
        var addr     = p['物件地址'] || '';
        var popup    = '<div style="font-size:13px;line-height:1.8;">'
          + '<b>' + rawLabel + '</b><br>'
          + '類別：' + cat + '<br>'
          + '售價：' + price + '<br>'
          + '經紀人：' + agent + '<br>'
          + (addr ? '地址：' + addr + '<br>' : '')
          + '</div>';
        var marker = L.marker([p.lat, p.lng], { icon: _makeIcon(p) });
        marker.bindPopup(popup);
        marker.addTo(_mapObj);
      });
    }

    // 切換圖釘模式
    window.mapTogglePinMode = function() {
      _mapDotMode = !_mapDotMode;
      var btn = document.getElementById('map-pin-toggle');
      btn.textContent = _mapDotMode ? '🏷️ 標籤模式' : '🔵 圓點模式';
      _mapRenderMarkers();
    };

    function mapLoad() {
      document.getElementById('map-stat-text').textContent = '載入中...';
      // 組裝篩選 query string
      var params = new URLSearchParams();
      if (_mapSel.cat.size)   params.set('cats',   Array.from(_mapSel.cat).join(','));
      if (_mapSel.area.size)  params.set('areas',  Array.from(_mapSel.area).join(','));
      if (_mapSel.agent.size) params.set('agents', Array.from(_mapSel.agent).join(','));
      var url = '/api/map/properties' + (params.toString() ? '?' + params.toString() : '');
      fetch(url)
        .then(function(r){ return r.json(); })
        .then(function(d) {
          if (d.error) { document.getElementById('map-stat-text').textContent = '❌ ' + d.error; return; }
          _mapItems = d.items || [];
          var hasFilter = _mapSel.cat.size || _mapSel.area.size || _mapSel.agent.size;
          document.getElementById('map-stat-text').textContent =
            '🗺️ 銷售中（有座標）' + (hasFilter ? '篩選後' : '') + '：' + _mapItems.length + ' 筆';
          _mapRenderMarkers();
          // 自動調整視野含蓋所有標記
          if (_mapItems.length > 0) {
            var bounds = L.latLngBounds(_mapItems.map(function(p){ return [p.lat, p.lng]; }));
            _mapObj.fitBounds(bounds, { padding: [40, 40] });
          }
        })
        .catch(function(e) {
          document.getElementById('map-stat-text').textContent = '❌ 無法載入地圖資料';
          console.error('mapLoad error', e);
        });
    }

    // ══ 情境書籤 ══
    var _mapPresets = [];  // 快取情境清單

    window.mapLoadPresets = function() {
      fetch('/api/map-presets')
        .then(function(r){ return r.json(); })
        .then(function(d) {
          _mapPresets = d.items || [];
          var sel = document.getElementById('map-preset-select');
          if (!sel) return;
          var current = sel.value;
          sel.innerHTML = '<option value="">— 選擇情境 —</option>'
            + _mapPresets.map(function(p) {
                return '<option value="' + escapeHtml(p.id) + '">' + escapeHtml(p.name) + '</option>';
              }).join('');
          if (current) sel.value = current;
          mapUpdatePresetDeleteBtn();
        })
        .catch(function(){});
    };

    function mapUpdatePresetDeleteBtn() {
      var sel = document.getElementById('map-preset-select');
      var btn = document.getElementById('map-preset-delete-btn');
      if (!sel || !btn) return;
      btn.classList.toggle('hidden', !sel.value);
    }

    window.mapApplyPreset = function() {
      var sel = document.getElementById('map-preset-select');
      mapUpdatePresetDeleteBtn();
      if (!sel || !sel.value) return;
      var preset = _mapPresets.find(function(p){ return p.id === sel.value; });
      if (!preset || !preset.params) return;
      var params = preset.params;
      // 還原各篩選 Set
      ['cat','area','agent'].forEach(function(t) {
        var vals = params['sel_' + t] ? params['sel_' + t].split(',').filter(Boolean) : [];
        _mapSel[t] = new Set(vals);
        _mapUpdateLabel(t);
        // 同步 checkbox
        var panels = ['map-' + t + '-list', 'map-' + t + '-active-list', 'map-' + t + '-inactive-list'];
        panels.forEach(function(pid) {
          var el = document.getElementById(pid);
          if (!el) return;
          el.querySelectorAll('input[type=checkbox]').forEach(function(cb) {
            cb.checked = vals.indexOf(cb.value) !== -1;
          });
        });
      });
      mapLoad();
      toast('✅ 已套用情境「' + preset.name + '」', 'info');
    };

    window.mapSavePreset = function() {
      var name = prompt('請輸入情境名稱（同名會覆蓋）：');
      if (!name || !name.trim()) return;
      var params = {
        sel_cat:   Array.from(_mapSel.cat).join(','),
        sel_area:  Array.from(_mapSel.area).join(','),
        sel_agent: Array.from(_mapSel.agent).join(','),
      };
      fetch('/api/map-presets', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ name: name.trim(), params: params }),
      })
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (d.error) { toast(d.error, 'error'); return; }
        toast('💾 情境「' + name.trim() + '」已儲存', 'success');
        mapLoadPresets();
        setTimeout(function() {
          var sel = document.getElementById('map-preset-select');
          if (sel && d.id) { sel.value = d.id; mapUpdatePresetDeleteBtn(); }
        }, 400);
      })
      .catch(function(){ toast('儲存失敗', 'error'); });
    };

    window.mapDeletePreset = function() {
      var sel = document.getElementById('map-preset-select');
      if (!sel || !sel.value) return;
      var preset = _mapPresets.find(function(p){ return p.id === sel.value; });
      if (!preset) return;
      if (!confirm('確定刪除情境「' + preset.name + '」？')) return;
      fetch('/api/map-presets/' + encodeURIComponent(sel.value), { method: 'DELETE' })
        .then(function(r){ return r.json(); })
        .then(function(d) {
          if (d.error) { toast(d.error, 'error'); return; }
          toast('已刪除情境「' + preset.name + '」', 'info');
          sel.value = '';
          mapUpdatePresetDeleteBtn();
          mapLoadPresets();
        })
        .catch(function(){ toast('刪除失敗', 'error'); });
    };
  })();
  // ══ 地圖分頁 JS 結束 ══

  // 頁面載入後：若 URL 有 ?tab= 就切到指定分頁；?cp=<id> 自動開物件詳情
  (function() {
    var _params = new URLSearchParams(window.location.search);
    var _initTab = _params.get('tab') || 'company';
    var _initCp = _params.get('cp');
    var _allowed = ['company','sellers','map','dbview','settings','org'];
    switchTab(_allowed.indexOf(_initTab) >= 0 ? _initTab : 'company');
    if (_initCp) {
      // 等公司物件庫載入後再開（cpOpenDetail 內會自己 fetch 資料）
      setTimeout(function() {
        try { cpOpenDetail(_initCp); } catch (e) { console.error(e); }
      }, 800);
    }
    // 清除 URL 裡的參數，避免重整後仍帶著
    if (window.location.search) history.replaceState(null, '', window.location.pathname);
  })();

  // ══ 主題系統 ══
  (function() {
    var STYLE_MODES = {
      navy:    { dark:'navy-dark',    light:'navy-light'    },
      forest:  { dark:'forest-dark',  light:'forest-light'  },
      amber:   { dark:'amber-dark',   light:'amber-light'   },
      minimal: { dark:'minimal-dark', light:'minimal-light' },
      rose:    { dark:'rose-dark',    light:'rose-light'    },
      oled:    { dark:'oled-dark',    light:'oled-dark'     },
    };
    var DARK_ONLY = ['oled'];
    var _style = 'navy';
    var _mode  = 'system';

    function _applyTheme() {
      var sys = window.matchMedia('(prefers-color-scheme: dark)').matches;
      var eff = _mode === 'system' ? (sys ? 'dark' : 'light') : _mode;
      if (DARK_ONLY.indexOf(_style) >= 0) eff = 'dark';
      var themeVal = (STYLE_MODES[_style] || STYLE_MODES.navy)[eff];
      document.documentElement.setAttribute('data-theme', themeVal);
      document.body.setAttribute('data-theme', themeVal);
      ['dark','light','system'].forEach(function(m) {
        var btn = document.getElementById('tp-btn-' + m);
        if (btn) btn.classList.toggle('active', m === _mode);
      });
      Object.keys(STYLE_MODES).forEach(function(s) {
        var card = document.getElementById('tp-card-' + s);
        if (card) card.classList.toggle('selected', s === _style);
      });
      var isOled = DARK_ONLY.indexOf(_style) >= 0;
      ['light','system'].forEach(function(m) {
        var btn = document.getElementById('tp-btn-' + m);
        if (btn) { btn.disabled = isOled; btn.style.opacity = isOled ? '0.4' : '1'; }
      });
    }

    window._tpSetMode = function(m) {
      _mode = m; localStorage.setItem('up_mode', m); _applyTheme();
      fetch('/api/theme', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({mode:m})}).catch(function(){});
    };
    window._tpAdminSetStyle = function(s) { _style = s; localStorage.setItem("up_style", s); _applyTheme(); };
    window._tpSaveStyle = function() {
      fetch('/api/theme', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body:JSON.stringify({style:_style})
      }).then(function(r){ return r.json(); }).then(function(d) {
        if (d.ok) {
          localStorage.setItem('up_style', _style);
          var msg = document.getElementById('tp-save-msg');
          if (msg) { msg.style.display='block'; setTimeout(function(){msg.style.display='none';},3000); }
        }
      }).catch(function(){});
    };

    // 立即套用快取防閃白（html + body 都設，與其他工具一致）
    (function() {
      var s = localStorage.getItem('up_style') || 'navy';
      var m = localStorage.getItem('up_mode') || 'system';
      var sys = window.matchMedia('(prefers-color-scheme: dark)').matches;
      var eff = m === 'system' ? (sys ? 'dark' : 'light') : m;
      if (DARK_ONLY.indexOf(s) >= 0) eff = 'dark';
      var themeVal = (STYLE_MODES[s] || STYLE_MODES.navy)[eff];
      document.documentElement.setAttribute('data-theme', themeVal);
      document.body.setAttribute('data-theme', themeVal);
    })();

    document.addEventListener('DOMContentLoaded', function() {
      _mode = localStorage.getItem('up_mode') || 'system';
      _style = localStorage.getItem('up_style') || 'navy';
      _applyTheme();
      // 無條件從 Firestore 讀取 style 和 mode，與 Portal/其他工具同步
      fetch('/api/theme').then(function(r){ return r.json(); }).then(function(d) {
        var changed = false;
        if (d.style && d.style !== _style) { _style = d.style; localStorage.setItem('up_style', _style); changed = true; }
        if (d.mode != null && d.mode !== _mode) { _mode = d.mode; localStorage.setItem('up_mode', _mode); changed = true; }
        if (changed) _applyTheme();
      }).catch(function(){});
      // 管理員顯示儲存按鈕
      var adminEl = document.getElementById('tp-admin-only');
      if (adminEl) {
        fetch('/api/me').then(function(r){ return r.json(); }).then(function(u) {
          if (u.is_admin) adminEl.style.display = 'block';
        }).catch(function(){});
      }
      window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', function() {
        if (_mode === 'system') _applyTheme();
      });
    });
  })();
</script>
<script>
/* ── 更多選單（手機） ── */
function toggleMoreMenu(){
  var m=document.getElementById('more-menu'),o=document.getElementById('more-menu-overlay');
  var isOpen=m.style.display!=='none'&&m.style.display!=='';
  m.style.display=isOpen?'none':'block';
  o.style.display=isOpen?'none':'block';
}
function closeMoreMenu(){ toggleMoreMenu(); }
/* 手機才顯示底部 Tab Bar */
(function(){
  var tb=document.getElementById('app-tab-bar');
  if(!tb)return;
  function chk(){tb.style.display=window.innerWidth<=767?'block':'none';}
  chk();
  window.addEventListener('resize',chk);
})();

  // ── Global feedback dialog ──
  document.addEventListener("DOMContentLoaded", function() {
    const overlay = document.getElementById("gf-overlay");
    const cancelBtn = document.getElementById("gf-cancel");
    const submitBtn = document.getElementById("gf-submit");
    const textEl = document.getElementById("gf-text");
    const catEl = document.getElementById("gf-category");
    const toast = document.getElementById("gf-toast");
    if (!overlay || !cancelBtn || !submitBtn) return;

    cancelBtn.addEventListener("click", () => { overlay.classList.remove("show"); });
    overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.classList.remove("show"); });

    submitBtn.addEventListener("click", () => {
      const text = textEl.value.trim();
      if (!text) { textEl.style.borderColor = "var(--danger)"; return; }
      textEl.style.borderColor = "";
      fetch("/api/general-feedback", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text, category: catEl.value }),
      }).then(r => r.json()).then(d => {
        toast.textContent = "✅ 感謝您的意見！已記錄。";
        toast.style.display = "block";
        textEl.value = "";
        setTimeout(() => { toast.style.display = "none"; overlay.classList.remove("show"); }, 1500);
      });
    });
  });
</script>
  <!-- 通用意見反饋對話框 -->
  <div class="gf-overlay" id="gf-overlay">
    <div class="gf-dialog">
      <h3>💬 意見反饋</h3>
      <select id="gf-category">
        <option value="功能建議">功能建議</option>
        <option value="分類錯誤">分類錯誤回報</option>
        <option value="資料問題">資料不準確</option>
        <option value="介面體驗">介面體驗改進</option>
        <option value="其他">其他</option>
      </select>
      <textarea id="gf-text" placeholder="請輸入您的意見或建議…"></textarea>
      <div class="gf-actions">
        <button id="gf-cancel">取消</button>
        <button id="gf-submit" class="primary">送出</button>
      </div>
      <div class="gf-toast" id="gf-toast" style="display:none;"></div>
    </div>
  </div>
  <link rel="stylesheet" href="https://real-estate-portal-334765337861.asia-east1.run.app/static/feedback-widget.css">
  <script src="https://real-estate-portal-334765337861.asia-east1.run.app/static/feedback-widget.js"></script>
  <script>FeedbackWidget.init({ tool: 'library' });</script>
  <link rel="stylesheet" href="https://real-estate-portal-334765337861.asia-east1.run.app/static/upload-menu.css">
  <script src="https://real-estate-portal-334765337861.asia-east1.run.app/static/upload-menu.js"></script>
  <script>
    /* 右鍵 / 長按選單：賣方頭像 + 圖檔上傳 */
    document.addEventListener('DOMContentLoaded', function() {
      if (!window.UploadMenu) return;
      var avatarWrap = document.getElementById('sl-avatar-wrap');
      if (avatarWrap) {
        window.UploadMenu.attach(avatarWrap, {
          paste: true, file: true, camera: true,
          accept: 'image/*', multiple: false,
          onFiles: function(files) {
            if (files[0] && typeof slAvatarUpload === 'function') {
              slAvatarUpload({ files: [files[0]], value: '' });
            }
          },
        });
      }
      var filesBtn = document.getElementById('sl-files-upload-btn');
      if (filesBtn) {
        window.UploadMenu.attach(filesBtn, {
          paste: true, file: true, camera: true,
          accept: 'image/*,.pdf', multiple: true,
          onFiles: function(files) {
            if (typeof slFilesUpload === 'function') {
              slFilesUpload({ files: files, value: '' });
            }
          },
        });
      }
    });
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    os.makedirs(USERS_DIR, exist_ok=True)
    port = int(os.environ.get("PORT", "5004"))
    app.run(debug=bool(os.environ.get("FLASK_DEBUG")), host="0.0.0.0", port=port)
