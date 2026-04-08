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

try:
    from dotenv import load_dotenv
    _dir = os.path.dirname(os.path.abspath(__file__))
    for p in (os.path.join(_dir, ".env"), os.path.join(_dir, "..", ".env")):
        if os.path.isfile(p):
            load_dotenv(p, override=False)
            break
except Exception:
    pass

# Firestore（有環境就啟用，否則 None）
try:
    from google.cloud import firestore as _firestore
    _db = None  # 延遲初始化
except ImportError:
    _firestore = None
    _db = None


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
        return jsonify(obj)

    # 一般個人庫邏輯
    target = request.args.get("user", "").strip() or email
    if not _can_access(email, target, _is_admin(email)):
        return jsonify({"error": "無權限"}), 403
    obj = _load_object(target, obj_id)
    if not obj:
        return jsonify({"error": "物件不存在"}), 404
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


def _get_sheets_service():
    """建立 Sheets API service（讀寫權限）。"""
    import google.auth
    from googleapiclient.discovery import build
    creds, _ = google.auth.default(scopes=[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/spreadsheets",   # 讀寫（原為 readonly）
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
        if result.get("ok"):
            return jsonify({"ok": True, "total": len(seq_to_selling),
                            "updated": result["updated"],
                            "message": f"已掃描 {len(seq_to_selling)} 筆，回寫 {result['updated']} 筆"})
        else:
            return jsonify({"ok": False, "error": result.get("error", "未知錯誤")}), 500
    except Exception as e:
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
    """正規化欄位值供 ACCESS 比對（去單位、統一日期格式）"""
    v = str(val).strip() if val is not None else ""
    if not v:
        return ""
    if field in _ACCESS_NUM_FIELDS:
        # 去萬、坪、元、逗號等後轉 float 字串
        cleaned = re.sub(r'[萬坪元,，\s]', '', v)
        try:
            return str(round(float(cleaned), 2))
        except Exception:
            return v
    if field in _ACCESS_DATE_FIELDS:
        # 民國年 → 西元：113年1月15日 → 2024/01/15
        m = re.match(r'^(\d{2,3})[\s年/\-](\d{1,2})[\s月/\-](\d{1,2})', v)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if y < 1000:
                y += 1911
            return f"{y}/{mo:02d}/{d:02d}"
        # 已是西元格式
        m2 = re.match(r'^(\d{4})[\-/](\d{1,2})[\-/](\d{1,2})', v)
        if m2:
            return f"{m2.group(1)}/{int(m2.group(2)):02d}/{int(m2.group(3)):02d}"
        return v
    # 一般字串：壓縮空白
    return re.sub(r'\s+', ' ', v)

def _access_make_key(d):
    """建立比對主鍵：委託編號正規化；空白時 fallback → 案名 + 物件地址"""
    comm = re.sub(r'\.0$', '', str(d.get("委託編號", "") or "").strip())
    comm = re.sub(r'\s+', ' ', comm).strip()
    if comm:
        return ("comm", comm)
    name = re.sub(r'\s+', '', str(d.get("案名", "") or "").strip())
    addr = re.sub(r'\s+', '', str(d.get("物件地址", "") or "").strip())
    return ("name_addr", f"{name}|{addr}")

def _access_row_to_dict(headers, row):
    """把一行資料轉成 {欄位名: 值} dict（按 header 名稱對應）"""
    d = {}
    for i, h in enumerate(headers):
        if h and h.strip():
            d[h.strip()] = row[i].strip() if i < len(row) else ""
    return d


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
    new_sheet_id   = (data.get("new_sheet_id") or "").strip()
    new_sheet_name = (data.get("new_sheet_name") or "").strip()
    if not new_sheet_id:
        return jsonify({"error": "請提供新 Sheets ID"}), 400

    try:
        service = _get_sheets_service()

        # ── 讀原始 Sheets（只取 A~AU 欄，跳過 AV+ 自訂欄）──
        orig_result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A1:AU9999"
        ).execute()
        orig_all = orig_result.get("values", [])
        if len(orig_all) < 4:
            return jsonify({"error": "原始 Sheets 資料不足（需至少4行）"}), 400

        orig_headers = [h.strip() for h in orig_all[1]]  # 第 2 行是 header

        def _is_hdr(row):
            return bool(row) and row[0].strip() == (orig_headers[0] if orig_headers else "")

        # 第 4 行起為資料；同時記錄每行的 Sheets 列號（1-based）
        orig_data = []
        orig_row_numbers = []
        for i, row in enumerate(orig_all[3:]):
            if any(c.strip() for c in row) and not _is_hdr(row):
                orig_data.append(row)
                orig_row_numbers.append(4 + i)  # Sheets 第 4 行 = index 3 + 1-based

        # ── 讀新 Sheets ──
        if not new_sheet_name:
            # 自動取第一個分頁名稱
            meta = service.spreadsheets().get(spreadsheetId=new_sheet_id).execute()
            new_sheet_name = meta["sheets"][0]["properties"]["title"]

        new_result = service.spreadsheets().values().get(
            spreadsheetId=new_sheet_id,
            range=f"{new_sheet_name}!A1:AU9999"
        ).execute()
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
        new_data = [r for r in new_all[new_header_idx + 1:] if any(c.strip() for c in r)]
        # 過濾重複的 header 行
        new_data = [r for r in new_data if not (r and r[0].strip() == new_headers[0])]

        # ── 決定比較欄位（取兩邊 header 的交集，排除資料序號）──
        orig_hdr_set = set(h for h in orig_headers if h)
        new_hdr_set  = set(h for h in new_headers  if h)
        _IMPORTANT = ["案名", "售價(萬)", "委託到期日", "銷售中", "物件地址",
                      "經紀人", "委託編號", "物件類別", "地坪", "建坪", "室內坪"]
        all_common = [h for h in orig_headers if h and h in new_hdr_set and h != "資料序號"]
        # 重要欄位前置，其餘依原始順序
        compare_fields = [f for f in _IMPORTANT if f in orig_hdr_set and f in new_hdr_set] + \
                         [f for f in all_common if f not in _IMPORTANT]

        # ── 建立原始 Sheets index：key → {row_number, data_dict, seq} ──
        orig_index = {}
        for i, row in enumerate(orig_data):
            d = _access_row_to_dict(orig_headers, row)
            k = _access_make_key(d)
            orig_index[k] = {
                "row_number": orig_row_numbers[i],
                "data": d,
                "seq": d.get("資料序號", "").strip()
            }

        # ── 比對 ──
        added    = []
        modified = []
        new_keys = set()

        for new_row in new_data:
            nd = _access_row_to_dict(new_headers, new_row)
            k  = _access_make_key(nd)
            new_keys.add(k)

            if k not in orig_index:
                # 新增：Access 有但原始 Sheets 沒有
                added.append({
                    "key":          str(k),
                    "data":         {f: nd.get(f, "") for f in new_headers if f},
                    "display_name": nd.get("案名", "") or nd.get("委託編號", "（未知案名）"),
                    "price":        nd.get("售價(萬)", ""),
                    "agent":        nd.get("經紀人", ""),
                    "comm":         nd.get("委託編號", ""),
                })
            else:
                # 比較差異（只看交集欄位）
                od = orig_index[k]["data"]
                changed_fields = []
                for field in compare_fields:
                    nv = _access_norm_val(field, nd.get(field, ""))
                    ov = _access_norm_val(field, od.get(field, ""))
                    if nv != ov:
                        changed_fields.append({
                            "field": field,
                            "old":   od.get(field, ""),
                            "new":   nd.get(field, ""),
                        })
                if changed_fields:
                    modified.append({
                        "key":           str(k),
                        "row_in_orig":   orig_index[k]["row_number"],
                        "seq":           orig_index[k]["seq"],
                        "display_name":  od.get("案名", "") or str(k),
                        "changed_fields": changed_fields,
                        "new_data":      {f: nd.get(f, "") for f in new_headers if f},
                    })

        # 可能下架：原始 Sheets 有、Access 沒有
        removed = []
        for k, entry in orig_index.items():
            if k not in new_keys:
                d = entry["data"]
                removed.append({
                    "key":          str(k),
                    "seq":          entry["seq"],
                    "display_name": d.get("案名", "") or str(k),
                    "comm":         d.get("委託編號", ""),
                })

        return jsonify({
            "ok":             True,
            "added":          added,
            "modified":       modified,
            "removed":        removed,
            "compare_fields": compare_fields,
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
    apply_modified = data.get("apply_modified", [])
    apply_added    = data.get("apply_added", [])

    if not apply_modified and not apply_added:
        return jsonify({"ok": True, "modified_count": 0, "added_count": 0,
                        "message": "無變更需套用"})

    try:
        service = _get_sheets_service()

        # 讀取原始 Sheets header 行，用名稱定位欄號（不用固定數字，防欄位順序改變出錯）
        hdr_result = service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A2:AU2"
        ).execute()
        raw_headers = hdr_result.get("values", [[]])[0]
        headers = [h.strip() for h in raw_headers]
        header_to_col = {h: i for i, h in enumerate(headers) if h}

        # ── 修改：逐欄位建立 batchUpdate 請求 ──
        value_updates = []
        for item in apply_modified:
            row_num = item.get("row_in_orig")
            fields  = item.get("fields", {})
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

        # ── 新增：找最大資料序號後追加 ──
        added_count = 0
        if apply_added:
            orig_all = service.spreadsheets().values().get(
                spreadsheetId=SHEET_ID,
                range=f"{SHEET_NAME}!A1:AU9999"
            ).execute().get("values", [])

            orig_headers = [h.strip() for h in (orig_all[1] if len(orig_all) > 1 else headers)]
            seq_col_idx  = next((i for i, h in enumerate(orig_headers) if h == "資料序號"), -1)

            max_seq = 0
            if seq_col_idx >= 0:
                for row in orig_all[3:]:
                    if seq_col_idx < len(row):
                        try:
                            v = int(float(row[seq_col_idx].strip()))
                            if v > max_seq:
                                max_seq = v
                        except Exception:
                            pass

            new_seq = max_seq + 1
            new_rows = []
            for new_item in apply_added:
                row_vals = []
                for h in orig_headers:
                    if h == "資料序號":
                        row_vals.append(str(new_seq))
                        new_seq += 1
                    else:
                        row_vals.append(new_item.get(h, ""))
                new_rows.append(row_vals)

            if new_rows:
                service.spreadsheets().values().append(
                    spreadsheetId=SHEET_ID,
                    range=f"{SHEET_NAME}!A1",
                    valueInputOption="USER_ENTERED",
                    insertDataOption="INSERT_ROWS",
                    body={"values": new_rows}
                ).execute()
                added_count = len(new_rows)

        # ── 批次更新修改的欄位 ──
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

        # 讀取現有 Firestore 文件 ID 集合，用來偵測已刪除的資料
        existing_ids = {doc.id for doc in col.select([]).stream()}

        written = skipped = deleted = 0
        seen_ids = set()

        for row in data_rows:
            d = _row_to_doc(headers, row)
            seq = str(d.get("資料序號", "")).strip()
            if not seq or not seq.isdigit():
                skipped += 1
                continue

            doc_id = seq
            seen_ids.add(doc_id)
            d["_synced_at"] = started
            # 標記組織歸屬，讓不同公司的資料互相隔離
            if org_id:
                d["org_id"] = org_id
            # merge=True：只更新有值的欄位，不覆蓋 Firestore 中沒在 Sheets 的欄位
            # 特別保護「銷售中」：由 Word 審查或手動回寫管理，不被 Sheets 覆蓋
            col.document(doc_id).set(d, merge=True)
            written += 1
            if written % 200 == 0:
                log.info(f"進度：{written}/{len(data_rows)}")

        # 刪除 Firestore 中已不存在於 Sheets 的文件（避免髒資料）
        to_delete = existing_ids - seen_ids
        for doc_id in to_delete:
            col.document(doc_id).delete()
            deleted += 1

        result = {
            "ok": True,
            "written": written,
            "skipped": skipped,
            "deleted": deleted,
            "started": started,
            "finished": datetime.now(timezone.utc).isoformat()
        }
        log.info(f"同步完成：{result}")

        # 同步完成後，更新物件快速搜尋索引（存入 Firestore meta 文件）
        try:
            _rebuild_prop_index(db, col)
            log.info("物件搜尋索引更新完成")
        except Exception as ex:
            log.warning(f"索引更新失敗（不影響同步結果）: {ex}")

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
            "段別", "地號"  # FOUNDI 土地查詢用
        }
        slim = [{k: r[k] for k in card_fields if k in r} for r in page_data]
        # 補上 id，並將「銷售中」統一轉為布林值，避免前端收到字串導致判斷錯誤
        for orig, s in zip(page_data, slim):
            s["id"] = orig["id"]
            s["銷售中"] = _is_selling(orig)  # 統一轉布林

        return jsonify({
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": (total + per_page - 1) // per_page,
            "items": slim
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
    docs = list(col.stream())

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
        """不動產硬資料：面積欄位精確比對（2% 容差）
        同一物件的實體面積不會因換手或改價而改變，是最可靠的比對基準。
        回傳 (score, has_hard_match)
        """
        score = 0
        has_hard = False
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
            if _sm(wv, dv, 0.02):           # 2% 容差：硬資料命中
                score += pts
                has_hard = True
            elif not _sm(wv, dv, 0.20):     # 差距超過 20%：幾乎確定是不同物件
                score -= pts
        return score, has_hard

    # 從 Firestore 載入所有物件並建立索引（Word 是主體，Firestore 是查詢對象）
    col     = db.collection("company_properties")
    db_docs = list(col.stream())

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
        # 地址索引（正規化去空白）
        dba = re.sub(r'\s+', '', str(dd.get('物件地址', '') or ''))
        if dba and len(dba) >= 6:
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
                area_sc, has_hard = _hard_area_score(row, cand)
                s = area_sc
                # 售價：輔助參考（正常波動不扣重分）
                dbp = _pn(cand.get('售價(萬)'))
                ps  = _sm(price, dbp, 0.05)
                if ps is True:                            s += 2
                elif ps is False and not _sm(price, dbp, 0.30): s -= 2
                # 經紀人：軟資料，換手正常 → 只加分不扣分
                s += _agent_score(cg, agent)
                if cand.get('委託到期日'): s += 1
                if s > best_score:
                    best_score = s
                    best = cand
                    best_has_hard = has_hard
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
                            area_sc, _ = _hard_area_score(row, cand)  # 面積也納入近似候選評分
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
                    area_sc2, _ = _hard_area_score(row, cand)
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
                nm_name_match = _nn(nm_name) == _nn(name)   # 案名規範化後相同
                # 案名完全相同 → 升高信心（主迴圈漏配，近似搜尋補救）
                # 案名不同但其他資料吻合 → 升中信心，讓使用者確認
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
            "match_by":      match_by,
            "score":         score,
            "has_hard":      best_has_hard,
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
        if score == 99 or (match_by in ("委託號碼", "資料序號", "物件地址") and not addr_mismatch) or score >= 3:
            high.append(item)
        elif score >= 0:
            medium.append(item)
        elif best_has_hard:
            # 有硬資料（面積）且明顯衝突 → 確實是不同物件
            item["conflict_reason"] = f"面積不符（分數 {score}）"
            conflict.append(item)
        else:
            # 無面積資料可驗證，僅軟資料不符 → 歸中信心，人工確認
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
        return jsonify({"starred": new_val})
    except Exception as e:
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
    """取得準賣方卡片的自訂排列順序。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        doc = db.collection("user_settings").document(email).get()
        if doc.exists:
            return jsonify({"order": doc.to_dict().get("seller_sort_order", [])})
        return jsonify({"order": []})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/sort-order", methods=["PUT"])
def api_sellers_sort_order_put():
    """儲存準賣方卡片的自訂排列順序。"""
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
        db.collection("user_settings").document(email).set(
            {"seller_sort_order": order}, merge=True)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers", methods=["GET"])
def api_sellers_list():
    """取得目前登入者的所有準賣方（依最後追蹤時間排序）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        docs = db.collection("seller_prospects").where("created_by", "==", email).stream()
        items = []
        for doc in docs:
            d = doc.to_dict()
            d["id"] = doc.id
            items.append(d)
        # 依最後追蹤時間降冪排序，未追蹤的排最後
        items.sort(key=lambda x: x.get("last_contact_at") or "", reverse=True)
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers", methods=["POST"])
def api_sellers_create():
    """新增準賣方。"""
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
    """取得單筆準賣方資料。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        doc = db.collection("seller_prospects").document(seller_id).get()
        if not doc.exists:
            return jsonify({"error": "找不到此準賣方"}), 404
        d = doc.to_dict()
        if d.get("created_by") != email and not _is_admin(email):
            return jsonify({"error": "無權限"}), 403
        d["id"] = doc.id
        return jsonify(d)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/<seller_id>", methods=["PUT"])
def api_seller_update(seller_id):
    """更新準賣方資料。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    data = request.get_json(silent=True) or {}
    try:
        ref = db.collection("seller_prospects").document(seller_id)
        doc = ref.get()
        if not doc.exists:
            return jsonify({"error": "找不到此準賣方"}), 404
        item = doc.to_dict()
        if item.get("created_by") != email and not _is_admin(email):
            return jsonify({"error": "無權限"}), 403
        update = {
            "name":          str(data.get("name", item.get("name", ""))).strip(),
            "phone":         str(data.get("phone", item.get("phone", ""))).strip(),
            "address":       str(data.get("address", item.get("address", ""))).strip(),
            "land_number":   str(data.get("land_number", item.get("land_number", ""))).strip(),
            "category":      str(data.get("category", item.get("category", ""))).strip(),
            "owner_price":   data.get("owner_price", item.get("owner_price")),
            "suggest_price": data.get("suggest_price", item.get("suggest_price")),
            "source":        str(data.get("source", item.get("source", ""))).strip(),
            "status":        data.get("status", item.get("status", "培養中")),
            "note":          str(data.get("note", item.get("note", ""))).strip(),
            "card_color":    str(data.get("card_color", item.get("card_color", ""))).strip(),
            "updated_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        ref.update(update)
        return jsonify({"ok": True, "id": seller_id, **update})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/<seller_id>", methods=["DELETE"])
def api_seller_delete(seller_id):
    """刪除準賣方（同時刪除其互動記事）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        ref = db.collection("seller_prospects").document(seller_id)
        doc = ref.get()
        if not doc.exists:
            return jsonify({"error": "找不到此準賣方"}), 404
        if doc.to_dict().get("created_by") != email and not _is_admin(email):
            return jsonify({"error": "無權限"}), 403
        # 刪除互動記事
        contacts = db.collection("seller_contacts").where("seller_id", "==", seller_id).stream()
        for c in contacts:
            c.reference.delete()
        ref.delete()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── 準賣方互動記事 ──

@app.route("/api/sellers/<seller_id>/contacts", methods=["GET"])
def api_seller_contacts_list(seller_id):
    """取得準賣方的所有互動記事（依時間降冪）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        docs = db.collection("seller_contacts").where("seller_id", "==", seller_id).stream()
        items = []
        for doc in docs:
            d = doc.to_dict()
            d["id"] = doc.id
            items.append(d)
        items.sort(key=lambda x: x.get("contact_at", ""), reverse=True)
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/<seller_id>/contacts", methods=["POST"])
def api_seller_contact_create(seller_id):
    """新增互動記事。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    data = request.get_json(silent=True) or {}
    content = str(data.get("content", "")).strip()
    if not content:
        return jsonify({"error": "請填寫互動內容"}), 400
    try:
        contact_at = (data.get("contact_at") or "").strip()
        if not contact_at:
            contact_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        doc = {
            "seller_id":  seller_id,
            "content":    content,
            "contact_at": contact_at,
            "created_by": email,
        }
        ref = db.collection("seller_contacts").document()
        ref.set(doc)
        _recalc_seller_last_contact(db, seller_id)
        return jsonify({"ok": True, "id": ref.id, **doc})
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
    data = request.get_json(silent=True) or {}
    try:
        ref = db.collection("seller_contacts").document(contact_id)
        doc = ref.get()
        if not doc.exists:
            return jsonify({"error": "找不到此記事"}), 404
        item = doc.to_dict()
        if item.get("created_by") != email and not _is_admin(email):
            return jsonify({"error": "無權限"}), 403
        update = {
            "content":    str(data.get("content", item.get("content", ""))).strip(),
            "contact_at": (data.get("contact_at") or item.get("contact_at", "")).strip(),
        }
        ref.update(update)
        _recalc_seller_last_contact(db, seller_id)
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
        ref = db.collection("seller_contacts").document(contact_id)
        doc = ref.get()
        if not doc.exists:
            return jsonify({"error": "找不到此記事"}), 404
        if doc.to_dict().get("created_by") != email and not _is_admin(email):
            return jsonify({"error": "無權限"}), 403
        ref.delete()
        _recalc_seller_last_contact(db, seller_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── 準賣方頭像上傳 ──

@app.route("/api/sellers/<seller_id>/avatar", methods=["POST"])
def api_seller_avatar_upload(seller_id):
    """上傳準賣方頭像（存入 GCS，URL 寫回 Firestore）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    if "file" not in request.files:
        return jsonify({"error": "請選擇圖片"}), 400
    f = request.files["file"]
    ext = os.path.splitext(f.filename.lower())[1]
    if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        return jsonify({"error": "僅支援 jpg/png/webp/gif"}), 400
    try:
        # 確認有權限
        doc = db.collection("seller_prospects").document(seller_id).get()
        if not doc.exists:
            return jsonify({"error": "找不到此準賣方"}), 404
        if doc.to_dict().get("created_by") != email and not _is_admin(email):
            return jsonify({"error": "無權限"}), 403

        raw = f.read()
        b = _get_gcs_bucket()
        if b is None:
            return jsonify({"error": "GCS 未設定"}), 503
        # 固定路徑：sellers/{seller_id}/avatar{ext}（覆蓋舊頭像）
        gcs_path = f"sellers/{seller_id}/avatar{ext}"
        blob = b.blob(gcs_path)
        mime = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
                "webp": "image/webp", "gif": "image/gif"}.get(ext.lstrip("."), "image/jpeg")
        blob.upload_from_string(raw, content_type=mime)
        blob.make_public()
        url = blob.public_url
        # 寫回 Firestore
        db.collection("seller_prospects").document(seller_id).update({
            "avatar_url": url,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })
        return jsonify({"ok": True, "url": url})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── 準賣方相關圖檔 ──

@app.route("/api/sellers/<seller_id>/files", methods=["POST"])
def api_seller_file_upload(seller_id):
    """上傳相關圖檔（存入 GCS，清單寫回 Firestore）。"""
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
        doc_ref = db.collection("seller_prospects").document(seller_id)
        doc = doc_ref.get()
        if not doc.exists:
            return jsonify({"error": "找不到此準賣方"}), 404
        if doc.to_dict().get("created_by") != email and not _is_admin(email):
            return jsonify({"error": "無權限"}), 403

        raw = f.read()
        b = _get_gcs_bucket()
        if b is None:
            return jsonify({"error": "GCS 未設定"}), 503
        # 用 uuid 避免重名
        file_id = str(uuid.uuid4())[:8]
        safe_name = re.sub(r"[^\w.\-]", "_", f.filename)
        gcs_path = f"sellers/{seller_id}/files/{file_id}_{safe_name}"
        blob = b.blob(gcs_path)
        mime_map = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
                    "webp": "image/webp", "gif": "image/gif", "pdf": "application/pdf"}
        mime = mime_map.get(ext.lstrip("."), "application/octet-stream")
        blob.upload_from_string(raw, content_type=mime)
        blob.make_public()
        url = blob.public_url
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 把新檔案加到 Firestore files 陣列
        item = {"file_id": file_id, "name": safe_name, "url": url,
                "gcs_path": gcs_path, "uploaded_at": now}
        existing = doc.to_dict().get("files", []) or []
        existing.append(item)
        doc_ref.update({"files": existing, "updated_at": now})
        return jsonify({"ok": True, **item})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sellers/<seller_id>/files/<file_id>", methods=["DELETE"])
def api_seller_file_delete(seller_id, file_id):
    """刪除相關圖檔（從 GCS 和 Firestore 同時移除）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        doc_ref = db.collection("seller_prospects").document(seller_id)
        doc = doc_ref.get()
        if not doc.exists:
            return jsonify({"error": "找不到此準賣方"}), 404
        d = doc.to_dict()
        if d.get("created_by") != email and not _is_admin(email):
            return jsonify({"error": "無權限"}), 403
        files = d.get("files", []) or []
        target = next((x for x in files if x.get("file_id") == file_id), None)
        if not target:
            return jsonify({"error": "找不到此檔案"}), 404
        # 從 GCS 刪除
        b = _get_gcs_bucket()
        if b:
            try:
                b.blob(target["gcs_path"]).delete()
            except Exception:
                pass
        # 從 Firestore 移除
        new_files = [x for x in files if x.get("file_id") != file_id]
        doc_ref.update({"files": new_files, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
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

  <!-- 管理員工具列（只有管理員看得到） -->
  <div id="cp-sync-bar" class="hidden mb-3 flex flex-wrap items-center gap-3 rounded-xl px-4 py-2" style="background:var(--bg-t);border:1px solid var(--bd);">
    <span class="flex-1" style="font-size:0.75rem;color:var(--txs);">上次同步：<span id="cp-last-sync" style="color:var(--tx);">讀取中…</span></span>
    <button id="cp-sync-btn" onclick="cpTriggerSync()"
      class="px-4 py-1.5 rounded-lg bg-amber-600 hover:bg-amber-500 text-white text-xs font-semibold transition">
      🔄 同步 Sheets
    </button>
    <!-- 比對審查：上傳 CSV → 審查配對 → 確認後寫入（僅日盛房屋管理員） -->
    <label class="flex items-center gap-1 px-4 py-1.5 rounded-lg bg-teal-700 hover:bg-teal-600 text-white text-xs font-semibold transition cursor-pointer"
      title="上傳 export_word_table.py 產出的 CSV，審查高/中信心配對後寫入 Firestore">
      🔍 比對審查
      <input type="file" accept=".csv,.json,.doc,.docx" multiple class="hidden" onchange="cpOpenReview(this)">
    </label>
    <!-- 回寫銷售中 → Sheets -->
    <button id="cp-writeback-btn" onclick="cpWritebackSelling()"
      class="px-4 py-1.5 rounded-lg bg-indigo-700 hover:bg-indigo-600 text-white text-xs font-semibold transition"
      title="把 Firestore 目前所有物件的「銷售中」狀態，一次回寫到 Google Sheets（只動銷售中欄）">
      📤 回寫銷售中
    </button>
    <!-- ACCESS 比對更新 -->
    <button onclick="openAccessCompareModal()"
      class="px-4 py-1.5 rounded-lg bg-emerald-700 hover:bg-emerald-600 text-white text-xs font-semibold transition"
      title="把最新 Access 資料貼到新 Sheets，比對與我的物件庫有何差異，選擇要套用哪些變更">
      📋 ACCESS比對
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
              <p style="color:var(--txs);font-size:12px;margin:0;">上傳由本機工具 <code style="background:var(--bg-p);padding:1px 5px;border-radius:4px;color:var(--ac);">export_word_table.py</code> 產出的 CSV（公寓/房屋/農地/建地）及 <code style="background:var(--bg-p);padding:1px 5px;border-radius:4px;color:var(--ac);">word_meta.json</code>。系統分析與 Firestore 的配對結果，分為<strong style="color:var(--ok);">高信心</strong>、<strong style="color:var(--warn);">中信心</strong>、問題三組，讓你逐一確認後寫入。</p>
              <p style="color:var(--txm);font-size:11px;margin:6px 0 0;">💡 一次可選取 4 個 CSV + 1 個 word_meta.json，共 5 個檔案一起選取。</p>
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
            <div style="display:flex;flex-direction:column;gap:7px;">
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--tx);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">1</span><span style="color:var(--txs);font-size:12px;">本機執行：<code style="background:var(--bg-p);padding:1px 6px;border-radius:4px;color:var(--ac);font-size:11px;">python3 export_word_table.py</code><br><span style="color:var(--txm);font-size:11px;">→ 產出 4 個 CSV + word_meta.json（於 ~/Projects/）</span></span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--tx);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">2</span><span style="color:var(--txs);font-size:12px;">回此頁按「<strong style="color:var(--ac);">🔍 比對審查</strong>」，選取 4 個 CSV（可一併選 word_meta.json）</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--bg-h);color:var(--tx);border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">3</span><span style="color:var(--txs);font-size:12px;">審查介面顯示三組結果：✅ 高信心全選套用、⚠️ 中信心逐一確認、❓ 問題筆數供參考</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:var(--ok);color:#fff;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">✓</span><span style="color:var(--ok);font-size:12px;"><strong>按「套用確認的配對」→ 直接寫入 Firestore，完成！</strong></span></div>
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
            <p style="font-size:12px;color:var(--txs);margin:0 0 10px;">以下物件配對有些不確定，請逐一確認。✅ 確認配對，❌ 跳過此筆。</p>
            <div id="rv-medium-list" style="display:flex;flex-direction:column;gap:8px;"></div>
          </div>
          <!-- 問題（衝突 + 未配對） -->
          <div id="rv-pane-issues" style="display:none;">
            <p style="font-size:12px;color:var(--txs);margin:0 0 10px;">以下 Word 條目在 Firestore 找不到對應（新物件尚未匯入），或同名但特徵衝突。僅供參考，不會自動套用。</p>
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

  <!-- ACCESS 比對更新 Modal（管理員限定） -->
  <div id="ac-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:650;align-items:flex-start;justify-content:center;padding-top:28px;"
    onclick="if(event.target===this)document.getElementById('ac-modal').style.display='none'">
    <div style="background:var(--bg-s);border:1px solid var(--bd);border-radius:16px;width:96%;max-width:820px;max-height:90vh;display:flex;flex-direction:column;box-shadow:var(--sh);overflow:hidden;">

      <!-- Header -->
      <div style="padding:16px 22px 12px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:10px;flex-shrink:0;">
        <span style="font-size:15px;font-weight:700;color:var(--tx);">📋 ACCESS 比對更新</span>
        <span id="ac-subtitle" style="font-size:12px;color:var(--txs);"></span>
        <button onclick="document.getElementById('ac-modal').style.display='none'"
          style="margin-left:auto;background:none;border:none;color:var(--txm);font-size:20px;cursor:pointer;line-height:1;">✕</button>
      </div>

      <!-- 輸入區 -->
      <div id="ac-input-section" style="padding:16px 22px;border-bottom:1px solid var(--bd);flex-shrink:0;">
        <p style="font-size:12px;color:var(--txs);margin:0 0 12px;">
          把公司最新 Access 資料（欄位與主頁相同的 A~AU 欄）貼到一張全新的 Google Sheets，再把 Sheets ID 貼到下方開始比對。
        </p>
        <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;">
          <div style="flex:1;min-width:220px;">
            <label style="font-size:11px;color:var(--txs);display:block;margin-bottom:4px;">新 Sheets ID（必填）</label>
            <input id="ac-sheet-id" type="text" placeholder="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"
              style="width:100%;padding:7px 10px;border-radius:8px;border:1px solid var(--bd);background:var(--bg-t);color:var(--tx);font-size:12px;box-sizing:border-box;">
          </div>
          <div style="width:160px;">
            <label style="font-size:11px;color:var(--txs);display:block;margin-bottom:4px;">分頁名稱（空白=第一個分頁）</label>
            <input id="ac-sheet-name" type="text" placeholder="工作表1"
              style="width:100%;padding:7px 10px;border-radius:8px;border:1px solid var(--bd);background:var(--bg-t);color:var(--tx);font-size:12px;box-sizing:border-box;">
          </div>
          <button id="ac-compare-btn" onclick="accessRunCompare()"
            style="padding:8px 18px;border-radius:8px;background:var(--ac);color:#fff;border:none;font-size:13px;font-weight:700;cursor:pointer;white-space:nowrap;">
            開始比對
          </button>
        </div>
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
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">
              <label style="display:flex;align-items:center;gap:5px;font-size:12px;color:var(--txs);cursor:pointer;">
                <input type="checkbox" id="ac-mod-all" checked onchange="acToggleAll('mod',this)"> 全選
              </label>
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
        <div style="position:relative;width:64px;height:64px;flex-shrink:0;">
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
            <label class="px-3 py-1 rounded-lg text-xs font-semibold cursor-pointer text-white" style="background:var(--ac);">
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
<div id="cp-detail-modal" role="dialog" aria-modal="true"
  class="hidden fixed inset-0 z-[200] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm"
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
    _cpLastQuery = {
      keyword:   document.getElementById('cp-keyword').value.trim(),
      category:  Array.from(_cpSelected.cat).join(','),
      area:      Array.from(_cpSelected.area).join(','),
      price_min: document.getElementById('cp-price-min').value,
      price_max: document.getElementById('cp-price-max').value,
      status:    document.getElementById('cp-status').value,
      agent:     Array.from(_cpSelected.agent).join(','),
      sort:      (document.getElementById('cp-sort') || {}).value || 'serial_desc',
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
        var dt = data.uploaded_at ? new Date(data.uploaded_at).toLocaleDateString('zh-TW') : '';
        el.textContent = '總表：' + (data.filename || '') + '（' + dt + '，' + (data.count||0) + '筆）';
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
          // 更新總表日期顯示
          if (d.doc_date) {
            var el = document.getElementById('cp-doc-date');
            if (el) el.textContent = '📄 總表：' + d.doc_date;
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
    document.getElementById('rv-subtitle').textContent =
      'Word 共 ' + totalWord + ' 筆｜高信心 ' + d.high.length + ' ／ 中信心 ' + d.medium.length + ' ／ 問題 ' + issueCount;

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
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;text-transform:uppercase;">Word 物件總表</div>'
        + '<div style="font-size:13px;color:var(--tx);font-weight:600;">' + (item.csv_name || item.db_name) + '</div>'
        + fmtR('售價', item.csv_price!=null ? item.csv_price+' 萬' : '')
        + fmtHardCols(item, false)
        + fmtR('委託號', item.csv_comm)
        + fmtR('到期', item.csv_expiry)
        + fmtR('經紀人', item.csv_agent)
        + '</div>';
      var rightCol = '<div style="flex:1;min-width:0;padding-left:12px;border-left:1px solid var(--bd);">'
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;">FIRESTORE 現有</div>'
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
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;text-transform:uppercase;">Word 物件總表</div>'
        + '<div style="font-size:13px;color:var(--tx);font-weight:600;">' + (item.csv_name || item.db_name) + '</div>'
        + fmtR('售價', item.csv_price!=null ? item.csv_price+' 萬' : '')
        + fmtHardCols(item, false)
        + fmtR('委託號', item.csv_comm)
        + fmtR('到期', item.csv_expiry)
        + fmtR('經紀人', item.csv_agent)
        + '</div>';
      var rightCol = '<div style="flex:1;min-width:0;padding-left:12px;border-left:1px solid var(--bd);">'
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;">FIRESTORE 現有</div>'
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
        name_changed: item.name_changed, old_name: item.db_name, new_name: item.csv_name
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
        + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;">FIRESTORE 現有</div>'
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
          old_name: item.nm_name, new_name: item.csv_name
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
          + '<div style="font-size:10px;color:var(--txs);font-weight:600;margin-bottom:4px;">FIRESTORE 近似候選（分數 ' + item.nm_score + '）</div>'
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

  // 中信心：確認一筆
  function rvAcceptMedium(btn) {
    var docId = btn.dataset.docid;
    try {
      var data = JSON.parse(btn.dataset.item);
      _rvConfirmed[docId] = data;
    } catch(e) {
      _rvConfirmed[docId] = {doc_id: docId};
    }
    btn.textContent = '✅ 已確認';
    btn.disabled = true;
    btn.style.opacity = '0.6';
    btn.nextElementSibling.style.display = 'none';
    _rvUpdateCount();
  }

  // 中信心：跳過一筆
  function rvSkipMedium(btn) {
    btn.textContent = '跳過';
    btn.disabled = true;
    btn.style.opacity = '0.5';
    btn.previousElementSibling.style.display = 'none';
  }

  // 問題：確認近似候選是同一物件
  function rvAcceptUnmatched(btn) {
    var cardId = btn.getAttribute('data-cardid');
    var raw = btn.getAttribute('data-item').replace(/&quot;/g, '"');
    var item = JSON.parse(raw);
    _rvConfirmed[item.doc_id] = item;
    _rvUpdateCount();
    var card = document.getElementById(cardId);
    if (card) { card.style.opacity = '0.5'; card.style.borderColor = 'var(--ok)'; }
    btn.disabled = true; btn.textContent = '✅ 已確認';
    btn.nextElementSibling.style.display = 'none';
  }
  // 問題：略過此筆
  function rvSkipUnmatched(btn) {
    var cardId = btn.getAttribute('data-cardid');
    var card = document.getElementById(cardId);
    if (card) card.style.opacity = '0.4';
    btn.disabled = true; btn.textContent = '略過';
    btn.previousElementSibling.style.display = 'none';
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
      document.getElementById('cp-review-modal').style.display = 'none';
      // 上傳 word_meta.json（若有的話）
      _rvMetaFiles.forEach(function(jf){ _rvUploadMeta(jf); });
      _rvMetaFiles = [];
      // 重新整理物件列表
      setTimeout(function(){ cpFetch(); }, 600);
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
          return { items: all, total: data.total, allLoaded: true };
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
          html += '</div>';
          // 帶看摘要區（預設摺疊）
          if (BUYER_URL) {
            html += '<div id="showing-panel-' + safeId + '" class="hidden mt-2 pt-2" style="border-top:1px solid var(--bd);">'
                  + '<p style="font-size:0.75rem;color:var(--txm);text-align:center;padding:0.5rem 0;">載入中…</p></div>';
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

      document.getElementById('cp-detail-body').innerHTML = html || '<p class="text-slate-500">無資料</p>';
      document.getElementById('cp-detail-modal').classList.remove('hidden');
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
              toast('同步完成！', 'success');
            }
          }
        });
      }, 3000);
    }).catch(function(e){ toast('\u547c\u53eb\u5931\u6557: ' + e, 'error'); btn.disabled=false; });
  }

  // 一鍵回寫 Firestore 銷售中 → Google Sheets
  function cpWritebackSelling() {
    if (!confirm('確定要把 Firestore 所有物件的「銷售中」狀態回寫到 Google Sheets 嗎？（只更新銷售中欄，其他欄位不動）')) return;
    var btn = document.getElementById('cp-writeback-btn');
    btn.disabled = true;
    btn.textContent = '回寫中…';
    fetch('/api/sheets/writeback-selling', { method: 'POST' })
      .then(function(r){ return r.json(); })
      .then(function(d) {
        btn.disabled = false;
        btn.textContent = '📤 回寫銷售中';
        if (d.ok) {
          toast('✅ ' + d.message, 'success');
        } else {
          toast('❌ 回寫失敗：' + (d.error || '未知錯誤'), 'error');
        }
      })
      .catch(function(e){
        btn.disabled = false;
        btn.textContent = '📤 回寫銷售中';
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

  // 更新「套用 X 筆」計數
  function _acUpdateApplyCount() {
    var modCnt = document.querySelectorAll('#ac-list-mod input[type=checkbox]:checked').length;
    var addCnt = document.querySelectorAll('#ac-list-add input[type=checkbox]:checked').length;
    var total  = modCnt + addCnt;
    var el = document.getElementById('ac-apply-count');
    if (el) el.textContent = '已勾選：修改 ' + modCnt + ' 筆、新增 ' + addCnt + ' 筆（共 ' + total + ' 筆）';
  }

  // 開始比對
  function accessRunCompare() {
    var sheetId   = (document.getElementById('ac-sheet-id').value || '').trim();
    var sheetName = (document.getElementById('ac-sheet-name').value || '').trim();
    if (!sheetId) { toast('請輸入新 Sheets ID', 'error'); return; }

    var btn = document.getElementById('ac-compare-btn');
    btn.disabled = true;
    btn.textContent = '比對中…';
    document.getElementById('ac-loading').style.display = '';
    document.getElementById('ac-results').style.display = 'none';

    fetch('/api/access-compare', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ new_sheet_id: sheetId, new_sheet_name: sheetName })
    })
    .then(function(r) { return r.json(); })
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
      btn.disabled = false;
      btn.textContent = '開始比對';
      document.getElementById('ac-loading').style.display = 'none';
      toast('❌ 呼叫失敗：' + e, 'error');
    });
  }

  // 渲染比對結果
  function _acRenderResults(d) {
    // 更新 Tab 計數
    document.getElementById('ac-cnt-mod').textContent = d.modified.length;
    document.getElementById('ac-cnt-add').textContent = d.added.length;
    document.getElementById('ac-cnt-rem').textContent = d.removed.length;

    var total = d.modified.length + d.added.length;
    document.getElementById('ac-subtitle').textContent =
      '修改 ' + d.modified.length + ' ／ 新增 ' + d.added.length + ' ／ 可能下架 ' + d.removed.length;

    // ── 修改列表 ──
    var modList = document.getElementById('ac-list-mod');
    modList.innerHTML = '';
    if (d.modified.length === 0) {
      modList.innerHTML = '<p style="color:var(--txs);font-size:13px;">✅ 無修改差異</p>';
    } else {
      d.modified.forEach(function(item, idx) {
        var fieldsHtml = item.changed_fields.map(function(f) {
          return '<div style="display:flex;gap:6px;align-items:baseline;flex-wrap:wrap;margin-bottom:3px;">' +
            '<span style="font-size:11px;color:var(--txm);min-width:80px;">' + _escHtml(f.field) + '</span>' +
            '<span style="font-size:12px;color:#f87171;text-decoration:line-through;">' + _escHtml(f.old || '（空）') + '</span>' +
            '<span style="font-size:11px;color:var(--txs);">→</span>' +
            '<span style="font-size:12px;color:#4ade80;font-weight:600;">' + _escHtml(f.new || '（空）') + '</span>' +
            '</div>';
        }).join('');
        var card = '<div style="border:1px solid var(--bd);border-radius:8px;padding:10px 12px;background:var(--bg-t);">' +
          '<label style="display:flex;gap:8px;align-items:flex-start;cursor:pointer;">' +
          '<input type="checkbox" class="ac-mod-cb" data-idx="' + idx + '" checked onchange="_acUpdateApplyCount()" style="margin-top:3px;flex-shrink:0;">' +
          '<div style="flex:1;">' +
          '<div style="font-size:13px;font-weight:600;color:var(--tx);margin-bottom:6px;">' +
          _escHtml(item.display_name) +
          (item.seq ? '<span style="font-size:11px;color:var(--txs);font-weight:400;margin-left:6px;">序號 ' + _escHtml(item.seq) + '</span>' : '') +
          '</div>' +
          fieldsHtml +
          '</div></label></div>';
        modList.innerHTML += card;
      });
    }

    // ── 新增列表 ──
    var addList = document.getElementById('ac-list-add');
    addList.innerHTML = '';
    if (d.added.length === 0) {
      addList.innerHTML = '<p style="color:var(--txs);font-size:13px;">✅ 無新增物件</p>';
    } else {
      d.added.forEach(function(item, idx) {
        var card = '<div style="border:1px solid var(--bd);border-radius:8px;padding:10px 12px;background:var(--bg-t);">' +
          '<label style="display:flex;gap:8px;align-items:center;cursor:pointer;">' +
          '<input type="checkbox" class="ac-add-cb" data-idx="' + idx + '" checked onchange="_acUpdateApplyCount()" style="flex-shrink:0;">' +
          '<div>' +
          '<span style="font-size:13px;font-weight:600;color:var(--tx);">' + _escHtml(item.display_name) + '</span>' +
          (item.price ? '<span style="font-size:11px;color:var(--txs);margin-left:8px;">售價 ' + _escHtml(item.price) + ' 萬</span>' : '') +
          (item.agent ? '<span style="font-size:11px;color:var(--txs);margin-left:8px;">經紀人 ' + _escHtml(item.agent) + '</span>' : '') +
          (item.comm  ? '<span style="font-size:11px;color:var(--txs);margin-left:8px;">委託 ' + _escHtml(item.comm) + '</span>' : '') +
          '</div></label></div>';
        addList.innerHTML += card;
      });
    }

    // ── 可能下架列表（只顯示，不勾選）──
    var remList = document.getElementById('ac-list-rem');
    remList.innerHTML = '';
    if (d.removed.length === 0) {
      remList.innerHTML = '<p style="color:var(--txs);font-size:13px;">✅ 無可能下架物件</p>';
    } else {
      d.removed.forEach(function(item) {
        remList.innerHTML += '<div style="padding:7px 10px;border-radius:6px;background:var(--bg-t);border:1px solid var(--bd);font-size:12px;color:var(--txm);">' +
          _escHtml(item.display_name) +
          (item.comm ? '<span style="color:var(--txs);margin-left:6px;">委託 ' + _escHtml(item.comm) + '</span>' : '') +
          (item.seq  ? '<span style="color:var(--txs);margin-left:6px;">序號 ' + _escHtml(item.seq) + '</span>' : '') +
          '</div>';
      });
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
      var fields = {};
      item.changed_fields.forEach(function(f) { fields[f.field] = f.new; });
      applyModified.push({ row_in_orig: item.row_in_orig, fields: fields });
    });

    // ── 收集新增項目 ──
    var applyAdded = [];
    document.querySelectorAll('#ac-list-add .ac-add-cb:checked').forEach(function(cb) {
      var idx  = parseInt(cb.dataset.idx);
      var item = _acData.added[idx];
      if (!item) return;
      applyAdded.push(item.data);
    });

    if (applyModified.length === 0 && applyAdded.length === 0) {
      toast('請先勾選要套用的項目', 'error');
      return;
    }

    var msg = '確定要套用 ' + applyModified.length + ' 筆修改';
    if (applyAdded.length > 0) msg += '＋新增 ' + applyAdded.length + ' 筆';
    msg += ' 到原始 Sheets 主頁？\n\n此操作會直接寫入 Google Sheets，無法復原。';
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

  // 刪除準賣方
  function slDelete(id) {
    // 可從卡片按鈕直接呼叫（傳入 id），或從 Modal 內的刪除按鈕呼叫（無參數，用 _slCurrent）
    var targetId = id || _slCurrent;
    if (!targetId) return;
    var item = _slData.find(function(x){ return x.id === targetId; });
    var name = item ? item.name : '';
    var msg = name ? ('確定要刪除「' + name + '」及所有互動記事？') : '確定要刪除此準賣方及所有互動記事？';
    if (!confirm(msg)) return;
    fetch('/api/sellers/' + targetId, { method: 'DELETE' })
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (d.error) { toast('❌ ' + d.error, 'error'); return; }
        toast('✅ 已刪除', 'success');
        if (_slCurrent) slModalClose();  // 若從 Modal 刪才關 Modal
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
  function slAvatarUpload(input) {
    if (!_slCurrent || !input.files || !input.files[0]) return;
    var fd = new FormData();
    fd.append('file', input.files[0]);
    input.value = '';
    toast('⏳ 上傳頭像中…', 'info');
    fetch('/api/sellers/' + _slCurrent + '/avatar', { method: 'POST', body: fd })
      .then(function(r){ return r.json(); })
      .then(function(d) {
        if (d.error) { toast('❌ ' + d.error, 'error'); return; }
        // 更新 Modal 頭像顯示
        var imgEl = document.getElementById('sl-avatar-img');
        var phEl  = document.getElementById('sl-avatar-placeholder');
        imgEl.src = d.url + '?t=' + Date.now();
        imgEl.style.display = 'block';
        phEl.style.display  = 'none';
        // 更新本地資料
        var s = _slData.find(function(x){ return x.id === _slCurrent; });
        if (s) s.avatar_url = d.url;
        toast('✅ 頭像已更新', 'success');
        slFilterRender();  // 重新渲染列表卡片
      })
      .catch(function(){ toast('❌ 上傳失敗', 'error'); });
  }

  // 載入相關圖檔（從已有的準賣方資料）
  function slFilesLoad(s) {
    var el = document.getElementById('sl-files-list');
    if (!el) return;
    var files = s.files || [];
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

  // 頁面載入後：若 URL 有 ?tab= 就切到指定分頁，否則預設公司物件庫
  (function() {
    var _initTab = new URLSearchParams(window.location.search).get('tab') || 'company';
    var _allowed = ['company','sellers','map','dbview','settings','org'];
    switchTab(_allowed.indexOf(_initTab) >= 0 ? _initTab : 'company');
    // 清除 URL 裡的 tab 參數，避免重整後仍帶著
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
  (function() {
    const overlay = document.getElementById("gf-overlay");
    const cancelBtn = document.getElementById("gf-cancel");
    const submitBtn = document.getElementById("gf-submit");
    const textEl = document.getElementById("gf-text");
    const catEl = document.getElementById("gf-category");
    const toast = document.getElementById("gf-toast");

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
  })();
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
</body>
</html>
"""


if __name__ == "__main__":
    os.makedirs(USERS_DIR, exist_ok=True)
    port = int(os.environ.get("PORT", "5004"))
    app.run(debug=bool(os.environ.get("FLASK_DEBUG")), host="0.0.0.0", port=port)
