# -*- coding: utf-8 -*-
"""
房仲工具 — 物件庫（real-estate-library）
物件新增/刪除/編輯，並整合 Survey 環境總結與 AD 產出。每用戶獨立，管理員可查看各用戶。
"""

import os
import json
import re
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
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = not os.environ.get("FLASK_DEBUG")

PORTAL_URL = (os.environ.get("PORTAL_URL") or "").strip()
BUYER_URL  = (os.environ.get("BUYER_URL") or "").strip()
ADMIN_EMAILS = [e.strip() for e in (os.environ.get("ADMIN_EMAILS") or "").split(",") if e.strip()]
SERVICE_API_KEY = (os.environ.get("SERVICE_API_KEY") or "").strip()
TOKEN_SERIALIZER = URLSafeTimedSerializer(app.secret_key)
TOKEN_MAX_AGE = 60

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
    db = _get_db()
    style = "navy"
    if db:
        try:
            doc = db.collection("system_settings").document("theme").get()
            if doc.exists:
                style = doc.to_dict().get("style", "navy")
        except Exception:
            pass
    return jsonify({"style": style})

@app.route("/api/theme", methods=["POST"])
def api_theme_set():
    email = session.get("user_email", "")
    if not email or not _is_admin(email):
        return jsonify({"error": "無管理權限"}), 403
    data = request.get_json(silent=True) or {}
    style = data.get("style", "navy")
    if style not in VALID_THEME_STYLES:
        return jsonify({"error": "無效風格"}), 400
    db = _get_db()
    if db:
        try:
            db.collection("system_settings").document("theme").set({"style": style})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"ok": True, "style": style})

@app.route("/health")
def health():
    return {"service": "real-estate-library", "status": "ok"}, 200


@app.route("/auth/portal-login")
def auth_portal_login():
    token = request.args.get("token", "")
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
    session["user_email"] = email
    session["user_name"] = payload.get("name", "")
    session["user_picture"] = payload.get("picture", "")
    next_url = request.args.get("next", "/")
    if not next_url.startswith("/"):
        next_url = "/"
    return redirect(next_url)


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    session.clear()
    if request.headers.get("Accept", "").find("application/json") >= 0:
        return {"ok": True}, 200
    return redirect(PORTAL_URL or "/")


@app.route("/api/me", methods=["GET"])
def api_me():
    """回傳目前登入者基本資訊（供前端預設篩選用）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    return jsonify({
        "email":   email,
        "name":    session.get("user_name", ""),
        "picture": session.get("user_picture", ""),
        "is_admin": _is_admin(email),
    })


@app.route("/api/objects", methods=["GET"])
def api_objects_list():
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    is_admin = _is_admin(email)
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
                "id": obj.get("id", oid),
                "custom_title": obj.get("custom_title", ""),
                "project_name": obj.get("project_name", ""),
                "address": obj.get("address", ""),
                "created_at": obj.get("created_at", ""),
                "updated_at": obj.get("updated_at", ""),
                "owner_email": obj.get("owner_email", target),
            })
    return jsonify({"items": items, "target_user": target, "is_admin": is_admin})


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
    if _save_object(email, obj_id, obj):
        return jsonify({"ok": True, "id": obj_id, "object": obj}), 201
    return jsonify({"error": "儲存失敗"}), 500


@app.route("/api/objects/list-for-service", methods=["GET"])
def api_objects_list_for_service():
    """供 AD 等後端服務以 X-Service-Key 列出指定用戶的物件清單。
    Query: email=xxx（必填）"""
    if not _verify_service_key():
        return jsonify({"error": "需要有效的 X-Service-Key"}), 401
    email = (request.args.get("email") or "").strip()
    if not email or "@" not in email:
        return jsonify({"error": "缺少有效的 email"}), 400
    ids = _list_user_ids(email)
    items = []
    for oid in sorted(ids, reverse=True):
        obj = _load_object(email, oid)
        if obj:
            items.append({
                "id": obj.get("id", oid),
                "custom_title": obj.get("custom_title", ""),
                "project_name": obj.get("project_name", ""),
                "address": obj.get("address", ""),
                "price": obj.get("price", ""),
                "building_ping": obj.get("building_ping", ""),
                "land_ping": obj.get("land_ping", ""),
                "authority_ping": obj.get("authority_ping", ""),
                "layout": obj.get("layout", ""),
                "floor": obj.get("floor", ""),
                "age": obj.get("age", ""),
                "parking": obj.get("parking", ""),
                "case_number": obj.get("case_number", ""),
                "location_area": obj.get("location_area", ""),
                "env_description": obj.get("env_description", ""),
                "survey_summary": obj.get("survey_summary", ""),
                "created_at": obj.get("created_at", ""),
            })
    return jsonify({"items": items})


@app.route("/api/objects/from-service", methods=["POST"])
def api_objects_from_service():
    """由 AD 等服務以 X-Service-Key 代用戶寫入物件。Body: { "email": "user@example.com", "object": { project_name, address, ... } }"""
    if not _verify_service_key():
        return jsonify({"error": "需要有效的 X-Service-Key"}), 401
    data = request.get_json() or {}
    email = (data.get("email") or "").strip()
    if not email or "@" not in email:
        return jsonify({"error": "缺少有效的 email"}), 400
    payload = data.get("object") or data
    now = datetime.now()
    obj_id = now.strftime("%Y%m%d_%H%M%S")
    title = (payload.get("custom_title") or payload.get("project_name") or "未命名").strip()
    if title:
        safe_title = re.sub(r'[^\w\u4e00-\u9fff\-]', '_', title)[:24]
        obj_id = f"{obj_id}_{safe_title}"
    obj = {"id": obj_id, "created_at": now.isoformat(), "owner_email": email}
    for key, _label, _typ in PROPERTY_FIELDS + EXTRA_FIELDS:
        if key == "ad_outputs":
            continue
        if key in payload:
            obj[key] = payload[key]
        elif key == "custom_title":
            obj[key] = payload.get("project_name", "") or ""
        else:
            obj[key] = obj.get(key, "")
    obj[AD_OUTPUTS_KEY] = payload.get(AD_OUTPUTS_KEY, [])
    if _save_object(email, obj_id, obj):
        return jsonify({"ok": True, "id": obj_id, "object": obj}), 201
    return jsonify({"error": "儲存失敗"}), 500


@app.route("/api/objects/<obj_id>/ad-outputs", methods=["PATCH"])
def api_objects_update_ad_outputs(obj_id):
    """由 AD 服務以 X-Service-Key 更新指定物件的 ad_outputs（不動其他欄位）。
    Body: { "email": "user@example.com", "ad_outputs": [...] }"""
    if not _verify_service_key():
        return jsonify({"error": "需要有效的 X-Service-Key"}), 401
    data = request.get_json() or {}
    email = (data.get("email") or "").strip()
    if not email or "@" not in email:
        return jsonify({"error": "缺少有效的 email"}), 400
    ad_outputs = data.get("ad_outputs")
    if not isinstance(ad_outputs, list):
        return jsonify({"error": "ad_outputs 必須為陣列"}), 400
    obj = _load_object(email, obj_id)
    if not obj:
        return jsonify({"error": "物件不存在"}), 404
    obj[AD_OUTPUTS_KEY] = ad_outputs
    obj["updated_at"] = datetime.now().isoformat()
    if _save_object(email, obj_id, obj):
        return jsonify({"ok": True, "id": obj_id})
    return jsonify({"error": "儲存失敗"}), 500


@app.route("/api/objects/<obj_id>", methods=["GET"])
def api_objects_get(obj_id):
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
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
    target = request.args.get("user", "").strip() or email
    if not _can_access(email, target, _is_admin(email)):
        return jsonify({"error": "無權限"}), 403
    obj = _load_object(target, obj_id)
    if not obj:
        return jsonify({"error": "物件不存在"}), 404
    data = request.get_json() or {}
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
    target = request.args.get("user", "").strip() or email
    if not _can_access(email, target, _is_admin(email)):
        return jsonify({"error": "無權限"}), 403
    if _delete_object(target, obj_id):
        return jsonify({"ok": True})
    return jsonify({"error": "刪除失敗或物件不存在"}), 404


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


def _sheets_read_all():
    """用 ADC 讀取整張 Sheets，回傳 (headers, data_rows)。"""
    import google.auth
    from googleapiclient.discovery import build
    creds, _ = google.auth.default(scopes=[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/spreadsheets.readonly"
    ])
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
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
            data["銷售中"] = str(val).strip().lower() not in ("no", "否", "false", "0")
        else:
            data[h] = val
    return data


def _do_sync():
    """執行同步（在背景執行緒中跑）。回傳結果 dict。"""
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
            col.document(doc_id).set(d)
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

    # 背景執行（避免 Cloud Scheduler timeout）
    def run():
        _sync_status["running"] = True
        try:
            result = _do_sync()
            _sync_status["last_result"] = result
            _sync_status["last_run"] = result.get("finished") or result.get("started")
        finally:
            _sync_status["running"] = False

    t = threading.Thread(target=run, daemon=True)
    t.start()

    return jsonify({"ok": True, "message": "同步已在背景啟動，約需 1-2 分鐘完成"})


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


# ── 公司物件庫搜尋 API（Firestore company_properties 集合） ──

@app.route("/api/company-properties/search", methods=["GET"])
def api_company_properties_search():
    """搜尋公司物件庫，支援多條件篩選。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

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
            "資料序號", "鄉/市/鎮", "已加星", "舊案名", "所有權人",
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

    db = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    try:
        doc = db.collection("company_properties").document(prop_id).get()
        if not doc.exists:
            return jsonify({"error": "找不到物件"}), 404

        data = {"id": doc.id, **doc.to_dict()}

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
        email_s = session.get("email", "")
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


@app.route("/")
def index():
    email = session.get("user_email")
    if not email:
        return redirect(PORTAL_URL or "/") if PORTAL_URL else "<h1>請從入口登入</h1>"
    return _render_app()


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
    return html


OBJECTS_APP_HTML = """
<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>物件庫 - 房仲 AI 工具平台</title>
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
    /* ── 統一 Sidebar ── */
    #app-sidebar{position:fixed;top:0;left:0;height:100%;width:224px;background:var(--bg-s);border-right:1px solid var(--bd);display:flex;flex-direction:column;z-index:300;transition:background 0.3s,border-color 0.3s;}
    #app-sidebar .sb-logo{display:flex;align-items:center;gap:8px;padding:14px 16px;border-bottom:1px solid var(--bd);font-weight:600;color:var(--tx);font-size:0.85rem;}
    #app-sidebar nav{flex:1;padding:12px 8px;display:flex;flex-direction:column;gap:2px;}
    #app-sidebar nav a{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:10px;color:var(--txs);font-size:0.85rem;text-decoration:none;transition:background 0.15s,color 0.15s;}
    #app-sidebar nav a:hover,#app-sidebar nav a.active{background:var(--acs);color:var(--ac);}
    #app-sidebar .sb-user{padding:10px 8px;border-top:1px solid var(--bd);}
    #app-sidebar .sb-user button{width:100%;display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:10px;border:none;background:none;cursor:pointer;color:var(--txs);font-size:0.85rem;text-align:left;transition:background 0.15s;}
    #app-sidebar .sb-user button:hover{background:var(--bg-h);}
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
    @media(min-width:768px){body{padding-left:calc(224px + 1.5rem);padding-right:1.5rem;}}
    @media(max-width:767px){#app-sidebar{display:none;}#app-header{display:flex;}body{padding-left:1rem;padding-right:1rem;padding-bottom:72px;}}
    /* 手機底部 Tab Bar */
    .lib-mobile-tabbar{position:fixed;bottom:0;left:0;right:0;z-index:250;background:var(--bg-s);backdrop-filter:blur(8px);border-top:1px solid var(--bd);display:none;transition:background 0.3s;}
    @media(max-width:767px){.lib-mobile-tabbar{display:flex!important;}}
    .lib-tb-item{flex:1;display:flex;flex-direction:column;align-items:center;gap:2px;padding:8px 4px;color:var(--txm);font-size:0.65rem;text-decoration:none;transition:color 0.15s;}
    .lib-tb-item:hover,.lib-tb-active{color:var(--tx)!important;}
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
    body{background:var(--bg-p)!important;color:var(--tx)!important;}
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
  <div id="tp-admin-only" style="display:none;">
    <button onclick="window._tpSaveStyle()" style="width:100%;padding:9px;border-radius:8px;background:var(--ac);color:var(--act);border:none;cursor:pointer;font-size:0.85rem;font-weight:600;">💾 套用到所有工具</button>
    <div id="tp-save-msg" style="text-align:center;font-size:0.75rem;color:var(--ok);margin-top:6px;display:none;">✓ 已儲存！所有工具同步套用</div>
  </div>
  <div style="margin-top:14px;padding:10px;border-radius:8px;background:var(--bg-t);border:1px solid var(--bd);font-size:0.7rem;color:var(--txm);line-height:1.6;">
    💡 風格由管理員統一設定，明暗模式依個人裝置偏好儲存。
  </div>
</div>

<!-- ── 桌機左側 Sidebar ── -->
<aside id="app-sidebar">
  <div class="sb-logo">
    <span style="font-size:1.3rem;">📁</span>
    <span>物件庫</span>
  </div>
  <nav>
    <a href="__PORTAL_LINK__" target="tool-portal" id="sb-portal-home" class="hidden">🏠 工具首頁</a>
    <a href="javascript:void(0)" id="sb-ad" class="hidden">📝 廣告文案</a>
    <a href="#" class="active">📁 物件庫</a>
    <a href="javascript:void(0)" id="sb-buyer" class="hidden">👥 買方管理</a>
    <a href="javascript:void(0)" id="sb-survey" class="hidden">📍 周邊調查</a>
  </nav>
  <div style="padding:8px 8px 4px;">
    <button type="button" id="btn-new-obj" onclick="openNewModal()" title="建立物件資訊"
      style="width:100%;display:flex;align-items:center;justify-content:center;gap:6px;padding:8px 12px;border-radius:10px;border:none;background:var(--ac);color:var(--act);font-size:0.85rem;font-weight:600;cursor:pointer;transition:background 0.15s;"
      onmouseover="this.style.background='var(--ach)'" onmouseout="this.style.background='var(--ac)'">
      ＋ 建立
    </button>
  </div>
  <div class="sb-user">
    <button type="button" onclick="libToggleDropdown(event)">
      <div id="sb-avatar" class="av-wrap" style="width:36px;height:36px;flex-shrink:0;"><div class="av-fb">?</div></div>
      <div style="min-width:0;flex:1;">
        <div id="sb-name" style="font-size:0.82rem;font-weight:600;color:var(--tx);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"></div>
        <span id="sb-badge" class="points-pill points" style="margin-top:2px;">— 點</span>
      </div>
      <svg style="width:16px;height:16px;color:var(--txm);flex-shrink:0;" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
    </button>
  </div>
</aside>

<!-- ── 手機頂部 Header ── -->
<header id="app-header">
  <div class="hd-logo">
    <span>📁 物件庫</span>
  </div>
  <div style="display:flex;align-items:center;gap:8px;">
    <button type="button" id="btn-new-obj-mobile" onclick="openNewModal()" title="建立物件資訊"
      style="background:var(--ac);color:var(--act);border:none;border-radius:8px;padding:6px 12px;font-size:0.82rem;font-weight:600;cursor:pointer;white-space:nowrap;">＋ 建立</button>
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
  </div>
  <div class="dd-divider"></div>
  <div style="padding:4px 0;">
    <button class="dd-danger" onclick="libDoLogout()">🚪 登出</button>
  </div>
</div>
<div id="user-dropdown-backdrop" style="display:none;position:fixed;inset:0;z-index:499;" onclick="libCloseDropdown()"></div>

<!-- ── 手機底部 Tab Bar ── -->
<nav style="display:none;" id="lib-tab-bar" class="lib-mobile-tabbar">
  <a href="__PORTAL_LINK__" id="tb-home" target="tool-portal" class="lib-tb-item">
    <span style="font-size:1.3rem;">🏠</span>
    <span>首頁</span>
  </a>
  <a href="javascript:void(0)" id="tb-ad" class="lib-tb-item hidden">
    <span style="font-size:1.3rem;">📝</span>
    <span>廣告</span>
  </a>
  <a href="#" class="lib-tb-item lib-tb-active">
    <span style="font-size:1.3rem;">📁</span>
    <span>物件庫</span>
  </a>
  <a href="javascript:void(0)" id="tb-buyer" class="lib-tb-item hidden">
    <span style="font-size:1.3rem;">👥</span>
    <span>買方</span>
  </a>
  <a href="javascript:void(0)" id="tb-survey" class="lib-tb-item hidden">
    <span style="font-size:1.3rem;">📍</span>
    <span>周邊</span>
  </a>
</nav>

<div id="toast-container"></div>

<!-- 頂部分頁列（移除舊的導覽 header，只保留分頁標籤） -->
<header class="sticky top-0 z-50 backdrop-blur shadow" style="background:var(--bg-s);border-bottom:1px solid var(--bd);">
  <!-- 分頁標籤 -->
  <div class="flex" style="border-top:1px solid var(--bd);">
    <button id="tab-my" onclick="switchTab('my')"
      class="tab-btn flex-1 py-2 text-sm font-medium border-b-2 transition" style="color:var(--ac);border-color:var(--ac);">
      📂 我的物件
    </button>
    <button id="tab-company" onclick="switchTab('company')"
      class="tab-btn flex-1 py-2 text-sm font-medium border-b-2 border-transparent transition" style="color:var(--txs);">
      🏢 公司物件庫
    </button>
    <!-- 設定 tab：僅管理員看得到（由 JS 控制顯示） -->
    <button id="tab-settings" onclick="switchTab('settings')"
      class="tab-btn hidden flex-1 py-2 text-sm font-medium border-b-2 border-transparent transition" style="color:var(--txs);">
      ⚙️ 設定
    </button>
  </div>
</header>

<!-- ══ 我的物件分頁 ══ -->
<div id="pane-my" class="max-w-3xl mx-auto px-4 py-6">
  __ADMIN_BAR__
  <div id="listPanel" class="space-y-3"></div>

  <!-- 編輯物件面板（原地編輯用，新增改由 Modal） -->
  <div id="formPanel" class="hidden rounded-2xl p-5 mb-4" style="background:var(--bg-t);border:1px solid var(--bd);">
    <h2 id="formTitle" class="font-bold mb-4" style="color:var(--tx);">編輯物件</h2>
    <form id="objForm">
      <input type="hidden" id="objId" name="id">
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
        __FIELDS_HTML__
      </div>
      <div class="flex gap-3 mt-4">
        <button type="submit" class="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition">儲存</button>
        <button type="button" onclick="hideForm()" class="px-4 py-2 rounded-lg text-sm transition" style="background:var(--bg-h);color:var(--txs);">取消</button>
      </div>
    </form>
  </div>

  <!-- 詳情面板 -->
  <div id="detailPanel" class="hidden rounded-2xl p-5 mb-4" style="background:var(--bg-t);border:1px solid var(--bd);">
    <h2 id="detailTitle" class="font-bold mb-3" style="color:var(--tx);">物件詳情</h2>
    <div id="detailContent" class="space-y-1 text-sm" style="color:var(--txs);"></div>
    <div class="flex gap-3 mt-4">
      <button type="button" onclick="editCurrentDetail()" class="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition">編輯</button>
      <button type="button" onclick="closeDetail()" class="px-4 py-2 rounded-lg text-sm transition" style="background:var(--bg-h);color:var(--txs);">關閉</button>
    </div>
  </div>
</div>

<!-- ══ 公司物件庫分頁 ══ -->
<div id="pane-company" style="display:none" class="max-w-4xl mx-auto px-4 py-6">

  <!-- 搜尋條件列 -->
  <div class="rounded-2xl p-4 mb-4" style="background:var(--bg-t);border:1px solid var(--bd);">
    <!-- 第一列：關鍵字 + 售價 + 狀態 -->
    <div class="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-3">
      <input id="cp-keyword" type="text" placeholder="🔍 案名 / 地址 / 委託編號"
        class="col-span-2 bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500"
        onkeydown="if(event.key==='Enter')cpSearch()">
      <input id="cp-price-min" type="number" placeholder="最低售價（萬）"
        class="bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
      <input id="cp-price-max" type="number" placeholder="最高售價（萬）"
        class="bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
    </div>
    <!-- 第二列：狀態（單選）+ 複選下拉觸發器 -->
    <div class="flex flex-wrap gap-2 mb-3 items-center">
      <!-- 狀態（保留 select，不需複選） -->
      <select id="cp-status"
        class="bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none">
        <option value="selling">銷售中</option>
        <option value="">全部狀態</option>
        <option value="sold">已成交</option>
        <option value="delisted">已下架</option>
      </select>
      <!-- 委託到期日篩選（前端過濾） -->
      <select id="cp-expiry"
        onchange="cpFetch()"
        class="bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none">
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
          class="bg-slate-700 border border-slate-600 text-slate-200 text-xs rounded-lg px-2 py-1.5 focus:ring-1 focus:ring-blue-500">
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
        <span class="text-xs text-slate-400">情境：</span>
        <select id="cp-preset-select"
          onchange="cpApplyPreset()"
          class="bg-slate-700 border border-slate-600 text-slate-200 text-xs rounded-lg px-2 py-1.5 focus:ring-1 focus:ring-purple-500 max-w-[140px]"
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
    <!-- CSV 上傳（精確更新：銷售中+到期日+售價），可一併選 word_meta.json 更新總表日期 -->
    <label class="flex items-center gap-1 px-4 py-1.5 rounded-lg bg-teal-700 hover:bg-teal-600 text-white text-xs font-semibold transition cursor-pointer"
      title="上傳 export_word_table.py 產出的 CSV（公寓/房屋/農地/建地）及 word_meta.json，精確更新 Firestore 銷售中、到期日、售價">
      📊 上傳解析 CSV
      <input type="file" accept=".csv,.json" multiple class="hidden" onchange="cpUploadCsv(this)">
    </label>
    <!-- 說明按鈕 -->
    <button onclick="document.getElementById('cp-sync-help-modal').style.display='flex'"
      class="px-3 py-1.5 rounded-lg bg-slate-600 hover:bg-slate-500 text-white text-xs font-semibold transition"
      title="查看按鈕說明與操作流程">
      ❓ 說明
    </button>
    <span id="cp-word-status" style="font-size:0.75rem;color:var(--txs);"></span>
    <!-- 物件總表日期標籤 -->
    <span id="cp-doc-date" style="font-size:0.75rem;color:var(--txm);margin-left:0.25rem;" title="物件總表更新日期"></span>
  </div>

  <!-- 同步說明 Modal -->
  <div id="cp-sync-help-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:500;align-items:center;justify-content:center;"
    onclick="if(event.target===this)this.style.display='none'">
    <div style="background:#1e293b;border:1px solid #334155;border-radius:16px;padding:28px 32px;max-width:620px;width:92%;max-height:88vh;display:flex;flex-direction:column;box-shadow:0 8px 40px rgba(0,0,0,.5);position:relative;">
      <button onclick="document.getElementById('cp-sync-help-modal').style.display='none'"
        style="position:absolute;top:14px;right:18px;background:none;border:none;color:#94a3b8;font-size:20px;cursor:pointer;line-height:1;">✕</button>
      <h2 style="color:#f1f5f9;font-size:16px;font-weight:700;margin:0 0 16px;">📋 按鈕說明與操作流程</h2>
      <div style="overflow-y:auto;flex:1;padding-right:4px;">

        <!-- 按鈕說明 -->
        <div style="margin-bottom:20px;">
          <p style="color:#94a3b8;font-size:12px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;margin:0 0 10px;">按鈕功能</p>
          <div style="display:flex;flex-direction:column;gap:10px;">
            <div style="background:#1a2f1a;border:1px solid #166534;border-radius:10px;padding:12px 14px;">
              <p style="color:#4ade80;font-weight:700;margin:0 0 4px;font-size:13px;">🔄 同步 Sheets</p>
              <p style="color:#cbd5e1;font-size:12px;margin:0;">從 Google Sheets 把物件基本資料（案名、地址、類別、經紀人、售價等）同步到 Firestore 資料庫。這是物件資料的<strong style="color:#f1f5f9;">主要來源</strong>，Sheets 有新增/修改物件後要按此更新。</p>
              <p style="color:#64748b;font-size:11px;margin:6px 0 0;">⏱ 資料量大時需等待 1～10 分鐘，同步中請勿重複點擊。</p>
            </div>
            <div style="background:#0f2a2a;border:1px solid #0f766e;border-radius:10px;padding:12px 14px;">
              <p style="color:#2dd4bf;font-weight:700;margin:0 0 4px;font-size:13px;">📊 上傳解析 CSV（推薦）</p>
              <p style="color:#cbd5e1;font-size:12px;margin:0;">上傳由本機工具 <code style="background:#0f172a;padding:1px 5px;border-radius:4px;color:#7dd3fc;">export_word_table.py</code> 解析後產出的 CSV 檔（公寓/房屋/農地/建地）及 <code style="background:#0f172a;padding:1px 5px;border-radius:4px;color:#7dd3fc;">word_meta.json</code>，精確更新 Firestore 的<strong style="color:#f1f5f9;">銷售中狀態、委託到期日、最新售價</strong>。</p>
              <p style="color:#64748b;font-size:11px;margin:6px 0 0;">💡 一次可選取 4 個 CSV + 1 個 word_meta.json，共 5 個檔案一起上傳。</p>
            </div>
          </div>
        </div>

        <!-- 兩條資料來源說明 -->
        <div style="margin-bottom:20px;">
          <p style="color:#94a3b8;font-size:12px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;margin:0 0 10px;">資料來源與分工</p>
          <div style="background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px 16px;display:flex;flex-direction:column;gap:8px;">
            <div style="display:flex;gap:10px;align-items:flex-start;">
              <span style="background:#f59e0b;color:#000;border-radius:6px;padding:2px 7px;font-size:11px;font-weight:700;white-space:nowrap;">Sheets</span>
              <span style="color:#cbd5e1;font-size:12px;">物件<strong style="color:#f1f5f9;">基本資料</strong>（案名、地址、類別、經紀人）的主要來源。新增或修改物件後，在此頁按「🔄 同步 Sheets」更新 Firestore。</span>
            </div>
            <div style="border-top:1px solid #1e293b;"></div>
            <div style="display:flex;gap:10px;align-items:flex-start;">
              <span style="background:#0d9488;color:#fff;border-radius:6px;padding:2px 7px;font-size:11px;font-weight:700;white-space:nowrap;">物件總表</span>
              <span style="color:#cbd5e1;font-size:12px;"><strong style="color:#f1f5f9;">銷售中狀態、委託到期日、最新售價</strong>的來源。Sheets 不含這些資訊，需靠 Word 物件總表補充。由本機工具處理後寫入 Firestore，<strong style="color:#2dd4bf;">不需要回此頁上傳</strong>（除非跳過比對審查，直接上傳 CSV）。</span>
            </div>
          </div>

          <!-- 資料安全說明 -->
          <div style="background:#0f172a;border:1px solid #854d0e;border-radius:10px;padding:14px 16px;margin-top:10px;">
            <p style="color:#fbbf24;font-size:12px;font-weight:700;margin:0 0 8px;">🛡️ 資料安全：CSV 上傳只會動哪些欄位？</p>
            <div style="display:flex;flex-direction:column;gap:6px;font-size:12px;">
              <div style="display:flex;gap:8px;align-items:flex-start;">
                <span style="color:#4ade80;font-weight:700;white-space:nowrap;">只會改</span>
                <span style="color:#cbd5e1;">銷售中狀態、委託到期日、售價（萬）— 這三個欄位</span>
              </div>
              <div style="display:flex;gap:8px;align-items:flex-start;">
                <span style="color:#f87171;font-weight:700;white-space:nowrap;">不會動</span>
                <span style="color:#cbd5e1;">案名、地址、類別、經紀人、所有權人等基本資料</span>
              </div>
              <div style="border-top:1px solid #1e293b;padding-top:6px;display:flex;gap:8px;align-items:flex-start;">
                <span style="color:#fbbf24;font-weight:700;white-space:nowrap;">配對邏輯</span>
                <span style="color:#cbd5e1;">同名物件會同時比對<strong style="color:#f1f5f9;">委託號碼 → 經紀人 → 售價 → 面積</strong>，層層過濾，避免同名不同人的物件互相污染。</span>
              </div>
              <div style="border-top:1px solid #1e293b;padding-top:6px;display:flex;gap:8px;align-items:flex-start;">
                <span style="color:#60a5fa;font-weight:700;white-space:nowrap;">萬一誤改</span>
                <span style="color:#cbd5e1;">在此頁按「🔄 同步 Sheets」即可將售價還原回 Sheets 的原始值。Sheets 是最終原始依據，永遠可以用來救回資料。</span>
              </div>
            </div>
          </div>
        </div>

        <!-- 操作流程 -->
        <div style="margin-bottom:20px;">
          <p style="color:#94a3b8;font-size:12px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;margin:0 0 10px;">操作流程</p>

          <!-- 情境一 -->
          <div style="background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px 16px;margin-bottom:10px;">
            <p style="color:#fbbf24;font-size:12px;font-weight:700;margin:0 0 10px;">📌 情境一：Sheets 有新增或修改物件</p>
            <div style="display:flex;flex-direction:column;gap:6px;">
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#f59e0b;color:#000;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">1</span><span style="color:#cbd5e1;font-size:12px;">在此頁按「<strong style="color:#fbbf24;">🔄 同步 Sheets</strong>」→ 等待完成（約 1～10 分鐘）</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#334155;color:#94a3b8;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">✓</span><span style="color:#94a3b8;font-size:12px;">基本資料更新完成。<em>銷售中狀態/到期日/售價</em>若需同步，請另跑物件總表流程（情境二）。</span></div>
            </div>
          </div>

          <!-- 情境二 -->
          <div style="background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px 16px;margin-bottom:10px;">
            <p style="color:#2dd4bf;font-size:12px;font-weight:700;margin:0 0 10px;">📌 情境二：公司發下新版物件總表 Word 檔（完整流程）</p>
            <div style="display:flex;flex-direction:column;gap:7px;">
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#374151;color:#e5e7eb;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">1</span><span style="color:#cbd5e1;font-size:12px;">把新 Word 檔存至：<br><code style="background:#1e293b;padding:1px 6px;border-radius:4px;color:#7dd3fc;font-size:11px;">/Users/chenweiliang/Documents/日盛同步/物件總表/</code></span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#374151;color:#e5e7eb;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">2</span><span style="color:#cbd5e1;font-size:12px;">本機開啟<strong style="color:#c084fc;">物件總表比對審查</strong>：<code style="background:#1e293b;padding:1px 6px;border-radius:4px;color:#7dd3fc;font-size:11px;">python3 review_v2.py</code><br>瀏覽器開 <code style="background:#1e293b;padding:1px 6px;border-radius:4px;color:#7dd3fc;font-size:11px;">localhost:5100</code></span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#374151;color:#e5e7eb;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">3</span><span style="color:#cbd5e1;font-size:12px;">在比對審查頁面依序按完所有按鈕（解析 Word → 套用高信心 → 送出決定 → 完成審查）</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#0d9488;color:#fff;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">✓</span><span style="color:#4ade80;font-size:12px;"><strong>完成！已直接寫入 Firestore，不需要回此頁再按任何按鈕。</strong></span></div>
            </div>
          </div>

          <!-- 情境三（快速流程） -->
          <div style="background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px 16px;">
            <p style="color:#a78bfa;font-size:12px;font-weight:700;margin:0 0 10px;">📌 情境三：快速更新（跳過比對審查）</p>
            <div style="display:flex;flex-direction:column;gap:6px;">
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#374151;color:#e5e7eb;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">1</span><span style="color:#cbd5e1;font-size:12px;">本機執行：<code style="background:#1e293b;padding:1px 6px;border-radius:4px;color:#7dd3fc;font-size:11px;">python3 export_word_table.py</code><br><span style="color:#64748b;font-size:11px;">→ 產出 4 個 CSV + word_meta.json</span></span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#0d9488;color:#fff;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">2</span><span style="color:#cbd5e1;font-size:12px;">回此頁按「<strong style="color:#2dd4bf;">📊 上傳解析 CSV</strong>」，選取 5 個檔案一次上傳</span></div>
              <div style="display:flex;align-items:flex-start;gap:10px;"><span style="background:#78350f;color:#fbbf24;border-radius:50%;width:20px;height:20px;min-width:20px;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;margin-top:1px;">!</span><span style="color:#94a3b8;font-size:12px;">跳過了人工比對確認，若 Word 有解析錯誤可能造成資料不準確，建議定期仍做完整流程（情境二）。</span></div>
            </div>
          </div>
        </div>

        <!-- 本機工具說明 -->
        <div>
          <p style="color:#94a3b8;font-size:12px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;margin:0 0 10px;">本機工具說明</p>
          <div style="background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px 16px;display:flex;flex-direction:column;gap:10px;">
            <div>
              <p style="color:#7dd3fc;font-weight:600;font-size:12px;margin:0 0 3px;"><code style="background:#1e293b;padding:1px 5px;border-radius:4px;">export_word_table.py</code></p>
              <p style="color:#cbd5e1;font-size:12px;margin:0;">讀取 Word 物件總表，精確解析各類型（公寓/房屋/農地/建地）的欄位，輸出 CSV 檔。解析規則經過多次磨合，是目前最精確的版本。<br>路徑：<code style="background:#1e293b;padding:1px 5px;border-radius:4px;color:#7dd3fc;font-size:11px;">/Users/chenweiliang/Projects/export_word_table.py</code></p>
            </div>
            <div style="border-top:1px solid #1e293b;padding-top:10px;">
              <p style="color:#c084fc;font-weight:600;font-size:12px;margin:0 0 3px;"><code style="background:#1e293b;padding:1px 5px;border-radius:4px;">review_v2.py</code>（物件總表比對審查）</p>
              <p style="color:#cbd5e1;font-size:12px;margin:0;">比對 Word 解析結果與 Firestore 現有資料，找出案名異動的物件、確認低/高信心配對，<strong style="color:#f1f5f9;">審查確認後直接寫回 Firestore</strong>（不需再回此頁上傳 CSV）。已確認的配對會記憶，下次自動套用。<br>路徑：<code style="background:#1e293b;padding:1px 5px;border-radius:4px;color:#7dd3fc;font-size:11px;">/Users/chenweiliang/Projects/review_v2.py</code></p>
            </div>
          </div>
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
<div id="pane-settings" style="display:none" class="max-w-2xl mx-auto px-4 py-6">
  <h2 class="font-bold text-lg mb-4" style="color:var(--tx);">⚙️ 系統設定</h2>

  <!-- 經紀人 Email 管理 -->
  <div class="rounded-2xl p-5 mb-6" style="background:var(--bg-t);border:1px solid var(--bd);">
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
  <div class="rounded-2xl p-5 mb-6" style="background:var(--bg-t);border:1px solid var(--bd);">
    <h3 class="font-semibold mb-1" style="color:var(--tx);">🔍 物件搜尋索引</h3>
    <p class="text-xs mb-3" style="color:var(--txs);">買方管理輸入物件名稱時的自動完成資料來源。每次「立即同步 Sheets」後自動更新，也可手動重建。</p>
    <button onclick="rebuildPropIndex()"
      class="px-4 py-2 rounded-lg text-white text-sm font-semibold transition" style="background:var(--bg-h);color:var(--txs);border:1px solid var(--bd);">
      重建物件索引
    </button>
    <p id="prop-index-result" class="text-xs mt-2" style="color:var(--txs);"></p>
  </div>

  <!-- 手動觸發通知測試 -->
  <div class="bg-slate-800 border border-slate-700 rounded-2xl p-5">
    <h3 class="font-semibold text-slate-100 mb-1">🔔 到期通知測試</h3>
    <p class="text-xs text-slate-400 mb-3">手動觸發一次到期日通知，確認 Email 是否正常發送（每天早上 8 點自動執行）</p>
    <button onclick="triggerNotify()"
      class="px-4 py-2 rounded-lg bg-amber-600 hover:bg-amber-500 text-white text-sm font-semibold transition">
      立即執行通知
    </button>
    <p id="notify-result" class="text-xs text-slate-400 mt-2"></p>
  </div>
</div>

<!-- 公司物件詳情 Modal -->
<div id="cp-detail-modal" role="dialog" aria-modal="true"
  class="hidden fixed inset-0 z-[200] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm"
  onclick="if(event.target===this)closeCpDetail()">
  <div class="w-full max-w-2xl rounded-2xl bg-slate-800 border border-slate-600 shadow-2xl flex flex-col max-h-[90vh]"
    onclick="event.stopPropagation()">
    <div class="flex items-center justify-between px-6 py-4 border-b border-slate-700 shrink-0">
      <h3 id="cp-detail-title" class="font-bold text-slate-100 text-lg">物件詳情</h3>
      <button onclick="closeCpDetail()" class="text-slate-400 hover:text-slate-200 text-xl leading-none">✕</button>
    </div>
    <div id="cp-detail-body" class="overflow-y-auto px-6 py-5 space-y-1 text-sm"></div>
    <div class="px-6 py-4 border-t border-slate-700 shrink-0">
      <button onclick="closeCpDetail()" class="px-4 py-2 rounded-lg bg-slate-700 hover:bg-slate-600 text-slate-300 text-sm transition">關閉</button>
    </div>
  </div>
</div>

<!-- ── 建立物件資訊 Modal（含圖片辨識） ── -->
<div id="new-prop-modal" role="dialog" aria-modal="true"
  class="hidden fixed inset-0 z-[200] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm overflow-y-auto"
  onclick="if(event.target===this)closeNewModal()">
  <div class="w-full max-w-lg rounded-2xl bg-slate-800 border border-slate-600 shadow-2xl" onclick="event.stopPropagation()">
    <div class="flex items-center justify-between px-6 py-4 border-b border-slate-700">
      <h3 class="font-bold text-slate-100">建立物件資訊</h3>
      <button onclick="closeNewModal()" class="text-slate-400 hover:text-slate-200 text-xl leading-none">✕</button>
    </div>
    <div class="px-6 py-5 space-y-3 max-h-[65vh] overflow-y-auto">
      <!-- 圖片辨識（扣 2 點） -->
      <div class="bg-slate-700/50 rounded-xl p-4 border border-slate-600">
        <p class="text-xs text-slate-400 mb-2 font-medium">📷 圖片辨識（自儲值扣 2 點）</p>
        <div class="flex flex-wrap gap-2 items-center">
          <input type="file" id="lib-image-input" accept="image/*" class="hidden" onchange="onLibImageSelected(event)">
          <button type="button" onclick="document.getElementById('lib-image-input').click()"
            class="px-3 py-2 rounded-lg bg-slate-600 hover:bg-slate-500 text-slate-200 text-sm transition">選擇圖片</button>
          <button type="button" onclick="handleLibPaste()"
            class="px-3 py-2 rounded-lg bg-slate-600 hover:bg-slate-500 text-slate-200 text-sm transition">貼上</button>
          <span id="lib-image-name" class="text-xs text-slate-500 truncate max-w-[140px]"></span>
          <button type="button" id="lib-extract-btn" onclick="runLibExtractFromImage()" disabled
            class="px-3 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium transition disabled:opacity-50 disabled:cursor-not-allowed">辨識並帶入（2 點）</button>
        </div>
        <p class="text-xs text-slate-500 mt-2 min-h-[1em]" id="lib-extract-status"></p>
        <div class="mt-3 pt-3 border-t border-slate-600">
          <p class="text-xs text-slate-400 mb-1">或輸入物件網址（自動截圖後辨識）</p>
          <p class="text-xs text-amber-400 mb-2">⚠️ 注意：YES319、591 等網站有 Cloudflare 防護，截圖功能無法使用。請改用上方「選擇圖片／貼上」功能：在瀏覽器按 Cmd+Shift+4 截圖後貼上即可。</p>
          <div class="flex gap-2 items-center">
            <input type="url" id="lib-url-input" placeholder="適用無 Cloudflare 保護的網站"
              class="flex-1 min-w-0 bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" />
            <button type="button" id="lib-url-btn" onclick="runLibExtractFromUrl()"
              class="px-3 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium whitespace-nowrap transition">截圖並辨識</button>
          </div>
          <p class="text-xs text-slate-500 mt-2 min-h-[1em]" id="lib-url-status"></p>
        </div>
      </div>
      <!-- 從 AD 歷史匯入 -->
      <div class="bg-slate-700/50 rounded-xl p-4 border border-slate-600">
        <p class="text-xs text-slate-400 mb-2 font-medium">📋 從 AD 歷史匯入</p>
        <p class="text-xs text-slate-500 mb-2">若改版前曾在「廣告工具」存過紀錄，可從下方匯入</p>
        <div class="flex gap-2 items-center">
          <select id="lib-ad-history-select"
            class="flex-1 min-w-0 bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
            <option value="">— 從 AD 歷史匯入 —</option>
          </select>
          <button type="button" onclick="libImportFromAd()"
            class="px-3 py-2 rounded-lg bg-amber-600 hover:bg-amber-500 text-white text-sm font-medium whitespace-nowrap">匯入為物件</button>
        </div>
        <p class="text-xs text-slate-500 mt-2 min-h-[1em]" id="lib-import-status"></p>
      </div>
      <!-- 物件欄位 -->
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <div><label class="block text-xs text-slate-400 mb-1">物件名稱</label><input id="n-name" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="如：信義路三段電梯大樓"></div>
        <div><label class="block text-xs text-slate-400 mb-1">總價（萬）</label><input id="n-price" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="數字"></div>
        <div><label class="block text-xs text-slate-400 mb-1">區域</label><input id="n-area" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="如：台北市信義區"></div>
        <div><label class="block text-xs text-slate-400 mb-1">地址</label><input id="n-addr" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="完整地址"></div>
        <div><label class="block text-xs text-slate-400 mb-1">建坪</label><input id="n-bping" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="數字"></div>
        <div><label class="block text-xs text-slate-400 mb-1">地坪</label><input id="n-lping" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="數字"></div>
        <div><label class="block text-xs text-slate-400 mb-1">權狀</label><input id="n-aping" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="數字"></div>
        <div><label class="block text-xs text-slate-400 mb-1">格局</label><input id="n-layout" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="3房2廳2衛"></div>
        <div><label class="block text-xs text-slate-400 mb-1">樓層</label><input id="n-floor" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="3/12"></div>
        <div><label class="block text-xs text-slate-400 mb-1">屋齡</label><input id="n-age" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="5年"></div>
        <div><label class="block text-xs text-slate-400 mb-1">車位</label><input id="n-parking" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="有/無"></div>
        <div><label class="block text-xs text-slate-400 mb-1">案號</label><input id="n-case" type="text" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500" placeholder="可選填"></div>
      </div>
      <div>
        <label class="block text-xs text-slate-400 mb-1">環境說明（選填）</label>
        <textarea id="n-env" rows="3" class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 resize-none focus:outline-none focus:border-blue-500" placeholder="步行3分鐘到全聯…"></textarea>
      </div>
    </div>
    <div class="flex items-center justify-between px-6 py-4 border-t border-slate-700">
      <button onclick="closeNewModal()" class="px-4 py-2 rounded-lg bg-slate-700 text-slate-300 hover:text-slate-100 text-sm transition">取消</button>
      <button onclick="saveNewProp()" class="px-5 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition">儲存物件</button>
    </div>
  </div>
</div>

<script>
  const fields = __FIELDS_JSON__;
  const isAdmin   = __IS_ADMIN_JSON__;
  const BUYER_URL = __BUYER_URL__;
  var _libImageFile = null;

  // 管理員才顯示「設定」tab
  if (isAdmin) {
    var settingsTab = document.getElementById('tab-settings');
    if (settingsTab) settingsTab.classList.remove('hidden');
  }

  // ══ 經紀人 Email 管理 ══
  function agentEmailLoad() {
    var list = document.getElementById('agent-email-list');
    if (!list) return;
    list.innerHTML = '<p class="text-slate-500 text-sm text-center py-4">載入中…</p>';
    fetch('/api/agent-emails')
      .then(function(r){ return r.json(); })
      .then(function(data) {
        if (data.error) { list.innerHTML = '<p class="text-red-400 text-sm">' + escapeHtml(data.error) + '</p>'; return; }
        var items = data.items || [];
        if (!items.length) {
          list.innerHTML = '<p class="text-slate-500 text-sm text-center py-4">尚無設定，請點「＋ 新增」</p>';
          return;
        }
        var html = '';
        items.forEach(function(item) {
          html += '<div class="flex items-center justify-between bg-slate-700/50 rounded-xl px-4 py-2.5">';
          html += '<div>';
          html += '<span class="text-sm font-medium text-slate-100">' + escapeHtml(item.name) + '</span>';
          html += '<span class="text-xs text-slate-400 ml-3">' + (item.email ? escapeHtml(item.email) : '<em class="text-slate-500">未設定</em>') + '</span>';
          html += '</div>';
          html += '<div class="flex gap-2">';
          html += '<button class="ae-edit-btn text-xs text-blue-400 hover:text-blue-300 px-2 py-1 rounded border border-slate-600 hover:border-blue-500 transition" '
                + 'data-name="' + escapeHtml(item.name) + '" data-email="' + escapeHtml(item.email||'') + '">編輯</button>';
          html += '<button class="ae-del-btn text-xs text-red-400 hover:text-red-300 px-2 py-1 rounded border border-slate-600 hover:border-red-500 transition" '
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
  function apiUrl(path) { var u = targetUser() ? '?user=' + encodeURIComponent(targetUser()) : ''; return path + u; }

  // ── 載入列表 ──
  function loadList() {
    fetch(apiUrl('/api/objects')).then(r => r.json()).then(data => {
      if (data.error) { toast(data.error, 'error'); return; }
      var el = document.getElementById('listPanel');
      el.innerHTML = '';
      var items = data.items || [];
      if (!items.length) {
        el.innerHTML = '<p class="text-slate-500 text-center py-8">尚無物件，點「＋ 建立物件資訊」開始建立。</p>';
        return;
      }
      items.forEach(function(o) {
        var title = o.custom_title || o.project_name || o.address || o.id || '未命名';
        var meta = [o.address, o.updated_at ? o.updated_at.slice(0,10) : ''].filter(Boolean).join(' · ');
        var id = o.id;

        var wrap = document.createElement('div');
        wrap.className = 'bg-slate-800 border border-slate-600 rounded-xl px-4 py-3 flex justify-between items-center gap-3 flex-wrap';

        var info = document.createElement('div');
        var titleEl = document.createElement('div');
        titleEl.className = 'font-semibold text-slate-100';
        titleEl.textContent = title;
        var metaEl = document.createElement('div');
        metaEl.className = 'text-xs text-slate-400 mt-0.5';
        metaEl.textContent = meta;
        info.appendChild(titleEl);
        info.appendChild(metaEl);

        var btns = document.createElement('div');
        btns.className = 'flex gap-2';

        var bView = document.createElement('button');
        bView.className = 'px-3 py-1.5 rounded-lg bg-slate-600 hover:bg-slate-500 text-slate-200 text-xs transition';
        bView.textContent = '查看';
        bView.onclick = function() { viewDetail(id); };

        var bEdit = document.createElement('button');
        bEdit.className = 'px-3 py-1.5 rounded-lg bg-slate-600 hover:bg-slate-500 text-slate-200 text-xs transition';
        bEdit.textContent = '編輯';
        bEdit.onclick = function() { editObj(id); };

        var bDel = document.createElement('button');
        bDel.className = 'px-3 py-1.5 rounded-lg bg-rose-700 hover:bg-rose-600 text-white text-xs transition';
        bDel.textContent = '刪除';
        bDel.onclick = function() { delObj(id); };

        btns.appendChild(bView);
        btns.appendChild(bEdit);
        btns.appendChild(bDel);
        wrap.appendChild(info);
        wrap.appendChild(btns);
        el.appendChild(wrap);
      });
    }).catch(function() { toast('載入失敗', 'error'); });
  }

  function loadUsers() {
    if (!isAdmin) return;
    fetch('/api/users').then(r => r.json()).then(data => {
      if (data.error) return;
      var sel = document.getElementById('userSelect');
      if (!sel) return;
      sel.innerHTML = '<option value="">（自己）</option>' + (data.users || []).map(u => '<option value="' + escapeHtml(u) + '">' + escapeHtml(u) + '</option>').join('');
      sel.onchange = loadList;
    });
  }

  // ── 新增 Modal 開關 ──
  function openNewModal() {
    _libImageFile = null;
    document.getElementById('lib-image-input').value = '';
    document.getElementById('lib-image-name').textContent = '';
    document.getElementById('lib-extract-btn').disabled = true;
    document.getElementById('lib-extract-status').textContent = '';
    document.getElementById('lib-url-input').value = '';
    document.getElementById('lib-url-status').textContent = '';
    ['n-name','n-price','n-area','n-addr','n-bping','n-lping','n-aping','n-layout','n-floor','n-age','n-parking','n-case','n-env'].forEach(function(id) {
      document.getElementById(id).value = '';
    });
    loadLibAdHistory();
    var m = document.getElementById('new-prop-modal');
    m.classList.remove('hidden'); m.classList.add('flex');
  }

  function closeNewModal() {
    var m = document.getElementById('new-prop-modal');
    m.classList.add('hidden'); m.classList.remove('flex');
  }

  // ── 圖片選取 ──
  function onLibImageSelected(ev) {
    var f = ev.target && ev.target.files && ev.target.files[0];
    _libImageFile = f || null;
    document.getElementById('lib-image-name').textContent = f ? f.name : '';
    document.getElementById('lib-extract-btn').disabled = !_libImageFile;
    document.getElementById('lib-extract-status').textContent = '';
  }

  // ── 剪貼簿貼上 ──
  async function handleLibPaste() {
    try {
      var items = await navigator.clipboard.read();
      for (var item of items) {
        for (var type of item.types) {
          if (type.startsWith('image/')) {
            var blob = await item.getType(type);
            _libImageFile = new File([blob], 'pasted.png', { type: type });
            document.getElementById('lib-image-name').textContent = '已貼上圖片';
            document.getElementById('lib-extract-btn').disabled = false;
            document.getElementById('lib-extract-status').textContent = '';
            return;
          }
        }
      }
      toast('剪貼簿沒有圖片，請先複製一張截圖', 'error');
    } catch (e) {
      toast('無法讀取剪貼簿，請改用「選擇圖片」', 'error');
    }
  }

  // ── 將辨識結果填入表單 ──
  function fillLibForm(ext) {
    if (!ext) return;
    // 欄位 ID 與 extracted 欄位對應表
    var mapping = [
      ['n-name',    ext.project_name],
      ['n-price',   ext.price != null ? String(ext.price) : ''],
      ['n-area',    ext.location_area],
      ['n-addr',    ext.address],
      ['n-bping',   ext.building_ping != null ? String(ext.building_ping) : ''],
      ['n-lping',   ext.land_ping != null ? String(ext.land_ping) : ''],
      ['n-aping',   ext.authority_ping != null ? String(ext.authority_ping) : ''],
      ['n-layout',  ext.layout],
      ['n-floor',   ext.floor],
      ['n-age',     ext.age],
      ['n-parking', ext.parking],
      ['n-case',    ext.case_number],
    ];
    mapping.forEach(function(pair) {
      var el = document.getElementById(pair[0]);
      if (!el) { console.warn('[fillLibForm] 找不到元素 #' + pair[0]); return; }
      el.value = pair[1] || '';
    });
  }

  // ── 圖片辨識（透過 Library 代理路由） ──
  async function runLibExtractFromImage() {
    if (!_libImageFile) return;
    var statusEl = document.getElementById('lib-extract-status');
    var btn = document.getElementById('lib-extract-btn');
    statusEl.textContent = '辨識中…';
    btn.disabled = true;
    try {
      var fd = new FormData();
      fd.append('image', _libImageFile);
      var r = await fetch('/api/extract-from-image', { method: 'POST', body: fd });
      var d = await r.json();
      if (d.ok && d.extracted) {
        fillLibForm(d.extracted);
        statusEl.textContent = '已帶入欄位，剩餘 ' + (d.points ?? '') + ' 點';
        statusEl.className = 'text-xs text-emerald-400 mt-2 min-h-[1em]';
      } else {
        statusEl.textContent = d.error || '辨識失敗';
        statusEl.className = 'text-xs text-rose-400 mt-2 min-h-[1em]';
      }
    } catch (e) {
      statusEl.textContent = '連線失敗：' + (e.message || '');
      statusEl.className = 'text-xs text-rose-400 mt-2 min-h-[1em]';
    }
    btn.disabled = false;
  }

  // ── 網址截圖辨識 ──
  async function runLibExtractFromUrl() {
    var url = (document.getElementById('lib-url-input').value || '').trim();
    if (!url) { document.getElementById('lib-url-status').textContent = '請輸入網址'; return; }
    var statusEl = document.getElementById('lib-url-status');
    var btn = document.getElementById('lib-url-btn');
    statusEl.textContent = '截圖與辨識中…（約 15–30 秒）';
    statusEl.className = 'text-xs text-slate-400 mt-2 min-h-[1em]';
    btn.disabled = true;
    try {
      // 1. 送出非同步工作，取得 job_id
      var r = await fetch('/api/extract-from-url', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ url: url }),
      });
      var d = await r.json();
      if (d.error) {
        statusEl.textContent = d.error;
        statusEl.className = 'text-xs text-rose-400 mt-2 min-h-[1em]';
        btn.disabled = false;
        return;
      }
      var jobId = d.job_id;
      // 2. 輪詢結果（每 2 秒輪詢一次，最多 60 次 = 2 分鐘）
      var dots = 0;
      for (var i = 0; i < 60; i++) {
        await new Promise(function(res) { setTimeout(res, 2000); });
        dots = (dots + 1) % 4;
        statusEl.textContent = '截圖與辨識中' + '.'.repeat(dots + 1) + '（約 15–30 秒）';
        var pr = await fetch('/api/extract-from-url/poll/' + jobId);
        var pd = await pr.json();
        if (pd.error && pd.done === undefined) {
          // 工作不存在
          statusEl.textContent = pd.error;
          statusEl.className = 'text-xs text-rose-400 mt-2 min-h-[1em]';
          btn.disabled = false;
          return;
        }
        if (pd.done) {
          if (pd.ok && pd.extracted) {
            // 顯示截圖預覽（在 console 點圖片可看 Screenshotone 截到什麼）
            if (pd.debug_img) {
              var img = new Image(); img.src = 'data:image/jpeg;base64,' + pd.debug_img;
              console.log('[截圖辨識] 截圖預覽（右鍵另存可查看）:', img);
            }
            console.log('[截圖辨識] extracted:', JSON.stringify(pd.extracted));
            fillLibForm(pd.extracted);
            // 顯示辨識到的欄位名稱，方便確認
            var keys = Object.keys(pd.extracted).filter(function(k){ return pd.extracted[k] != null && pd.extracted[k] !== ''; });
            statusEl.textContent = '✅ 辨識完成，已帶入：' + (keys.length ? keys.join(', ') : '（無資料）');
            statusEl.className = 'text-xs text-emerald-400 mt-2 min-h-[1em]';
          } else {
            statusEl.textContent = pd.error || '截圖或辨識失敗';
            statusEl.className = 'text-xs text-rose-400 mt-2 min-h-[1em]';
          }
          btn.disabled = false;
          return;
        }
      }
      statusEl.textContent = '辨識逾時，請稍後再試';
      statusEl.className = 'text-xs text-rose-400 mt-2 min-h-[1em]';
    } catch (e) {
      statusEl.textContent = '連線失敗：' + (e.message || '');
      statusEl.className = 'text-xs text-rose-400 mt-2 min-h-[1em]';
    }
    btn.disabled = false;
  }

  // ── 載入 AD 歷史 ──
  async function loadLibAdHistory() {
    var sel = document.getElementById('lib-ad-history-select');
    var first = sel.options[0];
    sel.innerHTML = '';
    sel.appendChild(first);
    try {
      var r = await fetch('/api/proxy/ad-history-list');
      var list = await r.json();
      if (Array.isArray(list) && list.length) {
        list.forEach(function(p) {
          var opt = document.createElement('option');
          opt.value = p.id || '';
          opt.textContent = (p.title || p.project_name || p.id || '未命名') + (p.created_at ? ' · ' + (p.created_at+'').slice(0,10) : '');
          sel.appendChild(opt);
        });
      }
    } catch (e) {}
  }

  // ── 從 AD 歷史匯入 ──
  async function libImportFromAd() {
    var sel = document.getElementById('lib-ad-history-select');
    var id = (sel && sel.value || '').trim();
    var statusEl = document.getElementById('lib-import-status');
    if (!id) { statusEl.textContent = '請先選擇一筆 AD 歷史'; statusEl.className = 'text-xs text-amber-400 mt-2 min-h-[1em]'; return; }
    statusEl.textContent = '匯入中…'; statusEl.className = 'text-xs text-slate-400 mt-2 min-h-[1em]';
    try {
      var r = await fetch('/api/proxy/import-from-ad-history', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ ad_history_id: id }),
      });
      var d = await r.json();
      if (d.ok && d.property) {
        fillLibForm(d.property);
        if (d.property.env_description) document.getElementById('n-env').value = d.property.env_description;
        statusEl.textContent = '已匯入並帶入表單，可編輯後儲存';
        statusEl.className = 'text-xs text-emerald-400 mt-2 min-h-[1em]';
      } else {
        statusEl.textContent = d.error || '匯入失敗';
        statusEl.className = 'text-xs text-rose-400 mt-2 min-h-[1em]';
      }
    } catch (e) {
      statusEl.textContent = '連線失敗：' + (e.message || '');
      statusEl.className = 'text-xs text-rose-400 mt-2 min-h-[1em]';
    }
  }

  // ── 儲存新物件 ──
  async function saveNewProp() {
    var payload = {
      project_name:   document.getElementById('n-name').value.trim(),
      price:          document.getElementById('n-price').value.trim(),
      location_area:  document.getElementById('n-area').value.trim(),
      address:        document.getElementById('n-addr').value.trim(),
      building_ping:  document.getElementById('n-bping').value.trim(),
      land_ping:      document.getElementById('n-lping').value.trim(),
      authority_ping: document.getElementById('n-aping').value.trim(),
      layout:         document.getElementById('n-layout').value.trim(),
      floor:          document.getElementById('n-floor').value.trim(),
      age:            document.getElementById('n-age').value.trim(),
      parking:        document.getElementById('n-parking').value.trim(),
      case_number:    document.getElementById('n-case').value.trim(),
      env_description:document.getElementById('n-env').value.trim(),
    };
    if (!payload.project_name && !payload.address) {
      toast('至少填寫物件名稱或地址', 'error'); return;
    }
    try {
      var r = await fetch(apiUrl('/api/objects'), {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload),
      });
      var d = await r.json();
      if (d.ok || d.id) {
        toast('物件已儲存', 'success');
        closeNewModal();
        loadList();
      } else {
        toast('儲存失敗：' + (d.error || ''), 'error');
      }
    } catch (e) { toast('連線失敗：' + (e.message || ''), 'error'); }
  }

  // ── 編輯物件（沿用舊表單） ──
  function editObj(id) {
    document.getElementById('listPanel').classList.add('hidden');
    document.getElementById('formPanel').classList.remove('hidden');
    document.getElementById('detailPanel').classList.add('hidden');
    document.getElementById('formTitle').textContent = '編輯物件';
    document.getElementById('objForm').reset();
    document.getElementById('objId').value = id;
    fetch(apiUrl('/api/objects/' + encodeURIComponent(id))).then(r => r.json()).then(o => {
      if (o.error) { toast(o.error, 'error'); return; }
      fields.forEach(function(kv){ var k=kv[0]; var el=document.getElementById('f_'+k); if(el) el.value = o[k]!=null ? o[k] : ''; });
      document.getElementById('objId').value = o.id || id;
    });
  }

  function hideForm() {
    document.getElementById('formPanel').classList.add('hidden');
    document.getElementById('listPanel').classList.remove('hidden');
    loadList();
  }

  document.getElementById('objForm').onsubmit = function(e) {
    e.preventDefault();
    var id = document.getElementById('objId').value;
    var payload = {};
    fields.forEach(function(kv){ var k=kv[0]; var el=document.getElementById('f_'+k); if(el) payload[k]=el.value; });
    if (id) payload.id = id;
    fetch(apiUrl('/api/objects/' + encodeURIComponent(id)), {
      method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)
    }).then(r => r.json()).then(function(d){ if (d.error) toast(d.error, 'error'); else hideForm(); }).catch(function(){ toast('儲存失敗', 'error'); });
  };

  // ── 查看詳情 ──
  function viewDetail(id) {
    fetch(apiUrl('/api/objects/' + encodeURIComponent(id))).then(r => r.json()).then(o => {
      if (o.error) { toast(o.error, 'error'); return; }
      window._detailId = id;
      document.getElementById('listPanel').classList.add('hidden');
      document.getElementById('formPanel').classList.add('hidden');
      document.getElementById('detailPanel').classList.remove('hidden');
      document.getElementById('detailTitle').textContent = o.custom_title || o.project_name || o.address || id || '物件詳情';
      var html = '';
      fields.forEach(function(kv){ var k=kv[0], l=kv[1], v=o[k]; if (v==null||v==='') return; html += '<p><strong class="text-slate-400">'+escapeHtml(l)+'</strong>：'+escapeHtml(String(v))+'</p>'; });
      if (o.ad_outputs && o.ad_outputs.length) {
        html += '<div class="mt-4 pt-3 border-t border-slate-600">';
        html += '<p class="text-xs text-slate-400 mb-3 font-medium">廣告產出</p>';
        o.ad_outputs.forEach(function(ad) {
          html += '<div class="mb-4 bg-slate-700/50 rounded-xl p-3 border border-slate-600">';
          html += '<p class="text-xs font-semibold text-blue-400 mb-2">' + escapeHtml(ad.type || ad.title || '') + '</p>';
          html += '<div class="text-sm text-slate-200 leading-relaxed whitespace-pre-wrap">' + escapeHtml(ad.content || '（無內容）') + '</div>';
          html += '</div>';
        });
        html += '</div>';
      }
      document.getElementById('detailContent').innerHTML = html || '<p class="text-slate-500">無內容</p>';
    });
  }

  function editCurrentDetail() { if (window._detailId) editObj(window._detailId); }
  function closeDetail() { document.getElementById('detailPanel').classList.add('hidden'); document.getElementById('listPanel').classList.remove('hidden'); loadList(); }

  function delObj(id) {
    if (!confirm('確定刪除此物件？')) return;
    fetch(apiUrl('/api/objects/' + encodeURIComponent(id)), { method: 'DELETE' }).then(r => r.json()).then(function(d){
      if (d.error) toast(d.error, 'error'); else { toast('已刪除', 'success'); loadList(); }
    });
  }

  try { loadUsers(); } catch(e) { console.error('loadUsers 失敗:', e); }
  try { loadList();  } catch(e) { console.error('loadList 失敗:', e); }

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

  // ══ 分頁切換 ══
  function switchTab(tab) {
    var paneMyEl       = document.getElementById('pane-my');
    var paneCompanyEl  = document.getElementById('pane-company');
    var paneSettingsEl = document.getElementById('pane-settings');
    var btnNewObj       = document.getElementById('btn-new-obj');
    var btnNewObjMobile = document.getElementById('btn-new-obj-mobile');

    // 全部隱藏（加 null check 防止任一元素不存在時崩潰）
    if (paneMyEl)       paneMyEl.style.display       = 'none';
    if (paneCompanyEl)  paneCompanyEl.style.display  = 'none';
    if (paneSettingsEl) paneSettingsEl.style.display = 'none';
    if (btnNewObj)       btnNewObj.style.display       = 'none';
    if (btnNewObjMobile) btnNewObjMobile.style.display = 'none';

    if (tab === 'my') {
      if (paneMyEl) paneMyEl.style.display = 'block';
      if (btnNewObj)       btnNewObj.style.display       = '';
      if (btnNewObjMobile) btnNewObjMobile.style.display = '';
    } else if (tab === 'company') {
      if (paneCompanyEl) paneCompanyEl.style.display = 'block';
    } else if (tab === 'buyers') {
      if (paneBuyersEl) paneBuyersEl.style.display = 'block';
    } else if (tab === 'war') {
      if (paneWarEl) paneWarEl.style.display = 'block';
    } else if (tab === 'settings') {
      if (paneSettingsEl) paneSettingsEl.style.display = 'block';
      agentEmailLoad();  // 進入設定頁自動載入列表
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

  function cpFetch() {
    var list = document.getElementById('cp-list');
    list.innerHTML = '<p class="text-slate-400 text-center py-8">載入中…</p>';
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
          if (data.error) return Promise.reject(data.error);
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
        if (data.error) { list.innerHTML = '<p class="text-red-400 text-center py-8">' + escapeHtml(data.error) + '</p>'; return; }
        var items = data.items || [];
        if (!items.length) {
          list.innerHTML = '<p class="text-slate-500 text-center py-10">找不到符合條件的物件</p>';
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
          // 若有舊案名（案名曾改動），顯示「原：舊案名」灰色小字
          var oldNameBadge = item['舊案名']
            ? '<span style="font-size:0.75rem;color:var(--txm);font-style:italic;">原：' + escapeHtml(item['舊案名']) + '</span>'
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
          if (oldNameBadge) html += '<p class="truncate mt-0">' + oldNameBadge + '</p>';
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
            panel.innerHTML = '<p class="text-xs text-slate-500 text-center py-2">載入中…</p>';
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
                  panel.innerHTML = '<p class="text-xs text-slate-400 text-center py-2">尚無帶看紀錄</p>' + addLink;
                  return;
                }
                var html = '<div class="space-y-1.5">';
                items.forEach(function(s) {
                  var icon = reactionIcon[s.reaction] || '•';
                  html += '<div class="text-xs text-slate-300 leading-snug">'
                        + '<span class="mr-1">' + icon + '</span>'
                        + '<span class="font-medium text-slate-100">' + escapeHtml(s.buyer_name) + '</span>'
                        + '<span class="text-slate-500 ml-2">' + escapeHtml(s.date) + '</span>'
                        + (s.note ? '<span class="block text-slate-500 pl-4 italic truncate">' + escapeHtml(s.note) + '</span>' : '')
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
      list.innerHTML = '<p class="text-red-400 text-center py-8">載入失敗：' + escapeHtml(String(e)) + '</p>';
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
        return '<div class="flex gap-2 py-1.5 border-b border-slate-700/40 last:border-0">'
          + '<span class="text-slate-500 w-20 shrink-0 text-xs pt-0.5">' + escapeHtml(label) + '</span>'
          + '<span class="text-slate-200 text-sm flex-1">' + fmtVal(key, v) + '</span></div>';
      }

      // 渲染分組區塊
      function section(icon, title, rows) {
        var inner = rows.join('');
        if (!inner) return '';
        return '<div class="mb-3 bg-slate-800/60 rounded-xl border border-slate-700/60 overflow-hidden">'
          + '<div class="flex items-center gap-2 px-4 py-2 bg-slate-700/40 border-b border-slate-700/60">'
          + '<span class="text-base">' + icon + '</span>'
          + '<span class="text-xs font-semibold text-slate-300 tracking-wide">' + title + '</span></div>'
          + '<div class="px-4 py-1">' + inner + '</div></div>';
      }

      // 狀態徽章
      var isSelling = data['銷售中'] !== false && data['銷售中'] !== '已下架' && data['銷售中'] !== '已成交';
      var statusBadge = isSelling
        ? '<span class="inline-block bg-green-700 text-green-200 text-xs px-2 py-0.5 rounded-full">銷售中</span>'
        : (data['成交日期']
            ? '<span class="inline-block bg-blue-900 text-blue-300 text-xs px-2 py-0.5 rounded-full">已成交</span>'
            : '<span class="inline-block bg-slate-600 text-slate-400 text-xs px-2 py-0.5 rounded-full">已下架</span>');

      var html = '';

      // ── 狀態橫幅 ──
      html += '<div class="flex items-center gap-3 mb-3 px-1">'
        + statusBadge
        + (data['物件類別'] ? '<span class="text-xs text-slate-400 bg-slate-700 px-2 py-0.5 rounded-full">' + escapeHtml(data['物件類別']) + '</span>' : '')
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
        el.textContent = dt.toLocaleString('zh-TW') + '\uff08\u5beb\u5165 ' + (r.written||0) + ' \u7b46\uff0c\u522a\u9664 ' + (r.deleted||0) + ' \u7b46\uff09';
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
            window._cpSearched = false;
            cpSearch();
            toast('\u540c\u6b65\u5b8c\u6210\uff01', 'success');
          }
        });
      }, 3000);
    }).catch(function(e){ toast('\u547c\u53eb\u5931\u6557: ' + e, 'error'); btn.disabled=false; });
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
        // Tab Bar 廣告和周邊連結（透過 Portal /api/enter/ 跳轉）
        var portalBase = PORTAL_URL_JS.replace(/[/]$/, '');
        var tbAd = document.getElementById('tb-ad');
        if (tbAd) { tbAd.href = portalBase + '/api/enter/post'; tbAd.target = 'tool-post'; tbAd.classList.remove('hidden'); }
        var tbSurvey = document.getElementById('tb-survey');
        if (tbSurvey) { tbSurvey.href = portalBase + '/api/enter/survey'; tbSurvey.target = 'tool-survey'; tbSurvey.classList.remove('hidden'); }
        // Sidebar 廣告和周邊連結
        var sbAd = document.getElementById('sb-ad');
        if (sbAd) { sbAd.href = portalBase + '/api/enter/post'; sbAd.target = 'tool-post'; sbAd.classList.remove('hidden'); }
        var sbSurvey = document.getElementById('sb-survey');
        if (sbSurvey) { sbSurvey.href = portalBase + '/api/enter/survey'; sbSurvey.target = 'tool-survey'; sbSurvey.classList.remove('hidden'); }
        var ddPlans = document.getElementById('dd-plans');
        if (ddPlans) { ddPlans.href = plansUrl; ddPlans.classList.remove('hidden'); }
        var ddAccount = document.getElementById('dd-account');
        if (ddAccount) { ddAccount.href = accountUrl; ddAccount.classList.remove('hidden'); }
        if (u.is_admin) {
          var ddAdmin = document.getElementById('dd-admin');
          if (ddAdmin) { ddAdmin.href = adminUrl; ddAdmin.classList.remove('hidden'); }
        }
      }
      // 買方管理連結（sidebar 和 tab bar，不放 dropdown）
      if (BUYER_URL_JS) {
        var sbBuyer = document.getElementById('sb-buyer');
        if (sbBuyer) { sbBuyer.href = BUYER_URL_JS; sbBuyer.target = '_blank'; sbBuyer.classList.remove('hidden'); }
        var tbBuyer = document.getElementById('tb-buyer');
        if (tbBuyer) { tbBuyer.href = BUYER_URL_JS; tbBuyer.classList.remove('hidden'); }
      }
    }).catch(function(){});
  })();

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
    var _PORTAL = (PORTAL_URL_JS || '').replace(/[/]$/, '');

    function _applyTheme() {
      var sys = window.matchMedia('(prefers-color-scheme: dark)').matches;
      var eff = _mode === 'system' ? (sys ? 'dark' : 'light') : _mode;
      if (DARK_ONLY.indexOf(_style) >= 0) eff = 'dark';
      document.body.setAttribute('data-theme', (STYLE_MODES[_style] || STYLE_MODES.navy)[eff]);
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

    window._tpSetMode = function(m) { _mode = m; localStorage.setItem('up_mode', m); _applyTheme(); };
    window._tpAdminSetStyle = function(s) { _style = s; localStorage.setItem("up_style", s); _applyTheme(); };
    window._tpSaveStyle = function() {
      if (!_PORTAL) return;
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

    // 立即套用快取防閃白
    (function() {
      var s = localStorage.getItem('up_style') || 'navy';
      var m = localStorage.getItem('up_mode') || 'system';
      var sys = window.matchMedia('(prefers-color-scheme: dark)').matches;
      var eff = m === 'system' ? (sys ? 'dark' : 'light') : m;
      if (DARK_ONLY.indexOf(s) >= 0) eff = 'dark';
      document.body.setAttribute('data-theme', (STYLE_MODES[s] || STYLE_MODES.navy)[eff]);
    })();

    document.addEventListener('DOMContentLoaded', function() {
      _mode = localStorage.getItem('up_mode') || 'system';
      _style = localStorage.getItem('up_style') || 'navy';
      _applyTheme();
      // 取後台最新風格
      if (_PORTAL) {
        fetch('/api/theme').then(function(r){ return r.json(); }).then(function(d) {
          if (d.style && d.style !== _style) {
            _style = d.style; localStorage.setItem('up_style', _style); _applyTheme();
          }
        }).catch(function(){});
      }
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
</body>
</html>
"""


if __name__ == "__main__":
    os.makedirs(USERS_DIR, exist_ok=True)
    port = int(os.environ.get("PORT", "5004"))
    app.run(debug=bool(os.environ.get("FLASK_DEBUG")), host="0.0.0.0", port=port)
