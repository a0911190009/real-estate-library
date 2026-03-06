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
from datetime import datetime, timezone

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
ADMIN_EMAILS = [e.strip() for e in (os.environ.get("ADMIN_EMAILS") or "").split(",") if e.strip()]
SERVICE_API_KEY = (os.environ.get("SERVICE_API_KEY") or "").strip()
TOKEN_SERIALIZER = URLSafeTimedSerializer(app.secret_key)
TOKEN_MAX_AGE = 60

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
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
        return result

    except Exception as e:
        log.exception("同步失敗")
        return {"ok": False, "error": str(e), "started": started}


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


# ── 買方需求 CRUD API（Firestore buyers 集合） ──

def _buyers_col():
    """取得 buyers Firestore 集合參照。"""
    db = _get_db()
    if db is None:
        return None
    return db.collection("buyers")


@app.route("/api/buyers", methods=["GET"])
def api_buyers_list():
    """列出所有買方需求（管理員可看全部，一般用戶只看自己）。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    col = _buyers_col()
    if col is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    try:
        if _is_admin(email):
            docs = list(col.limit(200).stream())
        else:
            # 不加 order_by 避免需要複合索引，用 Python 端排序
            docs = list(col.where("created_by", "==", email).limit(200).stream())
        items = [{"id": d.id, **d.to_dict()} for d in docs]
        # Python 端依 created_at 降冪排序（新的在前）
        items.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/buyers", methods=["POST"])
def api_buyers_create():
    """新增買方需求。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    col = _buyers_col()
    if col is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    data = request.get_json() or {}
    if not data.get("name"):
        return jsonify({"error": "買方姓名必填"}), 400
    now = datetime.now(timezone.utc).isoformat()
    data["created_by"] = email
    data["created_at"] = now
    data["updated_at"] = now
    ref = col.document()
    ref.set(data)
    return jsonify({"id": ref.id, **data}), 201


@app.route("/api/buyers/<buyer_id>", methods=["GET"])
def api_buyers_get(buyer_id):
    """取得單筆買方需求。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    col = _buyers_col()
    if col is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    doc = col.document(buyer_id).get()
    if not doc.exists:
        return jsonify({"error": "找不到此買方"}), 404
    d = doc.to_dict()
    if not _is_admin(email) and d.get("created_by") != email:
        return jsonify({"error": "無權限"}), 403
    return jsonify({"id": doc.id, **d})


@app.route("/api/buyers/<buyer_id>", methods=["PUT"])
def api_buyers_update(buyer_id):
    """更新買方需求。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    col = _buyers_col()
    if col is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    doc = col.document(buyer_id).get()
    if not doc.exists:
        return jsonify({"error": "找不到此買方"}), 404
    d = doc.to_dict()
    if not _is_admin(email) and d.get("created_by") != email:
        return jsonify({"error": "無權限"}), 403
    data = request.get_json() or {}
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    col.document(buyer_id).update(data)
    return jsonify({"ok": True})


@app.route("/api/buyers/<buyer_id>", methods=["DELETE"])
def api_buyers_delete(buyer_id):
    """刪除買方需求。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]
    col = _buyers_col()
    if col is None:
        return jsonify({"error": "Firestore 未連線"}), 503
    doc = col.document(buyer_id).get()
    if not doc.exists:
        return jsonify({"error": "找不到此買方"}), 404
    d = doc.to_dict()
    if not _is_admin(email) and d.get("created_by") != email:
        return jsonify({"error": "無權限"}), 403
    col.document(buyer_id).delete()
    return jsonify({"ok": True})


@app.route("/api/buyers/<buyer_id>/match", methods=["GET"])
def api_buyers_match(buyer_id):
    """根據買方需求條件，配對公司物件庫。"""
    email, err = _require_user()
    if err:
        return jsonify({"error": err[0]}), err[1]

    buyers_col = _buyers_col()
    db = _get_db()
    if buyers_col is None or db is None:
        return jsonify({"error": "Firestore 未連線"}), 503

    # 取得買方資料
    bdoc = buyers_col.document(buyer_id).get()
    if not bdoc.exists:
        return jsonify({"error": "找不到此買方"}), 404
    buyer = bdoc.to_dict()
    if not _is_admin(email) and buyer.get("created_by") != email:
        return jsonify({"error": "無權限"}), 403

    # 從 Firestore 取物件（先用可索引條件縮小範圍）
    prop_col = db.collection("company_properties")
    query = prop_col

    areas = [a.strip() for a in str(buyer.get("areas", "")).split(",") if a.strip()]
    # 將類別展開（支援大類，「其他」特殊處理）
    raw_cats = [c.strip() for c in str(buyer.get("categories", "")).split(",") if c.strip()]
    categories = []
    has_other_cat = False
    for c in raw_cats:
        if c == _OTHER_GROUP:
            has_other_cat = True
        else:
            categories.extend(_expand_category_group(c))

    # Firestore 只能單欄位 ==，多條件用 Python 端過濾
    if len(areas) == 1:
        query = query.where("鄉/市/鎮", "==", areas[0])
    # 類別展開後可能多個，統一在 Python 端過濾

    # 全量讀取，不設上限
    docs = list(query.stream())
    results = [{"id": d.id, **d.to_dict()} for d in docs]

    # Python 端多條件過濾
    price_min = buyer.get("price_min")
    price_max = buyer.get("price_max")
    ping_min  = buyer.get("ping_min")
    ping_max  = buyer.get("ping_max")
    status_req = buyer.get("status", "selling")  # selling / sold / all

    all_known_cats_match = {c for cats in CATEGORY_GROUPS.values() for c in cats}

    def match(r):
        # 地區過濾
        if areas:
            if r.get("鄉/市/鎮") not in areas:
                return False
        # 類別過濾（已展開為原始類別 list）
        if categories or has_other_cat:
            rc = r.get("物件類別")
            in_expanded = rc in categories if categories else False
            in_other = (rc not in all_known_cats_match) if has_other_cat else False
            if not in_expanded and not in_other:
                return False
        # 銷售狀態
        if status_req == "selling" and r.get("銷售中") is False:
            return False
        if status_req == "sold" and r.get("銷售中") is not False:
            return False
        # 售價範圍
        price = _parse_price(r.get("售價(萬)"))
        if price_min and price is not None and price < float(price_min):
            return False
        if price_max and price is not None and price > float(price_max):
            return False
        # 坪數範圍（優先建坪，次選地坪）
        ping = _parse_price(r.get("建坪") or r.get("地坪"))
        if ping_min and ping is not None and ping < float(ping_min):
            return False
        if ping_max and ping is not None and ping > float(ping_max):
            return False
        return True

    matched = [r for r in results if match(r)]

    # 排序：售價升冪
    matched.sort(key=lambda r: (_parse_price(r.get("售價(萬)")) or 99999))

    # 移除個資欄位
    sensitive = {"身份証字號", "室內電話1", "行動電話1",
                 "連絡人室內電話2", "連絡人行動電話2",
                 "買方電話", "買方生日", "賣方生日",
                 "買方姓名", "買方住址", "_imported", "_synced_at"}
    for r in matched:
        for k in sensitive:
            r.pop(k, None)

    return jsonify({
        "total": len(matched),
        "buyer_name": buyer.get("name", ""),
        "items": matched[:50]   # 最多回傳50筆
    })


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
    page       = max(1, int(request.args.get("page", 1)))
    per_page   = 20

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

        # Firestore 只能單欄位 == 過濾，全量讀取後在 Python 端過濾
        # 只有選單一地區且無其他地區複選時才下推到 Firestore
        if len(areas) == 1:
            query = query.where("鄉/市/鎮", "==", areas[0])

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

        # Python 端：地區多選（已有單選下推，這裡補多選情況）
        if len(areas) > 1:
            results = [r for r in results if r.get("鄉/市/鎮") in set(areas)]

        # Python 端：關鍵字
        if keyword:
            kw = keyword.lower()
            results = [r for r in results if
                       kw in str(r.get("案名", "")).lower() or
                       kw in str(r.get("物件地址", "")).lower() or
                       kw in str(r.get("委託編號", "")).lower()]

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

        # Python 端：狀態（銷售中 / 已成交 / 已下架）
        # Firestore 中：銷售中=True 或無此欄位、已成交=False 且有成交日期、已下架=False 且無成交日期
        if status == "selling":
            results = [r for r in results if r.get("銷售中") is not False]
        elif status == "sold":
            results = [r for r in results if r.get("銷售中") is False and r.get("成交日期")]
        elif status == "delisted":
            results = [r for r in results if r.get("銷售中") is False and not r.get("成交日期")]

        # Python 端：經紀人多選（包含比對，應對多人合寫情況）
        if agents:
            def _agent_match(r):
                raw = str(r.get("經紀人", ""))
                return any(ag in raw for ag in agents)
            results = [r for r in results if _agent_match(r)]

        # 排序：資料序號降冪（新資料在前）
        results.sort(key=lambda r: -int(r.get("資料序號", 0) or 0))

        total = len(results)
        start = (page - 1) * per_page
        page_data = results[start:start + per_page]

        # 列表只回傳卡片需要的欄位（減少傳輸量）
        card_fields = {
            "id", "案名", "物件地址", "物件類別", "售價(萬)",
            "建坪", "地坪", "經紀人", "銷售中", "成交日期", "委託到期日",
            "資料序號", "鄉/市/鎮"
        }
        slim = [{k: r[k] for k in card_fields if k in r} for r in page_data]
        # 補上 id（已在 card_fields，但確保有）
        for orig, s in zip(page_data, slim):
            s["id"] = orig["id"]

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

            for e in all_entries:
                name = e.get("案名","").strip()
                price = e.get("售價萬","")
                comm  = str(e.get("委託號碼","") or "").zfill(6) if e.get("委託號碼") else ""
                if not name or not price:
                    continue
                try:
                    price_f = float(str(price).replace(",",""))
                except Exception:
                    continue
                key = _norm(name)
                if not key:
                    continue
                existing = results.get(key)
                # 保留委託號碼較大（較新）的
                if not existing or comm > existing.get("委託號碼",""):
                    results[key] = {"案名": name, "委託號碼": comm, "售價萬": price_f}
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

    # 存入 Firestore（單一文件，記錄上傳時間與解析結果）
    now_str = datetime.now(timezone.utc).isoformat()
    doc_ref = db.collection("word_snapshot").document("latest")
    doc_ref.set({
        "uploaded_at": now_str,
        "uploaded_by": email,
        "filename":    f.filename,
        "count":       len(price_map),
        "prices":      price_map,   # {normalized案名: {案名, 委託號碼, 售價萬}}
    })

    return jsonify({
        "ok": True,
        "uploaded_at": now_str,
        "count": len(price_map),
        "message": f"解析完成，共 {len(price_map)} 筆物件售價已更新"
    })


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
            '<div class="flex items-center gap-3 px-2 py-2 mb-3 bg-slate-800/60 rounded-xl border border-slate-700 text-sm text-slate-400">'
            '<span>查看用戶：</span>'
            '<select id="userSelect" class="bg-slate-700 border border-slate-600 rounded-lg px-3 py-1 text-slate-200 text-sm focus:outline-none">'
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
        lbl = f'<label class="block text-xs text-slate-400 mb-1" for="f_{key}">{label}</label>'
        if key in textarea_keys:
            inp = (f'<textarea id="f_{key}" name="{key}" rows="3"'
                   f' class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 resize-none focus:outline-none focus:border-blue-500"'
                   f' placeholder="{label}"></textarea>')
        else:
            inp = (f'<input type="text" id="f_{key}" name="{key}"'
                   f' class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500"'
                   f' placeholder="{label}">')
        fields_html_parts.append(f"{div_class}{lbl}{inp}</div>")
    fields_html = "\n        ".join(fields_html_parts)

    # 用 Python 字串替換，完全避免 Jinja2 誤解析 JS {} 語法
    html = OBJECTS_APP_HTML
    html = html.replace("__PORTAL_LINK__", portal_link)
    html = html.replace("__FIELDS_JSON__", json.dumps(fields, ensure_ascii=False))
    html = html.replace("__IS_ADMIN_JSON__", json.dumps(is_admin))
    html = html.replace("__ADMIN_BAR__", admin_bar)
    html = html.replace("__FIELDS_HTML__", fields_html)
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
</head>
<body class="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 text-slate-100 font-sans antialiased">

<div id="toast-container"></div>

<!-- 頂部導覽 -->
<header class="sticky top-0 z-50 bg-slate-900/95 backdrop-blur border-b border-slate-700 shadow z-50">
  <div class="flex items-center justify-between px-5 py-3">
    <span class="font-bold text-slate-100">📁 物件庫</span>
    <div class="flex gap-2">
      <a href="__PORTAL_LINK__" target="tool-portal" class="px-3 py-2 rounded-lg bg-slate-700 hover:bg-slate-600 text-slate-300 text-sm transition">🏠 返回入口</a>
      <button type="button" id="btn-new-obj" onclick="openNewModal()"
        class="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition shadow">
        ＋ 建立物件資訊
      </button>
    </div>
  </div>
  <!-- 分頁標籤 -->
  <div class="flex border-t border-slate-700/60">
    <button id="tab-my" onclick="switchTab('my')"
      class="tab-btn flex-1 py-2 text-sm font-medium text-blue-400 border-b-2 border-blue-500 transition">
      📂 我的物件
    </button>
    <button id="tab-company" onclick="switchTab('company')"
      class="tab-btn flex-1 py-2 text-sm font-medium text-slate-400 border-b-2 border-transparent hover:text-slate-200 transition">
      🏢 公司物件庫
    </button>
    <button id="tab-buyers" onclick="switchTab('buyers')"
      class="tab-btn flex-1 py-2 text-sm font-medium text-slate-400 border-b-2 border-transparent hover:text-slate-200 transition">
      👥 買方需求
    </button>
  </div>
</header>

<!-- ══ 我的物件分頁 ══ -->
<div id="pane-my" class="max-w-3xl mx-auto px-4 py-6">
  __ADMIN_BAR__
  <div id="listPanel" class="space-y-3"></div>

  <!-- 編輯物件面板（原地編輯用，新增改由 Modal） -->
  <div id="formPanel" class="hidden bg-slate-800 rounded-2xl border border-slate-600 p-5 mb-4">
    <h2 id="formTitle" class="font-bold text-slate-100 mb-4">編輯物件</h2>
    <form id="objForm">
      <input type="hidden" id="objId" name="id">
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
        __FIELDS_HTML__
      </div>
      <div class="flex gap-3 mt-4">
        <button type="submit" class="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition">儲存</button>
        <button type="button" onclick="hideForm()" class="px-4 py-2 rounded-lg bg-slate-700 hover:bg-slate-600 text-slate-300 text-sm transition">取消</button>
      </div>
    </form>
  </div>

  <!-- 詳情面板 -->
  <div id="detailPanel" class="hidden bg-slate-800 rounded-2xl border border-slate-600 p-5 mb-4">
    <h2 id="detailTitle" class="font-bold text-slate-100 mb-3">物件詳情</h2>
    <div id="detailContent" class="space-y-1 text-sm text-slate-300"></div>
    <div class="flex gap-3 mt-4">
      <button type="button" onclick="editCurrentDetail()" class="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition">編輯</button>
      <button type="button" onclick="closeDetail()" class="px-4 py-2 rounded-lg bg-slate-700 hover:bg-slate-600 text-slate-300 text-sm transition">關閉</button>
    </div>
  </div>
</div>

<!-- ══ 公司物件庫分頁 ══ -->
<div id="pane-company" style="display:none" class="max-w-4xl mx-auto px-4 py-6">

  <!-- 搜尋條件列 -->
  <div class="bg-slate-800 rounded-2xl border border-slate-700 p-4 mb-4">
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
      <!-- 類別複選按鈕 -->
      <div class="relative">
        <button id="cp-cat-btn" onclick="cpToggleDropdown('cat')"
          class="flex items-center gap-1 bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 hover:border-slate-400 transition">
          <span id="cp-cat-label">全部類別</span>
          <svg class="w-3 h-3 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
        </button>
        <div id="cp-cat-panel" class="hidden absolute left-0 top-full mt-1 z-50 bg-slate-800 border border-slate-600 rounded-xl shadow-2xl p-3 min-w-[180px] max-h-72 overflow-y-auto">
          <div id="cp-cat-list" class="space-y-1"></div>
        </div>
      </div>
      <!-- 地區複選按鈕 -->
      <div class="relative">
        <button id="cp-area-btn" onclick="cpToggleDropdown('area')"
          class="flex items-center gap-1 bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 hover:border-slate-400 transition">
          <span id="cp-area-label">全部地區</span>
          <svg class="w-3 h-3 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
        </button>
        <div id="cp-area-panel" class="hidden absolute left-0 top-full mt-1 z-50 bg-slate-800 border border-slate-600 rounded-xl shadow-2xl p-3 min-w-[200px] max-h-72 overflow-y-auto">
          <div id="cp-area-list" class="space-y-1"></div>
        </div>
      </div>
      <!-- 經紀人複選按鈕 -->
      <div class="relative">
        <button id="cp-agent-btn" onclick="cpToggleDropdown('agent')"
          class="flex items-center gap-1 bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 hover:border-slate-400 transition">
          <span id="cp-agent-label">全部經紀人</span>
          <svg class="w-3 h-3 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"/></svg>
        </button>
        <div id="cp-agent-panel" class="hidden absolute left-0 top-full mt-1 z-50 bg-slate-800 border border-slate-600 rounded-xl shadow-2xl p-3 min-w-[180px] max-h-72 overflow-y-auto">
          <p class="text-xs text-slate-500 mb-2">── 在線人員 ──</p>
          <div id="cp-agent-active-list" class="space-y-1 mb-2"></div>
          <p class="text-xs text-slate-500 mb-2">── 其他 ──</p>
          <div id="cp-agent-inactive-list" class="space-y-1"></div>
        </div>
      </div>
    </div>
    <div class="flex gap-2">
      <button onclick="cpSearch()"
        class="px-5 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition">搜尋</button>
      <button onclick="cpReset()"
        class="px-4 py-2 rounded-lg bg-slate-700 hover:bg-slate-600 text-slate-300 text-sm transition">重設</button>
    </div>
  </div>

  <!-- 管理員工具列（只有管理員看得到） -->
  <div id="cp-sync-bar" class="hidden mb-3 flex flex-wrap items-center gap-3 bg-slate-800/60 border border-slate-700 rounded-xl px-4 py-2">
    <span class="text-xs text-slate-400 flex-1">上次同步：<span id="cp-last-sync" class="text-slate-300">讀取中…</span></span>
    <button id="cp-sync-btn" onclick="cpTriggerSync()"
      class="px-4 py-1.5 rounded-lg bg-amber-600 hover:bg-amber-500 text-white text-xs font-semibold transition">
      🔄 同步 Sheets
    </button>
    <!-- Word 物件總表上傳 -->
    <label class="flex items-center gap-1 px-4 py-1.5 rounded-lg bg-purple-700 hover:bg-purple-600 text-white text-xs font-semibold transition cursor-pointer"
      title="上傳最新 Word 物件總表，自動解析售價並顯示在卡片上">
      📄 上傳物件總表
      <input type="file" accept=".doc,.docx" class="hidden" onchange="cpUploadWordSnapshot(this)">
    </label>
    <span id="cp-word-status" class="text-xs text-slate-400"></span>
  </div>

  <!-- 結果資訊列 -->
  <div id="cp-info" class="text-sm text-slate-400 mb-3 hidden">
    共 <span id="cp-total" class="font-bold text-slate-200">0</span> 筆，第
    <span id="cp-page-num" class="font-bold text-slate-200">1</span> /
    <span id="cp-total-pages" class="font-bold text-slate-200">1</span> 頁
  </div>

  <!-- 結果列表 -->
  <div id="cp-list" class="space-y-2"></div>

  <!-- 分頁控制 -->
  <div id="cp-pagination" class="flex gap-2 justify-center mt-4 hidden">
    <button id="cp-prev" onclick="cpChangePage(-1)"
      class="px-4 py-2 rounded-lg bg-slate-700 hover:bg-slate-600 text-sm text-slate-300 transition disabled:opacity-40">← 上一頁</button>
    <button id="cp-next" onclick="cpChangePage(1)"
      class="px-4 py-2 rounded-lg bg-slate-700 hover:bg-slate-600 text-sm text-slate-300 transition disabled:opacity-40">下一頁 →</button>
  </div>

  <!-- 初始提示 -->
  <div id="cp-placeholder" class="text-center py-16 text-slate-500">
    <div class="text-5xl mb-3">🏢</div>
    <p class="text-lg font-medium text-slate-400">公司物件庫</p>
    <p class="text-sm mt-1">輸入條件後按「搜尋」，或直接按搜尋顯示全部物件</p>
  </div>
</div>

<!-- ══ 買方需求分頁 ══ -->
<div id="pane-buyers" style="display:none" class="max-w-3xl mx-auto px-4 py-6">
  <div class="flex items-center justify-between mb-4">
    <h2 class="font-bold text-slate-100 text-lg">👥 買方需求管理</h2>
    <button onclick="buyerOpenNew()"
      class="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition">
      ＋ 新增買方
    </button>
  </div>
  <div id="buyer-list" class="space-y-3">
    <p class="text-slate-500 text-center py-10">載入中…</p>
  </div>
</div>

<!-- 新增/編輯買方 Modal -->
<div id="buyer-modal" role="dialog" aria-modal="true"
  class="hidden fixed inset-0 z-[200] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm"
  onclick="if(event.target===this)buyerCloseModal()">
  <div class="w-full max-w-lg rounded-2xl bg-slate-800 border border-slate-600 shadow-2xl flex flex-col max-h-[90vh]"
    onclick="event.stopPropagation()">
    <div class="flex items-center justify-between px-6 py-4 border-b border-slate-700 shrink-0">
      <h3 id="buyer-modal-title" class="font-bold text-slate-100">新增買方</h3>
      <button onclick="buyerCloseModal()" class="text-slate-400 hover:text-slate-200 text-xl leading-none">✕</button>
    </div>
    <div class="overflow-y-auto px-6 py-5 space-y-4">
      <input type="hidden" id="buyer-id">

      <!-- 基本資料 -->
      <div class="grid grid-cols-2 gap-3">
        <div class="col-span-2">
          <label class="block text-xs text-slate-400 mb-1">買方姓名 <span class="text-red-400">*</span></label>
          <input id="buyer-name" type="text" placeholder="例：陳小明"
            class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
        </div>
        <div>
          <label class="block text-xs text-slate-400 mb-1">聯絡電話</label>
          <input id="buyer-phone" type="text" placeholder="0912-345678"
            class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
        </div>
        <div>
          <label class="block text-xs text-slate-400 mb-1">負責經紀人</label>
          <input id="buyer-agent" type="text" placeholder="你的名字"
            class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
        </div>
      </div>

      <!-- 需求條件 -->
      <div class="bg-slate-700/50 rounded-xl p-4 border border-slate-600 space-y-3">
        <p class="text-xs font-semibold text-slate-300">📋 購屋需求條件</p>
        <div>
          <label class="block text-xs text-slate-400 mb-1">希望地區（可複選，逗號分隔）</label>
          <input id="buyer-areas" type="text" placeholder="例：台東, 鹿野"
            class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
        </div>
        <div>
          <label class="block text-xs text-slate-400 mb-1">物件類別（可複選，逗號分隔）</label>
          <input id="buyer-categories" type="text" placeholder="例：透天, 公寓, 別墅"
            class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="block text-xs text-slate-400 mb-1">最低預算（萬）</label>
            <input id="buyer-price-min" type="number" placeholder="0"
              class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
          </div>
          <div>
            <label class="block text-xs text-slate-400 mb-1">最高預算（萬）</label>
            <input id="buyer-price-max" type="number" placeholder="9999"
              class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
          </div>
          <div>
            <label class="block text-xs text-slate-400 mb-1">最小坪數</label>
            <input id="buyer-ping-min" type="number" placeholder="0"
              class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
          </div>
          <div>
            <label class="block text-xs text-slate-400 mb-1">最大坪數</label>
            <input id="buyer-ping-max" type="number" placeholder="999"
              class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-blue-500">
          </div>
        </div>
        <div>
          <label class="block text-xs text-slate-400 mb-1">物件狀態</label>
          <select id="buyer-status"
            class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none">
            <option value="selling">僅銷售中</option>
            <option value="all">銷售中 + 已成交</option>
            <option value="sold">僅已成交</option>
          </select>
        </div>
      </div>

      <!-- 備註 -->
      <div>
        <label class="block text-xs text-slate-400 mb-1">其他備註需求</label>
        <textarea id="buyer-notes" rows="3" placeholder="例：需要車位，喜歡安靜巷弄，有小孩需近學校…"
          class="w-full bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-100 resize-none focus:outline-none focus:border-blue-500"></textarea>
      </div>
    </div>
    <div class="px-6 py-4 border-t border-slate-700 flex gap-3 shrink-0">
      <button onclick="buyerSave()"
        class="px-5 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition">儲存</button>
      <button onclick="buyerCloseModal()"
        class="px-4 py-2 rounded-lg bg-slate-700 hover:bg-slate-600 text-slate-300 text-sm transition">取消</button>
    </div>
  </div>
</div>

<!-- 配對結果 Modal -->
<div id="match-modal" role="dialog" aria-modal="true"
  class="hidden fixed inset-0 z-[200] flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm"
  onclick="if(event.target===this)matchClose()">
  <div class="w-full max-w-2xl rounded-2xl bg-slate-800 border border-slate-600 shadow-2xl flex flex-col max-h-[90vh]"
    onclick="event.stopPropagation()">
    <div class="flex items-center justify-between px-6 py-4 border-b border-slate-700 shrink-0">
      <div>
        <h3 id="match-title" class="font-bold text-slate-100">配對結果</h3>
        <p id="match-subtitle" class="text-xs text-slate-400 mt-0.5"></p>
      </div>
      <button onclick="matchClose()" class="text-slate-400 hover:text-slate-200 text-xl leading-none">✕</button>
    </div>
    <div id="match-body" class="overflow-y-auto px-6 py-4 space-y-3"></div>
    <div class="px-6 py-4 border-t border-slate-700 shrink-0">
      <button onclick="matchClose()" class="px-4 py-2 rounded-lg bg-slate-700 hover:bg-slate-600 text-slate-300 text-sm transition">關閉</button>
    </div>
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
  const isAdmin = __IS_ADMIN_JSON__;
  var _libImageFile = null;

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

  loadUsers(); loadList();

  // ══ 分頁切換 ══
  function switchTab(tab) {
    var paneMyEl      = document.getElementById('pane-my');
    var paneCompanyEl = document.getElementById('pane-company');
    var paneBuyersEl  = document.getElementById('pane-buyers');
    var btnNewObj     = document.getElementById('btn-new-obj');

    paneMyEl.style.display      = 'none';
    paneCompanyEl.style.display = 'none';
    paneBuyersEl.style.display  = 'none';
    if (btnNewObj) btnNewObj.style.display = 'none';

    if (tab === 'my') {
      paneMyEl.style.display = 'block';
      if (btnNewObj) btnNewObj.style.display = '';
    } else if (tab === 'company') {
      paneCompanyEl.style.display = 'block';
    } else if (tab === 'buyers') {
      paneBuyersEl.style.display = 'block';
    }

    // 分頁按鈕樣式
    document.querySelectorAll('.tab-btn').forEach(function(btn) {
      btn.style.color        = '#94a3b8';   // slate-400
      btn.style.borderBottom = '2px solid transparent';
      btn.style.fontWeight   = '400';
    });
    var activeBtn = document.getElementById('tab-' + tab);
    if (activeBtn) {
      activeBtn.style.color        = '#60a5fa'; // blue-400
      activeBtn.style.borderBottom = '2px solid #3b82f6'; // blue-500
      activeBtn.style.fontWeight   = '600';
    }

    // 切換到公司物件時：載入篩選選項 + 自動以登入者×銷售中搜尋 + 顯示管理員工具列
    if (tab === 'company') {
      if (!window._cpOptionsLoaded) { cpLoadOptions(); }
      if (!window._cpSearched) { window._cpSearched = true; cpLoadMe(); }
      if (!window._cpWordLoaded) { window._cpWordLoaded = true; cpLoadWordSnapshot(); }
      if (isAdmin) {
        document.getElementById('cp-sync-bar').style.display = 'flex';
        cpLoadSyncStatus();
        cpLoadWordSnapshotStatus();
      }
    }
    // 切換到買方需求時：載入列表
    if (tab === 'buyers') {
      buyerLoadList();
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
    document.getElementById('cp-list').innerHTML = '';
    document.getElementById('cp-info').classList.add('hidden');
    document.getElementById('cp-pagination').classList.add('hidden');
    document.getElementById('cp-placeholder').classList.remove('hidden');
  }

  function cpChangePage(dir) {
    _cpPage = Math.max(1, _cpPage + dir);
    cpFetch();
    window.scrollTo(0, 0);
  }

  // ══ Word Snapshot 售價對比 ══
  var _cpWordPrices = {};   // {normalized案名: {案名, 委託號碼, 售價萬}}

  // 正規化案名（和後端一致）
  function _normName(s) {
    return String(s || '').replace(/\s+/g, '').replace(/(?<!\d)\d{5,6}(?!\d)/g, '').trim();
  }

  // 載入目前 snapshot 的售價字典
  function cpLoadWordSnapshot() {
    fetch('/api/word-snapshot/prices').then(r => r.json()).then(function(data) {
      _cpWordPrices = data || {};
    }).catch(function() { _cpWordPrices = {}; });
  }

  // 顯示 Word snapshot 狀態（管理員）
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
  }

  // 上傳 Word 物件總表
  function cpUploadWordSnapshot(input) {
    if (!input.files || !input.files[0]) return;
    var file = input.files[0];
    var el = document.getElementById('cp-word-status');
    if (el) el.textContent = '解析中…';
    var fd = new FormData();
    fd.append('file', file);
    fetch('/api/word-snapshot/upload', { method:'POST', body:fd })
      .then(r => r.json()).then(function(data) {
        if (data.error) { toast(data.error, 'error'); if(el) el.textContent = '上傳失敗'; return; }
        toast(data.message || '物件總表更新完成', 'success');
        if (el) el.textContent = '剛剛上傳（' + (data.count||0) + '筆）';
        // 重新載入售價字典並刷新列表
        cpLoadWordSnapshot();
        setTimeout(function(){ cpFetch(); }, 800);
        // 清除 input 讓下次可重新選同一檔
        input.value = '';
      }).catch(function(e) {
        toast('上傳失敗：' + e, 'error');
        if (el) el.textContent = '上傳失敗';
      });
  }

  function cpFetch() {
    var list = document.getElementById('cp-list');
    list.innerHTML = '<p class="text-slate-400 text-center py-8">載入中…</p>';
    document.getElementById('cp-placeholder').classList.add('hidden');

    var q = Object.assign({}, _cpLastQuery, { page: _cpPage });
    var params = new URLSearchParams();
    Object.entries(q).forEach(function([k, v]) { if (v !== '') params.set(k, v); });

    fetch('/api/company-properties/search?' + params.toString())
      .then(r => r.json()).then(function(data) {
        if (data.error) { list.innerHTML = '<p class="text-red-400 text-center py-8">' + escapeHtml(data.error) + '</p>'; return; }
        var items = data.items || [];
        if (!items.length) {
          list.innerHTML = '<p class="text-slate-500 text-center py-10">找不到符合條件的物件</p>';
          document.getElementById('cp-info').classList.add('hidden');
          document.getElementById('cp-pagination').classList.add('hidden');
          return;
        }

        // 更新資訊列
        document.getElementById('cp-total').textContent = data.total;
        document.getElementById('cp-page-num').textContent = data.page;
        document.getElementById('cp-total-pages').textContent = data.pages;
        document.getElementById('cp-info').classList.remove('hidden');

        // 分頁按鈕
        var pg = document.getElementById('cp-pagination');
        pg.classList.remove('hidden');
        document.getElementById('cp-prev').disabled = data.page <= 1;
        document.getElementById('cp-next').disabled = data.page >= data.pages;

        // 計算委託到期日剩餘天數
        function calcDaysLeft(dateStr) {
          if (!dateStr) return null;
          // 支援「115年6月30日」民國格式
          var m = String(dateStr).match(/(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日/);
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
          var selling = item['銷售中'];
          var hasDeal = !!item['成交日期'];
          var statusBadge;
          if (selling === false && hasDeal) {
            statusBadge = '<span class="text-xs bg-blue-900 text-blue-300 px-2 py-0.5 rounded-full">已成交</span>';
          } else if (selling === false && !hasDeal) {
            statusBadge = '<span class="text-xs bg-slate-600 text-slate-400 px-2 py-0.5 rounded-full">已下架</span>';
          } else {
            statusBadge = '<span class="text-xs bg-green-700 text-green-200 px-2 py-0.5 rounded-full">銷售中</span>';
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
            // 有新售價且不同 → 顯示對比
            price = '<span class="line-through text-slate-500 text-xs">' + escapeHtml(String(dbPrice||'-')) + '萬</span>'
                  + ' <span class="text-yellow-300 font-bold">' + escapeHtml(String(wordHit['售價萬'])) + '萬</span>'
                  + '<span class="text-xs text-yellow-500 ml-0.5">↑Word</span>';
          } else {
            price = dbPrice ? dbPrice + ' 萬' : '-';
          }
          var buildPing = item['建坪'] ? item['建坪'] + ' 坪' : (item['地坪'] ? item['地坪'] + ' 坪地' : '');
          var cat = item['物件類別'] ? '<span class="text-xs text-amber-400">' + escapeHtml(item['物件類別']) + '</span>' : '';
          var agent = item['經紀人'] ? '<span class="text-xs text-slate-500">' + escapeHtml(item['經紀人']) + '</span>' : '';
          var safeId = String(item.id).replace(/'/g, '');
          var name = escapeHtml(item['案名'] || '（無案名）');
          var addr = escapeHtml(item['物件地址'] || '-');

          // 委託到期日剩餘天數標示
          var expiryBadge = '';
          if (selling !== false) {  // 銷售中才顯示到期警示
            var daysLeft = calcDaysLeft(item['委託到期日']);
            if (daysLeft !== null) {
              if (daysLeft < 0) {
                expiryBadge = '<span class="text-xs bg-red-900 text-red-300 px-2 py-0.5 rounded-full">⚠️ 已到期 ' + Math.abs(daysLeft) + '天</span>';
              } else if (daysLeft <= 15) {
                expiryBadge = '<span class="text-xs bg-orange-800 text-orange-200 px-2 py-0.5 rounded-full animate-pulse">⏰ 剩 ' + daysLeft + ' 天</span>';
              } else {
                expiryBadge = '<span class="text-xs text-slate-500">到期：剩' + daysLeft + '天</span>';
              }
            }
          }

          html += '<div class="bg-slate-800 border border-slate-700 hover:border-slate-500 rounded-xl p-4 cursor-pointer transition" onclick="cpOpenDetail(\'' + safeId + '\')">';
          html += '<div class="flex items-start justify-between gap-2">';
          html += '<div class="min-w-0"><p class="font-semibold text-slate-100 truncate">' + name + '</p>';
          html += '<p class="text-xs text-slate-400 truncate mt-0.5">' + addr + '</p></div>';
          // price 若含 HTML 標籤（售價對比）則直接插入，否則 escape
          var priceHtml = (price.indexOf('<') >= 0) ? price : '<span class="font-bold text-blue-300 text-sm">' + escapeHtml(price) + '</span>';
          html += '<div class="shrink-0 text-right"><p class="text-sm leading-tight">' + priceHtml + '</p>' + statusBadge + '</div>';
          html += '</div>';
          html += '<div class="flex gap-3 mt-2 flex-wrap items-center">' + cat;
          html += buildPing ? '<span class="text-xs text-slate-400">' + escapeHtml(buildPing) + '</span>' : '';
          html += agent;
          html += expiryBadge + '</div></div>';
        }
        list.innerHTML = html;
    });
  }

  function cpOpenDetail(id) {
    fetch('/api/company-properties/' + encodeURIComponent(id)).then(r => r.json()).then(function(data) {
      if (data.error) { toast(data.error, 'error'); return; }

      document.getElementById('cp-detail-title').textContent = data['案名'] || '物件詳情';

      // 欄位順序與顯示名稱
      var LABELS = {
        '委託編號':'委託編號','委託日':'委託日','案名':'案名','所有權人':'所有權人',
        '物件類別':'類別','物件地址':'地址','鄉/市/鎮':'鄉鎮市',
        '段別':'段別','地號':'地號','建號':'建號','座向':'座向',
        '竣工日期':'竣工日期','格局':'格局','現況':'現況',
        '地坪':'地坪','建坪':'建坪','樓別':'樓別','管理費(元)':'管理費',
        '車位':'車位','委託價(萬)':'委託價','售價(萬)':'售價',
        '現有貸款(萬)':'現有貸款','債權人':'債權人','售屋原因':'售屋原因',
        '委託到期日':'委託到期日','經紀人':'經紀人','契變':'契變',
        '備註':'備註','成交日期':'成交日期','成交金額(萬)':'成交金額',
        '連絡人姓名':'連絡人','連絡人與所有權人關係':'與業主關係',
        '銷售中':'狀態','GOOGLE地圖':'Google地圖','座標':'座標',
        '資料序號':'資料序號'
      };

      var html = '';
      var statusVal = data['銷售中'] === false ? '已成交' : '銷售中';
      Object.entries(LABELS).forEach(function([key, label]) {
        var val = data[key];
        if (key === '銷售中') val = statusVal;
        if (val == null || val === '' || val === true || val === false && key !== '銷售中') {
          if (key === '銷售中') {} else return;
        }
        var valStr = String(val);
        var isUrl = valStr.startsWith('http');
        html += '<div class="flex gap-2 py-1 border-b border-slate-700/50">'
          + '<span class="text-slate-500 w-24 shrink-0 text-xs mt-0.5">' + escapeHtml(label) + '</span>'
          + '<span class="text-slate-200 text-sm flex-1">'
          + (isUrl ? '<a href="' + escapeHtml(valStr) + '" target="_blank" class="text-blue-400 underline hover:text-blue-300">開啟連結</a>' : escapeHtml(valStr))
          + '</span></div>';
      });

      document.getElementById('cp-detail-body').innerHTML = html || '<p class="text-slate-500">無資料</p>';
      document.getElementById('cp-detail-modal').classList.remove('hidden');
    });
  }

  function closeCpDetail() {
    document.getElementById('cp-detail-modal').classList.add('hidden');
  }

  // ══ 買方需求 ══

  function buyerLoadList() {
    var el = document.getElementById('buyer-list');
    el.innerHTML = '<p class="text-slate-400 text-center py-8">\u8f09\u5165\u4e2d\u2026</p>';
    fetch('/api/buyers').then(function(r){ return r.json(); }).then(function(data) {
      if (data.error) { el.innerHTML = '<p class="text-red-400 text-center py-8">' + escapeHtml(data.error) + '</p>'; return; }
      var items = data.items || [];
      if (!items.length) {
        el.innerHTML = '<p class="text-slate-500 text-center py-10">\u5c1a\u7121\u8cb7\u65b9\u8cc7\u6599\uff0c\u9ede\u300e\uff0b \u65b0\u589e\u8cb7\u65b9\u300f\u958b\u59cb\u5efa\u7acb</p>';
        return;
      }
      var html = '';
      for (var i = 0; i < items.length; i++) {
        var b = items[i];
        var areas = b.areas || '';
        var cats = b.categories || '';
        var budget = (b.price_min || '') + (b.price_min && b.price_max ? ' ~ ' : '') + (b.price_max ? b.price_max + ' \u842c' : '');
        var ping = (b.ping_min || '') + (b.ping_min && b.ping_max ? ' ~ ' : '') + (b.ping_max ? b.ping_max + ' \u576a' : '');
        var safeId = String(b.id).replace(/'/g, '');
        html += '<div class="bg-slate-800 border border-slate-700 rounded-xl p-4">';
        html += '<div class="flex items-start justify-between gap-2 mb-2">';
        html += '<div><p class="font-semibold text-slate-100">' + escapeHtml(b.name || '') + '</p>';
        if (b.phone) html += '<p class="text-xs text-slate-400">' + escapeHtml(b.phone) + '</p>';
        html += '</div>';
        html += '<div class="flex gap-2 shrink-0">';
        html += '<button onclick="buyerMatch(' + "'" + safeId + "'" + ')" class="px-3 py-1.5 rounded-lg bg-green-700 hover:bg-green-600 text-white text-xs font-semibold transition">&#128269; \u914d\u5c0d\u7269\u4ef6</button>';
        html += '<button onclick="buyerOpenEdit(' + "'" + safeId + "'" + ')" class="px-3 py-1.5 rounded-lg bg-slate-600 hover:bg-slate-500 text-slate-200 text-xs transition">\u7de8\u8f2f</button>';
        html += '<button onclick="buyerDelete(' + "'" + safeId + "'" + ')" class="px-3 py-1.5 rounded-lg bg-red-800/60 hover:bg-red-700 text-red-300 text-xs transition">\u522a\u9664</button>';
        html += '</div></div>';
        html += '<div class="flex flex-wrap gap-2 text-xs">';
        if (areas) html += '<span class="bg-slate-700 text-slate-300 px-2 py-0.5 rounded-full">&#128205; ' + escapeHtml(areas) + '</span>';
        if (cats)  html += '<span class="bg-slate-700 text-slate-300 px-2 py-0.5 rounded-full">&#127968; ' + escapeHtml(cats) + '</span>';
        if (budget) html += '<span class="bg-slate-700 text-slate-300 px-2 py-0.5 rounded-full">&#128176; ' + escapeHtml(budget) + '</span>';
        if (ping)  html += '<span class="bg-slate-700 text-slate-300 px-2 py-0.5 rounded-full">&#128207; ' + escapeHtml(ping) + '</span>';
        if (b.notes) html += '<span class="bg-slate-700/50 text-slate-400 px-2 py-0.5 rounded-full">&#128221; ' + escapeHtml(b.notes.substring(0,30)) + (b.notes.length>30?'...':'') + '</span>';
        html += '</div></div>';
      }
      el.innerHTML = html;
    });
  }

  function buyerOpenNew() {
    document.getElementById('buyer-id').value = '';
    document.getElementById('buyer-modal-title').textContent = '\u65b0\u589e\u8cb7\u65b9';
    ['name','phone','agent','areas','categories','notes'].forEach(function(f){ document.getElementById('buyer-'+f).value=''; });
    ['price-min','price-max','ping-min','ping-max'].forEach(function(f){ document.getElementById('buyer-'+f).value=''; });
    document.getElementById('buyer-status').value = 'selling';
    document.getElementById('buyer-modal').classList.remove('hidden');
  }

  function buyerOpenEdit(id) {
    fetch('/api/buyers/' + encodeURIComponent(id)).then(function(r){ return r.json(); }).then(function(b) {
      if (b.error) { toast(b.error, 'error'); return; }
      document.getElementById('buyer-id').value = b.id;
      document.getElementById('buyer-modal-title').textContent = '\u7de8\u8f2f\u8cb7\u65b9';
      document.getElementById('buyer-name').value = b.name || '';
      document.getElementById('buyer-phone').value = b.phone || '';
      document.getElementById('buyer-agent').value = b.agent || '';
      document.getElementById('buyer-areas').value = b.areas || '';
      document.getElementById('buyer-categories').value = b.categories || '';
      document.getElementById('buyer-price-min').value = b.price_min || '';
      document.getElementById('buyer-price-max').value = b.price_max || '';
      document.getElementById('buyer-ping-min').value = b.ping_min || '';
      document.getElementById('buyer-ping-max').value = b.ping_max || '';
      document.getElementById('buyer-status').value = b.status || 'selling';
      document.getElementById('buyer-notes').value = b.notes || '';
      document.getElementById('buyer-modal').classList.remove('hidden');
    });
  }

  function buyerCloseModal() {
    document.getElementById('buyer-modal').classList.add('hidden');
  }

  function buyerSave() {
    var name = document.getElementById('buyer-name').value.trim();
    if (!name) { toast('\u8acb\u586b\u5beb\u8cb7\u65b9\u59d3\u540d', 'error'); return; }
    var data = {
      name: name,
      phone: document.getElementById('buyer-phone').value.trim(),
      agent: document.getElementById('buyer-agent').value.trim(),
      areas: document.getElementById('buyer-areas').value.trim(),
      categories: document.getElementById('buyer-categories').value.trim(),
      price_min: document.getElementById('buyer-price-min').value || null,
      price_max: document.getElementById('buyer-price-max').value || null,
      ping_min:  document.getElementById('buyer-ping-min').value || null,
      ping_max:  document.getElementById('buyer-ping-max').value || null,
      status: document.getElementById('buyer-status').value,
      notes: document.getElementById('buyer-notes').value.trim()
    };
    var id = document.getElementById('buyer-id').value;
    var url = id ? '/api/buyers/' + encodeURIComponent(id) : '/api/buyers';
    var method = id ? 'PUT' : 'POST';
    fetch(url, { method: method, headers: {'Content-Type':'application/json'}, body: JSON.stringify(data) })
      .then(function(r){ return r.json(); }).then(function(d) {
        if (d.error) { toast(d.error, 'error'); return; }
        toast(id ? '\u5df2\u66f4\u65b0' : '\u5df2\u65b0\u589e\u8cb7\u65b9', 'success');
        buyerCloseModal();
        buyerLoadList();
      });
  }

  function buyerDelete(id) {
    if (!confirm('\u78ba\u5b9a\u8981\u522a\u9664\u6b64\u8cb7\u65b9\u8cc7\u6599\uff1f')) return;
    fetch('/api/buyers/' + encodeURIComponent(id), { method: 'DELETE' })
      .then(function(r){ return r.json(); }).then(function(d) {
        if (d.error) { toast(d.error, 'error'); return; }
        toast('\u5df2\u522a\u9664', 'success');
        buyerLoadList();
      });
  }

  // ══ 客戶配對 ══

  function buyerMatch(id) {
    var body = document.getElementById('match-body');
    body.innerHTML = '<p class="text-slate-400 text-center py-8">\u914d\u5c0d\u4e2d\uff0c\u8acb\u7a0d\u5019\u2026</p>';
    document.getElementById('match-title').textContent = '\u914d\u5c0d\u7d50\u679c';
    document.getElementById('match-subtitle').textContent = '';
    document.getElementById('match-modal').classList.remove('hidden');

    fetch('/api/buyers/' + encodeURIComponent(id) + '/match')
      .then(function(r){ return r.json(); }).then(function(data) {
        if (data.error) { body.innerHTML = '<p class="text-red-400 text-center py-8">' + escapeHtml(data.error) + '</p>'; return; }
        document.getElementById('match-title').textContent = '\u300c' + escapeHtml(data.buyer_name) + '\u300d\u914d\u5c0d\u7d50\u679c';
        document.getElementById('match-subtitle').textContent = '\u5171\u627e\u5230 ' + data.total + ' \u7b46\u7b26\u5408\u6761\u4ef6\u7684\u7269\u4ef6';
        var items = data.items || [];
        if (!items.length) {
          body.innerHTML = '<p class="text-slate-500 text-center py-10">\u627e\u4e0d\u5230\u7b26\u5408\u689d\u4ef6\u7684\u7269\u4ef6\uff0c\u8acb\u8abf\u6574\u9700\u6c42\u689d\u4ef6\u5f8c\u518d\u8a66</p>';
          return;
        }
        var html = '';
        for (var i = 0; i < items.length; i++) {
          var item = items[i];
          var isSold = item['\u9500\u552e\u4e2d'] === false;
          var badge = isSold
            ? '<span class="text-xs bg-slate-600 text-slate-300 px-2 py-0.5 rounded-full">\u5df2\u6210\u4ea4</span>'
            : '<span class="text-xs bg-green-700 text-green-200 px-2 py-0.5 rounded-full">\u9500\u552e\u4e2d</span>';
          var price = item['\u552e\u50f9(\u842c)'] ? item['\u552e\u50f9(\u842c)'] + ' \u842c' : '-';
          var bping = item['\u5efa\u576a'] ? item['\u5efa\u576a'] + '\u576a' : (item['\u5730\u576a'] ? item['\u5730\u576a'] + '\u576a\u5730' : '');
          var safeId = String(item.id).replace(/'/g, '');
          html += '<div class="bg-slate-700/60 border border-slate-600 hover:border-slate-400 rounded-xl p-4 cursor-pointer transition" onclick="cpOpenDetail(' + "'" + safeId + "'" + ')">';
          html += '<div class="flex items-start justify-between gap-2">';
          html += '<div class="min-w-0"><p class="font-semibold text-slate-100 truncate">' + escapeHtml(item['\u6848\u540d'] || '\uff08\u7121\u6848\u540d\uff09') + '</p>';
          html += '<p class="text-xs text-slate-400 truncate mt-0.5">' + escapeHtml(item['\u7269\u4ef6\u5730\u5740'] || '-') + '</p></div>';
          html += '<div class="shrink-0 text-right"><p class="font-bold text-blue-300 text-sm">' + escapeHtml(price) + '</p>' + badge + '</div>';
          html += '</div>';
          html += '<div class="flex gap-2 mt-2 flex-wrap text-xs">';
          if (item['\u7269\u4ef6\u985e\u5225']) html += '<span class="text-amber-400">' + escapeHtml(item['\u7269\u4ef6\u985e\u5225']) + '</span>';
          if (bping) html += '<span class="text-slate-400">' + escapeHtml(bping) + '</span>';
          if (item['\u9109/\u5e02/\u93ae']) html += '<span class="text-slate-400">' + escapeHtml(item['\u9109/\u5e02/\u93ae']) + '</span>';
          html += '</div></div>';
        }
        body.innerHTML = html;
      });
  }

  function matchClose() {
    document.getElementById('match-modal').classList.add('hidden');
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
</script>
</body>
</html>
"""


if __name__ == "__main__":
    os.makedirs(USERS_DIR, exist_ok=True)
    port = int(os.environ.get("PORT", "5004"))
    app.run(debug=bool(os.environ.get("FLASK_DEBUG")), host="0.0.0.0", port=port)
