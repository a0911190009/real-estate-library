# -*- coding: utf-8 -*-
"""
房仲工具 — 物件庫（real-estate-library）
物件新增/刪除/編輯，並整合 Survey 環境總結與 AD 產出。每用戶獨立，管理員可查看各用戶。
"""

import os
import json
import re
import base64
import urllib.request
import urllib.error
from datetime import datetime

from flask import Flask, request, session, redirect, jsonify, render_template_string
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature

# Gemini 圖片辨識（直接呼叫，不經由 Portal 代理）
# 優先用新版 google.genai，fallback 到舊版 google.generativeai
_GEMINI_KEY = (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or "").strip()
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
    safe = _safe_email(email)
    if not safe:
        return None, None
    if GCS_BUCKET:
        return None, f"users/{safe}/{OBJECTS_GCS_PREFIX}"
    d = os.path.join(USERS_DIR, safe, "objects")
    os.makedirs(d, exist_ok=True)
    return d, None


def _list_user_ids(email):
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
    safe = _safe_email(email)
    if not safe or not obj_id:
        return None
    obj_id = os.path.basename(obj_id).replace("..", "")
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
    safe = _safe_email(email)
    if not safe or not obj_id:
        return False
    obj_id = os.path.basename(obj_id).replace("..", "")
    data["updated_at"] = datetime.now().isoformat()
    data["owner_email"] = email
    if "id" not in data:
        data["id"] = obj_id
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
    safe = _safe_email(email)
    if not safe or not obj_id:
        return False
    obj_id = os.path.basename(obj_id).replace("..", "")
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
    '"age":"5年","parking":"有","case_number":"A123456","location_area":"台東縣"}\n'
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
            '<div class="flex items-center gap-3 px-5 py-2 bg-slate-800 border-b border-slate-700 text-sm text-slate-400">'
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
  </style>
</head>
<body class="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 text-slate-100 font-sans antialiased">

<div id="toast-container"></div>

<!-- 頂部導覽 -->
<header class="sticky top-0 z-50 flex items-center justify-between px-5 py-3 bg-slate-900/95 backdrop-blur border-b border-slate-700 shadow">
  <span class="font-bold text-slate-100">📁 物件庫</span>
  <div class="flex gap-2">
    <a href="__PORTAL_LINK__" class="px-3 py-2 rounded-lg bg-slate-700 hover:bg-slate-600 text-slate-300 text-sm transition">🏠 返回入口</a>
    <button type="button" onclick="openNewModal()"
      class="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold transition shadow">
      ＋ 建立物件資訊
    </button>
  </div>
</header>

__ADMIN_BAR__

<div class="max-w-3xl mx-auto px-4 py-6">
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
        <div class="mt-3 pt-3 border-t border-slate-600 opacity-50">
          <p class="text-xs text-slate-400 mb-2">或輸入物件網址（截圖辨識，功能即將推出）</p>
          <div class="flex gap-2 items-center">
            <input type="url" id="lib-url-input" placeholder="https:// 房仲或售屋網頁" disabled
              class="flex-1 min-w-0 bg-slate-700 border border-slate-600 rounded-lg px-3 py-2 text-sm text-slate-500 cursor-not-allowed" />
            <button type="button" id="lib-url-btn" disabled
              class="px-3 py-2 rounded-lg bg-slate-600 text-slate-500 text-sm font-medium whitespace-nowrap cursor-not-allowed">截圖辨識（即將開放）</button>
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
    document.getElementById('n-name').value  = ext.project_name || '';
    document.getElementById('n-price').value = (ext.price != null) ? String(ext.price) : '';
    document.getElementById('n-area').value  = ext.location_area || '';
    document.getElementById('n-addr').value  = ext.address || '';
    document.getElementById('n-bping').value = (ext.building_ping != null) ? String(ext.building_ping) : '';
    document.getElementById('n-lping').value = (ext.land_ping != null) ? String(ext.land_ping) : '';
    document.getElementById('n-aping').value = (ext.authority_ping != null) ? String(ext.authority_ping) : '';
    document.getElementById('n-layout').value  = ext.layout || '';
    document.getElementById('n-floor').value   = ext.floor || '';
    document.getElementById('n-age').value     = ext.age || '';
    document.getElementById('n-parking').value = ext.parking || '';
    document.getElementById('n-case').value    = ext.case_number || '';
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
    statusEl.textContent = '截圖與辨識中…（約 10–30 秒）';
    statusEl.className = 'text-xs text-slate-400 mt-2 min-h-[1em]';
    btn.disabled = true;
    try {
      var r = await fetch('/api/proxy/extract-from-url', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ url: url }),
      });
      var d = await r.json();
      if (d.ok && d.extracted) {
        fillLibForm(d.extracted);
        statusEl.textContent = '已帶入欄位，剩餘 ' + (d.points ?? '') + ' 點';
        statusEl.className = 'text-xs text-emerald-400 mt-2 min-h-[1em]';
      } else {
        statusEl.textContent = d.error || '截圖或辨識失敗';
        statusEl.className = 'text-xs text-rose-400 mt-2 min-h-[1em]';
      }
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
</script>
</body>
</html>
"""


if __name__ == "__main__":
    os.makedirs(USERS_DIR, exist_ok=True)
    port = int(os.environ.get("PORT", "5004"))
    app.run(debug=bool(os.environ.get("FLASK_DEBUG")), host="0.0.0.0", port=port)
