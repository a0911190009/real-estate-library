# -*- coding: utf-8 -*-
"""
房仲工具 — 物件庫（real-estate-library）
物件新增/刪除/編輯，並整合 Survey 環境總結與 AD 產出。每用戶獨立，管理員可查看各用戶。
"""

import os
import json
import re
from datetime import datetime

from flask import Flask, request, session, redirect, jsonify, render_template_string
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature

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
    return render_template_string(OBJECTS_APP_HTML, name=name, portal_link=portal_link, is_admin=is_admin, fields=fields)


OBJECTS_APP_HTML = """
<!DOCTYPE html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>物件庫 - 房仲 AI 工具平台</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: -apple-system, sans-serif; margin: 0; background: #1e293b; color: #e2e8f0; min-height: 100vh; }
    .top { display: flex; align-items: center; justify-content: space-between; padding: 12px 20px; background: #0f172a; border-bottom: 1px solid #334155; }
    .top h1 { font-size: 1.1rem; margin: 0; }
    .top a, .top button { padding: 8px 14px; border-radius: 8px; text-decoration: none; background: #3b82f6; color: #fff; border: none; cursor: pointer; font-size: 14px; margin-left: 8px; }
    .top a:hover, .top button:hover { background: #2563eb; }
    .admin-bar { padding: 8px 20px; background: #334155; font-size: 13px; display: flex; align-items: center; gap: 10px; }
    .admin-bar select { padding: 6px 10px; border-radius: 6px; background: #1e293b; color: #e2e8f0; border: 1px solid #475569; }
    .container { max-width: 900px; margin: 0 auto; padding: 20px; }
    .list { display: flex; flex-direction: column; gap: 10px; }
    .card { background: #334155; border-radius: 12px; padding: 14px 18px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; }
    .card h3 { margin: 0; font-size: 1rem; }
    .card .meta { font-size: 12px; color: #94a3b8; }
    .card .acts { display: flex; gap: 8px; }
    .card .acts button { padding: 6px 12px; border-radius: 6px; border: none; cursor: pointer; font-size: 13px; }
    .btn-pri { background: #3b82f6; color: #fff; }
    .btn-sec { background: #475569; color: #e2e8f0; }
    .btn-danger { background: #dc2626; color: #fff; }
    .form-panel { background: #334155; border-radius: 12px; padding: 20px; margin-bottom: 20px; }
    .form-panel h2 { margin: 0 0 16px; font-size: 1.1rem; }
    .form-panel label { display: block; font-size: 12px; color: #94a3b8; margin-top: 10px; margin-bottom: 4px; }
    .form-panel input, .form-panel textarea { width: 100%; padding: 8px 12px; border-radius: 6px; border: 1px solid #475569; background: #1e293b; color: #e2e8f0; font-size: 14px; }
    .form-panel textarea { min-height: 80px; resize: vertical; }
    .form-panel .row { display: flex; gap: 12px; margin-top: 12px; }
    .detail-section { margin-top: 16px; padding-top: 16px; border-top: 1px solid #475569; }
    .detail-section h4 { margin: 0 0 8px; font-size: 13px; color: #94a3b8; }
    .hidden { display: none !important; }
  </style>
</head>
<body>
  <div class="top">
    <h1>📁 物件庫</h1>
    <div>
      <a href="{{ portal_link }}">🏠 返回入口</a>
      <button type="button" onclick="showForm()">＋ 新增物件</button>
    </div>
  </div>
  {% if is_admin %}
  <div class="admin-bar">
    <span>查看用戶：</span>
    <select id="userSelect">
      <option value="">載入中…</option>
    </select>
  </div>
  {% endif %}
  <div class="container">
    <div id="listPanel" class="list"></div>
    <div id="formPanel" class="form-panel hidden">
      <h2 id="formTitle">新增物件</h2>
      <form id="objForm">
        <input type="hidden" id="objId" name="id">
        {% for key, label in fields %}
        <label for="f_{{ key }}">{{ label }}</label>
        {% if key == 'env_description' or key == 'survey_summary' %}
        <textarea id="f_{{ key }}" name="{{ key }}" placeholder="{{ label }}" rows="4"></textarea>
        {% else %}
        <input type="text" id="f_{{ key }}" name="{{ key }}" placeholder="{{ label }}">
        {% endif %}
        {% endfor %}
        <div class="row">
          <button type="submit" class="btn-pri">儲存</button>
          <button type="button" class="btn-sec" onclick="hideForm()">取消</button>
        </div>
      </form>
    </div>
    <div id="detailPanel" class="form-panel hidden">
      <h2 id="detailTitle">物件詳情</h2>
      <div id="detailContent"></div>
      <div class="row" style="margin-top:16px;">
        <button type="button" class="btn-pri" onclick="editCurrentDetail()">編輯</button>
        <button type="button" class="btn-sec" onclick="closeDetail()">關閉</button>
      </div>
    </div>
  </div>
  <script>
    const fields = {{ fields | tojson }};
    const isAdmin = {{ is_admin | tojson }};
    function targetUser() { return isAdmin && document.getElementById('userSelect') ? document.getElementById('userSelect').value : ''; }
    function apiUrl(path) { const u = targetUser() ? '?user=' + encodeURIComponent(targetUser()) : ''; return path + u; }
    function loadList() {
      fetch(apiUrl('/api/objects')).then(r => {
        return r.json().then(data => {
          if (!r.ok) { alert(data.error || '載入失敗'); return; }
          if (data.error) { alert(data.error); return; }
          const list = document.getElementById('listPanel');
          list.innerHTML = (data.items || []).map(o => {
            const title = o.custom_title || o.project_name || o.address || o.id || '未命名';
            const meta = [o.address, o.updated_at ? o.updated_at.slice(0,10) : ''].filter(Boolean).join(' · ');
            return '<div class="card"><div><h3>' + escapeHtml(title) + '</h3><div class="meta">' + escapeHtml(meta) + '</div></div><div class="acts"><button type="button" class="btn-pri" onclick="viewDetail(\\'' + o.id.replace(/\\\\/g,'\\\\').replace(/'/g,"\\\\'") + '\\')">查看</button><button type="button" class="btn-sec" onclick="editObj(\\'' + o.id.replace(/\\\\/g,'\\\\').replace(/'/g,"\\\\'") + '\\')">編輯</button><button type="button" class="btn-danger" onclick="delObj(\\'' + o.id.replace(/\\\\/g,'\\\\').replace(/'/g,"\\\\'") + '\\')">刪除</button></div></div>';
          }).join('') || '<p class="meta">尚無物件，點「新增物件」建立。</p>';
        });
      }).catch(e => { alert('載入失敗'); });
    }
    function loadUsers() {
      if (!isAdmin) return;
      fetch('/api/users').then(r => r.json()).then(data => {
        if (data.error) return;
        const sel = document.getElementById('userSelect');
        if (!sel) return;
        sel.innerHTML = '<option value="">（自己）</option>' + (data.users || []).map(u => '<option value="' + escapeHtml(u) + '">' + escapeHtml(u) + '</option>').join('');
        sel.onchange = loadList;
      });
    }
    function escapeHtml(s) { if (s == null) return ''; var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
    function showForm(id) {
      document.getElementById('listPanel').classList.add('hidden');
      document.getElementById('formPanel').classList.remove('hidden');
      document.getElementById('detailPanel').classList.add('hidden');
      document.getElementById('formTitle').textContent = id ? '編輯物件' : '新增物件';
      document.getElementById('objForm').reset();
      document.getElementById('objId').value = id || '';
      if (id) loadObjIntoForm(id);
    }
    function hideForm() { document.getElementById('formPanel').classList.add('hidden'); document.getElementById('listPanel').classList.remove('hidden'); loadList(); }
    function loadObjIntoForm(id) {
      fetch(apiUrl('/api/objects/' + encodeURIComponent(id))).then(r => r.json()).then(o => {
        if (o.error) { alert(o.error); return; }
        fields.forEach(function(kv){ var k=kv[0]; var el=document.getElementById('f_'+k); if(el) el.value = o[k]!=null ? o[k] : ''; });
        document.getElementById('objId').value = o.id || id;
      });
    }
    document.getElementById('objForm').onsubmit = function(e) {
      e.preventDefault();
      var id = document.getElementById('objId').value;
      var payload = {};
      fields.forEach(function(kv){ var k=kv[0]; var el=document.getElementById('f_'+k); if(el) payload[k]=el.value; });
      var url = id ? apiUrl('/api/objects/' + encodeURIComponent(id)) : apiUrl('/api/objects');
      var method = id ? 'PUT' : 'POST';
      if (id) payload.id = id;
      fetch(url, { method: method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) }).then(function(r){ return r.json(); }).then(function(data){ if (data.error) alert(data.error); else hideForm(); }).catch(function(){ alert('儲存失敗'); });
    };
    function editObj(id) { showForm(id); }
    function viewDetail(id) {
      fetch(apiUrl('/api/objects/' + encodeURIComponent(id))).then(r => r.json()).then(o => {
        if (o.error) { alert(o.error); return; }
        window._detailId = id;
        document.getElementById('listPanel').classList.add('hidden');
        document.getElementById('formPanel').classList.add('hidden');
        document.getElementById('detailPanel').classList.remove('hidden');
        document.getElementById('detailTitle').textContent = o.custom_title || o.project_name || o.address || o.id || '物件詳情';
        var html = '';
        fields.forEach(function(kv){ var k=kv[0], l=kv[1], v=o[k]; if (v==null||v==='') return; html += '<p><strong>'+escapeHtml(l)+'</strong>: '+escapeHtml(String(v))+'</p>'; });
        if (o.ad_outputs && o.ad_outputs.length) { html += '<div class="detail-section"><h4>廣告產出</h4>'; o.ad_outputs.forEach(function(ad){ html += '<p><strong>'+escapeHtml(ad.type||'')+'</strong> '+escapeHtml(ad.title||'')+'<br><pre style="white-space:pre-wrap;font-size:12px;">'+escapeHtml(ad.content||'')+'</pre></p>'; }); html += '</div>'; }
        document.getElementById('detailContent').innerHTML = html || '<p class="meta">無內容</p>';
      });
    }
    function editCurrentDetail() { if (window._detailId) showForm(window._detailId); }
    function closeDetail() { document.getElementById('detailPanel').classList.add('hidden'); document.getElementById('listPanel').classList.remove('hidden'); loadList(); }
    function delObj(id) { if (!confirm('確定刪除此物件？')) return; fetch(apiUrl('/api/objects/' + encodeURIComponent(id)), { method: 'DELETE' }).then(r => r.json()).then(function(data){ if (data.error) alert(data.error); else loadList(); }); }
    loadUsers(); loadList();
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    os.makedirs(USERS_DIR, exist_ok=True)
    port = int(os.environ.get("PORT", "5004"))
    app.run(debug=bool(os.environ.get("FLASK_DEBUG")), host="0.0.0.0", port=port)
