# -*- coding: utf-8 -*-
"""
跨工具回饋系統 — 共用端點 Blueprint
複製到各工具專案根目錄後：

    from feedback_endpoint import bp as feedback_bp
    app.register_blueprint(feedback_bp)

寫入 Firestore improvement_logs collection（與 portal 共用）。
截圖傳到 GCS feedback/{date}/{uuid}.{ext}。
"""

import os
import hashlib
import uuid
from datetime import datetime, timezone

from flask import Blueprint, request, session, jsonify

bp = Blueprint("feedback_endpoint", __name__)


def _get_db():
    """延遲初始化 Firestore client（每個 process 共用一份）。"""
    if not hasattr(_get_db, "_db"):
        try:
            from google.cloud import firestore as _firestore
            _get_db._db = _firestore.Client(
                project=os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT")
            )
            _get_db._firestore = _firestore
        except Exception:
            _get_db._db = None
            _get_db._firestore = None
    return _get_db._db, _get_db._firestore


@bp.route("/api/feedback", methods=["POST"])
def submit_feedback():
    email = session.get("user_email")
    if not email:
        return jsonify({"error": "請先登入"}), 401
    db, fs = _get_db()
    if db is None:
        return jsonify({"error": "Firestore 未初始化"}), 503

    tool = (request.form.get("tool") or "other").strip()
    type_ = (request.form.get("type") or "other").strip()
    title = (request.form.get("title") or "").strip()
    content = (request.form.get("content") or "").strip()
    if not title and not content:
        return jsonify({"error": "title 與 content 至少填一個"}), 400
    page_url = (request.form.get("page_url") or "").strip()

    # 截圖傳 GCS
    screenshots = []
    files = request.files.getlist("screenshots") or []
    bucket_name = os.environ.get("GCS_BUCKET", "")
    if files and bucket_name:
        try:
            from google.cloud import storage as _gstorage
            sclient = _gstorage.Client()
            sbucket = sclient.bucket(bucket_name)
            date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
            for f in files[:3]:
                if not f or not f.filename:
                    continue
                ext = ""
                if "." in f.filename:
                    ext = f.filename.rsplit(".", 1)[1].lower()
                fid = uuid.uuid4().hex[:12]
                gcs_path = f"feedback/{date_str}/{fid}.{ext}" if ext else f"feedback/{date_str}/{fid}"
                blob = sbucket.blob(gcs_path)
                content_type = f.mimetype or "application/octet-stream"
                blob.upload_from_string(f.read(), content_type=content_type)
                screenshots.append({
                    "gcs_path": gcs_path,
                    "filename": f.filename,
                    "mime_type": content_type,
                })
        except Exception as e:
            import logging
            logging.warning("Feedback screenshot upload failed: %s", e)

    sim_base = f"{tool}|{type_}|{title[:30]}|{content[:60]}"
    sim_hash = hashlib.md5(sim_base.encode("utf-8")).hexdigest()[:16]

    payload = {
        "tool": tool,
        "type": type_,
        "source": "user_reported",
        "title": title,
        "content": content,
        "context": {
            "page_url": page_url,
            "user_agent": request.headers.get("User-Agent", "")[:200],
        },
        "screenshots": screenshots,
        "count": 1,
        "similarity_hash": sim_hash,
        "status": "open",
        "priority": 5,
        "created_at": fs.SERVER_TIMESTAMP,
        "updated_at": fs.SERVER_TIMESTAMP,
        "resolved_at": None,
        "created_by": email,
    }
    try:
        ref = db.collection("improvement_logs").document()
        ref.set(payload)
        return jsonify({"ok": True, "id": ref.id}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500
