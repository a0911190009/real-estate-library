"""
SELLER → PEOPLE 整合層

把 PEOPLE 集合 + roles/seller 子文件包裝成 LIBRARY 準賣方介面期待的格式。
所有「準賣方」資料來自 people 集合（active_roles 含 'seller'）。
seller_prospects/ 與 seller_contacts/ 保留為備份，不再讀寫。

回傳的 seller dict 格式（與舊 seller_prospects 集合相容）：
    {
        id, name, phone,
        address, land_number, category,
        owner_price, suggest_price, source, status, note,
        avatar_url, files: [{file_id, name, url, gcs_path, uploaded_at}],
        card_color, sort_order,
        created_by, created_at, updated_at, last_contact_at,
    }
"""
from __future__ import annotations
import logging

VALID_CATEGORIES = {
    "透天", "別墅", "農舍", "公寓", "華廈", "套房",
    "建地", "農地", "店面", "店住", "房屋", "其他",
}
VALID_SELLER_STATUSES = {"培養中", "已報價", "已簽委託", "已成交", "放棄"}
SELLER_ROLE_DOC_ID = "seller"
PEOPLE_BUCKET_DEFAULT = "normal"


def _ts_to_str(v):
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        try:
            return v.isoformat()
        except Exception:
            pass
    return v


def _files_list(files_docs) -> list[dict]:
    items = []
    for d in files_docs:
        data = d.to_dict() or {}
        items.append({
            "file_id":     data.get("id") or d.id,
            "name":        data.get("filename") or data.get("name"),
            "url":         data.get("url"),         # 公開 URL（GCS public_url）
            "gcs_path":    data.get("gcs_path"),
            "uploaded_at": _ts_to_str(data.get("uploaded_at")),
        })
    return items


def _normalize_avatar(avatar_b64):
    """確保 avatar 是完整 data URL。"""
    if not avatar_b64:
        return None
    if isinstance(avatar_b64, str) and avatar_b64.startswith("data:"):
        return avatar_b64
    return "data:image/jpeg;base64," + avatar_b64


def person_to_seller(person: dict, role: dict | None, files: list[dict]) -> dict:
    role = role or {}
    return {
        "id":            person.get("id"),
        "name":          person.get("name", ""),
        "phone":         person.get("phone", "") or "",
        "note":          person.get("note", "") or "",
        "card_color":    person.get("card_color", "") or "",
        # avatar：LIBRARY 前端用 avatar_url；統一回傳 base64 形式（與 BUYER 一致）
        "avatar_url":    _normalize_avatar(person.get("avatar_b64")),
        # 角色欄位
        "address":       role.get("property_address") or "",
        "land_number":   role.get("land_number") or "",
        "category":      role.get("category") or "",
        "owner_price":   role.get("owner_price"),
        "suggest_price": role.get("suggest_price"),
        "source":        (person.get("source") or {}).get("note") if isinstance(person.get("source"), dict) else "",
        "status":        role.get("status") or "培養中",
        "files":         files,
        # 元資料
        "sort_order":      person.get("sort_order"),
        "created_by":      person.get("created_by"),
        "created_at":      _ts_to_str(person.get("created_at")),
        "updated_at":      _ts_to_str(person.get("updated_at")),
        "last_contact_at": _ts_to_str(person.get("last_contact_at")),
    }


def extract_person_patch(seller_data: dict) -> dict:
    """seller dict → people 主檔欄位（PATCH 用）。"""
    out = {}
    if "name" in seller_data:
        v = str(seller_data.get("name", "") or "").strip()
        if v:
            out["name"] = v
    if "phone" in seller_data:
        out["phone"] = str(seller_data.get("phone", "") or "").strip() or None
    if "note" in seller_data:
        out["note"] = str(seller_data.get("note", "") or "").strip() or None
    if "card_color" in seller_data:
        cc = str(seller_data.get("card_color", "") or "").strip()
        out["card_color"] = cc if (cc.startswith("#") and 4 <= len(cc) <= 9) else None
    return out


def extract_role_payload(seller_data: dict) -> dict:
    """seller dict → roles/seller 欄位。"""
    out = {}
    if "address" in seller_data:
        out["property_address"] = str(seller_data.get("address", "") or "").strip() or None
    if "land_number" in seller_data:
        out["land_number"] = str(seller_data.get("land_number", "") or "").strip() or None
    if "category" in seller_data:
        cat = str(seller_data.get("category", "") or "").strip()
        out["category"] = cat if cat in VALID_CATEGORIES else None
    for src, dst in [("owner_price", "owner_price"), ("suggest_price", "suggest_price")]:
        if src in seller_data:
            v = seller_data.get(src)
            try:
                out[dst] = float(v) if v not in (None, "", 0) else None
            except (TypeError, ValueError):
                out[dst] = None
    if "status" in seller_data:
        v = seller_data.get("status")
        if v in VALID_SELLER_STATUSES:
            out["status"] = v
    return out


def list_sellers(db, email: str, is_admin: bool) -> list[dict]:
    """列出準賣方。同 buyer_facade 的策略，先以 created_by 篩，Python 端再過濾 active_roles。"""
    if is_admin:
        q = db.collection("people").where("active_roles", "array_contains", "seller")
    else:
        q = db.collection("people").where("created_by", "==", email)
    items = []
    for doc in q.stream():
        person = doc.to_dict() or {}
        if person.get("is_group") or person.get("deleted_at"):
            continue
        if "seller" not in (person.get("active_roles") or []):
            continue
        person["id"] = doc.id
        role_snap = doc.reference.collection("roles").document(SELLER_ROLE_DOC_ID).get()
        role = role_snap.to_dict() if role_snap.exists else {}
        files = _files_list(doc.reference.collection("files").stream())
        items.append(person_to_seller(person, role, files))
    items.sort(key=lambda s: (
        0 if s.get("sort_order") is not None else 1,
        s.get("sort_order") if s.get("sort_order") is not None else 0,
    ))
    return items


def get_seller(db, person_id: str, email: str, is_admin: bool):
    ref = db.collection("people").document(person_id)
    snap = ref.get()
    if not snap.exists:
        return None
    person = snap.to_dict() or {}
    if not is_admin and person.get("created_by") != email:
        return False
    if "seller" not in (person.get("active_roles") or []):
        return None
    person["id"] = person_id
    role_snap = ref.collection("roles").document(SELLER_ROLE_DOC_ID).get()
    role = role_snap.to_dict() if role_snap.exists else {}
    files = _files_list(ref.collection("files").stream())
    return person_to_seller(person, role, files)


def create_seller(db, email: str, data: dict, server_timestamp_fn) -> str:
    name = str(data.get("name", "") or "").strip()
    if not name:
        raise ValueError("請填寫屋主姓名")

    person_payload = {
        "name": name,
        "display_name": None,
        "birthday": None,
        "zodiac": None,
        "gender": None,
        "company": None,
        "contacts": [],
        "addresses": [],
        "bucket": PEOPLE_BUCKET_DEFAULT,
        "warning": None,
        "source": {"channel": "other", "referrer_person_id": None,
                   "note": str(data.get("source", "") or "").strip()},
        "is_group": False,
        "group_type": None,
        "members": [],
        "card_color": None,
        "note": None,
        "phone": None,
        "avatar_b64": None,
        "active_roles": ["seller"],
        "has_completed_deal": False,
        "relations": [],
        "legacy_buyer_id": None,
        "legacy_seller_id": None,
        "last_contact_at": None,
        "sort_order": None,
        "deleted_at": None,
        "created_by": email,
        "created_at": server_timestamp_fn(),
        "updated_at": server_timestamp_fn(),
    }
    person_payload.update(extract_person_patch(data))

    person_ref = db.collection("people").document()
    person_ref.set(person_payload)

    role_payload = extract_role_payload(data)
    role_payload.setdefault("status", "培養中")
    role_payload["created_at"] = server_timestamp_fn()
    role_payload["created_by"] = email
    role_payload["updated_at"] = server_timestamp_fn()
    role_payload["archived_at"] = None
    person_ref.collection("roles").document(SELLER_ROLE_DOC_ID).set(role_payload)

    return person_ref.id


def update_seller(db, person_id: str, email: str, is_admin: bool, data: dict, server_timestamp_fn):
    ref = db.collection("people").document(person_id)
    snap = ref.get()
    if not snap.exists:
        return None
    person = snap.to_dict() or {}
    if not is_admin and person.get("created_by") != email:
        return False

    person_patch = extract_person_patch(data)
    # source 在 LIBRARY 中是字串（來源說明），寫到 people.source.note
    if "source" in data:
        existing_src = person.get("source") or {}
        if not isinstance(existing_src, dict):
            existing_src = {}
        person_patch["source"] = {
            "channel": existing_src.get("channel") or "other",
            "referrer_person_id": existing_src.get("referrer_person_id"),
            "note": str(data.get("source") or "").strip(),
        }

    if person_patch:
        person_patch["updated_at"] = server_timestamp_fn()
        ref.update(person_patch)

    role_patch = extract_role_payload(data)
    if role_patch:
        role_patch["updated_at"] = server_timestamp_fn()
        role_ref = ref.collection("roles").document(SELLER_ROLE_DOC_ID)
        rsnap = role_ref.get()
        if not rsnap.exists:
            role_patch["created_at"] = server_timestamp_fn()
            role_patch["created_by"] = email
            role_patch["archived_at"] = None
            role_patch.setdefault("status", "培養中")
        role_ref.set(role_patch, merge=True)
        active = list(person.get("active_roles") or [])
        if "seller" not in active:
            active.append("seller")
            ref.update({"active_roles": active})

    return True


def archive_seller(db, person_id: str, email: str, is_admin: bool, server_timestamp_fn):
    """撕去賣方標籤。主檔保留。"""
    ref = db.collection("people").document(person_id)
    snap = ref.get()
    if not snap.exists:
        return None
    person = snap.to_dict() or {}
    if not is_admin and person.get("created_by") != email:
        return False
    role_ref = ref.collection("roles").document(SELLER_ROLE_DOC_ID)
    rsnap = role_ref.get()
    if rsnap.exists:
        role_ref.update({"archived_at": server_timestamp_fn()})
    active = [r for r in (person.get("active_roles") or []) if r != "seller"]
    ref.update({"active_roles": active, "updated_at": server_timestamp_fn()})
    return True


def soft_delete_person(db, person_id: str, email: str, is_admin: bool, server_timestamp_fn):
    """連人脈一起刪。"""
    ref = db.collection("people").document(person_id)
    snap = ref.get()
    if not snap.exists:
        return None
    person = snap.to_dict() or {}
    if not is_admin and person.get("created_by") != email:
        return False
    ref.update({
        "deleted_at": server_timestamp_fn(),
        "updated_at": server_timestamp_fn(),
    })
    return True
