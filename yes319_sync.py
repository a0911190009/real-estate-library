"""yes319 公司網頁 → home-start 同步模組（搬自本機 ~/Projects/yes319_*.py）。

工作流程：
1. 爬列表頁 + 詳情頁，dump 所有公司網頁物件
2. 跟 home-start 已有物件比對（售價精確 + 案名 fuzzy）
3. 把比對成功的 features / amenities / age / floor / yes319_objno 推到 home-start

由 library 後端的 /api/yes319/sync endpoint 呼叫。
"""
import logging
import os
import re
import time
from difflib import SequenceMatcher

import requests

BASE = "http://www.xn--ogt71lbodiqy.tw"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

log = logging.getLogger("yes319")


def _fetch(url, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.encoding = r.apparent_encoding or "utf-8"
            r.raise_for_status()
            return r.text
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(2)


# ───── 列表頁 ─────
def _crawl_list(kind, max_pages=20):
    """kind = 'm2'(房屋) or 'm3'(土地)。回傳 set of objno。"""
    objs = set()
    for page in range(1, max_pages + 1):
        html = _fetch(f"{BASE}/{kind}/?page={page}")
        page_objs = set(re.findall(r"showobj\.php\?objno=([a-zA-Z0-9]+)", html))
        new = page_objs - objs
        if not new and page > 1:
            break
        objs |= page_objs
        time.sleep(0.3)
    return objs


# ───── 詳情頁 parser ─────
def _clean(s):
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"&nbsp;", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _td_after(html, label):
    m = re.search(
        rf"(?:[\s>;]){re.escape(label)}\s*</td>\s*<td[^>]*>(.*?)</td>", html, re.S
    )
    return _clean(m.group(1)) if m else ""


def _features(html):
    m = re.search(r"特色\s*</td>\s*<td[^>]*?class=\"[^\"]*?p1[^\"]*\"[^>]*>(.*?)</td>", html, re.S)
    raw = m.group(1) if m else None
    if not raw:
        m2 = re.search(r'<div\s+class="markdown-body">(.*?)</div>', html, re.S)
        raw = m2.group(1) if m2 else ""
    paras = re.findall(r"<p>(.*?)</p>", raw, re.S)
    if not paras:
        return _clean(raw)
    return "\n".join(_clean(p) for p in paras if _clean(p))


def _amenities(html):
    m = re.search(r"生活機能\s*</td>\s*<td[^>]*>(.*?)</td>\s*</tr>", html, re.S)
    if not m:
        return []
    rows = re.findall(
        r'<td[^>]*>\s*<img\s+src="[^"]*?(Checked(?:Not)?)\.webp"[^>]*/?>\s*([^<]+?)\s*</td>',
        m.group(1),
    )
    return [n.replace("&nbsp;", "").strip() for s, n in rows if s == "Checked" and n.strip()]


def _price(html):
    m = re.search(
        r'總價</td>\s*<td[^>]*>\s*<span[^>]*>([\d,\.]+)</span>\s*<span[^>]*>萬',
        html,
    )
    return float(m.group(1).replace(",", "")) if m else None


def _parse_age(s):
    m = re.search(r"屋齡約\(\s*(\d+)\s*年\)", s or "")
    return int(m.group(1)) if m else None


def _parse_floor(s):
    if not s:
        return ""
    m = re.match(r"(\d+)樓/整棟", s)
    if m:
        return f"T{m.group(1)}"
    m = re.match(r"(\d+)樓/共(\d+)樓", s)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    return s


def _parse_detail(kind, objno):
    html = _fetch(f"{BASE}/{kind}/showobj.php?objno={objno}")
    return {
        "objno": objno,
        "kind": kind,
        "title": _td_after(html, "案名"),
        "price_wan": _price(html),
        "address": _td_after(html, "地址"),
        "age_str": _td_after(html, "建築完成日期"),
        "floor_str": _td_after(html, "樓層/樓高"),
        "features": _features(html),
        "amenities": _amenities(html),
        "photos": _photos_from_html(html),
    }


def _photos_from_html(html):
    """抽 yes319 詳情頁的照片 URL（carousel 內 src）。"""
    m = re.search(r'class="carousel-inner"\s*>([\s\S]*?)</div>\s*<!--', html, re.S)
    if not m:
        return []
    block = m.group(1)
    urls = re.findall(r'<img\s+[^>]*?src="(/upload/[^"]+)"', block)
    seen, result = set(), []
    for u in urls:
        full = u if u.startswith("http") else (BASE + u if u.startswith("/") else BASE + "/" + u)
        if full not in seen:
            seen.add(full)
            result.append(full)
    return result


# ───── 比對 ─────
def _norm(s):
    if not s:
        return ""
    s = str(s).translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    s = s.replace("一段", "1段").replace("二段", "2段").replace("三段", "3段")
    s = s.replace("之", "-")
    # yes319 把專任委託物件的案名後面加「-專任」（如「開發隊5台分農地-專任」）→ 比對時要去掉
    # 涵蓋常見後綴格式：-專任 / （專任）/ (專任) / 【專任】 / [專任]
    s = re.sub(r"[-－﹣―\s]*[\(（【\[]?專任[\)）】\]]?\s*$", "", s)
    return re.sub(r"\s+", "", s).lower()


def _title_sim(a, b):
    return SequenceMatcher(None, _norm(a), _norm(b)).ratio()


def _addr_overlap(a, b):
    a, b = _norm(a), _norm(b)
    if not a or not b:
        return 0
    keys = re.findall(r"[一-鿿]{1,5}[路街巷弄段]", b)
    return sum(1 for k in keys if k in a) / max(1, len(keys)) if keys else 0


def _score(yes, hs):
    yp, hp = yes.get("price_wan"), float(hs.get("price") or 0)
    if not yp or not hp or abs(yp - hp) > 0.1:
        return None
    return _title_sim(yes.get("title"), hs.get("title")) * 0.7 + \
           _addr_overlap(yes.get("address"), hs.get("address")) * 0.3


# ───── home-start 物件清單（含房屋 + 土地兩種；/api/properties 預設只回房屋） ─────
def _fetch_all_hs_properties(home_start_url):
    """合併拉 house + land 兩種類別。土地 (農地/建地/道路用地) 預設不在 /api/properties。"""
    base = home_start_url.rstrip("/")
    out = []
    for type_ in ("house", "land"):
        try:
            r = requests.get(f"{base}/api/properties?type={type_}&limit=500", timeout=20)
            d = r.json()
            items = d if isinstance(d, list) else d.get("items", [])
            out.extend(items)
        except Exception as e:
            log.warning(f"[yes319] 拉 {type_} 清單失敗：{e}")
    return out


# ───── 推送到 home-start ─────
def _push_to_home_start(home_start_url, service_key, hs_id, payload):
    url = f"{home_start_url.rstrip('/')}/admin/property/{hs_id}/meta"
    r = requests.post(
        url, json=payload,
        headers={"X-Service-Key": service_key, "Content-Type": "application/json"},
        timeout=15,
    )
    return r.status_code, (r.json() if r.text else {})


# ───── 主流程 ─────
def run_full_sync(home_start_url, service_key, threshold=0.6, dry_run=False):
    """爬 yes319 + 比對 home-start + 推送。回傳結果摘要 dict。"""
    if not home_start_url or not service_key:
        return {"ok": False, "error": "缺少 home_start_url 或 service_key"}

    started = time.time()
    log.info("[yes319] step 1/4 爬列表頁...")
    objs_m2 = _crawl_list("m2")
    objs_m3 = _crawl_list("m3")
    targets = [("m2", o) for o in sorted(objs_m2)] + \
              [("m3", o) for o in sorted(objs_m3)]
    log.info(f"[yes319] m2={len(objs_m2)} m3={len(objs_m3)} total={len(targets)}")

    log.info("[yes319] step 2/4 爬詳情頁...")
    yes_items = []
    crawl_fails = []
    seen_obj = set()
    for kind, obj in targets:
        if obj in seen_obj:
            continue
        seen_obj.add(obj)
        try:
            yes_items.append(_parse_detail(kind, obj))
        except Exception as e:
            crawl_fails.append({"objno": obj, "error": str(e)})
        time.sleep(0.3)
    log.info(f"[yes319] 成功爬 {len(yes_items)} 筆、失敗 {len(crawl_fails)}")

    log.info("[yes319] step 3/4 拉 home-start 物件清單（房屋 + 土地）...")
    hs_items = _fetch_all_hs_properties(home_start_url)
    log.info(f"[yes319] home-start {len(hs_items)} 筆（含土地）")

    log.info("[yes319] step 4/4 比對 + 推送...")
    matched = []
    suspect = []
    unmatched = []
    pushed_ok = 0
    pushed_fail = 0
    for yes in yes_items:
        candidates = sorted(
            [(s, hs) for hs in hs_items if (s := _score(yes, hs)) is not None],
            key=lambda x: -x[0],
        )
        if not candidates:
            unmatched.append(yes["objno"])
            continue
        best_s, best_hs = candidates[0]
        ambiguous = len(candidates) > 1 and (candidates[0][0] - candidates[1][0]) < 0.05
        if best_s < threshold or ambiguous:
            suspect.append({"objno": yes["objno"], "hs_id": best_hs.get("id"),
                            "score": round(best_s, 3)})
            continue
        # 高信心 → 推送
        payload = {
            "features": yes["features"],
            "amenities": yes["amenities"],
            "yes319_objno": yes["objno"],
        }
        age_int = _parse_age(yes["age_str"])
        floor_norm = _parse_floor(yes["floor_str"])
        if age_int is not None:
            payload["age"] = age_int
        if floor_norm:
            payload["floor"] = floor_norm
        if dry_run:
            matched.append({"objno": yes["objno"], "hs_id": best_hs.get("id"), "score": round(best_s, 3), "would_push": payload})
            continue
        try:
            status, body = _push_to_home_start(home_start_url, service_key, best_hs.get("id"), payload)
            if status == 200 and body.get("ok"):
                pushed_ok += 1
                matched.append({"objno": yes["objno"], "hs_id": best_hs.get("id"),
                                "title": yes["title"], "score": round(best_s, 3)})
            else:
                pushed_fail += 1
        except Exception as e:
            pushed_fail += 1
            log.warning(f"[yes319] push #{best_hs.get('id')} 失敗：{e}")

    elapsed = round(time.time() - started, 1)
    return {
        "ok": True,
        "dry_run": dry_run,
        "yes319_crawled": len(yes_items),
        "yes319_fails": len(crawl_fails),
        "home_start_total": len(hs_items),
        "matched": len(matched),
        "suspect": len(suspect),
        "unmatched": len(unmatched),
        "pushed_ok": pushed_ok,
        "pushed_fail": pushed_fail,
        "elapsed_sec": elapsed,
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "matched_items": matched[:20],   # 前 20 筆樣本
    }


# ───── 下架：home-start 已連結 yes319_objno 但 yes319 已沒在賣 → 標記下架 ─────
def run_unlist_missing(home_start_url, service_key, dry_run=False):
    """找出 home-start 已連結 yes319_objno 但 yes319 已不存在的物件，標記下架。
    需要 home-start 有對應的 admin API 或用 webhook 觸發。
    """
    if not home_start_url or not service_key:
        return {"ok": False, "error": "缺少 home_start_url 或 service_key"}
    started = time.time()
    # 1. 收集 yes319 現有所有 objno
    log.info("[unlist] 抓 yes319 列表...")
    objs_m2 = _crawl_list("m2")
    objs_m3 = _crawl_list("m3")
    yes_set = objs_m2 | objs_m3
    log.info(f"[unlist] yes319 現有 {len(yes_set)} 筆 objno")

    # 2. 拉 home-start 現有物件
    hs_items = _fetch_all_hs_properties(home_start_url)

    # 3. 找出「有 yes319_objno 但已不在 yes319 set 中」的物件
    to_unlist = []
    for p in hs_items:
        objno = p.get("yes319_objno")
        if objno and objno not in yes_set:
            to_unlist.append({"id": p["id"], "title": p.get("title", ""),
                              "objno": objno, "price": p.get("price")})
    log.info(f"[unlist] 需下架: {len(to_unlist)} 筆")

    if dry_run:
        return {"ok": True, "dry_run": True, "yes319_total": len(yes_set),
                "to_unlist": to_unlist, "elapsed_sec": round(time.time() - started, 1)}

    # 4. 透過 home-start 的 sync webhook 標記 unlist
    unlisted_ok, unlisted_fail = 0, 0
    for t in to_unlist:
        try:
            r = requests.post(
                f"{home_start_url.rstrip('/')}/api/sync/webhook",
                json={"property_id": str(t["id"]), "action": "unlist"},
                headers={"X-Service-Key": service_key, "Content-Type": "application/json"},
                timeout=10,
            )
            if r.status_code == 200:
                unlisted_ok += 1
            else:
                unlisted_fail += 1
        except Exception as e:
            unlisted_fail += 1
            log.warning(f"[unlist] #{t['id']} 失敗：{e}")
    return {"ok": True, "yes319_total": len(yes_set), "unlisted_ok": unlisted_ok,
            "unlisted_fail": unlisted_fail, "to_unlist": to_unlist,
            "elapsed_sec": round(time.time() - started, 1)}


# ───── 新增 dry-run：yes319 有但 home-start 沒對應的物件清單 ─────
def run_create_missing_dryrun(home_start_url, service_key):
    """列出 yes319 有但 home-start 找不到對應的物件（dry-run，不寫入）。"""
    started = time.time()
    objs_m2 = _crawl_list("m2")
    objs_m3 = _crawl_list("m3")
    targets = [("m2", o) for o in sorted(objs_m2)] + [("m3", o) for o in sorted(objs_m3)]
    yes_items = []
    seen = set()
    for kind, obj in targets:
        if obj in seen: continue
        seen.add(obj)
        try:
            yes_items.append(_parse_detail(kind, obj))
        except Exception:
            pass
        time.sleep(0.3)

    hs_items = _fetch_all_hs_properties(home_start_url)

    missing = []
    for yes in yes_items:
        if any(_score(yes, hs) is not None and _score(yes, hs) >= 0.6 for hs in hs_items):
            continue
        missing.append({
            "objno": yes["objno"], "kind": yes["kind"], "title": yes["title"],
            "price_wan": yes["price_wan"], "address": yes["address"],
            "n_photos": len(yes.get("photos", [])),
            "has_features": bool(yes.get("features")),
        })
    return {"ok": True, "yes319_total": len(yes_items),
            "home_start_total": len(hs_items),
            "missing_count": len(missing),
            "missing": missing,
            "elapsed_sec": round(time.time() - started, 1)}


# ───── 補照片：對 home-start 沒照片但有 yes319_objno 的物件，從 yes319 下載照片補上 ─────
def run_photo_sync(home_start_url, service_key, max_per_prop=15):
    """對 home-start 已連結 yes319 但無照片的物件，從 yes319 下載並上傳照片。"""
    if not home_start_url or not service_key:
        return {"ok": False, "error": "缺少 home_start_url 或 service_key"}
    started = time.time()
    log.info("[yes319-photo] 拉 home-start 物件清單...")
    hs_items = _fetch_all_hs_properties(home_start_url)

    # 找出「有 yes319_objno 但 photos 為空」的物件
    targets = []
    for p in hs_items:
        if p.get("yes319_objno") and not (p.get("photos") or []):
            targets.append({"id": p["id"], "title": p.get("title", ""), "objno": p["yes319_objno"]})
    log.info(f"[yes319-photo] 需補照片: {len(targets)} 筆")
    if not targets:
        return {"ok": True, "targets": 0, "elapsed_sec": round(time.time() - started, 1)}

    total_uploaded = 0
    total_failed = 0
    detail = []
    for t in targets:
        # 從 yes319 抓詳情頁的照片清單（試 m2 失敗就 m3）
        photos = []
        for kind in ("m2", "m3"):
            try:
                html = _fetch(f"{BASE}/{kind}/showobj.php?objno={t['objno']}")
                if "obj-detail-photo" in html or "carousel-inner" in html:
                    photos = _photos_from_html(html)
                    if photos:
                        break
            except Exception:
                continue
        photos = photos[:max_per_prop]
        if not photos:
            detail.append({"id": t["id"], "objno": t["objno"], "uploaded": 0, "skipped": "no yes319 photos"})
            continue

        n_ok, n_fail = 0, 0
        for url in photos:
            try:
                # 下載圖
                img_r = requests.get(url, headers={**HEADERS, "Referer": BASE}, timeout=30)
                img_r.raise_for_status()
                content = img_r.content
                # 推到 home-start
                ext = url.rsplit(".", 1)[-1].split("?")[0].lower()
                filename = f"yes319_{t['objno']}.{ext}"
                content_type = img_r.headers.get("Content-Type", "image/jpeg")
                up = requests.post(
                    f"{home_start_url.rstrip('/')}/admin/photos/upload",
                    files={"photo": (filename, content, content_type)},
                    data={"property_id": str(t["id"])},
                    headers={"X-Service-Key": service_key},
                    timeout=60,
                )
                if up.status_code == 200:
                    n_ok += 1
                else:
                    n_fail += 1
                    log.warning(f"[yes319-photo] upload #{t['id']} 失敗 {up.status_code}: {up.text[:200]}")
            except Exception as e:
                n_fail += 1
                log.warning(f"[yes319-photo] download/upload #{t['id']} 失敗：{e}")
        total_uploaded += n_ok
        total_failed += n_fail
        detail.append({"id": t["id"], "title": t["title"], "objno": t["objno"],
                       "uploaded": n_ok, "failed": n_fail})
        log.info(f"[yes319-photo] #{t['id']} {t['title']}：{n_ok} 張成功 / {n_fail} 張失敗")

    return {
        "ok": True,
        "targets": len(targets),
        "total_uploaded": total_uploaded,
        "total_failed": total_failed,
        "elapsed_sec": round(time.time() - started, 1),
        "detail": detail,
    }
