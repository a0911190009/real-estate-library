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
    }


# ───── 比對 ─────
def _norm(s):
    if not s:
        return ""
    s = str(s).translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    s = s.replace("一段", "1段").replace("二段", "2段").replace("三段", "3段")
    s = s.replace("之", "-")
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

    log.info("[yes319] step 3/4 拉 home-start 物件清單...")
    r = requests.get(f"{home_start_url.rstrip('/')}/api/properties", timeout=15)
    hs_resp = r.json()
    hs_items = hs_resp if isinstance(hs_resp, list) else hs_resp.get("items", [])
    log.info(f"[yes319] home-start {len(hs_items)} 筆")

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
