#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
word_parser.py — 伺服器端 Word 物件總表解析模組

本模組是 export_word_table.py 的伺服器版本，供 app.py import 使用。
差異：
  - 使用 antiword 替代 textutil（Linux/Cloud Run 環境）
  - 移除 main()、write_csv()、Firestore 比對等本機專用功能
  - 新增 parse_doc(path) 作為統一入口，回傳四類物件清單與文件日期

使用方式：
    from word_parser import parse_doc
    result = parse_doc("/path/to/物件總表.doc")
    # result["condo"], result["house"], result["farm"], result["build"], result["doc_date"]
"""

import re, warnings, datetime, unicodedata
warnings.filterwarnings("ignore")

# ────────────────────────────────────────────
# 常數
# ────────────────────────────────────────────

FEN_TO_PING = 293.4  # 1分 = 293.4坪（地籍換算）

AGENT_MAP = {
    "澤": "張文澤", "海": "雷文海", "芯": "許荺芯",
    "芳": "蔡秀芳", "良": "陳威良", "妤": "歐芷妤", "迎": "李振迎",
}

ORIENTATIONS = {'東', '西', '南', '北', '東南', '東北', '西南', '西北'}
STATUSES     = {'空', '自', '租', '空租', '空/自', '部分空'}

# ────────────────────────────────────────────
# 工具函數
# ────────────────────────────────────────────

def get_word_text(path):
    """
    使用 antiword -x db 將 .doc 轉為 DocBook XML，再轉成 tab 分隔文字。
    antiword 預設輸出用空格排版，無法用 split('\t') 切出欄位；
    改用 -x db 取得結構化 XML，解析表格後重組為 tab-separated 格式，
    讓後面的 state machine 解析器繼續可以使用。
    """
    import subprocess
    result = subprocess.run(
        ["antiword", "-x", "db", path],
        capture_output=True, text=True, encoding='utf-8', errors='replace'
    )
    if result.returncode != 0:
        raise RuntimeError(f"antiword 轉換失敗：{result.stderr[:200]}")
    return _docbook_to_tabtext(result.stdout)


def _docbook_to_tabtext(xml_str):
    """
    將 antiword -x db 輸出的 DocBook XML 轉成 tab-separated 文字。
    - 表格每 row 輸出一行，欄位以 \t 分隔（與原 textutil 輸出一致）
    - 段落 <para> 直接輸出純文字（保留段落標題讓 state machine 偵測）
    """
    import xml.etree.ElementTree as ET

    # 移除 DOCTYPE 宣告（ET 不支援外部 DTD，會拋 ParseError）
    # antiword 輸出格式：<!DOCTYPE article PUBLIC "..." "..." []>
    xml_str = re.sub(r'<!DOCTYPE\s.*?\]\s*>', '', xml_str, flags=re.DOTALL)
    xml_str = re.sub(r'<!DOCTYPE\s[^>]*>', '', xml_str, flags=re.DOTALL)

    # 找到根元素起點（容錯）
    m = re.search(r'<article\b', xml_str)
    if m:
        xml_str = xml_str[m.start():]

    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError as e:
        raise RuntimeError(f"DocBook XML 解析失敗：{e}")

    lines = []

    def get_text(elem):
        """遞迴取得元素所有文字內容，合併成一個字串"""
        return ' '.join(t.strip() for t in elem.itertext() if t.strip())

    def walk(elem):
        tag = elem.tag  # antiword 的 DocBook 不帶 namespace
        if tag in ('table', 'informaltable'):
            # 走遍 tgroup → tbody → row → entry
            for row in elem.iter('row'):
                cells = [get_text(entry) for entry in row if entry.tag == 'entry']
                if cells:
                    lines.append('\t'.join(cells))
        elif tag == 'para':
            t = get_text(elem)
            if t:
                lines.append(t)
        else:
            for child in elem:
                walk(child)

    walk(root)
    return '\n'.join(lines)


def extract_doc_date(text):
    """
    從 Word 文字中擷取右上角的更新日期，格式如「115年3月2日」。
    回傳 dict：{"minguo": "115年3月2日", "western": "2026-03-02"}
    找不到則回傳 None。
    """
    # 在前幾行中找「XXX年X月X日」，X 是民國年（3位數）
    head = "\n".join(text.split("\n")[:5])
    m = re.search(r'(1\d{2})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日', head)
    if not m:
        return None
    y_minguo = int(m.group(1))
    mon = int(m.group(2))
    day = int(m.group(3))
    y_western = y_minguo + 1911
    return {
        "minguo":  f"{y_minguo}年{mon}月{day}日",
        "western": f"{y_western}-{mon:02d}-{day:02d}",
        "parsed_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

def nospace(s):
    """移除所有空白（用於標題比對）"""
    return re.sub(r'\s+', '', str(s).strip())

def is_commission_no(s):
    """5~6 碼委託號碼（含前導零省略的情況）"""
    return bool(re.match(r'^\d{5,6}$', str(s).strip()))

def extract_commission_no(s):
    """提取 5~6 碼委託號碼，並補回前導零（如 91746 → 091746）
    支援緊接在中文字後面的號碼（如「臨海路小筆農地088670」）"""
    # 優先找 6 碼（可能緊接中文後）
    m = re.search(r'(?<!\d)(\d{5,6})(?!\d)', str(s))
    if m:
        return m.group(1).zfill(6)
    return ""

def normalize_commission_no(s):
    """確保委託號碼為 6 碼（補前導零）"""
    s = str(s).strip()
    if re.match(r'^\d{5,6}$', s):
        return s.zfill(6)
    return s

def is_minguo_year(s):
    """民國年 100~120（涵蓋近年到未來幾年）"""
    try:
        v = int(str(s).strip())
        return 100 <= v <= 120
    except:
        return False

def is_expiry_date(s):
    """到期日格式 M/D 或 M/D K"""
    return bool(re.match(r'^\d{1,2}/\d{1,2}(\s*[kK])?$', str(s).strip()))

def has_key(s):
    """到期日含 K → 有鑰匙"""
    return bool(re.search(r'[kK]', str(s)))

def is_online_note(s):
    return bool(re.search(r'網路沒上|不上網', str(s)))

def is_rental_token(s):
    """租件特徵 token（押金格式如「2個月」「3個月」，或租件面積備註如「地161.46坪」），非案名"""
    s = str(s).strip()
    if re.match(r'^\d+個?月$', s): return True
    # 「地X坪」「地約X坪」格式（租件住址欄的面積備註）
    if re.match(r'^地\s*[\d,\.]+\s*坪$', s): return True
    return False

def is_date_like(s):
    """建築完成日 YY.M.D 格式"""
    return bool(re.match(r'^\d{2,3}\.\d{1,2}\.?\d*$', str(s).strip()))

def is_layout(s):
    """格局 房/廳/衛"""
    return bool(re.match(r'^\d+\s*/\s*\d+\s*/\s*\d*', str(s).strip()))

def is_floor(s):
    """樓層 所在/總層 或 T1、T2+1、B1"""
    s = str(s).strip()
    return bool(re.match(r'^\d+/\d+$', s) or re.match(r'^[TB]\d', s))

def is_price_token(s):
    """含萬字的價格"""
    return bool(re.search(r'[\d,\.]+\s*萬', str(s)))

def parse_price(s):
    """解析萬元金額，支援「X億Y萬」格式（如「1億0444萬」→ 10444萬）"""
    s = str(s).strip()
    if re.match(r'^[分坪]', s) or re.search(r'/[分坪]', s):
        return None
    # 「X億Y萬」格式
    m_yi = re.search(r'([\d,\.]+)\s*億\s*([\d,\.]*)\s*萬', s)
    if m_yi:
        try:
            yi  = float(m_yi.group(1).replace(',', ''))
            wan = float(m_yi.group(2).replace(',', '')) if m_yi.group(2) else 0
            v = yi * 10000 + wan
            return v if v > 0 else None
        except:
            pass
    # 純「X萬」格式
    m = re.search(r'([\d,\.]+)\s*萬', s)
    if m:
        try:
            v = float(m.group(1).replace(',', ''))
            return v if v > 0 else None
        except:
            pass
    return None

def parse_unit_price(s):
    """分售/坪售單價（前綴「分」「坪」）"""
    m = re.match(r'^[分坪]([\d,\.]+)\s*萬', str(s).strip())
    if m:
        try:
            return float(m.group(1).replace(',', ''))
        except:
            pass
    return None

def parse_fen_to_ping(s):
    """面積：X分 → 坪；或直接含坪字"""
    s = str(s).strip()
    m = re.search(r'([\d\.]+)\s*分', s)
    if m:
        return round(float(m.group(1)) * FEN_TO_PING, 1)
    m2 = re.search(r'([\d,\.]+)\s*坪', s)
    if m2:
        try:
            return round(float(m2.group(1).replace(',', '')), 1)
        except:
            pass
    return None

def extract_agents(s):
    found = [AGENT_MAP[ch] for ch in str(s) if ch in AGENT_MAP]
    return "、".join(dict.fromkeys(found)) if found else ""

def is_pure_number(s):
    """純小數或整數（可能是坪數）"""
    return bool(re.match(r'^[\d,]+\.?\d*$', str(s).strip()))

def clean_name(s):
    """清除案名中的委託號碼（5~6位數）、不上網備註、末尾民國年"""
    # 去除 5~6 位連續數字（委託號碼），前後不能是數字
    s = re.sub(r'(?<!\d)\d{5,6}(?!\d)', '', str(s))
    s = re.sub(r'\s*(網路沒上|不上網)\s*', '', s)
    s = re.sub(r'\s+1\d\d\s*$', '', s)
    return re.sub(r'\s+', ' ', s).strip()

def clean_address(s):
    """清除地址末尾夾帶的民國年"""
    # 有空格版：「巷10號 115」
    s = re.sub(r'\s+1\d\d\s*$', '', str(s))
    # 無空格版：「巷88號115」（號後面直接接民國年，後面沒有室/樓/之/弄）
    s = re.sub(r'(號)(1\d\d)\s*$', r'\1', s)
    return s.strip()


def extract_minguo_from_address(s):
    """從地址末尾抓取民國年（有空格或緊接在號後），回傳整數或 None"""
    s = str(s)
    # 有空格版：「號 115」
    m = re.search(r'\s+(1\d\d)\s*$', s)
    if m:
        return int(m.group(1))
    # 無空格版：「號115」（後面沒有室/樓/之）
    m = re.search(r'號(1\d\d)\s*$', s)
    if m:
        return int(m.group(1))
    return None

HEADER_NORM = {
    nospace(h) for h in [
        "編號","編 號","物件地址","座向","完成日","格局","格 局","現況",
        "地坪","建坪","室內","樓層","總價","總 價","sale","Sale","到期",
        "區域","面積","分售","坪售","農地","建地","公寓","住家","別墅",
        "店住","店面","類別","案名","租金","押金","業務","住址","看",
        "注意事項","面      積","分    售","總      價","坪    售",
        "農     地","建     地","公  寓","住  家","別  墅","店  住","店  面",
        "售出止",
    ]
}

def is_header(s):
    return nospace(s) in HEADER_NORM

def is_numeric_unit(s):
    """數字+單位（分/坪/萬），不可能是案名"""
    s = str(s).strip()
    if re.match(r'^[\d,\.]+\s*(分|坪|萬)', s): return True
    if re.match(r'^[分坪][\d,\.]+\s*萬', s): return True
    if re.match(r'^[\d,\.]+\s*坪[\s(（]', s): return True
    return False

def is_mixed_land_area(s):
    """農建混合面積行：如「建地14.37坪+農地287.45坪」、「農地9.15分+建地50.22坪」
    或「建20.34坪+農6110.36坪」、「1030.24坪+道路地143.53坪」
    這些行不是案名，是面積描述。
    注意：「志航建地+農地」「興昌農地+建地」等純文字含+的是案名，不在此判斷。"""
    s = str(s).strip()
    if '+' not in s:
        return False
    # 必須含「數字+坪/分」才算面積行（純文字的案名不含數字量詞）
    has_digit_unit = bool(re.search(r'[\d\.]+\s*(坪|分)', s))
    if not has_digit_unit:
        return False
    # 不含萬（避免誤判售價行）
    has_price = bool(re.search(r'萬', s))
    return not has_price

def is_total_area_line(s):
    """加總面積行：如「共301.83坪」「共4.68分」「共1539.42坪」"""
    s = str(s).strip()
    return bool(re.match(r'^共\s*[\d,\.]+\s*(坪|分)', s))

def is_area_region(s):
    """地區名稱（如「台東市」「卑南鄉」「花蓮縣」），非案名"""
    return bool(re.match(r'^.{2,5}[市鄉鎮區縣]$', str(s).strip()))

# ────────────────────────────────────────────
# Tab 段解析（固定欄位順序狀態機）
# ────────────────────────────────────────────

def collect_tab_tokens(text, section_type):
    """
    收集指定段落的所有 token（含標題行裡夾帶的資料 token）。
    遇到租件標題（押金+租金）→ 跳過該行。
    遇到農地段開始（「編 號」單行）→ 停止。
    遇到其他房屋類型的段標題行（如住家段收到別墅/店住標題）→ 停止，避免跨段污染。
    """
    # 所有房屋類型關鍵字（用來偵測「其他段」標題）
    OTHER_TYPES = {'公寓', '住家', '別墅', '店住', '店面'}
    target_ns = nospace(section_type)

    tokens = []
    in_section = False

    for line in text.split('\n'):
        parts_raw = [p.strip() for p in line.split('\t')]  # 使用 \t 作為分隔符

        # 租件標題行 → 跳過
        if '押金' in parts_raw and '租金' in parts_raw:
            continue

        # 租件資料行：含押金特徵（「X個月」）→ 整行跳過，避免月租金/押金污染 token 流
        if any(re.match(r'^\d+個?月$', p.strip()) for p in parts_raw if p.strip()):
            continue

        parts = [p for p in parts_raw if p]
        if not parts:
            continue

        # 農地段開始（單行「編 號」）→ 停止收集
        if parts == ['編 號']:
            if in_section:
                break
            continue

        ns_parts = [nospace(p) for p in parts]

        # 識別本段標題行（含「編號」且含本段類型）
        if '編號' in ns_parts and any(ns == target_ns for ns in ns_parts):
            in_section = True
            # 標題行本身可能夾帶資料（從最後一個標題欄位後面開始）
            last_h = 0
            for idx, p in enumerate(parts):
                if is_header(p) or p.lower() == 'sale':
                    last_h = idx
            tokens.extend(parts[last_h + 1:])
            continue

        # in_section 時，遇到其他類型的段標題行 → 停止（避免跨段污染）
        if in_section and '編號' in ns_parts:
            other_types_found = OTHER_TYPES - {target_ns}
            if any(ns in other_types_found for ns in ns_parts):
                break

        if not in_section:
            continue

        tokens.extend(parts)

    return tokens


def parse_tab_fixed(tokens, section_type):
    """
    用固定欄位順序狀態機解析 Tab 段 token 串。

    欄位順序（公寓）：
      [委託號碼] 案名 地址 [座向] 完成日 格局 現況 室內坪 建坪 樓層 售價 經紀人 [到期日] [委託號碼→下一筆]

    欄位順序（住家/別墅/店住）：
      [委託號碼] 案名 地址 座向 完成日 格局 現況 地坪 建坪 樓層 售價 經紀人 [到期日] [委託號碼→下一筆]

    狀態：
      0: 等委託號碼或案名
      1: 等地址
      2: 等座向（公寓通常空）或完成日
      3: 等完成日
      4: 等格局
      5: 等現況
      6: 等地坪/室內坪
      7: 等建坪
      8: 等樓層
      9: 等售價
      10: 等經紀人
      11: 收尾（等到期日或下一筆委託號碼）
    """
    is_condo = (section_type == "公寓")
    entries = []

    # 當前物件的暫存
    cur = None

    def new_entry(commission="", name="", online="是"):
        return {
            "類型": section_type,
            "委託號碼": commission,
            "案名": name,
            "物件地址": "",
            "座向": "",
            "完成日": "",
            "格局": "",
            "現況": "",
            "室內坪": "",   # 公寓用
            "地坪": "",     # 住家/別墅/店住用
            "建坪": "",
            "樓層": "",
            "售價萬": "",
            "經紀人": "",
            "到期日": "",
            "有鑰匙": "",
            "上網": online,
            "_minguo": None,   # 從地址末尾抓到的民國年，用於組合完整到期日
        }

    def commit(entry):
        """儲存一筆物件（有案名且有售價或坪數才算有效）"""
        if not entry or not entry["案名"]:
            return
        if not (entry["售價萬"] or entry["地坪"] or entry["室內坪"]):
            return
        # 排除預售屋：現況欄填「預售」視為預售物件
        if str(entry.get("現況", "")).strip() == "預售":
            print(f"  [略過-預售] {entry['案名']}")
            entry.pop("_minguo", None)
            return
        # 排除地址空白或地址不含路/街/巷/弄/號（如只有單元代號 A1、A2）
        addr = str(entry.get("物件地址", "")).strip()
        if not addr or not re.search(r'(路|街|巷|弄|號|村|里|段)', addr):
            print(f"  [略過-無有效地址] {entry['案名']} | 地址={repr(addr)}")
            entry.pop("_minguo", None)
            return
        entry.pop("_minguo", None)  # 清除內部暫存欄位
        entries.append(entry)

    state = 0           # 狀態機
    pending_commission = ""   # 等待分配給下一筆案名的委託號碼
    i = 0

    while i < len(tokens):
        tok = tokens[i]

        # ── 任何狀態下都可能插入的雜訊 token ──
        # 租件押金格式（如「2個月」「3個月」）→ 直接跳過
        if is_rental_token(tok):
            i += 1
            continue

        # 「售出止」→ 標記當前物件到期日為「售出止」，並提交，準備下一筆
        if tok == '售出止':
            if cur:
                if not cur["到期日"]:
                    cur["到期日"] = "售出止"
                commit(cur)
                cur = None
            state = 0
            i += 1
            continue

        if is_online_note(tok):
            # 去除「網路沒上/不上網」備註後的剩餘文字
            clean_part = re.sub(r'\s*(網路沒上|不上網)\s*', '', tok).strip()
            if cur:
                cur["上網"] = "否"

            if state == 0 and len(clean_part) >= 3 and re.search(r'[\u4e00-\u9fff]', clean_part):
                # 「案名 不上網」→ 取出案名繼續走 state=0 邏輯
                tok = clean_part
                # 不 continue，讓下面的 state=0 判斷接著跑
            elif state == 1 and re.search(r'(路|街|巷|弄|號|村|里|段)', clean_part) and len(clean_part) > 3:
                # 「地址 不上網」→ state=1 等地址，直接存入地址欄
                cur["物件地址"] = clean_address(clean_part)
                state = 2
                i += 1
                continue
            else:
                i += 1
                continue

        if is_minguo_year(tok):
            # 民國年被塞進其他欄位，直接忽略（或記為備註）
            i += 1
            continue

        # ── 狀態 0：等委託號碼或案名 ──
        if state == 0:
            if is_commission_no(tok):
                pending_commission = tok
                i += 1
                continue

            # 案名判斷（排除地址：末尾含路/街/巷/弄/號/段且長度>8）
            name_c = clean_name(tok)
            tok_addr_like = (
                re.search(r'(路|街|巷|弄|號|段)\s*\d*\s*$', name_c) and len(name_c) > 6
            )
            if (len(name_c) >= 3
                    and re.search(r'[\u4e00-\u9fff]', name_c)
                    and not is_header(tok)
                    and not is_price_token(tok)
                    and not is_date_like(tok)
                    and not is_floor(tok)
                    and not is_layout(tok)
                    and tok not in ORIENTATIONS
                    and tok not in STATUSES
                    and not is_pure_number(tok)
                    and not is_numeric_unit(tok)
                    and not is_rental_token(tok)
                    and not tok_addr_like):

                # 提交上一筆（如果有）
                commit(cur)
                online = "否" if is_online_note(tok) else "是"
                commission = pending_commission or extract_commission_no(tok)
                pending_commission = ""
                cur = new_entry(commission=commission, name=name_c, online=online)
                state = 1
            else:
                # 不認識的 token，維持 state=0
                pass

            i += 1
            continue

        # ── 狀態 1：等地址 ──
        if state == 1:
            if re.search(r'(路|街|巷|弄|號|村|里|段)', tok) and len(tok) > 4:
                # 地址末尾可能夾帶民國年（有空格或緊接門號後），先抓出來
                yr = extract_minguo_from_address(tok)
                if yr:
                    cur["_minguo"] = yr
                cur["物件地址"] = clean_address(tok)
                state = 2
            elif is_date_like(tok):
                # 沒地址，直接到完成日
                cur["完成日"] = tok
                state = 4
            elif is_commission_no(tok):
                # 本筆沒地址就結束，下一筆的委託號碼
                commit(cur); cur = None
                pending_commission = tok
                state = 0
            i += 1
            continue

        # ── 狀態 2：等座向（公寓通常空）或完成日 ──
        if state == 2:
            if tok in ORIENTATIONS:
                cur["座向"] = tok
                state = 3
            elif is_date_like(tok):
                cur["完成日"] = tok
                state = 4
            elif is_commission_no(tok):
                commit(cur); cur = None
                pending_commission = tok
                state = 0
            i += 1
            continue

        # ── 狀態 3：等完成日 ──
        if state == 3:
            if is_date_like(tok):
                cur["完成日"] = tok
                state = 4
            elif is_layout(tok):
                state = 4  # 直接跳到格局
                cur["格局"] = tok
                state = 5
            elif is_commission_no(tok):
                commit(cur); cur = None
                pending_commission = tok
                state = 0
            i += 1
            continue

        # ── 狀態 4：等格局 ──
        if state == 4:
            if is_layout(tok):
                cur["格局"] = tok
                state = 5
            elif tok in STATUSES:
                # 沒格局，直接現況
                cur["現況"] = tok
                state = 6
            elif is_commission_no(tok):
                commit(cur); cur = None
                pending_commission = tok
                state = 0
            i += 1
            continue

        # ── 狀態 5：等現況 ──
        if state == 5:
            if tok in STATUSES:
                cur["現況"] = tok
                state = 6
            elif is_pure_number(tok) and not is_minguo_year(tok):
                # 沒現況，直接到坪數
                v = float(tok.replace(',', ''))
                if 0 < v < 10000:
                    if is_condo:
                        cur["室內坪"] = str(round(v, 2))
                    else:
                        cur["地坪"] = str(round(v, 2))
                    state = 7
            elif is_commission_no(tok):
                commit(cur); cur = None
                pending_commission = tok
                state = 0
            i += 1
            continue

        # ── 狀態 6：等地坪（公寓=室內坪）──
        if state == 6:
            if is_pure_number(tok) and not is_minguo_year(tok):
                v_str = tok.replace(',', '')
                try:
                    v = float(v_str)
                    if 0 < v < 10000:
                        if is_condo:
                            cur["室內坪"] = str(round(v, 2))
                        else:
                            cur["地坪"] = str(round(v, 2))
                        state = 7
                except:
                    pass
            elif tok.startswith('國有約'):
                # 特殊：「國有約130」
                cur["地坪"] = tok
                state = 7
            elif is_commission_no(tok):
                commit(cur); cur = None
                pending_commission = tok
                state = 0
            i += 1
            continue

        # ── 狀態 7：等建坪 ──
        if state == 7:
            if is_pure_number(tok) and not is_minguo_year(tok):
                v_str = tok.replace(',', '')
                try:
                    v = float(v_str)
                    if 0 < v < 10000:
                        cur["建坪"] = str(round(v, 2))
                        state = 8
                except:
                    pass
            elif is_floor(tok):
                # 沒建坪，直接到樓層
                cur["樓層"] = tok
                state = 9
            elif is_commission_no(tok):
                commit(cur); cur = None
                pending_commission = tok
                state = 0
            i += 1
            continue

        # ── 狀態 8：等樓層 ──
        if state == 8:
            if is_floor(tok):
                cur["樓層"] = tok
                state = 9
            elif is_price_token(tok):
                # 沒樓層，直接到售價
                p = parse_price(tok)
                if p and p > 10:
                    cur["售價萬"] = str(p)
                    state = 10
            elif is_commission_no(tok):
                commit(cur); cur = None
                pending_commission = tok
                state = 0
            i += 1
            continue

        # ── 狀態 9：等售價 ──
        if state == 9:
            if is_price_token(tok):
                p = parse_price(tok)
                if p and p > 10:
                    cur["售價萬"] = str(p)
                    state = 10
            elif is_commission_no(tok):
                commit(cur); cur = None
                pending_commission = tok
                state = 0
            i += 1
            continue

        # ── 狀態 10：等經紀人 ──
        if state == 10:
            agents = extract_agents(tok)
            if agents:
                cur["經紀人"] = agents
                state = 11
            elif is_commission_no(tok):
                commit(cur); cur = None
                pending_commission = tok
                state = 0
            elif is_expiry_date(tok):
                # 沒經紀人，直接到期日
                raw_d = re.sub(r'\s*[kK]\s*$', '', tok).strip()
                cur["有鑰匙"] = "是" if has_key(tok) else ""
                minguo = cur.pop("_minguo", None)
                m_md = re.match(r'^(\d{1,2})/(\d{1,2})$', raw_d)
                if m_md and minguo:
                    cur["到期日"] = f"{minguo}年{int(m_md.group(1))}月{int(m_md.group(2))}日"
                else:
                    cur["到期日"] = raw_d
                state = 0
                commit(cur); cur = None
            i += 1
            continue

        # ── 狀態 11：收尾（等到期日或下一筆委託號碼/案名）──
        if state == 11:
            if is_expiry_date(tok):
                raw_d = re.sub(r'\s*[kK]\s*$', '', tok).strip()
                cur["有鑰匙"] = "是" if has_key(tok) else ""
                # 若有從地址抓到的民國年，組合成完整格式
                minguo = cur.pop("_minguo", None)
                m_md = re.match(r'^(\d{1,2})/(\d{1,2})$', raw_d)
                if m_md and minguo:
                    cur["到期日"] = f"{minguo}年{int(m_md.group(1))}月{int(m_md.group(2))}日"
                else:
                    cur["到期日"] = raw_d
                i += 1
                # 等待下一個 commission no 或新案名
                state = 0
                continue
            elif is_commission_no(tok):
                commit(cur); cur = None
                pending_commission = tok
                state = 0
                i += 1
                continue
            else:
                # 可能直接跳到下一筆案名
                name_c = clean_name(tok)
                if (len(name_c) >= 3
                        and re.search(r'[\u4e00-\u9fff]', name_c)
                        and not is_header(tok)
                        and not is_price_token(tok)):
                    commit(cur); cur = None
                    pending_commission = ""
                    cur = new_entry(name=name_c, online="否" if is_online_note(tok) else "是")
                    state = 1
                    i += 1
                    continue
            i += 1
            continue

        i += 1

    commit(cur)
    return entries


def parse_house_section(text, section_type):
    """解析住家/別墅/店住（含委託號碼，有座向）"""
    tokens = collect_tab_tokens(text, section_type)
    return parse_tab_fixed(tokens, section_type)


def parse_condo_section(text):
    """解析公寓（委託號碼在末尾，無座向）"""
    tokens = collect_tab_tokens(text, "公寓")
    return parse_tab_fixed(tokens, "公寓")


# ────────────────────────────────────────────
# 農地段解析（逐行，固定欄位順序）
# ────────────────────────────────────────────

def parse_farm_entries(text):
    """
    農地每欄獨立一行：
    案名[+委託號碼] → 區域 → 面積 → [分售單價] → 總價 → 經紀人 → [到期日]
    """
    entries = []
    all_lines = [l.strip() for l in text.split('\n')]

    # 找農地段
    start_idx = None
    for idx, line in enumerate(all_lines):
        if line == '編 號':
            nxt = [l for l in all_lines[idx+1:idx+5] if l]
            if nxt and nospace(nxt[0]) == '農地':
                start_idx = idx
                break
    if start_idx is None:
        return entries

    end_idx = len(all_lines)
    for idx in range(start_idx + 2, len(all_lines)):
        if all_lines[idx] == '編 號':
            nxt = [l for l in all_lines[idx+1:idx+5] if l]
            if nxt and nospace(nxt[0]) == '建地':
                end_idx = idx
                break

    farm_lines = all_lines[start_idx:end_idx]

    def is_farm_name(s):
        if not s or is_header(s) or is_numeric_unit(s): return False
        if re.match(r'^[\d,\.]+$', s): return False
        if is_price_token(s) and not re.search(r'[\u4e00-\u9fff]', s): return False
        cn = clean_name(s)
        if len(cn) < 3 or not re.search(r'[\u4e00-\u9fff]', cn): return False
        if is_area_region(s): return False
        if is_expiry_date(s): return False
        # 農建混合面積行（如「農地9.15分+建地50.22坪」）不是案名
        if is_mixed_land_area(s): return False
        # 「共X坪/分」加總行不是案名
        if is_total_area_line(s): return False
        # 純面積行（如「4.2分」「2.63分」「308.89坪」「740.46坪(共2.52分)」）不是案名
        # 正規化後若只剩數字+分/坪（含括號加總說明），排除
        s_clean = re.sub(r'\d{5,6}', '', s).strip()  # 去委託號碼
        s_clean = re.sub(r'\s*(網路沒上|不上網)\s*', '', s_clean).strip()
        # 純「數字坪/分」
        if re.match(r'^[\d,\.]+\s*(分|坪)$', s_clean): return False
        # 「數字坪(共X分)」「數字坪(共X.XX分)」這類含括號的面積說明行
        if re.match(r'^[\d,\.]+\s*(坪|分)\s*[\(（]', s_clean): return False
        # 「分X.XX分」格式（分售單價誤觸）不是案名
        if re.match(r'^分[\d,\.]+分$', nospace(s)): return False
        # 「分X萬」「坪X萬」格式（分/坪售單價）不是案名
        if re.match(r'^[分坪][\d,\.]+萬?$', nospace(s)): return False
        # 「X億Y萬」純金額格式不是案名
        if re.match(r'^[\d,\.]+\s*億[\d,\.]*\s*萬?$', s.strip()): return False
        return True

    def extract_area_from_line(s):
        """從可能含委託號碼或不上網備註的行中，提取面積（分或坪）
        支援：純「X分」「X坪」，以及「私有X坪+承租權X坪+使用權X坪」等複合格式
        回傳 (面積原文, 面積坪數) 或 None
        """
        # 去委託號碼和不上網備註
        clean = re.sub(r'(?<!\d)\d{5,6}(?!\d)', '', s)
        clean = re.sub(r'網路沒上|不上網', '', clean).strip()

        # 純「X分」或「X坪」
        m_fen = re.match(r'^([\d,\.]+)\s*分$', clean.strip())
        m_ping = re.match(r'^([\d,\.]+)\s*坪$', clean.strip())
        if m_fen:
            val = float(m_fen.group(1).replace(',',''))
            return (clean.strip(), round(val * FEN_TO_PING, 1))
        if m_ping:
            val = float(m_ping.group(1).replace(',',''))
            return (clean.strip(), round(val, 1))

        # 「X坪(共Y分)」或「X坪（共Y分）」格式：以括號外的坪數為主
        m_ping_fen = re.match(r'^([\d,\.]+)\s*坪\s*[\(（]', clean.strip())
        if m_ping_fen:
            val = float(m_ping_fen.group(1).replace(',',''))
            return (clean.strip(), round(val, 1))

        # 複合面積格式：「私有X坪+承租權X坪+使用權X坪」等，各項以「+」連接
        # 只要整行裡有「X坪」或「X分」的片段（可能前面帶中文說明），就加總
        if '+' in clean and re.search(r'[\d,\.]+\s*(坪|分)', clean):
            # 用「+」分割各段，各段提取數字+單位
            parts = clean.split('+')
            total_ping = 0.0
            found = False
            unit_final = '坪'
            for part in parts:
                m = re.search(r'([\d,\.]+)\s*(坪|分)', part)
                if m:
                    val = float(m.group(1).replace(',',''))
                    u = m.group(2)
                    if u == '分':
                        total_ping += val * FEN_TO_PING
                        unit_final = '分'
                    else:
                        total_ping += val
                    found = True
            if found:
                return (clean.strip(), round(total_ping, 1))

        return None

    SKIP = {nospace(h) for h in [
        '編 號','農地','農     地','區域','面積','面      積',
        '分售','分    售','總價','總      價','Sale','sale','到期'
    ]}

    i = 0
    while i < len(farm_lines):
        line = farm_lines[i]
        if not line or nospace(line) in SKIP:
            i += 1; continue
        if not is_farm_name(line):
            i += 1; continue

        entry = {
            "類型": "農地",
            "委託號碼": extract_commission_no(line),  # 補前導零
            "案名": clean_name(line),
            "區域": "", "面積原文": "", "面積坪": "",
            "分售單價萬": "", "售價萬": "",
            "到期日": "", "有鑰匙": "",
            "經紀人": "", "上網": "否" if is_online_note(line) else "是",
            "_minguo": None, "_raw_expiry": "",
        }

        all_prices = []
        next_i = i + 1

        # 預掃：是否有「分X.XX分」格式的面積行（代表面積用前綴「分」格式記錄）
        has_prefixed_fen_area = any(
            re.match(r'^分[\d,\.]+分$', nospace(farm_lines[k]))
            for k in range(i+1, min(i+12, len(farm_lines)))
            if farm_lines[k].strip()
        )

        for j in range(i+1, min(i+12, len(farm_lines))):
            w = farm_lines[j].strip()
            if not w: continue
            if is_farm_name(w):
                next_i = j; break

            if is_online_note(w):
                entry["上網"] = "否"
                # 「不上網   X.XX分/坪（含複合面積）」同行 → 嘗試提取面積
                area_result = extract_area_from_line(w)
                if area_result and not entry["面積坪"]:
                    entry["面積原文"] = area_result[0]
                    entry["面積坪"] = str(area_result[1])
                continue
            # 委託號碼（含「128771   5.28分」同行的情況）
            if re.search(r'(?<!\d)\d{5,6}(?!\d)', w):
                no_match = re.search(r'(?<!\d)(\d{5,6})(?!\d)', w)
                if no_match and not entry["委託號碼"]:
                    entry["委託號碼"] = no_match.group(1).zfill(6)
                # 去掉委託號碼後看是否還有面積
                area_result = extract_area_from_line(w)
                if area_result and not entry["面積坪"]:
                    entry["面積原文"] = area_result[0]
                    entry["面積坪"] = str(area_result[1])
                continue
            if is_minguo_year(w):
                entry["_minguo"] = int(w); continue
            if is_expiry_date(w):
                entry["_raw_expiry"] = re.sub(r'\s*[kK]', '', w).strip()
                entry["有鑰匙"] = "是" if has_key(w) else ""
                continue

            # 區域（短地名）
            if not entry["區域"] and is_area_region(w):
                entry["區域"] = w; continue

            # 農建混合複合面積行（如「農地9.15分+建地50.22坪」）
            if is_mixed_land_area(w):
                entry["面積原文"] = w
                pings = re.findall(r'([\d,\.]+)\s*坪', w)
                fens = re.findall(r'([\d,\.]+)\s*分', w)
                total = sum(float(p.replace(',','')) for p in pings)
                total += sum(float(f.replace(',','')) * FEN_TO_PING for f in fens)
                if total > 0:
                    entry["面積坪"] = str(round(total, 1))
                continue

            # 加總確認行（如「共4.68分」「共301.83坪」）→ 覆蓋面積
            if is_total_area_line(w):
                m_total = re.match(r'^共\s*([\d,\.]+)\s*(坪|分)', w)
                if m_total:
                    val = float(m_total.group(1).replace(',', ''))
                    unit = m_total.group(2)
                    ping_val = round(val * FEN_TO_PING, 1) if unit == '分' else round(val, 1)
                    entry["面積原文"] = entry["面積原文"] + f" 共{m_total.group(1)}{unit}" if entry["面積原文"] else w
                    entry["面積坪"] = str(ping_val)
                continue

            # 面積（「分X.XX分」格式：前綴「分」是欄位名殘留，後面的數字+分才是面積）
            # 例：「分15.65分」→ 面積15.65分；「分5.28分」→ 面積5.28分
            if not entry["面積坪"] and re.match(r'^分[\d,\.]+分$', nospace(w)):
                m_fen2 = re.search(r'([\d,\.]+)\s*分$', w)
                if m_fen2:
                    val = float(m_fen2.group(1).replace(',', ''))
                    entry["面積原文"] = f"{m_fen2.group(1)}分"
                    entry["面積坪"] = str(round(val * FEN_TO_PING, 1))
                    continue

            if not entry["面積坪"]:
                ping = parse_fen_to_ping(w)
                if ping:
                    # 若這個物件用「分X.XX分」前綴格式記面積，
                    # 則純「X.XX分」應視為分售單價（如 51.07分 = 51.07萬/分）
                    if has_prefixed_fen_area and re.match(r'^[\d,\.]+\s*分$', w.strip()):
                        entry["分售單價萬"] = str(ping / FEN_TO_PING)  # 還原為萬/分
                        # 直接嘗試視為分售單價（ping 是坪換算值，要反算回萬）
                        m_val = re.match(r'^([\d,\.]+)\s*分$', w.strip())
                        if m_val:
                            entry["分售單價萬"] = m_val.group(1)
                        continue
                    entry["面積原文"] = w
                    entry["面積坪"] = str(ping)
                    continue

            # 分售單價
            up = parse_unit_price(w)
            if up: entry["分售單價萬"] = str(up); continue

            # 售價（同時抓末尾民國年）
            p = parse_price(w)
            if p and p > 0:
                all_prices.append(p)
                m_yr = re.search(r'\s+(1\d\d)\s*$', w)
                if m_yr and 100 <= int(m_yr.group(1)) <= 120:
                    entry["_minguo"] = int(m_yr.group(1))
                continue

            # 經紀人
            a = extract_agents(w)
            if a and not entry["經紀人"]: entry["經紀人"] = a; continue

            next_i = j + 1

        if all_prices:
            entry["售價萬"] = str(max(all_prices))

        # 組合完整到期日（民國年+月/日）
        raw_exp = entry.pop("_raw_expiry", "")
        minguo = entry.pop("_minguo", None)
        if raw_exp:
            m = re.match(r'^(\d{1,2})/(\d{1,2})', raw_exp)
            if m and minguo:
                entry["到期日"] = f"{minguo}年{int(m.group(1))}月{int(m.group(2))}日"
            else:
                entry["到期日"] = raw_exp

        if entry["案名"] and (entry["售價萬"] or entry["面積坪"]):
            entries.append(entry)

        i = next_i

    return entries


# ────────────────────────────────────────────
# 建地段解析（逐行，固定欄位順序）
# ────────────────────────────────────────────

def parse_build_entries(text):
    """
    建地每欄獨立一行：
    [委託號碼] 案名[附註] → 面積坪[附註合併] → 坪售單價 → 總價[民國年] → 經紀人 → [到期日]

    特殊情況：
    - 案名或面積欄含未閉合的左括號「(」→ 與下一行合併補完附註
    - 括號內容為附註（如地址提示、備用面積）
    - 售價行末尾可能夾帶民國年（如「3212萬   115」），用於組成完整到期日
    - 委託號碼可能在案名行上一行（單獨一行）或塞在案名後
    - 售出止：標記此物件已售出
    """
    entries = []
    all_lines = [l.strip() for l in text.split('\n')]

    start_idx = None
    for idx, line in enumerate(all_lines):
        if line == '編 號':
            nxt = [l for l in all_lines[idx+1:idx+5] if l]
            if nxt and nospace(nxt[0]) == '建地':
                start_idx = idx
                break
    if start_idx is None:
        return entries

    # ── 預處理：把跨行的括號附註合併成同一行 ──
    # 例：「大面寬店面建地 (中興路一段」+ 下一行「近泰安街)     53.54坪」
    # → 合併為「大面寬店面建地 (中興路一段近泰安街)」+ 面積「53.54坪」
    raw = all_lines[start_idx:]
    merged = []
    j = 0
    while j < len(raw):
        line = raw[j]
        # 偵測未閉合的左括號
        if line.count('(') > line.count(')') and j + 1 < len(raw):
            next_line = raw[j+1].strip()
            # 合併：括號補完
            merged_line = line + next_line
            merged.append(merged_line)
            j += 2
        else:
            merged.append(line)
            j += 1
    build_lines = merged

    def is_build_name(s):
        if not s or is_header(s) or is_numeric_unit(s): return False
        if re.match(r'^[\d,\.]+$', s): return False
        if is_price_token(s) and not re.search(r'[\u4e00-\u9fff]', s): return False
        # 「X億Y萬」純金額格式（如「1億0444萬」）不是案名
        if re.match(r'^[\d,\.]+\s*億[\d,\.]*\s*萬?$', s.strip()): return False
        cn = clean_name(s)
        if len(cn) < 3 or not re.search(r'[\u4e00-\u9fff]', cn): return False
        if is_area_region(s): return False
        if is_expiry_date(s): return False
        if nospace(s) in {'售出止'}: return False
        # 農建混合面積行不是案名（如「建地14.37坪+農地287.45坪」）
        if is_mixed_land_area(s): return False
        # 「共X坪」加總行不是案名
        if is_total_area_line(s): return False
        return True

    SKIP = {nospace(h) for h in [
        '編 號','建地','建     地','面積','面      積',
        '坪售','坪    售','總價','總      價','Sale','sale','到期','網路沒上','不上網'
    ]}

    def extract_bracket_note(s):
        """從字串中提取括號內的附註，回傳（清除附註後的主文, 附註）"""
        notes = re.findall(r'\([^)]*\)', s)
        note_str = '、'.join(n.strip('()').strip() for n in notes) if notes else ""
        main = re.sub(r'\([^)]*\)', '', s).strip()
        return main, note_str

    def extract_area_from_merged(s):
        """
        從合併後的行中提取面積坪數。
        合併行如：「大面寬店面建地 (中興路一段近泰安街)     53.54坪」
        或：「更生路建地+店面 (更生路272號 建79.66坪)  229.30坪」
        → 取括號外最大的坪數（最後出現的坪數通常是總面積）
        """
        # 找所有「數字+坪」，不在括號內的
        no_bracket = re.sub(r'\([^)]*\)', '', s)  # 去掉括號內容
        matches = re.findall(r'([\d,\.]+)\s*坪', no_bracket)
        if matches:
            vals = []
            for m in matches:
                try:
                    vals.append(float(m.replace(',', '')))
                except:
                    pass
            return max(vals) if vals else None
        # fallback：含括號內也找
        matches2 = re.findall(r'([\d,\.]+)\s*坪', s)
        if matches2:
            vals = [float(m.replace(',', '')) for m in matches2 if m.replace(',','').replace('.','').isdigit()]
            return max(vals) if vals else None
        return None

    def parse_price_with_minguo(s):
        """解析售價，同時提取末尾夾帶的民國年（如「3212萬   115」→ (3212, 115)）"""
        s = str(s).strip()
        minguo = None
        # 找末尾的民國年
        m_year = re.search(r'\s+(1\d\d)\s*$', s)
        if m_year:
            yr = int(m_year.group(1))
            if 100 <= yr <= 120:
                minguo = yr
        # 解析售價
        price = parse_price(s)
        return price, minguo

    def format_expiry(month_day_str, minguo_year):
        """組合完整到期日：115年6月30日"""
        if not month_day_str:
            return ""
        m = re.match(r'^(\d{1,2})/(\d{1,2})', month_day_str)
        if m:
            mon, day = int(m.group(1)), int(m.group(2))
            if minguo_year:
                return f"{minguo_year}年{mon}月{day}日"
            else:
                return f"{mon}/{day}"
        return month_day_str

    i = 0
    pending_commission = ""
    pending_online = False   # 記錄案名前的「網路沒上」單獨行

    while i < len(build_lines):
        line = build_lines[i].strip()

        # 跳過標題與空行
        if not line or nospace(line) in SKIP:
            i += 1; continue

        # 售出止備註 → 下一筆標記
        if nospace(line) == '售出止':
            i += 1; continue

        # 單獨的委託號碼行（在案名行前一行）
        if is_commission_no(line):
            pending_commission = normalize_commission_no(line)
            i += 1; continue

        # 不上網備註（單獨行）→ 記下來，給下一筆 entry 使用
        if is_online_note(line):
            pending_online = True
            i += 1; continue

        if not is_build_name(line):
            i += 1; continue

        # ── 解析案名，提取括號附註 ──
        main_name, bracket_note = extract_bracket_note(line)
        main_name = clean_name(main_name)
        # 清除案名末尾夾帶的坪數（如「大面寬店面建地 53.54坪」→「大面寬店面建地」）
        main_name = re.sub(r'\s+[\d,\.]+\s*坪\s*$', '', main_name).strip()

        # 案名行本身可能含坪數（括號外的坪數是面積）
        area_in_name = None
        if '坪' in line:
            area_in_name = extract_area_from_merged(line)

        # 上網狀態：案名行本身有不上網備註，或案名前有獨立不上網行
        is_offline = pending_online or is_online_note(line)

        entry = {
            "類型": "建地",
            "委託號碼": pending_commission or extract_commission_no(line),
            "案名": main_name,
            "附註": bracket_note,
            "面積原文": "", "面積坪": str(round(area_in_name, 1)) if area_in_name else "",
            "坪售單價萬": "", "售價萬": "",
            "到期日": "", "有鑰匙": "",
            "經紀人": "",
            "上網": "否" if is_offline else "是",
        }
        pending_commission = ""
        pending_online = False   # 用完就清除

        all_prices = []
        minguo_year = None   # 從售價行末尾提取的民國年
        raw_expiry = ""      # 原始到期日（月/日）
        next_i = i + 1

        for j in range(i+1, min(i+12, len(build_lines))):
            w = build_lines[j].strip()
            if not w: continue
            if is_build_name(w) and not nospace(w) in SKIP:
                next_i = j; break
            if nospace(w) == '售出止': continue
            if is_commission_no(w):
                pending_commission = normalize_commission_no(w)
                continue
            if is_online_note(w):
                entry["上網"] = "否"
                # 若「網路沒上」緊接在下一筆案名前，同時設 pending_online 給下一筆用
                for jj in range(j+1, min(j+4, len(build_lines))):
                    nw = build_lines[jj].strip()
                    if not nw: continue
                    if is_build_name(nw) and nospace(nw) not in SKIP:
                        pending_online = True
                    break
                continue
            if is_expiry_date(w):
                raw_expiry = re.sub(r'\s*[kK]\s*', '', w).strip()
                entry["有鑰匙"] = "是" if has_key(w) else ""
                continue

            # 農建混合複合面積行（如「建地14.37坪+農地287.45坪」）→ 記錄原文，坪數加總
            if is_mixed_land_area(w):
                entry["面積原文"] = w
                pings = re.findall(r'([\d,\.]+)\s*坪', w)
                fens = re.findall(r'([\d,\.]+)\s*分', w)
                total = sum(float(p.replace(',','')) for p in pings)
                total += sum(float(f.replace(',','')) * FEN_TO_PING for f in fens)
                if total > 0:
                    entry["面積坪"] = str(round(total, 1))
                continue

            # 加總確認行（如「共301.83坪」）→ 覆蓋面積（加總值更精確）
            if is_total_area_line(w):
                m_total = re.match(r'^共\s*([\d,\.]+)\s*(坪|分)', w)
                if m_total:
                    val = float(m_total.group(1).replace(',', ''))
                    unit = m_total.group(2)
                    if unit == '分':
                        val = round(val * FEN_TO_PING, 1)
                    entry["面積原文"] = entry["面積原文"] + f" 共{m_total.group(1)}{unit}" if entry["面積原文"] else w
                    entry["面積坪"] = str(round(val, 1))
                continue

            # 面積（若案名行已含則不再覆蓋）
            if not entry["面積坪"]:
                # 先試合併括號後的面積
                area = extract_area_from_merged(w)
                if area:
                    main_w, note_w = extract_bracket_note(w)
                    entry["面積原文"] = w
                    entry["面積坪"] = str(round(area, 1))
                    if note_w and not entry["附註"]:
                        entry["附註"] = note_w
                    continue
                ping = parse_fen_to_ping(w)
                if ping:
                    entry["面積原文"] = w
                    entry["面積坪"] = str(ping)
                    continue

            # 坪售單價
            # 優先嘗試「坪X萬」前綴格式
            up = parse_unit_price(w)
            if up:
                entry["坪售單價萬"] = str(up); continue

            # 售價（同時提取民國年）
            p, yr = parse_price_with_minguo(w)
            if p and p > 0:
                all_prices.append(p)
                if yr: minguo_year = yr
                continue

            # 不上網備註夾在售價行（如「不上網        572坪」）
            if is_online_note(w): entry["上網"] = "否"; continue
            if not entry["面積坪"]:
                ping_inline = parse_fen_to_ping(w)
                if ping_inline:
                    entry["面積坪"] = str(ping_inline); continue

            # 經紀人
            a = extract_agents(w)
            if a and not entry["經紀人"]: entry["經紀人"] = a; continue

            next_i = j + 1

        # 組合完整到期日
        entry["到期日"] = format_expiry(raw_expiry, minguo_year)

        # 建地排除坪售單價
        if all_prices:
            area = float(entry["面積坪"]) if entry["面積坪"] else None
            valid = []
            for p in all_prices:
                if area and p < area * 10 and p < 300 and not entry["坪售單價萬"]:
                    entry["坪售單價萬"] = str(p)
                else:
                    valid.append(p)
            entry["售價萬"] = str(max(valid)) if valid else (str(max(all_prices)) if all_prices else "")

        if entry["案名"] and (entry["售價萬"] or entry["面積坪"]):
            entries.append(entry)

        i = next_i

    return entries


# ────────────────────────────────────────────
# 欄位定義（供 app.py 參考）
# ────────────────────────────────────────────

CONDO_COLS = [
    "流水號", "委託號碼", "案名", "物件地址", "完成日", "格局",
    "現況", "室內坪", "建坪", "樓層", "售價萬", "經紀人", "到期日", "有鑰匙", "上網"
]

HOUSE_COLS = [
    "流水號", "類型", "委託號碼", "案名", "物件地址", "座向", "完成日", "格局",
    "現況", "地坪", "建坪", "樓層", "售價萬", "經紀人", "到期日", "有鑰匙", "上網"
]

FARM_COLS = [
    "流水號", "委託號碼", "案名", "區域", "面積原文", "面積坪",
    "分售單價萬", "售價萬", "經紀人", "到期日", "有鑰匙", "上網"
]

BUILD_COLS = [
    "流水號", "委託號碼", "案名", "附註", "面積原文", "面積坪",
    "坪售單價萬", "售價萬", "經紀人", "到期日", "有鑰匙", "上網"
]


def dedup_by_address(entries):
    """
    公寓/房屋去重：同地址視為同一物件，保留第一筆。
    例外：若兩筆的「現況」（所有權人/自住租空）或「經紀人」不同，則視為不同物件保留。
    回傳去重後的清單，並在 console 印出被合併的筆數。
    """
    seen = {}   # {地址: entry}
    result = []
    merged = 0
    for e in entries:
        addr = e.get('物件地址', '').strip()
        if not addr:
            result.append(e)
            continue
        if addr not in seen:
            seen[addr] = e
            result.append(e)
        else:
            prev = seen[addr]
            # 現況或經紀人有任一不同 → 視為不同物件
            if (e.get('現況','') != prev.get('現況','') or
                    e.get('經紀人','') != prev.get('經紀人','')):
                result.append(e)
            else:
                merged += 1
                print(f"  [去重] 地址重複合併：{addr}（案名：{e.get('案名','')}）")
    if merged:
        print(f"  共去重 {merged} 筆")
    return result


# ────────────────────────────────────────────
# 統一入口函數（供 app.py import 使用）
# ────────────────────────────────────────────

def parse_doc(path):
    """
    解析 .doc 物件總表，回傳四類物件的清單與文件日期。
    回傳 dict: {"condo": [...], "house": [...], "farm": [...], "build": [...], "doc_date": {...} or None}
    """
    text = get_word_text(path)
    doc_date = extract_doc_date(text)

    # 公寓
    condo_raw = parse_condo_section(text)
    condo = dedup_by_address(condo_raw)

    # 住家/別墅/店住
    house = []
    for stype in ["住家", "別墅", "店住"]:
        house.extend(parse_house_section(text, stype))
    house = dedup_by_address(house)

    # 農地
    farm = parse_farm_entries(text)

    # 建地
    build = parse_build_entries(text)

    return {
        "condo": condo,
        "house": house,
        "farm": farm,
        "build": build,
        "doc_date": doc_date,
    }
