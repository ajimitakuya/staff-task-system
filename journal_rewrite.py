import time
import uuid
import json
import re
import pandas as pd
import streamlit as st
import google.generativeai as genai
from selenium.webdriver.common.by import By
from common import now_jst
from data_access import load_db, save_db, get_companies_df

# =========================================
# Gemini JSON生成
# =========================================
def _get_gemini_api_key():
    api_key = ""
    try:
        api_key = st.secrets.get("GEMINI_API_KEY", "")
    except Exception:
        api_key = ""

    if not api_key:
        import os
        api_key = os.environ.get("GEMINI_API_KEY", "")

    return str(api_key or "").strip()


def get_current_company_saved_knowbe_info():
    company_id = str(st.session_state.get("company_id", "")).strip()
    if not company_id:
        return "", ""

    df = get_companies_df()
    if df is None or df.empty:
        return "", ""

    hit = df[df["company_id"].astype(str).str.strip() == company_id]
    if hit.empty:
        return "", ""

    row = hit.iloc[0]
    return (
        str(row.get("knowbe_login_username", "")).strip(),
        str(row.get("knowbe_login_password", "")).strip(),
    )

def _now_str():
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# =========================================
# ログ書き込み
# =========================================
def append_journal_log(row_dict):
    df = load_db("journal_rewrite_logs")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "exec_id", "exec_time", "user", "company",
            "resident_name", "target_month",
            "result", "count", "message"
        ])

    df = pd.concat([df, pd.DataFrame([row_dict])], ignore_index=True)
    save_db(df, "journal_rewrite_logs")

# =========================================
# 月単位の日誌再生成
# =========================================
def generate_json_with_gemini_local(page_text: str, outside_workplace: str = ""):
    api_key = _get_gemini_api_key()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY が見つかりません")

    genai.configure(api_key=api_key)

    system_instruction = """
支援記録リライト専用 命令書 v7.1
あなたの仕事は、Knowbe支援記録の原文をもとに、「利用者状態」と「職員考察」を、
そのまま現場で貼り付けできる自然で完成度の高い文章へ再構成することです。

【最重要原則】
1. 捏造禁止
2. 利用者状態は必ず完成文にする
3. 職員考察は支援者視点のみ
4. 出力はJSONのみ
"""

    outside_info = str(outside_workplace or "").strip() or "未指定"
    print(f"[FIX] Geminiへ送る施設外就労先 = {outside_info}", flush=True)

    prompt = f"""
以下はKnowbeの支援記録ページを月単位で取得した本文です。
本文を読み取り、日付ごとに「利用者状態」と「職員考察」をJSONのみで返してください。

【今回の施設外就労先指定】
{outside_info}

【絶対条件】
- 出力はJSONのみ
- キーは YYYY-MM-DD
- 値は user_state / staff_note の2つ
- 捏造禁止
- 利用者状態は必ず完成文にする
- 利用者状態には、開始時の様子、体調、作業内容、終了時の報告が分かるようにする
- 「精神安定、体調不安定」などの短語のみは禁止

【支援記録本文】
{page_text}
"""

    model = genai.GenerativeModel(
        model_name="gemini-2.5-flash",
        system_instruction=system_instruction
    )

    response = model.generate_content(prompt)
    text = str(getattr(response, "text", "") or "").strip()

    print("[JR] Gemini raw response start", flush=True)
    print(text[:3000], flush=True)
    print("[JR] Gemini raw response end", flush=True)

    if not text:
        return {}

    no_record_words = [
        "利用実績がありません",
        "利用実績を入力後、ご利用ください",
        "支援記録がありません",
        "JSONを生成できません",
    ]
    if any(w in text for w in no_record_words):
        print("[JR] Gemini returned no-record message; skip month", flush=True)
        return {}

    cleaned = text.replace("```json", "").replace("```", "").strip()
    json_start = cleaned.find("{")
    json_end = cleaned.rfind("}")
    if json_start == -1 or json_end == -1 or json_end <= json_start:
        print("[JR] Gemini JSON block not found; skip month", flush=True)
        return {}

    json_text = cleaned[json_start:json_end + 1]
    try:
        return json.loads(json_text)
    except Exception as e:
        print(f"[JR] Gemini JSON parse failed; skip month: {e}", flush=True)
        return {}


# =========================================
# textarea操作
# =========================================
def _set_react_textarea_value(driver, el, value: str):
    value = "" if value is None else str(value)

    driver.execute_script("""
        const el = arguments[0];
        const value = arguments[1];

        el.focus();
        el.value = value;

        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        el.dispatchEvent(new Event('blur', { bubbles: true }));
    """, el, value)

    time.sleep(0.2)


def _find_row_textareas_for_support_record(row):
    areas = []
    for ta in row.find_elements(By.TAG_NAME, "textarea"):
        try:
            if ta.is_displayed():
                areas.append(ta)
        except Exception:
            continue
    return areas


def _textarea_value(el):
    try:
        return str(el.get_attribute("value") or "").strip()
    except Exception:
        return ""


# =========================================
# 後処理ユーティリティ
# =========================================
def _normalize_text(s):
    return str(s or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def _sentencize_jp(text: str):
    s = _normalize_text(text).replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return []
    return [x.strip() for x in re.split(r"(?<=[。！？])\s*", s) if x.strip()]


def _looks_like_short_health_only(text: str):
    s = re.sub(r"\s+", "", _normalize_text(text)).rstrip("。")
    if not s:
        return True

    # 記号ゆれ吸収
    s2 = s.replace("、", ",").replace("，", ",").replace("・", ",")

    short_set = {
        "体調良好", "体調は良好", "体調が良好", "良好", "元気", "元気です",
        "体調普通", "体調は普通", "体調が普通", "普通",
        "体調まあまあ", "体調はまあまあ", "体調がまあまあ", "まあまあ",
        "体調まぁまぁ", "体調はまぁまぁ", "体調がまぁまぁ", "まぁまぁ",
        "体調大丈夫", "体調は大丈夫", "体調が大丈夫", "大丈夫",
        "体調安定", "精神安定", "体調不安定", "精神不安定",
        "精神安定,体調安定", "体調安定,精神安定",
        "精神不安定,体調不安定", "体調不安定,精神不安定",
        "精神安定,体調不安定", "体調不安定,精神安定",
        "精神不安定,体調安定", "体調安定,精神不安定",
    }

    if s in short_set or s2 in short_set:
        return True

    # 「精神○○」「体調○○」だけで構成される短文も弾く
    labels = ["精神安定", "精神不安定", "体調安定", "体調不安定", "体調良好", "体調普通", "元気", "良好", "普通"]
    temp = s2
    for lb in labels:
        temp = temp.replace(lb, "")
    temp = temp.replace(",", "").strip()

    if not temp and len(_sentencize_jp(s)) <= 1:
        return True

    return len(_sentencize_jp(s)) <= 1 and len(s) <= 20 and ("体調" in s or "精神" in s or s in short_set or s2 in short_set)


def _contains_explicit_no_work_reason(text: str):
    s = _normalize_text(text)
    bad_patterns = [
        r"できず", r"できませんでした", r"できていません", r"全くできず", r"全くできていません",
        r"しんどくて", r"体調[^。]*優れない", r"体調不良", r"休養", r"休ま", r"困難"
    ]
    return any(re.search(p, s) for p in bad_patterns)


def _work_default_unit(work: str):
    w = _normalize_text(work)
    if not w:
        return "個"
    if "塗り絵" in w or "チラシ" in w or "コースター" in w:
        return "枚"
    if "お箸" in w or "箸入れ" in w:
        return "膳"
    if "折り鶴" in w:
        return "羽"
    if "本" in w:
        return "本"
    return "個"


def _is_short_user_state(text: str) -> bool:
    t = str(text or "").replace("\u3000", " ").strip()
    if not t:
        return True

    short_keywords = ["体調良好", "体調普通", "良好", "普通", "元気"]
    if t in short_keywords:
        return True

    return len(t) <= 12


def _rebuild_user_state_from_existing(before_user: str, before_staff: str, work_label: str) -> str:
    user_text = str(before_user or "").replace("\u3000", " ").strip()
    staff_text = str(before_staff or "").replace("\u3000", " ").strip()
    work = str(work_label or "").strip() or "作業"
    unit = _work_default_unit(work)

    if not _is_short_user_state(user_text):
        return user_text

    text = staff_text
    start_sentence = ""
    if "作業開始" in text or "開始の連絡" in text or "電話連絡" in text:
        if "体調いい" in text or "体調はいい" in text or "体調良好" in text or "体調が良好" in text:
            start_sentence = "作業開始の連絡があり、体調は良好であると話されていました。"
        elif "元気" in text:
            start_sentence = "作業開始の連絡があり、元気そうなご様子で話されていました。"
        else:
            start_sentence = "作業開始の連絡があり、体調について報告がありました。"

    amount_sentence = ""

    m = re.search(rf"{re.escape(work)}を\s*(\d+)\s*({unit})", text)
    if m:
        amount_sentence = f"作業終了の連絡があり、{work}を{m.group(1)}{m.group(2)}やりましたと報告がありました。"

    if not amount_sentence:
        m2 = re.search(r"(\d+)\s*(枚|個|膳|本|羽)\s*(やりました|出来ました|できました|実施)", text)
        if m2:
            amount_sentence = f"作業終了の連絡があり、{work}を{m2.group(1)}{m2.group(2)}やりましたと報告がありました。"

    if not amount_sentence and ("8割" in text or "半分" in text or "少し" in text or "ちょっと" in text):
        amount_sentence = f"作業終了の連絡があり、{work}を1{unit}やりましたと報告がありました。"

    if not amount_sentence and ("終了の連絡" in text or "作業終了" in text or "やりました" in text or "出来ました" in text or "できました" in text):
        amount_sentence = f"作業終了の連絡があり、{work}に取り組まれたことを報告されました。"

    rebuilt = " ".join([s for s in [start_sentence, amount_sentence] if s]).strip()
    return rebuilt or user_text


def _split_support_record_blocks(page_text: str):
    text = _normalize_text(page_text)
    if not text:
        return {}

    day_pat = re.compile(r"(?m)^(\d{1,2}日（[^）]+）)")
    matches = list(day_pat.finditer(text))
    out = {}

    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        block = text[start:end].strip()
        day_label = m.group(1)
        day_m = re.match(r"(\d{1,2})日", day_label)
        if not day_m:
            continue
        day = int(day_m.group(1))

        mm_work = re.search(r"(?ms)^作業\n(.*?)(?=^利用者状態\n|^職員考察\n|^面談\n|^その他\n|\Z)", block)
        work = mm_work.group(1).strip() if mm_work else ""

        mm_user = re.search(r"(?ms)^利用者状態\n(.*?)(?=^職員考察\n|^面談\n|^その他\n|\Z)", block)
        user_state_raw = mm_user.group(1).strip() if mm_user else ""

        mm_staff = re.search(r"(?ms)^職員考察\n(.*?)(?=^面談\n|^その他\n|\Z)", block)
        staff_note_raw = mm_staff.group(1).strip() if mm_staff else ""

        out[day] = {
            "date_label": day_label,
            "work": work,
            "user_state_raw": user_state_raw,
            "staff_note_raw": staff_note_raw,
            "all_text": block,
        }

    return out


def _normalize_work_quantity_phrase(text: str, work: str):
    s = _normalize_text(text)
    if not s or not work:
        return s

    s = s.replace(f"{work}を{work}を", f"{work}を")
    s = s.replace(f"{work}作業を{work}を", f"{work}を")
    s = s.replace(f"{work}を{work}", f"{work}")

    qty_pat = r"(\d+\s*(?:枚|個|膳|本|羽)|[一二三四五六七八九十]+\s*(?:枚|個|膳|本|羽))"
    vague_pat = r"([0-9０-９]+割(?:程度)?|半分(?:くらい)?|少し)"

    replacements = [
        (rf"(?<!{re.escape(work)}を){qty_pat}やりました", rf"{work}を\1やりました"),
        (rf"(?<!{re.escape(work)}を){qty_pat}できました", rf"{work}を\1できました"),
        (rf"(?<!{re.escape(work)}を){qty_pat}出来ました", rf"{work}を\1出来ました"),
        (rf"(?<!{re.escape(work)}を){qty_pat}仕上げました", rf"{work}を\1仕上げました"),
        (rf"(?<!{re.escape(work)}を){qty_pat}完成(?:させました)?", rf"{work}を\1完成させました"),
        (rf"(?<!{re.escape(work)}を){qty_pat}です", rf"{work}を\1です"),
        (rf"(?<!{re.escape(work)}を){vague_pat}やりました", rf"{work}を\1やりました"),
        (rf"(?<!{re.escape(work)}を){vague_pat}できました", rf"{work}を\1できました"),
        (rf"(?<!{re.escape(work)}を){vague_pat}出来ました", rf"{work}を\1出来ました"),
    ]

    for pat, rep in replacements:
        s = re.sub(pat, rep, s)

    while f"{work}を{work}を" in s:
        s = s.replace(f"{work}を{work}を", f"{work}を")

    return s


def _convert_ambiguous_quantity_to_one_or_more(text: str, work: str, allow_zero: bool):
    s = _normalize_text(text)
    if not s or allow_zero or not work:
        return s

    unit = _work_default_unit(work)
    patterns = [
        (rf"{re.escape(work)}を([0-9０-９]+割(?:程度)?|半分(?:くらい)?|少し)やりました", f"{work}を1{unit}やりました"),
        (rf"{re.escape(work)}を([0-9０-９]+割(?:程度)?|半分(?:くらい)?|少し)できました", f"{work}を1{unit}できました"),
        (rf"{re.escape(work)}を([0-9０-９]+割(?:程度)?|半分(?:くらい)?|少し)出来ました", f"{work}を1{unit}出来ました"),
        (rf"{re.escape(work)}を([0-9０-９]+割(?:程度)?|半分(?:くらい)?|少し)です", f"{work}を1{unit}です"),
        (r"「([0-9０-９]+割(?:程度)?|半分(?:くらい)?|少し)やりました", f"「{work}を1{unit}やりました"),
        (r"「([0-9０-９]+割(?:程度)?|半分(?:くらい)?|少し)できました", f"「{work}を1{unit}できました"),
        (r"「([0-9０-９]+割(?:程度)?|半分(?:くらい)?|少し)出来ました", f"「{work}を1{unit}出来ました"),
    ]
    for pat, rep in patterns:
        s = re.sub(pat, rep, s)
    return s


def _has_explicit_quantity(text: str):
    s = _normalize_text(text)
    return bool(re.search(r"(?:\d+\s*(?:枚|個|膳|本|羽)|[一二三四五六七八九十]+\s*(?:枚|個|膳|本|羽))", s))


def _append_default_quantity_if_missing(text: str, work: str, allow_zero: bool):
    """
    最重要ルール:
    - 元文や生成文に数量があるなら絶対に触らない
    - 数量補完は数量がない場合のみ
    - 塗り絵は数量なしなら1枚固定
    - 複合作業は数量補完しない
    - 観葉植物など数量が不自然なものには補完しない
    """
    s = _normalize_text(text)
    w = _normalize_text(work)

    if not s or not w or allow_zero:
        return s

    # 既に数量がある場合は絶対に触らない
    if _has_explicit_quantity(s):
        return s

    work_items = _split_work_items(w)

    # 複合作業は数量補完しない
    if len(work_items) != 1:
        return s

    single = work_items[0]

    # 塗り絵は数量なしなら1枚固定
    if "塗り絵" in single:
        if re.search(r"作業|やりました|できました|出来ました|実施|仕上げ|完成|順調に|取り組", s):
            return s + " 塗り絵を1枚実施されました。"
        return s

    # 数量が不自然な作業には補完しない
    if (not _is_quantifiable_work(single)) or ("清掃" in single):
        return s

    if re.search(r"作業|やりました|できました|出来ました|実施|仕上げ|完成|順調に|取り組", s):
        unit = _work_default_unit(single)
        return s + f" {single}を1{unit}実施されました。"

    return s


def _compose_user_state_from_raw(work: str, raw_user: str, raw_staff: str):
    raw_user = _normalize_text(raw_user)
    raw_staff = _normalize_text(raw_staff)
    source = " ".join([x for x in [raw_user, raw_staff] if x]).strip()
    if not source:
        return raw_user or raw_staff or ""

    unit = _work_default_unit(work)
    allow_zero = _contains_explicit_no_work_reason(source)
    sentences = []

    if re.search(r"作業開始|開始の連絡|電話連絡", source):
        if re.search(r"体調[^。]*?(良好|いい|元気)", source):
            sentences.append("作業開始の連絡があり、体調は良好であると話されていました。")
        elif re.search(r"体調[^。]*?(普通|まあまあ|まぁまぁ|大丈夫)", source):
            m = re.search(r"(体調[^。]*?(?:普通|まあまあ|まぁまぁ|大丈夫)[^。]*)", source)
            if m:
                sentences.append("作業開始の連絡があり、" + m.group(1).rstrip("。") + "。")
            else:
                sentences.append("作業開始の連絡がありました。")
        elif re.search(r"元気", source):
            sentences.append("作業開始の連絡があり、元気そうなご様子で話されていました。")
        else:
            sentences.append("作業開始の連絡がありました。")
    elif raw_user and _looks_like_short_health_only(raw_user):
        sentences.append(raw_user.rstrip("。") + "です。")

    q = re.search(r"「([^」]{1,80})」", source)
    if q:
        quote = q.group(1).strip()
        quote = _normalize_work_quantity_phrase(quote, work)
        quote = _convert_ambiguous_quantity_to_one_or_more(quote, work, allow_zero)
        if quote:
            sentences.append(f"作業終了の連絡があり、「{quote}」と報告がありました。")
    else:
        qty = re.search(rf"{re.escape(work)}[^。]*?(\d+\s*(?:枚|個|膳|本|羽)|[一二三四五六七八九十]+\s*(?:枚|個|膳|本|羽))[^。]*", source) if work else None
        if qty:
            sentences.append(qty.group(0).rstrip("。") + "。")
        elif allow_zero:
            sentences.append(f"作業終了時には、{work}は実施できなかったとの報告がありました。")
        else:
            if re.search(r"8割|半分|少し|やりました|できました|出来ました|順調に", source):
                sentences.append(f"作業終了の連絡があり、{work}を1{unit}やりましたと報告がありました。")

    result = " ".join(sentences).strip()
    result = _normalize_work_quantity_phrase(result, work)
    result = _convert_ambiguous_quantity_to_one_or_more(result, work, allow_zero)
    result = _append_default_quantity_if_missing(result, work, allow_zero)
    return result.strip()

def _extract_sentence_by_keywords(text: str, keywords):
    for s in _sentencize_jp(text):
        if any(k in s for k in keywords):
            return s.rstrip("。") + "。"
    return ""

def _split_lines_keep_order(text: str):
    s = _normalize_text(text)
    if not s:
        return []
    lines = []
    for line in re.split(r'[\n]+', s):
        line = line.strip()
        if line:
            lines.append(line)
    return lines


def _pick_first_matching_line(text: str, keywords):
    for line in _split_lines_keep_order(text):
        if any(k in line for k in keywords):
            return line.rstrip("。") + "。"
    return ""


def _pick_all_matching_lines(text: str, keywords, max_count=3):
    out = []
    for line in _split_lines_keep_order(text):
        if any(k in line for k in keywords):
            line2 = line.rstrip("。") + "。"
            if line2 not in out:
                out.append(line2)
        if len(out) >= max_count:
            break
    return out


def _clean_quote_style(text: str) -> str:
    s = _normalize_text(text)
    s = s.replace("『", "「").replace("』", "」")
    s = re.sub(r'「\s+', '「', s)
    s = re.sub(r'\s+」', '」', s)
    s = s.replace("？。", "？").replace("。 」", "。」")
    return s


def _lighten_journal_tone(text: str) -> str:
    s = _normalize_text(text)
    replacements = {
        "してまいります": "していきます",
        "支援してまいります": "支援していきます",
        "継続してまいります": "続けていきます",
        "見守ってまいります": "見守っていきます",
        "努めていきます": "心掛けていきます",
        "見受けられました": "見られました",
        "伺えました": "見られました",
        "考えられます": "感じられます",
        "されていました": "していました",
        "おられました": "いました",
        "ございます": "あります",
        "ございました": "ありました",
    }
    for k, v in replacements.items():
        s = s.replace(k, v)
    s = s.replace("ご本人", "本人")
    return _clean_quote_style(s)


def _mode_opening_phrase(mode: str, source: str) -> str:
    src = _normalize_text(source)

    if mode == "施設外":
        for cand in [
            _pick_first_matching_line(src, ["現場到着", "到着後", "開始前", "作業前", "挨拶"]),
            _pick_first_matching_line(src, ["体調確認", "体調について確認", "確認を行うと"]),
        ]:
            if cand:
                return cand

        health = _pick_first_matching_line(src, ["体調", "精神", "元気", "しんどい", "不安定", "安定"])
        if health:
            return health
        return ""

    if mode == "通所":
        for cand in [
            _pick_first_matching_line(src, ["来所時", "来所後", "開始前", "体調確認"]),
            _pick_first_matching_line(src, ["体調について確認", "体調確認を行うと"]),
        ]:
            if cand:
                return cand
        return ""

    # 在宅
    for cand in [
        _pick_first_matching_line(src, ["作業開始前に連絡", "開始時の連絡", "作業開始時に", "開始前に連絡"]),
        _pick_first_matching_line(src, ["体調について確認", "体調確認を行った", "体調確認"]),
    ]:
        if cand:
            return cand
    return ""


def _mode_closing_phrase(mode: str, source: str, work_label: str) -> str:
    src = _normalize_text(source)

    for cand in [
        _pick_first_matching_line(src, ["作業終了時に連絡", "終了時に連絡", "終了連絡", "作業終了時には", "作業後には"]),
        _pick_first_matching_line(src, ["報告があった", "報告がありました", "報告されました"]),
    ]:
        if cand:
            return cand

    qty = _estimate_quantity_phrase(work_label, src)
    if qty:
        if mode == "在宅":
            return f"作業終了時に連絡があり、{qty}行ったとの報告がありました。"
        return f"作業後には、{qty}取り組んだことを報告されました。"

    return ""


def _mode_work_lines(mode: str, source: str, work_label: str):
    src = _normalize_text(source)
    lines = []

    # rawに具体作業があるなら優先
    work_hits = _pick_all_matching_lines(
        src,
        ["作業", "水やり", "塗り絵", "内職", "清掃", "掃き掃除", "モップ", "手すり", "ゴミ拾い", "消火器", "配電盤", "検品", "袋詰め", "封入", "チラシ", "折り鶴", "箱折り", "ラベル貼り", "仕分け"],
        max_count=3
    )
    for w in work_hits:
        if w not in lines:
            lines.append(w)

    # 何も拾えないときだけ work_label を使う
    if not lines and _normalize_text(work_label):
        if mode == "施設外" and "清掃" in work_label:
            lines.append("廊下の掃き掃除や手すり拭きなどの清掃作業に取り組みました。")
        else:
            lines.append(f"{_normalize_text(work_label)}に取り組みました。")

    return lines


def _mode_staff_support_sentence(mode: str, merged: str) -> str:
    src = _normalize_text(merged)

    if any(k in src for k in ["しんどい", "不安定", "優れない", "だるい", "頭痛", "眠れ", "めまい", "痛み"]):
        return "体調や気分の変化を確認しながら、無理のない範囲で続けられるよう支援していきます。"

    if any(k in src for k in ["意欲", "積極", "頑張", "前向き", "責任感"]):
        return "取り組みやすい声掛けを続けながら、安定して作業できるよう支援していきます。"

    if mode == "施設外":
        return "その日の体調や作業の様子を確認しながら、無理なく続けられるよう支援していきます。"

    return "その日の状態を確認しながら、無理なく続けられるよう支援していきます。"

def _dedupe_sentences(text: str) -> str:
    seen = set()
    result = []
    for s in _sentencize_jp(text):
        key = re.sub(r"\s+", "", s.rstrip("。"))
        if key and key not in seen:
            seen.add(key)
            result.append(s.rstrip("。") + "。")
    return " ".join(result).strip()

def _strip_unwanted_words(text: str) -> str:
    s = _normalize_text(text)

    # 時刻は不要
    s = re.sub(r'\b\d{1,2}:\d{2}\b', '', s)

    # 在宅という語は不要
    s = s.replace("在宅にて、", "")
    s = s.replace("在宅で", "")
    s = s.replace("在宅作業開始の電話で", "")
    s = s.replace("在宅", "")

    # 不自然な敬語・硬すぎる表現を軽くする
    replacements = {
        "してまいります": "していきます",
        "支援してまいります": "支援していきます",
        "継続してまいります": "続けていきます",
        "見守ってまいります": "見守っていきます",
        "伺えました": "見られました",
        "見受けられました": "見られました",
        "判断いたします": "考えられます",
        "評価いたします": "感じられました",
        "取り組まれていました": "取り組んでいました",
        "されておられました": "されていました",
        "ございました": "ありました",
        "ございます": "あります",
    }
    for k, v in replacements.items():
        s = s.replace(k, v)

    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _extract_duration_minutes(text: str) -> int:
    s = _normalize_text(text)
    m = re.search(r'(\d{1,2}):(\d{2})\s*〜\s*(\d{1,2}):(\d{2})', s)
    if not m:
        return 0
    sh, sm, eh, em = map(int, m.groups())
    start = sh * 60 + sm
    end = eh * 60 + em
    if end <= start:
        return 0
    return end - start


def _estimate_quantity_phrase(work_label: str, row_text: str) -> str:
    work = _normalize_text(work_label)
    mins = _extract_duration_minutes(row_text)

    if not work or mins <= 0:
        return ""

    # 清掃・観葉植物は数量化しない
    if "清掃" in work or "観葉植物" in work:
        return ""

    # 塗り絵は固定
    if "塗り絵" in work:
        return "塗り絵を1枚"

    # 割り箸・箸入れ系は 2分で1膳
    if "箸" in work or "箸入れ" in work or "お箸" in work:
        qty = max(1, mins // 2)
        return f"{work}を{qty}膳"

    # チラシ・袋・箱・コースター・折り鶴は保守的に推定
    if "チラシ" in work:
        qty = max(1, mins // 5)
        return f"{work}を{qty}枚"
    if "コースター" in work:
        qty = max(1, mins // 10)
        return f"{work}を{qty}枚"
    if "袋" in work or "箱" in work:
        qty = max(1, mins // 5)
        return f"{work}を{qty}個"
    if "折り鶴" in work or "鶴" in work:
        qty = max(1, mins // 10)
        return f"{work}を{qty}羽"

    return ""

def _split_work_items(work_label: str):
    work = _normalize_text(work_label)
    if not work:
        return []
    parts = re.split(r"[、,／/・]+", work)
    return [p.strip() for p in parts if p.strip()]


def _is_quantifiable_work(work_name: str) -> bool:
    w = _normalize_text(work_name)
    quantifiable_keywords = [
        "塗り絵", "チラシ", "箸", "箸入れ", "お箸",
        "折り鶴", "箱", "袋", "コースター"
    ]
    return any(k in w for k in quantifiable_keywords)


def _pick_result_work(work_label: str, source: str) -> str:
    """
    数量を紐づける作業を1つだけ選ぶ
    - 明示的に数量つきで出ている作業を最優先
    - 複合作業で数量が不明なら、無理に「1枚/1個」を付けない
    """
    works = _split_work_items(work_label)
    src = _normalize_text(source)

    if not works:
        return ""

    # 明示的に「作業名 + 数量」があるものを最優先
    for w in works:
        if re.search(rf"{re.escape(w)}[^。]*?(\d+\s*(?:枚|個|膳|本|羽))", src):
            return w

    # 明示的な数量がなくても、発言や文脈で最も自然なもの
    preferred = ["塗り絵", "チラシ", "箸入れ", "お箸", "折り鶴", "箱", "袋", "コースター"]
    for p in preferred:
        for w in works:
            if p in w and p in src:
                return w

    # 単一作業ならそれを返す
    if len(works) == 1:
        return works[0]

    # 複合作業で明確な根拠がないときは空にして無理な数量補完を避ける
    return ""


def _build_work_result_phrase(work_label: str, *texts) -> str:
    """
    最重要ルール:
    1. 明示数量がある場合は絶対それを使う
    2. 数量補完は数量がない場合のみ
    3. 塗り絵は数量なしなら1日1枚固定
    4. 観葉植物など数量が不自然なものには付けない
    """
    work = _normalize_text(work_label) or "作業"
    source = " ".join([_normalize_text(t) for t in texts if _normalize_text(t)])
    works = _split_work_items(work)

    result_work = _pick_result_work(work, source)

    # 1. 明示数量がある場合は絶対それを使う
    if result_work:
        m = re.search(rf"{re.escape(result_work)}[^。]*?(\d+\s*(?:枚|個|膳|本|羽))", source)
        if m:
            if not _is_quantifiable_work(result_work):
                return f"{result_work}の作業に取り組まれました"
            return f"{result_work}を{m.group(1)}"

    # 2. ここから下は「数量がない場合のみ」の補完
    if result_work:
        # 塗り絵は数量なしなら1日1枚固定
        if "塗り絵" in result_work:
            return "塗り絵を1枚"

        # あいまい数量は無理に数字にしない
        if any(k in source for k in ["8割", "半分", "少し", "ちょっと"]):
            return f"{result_work}に取り組まれました"

        # 単一作業かつ数量化が自然なものだけ補完
        if len(works) == 1 and _is_quantifiable_work(result_work):
            return f"{result_work}を1{_work_default_unit(result_work)}"

        return f"{result_work}に取り組まれました"

    # 複合作業で特定不能な場合は数量を付けない
    if len(works) >= 2:
        natural = [w for w in works if _is_quantifiable_work(w)]
        if natural:
            return f"{'や'.join(works[:2])}などの作業に取り組まれました"
        return f"{work}の作業に取り組まれました"

    # 単一作業
    if len(works) == 1:
        single = works[0]

        # 塗り絵は数量なしなら1枚固定
        if "塗り絵" in single:
            return "塗り絵を1枚"

        # 数量が不自然なものは数量なし
        if not _is_quantifiable_work(single):
            return f"{single}の作業に取り組まれました"

        return f"{single}を1{_work_default_unit(single)}"

    return "作業に取り組まれました"

def _facility_cleaning_detail_phrase(text: str) -> str:
    s = _normalize_text(text)

    # 原文に具体作業があるならそれを優先
    if any(k in s for k in ["廊下", "モップ", "手すり", "ゴミ拾い", "消火器", "配電盤"]):
        parts = []
        if "廊下" in s or "ほうき" in s:
            parts.append("廊下の掃き掃除")
        if "モップ" in s:
            parts.append("モップ掛け")
        if "手すり" in s:
            parts.append("手すり拭き")
        if "ゴミ拾い" in s:
            parts.append("マンション周囲のゴミ拾い")
        if "消火器" in s or "配電盤" in s:
            parts.append("消火器や配電盤の拭き掃除")

        parts = list(dict.fromkeys(parts))
        if parts:
            if len(parts) == 1:
                return parts[0]
            if len(parts) == 2:
                return f"{parts[0]}や{parts[1]}"
            return "、".join(parts[:-1]) + f"や{parts[-1]}"

    # 原文に細目がなくても、施設外清掃の定型を軽く補う
    return "廊下の掃き掃除や手すり拭きなどの清掃作業"

def _force_diamond_user_state(
    user_state: str,
    before_user: str,
    before_staff: str,
    work_label: str,
    mode: str = "",
    row_text: str = "",
) -> str:
    """
    利用者状態は「作文」ではなく、rawの事実を時系列で並べる。
    優先順:
    1. モード判定（在宅 / 通所 / 施設外）
    2. raw の事実
    3. 作業名
    4. 数量（明示優先、なければ推定）
    """
    src_user = _normalize_text(user_state)
    src_before_user = _normalize_text(before_user)
    src_before_staff = _normalize_text(before_staff)
    src_row = _normalize_text(row_text)

    merged = " ".join([x for x in [src_user, src_before_user, src_before_staff, src_row] if x]).strip()
    merged = _lighten_journal_tone(merged)

    opening = _mode_opening_phrase(mode, merged)

    # 本人発言・体調
    personal_lines = _pick_all_matching_lines(
        merged,
        ["本人より", "話があった", "話されていた", "とのことだった", "とのことでした", "元気", "普通", "不安定", "安定", "しんどい", "眠れ", "だるい", "痛い", "体調", "精神"],
        max_count=3
    )

    # 作業内容
    work_lines = _mode_work_lines(mode, merged, work_label)

    # 数量
    qty_line = ""
    explicit_qty_line = _pick_first_matching_line(
        merged,
        ["膳", "枚", "個", "本", "羽", "通", "袋", "仕上げた", "終わった", "やった", "作成", "完成"]
    )
    if explicit_qty_line:
        qty_line = explicit_qty_line
    else:
        qty = _estimate_quantity_phrase(work_label, src_row)
        if qty and not ("清掃" in _normalize_text(work_label) or "観葉植物" in _normalize_text(work_label)):
            qty_line = f"{qty}実施されました。"

    # 終了
    closing = _mode_closing_phrase(mode, merged, work_label)

    parts = []

    if opening:
        parts.append(opening)

    for line in personal_lines:
        if line not in parts:
            parts.append(line)

    for line in work_lines:
        if line not in parts:
            parts.append(line)

    if qty_line and qty_line not in parts:
        parts.append(qty_line)

    if closing and closing not in parts:
        parts.append(closing)

    # 4大要素の最低保証
    if not parts:
        health = _extract_sentence_by_keywords(
            merged,
            ["体調", "精神", "元気", "不安定", "安定", "普通", "しんどい"]
        )
        if health:
            parts.append(health)

        if _normalize_text(work_label):
            if mode == "施設外" and "清掃" in work_label:
                parts.append("廊下の掃き掃除や手すり拭きなどの清掃作業に取り組みました。")
            else:
                parts.append(f"{_normalize_text(work_label)}に取り組みました。")

        if qty_line:
            parts.append(qty_line)

    result = " ".join([p for p in parts if _normalize_text(p)])
    result = _normalize_work_quantity_phrase(result, _normalize_text(work_label))

    allow_zero = _contains_explicit_no_work_reason(merged)
    result = _convert_ambiguous_quantity_to_one_or_more(result, _normalize_text(work_label), allow_zero)

    # 清掃・観葉植物は数量にしない
    if "清掃" in _normalize_text(work_label):
        result = re.sub(r'清掃を\d+(?:枚|個|膳|本|羽|通|袋)実施されました。?', '清掃に取り組みました。', result)
        result = re.sub(r'清掃を\d+(?:枚|個|膳|本|羽|通|袋)行ったとの報告がありました。?', '清掃に取り組んだことを報告されました。', result)

    result = result.replace("清掃作業作業", "清掃作業")
    result = _lighten_journal_tone(result)
    result = _strip_unwanted_words(result)
    result = _dedupe_sentences(result)
    return result.strip()


def _force_diamond_staff_note(staff_note: str, before_staff: str) -> str:
    """
    職員考察は
    1. 事実に基づく短い評価
    2. 今後どう支援するか
    の2段構成で作る
    """
    out = _lighten_journal_tone(_normalize_text(staff_note))
    raw = _lighten_journal_tone(_normalize_text(before_staff))
    merged = " ".join([x for x in [out, raw] if x]).strip()

    # 1文目: 事実評価
    eval_line = _pick_first_matching_line(
        merged,
        [
            "作業量", "集中", "体調", "精神", "不安定", "安定", "意欲", "積極",
            "無理をせず", "継続", "自己調整", "報告", "具体的", "丁寧", "責任感",
            "だるい", "痛み", "眠れ", "疲れ", "しんどい"
        ]
    )

    if not eval_line:
        if any(k in merged for k in ["不安定", "しんどい", "疲れ", "だるい", "眠れ", "痛み"]):
            eval_line = "体調や気分に波はあったが、無理のない範囲で作業に取り組めていました。"
        elif any(k in merged for k in ["意欲", "積極", "前向き", "責任感"]):
            eval_line = "その日の状態に合わせながら、前向きに取り組めていました。"
        else:
            eval_line = "その日の状態に応じて、無理なく作業を進められていました。"

    # 2文目: 支援方針
    support_line = _pick_first_matching_line(
        merged,
        ["支援", "声掛け", "お伝え", "配慮", "継続", "確認", "促し", "無理のない", "見守り"]
    )
    if not support_line:
        support_line = _mode_staff_support_sentence("", merged)

    result = _dedupe_sentences(_lighten_journal_tone(eval_line + " " + support_line))
    return result


def _update_live_status(live_status_box, text: str, level: str = "info"):
    if live_status_box is None:
        return

    text = str(text or "").strip()
    if not text:
        return

    if level == "success":
        live_status_box.success(text)
    elif level == "error":
        live_status_box.error(text)
    elif level == "warning":
        live_status_box.warning(text)
    else:
        live_status_box.info(text)

def _detect_service_mode(row_text: str, work_text: str = "", user_text: str = "", staff_text: str = "") -> str:
    """
    自動判定ルール
    1. 施設外と明記 → 施設外
    2. 通所 + 食事あり → 通所（実来所）
    3. 通所のみで食事あり等なし → 在宅
    4. 在宅明記 → 在宅
    5. 補助判定
    """
    row_src = _normalize_text(row_text)
    work_src = _normalize_text(work_text)
    user_src = _normalize_text(user_text)
    staff_src = _normalize_text(staff_text)
    src = " ".join([row_src, work_src, user_src, staff_src])

    outside_keywords = ["施設外", "施設外就労", "施設外支援", "居酒屋", "琴", "エバーグリーン", "清掃"]
    if any(k in src for k in outside_keywords):
        return "施設外"

    if "在宅" in src:
        return "在宅"

    has_day_service = "通所" in src
    has_meal = any(k in src for k in ["食事あり", "昼食あり", "昼食提供", "食事提供"])

    if has_day_service and has_meal:
        return "通所"

    if has_day_service and not has_meal:
        return "在宅"

    if any(k in src for k in ["来所", "来られ", "来室", "来訪"]):
        return "通所"

    return "在宅"


def _apply_mode_prefix_to_user_state(mode: str, user_state: str) -> str:
    text = _normalize_text(user_state)
    if not text:
        return text

    text = _strip_unwanted_words(text)

    # 会社名・店名の直接表現は除去
    text = text.replace("合同会社エバーグリーン", "")
    text = text.replace("居酒屋 琴", "")
    text = text.replace("居酒屋琴", "")

    # 施設外の禁忌表現
    forbidden_patterns = [
        r'作業開始の連絡がありましたが、',
        r'作業開始の連絡がありました。',
        r'作業開始の連絡があり、',
        r'作業開始時には、',
        r'作業終了の連絡がありましたが、',
        r'作業終了の連絡がありました。',
        r'作業終了の連絡があり、',
        r'作業終了時には、',
        r'施設外就労として、',
        r'施設外就労先にて、',
        r'施設外にて、',
        r'にて施設外就労の',
    ]
    if mode == "施設外":
        for pat in forbidden_patterns:
            text = re.sub(pat, '', text)

    # 在宅は「在宅」という語を出さない
    if mode == "在宅":
        text = text.replace("在宅", "")

    text = re.sub(r'\s+', ' ', text).strip()
    return _dedupe_sentences(text)


def _apply_mode_prefix_to_staff_note(mode: str, staff_note: str) -> str:
    text = _normalize_text(staff_note)
    if not text:
        return text

    # 施設外は接頭辞を付けない
    if mode == "施設外":
        text = text.replace("合同会社エバーグリーン", "")
        text = text.replace("居酒屋 琴", "")
        text = text.replace("居酒屋琴", "")
        text = re.sub(r'^施設外での作業状況を踏まえ、', '', text)
        text = re.sub(r'^施設外就労先での状況を踏まえ、', '', text)
        return _dedupe_sentences(text)

    # 在宅だけ軽く補正
    if mode == "在宅":
        return _dedupe_sentences(text)

    return _dedupe_sentences(text)

def _enforce_mode_phrasing(mode: str, user_state: str, staff_note: str):
    user = _normalize_text(user_state)
    staff = _normalize_text(staff_note)

    # 共通で時刻は消す
    user = re.sub(r'\b\d{1,2}:\d{2}\b', '', user)
    staff = re.sub(r'\b\d{1,2}:\d{2}\b', '', staff)

    if mode == "在宅":
        # 来所・施設外は禁止
        forbidden_user = [
            r'時間通りに来所[^。]*。?',
            r'来所時[^。]*。?',
            r'来所され[^。]*。?',
            r'通所され[^。]*。?',
            r'施設外就労[^。]*。?',
            r'施設外支援[^。]*。?',
        ]
        for pat in forbidden_user:
            user = re.sub(pat, '', user)

        forbidden_staff = [
            r'来所時[^。]*。?',
            r'来所され[^。]*。?',
            r'施設外就労[^。]*。?',
            r'施設外支援[^。]*。?',
        ]
        for pat in forbidden_staff:
            staff = re.sub(pat, '', staff)

    elif mode == "通所":
        # 在宅・電話開始は禁止
        forbidden_user = [
            r'在宅[^。]*。?',
            r'作業開始の電話があり[^。]*。?',
            r'電話で[^。]*作業開始[^。]*。?',
            r'施設外就労[^。]*。?',
            r'施設外支援[^。]*。?',
        ]
        for pat in forbidden_user:
            user = re.sub(pat, '', user)

        forbidden_staff = [
            r'在宅[^。]*。?',
            r'施設外就労[^。]*。?',
            r'施設外支援[^。]*。?',
        ]
        for pat in forbidden_staff:
            staff = re.sub(pat, '', staff)

    elif mode == "施設外":
        # 電話開始・来所は禁止
        forbidden_user = [
            r'作業開始の電話があり[^。]*。?',
            r'作業開始の連絡があり[^。]*。?',
            r'来所時[^。]*。?',
            r'来所され[^。]*。?',
            r'時間通りに来所[^。]*。?',
            r'在宅[^。]*。?',
        ]
        for pat in forbidden_user:
            user = re.sub(pat, '', user)

        forbidden_staff = [
            r'来所時[^。]*。?',
            r'来所され[^。]*。?',
            r'在宅[^。]*。?',
        ]
        for pat in forbidden_staff:
            staff = re.sub(pat, '', staff)

    user = _dedupe_sentences(re.sub(r'\s+', ' ', user).strip())
    staff = _dedupe_sentences(re.sub(r'\s+', ' ', staff).strip())
    return user, staff

def _postprocess_gemini_result(page_text: str, result_json: dict, year: int, month: int, outside_workplace: str = ""):
    blocks = _split_support_record_blocks(page_text)
    fixed = {}

    for date_str, content in (result_json or {}).items():
        m = re.search(r"\d{4}-\d{2}-(\d{1,2})", str(date_str))
        if not m:
            continue

        day = int(m.group(1))
        block = blocks.get(day, {})

        work = _normalize_text(block.get("work", ""))
        raw_user = _normalize_text(block.get("user_state_raw", ""))
        raw_staff = _normalize_text(block.get("staff_note_raw", ""))

        user_state = _normalize_text((content or {}).get("user_state", ""))
        staff_note = _normalize_text((content or {}).get("staff_note", ""))

        all_text = " ".join([work, raw_user, raw_staff])
        is_outside_day = any(k in all_text for k in [
            "施設外就労", "清掃", "居酒屋", "琴", "エバーグリーン"
        ])

        source_all = " ".join([user_state, staff_note, raw_user, raw_staff]).strip()
        allow_zero = _contains_explicit_no_work_reason(source_all)

        if _looks_like_short_health_only(user_state) or len(_sentencize_jp(user_state)) < 2:
            rebuilt = _compose_user_state_from_raw(work, raw_user, raw_staff)
            if rebuilt:
                user_state = rebuilt

        user_state = _normalize_work_quantity_phrase(user_state, work)
        staff_note = _normalize_work_quantity_phrase(staff_note, work)

        user_state = _convert_ambiguous_quantity_to_one_or_more(user_state, work, allow_zero)
        staff_note = _convert_ambiguous_quantity_to_one_or_more(staff_note, work, allow_zero)

        user_state = _append_default_quantity_if_missing(user_state, work, allow_zero)

        if _looks_like_short_health_only(user_state) or len(_sentencize_jp(user_state)) < 2:
            rebuilt = _compose_user_state_from_raw(work, raw_user, raw_staff)
            if rebuilt:
                user_state = rebuilt

        # 施設外の書き出し補正（会社名・施設外前置きは禁止）
        if is_outside_day:
            user_state = user_state.replace("合同会社エバーグリーン", "")
            user_state = user_state.replace("居酒屋 琴", "")
            user_state = user_state.replace("居酒屋琴", "")
            staff_note = staff_note.replace("合同会社エバーグリーン", "")
            staff_note = staff_note.replace("居酒屋 琴", "")
            staff_note = staff_note.replace("居酒屋琴", "")

            user_state = re.sub(r'^.*?での作業として、', '', user_state)
            staff_note = re.sub(r'^.*?での作業として、', '', staff_note)

            user_state = re.sub(r'^施設外就労として、', '', user_state)
            user_state = re.sub(r'^施設外就労先にて、', '', user_state)
            user_state = re.sub(r'^施設外にて、', '', user_state)

            forbidden_patterns = [
                r'作業開始の連絡がありましたが、',
                r'作業開始の連絡がありました。',
                r'作業開始の連絡があり、',
                r'作業開始時には、',
                r'作業終了の連絡がありましたが、',
                r'作業終了の連絡がありました。',
                r'作業終了の連絡があり、',
                r'作業終了時には、',
                r'施設外就労として、',
                r'施設外就労先にて、',
                r'施設外にて、',
                r'にて施設外就労の',
            ]
            for pat in forbidden_patterns:
                user_state = re.sub(pat, '', user_state)

            user_state = _dedupe_sentences(user_state)
            staff_note = _dedupe_sentences(staff_note)

        fixed[date_str] = {
            "user_state": user_state,
            "staff_note": staff_note,
        }

    for day, block in blocks.items():
        key = f"{year:04d}-{month:02d}-{day:02d}"
        if key in fixed:
            continue

        work = _normalize_text(block.get("work", ""))
        raw_user = _normalize_text(block.get("user_state_raw", ""))
        raw_staff = _normalize_text(block.get("staff_note_raw", ""))

        all_text = " ".join([work, raw_user, raw_staff])

        rebuilt_user = _compose_user_state_from_raw(work, raw_user, raw_staff)
        rebuilt_staff = raw_staff

        is_outside_day = any(k in all_text for k in [
            "施設外就労", "清掃", "居酒屋", "琴", "エバーグリーン"
        ])

        if is_outside_day:
            rebuilt_user = rebuilt_user.replace("合同会社エバーグリーン", "")
            rebuilt_user = rebuilt_user.replace("居酒屋 琴", "")
            rebuilt_user = rebuilt_user.replace("居酒屋琴", "")
            rebuilt_staff = rebuilt_staff.replace("合同会社エバーグリーン", "")
            rebuilt_staff = rebuilt_staff.replace("居酒屋 琴", "")
            rebuilt_staff = rebuilt_staff.replace("居酒屋琴", "")

            rebuilt_user = re.sub(r'^.*?での作業として、', '', rebuilt_user)
            rebuilt_staff = re.sub(r'^.*?での作業として、', '', rebuilt_staff)

        fixed[key] = {
            "user_state": rebuilt_user,
            "staff_note": rebuilt_staff,
        }

    return fixed

# =========================================
# 1ヶ月処理
# =========================================
def process_one_month(driver, resident_name, year, month, exec_id, user, company, outside_workplace="", live_status_box=None):
    from run_assistance import (
        goto_support_record_month,
        fetch_support_record_page_text,
        enter_edit_mode,
        save_all,
    )

    ym = f"{year}-{month:02d}"

    try:
        ok = goto_support_record_month(driver, year, month)
        if not ok:
            print("[JR] 月移動失敗")
            return

        time.sleep(2)

        page_text = fetch_support_record_page_text(driver)
        page_text_str = str(page_text or "").strip()

        if not page_text_str:
            print("[JR] 日誌なし")
            return

        result_json = generate_json_with_gemini_local(page_text_str, outside_workplace)
        result_json = _postprocess_gemini_result(page_text_str, result_json, year, month, outside_workplace)

        print("[FIX] 編集モードへ", flush=True)
        enter_edit_mode(driver)

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        success_count = 0

        for date_str in sorted(result_json.keys()):
            content = result_json[date_str]
            try:
                m = re.search(r"\d{4}-\d{2}-(\d{1,2})", date_str)
                if not m:
                    continue

                target_day = int(m.group(1))
                target_label = f"{target_day}日"

                user_state = content.get("user_state", "")
                staff_note = content.get("staff_note", "")

                if not user_state and not staff_note:
                    continue

                print(f"[FIX] 入力対象: {target_label}", flush=True)

                for row in rows:
                    row_text_full = row.text.strip()
                    if re.match(rf"^{target_day}日（", row_text_full):
                        areas = _find_row_textareas_for_support_record(row)
                        print(f"[FIX] {target_label} textarea count = {len(areas)}", flush=True)

                        if len(areas) < 2:
                            raise RuntimeError(f"textarea不足: {target_label}")

                        user_state_el = areas[0]
                        staff_note_el = areas[1]

                        before_user = _textarea_value(user_state_el)
                        before_staff = _textarea_value(staff_note_el)

                        print(f"[FIX] before user_state = {before_user[:80]}", flush=True)
                        print(f"[FIX] before staff_note = {before_staff[:80]}", flush=True)

                        row_text = row.text

                        mode = _detect_service_mode(
                            row_text=row.text,
                            work_text=row.text,
                            user_text=before_user,
                            staff_text=before_staff,
                        )

                        work_label = ""
                        if "塗り絵" in row_text:
                            work_label = "塗り絵"
                        elif "箱" in row_text:
                            work_label = "箱の組み立て"
                        elif "袋" in row_text:
                            work_label = "袋詰め"
                        elif "箸" in row_text:
                            work_label = "箸入れ"
                        elif "チラシ" in row_text:
                            work_label = "チラシ作業"
                        elif "折り鶴" in row_text or "鶴" in row_text:
                            work_label = "折り鶴"
                        else:
                            # row_text の「作業」欄が複合作業のときは、そのまま拾う
                            work_match = re.search(r"作業\s*(.+?)\s*利用者状態", row_text.replace("\n", " "), re.DOTALL)
                            if work_match:
                                work_label = _normalize_text(work_match.group(1))
                            else:
                                work_label = "作業"

                        final_user_state = _normalize_text(user_state)
                        final_staff_note = _normalize_text(staff_note)

                        # Gemini が弱いときだけ既存情報から補完する。
                        # 既存画面が短文でも、Gemini が長文を返しているなら Gemini を優先する。
                        gemini_is_weak = (
                            not final_user_state
                            or _looks_like_short_health_only(final_user_state)
                            or len(_sentencize_jp(final_user_state)) < 2
                        )

                        if gemini_is_weak:
                            rebuilt = _rebuild_user_state_from_existing(
                                before_user=before_user,
                                before_staff=before_staff,
                                work_label=work_label,
                            )

                            if rebuilt and (
                                not _looks_like_short_health_only(rebuilt)
                                and len(_sentencize_jp(rebuilt)) >= 2
                            ):
                                final_user_state = rebuilt
                            elif rebuilt and not final_user_state:
                                final_user_state = rebuilt

                        # ダイアモンドルールを強制
                        final_user_state = _force_diamond_user_state(
                            user_state=final_user_state,
                            before_user=before_user,
                            before_staff=before_staff,
                            work_label=work_label,
                            mode=mode,
                            row_text=row_text,
                        )
                        if mode == "施設外" and "清掃" in work_label:
                            cleaning_detail = _facility_cleaning_detail_phrase(
                                " ".join([row_text, before_user, before_staff, final_user_state, final_staff_note])
                            )
                            if cleaning_detail and cleaning_detail not in final_user_state:
                                if "清掃" in final_user_state:
                                    final_user_state = final_user_state.replace(
                                        "清掃",
                                        cleaning_detail,
                                        1
                                    )
                                else:
                                    final_user_state = _dedupe_sentences(
                                        final_user_state + f" {cleaning_detail}に取り組まれました。"
                                    )                        
                        final_staff_note = _force_diamond_staff_note(
                            staff_note=final_staff_note,
                            before_staff=before_staff,
                        )

                        # 在宅 / 通所 / 施設外 の自動判定を反映
                        final_user_state = _apply_mode_prefix_to_user_state(mode, final_user_state)
                        final_staff_note = _apply_mode_prefix_to_staff_note(mode, final_staff_note)

                        final_user_state, final_staff_note = _enforce_mode_phrasing(
                            mode, final_user_state, final_staff_note
                        )                        

                        # 作業名・数量の最終補正
                        allow_zero = _contains_explicit_no_work_reason(
                            " ".join([final_user_state, final_staff_note, before_user, before_staff])
                        )
                        final_user_state = _normalize_work_quantity_phrase(final_user_state, work_label)
                        final_user_state = _convert_ambiguous_quantity_to_one_or_more(
                            final_user_state, work_label, allow_zero
                        )
                        final_user_state = _append_default_quantity_if_missing(
                            final_user_state, work_label, allow_zero
                        )
                        
                    if mode == "施設外":
                        final_user_state = re.sub(r'^\s*にて', '', final_user_state)
                        final_user_state = re.sub(r'。+\s*にて', '。', final_user_state)
                        final_user_state = re.sub(r'施設外就労の\s*', '', final_user_state)

                        final_user_state = _dedupe_sentences(final_user_state)
                        final_staff_note = _dedupe_sentences(final_staff_note)                     

                        _set_react_textarea_value(driver, user_state_el, final_user_state)
                        _set_react_textarea_value(driver, staff_note_el, final_staff_note)

                        after_user = _textarea_value(user_state_el)
                        after_staff = _textarea_value(staff_note_el)

                        print(f"[FIX] after user_state = {after_user[:80]}", flush=True)
                        print(f"[FIX] after staff_note = {after_staff[:80]}", flush=True)

                        if after_user == str(final_user_state).strip() and after_staff == str(final_staff_note).strip():
                            success_count += 1
                            print(f"[FIX] 入力成功: {target_label}", flush=True)

                            _update_live_status(
                                live_status_box,
                                f"成功: {resident_name} / {year}-{month:02d} / {target_label} / {mode}",
                                "success"
                            )

                        else:
                            raise RuntimeError(
                                f"入力反映失敗: {target_label} / "
                                f"user_match={after_user == str(final_user_state).strip()} / "
                                f"staff_match={after_staff == str(final_staff_note).strip()}"
                            )

                        break

            except Exception as e:
                print(f"[JR] 日付処理失敗: {date_str} -> {e}", flush=True)
                _update_live_status(
                    live_status_box,
                    f"失敗: {resident_name} / {year}-{month:02d} / {date_str} / {e}",
                    "error"
                )

        print("[FIX] 保存開始", flush=True)
        save_all(driver)
        print(f"[FIX] 完了 件数={success_count}", flush=True)

    except Exception as e:
        print(f"[JR] month error: {resident_name} {ym} -> {e}", flush=True)
        _update_live_status(
            live_status_box,
            f"月処理失敗: {resident_name} / {ym} / {e}",
            "error"
        )

def generate_journal_from_memo(memo: str, work_label: str, start_time: str = "", end_time: str = ""):
    """
    メモから日誌を生成する（ダイアモンドルール完全適用）
    """
    memo = _normalize_text(memo)
    work = _normalize_text(work_label)

    mode = _detect_service_mode(
        row_text=memo,
        work_text=work,
        user_text=memo,
        staff_text=""
    )

    user_state = _force_diamond_user_state(
        user_state=memo,
        before_user=memo,
        before_staff="",
        work_label=work,
        mode=mode
    )

    user_state = _apply_mode_prefix_to_user_state(mode, user_state)

    staff_note = _force_diamond_staff_note(
        staff_note="",
        before_staff=memo
    )
    staff_note = _apply_mode_prefix_to_staff_note(mode, staff_note)

    user_state, staff_note = _enforce_mode_phrasing(mode, user_state, staff_note)

    return {
        "user_state": user_state,
        "staff_note": staff_note,
        "mode": mode
    }

# =========================================
# メイン処理
# =========================================
def run_bulk_rewrite(driver, residents, start_y, start_m, end_y, end_m, outside_workplace="", live_status_box=None):
    from run_assistance import goto_users_summary
    from run_assistance import apply_users_summary_filter_show_expired
    from run_assistance import open_support_record_for_resident

    exec_id = str(uuid.uuid4())
    user = str(st.session_state.get("user_id", "")).strip()
    company = str(st.session_state.get("company_id", "")).strip()

    st.session_state["jr_running"] = True

    try:
        for resident in residents:
            ok = goto_users_summary(driver)
            if not ok:
                append_journal_log({
                    "exec_id": exec_id,
                    "exec_time": _now_str(),
                    "user": user,
                    "company": company,
                    "resident_name": resident,
                    "target_month": "",
                    "result": "エラー",
                    "count": 0,
                    "message": "利用者ごと一覧へ移動できませんでした"
                })
                continue

            apply_users_summary_filter_show_expired(driver)

            ok = open_support_record_for_resident(driver, resident)
            if not ok:
                append_journal_log({
                    "exec_id": exec_id,
                    "exec_time": _now_str(),
                    "user": user,
                    "company": company,
                    "resident_name": resident,
                    "target_month": "",
                    "result": "エラー",
                    "count": 0,
                    "message": "対象利用者の支援記録を開けませんでした"
                })
                continue

            y, m = int(start_y), int(start_m)

            while (y < int(end_y)) or (y == int(end_y) and m <= int(end_m)):
                _update_live_status(
                    live_status_box,
                    f"処理中: {resident} / {y}-{m:02d}",
                    "info"
                )

                process_one_month(
                    driver=driver,
                    resident_name=resident,
                    year=y,
                    month=m,
                    exec_id=exec_id,
                    user=user,
                    company=company,
                    outside_workplace=outside_workplace,
                    live_status_box=live_status_box,
                )

                if m == 12:
                    y += 1
                    m = 1
                else:
                    m += 1

    finally:
        st.session_state["jr_running"] = False


# =========================================
# ページUI
# =========================================
def render_journal_rewrite_page():
    from run_assistance import build_chrome_driver, manual_login_wait

    st.header("過去日誌訂正（自動上書き）")
    st.caption("Knowbeの支援記録を月単位で取得し、Geminiで利用者状態と職員考察を再生成して上書きします。")

    if "jr_running" not in st.session_state:
        st.session_state["jr_running"] = False

    if not st.session_state.get("is_admin", False):
        st.session_state.pop("journal_rewrite_residents", None)
        st.session_state.pop("jr_outside_workplace", None)
        st.session_state["jr_running"] = False
        st.error("このページは管理者専用です。")
        return

    company_id = str(st.session_state.get("company_id", "")).strip()
    user_id = str(st.session_state.get("user_id", "")).strip()

    master_df = load_db("resident_master")
    if master_df is None or master_df.empty:
        st.warning("利用者マスタが見つかりません。")
        return

    work = master_df.copy()
    if "company_id" in work.columns:
        work = work[work["company_id"].astype(str).str.strip() == company_id].copy()

    if "resident_name" not in work.columns:
        st.warning("利用者マスタに resident_name 列がありません。")
        return

    work["resident_name"] = work["resident_name"].fillna("").astype(str).str.strip()
    work = work[work["resident_name"] != ""].copy()
    resident_options = sorted(work["resident_name"].unique().tolist())

    selected_residents = st.multiselect(
        "対象利用者（複数選択可）",
        resident_options,
        key="journal_rewrite_residents"
    )

    outside_options = ["未指定", "居酒屋 琴", "合同会社エバーグリーン"]
    if "jr_outside_workplace" not in st.session_state:
        st.session_state["jr_outside_workplace"] = "未指定"

    outside_workplace = st.selectbox(
        "施設外就労先",
        outside_options,
        index=outside_options.index(st.session_state["jr_outside_workplace"]) if st.session_state["jr_outside_workplace"] in outside_options else 0,
        key="jr_outside_workplace",
    )

    c1, c2 = st.columns(2)
    with c1:
        start_y = st.number_input("開始年", min_value=2024, max_value=2035, value=2025, step=1, key="jr_start_y")
        start_m = st.number_input("開始月", min_value=1, max_value=12, value=8, step=1, key="jr_start_m")
    with c2:
        end_y = st.number_input("終了年", min_value=2024, max_value=2035, value=2026, step=1, key="jr_end_y")
        end_m = st.number_input("終了月", min_value=1, max_value=12, value=3, step=1, key="jr_end_m")

    live_status_box = st.empty()

    run_clicked = st.button(
        "自動上書きを実行",
        key="run_journal_rewrite",
        use_container_width=True,
        disabled=st.session_state.get("jr_running", False),
    )

    if st.session_state.get("jr_running", False):
        st.info("現在実行中ある。")

    if run_clicked:
        if not selected_residents:
            st.error("利用者を1人以上選んでください。")
            return

        if (int(start_y), int(start_m)) > (int(end_y), int(end_m)):
            st.error("開始年月が終了年月より後になっています。")
            return

        login_username, login_password = get_current_company_saved_knowbe_info()
        if not login_username or not login_password:
            st.error("この事業所のKnowbeログイン情報が未登録です。『Knowbe情報登録』で保存してください。")
            return

        driver = None
        try:
            with st.spinner("Knowbeへ接続して自動上書きを実行中です…"):
                driver = build_chrome_driver()
                driver.get("https://mgr.knowbe.jp/v2/")
                time.sleep(2.0)
                manual_login_wait(driver, login_username, login_password)

                run_bulk_rewrite(
                    driver=driver,
                    residents=list(selected_residents),
                    start_y=int(start_y),
                    start_m=int(start_m),
                    end_y=int(end_y),
                    end_m=int(end_m),
                    outside_workplace=outside_workplace,
                    live_status_box=live_status_box,
                )

            st.success("自動上書き処理が完了したある。")
        except Exception as e:
            st.error(f"実行中にエラーが発生しました: {e}")
        finally:
            st.session_state["jr_running"] = False
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

    log_df = load_db("journal_rewrite_logs")
    if log_df is not None and not log_df.empty:
        st.markdown("### 実行ログ")
        st.dataframe(log_df.tail(50), use_container_width=True)
