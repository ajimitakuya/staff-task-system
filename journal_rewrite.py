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
支援記録リライト専用 命令書 v6.1

あなたの仕事は、Knowbe支援記録の原文をもとに、「利用者状態」と「職員考察」を再構成することである。
目的は、元の事実を壊さず、現場でそのまま貼り付けできる自然で元の文よりも文章量の多い文へ整えることである。

【最重要原則】
1. 捏造禁止
原文にない新事実、新活動、新しい体調変化、新しい支援行為を作らないこと。
ただし、原文中にある事実を、利用者状態と職員考察へ適切に再配置することは許可する。

2. 利用者状態は「その日の流れ」を書く
利用者状態は単語だけで終えてはならない。
利用者状態には、開始時の連絡、本人の体調、本人の発言、声の様子、作業内容、作業量、終了時の報告、本人の反応や様子を入れること。

3. 職員考察は「考察だけ」を書く
職員考察には、支援者の評価、見立て、配慮、今後の支援方針のみを書くこと。
開始連絡があった、終了時に何枚できたと言った等の事実描写は、職員考察ではなく利用者状態へ入れること。

4. 数量の扱い
原文に明確な枚数・個数・膳数がある場合はそれを優先すること。
半分、8割、少し、ちょっとだけ等の表現は、数量は必ず数値化して1以上にすること。
ただし、原文に「しんどくてできなかった」「全くできなかった」等の明確な不実施理由がある日は0件のままとすること。

5. 文体
です・ます調で書くこと。
自然な公文書調にすること。

【強制補完ルール】
- 利用者状態が「体調良好」「体調普通」など短語のみの場合は不完全とみなすこと
- その場合、開始連絡、体調、終了報告、作業内容、作業量を使って必ず2文以上へ補うこと
- 作業内容と数量は利用者状態に必ず入れること

【出力形式】
出力はJSONのみ。
各日付の値は user_state / staff_note の2つだけにすること。
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
- 利用者状態と職員考察を分ける
- 利用者状態には、作業開始、体調、作業内容、作業量、終了報告が分かるようにする
- 利用者状態が短語のみの場合は、必ず本文から補って2文以上にする

【出力形式】
{{
  "2025-08-01": {{
    "user_state": "利用者状態の本文",
    "staff_note": "職員考察の本文"
  }}
}}

【支援記録本文】
{page_text}
"""

    model = genai.GenerativeModel(
        model_name="gemini-2.5-flash",
        system_instruction=system_instruction
    )

    response = model.generate_content(prompt)
    text = str(getattr(response, "text", "")).strip()

    print("[JR] Gemini raw response start", flush=True)
    print(text[:3000], flush=True)
    print("[JR] Gemini raw response end", flush=True)

    if not text:
        raise RuntimeError("Geminiの応答が空です")

    text = text.replace("```json", "").replace("```", "").strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError(f"GeminiのJSON抽出に失敗しました: {text[:500]}")

    json_text = text[start:end + 1]
    try:
        return json.loads(json_text)
    except Exception as e:
        raise RuntimeError(f"Gemini JSON解析失敗: {e}\nJSON本文:\n{json_text[:1000]}")


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
    short_set = {
        "体調良好", "体調は良好", "体調が良好", "良好", "元気", "元気です",
        "体調普通", "体調は普通", "体調が普通", "普通",
        "体調まあまあ", "体調はまあまあ", "体調がまあまあ", "まあまあ",
        "体調まぁまぁ", "体調はまぁまぁ", "体調がまぁまぁ", "まぁまぁ",
        "体調大丈夫", "体調は大丈夫", "体調が大丈夫", "大丈夫",
    }
    if s in short_set:
        return True
    return len(_sentencize_jp(s)) <= 1 and len(s) <= 12 and ("体調" in s or s in short_set)


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
    s = _normalize_text(text)
    if not s or not work or allow_zero:
        return s
    if _has_explicit_quantity(s):
        return s
    if re.search(r"作業|やりました|できました|出来ました|実施|仕上げ|完成|順調に", s):
        unit = _work_default_unit(work)
        return s + f" {work}を1{unit}実施されました。"
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


def _postprocess_gemini_result(page_text: str, result_json: dict, year: int, month: int):
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

        fixed[date_str] = {
            "user_state": user_state,
            "staff_note": staff_note,
        }

    # Gemini が返さなかった日も raw から補完する
    for day, block in blocks.items():
        key = f"{year:04d}-{month:02d}-{day:02d}"
        if key in fixed:
            continue

        work = _normalize_text(block.get("work", ""))
        raw_user = _normalize_text(block.get("user_state_raw", ""))
        raw_staff = _normalize_text(block.get("staff_note_raw", ""))

        rebuilt_user = _compose_user_state_from_raw(work, raw_user, raw_staff)
        rebuilt_staff = raw_staff

        fixed[key] = {
            "user_state": rebuilt_user,
            "staff_note": rebuilt_staff,
        }

    return fixed


# =========================================
# 1ヶ月処理
# =========================================
def process_one_month(driver, resident_name, year, month, exec_id, user, company, outside_workplace=""):
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
        result_json = _postprocess_gemini_result(page_text_str, result_json, year, month)

        print("[FIX] 編集モードへ", flush=True)
        enter_edit_mode(driver)

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        success_count = 0

        for date_str, content in result_json.items():
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
                    if target_label in row.text:
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

                        final_user_state = str(user_state or "").strip()
                        if _is_short_user_state(final_user_state):
                            row_text = row.text
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

                            final_user_state = _rebuild_user_state_from_existing(
                                before_user=before_user,
                                before_staff=before_staff,
                                work_label=work_label,
                            )

                        _set_react_textarea_value(driver, user_state_el, final_user_state)
                        _set_react_textarea_value(driver, staff_note_el, staff_note)

                        after_user = _textarea_value(user_state_el)
                        after_staff = _textarea_value(staff_note_el)

                        print(f"[FIX] after user_state = {after_user[:80]}", flush=True)
                        print(f"[FIX] after staff_note = {after_staff[:80]}", flush=True)

                        if after_user == str(final_user_state).strip() and after_staff == str(staff_note).strip():
                            success_count += 1
                            print(f"[FIX] 入力成功: {target_label}", flush=True)
                        else:
                            raise RuntimeError(
                                f"入力反映失敗: {target_label} / "
                                f"user_match={after_user == str(final_user_state).strip()} / "
                                f"staff_match={after_staff == str(staff_note).strip()}"
                            )

                        break

            except Exception as e:
                print(f"[JR] 日付処理失敗: {date_str} -> {e}", flush=True)

        print("[FIX] 保存開始", flush=True)
        save_all(driver)
        print(f"[FIX] 完了 件数={success_count}", flush=True)

    except Exception as e:
        print(f"[JR] month error: {resident_name} {ym} -> {e}", flush=True)


# =========================================
# メイン処理
# =========================================
def run_bulk_rewrite(driver, residents, start_y, start_m, end_y, end_m, outside_workplace=""):
    from run_assistance import goto_users_summary
    from run_assistance import apply_users_summary_filter_show_expired
    from run_assistance import open_support_record_for_resident

    exec_id = str(uuid.uuid4())
    user = st.session_state.get("user_id", "")
    company = st.session_state.get("company_id", "")

    for resident in residents:
        ok = goto_users_summary(driver)
        if not ok:
            append_journal_log({
                "exec_id": exec_id,
                "exec_time": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
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
                "exec_time": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
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
            process_one_month(
                driver=driver,
                resident_name=resident,
                year=y,
                month=m,
                exec_id=exec_id,
                user=user,
                company=company,
                outside_workplace=outside_workplace,
            )

            if m == 12:
                y += 1
                m = 1
            else:
                m += 1


# =========================================
# ページUI
# =========================================
def render_journal_rewrite_page():
    from run_assistance import build_chrome_driver, manual_login_wait

    st.header("過去日誌訂正（自動上書き）")
    st.caption("Knowbeの支援記録を月単位で取得し、Geminiで利用者状態と職員考察を再生成して上書きします。")

    if not st.session_state.get("is_admin", False):
        st.error("このページは管理者専用です。")
        return

    company_id = str(st.session_state.get("company_id", "")).strip()

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

    outside_workplace = st.selectbox(
        "施設外就労先",
        ["未指定", "居酒屋 琴", "合同会社エバーグリーン"],
        index=0,
        key="jr_outside_workplace",
    )

    c1, c2 = st.columns(2)
    with c1:
        start_y = st.number_input("開始年", min_value=2024, max_value=2035, value=2025, step=1, key="jr_start_y")
        start_m = st.number_input("開始月", min_value=1, max_value=12, value=8, step=1, key="jr_start_m")
    with c2:
        end_y = st.number_input("終了年", min_value=2024, max_value=2035, value=2026, step=1, key="jr_end_y")
        end_m = st.number_input("終了月", min_value=1, max_value=12, value=3, step=1, key="jr_end_m")

    if st.button("自動上書きを実行", key="run_journal_rewrite", use_container_width=True):
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
                    residents=selected_residents,
                    start_y=int(start_y),
                    start_m=int(start_m),
                    end_y=int(end_y),
                    end_m=int(end_m),
                    outside_workplace=outside_workplace,
                )

            st.success("自動上書き処理が完了しました。")
        except Exception as e:
            st.error(f"実行中にエラーが発生しました: {e}")
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

    log_df = load_db("journal_rewrite_logs")
    if log_df is not None and not log_df.empty:
        st.markdown("### 実行ログ")
        st.dataframe(log_df.tail(50), use_container_width=True)