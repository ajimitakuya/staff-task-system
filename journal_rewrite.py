import time
import uuid
import json
import re
import pandas as pd
import streamlit as st
from openai import OpenAI
from selenium.webdriver.common.by import By
from common import now_jst
from data_access import load_db, save_db, get_companies_df
import random


OPENINGS_HOME = [
    "作業開始前の連絡では、",
    "開始時の連絡では、",
    "朝の連絡では、",
    "作業に入る前の連絡では、"
]

SUPPORT_PATTERNS = [
    "無理のない範囲で取り組めるよう配慮した。",
    "体調に配慮しながら無理のない範囲で進められるよう支援した。",
    "その日の状態に合わせて無理のない形で取り組めるよう配慮した。",
]

FUTURE_PATTERNS = [
    "今後も体調に配慮しながら支援していく。",
    "引き続き体調面に留意しながら安定して取り組めるよう支援する。",
    "今後も無理のない範囲で継続できるよう支援していく。",
]

OUTSIDE_OPENINGS = [
    "作業開始前に体調確認を行うと、",
    "開始時に体調確認を行ったところ、",
    "作業前に体調確認を行うと、"
]

OUTSIDE_SUPPORT = [
    "無理のない範囲で作業に取り組めるよう配慮した。",
    "体調面に配慮しながら無理のない範囲で進められるよう支援した。",
    "身体への負担を考慮しながら作業できるよう声かけを行った。"
]

OUTSIDE_FUTURE = [
    "今後も体調に配慮しながら支援していく。",
    "引き続き無理のない範囲で継続できるよう支援する。",
    "体調変化に留意しながら安全に取り組めるよう支援していく。"
]

# =========================================
# Gemini JSON生成
# =========================================
def _get_openai_api_key():
    api_key = ""
    try:
        api_key = st.secrets.get("OPENAI_API_KEY", "")
    except Exception:
        api_key = ""

    if not api_key:
        import os
        api_key = os.environ.get("OPENAI_API_KEY", "")

    return str(api_key or "").strip()


def _get_openai_client():
    api_key = _get_openai_api_key()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY が見つかりません")

    return OpenAI(
        api_key=api_key,
        timeout=60.0,
        max_retries=1,
    )


def _extract_openai_text(response) -> str:
    try:
        text = str(getattr(response, "output_text", "") or "").strip()
        if text:
            return text
    except Exception:
        pass

    try:
        parts = []
        for item in getattr(response, "output", []) or []:
            for content in getattr(item, "content", []) or []:
                if getattr(content, "type", "") == "output_text":
                    parts.append(str(getattr(content, "text", "") or ""))
        return "\n".join([p for p in parts if p]).strip()
    except Exception:
        return ""


def _openai_generate_text(prompt: str, system_instruction: str = "") -> str:
    client = _get_openai_client()

    last_error = None
    for model_name in ["gpt-5.2", "gpt-5.1", "gpt-4.1"]:
        try:
            response = client.responses.create(
                model=model_name,
                instructions=system_instruction,
                input=prompt,
            )
            text = _extract_openai_text(response)
            if text:
                return text
            last_error = RuntimeError(f"{model_name} empty response")
        except Exception as e:
            last_error = e
            print(f"[JR-DAY] OpenAI error model={model_name}: {e}", flush=True)
            continue

    raise RuntimeError(f"OpenAI生成失敗: {last_error}")


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
def generate_json_with_gemini_one_day(day_key: str, day_text: str, outside_workplace: str = ""):
    """
    互換維持のため関数名は gemini のまま。
    実体は OpenAI Responses API を使用する。
    """
    system_instruction = """
支援記録リライト専用 命令書 v9.0

あなたの仕事は、Knowbe支援記録の原文をもとに、
「利用者状態」と「職員考察」を、現場でそのまま貼り付けできる自然な文章へ再構成することです。

【共通原則】
1. 捏造禁止。
2. 原文にある本人発言は、使えるものを優先して残す。
3. 「体調安定」「精神安定」「精神不安定」「体調良好」などのラベル表現をそのまま本文に使わない。
4. 数量は原文に明示されている場合のみ使う。
5. 作業内容が曖昧な場合、勝手に数量を作らない。
6. 出力はJSONのみ。
"""

    outside_workplace = str(outside_workplace or "").strip()
    day_text = str(day_text or "")

    # =========================
    # ① 抽出フェーズ
    # =========================
    prompt_extract = f"""
以下はKnowbeの支援記録の1日分です。

【日付】
{day_key}

【本文】
{day_text}

【目的】
この文章から、日誌作成に必要な事実情報だけを抽出してください。
文章を綺麗に整える必要はありません。

【抽出ルール】
・本人の発言「」は必ず残す
・体調、不調、気分、作業内容、作業量、終了時の発言を抽出する
・推測は禁止
・原文にない数量を作らない
・時刻は抽出しなくてよい

【出力形式 JSONのみ】
{{
  "{day_key}": {{
    "start_contact": "",
    "condition": "",
    "start_quote": "",
    "staff_start_reply": "",
    "work_plan": "",
    "extra_note_before_end": "",
    "end_contact": "",
    "work_result": "",
    "end_quote": "",
    "staff_end_reply": "",
    "after_note": "",
    "staff_observation": "",
    "support_hint": ""
  }}
}}
"""

    print(f"[JR-DAY] {day_key} extract start", flush=True)

    try:
        text1 = _openai_generate_text(prompt_extract, system_instruction)
    except Exception as e:
        print(f"[JR-DAY] {day_key} extract error: {e}", flush=True)
        return {}

    cleaned1 = text1.replace("```json", "").replace("```", "").strip()

    try:
        json_start = cleaned1.find("{")
        json_end = cleaned1.rfind("}")
        extracted_json = json.loads(cleaned1[json_start:json_end + 1])
    except Exception as e:
        print(f"[JR-DAY] {day_key} extract parse error: {e}", flush=True)
        return {}

    # =========================
    # ② 整形フェーズ
    # =========================
    prompt_build = f"""
以下は支援記録から抽出した情報です。

{json.dumps(extracted_json, ensure_ascii=False)}

【重要】
この段階では、下記の理想文の流れに合わせて文章を作成してください。
ただし、最終的にはPython側でさらに型固定整形します。

【在宅の理想フォーマット】

■利用者状態
作業開始前に連絡があり、体調確認を行った。
本人より「〇〇」との話があった。
職員より「〇〇」と伝えると、「〇〇」と返答があった。
在宅での作業のため、本人からの報告にて確認した。
本日は〇〇を予定しているとのことだった。
必要があれば、原文にある通院・花粉・外出・生活状況などの補足をここに自然に入れる。

作業終了時に連絡があり、「〇〇」と報告があった。
作業量は〇〇とのことだった。
職員より「〇〇」と声をかけた。
本人からは「〇〇」と返答があった。
その後は休養されるとのことだった。

■職員考察
体調や気分の状態に関する見立て。
作業の区切り方や報告状況に関する評価。
今後の支援内容。

【施設外の理想フォーマット】

■利用者状態
作業開始前に体調確認を行うと、〇〇とのことだった。
本人より「〇〇」との話があった。
職員より「〇〇」と返答した。
〇〇を行う予定とのことだった。
作業は〇〇との報告があった。
途中で休憩を挟みながら対応されていた様子であった。
作業終了時には予定範囲を実施したとの連絡があった。
必要があれば本人発言を1つ入れる。

■職員考察
体調や作業状況に関する見立て。
無理のない範囲で取り組めているかの評価。
作業の質や報告状況。
今後の支援内容。

【通所の理想フォーマット】

■利用者状態
来所時に体調確認を行った。
本人より「〇〇」との話があった。
本日は〇〇に取り組まれた。
数量が明示されている場合のみ、作業量を記載する。
作業終了時には〇〇との報告があった。

■職員考察
来所時の体調や様子。
作業中の様子。
作業量や集中状況。
今後の支援内容。

【禁止】
・原文にない数量を作らない
・通所で「自分でこの辺でやめる」と書かない
・「精神安定」「体調安定」などをそのまま書かない
・「清掃作業をした」だけで終わらせない
・職員考察に本人発言だけを並べない

【出力形式 JSONのみ】
{{
  "{day_key}": {{
    "user_state": "",
    "staff_note": ""
  }}
}}
"""

    print(f"[JR-DAY] {day_key} build start", flush=True)

    try:
        text2 = _openai_generate_text(prompt_build, system_instruction)
    except Exception as e:
        print(f"[JR-DAY] {day_key} build error: {e}", flush=True)
        return {}

    print(f"[JR-DAY] {day_key} final raw start", flush=True)
    print(text2[:2000], flush=True)
    print(f"[JR-DAY] {day_key} final raw end", flush=True)

    cleaned2 = text2.replace("```json", "").replace("```", "").strip()

    try:
        json_start = cleaned2.find("{")
        json_end = cleaned2.rfind("}")
        data = json.loads(cleaned2[json_start:json_end + 1])

        if day_key in data:
            item = data.get(day_key, {})

            raw_user_state = str(item.get("user_state", "") or "")
            raw_staff_note = str(item.get("staff_note", "") or "")

            # =========================
            # 作業ラベルの初期化
            # ※ここでは outside_workplace を使わない
            # =========================
            base_work_label = str(item.get("work", "") or "").strip()

            if not base_work_label:
                base_work_label = _infer_home_work_label(
                    "\n".join([raw_user_state, raw_staff_note, day_text]),
                    "作業"
                )

            # =========================
            # モード判定
            # =========================
            mode = _detect_service_mode(
                row_text=day_text,
                work_text=base_work_label,
                user_text=raw_user_state,
                staff_text=raw_staff_note,
            )

            # =========================
            # モード強制補正
            # ※AI生成文の「来所時」は信用しない
            # ※原文 day_text だけで判定する
            # =========================
            raw_original = _normalize_text(day_text)

            if any(k in raw_original for k in [
                "施設外",
                "施設外就労",
            ]):
                mode = "施設外"

            elif any(k in raw_original for k in [
                "食事はあり",
                "食事：あり",
                "食事あり",
                "食事\nあり",
            ]):
                mode = "通所"

            else:
                mode = "在宅"

            registered_tasks_text = ""

            # =========================
            # 作業ラベル確定
            # 施設外のときだけ outside_workplace / 登録作業を使う
            # =========================
            if mode == "施設外" and str(outside_workplace or "").strip() and str(outside_workplace or "").strip() != "未指定":
                work_label = str(outside_workplace or "").strip() or base_work_label

                registered_tasks_text = _pick_outside_registered_tasks(outside_workplace)
                if registered_tasks_text:
                    work_label = registered_tasks_text
                    print(f"[OUTSIDE_TASK] selected = {work_label}", flush=True)

            else:
                # 通所・在宅では施設外就労先を絶対に混ぜない
                work_label = base_work_label

            if mode == "在宅":
                work_label = _infer_home_work_label(
                    "\n".join([raw_user_state, raw_staff_note, day_text]),
                    work_label
                )

                user_state, staff_note = _force_final_home_format(
                    raw_user_state,
                    raw_staff_note,
                    day_text,
                    work_label,
                )

            elif mode == "施設外":
                user_state, staff_note = _force_final_outside_format(
                    raw_user_state,
                    raw_staff_note,
                    day_text,
                    work_label,
                )

            else:
                user_state, staff_note = _force_final_office_format(
                    raw_user_state,
                    raw_staff_note,
                    day_text,
                    work_label,
                )

            # =========================
            # 施設外：登録済み作業内容を本文へ強制反映
            # =========================
            if mode == "施設外" and registered_tasks_text:
                task_text = registered_tasks_text

                old_line_pattern = r"[^。\n]*(通路清掃|手すり拭き|共用部の清掃|清掃作業|清掃)[^。\n]*予定[^。\n]*。"
                new_line = f"{task_text}を行う予定とのことだった。"

                if re.search(old_line_pattern, user_state):
                    user_state = re.sub(old_line_pattern, new_line, user_state, count=1)
                else:
                    user_state = user_state.rstrip()
                    user_state = user_state + "\n" + new_line

            data[day_key]["user_state"] = _final_cleanup_journal_text(user_state)
            data[day_key]["staff_note"] = _final_cleanup_journal_text(staff_note)
            data[day_key]["mode"] = mode

            print(f"[JR-DAY] {day_key} final formatted mode={mode}", flush=True)
            print(data[day_key]["user_state"], flush=True)
            print(data[day_key]["staff_note"], flush=True)

        return data

    except Exception as e:
        print(f"[JR-DAY] {day_key} build parse/final format error: {e}", flush=True)
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

def _pick_outside_registered_tasks(outside_workplace: str, max_optional: int = 3) -> str:
    """
    施設外就労先名から登録済み作業を取得し、
    必須作業は全部、選択作業は優先度順から最大3つ使う。
    """
    workplace_name = _normalize_text(outside_workplace)
    if not workplace_name:
        return ""

    try:
        workplaces_df = load_db("outside_workplaces")
        tasks_df = load_db("outside_work_tasks")
    except Exception as e:
        print(f"[OUTSIDE_TASK] load error: {e}", flush=True)
        return ""

    if workplaces_df is None or workplaces_df.empty or tasks_df is None or tasks_df.empty:
        return ""

    workplaces_df = workplaces_df.fillna("").copy()
    tasks_df = tasks_df.fillna("").copy()

    company_id = str(st.session_state.get("company_id", "")).strip()

    hit = workplaces_df[
        (workplaces_df["status"].astype(str).str.strip() == "active") &
        (workplaces_df["workplace_name"].astype(str).str.strip() == workplace_name)
    ].copy()

    if company_id and "company_id" in hit.columns:
        hit = hit[hit["company_id"].astype(str).str.strip() == company_id].copy()

    if hit.empty:
        return ""

    workplace_id = str(hit.iloc[0].get("workplace_id", "")).strip()
    if not workplace_id:
        return ""

    work = tasks_df[
        (tasks_df["workplace_id"].astype(str).str.strip() == workplace_id) &
        (tasks_df["status"].astype(str).str.strip() == "active")
    ].copy()

    if work.empty:
        return ""

    if "task_type" not in work.columns:
        work["task_type"] = "optional"

    try:
        work["priority_num"] = pd.to_numeric(work["priority"], errors="coerce").fillna(99)
        work = work.sort_values(["priority_num", "task_text"])
    except Exception:
        pass

    required = work[work["task_type"].astype(str).str.strip() == "required"]
    optional = work[work["task_type"].astype(str).str.strip() != "required"]

    required_list = [
        _normalize_text(x)
        for x in required["task_text"].tolist()
        if _normalize_text(x)
    ]

    optional_list = [
        _normalize_text(x)
        for x in optional["task_text"].tolist()
        if _normalize_text(x)
    ]

    # 今は安定優先でランダムではなく優先度順。あとでランダム化可能。
    picked_optional = optional_list[:max_optional]

    tasks = []
    for x in required_list + picked_optional:
        if x and x not in tasks:
            tasks.append(x)

    if not tasks:
        return ""

    if len(tasks) == 1:
        return tasks[0]

    return "、".join(tasks)

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

import re

def _extract_quantity(text: str):
    """
    '200枚', '70個', '100本' などを抽出
    """
    if not text:
        return ""

    patterns = [
        r'(\d+)\s*枚',
        r'(\d+)\s*個',
        r'(\d+)\s*本',
        r'(\d+)\s*セット',
    ]

    for p in patterns:
        m = re.search(p, text)
        if m:
            return m.group(0)

    return ""

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

def _extract_first_sentence_by_keywords(text: str, keywords):
    for s in _sentencize_jp(text):
        if any(k in s for k in keywords):
            return s.rstrip("。") + "。"
    return ""

def _uniq_sentences(*args):
    seen = set()
    result = []
    for a in args:
        a = _normalize_text(a)
        if not a:
            continue
        key = re.sub(r"\s+", "", a.rstrip("。"))
        if key and key not in seen:
            seen.add(key)
            result.append(a.rstrip("。") + "。")
    return result

def _extract_home_work_result(user_source: str, work_label: str, row_text: str) -> str:
    user_src = _normalize_text(user_source)
    work = _normalize_text(work_label)
    row = _normalize_text(row_text)

    # 1. 作業名＋数量が同じ文にあるものを最優先
    if work:
        for s in _sentencize_jp(user_src):
            s2 = _normalize_work_quantity_phrase(s, work)
            if work in s2 and re.search(r"\d+\s*(枚|個|膳|本|羽)", s2):
                return s2.rstrip("。") + "。"

    # 2. 数量文だけある場合（塗り絵を1枚など）
    for s in _sentencize_jp(user_src):
        s2 = _normalize_work_quantity_phrase(s, work)
        if re.search(r"\d+\s*(枚|個|膳|本|羽)", s2):
            return s2.rstrip("。") + "。"

    # 3. work_labelから自然に作る
    result_phrase = _build_work_result_phrase(work, user_src, row)

    if "に取り組まれました" in result_phrase and _is_quantifiable_work(work):
        est = _estimate_quantity_phrase(work, row) or _estimate_quantity_phrase(work, user_src)
        if est:
            return f"{est}実施されました。"

        if "塗り絵" in work:
            return "塗り絵を1枚実施されました。"
        elif "箸" in work or "箸入れ" in work or "お箸" in work:
            return f"{work}を1膳実施されました。"
        elif "チラシ" in work:
            return f"{work}を1枚実施されました。"
        elif "折り鶴" in work or "鶴" in work:
            return f"{work}を1羽実施されました。"

    if result_phrase:
        return result_phrase.rstrip("。") + "。"

    if work:
        return f"{work}に取り組まれました。"

    return "作業に取り組まれました。"

def _rebuild_home_record_strict(raw_user: str, raw_staff: str, work_label: str, row_text: str = ""):
    raw_u = _normalize_text(raw_user)
    raw_s = _normalize_text(raw_staff)
    row = _normalize_text(row_text)
    work = _normalize_text(work_label)

    merged = " ".join([x for x in [raw_u, raw_s, row] if x]).strip()

    # ---------------------------
    # ① 利用者状態で使う要素
    # ---------------------------

    # 作業開始
    start_line = _extract_first_sentence_by_keywords(
        raw_u,
        ["作業開始", "開始時", "開始前", "開始の連絡", "朝の連絡"]
    )

    # 体調（raw_user優先、なければraw_staffから救う）
    health_line = _extract_first_sentence_by_keywords(
        raw_u,
        ["体調", "元気", "不調", "しんどい", "倦怠感", "安定", "不安定", "眠れ", "気分"]
    )

    # ★ここが追加（重要）
    if not health_line:
        health_line = _extract_first_sentence_by_keywords(
            raw_s,
            ["体調", "元気", "不調", "しんどい", "倦怠感", "安定", "不安定", "眠れ", "気分"]
        )

    # ★さらに補強（重要）
    if health_line:
        health_line = health_line.replace("との連絡を受けるが", "と報告がありました")
        health_line = health_line.replace("との事でした", "と報告がありました")
        health_line = health_line.replace("とされていた", "と報告がありました")
        health_line = health_line.replace("と聞いている", "と報告がありました")
        health_line = health_line.replace("と伺っている", "と報告がありました")
    if not health_line:
        health_line = _extract_first_sentence_by_keywords(
            raw_s,
            ["体調", "元気", "不調", "しんどい", "倦怠感", "安定", "不安定", "眠れ", "気分"]
        )

    # 終了文は「終了連絡 + 作業内容」を1文にまとめる
    combined_end_line = ""

    # ←この行を追加する
    work_result_line = _extract_home_work_result(raw_u, work, row)

    if work_result_line:
        work_core = work_result_line.rstrip("。")
        work_core = re.sub(r'^(作業終了時に連絡があり、|作業終了の連絡があり、)', '', work_core)
        combined_end_line = f"作業終了の連絡があり、{work_core}。"
    else:
        combined_end_line = "作業終了の連絡がありました。"

    # ---------------------------
    # ② 職員考察で使う要素
    # ---------------------------

    opinion_line = _extract_first_sentence_by_keywords(
        raw_s,
        ["様子", "見られ", "感じられ", "うかがえ", "状態", "安定", "不安定", "しんどい", "倦怠感"]
    )

    support_line = _extract_first_sentence_by_keywords(
        raw_s,
        ["支援", "声掛け", "配慮", "確認", "見守り", "無理のない範囲", "継続"]
    )

    # ---------------------------
    # ③ 不足分だけ補完
    # ---------------------------

    if not start_line:
        start_line = "作業開始時に連絡がありました。"

    if not health_line:
        health_line = _extract_home_health_phrase(raw_u, raw_s, row)
        if not health_line:
            health_line = "体調について確認すると、大きな変化はない様子でした。"

    # 体調文を利用者状態向けに軽く整える
    health_line = health_line.replace("との連絡を受けるが", "と報告がありました")
    health_line = health_line.replace("との事でした", "との報告がありました")
    health_line = health_line.replace("と話される", "と報告がありました")
    health_line = health_line.replace("ご本人様", "利用者さん")
    health_line = health_line.replace("利用者様", "利用者さん")

    if not work_result_line:
        work_result_line = "作業に取り組まれました。"

    # 終了文は内容を重ねない
    if not end_line:
        end_line = "作業終了時に連絡がありました。"

    # 職員考察は「体調への見立て」と「支援」のみ
    if not opinion_line:
        if any(k in merged for k in ["不調", "しんどい", "倦怠感", "眠れない", "眠りが浅い", "不安定"]):
            opinion_line = "体調に波がある中でも、無理のない範囲で作業に取り組まれていたようである。"
        elif any(k in merged for k in ["良好", "元気", "普通", "大丈夫", "安定", "変わりなく"]):
            opinion_line = "体調が安定していたことで、落ち着いて作業が進められたようである。"
        else:
            opinion_line = "その日の体調に応じて、無理なく作業に取り組めていたようである。"

    # 作業そのものの説明や意味不明文は落とす
    if opinion_line:
        if any(k in opinion_line for k in ["塗り絵", "内職", "観葉植物", "作業開始の連絡", "希望にて", "にて作業"]):
            opinion_line = ""

    if not opinion_line:
        if any(k in merged for k in ["不調", "しんどい", "倦怠感", "眠れない", "眠りが浅い", "不安定"]):
            opinion_line = "体調が優れない中でも、できる範囲で作業に取り組まれていたようである。"
        else:
            opinion_line = "体調は安定しており、落ち着いて作業に取り組まれていたようである。"

    if not support_line:
        if any(k in merged for k in ["不調", "しんどい", "倦怠感", "眠れない", "眠りが浅い", "不安定"]):
            support_line = "体調に配慮しながら、無理のない範囲で継続できるよう支援していきます。"
        else:
            support_line = "体調や作業の様子を確認しながら、安定して継続できるよう支援していきます。"

    # ---------------------------
    # ④ 利用者状態＝開始・体調・作業・終了
    # ---------------------------
    user_parts = _uniq_sentences(start_line, health_line, combined_end_line)

    # ---------------------------
    # ⑤ 職員考察＝体調への意見・支援
    # ---------------------------
    staff_parts = _uniq_sentences(opinion_line, support_line)

    user_state = " ".join(user_parts).strip()
    staff_note = " ".join(staff_parts).strip()

    # 数量表現の整形
    user_state = _normalize_work_quantity_phrase(user_state, work)
    user_state = _convert_ambiguous_quantity_to_one_or_more(user_state, work, False)
    user_state = _append_default_quantity_if_missing(user_state, work, False)

    # 丁寧すぎる言い回しを落とす
    user_state = user_state.replace("お聞きしております", "と報告がありました")
    user_state = user_state.replace("お聞きしました", "と報告がありました")
    user_state = user_state.replace("伺っております", "と報告がありました")
    user_state = user_state.replace("伺いました", "と報告がありました")

    staff_note = staff_note.replace("お聞きしております", "と報告がありました")
    staff_note = staff_note.replace("お聞きしました", "と報告がありました")
    staff_note = staff_note.replace("伺っております", "と報告がありました")
    staff_note = staff_note.replace("伺いました", "と報告がありました")

    # 呼称を統一
    user_state = user_state.replace("ご本人様", "利用者さん")
    user_state = user_state.replace("利用者様", "利用者さん")
    staff_note = staff_note.replace("ご本人様", "利用者さん")
    staff_note = staff_note.replace("利用者様", "利用者さん")

    user_state = _fix_japanese_artifacts(user_state)
    staff_note = _fix_japanese_artifacts(staff_note)

    user_state = _cleanup_user_state_garbage(user_state, "在宅")
    staff_note = _cleanup_staff_note_garbage(staff_note)

    user_state = _dedupe_sentences(user_state)
    staff_note = _dedupe_sentences(staff_note)

    return user_state, staff_note

def _fix_japanese_artifacts(text: str) -> str:
    text = text.replace("とと報告がありました", "との報告がありました")
    text = text.replace("。。", "。")
    text = text.replace("..", ".")
    text = text.replace("にてにて", "にて")

    # 時刻は不要
    text = re.sub(r'\b\d{1,2}:\d{2}\b', '', text)
    text = re.sub(r'\b\d{1,2}時\d{1,2}分\b', '', text)
    text = re.sub(r'\b\d{1,2}時\b', '', text)

    # 時刻を消した後の不自然な残りを軽く整理
    text = text.replace("に作業開始の連絡があった", "作業開始の連絡があった")
    text = text.replace("に作業開始の連絡がありました", "作業開始の連絡がありました")
    text = text.replace("に作業を開始した", "作業を開始した")
    text = text.replace("に作業を開始する連絡があった", "作業開始の連絡があった")
    text = text.replace("に作業終了の連絡があった", "作業終了の連絡があった")
    text = text.replace("に作業終了の連絡がありました", "作業終了の連絡がありました")
    text = text.replace("に作業を終了した", "作業を終了した")

    text = re.sub(r"\s+", " ", text)
    return text.strip()

def _force_end_sentence_order(user_state: str, work_label: str) -> str:
    s = _normalize_text(user_state)
    work = _normalize_text(work_label)

    if not s:
        return s

    # まず時刻を消しておく
    s = re.sub(r'\b\d{1,2}:\d{2}\b', '', s)
    s = re.sub(r'\b\d{1,2}時\d{1,2}分\b', '', s)
    s = re.sub(r'\b\d{1,2}時\b', '', s)
    s = re.sub(r'\s+', ' ', s).strip()

    # 「作業終了の連絡」があり、かつ作業数量が別文にある場合に結合する
    has_end = any(k in s for k in ["作業終了の連絡", "作業終了", "終了の連絡"])
    if not has_end:
        return s

    qty_match = re.search(r'([^。]*\d+\s*(?:枚|個|膳|本|羽)[^。]*)。?', s)
    if not qty_match:
        return s

    qty_phrase = qty_match.group(1).strip()

    # 既に「作業終了の連絡があり、～」の形ならそのまま
    if re.search(r'作業終了[^。]*、[^。]*\d+\s*(?:枚|個|膳|本|羽)', s):
        return s

    # 数量文を消して、終了文をまとめ直す
    s = re.sub(r'[^。]*\d+\s*(?:枚|個|膳|本|羽)[^。]*。?', '', s)
    s = re.sub(r'作業終了[^。]*。?', '', s)
    s = re.sub(r'\s+', ' ', s).strip()

    end_line = f"作業終了の連絡があり、{qty_phrase}。"

    if s:
        return (s.rstrip("。") + "。 " + end_line).strip()
    return end_line

def _is_health_only_user_state(text: str) -> bool:
    s = _normalize_text(text)
    if not s:
        return False

    # 時刻削除
    s = re.sub(r'\b\d{1,2}:\d{2}\b', '', s)
    s = re.sub(r'\b\d{1,2}時\d{1,2}分\b', '', s)
    s = re.sub(r'\b\d{1,2}時\b', '', s)
    s = re.sub(r'\s+', ' ', s).strip()

    # 体調・精神だけで、作業や終了がないなら health only
    has_health = any(k in s for k in ["体調", "精神", "良好", "安定", "不安定", "不調", "元気"])
    has_work = any(k in s for k in ["塗り絵", "内職", "観葉植物", "箸", "チラシ", "折り鶴", "作業"])
    has_end = any(k in s for k in ["作業終了", "終了の連絡", "作業開始", "開始の連絡"])

    return has_health and not has_work and not has_end

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
        ["水やり", "塗り絵", "内職", "清掃", "掃き掃除", "モップ", "手すり", "ゴミ拾い", "消火器", "配電盤", "検品", "袋詰め", "封入", "チラシ", "折り鶴", "箱折り", "ラベル貼り", "仕分け"],
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

    # 長くなりすぎるのを防ぐため、作業文は1本だけ返す
    if lines:
        return [lines[0]]

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

def _remove_status_labels(text: str) -> str:
    s = _normalize_text(text)
    if not s:
        return s

    # 単独ラベル系を削除
    s = re.sub(r'(精神安定|精神不安定|体調安定|体調不安定|体調良好|体調普通|元気)\s*[、，]?\s*', '', s)

    # モード系ラベルを削除
    s = re.sub(r'(在宅利用|在宅|通所|施設外)\s*[。 ]*', '', s)

    # 変な残骸
    s = re.sub(r'\b利用\b[。 ]*', '', s)

    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _cleanup_user_state_garbage(text: str, mode: str) -> str:
    s = _normalize_text(text)
    if not s:
        return s

    s = _remove_status_labels(s)

    # 利用者状態に入れてはいけない支援・考察文
    bad_patterns = [
        r'今後も[^。]*支援[^。]*。',
        r'引き続き[^。]*支援[^。]*。',
        r'取り組みやすい声掛け[^。]*。',
        r'その日の状態を確認しながら[^。]*。',
        r'体調や精神面の変化に[^。]*。',
        r'体調や気分の変化を確認しながら[^。]*。',
        r'無理のない範囲で[^。]*支援[^。]*。',
        r'安心して[^。]*支援[^。]*。',
    ]
    for pat in bad_patterns:
        s = re.sub(pat, '', s)

    # 禁止書き出し残骸
    s = re.sub(r'^(にて|在宅にて|での作業として、|での)\s*', '', s)
    s = re.sub(r'(^|。)\s*(にて|在宅にて|での作業として、|での)\s*', r'\1', s)

    # 不自然な断片
    s = re.sub(r'無理を。', '', s)
    s = re.sub(r'朝の\s*$', '', s)
    s = re.sub(r'体調や精神面の変化に\s*$', '', s)
    s = re.sub(r'体調や気分の変化を確認しながら\s*$', '', s)

    # 記号
    s = s.replace("？。", "。").replace("?。", "。")
    s = re.sub(r'[？?]\s*。', '。', s)

    s = re.sub(r'\s+', ' ', s).strip()
    return _dedupe_sentences(s)


def _cleanup_staff_note_garbage(text: str) -> str:
    s = _normalize_text(text)
    if not s:
        return s

    s = _remove_status_labels(s)

    # Gemini由来の残骸
    bad_prefixes = [
        r'^在宅での取り組み状況を踏まえ、',
        r'^施設外での作業状況を踏まえ、',
        r'^施設外就労先での状況を踏まえ、',
        r'^の取り組み状況を踏まえ、',
    ]
    for pat in bad_prefixes:
        s = re.sub(pat, '', s)

    # 禁止書き出し残骸
    s = re.sub(r'(^|。)\s*(在宅利用|在宅にて|にて|での作業として、|での)\s*', r'\1', s)

    # 記号
    s = s.replace("？。", "。").replace("?。", "。")
    s = re.sub(r'[？?]\s*。', '。', s)

    s = re.sub(r'\s+', ' ', s).strip()
    return _dedupe_sentences(s)


def _force_diamond_user_state(
    user_state: str,
    before_user: str,
    before_staff: str,
    work_label: str,
    mode: str = "",
    row_text: str = "",
) -> str:
    src_before_user = _normalize_text(before_user)
    src_before_staff = _normalize_text(before_staff)
    src_row = _normalize_text(row_text)

    merged = " ".join([x for x in [src_before_user, src_before_staff, src_row] if x]).strip()
    merged = _lighten_journal_tone(merged)

    opening = _mode_opening_phrase(mode, merged)

    # 体調・開始系だけ拾う（精神安定などのラベルは後で消す）
    personal_lines = _pick_all_matching_lines(
        src_before_user + "\n" + src_row,
        ["本人より", "連絡", "体調", "しんどい", "元気", "良好", "普通", "不安定", "安定"],
        max_count=2
    )

    work_lines = _mode_work_lines(mode, merged, work_label)

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

    if not parts:
        if _normalize_text(work_label):
            if mode == "施設外" and "清掃" in work_label:
                parts.append("廊下の掃き掃除や手すり拭きなどの清掃作業に取り組みました。")
            else:
                parts.append(f"{_normalize_text(work_label)}に取り組みました。")

    result = " ".join([p for p in parts if _normalize_text(p)])
    result = _normalize_work_quantity_phrase(result, _normalize_text(work_label))

    allow_zero = _contains_explicit_no_work_reason(merged)
    result = _convert_ambiguous_quantity_to_one_or_more(result, _normalize_text(work_label), allow_zero)

    if "清掃" in _normalize_text(work_label):
        result = re.sub(r'清掃を\d+(?:枚|個|膳|本|羽|通|袋)実施されました。?', '清掃に取り組みました。', result)
        result = re.sub(r'清掃を\d+(?:枚|個|膳|本|羽|通|袋)行ったとの報告がありました。?', '清掃に取り組んだことを報告されました。', result)

    result = result.replace("清掃作業作業", "清掃作業")
    result = _lighten_journal_tone(result)
    result = _strip_unwanted_words(result)
    return result.strip()


def _force_diamond_staff_note(staff_note: str, before_staff: str) -> str:
    out = _lighten_journal_tone(_normalize_text(staff_note))
    raw = _lighten_journal_tone(_normalize_text(before_staff))
    merged = " ".join([x for x in [out, raw] if x]).strip()

    eval_line = _pick_first_matching_line(
        merged,
        [
            "作業量", "集中", "体調", "不安定", "安定", "意欲", "積極",
            "無理をせず", "継続", "報告", "具体的", "丁寧", "責任感",
            "だるい", "痛み", "眠れ", "疲れ", "しんどい", "様子"
        ]
    )

    if not eval_line:
        if any(k in merged for k in ["不安定", "しんどい", "疲れ", "だるい", "眠れ", "痛み"]):
            eval_line = "体調や気分に波はあったが、無理のない範囲で作業に取り組めていました。"
        elif any(k in merged for k in ["意欲", "積極", "前向き", "責任感"]):
            eval_line = "その日の状態に合わせながら、前向きに取り組めていました。"
        else:
            eval_line = "その日の状態に応じて、無理なく作業を進められていました。"

    support_line = _pick_first_matching_line(
        merged,
        ["支援", "声掛け", "お伝え", "配慮", "継続", "確認", "促し", "無理のない", "見守り"]
    )
    if not support_line:
        support_line = _mode_staff_support_sentence("", merged)

    result = _dedupe_sentences(_lighten_journal_tone(eval_line + " " + support_line))
    return result.strip()

def _extract_home_health_phrase(*texts) -> str:
    source = " ".join([_normalize_text(t) for t in texts if _normalize_text(t)])

    patterns = [
        r"(体調[^。]*?(?:良好|良い|よい|普通|まあまあ|まぁまぁ|大丈夫|不調|優れない|優れず|しんどい|安定|不安定)[^。]*。)",
        r"(精神[^。]*?(?:安定|不安定)[^。]*。)",
        r"(元気[^。]*。)",
    ]
    for pat in patterns:
        m = re.search(pat, source)
        if m:
            return m.group(1).strip()

    if any(k in source for k in ["不調", "優れない", "優れず", "しんどい", "だるい", "眠れない", "眠りが浅い"]):
        return "作業開始時に体調について確認すると、あまり良くない状態との報告がありました。"

    if any(k in source for k in ["良好", "元気", "普通", "大丈夫", "安定", "変わりなく"]):
        return "作業開始時に体調について確認すると、大きな変化はなく落ち着いている様子でした。"

    return "作業開始時に体調について確認すると、体調について報告がありました。"


def _ensure_home_required_items(
    user_state: str,
    staff_note: str,
    raw_user: str,
    raw_staff: str,
    work_label: str,
    row_text: str = "",
):
    work = _normalize_text(work_label) or "作業"
    src_user = _normalize_text(user_state)
    src_staff = _normalize_text(staff_note)
    raw_u = _normalize_text(raw_user)
    raw_s = _normalize_text(raw_staff)
    row = _normalize_text(row_text)

    source_all = " ".join([src_user, src_staff, raw_u, raw_s, row]).strip()

    # 0や半分はNGなので、在宅では allow_zero=False 扱いで固定
    allow_zero = False

    # ① 作業開始・終了
    has_start = any(k in src_user for k in ["作業開始", "開始時", "開始前"])
    has_end = any(k in src_user for k in ["作業終了", "終了時", "終了後"])

    start_line = ""
    end_line = ""

    if not has_start:
        start_line = _extract_home_health_phrase(raw_u, raw_s, row)
        if not any(k in start_line for k in ["作業開始", "開始時", "開始前"]):
            start_line = "作業開始時に体調について確認すると、" + start_line.rstrip("。") + "。"

    # ② 体調
    has_health = any(k in src_user for k in ["体調", "精神", "元気", "不調", "しんどい", "安定", "不安定"])
    if not has_health:
        health_line = _extract_home_health_phrase(raw_u, raw_s, row)
    else:
        health_line = ""

    # ③ 作業内容＋具体的数量
    result_phrase = _build_work_result_phrase(work, src_user, src_staff, raw_u, raw_s, row)
    if "に取り組まれました" in result_phrase and _is_quantifiable_work(work):
        # 数量が出ていないときは関数補完へ
        est = _estimate_quantity_phrase(work, row) or _estimate_quantity_phrase(work, source_all)
        if est:
            result_phrase = est
        else:
            # 最後の保険
            if "塗り絵" in work:
                result_phrase = "塗り絵を1枚"
            elif "箸" in work or "箸入れ" in work or "お箸" in work:
                result_phrase = f"{work}を1膳"
            elif "チラシ" in work:
                result_phrase = f"{work}を1枚"
            elif "折り鶴" in work or "鶴" in work:
                result_phrase = f"{work}を1羽"

    has_qty = bool(re.search(r"\d+\s*(枚|個|膳|本|羽)", src_user))
    if not has_end or not has_qty:
        end_line = f"作業終了時に連絡があり、{result_phrase}行ったとの報告がありました。"

    # user_state 再構成
    user_parts = []
    if start_line:
        user_parts.append(start_line)
    elif has_start:
        user_parts.append(src_user)

    if health_line and health_line not in " ".join(user_parts):
        user_parts.append(health_line)

    if end_line:
        user_parts.append(end_line)
    elif src_user:
        user_parts.append(src_user)

    rebuilt_user = " ".join([p for p in user_parts if _normalize_text(p)])
    rebuilt_user = _normalize_work_quantity_phrase(rebuilt_user, work)
    rebuilt_user = _convert_ambiguous_quantity_to_one_or_more(rebuilt_user, work, allow_zero)
    rebuilt_user = _append_default_quantity_if_missing(rebuilt_user, work, allow_zero)
    rebuilt_user = _dedupe_sentences(rebuilt_user)

    # ④ 支援内容
    has_support = any(k in src_staff for k in ["支援", "声掛け", "確認", "配慮", "見守り", "無理のない範囲", "継続"])
    if not has_support:
        if any(k in source_all for k in ["不調", "優れない", "しんどい", "だるい", "眠れない", "不安定"]):
            support_line = "体調に配慮しながら、無理のない範囲で取り組めるよう支援していきます。"
        else:
            support_line = "その日の状態を確認しながら、無理のない範囲で継続できるよう支援していきます。"

        if src_staff:
            rebuilt_staff = _dedupe_sentences(src_staff + " " + support_line)
        else:
            rebuilt_staff = support_line
    else:
        rebuilt_staff = _dedupe_sentences(src_staff)

    rebuilt_staff = _cleanup_staff_note_garbage(rebuilt_staff)
    rebuilt_user = _cleanup_user_state_garbage(rebuilt_user, "在宅")

    return rebuilt_user, rebuilt_staff

def _finalize_non_home_mode(
    mode: str,
    user_state: str,
    staff_note: str,
    raw_user: str,
    raw_staff: str,
    work_label: str,
    row_text: str = "",
):
    user = _normalize_text(user_state)
    staff = _normalize_text(staff_note)
    raw_u = _normalize_text(raw_user)
    raw_s = _normalize_text(raw_staff)
    work = _normalize_text(work_label)
    row = _normalize_text(row_text)

    # まず既存のモード整形を必ず通す
    user = _apply_mode_prefix_to_user_state(mode, user)
    staff = _apply_mode_prefix_to_staff_note(mode, staff)
    user, staff = _enforce_mode_phrasing(mode, user, staff)

    if mode == "通所":
        forbidden_user = [
            r'作業開始の連絡があり[^。]*。?',
            r'作業終了の連絡があり[^。]*。?',
            r'開始前に連絡があり[^。]*。?',
            r'終了時に連絡があり[^。]*。?',
            r'電話連絡[^。]*。?',
            r'電話で[^。]*作業開始[^。]*。?',
            r'電話で[^。]*報告[^。]*。?',
        ]
        forbidden_staff = [
            r'作業開始の連絡があり[^。]*。?',
            r'作業終了の連絡があり[^。]*。?',
            r'電話連絡[^。]*。?',
        ]
        for pat in forbidden_user:
            user = re.sub(pat, '', user)
        for pat in forbidden_staff:
            staff = re.sub(pat, '', staff)

        # 通所なのに崩れたら、通所専用再構成
        if (not user.strip()) or any(k in user for k in ["作業開始の連絡", "作業終了の連絡", "電話連絡", "在宅"]):
            row_data = {
                "利用者状態": raw_u,
                "職員考察": raw_s,
                "作業": work,
                "row_text": row,
            }
            user = _build_office_user_state(row_data)
            staff = _build_office_staff_note(row_data)

        user = _apply_mode_prefix_to_user_state(mode, user)
        staff = _apply_mode_prefix_to_staff_note(mode, staff)
        user, staff = _enforce_mode_phrasing(mode, user, staff)

    elif mode == "施設外":
        forbidden_user = [
            r'作業開始の連絡があり[^。]*。?',
            r'作業終了の連絡があり[^。]*。?',
            r'開始前に連絡があり[^。]*。?',
            r'終了時に連絡があり[^。]*。?',
            r'電話連絡[^。]*。?',
            r'電話で[^。]*作業開始[^。]*。?',
            r'来所時[^。]*。?',
            r'来所され[^。]*。?',
            r'時間通りに来所[^。]*。?',
            r'通所[^。]*。?',
        ]
        forbidden_staff = [
            r'作業開始の連絡があり[^。]*。?',
            r'作業終了の連絡があり[^。]*。?',
            r'電話連絡[^。]*。?',
            r'来所時[^。]*。?',
            r'通所[^。]*。?',
        ]
        for pat in forbidden_user:
            user = re.sub(pat, '', user)
        for pat in forbidden_staff:
            staff = re.sub(pat, '', staff)

        # 施設外なのに崩れたら最低限の施設外文へ戻す
        if (not user.strip()) or any(k in user for k in ["作業開始の連絡", "作業終了の連絡", "電話連絡", "来所", "在宅"]):
            detail = _facility_cleaning_detail_phrase(" ".join([raw_u, raw_s, row, work]))
            user = f"{detail}に取り組まれました。"
            if raw_u and any(k in raw_u for k in ["体調", "元気", "良好", "普通", "不調", "しんどい"]):
                user = raw_u.rstrip("。") + "。 " + user

            if raw_s:
                staff = raw_s
            else:
                staff = "その日の体調や作業の様子を確認しながら、無理なく続けられるよう支援していきます。"

        user = _apply_mode_prefix_to_user_state(mode, user)
        staff = _apply_mode_prefix_to_staff_note(mode, staff)
        user, staff = _enforce_mode_phrasing(mode, user, staff)

    user = _cleanup_user_state_garbage(user, mode)
    staff = _cleanup_staff_note_garbage(staff)
    user = _dedupe_sentences(user)
    staff = _dedupe_sentences(staff)

    return user.strip(), staff.strip()

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

def _should_preserve_office_raw(raw_user: str, raw_staff: str, row_text: str) -> bool:
    """
    通所の長文・具体文はChatGPTやロジック生成で潰さず、そのまま活かすための判定
    True なら raw を優先して整形だけにする
    """
    user = _normalize_text(raw_user)
    staff = _normalize_text(raw_staff)
    row = _normalize_text(row_text)
    merged = " ".join([user, staff, row]).strip()

    # ラベルだけの日は preserve しない
    if _has_unreliable_label(user):
        return False

    # 文量が十分あるか
    user_sent_count = len(_sentencize_jp(user))
    staff_sent_count = len(_sentencize_jp(staff))
    long_enough = (
        len(user) >= 60 or
        len(staff) >= 80 or
        user_sent_count >= 3 or
        staff_sent_count >= 3
    )

    # 具体情報があるか
    detail_keywords = [
        "来所", "挨拶", "笑顔", "元気", "遅れて", "寝坊", "病院", "受診",
        "昼食", "完食", "休憩", "帰り際", "また明日", "明日も来ます",
        "パッキン", "ホッチキス", "袋詰め", "検品", "糸くず", "マスキングテープ",
        "箸", "箸袋", "ハンドメイド", "ビーズ", "ブレスレット", "ケーキ",
        "宝くじ", "カレー", "写真", "差し入れ", "会話", "交流", "不満", "謝罪",
        "1000枚", "10本", "15本", "20個", "17個"
    ]
    has_detail = any(k in merged for k in detail_keywords)

    # 通所なのに在宅系が混じっていたら preserve しない
    ng_keywords = ["在宅にて", "作業開始の連絡", "作業終了の連絡", "電話連絡"]
    has_ng = any(k in merged for k in ng_keywords)

    return long_enough and has_detail and not has_ng

def _light_preserve_text(text: str) -> str:
    s = _normalize_text(text)
    if not s:
        return s
    s = re.sub(r'\n{2,}', '\n', s)
    s = re.sub(r'[ \t]+', ' ', s)
    s = s.replace("。。", "。")
    return s.strip()

def _force_user_state_shape(text: str, mode: str = "") -> str:
    return _normalize_text(text)

def _force_staff_note_shape(text: str, mode: str = "") -> str:
    return _normalize_text(text)

def _detect_service_mode(row_text: str, work_text: str = "", user_text: str = "", staff_text: str = "") -> str:
    row_src = _normalize_text(row_text)
    work_src = _normalize_text(work_text)
    user_src = _normalize_text(user_text)
    staff_src = _normalize_text(staff_text)
    src = " ".join([row_src, work_src, user_src, staff_src])

    # 1. 通所の明確な根拠を最優先
    has_tsuusho = "通所" in row_src
    has_time_range = bool(re.search(r'\d{1,2}:\d{2}\s*〜\s*\d{1,2}:\d{2}', row_src))
    has_meal = any(k in row_src for k in ["食事あり", "昼食あり", "昼食提供", "食事提供"])

    if has_tsuusho and has_meal:
        return "通所"

    if has_tsuusho and has_time_range:
        if has_meal:
            return "通所"
        return "在宅"

    # 2. 施設外は「施設外」という明示があるときだけ
    outside_keywords = [
        "施設外", "施設外就労", "施設外支援"
    ]
    if any(k in src for k in outside_keywords):
        return "施設外"

    # 2. 「通所」+ 時間帯あり の Knowbe行は、
    #    食事あり がなければ在宅判定にする（ゆー運用の最重要ルール）
    has_tsuusho = "通所" in row_src
    has_time_range = bool(re.search(r'\d{1,2}:\d{2}\s*〜\s*\d{1,2}:\d{2}', row_src))
    has_meal = any(k in row_src for k in ["食事あり", "昼食あり", "昼食提供", "食事提供"])

    if has_tsuusho and has_time_range:
        if has_meal:
            return "通所"
        return "在宅"

    # 3. 在宅の強い根拠
    remote_keywords = [
        "在宅",
        "在宅利用",
        "作業開始の連絡",
        "作業終了の連絡",
        "開始前に連絡",
        "終了時に連絡",
        "電話連絡",
        "自宅にて",
        "自宅で作業"
    ]
    if any(k in src for k in remote_keywords):
        return "在宅"

    # 4. 通所の強い根拠（食事あり以外の補助判定）
    office_keywords = [
        "来所", "来られ", "来室", "来訪",
        "作業場に来", "作業場へ来",
        "時間通りに来所", "定刻通りに来所",
        "予定通り来所", "帰宅", "退所"
    ]
    if any(k in src for k in office_keywords):
        return "通所"

    # 5. その他に在宅利用があれば在宅優先
    if "在宅利用" in row_src:
        return "在宅"

    # 6. デフォルトは在宅寄りにしておく
    return "在宅"

def _apply_mode_prefix_to_user_state(mode: str, user_state: str) -> str:
    text = _normalize_text(user_state)
    if not text:
        return text

    text = _strip_unwanted_words(text)
    text = _remove_status_labels(text)

    text = text.replace("合同会社エバーグリーン", "")
    text = text.replace("居酒屋 琴", "")
    text = text.replace("居酒屋琴", "")

    if mode == "施設外":
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
            text = re.sub(pat, '', text)

    text = re.sub(r'\s+', ' ', text).strip()
    text = _cleanup_user_state_garbage(text, mode)
    return text


def _apply_mode_prefix_to_staff_note(mode: str, staff_note: str) -> str:
    text = _normalize_text(staff_note)
    if not text:
        return text

    text = text.replace("合同会社エバーグリーン", "")
    text = text.replace("居酒屋 琴", "")
    text = text.replace("居酒屋琴", "")

    text = _cleanup_staff_note_garbage(text)
    return text

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
            r'作業開始の連絡があり[^。]*。?',
            r'作業終了の連絡があり[^。]*。?',
            r'開始前に連絡があり[^。]*。?',
            r'終了時に連絡があり[^。]*。?',
            r'電話で[^。]*作業開始[^。]*。?',
            r'電話連絡[^。]*。?',
            r'施設外就労[^。]*。?',
            r'施設外支援[^。]*。?',
        ]
        for pat in forbidden_user:
            user = re.sub(pat, '', user)

        forbidden_staff = [
            r'在宅[^。]*。?',
            r'作業開始の連絡があり[^。]*。?',
            r'作業終了の連絡があり[^。]*。?',
            r'電話連絡[^。]*。?',
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
            r'作業終了の連絡があり[^。]*。?',
            r'開始前に連絡があり[^。]*。?',
            r'終了時に連絡があり[^。]*。?',
            r'電話連絡[^。]*。?',
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
            r'作業開始の連絡があり[^。]*。?',
            r'作業終了の連絡があり[^。]*。?',
            r'電話連絡[^。]*。?',
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

        row_data = {
            "利用者状態": raw_user,
            "職員考察": raw_staff,
            "作業": work,
            "row_text": _normalize_text(block.get("all_text", "")),
        }

        has_label = _has_unreliable_label(raw_user)
        mode = _detect_service_mode(
            row_data.get("row_text", ""),
            row_data.get("作業", ""),
            row_data.get("利用者状態", ""),
            row_data.get("職員考察", ""),
        )

        user_state = _normalize_text((content or {}).get("user_state", ""))
        staff_note = _normalize_text((content or {}).get("staff_note", ""))

        all_text = " ".join([work, raw_user, raw_staff])
        is_outside_day = (
            mode == "施設外"
            and any(k in all_text for k in [
                "施設外就労", "清掃", "居酒屋", "琴", "エバーグリーン"
            ])
        )

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

        # 通所ロジック
        if mode == "通所":
            preserve_raw = _should_preserve_office_raw(
                raw_user,
                raw_staff,
                row_data.get("row_text", ""),
            )

            if preserve_raw:
                user_state = _light_preserve_text(raw_user)
                staff_note = _light_preserve_text(raw_staff)
            elif has_label:
                user_state = _build_office_user_state(row_data)
                staff_note = _build_office_staff_note(row_data)
            else:
                user_state = _normalize_text(user_state)
                staff_note = _normalize_text(staff_note)

                ng_words = ["在宅", "在宅利用", "作業開始の連絡", "作業開始の電話", "電話で作業開始"]
                if any(w in user_state for w in ng_words) or any(w in staff_note for w in ng_words):
                    user_state = _build_office_user_state(row_data)
                    staff_note = _build_office_staff_note(row_data)

        if mode == "在宅":
            user_state = _fix_japanese_artifacts(_normalize_text(user_state))
            staff_note = _fix_japanese_artifacts(_normalize_text(staff_note))

            user_state = _cleanup_user_state_garbage(user_state, "在宅")
            staff_note = _cleanup_staff_note_garbage(staff_note)

            user_state = _dedupe_sentences(user_state)
            staff_note = _dedupe_sentences(staff_note)

        if mode in ["通所", "施設外"]:
            user_state, staff_note = _finalize_non_home_mode(
                mode=mode,
                user_state=user_state,
                staff_note=staff_note,
                raw_user=raw_user,
                raw_staff=raw_staff,
                work_label=work,
                row_text=row_data.get("row_text", ""),
            )

        fixed[date_str] = {
            "user_state": user_state,
            "staff_note": staff_note,
            "preserve_raw": bool(mode == "通所" and preserve_raw) if mode == "通所" else False,
        }

    for day, block in blocks.items():
        key = f"{year:04d}-{month:02d}-{day:02d}"
        if key in fixed:
            continue

        work = _normalize_text(block.get("work", ""))
        raw_user = _normalize_text(block.get("user_state_raw", ""))
        raw_staff = _normalize_text(block.get("staff_note_raw", ""))

        row_data = {
            "利用者状態": raw_user,
            "職員考察": raw_staff,
            "作業": work,
            "row_text": _normalize_text(block.get("all_text", "")),
        }

        has_label = _has_unreliable_label(raw_user)
        mode = _detect_service_mode(
            row_data.get("row_text", ""),
            row_data.get("作業", ""),
            row_data.get("利用者状態", ""),
            row_data.get("職員考察", ""),
        )

        all_text = " ".join([work, raw_user, raw_staff])

        rebuilt_user = _compose_user_state_from_raw(work, raw_user, raw_staff)
        rebuilt_staff = raw_staff

        is_outside_day = (
            mode == "施設外"
            and any(k in all_text for k in [
                "施設外就労", "清掃", "居酒屋", "琴", "エバーグリーン"
            ])
        )
        if is_outside_day:
            rebuilt_user = rebuilt_user.replace("合同会社エバーグリーン", "")
            rebuilt_user = rebuilt_user.replace("居酒屋 琴", "")
            rebuilt_user = rebuilt_user.replace("居酒屋琴", "")
            rebuilt_staff = rebuilt_staff.replace("合同会社エバーグリーン", "")
            rebuilt_staff = rebuilt_staff.replace("居酒屋 琴", "")
            rebuilt_staff = rebuilt_staff.replace("居酒屋琴", "")

            rebuilt_user = re.sub(r'^.*?での作業として、', '', rebuilt_user)
            rebuilt_staff = re.sub(r'^.*?での作業として、', '', rebuilt_staff)

        # fallback側にも通所ロジックを同じように適用
        if mode == "通所":
            preserve_raw = _should_preserve_office_raw(
                raw_user,
                raw_staff,
                row_data.get("row_text", ""),
            )

            if preserve_raw:
                rebuilt_user = _light_preserve_text(raw_user)
                rebuilt_staff = _light_preserve_text(raw_staff)
            elif has_label:
                rebuilt_user = _build_office_user_state(row_data)
                rebuilt_staff = _build_office_staff_note(row_data)
            else:
                rebuilt_user = _normalize_text(rebuilt_user)
                rebuilt_staff = _normalize_text(rebuilt_staff)

                ng_words = ["在宅", "在宅利用", "作業開始の連絡", "作業開始の電話", "電話で作業開始"]
                if any(w in rebuilt_user for w in ng_words) or any(w in rebuilt_staff for w in ng_words):
                    rebuilt_user = _build_office_user_state(row_data)
                    rebuilt_staff = _build_office_staff_note(row_data)

        if mode == "在宅":
            rebuilt_user = _fix_japanese_artifacts(_normalize_text(rebuilt_user))
            rebuilt_staff = _fix_japanese_artifacts(_normalize_text(rebuilt_staff))

            rebuilt_user = _cleanup_user_state_garbage(rebuilt_user, "在宅")
            rebuilt_staff = _cleanup_staff_note_garbage(rebuilt_staff)

            rebuilt_user = _dedupe_sentences(rebuilt_user)
            rebuilt_staff = _dedupe_sentences(rebuilt_staff)

        if mode in ["通所", "施設外"]:
            rebuilt_user, rebuilt_staff = _finalize_non_home_mode(
                mode=mode,
                user_state=rebuilt_user,
                staff_note=rebuilt_staff,
                raw_user=raw_user,
                raw_staff=raw_staff,
                work_label=work,
                row_text=row_data.get("row_text", ""),
            )

        fixed[key] = {
            "user_state": rebuilt_user,
            "staff_note": rebuilt_staff,
            "preserve_raw": bool(mode == "通所" and preserve_raw)
        }

    return fixed

def _extract_condition(text: str) -> str:
    s = _normalize_text(text)

    # まずは後半の自然文から拾う
    if any(k in s for k in ["頭痛", "しんどい", "体調が悪い", "体調不良", "優れず", "優れない", "眠れない", "眠りが浅い", "落ち込んでいる", "気分が乗らない", "寒くて"]):
        return "あまり良くない状態"

    if any(k in s for k in ["良好", "元気", "調子がよい", "調子が良い", "落ち着いている", "大きな変化なく", "変わりなく", "安定している"]):
        return "大きな変化はなく落ち着いている"

    # 初期ラベル系
    if "体調不安定" in s or "精神不安定" in s:
        return "あまり良くない状態"
    if "体調安定" in s or "精神安定" in s:
        return "大きな変化はなく落ち着いている"

    # 何も取れないときの保険
    return "大きな変化はなく落ち着いている"


def _extract_quote(text: str) -> str:
    if "とのこと" in text:
        return text.split("とのこと")[0]
    return "今日は体調は普通です"


def _estimate_quantity(work: str, minutes: int) -> str:
    if "塗り絵" in work:
        return "塗り絵を1枚"
    if "観葉植物" in work:
        return "観葉植物への水やり"
    # 内職 fallback
    num = max(1, minutes // 30)
    return f"ハンドメイドを{num}個"

def _build_home_user_state(row):
    text = row.get("利用者状態", "")
    work = row.get("作業", "")

    cond = _extract_condition(text)
    quote = _extract_quote(text)
    qty = _estimate_quantity(work, 120)

    opening = random.choice(OPENINGS_HOME)

    return (
        f"{opening}{cond}との様子が確認された。 "
        f"「{quote}」と話されていた。 "
        f"{cond}であり、無理のない範囲で過ごされている様子であった。 "
        f"作業終了の連絡では、「{qty.replace('1枚','一枚')}やりました」と報告があった。"
    )

def _build_home_staff_note(row):
    text = row.get("利用者状態", "")
    work = row.get("作業", "")

    cond = _extract_condition(text)
    qty = _estimate_quantity(work, 120)

    support = random.choice(SUPPORT_PATTERNS)
    future = random.choice(FUTURE_PATTERNS)

    return (
        f"{cond}であったため、{support} "
        f"{qty.replace('1枚','一枚')}の実施が確認され、その日の状態に応じて作業ができている様子であった。 "
        f"{future}"
    )

def _build_outside_user_state(row):
    text = _normalize_text(row.get("利用者状態", ""))
    cond = _extract_condition(text)
    opening = random.choice(OUTSIDE_OPENINGS)

    # 施設外は清掃固定。数量化しない。内職・箱補完もしない。
    return (
        f"{opening}体調に{cond}との報告があった。 "
        f"廊下の掃き掃除や手すり拭きなどの清掃作業に取り組まれた。 "
        f"体調に{cond}であったが、無理のない範囲で作業に取り組まれていた。"
    )

def _build_outside_staff_note(row):
    text = _normalize_text(row.get("利用者状態", ""))
    cond = _extract_condition(text)

    support = random.choice(OUTSIDE_SUPPORT)
    future = random.choice(OUTSIDE_FUTURE)

    if "あまり良くない" in cond or "不調" in cond:
        middle = "体調に波がある中でも、できる範囲で作業に取り組まれていた。"
    else:
        middle = "無理のない範囲で安定して作業に取り組まれていた。"

    return (
        f"体調に{cond}が見られたが、{support} "
        f"{middle} "
        f"{future}"
    )

def _build_outside_month_result_json(page_text: str, year: int, month: int) -> dict:
    blocks = _split_support_record_blocks(page_text)
    out = {}

    for day, block in blocks.items():
        key = f"{year:04d}-{month:02d}-{day:02d}"

        row_data = {
            "利用者状態": _normalize_text(block.get("user_state_raw", "")),
            "職員考察": _normalize_text(block.get("staff_note_raw", "")),
            "作業": _normalize_text(block.get("work", "")),
            "row_text": _normalize_text(block.get("all_text", "")),
        }

        out[key] = {
            "user_state": _build_outside_user_state(row_data),
            "staff_note": _build_outside_staff_note(row_data),
        }

    return out

def _has_unreliable_label(text: str) -> bool:
    s = _normalize_text(text)
    labels = ["精神安定", "体調安定", "体調良好", "精神不安定", "体調不安定"]
    return any(l in s for l in labels)

OFFICE_OPENINGS = [
    "作業開始時には、",
    "通所後、体調を確認すると、",
    "作業に入る前に体調を確認すると、",
]

def _office_condition_phrase(user_text: str, staff_text: str) -> str:
    src = _normalize_text(user_text + " " + staff_text)

    if any(k in src for k in ["しんどい", "体調不良", "優れず", "優れない", "疲れて", "寝不足", "浮き沈み", "不安定"]):
        return "今日は少ししんどい"
    if any(k in src for k in ["元気", "良好", "安定", "落ち着いて", "意欲的", "明るく"]):
        return "体調は大きな変化なく落ち着いている"
    return "体調は大きな変化なく落ち着いている"

OFFICE_GREETING_PATTERNS = [
    "部屋に入ってきて挨拶をされた。",
    "来所後、落ち着いた様子で挨拶をされた。",
    "入室後、職員へ挨拶をされてから席につかれた。",
]

OFFICE_CONFIRM_PATTERNS = [
    "体調を尋ねると、「{cond}」との報告があった。",
    "体調について確認すると、「{cond}」と話された。",
    "その日の体調を確認したところ、「{cond}」とのことだった。",
]

OFFICE_WORKINTRO_PATTERNS = [
    "作業を始める前に、{work}の流れについて職員から説明があり、自分の担当を確認されていた。",
    "開始前には、{work}の進め方について説明があり、担当する内容を確認されていた。",
    "作業前に、{work}の流れや役割について説明があり、自分の担当を把握されていた。",
]

OFFICE_DOING_PATTERNS = [
    "作業が始まると、{work}に落ち着いて取り組まれていた。",
    "作業開始後は、{work}に集中して取り組まれていた。",
    "実際の作業に入ると、{work}を自分のペースで進められていた。",
]

OFFICE_FIRST_PATTERNS_GOOD = [
    "挨拶の声や表情からは落ち着いた様子が伝わってきた。また本人からも「{cond}」との話があった。",
    "来所時の様子は安定しており、挨拶の声からも落ち着きが感じられた。また本人からも「{cond}」との話があった。",
    "入室時の表情や挨拶の様子からは大きな変化は見られず、本人からも「{cond}」との話があった。",
]

OFFICE_FIRST_PATTERNS_BAD = [
    "挨拶の様子からも、今日は少ししんどい様子がうかがえた。また本人からも「{cond}」との話があった。",
    "来所時の表情や声の調子からは、少ししんどさがあるように感じられた。また本人からも「{cond}」との話があった。",
    "入室時の様子からは体調面の負担も感じられ、本人からも「{cond}」との話があった。",
]

OFFICE_SECOND_PATTERNS = [
    "{work}は問題なく進められており、その日の流れに沿って落ち着いて取り組めていた。",
    "{work}は大きな混乱なく進められており、自分のペースを保ちながら取り組めていた。",
    "{work}は安定して進められており、その日の流れを確認しながら取り組めていた。",
]

OFFICE_FUTURE_PATTERNS = [
    "今後も作業に取り組んでもらえるよう、体調や精神面に配慮しながら支援を続けていく。",
    "今後も安心して作業に取り組めるよう、体調や気分の変化に配慮しながら支援していく。",
    "今後も無理なく通所と作業が続けられるよう、体調面や精神面に配慮して支援していく。",
]

def _build_office_user_state(row):
    user_text = _normalize_text(row.get("利用者状態", ""))
    staff_text = _normalize_text(row.get("職員考察", ""))
    work_text = _normalize_text(row.get("作業", ""))

    cond = _office_condition_phrase(user_text, staff_text)
    work_label = work_text if work_text else "内職"

    greet = random.choice(OFFICE_GREETING_PATTERNS)
    confirm = random.choice(OFFICE_CONFIRM_PATTERNS).format(cond=cond)
    intro = random.choice(OFFICE_WORKINTRO_PATTERNS).format(work=work_label)
    doing = random.choice(OFFICE_DOING_PATTERNS).format(work=work_label)

    return f"{greet} {confirm} {intro} {doing}"

def _build_office_staff_note(row):
    user_text = _normalize_text(row.get("利用者状態", ""))
    staff_text = _normalize_text(row.get("職員考察", ""))
    work_text = _normalize_text(row.get("作業", ""))

    cond = _office_condition_phrase(user_text, staff_text)
    work_label = work_text if work_text else "内職"

    if "少ししんどい" in cond:
        first_sentence = random.choice(OFFICE_FIRST_PATTERNS_BAD).format(cond=cond)
    else:
        first_sentence = random.choice(OFFICE_FIRST_PATTERNS_GOOD).format(cond=cond)

    second_sentence = random.choice(OFFICE_SECOND_PATTERNS).format(work=work_label)
    third_sentence = random.choice(OFFICE_FUTURE_PATTERNS)

    return f"{first_sentence} {second_sentence} {third_sentence}"

# =========================================
# 1ヶ月処理
# =========================================
def process_one_month(
    driver,
    resident_name,
    year,
    month,
    exec_id,
    user,
    company,
    outside_workplace="",
    live_status_box=None,
    progress_callback=None,
):
    from run_assistance import (
        goto_support_record_month,
        fetch_support_record_page_text,
        enter_edit_mode,
        save_all,
    )

    ym = f"{year}-{month:02d}"

    def step(msg, level="info"):
        if progress_callback:
            progress_callback(msg, level)

    try:
        step("月ページへ移動中")
        ok = goto_support_record_month(driver, year, month)
        if not ok:
            _update_live_status(live_status_box, f"月移動失敗: {resident_name} / {ym}", "error")
            return {"result": "エラー", "count": 0, "message": "月移動失敗"}

        time.sleep(1.0)

        step("支援記録本文を取得中")
        page_text = fetch_support_record_page_text(driver) or ""
        page_text_str = _normalize_text(page_text)

        if not page_text_str:
            _update_live_status(live_status_box, f"支援記録取得失敗: {resident_name} / {ym}", "error")
            return {"result": "エラー", "count": 0, "message": "支援記録取得失敗"}

        blocks = _split_support_record_blocks(page_text_str)
        step(f"支援記録を取得しました。対象日数: {len(blocks)}日")

        result_json = {}

        for day, block in sorted(blocks.items()):
            day_key = f"{year:04d}-{month:02d}-{day:02d}"
            day_text = _normalize_text(block.get("all_text", ""))

            print(f"[JR-DAY] processing {resident_name} {day_key}", flush=True)
            step(f"{day_key} ChatGPT生成中")

            day_json = generate_json_with_gemini_one_day(
                day_key=day_key,
                day_text=day_text,
                outside_workplace=outside_workplace,
            )

            step(f"{day_key} ChatGPT生成完了")

            if day_key in day_json:
                result_json[day_key] = day_json[day_key]
            else:
                result_json[day_key] = {
                    "user_state": "",
                    "staff_note": "",
                    "preserve_raw": False,
                }

        step("生成結果を後処理中")
        result_json = _postprocess_gemini_result(
            page_text_str,
            result_json,
            year,
            month,
            outside_workplace
        )

        if not result_json:
            append_journal_log({
                "exec_id": exec_id,
                "exec_time": _now_str(),
                "user": user,
                "company": company,
                "resident_name": resident_name,
                "target_month": ym,
                "result": "スキップ",
                "count": 0,
                "message": "対象月の支援記録がないか、JSON生成結果が空でした"
            })
            _update_live_status(live_status_box, f"スキップ: {resident_name} / {ym}", "warning")
            return {"result": "スキップ", "count": 0, "message": "対象月の支援記録なし"}

        step("Knowbe編集モードへ移行中")
        ok = enter_edit_mode(driver)
        if not ok:
            _update_live_status(live_status_box, f"編集モード失敗: {resident_name} / {ym}", "error")
            return {"result": "エラー", "count": 0, "message": "編集モード失敗"}

        success_count = 0

        for date_str, content in sorted(result_json.items()):
            try:
                step(f"{date_str} Knowbeへ入力中")
                print(f"[FIX] 入力対象: {date_str}", flush=True)

                m = re.match(r"(\d{4})-(\d{2})-(\d{2})", str(date_str))
                if not m:
                    continue

                d = int(m.group(3))
                target_label = f"{d}日"

                table_rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")

                hit_row = None
                for row in table_rows:
                    try:
                        row_text_full = _normalize_text(row.text)
                        if re.match(rf"^{d}日（", row_text_full):
                            hit_row = row
                            break
                    except Exception:
                        continue

                if hit_row is None:
                    raise RuntimeError(f"対象行が見つかりません: {target_label}")

                areas = _find_row_textareas_for_support_record(hit_row)
                if len(areas) < 2:
                    raise RuntimeError(f"textarea不足: {target_label}")

                user_state_el = areas[0]
                staff_note_el = areas[1]

                before_user = _textarea_value(user_state_el)
                before_staff = _textarea_value(staff_note_el)
                row_text = _normalize_text(hit_row.text)

                work_label = ""
                work_match = re.search(r"作業\s*(.+?)\s*利用者状態", row_text.replace("\n", " "), re.DOTALL)
                if work_match:
                    work_label = _normalize_text(work_match.group(1))
                else:
                    work_label = "作業"

                mode = _detect_service_mode(
                    row_text=row_text,
                    work_text=work_label,
                    user_text=before_user,
                    staff_text=before_staff,
                )

                registered_tasks_text = ""

                if mode == "施設外" and str(outside_workplace or "").strip():
                    registered_tasks_text = _pick_outside_registered_tasks(outside_workplace)
                    if registered_tasks_text:
                        work_label = registered_tasks_text
                        print(f"[OUTSIDE_TASK] selected = {work_label}", flush=True)

                final_user_state = _normalize_text((content or {}).get("user_state", ""))
                final_staff_note = _normalize_text((content or {}).get("staff_note", ""))
                preserve_raw = bool((content or {}).get("preserve_raw", False))

                gemini_is_weak = (
                    not final_user_state
                    or _looks_like_short_health_only(final_user_state)
                    or len(_sentencize_jp(final_user_state)) < 2
                )

                if (gemini_is_weak or _is_health_only_user_state(final_user_state)) and not preserve_raw:
                    if mode == "在宅":
                        rebuilt_user, rebuilt_staff = _rebuild_home_record_strict(
                            raw_user="",
                            raw_staff=before_staff,
                            work_label=work_label,
                            row_text=row_text,
                        )
                        if rebuilt_user:
                            final_user_state = rebuilt_user
                        if rebuilt_staff and not final_staff_note:
                            final_staff_note = rebuilt_staff
                    else:
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

                if preserve_raw:
                    final_user_state = _light_preserve_text(final_user_state or before_user)
                    final_staff_note = _light_preserve_text(final_staff_note or before_staff)
                else:
                    final_user_state = _normalize_text(final_user_state)
                    final_staff_note = _normalize_text(final_staff_note)

                    final_user_state = _apply_mode_prefix_to_user_state(mode, final_user_state)
                    final_staff_note = _apply_mode_prefix_to_staff_note(mode, final_staff_note)

                    final_user_state, final_staff_note = _enforce_mode_phrasing(
                        mode, final_user_state, final_staff_note
                    )

                    allow_zero = _contains_explicit_no_work_reason(
                        " ".join([final_user_state, final_staff_note])
                    )
                    final_user_state = _normalize_work_quantity_phrase(final_user_state, work_label)
                    final_user_state = _convert_ambiguous_quantity_to_one_or_more(
                        final_user_state, work_label, allow_zero
                    )
                    final_user_state = _append_default_quantity_if_missing(
                        final_user_state, work_label, allow_zero
                    )

                bad_prefixes = [
                    r'^在宅での取り組み状況を踏まえ、',
                    r'^施設外での作業状況を踏まえ、',
                    r'^施設外就労先での状況を踏まえ、',
                    r'^の取り組み状況を踏まえ、',
                ]
                for pat in bad_prefixes:
                    final_user_state = re.sub(pat, '', final_user_state)
                    final_staff_note = re.sub(pat, '', final_staff_note)

                final_user_state = re.sub(r'\d{1,2}日（[^）]+）。?', '', final_user_state)
                final_user_state = re.sub(r'作業。[^。]*。', '', final_user_state)
                final_user_state = re.sub(r'面談。[^。]*。', '', final_user_state)
                final_user_state = re.sub(r'その他。[^。]*。', '', final_user_state)

                final_staff_note = re.sub(r'\d{1,2}日（[^）]+）。?', '', final_staff_note)
                final_staff_note = re.sub(r'作業。[^。]*。', '', final_staff_note)
                final_staff_note = re.sub(r'面談。[^。]*。', '', final_staff_note)
                final_staff_note = re.sub(r'その他。[^。]*。', '', final_staff_note)

                if mode == "施設外":
                    final_user_state = re.sub(r'^\s*にて', '', final_user_state)
                    final_user_state = re.sub(r'。+\s*にて', '。', final_user_state)
                    final_user_state = re.sub(r'施設外就労の\s*', '', final_user_state)
                    final_user_state = re.sub(r'(^|。)\s*での作業として、', r'\1', final_user_state)
                    final_user_state = re.sub(r'(^|。)\s*での', r'\1', final_user_state)
                    final_user_state = final_user_state.replace("清掃作業作業", "清掃作業")

                    if registered_tasks_text:
                        task_line = f"{registered_tasks_text}を行う予定とのことだった。"

                        old_line_pattern = (
                            r"[^。\n]*"
                            r"(通路清掃|手すり拭き|共用部の清掃|清掃作業|清掃)"
                            r"[^。\n]*(予定|取り組む予定|行う予定)"
                            r"[^。\n]*。"
                        )

                        if re.search(old_line_pattern, final_user_state):
                            final_user_state = re.sub(
                                old_line_pattern,
                                task_line,
                                final_user_state,
                                count=1
                            )
                        else:
                            insert_marker = "職員より"
                            if insert_marker in final_user_state:
                                parts = final_user_state.split("。")
                                new_parts = []
                                inserted = False
                                for p in parts:
                                    p = p.strip()
                                    if not p:
                                        continue
                                    new_parts.append(p + "。")
                                    if (not inserted) and "職員より" in p:
                                        new_parts.append(task_line)
                                        inserted = True
                                final_user_state = "\n".join(new_parts)
                            else:
                                final_user_state = final_user_state.rstrip("。") + "。\n" + task_line

                        # AIが勝手に作った登録外ワードを消す
                        final_user_state = final_user_state.replace("共用部の清掃", registered_tasks_text)
                        final_user_state = final_user_state.replace("通路清掃や手すり拭き", registered_tasks_text)

                # 改行は残し、行内の余分な空白だけ整える
                final_user_state = "\n".join(
                    re.sub(r"[ \t　]+", " ", line).strip()
                    for line in str(final_user_state).splitlines()
                    if line.strip()
                )

                final_staff_note = "\n".join(
                    re.sub(r"[ \t　]+", " ", line).strip()
                    for line in str(final_staff_note).splitlines()
                    if line.strip()
                )

                final_user_state = _fix_japanese_artifacts(final_user_state)
                final_staff_note = _fix_japanese_artifacts(final_staff_note)

                final_user_state = _force_end_sentence_order(final_user_state, work_label)

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
                    step(f"{date_str} 入力完了")
                    _update_live_status(
                        live_status_box,
                        f"成功: {resident_name} / {ym} / {target_label} / {mode}",
                        "success"
                    )
                else:
                    raise RuntimeError(
                        f"入力反映失敗: {target_label} / "
                        f"user_match={after_user == str(final_user_state).strip()} / "
                        f"staff_match={after_staff == str(final_staff_note).strip()}"
                    )

            except Exception as e:
                print(f"[JR] 日付処理失敗: {date_str} -> {e}", flush=True)
                step(f"{date_str} 入力失敗: {e}", "error")
                _update_live_status(
                    live_status_box,
                    f"失敗: {resident_name} / {ym} / {date_str} / {e}",
                    "error"
                )

        print("[FIX] 保存開始", flush=True)
        step("Knowbe保存中")
        save_all(driver)
        step(f"保存完了: {success_count}件更新")
        print(f"[FIX] 完了 件数={success_count}", flush=True)

        append_journal_log({
            "exec_id": exec_id,
            "exec_time": _now_str(),
            "user": user,
            "company": company,
            "resident_name": resident_name,
            "target_month": ym,
            "result": "成功",
            "count": success_count,
            "message": f"{success_count}件更新"
        })

        return {
            "result": "成功",
            "count": success_count,
            "message": f"{success_count}件更新",
        }

    except Exception as e:
        print(f"[JR] month error: {resident_name} {ym} -> {e}", flush=True)
        append_journal_log({
            "exec_id": exec_id,
            "exec_time": _now_str(),
            "user": user,
            "company": company,
            "resident_name": resident_name,
            "target_month": ym,
            "result": "エラー",
            "count": 0,
            "message": str(e),
        })
        _update_live_status(
            live_status_box,
            f"月処理失敗: {resident_name} / {ym} / {e}",
            "error"
        )
        return {
            "result": "エラー",
            "count": 0,
            "message": str(e),
        }

def _extract_piecework_steps_from_memo(memo: str) -> str:
    memo = _normalize_text(memo)

    marker = "本日実施した工程："
    if marker not in memo:
        return ""

    after = memo.split(marker, 1)[1].strip()
    lines = []

    for line in after.splitlines():
        line = line.strip()
        if not line:
            continue

        # 次の入力項目っぽい行に入ったら止める
        if line.startswith("①") or line.startswith("②") or line.startswith("③") or line.startswith("④") or line.startswith("⑤") or line.startswith("⑥"):
            break

        lines.append(line)

    return "\n".join(lines).strip()


def _build_piecework_step_sentence(piecework_name: str, steps_text: str) -> str:
    piecework_name = _normalize_text(piecework_name)
    steps_text = _normalize_text(steps_text)

    if not steps_text:
        return ""

    step_names = []
    for line in steps_text.splitlines():
        line = line.strip(" ・\n\r\t")
        if not line:
            continue

        # "1. クリップの組み立て（詳細）" → "クリップの組み立て"
        if "." in line:
            line = line.split(".", 1)[1].strip()

        if "（" in line:
            line = line.split("（", 1)[0].strip()

        if line:
            step_names.append(line)

    if not step_names:
        return ""

    steps_joined = "、".join(step_names)

    if piecework_name:
        return f"本日は{piecework_name}の工程のうち、{steps_joined}に取り組まれた。"

    return f"本日は{steps_joined}に取り組まれた。"

def generate_journal_from_memo(memo: str, work_label: str, start_time: str = "", end_time: str = ""):
    """
    メモから日誌を生成する（最終整形ルート）
    通所は「内職内容＝作業」として原文を最大限残す
    """
    memo = _normalize_text(memo)

    # 🔥 選択された内職工程だけを抽出
    piecework_steps_text = _extract_piecework_steps_from_memo(memo)

    # 🔥 超重要：作業名は work_label 優先
    work = _normalize_text(work_label) or memo

    # 🔥 工程が選ばれている場合、作業名に全工程ではなく「選択工程だけ」を反映
    piecework_step_sentence = _build_piecework_step_sentence(work, piecework_steps_text)

    mode = _detect_service_mode(
        row_text=memo,
        work_text=work,
        user_text=memo,
        staff_text=memo,
    )

    row_data = {
        "利用者状態": memo,
        "職員考察": memo,
        "作業": work,
        "row_text": memo,
    }

    if mode == "在宅":
        user_state = _build_home_user_state(row_data)
        staff_note = _build_home_staff_note(row_data)
        user_state, staff_note = _force_final_home_format(user_state, staff_note, memo, work)

    elif mode == "施設外":
        user_state = _build_outside_user_state(row_data)
        staff_note = _build_outside_staff_note(row_data)
        user_state, staff_note = _force_final_outside_format(user_state, staff_note, memo, work)

    else:
        # ===== 通所 =====
        user_state = _build_office_user_state(row_data)
        staff_note = _build_office_staff_note(row_data)

        # 🔥 内職内容を強制反映（AIの丸め防止）
        if work:
            lines = user_state.split("。")
            new_lines = []
            inserted = False

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # 作業系の行を検出して置き換え
                if (not inserted) and ("作業" in line or "取り組" in line):
                    new_lines.append(piecework_step_sentence or f"本日は{work}に取り組まれた。")
                    inserted = True
                else:
                    new_lines.append(line + "。")

            # 作業文がなかった場合は追加
            if not inserted:
                new_lines.insert(1, piecework_step_sentence or f"本日は{work}に取り組まれた。")
            user_state = "\n".join(new_lines)

        user_state, staff_note = _force_final_office_format(user_state, staff_note, memo, work)

    # 🔥 在宅・通所どちらでも、選択工程がある場合は「選んだ工程だけ」を最終文に反映
    if piecework_step_sentence and piecework_step_sentence not in user_state:
        user_state = user_state.strip()
        if user_state and not user_state.endswith("。"):
            user_state += "。"
        user_state += "\n" + piecework_step_sentence

    if piecework_steps_text:
        support_step_sentence = "登録された工程のうち、本日選択された工程に沿って作業内容を確認した。"
        if support_step_sentence not in staff_note:
            staff_note = staff_note.strip()
            if staff_note and not staff_note.endswith("。"):
                staff_note += "。"
            staff_note += "\n" + support_step_sentence

    user_state = _final_cleanup_journal_text(user_state)
    staff_note = _final_cleanup_journal_text(staff_note)

    return {
        "user_state": user_state,
        "staff_note": staff_note,
        "mode": mode
    }

def _is_empty_work_label(work: str) -> bool:
    w = _normalize_text(work)
    return (not w) or w in ["未指定", "作業", "-", "なし"]


def _infer_home_work_label(source: str, fallback: str = "") -> str:
    src = _normalize_text(source)
    fb = _normalize_text(fallback)

    if not _is_empty_work_label(fb):
        return fb

    known = [
        "箱折り", "仕分け", "シール貼り", "チラシ折り", "袋詰め",
        "観葉植物の水やり", "観葉植物", "塗り絵", "内職",
        "箸入れ", "お箸", "折り鶴"
    ]

    hits = []
    for k in known:
        if k in src and k not in hits:
            hits.append(k)

    if hits:
        return "、".join(hits[:3])

    return "作業"

def _format_home_work_result_naturally(work_label: str, source: str = "") -> str:
    """
    在宅用：作業内容＋数量を自然文にする。
    例：
    塗り絵をする + 1枚 → 塗り絵を1枚行った
    観葉植物への水やり + 1回 → 観葉植物への水やりを1回行った
    """

    w = _normalize_text(work_label)
    src = _normalize_text(source)

    # 登録内職を優先
    matched = _match_registered_piecework(src, "home")
    if matched:
        step_text = str(matched.get("step_text", "") or "").strip()
        piecework_name = str(matched.get("piecework_name", "") or "").strip()
        quantity = str(matched.get("quantity", "") or "").strip()

        work_text = step_text or piecework_name or w or "作業"

        # 「〜をする」「〜する」系を除去して二重助詞を防ぐ
        work_text = re.sub(r"(を)?する$", "", work_text).strip()

        if quantity:
            return f"{work_text}を{quantity}行った"

        return f"{work_text}を行った"

    # フォールバック
    qty = _extract_quantity(src)

    work_text = w or "作業"
    work_text = re.sub(r"(を)?する$", "", work_text).strip()

    if qty:
        return f"{work_text}を{qty}行った"

    return f"{work_text}を行った"

def _extract_first_quote(source: str) -> str:
    src = _normalize_text(source)
    m = re.search(r"「([^」]{1,120})」", src)
    if m:
        return m.group(1).strip()
    return ""


def _extract_last_quote(source: str) -> str:
    src = _normalize_text(source)
    qs = re.findall(r"「([^」]{1,120})」", src)
    if qs:
        return qs[-1].strip()
    return ""


def _extract_home_quantity_line(source: str, work_label: str) -> str:
    src = _normalize_text(source)
    work = _infer_home_work_label(src, work_label)

    # 作業名＋数量が同じ文にある場合
    for s in _sentencize_jp(src):
        if re.search(r"\d+\s*(枚|個|膳|本|羽|袋|点)", s):
            s2 = s.rstrip("。")
            s2 = re.sub(r"^作業終了[^。]*?、", "", s2)
            s2 = s2.replace("行いました", "行った")
            s2 = s2.replace("取り組みました", "取り組んだ")
            return s2 + "。"

    # 数量なしだが作業名がある場合
    if work and work != "作業":
        return f"{work}に取り組まれたとのことだった。"

    return "作業に取り組まれたとのことだった。"


def _build_home_staff_reply(health_text: str) -> str:
    h = _normalize_text(health_text)

    if any(k in h for k in ["しんど", "不調", "頭痛", "痛", "不安定", "気分", "疲", "風邪"]):
        return "職員より「無理せず休みながらで大丈夫ですよ」と伝えた。"

    if any(k in h for k in ["良好", "安定", "調子がよ", "元気", "普通"]):
        return "職員より「無理のない範囲で、この調子で進めてください」と伝えた。"

    return "職員より「無理のない範囲で進めてください」と伝えた。"


def _build_home_end_reply(source: str) -> str:
    src = _normalize_text(source)

    if any(k in src for k in ["しんど", "不調", "頭痛", "痛", "不安定", "疲", "気分"]):
        return "職員より「無理せずできましたね」と声をかけた。"

    if any(k in src for k in ["良好", "安定", "調子がよ", "元気"]):
        return "職員より「落ち着いて取り組めていますね」と声をかけた。"

    return "職員より「無理のない範囲で取り組めていますね」と声をかけた。"

def _extract_home_context_line(source: str) -> str:
    """
    在宅記録で、必須項目ではないが残すべき近況を1文だけ拾う。
    例：通院、次回予約、花粉、外出、映画、生活上の出来事など。
    作業終了文の直前に入れる用。
    """
    src = _normalize_text(source)

    # ラベル・作業・開始終了・職員考察系は除外
    ng_words = [
        "作業開始", "作業終了", "開始の連絡", "終了の連絡",
        "観葉植物", "塗り絵", "内職", "作業量",
        "職員より", "支援", "見守り", "取り組まれていた",
        "体調安定", "精神安定", "精神不安定", "体調良好",
    ]

    keep_words = [
        "病院", "通院", "予約", "受診",
        "花粉", "くしゃみ", "鼻水", "咳",
        "映画", "外出", "買い物", "散歩",
        "家でゆっくり", "自宅で過ごす",
        "眠れ", "睡眠", "食欲",
    ]

    for s in _sentencize_jp(src):
        s = s.strip().rstrip("。")

        if not s:
            continue

        if any(ng in s for ng in ng_words):
            continue

        if any(k in s for k in keep_words):
            # 本人発言っぽい場合は自然な形にする
            q = re.search(r"「([^」]{1,120})」", s)
            if q:
                return f"また、本人より「{q.group(1).strip()}」との話があった。"

            # そのままでも自然な文にする
            s = s.replace("とのこと", "との話があった")
            s = s.replace("との事", "との話があった")
            if not s.endswith("話があった"):
                return f"また、{s}との話があった。"
            return f"また、{s}。"

    return ""

def _force_final_home_format(user_state: str, staff_note: str, memo: str, work: str):

    import re

    source = _normalize_text("\n".join([user_state or "", staff_note or "", memo or ""]))
    original = _normalize_text(memo)

    # -------------------------
    # 💥 ゴミ除去（最重要）
    # -------------------------
    original = re.sub(
        r"\d{1,2}日（.*?）\s*(通所|在宅|施設外).*?利用者状態",
        "",
        original
    )
    original = re.sub(r"職員考察.*", "", original)

    work_label = _infer_home_work_label(source, work)

    health_quote = "今日は無理のない範囲で進めます"

    # -------------------------
    # 🔥 原文重要情報抽出（重複防止版）
    # -------------------------
    def _context(src):
        keep = [
            "病院", "通院", "受診", "帰宅後", "予定", "遅くなる",
            "痛", "膝", "足", "腰", "左手", "眠れ", "花粉"
        ]

        ng = [
            "〜 作業", "作業 塗り絵", "利用者状態", "職員考察",
            "通所", "職員より", "ありがとう", "お疲れさま",
            "作業してくださり", "通院で大変な中",
            "作業終了時", "作業量", "その後は",
            "無理のない範囲", "支援していく"
        ]

        out = []

        for s in _sentencize_jp(src):
            s = str(s).strip()
            if not s:
                continue

            if any(x in s for x in ng):
                continue

            if any(k in s for k in keep):

                # 💥 ゴミヘッダー削除（ここが本体）
                s = re.sub(r".*?利用者状態", "", s)
                s = re.sub(r".*?作業\s*\S+\s*利用者状態", "", s)
                s = re.sub(r"\d{1,2}日（.*?）", "", s)

                # 💡 軽い整形
                s = s.replace("本人より", "").strip()

                if s and s not in out:
                    out.append(s.rstrip("。") + "。")

            if len(out) >= 2:
                break

        return " ".join(out)

    context_line = _context(user_state)

    # -------------------------
    # 本人発言（安定版）
    # -------------------------
    first_quote = _extract_first_quote(source) or ""

    bad_words = [
        "お疲れさま", "ありがとう", "ありがとうございました",
        "無理しない", "気をつけて", "進めてください",
        "通院で大変な中", "作業してくださり", "下さいね"
    ]

    if (not first_quote) or any(k in first_quote for k in bad_words):
        if any(k in source for k in ["良い","良好","変わりない","いつも通り"]):
            first_quote = "体調に大きな変わりはありません"
        elif any(k in source for k in ["痛","しんど","眠れ","不調","疲"]):
            first_quote = "少し体調面で気になるところがあります"
        else:
            first_quote = health_quote

    # -------------------------
    # 🔥 内職優先（絶対）
    # -------------------------
    match = _match_registered_piecework(source, "home")

    if match:
        work_label = match.get("piecework_name", work_label)
        qty = match.get("quantity", "")
    else:
        qty = _extract_quantity(source)

    if not work_label:
        work_label = "作業"

    work_text = re.sub(r"(を)?する$", "", str(work_label)).strip()

    if qty:
        end_line = f"作業終了時に連絡があり、{work_text}を{qty}行ったとの報告があった。"
    else:
        end_line = f"作業終了時に連絡があり、{work_text}を行ったとの報告があった。"

    # -------------------------
    # 利用者状態（確定）
    # -------------------------
    user_lines = [
        "作業開始前に連絡があり、体調について確認を行った。",
        f"本人より「{first_quote}」との話があった。",
        "職員より「無理のない範囲で進めてください」と伝えた。",
        "在宅での作業のため、本人からの報告にて確認した。",
        context_line,
        end_line,
        "職員より「無理なく取り組めていますね」と声をかけた。",
        "その後は無理のない範囲で過ごされるとのことだった。"
    ]

    user_lines = [x for x in user_lines if x]

    # -------------------------
    # 職員考察（固定）
    # -------------------------
    staff_lines = [
        "その日の状態に合わせて、無理のない範囲で作業に取り組まれていた。",
        "作業終了の報告も行えており、状況共有は適切に行われている。",
        "今後も本人の状態を確認しながら、無理なく続けられるよう支援していく。"
    ]

    user_result = _final_cleanup_journal_text("\n".join(user_lines))
    staff_result = _final_cleanup_journal_text("\n".join(staff_lines))

    return user_result, staff_result

def _detect_outside_place(source: str, work: str = "") -> str:
    s = _normalize_text(" ".join([source or "", work or ""]))

    if any(k in s for k in ["居酒屋", "琴", "店内", "机", "いす", "椅子", "トイレ", "キッチン", "窓ふき", "メニュー", "ゴミ出し"]):
        return "居酒屋琴"

    if any(k in s for k in ["マンション", "廊下", "手すり", "モップ", "消火器", "配電盤", "共用通路", "玄関前"]):
        return "マンション清掃"

    if "清掃" in s:
        return "清掃"

    return "施設外"


def _outside_work_sentence_by_place(source: str, work: str = "") -> str:
    place = _detect_outside_place(source, work)

    if place == "居酒屋琴":
        return (
            "就労先にて店内の清掃、机や椅子の拭き取り、トイレやキッチン周りの清掃、"
            "メニュー類の配置確認、入口周辺の掃き掃除やゴミ出しなどに取り組まれていた。"
        )

    if place == "マンション清掃":
        return (
            "就労先にて廊下の掃き掃除やモップ掛け、手すりの拭き取り、"
            "建物周囲のゴミ拾い、消火器や配電盤周辺の拭き掃除などに取り組まれていた。"
        )

    if place == "清掃":
        return "就労先にて清掃作業に取り組まれていた。"

    return f"就労先にて{_normalize_text(work) or '作業'}に取り組まれていた。"

def _force_final_outside_format(user_state: str, staff_note: str, memo: str, work: str):

    source = _normalize_text("\n".join([user_state or "", staff_note or "", memo or ""]))

    # -------------------------
    # 場所判定（ゆーのロジック使用）
    # -------------------------
    place = _detect_outside_place(source, work)

    # -------------------------
    # 本人発言（開始・終了）
    # -------------------------
    quote1 = _extract_first_quote(source) or "今日は無理のない範囲で進めます"
    quote2 = _extract_last_quote(source) or "無理のない範囲で終えました"

    # -------------------------
    # 職員返答
    # -------------------------
    if any(k in source for k in ["しんど", "不調", "痛", "疲", "不安"]):
        reply = "様子を見ながら無理のない範囲で進めてください"
    else:
        reply = "無理せずこのまま進めてください"

    # -------------------------
    # 作業内容（施設外就労先・作業詳細を優先）
    # -------------------------
    detail_text = _normalize_text(source)

    outside_detail = ""
    m = re.search(r"④数量・施設外での具体的作業内容：(.+)", detail_text)
    if m:
        outside_detail = _normalize_text(m.group(1))

    if outside_detail:
        work_line = f"{outside_detail}を行う予定とのことだった。"

    elif place == "居酒屋琴":
        work_line = "店内清掃や机・いす拭き、トイレ清掃等を行う予定とのことだった。"

    elif place == "マンション清掃":
        work_line = "通路清掃や手すり拭き、共用部の清掃を行う予定とのことだった。"

    elif place == "清掃":
        work_line = "清掃作業を中心に取り組む予定とのことだった。"

    else:
        work_line = "施設外作業に取り組む予定とのことだった。"

    # -------------------------
    # 作業の進み方
    # -------------------------
    if any(k in source for k in ["昨日より", "回復", "マシ"]):
        progress_line = "前日よりも安定して進められていたとの報告があった。"
    elif any(k in source for k in ["しんど", "不調"]):
        progress_line = "無理のないペースで進められていたとの報告があった。"
    else:
        progress_line = "一定のペースで進められていたとの報告があった。"

    # -------------------------
    # 途中対応
    # -------------------------
    middle_line = "途中で休憩を挟みながら対応されていた様子であった。"

    # -------------------------
    # 終了内容
    # -------------------------
    if any(k in source for k in ["広げ", "奥まで"]):
        finish_line = "作業終了時には範囲を広げたとの連絡があった。"
    elif any(k in source for k in ["できた", "完了"]):
        finish_line = "作業終了時には予定範囲を実施したとの連絡があった。"
    else:
        finish_line = "作業終了時には予定範囲を概ね実施したとの連絡があった。"

    # -------------------------
    # 利用者状態（改行構造）
    # -------------------------
    user_lines = [
        "作業開始前に体調確認を行うと、比較的安定しているとのことだった。",
        f"本人より「{quote1}」との話があった。",
        f"職員より「{reply}」と返答した。",
        "",
        work_line,
        progress_line,
        middle_line,
        "",
        finish_line,
        f"本人より「{quote2}」との話があった。",
    ]

    # -------------------------
    # 職員考察（分岐あり）
    # -------------------------
    if any(k in source for k in ["昨日より", "回復"]):
        opinion = "回復傾向が見られ、安定した作業ができていた。"
    elif any(k in source for k in ["しんど", "不調"]):
        opinion = "体調に配慮しながら無理のない範囲で取り組めていた。"
    else:
        opinion = "安定した状態で作業に取り組めていた。"

    staff_lines = [
        opinion,
        "無理のない範囲で継続できている。",
        "作業の質も保たれている。",
        "報告も適切である。",
        "今後も無理のない範囲で支援していく。",
    ]

    # -------------------------
    # 仕上げ
    # -------------------------
    user_result = "\n".join(user_lines).strip()
    staff_result = "\n".join(staff_lines).strip()

    user_result = _final_cleanup_journal_text(user_result)
    staff_result = _final_cleanup_journal_text(staff_result)

    return user_result, staff_result


def _extract_office_quantity(source: str) -> str:
    s = _normalize_text(source)

    # 明示数量だけ拾う。推定は禁止。
    m = re.search(r"([ぁ-んァ-ン一-龥A-Za-z0-9ー・の\s]{0,30}?)(\d+|[０-９]+)\s*(個|本|枚|袋|点|膳|セット)", s)
    if m:
        work_part = m.group(1).strip()
        qty = m.group(2).translate(str.maketrans("０１２３４５６７８９", "0123456789"))
        unit = m.group(3)
        work_part = re.sub(r"^(作業量は|作業内容は|本日は|作業終了時には|、)", "", work_part).strip()
        if work_part:
            return f"{work_part}{qty}{unit}"
        return f"{qty}{unit}"

    return ""


def _infer_office_work_label(source: str, work: str) -> str:
    s = _normalize_text(source)
    w = _normalize_text(work)

    # 「内職」「未実施」だけでは弱いので、本文から具体作業名を拾う
    candidates = [
        "スマホのクリップスタンド",
        "スマートフォンフォルダー",
        "ダニトリシート",
        "ダニ取りシート",
        "スプーン作業",
        "スプーン",
        "ビーズ",
        "箸",
        "お箸",
        "袋詰め",
        "箱折り",
        "シール貼り",
        "チラシ折り",
        "内職",
    ]

    for c in candidates:
        if c in s:
            return c

    if w and w not in ["未実施", "-", "なし"]:
        return w

    return "作業"

def _extract_piecework_quantity(source: str) -> str:
    """
    元文から数量を抽出する。
    時刻ではなく、枚・個・本・袋・点・膳・セットなどの作業数量だけ拾う。
    """
    src = _normalize_text(source)

    m = re.search(r"([0-9０-９]+)\s*(枚|個|本|袋|点|膳|セット|部|件)", src)
    if not m:
        return ""

    num = m.group(1).translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    unit = m.group(2)

    return f"{num}{unit}"


def _load_registered_piecework(work_mode: str):
    """
    work_mode:
      office = 通所内職
      home   = 在宅内職
    """
    try:
        master_df = load_db("piecework_master")
        steps_df = load_db("piecework_steps")
    except Exception as e:
        print(f"[PIECEWORK] load error: {e}", flush=True)
        return pd.DataFrame(), pd.DataFrame()

    if master_df is None or master_df.empty:
        master_df = pd.DataFrame()
    else:
        master_df = master_df.fillna("").copy()

    if steps_df is None or steps_df.empty:
        steps_df = pd.DataFrame()
    else:
        steps_df = steps_df.fillna("").copy()

    for col in ["id", "company_id", "work_mode", "piecework_name", "is_active"]:
        if col not in master_df.columns:
            master_df[col] = ""

    for col in ["id", "company_id", "piecework_master_id", "step_no", "step_name", "step_detail", "is_active"]:
        if col not in steps_df.columns:
            steps_df[col] = ""

    company_id = str(st.session_state.get("company_id", "")).strip()

    master_df = master_df[
        (master_df["work_mode"].astype(str).str.strip() == str(work_mode).strip()) &
        (master_df["is_active"].astype(str).str.lower().isin(["true", "1", "yes", ""]))
    ].copy()

    if company_id:
        master_df = master_df[
            master_df["company_id"].astype(str).str.strip() == company_id
        ].copy()

        steps_df = steps_df[
            steps_df["company_id"].astype(str).str.strip() == company_id
        ].copy()

    steps_df = steps_df[
        steps_df["is_active"].astype(str).str.lower().isin(["true", "1", "yes", ""])
    ].copy()

    return master_df, steps_df


def _match_registered_piecework(source: str, work_mode: str, company_id: str = "") -> dict:

    src = _normalize_text(source)

    if not src:
        return {}

    try:
        company_id = company_id or str(st.session_state.get("company_id", "")).strip()
    except:
        company_id = ""

    try:
        master_df = load_db("piecework_master")
        steps_df = load_db("piecework_steps")
    except:
        return {}

    if master_df is None or master_df.empty:
        return {}

    master_df = master_df.fillna("").copy()
    steps_df = pd.DataFrame() if steps_df is None else steps_df.fillna("").copy()

    # フィルタ
    if company_id:
        master_df = master_df[
            master_df["company_id"].astype(str).str.strip() == company_id
        ]

    master_df = master_df[
        master_df["work_mode"].astype(str).str.strip() == work_mode
    ]

    if master_df.empty:
        return {}

    raw_qty = _extract_quantity(src)

    def _keys(name):
        name = str(name).strip()
        return [
            name,
            name.replace("育成", ""),
            name.replace("作業", ""),
            name.replace("内職", "")
        ]

    # 一致したときだけ返す
    for _, row in master_df.iterrows():
        name = str(row.get("piecework_name", "")).strip()

        if any(k and k in src for k in _keys(name)):
            return {
                "piecework_name": name,
                "quantity": raw_qty
            }

    return {}

def _build_registered_piecework_work_line(match: dict) -> str:
    """
    登録内職照合結果から作業文を作る。
    """

    if not match:
        return ""

    piecework_name = str(match.get("piecework_name", "") or "").strip()
    step_text = str(match.get("step_text", "") or "").strip()
    quantity = str(match.get("quantity", "") or "").strip()

    work_text = step_text or piecework_name

    if not work_text:
        return ""

    if quantity:
        return f"{work_text}を{quantity}行った。"

    return f"{work_text}に取り組まれた。"

def _force_final_office_format(user_state: str, staff_note: str, memo: str, work: str):
    """
    通所用：型固定版
    - 数量は明示があるときだけ入れる
    - 通所では「本人判断で作業をやめた」表現は禁止
    - 来所時確認 → 作業内容 → 作業中の様子 → 終了時確認 の流れに固定
    """
    source = _normalize_text("\n".join([user_state or "", staff_note or "", memo or ""]))

    # =========================
    # 登録済み内職・工程との照合（通所）
    # =========================
    matched_piecework = _match_registered_piecework(source, "office")

    registered_work_line = ""
    registered_end_line = ""

    if matched_piecework:
        piecework_name = str(matched_piecework.get("piecework_name", "")).strip()
        step_text = str(matched_piecework.get("step_text", "")).strip()
        matched_quantity = str(matched_piecework.get("quantity", "")).strip()

        if matched_quantity:
            qty_phrase = matched_quantity
        else:
            qty_phrase = _extract_office_quantity(source) or _extract_quantity(source)

        if step_text and qty_phrase:
            registered_work_line = f"本日は{step_text}作業に取り組まれた。"
            registered_end_line = f"作業終了時には、{step_text}作業を{qty_phrase}行ったことを職員が確認した。"
        elif piecework_name and qty_phrase:
            registered_work_line = f"本日は{piecework_name}に取り組まれた。"
            registered_end_line = f"作業終了時には、{piecework_name}を{qty_phrase}行ったことを職員が確認した。"
        elif step_text:
            registered_work_line = f"本日は{step_text}作業に取り組まれた。"
        elif piecework_name:
            registered_work_line = f"本日は{piecework_name}に取り組まれた。"

        work_label = step_text or piecework_name or _infer_office_work_label(source, work)

    else:
        work_label = _infer_office_work_label(source, work)
        qty_phrase = _extract_office_quantity(source)

        # 念のため、数量抽出の保険
        if not qty_phrase:
            qty_phrase = _extract_quantity(source)

    # 本人発言
    first_quote = _extract_first_quote(source)
    bad_quote_words = ["この辺でやめ", "やめときます", "終わりにします", "休みます"]

    if not first_quote or any(k in first_quote for k in bad_quote_words):
        if any(k in source for k in ["良好", "普通", "いつも通り", "大丈夫", "安定"]):
            first_quote = "今日はいつも通りです"
        elif any(k in source for k in ["しんど", "不調", "疲", "痛", "眠"]):
            first_quote = "今日は少し体調が優れないです"
        else:
            first_quote = "無理のない範囲で取り組みます"

    # 体調・声かけ
    if any(k in source for k in ["しんど", "不調", "疲", "痛", "眠"]):
        health_line = "来所時に体調確認を行うと、やや不調があるとの報告があった。"
        reply_line = "職員より「無理のない範囲で進めてください」と声をかけた。"
        staff_1 = "来所時に体調面への配慮が必要な様子が見られた。"
        staff_3 = "今後も体調の変化を確認しながら、無理のない範囲で作業に取り組めるよう支援していく。"
    else:
        health_line = "来所時に体調確認を行うと、体調は大きく変わりないとの報告があった。"
        reply_line = "職員より「この調子で無理なく進めてください」と声をかけた。"
        staff_1 = "来所時の体調に大きな変化はなく、落ち着いて作業に入ることができていた。"
        staff_3 = "今後も体調や作業の様子を確認しながら、安定して取り組めるよう支援していく。"

    # 作業内容文：登録済み内職・工程があれば最優先
    if registered_work_line:
        work_line = registered_work_line
    elif work_label and work_label not in ["作業", "内職", "通所", "施設外就労"]:
        work_line = f"本日は{work_label}に取り組まれた。"
    else:
        work_line = ""

    # 作業中の様子
    if any(k in source for k in ["丁寧", "手元", "確認"]):
        work_status = "作業中は手元を確認しながら、丁寧に進められていた。"
        staff_2 = "作業中は手順を確認しながら進められており、丁寧に取り組む姿勢が見られた。"
    elif any(k in source for k in ["集中", "一定", "リズム", "ペース"]):
        work_status = "作業中は一定のペースを保ちながら、集中して取り組まれていた。"
        staff_2 = "作業中は一定のペースで取り組めており、継続して作業する姿勢が見られた。"
    else:
        work_status = "作業中は落ち着いた様子で、無理のない範囲で取り組まれていた。"
        staff_2 = "作業中は落ち着いて取り組めており、その日の状態に合わせて進められていた。"

    # 終了文：登録済み内職・工程＋数量があれば最優先
    if registered_end_line:
        end_line = registered_end_line
    elif qty_phrase and work_label and work_label not in ["作業", "内職", "通所", "施設外就労"]:
        end_line = f"作業終了時には、{work_label}を{qty_phrase}仕上げたことを職員が確認した。"
    elif qty_phrase:
        end_line = f"作業終了時には、{qty_phrase}仕上げたことを職員が確認した。"
    else:
        end_line = "作業終了時には、取り組み状況を職員が確認した。"

    user_lines = [
        health_line,
        f"本人より「{first_quote}」との話があった。",
        reply_line,
        "",
        work_line,
        work_status,
        end_line,
    ]

    staff_lines = [
        staff_1,
        staff_2,
        staff_3,
    ]

    user_result = "\n".join([line for line in user_lines if str(line).strip()]).strip()
    staff_result = "\n".join([line for line in staff_lines if str(line).strip()]).strip()

    user_result = _final_cleanup_journal_text(user_result)
    staff_result = _final_cleanup_journal_text(staff_result)

    return user_result, staff_result

def _match_registered_piecework(source: str, work_mode: str, company_id: str = ""):
    """
    登録済み内職マスターを使って、元文・作業名・工程名を照合する。
    - 通所/在宅を分ける
    - company_id を見る
    - 「観葉植物育成」登録でも「観葉植物」で拾う
    - 登録数量範囲を超えた数量は上限に丸める
    """

    src = _normalize_text(source)
    mode = str(work_mode or "").strip()

    if not company_id:
        try:
            company_id = str(st.session_state.get("company_id", "")).strip()
        except Exception:
            company_id = ""

    try:
        master_df = load_db("piecework_master")
        steps_df = load_db("piecework_steps")
    except Exception as e:
        print("[PIECEWORK] load error:", e, flush=True)
        return {}

    if master_df is None or master_df.empty:
        return {}

    master_df = master_df.fillna("").copy()

    if steps_df is None:
        steps_df = pd.DataFrame()
    else:
        steps_df = steps_df.fillna("").copy()

    for col in ["id", "company_id", "work_mode", "piecework_name", "quantity_min", "quantity_max", "unit", "priority", "is_active"]:
        if col not in master_df.columns:
            master_df[col] = ""

    for col in ["id", "company_id", "piecework_master_id", "step_no", "step_name", "step_detail", "is_active"]:
        if col not in steps_df.columns:
            steps_df[col] = ""

    # company / mode / active で絞る
    if company_id:
        master_df = master_df[
            master_df["company_id"].astype(str).str.strip() == str(company_id).strip()
        ].copy()
        steps_df = steps_df[
            steps_df["company_id"].astype(str).str.strip() == str(company_id).strip()
        ].copy()

    if mode:
        master_df = master_df[
            master_df["work_mode"].astype(str).str.strip() == mode
        ].copy()

    master_df = master_df[
        master_df["is_active"].astype(str).str.strip().str.lower().isin(["true", "1", "yes"])
    ].copy()

    steps_df = steps_df[
        steps_df["is_active"].astype(str).str.strip().str.lower().isin(["true", "1", "yes"])
    ].copy()

    if master_df.empty:
        return {}

    # 優先順位順
    master_df["priority_num"] = pd.to_numeric(master_df["priority"], errors="coerce").fillna(9999)
    master_df = master_df.sort_values(["priority_num", "piecework_name"])

    # 元文の数量を抽出
    raw_qty = _extract_quantity(src)

    def _split_qty(q):
        q = str(q or "").strip()
        m = re.search(r"(\d+)\s*(枚|個|回|本|袋|点|膳|セット)?", q)
        if not m:
            return None, ""
        return int(m.group(1)), str(m.group(2) or "").strip()

    qty_num, qty_unit = _split_qty(raw_qty)

    def _registered_qty(row):
        qmin = pd.to_numeric(row.get("quantity_min", ""), errors="coerce")
        qmax = pd.to_numeric(row.get("quantity_max", ""), errors="coerce")
        unit = str(row.get("unit", "") or "").strip()

        if pd.isna(qmin):
            qmin = None
        else:
            qmin = int(qmin)

        if pd.isna(qmax):
            qmax = None
        else:
            qmax = int(qmax)

        return qmin, qmax, unit

    def _fix_quantity(row):
        qmin, qmax, unit = _registered_qty(row)

        # 原文に数量あり
        if qty_num is not None:
            n = qty_num

            # 登録上限を超えたら上限に丸める
            if qmax is not None and qmax > 0 and n > qmax:
                n = qmax

            # 登録下限より小さければ下限へ
            if qmin is not None and qmin > 0 and n < qmin:
                n = qmin

            u = qty_unit or unit
            return f"{n}{u}" if u else str(n)

        # 原文に数量なしでも、1〜1 のような固定数量だけは登録値を使う
        if qmin is not None and qmax is not None and qmin == qmax and qmin > 0:
            return f"{qmin}{unit}" if unit else str(qmin)

        return ""

    def _steps_for_master(master_id: str):
        mid = str(master_id or "").strip()
        if not mid or steps_df.empty:
            return pd.DataFrame()

        target = steps_df[
            steps_df["piecework_master_id"].astype(str).str.strip() == mid
        ].copy()

        if target.empty:
            return target

        target["step_no_num"] = pd.to_numeric(target["step_no"], errors="coerce").fillna(9999)
        return target.sort_values(["step_no_num", "step_name"])

    def _match_keys(name: str):
        name = str(name or "").strip()
        keys = [
            name,
            name.replace("育成", ""),
            name.replace("作業", ""),
            name.replace("内職", ""),
        ]
        return [k.strip() for k in keys if k.strip()]

    def _build_result(row, step_text: str = "", reason: str = ""):
        master_id = str(row.get("id", "")).strip()
        piecework_name = str(row.get("piecework_name", "")).strip()
        quantity = _fix_quantity(row)

        target_steps = _steps_for_master(master_id)

        # 元文に工程名があればそれを優先
        if not step_text and not target_steps.empty:
            for _, step in target_steps.iterrows():
                sname = str(step.get("step_name", "")).strip()
                if sname and sname in src:
                    step_text = sname
                    break

        # 工程名が元文にない場合は、登録済みの先頭工程
        if not step_text and not target_steps.empty:
            step_text = str(target_steps.iloc[0].get("step_name", "")).strip()

        return {
            "piecework_id": master_id,
            "piecework_name": piecework_name,
            "step_text": step_text,
            "quantity": quantity,
            "work_mode": mode,
            "match_reason": reason,
        }

    # 1. 内職名の部分一致
    for _, row in master_df.iterrows():
        name = str(row.get("piecework_name", "")).strip()
        if not name:
            continue

        keys = _match_keys(name)
        if any(k in src for k in keys):
            return _build_result(row, reason="piecework_name_partial_match")

    # 2. 工程名一致
    if not steps_df.empty:
        for _, step in steps_df.iterrows():
            step_name = str(step.get("step_name", "")).strip()
            master_id = str(step.get("piecework_master_id", "")).strip()

            if not step_name or step_name not in src:
                continue

            hit = master_df[
                master_df["id"].astype(str).str.strip() == master_id
            ]

            if hit.empty:
                continue

            return _build_result(hit.iloc[0], step_text=step_name, reason="step_name_match")

    # 3. 作業文脈・数量だけある場合は、優先順位1位を採用
    has_work_context = any(k in src for k in [
        "内職", "作業", "仕上げ", "仕上げました", "できました",
        "出来ました", "行いました", "実施", "取り組", "完成"
    ])

    if has_work_context or raw_qty:
        return _build_result(master_df.iloc[0], reason="default_priority_match")

    return {}

def _final_cleanup_journal_text(text: str) -> str:
    text = _normalize_text(text)

    text = re.sub(r"\b\d{1,2}:\d{2}\b", "", text)
    text = re.sub(r"\b\d{1,2}時\d{1,2}分\b", "", text)
    text = re.sub(r"\b\d{1,2}時\b", "", text)

    text = text.replace("。。", "。")
    text = text.replace("？。", "。")
    text = text.replace("?。", "。")
    text = text.replace("とと報告があった", "との報告があった")
    text = text.replace("とと報告がありました", "との報告がありました")

    text = text.replace("ございます", "あります")
    text = text.replace("ございました", "ありました")
    text = text.replace("してまいります", "していきます")
    text = text.replace("支援してまいります", "支援していきます")

    lines = []
    seen = set()

    for sentence in _sentencize_jp(text):
        sentence = sentence.strip().rstrip("。") + "。"
        key = re.sub(r"\s+", "", sentence)
        if key and key not in seen:
            seen.add(key)
            lines.append(sentence)

    return "\n".join(lines).strip()

def _jr_fmt_seconds(sec):
    try:
        sec = int(sec)
    except Exception:
        sec = 0

    if sec < 60:
        return f"{sec}秒"

    m, s = divmod(sec, 60)
    if m < 60:
        return f"{m}分{s}秒"

    h, m = divmod(m, 60)
    return f"{h}時間{m}分"


def _jr_make_month_tasks(residents, start_y, start_m, end_y, end_m):
    tasks = []

    for resident in residents:
        y, m = int(start_y), int(start_m)

        while (y < int(end_y)) or (y == int(end_y) and m <= int(end_m)):
            tasks.append({
                "resident": resident,
                "year": y,
                "month": m,
                "ym": f"{y}-{m:02d}",
            })

            if m == 12:
                y += 1
                m = 1
            else:
                m += 1

    return tasks


def _jr_update_dashboard(progress_ui, state, current_text="", step_text="", level="info"):
    if not progress_ui:
        return

    now = time.time()
    elapsed = now - state.get("start_time", now)

    total = max(int(state.get("total", 0)), 1)
    done = int(state.get("done", 0))
    success = int(state.get("success", 0))
    error = int(state.get("error", 0))
    skip = int(state.get("skip", 0))

    progress = min(max(done / total, 0), 1)

    avg = elapsed / done if done > 0 else 0
    eta = avg * (total - done) if done > 0 else 0

    try:
        progress_ui["bar"].progress(progress)
    except Exception:
        pass

    text = (
        f"進捗: {done}/{total}件（{progress * 100:.1f}%）\n"
        f"成功: {success}件 / スキップ: {skip}件 / エラー: {error}件\n"
        f"経過: {_jr_fmt_seconds(elapsed)} / 残り目安: {_jr_fmt_seconds(eta)} / 平均: {avg:.1f}秒/件"
    )

    try:
        progress_ui["summary"].info(text)
    except Exception:
        pass

    if current_text:
        try:
            if level == "error":
                progress_ui["current"].error(current_text)
            elif level == "warning":
                progress_ui["current"].warning(current_text)
            else:
                progress_ui["current"].info(current_text)
        except Exception:
            pass

    if step_text:
        logs = state.setdefault("logs", [])
        logs.append(step_text)
        if len(logs) > 12:
            logs[:] = logs[-12:]

        try:
            progress_ui["log"].code("\n".join(logs))
        except Exception:
            pass

# =========================================
# メイン処理
# =========================================
def run_bulk_rewrite(
    driver,
    residents,
    start_y,
    start_m,
    end_y,
    end_m,
    outside_workplace="",
    live_status_box=None,
    progress_ui=None,
):
    from run_assistance import goto_users_summary
    from run_assistance import apply_users_summary_filter_show_expired
    from run_assistance import open_support_record_for_resident

    exec_id = str(uuid.uuid4())
    user = str(st.session_state.get("user_id", "")).strip()
    company = str(st.session_state.get("company_id", "")).strip()

    st.session_state["jr_running"] = True

    tasks = _jr_make_month_tasks(residents, start_y, start_m, end_y, end_m)

    state = {
        "start_time": time.time(),
        "total": len(tasks),
        "done": 0,
        "success": 0,
        "error": 0,
        "skip": 0,
        "logs": [],
    }

    _jr_update_dashboard(
        progress_ui,
        state,
        current_text=f"開始準備中: 全{len(tasks)}件",
        step_text="処理を開始します。",
    )

    current_resident = None

    try:
        for task in tasks:
            resident = task["resident"]
            y = task["year"]
            m = task["month"]
            ym = task["ym"]

            try:
                _jr_update_dashboard(
                    progress_ui,
                    state,
                    current_text=f"処理中: {resident} / {ym}",
                    step_text=f"[{state['done'] + 1}/{state['total']}] {resident} / {ym} 開始",
                )

                # 利用者が変わった時だけ一覧から開き直す
                if current_resident != resident:
                    _jr_update_dashboard(
                        progress_ui,
                        state,
                        current_text=f"利用者ページへ移動中: {resident}",
                        step_text=f"{resident}: 利用者ごと一覧へ移動中",
                    )

                    ok = goto_users_summary(driver)
                    if not ok:
                        state["error"] += 1
                        state["done"] += 1
                        append_journal_log({
                            "exec_id": exec_id,
                            "exec_time": _now_str(),
                            "user": user,
                            "company": company,
                            "resident_name": resident,
                            "target_month": ym,
                            "result": "エラー",
                            "count": 0,
                            "message": "利用者ごと一覧へ移動できませんでした",
                        })
                        _jr_update_dashboard(
                            progress_ui,
                            state,
                            current_text=f"エラー: {resident} / {ym}",
                            step_text=f"{resident} / {ym}: 利用者ごと一覧へ移動失敗",
                            level="error",
                        )
                        continue

                    apply_users_summary_filter_show_expired(driver)

                    ok = open_support_record_for_resident(driver, resident)
                    if not ok:
                        state["error"] += 1
                        state["done"] += 1
                        append_journal_log({
                            "exec_id": exec_id,
                            "exec_time": _now_str(),
                            "user": user,
                            "company": company,
                            "resident_name": resident,
                            "target_month": ym,
                            "result": "エラー",
                            "count": 0,
                            "message": "対象利用者の支援記録を開けませんでした",
                        })
                        _jr_update_dashboard(
                            progress_ui,
                            state,
                            current_text=f"エラー: {resident} / {ym}",
                            step_text=f"{resident} / {ym}: 支援記録を開けませんでした",
                            level="error",
                        )
                        continue

                    current_resident = resident

                def progress_callback(step_text, level="info"):
                    _jr_update_dashboard(
                        progress_ui,
                        state,
                        current_text=f"処理中: {resident} / {ym}",
                        step_text=f"{resident} / {ym}: {step_text}",
                        level=level,
                    )

                result = process_one_month(
                    driver=driver,
                    resident_name=resident,
                    year=y,
                    month=m,
                    exec_id=exec_id,
                    user=user,
                    company=company,
                    outside_workplace=outside_workplace,
                    live_status_box=live_status_box,
                    progress_callback=progress_callback,
                )

                result = result or {}
                result_type = str(result.get("result", "")).strip()

                if result_type == "成功":
                    state["success"] += 1
                elif result_type == "スキップ":
                    state["skip"] += 1
                else:
                    state["error"] += 1

                state["done"] += 1

                _jr_update_dashboard(
                    progress_ui,
                    state,
                    current_text=f"完了: {resident} / {ym}",
                    step_text=f"{resident} / {ym}: {result_type or '完了'} / {result.get('message', '')}",
                )

            except Exception as e:
                state["error"] += 1
                state["done"] += 1

                append_journal_log({
                    "exec_id": exec_id,
                    "exec_time": _now_str(),
                    "user": user,
                    "company": company,
                    "resident_name": resident,
                    "target_month": ym,
                    "result": "エラー",
                    "count": 0,
                    "message": str(e),
                })

                _jr_update_dashboard(
                    progress_ui,
                    state,
                    current_text=f"エラー: {resident} / {ym}",
                    step_text=f"{resident} / {ym}: エラー {e}",
                    level="error",
                )

    finally:
        st.session_state["jr_running"] = False
        _jr_update_dashboard(
            progress_ui,
            state,
            current_text="処理が終了しました。",
            step_text="全体処理が終了しました。",
        )

# =========================================
# ページUI
# =========================================
def render_journal_rewrite_page():
    from run_assistance import build_chrome_driver, manual_login_wait

    st.header("過去日誌訂正（自動上書き）")
    st.caption("Knowbeの支援記録を月単位で取得し、ChatGPTで利用者状態と職員考察を再生成して上書きします。")

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

    # =========================
    # 登録済み内職・工程の確認表示
    # =========================
    with st.expander("登録済み内職・工程を確認する", expanded=False):
        try:
            piecework_df = load_db("piecework_master")
            steps_df = load_db("piecework_steps")
        except Exception as e:
            st.error(f"内職マスターの読み込みでエラー: {e}")
            piecework_df = pd.DataFrame()
            steps_df = pd.DataFrame()

        if piecework_df is None or piecework_df.empty:
            st.info("登録済み内職がありません。")
        else:
            piecework_df = piecework_df.fillna("").copy()

            for col in ["id", "company_id", "work_mode", "piecework_name", "quantity_min", "quantity_max", "unit", "priority", "is_active"]:
                if col not in piecework_df.columns:
                    piecework_df[col] = ""

            piecework_df = piecework_df[
                (piecework_df["company_id"].astype(str).str.strip() == company_id) &
                (piecework_df["is_active"].astype(str).str.lower().isin(["true", "1", "yes", ""]))
            ].copy()

            if steps_df is None or steps_df.empty:
                steps_df = pd.DataFrame(columns=["id", "company_id", "piecework_master_id", "step_no", "step_name", "step_detail", "is_active"])
            else:
                steps_df = steps_df.fillna("").copy()

            for col in ["id", "company_id", "piecework_master_id", "step_no", "step_name", "step_detail", "is_active"]:
                if col not in steps_df.columns:
                    steps_df[col] = ""

            steps_df = steps_df[
                (steps_df["company_id"].astype(str).str.strip() == company_id) &
                (steps_df["is_active"].astype(str).str.lower().isin(["true", "1", "yes", ""]))
            ].copy()

            if piecework_df.empty:
                st.info("この事業所に登録済み内職がありません。")
            else:
                view_master = piecework_df.copy()
                view_master["内職種別"] = view_master["work_mode"].replace({
                    "home": "在宅内職",
                    "office": "通所内職",
                })

                st.markdown("#### 登録済み内職")
                st.dataframe(
                    view_master[["内職種別", "piecework_name", "quantity_min", "quantity_max", "unit", "priority"]],
                    use_container_width=True
                )

                if not steps_df.empty:
                    merged = steps_df.merge(
                        piecework_df[["id", "work_mode", "piecework_name"]],
                        left_on="piecework_master_id",
                        right_on="id",
                        how="left",
                        suffixes=("", "_master")
                    )

                    merged["内職種別"] = merged["work_mode"].replace({
                        "home": "在宅内職",
                        "office": "通所内職",
                    })

                    merged["step_no_num"] = pd.to_numeric(merged["step_no"], errors="coerce").fillna(9999)
                    merged = merged.sort_values(["work_mode", "piecework_name", "step_no_num"])

                    st.markdown("#### 登録済み工程")
                    st.dataframe(
                        merged[["内職種別", "piecework_name", "step_no", "step_name", "step_detail"]],
                        use_container_width=True
                    )
                else:
                    st.info("登録済み工程がありません。")

    c1, c2 = st.columns(2)
    with c1:
        start_y = st.number_input("開始年", min_value=2024, max_value=2035, value=2025, step=1, key="jr_start_y")
        start_m = st.number_input("開始月", min_value=1, max_value=12, value=8, step=1, key="jr_start_m")
    with c2:
        end_y = st.number_input("終了年", min_value=2024, max_value=2035, value=2026, step=1, key="jr_end_y")
        end_m = st.number_input("終了月", min_value=1, max_value=12, value=3, step=1, key="jr_end_m")

    live_status_box = st.empty()

    progress_bar = st.progress(0)
    progress_summary_box = st.empty()
    progress_current_box = st.empty()
    progress_log_box = st.empty()

    progress_ui = {
        "bar": progress_bar,
        "summary": progress_summary_box,
        "current": progress_current_box,
        "log": progress_log_box,
    }

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
                    progress_ui=progress_ui,
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
