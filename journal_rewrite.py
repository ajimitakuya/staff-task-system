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

def _load_jr_control_df():
    df = load_db("journal_rewrite_control")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "company_id", "user_id", "exec_id",
            "status", "stop_requested",
            "started_at", "updated_at", "message"
        ])
    return df


def _save_jr_control_df(df):
    save_db(df, "journal_rewrite_control")


def _now_str():
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")

def _jr_register_run(exec_id: str, company_id: str, user_id: str, message: str = ""):
    df = _load_jr_control_df()

    # 同一 company/user の古い running は止める
    if not df.empty:
        mask_old = (
            df["company_id"].astype(str).str.strip() == str(company_id).strip()
        ) & (
            df["user_id"].astype(str).str.strip() == str(user_id).strip()
        ) & (
            df["status"].astype(str).str.strip() == "running"
        )
        if mask_old.any():
            df.loc[mask_old, "status"] = "stopped"
            df.loc[mask_old, "stop_requested"] = 1
            df.loc[mask_old, "updated_at"] = _now_str()
            df.loc[mask_old, "message"] = "新規実行により旧実行を停止"

    row = {
        "company_id": str(company_id).strip(),
        "user_id": str(user_id).strip(),
        "exec_id": str(exec_id).strip(),
        "status": "running",
        "stop_requested": 0,
        "started_at": _now_str(),
        "updated_at": _now_str(),
        "message": str(message or "").strip(),
    }

    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    _save_jr_control_df(df)

def _jr_request_stop(company_id: str, user_id: str = ""):
    df = _load_jr_control_df()
    if df.empty:
        return False

    mask = df["company_id"].astype(str).str.strip() == str(company_id).strip()

    if user_id:
        mask = mask & (df["user_id"].astype(str).str.strip() == str(user_id).strip())

    mask = mask & (df["status"].astype(str).str.strip() == "running")

    if not mask.any():
        return False

    df.loc[mask, "stop_requested"] = 1
    df.loc[mask, "updated_at"] = _now_str()
    df.loc[mask, "message"] = "停止予約"
    _save_jr_control_df(df)
    return True

def _jr_should_stop(exec_id: str, company_id: str, user_id: str) -> bool:
    df = _load_jr_control_df()
    if df.empty:
        return False

    mask = (
        df["company_id"].astype(str).str.strip() == str(company_id).strip()
    ) & (
        df["user_id"].astype(str).str.strip() == str(user_id).strip()
    ) & (
        df["exec_id"].astype(str).str.strip() == str(exec_id).strip()
    )

    hit = df[mask]
    if hit.empty:
        return False

    row = hit.iloc[-1]
    return str(row.get("stop_requested", "0")).strip() in ("1", "True", "true")

def _jr_finish_run(exec_id: str, company_id: str, user_id: str, status: str = "done", message: str = ""):
    df = _load_jr_control_df()
    if df.empty:
        return

    mask = (
        df["company_id"].astype(str).str.strip() == str(company_id).strip()
    ) & (
        df["user_id"].astype(str).str.strip() == str(user_id).strip()
    ) & (
        df["exec_id"].astype(str).str.strip() == str(exec_id).strip()
    )

    if not mask.any():
        return

    df.loc[mask, "status"] = status
    df.loc[mask, "updated_at"] = _now_str()
    df.loc[mask, "message"] = str(message or "").strip()
    _save_jr_control_df(df)

def _jr_clear_control(company_id: str, user_id: str = ""):
    df = _load_jr_control_df()
    if df.empty:
        return

    mask = df["company_id"].astype(str).str.strip() == str(company_id).strip()
    if user_id:
        mask = mask & (df["user_id"].astype(str).str.strip() == str(user_id).strip())

    df = df.loc[~mask].copy()
    _save_jr_control_df(df)

# =========================================
# 月単位の日誌再生成
# =========================================
def generate_json_with_gemini_local(page_text: str, outside_workplace: str = ""):
    api_key = _get_gemini_api_key()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY が見つかりません")

    genai.configure(api_key=api_key)

    system_instruction = """
支援記録リライト専用 命令書 v7.0

あなたの仕事は、Knowbe支援記録の原文をもとに、「利用者状態」と「職員考察」を、
そのまま現場で貼り付けできる自然で完成度の高い文章へ再構成することです。

【最重要原則】
1. 捏造禁止
原文にない新事実、新活動、新しい体調変化、新しい支援行為を作らないこと。
ただし、原文中にある事実を自然な順序に並べ替えることは許可する。

2. 利用者状態は必ず「完成文」にする
利用者状態は単語やラベルの並びで終えてはならない。
「精神安定、体調不安定」「体調良好」「観葉植物、塗り絵」などの短語だけは絶対禁止。
必ず以下を自然な日本語として含めること。
- 作業開始時の様子または開始連絡
- 体調や精神面の様子
- その日に行った作業内容
- 数量が原文にある場合はその数量
- 作業終了時の報告や様子

3. 利用者状態の理想形
20日分のような、完成された1段落の文章にすること。
単に情報を並べるのではなく、
「開始時 → 体調 → 作業 → 終了報告」の流れが読める文にすること。

4. 職員考察
職員考察は支援者視点のみ。
事実の再説明ではなく、配慮、見立て、支援方針を書くこと。

5. 数量
原文に明確な数量がある場合は必ず優先すること。
曖昧表現（少し、半分、8割など）は、原文に明確数値がない場合のみ自然文のまま扱ってよい。
無理に全件数値化しないこと。
ただし「全くできなかった」「しんどくてできなかった」等は0件扱い可。

【禁止事項】
- 「精神安定、体調不安定」だけで終える
- 「観葉植物、塗り絵」だけで終える
- 作業内容だけ、体調だけ、終了報告だけで終える
- 箇条書き調
- JSON以外の出力

【出力形式】
JSONのみ。
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
- 利用者状態は必ず完成文にする
- 利用者状態には、開始時の様子、体調、作業内容、終了時の報告が分かるようにする
- 「精神安定、体調不安定」などの短語のみは禁止
- 20日分のような自然で完成度の高い文体にする

【悪い例】
"精神安定、体調不安定です。作業終了時には、観葉植物や塗り絵などの作業に取り組まれました。"

【良い例】
"作業開始時には、朝の通所時より精神面は安定していましたが、体調には不安定さが見られました。その後は観葉植物や塗り絵の作業に取り組まれ、無理のない範囲で活動を進められていました。作業終了時には、その日の体調に配慮しながら過ごされた様子がうかがえました。"

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

def _force_polished_user_state(
    user_state: str,
    before_user: str,
    before_staff: str,
    work_label: str,
    mode: str = "",
) -> str:

    out = _normalize_text(user_state)
    before_user = _normalize_text(before_user)
    before_staff = _normalize_text(before_staff)
    work_label = _clean_work_label(work_label)  # ★ここ重要

    source = " ".join([out, before_user, before_staff]).strip()
    allow_zero = _contains_explicit_no_work_reason(source)

    # 短文なら再構築
    if (not out) or _looks_like_short_health_only(out) or len(_sentencize_jp(out)) < 2:
        rebuilt = _compose_user_state_from_raw(work_label, before_user, before_staff)
        if rebuilt:
            out = _normalize_text(rebuilt)

    # ダイアモンド構造
    out = _force_diamond_user_state(
        user_state=out,
        before_user=before_user,
        before_staff=before_staff,
        work_label=work_label,
    )

    if mode:
        out = _apply_mode_prefix_to_user_state(mode, out)

    # 数量補正（ただし複合作業はスキップされる）
    out = _normalize_work_quantity_phrase(out, work_label)
    out = _convert_ambiguous_quantity_to_one_or_more(out, work_label, allow_zero)
    out = _append_default_quantity_if_missing(out, work_label, allow_zero)

    # 🔥ここが強化ポイント（分量アップ）
    if len(_sentencize_jp(out)) < 3:

        health = _extract_sentence_by_keywords(
            source,
            ["体調", "精神", "良好", "普通", "不安定", "安定", "元気", "まぁまぁ", "まあまあ", "大丈夫"]
        )
        if not health:
            health = "体調について報告がありました。"

        if "通所" in source or "来所" in source:
            s1 = f"作業開始時には、{health.rstrip('。')}。"
        else:
            s1 = f"作業開始の連絡があり、{health.rstrip('。')}。"

        # ★複合作業は数量をつけない
        if _is_multi_work_label(work_label):
            s2 = f"その後は、{work_label}の作業に取り組まれました。"
        else:
            work_result = _build_work_result_phrase(work_label, out, before_user, before_staff)
            s2 = f"その後は、{work_result}実施されました。"

        s3 = "作業終了時には、その日の状態に合わせて無理のない範囲で過ごされた様子がうかがえました。"

        out = _dedupe_sentences(" ".join([s1, s2, s3]))

    return _normalize_text(out)

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

    labels = [
        "精神安定", "精神不安定",
        "体調安定", "体調不安定",
        "体調良好", "体調普通",
        "元気", "良好", "普通", "大丈夫", "まあまあ", "まぁまぁ"
    ]
    temp = s2
    for lb in labels:
        temp = temp.replace(lb, "")
    temp = temp.replace(",", "").strip()

    if not temp and len(_sentencize_jp(s)) <= 1:
        return True

    # ここを強化：
    # 20文字以下で体調/精神ワード中心なら無条件で短文扱い
    if len(s) <= 20 and any(k in s for k in ["体調", "精神", "良好", "普通", "安定", "不安定", "元気", "大丈夫"]):
        if len(_sentencize_jp(s)) <= 1:
            return True

    return False


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

def _is_multi_work_label(work: str) -> bool:
    w = _normalize_text(work)
    return any(sep in w for sep in ["、", ",", "，", "・", "/", "／"])

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

    # 追加：複合作業はここで即終了
    if _is_multi_work_label(w):
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
    if not _is_quantifiable_work(single):
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


def _dedupe_sentences(text: str) -> str:
    seen = set()
    result = []
    for s in _sentencize_jp(text):
        key = re.sub(r"\s+", "", s.rstrip("。"))
        if key and key not in seen:
            seen.add(key)
            result.append(s.rstrip("。") + "。")
    return " ".join(result).strip()


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

def _force_diamond_user_state(
    user_state: str,
    before_user: str,
    before_staff: str,
    work_label: str,
) -> str:
    """
    ダイアモンドルール:
    ①作業開始と作業終了がわかる言葉
    ②体調に関する記述
    ③作業内容と成果物の数量（数量が自然な場合のみ）
    を必ず入れる
    """
    out = _normalize_text(user_state)
    source = " ".join([
        _normalize_text(user_state),
        _normalize_text(before_user),
        _normalize_text(before_staff),
    ]).strip()

    work_result = _build_work_result_phrase(work_label, user_state, before_user, before_staff)

    health_sentence = _extract_sentence_by_keywords(
        source,
        ["体調", "精神", "良好", "普通", "不安定", "安定", "元気", "まぁまぁ", "まあまあ", "大丈夫"]
    )
    if not health_sentence:
        health_sentence = "体調について報告がありました。"

    has_start = ("作業開始" in out) or ("開始の連絡" in out) or ("来所時" in out)
    has_end = ("作業終了" in out) or ("終了の連絡" in out) or ("報告されました" in out)
    has_health = any(k in out for k in ["体調", "精神", "良好", "普通", "不安定", "安定", "元気"])
    has_work = bool(_normalize_text(work_label)) and (_normalize_text(work_label).split("、")[0] in out or "作業" in out)
    has_qty = _has_explicit_quantity(out)

    parts = []

    # ①開始 + ②体調
    if not has_start or not has_health:
        if "来所" in source or "通所" in source:
            parts.append(f"作業開始時には、{health_sentence.rstrip('。')}。")
        else:
            parts.append(f"作業開始の連絡があり、{health_sentence.rstrip('。')}。")

    # Gemini本文そのもの
    if out:
        parts.append(out)

    # ③作業内容・数量
    if not has_work:
        if work_result.endswith("に取り組まれました") or work_result.endswith("の作業に取り組まれました"):
            parts.append(work_result.rstrip("。") + "。")
        else:
            parts.append(f"{work_result}実施されました。")
    elif not has_qty and ("を" in work_result and any(u in work_result for u in ["枚", "個", "膳", "本", "羽"])):
        parts.append(f"{work_result}実施されました。")

    # ①終了
    if not has_end:
        if work_result.endswith("に取り組まれました") or work_result.endswith("の作業に取り組まれました"):
            parts.append(f"作業終了時には、{work_result.rstrip('。')}ことを報告されました。")
        else:
            parts.append(f"作業終了時には、{work_result}行ったことを報告されました。")

    result = " ".join([p for p in parts if _normalize_text(p)])
    result = _normalize_work_quantity_phrase(result, _normalize_text(work_label))

    allow_zero = _contains_explicit_no_work_reason(source)
    result = _convert_ambiguous_quantity_to_one_or_more(result, _normalize_text(work_label), allow_zero)

    # 数量補完は「単一かつ数量化が自然な作業」のときだけ
    work_items = _split_work_items(work_label)
    if len(work_items) == 1 and _is_quantifiable_work(work_items[0]):
        result = _append_default_quantity_if_missing(result, _normalize_text(work_label), allow_zero)

    result = _dedupe_sentences(result)
    return result.strip()


def _force_diamond_staff_note(staff_note: str, before_staff: str) -> str:
    """
    ④支援に関する内容を必ず入れる
    """
    out = _normalize_text(staff_note)

    if out and any(k in out for k in ["支援", "声掛け", "お伝え", "見守", "配慮", "継続", "確認", "促し"]):
        return _dedupe_sentences(out)

    fallback = _extract_sentence_by_keywords(
        before_staff,
        ["支援", "声掛け", "お伝え", "見守", "配慮", "継続", "確認", "促し"]
    )
    if fallback:
        return _dedupe_sentences(fallback)

    if out:
        return _dedupe_sentences(out)

    return "本人の体調や精神面に配慮しながら、無理のない範囲で取り組めるよう支援を継続します。"

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

    # ❌ 不自然な「場所＋来て」を削除
    text = re.sub(r"(合同会社エバーグリーン|居酒屋.?琴)[^。]*来て[^。]*。?", "", text)

    # 二重付与防止
    if mode == "在宅" and "在宅" in text:
        return text
    if mode == "施設外" and "施設外" in text:
        return text

    if mode == "在宅":
        if "作業開始" in text:
            text = text.replace("作業開始", "在宅で作業開始", 1)
        else:
            text = "在宅にて、" + text

    elif mode == "施設外":
        # 👍 ゆるい自然表現だけ付与
        if "作業開始" in text:
            text = text.replace("作業開始", "施設外にて作業開始", 1)
        else:
            text = "施設外就労先にて、" + text

    return text


def _apply_mode_prefix_to_staff_note(mode: str, staff_note: str) -> str:
    text = _normalize_text(staff_note)
    if not text:
        return text

    if mode == "在宅" and "在宅" not in text:
        return "在宅での取り組み状況を踏まえ、" + text

    if mode == "施設外" and "施設外" not in text and "エバーグリーン" not in text and "居酒屋" not in text and "琴" not in text:
        return "施設外での作業状況を踏まえ、" + text

    return text

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

        mode = _detect_service_mode(
            row_text=all_text,
            work_text=work,
            user_text=raw_user,
            staff_text=raw_staff,
        )

        user_state = _force_polished_user_state(
            user_state=user_state,
            before_user=raw_user,
            before_staff=raw_staff,
            work_label=work,
            mode=mode,
        )

        staff_note = _force_diamond_staff_note(
            staff_note=staff_note,
            before_staff=raw_staff,
        )
        staff_note = _apply_mode_prefix_to_staff_note(mode, staff_note)

        if is_outside_day and outside_workplace and outside_workplace != "未指定":
            place = outside_workplace.strip()
            if place not in user_state and place not in staff_note:
                user_state = f"{place}での作業として、{user_state}"

        fixed[date_str] = {
            "user_state": user_state,
            "staff_note": staff_note,
        }

    # Gemini未返却日も同じ完成文ロジックで補完
    for day, block in blocks.items():
        key = f"{year:04d}-{month:02d}-{day:02d}"
        if key in fixed:
            continue

        work = _normalize_text(block.get("work", ""))
        raw_user = _normalize_text(block.get("user_state_raw", ""))
        raw_staff = _normalize_text(block.get("staff_note_raw", ""))
        all_text = " ".join([work, raw_user, raw_staff])

        mode = _detect_service_mode(
            row_text=all_text,
            work_text=work,
            user_text=raw_user,
            staff_text=raw_staff,
        )

        rebuilt_user = _force_polished_user_state(
            user_state="",
            before_user=raw_user,
            before_staff=raw_staff,
            work_label=work,
            mode=mode,
        )

        rebuilt_staff = _force_diamond_staff_note(
            staff_note="",
            before_staff=raw_staff,
        )
        rebuilt_staff = _apply_mode_prefix_to_staff_note(mode, rebuilt_staff)

        is_outside_day = any(k in all_text for k in [
            "施設外就労", "清掃", "居酒屋", "琴", "エバーグリーン"
        ])
        if is_outside_day and outside_workplace and outside_workplace != "未指定":
            place = outside_workplace.strip()
            if place not in rebuilt_user and place not in rebuilt_staff:
                rebuilt_user = f"{place}での作業として、{rebuilt_user}"

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
        if _jr_should_stop(exec_id, company, user):
            print(f"[JR] stop requested before month: {resident_name} {ym}", flush=True)
            return

        ok = goto_support_record_month(driver, year, month)
        if not ok:
            print("[JR] 月移動失敗", flush=True)
            return

        time.sleep(2)

        if _jr_should_stop(exec_id, company, user):
            print(f"[JR] stop requested after month move: {resident_name} {ym}", flush=True)
            return

        page_text = fetch_support_record_page_text(driver)
        page_text_str = str(page_text or "").strip()

        if not page_text_str:
            print("[JR] 日誌なし", flush=True)
            return

        result_json = generate_json_with_gemini_local(page_text_str, outside_workplace)
        result_json = _postprocess_gemini_result(page_text_str, result_json, year, month, outside_workplace)

        if _jr_should_stop(exec_id, company, user):
            print(f"[JR] stop requested before edit mode: {resident_name} {ym}", flush=True)
            return

        print("[FIX] 編集モードへ", flush=True)
        enter_edit_mode(driver)

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        success_count = 0

        for date_str in sorted(result_json.keys()):
            if _jr_should_stop(exec_id, company, user):
                print(f"[JR] stop requested during day loop: {resident_name} {ym} {date_str}", flush=True)
                break

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
                    if not re.match(rf"^{target_day}日（", row_text_full):
                        continue

                    areas = _find_row_textareas_for_support_record(row)
                    print(f"[FIX] {target_label} textarea count = {len(areas)}", flush=True)

                    if len(areas) < 2:
                        raise RuntimeError(f"textarea不足: {target_label}")

                    user_state_el = areas[0]
                    staff_note_el = areas[1]

                    before_user = _textarea_value(user_state_el)
                    before_staff = _textarea_value(staff_note_el)

                    row_text = row.text

                    mode = _detect_service_mode(
                        row_text=row_text,
                        work_text=row_text,
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
                        work_match = re.search(
                            r"作業\s*(.+?)\s*利用者状態",
                            row_text.replace("\n", " "),
                            re.DOTALL
                        )
                        if work_match:
                            work_label = _normalize_text(work_match.group(1))
                        else:
                            work_label = "作業"

                    final_user_state = _force_polished_user_state(
                        user_state=_normalize_text(user_state),
                        before_user=before_user,
                        before_staff=before_staff,
                        work_label=work_label,
                        mode=mode,
                    )

                    final_staff_note = _force_diamond_staff_note(
                        staff_note=_normalize_text(staff_note),
                        before_staff=before_staff,
                    )
                    final_staff_note = _apply_mode_prefix_to_staff_note(mode, final_staff_note)

                    _set_react_textarea_value(driver, user_state_el, final_user_state)
                    _set_react_textarea_value(driver, staff_note_el, final_staff_note)

                    after_user = _textarea_value(user_state_el)
                    after_staff = _textarea_value(staff_note_el)

                    print(f"[FIX] final user_state = {final_user_state[:200]}", flush=True)
                    print(f"[FIX] final staff_note = {final_staff_note[:200]}", flush=True)
                    print(f"[FIX] after user_state = {after_user[:200]}", flush=True)
                    print(f"[FIX] after staff_note = {after_staff[:200]}", flush=True)

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

        if _jr_should_stop(exec_id, company, user):
            print("[FIX] 停止要求を検知したため保存して終了", flush=True)
            _update_live_status(
                live_status_box,
                f"停止しました: {resident_name} / {year}-{month:02d}",
                "warning"
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

    # 作業抽出
    work = _normalize_text(work_label)

    # 数量付き作業文を生成
    work_phrase = _build_work_result_phrase(work, memo)

    # 利用者状態
    user_state = memo

    # ダイアモンド補正
    user_state = _force_diamond_user_state(
        user_state=user_state,
        before_user=memo,
        before_staff="",
        work_label=work
    )

    # モード判定
    mode = _detect_service_mode(
        row_text=memo,
        work_text=work,
        user_text=user_state,
        staff_text=""
    )

    user_state = _apply_mode_prefix_to_user_state(mode, user_state)

    # 職員考察
    staff_note = _force_diamond_staff_note(
        staff_note="",
        before_staff=memo
    )

    staff_note = _apply_mode_prefix_to_staff_note(mode, staff_note)

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
    _jr_register_run(exec_id, company, user, "自動上書き開始")

    try:
        for resident in residents:
            if _jr_should_stop(exec_id, company, user):
                _update_live_status(live_status_box, f"停止しました: {resident} の前", "warning")
                _jr_finish_run(exec_id, company, user, status="stopped", message="利用者ループ前で停止")
                return

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
                if _jr_should_stop(exec_id, company, user):
                    _update_live_status(live_status_box, f"停止しました: {resident} / {y}-{m:02d} の前", "warning")
                    _jr_finish_run(exec_id, company, user, status="stopped", message=f"{resident} {y}-{m:02d} 前で停止")
                    return

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

        _jr_finish_run(exec_id, company, user, status="done", message="正常終了")

    except Exception as e:
        _jr_finish_run(exec_id, company, user, status="stopped", message=f"例外終了: {e}")
        raise
    finally:
        st.session_state["jr_running"] = False
        st.session_state["jr_stop_requested"] = False

def _clean_work_label(work: str) -> str:
    """
    作業ラベルの重複削除
    例: 観葉植物、塗り絵、内職、観葉植物 → 観葉植物や塗り絵、内職
    """
    w = _normalize_text(work)
    if not w:
        return w

    parts = re.split(r"[、,，・/／]", w)
    parts = [p.strip() for p in parts if p.strip()]

    seen = []
    for p in parts:
        if p not in seen:
            seen.append(p)

    if len(seen) <= 1:
        return seen[0] if seen else w

    # 「や」で自然接続
    return "や".join([seen[0], "、".join(seen[1:])]) if len(seen) > 2 else "や".join(seen)

# =========================================
# ページUI
# =========================================
def render_journal_rewrite_page():
    from run_assistance import build_chrome_driver, manual_login_wait

    st.header("過去日誌訂正（自動上書き）")
    st.caption("Knowbeの支援記録を月単位で取得し、Geminiで利用者状態と職員考察を再生成して上書きします。")

    if "jr_running" not in st.session_state:
        st.session_state["jr_running"] = False
    if "jr_stop_requested" not in st.session_state:
        st.session_state["jr_stop_requested"] = False

    if not st.session_state.get("is_admin", False):
        st.session_state.pop("journal_rewrite_residents", None)
        st.session_state.pop("jr_outside_workplace", None)
        st.session_state["jr_running"] = False
        st.session_state["jr_stop_requested"] = False
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
        index=outside_options.index(st.session_state["jr_outside_workplace"])
        if st.session_state["jr_outside_workplace"] in outside_options else 0,
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

    b1, b2, b3, b4 = st.columns(4)

    with b1:
        run_clicked = st.button(
            "自動上書きを実行",
            key="run_journal_rewrite",
            use_container_width=True,
            disabled=st.session_state.get("jr_running", False),
        )

    with b2:
        stop_clicked = st.button(
            "停止予約",
            key="stop_journal_rewrite",
            use_container_width=True,
        )
        if stop_clicked:
            ok = _jr_request_stop(company_id, user_id)
            if ok:
                st.session_state["jr_stop_requested"] = True
                st.warning("停止予約を保存したある。次の区切りで停止するある。")
            else:
                st.info("停止対象の実行中データが見つからなかったある。")

    with b3:
        reset_clicked = st.button(
            "UI選択を初期化",
            key="reset_journal_rewrite_ui",
            use_container_width=True,
        )
        if reset_clicked:
            st.session_state.pop("journal_rewrite_residents", None)
            st.session_state.pop("jr_outside_workplace", None)
            st.session_state["jr_running"] = False
            st.session_state["jr_stop_requested"] = False
            st.success("画面上の選択を初期化したある。")
            st.rerun()

    with b4:
        clear_control_clicked = st.button(
            "停止/実行データ初期化",
            key="clear_journal_rewrite_control",
            use_container_width=True,
        )
        if clear_control_clicked:
            _jr_clear_control(company_id, user_id)
            st.session_state["jr_running"] = False
            st.session_state["jr_stop_requested"] = False
            st.success("スプシ上の停止・実行データを初期化したある。")
            st.rerun()

    if st.session_state.get("jr_running", False):
        st.info("現在実行中ある。停止したい場合は『停止予約』を押すある。")

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

        st.session_state["jr_stop_requested"] = False

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
            st.session_state["jr_stop_requested"] = False
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass