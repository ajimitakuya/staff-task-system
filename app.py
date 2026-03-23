import streamlit as st
import pandas as pd
import base64
import time
import random
import json
from io import BytesIO
from datetime import datetime, timedelta, timezone, date
import calendar as py_calendar
from openpyxl import load_workbook
from streamlit_gsheets import GSheetsConnection
from streamlit_calendar import calendar as st_calendar
import google.generativeai as genai
import tempfile
from openpyxl import Workbook

JST = timezone(timedelta(hours=9))
def now_jst():
    return datetime.now(JST)

def get_genai_client():
    api_key = ""

    try:
        api_key = st.secrets.get("GEMINI_API_KEY", "")
    except Exception:
        api_key = ""

    if not api_key:
        import os
        api_key = os.environ.get("GEMINI_API_KEY", "")

    if not api_key:
        raise ValueError("GEMINI_API_KEY が設定されてないある")

    import google.generativeai as genai
    genai.configure(api_key=api_key)

    return genai

# --- ページ基本設定 ---
st.set_page_config(page_title="作業管理システム", layout="wide")
st.caption("APP_VERSION = 2026-03-21-knowbe-debug-01")

# --- 🔌 スプレッドシート接続設定 ---
conn = st.connection("gsheets", type=GSheetsConnection)

COMMON_SHEETS = {
    "task",
    "chat",
    "manual",
    "record_status",
    "calendar",
    "active_users",
}

OFFICE_SHEETS = {
    "resident_master",
    "resident_schedule",
    "resident_notes",
    "document_master",
    "external_contacts",
    "resident_links",
    "saved_documents",
    "diary_input_rules",
    "staff_examples",
    "personal_rules",
    "assistant_plans",
}

def get_current_office_key():
    office_key = str(st.session_state.get("office_key", "support")).strip().lower()
    if office_key not in ("support", "home"):
        office_key = "support"
    return office_key

def get_sheet_name_candidates(file):
    if file in COMMON_SHEETS:
        return [file]

    if file in OFFICE_SHEETS:
        office_key = get_current_office_key()
        return [
            f"{office_key}_{file}",
            file,
        ]

    raise ValueError(f"未対応のシート名ある: {file}")

def get_sheet_name(file):
    candidates = get_sheet_name_candidates(file)
    return candidates[0]


def load_db(file, retries=3, delay=0.8):
    sheet_candidates = get_sheet_name_candidates(file)

    last_error = None

    for s_name in sheet_candidates:
        for attempt in range(retries):
            try:
                df = conn.read(worksheet=s_name, ttl=60)

                if df is None:
                    df = pd.DataFrame()

                # record_status の列名補正
                if file == "record_status" and not df.empty:
                    cols = list(df.columns)
                    fixed_cols = []
                    current_year = None

                    for col in cols:
                        col_str = str(col).strip()

                        if col_str == "resident_name":
                            fixed_cols.append("resident_name")
                            continue

                        if "年" in col_str and "月" in col_str:
                            current_year = col_str.split("年")[0]
                            fixed_cols.append(col_str)
                            continue

                        if col_str.endswith("月") and current_year is not None:
                            fixed_cols.append(f"{current_year}年{col_str}")
                        else:
                            fixed_cols.append(col_str)

                    df.columns = fixed_cols

                record_status_cols = ["resident_name"]
                for year in range(2025, 2027):
                    for month in range(1, 13):
                        record_status_cols.append(f"{year}年{month}月")

                calendar_cols = ["id", "title", "start", "end", "user", "memo", "source_type", "source_task_id"]

                expected_cols = {
                    "task": ["id", "task", "status", "user", "limit", "priority", "updated_at"],
                    "chat": ["date", "time", "user", "message", "image_data"],
                    "manual": ["id", "title", "content", "image_data", "created_at"],
                    "record_status": record_status_cols,
                    "calendar": calendar_cols,
                    "active_users": ["user", "login_at", "last_seen"],
                    "resident_master": [
                        "resident_id", "resident_name", "status",
                        "consultant", "consultant_phone",
                        "caseworker", "caseworker_phone",
                        "hospital", "hospital_phone",
                        "nurse", "nurse_phone",
                        "care", "care_phone",
                        "created_at", "updated_at"
                    ],
                    "resident_schedule": [
                        "id", "resident_id", "weekday", "service_type",
                        "start_time", "end_time", "place", "phone", "person_in_charge", "memo"
                    ],
                    "resident_notes": [
                        "id", "resident_id", "date", "user", "note"
                    ],
                    "document_master": [
                        "document_id", "category1", "category2", "category3",
                        "title", "file_type", "url", "summary", "memo",
                        "status", "updated_at", "created_at",
                        "original_filename", "file_data_base64"
                    ],
                    "external_contacts": [
                        "contact_id", "category1", "category2",
                        "name", "organization", "phone", "memo"
                    ],
                    "resident_links": [
                        "id", "resident_id", "contact_id", "role"
                    ],
                    "saved_documents": [
                        "record_id",
                        "resident_id",
                        "resident_name",
                        "doc_type",
                        "created_at",
                        "updated_at",
                        "json_data"
                    ],
                    "diary_input_rules": [
                        "record_id", "company_id", "date", "resident_id", "resident_name",
                        "start_time", "end_time", "work_start_time", "work_end_time", "work_break_time",
                        "meal_flag", "note",
                        "start_memo", "end_memo", "staff_name",
                        "generated_status", "generated_support", "created_at",
                        "service_type", "knowbe_target", "send_status", "sent_at", "send_error",
                        "record_mode"
                    ],
                    "staff_examples": [
                        "staff_name",
                        "home_start_example", "home_end_example",
                        "day_start_example", "day_end_example",
                        "outside_start_example", "outside_end_example",
                        "updated_at"
                    ],
                    "personal_rules": [
                        "staff_name", "rule_text", "updated_at"
                    ],
                    "assistant_plans": [
                        "resident_id", "long_term_goal", "short_term_goal", "updated_at"
                    ],
                }

                for col in expected_cols[file]:
                    if col not in df.columns:
                        df[col] = ""

                return df

            except Exception as e:
                last_error = e
                if attempt < retries - 1:
                    time.sleep(delay + random.random() * 0.5)
                else:
                    pass

    if last_error is not None:
        raise last_error

    return pd.DataFrame()


def get_resident_master_df():
    return get_resident_master_df_cached().copy()

def get_document_master_df():
    return get_document_master_df_cached().copy()

def get_saved_documents_df():
    df = load_db("saved_documents")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "record_id",
            "resident_id",
            "resident_name",
            "doc_type",
            "created_at",
            "updated_at",
            "json_data"
        ])
    else:
        for col in [
            "record_id",
            "resident_id",
            "resident_name",
            "doc_type",
            "created_at",
            "updated_at",
            "json_data"
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")

@st.cache_data(ttl=60)
def get_diary_input_rules_df_cached():
    df = load_db("diary_input_rules")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "record_id", "company_id", "date", "resident_id", "resident_name",
            "start_time", "end_time", "work_start_time", "work_end_time", "work_break_time",
            "meal_flag", "note",
            "start_memo", "end_memo", "staff_name",
            "generated_status", "generated_support", "created_at",
            "service_type", "knowbe_target", "send_status", "sent_at", "send_error",
            "record_mode"
        ])
    else:
        for col in [
            "record_id", "company_id", "date", "resident_id", "resident_name",
            "start_time", "end_time", "work_start_time", "work_end_time", "work_break_time",
            "meal_flag", "note",
            "start_memo", "end_memo", "staff_name",
            "generated_status", "generated_support", "created_at",
            "service_type", "knowbe_target", "send_status", "sent_at", "send_error",
            "record_mode"
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")

def save_diary_input_record(
    date,
    resident_id,
    resident_name,
    start_time,
    end_time,
    work_start_time,
    work_end_time,
    work_break_time,
    meal_flag,
    note,
    start_memo,
    end_memo,
    staff_name,
    generated_status="",
    generated_support="",
    service_type="在宅",
    knowbe_target="support",
    send_status="draft",
    sent_at="",
    send_error="",
    record_mode="gemini",
    company_id=""
):
    df = get_diary_input_rules_df()

    if df.empty:
        next_id = 1
    else:
        nums = pd.to_numeric(df["record_id"], errors="coerce").dropna()
        next_id = int(nums.max()) + 1 if not nums.empty else 1

    created_at = now_jst().strftime("%Y-%m-%d %H:%M:%S")

    if not company_id:
        company_id = get_current_office_key()

    new_row = pd.DataFrame([{
        "record_id": next_id,
        "company_id": str(company_id),
        "date": str(date),
        "resident_id": str(resident_id),
        "resident_name": str(resident_name),
        "start_time": str(start_time),
        "end_time": str(end_time),
        "work_start_time": str(work_start_time),
        "work_end_time": str(work_end_time),
        "work_break_time": str(work_break_time),
        "meal_flag": str(meal_flag),
        "note": str(note),
        "start_memo": str(start_memo),
        "end_memo": str(end_memo),
        "staff_name": str(staff_name),
        "generated_status": str(generated_status),
        "generated_support": str(generated_support),
        "created_at": created_at,
        "service_type": str(service_type),
        "knowbe_target": str(knowbe_target),
        "send_status": str(send_status),
        "sent_at": str(sent_at),
        "send_error": str(send_error),
        "record_mode": str(record_mode),
    }])

    df = pd.concat([df, new_row], ignore_index=True)
    save_db(df, "diary_input_rules")
    return next_id

def update_diary_input_record_status(record_id, send_status, sent_at="", send_error=""):
    df = get_diary_input_rules_df()

    if df is None or df.empty:
        return False

    mask = df["record_id"].astype(str) == str(record_id)

    if not mask.any():
        return False

    df.loc[mask, "send_status"] = str(send_status)
    df.loc[mask, "sent_at"] = str(sent_at)
    df.loc[mask, "send_error"] = str(send_error)

    save_db(df, "diary_input_rules")
    return True

def get_diary_input_rules_df():
    return get_diary_input_rules_df_cached().copy()

def _to_minutes(hhmm: str):
    s = str(hhmm).strip()
    if not s or ":" not in s:
        return None
    try:
        h, m = s.split(":", 1)
        return int(h) * 60 + int(m)
    except Exception:
        return None


def _normalize_weekday_label(dt_value):
    try:
        if hasattr(dt_value, "weekday"):
            wd = dt_value.weekday()
        else:
            wd = pd.to_datetime(dt_value).weekday()
    except Exception:
        return ""

    weekday_map = {
        0: "月",
        1: "火",
        2: "水",
        3: "木",
        4: "金",
        5: "土",
        6: "日",
    }
    return weekday_map.get(wd, "")


def is_time_overlap(start1, end1, start2, end2):
    s1 = _to_minutes(start1)
    e1 = _to_minutes(end1)
    s2 = _to_minutes(start2)
    e2 = _to_minutes(end2)

    if None in (s1, e1, s2, e2):
        return False

    return max(s1, s2) < min(e1, e2)


def validate_bee_times(
    resident_id,
    target_date,
    start_time,
    end_time,
    work_start_time,
    work_end_time,
):
    errors = []

    s = _to_minutes(start_time)
    e = _to_minutes(end_time)
    ws = _to_minutes(work_start_time)
    we = _to_minutes(work_end_time)

    if None in (s, e, ws, we):
        errors.append("時間の形式が正しくないある。HH:MM で入れてほしいある。")
        return errors

    if s >= e:
        errors.append("開始時間と終了時間の大小が正しくないある。")

    if ws >= we:
        errors.append("作業開始時間と作業終了時間の大小が正しくないある。")

    if ws < s or we > e:
        errors.append("作業時間が通所時間の範囲をはみ出してるある。")

    weekday_label = _normalize_weekday_label(target_date)
    schedule_df = get_resident_schedule_df()

    if schedule_df is not None and not schedule_df.empty and weekday_label:
        work = schedule_df.copy()
        work["resident_id"] = work["resident_id"].astype(str)
        work["weekday"] = work["weekday"].astype(str).str.strip()
        work["service_type"] = work["service_type"].astype(str).str.strip()

        target_rows = work[
            (work["resident_id"] == str(resident_id)) &
            (work["weekday"] == weekday_label)
        ].copy()

        for _, row in target_rows.iterrows():
            sv = str(row.get("service_type", "")).strip()
            rs = str(row.get("start_time", "")).strip()
            re = str(row.get("end_time", "")).strip()

            if sv in ["看護", "介護"] and is_time_overlap(work_start_time, work_end_time, rs, re):
                errors.append(f"{sv}の予定（{rs}〜{re}）と作業時間が重なってるある。")

    return errors

def save_document_record(resident_id, resident_name, doc_type, form_data):
    df = get_saved_documents_df()

    now_str = now_jst().strftime("%Y-%m-%d %H:%M")

    if df.empty:
        next_id = 1
    else:
        ids = pd.to_numeric(df["record_id"], errors="coerce").dropna()
        next_id = int(ids.max()) + 1 if not ids.empty else 1

    new_row = pd.DataFrame([{
        "record_id": next_id,
        "resident_id": resident_id,
        "resident_name": resident_name,
        "doc_type": doc_type,
        "created_at": now_str,
        "updated_at": now_str,
        "json_data": json.dumps(form_data, ensure_ascii=False)
    }])

    merged_df = pd.concat([df, new_row], ignore_index=True)
    save_db(merged_df, "saved_documents")
    return next_id


def update_document_record(record_id, form_data):
    df = get_saved_documents_df()
    if df.empty:
        return False

    now_str = now_jst().strftime("%Y-%m-%d %H:%M")
    mask = df["record_id"].astype(str) == str(record_id)

    if not mask.any():
        return False

    df.loc[mask, "updated_at"] = now_str
    df.loc[mask, "json_data"] = json.dumps(form_data, ensure_ascii=False)

    save_db(df, "saved_documents")
    return True


def get_document_records(doc_type, resident_id):
    df = get_saved_documents_df()
    if df.empty:
        return df

    df = df[
        (df["doc_type"].astype(str) == str(doc_type)) &
        (df["resident_id"].astype(str) == str(resident_id))
    ].copy()

    if df.empty:
        return df

    try:
        df["record_id_num"] = pd.to_numeric(df["record_id"], errors="coerce")
        df = df.sort_values(["record_id_num"], ascending=[False])
    except Exception:
        pass

    return df


def load_document_json(record_id):
    df = get_saved_documents_df()
    if df.empty:
        return None

    target = df[df["record_id"].astype(str) == str(record_id)]
    if target.empty:
        return None

    json_str = str(target.iloc[0]["json_data"]).strip()
    if not json_str:
        return None

    try:
        return json.loads(json_str)
    except Exception:
        return None

def get_gemini_api_key_from_app():
    api_key = ""

    try:
        api_key = st.secrets.get("GEMINI_API_KEY", "")
    except Exception:
        api_key = ""

    if not api_key:
        import os
        api_key = os.environ.get("GEMINI_API_KEY", "")

    return str(api_key).strip()


def call_gemini_json(prompt: str):
    api_key = get_gemini_api_key_from_app()
    if not api_key:
        raise RuntimeError("APIキーないある")

    genai.configure(api_key=api_key)

    model_candidates = [
        "gemini-2.5-flash",
        "gemini-1.0-pro",
    ]

    last_error = None

    for model_name in model_candidates:
        try:
            model = genai.GenerativeModel(model_name)
            response = model.generate_content(prompt)
            text = str(getattr(response, "text", "")).strip()

            if not text:
                continue

            cleaned = text.replace("```json", "").replace("```", "").strip()
            return json.loads(cleaned)

        except Exception as e:
            last_error = e
            continue

    raise RuntimeError(f"Gemini全部失敗ある: {last_error}")


def get_latest_saved_document(resident_id, doc_type):
    df = get_saved_documents_df()
    if df is None or df.empty:
        return None

    work = df.copy()
    work["resident_id"] = work["resident_id"].astype(str)
    work["doc_type"] = work["doc_type"].astype(str)

    work = work[
        (work["resident_id"] == str(resident_id)) &
        (work["doc_type"] == str(doc_type))
    ].copy()

    if work.empty:
        return None

    try:
        work["record_id_num"] = pd.to_numeric(work["record_id"], errors="coerce")
        work = work.sort_values(["record_id_num"], ascending=[False])
    except Exception:
        pass

    return work.iloc[0].to_dict()


def get_latest_saved_document_json(resident_id, doc_type):
    row = get_latest_saved_document(resident_id, doc_type)
    if not row:
        return None

    json_str = str(row.get("json_data", "")).strip()
    if not json_str:
        return None

    try:
        return json.loads(json_str)
    except Exception:
        return None


def safe_text(v):
    if v is None:
        return ""
    return str(v).strip()


def build_plan_draft_generation_prompt(
    resident_name,
    source_label,
    source_data,
    new_policy="",
    new_long_goal="",
    new_short_goal="",
    new_goal_rows_policy="",
):
    source_data = source_data or {}

    prompt = f'''
就労継続支援B型の個別支援計画案を作成するある。

【利用者名】
{safe_text(resident_name)}

【参照元の種類】
{safe_text(source_label)}

【参照元データ】
サービス等利用計画の総合的な方針:
{safe_text(source_data.get("policy", ""))}

長期目標:
{safe_text(source_data.get("long_goal", ""))}

短期目標:
{safe_text(source_data.get("short_goal", ""))}

具体的到達目標1:
{safe_text(source_data.get("target_1", ""))}
本人の役割1:
{safe_text(source_data.get("role_1", ""))}
支援内容1:
{safe_text(source_data.get("support_1", ""))}
支援期間1:
{safe_text(source_data.get("period_1", ""))}
担当者1:
{safe_text(source_data.get("person_1", ""))}
優先順位1:
{safe_text(source_data.get("priority_1", ""))}

具体的到達目標2:
{safe_text(source_data.get("target_2", ""))}
本人の役割2:
{safe_text(source_data.get("role_2", ""))}
支援内容2:
{safe_text(source_data.get("support_2", ""))}
支援期間2:
{safe_text(source_data.get("period_2", ""))}
担当者2:
{safe_text(source_data.get("person_2", ""))}
優先順位2:
{safe_text(source_data.get("priority_2", ""))}

具体的到達目標3:
{safe_text(source_data.get("target_3", ""))}
本人の役割3:
{safe_text(source_data.get("role_3", ""))}
支援内容3:
{safe_text(source_data.get("support_3", ""))}
支援期間3:
{safe_text(source_data.get("period_3", ""))}
担当者3:
{safe_text(source_data.get("person_3", ""))}
優先順位3:
{safe_text(source_data.get("priority_3", ""))}

【新しい方針】
サービス等利用計画の総合的な方針について:
{safe_text(new_policy)}

長期目標について:
{safe_text(new_long_goal)}

短期目標について:
{safe_text(new_short_goal)}

具体的到達目標3組全体について:
{safe_text(new_goal_rows_policy)}

【作成する項目】
- サービス等利用計画の総合的な方針
- 長期目標
- 短期目標
- 具体的到達目標・本人の役割・支援内容・支援期間・担当者・優先順位を3組

【ルール】
- 新しい方針が空でも作成すること
- 参照元データが少なくても自然に補うこと
- 就労継続支援B型の書類として自然な内容にすること
- 出力はJSONのみ
- 文章は日本語
- 余計な説明文は不要

【出力形式】
{{
  "policy": "サービス等利用計画の総合的な方針",
  "long_goal": "長期目標",
  "short_goal": "短期目標",
  "goal_rows": [
    {{
      "target": "具体的到達目標1",
      "role": "本人の役割1",
      "support": "支援内容1",
      "period": "支援期間1",
      "person": "担当者1",
      "priority": "優先順位1"
    }},
    {{
      "target": "具体的到達目標2",
      "role": "本人の役割2",
      "support": "支援内容2",
      "period": "支援期間2",
      "person": "担当者2",
      "priority": "優先順位2"
    }},
    {{
      "target": "具体的到達目標3",
      "role": "本人の役割3",
      "support": "支援内容3",
      "period": "支援期間3",
      "person": "担当者3",
      "priority": "優先順位3"
    }}
  ]
}}
'''
    return prompt

def get_next_numeric_id(df, col_name="id", start=1):
    if df is None or df.empty or col_name not in df.columns:
        return start
    ids = pd.to_numeric(df[col_name], errors="coerce").dropna()
    return int(ids.max()) + 1 if not ids.empty else start


def get_next_resident_id(master_df):
    if master_df is None or master_df.empty or "resident_id" not in master_df.columns:
        return "R0001"

    numbers = []
    for x in master_df["resident_id"].fillna("").astype(str):
        x = x.strip().upper()
        if x.startswith("R"):
            num = x[1:]
            if num.isdigit():
                numbers.append(int(num))

    next_num = max(numbers) + 1 if numbers else 1
    return f"R{next_num:04d}"


@st.cache_data(ttl=60)
def get_resident_master_df_cached():
    df = load_db("resident_master")

    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "resident_id", "resident_name", "status",
            "consultant", "consultant_phone",
            "caseworker", "caseworker_phone",
            "hospital", "hospital_phone",
            "nurse", "nurse_phone",
            "care", "care_phone",
            "created_at", "updated_at"
        ])
    else:
        for col in [
            "resident_id", "resident_name", "status",
            "consultant", "consultant_phone",
            "caseworker", "caseworker_phone",
            "hospital", "hospital_phone",
            "nurse", "nurse_phone",
            "care", "care_phone",
            "created_at", "updated_at"
        ]:
            if col not in df.columns:
                df[col] = ""

    return df.fillna("")

@st.cache_data(ttl=60)
def get_resident_schedule_df_cached():
    df = load_db("resident_schedule")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "id", "resident_id", "weekday", "service_type",
            "start_time", "end_time", "place", "phone", "person_in_charge", "memo"
        ])
    else:
        for col in [
            "id", "resident_id", "weekday", "service_type",
            "start_time", "end_time", "place", "phone", "person_in_charge", "memo"
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")

def save_uploaded_document(
    category1,
    category2,
    category3,
    title,
    summary,
    memo,
    uploaded_file
):
    df = get_document_master_df()

    now_str = now_jst().strftime("%Y-%m-%d %H:%M")

    if df.empty:
        next_id = 1
    else:
        ids = pd.to_numeric(df["document_id"], errors="coerce").dropna()
        next_id = int(ids.max()) + 1 if not ids.empty else 1

    file_bytes = uploaded_file.read()
    file_data_base64 = base64.b64encode(file_bytes).decode("utf-8")

    original_filename = uploaded_file.name
    lower_name = original_filename.lower()

    if lower_name.endswith(".xlsx"):
        file_type = "xlsx"
    elif lower_name.endswith(".xls"):
        file_type = "xls"
    elif lower_name.endswith(".pdf"):
        file_type = "pdf"
    elif lower_name.endswith(".docx"):
        file_type = "docx"
    elif lower_name.endswith(".doc"):
        file_type = "doc"
    else:
        file_type = "other"

    new_row = pd.DataFrame([{
        "document_id": next_id,
        "category1": category1,
        "category2": category2,
        "category3": category3,
        "title": title,
        "file_type": file_type,
        "url": "",
        "summary": summary,
        "memo": memo,
        "status": "有効",
        "updated_at": now_str,
        "created_at": now_str,
        "original_filename": original_filename,
        "file_data_base64": file_data_base64
    }])

    merged_df = pd.concat([df, new_row], ignore_index=True)
    save_db(merged_df, "document_master")

@st.cache_data(ttl=60)
def get_document_master_df_cached():
    df = load_db("document_master")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "document_id", "category1", "category2", "category3",
            "title", "file_type", "url", "summary", "memo",
            "status", "updated_at", "created_at",
            "original_filename", "file_data_base64"
        ])
    else:
        for col in [
            "document_id", "category1", "category2", "category3",
            "title", "file_type", "url", "summary", "memo",
            "status", "updated_at", "created_at",
            "original_filename", "file_data_base64"
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")

def get_download_file_data(row):
    file_data_base64 = str(row.get("file_data_base64", "")).strip()
    original_filename = str(row.get("original_filename", "")).strip()

    if not file_data_base64 or not original_filename:
        return None, None, None

    file_bytes = base64.b64decode(file_data_base64)

    lower_name = original_filename.lower()
    if lower_name.endswith(".xlsx"):
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif lower_name.endswith(".xls"):
        mime = "application/vnd.ms-excel"
    elif lower_name.endswith(".pdf"):
        mime = "application/pdf"
    elif lower_name.endswith(".docx"):
        mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    elif lower_name.endswith(".doc"):
        mime = "application/msword"
    else:
        mime = "application/octet-stream"

    return file_bytes, original_filename, mime

@st.cache_data(ttl=60)
def get_resident_notes_df_cached():
    df = load_db("resident_notes")
    if df is None or df.empty:
        df = pd.DataFrame(columns=["id", "resident_id", "date", "user", "note"])
    else:
        for col in ["id", "resident_id", "date", "user", "note"]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


@st.cache_data(ttl=60)
def get_external_contacts_df_cached():
    df = load_db("external_contacts")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "contact_id", "category1", "category2",
            "name", "organization", "phone", "memo"
        ])
    else:
        for col in [
            "contact_id", "category1", "category2",
            "name", "organization", "phone", "memo"
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


@st.cache_data(ttl=60)
def get_resident_links_df_cached():
    df = load_db("resident_links")
    if df is None or df.empty:
        df = pd.DataFrame(columns=["id", "resident_id", "contact_id", "role"])
    else:
        for col in ["id", "resident_id", "contact_id", "role"]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")

def save_db(df, file, retries=3, delay=1.0):
    sheet_candidates = get_sheet_name_candidates(file)

    last_error = None
    for s_name in sheet_candidates:
        for attempt in range(retries):
            try:
                conn.update(worksheet=s_name, data=df)
                st.cache_data.clear()
                return
            except Exception as e:
                last_error = e
                if attempt < retries - 1:
                    time.sleep(delay + random.random() * 0.7)
                else:
                    pass

    if last_error is not None:
        raise last_error

def update_active_user():
    active_df = load_db("active_users")

    if active_df is None or active_df.empty:
        active_df = pd.DataFrame(columns=["user", "login_at", "last_seen"])
    else:
        for col in ["user", "login_at", "last_seen"]:
            if col not in active_df.columns:
                active_df[col] = ""

    now_str = now_jst().strftime("%Y-%m-%d %H:%M")

    keep_rows = []
    now_naive = now_jst().replace(tzinfo=None)

    for _, row in active_df.fillna("").iterrows():
        last_seen = str(row.get("last_seen", "")).strip()
        if not last_seen:
            continue
        try:
            last_dt = pd.to_datetime(last_seen).to_pydatetime()
            if (now_naive - last_dt).total_seconds() <= 15 * 60:
                keep_rows.append(row)
        except Exception:
            pass

    active_df = pd.DataFrame(keep_rows) if keep_rows else pd.DataFrame(columns=["user", "login_at", "last_seen"])

    current_user = st.session_state.user

    if current_user in active_df["user"].astype(str).tolist():
        active_df.loc[active_df["user"] == current_user, "last_seen"] = now_str
    else:
        new_row = pd.DataFrame([{
            "user": current_user,
            "login_at": st.session_state.get("login_at", now_str),
            "last_seen": now_str
        }])
        active_df = pd.concat([active_df, new_row], ignore_index=True)

    save_db(active_df, "active_users")

def heartbeat_active_user():
    now_ts = now_jst().timestamp()
    last_ping = st.session_state.get("last_active_ping", 0)

    # 5分に1回だけ更新
    if now_ts - last_ping >= 300:
        update_active_user()
        st.session_state["last_active_ping"] = now_ts

def sync_task_events_to_calendar():
    task_df = load_db("task")
    cal_df = load_db("calendar")

    if task_df is None or task_df.empty:
        return

    if cal_df is None or cal_df.empty:
        cal_df = pd.DataFrame(columns=[
            "id", "title", "start", "end", "user", "memo", "source_type", "source_task_id"
        ])
    else:
        for col in ["id", "title", "start", "end", "user", "memo", "source_type", "source_task_id"]:
            if col not in cal_df.columns:
                cal_df[col] = ""

    task_df = task_df.fillna("")
    cal_df = cal_df.fillna("")

    # 既存の task由来イベントだけ一旦消して作り直すある
    cal_df = cal_df[~cal_df["source_type"].isin(["task_deadline", "task_active"])].copy()

    today = datetime.now().date()

    new_events = []

    # 次のID開始値
    if cal_df.empty:
        next_id = 1
    else:
        try:
            next_id = pd.to_numeric(cal_df["id"], errors="coerce").max()
            next_id = 1 if pd.isna(next_id) else int(next_id) + 1
        except Exception:
            next_id = len(cal_df) + 1

    for _, row in task_df.iterrows():
        task_id = str(row.get("id", "")).strip()
        task_name = str(row.get("task", "")).strip()
        status = str(row.get("status", "")).strip()
        user_name = str(row.get("user", "")).strip()
        limit_str = str(row.get("limit", "")).strip()
        updated_at = str(row.get("updated_at", "")).strip()

        # ① 締切イベント
        if limit_str:
            try:
                limit_date = pd.to_datetime(limit_str).date()
                if limit_date > today:
                    new_events.append({
                        "id": next_id,
                        "title": f"締切：{task_name}",
                        "start": str(limit_date),
                        "end": str(limit_date),
                        "user": user_name,
                        "memo": f"タスク期限 / 状態: {status}",
                        "source_type": "task_deadline",
                        "source_task_id": task_id
                    })
                    next_id += 1
            except Exception:
                pass

        # ② 作業中イベント
        if status == "作業中" and updated_at:
            try:
                active_date = pd.to_datetime(updated_at).date()
                new_events.append({
                    "id": next_id,
                    "title": f"作業中：{task_name}",
                    "start": str(active_date),
                    "end": str(active_date),
                    "user": user_name,
                    "memo": f"現在進行中 / 着手: {updated_at}",
                    "source_type": "task_active",
                    "source_task_id": task_id
                })
                next_id += 1
            except Exception:
                pass

    if new_events:
        add_df = pd.DataFrame(new_events)
        cal_df = pd.concat([cal_df, add_df], ignore_index=True)

    cal_df = cal_df.fillna("")
    save_db(cal_df, "calendar")

def get_urgent_tasks_df():
    df = load_db("task")

    if df is None or df.empty:
        return pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])

    for col in ["id", "task", "status", "user", "limit", "priority", "updated_at"]:
        if col not in df.columns:
            df[col] = ""

    df = df.fillna("").copy()

    urgent_df = df[
        df["priority"].astype(str).str.strip().isin(["至急", "重要"]) &
        (df["status"].astype(str).str.strip() != "完了")
    ].copy()

    if urgent_df.empty:
        return urgent_df

    prio_map = {"至急": 0, "重要": 1}
    urgent_df["prio_sort"] = urgent_df["priority"].map(prio_map).fillna(9)

    try:
        urgent_df["limit_sort"] = pd.to_datetime(urgent_df["limit"], errors="coerce")
    except Exception:
        urgent_df["limit_sort"] = pd.NaT

    urgent_df = urgent_df.sort_values(["prio_sort", "limit_sort", "updated_at"], ascending=[True, True, False])
    return urgent_df


def start_task(task_id):
    df = load_db("task")
    if df is None or df.empty:
        return

    df = df.fillna("").copy()
    df.loc[df["id"].astype(str) == str(task_id), ["status", "user", "updated_at"]] = [
        "作業中",
        st.session_state.user,
        now_jst().strftime("%Y-%m-%d %H:%M")
    ]
    save_db(df, "task")
    sync_task_events_to_calendar()


def complete_task(task_id):
    df = load_db("task")
    if df is None or df.empty:
        return

    df = df.fillna("").copy()
    df.loc[df["id"].astype(str) == str(task_id), ["status", "updated_at"]] = [
        "完了",
        now_jst().strftime("%Y-%m-%d %H:%M")
    ]
    save_db(df, "task")
    sync_task_events_to_calendar()


def go_to_page(page_name):
    st.session_state.current_page = page_name
    st.rerun()


def render_urgent_banner():
    urgent_df = get_urgent_tasks_df()

    if urgent_df.empty:
        return

    urgent_count = len(urgent_df)
    critical_count = len(urgent_df[urgent_df["priority"].astype(str).str.strip() == "至急"])
    important_count = len(urgent_df[urgent_df["priority"].astype(str).str.strip() == "重要"])

    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, #ff7b54 0%, #ff9f43 100%);
            color: white;
            padding: 14px 18px;
            border-radius: 12px;
            margin-bottom: 16px;
            font-weight: 700;
            box-shadow: 0 2px 8px rgba(0,0,0,0.12);
        ">
            🚨 緊急タスクあり　合計 {urgent_count}件（至急 {critical_count}件 / 重要 {important_count}件）
        </div>
        """,
        unsafe_allow_html=True
    )

    col_a, col_b = st.columns([1, 5])
    with col_a:
        if st.button("緊急一覧を開く", key="open_urgent_page_button", use_container_width=True):
            go_to_page("⑧ 緊急一覧")
    with col_b:
        st.caption("クリックして、至急・重要タスクの一覧を確認できるある。")

# ==========================================
# 🔑 ユーザー認証
# ==========================================
if "user" not in st.session_state:
    st.markdown("<style>[data-testid='stSidebarNav'] {display: none;}</style>", unsafe_allow_html=True)

    st.caption("APP_VERSION = 2026-03-21-knowbe-debug-01")
    st.warning("### 事業所と担当者を選んでログインしてください💻")

    if "office_key" not in st.session_state:
        st.session_state.office_key = "support"

    office_options = ["support", "home"]
    office_labels = {
        "support": "サポート",
        "home": "ホーム",
    }

    st.markdown("接続先事業所を選択してください")
    office_key = st.radio(
        "接続先事業所を選択してください",
        office_options,
        index=0 if st.session_state.get("office_key", "support") == "support" else 1,
        format_func=lambda x: office_labels.get(x, x),
        horizontal=True,
        label_visibility="collapsed",
        key="login_office_key",
    )

    st.session_state.office_key = office_key

    st.divider()

    user_list = [
        "木村 由美", "秋吉 幸雄", "安心院 拓也", "粟田 絵利菜", "小宅 正嗣",
        "土居 容子", "中本 匡", "中本 文代", "中本 雄斗", "伴 法子", "栁川 幸恵", "山口 晴彦"
    ]

    user = st.radio(
        "担当者を選択してください",
        user_list,
        index=None
    )

    if st.button("システムへログイン", use_container_width=True):
        if user:
            st.session_state.user = user
            st.session_state.login_at = now_jst().strftime("%Y-%m-%d %H:%M")
            st.session_state.last_active_ping = 0
            st.rerun()
        else:
            st.error("担当者を選択してください。")

    st.stop()

# ==========================================
# 🏠 メインメニュー
# ==========================================
heartbeat_active_user()

page_options = [
    "⓪ 検索",
    "① 未着手の任務（掲示板）",
    "② タスクの引き受け・報告",
    "③ 稼働状況・完了履歴",
    "④ チームチャット",
    "⑤ 業務マニュアル",
    "⑥ 日誌入力状況",
    "⑦ タスクカレンダー",
    "⑧ 緊急一覧",
    "⑨ 利用者情報",
    "⑩ 書類アップロード",
    "書類_個別支援計画案",
    "書類_サービス担当者会議",
    "書類_個別支援計画",
    "書類_モニタリング",
    "書類_在宅評価シート",
    "書類_アセスメント",
    "書類_基本シート",
    "書類_就労分野シート",
    "🐝knowbe日誌入力🐝",
]

if "current_page" not in st.session_state or st.session_state.current_page not in page_options:
    st.session_state.current_page = "① 未着手の任務（掲示板）"

st.sidebar.markdown(
"""
<div style="display:flex;align-items:center;gap:10px;margin-left:20px;">
    <div style="font-size:36px;">&#128029;</div>
    <div>
        <div style="font-weight:bold;font-size:24px;">
        Sue for Bee
        </div>
        <div style="font-size:16px;color:gray;">
        Assistance System
        </div>
    </div>
</div>
""",
unsafe_allow_html=True
)

st.sidebar.markdown("<div style='margin-top:30px;'></div>", unsafe_allow_html=True)
st.sidebar.markdown("メニューを選択してください")

st.sidebar.markdown(
    """
    <style>
    section[data-testid="stSidebar"] .stButton {
        margin-bottom: 12px !important;
    }

    section[data-testid="stSidebar"] .stButton > button {
        width: 100% !important;
        height: 56px !important;
        min-height: 56px !important;
        border-radius: 12px !important;
        border: 1px solid #d9d9d9 !important;
        background: #ffffff !important;
        color: #1f2d3d !important;
        font-weight: 700 !important;
        padding: 0 !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
        display: flex !important;
        align-items: center !important;
        justify-content: flex-start !important;
        text-align: left !important;
        box-sizing: border-box !important;
    }

    section[data-testid="stSidebar"] .stButton > button:hover {
        border-color: #ff9f43 !important;
        color: #ff7b54 !important;
        background: #fffaf5 !important;
    }

    section[data-testid="stSidebar"] .stButton > button > div {
        width: 100% !important;
        height: 56px !important;
        display: flex !important;
        align-items: center !important;
        justify-content: flex-start !important;
        text-align: left !important;
        padding: 0 16px !important;
        box-sizing: border-box !important;
    }

    section[data-testid="stSidebar"] .stButton > button > div p,
    section[data-testid="stSidebar"] .stButton > button > div span {
        width: 100% !important;
        margin: 0 !important;
        text-align: left !important;
        justify-content: flex-start !important;
        line-height: 1.2 !important;
    }

    .menu-selected-wrap {
        width: 100%;
        margin: 0 0 12px 0;
    }

    .menu-selected-box {
        width: 100%;
        height: 56px;
        border-radius: 12px;
        border: 1px solid #ff9f43;
        background: linear-gradient(90deg, #fff1e8 0%, #fff7e6 100%);
        color: #d35400;
        font-weight: 700;
        padding: 0 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        text-align: center;
        display: flex;
        align-items: center;
        justify-content: center;
        box-sizing: border-box;
        line-height: 1.2;
    }

    section[data-testid="stSidebar"] div[data-testid="stMarkdown"] {
        margin-bottom: 0 !important;
    }

    section[data-testid="stSidebar"] div[data-testid="stMarkdownContainer"] {
        margin: 0 !important;
        padding: 0 !important;
    }

    section[data-testid="stSidebar"] div[data-testid="stMarkdownContainer"] p {
        margin: 0 !important;
        padding: 0 !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

def heart_label(text: str) -> str:
    if not st.session_state.get("heart_mode", False):
        return str(text)

    s = str(text)

    if len(s) >= 2 and s[1] == " ":
        return f"💕 {s[2:]}"
    if len(s) >= 3 and s[2] == " ":
        return f"💕 {s[3:]}"

    if "knowbe" in s:
        return "💕knowbe日誌入力💕"

    return f"💕 {s}"

if "bee_menu_unlocked" not in st.session_state:
    st.session_state["bee_menu_unlocked"] = False
if "secret_doc_mode" not in st.session_state:
    st.session_state["secret_doc_mode"] = False
if "heart_mode" not in st.session_state:
    st.session_state["heart_mode"] = False

main_page_options = [
    "⓪ 検索",
    "① 未着手の任務（掲示板）",
    "② タスクの引き受け・報告",
    "③ 稼働状況・完了履歴",
    "④ チームチャット",
    "⑤ 業務マニュアル",
    "⑥ 日誌入力状況",
    "⑦ タスクカレンダー",
    "⑧ 緊急一覧",
    "⑨ 利用者情報",
    "⑩ 書類アップロード",
]

document_page_options = [
    ("書類_個別支援計画案", "🤫個別支援計画案🤫" if st.session_state.get("secret_doc_mode", False) else "個別支援計画案"),
    ("書類_サービス担当者会議", "🤫サービス担当者会議🤫" if st.session_state.get("secret_doc_mode", False) else "サービス担当者会議"),
    ("書類_個別支援計画", "🤫個別支援計画🤫" if st.session_state.get("secret_doc_mode", False) else "個別支援計画"),
    ("書類_モニタリング", "🤫モニタリング🤫" if st.session_state.get("secret_doc_mode", False) else "モニタリング"),
    ("書類_在宅評価シート", "在宅評価シート"),
    ("書類_アセスメント", "アセスメント"),
    ("書類_基本シート", "基本シート"),
    ("書類_就労分野シート", "就労分野シート"),
]

def process_secret_command():
    cmd = str(st.session_state.get("secret_bee_cmd", "")).strip()

    if cmd == "🐝":
        st.session_state["bee_menu_unlocked"] = True
    elif cmd == "🤫":
        st.session_state["secret_doc_mode"] = True
    elif cmd == "💕":
        st.session_state["heart_mode"] = True

    st.session_state["secret_bee_cmd"] = ""


# ===== メインメニュー =====
for p in main_page_options:
    is_selected = (st.session_state.current_page == p)
    display_p = heart_label(p)

    if is_selected:
        st.sidebar.markdown(
            f'<div class="menu-selected-wrap"><div class="menu-selected-box">● {display_p}</div></div>',
            unsafe_allow_html=True
        )
    else:
        if st.sidebar.button(display_p, key=f"menu_{p}", use_container_width=True):
            st.session_state.current_page = p
            st.rerun()


# ===== 利用者書類 =====
st.sidebar.markdown("### 利用者書類")

for page_key, label in document_page_options:
    is_selected = (st.session_state.current_page == page_key)
    display_label = heart_label(label)

    if is_selected:
        st.sidebar.markdown(
            f'<div class="menu-selected-wrap"><div class="menu-selected-box">● {display_label}</div></div>',
            unsafe_allow_html=True
        )
    else:
        if st.sidebar.button(display_label, key=f"menu_{page_key}", use_container_width=True):
            st.session_state.current_page = page_key
            st.rerun()


# ===== ログアウト（ここ固定） =====
if st.sidebar.button("ログアウト", use_container_width=True):
    if "user" in st.session_state:
        del st.session_state.user
    if "office_key" in st.session_state:
        del st.session_state.office_key
    if "login_at" in st.session_state:
        del st.session_state.login_at
    if "last_active_ping" in st.session_state:
        del st.session_state.last_active_ping
    if "current_page" in st.session_state:
        del st.session_state.current_page
    if "bee_menu_unlocked" in st.session_state:
        del st.session_state.bee_menu_unlocked
    if "secret_doc_mode" in st.session_state:
        del st.session_state.secret_doc_mode
    if "heart_mode" in st.session_state:
        del st.session_state.heart_mode
    if "secret_bee_cmd" in st.session_state:
        del st.session_state.secret_bee_cmd
    st.rerun()


# ===== 🐝 knowbe（条件表示） =====
if st.session_state.get("bee_menu_unlocked", False):
    knowbe_label = "🐝knowbe日誌入力🐝"
    if st.session_state.get("heart_mode", False):
        knowbe_label = "💕knowbe日誌入力💕"

    if st.sidebar.button(knowbe_label, key="knowbe_menu_button", use_container_width=True):
        st.session_state.current_page = "🐝knowbe日誌入力🐝"
        st.rerun()


# ===== 入力欄 =====
st.sidebar.text_input(
    "secret command",
    key="secret_bee_cmd",
    label_visibility="collapsed",
    on_change=process_secret_command,
)


# ===== 最下部 =====
st.sidebar.divider()
st.sidebar.caption("System Version 2.0")

page = st.session_state.current_page

render_urgent_banner()

TEMPLATE_FILES = {
    "個別支援計画案": "個別支援計画案.xlsx",
    "個別支援計画": "個別支援計画.xlsx",
    "サービス担当者会議": "サービス担当者会議.xlsx",
    "モニタリング": "モニタリング.xlsx",
    "在宅評価シート": "在宅評価シート.xlsx",
    "アセスメント": "アセスメントシート.xlsx",
    "基本シート": "基本シート.xlsx",
    "就労分野シート": "就労分野シート.xlsx",    
}


def create_excel_file(template_name, cell_data):

    template = TEMPLATE_FILES[template_name]

    wb = load_workbook(template)
    ws = wb.active

    for cell, value in cell_data.items():
        ws[cell] = value

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)

    return buffer

def render_plan_form_page(doc_title: str):
    st.title(f"📄 {doc_title}")
    st.caption("まずは入力しやすい形の試作ページある。まだ保存はしないある。")

    master_df = get_resident_master_df()

    if master_df is None or master_df.empty:
        st.warning("利用者情報がまだ登録されてないある。先に⑨ 利用者情報から利用者を登録してほしいある。")
        return

    master_df = master_df.fillna("").copy()

    resident_options = []
    resident_map = {}

    for _, row in master_df.iterrows():
        rid = str(row.get("resident_id", "")).strip()
        rname = str(row.get("resident_name", "")).strip()
        status = str(row.get("status", "")).strip()

        if not rname:
            continue

        label = f"{rname}"
        if rid:
            label += f" ({rid})"
        if status:
            label += f" / {status}"

        resident_options.append(label)
        resident_map[label] = row.to_dict()

    if not resident_options:
        st.warning("利用者情報がまだ登録されてないある。")
        return

    st.markdown("## 基本情報")

    selected_label = st.selectbox(
        "誰の書類を入力するか",
        resident_options,
        key=f"{doc_title}_resident_select"
    )

    selected_row = resident_map[selected_label]
    resident_name = str(selected_row.get("resident_name", "")).strip()

    basic_cols = st.columns([7, 2, 2, 2])

    with basic_cols[0]:
        st.text_input(
            "利用者氏名",
            value=resident_name,
            key=f"{doc_title}_resident_name",
            disabled=True
        )

    with basic_cols[1]:
        year_val = st.text_input("作成年（西暦）", key=f"{doc_title}_year", placeholder="2026")
    with basic_cols[2]:
        month_val = st.text_input("月", key=f"{doc_title}_month", placeholder="3")
    with basic_cols[3]:
        day_val = st.text_input("日", key=f"{doc_title}_day", placeholder="14")

    st.divider()

    st.markdown("## 本文入力")

    policy_val = st.text_area(
        "サービス等利用計画の総合的な方針",
        key=f"{doc_title}_policy",
        height=120,
        placeholder="B8 に入る内容ある"
    )

    long_goal_val = st.text_area(
        "長期目標（内容・期間等）",
        key=f"{doc_title}_long_goal",
        height=100,
        placeholder="B10 に入る内容ある"
    )

    short_goal_val = st.text_area(
        "短期目標（内容・期間等）",
        key=f"{doc_title}_short_goal",
        height=100,
        placeholder="B12 に入る内容ある"
    )

    st.divider()
    st.markdown("## 具体的達成目標（3行）")
    st.caption("帳票の 17〜19 行に入る部分ある。")

    header_cols = st.columns([5, 3, 4, 2, 2, 2])
    with header_cols[0]:
        st.markdown("**具体的達成目標**")
    with header_cols[1]:
        st.markdown("**本人の役割**")
    with header_cols[2]:
        st.markdown("**支援内容**")
    with header_cols[3]:
        st.markdown("**支援期間**")
    with header_cols[4]:
        st.markdown("**担当者**")
    with header_cols[5]:
        st.markdown("**優先順位**")

    row_data = []

    for i in range(1, 4):
        st.markdown(f"### {i}行目")
        row_cols = st.columns([5, 3, 4, 2, 2, 2])

        with row_cols[0]:
            target_val = st.text_area(
                f"{i}行目_具体的達成目標",
                key=f"{doc_title}_target_{i}",
                height=90,
                label_visibility="collapsed",
                placeholder="C17〜C19"
            )

        with row_cols[1]:
            role_val = st.text_area(
                f"{i}行目_本人の役割",
                key=f"{doc_title}_role_{i}",
                height=90,
                label_visibility="collapsed",
                placeholder="G17〜G19"
            )

        with row_cols[2]:
            support_val = st.text_area(
                f"{i}行目_支援内容",
                key=f"{doc_title}_support_{i}",
                height=90,
                label_visibility="collapsed",
                placeholder="J17〜J19"
            )

        with row_cols[3]:
            period_val = st.text_input(
                f"{i}行目_支援期間",
                key=f"{doc_title}_period_{i}",
                label_visibility="collapsed",
                placeholder="M17〜M19"
            )

        with row_cols[4]:
            person_val = st.text_input(
                f"{i}行目_担当者",
                key=f"{doc_title}_person_{i}",
                label_visibility="collapsed",
                placeholder="O17〜O19"
            )

        with row_cols[5]:
            priority_val = st.selectbox(
                f"{i}行目_優先順位",
                ["", "1", "2", "3"],
                key=f"{doc_title}_priority_{i}",
                label_visibility="collapsed"
            )

        row_data.append({
            "target": target_val,
            "role": role_val,
            "support": support_val,
            "period": period_val,
            "person": person_val,
            "priority": priority_val,
        })

    st.divider()
    st.markdown("## 同意・担当者")

    agree_cols = st.columns([2, 2, 2, 4])

    with agree_cols[0]:
        agree_year_val = st.text_input("同意日_西暦", key=f"{doc_title}_agree_year", placeholder="2026")
    with agree_cols[1]:
        agree_month_val = st.text_input("同意日_月", key=f"{doc_title}_agree_month", placeholder="3")
    with agree_cols[2]:
        agree_day_val = st.text_input("同意日_日", key=f"{doc_title}_agree_day", placeholder="14")
    with agree_cols[3]:
        manager_val = st.text_input("サービス担当責任者", key=f"{doc_title}_manager", placeholder="N21")

    st.divider()

    with st.expander("入力内容確認"):
        st.write(f"利用者氏名: {resident_name}")
        st.write(f"作成年月日: {year_val} / {month_val} / {day_val}")
        st.write(f"総合的な方針: {policy_val}")
        st.write(f"長期目標: {long_goal_val}")
        st.write(f"短期目標: {short_goal_val}")
        st.write(f"同意書日付: {agree_year_val} / {agree_month_val} / {agree_day_val}")
        st.write(f"サービス担当責任者: {manager_val}")

        for idx, item in enumerate(row_data, start=1):
            st.markdown(f"**{idx}行目**")
            st.write(item)

    st.divider()
    st.markdown("### Excel出力")

    if st.button("Excelを作成", key=f"{doc_title}_make_excel"):

        cell_data = {
            "C4": resident_name,
            "M3": year_val,
            "O3": month_val,
            "Q3": day_val,
            "M4": manager_val
        }

        template_name = doc_title
        file = create_excel_file(template_name, cell_data)

        st.download_button(
            label="ダウンロード",
            data=file,
            file_name=f"{doc_title}_{year_val}.{month_val}.{day_val}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{doc_title}_download_excel"
        )

def render_meeting_form_page(doc_title: str):
    st.title(f"📄 {doc_title}")
    st.caption("サービス担当者会議の入力UIある。まだ保存はしないある。")

    master_df = get_resident_master_df()

    if master_df is None or master_df.empty:
        st.warning("利用者情報がまだ登録されてないある。先に⑨ 利用者情報から利用者を登録してほしいある。")
        return

    master_df = master_df.fillna("").copy()

    resident_options = []
    resident_map = {}

    for _, row in master_df.iterrows():
        rid = str(row.get("resident_id", "")).strip()
        rname = str(row.get("resident_name", "")).strip()
        status = str(row.get("status", "")).strip()

        if not rname:
            continue

        label = f"{rname}"
        if rid:
            label += f" ({rid})"
        if status:
            label += f" / {status}"

        resident_options.append(label)
        resident_map[label] = row.to_dict()

    if not resident_options:
        st.warning("利用者情報がまだ登録されてないある。")
        return

    st.markdown("## 基本情報")

    selected_label = st.selectbox(
        "誰の書類を入力するか",
        resident_options,
        key=f"{doc_title}_resident_select"
    )

    selected_row = resident_map[selected_label]
    resident_name = str(selected_row.get("resident_name", "")).strip()

    # -----------------------------
    # 作成年月日
    # M3 / O3 / Q3
    # -----------------------------
    st.markdown("### 作成年月日")
    create_cols = st.columns([2, 2, 2, 6])

    with create_cols[0]:
        create_year = st.text_input("作成年（西暦）", key=f"{doc_title}_create_year", placeholder="2026")
    with create_cols[1]:
        create_month = st.text_input("月", key=f"{doc_title}_create_month", placeholder="3")
    with create_cols[2]:
        create_day = st.text_input("日", key=f"{doc_title}_create_day", placeholder="14")

    # -----------------------------
    # 利用者名 / 作成者
    # C4 / M4
    # -----------------------------
    base_cols = st.columns([6, 4])

    with base_cols[0]:
        st.text_input(
            "利用者名",
            value=resident_name,
            key=f"{doc_title}_resident_name",
            disabled=True
        )
    with base_cols[1]:
        creator_name = st.text_input(
            "作成者（担当者）名",
            value=st.session_state.user,
            key=f"{doc_title}_creator_name"
        )

    st.divider()

    # -----------------------------
    # 開催日時 / 開催場所
    # C5 E5 G5 / M5
    # -----------------------------
    st.markdown("## 開催情報")

    meeting_cols = st.columns([2, 2, 2, 6])

    with meeting_cols[0]:
        meeting_year = st.text_input("開催年（西暦）", key=f"{doc_title}_meeting_year", placeholder="2026")
    with meeting_cols[1]:
        meeting_month = st.text_input("開催月", key=f"{doc_title}_meeting_month", placeholder="3")
    with meeting_cols[2]:
        meeting_day = st.text_input("開催日", key=f"{doc_title}_meeting_day", placeholder="14")
    with meeting_cols[3]:
        meeting_place = st.text_input("開催場所", key=f"{doc_title}_meeting_place", placeholder="事業所相談室")

    st.divider()

    # -----------------------------
    # 会議出席者
    # E8 J8 O8 / E9 J9 O9 / E10 J10 O10
    # -----------------------------
    st.markdown("## 会議出席者")

    header_cols = st.columns(3)
    with header_cols[0]:
        st.markdown("**左列**")
    with header_cols[1]:
        st.markdown("**中央列**")
    with header_cols[2]:
        st.markdown("**右列**")

    row1 = st.columns(3)
    with row1[0]:
        manager_name = st.text_input("管理者名", key=f"{doc_title}_manager_name")
    with row1[1]:
        staff_name = st.text_input("支援員名", key=f"{doc_title}_staff_name")
    with row1[2]:
        attendee_user_name = st.text_input(
            "利用者名",
            value=resident_name,
            key=f"{doc_title}_attendee_user_name"
        )

    row2 = st.columns(3)
    with row2[0]:
        care_manager_name = st.text_input("ケアマネ", key=f"{doc_title}_care_manager_name")
    with row2[1]:
        nurse_name = st.text_input("看護師", key=f"{doc_title}_nurse_name")
    with row2[2]:
        family_name = st.text_input("親族", key=f"{doc_title}_family_name")

    row3 = st.columns(3)
    with row3[0]:
        service_manager_name = st.text_input("サービス管理責任者", key=f"{doc_title}_service_manager_name")
    with row3[1]:
        consultant_name = st.text_input("相談員", key=f"{doc_title}_consultant_name")
    with row3[2]:
        keyperson_name = st.text_input("キーパーソン", key=f"{doc_title}_keyperson_name")

    st.divider()

    # -----------------------------
    # 本文
    # C11〜C14
    # -----------------------------
    st.markdown("## 会議内容")

    agenda = st.text_area(
        "議題",
        key=f"{doc_title}_agenda",
        height=90,
        placeholder="C11"
    )

    discussion = st.text_area(
        "検討内容",
        key=f"{doc_title}_discussion",
        height=140,
        placeholder="C12"
    )

    issues_left = st.text_area(
        "残された課題",
        key=f"{doc_title}_issues_left",
        height=100,
        placeholder="C13"
    )

    conclusion = st.text_area(
        "結論",
        key=f"{doc_title}_conclusion",
        height=100,
        placeholder="C14"
    )

    st.divider()

    with st.expander("入力内容確認"):
        st.write(f"利用者名: {resident_name}")
        st.write(f"作成年月日: {create_year} / {create_month} / {create_day}")
        st.write(f"作成者: {creator_name}")
        st.write(f"開催日時: {meeting_year} / {meeting_month} / {meeting_day}")
        st.write(f"開催場所: {meeting_place}")

        st.markdown("**会議出席者**")
        st.write({
            "管理者名": manager_name,
            "支援員名": staff_name,
            "利用者名": attendee_user_name,
            "ケアマネ": care_manager_name,
            "看護師": nurse_name,
            "親族": family_name,
            "サービス管理責任者": service_manager_name,
            "相談員": consultant_name,
            "キーパーソン": keyperson_name,
        })

        st.write(f"議題: {agenda}")
        st.write(f"検討内容: {discussion}")
        st.write(f"残された課題: {issues_left}")
        st.write(f"結論: {conclusion}")

    st.divider()
    st.markdown("### Excel出力")

    if st.button("Excelを作成", key=f"{doc_title}_make_excel"):

        cell_data = {
            "M3": create_year,
            "O3": create_month,
            "Q3": create_day,
            "C4": resident_name,
            "M4": creator_name,
            "C5": meeting_year,
            "E5": meeting_month,
            "G5": meeting_day,
            "M5": meeting_place,
            "E8": manager_name,
            "J8": staff_name,
            "O8": attendee_user_name,
            "E9": care_manager_name,
            "J9": nurse_name,
            "O9": family_name,
            "E10": service_manager_name,
            "J10": consultant_name,
            "O10": keyperson_name,
            "C11": agenda,
            "C12": discussion,
            "C13": issues_left,
            "C14": conclusion,
        }

        file = create_excel_file("サービス担当者会議", cell_data)

        st.download_button(
            label="ダウンロード",
            data=file,
            file_name=f"サービス担当者会議_{create_year}.{create_month}.{create_day}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{doc_title}_download_excel"
        )


def render_monitoring_form_page(doc_title: str):
    st.title(f"📄 {doc_title}")
    st.caption("モニタリングの入力UIある。まだ保存はしないある。")

    master_df = get_resident_master_df()

    if master_df is None or master_df.empty:
        st.warning("利用者情報がまだ登録されてないある。先に⑨ 利用者情報から利用者を登録してほしいある。")
        return

    master_df = master_df.fillna("").copy()

    resident_options = []
    resident_map = {}

    for _, row in master_df.iterrows():
        rid = str(row.get("resident_id", "")).strip()
        rname = str(row.get("resident_name", "")).strip()
        status = str(row.get("status", "")).strip()

        if not rname:
            continue

        label = f"{rname}"
        if rid:
            label += f" ({rid})"
        if status:
            label += f" / {status}"

        resident_options.append(label)
        resident_map[label] = row.to_dict()

    if not resident_options:
        st.warning("利用者情報がまだ登録されてないある。")
        return

    st.markdown("## 基本情報")

    selected_label = st.selectbox(
        "誰の書類を入力するか",
        resident_options,
        key=f"{doc_title}_resident_select"
    )

    selected_row = resident_map[selected_label]
    resident_name = str(selected_row.get("resident_name", "")).strip()

    base_cols = st.columns([6, 2, 2, 2])

    with base_cols[0]:
        st.text_input(
            "利用者名",
            value=resident_name,
            key=f"{doc_title}_resident_name",
            disabled=True
        )

    with base_cols[1]:
        year_val = st.text_input("実施年（西暦）", key=f"{doc_title}_year", placeholder="2026")
    with base_cols[2]:
        month_val = st.text_input("月", key=f"{doc_title}_month", placeholder="3")
    with base_cols[3]:
        day_val = st.text_input("日", key=f"{doc_title}_day", placeholder="14")

    st.divider()

    st.markdown("## モニタリング内容")
    st.caption("具体的達成目標番号ごとに入力するある。")

    row_data = []

    for i in range(1, 4):
        st.markdown(f"### 具体的達成目標番号{i}")

        row_cols = st.columns([2, 5, 5])

        with row_cols[0]:
            status_val = st.selectbox(
                f"達成状況の評価_{i}",
                ["", "達成", "継続", "一部達成", "終了"],
                key=f"{doc_title}_status_{i}"
            )

        with row_cols[1]:
            detail_val = st.text_area(
                f"達成できている点と未達成点（要因も）_{i}",
                key=f"{doc_title}_detail_{i}",
                height=120,
                placeholder=f"D{7+i}"
            )

        with row_cols[2]:
            future_val = st.text_area(
                f"今後の対応（支援内容・方法の変更・継続・終了）_{i}",
                key=f"{doc_title}_future_{i}",
                height=120,
                placeholder=f"E{7+i}"
            )

        row_data.append({
            "status": status_val,
            "detail": detail_val,
            "future": future_val,
        })

        st.divider()

    with st.expander("入力内容確認"):
        st.write(f"利用者名: {resident_name}")
        st.write(f"実施年月日: {year_val} / {month_val} / {day_val}")

        for idx, item in enumerate(row_data, start=1):
            st.markdown(f"**具体的達成目標番号{idx}**")
            st.write(item)

    st.divider()
    st.markdown("### Excel出力")

    if st.button("Excelを作成", key=f"{doc_title}_make_excel"):

        cell_data = {
            "C5": resident_name,
            "E5": year_val,
            "G5": month_val,
            "I5": day_val,

            "C8": row_data[0]["status"],
            "D8": row_data[0]["detail"],
            "E8": row_data[0]["future"],

            "C9": row_data[1]["status"],
            "D9": row_data[1]["detail"],
            "E9": row_data[1]["future"],

            "C10": row_data[2]["status"],
            "D10": row_data[2]["detail"],
            "E10": row_data[2]["future"],
        }

        file = create_excel_file("モニタリング", cell_data)

        st.download_button(
            label="ダウンロード",
            data=file,
            file_name=f"モニタリング_{year_val}.{month_val}.{day_val}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{doc_title}_download_excel"
        )

def get_saturday_dates_for_month(year: int, month: int):
    cal = py_calendar.monthcalendar(year, month)
    saturdays = []

    for week in cal:
        sat_day = week[py_calendar.SATURDAY]
        if sat_day != 0:
            saturdays.append(date(year, month, sat_day))

    return saturdays


def render_home_evaluation_form_page(doc_title: str):
    st.title(f"📄 {doc_title}")
    st.caption("在宅評価シートの入力UIある。まだ保存はしないある。")

    master_df = get_resident_master_df()

    if master_df is None or master_df.empty:
        st.warning("利用者情報がまだ登録されてないある。先に⑨ 利用者情報から利用者を登録してほしいある。")
        return

    master_df = master_df.fillna("").copy()

    resident_options = []
    resident_map = {}

    for _, row in master_df.iterrows():
        rid = str(row.get("resident_id", "")).strip()
        rname = str(row.get("resident_name", "")).strip()
        status = str(row.get("status", "")).strip()

        if not rname:
            continue

        label = f"{rname}"
        if rid:
            label += f" ({rid})"
        if status:
            label += f" / {status}"

        resident_options.append(label)
        resident_map[label] = row.to_dict()

    if not resident_options:
        st.warning("利用者情報がまだ登録されてないある。")
        return

    st.markdown("## 基本情報")

    selected_label = st.selectbox(
        "誰の書類を入力するか",
        resident_options,
        key=f"{doc_title}_resident_select"
    )

    selected_row = resident_map[selected_label]
    resident_name = str(selected_row.get("resident_name", "")).strip()

    base_cols = st.columns([6, 2, 2])

    with base_cols[0]:
        st.text_input(
            "利用者名",
            value=resident_name,
            key=f"{doc_title}_resident_name",
            disabled=True
        )

    with base_cols[1]:
        year_val = st.text_input("年（西暦）", key=f"{doc_title}_year", placeholder="2026")
    with base_cols[2]:
        month_val = st.text_input("月", key=f"{doc_title}_month", placeholder="3")

    st.divider()

    st.markdown("## 目標")
    goal1 = st.text_area("目標1", key=f"{doc_title}_goal1", height=80, placeholder="B7")
    goal2 = st.text_area("目標2", key=f"{doc_title}_goal2", height=80, placeholder="B8")
    goal3 = st.text_area("目標3", key=f"{doc_title}_goal3", height=80, placeholder="B9")

    st.divider()

    service_manager = st.text_input(
        "サービス管理責任者",
        value=st.session_state.user,
        key=f"{doc_title}_service_manager",
        placeholder="H11"
    )

    st.divider()

    st.markdown("## 月間評価")
    monthly1 = st.text_area("月間評価1", key=f"{doc_title}_monthly1", height=90, placeholder="B12")
    monthly2 = st.text_area("月間評価2", key=f"{doc_title}_monthly2", height=90, placeholder="B13")
    monthly3 = st.text_area("月間評価3", key=f"{doc_title}_monthly3", height=90, placeholder="B14")

    st.divider()

    st.markdown("## 週ごとの評価（週報）")
    st.caption("土曜日の日付は、入力した年・月から自動表示するある。")

    ym_key = f"{doc_title}_auto_year_month"
    current_ym = f"{year_val}-{month_val}"

    # 先に土曜日一覧を計算する
    try:
        y = int(str(year_val).strip())
        m = int(str(month_val).strip())
        if 1 <= m <= 12:
            saturday_dates = get_saturday_dates_for_month(y, m)
        else:
            saturday_dates = []
    except Exception:
        saturday_dates = []

    # 年月が変わったときだけ自動反映
    if st.session_state.get(ym_key) != current_ym:
        for i in range(1, 6):
            sat_key = f"{doc_title}_sat_{i}"
            sat_text = ""
            if len(saturday_dates) >= i:
                sat_text = saturday_dates[i - 1].strftime("%Y-%m-%d")
            st.session_state[sat_key] = sat_text

        st.session_state[ym_key] = current_ym
    try:
        y = int(str(year_val).strip())
        m = int(str(month_val).strip())
        if 1 <= m <= 12:
            saturday_dates = get_saturday_dates_for_month(y, m)
    except Exception:
        saturday_dates = []

    week_rows = []

    for i in range(1, 6):
        st.markdown(f"### 第{i}週")

        sat_key = f"{doc_title}_sat_{i}"

        row1 = st.columns([2, 8])
        with row1[0]:
            sat_input = st.text_input(
                f"第{i}週 土曜日日付",
                key=sat_key
            )
        with row1[1]:
            weekly_report = st.text_area(
                f"第{i}週 週報",
                key=f"{doc_title}_weekly_report_{i}",
                height=80,
                placeholder=f"C{17 + i * 2}"
            )

        row2 = st.columns([6, 4])
        with row2[1]:
            visit_manager = st.text_input(
                f"第{i}週 訪問したサービス管理責任者名",
                key=f"{doc_title}_visit_manager_{i}",
                placeholder=f"J{18 + i * 2}"
            )

        week_rows.append({
            "sat_date": sat_input,
            "weekly_report": weekly_report,
            "visit_manager": visit_manager,
        })

        st.divider()

    with st.expander("入力内容確認"):
        st.write(f"利用者名: {resident_name}")
        st.write(f"対象年月: {year_val}年 / {month_val}月")
        st.write(f"サービス管理責任者: {service_manager}")

        st.markdown("**目標**")
        st.write({
            "目標1": goal1,
            "目標2": goal2,
            "目標3": goal3,
        })

        st.markdown("**月間評価**")
        st.write({
            "月間評価1": monthly1,
            "月間評価2": monthly2,
            "月間評価3": monthly3,
        })

        for idx, item in enumerate(week_rows, start=1):
            st.markdown(f"**第{idx}週**")
            st.write(item)

    st.divider()
    st.markdown("### Excel出力")

    if st.button("Excelを作成", key=f"{doc_title}_make_excel"):

        cell_data = {
            "B3": resident_name,
            "J3": year_val,
            "L3": month_val,

            "B7": goal1,
            "B8": goal2,
            "B9": goal3,

            "H11": service_manager,

            "B12": monthly1,
            "B13": monthly2,
            "B14": monthly3,

            "A19": week_rows[0]["sat_date"],
            "C19": week_rows[0]["weekly_report"],
            "J20": week_rows[0]["visit_manager"],

            "A21": week_rows[1]["sat_date"],
            "C21": week_rows[1]["weekly_report"],
            "J22": week_rows[1]["visit_manager"],

            "A23": week_rows[2]["sat_date"],
            "C23": week_rows[2]["weekly_report"],
            "J24": week_rows[2]["visit_manager"],

            "A25": week_rows[3]["sat_date"],
            "C25": week_rows[3]["weekly_report"],
            "J26": week_rows[3]["visit_manager"],

            "A27": week_rows[4]["sat_date"],
            "C27": week_rows[4]["weekly_report"],
            "J28": week_rows[4]["visit_manager"],
        }

        file = create_excel_file("在宅評価シート", cell_data)

        st.download_button(
            label="ダウンロード",
            data=file,
            file_name=f"在宅評価シート_{year_val}.{month_val}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{doc_title}_download_excel"
        )

# ==========================================
# 書類_アセスメント（フェイスシート・試作UI）
# ==========================================
def render_assessment_form_page(doc_title: str):
    st.title("📋 アセスメントシート")
    st.caption("フェイスシート入力ページある。入力とExcel出力までつなぐある。")

    st.markdown("## 基本情報")

    master_df = get_resident_master_df()

    if master_df is None or master_df.empty:
        st.warning("利用者情報がまだ登録されてないある。先に⑨ 利用者情報から利用者を登録してほしいある。")
        return

    master_df = master_df.fillna("").copy()

    resident_options = []
    resident_map = {}

    for _, row in master_df.iterrows():
        rid = str(row.get("resident_id", "")).strip()
        rname = str(row.get("resident_name", "")).strip()
        status = str(row.get("status", "")).strip()

        if not rname:
            continue

        label = f"{rname}"
        if rid:
            label += f" ({rid})"
        if status:
            label += f" / {status}"

        resident_options.append(label)
        resident_map[label] = row.to_dict()

    if not resident_options:
        st.warning("利用者情報がまだ登録されてないある。")
        return

    selected_label = st.selectbox(
        "誰の書類を入力するか",
        resident_options,
        key=f"{doc_title}_resident_select"
    )

    selected_row = resident_map[selected_label]
    resident_name = str(selected_row.get("resident_name", "")).strip()

    # -----------------------------
    # 聴き取り情報
    # P1 / Y1 / AD1 / AG1
    # -----------------------------
    st.markdown("### 聴き取り情報")

    hear_cols = st.columns([3, 2, 1, 1])
    with hear_cols[0]:
        interviewer_name = st.text_input("聴き取り者名", key=f"{doc_title}_interviewer_name")
    with hear_cols[1]:
        hear_year = st.text_input("聞き取り日（西暦）", key=f"{doc_title}_hear_year", placeholder="2026")
    with hear_cols[2]:
        hear_month = st.text_input("月", key=f"{doc_title}_hear_month", placeholder="3")
    with hear_cols[3]:
        hear_day = st.text_input("日", key=f"{doc_title}_hear_day", placeholder="15")

    st.divider()

    # -----------------------------
    # 氏名
    # F4 / F5
    # -----------------------------
    st.markdown("### 氏名")

    name_cols = st.columns([3, 4])
    with name_cols[0]:
        furigana = st.text_input("フリガナ", key=f"{doc_title}_furigana")
    with name_cols[1]:
        full_name = st.text_input(
            "氏名",
            value=resident_name,
            key=f"{doc_title}_full_name"
        )

    # -----------------------------
    # 生年月日
    # U4 / AA4 / AC4 / AG4
    # -----------------------------
    st.markdown("### 生年月日")

    birth_cols = st.columns([2, 1, 1, 1])
    with birth_cols[0]:
        birth_year = st.text_input("西暦", key=f"{doc_title}_birth_year", placeholder="1980")
    with birth_cols[1]:
        birth_month = st.text_input("月", key=f"{doc_title}_birth_month", placeholder="1")
    with birth_cols[2]:
        birth_day = st.text_input("日", key=f"{doc_title}_birth_day", placeholder="1")
    with birth_cols[3]:
        age = st.text_input("年齢", key=f"{doc_title}_age", placeholder="45")

    st.divider()

    # -----------------------------
    # 現住所
    # H7 / K7 / F8 / AA7 / AA9
    # -----------------------------
    st.markdown("## 現住所")

    current_top_cols = st.columns([1, 1, 2, 2])
    with current_top_cols[0]:
        current_zip_1 = st.text_input("郵便番号（上3桁）", key=f"{doc_title}_current_zip_1", placeholder="557")
    with current_top_cols[1]:
        current_zip_2 = st.text_input("郵便番号（下4桁）", key=f"{doc_title}_current_zip_2", placeholder="0000")
    with current_top_cols[2]:
        current_phone = st.text_input("連絡先電話番号", key=f"{doc_title}_current_phone")
    with current_top_cols[3]:
        nearest_station = st.text_input("最寄り駅", key=f"{doc_title}_nearest_station")

    current_address = st.text_area(
        "住所",
        key=f"{doc_title}_current_address",
        height=100
    )

    st.divider()

    # -----------------------------
    # 緊急連絡先
    # H11 / K11 / F12 / AA11 / AA13
    # -----------------------------
    st.markdown("## 緊急連絡先")

    emergency_top_cols = st.columns([1, 1, 2, 2])
    with emergency_top_cols[0]:
        emergency_zip_1 = st.text_input("郵便番号（上3桁）", key=f"{doc_title}_emergency_zip_1", placeholder="557")
    with emergency_top_cols[1]:
        emergency_zip_2 = st.text_input("郵便番号（下4桁）", key=f"{doc_title}_emergency_zip_2", placeholder="0000")
    with emergency_top_cols[2]:
        emergency_relation = st.text_input("続柄", key=f"{doc_title}_emergency_relation")
    with emergency_top_cols[3]:
        emergency_phone_fax = st.text_input("電話 / FAX", key=f"{doc_title}_emergency_phone_fax")

    emergency_address = st.text_area(
        "住所",
        key=f"{doc_title}_emergency_address",
        height=100
    )

    st.divider()

    # -----------------------------
    # 援護実施機関
    # F15 / J15 / X15 / AF15
    # -----------------------------
    st.markdown("## 援護実施機関")

    support_cols = st.columns([2, 1, 3, 2])
    with support_cols[0]:
        support_city = st.text_input("市区名", key=f"{doc_title}_support_city")
    with support_cols[1]:
        support_city_type = st.selectbox(
            "区 / 市",
            ["区", "市"],
            key=f"{doc_title}_support_city_type"
        )
    with support_cols[2]:
        support_office = st.text_input("相談支援事業所", key=f"{doc_title}_support_office")
    with support_cols[3]:
        support_worker = st.text_input("担当ワーカー", key=f"{doc_title}_support_worker")

    st.divider()

    # -----------------------------
    # 手帳・福祉制度
    # -----------------------------
    st.markdown("## 手帳・福祉制度")

    handbook_cols = st.columns([2, 2, 1, 1])
    with handbook_cols[0]:
        handbook_grade = st.text_input("手帳に記入されている級", key=f"{doc_title}_handbook_grade")
    with handbook_cols[1]:
        handbook_year = st.text_input("手帳取得年（西暦）", key=f"{doc_title}_handbook_year", placeholder="2020")
    with handbook_cols[2]:
        handbook_month = st.text_input("月", key=f"{doc_title}_handbook_month", placeholder="1")
    with handbook_cols[3]:
        handbook_day = st.text_input("日", key=f"{doc_title}_handbook_day", placeholder="1")

    disability_summary = st.text_area(
        "障害状況の概要、および手帳取得経緯",
        key=f"{doc_title}_disability_summary",
        height=120
    )

    welfare_cols1 = st.columns(2)
    with welfare_cols1[0]:
        support_level = st.selectbox(
            "障害支援区分",
            ["区分無し", "区分1", "区分2", "区分3", "区分4", "区分5", "区分6"],
            key=f"{doc_title}_support_level"
        )
    with welfare_cols1[1]:
        guardian_status = st.selectbox(
            "成年後見人の有無",
            ["なし", "あり"],
            key=f"{doc_title}_guardian_status"
        )

    guardian_name = st.text_input(
        "成年後見人の氏名（ありの場合）",
        key=f"{doc_title}_guardian_name"
    )

    welfare_cols2 = st.columns(2)
    with welfare_cols2[0]:
        pension_status = st.selectbox(
            "年金（種別）の有無",
            ["なし", "あり"],
            key=f"{doc_title}_pension_status"
        )
    with welfare_cols2[1]:
        allowance_status = st.selectbox(
            "特別児童扶養手当その他の有無",
            ["なし", "あり"],
            key=f"{doc_title}_allowance_status"
        )

    pension_detail = st.text_input(
        "年金詳細（何級・月額いくら等）",
        key=f"{doc_title}_pension_detail"
    )
    allowance_detail = st.text_input(
        "特別児童扶養手当その他詳細（何級・月額いくら等）",
        key=f"{doc_title}_allowance_detail"
    )

    welfare_cols3 = st.columns(3)
    with welfare_cols3[0]:
        transport_pass = st.selectbox(
            "大阪市交通局優待乗車証",
            ["無料", "半額", "なし"],
            key=f"{doc_title}_transport_pass"
        )
    with welfare_cols3[1]:
        welfare_status = st.selectbox(
            "生活保護の受給状況",
            ["あり", "なし"],
            key=f"{doc_title}_welfare_status"
        )
    with welfare_cols3[2]:
        public_support = st.text_input(
            "公費医療・福祉用具等の利用",
            key=f"{doc_title}_public_support"
        )

    st.divider()

    # -----------------------------
    # 家族の状況
    # -----------------------------
    st.markdown("## 家族の状況")

    family_rows = []
    for i in range(4):
        st.markdown(f"### 家族{i+1}")
        cols = st.columns([2, 1, 1, 2, 1, 2])

        with cols[0]:
            fam_name = st.text_input("氏名", key=f"{doc_title}_fam_name_{i}")
        with cols[1]:
            fam_relation = st.text_input("続柄", key=f"{doc_title}_fam_relation_{i}")
        with cols[2]:
            fam_age = st.text_input("年齢", key=f"{doc_title}_fam_age_{i}")
        with cols[3]:
            fam_job = st.text_input("職業等", key=f"{doc_title}_fam_job_{i}")
        with cols[4]:
            fam_live = st.selectbox("同居・別居", ["同居", "別居", ""], key=f"{doc_title}_fam_live_{i}")
        with cols[5]:
            fam_note = st.text_input("特記事項", key=f"{doc_title}_fam_note_{i}")

        family_rows.append({
            "name": fam_name,
            "relation": fam_relation,
            "age": fam_age,
            "job": fam_job,
            "live": fam_live,
            "note": fam_note
        })

    st.divider()

    # -----------------------------
    # 住居環境等
    # -----------------------------
    st.markdown("## 住居環境等")

    housing_cols = st.columns(2)
    with housing_cols[0]:
        housing_transport = st.selectbox(
            "住居の状況（交通手段）",
            ["電車", "バス", "自転車", "その他"],
            key=f"{doc_title}_housing_transport"
        )
    with housing_cols[1]:
        housing_transport_other = st.text_input(
            "住居の状況（その他内容）",
            key=f"{doc_title}_housing_transport_other"
        )

    housing_use_status = st.selectbox(
        "住居の状況 利用する場合具体的な状況",
        ["単独利用", "家族等の付き添い", "その他"],
        key=f"{doc_title}_housing_use_status"
    )
    housing_use_status_other = st.text_input(
        "住居の状況 利用状況（その他内容）",
        key=f"{doc_title}_housing_use_status_other"
    )

    transport_cols = st.columns(2)
    with transport_cols[0]:
        available_transport = st.selectbox(
            "利用可能な交通手段",
            ["電車", "バス", "自転車", "その他"],
            key=f"{doc_title}_available_transport"
        )
    with transport_cols[1]:
        available_transport_other = st.text_input(
            "利用可能な交通手段（その他内容）",
            key=f"{doc_title}_available_transport_other"
        )

    available_use_status = st.selectbox(
        "利用可能交通手段 利用する場合具体的な状況",
        ["単独利用", "家族等の付き添い", "その他"],
        key=f"{doc_title}_available_use_status"
    )
    available_use_status_other = st.text_input(
        "利用可能交通手段 利用状況（その他内容）",
        key=f"{doc_title}_available_use_status_other"
    )

    st.divider()

    # -----------------------------
    # 生活歴
    # -----------------------------
    st.markdown("## 生活歴")

    life_rows = []
    for i in range(3):
        st.markdown(f"### 生活歴{i+1}")
        cols = st.columns([1, 3])
        with cols[0]:
            life_date = st.text_input("西暦年月日", key=f"{doc_title}_life_date_{i}", placeholder="2000/04")
        with cols[1]:
            life_history = st.text_area(
                "生活歴（学歴や転居等の経緯）",
                key=f"{doc_title}_life_history_{i}",
                height=80
            )
        life_rows.append({
            "date": life_date,
            "history": life_history
        })

    st.divider()

    # -----------------------------
    # 医療機関の受診状況等
    # -----------------------------
    st.markdown("## 医療機関の受診状況等")

    medical_cols1 = st.columns([2, 3])
    with medical_cols1[0]:
        disease_name = st.text_input("病名（複数入力可）", key=f"{doc_title}_disease_name")
    with medical_cols1[1]:
        disease_symptom = st.text_input("症状（複数入力可）", key=f"{doc_title}_disease_symptom")

    st.markdown("### 医療機関")
    medical_cols2 = st.columns([2, 2, 2, 1, 2])
    with medical_cols2[0]:
        hospital_name = st.text_input("病院名", key=f"{doc_title}_hospital_name")
    with medical_cols2[1]:
        doctor_name = st.text_input("医師名", key=f"{doc_title}_doctor_name")
    with medical_cols2[2]:
        hospital_contact = st.text_input("連絡先", key=f"{doc_title}_hospital_contact")
    with medical_cols2[3]:
        visit_frequency = st.text_input("通院頻度", key=f"{doc_title}_visit_frequency")
    with medical_cols2[4]:
        medication_status = st.text_input("服薬状況等", key=f"{doc_title}_medication_status")

    st.divider()

    # -----------------------------
    # 心身状況等
    # -----------------------------
    st.markdown("## 心身状況等")

    mind_rows = []
    for i in range(2):
        st.markdown(f"### 心身状況{i+1}")
        cols = st.columns([2, 3, 3])
        with cols[0]:
            mind_disease = st.text_input("障害名・病名", key=f"{doc_title}_mind_disease_{i}")
        with cols[1]:
            mind_symptom = st.text_input("症状など", key=f"{doc_title}_mind_symptom_{i}")
        with cols[2]:
            mind_support = st.text_input("必要な支援の内容", key=f"{doc_title}_mind_support_{i}")

        mind_rows.append({
            "disease": mind_disease,
            "symptom": mind_symptom,
            "support": mind_support
        })

    st.divider()

    # -----------------------------
    # 障害福祉サービスなどの利用状況
    # -----------------------------
    st.markdown("## 障害福祉サービスなどの利用状況")

    service_rows = []
    for i in range(3):
        st.markdown(f"### サービス利用{i+1}")
        cols = st.columns([1, 2, 1, 2])
        with cols[0]:
            service_date = st.text_input("利用開始日（時期）", key=f"{doc_title}_service_date_{i}")
        with cols[1]:
            service_name = st.text_input("サービス名", key=f"{doc_title}_service_name_{i}")
        with cols[2]:
            service_amount = st.text_input("利用量/月", key=f"{doc_title}_service_amount_{i}")
        with cols[3]:
            service_office = st.text_input("事業所名", key=f"{doc_title}_service_office_{i}")

        service_rows.append({
            "date": service_date,
            "name": service_name,
            "amount": service_amount,
            "office": service_office
        })

    st.divider()

    # -----------------------------
    # 生活の流れ等
    # -----------------------------
    st.markdown("## 生活の流れ等")

    day_flow = st.text_area(
        "標準的な１日の生活の流れ（起床から就寝まで）",
        key=f"{doc_title}_day_flow",
        height=120
    )
    special_note = st.text_area(
        "特記事項（１週間の過ごし方やいきがい、趣味、特技など）",
        key=f"{doc_title}_special_note",
        height=120
    )

    st.divider()

    # -----------------------------
    # 総合所見
    # -----------------------------
    st.markdown("## 総合所見")

    wish_user = st.text_area(
        "当事業所のサービス利用に対する本人の希望",
        key=f"{doc_title}_wish_user",
        height=100
    )
    wish_family = st.text_area(
        "当事業所のサービス利用に対する保護者・関係者の希望する方向性",
        key=f"{doc_title}_wish_family",
        height=100
    )
    future_direction = st.text_area(
        "フェイスシートからみる課題や今後の方向性",
        key=f"{doc_title}_future_direction",
        height=120
    )

    st.divider()

    with st.expander("入力内容確認"):
        st.write({
            "聴き取り者名": interviewer_name,
            "聞き取り日": f"{hear_year}/{hear_month}/{hear_day}",
            "フリガナ": furigana,
            "氏名": full_name,
            "生年月日": f"{birth_year}/{birth_month}/{birth_day}",
            "年齢": age,
            "現住所_郵便番号上3桁": current_zip_1,
            "現住所_郵便番号下4桁": current_zip_2,
            "現住所": current_address,
            "現住所_電話番号": current_phone,
            "現住所_最寄り駅": nearest_station,
            "緊急連絡先_郵便番号上3桁": emergency_zip_1,
            "緊急連絡先_郵便番号下4桁": emergency_zip_2,
            "緊急連絡先_住所": emergency_address,
            "緊急連絡先_続柄": emergency_relation,
            "緊急連絡先_電話FAX": emergency_phone_fax,
            "援護実施機関_市区名": support_city,
            "援護実施機関_区市": support_city_type,
            "援護実施機関_相談支援事業所": support_office,
            "援護実施機関_担当ワーカー": support_worker,
            "手帳級": handbook_grade,
            "手帳取得日": f"{handbook_year}/{handbook_month}/{handbook_day}",
            "障害概要・取得経緯": disability_summary,
            "障害支援区分": support_level,
            "成年後見人": guardian_status,
            "成年後見人名": guardian_name,
            "年金": pension_status,
            "年金詳細": pension_detail,
            "特別児童扶養手当等": allowance_status,
            "特別児童扶養手当等詳細": allowance_detail,
            "大阪市交通局優待乗車証": transport_pass,
            "生活保護": welfare_status,
            "公費医療・福祉用具等": public_support,
            "家族状況": family_rows,
            "住居の状況": [housing_transport, housing_transport_other, housing_use_status, housing_use_status_other],
            "利用可能交通手段": [available_transport, available_transport_other, available_use_status, available_use_status_other],
            "生活歴": life_rows,
            "病名": disease_name,
            "症状": disease_symptom,
            "医療機関": [hospital_name, doctor_name, hospital_contact, visit_frequency, medication_status],
            "心身状況": mind_rows,
            "福祉サービス利用": service_rows,
            "1日の生活": day_flow,
            "特記事項": special_note,
            "本人の希望": wish_user,
            "家族・関係者の希望": wish_family,
            "課題・今後方向": future_direction,
        })

    # -----------------------------
    # 保存用データ作成
    # -----------------------------
    form_data = {
        "interviewer_name": interviewer_name,
        "hear_year": hear_year,
        "hear_month": hear_month,
        "hear_day": hear_day,
        "furigana": furigana,
        "full_name": full_name,
        "birth_year": birth_year,
        "birth_month": birth_month,
        "birth_day": birth_day,
        "age": age,
        "current_zip_1": current_zip_1,
        "current_zip_2": current_zip_2,
        "current_phone": current_phone,
        "nearest_station": nearest_station,
        "current_address": current_address,
        "emergency_zip_1": emergency_zip_1,
        "emergency_zip_2": emergency_zip_2,
        "emergency_relation": emergency_relation,
        "emergency_phone_fax": emergency_phone_fax,
        "emergency_address": emergency_address,
        "support_city": support_city,
        "support_city_type": support_city_type,
        "support_office": support_office,
        "support_worker": support_worker,
        "handbook_grade": handbook_grade,
        "handbook_year": handbook_year,
        "handbook_month": handbook_month,
        "handbook_day": handbook_day,
        "disability_summary": disability_summary,
        "support_level": support_level,
        "guardian_status": guardian_status,
        "guardian_name": guardian_name,
        "pension_status": pension_status,
        "allowance_status": allowance_status,
        "pension_detail": pension_detail,
        "allowance_detail": allowance_detail,
        "transport_pass": transport_pass,
        "welfare_status": welfare_status,
        "public_support": public_support,
        "family_rows": family_rows,
        "housing_transport": housing_transport,
        "housing_transport_other": housing_transport_other,
        "housing_use_status": housing_use_status,
        "housing_use_status_other": housing_use_status_other,
        "available_transport": available_transport,
        "available_transport_other": available_transport_other,
        "available_use_status": available_use_status,
        "available_use_status_other": available_use_status_other,
        "life_rows": life_rows,
        "disease_name": disease_name,
        "disease_symptom": disease_symptom,
        "hospital_name": hospital_name,
        "doctor_name": doctor_name,
        "hospital_contact": hospital_contact,
        "visit_frequency": visit_frequency,
        "medication_status": medication_status,
        "mind_rows": mind_rows,
        "service_rows": service_rows,
        "day_flow": day_flow,
        "special_note": special_note,
        "wish_user": wish_user,
        "wish_family": wish_family,
        "future_direction": future_direction,
    }

    st.divider()
    st.markdown("### Excel出力")

    if st.button("Excelを作成", key=f"{doc_title}_make_excel"):

        guardian_value = "なし" if guardian_status == "なし" else f"あり：{guardian_name}" if guardian_name.strip() else "あり"
        pension_value = "なし" if pension_status == "なし" else f"あり：{pension_detail}" if pension_detail.strip() else "あり"
        allowance_value = "なし" if allowance_status == "なし" else f"あり：{allowance_detail}" if allowance_detail.strip() else "あり"

        housing_transport_value = housing_transport_other.strip() if housing_transport == "その他" and housing_transport_other.strip() else housing_transport
        housing_status_value = housing_use_status_other.strip() if housing_use_status == "その他" and housing_use_status_other.strip() else housing_use_status

        available_transport_value = available_transport_other.strip() if available_transport == "その他" and available_transport_other.strip() else available_transport
        available_status_value = available_use_status_other.strip() if available_use_status == "その他" and available_use_status_other.strip() else available_use_status

        cell_data = {
            "P1": interviewer_name,
            "Y1": hear_year,
            "AD1": hear_month,
            "AG1": hear_day,

            "F4": furigana,
            "F5": full_name,

            "U4": birth_year,
            "AA4": birth_month,
            "AC4": birth_day,
            "AG4": age,

            "H7": current_zip_1,
            "K7": current_zip_2,
            "F8": current_address,
            "AA7": current_phone,
            "AA9": nearest_station,

            "H11": emergency_zip_1,
            "K11": emergency_zip_2,
            "F12": emergency_address,
            "AA11": emergency_relation,
            "AA13": emergency_phone_fax,

            "F15": support_city,
            "J15": support_city_type,
            "X15": support_office,
            "AF15": support_worker,

            "G19": handbook_grade,
            "Y19": handbook_year,
            "AD19": handbook_month,
            "AG19": handbook_day,
            "G21": disability_summary,
            "G24": support_level,
            "X24": guardian_value,
            "G26": pension_value,
            "X26": allowance_value,
            "G28": transport_pass,
            "Q28": welfare_status,
            "Z28": public_support,

            "G33": family_rows[0]["name"],
            "N33": family_rows[0]["relation"],
            "Q33": family_rows[0]["age"],
            "T33": family_rows[0]["job"],
            "X33": family_rows[0]["live"],
            "AB33": family_rows[0]["note"],

            "G35": family_rows[1]["name"],
            "N35": family_rows[1]["relation"],
            "Q35": family_rows[1]["age"],
            "T35": family_rows[1]["job"],
            "X35": family_rows[1]["live"],
            "AB35": family_rows[1]["note"],

            "G37": family_rows[2]["name"],
            "N37": family_rows[2]["relation"],
            "Q37": family_rows[2]["age"],
            "T37": family_rows[2]["job"],
            "X37": family_rows[2]["live"],
            "AB37": family_rows[2]["note"],

            "G39": family_rows[3]["name"],
            "N39": family_rows[3]["relation"],
            "Q39": family_rows[3]["age"],
            "T39": family_rows[3]["job"],
            "X39": family_rows[3]["live"],
            "AB39": family_rows[3]["note"],

            "G43": housing_transport_value,
            "G45": available_transport_value,

            "A50": life_rows[0]["date"],
            "G50": life_rows[0]["history"],
            "A52": life_rows[1]["date"],
            "G52": life_rows[1]["history"],
            "A54": life_rows[2]["date"],
            "G54": life_rows[2]["history"],

            "A60": disease_name,
            "M60": disease_symptom,
            "C64": hospital_name,
            "C65": doctor_name,
            "C66": hospital_contact,
            "M64": visit_frequency,
            "R64": medication_status,

            "A70": mind_rows[0]["disease"],
            "L70": mind_rows[0]["symptom"],
            "X70": mind_rows[0]["support"],
            "A73": mind_rows[1]["disease"],
            "L73": mind_rows[1]["symptom"],
            "X73": mind_rows[1]["support"],

            "A79": service_rows[0]["date"],
            "G79": service_rows[0]["name"],
            "Q79": service_rows[0]["amount"],
            "Y79": service_rows[0]["office"],

            "A81": service_rows[1]["date"],
            "G81": service_rows[1]["name"],
            "Q81": service_rows[1]["amount"],
            "Y81": service_rows[1]["office"],

            "A83": service_rows[2]["date"],
            "G83": service_rows[2]["name"],
            "Q83": service_rows[2]["amount"],
            "Y83": service_rows[2]["office"],

            "A88": day_flow,
            "A94": special_note,

            "M100": wish_user,
            "M103": wish_family,
            "M107": future_direction,
        }

        file = create_excel_file("アセスメント", cell_data)

        st.download_button(
            label="ダウンロード",
            data=file,
            file_name=f"アセスメント_{full_name if full_name else resident_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{doc_title}_download_excel"
        )

    resident_id = str(selected_row.get("resident_id", "")).strip()

    saved_df = get_document_records("アセスメント", resident_id)

    saved_options = ["新規作成"]
    saved_map = {"新規作成": None}

    if saved_df is not None and not saved_df.empty:
        for _, row in saved_df.iterrows():
            rid = str(row.get("record_id", "")).strip()
            updated_at = str(row.get("updated_at", "")).strip()
            label = f"{rid} / {updated_at}"
            saved_options.append(label)
            saved_map[label] = rid

    basic_cols = st.columns([7, 2, 2, 2])

    with basic_cols[0]:
        st.text_input(
            "利用者氏名",
            value=resident_name,
            key=f"{doc_title}_resident_name",
            disabled=True
        )

    with basic_cols[1]:
        year_val = st.text_input("作成年（西暦）", key=f"{doc_title}_year", placeholder="2026")
    with basic_cols[2]:
        month_val = st.text_input("月", key=f"{doc_title}_month", placeholder="3")
    with basic_cols[3]:
        day_val = st.text_input("日", key=f"{doc_title}_day", placeholder="14")

    st.divider()

    st.markdown("## 本文入力")

    policy_val = st.text_area(
        "サービス等利用計画の総合的な方針",
        key=f"{doc_title}_policy",
        height=120,
        placeholder="B8 に入る内容ある"
    )

    long_goal_val = st.text_area(
        "長期目標（内容・期間等）",
        key=f"{doc_title}_long_goal",
        height=100,
        placeholder="B10 に入る内容ある"
    )

    short_goal_val = st.text_area(
        "短期目標（内容・期間等）",
        key=f"{doc_title}_short_goal",
        height=100,
        placeholder="B12 に入る内容ある"
    )

    st.divider()
    st.markdown("## 具体的達成目標（3行）")
    st.caption("帳票の 17〜19 行に入る部分ある。")

    header_cols = st.columns([5, 3, 4, 2, 2, 2])
    with header_cols[0]:
        st.markdown("**具体的達成目標**")
    with header_cols[1]:
        st.markdown("**本人の役割**")
    with header_cols[2]:
        st.markdown("**支援内容**")
    with header_cols[3]:
        st.markdown("**支援期間**")
    with header_cols[4]:
        st.markdown("**担当者**")
    with header_cols[5]:
        st.markdown("**優先順位**")

    row_data = []

    for i in range(1, 4):
        st.markdown(f"### {i}行目")
        row_cols = st.columns([5, 3, 4, 2, 2, 2])

        with row_cols[0]:
            target_val = st.text_area(
                f"{i}行目_具体的達成目標",
                key=f"{doc_title}_target_{i}",
                height=90,
                label_visibility="collapsed",
                placeholder="C17〜C19"
            )

        with row_cols[1]:
            role_val = st.text_area(
                f"{i}行目_本人の役割",
                key=f"{doc_title}_role_{i}",
                height=90,
                label_visibility="collapsed",
                placeholder="G17〜G19"
            )

        with row_cols[2]:
            support_val = st.text_area(
                f"{i}行目_支援内容",
                key=f"{doc_title}_support_{i}",
                height=90,
                label_visibility="collapsed",
                placeholder="J17〜J19"
            )

        with row_cols[3]:
            period_val = st.text_input(
                f"{i}行目_支援期間",
                key=f"{doc_title}_period_{i}",
                label_visibility="collapsed",
                placeholder="M17〜M19"
            )

        with row_cols[4]:
            person_val = st.text_input(
                f"{i}行目_担当者",
                key=f"{doc_title}_person_{i}",
                label_visibility="collapsed",
                placeholder="O17〜O19"
            )

        with row_cols[5]:
            priority_val = st.selectbox(
                f"{i}行目_優先順位",
                ["", "1", "2", "3"],
                key=f"{doc_title}_priority_{i}",
                label_visibility="collapsed"
            )

        row_data.append({
            "target": target_val,
            "role": role_val,
            "support": support_val,
            "period": period_val,
            "person": person_val,
            "priority": priority_val,
        })

    st.divider()
    st.markdown("## 同意・担当者")

    agree_cols = st.columns([2, 2, 2, 4])

    with agree_cols[0]:
        agree_year_val = st.text_input("同意日_西暦", key=f"{doc_title}_agree_year", placeholder="2026")
    with agree_cols[1]:
        agree_month_val = st.text_input("同意日_月", key=f"{doc_title}_agree_month", placeholder="3")
    with agree_cols[2]:
        agree_day_val = st.text_input("同意日_日", key=f"{doc_title}_agree_day", placeholder="14")
    with agree_cols[3]:
        manager_val = st.text_input("サービス担当責任者", key=f"{doc_title}_manager", placeholder="N21")

    st.divider()

    form_data = {
        "interviewer_name": interviewer_name,
        "hear_year": hear_year,
        "hear_month": hear_month,
        "hear_day": hear_day,
        "furigana": furigana,
        "full_name": full_name,
        "birth_year": birth_year,
        "birth_month": birth_month,
        "birth_day": birth_day,
        "age": age,
        "current_zip_1": current_zip_1,
        "current_zip_2": current_zip_2,
        "current_phone": current_phone,
        "nearest_station": nearest_station,
        "current_address": current_address,
        "emergency_zip_1": emergency_zip_1,
        "emergency_zip_2": emergency_zip_2,
        "emergency_relation": emergency_relation,
        "emergency_phone_fax": emergency_phone_fax,
        "emergency_address": emergency_address,
        "support_city": support_city,
        "support_city_type": support_city_type,
        "support_office": support_office,
        "support_worker": support_worker,
        "handbook_grade": handbook_grade,
        "handbook_year": handbook_year,
        "handbook_month": handbook_month,
        "handbook_day": handbook_day,
        "disability_summary": disability_summary,
        "support_level": support_level,
        "guardian_status": guardian_status,
        "guardian_name": guardian_name,
        "pension_status": pension_status,
        "allowance_status": allowance_status,
        "pension_detail": pension_detail,
        "allowance_detail": allowance_detail,
        "transport_pass": transport_pass,
        "welfare_status": welfare_status,
        "public_support": public_support,
        "family_rows": family_rows,
        "housing_transport": housing_transport,
        "housing_transport_other": housing_transport_other,
        "housing_use_status": housing_use_status,
        "housing_use_status_other": housing_use_status_other,
        "available_transport": available_transport,
        "available_transport_other": available_transport_other,
        "available_use_status": available_use_status,
        "available_use_status_other": available_use_status_other,
        "life_rows": life_rows,
        "disease_name": disease_name,
        "disease_symptom": disease_symptom,
        "hospital_name": hospital_name,
        "doctor_name": doctor_name,
        "hospital_contact": hospital_contact,
        "visit_frequency": visit_frequency,
        "medication_status": medication_status,
        "mind_rows": mind_rows,
        "service_rows": service_rows,
        "day_flow": day_flow,
        "special_note": special_note,
        "wish_user": wish_user,
        "wish_family": wish_family,
        "future_direction": future_direction,
    }

    with st.expander("入力内容確認"):
        st.write(f"利用者氏名: {resident_name}")
        st.write(f"作成年月日: {year_val} / {month_val} / {day_val}")
        st.write(f"総合的な方針: {policy_val}")
        st.write(f"長期目標: {long_goal_val}")
        st.write(f"短期目標: {short_goal_val}")
        st.write(f"同意書日付: {agree_year_val} / {agree_month_val} / {agree_day_val}")
        st.write(f"サービス担当責任者: {manager_val}")

        for idx, item in enumerate(row_data, start=1):
            st.markdown(f"**{idx}行目**")
            st.write(item)

    st.markdown("### 保存済みデータ呼び出し")

    selected_saved_label = st.selectbox(
        "保存済みデータ",
        saved_options,
        key=f"{doc_title}_saved_record_select"
    )

    selected_record_id = saved_map[selected_saved_label]

    if st.button("保存済みを読み込む", key=f"{doc_title}_load_saved"):
        if selected_record_id is not None:
            saved_json = load_document_json(selected_record_id)

            if saved_json:
                for k, v in saved_json.items():
                    st.session_state[f"{doc_title}_{k}"] = v

                st.session_state[f"{doc_title}_loaded_record_id"] = selected_record_id
                st.success("保存済みデータを読み込んだある！")
                st.rerun()
            else:
                st.warning("保存データが見つからないある。")
        else:
            st.info("まだ保存済みデータがないので、新規保存してほしいある。")

    st.markdown("### 保存")

    loaded_record_id = st.session_state.get(f"{doc_title}_loaded_record_id")

    save_cols = st.columns([1, 1, 4])

    with save_cols[0]:
        if st.button("新規保存", key=f"{doc_title}_save_new"):
            new_id = save_document_record(
                resident_id=resident_id,
                resident_name=resident_name,
                doc_type="アセスメント",
                form_data=form_data
            )
            st.session_state[f"{doc_title}_loaded_record_id"] = new_id
            st.success(f"新規保存したある！ record_id = {new_id}")

    with save_cols[1]:
        if st.button("上書き保存", key=f"{doc_title}_save_update"):
            if loaded_record_id:
                ok = update_document_record(loaded_record_id, form_data)
                if ok:
                    st.success(f"上書き保存したある！ record_id = {loaded_record_id}")
                else:
                    st.warning("上書き対象が見つからないある。")
            else:
                st.warning("先に保存済みデータを読み込むか、新規保存してほしいある。")

def render_basic_sheet_form_page(doc_title: str):
    st.title("📋 基本シート")
    st.caption("基本シート入力ページある。入力と保存とExcel出力までつなぐある。")

    st.markdown("## 基本情報")

    master_df = get_resident_master_df()

    if master_df is None or master_df.empty:
        st.warning("利用者情報がまだ登録されてないある。先に⑨ 利用者情報から利用者を登録してほしいある。")
        return

    master_df = master_df.fillna("").copy()

    resident_options = []
    resident_map = {}

    for _, row in master_df.iterrows():
        rid = str(row.get("resident_id", "")).strip()
        rname = str(row.get("resident_name", "")).strip()
        status = str(row.get("status", "")).strip()

        if not rname:
            continue

        label = f"{rname}"
        if rid:
            label += f" ({rid})"
        if status:
            label += f" / {status}"

        resident_options.append(label)
        resident_map[label] = row.to_dict()

    if not resident_options:
        st.warning("利用者情報がまだ登録されてないある。")
        return

    selected_label = st.selectbox(
        "誰の書類を入力するか",
        resident_options,
        key=f"{doc_title}_resident_select"
    )

    selected_row = resident_map[selected_label]
    resident_id = str(selected_row.get("resident_id", "")).strip()
    resident_name = str(selected_row.get("resident_name", "")).strip()

    # -----------------------------
    # 保存済み一覧の準備
    # -----------------------------
    saved_df = get_document_records("基本シート", resident_id)

    saved_options = ["新規作成"]
    saved_map = {"新規作成": None}

    if saved_df is not None and not saved_df.empty:
        for _, row in saved_df.iterrows():
            rid = str(row.get("record_id", "")).strip()
            updated_at = str(row.get("updated_at", "")).strip()
            label = f"{rid} / {updated_at}"
            saved_options.append(label)
            saved_map[label] = rid

    # -----------------------------
    # 聞き取り情報
    # P1 / AB1 / AG1 / AJ1
    # -----------------------------
    st.markdown("### 聞き取り情報")

    hear_cols = st.columns([4, 2, 1, 1])
    with hear_cols[0]:
        interviewer_name = st.text_input("聞き取り者名", key=f"{doc_title}_interviewer_name")
    with hear_cols[1]:
        hear_year = st.text_input("聞き取り日（西暦）", key=f"{doc_title}_hear_year", placeholder="2026")
    with hear_cols[2]:
        hear_month = st.text_input("月", key=f"{doc_title}_hear_month", placeholder="3")
    with hear_cols[3]:
        hear_day = st.text_input("日", key=f"{doc_title}_hear_day", placeholder="15")

    st.divider()

    # -----------------------------
    # 1. 生活と家族に関すること
    # A10
    # -----------------------------
    st.markdown("## １．生活と家族に関すること")
    life_family = st.text_area(
        "経済環境、住環境、日常の意思決定、家庭内での立ち位置や役割など",
        key=f"{doc_title}_life_family",
        height=220
    )

    st.divider()

    # -----------------------------
    # 2. 健康維持に関すること
    # A29
    # -----------------------------
    st.markdown("## ２．健康維持に関すること")
    health = st.text_area(
        "服薬管理、食事管理、睡眠、病気への注意、疾病への認識、通院、具合が悪くなった時の対応、医学的管理、必要な体力の維持など",
        key=f"{doc_title}_health",
        height=260
    )

    st.divider()

    # -----------------------------
    # 3. 社会生活に関すること
    # A46
    # -----------------------------
    st.markdown("## ３．社会生活に関すること")
    social = st.text_area(
        "キーパーソンの存在、人付き合い、社会参加など",
        key=f"{doc_title}_social",
        height=220
    )

    st.divider()

    # -----------------------------
    # 4. その他
    # A63
    # -----------------------------
    st.markdown("## ４．その他")
    other = st.text_area(
        "日常生活動作、日常生活への支障や課題など",
        key=f"{doc_title}_other",
        height=220
    )

    st.divider()

    # -----------------------------
    # 5. 総合所見
    # I74 / I80
    # -----------------------------
    st.markdown("## ５．総合所見")

    opinion = st.text_area(
        "聞き取り者所見",
        key=f"{doc_title}_opinion",
        height=140
    )

    direction = st.text_area(
        "支援の方針・方向性など",
        key=f"{doc_title}_direction",
        height=140
    )

    st.divider()

    # -----------------------------
    # 保存用データ作成
    # -----------------------------
    form_data = {
        "interviewer_name": interviewer_name,
        "hear_year": hear_year,
        "hear_month": hear_month,
        "hear_day": hear_day,
        "life_family": life_family,
        "health": health,
        "social": social,
        "other": other,
        "opinion": opinion,
        "direction": direction,
    }

    with st.expander("入力内容確認"):
        st.write({
            "利用者名": resident_name,
            "聞き取り者名": interviewer_name,
            "聞き取り日": f"{hear_year}/{hear_month}/{hear_day}",
            "生活と家族に関すること": life_family,
            "健康維持に関すること": health,
            "社会生活に関すること": social,
            "その他": other,
            "聞き取り者所見": opinion,
            "支援の方針・方向性など": direction,
        })

    st.markdown("### 保存済みデータ呼び出し")

    selected_saved_label = st.selectbox(
        "保存済みデータ",
        saved_options,
        key=f"{doc_title}_saved_record_select"
    )

    selected_record_id = saved_map[selected_saved_label]

    if st.button("保存済みを読み込む", key=f"{doc_title}_load_saved"):
        if selected_record_id is not None:
            saved_json = load_document_json(selected_record_id)

            if saved_json:
                for k, v in saved_json.items():
                    st.session_state[f"{doc_title}_{k}"] = v

                st.session_state[f"{doc_title}_loaded_record_id"] = selected_record_id
                st.success("保存済みデータを読み込んだある！")
                st.rerun()
            else:
                st.warning("保存データが見つからないある。")
        else:
            st.info("まだ保存済みデータがないので、新規保存してほしいある。")

    st.markdown("### 保存")

    loaded_record_id = st.session_state.get(f"{doc_title}_loaded_record_id")

    save_cols = st.columns([1, 1, 4])

    with save_cols[0]:
        if st.button("新規保存", key=f"{doc_title}_save_new"):
            new_id = save_document_record(
                resident_id=resident_id,
                resident_name=resident_name,
                doc_type="基本シート",
                form_data=form_data
            )
            st.session_state[f"{doc_title}_loaded_record_id"] = new_id
            st.success(f"新規保存したある！ record_id = {new_id}")

    with save_cols[1]:
        if st.button("上書き保存", key=f"{doc_title}_save_update"):
            if loaded_record_id:
                ok = update_document_record(loaded_record_id, form_data)
                if ok:
                    st.success(f"上書き保存したある！ record_id = {loaded_record_id}")
                else:
                    st.warning("上書き対象が見つからないある。")
            else:
                st.warning("先に保存済みデータを読み込むか、新規保存してほしいある。")

    st.divider()
    st.markdown("### Excel出力")

    if st.button("Excelを作成", key=f"{doc_title}_make_excel"):
        cell_data = {
            "P1": interviewer_name,
            "AB1": hear_year,
            "AG1": hear_month,
            "AJ1": hear_day,
            "A10": life_family,
            "A29": health,
            "A46": social,
            "A63": other,
            "I74": opinion,
            "I80": direction,
        }

        file = create_excel_file("基本シート", cell_data)

        st.download_button(
            label="ダウンロード",
            data=file,
            file_name=f"基本シート_{resident_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{doc_title}_download_excel"
        )

def render_work_field_form_page(doc_title: str):
    st.title(f"📄 {doc_title}")
    st.info("就労分野シートはまだ作成中ある。")

def render_work_sheet_form_page(doc_title: str):
    st.title("📋 就労分野シート")
    st.caption("就労分野シート入力ページある。入力・保存・呼び出し・Excel出力まで対応版ある。")

    st.markdown("## 基本情報")

    master_df = get_resident_master_df()

    if master_df is None or master_df.empty:
        st.warning("利用者情報がまだ登録されてないある。先に⑨ 利用者情報から利用者を登録してほしいある。")
        return

    master_df = master_df.fillna("").copy()

    resident_options = []
    resident_map = {}

    for _, row in master_df.iterrows():
        rid = str(row.get("resident_id", "")).strip()
        rname = str(row.get("resident_name", "")).strip()
        status = str(row.get("status", "")).strip()

        if not rname:
            continue

        label = f"{rname}"
        if rid:
            label += f" ({rid})"
        if status:
            label += f" / {status}"

        resident_options.append(label)
        resident_map[label] = row.to_dict()

    if not resident_options:
        st.warning("利用者情報がまだ登録されてないある。")
        return

    selected_label = st.selectbox(
        "誰の書類を入力するか",
        resident_options,
        key=f"{doc_title}_resident_select"
    )

    selected_row = resident_map[selected_label]
    resident_id = str(selected_row.get("resident_id", "")).strip()
    resident_name = str(selected_row.get("resident_name", "")).strip()

    # ---------------------------------
    # 保存済みデータ一覧
    # ---------------------------------
    saved_df = get_document_records("就労分野シート", resident_id)

    saved_options = ["新規作成"]
    saved_map = {"新規作成": None}

    if saved_df is not None and not saved_df.empty:
        for _, row in saved_df.iterrows():
            rid = str(row.get("record_id", "")).strip()
            updated_at = str(row.get("updated_at", "")).strip()
            label = f"{rid} / {updated_at}"
            saved_options.append(label)
            saved_map[label] = rid

    # -----------------------------
    # 聞き取り情報
    # P1 / Y1 / AD1 / AG1
    # -----------------------------
    st.markdown("### 聞き取り情報")

    hear_cols = st.columns([4, 2, 1, 1])
    with hear_cols[0]:
        interviewer_name = st.text_input("聞き取り者名", key=f"{doc_title}_interviewer_name")
    with hear_cols[1]:
        hear_year = st.text_input("聞き取り日（西暦）", key=f"{doc_title}_hear_year", placeholder="2026")
    with hear_cols[2]:
        hear_month = st.text_input("月", key=f"{doc_title}_hear_month", placeholder="3")
    with hear_cols[3]:
        hear_day = st.text_input("日", key=f"{doc_title}_hear_day", placeholder="15")

    st.divider()

    # -----------------------------
    # 4〜8行目 表
    # -----------------------------
    st.markdown("### 評価基準")

    import pandas as pd

    criteria_df = pd.DataFrame({
        "段階": ["1", "2", "3", "4"],
        "評価基準": [
            "できる",
            "少し支援が必要",
            "支援が必要",
            "できない"
        ],
        "目安": [
            "本人の力で達成できる場合",
            "声かけやプログラムにより達成できる場合",
            "個別の支援や配慮など工夫が必要な場合",
            "障がい特性上、達成が困難な場合"
        ]
    })

    st.dataframe(criteria_df, use_container_width=True, hide_index=True)

    # -----------------------------
    # 1. 健康管理
    # -----------------------------
    st.markdown("## １．健康管理")

    health_item1 = st.selectbox("①体調管理　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_health_item1")
    health_item2 = st.selectbox("②服薬管理　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_health_item2")
    health_item3 = st.selectbox("③食事管理　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_health_item3")

    health_point = st.text_area(
        "支援ポイント等記載欄",
        key=f"{doc_title}_health_point",
        height=160
    )

    st.divider()

    # -----------------------------
    # 2. 日常生活管理
    # -----------------------------
    st.markdown("## ２．日常生活管理")

    daily_item1 = st.selectbox("①生活リズム　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_daily_item1")
    daily_item2 = st.selectbox("②みだしなみ　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_daily_item2")
    daily_item3 = st.selectbox("③清潔保持　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_daily_item3")
    daily_item4 = st.selectbox("④金銭管理　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_daily_item4")
    daily_item5 = st.selectbox("⑤移動　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_daily_item5")

    daily_point = st.text_area(
        "支援ポイント等記載欄",
        key=f"{doc_title}_daily_point",
        height=180
    )

    st.divider()

    # -----------------------------
    # 3. 就労に関すること
    # -----------------------------
    st.markdown("## ３．就労に関すること")

    work_item1 = st.selectbox("①就労理解　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_work_item1")
    work_item2 = st.selectbox("②就労意識　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_work_item2")
    work_item3 = st.selectbox("③就労意欲　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_work_item3")
    work_item4 = st.selectbox("④体力、精神力　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_work_item4")
    work_item5 = st.selectbox("⑤家族の支援　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_work_item5")

    work_point = st.text_area(
        "支援ポイント等記載欄",
        key=f"{doc_title}_work_point",
        height=180
    )

    st.divider()

    # -----------------------------
    # 5. 基本的労働習慣
    # -----------------------------
    st.markdown("## ５．基本的労働習慣")

    labor_item1 = st.selectbox("①あいさつ　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_labor_item1")
    labor_item2 = st.selectbox("②報告・連絡・相談　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_labor_item2")
    labor_item3 = st.selectbox("③毎日の通所　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_labor_item3")
    labor_item4 = st.selectbox("④規則の順守　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_labor_item4")

    labor_point = st.text_area(
        "支援ポイント等記載欄",
        key=f"{doc_title}_labor_point",
        height=160
    )

    st.divider()

    # -----------------------------
    # 6. 職業適性
    # -----------------------------
    st.markdown("## ６．職業適性")

    aptitude_item1 = st.selectbox("①指示の理解　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_aptitude_item1")
    aptitude_item2 = st.selectbox("②持続力、集中力　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_aptitude_item2")
    aptitude_item3 = st.selectbox("③正確性　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_aptitude_item3")
    aptitude_item4 = st.selectbox("④責任感　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_aptitude_item4")
    aptitude_item5 = st.selectbox("⑤自己理解①　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_aptitude_item5")
    aptitude_item6 = st.selectbox("⑥自己理解②　支援の必要性", ["1", "2", "3", "4"], key=f"{doc_title}_aptitude_item6")

    aptitude_point = st.text_area(
        "支援ポイント等記載欄",
        key=f"{doc_title}_aptitude_point",
        height=200
    )

    st.divider()

    # -----------------------------
    # 7. 利用経過及び就労歴
    # -----------------------------
    st.markdown("## ７．現状に至る障がい福祉サービスの利用経過及び就労歴")

    history_rows = []

    for i in range(3):
        st.markdown(f"### 就労歴 {i + 1}")

        col1 = st.columns([1, 2])
        with col1[0]:
            period = st.text_input("期間（時期）", key=f"{doc_title}_history_period_{i}")
        with col1[1]:
            company = st.text_input("企業・事業所名", key=f"{doc_title}_history_company_{i}")

        col2 = st.columns([2, 2])
        with col2[0]:
            business = st.text_area(
                "事業内容（具体的に）",
                key=f"{doc_title}_history_business_{i}",
                height=100
            )
        with col2[1]:
            condition = st.text_area(
                "労働条件（給与、労働時間、休日など）",
                key=f"{doc_title}_history_condition_{i}",
                height=100
            )

        note = st.text_area(
            "特記事項（職場でのエピソード、離職理由等）",
            key=f"{doc_title}_history_note_{i}",
            height=120
        )

        history_rows.append({
            "period": period,
            "company": company,
            "business": business,
            "condition": condition,
            "note": note,
        })

    hope_service = st.text_area(
        "当事業所のサービス利用に対する本人の希望",
        key=f"{doc_title}_hope_service",
        height=120
    )

    st.divider()

    # -----------------------------
    # 8. 総合所見
    # -----------------------------
    st.markdown("## ８．総合所見")

    hope_work = st.text_area(
        "本人が希望している就労内容",
        key=f"{doc_title}_hope_work",
        height=120
    )

    readiness = st.text_area(
        "本人が自覚している就労準備性の状況",
        key=f"{doc_title}_readiness",
        height=120
    )

    suitable_work = st.text_area(
        "本人に向いていると判断される就労内容",
        key=f"{doc_title}_suitable_work",
        height=120
    )

    opinion = st.text_area(
        "聞き取り者所見",
        key=f"{doc_title}_opinion",
        height=120
    )

    direction = st.text_area(
        "支援の方向性や方針",
        key=f"{doc_title}_direction",
        height=120
    )

    st.divider()

    # ---------------------------------
    # 保存用データ
    # ---------------------------------
    form_data = {
        "interviewer_name": interviewer_name,
        "hear_year": hear_year,
        "hear_month": hear_month,
        "hear_day": hear_day,
        "health_item1": health_item1,
        "health_item2": health_item2,
        "health_item3": health_item3,
        "health_point": health_point,
        "daily_item1": daily_item1,
        "daily_item2": daily_item2,
        "daily_item3": daily_item3,
        "daily_item4": daily_item4,
        "daily_item5": daily_item5,
        "daily_point": daily_point,
        "work_item1": work_item1,
        "work_item2": work_item2,
        "work_item3": work_item3,
        "work_item4": work_item4,
        "work_item5": work_item5,
        "work_point": work_point,
        "labor_item1": labor_item1,
        "labor_item2": labor_item2,
        "labor_item3": labor_item3,
        "labor_item4": labor_item4,
        "labor_point": labor_point,
        "aptitude_item1": aptitude_item1,
        "aptitude_item2": aptitude_item2,
        "aptitude_item3": aptitude_item3,
        "aptitude_item4": aptitude_item4,
        "aptitude_item5": aptitude_item5,
        "aptitude_item6": aptitude_item6,
        "aptitude_point": aptitude_point,
        "history_rows": history_rows,
        "hope_service": hope_service,
        "hope_work": hope_work,
        "readiness": readiness,
        "suitable_work": suitable_work,
        "opinion": opinion,
        "direction": direction,
    }

    with st.expander("入力内容確認"):
        st.write({
            "利用者名": resident_name,
            "聞き取り者名": interviewer_name,
            "聞き取り日": f"{hear_year}/{hear_month}/{hear_day}",
            "健康管理": [health_item1, health_item2, health_item3, health_point],
            "日常生活管理": [daily_item1, daily_item2, daily_item3, daily_item4, daily_item5, daily_point],
            "就労に関すること": [work_item1, work_item2, work_item3, work_item4, work_item5, work_point],
            "基本的労働習慣": [labor_item1, labor_item2, labor_item3, labor_item4, labor_point],
            "職業適性": [aptitude_item1, aptitude_item2, aptitude_item3, aptitude_item4, aptitude_item5, aptitude_item6, aptitude_point],
            "就労歴": history_rows,
            "サービス利用希望": hope_service,
            "総合所見": [hope_work, readiness, suitable_work, opinion, direction],
        })

    st.markdown("### 保存済みデータ呼び出し")

    selected_saved_label = st.selectbox(
        "保存済みデータ",
        saved_options,
        key=f"{doc_title}_saved_record_select"
    )

    selected_record_id = saved_map[selected_saved_label]

    if st.button("保存済みを読み込む", key=f"{doc_title}_load_saved"):
        if selected_record_id is not None:
            saved_json = load_document_json(selected_record_id)

            if saved_json:
                for k, v in saved_json.items():
                    st.session_state[f"{doc_title}_{k}"] = v

                st.session_state[f"{doc_title}_loaded_record_id"] = selected_record_id
                st.success("保存済みデータを読み込んだある！")
                st.rerun()
            else:
                st.warning("保存データが見つからないある。")
        else:
            st.info("まだ保存済みデータがないので、新規保存してほしいある。")

    st.markdown("### 保存")

    loaded_record_id = st.session_state.get(f"{doc_title}_loaded_record_id")

    save_cols = st.columns([1, 1, 4])

    with save_cols[0]:
        if st.button("新規保存", key=f"{doc_title}_save_new"):
            new_id = save_document_record(
                resident_id=resident_id,
                resident_name=resident_name,
                doc_type="就労分野シート",
                form_data=form_data
            )
            st.session_state[f"{doc_title}_loaded_record_id"] = new_id
            st.success(f"新規保存したある！ record_id = {new_id}")

    with save_cols[1]:
        if st.button("上書き保存", key=f"{doc_title}_save_update"):
            if loaded_record_id:
                ok = update_document_record(loaded_record_id, form_data)
                if ok:
                    st.success(f"上書き保存したある！ record_id = {loaded_record_id}")
                else:
                    st.warning("上書き対象が見つからないある。")
            else:
                st.warning("先に保存済みデータを読み込むか、新規保存してほしいある。")

    st.divider()
    st.markdown("### Excel出力")

    if st.button("Excelを作成", key=f"{doc_title}_make_excel"):
        cell_data = {
            "P1": interviewer_name,
            "Y1": hear_year,
            "AD1": hear_month,
            "AG1": hear_day,

            # 1. 健康管理
            "M14": health_item1,
            "M16": health_item2,
            "M18": health_item3,
            "P14": health_point,

            # 2. 日常生活管理
            "M22": daily_item1,
            "M24": daily_item2,
            "M26": daily_item3,
            "M28": daily_item4,
            "M30": daily_item5,
            "P22": daily_point,

            # 3. 就労に関すること
            "M44": work_item1,
            "M46": work_item2,
            "M48": work_item3,
            "M50": work_item4,
            "M52": work_item5,
            "P44": work_point,

            # 5. 基本的労働習慣
            "M58": labor_item1,
            "M60": labor_item2,
            "M62": labor_item3,
            "M64": labor_item4,
            "P58": labor_point,

            # 6. 職業適性
            "M68": aptitude_item1,
            "M70": aptitude_item2,
            "M72": aptitude_item3,
            "M74": aptitude_item4,
            "M76": aptitude_item5,
            "M78": aptitude_item6,
            "P68": aptitude_point,

            # 7. 利用経過及び就労歴
            "A84": history_rows[0]["period"],
            "A85": history_rows[0]["company"],
            "J84": history_rows[0]["business"],
            "W84": history_rows[0]["condition"],
            "A87": history_rows[0]["note"],

            "A91": history_rows[1]["period"],
            "A92": history_rows[1]["company"],
            "J91": history_rows[1]["business"],
            "W91": history_rows[1]["condition"],
            "A94": history_rows[1]["note"],

            "A98": history_rows[2]["period"],
            "A99": history_rows[2]["company"],
            "J98": history_rows[2]["business"],
            "W98": history_rows[2]["condition"],
            "A101": history_rows[2]["note"],

            "J99": hope_service,

            # 8. 総合所見
            "J105": hope_work,
            "J109": readiness,
            "J112": suitable_work,
            "J116": opinion,
            "J121": direction,
        }

        file = create_excel_file("就労分野シート", cell_data)

        st.download_button(
            label="ダウンロード",
            data=file,
            file_name=f"就労分野シート_{resident_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{doc_title}_download_excel"
        )

@st.cache_data(ttl=60)
def get_staff_examples_df_cached():
    df = load_db("staff_examples")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "staff_name",
            "home_start_example", "home_end_example",
            "day_start_example", "day_end_example",
            "outside_start_example", "outside_end_example",
            "updated_at"
        ])
    else:
        for col in [
            "staff_name",
            "home_start_example", "home_end_example",
            "day_start_example", "day_end_example",
            "outside_start_example", "outside_end_example",
            "updated_at"
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_staff_examples_df():
    return get_staff_examples_df_cached().copy()


@st.cache_data(ttl=60)
def get_personal_rules_df_cached():
    df = load_db("personal_rules")
    if df is None or df.empty:
        df = pd.DataFrame(columns=["staff_name", "rule_text", "updated_at"])
    else:
        for col in ["staff_name", "rule_text", "updated_at"]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_personal_rules_df():
    return get_personal_rules_df_cached().copy()


@st.cache_data(ttl=60)
def get_assistant_plans_df_cached():
    df = load_db("assistant_plans")
    if df is None or df.empty:
        df = pd.DataFrame(columns=["resident_id", "long_term_goal", "short_term_goal", "updated_at"])
    else:
        for col in ["resident_id", "long_term_goal", "short_term_goal", "updated_at"]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_assistant_plans_df():
    return get_assistant_plans_df_cached().copy()


def save_staff_examples_record(
    staff_name,
    home_start_example,
    home_end_example,
    day_start_example,
    day_end_example,
    outside_start_example,
    outside_end_example,
):
    df = get_staff_examples_df()

    updated_at = now_jst().strftime("%Y-%m-%d %H:%M:%S")

    hit_idx = df.index[df["staff_name"].astype(str) == str(staff_name)].tolist()

    row_data = {
        "staff_name": str(staff_name),
        "home_start_example": str(home_start_example),
        "home_end_example": str(home_end_example),
        "day_start_example": str(day_start_example),
        "day_end_example": str(day_end_example),
        "outside_start_example": str(outside_start_example),
        "outside_end_example": str(outside_end_example),
        "updated_at": updated_at,
    }

    if hit_idx:
        idx = hit_idx[0]
        for k, v in row_data.items():
            df.at[idx, k] = v
    else:
        df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)

    save_db(df, "staff_examples")
    return True


def save_personal_rules_record(staff_name, rule_text):
    df = get_personal_rules_df()

    updated_at = now_jst().strftime("%Y-%m-%d %H:%M:%S")

    hit_idx = df.index[df["staff_name"].astype(str) == str(staff_name)].tolist()

    row_data = {
        "staff_name": str(staff_name),
        "rule_text": str(rule_text),
        "updated_at": updated_at,
    }

    if hit_idx:
        idx = hit_idx[0]
        for k, v in row_data.items():
            df.at[idx, k] = v
    else:
        df = pd.concat([df, pd.DataFrame([row_data])], ignore_index=True)

    save_db(df, "personal_rules")
    return True


def get_staff_example_row(staff_name):
    df = get_staff_examples_df()
    hit = df[df["staff_name"].astype(str) == str(staff_name)]
    if hit.empty:
        return None
    return hit.iloc[0].to_dict()


def get_personal_rule_row(staff_name):
    df = get_personal_rules_df()
    hit = df[df["staff_name"].astype(str) == str(staff_name)]
    if hit.empty:
        return None
    return hit.iloc[0].to_dict()


def get_plan_row(resident_id):
    df = get_assistant_plans_df()
    hit = df[df["resident_id"].astype(str) == str(resident_id)]
    if hit.empty:
        return None
    return hit.iloc[0].to_dict()


def build_examples_text(service_type, example_row):
    if not example_row:
        return ""

    if service_type == "在宅":
        return (
            f"在宅作業開始例文:\n{example_row.get('home_start_example', '')}\n\n"
            f"在宅作業終了例文:\n{example_row.get('home_end_example', '')}"
        )
    elif service_type == "通所":
        return (
            f"通所作業開始例文:\n{example_row.get('day_start_example', '')}\n\n"
            f"通所作業終了例文:\n{example_row.get('day_end_example', '')}"
        )
    else:
        return (
            f"施設外作業開始例文:\n{example_row.get('outside_start_example', '')}\n\n"
            f"施設外作業終了例文:\n{example_row.get('outside_end_example', '')}"
        )


def generate_status_support_with_gemini(
    service_type,
    meal_flag,
    note,
    start_memo,
    end_memo,
    examples_text,
    rule_text,
    plan_text=""
):
    api_key = get_gemini_api_key_from_app()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY が取得できなかったある")

    prompt = f"""
あなたは障害福祉サービスのKnowbe日誌入力を補助するアシスタントです。

以下の条件で、必ずJSON形式のみで返してください。
キーは "generated_status" と "generated_support" の2つです。

【前提】
- service_type: {service_type}
- meal_flag: {meal_flag}
- 備考: {note}

【利用者状態メモ（そのままの意味を尊重）】
{start_memo}

【職員考察メモ（そのままの意味を尊重）】
{end_memo}

【スタッフ例文】
{examples_text}

【個人ルール】
{rule_text}

【支援計画】
{plan_text}

【絶対ルール】
- generated_status は「利用者状態」に入れる文章
- generated_support は「職員考察」に入れる文章
- start_memo の内容は generated_status に反映
- end_memo の内容は generated_support に反映
- 事実を勝手に増やさない
- 余計な見出しはつけない
- JSON以外は返さない
"""

    client = get_genai_client(api_key)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    text = (response.text or "").strip()

    if not text:
        raise RuntimeError("Geminiの応答が空ある")

    text = text.replace("```json", "").replace("```", "").strip()
    data = json.loads(text)

    return (
        str(data.get("generated_status", "")).strip(),
        str(data.get("generated_support", "")).strip(),
    )

def get_gemini_api_key_from_app():
    api_key = ""

    try:
        api_key = st.secrets.get("GEMINI_API_KEY", "")
    except Exception:
        api_key = ""

    if not api_key:
        try:
            if "gemini" in st.secrets and "api_key" in st.secrets["gemini"]:
                api_key = st.secrets["gemini"]["api_key"]
        except Exception:
            pass

    if not api_key:
        import os
        api_key = os.environ.get("GEMINI_API_KEY", "")

    return str(api_key).strip()


def generate_bee_texts(
    resident_name,
    service_type,
    meal_flag="",
    note="",
    start_memo="",
    end_memo="",
    examples_text="",
    rule_text="",
    plan_text="",
    note_text=None,
    staff_name=None,
):
    if (not str(note).strip()) and note_text is not None:
        note = note_text

    meal_flag = "" if meal_flag is None else str(meal_flag).strip()
    note = "" if note is None else str(note).strip()
    start_memo = "" if start_memo is None else str(start_memo).strip()
    end_memo = "" if end_memo is None else str(end_memo).strip()
    examples_text = "" if examples_text is None else str(examples_text).strip()
    rule_text = "" if rule_text is None else str(rule_text).strip()
    plan_text = "" if plan_text is None else str(plan_text).strip()

    api_key = get_gemini_api_key_from_app()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY が取得できなかったある")

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-2.5-flash")
    prompt = f"""
あなたは就労継続支援B型の支援記録作成アシスタントある。
以下の情報をもとに、Knowbeへそのまま貼り付けられる
「利用者状態」と「職員考察」を作るある。

【利用者名】
{resident_name}

【サービス種別】
{service_type}

【食事】
{meal_flag}

【備考】
{note}

【利用者状態メモ】
{start_memo}

【職員考察メモ】
{end_memo}

【スタッフ例文】
{examples_text}

【個人ルール】
{rule_text}

【支援計画】
{plan_text}

【ルール】
- 出力はJSONのみ
- generated_status は「利用者状態」
- generated_support は「職員考察」
- 事実を勝手に増やさない
- 見出しや箇条書きは不要
- 支援記録として自然で丁寧な文
- 3文程度
- 100〜150文字程度目安
- Knowbeへそのまま貼れる長さ

【出力形式】
{{
  "generated_status": "ここに利用者状態",
  "generated_support": "ここに職員考察"
}}
"""
    


    response = model.generate_content(prompt)
    result_text = (response.text or "").strip()

    if not result_text:
        raise RuntimeError("Geminiの応答が空ある")

    cleaned = result_text.replace("```json", "").replace("```", "").strip()

    try:
        data = json.loads(cleaned)
        generated_status = str(data.get("generated_status", "")).strip()
        generated_support = str(data.get("generated_support", "")).strip()
    except Exception:
        raise RuntimeError(f"Gemini出力の解析に失敗ある: {cleaned}")

    if not generated_status or not generated_support:
        raise RuntimeError(f"Gemini出力の解析に失敗ある: {cleaned}")

    return generated_status, generated_support

def get_knowbe_credentials_from_app():
    office_key = str(st.session_state.get("office_key", "support")).strip().lower()
    if office_key not in ("support", "home"):
        office_key = "support"

    username = ""
    password = ""

    secret_user_key = f"KB_LOGIN_USERNAME_{office_key.upper()}"
    secret_pass_key = f"KB_LOGIN_PASSWORD_{office_key.upper()}"

    try:
        username = st.secrets.get(secret_user_key, "")
        password = st.secrets.get(secret_pass_key, "")
    except Exception as e:
        st.error(f"st.secrets 読み取り例外ある: {e}")
        username = ""
        password = ""

    if not username:
        import os
        username = os.environ.get(secret_user_key, "")
    if not password:
        import os
        password = os.environ.get(secret_pass_key, "")

    st.info(f"DEBUG office_key = {office_key}")
    st.info(f"DEBUG user_key = {secret_user_key}")
    st.info(f"DEBUG pass_key = {secret_pass_key}")
    st.info(f"DEBUG username exists = {bool(str(username).strip())}")
    st.info(f"DEBUG password exists = {bool(str(password).strip())}")
    st.write("DEBUG keys:", list(st.secrets.keys()))

    return str(username).strip(), str(password).strip()

def send_to_knowbe_from_bee(
    record_id=None,
    company_id="",
    target_date="",
    resident_name="",
    service_type="",
    start_time="",
    end_time="",
    meal_flag="",
    note_text="",
    generated_status="",
    generated_support="",
    staff_name="",
    knowbe_target="support",
    work_start_time="",
    work_end_time="",
    work_break_time="",
    work_memo="",
):
    import traceback

    st.warning("SEND_TO_KNOWBE_FROM_BEE CALLED / 2026-03-21-knowbe-debug-02")

    login_username, login_password = get_knowbe_credentials_from_app()

    st.info(f"DEBUG username exists = {bool(str(login_username).strip())}")
    st.info(f"DEBUG password exists = {bool(str(login_password).strip())}")
    st.write("DEBUG keys:", list(st.secrets.keys()))

    if not login_username or not login_password:
        raise RuntimeError("app.py 側で KB_LOGIN_USERNAME / KB_LOGIN_PASSWORD を取得できなかったある")

    try:
        st.write("DEBUG 1: import send_one_record_from_app start")
        from run_assistance import send_one_record_from_app  # type: ignore
        st.write("DEBUG 2: import send_one_record_from_app done")

        st.write("DEBUG 3: send_one_record_from_app start")
        ok = send_one_record_from_app(
            target_date=str(target_date).strip(),
            resident_name=str(resident_name).strip(),
            service_type=str(service_type).strip(),
            start_time=str(start_time).strip(),
            end_time=str(end_time).strip(),
            meal_flag=str(meal_flag).strip(),
            note_text=str(note_text).strip(),
            generated_status=str(generated_status).strip(),
            generated_support=str(generated_support).strip(),
            staff_name=str(staff_name).strip(),
            knowbe_target=str(knowbe_target).strip(),
            login_username=login_username,
            login_password=login_password,
            work_start_time=str(work_start_time).strip(),
            work_end_time=str(work_end_time).strip(),
            work_break_time=str(work_break_time).strip(),
            work_memo=str(work_memo).strip(),
        )
        st.write(f"DEBUG 4: send_one_record_from_app returned = {ok}")

    except Exception as e:
        st.error(f"DEBUG EXCEPTION TYPE: {type(e).__name__}")
        st.error(f"DEBUG EXCEPTION MSG: {e}")
        st.code(traceback.format_exc())
        raise

    if not ok:
        raise RuntimeError("run_assistance.send_one_record_from_app が False を返したある")

    return True

def render_bee_journal_page():
    st.title("🐝knowbe日誌入力🐝")
    st.caption("Sue for Bee Assistance 専用の裏メニューある。")

    st.markdown("## 利用者選択")

    master_df = get_resident_master_df()
    if master_df is None or master_df.empty:
        st.warning("利用者情報がまだ登録されてないある。")
        return

    master_df = master_df.fillna("").copy()

    resident_options = []
    resident_map = {}

    for _, row in master_df.iterrows():
        rid = str(row.get("resident_id", "")).strip()
        rname = str(row.get("resident_name", "")).strip()

        if not rname:
            continue

        label = f"{rname}"
        if rid:
            label += f" ({rid})"

        resident_options.append(label)
        resident_map[label] = row.to_dict()

    if not resident_options:
        st.warning("利用者情報がまだ登録されてないある。")
        return

    selected_label = st.selectbox(
        "利用者を選ぶ",
        resident_options,
        key="bee_resident_select"
    )

    selected_row = resident_map[selected_label]
    resident_id = str(selected_row.get("resident_id", "")).strip()
    resident_name = str(selected_row.get("resident_name", "")).strip()

    st.markdown("### 保存データ呼び出し")

    diary_df = get_diary_input_rules_df()

    if diary_df is not None and not diary_df.empty:
        df_user = diary_df[
            diary_df["resident_name"].astype(str) == str(resident_name)
        ].copy()

        if not df_user.empty:
            try:
                df_user["record_id_num"] = pd.to_numeric(df_user["record_id"], errors="coerce")
                df_user = df_user.sort_values("record_id_num", ascending=False)
            except Exception:
                pass

            load_options = []
            load_map = {}

            for _, r in df_user.head(10).iterrows():
                label = f"{r.get('date', '')} / {r.get('start_time', '')}〜{r.get('end_time', '')} / ID:{r.get('record_id', '')}"
                load_options.append(label)
                load_map[label] = r.to_dict()

            load_col1, load_col2 = st.columns([5, 1])

            with load_col1:
                selected_saved_label = st.selectbox(
                    "過去の保存データ",
                    [""] + load_options,
                    key="bee_saved_record_select"
                )

            with load_col2:
                st.write("")
                st.write("")
                if st.button("呼び出す", key="bee_load_saved_record", use_container_width=True):
                    if selected_saved_label:
                        rec = load_map[selected_saved_label]

                        st.session_state["bee_target_date"] = (
                            pd.to_datetime(rec.get("date", "")).date()
                            if str(rec.get("date", "")).strip()
                            else now_jst().date()
                        )
                        st.session_state["start_time"] = str(rec.get("start_time", ""))
                        st.session_state["end_time"] = str(rec.get("end_time", ""))
                        st.session_state["bee_meal_flag"] = str(rec.get("meal_flag", "なし"))
                        st.session_state["bee_note_text"] = str(rec.get("note", ""))
                        st.session_state["bee_start_memo"] = str(rec.get("start_memo", ""))
                        st.session_state["bee_end_memo"] = str(rec.get("end_memo", ""))
                        st.session_state["bee_staff_name"] = str(rec.get("staff_name", ""))
                        st.session_state["bee_service_type"] = str(rec.get("service_type", "在宅"))
                        st.session_state["bee_knowbe_target"] = str(rec.get("knowbe_target", "support"))

                        st.success("保存データを呼び出したある！")
                        st.rerun()

    st.divider()
    st.markdown("## 日誌入力")

    target_date = st.date_input(
        "対象日",
        value=st.session_state.get("bee_target_date", now_jst().date()),
        key="bee_target_date"
    )

    input_cols = st.columns([1, 1, 1, 1])

    with input_cols[0]:
        start_time = st.text_input(
            "開始時間",
            value=st.session_state.get("start_time", ""),
            key="start_time",
            placeholder="10:00"
        )

    with input_cols[1]:
        end_time = st.text_input(
            "終了時間",
            value=st.session_state.get("end_time", ""),
            key="end_time",
            placeholder="10:50"
        )

    with input_cols[2]:
        meal_flag = st.selectbox(
            "食事提供",
            ["なし", "あり"],
            index=0 if st.session_state.get("bee_meal_flag", "なし") == "なし" else 1,
            key="bee_meal_flag"
        )

    with input_cols[3]:
        service_type = st.selectbox(
            "サービス種別",
            ["在宅", "通所", "施設外就労"],
            index=["在宅", "通所", "施設外就労"].index(
                st.session_state.get("bee_service_type", "在宅")
                if st.session_state.get("bee_service_type", "在宅") in ["在宅", "通所", "施設外就労"]
                else "在宅"
            ),
            key="bee_service_type"
        )

    work_time_col1, work_time_col2, work_time_col3 = st.columns(3)

    with work_time_col1:
        work_start_time = st.text_input(
            "作業開始時間",
            value=start_time,
            key="bee_work_start_time"
        )

    with work_time_col2:
        work_end_time = st.text_input(
            "作業終了時間",
            value=end_time,
            key="bee_work_end_time"
        )

    with work_time_col3:
        work_break_time = st.text_input(
            "休憩時間",
            value=work_break_time,
            key="bee_work_break_time"
        )

    knowbe_target = st.radio(
        "送信先",
        ["support", "home"],
        index=0 if st.session_state.get("bee_knowbe_target", "support") == "support" else 1,
        horizontal=True,
        key="bee_knowbe_target"
    )

    staff_name = st.text_input(
        "日誌入力者",
        value=st.session_state.get("bee_staff_name", st.session_state.get("user", "")),
        key="bee_staff_name"
    )

    note_mode = st.radio(
        "備考の入力方法",
        ["候補から選ぶ", "直接入力"],
        horizontal=True,
        key="bee_note_mode"
    )

    if note_mode == "候補から選ぶ":
        note_candidates = ["在宅利用", "食事摂取量 10/10", "施設外就労(実施報告書等添付)", "入院", ""]
        default_note = st.session_state.get("bee_note_text", "")
        default_index = note_candidates.index(default_note) if default_note in note_candidates else 0

        note = st.selectbox(
            "備考",
            note_candidates,
            index=default_index,
            key="bee_note_select"
        )
    else:
        note = st.text_area(
            "備考",
            value=st.session_state.get("bee_note_text", ""),
            key="bee_note_text",
            height=80
        )

    memo_cols = st.columns(2)

    with memo_cols[0]:
        start_memo = st.text_area(
            "開始メモ",
            value=st.session_state.get("bee_start_memo", ""),
            key="bee_start_memo",
            height=140
        )

    with memo_cols[1]:
        end_memo = st.text_area(
            "終了メモ",
            value=st.session_state.get("bee_end_memo", ""),
            key="bee_end_memo",
            height=140
        )

    st.divider()
    st.markdown("## 補助設定")

    use_plan = st.checkbox("個別支援計画を参照する", value=True, key="bee_use_plan")

    example_row = get_staff_example_row(staff_name)
    rule_row = get_personal_rule_row(staff_name)

    default_home_start = example_row.get("home_start_example", "") if example_row else ""
    default_home_end = example_row.get("home_end_example", "") if example_row else ""
    default_day_start = example_row.get("day_start_example", "") if example_row else ""
    default_day_end = example_row.get("day_end_example", "") if example_row else ""
    default_outside_start = example_row.get("outside_start_example", "") if example_row else ""
    default_outside_end = example_row.get("outside_end_example", "") if example_row else ""
    default_rule_text = rule_row.get("rule_text", "") if rule_row else ""

    st.divider()
    st.markdown("## スタッフ例文・個人ルール")

    ex_cols1 = st.columns(2)
    with ex_cols1[0]:
        home_start_example = st.text_area(
            "在宅作業開始例文",
            value=st.session_state.get("bee_home_start_example", default_home_start),
            key="bee_home_start_example",
            height=100
        )
    with ex_cols1[1]:
        home_end_example = st.text_area(
            "在宅作業終了例文",
            value=st.session_state.get("bee_home_end_example", default_home_end),
            key="bee_home_end_example",
            height=100
        )

    ex_cols2 = st.columns(2)
    with ex_cols2[0]:
        day_start_example = st.text_area(
            "通所作業開始例文",
            value=st.session_state.get("bee_day_start_example", default_day_start),
            key="bee_day_start_example",
            height=100
        )
    with ex_cols2[1]:
        day_end_example = st.text_area(
            "通所作業終了例文",
            value=st.session_state.get("bee_day_end_example", default_day_end),
            key="bee_day_end_example",
            height=100
        )

    ex_cols3 = st.columns(2)
    with ex_cols3[0]:
        outside_start_example = st.text_area(
            "施設外作業開始例文",
            value=st.session_state.get("bee_outside_start_example", default_outside_start),
            key="bee_outside_start_example",
            height=100
        )
    with ex_cols3[1]:
        outside_end_example = st.text_area(
            "施設外作業終了例文",
            value=st.session_state.get("bee_outside_end_example", default_outside_end),
            key="bee_outside_end_example",
            height=100
        )

    rule_text = st.text_area(
        "個人ルール",
        value=st.session_state.get("bee_rule_text", default_rule_text),
        key="bee_rule_text",
        height=160,
        placeholder="- 日誌は3文から5文程度で書く\n- 必ずセリフを入れる\n- 食事提供無しは伝聞調にする"
    )

    st.divider()
    st.markdown("## 入力内容確認")

    preview_note = note if note_mode == "候補から選ぶ" else st.session_state.get("bee_note_text", "")

    st.write({
        "date": str(target_date),
        "resident_id": resident_id,
        "resident_name": resident_name,
        "start_time": start_time,
        "end_time": end_time,
        "meal_flag": meal_flag,
        "note": preview_note,
        "start_memo": start_memo,
        "end_memo": end_memo,
        "staff_name": staff_name,
        "knowbe_target": knowbe_target,
        "use_plan": use_plan,
        "service_type": service_type,
    })

    st.divider()
    st.markdown("## 保存・送信")

    preview_note = note if note_mode == "候補から選ぶ" else st.session_state.get("bee_note_text", "")

    plan_row = get_plan_row(resident_id)
    plan_text = ""
    if use_plan and plan_row:
        plan_text = (
            f"長期目標: {plan_row.get('long_term_goal', '')}\n"
            f"短期目標: {plan_row.get('short_term_goal', '')}"
        )

    examples_text = build_examples_text(service_type, get_staff_example_row(staff_name))
    rule_row = get_personal_rule_row(staff_name)
    loaded_rule_text = rule_row.get("rule_text", "") if rule_row else ""

    record_mode = "gemini"

    send_cols = st.columns([1, 1, 1, 4])

    with send_cols[0]:
        time_errors = validate_bee_times(
            resident_id=resident_id,
            target_date=target_date,
            start_time=start_time,
            end_time=end_time,
            work_start_time=work_start_time,
            work_end_time=work_end_time,
        )

        if time_errors:
            for err in time_errors:
                st.error(err)
            st.stop()
        if st.button("下書きを保存", key="bee_save_draft"):
            if not start_time.strip():
                st.warning("開始時間を入れてほしいある。")
                st.stop()
            if not end_time.strip():
                st.warning("終了時間を入れてほしいある。")
                st.stop()
            if not staff_name.strip():
                st.warning("日誌入力者を入れてほしいある。")
                st.stop()

            record_id = save_diary_input_record(
                date=target_date,
                resident_id=resident_id,
                resident_name=resident_name,
                start_time=start_time,
                work_start_time=work_start_time,
                work_end_time=work_end_time,
                end_time=end_time,
                meal_flag=meal_flag,
                note=preview_note,
                start_memo=start_memo,
                end_memo=end_memo,
                staff_name=staff_name,
                generated_status="",
                generated_support="",
                service_type=service_type,
                knowbe_target=knowbe_target,
                send_status="draft",
                sent_at="",
                send_error="",
                record_mode=record_mode,
                company_id=get_current_office_key(),
            )
            st.success(f"下書きを保存したある！ record_id = {record_id}")

        with send_cols[1]:
            time_errors = validate_bee_times(
                resident_id=resident_id,
                target_date=target_date,
                start_time=start_time,
                end_time=end_time,
                work_start_time=work_start_time,
                work_end_time=work_end_time,
            )

            if time_errors:
                for err in time_errors:
                    st.error(err)
                st.stop()
            if st.button("Gemini編集なしでそのまま記録する", key="bee_send_raw"):
                if not start_time.strip():
                    st.warning("開始時間を入れてほしいある。")
                    st.stop()
                if not end_time.strip():
                    st.warning("終了時間を入れてほしいある。")
                    st.stop()
                if not staff_name.strip():
                    st.warning("日誌入力者を入れてほしいある。")
                    st.stop()

                generated_status = start_memo
                generated_support = end_memo

                # 先に保存（送信中）
                record_id = save_diary_input_record(
                    date=target_date,
                    resident_id=resident_id,
                    resident_name=resident_name,
                    start_time=start_time,
                    end_time=end_time,
                    work_start_time=work_start_time,
                    work_end_time=work_end_time,
                    meal_flag=meal_flag,
                    note=preview_note,
                    start_memo=start_memo,
                    end_memo=end_memo,
                    staff_name=staff_name,
                    generated_status=generated_status,
                    generated_support=generated_support,
                    service_type=service_type,
                    knowbe_target=knowbe_target,
                    send_status="sending",
                    sent_at="",
                    send_error="",
                    record_mode=record_mode,
                    company_id=get_current_office_key(),
                )

                st.info(f"送信開始ある… record_id = {record_id}")

                try:
                    ok = send_to_knowbe_from_bee(
                        record_id=record_id,
                        company_id=get_current_office_key(),
                        target_date=target_date,
                        resident_name=resident_name,
                        service_type=service_type,
                        start_time=start_time,
                        end_time=end_time,
                        meal_flag=meal_flag,
                        note_text=preview_note,
                        generated_status=generated_status,
                        generated_support=generated_support,
                        staff_name=staff_name,
                        knowbe_target=knowbe_target,
                        work_start_time=work_start_time,
                        work_end_time=work_end_time,
                        work_break_time=work_break_time,
                        work_memo="",
                    )

                    if ok:
                        update_diary_input_record_status(
                            record_id=record_id,
                            send_status="sent",
                            sent_at=now_jst().strftime("%Y-%m-%d %H:%M:%S"),
                            send_error=""
                        )
                        st.success(f"Knowbeへ送信完了ある！ record_id = {record_id}")
                    else:
                        update_diary_input_record_status(
                            record_id=record_id,
                            send_status="error",
                            sent_at="",
                            send_error="run_assistance.send_one_record_from_app returned False"
                        )
                        st.error(f"Knowbe送信失敗ある。 record_id = {record_id}")

                except Exception as e:
                    update_diary_input_record_status(
                        record_id=record_id,
                        send_status="error",
                        sent_at="",
                        send_error=str(e)
                    )
                    st.error(f"Knowbe送信失敗ある: {e}")

        with send_cols[2]:
            time_errors = validate_bee_times(
                resident_id=resident_id,
                target_date=target_date,
                start_time=start_time,
                end_time=end_time,
                work_start_time=work_start_time,
                work_end_time=work_end_time,
            )

            if time_errors:
                for err in time_errors:
                    st.error(err)
                st.stop()
            if st.button("Geminiで整えて送信", key="bee_send_gpt"):
                if not start_time.strip():
                    st.warning("開始時間を入れてほしいある。")
                    st.stop()
                if not end_time.strip():
                    st.warning("終了時間を入れてほしいある。")
                    st.stop()
                if not staff_name.strip():
                    st.warning("日誌入力者を入れてほしいある。")
                    st.stop()

                try:
                    generated_status, generated_support = generate_bee_texts(
                        resident_name=resident_name,
                        service_type=service_type,
                        start_memo=start_memo,
                        end_memo=end_memo,
                        note_text=preview_note,
                        staff_name=staff_name,
                        examples_text=examples_text,
                        rule_text=loaded_rule_text if loaded_rule_text else rule_text,
                        plan_text=plan_text
                    )

                    # 先に保存（送信中）
                    record_id = save_diary_input_record(
                        date=target_date,
                        resident_id=resident_id,
                        resident_name=resident_name,
                        start_time=start_time,
                        end_time=end_time,
                        work_start_time=work_start_time,
                        work_end_time=work_end_time,
                        meal_flag=meal_flag,
                        note=preview_note,
                        start_memo=start_memo,
                        end_memo=end_memo,
                        staff_name=staff_name,
                        generated_status=generated_status,
                        generated_support=generated_support,
                        service_type=service_type,
                        knowbe_target=knowbe_target,
                        send_status="sending",
                        sent_at="",
                        send_error="",
                        record_mode=record_mode,
                        company_id=get_current_office_key(),
                    )

                    st.info(f"送信開始ある… record_id = {record_id}")

                    ok = send_to_knowbe_from_bee(
                        record_id=record_id,
                        company_id=get_current_office_key(),
                        target_date=target_date,
                        resident_name=resident_name,
                        service_type=service_type,
                        start_time=start_time,
                        end_time=end_time,
                        meal_flag=meal_flag,
                        note_text=preview_note,
                        generated_status=generated_status,
                        generated_support=generated_support,
                        staff_name=staff_name,
                        knowbe_target=knowbe_target,
                        work_start_time=work_start_time,
                        work_end_time=work_end_time,
                        work_break_time=work_break_time,
                        work_memo="",
                    )

                    if ok:
                        update_diary_input_record_status(
                            record_id=record_id,
                            send_status="sent",
                            sent_at=now_jst().strftime("%Y-%m-%d %H:%M:%S"),
                            send_error=""
                        )
                        st.success(f"Knowbeへ送信完了ある！ record_id = {record_id}")
                    else:
                        update_diary_input_record_status(
                            record_id=record_id,
                            send_status="error",
                            sent_at="",
                            send_error="run_assistance.send_one_record_from_app returned False"
                        )
                        st.error(f"Knowbe送信失敗ある。 record_id = {record_id}")

                except Exception as e:
                    st.error(f"Gemini生成エラーある: {e}")

    st.divider()
    st.markdown("## 最近の保存データ")

    diary_df = get_diary_input_rules_df()

    if diary_df is not None and not diary_df.empty:
        if "record_id" in diary_df.columns:
            diary_df = diary_df.sort_values(by="record_id", ascending=False)
        st.dataframe(diary_df.head(10), use_container_width=True, hide_index=True)
    else:
        st.info("まだ保存データはないある。")

def get_resident_schedule_df():
    return get_resident_schedule_df_cached().copy()


def get_resident_notes_df():
    return get_resident_notes_df_cached().copy()


def get_external_contacts_df():
    return get_external_contacts_df_cached().copy()


def get_resident_links_df():
    return get_resident_links_df_cached().copy()


def get_next_contact_id(contact_df):
    if contact_df is None or contact_df.empty or "contact_id" not in contact_df.columns:
        return "C001"

    numbers = []
    for x in contact_df["contact_id"].fillna("").astype(str):
        x = x.strip().upper()
        if x.startswith("C"):
            num = x[1:]
            if num.isdigit():
                numbers.append(int(num))

    next_num = max(numbers) + 1 if numbers else 1
    return f"C{next_num:03d}"


def get_resident_contact_cards(resident_id):
    links_df = get_resident_links_df()
    contacts_df = get_external_contacts_df()

    if links_df.empty or contacts_df.empty:
        return pd.DataFrame(columns=[
            "contact_id", "category1", "category2",
            "name", "organization", "phone", "memo", "role"
        ])

    target_links = links_df[
        links_df["resident_id"].astype(str) == str(resident_id)
    ].copy()

    if target_links.empty:
        return pd.DataFrame(columns=[
            "contact_id", "category1", "category2",
            "name", "organization", "phone", "memo", "role"
        ])

    merged = target_links.merge(
        contacts_df,
        how="left",
        on="contact_id"
    )

    for col in ["category1", "category2", "name", "organization", "phone", "memo", "role"]:
        if col not in merged.columns:
            merged[col] = ""

    merged = merged.fillna("")
    return merged

def get_contact_residents(contact_id, links_df=None, master_df=None):
    if links_df is None:
        links_df = get_resident_links_df()
    if master_df is None:
        master_df = get_resident_master_df()

    if links_df.empty or master_df.empty:
        return pd.DataFrame(columns=["resident_id", "resident_name", "status", "role"])

    target_links = links_df[
        links_df["contact_id"].astype(str) == str(contact_id)
    ].copy()

    if target_links.empty:
        return pd.DataFrame(columns=["resident_id", "resident_name", "status", "role"])

    merged = target_links.merge(
        master_df[["resident_id", "resident_name", "status"]],
        how="left",
        on="resident_id"
    )

    for col in ["resident_id", "resident_name", "status", "role"]:
        if col not in merged.columns:
            merged[col] = ""

    return merged.fillna("")


def get_schedule_slot_num(row):
    memo_val = str(row.get("memo", "")).strip()
    if memo_val.startswith("slot:"):
        try:
            num = int(memo_val.replace("slot:", ""))
            return num if 1 <= num <= 4 else 0
        except Exception:
            return 0
    return 0

def build_schedule_form_base(schedule_df, resident_id):
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    service_types = ["病院", "看護", "介護"]

    base = {
        service: {
            "place": "",
            "phone": "",
            "person_in_charge": "",
            "days": {wd: ["", "", "", ""] for wd in weekdays}
        }
        for service in service_types
    }

    if schedule_df is None or schedule_df.empty:
        return base

    view = schedule_df[
        (schedule_df["resident_id"].astype(str) == str(resident_id)) &
        (schedule_df["service_type"].astype(str).isin(service_types))
    ].copy()

    if view.empty:
        return base

    view["_slot_num"] = view.apply(get_schedule_slot_num, axis=1)
    view = view.sort_values(["service_type", "weekday", "_slot_num", "start_time"])

    for _, hit in view.iterrows():
        service_type = str(hit.get("service_type", "")).strip()
        weekday = str(hit.get("weekday", "")).strip()

        if service_type not in base or weekday not in base[service_type]["days"]:
            continue

        if not base[service_type]["place"]:
            base[service_type]["place"] = str(hit.get("place", "")).strip()
        if not base[service_type]["phone"]:
            base[service_type]["phone"] = str(hit.get("phone", "")).strip()
        if not base[service_type]["person_in_charge"]:
            base[service_type]["person_in_charge"] = str(hit.get("person_in_charge", "")).strip()

        slot_num = get_schedule_slot_num(hit)
        if 1 <= slot_num <= 4:
            start_time = str(hit.get("start_time", "")).strip()
            end_time = str(hit.get("end_time", "")).strip()

            time_text = start_time
            if end_time:
                time_text += f"〜{end_time}"

            base[service_type]["days"][weekday][slot_num - 1] = time_text

    return base

def render_resident_schedule_html(schedule_view):
    schedule_view = schedule_view.copy()

    color_map = {
        "病院": "#f6c90e",
        "看護": "#9fd3e6",
        "介護": "#b7e20f",
    }
    col_order = ["月", "火", "水", "木", "金", "土", "日"]

    legend_html = ""
    for name, color in color_map.items():
        legend_html += f'''
        <div style="
            display:inline-block;
            background:{color};
            color:#111;
            font-weight:700;
            padding:8px 28px;
            border:2px solid #111;
            margin-right:10px;
            margin-bottom:10px;
            min-width:120px;
            text-align:center;
        ">{name}</div>
        '''

    table_html = '<table style="width:100%; border-collapse:collapse; table-layout:fixed;">'
    table_html += '<tr>'
    for wd in col_order:
        table_html += f'''
        <th style="
            border:2px solid #111;
            padding:8px;
            text-align:center;
            background:#fafafa;
            width:14.28%;
        ">{wd}</th>
        '''
    table_html += '</tr><tr>'

    for wd in col_order:
        day_df = schedule_view[schedule_view["weekday"].astype(str) == wd].copy()
        items = []

        if not day_df.empty:
            day_df["_slot_num"] = day_df.apply(get_schedule_slot_num, axis=1)
            day_df = day_df.sort_values(["_slot_num", "service_type", "start_time"])

            for _, hit in day_df.iterrows():
                service_type = str(hit.get("service_type", "")).strip()
                bg = color_map.get(service_type, "#eeeeee")

                start_time = str(hit.get("start_time", "")).strip()
                end_time = str(hit.get("end_time", "")).strip()

                time_text = start_time
                if end_time:
                    time_text += f"〜{end_time}"

                items.append(
                    f'''
                    <div style="
                        background:{bg};
                        border:2px solid #222;
                        padding:6px 8px;
                        margin:6px 0;
                        font-weight:700;
                        text-align:left;
                        min-height:34px;
                        box-sizing:border-box;
                        overflow:hidden;
                    ">{time_text}</div>
                    '''
                )

        table_html += f'''
        <td style="
            border:2px solid #111;
            vertical-align:top;
            padding:6px;
            height:190px;
            width:14.28%;
            box-sizing:border-box;
        ">
            {''.join(items)}
        </td>
        '''

    table_html += "</tr></table>"

    st.markdown(legend_html + table_html, unsafe_allow_html=True)


def go_resident_detail(resident_id):
    st.session_state.selected_resident_id = resident_id
    st.rerun()


def back_to_resident_list():
    st.session_state.selected_resident_id = ""
    st.rerun()

# ログイン中メンバー表示
try:
    active_df = load_db("active_users")
except Exception:
    active_df = pd.DataFrame(columns=["user", "login_at", "last_seen"])

st.sidebar.markdown(f"### 👤 ログイン中")

if active_df is None or active_df.empty:
    st.sidebar.write("現在ログイン中の人はいないある。")
else:
    active_df = active_df.fillna("")
    now_dt = now_jst().replace(tzinfo=None)
    visible_rows = []

    for _, row in active_df.iterrows():
        last_seen = str(row.get("last_seen", "")).strip()
        if not last_seen:
            continue
        try:
            last_dt = pd.to_datetime(last_seen).to_pydatetime()
            if (now_dt - last_dt).total_seconds() <= 15 * 60:
                visible_rows.append(row)
        except Exception:
            pass

    if visible_rows:
        for row in visible_rows:
            user_name = str(row.get("user", "")).strip()
            login_at = str(row.get("login_at", "")).strip()
            st.sidebar.write(f"**{user_name}**")
            st.sidebar.caption(f"ログイン: {login_at}")
    else:
        st.sidebar.write("現在ログイン中の人はいないある。")

# マイ状況
try:
    task_df = load_db("task").fillna("")
except Exception:
    task_df = pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])

my_active = task_df[
    (task_df["status"].astype(str).str.strip() == "作業中") &
    (task_df["user"].astype(str).str.strip() == st.session_state.user)
]

my_todo = task_df[
    (task_df["status"].astype(str).str.strip() == "未着手")
]

st.sidebar.divider()
st.sidebar.markdown("### 📌 マイ状況")

active_count = len(my_active)
todo_count = len(my_todo)

if st.sidebar.button(f"作業中: {active_count}件", key="go_my_active", use_container_width=True):
    st.session_state.current_page = "② タスクの引き受け・報告"
    st.rerun()

if st.sidebar.button(f"未着手全体: {todo_count}件", key="go_todo_board", use_container_width=True):
    st.session_state.current_page = "① 未着手の任務（掲示板）"
    st.rerun()

# ここを追加するある
page = st.session_state.get("current_page", "① 未着手の任務（掲示板）")
# ==========================================
# ① 未着手の任務（掲示板）
# ==========================================
if page == "① 未着手の任務（掲示板）":
    def show_task_board_page():
        st.title("📋 未着手タスク一覧")
        st.write("現在、依頼されている業務の一覧です。新しいタスクを登録することも可能です。")

        with st.expander("➕ 新規タスクを登録する"):
            with st.form("task_form"):
                bulk_tasks = st.text_area(
                    "タスク名（1行に1件。まとめて貼り付けOK）",
                    placeholder="請求書の確認\n支援記録の見直し\n送迎表の作成"
                )
                t_prio = st.select_slider("緊急度", options=["通常", "重要", "至急"])
                t_limit = st.date_input("完了期限", now_jst().date())

                if st.form_submit_button("タスクを登録"):
                    lines = [x.strip() for x in str(bulk_tasks).replace("\r\n", "\n").split("\n") if x.strip()]

                    if lines:
                        df = load_db("task")

                        if df.empty:
                            next_id = 1
                        else:
                            ids = pd.to_numeric(df["id"], errors="coerce").dropna()
                            next_id = int(ids.max()) + 1 if not ids.empty else 1

                        new_rows = []
                        now_str = now_jst().strftime("%Y-%m-%d %H:%M")

                        for task_name in lines:
                            new_rows.append({
                                "id": next_id,
                                "task": task_name,
                                "status": "未着手",
                                "user": "",
                                "limit": str(t_limit),
                                "priority": t_prio,
                                "updated_at": now_str
                            })
                            next_id += 1

                        add_df = pd.DataFrame(new_rows)
                        merged_df = pd.concat([df, add_df], ignore_index=True)

                        save_db(merged_df, "task")
                        sync_task_events_to_calendar()

                        st.success(f"{len(new_rows)}件のタスクを登録したある！")
                        st.rerun()
                    else:
                        st.error("タスクを1件以上入力してください。")

        df = load_db("task")
        todo = df[df["status"].astype(str).str.strip() == "未着手"].copy()

        if not todo.empty:
            prio_map = {"至急": 0, "重要": 1, "通常": 2}
            todo["p_val"] = todo["priority"].map(prio_map)
            st.dataframe(
                todo.sort_values(["p_val", "limit"])[["priority", "limit", "task"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("現在、未着手のタスクはありません。")

    show_task_board_page()
    
# ==========================================
# ② タスクの引き受け・報告
# ==========================================
elif page == "② タスクの引き受け・報告":
    # @st.fragment(run_every=180)
    def show_my_tasks_page():
        st.title("🎯 タスク管理")
        df = load_db("task")
        
        st.subheader("📦 新しくタスクを引き受ける")
        todo = df[df["status"] == "未着手"]
        if todo.empty:
            st.write("引き受け可能なタスクはありません。")
        for _, row in todo.iterrows():
            p_symbol = "🔴 [至急]" if row['priority'] == "至急" else "🟡 [重要]" if row['priority'] == "重要" else "⚪ [通常]"
            if st.button(f"{p_symbol} {row['task']} (期限:{row['limit']}) を開始する", key=f"get_{row['id']}"):
                df.loc[df["id"] == row["id"], ["status", "user", "updated_at"]] = ["作業中", st.session_state.user, now_jst().strftime('%Y-%m-%d %H:%M')]
                save_db(df, "task")
                sync_task_events_to_calendar()
                st.rerun()
        st.divider()
        st.subheader("⚡ 現在対応中のタスク")
        my_tasks = df[(df["status"] == "作業中") & (df["user"] == st.session_state.user)]
        if my_tasks.empty:
            st.write("現在、対応中のタスクはありません。")
        for _, row in my_tasks.iterrows():
            if st.button(f"✅ {row['task']} の完了を報告する", key=f"done_{row['id']}", type="primary"):
                df.loc[df["id"] == row["id"], ["status", "updated_at"]] = ["完了", now_jst().strftime('%Y-%m-%d %H:%M')]
                save_db(df, "task")
                sync_task_events_to_calendar()
                st.rerun()
    show_my_tasks_page()

# ==========================================
# ③ 稼働状況・完了履歴
# ==========================================
elif page == "③ 稼働状況・完了履歴":
    @st.fragment(run_every=180)
    def show_status_page():
        st.title("📊 チーム稼働状況")

        df = load_db("task")

        if df is None or df.empty:
            df = pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])
        else:
            for col in ["id", "task", "status", "user", "limit", "priority", "updated_at"]:
                if col not in df.columns:
                    df[col] = ""

        df = df.fillna("")

        # ------------------------------------------
        # 期限アラート
        # ------------------------------------------
        st.subheader("⏰ 期限アラート")

        today = now_jst().date()
        tomorrow = today + timedelta(days=1)

        deadline_alerts = []

        for _, row in df.iterrows():
            status = str(row.get("status", "")).strip()
            task_name = str(row.get("task", "")).strip()
            limit_str = str(row.get("limit", "")).strip()

            if status == "完了":
                continue

            if limit_str:
                try:
                    limit_date = pd.to_datetime(limit_str).date()

                    if limit_date < today:
                        deadline_alerts.append(("期限切れ", task_name, str(limit_date)))
                    elif limit_date == today:
                        deadline_alerts.append(("今日期限", task_name, str(limit_date)))
                    elif limit_date == tomorrow:
                        deadline_alerts.append(("明日期限", task_name, str(limit_date)))
                except Exception:
                    pass

        if deadline_alerts:
            for kind, task_name, d in deadline_alerts:
                if kind == "期限切れ":
                    st.error(f"🔴 {kind}: {task_name}（{d}）")
                elif kind == "今日期限":
                    st.warning(f"🟠 {kind}: {task_name}（{d}）")
                else:
                    st.info(f"🟡 {kind}: {task_name}（{d}）")
        else:
            st.write("期限アラートはないある。")

        st.divider()

        # ------------------------------------------
        # 現在稼働中のメンバー
        # ------------------------------------------
        st.subheader("🏃 現在稼働中のメンバー")

        active = df[df["status"].astype(str).str.strip() == "作業中"].copy()

        if active.empty:
            st.write("現在稼働中のメンバーはいません。")
        else:
            for _, row in active.iterrows():
                st.info(f"👤 **{row['user']}**： {row['task']} （着手時刻：{row['updated_at']}）")

        st.divider()

        # ------------------------------------------
        # 完了済みのタスク（過去7日間）
        # ------------------------------------------
        st.subheader("✅ 完了済みのタスク（過去7日間）")

        one_week_ago = now_jst() - timedelta(days=7)

        done_rows = []

        for _, row in df.iterrows():
            status = str(row.get("status", "")).strip()
            updated_at = str(row.get("updated_at", "")).strip()

            if status != "完了" or not updated_at:
                continue

            try:
                done_dt = pd.to_datetime(updated_at)
                if done_dt >= one_week_ago:
                    done_rows.append(row)
            except Exception:
                pass

        if done_rows:
            done_df = pd.DataFrame(done_rows)
            done_df = done_df.sort_values("updated_at", ascending=False)
            st.table(done_df[["updated_at", "user", "task"]])
        else:
            st.write("直近1週間以内の完了履歴はありません。")

    show_status_page()

# ==========================================
# ④ チームチャット（画像添付対応ある！）
# ==========================================
elif page == "④ チームチャット":
    @st.fragment(run_every=15)
    def show_chat_page():
        st.title("💬 業務連絡チャット")
        chat_df = load_db("chat")
        today = now_jst().strftime("%Y-%m-%d")

        # 必要列を保証
        if chat_df is None or chat_df.empty:
            chat_df = pd.DataFrame(columns=["date", "time", "user", "message", "image_data"])
        else:
            for col in ["date", "time", "user", "message", "image_data"]:
                if col not in chat_df.columns:
                    chat_df[col] = ""

        mode = st.radio("表示切り替え", ["本日の連絡", "ログ倉庫（過去分）"], horizontal=True)

        if mode == "本日の連絡":
            with st.form("chat_form", clear_on_submit=True):
                msg = st.text_area("業務連絡を入力してください")
                uploaded_image = st.file_uploader(
                    "画像を添付（ドラッグ＆ドロップ可）",
                    type=["png", "jpg", "jpeg"],
                    key="chat_image_uploader"
                )

                if uploaded_image is not None:
                    st.image(uploaded_image, caption="送信前プレビュー", use_container_width=True)

                if st.form_submit_button("送信"):
                    if msg or uploaded_image is not None:
                        image_data = ""

                        if uploaded_image is not None:
                            file_bytes = uploaded_image.read()
                            image_data = base64.b64encode(file_bytes).decode("utf-8")

                        new_msg = pd.DataFrame([{
                            "date": today,
                            "time": datetime.now().strftime("%H:%M"),
                            "user": st.session_state.user,
                            "message": msg,
                            "image_data": image_data
                        }])

                        save_db(pd.concat([chat_df, new_msg], ignore_index=True), "chat")
                        st.success("送信したある！")
                        st.rerun()
                    else:
                        st.error("メッセージか画像のどちらかを入れてほしいある。")

            st.divider()

            display_df = chat_df[chat_df["date"].astype(str) == today].copy()

            if display_df.empty:
                st.write("本日の連絡事項はありません。")

            display_df = display_df.fillna("")

            for _, row in display_df.sort_values("time", ascending=False).iterrows():
                user_name = str(row.get("user", ""))
                msg_text = str(row.get("message", ""))
                time_text = str(row.get("time", ""))
                image_data = str(row.get("image_data", ""))

                st.markdown(f"**{user_name}** ({time_text})")

                if msg_text.strip():
                    st.write(msg_text)

                if image_data.strip():
                    try:
                        image_bytes = base64.b64decode(image_data)
                        st.image(image_bytes, caption="添付画像", use_container_width=True)
                    except Exception:
                        st.warning("画像の読み込みに失敗したある。")

                st.divider()

        else:
            st.subheader("📚 過去の連絡ログ")

            if chat_df.empty:
                st.write("過去ログはありません。")
                return

            dates = sorted(
                [str(d).strip() for d in chat_df["date"].dropna().tolist() if str(d).strip()],
                reverse=True
            )

            if not dates:
                st.write("過去ログはありません。")
                return

            target_date = st.selectbox("参照する日付を選択してください", dates)

            if target_date:
                log_df = chat_df[chat_df["date"].astype(str) == target_date].copy().fillna("")

                for _, row in log_df.sort_values("time", ascending=False).iterrows():
                    user_name = str(row.get("user", ""))
                    msg_text = str(row.get("message", ""))
                    time_text = str(row.get("time", ""))
                    image_data = str(row.get("image_data", ""))

                    st.markdown(f"**{user_name}** ({time_text})")

                    if msg_text.strip():
                        st.write(msg_text)

                    if image_data.strip():
                        try:
                            image_bytes = base64.b64decode(image_data)
                            st.image(image_bytes, caption="添付画像", use_container_width=True)
                        except Exception:
                            st.warning("画像の読み込みに失敗したある。")

                    st.divider()

    show_chat_page()

# ==========================================
# ⑤ 業務マニュアル ver2 (画像D&D + 削除対応)
# ==========================================
elif page == "⑤ 業務マニュアル":
    @st.fragment(run_every=180)
    def show_manual_page():
        st.title("📚 業務マニュアル")

        m_df = load_db("manual")

        if m_df is None or m_df.empty:
            m_df = pd.DataFrame(columns=["id", "title", "content", "image_data", "created_at"])
        else:
            for col in ["id", "title", "content", "image_data", "created_at"]:
                if col not in m_df.columns:
                    m_df[col] = ""

        with st.expander("📝 新しいマニュアルを作成する"):
            with st.form("manual_form"):
                title = st.text_input("マニュアルのタイトル")
                content = st.text_area("手順の説明")

                uploaded_file = st.file_uploader(
                    "写真をドラッグ&ドロップ",
                    type=["png", "jpg", "jpeg"]
                )

                if uploaded_file is not None:
                    st.image(
                        uploaded_file,
                        caption="アップロード画像プレビュー",
                        use_container_width=True
                    )

                if st.form_submit_button("保存する"):
                    if title and content:
                        if m_df.empty:
                            next_id = 1
                        else:
                            ids = pd.to_numeric(m_df["id"], errors="coerce").dropna()
                            next_id = int(ids.max()) + 1 if not ids.empty else 1

                        image_data = ""
                        if uploaded_file is not None:
                            bytes_data = uploaded_file.getvalue()
                            image_data = base64.b64encode(bytes_data).decode("utf-8")

                        new_m = pd.DataFrame([{
                            "id": next_id,
                            "title": title,
                            "content": content,
                            "image_data": image_data,
                            "created_at": now_jst().strftime("%Y-%m-%d %H:%M")
                        }])

                        save_db(pd.concat([m_df, new_m], ignore_index=True), "manual")
                        st.success("マニュアルを保存したある！")
                        st.rerun()
                    else:
                        st.error("タイトルと説明は必須ある。")

        st.divider()

        m_df = load_db("manual")

        if m_df is None or m_df.empty:
            st.info("マニュアルはまだ登録されてないある。")
            return

        for col in ["id", "title", "content", "image_data", "created_at"]:
            if col not in m_df.columns:
                m_df[col] = ""

        m_df = m_df.fillna("")

        try:
            display_df = m_df.sort_values("created_at", ascending=False)
        except Exception:
            display_df = m_df.copy()

        for _, row in display_df.iterrows():
            manual_id = row.get("id", "")
            title = str(row.get("title", "")).strip()
            content = str(row.get("content", "")).strip()
            created_at = str(row.get("created_at", "")).strip()
            image_data = str(row.get("image_data", "")).strip()

            with st.expander(f"📖 {title} (作成日: {created_at})"):
                if content:
                    st.write(content)

                if image_data:
                    try:
                        image_bytes = base64.b64decode(image_data)
                        st.image(
                            image_bytes,
                            caption="添付画像",
                            use_container_width=True
                        )
                    except Exception:
                        st.warning("画像データの読み込みに失敗したある。")

                if st.button("🗑️ このマニュアルを削除する", key=f"delete_manual_{manual_id}"):
                    new_df = m_df[m_df["id"].astype(str) != str(manual_id)].copy()
                    save_db(new_df, "manual")
                    st.success("削除したある。")
                    st.rerun()

    show_manual_page()

# ==========================================
# ⑥ 日誌入力状況（年つき横表・Excel風ある！）
# ==========================================
elif page == "⑥ 日誌入力状況":
    @st.fragment(run_every=180)
    def show_record_status_page():
        st.title("📝 日誌入力状況管理")

        # 表示したい年の範囲
        start_year = 2025
        end_year = 2026

        # 年月列を作るある
        month_cols = []
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                month_cols.append(f"{year}年{month}月")

        required_cols = ["resident_name"] + month_cols

        r_df = load_db("record_status")

        # 空でも最低限の形に整えるある
        if r_df is None or r_df.empty:
            r_df = pd.DataFrame(columns=required_cols)
        else:
            for col in required_cols:
                if col not in r_df.columns:
                    r_df[col] = ""
            r_df = r_df[required_cols].copy()

        # ==========================================
        # 利用者一括追加
        # ==========================================
        with st.expander("👤 利用者をまとめて追加する"):
            bulk_names = st.text_area(
                "名前を入力（改行ごとに1人。Excelの縦列コピーもOK）",
                placeholder="荒木 和也\n石田 愛子\n岩城 勝也"
            )

            if st.button("利用者を追加する", key="add_residents_button"):
                names = [
                    n.strip()
                    for n in str(bulk_names).replace("\r\n", "\n").split("\n")
                    if n.strip()
                ]

                if not names:
                    st.error("名前を1人以上入力してほしいある。")
                else:
                    existing_names = set(
                        str(x).strip()
                        for x in r_df["resident_name"].dropna().tolist()
                        if str(x).strip()
                    )

                    new_rows = []
                    skipped = []

                    for name in names:
                        if name in existing_names:
                            skipped.append(name)
                        else:
                            row = {"resident_name": name}
                            for m in month_cols:
                                row[m] = ""
                            new_rows.append(row)

                    if new_rows:
                        add_df = pd.DataFrame(new_rows)
                        r_df = pd.concat([r_df, add_df], ignore_index=True)
                        save_db(r_df, "record_status")

                        if skipped:
                            st.success(
                                f"{len(new_rows)}人追加したある。重複スキップ: {', '.join(skipped)}"
                            )
                        else:
                            st.success(f"{len(new_rows)}人追加したある。")
                        st.rerun()
                    else:
                        st.warning("全員すでに登録済みある。")

        st.divider()
        st.caption("各セルに「未入力」「15日まで」「完了」など自由に入力できるある。")

        # data_editorで落ちないように、全部文字列にそろえるある
        r_df = r_df.fillna("")

        for col in required_cols:
            if col not in r_df.columns:
                r_df[col] = ""
            r_df[col] = r_df[col].astype(str)

        # 列設定を自動生成
        column_config = {
            "resident_name": st.column_config.TextColumn("氏名", width="medium")
        }
        for col in month_cols:
            column_config[col] = st.column_config.TextColumn(col, width="small")

        # 表示用
        edited_df = st.data_editor(
            r_df,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config=column_config,
            key="record_status_editor"
        )

        # 保存ボタン
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("💾 表を保存する", type="primary", key="save_record_status_button"):
                edited_df = edited_df.fillna("")

                # 氏名空欄の行は除外
                edited_df = edited_df[
                    edited_df["resident_name"].astype(str).str.strip() != ""
                ].copy()

                # 必要列だけにそろえる
                for col in required_cols:
                    if col not in edited_df.columns:
                        edited_df[col] = ""
                    edited_df[col] = edited_df[col].astype(str)

                edited_df = edited_df[required_cols]

                save_db(edited_df, "record_status")
                st.success("保存したある！")
                st.rerun()

    show_record_status_page()

# ==========================================
# ⑦ 勤務カレンダー
# ==========================================
elif page == "⑦ タスクカレンダー":
    sync_task_events_to_calendar()

    @st.fragment(run_every=180)
    def show_calendar_page():
        st.title("📅 タスクカレンダー")

        try:
            cal_df = load_db("calendar")
            task_df = load_db("task")
        except Exception:
            st.warning("Googleスプレッドシートとの通信が一時的に不安定ある。少し待って再読み込みしてほしいある。")
            return

        # calendarシートの最低列を保証
        if cal_df is None or cal_df.empty:
            cal_df = pd.DataFrame(columns=["id", "title", "start", "end", "user", "memo", "source_type", "source_task_id"])
        else:
            for col in ["id", "title", "start", "end", "user", "memo", "source_type", "source_task_id"]:
                if col not in cal_df.columns:
                    cal_df[col] = ""

        # taskシートの最低列を保証
        if task_df is None or task_df.empty:
            task_df = pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])
        else:
            for col in ["id", "task", "status", "user", "limit", "priority", "updated_at"]:
                if col not in task_df.columns:
                    task_df[col] = ""

        with st.expander("➕ 予定を追加する"):
            with st.form("calendar_form"):
                title = st.text_input("予定名")
                start_date = st.date_input("開始日")
                end_date = st.date_input("終了日")
                user_name = st.text_input("担当者", value=st.session_state.user)
                memo = st.text_area("メモ")

                if st.form_submit_button("予定を保存する"):
                    if title:
                        if cal_df.empty:
                            next_id = 1
                        else:
                            try:
                                next_id = pd.to_numeric(cal_df["id"], errors="coerce").max()
                                next_id = 1 if pd.isna(next_id) else int(next_id) + 1
                            except Exception:
                                next_id = len(cal_df) + 1

                        new_row = pd.DataFrame([{
                            "id": next_id,
                            "title": title,
                            "start": str(start_date),
                            "end": str(end_date),
                            "user": user_name,
                            "memo": memo,
                            "source_type": "manual",
                            "source_task_id": ""
                        }])

                        save_db(pd.concat([cal_df, new_row], ignore_index=True), "calendar")
                        st.success("予定を保存したある！")
                        st.rerun()
                    else:
                        st.error("予定名を入れてほしいある。")

        st.divider()

        events = []
        event_ids = set()

        # ------------------------------------------
        # ① 手入力の予定（calendarシート）
        # ------------------------------------------
        display_cal_df = cal_df.fillna("")

        for _, row in display_cal_df.iterrows():
            title = str(row.get("title", "")).strip()
            start = str(row.get("start", "")).strip()
            end = str(row.get("end", "")).strip()
            user_name = str(row.get("user", "")).strip()
            memo = str(row.get("memo", "")).strip()
            source_type = str(row.get("source_type", "")).strip()

            # task由来イベントは下で task_df から作るので、ここでは表示しないある
            if source_type in ["task_deadline", "task_active"]:
                continue

            if title and start:
                short_title = title if len(title) <= 10 else title[:10] + "…"
                event_title = short_title if not user_name else f"{user_name}：{short_title}"
                event_id = f"manual_{row.get('id', '')}"

                if event_id not in event_ids:
                    events.append({
                        "id": event_id,
                        "title": event_title,
                        "start": start,
                        "end": end if end else start,
                        "color": "#3788d8",
                        "extendedProps": {
                            "memo": memo,
                            "user": user_name,
                            "source_type": "manual",
                            "full_title": title,
                        }
                    })
                    event_ids.add(event_id)

        # ------------------------------------------
        # ② taskシートから締切イベントを自動生成
        # ------------------------------------------
        display_task_df = task_df.fillna("")
        today = now_jst().date()

        for _, row in display_task_df.iterrows():
            task_id = str(row.get("id", "")).strip()
            task_name = str(row.get("task", "")).strip()
            status = str(row.get("status", "")).strip()
            user_name = str(row.get("user", "")).strip()
            priority = str(row.get("priority", "")).strip()
            limit_str = str(row.get("limit", "")).strip()
            updated_at = str(row.get("updated_at", "")).strip()

            # 締切が今日以降なら出す
            if limit_str:
                try:
                    limit_date = pd.to_datetime(limit_str).date()
                    if limit_date >= today:
                        event_id = f"deadline_{task_id}"
                        short_title = task_name if len(task_name) <= 10 else task_name[:10] + "…"

                        if event_id not in event_ids:
                            events.append({
                                "id": event_id,
                                "title": f"締切：{short_title}",
                                "start": str(limit_date),
                                "end": str(limit_date),
                                "color": "#e74c3c",
                                "extendedProps": {
                                    "memo": f"優先度: {priority} / 状態: {status}",
                                    "user": user_name,
                                    "source_type": "task_deadline",
                                    "full_title": task_name,
                                }
                            })
                            event_ids.add(event_id)
                except Exception:
                    pass

            # 作業中なら開始日で出す
            if status == "作業中" and updated_at:
                try:
                    active_date = pd.to_datetime(updated_at).date()
                    event_id = f"active_{task_id}"
                    short_title = task_name if len(task_name) <= 10 else task_name[:10] + "…"

                    if event_id not in event_ids:
                        events.append({
                            "id": event_id,
                            "title": f"作業中：{short_title}",
                            "start": str(active_date),
                            "end": str(active_date),
                            "color": "#f39c12",
                            "extendedProps": {
                                "memo": f"着手: {updated_at}",
                                "user": user_name,
                                "source_type": "task_active",
                                "full_title": task_name,
                            }
                        })
                        event_ids.add(event_id)
                except Exception:
                    pass

        calendar_options = {
            "initialView": "dayGridMonth",
            "locale": "ja",
            "height": "auto",
            "dayMaxEvents": 3,
            "moreLinkClick": "popover",
            "displayEventTime": False,
            "eventDisplay": "block",
            "headerToolbar": {
                "left": "prev,next today",
                "center": "title",
                "right": "dayGridMonth,timeGridWeek,listWeek"
            }
        }

        state = st_calendar(
            events=events,
            options=calendar_options,
            key="work_calendar"
        )



        st.divider()
        st.subheader("📋 登録済み予定一覧")

        if not display_cal_df.empty:
            st.dataframe(
                display_cal_df[["start", "end", "user", "title", "memo"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("手入力の予定はまだないある。")

        # ------------------------------------------
        # 選択日の予定一覧
        # ------------------------------------------
        st.divider()
        st.subheader("🗓️ 選択日の予定一覧")

        selected_date = None

        if state and state.get("dateClick"):
            selected_date = str(state["dateClick"].get("date", ""))[:10]
        elif state and state.get("eventClick"):
            selected_date = str(state["eventClick"]["event"].get("start", ""))[:10]

        if selected_date:
            day_events = []

            for ev in events:
                ev_start = str(ev.get("start", ""))[:10]
                if ev_start == selected_date:
                    ext = ev.get("extendedProps", {})
                    source_type = ext.get("source_type", "")

                    type_label = "手入力予定"
                    if source_type == "task_deadline":
                        type_label = "締切タスク"
                    elif source_type == "task_active":
                        type_label = "作業中タスク"

                    day_events.append({
                        "種類": type_label,
                        "予定名": ext.get("full_title", ev.get("title", "")),
                        "担当者": ext.get("user", ""),
                        "メモ": ext.get("memo", "")
                    })

            if day_events:
                st.dataframe(
                    pd.DataFrame(day_events),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info(f"{selected_date} の予定はないある。")
        else:
            st.caption("日付か予定をクリックすると、その日の一覧を下に表示するある。")

        # ------------------------------------------
        # クリックした予定の詳細
        # ------------------------------------------
        if state and state.get("eventClick"):
            clicked = state["eventClick"]["event"]
            ext = clicked.get("extendedProps", {})

            st.divider()
            st.subheader("🔍 選択中の予定")

            full_title = ext.get("full_title", "") or clicked.get("title", "")
            source_type = ext.get("source_type", "")

            type_label = "手入力予定"
            if source_type == "task_deadline":
                type_label = "締切タスク"
            elif source_type == "task_active":
                type_label = "作業中タスク"

            st.write(f"**種類**: {type_label}")
            st.write(f"**予定名**: {full_title}")
            st.write(f"**開始**: {clicked.get('start', '')}")
            st.write(f"**終了**: {clicked.get('end', '')}")

            if ext.get("user"):
                st.write(f"**担当者**: {ext.get('user')}")

            if ext.get("memo"):
                st.write(f"**メモ**: {ext.get('memo')}")

    show_calendar_page()

# ==========================================
# ⑧ 緊急一覧
# ==========================================
elif page == "⑧ 緊急一覧":
    def show_urgent_page():
        st.title("🚨 緊急一覧")

        urgent_df = get_urgent_tasks_df()

        if urgent_df.empty:
            st.success("現在、至急・重要タスクはないある。")
            return

        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 3])

        with col1:
            if st.button("全部", key="urgent_filter_all", use_container_width=True):
                st.session_state.urgent_filter = "全部"
        with col2:
            if st.button("至急", key="urgent_filter_critical", use_container_width=True):
                st.session_state.urgent_filter = "至急"
        with col3:
            if st.button("重要", key="urgent_filter_important", use_container_width=True):
                st.session_state.urgent_filter = "重要"
        with col4:
            if st.button("未着手", key="urgent_filter_todo", use_container_width=True):
                st.session_state.urgent_filter = "未着手"

        current_filter = st.session_state.get("urgent_filter", "全部")

        if current_filter == "至急":
            urgent_df = urgent_df[urgent_df["priority"].astype(str).str.strip() == "至急"]
        elif current_filter == "重要":
            urgent_df = urgent_df[urgent_df["priority"].astype(str).str.strip() == "重要"]
        elif current_filter == "未着手":
            urgent_df = urgent_df[urgent_df["status"].astype(str).str.strip() == "未着手"]

        st.caption(f"現在の表示: {current_filter}")

        if urgent_df.empty:
            st.info("条件に合う緊急タスクはないある。")
            return

        # ===============================
        # 緊急カード表示（2列・期限色対応）
        # ===============================
        cols = st.columns(2)

        for i, (_, row) in enumerate(urgent_df.iterrows()):
            with cols[i % 2]:
                task_id = str(row.get("id", "")).strip()
                task_name = str(row.get("task", "")).strip()
                priority = str(row.get("priority", "")).strip()
                status = str(row.get("status", "")).strip()
                user_name = str(row.get("user", "")).strip()
                limit_str = str(row.get("limit", "")).strip()
                updated_at = str(row.get("updated_at", "")).strip()

                limit_date = None
                try:
                    limit_date = pd.to_datetime(limit_str, errors="coerce").date()
                except:
                    pass

                today = now_jst().date()

                if limit_date and limit_date < today:
                    icon = "🩸"
                    border_color = "#d63031"
                    bg_color = "#ffeaea"
                elif priority == "至急":
                    icon = "🚨"
                    border_color = "#ff4d4f"
                    bg_color = "#fff1f0"
                else:
                    icon = "⚠️"
                    border_color = "#ff9f43"
                    bg_color = "#fff7e6"

                assignee = user_name if user_name else "未割当"

                with st.container(border=True):
                    st.markdown(
                        f"""
                        <div style="
                            border-left: 8px solid {border_color};
                            background-color: {bg_color};
                            padding: 16px;
                            border-radius: 12px;
                            margin-bottom: 12px;
                            min-height: 160px;
                        ">
                        <div style="font-size:20px; font-weight:700; margin-bottom:6px;">
                        {icon} {priority}
                        </div>
                        <div style="font-size:22px; font-weight:700; margin-bottom:10px;">
                        {task_name}
                        </div>
                        <div style="line-height:1.9;">
                        <b>状態:</b> {status}<br>
                        <b>担当:</b> {assignee}<br>
                        <b>期限:</b> {limit_str}<br>
                        <b>更新:</b> {updated_at}
                        </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                    action_cols = st.columns([1, 1, 4])

                    if status == "未着手":
                        with action_cols[0]:
                            if st.button("開始する", key=f"urgent_start_{task_id}", use_container_width=True):
                                start_task(task_id)
                                st.success("タスクを開始したある！")
                                st.rerun()

                    elif status == "作業中":
                        if user_name == st.session_state.user:
                            with action_cols[1]:
                                if st.button("完了する", key=f"urgent_done_{task_id}", use_container_width=True):
                                    complete_task(task_id)
                                    st.success("タスクを完了したある！")
                                    st.rerun()
                        else:
                            with action_cols[2]:
                                st.caption(f"現在 {user_name} さんが対応中ある。")

    show_urgent_page()

# ==========================================
# ⑨ 利用者情報（軽量化版 + 至急アラート連動）
# ==========================================
elif page == "⑨ 利用者情報":
    def show_resident_page():
        st.title("👤 利用者情報")

        master_df = get_resident_master_df()

        if "resident_mode" not in st.session_state:
            st.session_state.resident_mode = "利用中"

        if "selected_resident_id" not in st.session_state:
            st.session_state.selected_resident_id = ""

        if "edit_resident_basic" not in st.session_state:
            st.session_state.edit_resident_basic = False

        if "edit_resident_schedule" not in st.session_state:
            st.session_state.edit_resident_schedule = False

        if "edit_resident_note" not in st.session_state:
            st.session_state.edit_resident_note = False

        def reset_resident_edit_flags():
            st.session_state.edit_resident_basic = False
            st.session_state.edit_resident_schedule = False
            st.session_state.edit_resident_note = False

        def parse_time_range(raw_text: str):
            raw = str(raw_text).strip()
            if not raw:
                return "", ""

            raw = raw.replace("～", "〜").replace("~", "〜").replace("-", "〜")
            if "〜" in raw:
                start_time, end_time = [x.strip() for x in raw.split("〜", 1)]
                return start_time, end_time

            return raw, ""

        # ------------------------------------------
        # 一覧モード
        # ------------------------------------------
        if not st.session_state.selected_resident_id:
            reset_resident_edit_flags()

            top_cols = st.columns([1, 1, 3])

            with top_cols[0]:
                if st.button("利用者一覧", use_container_width=True):
                    st.session_state.resident_mode = "利用中"
                    st.rerun()

            with top_cols[1]:
                if st.button("退所者一覧", use_container_width=True):
                    st.session_state.resident_mode = "退所"
                    st.rerun()

            with top_cols[2]:
                st.caption(f"現在表示: {st.session_state.resident_mode}")

            st.divider()

            with st.expander("➕ 新しい利用者を追加する"):
                weekdays = ["月", "火", "水", "木", "金", "土", "日"]

                service_defs = [
                    ("病院", "#F8E7A1"),
                    ("看護", "#CFEAF6"),
                    ("介護", "#DDEDB7"),
                ]

                slot_placeholders = [
                    "例 10:00〜11:00",
                    "2つ目があれば入力",
                    "3つ目があれば入力",
                    "4つ目があれば入力",
                ]

                def parse_add_time_range(raw_text: str):
                    raw = str(raw_text).strip()
                    if not raw:
                        return "", ""

                    raw = raw.replace("～", "〜").replace("~", "〜").replace("-", "〜")
                    if "〜" in raw:
                        start_time, end_time = [x.strip() for x in raw.split("〜", 1)]
                        return start_time, end_time

                    return raw, ""

                st.markdown(
                    """
                    <style>
                    .add-svc-title {
                        display:inline-block;
                        min-width:140px;
                        text-align:center;
                        font-weight:700;
                        font-size:20px;
                        color:#111;
                        border:3px solid #111;
                        padding:8px 18px;
                        margin-top:10px;
                        margin-bottom:12px;
                    }
                    .add-week-head {
                        text-align:center;
                        font-weight:700;
                        border:2px solid #111;
                        padding:7px 0;
                        background:#fafafa;
                        margin-bottom:4px;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True
                )

                with st.form("resident_add_form"):
                    st.markdown("### 基本情報")

                    basic1 = st.columns(3)
                    with basic1[0]:
                        resident_name = st.text_input("利用者名")
                    with basic1[1]:
                        status = st.selectbox("状態", ["利用中", "退所"])
                    with basic1[2]:
                        consultant = st.text_input("相談員")

                    basic2 = st.columns(3)
                    with basic2[0]:
                        consultant_phone = st.text_input("相談員電話")
                    with basic2[1]:
                        caseworker = st.text_input("ケースワーカー")
                    with basic2[2]:
                        caseworker_phone = st.text_input("ケースワーカー電話")

                    st.markdown("### 病院・看護・介護の週間予定")
                    st.caption("同じ曜日に2回以上ある場合は、同じ曜日の下の2つ目・3つ目・4つ目にもそのまま入力してほしいある。Enterは不要ある。")

                    weekly_inputs = {}

                    for service_name, service_color in service_defs:
                        st.markdown(
                            f'<div class="add-svc-title" style="background:{service_color};">{service_name}</div>',
                            unsafe_allow_html=True
                        )

                        top_info = st.columns([4, 4, 3])
                        with top_info[0]:
                            place_val = st.text_input(
                                f"{service_name}名",
                                key=f"add_{service_name}_place",
                                placeholder=f"{service_name}名"
                            )
                        with top_info[1]:
                            person_val = st.text_input(
                                f"{service_name}担当",
                                key=f"add_{service_name}_person",
                                placeholder="担当"
                            )
                        with top_info[2]:
                            phone_val = st.text_input(
                                f"{service_name}電話",
                                key=f"add_{service_name}_phone",
                                placeholder="電話"
                            )

                        day_cols = st.columns(7)

                        day_values = {}

                        for i, wd in enumerate(weekdays):
                            with day_cols[i]:
                                st.markdown(f'<div class="add-week-head">{wd}</div>', unsafe_allow_html=True)

                                slots = []
                                for slot_idx in range(4):
                                    slot_val = st.text_input(
                                        f"{service_name}_{wd}_{slot_idx+1}",
                                        key=f"add_{service_name}_{wd}_{slot_idx+1}",
                                        label_visibility="collapsed",
                                        placeholder=slot_placeholders[slot_idx]
                                    )
                                    slots.append(slot_val)

                                day_values[wd] = slots

                        weekly_inputs[service_name] = {
                            "place": str(place_val).strip(),
                            "person_in_charge": str(person_val).strip(),
                            "phone": str(phone_val).strip(),
                            "days": day_values
                        }

                    submit_cols = st.columns(2)
                    with submit_cols[0]:
                        add_resident = st.form_submit_button("利用者を登録する", use_container_width=True)
                    with submit_cols[1]:
                        cancel_add = st.form_submit_button("入力をやめる", use_container_width=True)

                    if add_resident:
                        if resident_name.strip():
                            next_resident_id = get_next_resident_id(master_df)
                            now_str = now_jst().strftime("%Y-%m-%d %H:%M")

                            new_master_row = pd.DataFrame([{
                                "resident_id": next_resident_id,
                                "resident_name": resident_name.strip(),
                                "status": status,
                                "consultant": consultant.strip(),
                                "consultant_phone": consultant_phone.strip(),
                                "caseworker": caseworker.strip(),
                                "caseworker_phone": caseworker_phone.strip(),
                                "hospital": weekly_inputs["病院"]["place"],
                                "hospital_phone": weekly_inputs["病院"]["phone"],
                                "nurse": weekly_inputs["看護"]["place"],
                                "nurse_phone": weekly_inputs["看護"]["phone"],
                                "care": weekly_inputs["介護"]["place"],
                                "care_phone": weekly_inputs["介護"]["phone"],
                                "created_at": now_str,
                                "updated_at": now_str
                            }])

                            new_master_df = pd.concat([master_df, new_master_row], ignore_index=True)
                            save_db(new_master_df, "resident_master")

                            schedule_df_add = get_resident_schedule_df()
                            next_schedule_id = get_next_numeric_id(schedule_df_add, "id", 1)
                            new_schedule_rows = []

                            for service_type, data in weekly_inputs.items():
                                place_name = str(data["place"]).strip()
                                phone_text = str(data["phone"]).strip()
                                person_text = str(data["person_in_charge"]).strip()

                                for wd, slot_list in data["days"].items():
                                    for slot_index, raw_time in enumerate(slot_list, start=1):
                                        start_time, end_time = parse_add_time_range(raw_time)
                                        if not start_time and not end_time:
                                            continue

                                        new_schedule_rows.append({
                                            "id": next_schedule_id,
                                            "resident_id": next_resident_id,
                                            "weekday": wd,
                                            "service_type": service_type,
                                            "start_time": start_time,
                                            "end_time": end_time,
                                            "place": place_name,
                                            "phone": phone_text,
                                            "person_in_charge": person_text,
                                            "memo": f"slot:{slot_index}"
                                        })
                                        next_schedule_id += 1

                            if new_schedule_rows:
                                add_schedule_df = pd.DataFrame(new_schedule_rows)
                                merged_schedule_df = pd.concat([schedule_df_add, add_schedule_df], ignore_index=True)
                                save_db(merged_schedule_df.fillna(""), "resident_schedule")

                            st.success("利用者を登録したある！")
                            st.rerun()
                        else:
                            st.error("利用者名を入力してほしいある。")

                    if cancel_add:
                        st.rerun()

            st.divider()

            search_word = st.text_input("名前検索", placeholder="利用者名を入力")

            list_df = master_df.copy()
            list_df = list_df[
                list_df["status"].astype(str).str.strip() == st.session_state.resident_mode
            ].copy()

            if search_word.strip():
                list_df = list_df[
                    list_df["resident_name"].astype(str).str.contains(search_word.strip(), case=False, na=False)
                ].copy()

            if not list_df.empty:
                list_df = list_df.sort_values("resident_name")

            if list_df.empty:
                st.info("該当する利用者はいないある。")
                return

            cols = st.columns(2)

            for i, (_, row) in enumerate(list_df.iterrows()):
                with cols[i % 2]:
                    resident_id = str(row.get("resident_id", "")).strip()
                    resident_name = str(row.get("resident_name", "")).strip()
                    status = str(row.get("status", "")).strip()
                    consultant = str(row.get("consultant", "")).strip()

                    if status == "利用中":
                        status_label = "🟢 利用中"
                        bg = "#eefbf0"
                        border = "#2ecc71"
                    else:
                        status_label = "⚫ 退所"
                        bg = "#f3f4f6"
                        border = "#7f8c8d"

                    with st.container(border=True):
                        st.markdown(
                            f"""
                            <div style="
                                border-left: 8px solid {border};
                                background-color: {bg};
                                padding: 14px;
                                border-radius: 10px;
                                min-height: 140px;
                                margin-bottom: 10px;
                            ">
                                <div style="font-size: 20px; font-weight: 700; margin-bottom: 8px;">
                                    {resident_name}
                                </div>
                                <div style="margin-bottom: 6px;">{status_label}</div>
                                <div>相談員: {consultant if consultant else '未登録'}</div>
                                <div>ID: {resident_id}</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                        if st.button("詳細を見る", key=f"open_resident_{resident_id}", use_container_width=True):
                            st.session_state.selected_resident_id = resident_id
                            reset_resident_edit_flags()
                            st.rerun()

            return

        # ------------------------------------------
        # 個人詳細モード
        # ------------------------------------------
        selected_id = str(st.session_state.selected_resident_id).strip()

        detail_df = master_df[master_df["resident_id"].astype(str) == selected_id].copy()
        if detail_df.empty:
            st.warning("利用者情報が見つからないある。")
            if st.button("一覧に戻る", use_container_width=True):
                st.session_state.selected_resident_id = ""
                reset_resident_edit_flags()
                st.rerun()
            return

        row = detail_df.iloc[0]
        resident_name = str(row.get("resident_name", "")).strip()
        status = str(row.get("status", "")).strip()

        schedule_df = get_resident_schedule_df()
        notes_df = get_resident_notes_df()

        try:
            task_df_detail = load_db("task")
            if task_df_detail is None or task_df_detail.empty:
                task_df_detail = pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])
            else:
                for col in ["id", "task", "status", "user", "limit", "priority", "updated_at"]:
                    if col not in task_df_detail.columns:
                        task_df_detail[col] = ""
                task_df_detail = task_df_detail.fillna("")
        except Exception:
            task_df_detail = pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])

        back_cols = st.columns([1, 5])
        with back_cols[0]:
            if st.button("← 一覧に戻る", use_container_width=True):
                st.session_state.selected_resident_id = ""
                reset_resident_edit_flags()
                st.rerun()

        status_label = "🟢 利用中" if status == "利用中" else "⚫ 退所"

        st.subheader(f"{resident_name} 様")
        st.caption(f"{status_label} / ID: {selected_id}")

        st.divider()
        st.markdown("### ☎️ 関係者・外部連携")

        contact_cards_df = get_resident_contact_cards(selected_id)

        if contact_cards_df.empty:
            st.info("この利用者に紐づく関係者はまだ登録されてないある。")
        else:
            category_icon_map = {
                "医療": "🏥",
                "外部連携": "🤝",
                "家族": "👪",
                "業者": "🛠️",
            }

            display_order = ["医療", "外部連携", "家族", "業者"]

            for cat1 in display_order:
                cat_df = contact_cards_df[
                    contact_cards_df["category1"].astype(str).str.strip() == cat1
                ].copy()

                if cat_df.empty:
                    continue

                icon = category_icon_map.get(cat1, "📌")
                st.markdown(f"#### {icon} {cat1}")

                for _, crow in cat_df.iterrows():
                    role = str(crow.get("role", "")).strip()
                    category2 = str(crow.get("category2", "")).strip()
                    name = str(crow.get("name", "")).strip()
                    org = str(crow.get("organization", "")).strip()
                    phone = str(crow.get("phone", "")).strip()
                    memo = str(crow.get("memo", "")).strip()
                    contact_id = str(crow.get("contact_id", "")).strip()

                    title_text = role if role else category2

                    with st.container(border=True):
                        st.markdown(
                            f"""
                            <div style="
                                border-left: 6px solid #9aa5b1;
                                background:#fafafa;
                                padding:12px 14px;
                                border-radius:10px;
                                margin-bottom:10px;
                            ">
                                <div style="font-size:18px; font-weight:700; margin-bottom:6px;">
                                    {title_text}
                                </div>
                                <div style="line-height:1.8;">
                                    <b>氏名:</b> {name}<br>
                                    <b>事業所:</b> {org}<br>
                                    <b>電話:</b> {phone}<br>
                                    <b>分類:</b> {category2}<br>
                                    <b>ID:</b> {contact_id}
                                </div>
                                <div style="margin-top:8px; color:#555;">
                                    {memo}
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )        

        # ------------------------------------------
        # この人の至急アラート
        # ------------------------------------------
        st.markdown("### 🚨 この人のために今すぐやること")

        urgent_person_df = task_df_detail[
            task_df_detail["priority"].astype(str).str.strip().isin(["至急", "重要"]) &
            (task_df_detail["status"].astype(str).str.strip() != "完了") &
            task_df_detail["task"].astype(str).str.contains(resident_name, case=False, na=False)
        ].copy()

        if not urgent_person_df.empty:
            prio_map = {"至急": 0, "重要": 1}
            urgent_person_df["prio_sort"] = urgent_person_df["priority"].map(prio_map).fillna(9)
            urgent_person_df["limit_sort"] = pd.to_datetime(urgent_person_df["limit"], errors="coerce")
            urgent_person_df = urgent_person_df.sort_values(
                ["prio_sort", "limit_sort", "updated_at"],
                ascending=[True, True, False]
            )

            for _, trow in urgent_person_df.iterrows():
                t_id = str(trow.get("id", "")).strip()
                t_name = str(trow.get("task", "")).strip()
                t_priority = str(trow.get("priority", "")).strip()
                t_status = str(trow.get("status", "")).strip()
                t_user = str(trow.get("user", "")).strip()
                t_limit = str(trow.get("limit", "")).strip()
                t_updated = str(trow.get("updated_at", "")).strip()

                limit_date = pd.to_datetime(t_limit, errors="coerce")
                today = now_jst().date()

                if pd.notna(limit_date) and limit_date.date() < today:
                    icon = "🩸"
                    border_color = "#d63031"
                    bg_color = "#ffeaea"
                elif t_priority == "至急":
                    icon = "🚨"
                    border_color = "#ff4d4f"
                    bg_color = "#fff1f0"
                else:
                    icon = "⚠️"
                    border_color = "#ff9f43"
                    bg_color = "#fff7e6"

                assignee = t_user if t_user else "未割当"

                with st.container(border=True):
                    st.markdown(
                        f"""
                        <div style="
                            border-left: 8px solid {border_color};
                            background-color: {bg_color};
                            padding: 14px;
                            border-radius: 10px;
                            margin-bottom: 10px;
                        ">
                            <div style="font-size:18px; font-weight:700; margin-bottom:6px;">
                                {icon} {t_priority} - {t_name}
                            </div>
                            <div style="line-height:1.8;">
                                <b>状態:</b> {t_status}<br>
                                <b>担当:</b> {assignee}<br>
                                <b>期限:</b> {t_limit}<br>
                                <b>更新:</b> {t_updated}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                    btn_cols = st.columns([1, 1, 4])

                    if t_status == "未着手":
                        with btn_cols[0]:
                            if st.button("開始する", key=f"resident_urgent_start_{t_id}", use_container_width=True):
                                start_task(t_id)
                                st.success("タスクを開始したある！")
                                st.rerun()

                    elif t_status == "作業中":
                        if t_user == st.session_state.user:
                            with btn_cols[1]:
                                if st.button("完了する", key=f"resident_urgent_done_{t_id}", use_container_width=True):
                                    complete_task(t_id)
                                    st.success("タスクを完了したある！")
                                    st.rerun()
                        else:
                            with btn_cols[2]:
                                st.caption(f"現在 {t_user} さんが対応中ある。")
        else:
            st.info("この利用者に連動した至急・重要タスクは今のところないある。")

        st.divider()
        st.markdown("### 基本情報")

        info_cols = st.columns(2)
        with info_cols[0]:
            st.write(f"**相談員**: {row.get('consultant', '')}")
            st.write(f"**相談員電話**: {row.get('consultant_phone', '')}")
            st.write(f"**ケースワーカー**: {row.get('caseworker', '')}")
            st.write(f"**ケースワーカー電話**: {row.get('caseworker_phone', '')}")
            st.write(f"**病院**: {row.get('hospital', '')}")
            st.write(f"**病院電話**: {row.get('hospital_phone', '')}")

        with info_cols[1]:
            st.write(f"**看護**: {row.get('nurse', '')}")
            st.write(f"**看護電話**: {row.get('nurse_phone', '')}")
            st.write(f"**介護**: {row.get('care', '')}")
            st.write(f"**介護電話**: {row.get('care_phone', '')}")
            st.write(f"**登録日**: {row.get('created_at', '')}")
            st.write(f"**更新日**: {row.get('updated_at', '')}")

        st.divider()
        st.markdown("### 週間予定")

        schedule_view = schedule_df[schedule_df["resident_id"].astype(str) == selected_id].copy()
        schedule_view = schedule_view[
            schedule_view["service_type"].astype(str).isin(["病院", "看護", "介護"])
        ].copy()

        if schedule_view.empty:
            st.info("週間予定はまだ登録されてないある。")
        else:
            render_resident_schedule_html(schedule_view)

        st.divider()
        st.markdown("### 共有メモ")

        notes_view = notes_df[notes_df["resident_id"].astype(str) == selected_id].copy()
        if not notes_view.empty:
            try:
                notes_view = notes_view.sort_values("date", ascending=False)
            except Exception:
                pass

            for _, note_row in notes_view.iterrows():
                with st.container(border=True):
                    st.write(f"**{note_row.get('date', '')}**  {note_row.get('user', '')}")
                    st.write(note_row.get("note", ""))
        else:
            st.info("共有メモはまだないある。")


        st.divider()
        st.markdown("### ➕ 関係者を登録する")

        with st.expander("新しい関係者を追加"):
            with st.form(f"resident_contact_add_form_{selected_id}"):
                form_col1, form_col2 = st.columns(2)

                with form_col1:
                    category1 = st.selectbox(
                        "大分類",
                        ["医療", "外部連携", "家族", "業者"],
                        key=f"contact_cat1_{selected_id}"
                    )

                    category2_map = {
                        "医療": ["主治医", "訪問看護", "薬局", "行政", "その他"],
                        "外部連携": ["ケアマネ", "相談員", "行政", "その他"],
                        "家族": ["家族", "成年後見人", "身元引受人", "その他"],
                        "業者": ["配食", "福祉用具", "修理", "その他"],
                    }

                    category2 = st.selectbox(
                        "分類",
                        category2_map.get(category1, ["その他"]),
                        key=f"contact_cat2_{selected_id}"
                    )

                    role = st.text_input(
                        "この利用者に対する役割",
                        value=category2,
                        key=f"contact_role_{selected_id}"
                    )

                    name = st.text_input("氏名", key=f"contact_name_{selected_id}")

                with form_col2:
                    organization = st.text_input("事業所名", key=f"contact_org_{selected_id}")
                    phone = st.text_input("電話番号", key=f"contact_phone_{selected_id}")
                    memo = st.text_area("メモ", key=f"contact_memo_{selected_id}")

                save_contact = st.form_submit_button("関係者を登録する", use_container_width=True)

                if save_contact:
                    if name.strip() or organization.strip():
                        contacts_df = get_external_contacts_df()
                        links_df = get_resident_links_df()

                        next_contact_id = get_next_contact_id(contacts_df)
                        next_link_id = get_next_numeric_id(links_df, "id", 1)

                        new_contact_row = pd.DataFrame([{
                            "contact_id": next_contact_id,
                            "category1": category1,
                            "category2": category2,
                            "name": name.strip(),
                            "organization": organization.strip(),
                            "phone": phone.strip(),
                            "memo": memo.strip(),
                        }])

                        new_contacts_df = pd.concat([contacts_df, new_contact_row], ignore_index=True)
                        save_db(new_contacts_df, "external_contacts")

                        new_link_row = pd.DataFrame([{
                            "id": next_link_id,
                            "resident_id": selected_id,
                            "contact_id": next_contact_id,
                            "role": role.strip(),
                        }])

                        new_links_df = pd.concat([links_df, new_link_row], ignore_index=True)
                        save_db(new_links_df, "resident_links")

                        st.success("関係者を登録したある！")
                        st.rerun()
                    else:
                        st.error("氏名か事業所名のどちらかは入れてほしいある。")


        st.divider()
        st.markdown("### 編集・追加")

        edit_cols = st.columns(3)
        with edit_cols[0]:
            if st.button("基本情報を編集", key=f"edit_basic_{selected_id}", use_container_width=True):
                st.session_state.edit_resident_basic = True
                st.session_state.edit_resident_schedule = False
                st.session_state.edit_resident_note = False
                st.rerun()

        with edit_cols[1]:
            if st.button("予定を追加", key=f"edit_schedule_{selected_id}", use_container_width=True):
                st.session_state.edit_resident_basic = False
                st.session_state.edit_resident_schedule = True
                st.session_state.edit_resident_note = False
                st.rerun()

        with edit_cols[2]:
            if st.button("メモを追加", key=f"edit_note_{selected_id}", use_container_width=True):
                st.session_state.edit_resident_basic = False
                st.session_state.edit_resident_schedule = False
                st.session_state.edit_resident_note = True
                st.rerun()

        # ------------------------------------------
        # 基本情報編集
        # ------------------------------------------
        if st.session_state.get("edit_resident_basic", False):
            st.divider()
            st.markdown("#### 基本情報を編集")

            with st.form(f"resident_basic_form_{selected_id}"):
                resident_name = st.text_input("利用者名", value=str(row.get("resident_name", "")))
                status = st.selectbox(
                    "状態",
                    ["利用中", "退所"],
                    index=0 if str(row.get("status", "")).strip() == "利用中" else 1
                )
                consultant = st.text_input("相談員", value=str(row.get("consultant", "")))
                consultant_phone = st.text_input("相談員電話", value=str(row.get("consultant_phone", "")))
                caseworker = st.text_input("ケースワーカー", value=str(row.get("caseworker", "")))
                caseworker_phone = st.text_input("ケースワーカー電話", value=str(row.get("caseworker_phone", "")))
                hospital = st.text_input("病院", value=str(row.get("hospital", "")))
                hospital_phone = st.text_input("病院電話", value=str(row.get("hospital_phone", "")))
                nurse = st.text_input("看護", value=str(row.get("nurse", "")))
                nurse_phone = st.text_input("看護電話", value=str(row.get("nurse_phone", "")))
                care = st.text_input("介護", value=str(row.get("care", "")))
                care_phone = st.text_input("介護電話", value=str(row.get("care_phone", "")))

                save_col1, save_col2 = st.columns(2)
                with save_col1:
                    save_basic = st.form_submit_button("基本情報を保存する", use_container_width=True)
                with save_col2:
                    cancel_basic = st.form_submit_button("キャンセル", use_container_width=True)

                if save_basic:
                    update_df = master_df.copy()
                    now_str = now_jst().strftime("%Y-%m-%d %H:%M")

                    target_mask = update_df["resident_id"].astype(str) == selected_id
                    update_df.loc[target_mask, "resident_name"] = resident_name.strip()
                    update_df.loc[target_mask, "status"] = status
                    update_df.loc[target_mask, "consultant"] = consultant.strip()
                    update_df.loc[target_mask, "consultant_phone"] = consultant_phone.strip()
                    update_df.loc[target_mask, "caseworker"] = caseworker.strip()
                    update_df.loc[target_mask, "caseworker_phone"] = caseworker_phone.strip()
                    update_df.loc[target_mask, "hospital"] = hospital.strip()
                    update_df.loc[target_mask, "hospital_phone"] = hospital_phone.strip()
                    update_df.loc[target_mask, "nurse"] = nurse.strip()
                    update_df.loc[target_mask, "nurse_phone"] = nurse_phone.strip()
                    update_df.loc[target_mask, "care"] = care.strip()
                    update_df.loc[target_mask, "care_phone"] = care_phone.strip()
                    update_df.loc[target_mask, "updated_at"] = now_str

                    save_db(update_df, "resident_master")
                    st.session_state.edit_resident_basic = False
                    st.success("基本情報を保存したある！")
                    st.rerun()

                if cancel_basic:
                    st.session_state.edit_resident_basic = False
                    st.rerun()

        # ------------------------------------------
        # 週間予定編集
        # ------------------------------------------
        if st.session_state.get("edit_resident_schedule", False):
            st.divider()
            st.markdown("#### 病院・看護・介護の週間予定を編集")
            st.caption("同じ曜日に2回以上ある場合は、同じ曜日の下の2つ目・3つ目・4つ目にもそのまま入力してほしいある。Enterは不要ある。")

            schedule_base = build_schedule_form_base(schedule_df, selected_id)
            weekdays = ["月", "火", "水", "木", "金", "土", "日"]
            service_types = ["病院", "看護", "介護"]

            color_map = {
                "病院": "#F8E7A1",
                "看護": "#CFEAF6",
                "介護": "#DDEDB7",
            }

            slot_placeholders = [
                "例 10:00〜11:00",
                "2つ目があれば入力",
                "3つ目があれば入力",
                "4つ目があれば入力",
            ]

            with st.form(f"resident_schedule_form_{selected_id}"):
                weekly_inputs = {}

                for service in service_types:
                    st.markdown(
                        f"""
                        <div style="
                            background:{color_map[service]};
                            border:2px solid #111;
                            padding:8px 16px;
                            font-weight:700;
                            display:inline-block;
                            min-width:120px;
                            text-align:center;
                            margin-top:8px;
                            margin-bottom:10px;
                        ">{service}</div>
                        """,
                        unsafe_allow_html=True
                    )

                    top_cols = st.columns([3, 3, 3])
                    with top_cols[0]:
                        place_val = st.text_input(
                            f"{service}名",
                            value=schedule_base[service]["place"],
                            key=f"{selected_id}_{service}_place"
                        )
                    with top_cols[1]:
                        phone_val = st.text_input(
                            f"{service}電話",
                            value=schedule_base[service]["phone"],
                            key=f"{selected_id}_{service}_phone"
                        )
                    with top_cols[2]:
                        person_val = st.text_input(
                            f"{service}担当",
                            value=schedule_base[service]["person_in_charge"],
                            key=f"{selected_id}_{service}_person"
                        )

                    st.markdown(
                        """
                        <style>
                        .weekly-head {
                            text-align:center;
                            font-weight:700;
                            padding:6px 0;
                            border:2px solid #111;
                            background:#fafafa;
                            margin-bottom:4px;
                        }
                        </style>
                        """,
                        unsafe_allow_html=True
                    )

                    day_values = {}
                    day_cols = st.columns(7)

                    for i, wd in enumerate(weekdays):
                        with day_cols[i]:
                            st.markdown(f'<div class="weekly-head">{wd}</div>', unsafe_allow_html=True)

                            slots = []
                            for slot_idx in range(4):
                                slot_val = schedule_base[service]["days"][wd][slot_idx]
                                new_val = st.text_input(
                                    f"{wd}{slot_idx+1}枠",
                                    value=slot_val,
                                    key=f"{selected_id}_{service}_{wd}_{slot_idx+1}",
                                    label_visibility="collapsed",
                                    placeholder=slot_placeholders[slot_idx]
                                )
                                slots.append(new_val)

                            day_values[wd] = slots

                    weekly_inputs[service] = {
                        "place": str(place_val).strip(),
                        "phone": str(phone_val).strip(),
                        "person_in_charge": str(person_val).strip(),
                        "days": day_values
                    }

                    st.markdown("<br>", unsafe_allow_html=True)

                save_col1, save_col2 = st.columns(2)
                with save_col1:
                    save_weekly = st.form_submit_button("週間予定を保存する", use_container_width=True)
                with save_col2:
                    cancel_weekly = st.form_submit_button("キャンセル", use_container_width=True)

                if save_weekly:
                    keep_df = schedule_df[
                        ~(
                            (schedule_df["resident_id"].astype(str) == selected_id) &
                            (schedule_df["service_type"].astype(str).isin(["病院", "看護", "介護"]))
                        )
                    ].copy()

                    next_id = get_next_numeric_id(schedule_df, "id", 1)
                    new_rows = []

                    for service_type, data in weekly_inputs.items():
                        place_name = data["place"]
                        phone_text = data["phone"]
                        person_text = data["person_in_charge"]

                        for wd, slot_list in data["days"].items():
                            for slot_index, time_range in enumerate(slot_list, start=1):
                                start_time, end_time = parse_time_range(time_range)
                                if not start_time and not end_time:
                                    continue

                                new_rows.append({
                                    "id": next_id,
                                    "resident_id": selected_id,
                                    "weekday": wd,
                                    "service_type": service_type,
                                    "start_time": start_time,
                                    "end_time": end_time,
                                    "place": place_name,
                                    "phone": phone_text,
                                    "person_in_charge": person_text,
                                    "memo": f"slot:{slot_index}"
                                })
                                next_id += 1

                    if new_rows:
                        add_df = pd.DataFrame(new_rows)
                        save_df = pd.concat([keep_df, add_df], ignore_index=True)
                    else:
                        save_df = keep_df.copy()

                    save_df = save_df.fillna("")
                    save_db(save_df, "resident_schedule")
                    st.session_state.edit_resident_schedule = False
                    st.success("週間予定を保存したある！")
                    st.rerun()

                if cancel_weekly:
                    st.session_state.edit_resident_schedule = False
                    st.rerun()

            st.markdown("#### 現在の週間予定")

            current_view = schedule_df[schedule_df["resident_id"].astype(str) == selected_id].copy()
            current_view = current_view[
                current_view["service_type"].astype(str).isin(["病院", "看護", "介護"])
            ].copy()

            if current_view.empty:
                st.info("週間予定はまだ登録されてないある。")
            else:
                render_resident_schedule_html(current_view)

            delete_target = schedule_df[
                (schedule_df["resident_id"].astype(str) == selected_id) &
                (schedule_df["service_type"].astype(str).isin(["病院", "看護", "介護"]))
            ].copy()

            if not delete_target.empty:
                if st.button("週間予定をすべて削除", key=f"delete_weekly_schedule_{selected_id}", use_container_width=True):
                    new_schedule_df = schedule_df[
                        ~(
                            (schedule_df["resident_id"].astype(str) == selected_id) &
                            (schedule_df["service_type"].astype(str).isin(["病院", "看護", "介護"]))
                        )
                    ].copy()
                    save_db(new_schedule_df, "resident_schedule")
                    st.success("週間予定を削除したある。")
                    st.rerun()

        # ------------------------------------------
        # メモ追加
        # ------------------------------------------
        if st.session_state.get("edit_resident_note", False):
            st.divider()
            st.markdown("#### 共有メモを追加")

            with st.form(f"resident_note_form_{selected_id}"):
                note_date = st.date_input("日付", value=now_jst().date())
                note_text = st.text_area("共有メモ")

                save_col1, save_col2 = st.columns(2)
                with save_col1:
                    add_note = st.form_submit_button("メモを追加する", use_container_width=True)
                with save_col2:
                    cancel_note = st.form_submit_button("キャンセル", use_container_width=True)

                if add_note:
                    if note_text.strip():
                        next_id = get_next_numeric_id(notes_df, "id", 1)
                        new_row = pd.DataFrame([{
                            "id": next_id,
                            "resident_id": selected_id,
                            "date": str(note_date),
                            "user": str(st.session_state.get("user", "")),
                            "note": note_text.strip()
                        }])

                        new_notes_df = pd.concat([notes_df, new_row], ignore_index=True)
                        save_db(new_notes_df, "resident_notes")
                        st.session_state.edit_resident_note = False
                        st.success("共有メモを追加したある！")
                        st.rerun()
                    else:
                        st.error("メモ内容を入力してほしいある。")

                if cancel_note:
                    st.session_state.edit_resident_note = False
                    st.rerun()

            notes_delete_df = notes_df[
                notes_df["resident_id"].astype(str) == selected_id
            ].copy()

            if not notes_delete_df.empty:
                try:
                    notes_delete_df = notes_delete_df.sort_values("date", ascending=False)
                except Exception:
                    pass

                st.caption("登録済みメモを削除する場合は下から選ぶある。")

                for _, nrow in notes_delete_df.iterrows():
                    nid = str(nrow.get("id", "")).strip()

                    with st.container(border=True):
                        st.write(f"**{nrow.get('date', '')}**  {nrow.get('user', '')}")
                        st.write(nrow.get("note", ""))

                        if st.button(
                            "このメモを削除",
                            key=f"delete_note_{selected_id}_{nid}",
                            use_container_width=True
                        ):
                            latest_notes_df = get_resident_notes_df()
                            new_notes_df = latest_notes_df[
                                latest_notes_df["id"].astype(str) != nid
                            ].copy()

                            save_db(new_notes_df, "resident_notes")
                            st.success("メモを削除したある。")
                            st.rerun()

                        if st.button(
                            "この紐づきを削除",
                            key=f"delete_contact_link_{selected_id}_{contact_id}",
                            use_container_width=True
                        ):
                            links_df = get_resident_links_df()
                            new_links_df = links_df[
                                ~(
                                    (links_df["resident_id"].astype(str) == str(selected_id)) &
                                    (links_df["contact_id"].astype(str) == str(contact_id))
                                )
                            ].copy()
                            save_db(new_links_df, "resident_links")
                            st.success("この利用者との紐づきを削除したある。")
                            st.rerun()

    show_resident_page()

elif page == "⓪ 検索":

    st.title("🔍 検索")
    st.write("利用者・関係者・書類をまとめて探せるページある。")

    st.markdown("### 📁 書類検索")

    CATEGORY1_OPTIONS = [
        "全部",
        "利用者関連",
        "運営関連",
        "外部連携",
        "その他",
    ]

    CATEGORY2_MAP = {
        "全部": ["全部"],
        "利用者関連": [
            "全部",
            "個別支援計画案",
            "サービス担当者会議",
            "個別支援計画",
            "モニタリング",
            "在宅評価シート",
            "アセスメント",
            "その他",
        ],
        "運営関連": [
            "全部",
            "マニュアル",
            "帳票",
            "研修",
            "行政提出",
            "その他",
        ],
        "外部連携": [
            "全部",
            "病院",
            "訪問看護",
            "ケアマネ",
            "薬局",
            "行政",
            "その他",
        ],
        "その他": [
            "全部",
            "その他",
        ],
    }

    doc_df = load_db("document_master")

    if doc_df is None or doc_df.empty:
        doc_df = pd.DataFrame(columns=[
            "document_id", "category1", "category2", "category3",
            "title", "file_type", "url", "summary", "memo",
            "status", "updated_at", "created_at"
        ])
    else:
        for col in [
            "document_id", "category1", "category2", "category3",
            "title", "file_type", "url", "summary", "memo",
            "status", "updated_at", "created_at"
        ]:
            if col not in doc_df.columns:
                doc_df[col] = ""

    doc_df = doc_df.fillna("").copy()

    search_cols = st.columns([2, 2, 2, 3])

    with search_cols[0]:
        cat1 = st.selectbox(
            "カテゴリ1",
            CATEGORY1_OPTIONS,
            key="doc_search_cat1"
        )

    with search_cols[1]:
        cat2_options = CATEGORY2_MAP.get(cat1, ["全部"])
        cat2 = st.selectbox(
            "カテゴリ2",
            cat2_options,
            key="doc_search_cat2"
        )

    with search_cols[2]:
        status_candidates = ["全部"]
        if not doc_df.empty:
            status_values = sorted([x for x in doc_df["status"].astype(str).unique().tolist() if str(x).strip()])
            status_candidates += status_values
        status_filter = st.selectbox(
            "状態",
            status_candidates,
            key="doc_search_status"
        )

    with search_cols[3]:
        kw = st.text_input(
            "キーワード",
            key="doc_search_kw",
            placeholder="タイトル・概要・メモで検索"
        )

    view_df = doc_df.copy()

    if cat1 != "全部":
        view_df = view_df[view_df["category1"].astype(str) == cat1]

    if cat2 != "全部":
        view_df = view_df[view_df["category2"].astype(str) == cat2]

    if status_filter != "全部":
        view_df = view_df[view_df["status"].astype(str) == status_filter]

    if kw.strip():
        kw_l = kw.strip().lower()
        view_df = view_df[
            view_df.apply(
                lambda row:
                    kw_l in str(row.get("title", "")).lower()
                    or kw_l in str(row.get("summary", "")).lower()
                    or kw_l in str(row.get("memo", "")).lower()
                    or kw_l in str(row.get("category1", "")).lower()
                    or kw_l in str(row.get("category2", "")).lower()
                    or kw_l in str(row.get("category3", "")).lower(),
                axis=1
            )
        ]

    if view_df.empty:
        st.info("該当する資料はありません。")
    else:
        try:
            view_df = view_df.sort_values("updated_at", ascending=False)
        except Exception:
            pass

        st.caption(f"{len(view_df)}件見つかったある。")

        for _, row in view_df.iterrows():
            document_id = str(row.get("document_id", "")).strip()
            category1 = str(row.get("category1", "")).strip()
            category2 = str(row.get("category2", "")).strip()
            category3 = str(row.get("category3", "")).strip()
            title = str(row.get("title", "")).strip()
            file_type = str(row.get("file_type", "")).strip()
            url = str(row.get("url", "")).strip()
            summary = str(row.get("summary", "")).strip()
            memo = str(row.get("memo", "")).strip()
            status = str(row.get("status", "")).strip()
            updated_at = str(row.get("updated_at", "")).strip()

            with st.container(border=True):
                st.markdown(
                    f"""
                    <div style="
                        border-left: 6px solid #c7ced6;
                        background:#ffffff;
                        padding:12px 14px;
                        border-radius:10px;
                        margin-bottom:10px;
                    ">
                        <div style="font-size:18px; font-weight:700; margin-bottom:6px;">
                            {title if title else '無題資料'}
                        </div>
                        <div style="line-height:1.8;">
                            <b>ID:</b> {document_id}<br>
                            <b>分類:</b> {category1} / {category2} / {category3}<br>
                            <b>種類:</b> {file_type}<br>
                            <b>状態:</b> {status}<br>
                            <b>更新日:</b> {updated_at}
                        </div>
                        <div style="margin-top:8px;">
                            <b>概要:</b> {summary}
                        </div>
                        <div style="margin-top:8px; color:#555;">
                            {memo}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                file_bytes, filename, mime = get_download_file_data(row)
                if file_bytes:
                    st.download_button(
                        "ダウンロード",
                        data=file_bytes,
                        file_name=filename,
                        mime=mime,
                        key=f"download_doc_{document_id}",
                        use_container_width=True
                    )

                if url:
                    st.link_button("資料を開く", url, use_container_width=True)

    st.divider()

    # ------------------------------------------
    # 関係者検索
    # ------------------------------------------
    st.markdown("### ☎️ 関係者検索")

    contacts_df = get_external_contacts_df()
    links_df = get_resident_links_df()
    master_df = get_resident_master_df()

    if contacts_df.empty:
        st.info("関係者データはまだ登録されてないある。")
    else:
        contact_kw = st.text_input(
            "関係者キーワード",
            key="contact_search_kw",
            placeholder="氏名・所属・電話番号など"
        )

        contact_view_df = contacts_df.copy()

        if contact_kw.strip():
            kw2 = contact_kw.strip().lower()
            contact_view_df = contact_view_df[
                contact_view_df.apply(
                    lambda row:
                        kw2 in str(row.get("name", "")).lower()
                        or kw2 in str(row.get("organization", "")).lower()
                        or kw2 in str(row.get("phone", "")).lower()
                        or kw2 in str(row.get("memo", "")).lower()
                        or kw2 in str(row.get("category1", "")).lower()
                        or kw2 in str(row.get("category2", "")).lower(),
                    axis=1
                )
            ]

        if contact_view_df.empty:
            st.info("該当する関係者はありません。")
        else:
            for _, row in contact_view_df.iterrows():
                contact_id = str(row.get("contact_id", "")).strip()
                name = str(row.get("name", "")).strip()
                organization = str(row.get("organization", "")).strip()
                phone = str(row.get("phone", "")).strip()
                memo = str(row.get("memo", "")).strip()
                category1 = str(row.get("category1", "")).strip()
                category2 = str(row.get("category2", "")).strip()

                linked_names = []
                if not links_df.empty and not master_df.empty:
                    target_links = links_df[links_df["contact_id"].astype(str) == contact_id]
                    for _, lrow in target_links.iterrows():
                        rid = str(lrow.get("resident_id", "")).strip()
                        hit = master_df[master_df["resident_id"].astype(str) == rid]
                        if not hit.empty:
                            linked_name = str(hit.iloc[0].get("resident_name", "")).strip()
                            if linked_name:
                                linked_names.append(linked_name)

                with st.container(border=True):
                    st.markdown(f"**{name if name else '名称未設定'}**")
                    st.write(f"{organization} / {category1} / {category2}")
                    if phone:
                        st.write(f"電話: {phone}")
                    if memo:
                        st.write(f"メモ: {memo}")
                    if linked_names:
                        st.write("担当利用者: " + "、".join(linked_names))

elif page == "⑩ 書類アップロード":

    st.title("📤 書類アップロード")
    st.write("書類を登録するページある。検索は⓪ 検索から行ってほしいある。")

    with st.expander("＋ 書類を登録", expanded=True):
        with st.form("document_upload_form", clear_on_submit=True):

            category1 = st.text_input("カテゴリ1")
            category2 = st.text_input("カテゴリ2")
            category3 = st.text_input("カテゴリ3")

            title = st.text_input("タイトル")
            summary = st.text_area("概要")
            memo = st.text_area("メモ")

            uploaded_file = st.file_uploader(
                "ファイル",
                type=["xlsx", "xls", "pdf", "docx", "doc"]
            )

            submitted = st.form_submit_button("登録")

            if submitted:
                if not uploaded_file:
                    st.error("ファイルを選択してください")
                elif not title.strip():
                    st.error("タイトルを入力してください")
                else:
                    save_uploaded_document(
                        category1,
                        category2,
                        category3,
                        title,
                        summary,
                        memo,
                        uploaded_file
                    )
                    st.success("書類を登録しました")
                    st.rerun()

    st.divider()

    if False:
        st.markdown("### 書類検索")

        doc_df = get_document_master_df()
        keyword = st.text_input("キーワード")

        if not doc_df.empty:
            result_df = doc_df.copy()

            if keyword.strip():
                kw = keyword.lower()

                result_df = result_df[
                    result_df.apply(
                        lambda row:
                            kw in str(row.get("title", "")).lower()
                            or kw in str(row.get("category1", "")).lower()
                            or kw in str(row.get("category2", "")).lower()
                            or kw in str(row.get("category3", "")).lower()
                            or kw in str(row.get("summary", "")).lower()
                            or kw in str(row.get("memo", "")).lower(),
                        axis=1
                    )
                ]

            if result_df.empty:
                st.info("該当する書類はありません")
            else:
                result_df = result_df.sort_values("updated_at", ascending=False)

                for _, row in result_df.iterrows():
                    st.write(row.get("title", ""))

                title = row["title"]
                cat1 = row["category1"]
                cat2 = row["category2"]
                cat3 = row["category3"]

                st.markdown(f"### {title}")
                st.caption(f"{cat1} / {cat2} / {cat3}")

                file_bytes, filename, mime = get_download_file_data(row)

                if file_bytes:

                    st.download_button(
                        label="ダウンロード",
                        data=file_bytes,
                        file_name=filename,
                        mime=mime,
                        key=f"doc_{row['document_id']}"
                    )

                st.divider()
                            

elif page == "⓪ 検索":

    st.title("🔍 検索")
    st.write("利用者・関係者・資料をまとめて探せるページある。")
    
    # ------------------------------------------
    # 簡易キーワード検索
    # ------------------------------------------
    st.markdown("## 書類検索")

    keyword = st.text_input("キーワード")

    # ------------------------------------------
    # 関係者検索
    # ------------------------------------------
    st.markdown("## ☎️ 関係者検索")

    contacts_df = get_external_contacts_df()
    links_df = get_resident_links_df()
    master_df = get_resident_master_df()

    if contacts_df.empty:
        st.info("関係者データはまだ登録されてないある。")
    else:
        for col in ["contact_id", "category1", "category2", "name", "organization", "phone", "memo"]:
            if col not in contacts_df.columns:
                contacts_df[col] = ""

        contacts_df = contacts_df.fillna("").copy()

        search_cols = st.columns([2, 2, 3])

        with search_cols[0]:
            contact_cat1 = st.selectbox(
                "大分類で絞る",
                ["全部", "医療", "外部連携", "家族", "業者"],
                key="contact_search_cat1"
            )

        with search_cols[1]:
            contact_cat2 = st.selectbox(
                "小分類で絞る",
                ["全部", "病院", "訪問看護", "ケアマネ", "相談員", "家族", "その他"],
                key="contact_search_cat2"
            )

        with search_cols[2]:
            contact_kw = st.text_input(
                "関係者キーワード",
                key="contact_search_kw",
                placeholder="氏名・所属・電話番号など"
            )

        contact_view_df = contacts_df.copy()

        if contact_cat1 != "全部":
            contact_view_df = contact_view_df[
                contact_view_df["category1"].astype(str) == contact_cat1
            ]

        if contact_cat2 != "全部":
            contact_view_df = contact_view_df[
                contact_view_df["category2"].astype(str) == contact_cat2
            ]

        if contact_kw.strip():
            kw2 = contact_kw.strip().lower()
            contact_view_df = contact_view_df[
                contact_view_df.apply(
                    lambda row:
                        kw2 in str(row.get("name", "")).lower()
                        or kw2 in str(row.get("organization", "")).lower()
                        or kw2 in str(row.get("phone", "")).lower()
                        or kw2 in str(row.get("memo", "")).lower()
                        or kw2 in str(row.get("category1", "")).lower()
                        or kw2 in str(row.get("category2", "")).lower(),
                    axis=1
                )
            ]

        if contact_view_df.empty:
            st.info("該当する関係者はありません。")
        else:
            for _, row in contact_view_df.iterrows():
                contact_id = str(row.get("contact_id", "")).strip()
                name = str(row.get("name", "")).strip()
                organization = str(row.get("organization", "")).strip()
                phone = str(row.get("phone", "")).strip()
                memo = str(row.get("memo", "")).strip()
                category1 = str(row.get("category1", "")).strip()
                category2 = str(row.get("category2", "")).strip()

                linked_names = []
                if not links_df.empty and not master_df.empty:
                    target_links = links_df[links_df["contact_id"].astype(str) == contact_id]
                    for _, lrow in target_links.iterrows():
                        rid = str(lrow.get("resident_id", "")).strip()
                        hit = master_df[master_df["resident_id"].astype(str) == rid]
                        if not hit.empty:
                            linked_name = str(hit.iloc[0].get("resident_name", "")).strip()
                            if linked_name:
                                linked_names.append(linked_name)

                with st.container(border=True):
                    st.markdown(f"**{name if name else '名称未設定'}**")
                    st.write(f"{organization} / {category1} / {category2}")
                    if phone:
                        st.write(f"電話: {phone}")
                    if memo:
                        st.write(f"メモ: {memo}")
                    if linked_names:
                        st.write("担当利用者: " + "、".join(linked_names))

    st.divider()

    # ------------------------------------------
    # 資料検索
    # ------------------------------------------
    st.markdown("## 📁 資料検索")

    CATEGORY1_OPTIONS = [
        "全部",
        "利用者関連",
        "運営関連",
        "外部連携",
        "その他",
    ]

    CATEGORY2_MAP = {
        "全部": ["全部"],
        "利用者関連": [
            "全部",
            "個別支援計画案",
            "サービス担当者会議",
            "個別支援計画",
            "モニタリング",
            "在宅評価シート",
            "アセスメント",
            "その他",
        ],
        "運営関連": [
            "全部",
            "マニュアル",
            "帳票",
            "研修",
            "行政提出",
            "その他",
        ],
        "外部連携": [
            "全部",
            "病院",
            "訪問看護",
            "ケアマネ",
            "薬局",
            "行政",
            "その他",
        ],
        "その他": [
            "全部",
            "その他",
        ],
    }

    doc_df = load_db("document_master")

    if doc_df is None or doc_df.empty:
        doc_df = pd.DataFrame(columns=[
            "document_id", "category1", "category2", "category3",
            "title", "file_type", "url", "summary", "memo",
            "status", "updated_at", "created_at"
        ])
    else:
        for col in [
            "document_id", "category1", "category2", "category3",
            "title", "file_type", "url", "summary", "memo",
            "status", "updated_at", "created_at"
        ]:
            if col not in doc_df.columns:
                doc_df[col] = ""

    doc_df = doc_df.fillna("").copy()

    search_cols = st.columns([2, 2, 2, 3])

    with search_cols[0]:
        cat1 = st.selectbox("カテゴリ1", CATEGORY1_OPTIONS, key="doc_search_cat1")

    with search_cols[1]:
        cat2_options = CATEGORY2_MAP.get(cat1, ["全部"])
        cat2 = st.selectbox("カテゴリ2", cat2_options, key="doc_search_cat2")

    with search_cols[2]:
        status_candidates = ["全部"]
        if not doc_df.empty:
            status_values = sorted([x for x in doc_df["status"].astype(str).unique().tolist() if str(x).strip()])
            status_candidates += status_values
        status_filter = st.selectbox("状態", status_candidates, key="doc_search_status")

    with search_cols[3]:
        kw = st.text_input(
            "キーワード",
            key="doc_search_kw",
            placeholder="タイトル・概要・メモで検索"
        )

    view_df = doc_df.copy()

    if cat1 != "全部":
        view_df = view_df[view_df["category1"].astype(str) == cat1]

    if cat2 != "全部":
        view_df = view_df[view_df["category2"].astype(str) == cat2]

    if status_filter != "全部":
        view_df = view_df[view_df["status"].astype(str) == status_filter]

    if kw.strip():
        kw_l = kw.strip().lower()
        view_df = view_df[
            view_df.apply(
                lambda row:
                    kw_l in str(row.get("title", "")).lower()
                    or kw_l in str(row.get("summary", "")).lower()
                    or kw_l in str(row.get("memo", "")).lower()
                    or kw_l in str(row.get("category1", "")).lower()
                    or kw_l in str(row.get("category2", "")).lower()
                    or kw_l in str(row.get("category3", "")).lower(),
                axis=1
            )
        ]

    if view_df.empty:
        st.info("条件に合う資料はないある。")
    else:
        try:
            view_df = view_df.sort_values("updated_at", ascending=False)
        except Exception:
            pass

        for _, row in view_df.iterrows():
            document_id = str(row.get("document_id", "")).strip()
            category1 = str(row.get("category1", "")).strip()
            category2 = str(row.get("category2", "")).strip()
            category3 = str(row.get("category3", "")).strip()
            title = str(row.get("title", "")).strip()
            file_type = str(row.get("file_type", "")).strip()
            summary = str(row.get("summary", "")).strip()
            memo = str(row.get("memo", "")).strip()
            status = str(row.get("status", "")).strip()

            with st.container(border=True):
                st.markdown(f"**{title if title else '無題資料'}**")
                st.write(f"{category1} / {category2} / {category3}")
                st.write(f"種類: {file_type} / 状態: {status}")
                if summary:
                    st.write(f"概要: {summary}")
                if memo:
                    st.write(f"メモ: {memo}")

                file_bytes, filename, mime = get_download_file_data(row)
                if file_bytes:
                    st.download_button(
                        "ダウンロード",
                        data=file_bytes,
                        file_name=filename,
                        mime=mime,
                        key=f"download_doc_{document_id}",
                        use_container_width=True
                    )


def render_secret_generation_panel(doc_title: str):
    st.info("🤫 秘密モードある。ここに後で『新しい方針欄』『過去のデータから作成』『Gemini自動入力』を追加していくある。")

def render_secret_page(doc_title: str):
    st.title(f"🤫 {doc_title}")
    render_secret_generation_panel(doc_title)
    st.divider()
    if doc_title == "サービス担当者会議":
        render_meeting_form_page(doc_title)
    elif doc_title == "モニタリング":
        render_monitoring_form_page(doc_title)
    else:
        render_plan_form_page(doc_title)

# ==========================================
# 利用者書類
# ==========================================

if page == "書類_個別支援計画案":
    if st.session_state.get("secret_doc_mode", False):
        render_secret_page("個別支援計画案")
    else:
        render_plan_form_page("個別支援計画案")
elif page == "書類_サービス担当者会議":
    if st.session_state.get("secret_doc_mode", False):
        render_secret_page("サービス担当者会議")
    else:
        render_meeting_form_page("サービス担当者会議")
elif page == "書類_個別支援計画":
    if st.session_state.get("secret_doc_mode", False):
        render_secret_page("個別支援計画")
    else:
        render_plan_form_page("個別支援計画")
elif page == "書類_モニタリング":
    if st.session_state.get("secret_doc_mode", False):
        render_secret_page("モニタリング")
    else:
        render_monitoring_form_page("モニタリング")
elif page == "書類_在宅評価シート":
    render_home_evaluation_form_page("在宅評価シート")
elif page == "書類_アセスメント":
    render_assessment_form_page("アセスメント")
elif page == "書類_基本シート":
    render_basic_sheet_form_page("基本シート")
elif page == "書類_就労分野シート":
    render_work_field_form_page("就労分野シート")
elif page == "🐝knowbe日誌入力🐝":
    render_bee_journal_page()