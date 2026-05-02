from common import get_sheet_name_candidates, normalize_company_scoped_df, filter_by_company_id
import time
import random
import pandas as pd
import streamlit as st
try:
    from streamlit_gsheets import GSheetsConnection
except Exception:
    GSheetsConnection = None

from supabase import create_client

import streamlit as st

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

conn = None
if GSheetsConnection is not None:
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
    except Exception:
        conn = None


SUPABASE_TABLES = {
    "users",
    "companies",
    "task",
    "attendance_logs",
    "attendance_display_settings",
    "ic_reader_bridge",
    "ic_card_users",
    "ic_attendance_logs",
    "ic_attendance_daily",
    "resident_master",
    "resident_schedule",
    "record_status",
    "saved_documents",
    "user_company_permissions",
    "outside_workplaces",
    "outside_work_tasks",
    "piecework_master",
    "piecework_steps",
}

@st.cache_data(ttl=60)
def load_db(file, retries=3, delay=0.8):

    if file in SUPABASE_TABLES:
        query = supabase.table(file).select("*")

        if file == "attendance_logs":
            query = query.order("timestamp", desc=True)
        elif file == "ic_attendance_logs":
            query = query.order("timestamp", desc=True)
        elif file == "ic_attendance_daily":
            query = query.order("date", desc=True)

        res = query.execute()
        return pd.DataFrame(res.data)

    sheet_candidates = get_sheet_name_candidates(file)
    last_error = None

    for s_name in sheet_candidates:
        for attempt in range(retries):
            try:
                df = conn.read(worksheet=s_name)

                if df is None:
                    df = pd.DataFrame()

                return df

            except Exception as e:
                last_error = e
                if attempt < retries - 1:
                    time.sleep(delay + random.random() * 0.5)

    if last_error is not None:
        raise last_error

    return pd.DataFrame()

def save_db(df, file, retries=3, delay=1.0):

    if df is None:
        df = pd.DataFrame()

    if file in SUPABASE_TABLES:
        work = df.copy()

        unique_keys = {
            "attendance_logs": "attendance_id",
            "ic_reader_bridge": "bridge_id",
            "ic_card_users": "card_id",
            "ic_attendance_logs": "log_id",
            "piecework_master": "id",
            "piecework_steps": "id",
            "outside_workplaces": "workplace_id",
            "outside_work_tasks": "task_id",
            "users": "user_id",
            "companies": "company_id",
            "resident_master": "resident_id",
            "resident_schedule": "id",
            "record_status": "id",
            "saved_documents": "id",
            "user_company_permissions": "permission_id",
            "task": "id",
            "attendance_display_settings": "setting_id",
        }

        key_col = unique_keys.get(file)
        if key_col and key_col in work.columns:
            work[key_col] = work[key_col].astype(str).str.strip()
            work = work[work[key_col] != ""].copy()
            work = work.drop_duplicates(subset=[key_col], keep="last").copy()

        work = work.where(pd.notnull(work), None)

        rows = work.to_dict(orient="records")

        for r in rows:
            for key in ["is_active", "can_use", "is_admin", "attendance_enabled"]:
                if key in r and r[key] == "":
                    r[key] = None

            for key in ["quantity_min", "quantity_max", "priority", "priority_num", "step_no"]:
                if key in r and r[key] == "":
                    r[key] = None

        if rows:
            supabase.table(file).upsert(rows).execute()

        st.cache_data.clear()
        return

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

    if last_error is not None:
        raise last_error

def get_companies_df_cached():
    df = load_db("companies")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "company_id",
            "company_name",
            "company_code",
            "company_login_id",
            "company_login_password",
            "knowbe_login_username",
            "knowbe_login_password",
            "status",
            "created_at",
            "updated_at",
            "memo",
        ])
    else:
        for col in [
            "company_id",
            "company_name",
            "company_code",
            "company_login_id",
            "company_login_password",
            "knowbe_login_username",
            "knowbe_login_password",
            "status",
            "created_at",
            "updated_at",
            "memo",
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_companies_df():
    return get_companies_df_cached()


def get_users_df_cached():
    df = load_db("users")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "user_id",
            "company_id",
            "user_login_id",
            "user_login_password",
            "display_name",
            "is_admin",
            "role_type",
            "login_card_id",
            "last_login_at",
            "status",
            "attendance_enabled",
            "display_order",
            "created_at",
            "updated_at",
            "memo",
        ])
    else:
        for col in [
            "user_id",
            "company_id",
            "user_login_id",
            "user_login_password",
            "display_name",
            "is_admin",
            "role_type",
            "login_card_id",
            "last_login_at",
            "status",
            "attendance_enabled",
            "display_order",
            "created_at",
            "updated_at",
            "memo",
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_users_df():
    return get_users_df_cached()


def get_user_company_permissions_df_cached():
    df = load_db("user_company_permissions")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "permission_id",
            "user_id",
            "company_id",
            "can_use",
            "is_admin",
            "status",
            "created_at",
            "updated_at",
            "memo",
        ])
    else:
        for col in [
            "permission_id",
            "user_id",
            "company_id",
            "can_use",
            "is_admin",
            "status",
            "created_at",
            "updated_at",
            "memo",
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_user_company_permissions_df():
    return get_user_company_permissions_df_cached()


def get_task_required_cols():
    return [
        "company_id",
        "id",
        "task",
        "status",
        "user",
        "limit",
        "priority",
        "updated_at",
    ]


def get_tasks_df(company_id):
    df = load_db("task")
    required_cols = get_task_required_cols()
    work = normalize_company_scoped_df(df, required_cols)
    return filter_by_company_id(work, company_id)


def get_urgent_tasks_df(company_id):
    df = get_tasks_df(company_id)
    required_cols = get_task_required_cols()
    df = normalize_company_scoped_df(df, required_cols)

    if df.empty:
        return pd.DataFrame(columns=required_cols)

    urgent_df = df[
        df["priority"].astype(str).str.strip().isin(["至急", "重要"]) &
        (df["status"].astype(str).str.strip() != "完了")
    ]

    if urgent_df.empty:
        return urgent_df

    return urgent_df


def get_resident_master_df(company_id):
    df = load_db("resident_master")

    required_cols = [
        "company_id",
        "resident_id",
        "resident_name",
        "status",
    ]

    work = normalize_company_scoped_df(df, required_cols)
    return filter_by_company_id(work, company_id)


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


def get_resident_schedule_df():
    return get_resident_schedule_df_cached()


def get_resident_notes_df_cached():
    df = load_db("resident_notes")
    if df is None or df.empty:
        df = pd.DataFrame(columns=["id", "resident_id", "date", "user", "note"])
    else:
        for col in ["id", "resident_id", "date", "user", "note"]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_resident_notes_df():
    return get_resident_notes_df_cached()


@st.cache_data(ttl=15)
def get_attendance_logs_df_cached():
    df = load_db("attendance_logs")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "attendance_id",
            "date",
            "user_id",
            "company_id",
            "action",
            "timestamp",
            "device_name",
            "recorded_by",
        ])
    else:
        for col in [
            "attendance_id",
            "date",
            "user_id",
            "company_id",
            "action",
            "timestamp",
            "device_name",
            "recorded_by",
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_attendance_logs_df():
    return get_attendance_logs_df_cached()


@st.cache_data(ttl=15)
def get_attendance_display_settings_df_cached():
    df = load_db("attendance_display_settings")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "setting_id",
            "group_id",
            "slot_no",
            "company_id",
            "status",
            "created_at",
            "registered_by",
        ])
    else:
        for col in [
            "setting_id",
            "group_id",
            "slot_no",
            "company_id",
            "status",
            "created_at",
            "registered_by",
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_attendance_display_settings_df():
    return get_attendance_display_settings_df_cached()


@st.cache_data(ttl=3)
def get_ic_reader_bridge_df_cached():
    df = load_db("ic_reader_bridge")
    cols = ["bridge_id", "device_name", "card_id", "touched_at", "status"]
    if df is None or df.empty:
        df = pd.DataFrame(columns=cols)
    else:
        for col in cols:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_ic_reader_bridge_df():
    return get_ic_reader_bridge_df_cached()


@st.cache_data(ttl=15)
def get_ic_card_users_df_cached():
    df = load_db("ic_card_users")
    cols = ["card_id", "user_id", "user_name", "company_id", "is_active", "note"]
    if df is None or df.empty:
        df = pd.DataFrame(columns=cols)
    else:
        for col in cols:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_ic_card_users_df():
    return get_ic_card_users_df_cached()


@st.cache_data(ttl=15)
def get_ic_attendance_logs_df_cached():
    df = load_db("ic_attendance_logs")
    cols = [
        "log_id", "date", "user_id", "user_name", "company_id",
        "action", "action_label", "timestamp", "device_name",
        "card_id", "source", "memo",
    ]
    if df is None or df.empty:
        df = pd.DataFrame(columns=cols)
    else:
        for col in cols:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_ic_attendance_logs_df():
    return get_ic_attendance_logs_df_cached()


@st.cache_data(ttl=15)
def get_ic_attendance_daily_df_cached():
    df = load_db("ic_attendance_daily")
    cols = [
        "date", "user_id", "user_name", "company_id",
        "clock_in", "break_start", "break_end", "clock_out",
        "break_minutes", "work_minutes", "status", "note",
    ]
    if df is None or df.empty:
        df = pd.DataFrame(columns=cols)
    else:
        for col in cols:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_ic_attendance_daily_df():
    return get_ic_attendance_daily_df_cached()
