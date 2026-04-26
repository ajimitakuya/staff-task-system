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

SUPABASE_URL = "https://qofabfhjorqeeyrlrnwv.supabase.co"
SUPABASE_KEY = "sb_publishable_G3oY4S2zu8piW0-wR5CNLQ_IaIrDekc"

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
    "resident_master",
    "resident_schedule",
    "record_status",
    "saved_documents",
    "user_company_permissions",
}

@st.cache_data(ttl=60)
def load_db(file, retries=3, delay=0.8):

    if file in SUPABASE_TABLES:
        query = supabase.table(file).select("*")

        if file == "attendance_logs":
            query = query.order("timestamp", desc=True)

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
        work = df.fillna("").copy()

        if file == "attendance_logs" and "attendance_id" in work.columns:
            work = work.drop_duplicates(subset=["attendance_id"], keep="last")

        rows = work.to_dict(orient="records")

        if rows:
            supabase.table(file).upsert(rows).execute()

        st.cache_data.clear()
        return

    # 👇 Sheets fallback
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