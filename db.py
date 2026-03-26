import streamlit as st
import pandas as pd
import time
import random

from streamlit_gsheets import GSheetsConnection

# --- 接続 ---
conn = st.connection("gsheets", type=GSheetsConnection)

# --- シート定義 ---
COMMON_SHEETS = {
    "active_users",
    "companies",
    "users",
    "admin_logs",
}

COMPANY_SCOPED_SHEETS = {
    "task",
    "chat",
    "manual",
    "record_status",
    "calendar",
    "chat_rooms",
    "chat_messages",
    "warehouse_files",
    "archive_files",
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

# --- シート名取得 ---
def get_sheet_name(file):
    if file in COMMON_SHEETS:
        return file
    if file in COMPANY_SCOPED_SHEETS:
        return file
    raise ValueError(f"未対応のシートある: {file}")

# --- DB読込 ---
def load_db(file, retries=3, delay=0.8):
    sheet_name = get_sheet_name(file)

    last_error = None

    for attempt in range(retries):
        try:
            df = conn.read(worksheet=sheet_name, ttl=60)

            if df is None:
                df = pd.DataFrame()

            return df

        except Exception as e:
            last_error = e
            if attempt < retries - 1:
                time.sleep(delay + random.random() * 0.5)

    if last_error:
        raise last_error

    return pd.DataFrame()

# --- DB保存 ---
def save_db(df, file):
    sheet_name = get_sheet_name(file)

    try:
        conn.update(worksheet=sheet_name, data=df)
    except Exception as e:
        raise Exception(f"保存失敗ある: {file} / {e}")

# --- company_idフィルタ ---
def filter_by_company(df, company_id):
    if df is None or df.empty:
        return df

    if "company_id" not in df.columns:
        return df

    return df[df["company_id"].astype(str).str.strip() == str(company_id).strip()].copy()

# --- 共通DF取得 ---
@st.cache_data(ttl=60)
def get_df_cached(file):
    df = load_db(file)
    if df is None:
        df = pd.DataFrame()
    return df.fillna("")

def get_df(file):
    return get_df_cached(file).copy()

# --- users ---
def get_users_df():
    return get_df("users")

# --- companies ---
def get_companies_df():
    return get_df("companies")

# --- chat ---
def get_chat_rooms_df():
    return get_df("chat_rooms")

def get_chat_messages_df():
    return get_df("chat_messages")

# --- warehouse ---
def get_warehouse_files_df():
    return get_df("warehouse_files")

# --- archive ---
def get_archive_files_df():
    return get_df("archive_files")

# --- admin logs ---
def get_admin_logs_df():
    return get_df("admin_logs")