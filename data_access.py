from common import get_sheet_name_candidates, normalize_company_scoped_df, filter_by_company_id
import time
import random
import pandas as pd
import streamlit as st
from streamlit_gsheets import GSheetsConnection
from common import get_sheet_name_candidates

conn = st.connection("gsheets", type=GSheetsConnection)

def load_db(file, retries=3, delay=0.8):
    sheet_candidates = get_sheet_name_candidates(file)

    last_error = None

    for s_name in sheet_candidates:
        for attempt in range(retries):
            try:
                ttl_sec = 300
                if file == "attendance_logs":
                    ttl_sec = 15
                elif file == "attendance_display_settings":
                    ttl_sec = 30

                df = conn.read(worksheet=s_name, ttl=300)

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

                calendar_cols = ["company_id", "id", "title", "start", "end", "user", "memo", "source_type", "source_task_id"]

                expected_cols = {
                    "task": ["company_id", "id", "task", "status", "user", "limit", "priority", "updated_at"],
                    "chat": ["company_id", "date", "time", "user", "message", "image_data"],
                    "manual": ["company_id", "id", "title", "content", "image_data", "created_at"],
                    "record_status": ["company_id"] + record_status_cols,
                    "calendar": calendar_cols,
                    "active_users": ["user", "login_at", "last_seen"],
                    "resident_master": [
                        "company_id",
                        "resident_id", "resident_name", "status", "public_assistance", 
                        "disability_type",
                        "consultant", "consultant_phone",
                        "caseworker", "caseworker_phone",
                        "hospital", "hospital_phone",
                        "nurse", "nurse_phone",
                        "care", "care_phone",
                        "created_at", "updated_at"
                    ],
                    "resident_schedule": [
                        "company_id",
                        "id", "resident_id", "weekday", "service_type",
                        "start_time", "end_time", "place", "phone", "person_in_charge", "memo"
                    ],
                    "resident_notes": [
                        "company_id",
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
                        "record_mode", "skip_knowbe",
                    ],
                    "staff_examples": [
                        "company_id",
                        "staff_name",
                        "home_start_example", "home_end_example",
                        "day_start_example", "day_end_example",
                        "outside_start_example", "outside_end_example",
                        "updated_at"
                    ],
                    "personal_rules": [
                        "company_id",
                        "staff_name", "rule_text", "updated_at"
                    ],
                    "assistant_plans": [
                        "company_id",
                        "resident_id", "long_term_goal", "short_term_goal", "updated_at"
                    ],
                    "users": [
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
                        "created_at",
                        "updated_at",
                        "memo",
                    ],
                    "chat_rooms": [
                        "room_id",
                        "room_name",
                        "room_type",
                        "room_password",
                        "created_by_user_id",
                        "created_by_company_id",
                        "description",
                        "status",
                        "created_at",
                        "updated_at",
                    ],
                    "chat_messages": [
                        "message_id",
                        "room_id",
                        "user_id",
                        "display_name",
                        "company_id",
                        "message_text",
                        "has_attachment",
                        "attachment_type",
                        "linked_file_id",
                        "is_deleted",
                        "created_at",
                        "updated_at",
                        "deleted_by_user_id",
                        "deleted_at",
                    ],
                    "archive_files": [
                        "archive_file_id",
                        "company_id",
                        "title",
                        "description",
                        "category_main",
                        "category_sub",
                        "tags",
                        "file_name",
                        "file_data",
                        "file_type",
                        "uploaded_by_user_id",
                        "visibility_type",
                        "download_password",
                        "is_deleted",
                        "created_at",
                        "updated_at",
                        "deleted_by_user_id",
                        "deleted_at",
                    ],
                    "warehouse_files": [
                        "file_id",
                        "title",
                        "description",
                        "category_main",
                        "category_sub",
                        "tags",
                        "file_name",
                        "file_data",
                        "file_type",
                        "uploaded_by_user_id",
                        "uploaded_by_company_id",
                        "source_room_id",
                        "visibility_type",
                        "download_password",
                        "is_searchable",
                        "is_deleted",
                        "created_at",
                        "updated_at",
                        "deleted_by_user_id",
                        "deleted_at",
                    ],   
                    "admin_logs": [
                        "log_id",
                        "company_id",
                        "acted_by_user_id",
                        "acted_by_display_name",
                        "action_type",
                        "target_type",
                        "target_id",
                        "action_detail",
                        "created_at",
                    ],
                    "user_company_permissions": [
                        "permission_id",
                        "user_id",
                        "company_id",
                        "can_use",
                        "is_admin",
                        "status",
                        "created_at",
                        "updated_at",
                        "memo",
                    ],
                    "companies": [
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
                    ],      
                    "contact_messages": [
                        "id",
                        "company_id",
                        "company_name",
                        "user_id",
                        "user_name",
                        "contact_type",
                        "message",
                        "status",
                        "created_at",
                    ],
                    "piecework_master": [
                        "company_id",
                        "piecework_id",
                        "piecework_name",
                        "status",
                        "unit",
                        "note",
                        "created_at",
                        "updated_at",
                        "is_deleted",
                    ],
                    "piecework_entries": [
                        "company_id",
                        "entry_id",
                        "piecework_id",
                        "entry_date",
                        "defect_quantity",
                        "final_delivery_quantity",
                        "income",
                        "created_at",
                        "updated_at",
                        "is_deleted",
                    ],
                    "piecework_production": [
                        "company_id",
                        "production_id",
                        "piecework_id",
                        "work_date",
                        "user_id",
                        "user_name",
                        "quantity",
                        "created_at",
                        "updated_at",
                        "is_deleted",
                    ],
                    "piecework_master": [
                        "company_id",
                        "piecework_id",
                        "piecework_name",
                        "client_name",
                        "arrival_date",
                        "delivery_date",
                        "quantity",
                        "unit_price",
                        "purchase_price",
                        "defect_quantity",
                        "final_delivery_quantity",
                        "income",
                        "unit",
                        "note",
                        "status",
                        "created_at",
                        "updated_at",
                        "is_deleted",
                    ],
                    "piecework_clients": [
                        "company_id",
                        "client_id",
                        "client_name",
                        "client_address",
                        "contact_1",
                        "contact_person_1",
                        "contact_2",
                        "contact_person_2",
                        "created_at",
                        "updated_at",
                        "is_deleted",
                    ],
                    "attendance_logs": [
                        "attendance_id",
                        "date",
                        "user_id",
                        "company_id",
                        "action",
                        "timestamp",
                        "device_name",
                        "recorded_by",
                    ],

                    "attendance_display_settings": [
                        "setting_id",
                        "group_id",
                        "slot_no",
                        "company_id",
                        "status",
                        "created_at",
                        "registered_by",
                    ],   
                    "ic_reader_bridge": [
                        "bridge_id",
                        "device_name",
                        "last_card_id",
                        "last_seen_at",
                        "status",
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
        # 必要に応じて追加
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

@st.cache_data(ttl=300)
def get_users_df_cached():
    df = load_db("users")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "user_id",
            "display_name",
            "login_id",
            "login_password",
            "login_card_id",
            "is_admin",
            "can_use",
            "status",
            "attendance_enabled",
            "display_order",
        ])
    else:
        for col in [
            "user_id",
            "display_name",
            "login_id",
            "login_password",
            "login_card_id",
            "is_admin",
            "can_use",
            "status",
            "attendance_enabled",
            "display_order",
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_users_df():
    return get_users_df_cached()


@st.cache_data(ttl=300)
def get_user_company_permissions_df_cached():
    df = load_db("user_company_permissions")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "company_id",
            "user_id",
            "can_use",
            "is_admin",
            "status",
            "display_order",
        ])
    else:
        for col in [
            "company_id",
            "user_id",
            "can_use",
            "is_admin",
            "status",
            "display_order",
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_user_company_permissions_df():
    return get_user_company_permissions_df_cached()