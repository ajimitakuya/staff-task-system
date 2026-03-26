import streamlit as st
import pandas as pd

from db import get_companies_df, get_users_df, save_db


# =========================
# 基本ユーティリティ
# =========================
def normalize_flag(value) -> bool:
    s = str(value).strip().lower()
    return s in {"1", "true", "yes", "on"}


def mask_secret_text(text: str) -> str:
    text = str(text or "").strip()
    if not text:
        return ""
    if len(text) <= 2:
        return "*" * len(text)
    return text[:1] + ("*" * (len(text) - 2)) + text[-1:]


# =========================
# session_state 初期化
# =========================
def init_auth_session():
    defaults = {
        "logged_in": False,
        "company_id": "",
        "company_name": "",
        "company_code": "",
        "company_login_id": "",
        "user_id": "",
        "user": "",
        "user_login_id": "",
        "is_admin": False,
        "role_type": "",
        "current_page": "① 未着手の任務（掲示板）",
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def clear_auth_session():
    keys_to_clear = [
        "logged_in",
        "company_id",
        "company_name",
        "company_code",
        "company_login_id",
        "user_id",
        "user",
        "user_login_id",
        "is_admin",
        "role_type",
    ]
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]

    # 必須デフォルトだけ戻す
    init_auth_session()


def is_logged_in() -> bool:
    return bool(st.session_state.get("logged_in", False))


# =========================
# 会社認証
# =========================
def get_companies_active_df():
    df = get_companies_df()
    if df is None or df.empty:
        return pd.DataFrame(columns=[
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

    work = df.copy()

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
        if col not in work.columns:
            work[col] = ""

    work = work.fillna("")
    work["status"] = work["status"].astype(str).str.strip().str.lower()

    # inactive 以外を有効扱い
    work = work[work["status"] != "inactive"].copy()
    return work


def authenticate_company_login(login_id: str, login_password: str):
    df = get_companies_active_df()
    if df.empty:
        return None

    work = df.copy()
    work["company_login_id"] = work["company_login_id"].astype(str).str.strip()
    work["company_login_password"] = work["company_login_password"].astype(str).str.strip()

    target = work[
        (work["company_login_id"] == str(login_id).strip()) &
        (work["company_login_password"] == str(login_password).strip())
    ].copy()

    if target.empty:
        return None

    return target.iloc[0].to_dict()


def get_company_row_by_company_id(company_id: str):
    df = get_companies_active_df()
    if df.empty:
        return None

    target = df[df["company_id"].astype(str).str.strip() == str(company_id).strip()].copy()
    if target.empty:
        return None

    return target.iloc[0].to_dict()


# =========================
# 職員認証
# =========================
def get_users_active_df():
    df = get_users_df()
    if df is None or df.empty:
        return pd.DataFrame(columns=[
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
        ])

    work = df.copy()

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
        "created_at",
        "updated_at",
        "memo",
    ]:
        if col not in work.columns:
            work[col] = ""

    work = work.fillna("")
    work["status"] = work["status"].astype(str).str.strip().str.lower()

    # inactive 以外を有効扱い
    work = work[work["status"] != "inactive"].copy()
    return work


def authenticate_user_login(company_id: str, user_login_id: str, user_login_password: str):
    df = get_users_active_df()
    if df.empty:
        return None

    work = df.copy()
    work["company_id"] = work["company_id"].astype(str).str.strip()
    work["user_login_id"] = work["user_login_id"].astype(str).str.strip()
    work["user_login_password"] = work["user_login_password"].astype(str).str.strip()

    target = work[
        (work["company_id"] == str(company_id).strip()) &
        (work["user_login_id"] == str(user_login_id).strip()) &
        (work["user_login_password"] == str(user_login_password).strip())
    ].copy()

    if target.empty:
        return None

    return target.iloc[0].to_dict()


def get_user_row_by_user_id(user_id: str):
    df = get_users_active_df()
    if df.empty:
        return None

    target = df[df["user_id"].astype(str).str.strip() == str(user_id).strip()].copy()
    if target.empty:
        return None

    return target.iloc[0].to_dict()


def update_user_last_login(user_id: str):
    df = get_users_df()
    if df is None or df.empty:
        return

    mask = df["user_id"].fillna("").astype(str).str.strip() == str(user_id).strip()
    if not mask.any():
        return

    now_str = pd.Timestamp.now(tz="Asia/Tokyo").strftime("%Y-%m-%d %H:%M:%S")
    df.loc[mask, "last_login_at"] = now_str
    df.loc[mask, "updated_at"] = now_str
    save_db(df, "users")


# =========================
# ログイン処理
# =========================
def login_with_credentials(company_login_id: str, company_login_password: str, user_login_id: str, user_login_password: str):
    company_row = authenticate_company_login(company_login_id, company_login_password)
    if company_row is None:
        return False, "事業所IDまたは事業所パスワードが違うある。"

    company_id = str(company_row.get("company_id", "")).strip()
    if not company_id:
        return False, "事業所データが壊れているある。"

    user_row = authenticate_user_login(
        company_id=company_id,
        user_login_id=user_login_id,
        user_login_password=user_login_password,
    )
    if user_row is None:
        return False, "職員IDまたは職員パスワードが違うある。"

    st.session_state.logged_in = True
    st.session_state.company_id = company_id
    st.session_state.company_name = str(company_row.get("company_name", "")).strip()
    st.session_state.company_code = str(company_row.get("company_code", "")).strip()
    st.session_state.company_login_id = str(company_row.get("company_login_id", "")).strip()

    st.session_state.user_id = str(user_row.get("user_id", "")).strip()
    st.session_state.user = str(user_row.get("display_name", "")).strip()
    st.session_state.user_login_id = str(user_row.get("user_login_id", "")).strip()
    st.session_state.is_admin = normalize_flag(user_row.get("is_admin", "0"))
    st.session_state.role_type = str(user_row.get("role_type", "")).strip()

    update_user_last_login(st.session_state.user_id)

    return True, "ログイン成功ある。"


def logout():
    clear_auth_session()


# =========================
# 会社保存Knowbe情報
# =========================
def get_company_saved_knowbe_info(company_id: str):
    row = get_company_row_by_company_id(company_id)
    if row is None:
        return "", ""

    return (
        str(row.get("knowbe_login_username", "")).strip(),
        str(row.get("knowbe_login_password", "")).strip(),
    )


def save_company_saved_knowbe_info(company_id: str, knowbe_login_username: str, knowbe_login_password: str):
    df = get_companies_df()
    if df is None or df.empty:
        return False

    mask = df["company_id"].fillna("").astype(str).str.strip() == str(company_id).strip()
    if not mask.any():
        return False

    now_str = pd.Timestamp.now(tz="Asia/Tokyo").strftime("%Y-%m-%d %H:%M:%S")
    df.loc[mask, "knowbe_login_username"] = str(knowbe_login_username).strip()
    df.loc[mask, "knowbe_login_password"] = str(knowbe_login_password).strip()
    df.loc[mask, "updated_at"] = now_str
    save_db(df, "companies")
    return True


# =========================
# 画面
# =========================
def render_login_page():
    st.title("🔐 作業管理システム")
    st.caption("事業所と職員のログイン情報を入れてほしいある。")

    with st.container():
        col1, col2 = st.columns(2)

        with col1:
            company_login_id = st.text_input("事業所ID", key="login_company_login_id")
            company_login_password = st.text_input("事業所パスワード", type="password", key="login_company_login_password")

        with col2:
            user_login_id = st.text_input("職員ID", key="login_user_login_id")
            user_login_password = st.text_input("職員パスワード", type="password", key="login_user_login_password")

        if st.button("ログイン", key="login_submit_button", use_container_width=True):
            ok, msg = login_with_credentials(
                company_login_id=company_login_id,
                company_login_password=company_login_password,
                user_login_id=user_login_id,
                user_login_password=user_login_password,
            )
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)


def render_logout_button():
    if st.button("ログアウト", key="logout_button", use_container_width=True):
        logout()
        st.rerun()