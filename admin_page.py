import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone

from db import get_users_df, save_db
from auth import get_companies_df


JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


def get_current_company_id():
    return str(st.session_state.get("company_id", "")).strip()


def is_current_user_admin():
    return bool(st.session_state.get("is_admin", False))


def get_user_required_cols():
    return [
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
    ]


def normalize_users_df(df):
    required_cols = get_user_required_cols()

    if df is None or df.empty:
        return pd.DataFrame(columns=required_cols)

    work = df.copy().fillna("")
    for col in required_cols:
        if col not in work.columns:
            work[col] = ""

    return work[required_cols].copy()


def get_company_users_df(company_id=None):
    if company_id is None:
        company_id = get_current_company_id()

    df = normalize_users_df(get_users_df())
    return df[df["company_id"].astype(str).str.strip() == str(company_id).strip()].copy()


def is_valid_user_password(raw_password: str) -> bool:
    pw = str(raw_password)

    if len(pw) < 8:
        return False
    if not any(c.islower() for c in pw):
        return False
    if not any(c.isupper() for c in pw):
        return False
    if not any(c.isdigit() for c in pw):
        return False

    return True


def get_next_user_id():
    df = normalize_users_df(get_users_df())

    if df is None or df.empty:
        return "U0001"

    nums = []
    for x in df["user_id"].fillna("").astype(str):
        x = x.strip().upper()
        if x.startswith("U"):
            num = x[1:]
            if num.isdigit():
                nums.append(int(num))

    next_num = max(nums) + 1 if nums else 1
    return f"U{next_num:04d}"


def create_user_for_current_company(
    user_login_id: str,
    user_login_password: str,
    display_name: str,
    role_type: str = "職員",
    is_admin: str = "0",
):
    company_id = get_current_company_id()
    users_df = normalize_users_df(get_users_df())
    now_str = now_jst().strftime("%Y-%m-%d %H:%M:%S")

    user_login_id = str(user_login_id).strip()
    user_login_password = str(user_login_password).strip()
    display_name = str(display_name).strip()
    role_type = str(role_type).strip()
    is_admin = "1" if str(is_admin).strip() == "1" else "0"

    if not user_login_id:
        return False, "ログインIDを入れてほしいある。"

    if not display_name:
        return False, "表示名を入れてほしいある。"

    if not is_valid_user_password(user_login_password):
        return False, "パスワードは8文字以上・英小文字・英大文字・数字を全部含めてほしいある。"

    dup = users_df[
        users_df["user_login_id"].astype(str).str.strip() == user_login_id
    ]
    if not dup.empty:
        return False, "そのログインIDはすでに使われてるある。"

    new_user = pd.DataFrame([{
        "user_id": get_next_user_id(),
        "company_id": company_id,
        "user_login_id": user_login_id,
        "user_login_password": user_login_password,
        "display_name": display_name,
        "is_admin": is_admin,
        "role_type": role_type,
        "login_card_id": "",
        "last_login_at": "",
        "status": "active",
        "created_at": now_str,
        "updated_at": now_str,
        "memo": "",
    }])

    users_df = pd.concat([users_df, new_user], ignore_index=True)
    save_db(users_df, "users")

    return True, str(new_user.iloc[0]["user_id"])


def set_user_status(user_id: str, new_status: str):
    users_df = normalize_users_df(get_users_df())
    now_str = now_jst().strftime("%Y-%m-%d %H:%M:%S")

    mask = users_df["user_id"].astype(str).str.strip() == str(user_id).strip()
    if not mask.any():
        return False, "対象スタッフが見つからないある。"

    users_df.loc[mask, "status"] = str(new_status).strip()
    users_df.loc[mask, "updated_at"] = now_str
    save_db(users_df, "users")
    return True, "更新したある。"


def get_current_company_info():
    company_id = get_current_company_id()
    df = get_companies_df()

    if df is None or df.empty:
        return {}

    work = df.copy().fillna("")
    hit = work[work["company_id"].astype(str).str.strip() == company_id]
    if hit.empty:
        return {}

    return hit.iloc[0].to_dict()


def render_admin_page():
    if not is_current_user_admin():
        st.error("このページは管理者専用ある。")
        return

    st.title("⑨ 管理者")
    st.caption("スタッフ管理や事業所設定を行うページある。")

    company_info = get_current_company_info()
    company_name = str(company_info.get("company_name", st.session_state.get("company_name", ""))).strip()
    company_code = str(company_info.get("company_code", "")).strip()

    st.info(f"対象事業所: {company_name} / company_id={get_current_company_id()} / code={company_code}")

    tab1, tab2, tab3 = st.tabs(["スタッフ一覧", "スタッフ登録", "事業所設定"])

    with tab1:
        st.subheader("👥 スタッフ一覧")

        users_df = get_company_users_df()

        if users_df.empty:
            st.info("この事業所のスタッフはまだいないある。")
        else:
            try:
                users_df = users_df.sort_values(
                    ["is_admin", "display_name"],
                    ascending=[False, True]
                )
            except Exception:
                pass

            for _, row in users_df.iterrows():
                user_id = str(row.get("user_id", "")).strip()
                display_name = str(row.get("display_name", "")).strip()
                user_login_id = str(row.get("user_login_id", "")).strip()
                role_type = str(row.get("role_type", "")).strip()
                is_admin = str(row.get("is_admin", "")).strip()
                status = str(row.get("status", "")).strip()
                last_login_at = str(row.get("last_login_at", "")).strip()

                with st.container(border=True):
                    c1, c2 = st.columns([4, 1])

                    with c1:
                        st.write(f"**{display_name}**")
                        st.caption(
                            f"user_id={user_id} / login_id={user_login_id} / "
                            f"権限={'管理者' if is_admin == '1' else '職員'} / "
                            f"役割={role_type or '-'} / "
                            f"状態={status or '-'} / "
                            f"最終ログイン={last_login_at or '-'}"
                        )

                    with c2:
                        if status != "inactive":
                            if st.button("無効化", key=f"deactivate_staff_{user_id}", use_container_width=True):
                                ok, msg = set_user_status(user_id, "inactive")
                                if ok:
                                    st.success(msg)
                                    st.rerun()
                                else:
                                    st.error(msg)
                        else:
                            if st.button("再有効化", key=f"activate_staff_{user_id}", use_container_width=True):
                                ok, msg = set_user_status(user_id, "active")
                                if ok:
                                    st.success(msg)
                                    st.rerun()
                                else:
                                    st.error(msg)

    with tab2:
        st.subheader("➕ スタッフ登録")

        display_name = st.text_input("表示名", key="admin_staff_display_name")
        user_login_id = st.text_input("ログインID", key="admin_staff_login_id")
        user_login_password = st.text_input("パスワード", type="password", key="admin_staff_login_pw")
        role_type = st.selectbox("役割", ["管理者", "職員"], key="admin_staff_role_type")

        st.caption("パスワードは8文字以上、英小文字・英大文字・数字を全部入れてほしいある。")

        if st.button("登録する", use_container_width=True, key="admin_staff_register_button"):
            is_admin = "1" if role_type == "管理者" else "0"

            ok, msg = create_user_for_current_company(
                user_login_id=user_login_id,
                user_login_password=user_login_password,
                display_name=display_name,
                role_type=role_type,
                is_admin=is_admin,
            )
            if ok:
                st.success(f"登録できたある！ user_id={msg}")
                st.rerun()
            else:
                st.error(msg)

    with tab3:
        st.subheader("🏢 事業所設定")

        st.write(f"**事業所名**: {company_name}")
        st.write(f"**事業所コード**: {company_code or '-'}")
        st.write(f"**company_id**: {get_current_company_id()}")

        st.divider()
        st.write("Knowbe情報の登録・更新は専用ページから行うある。")

        if st.button("🔐 Knowbe情報登録ページへ", key="go_company_settings_from_admin", use_container_width=True):
            st.session_state.current_page = "Knowbe情報登録"
            st.rerun()