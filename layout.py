import streamlit as st

from auth import render_logout_button


def render_app_header():
    company_name = str(st.session_state.get("company_name", "")).strip()
    user_name = str(st.session_state.get("user", "")).strip()
    role_type = str(st.session_state.get("role_type", "")).strip()

    st.markdown(
        f"""
        <div style="
            padding:14px 18px;
            border:1px solid #E5E7EB;
            border-radius:16px;
            background:#FFFFFF;
            box-shadow:0 1px 3px rgba(0,0,0,0.05);
            margin-bottom:14px;
        ">
            <div style="font-size:24px;font-weight:700;color:#111827;">
                作業管理システム
            </div>
            <div style="margin-top:8px;font-size:14px;color:#4B5563;">
                事業所: <b>{company_name}</b>
                &nbsp;&nbsp;|&nbsp;&nbsp;
                ログイン中: <b>{user_name}</b>
                {"&nbsp;&nbsp;|&nbsp;&nbsp;権限: <b>" + role_type + "</b>" if role_type else ""}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_sidebar_user_box():
    company_name = str(st.session_state.get("company_name", "")).strip()
    user_name = str(st.session_state.get("user", "")).strip()
    is_admin = bool(st.session_state.get("is_admin", False))
    badge = "管理者" if is_admin else "一般"

    with st.sidebar.container():
        st.markdown("### 現在の事業所")
        st.write(company_name)

        st.markdown("### ログイン中")
        st.write(user_name)

        st.caption(f"権限: {badge}")

        st.divider()


def render_sidebar_system_box():
    with st.sidebar.container():
        st.markdown("### システム案内")
        st.write("通常は自事業所固定で利用するある。")
        st.write("🐝 日誌入力だけ必要時に他事業所へ一時送信できるある。")
        st.divider()


def render_page_container_start(page_title: str = "", caption: str = ""):
    if page_title:
        st.title(page_title)
    if caption:
        st.caption(caption)


def render_simple_back_button(target_page: str, label: str = "← 戻る", key: str = "simple_back_button"):
    if st.button(label, key=key, use_container_width=True):
        st.session_state.current_page = str(target_page)
        st.rerun()


def render_two_col_topbar(
    left_button_label: str,
    left_target_page: str,
    left_button_key: str,
    right_text: str = "",
):
    cols = st.columns([1, 1])

    with cols[0]:
        if st.button(left_button_label, key=left_button_key, use_container_width=True):
            st.session_state.current_page = str(left_target_page)
            st.rerun()

    with cols[1]:
        if right_text:
            st.info(right_text)


def render_login_page_shell():
    st.markdown(
        """
        <div style="
            max-width:880px;
            margin:0 auto 18px auto;
            padding:24px 26px;
            border:1px solid #E5E7EB;
            border-radius:18px;
            background:#FFFFFF;
            box-shadow:0 1px 3px rgba(0,0,0,0.05);
        ">
            <div style="font-size:28px;font-weight:800;color:#111827;">
                作業管理システム
            </div>
            <div style="margin-top:8px;font-size:14px;color:#4B5563;line-height:1.8;">
                事業所と職員のログイン情報で入るある。<br>
                通常は自事業所固定、🐝日誌入力だけ必要時に例外送信できる設計ある。
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

def render_sidebar_common():
    render_sidebar_user_box()
    render_sidebar_system_box()
    render_logout_button()


def render_page_container_start(page_title: str = "", caption: str = ""):
    if page_title:
        st.title(page_title)
    if caption:
        st.caption(caption)


def render_simple_back_button(target_page: str, label: str = "← 戻る", key: str = "simple_back_button"):
    if st.button(label, key=key, use_container_width=True):
        st.session_state.current_page = str(target_page)
        st.rerun()


def render_two_col_topbar(
    left_button_label: str,
    left_target_page: str,
    left_button_key: str,
    right_text: str = "",
):
    cols = st.columns([1, 1])

    with cols[0]:
        if st.button(left_button_label, key=left_button_key, use_container_width=True):
            st.session_state.current_page = str(left_target_page)
            st.rerun()

    with cols[1]:
        if right_text:
            st.info(right_text)


def render_login_page_shell():
    st.markdown(
        """
        <div style="
            max-width:880px;
            margin:0 auto 18px auto;
            padding:24px 26px;
            border:1px solid #E5E7EB;
            border-radius:18px;
            background:#FFFFFF;
            box-shadow:0 1px 3px rgba(0,0,0,0.05);
        ">
            <div style="font-size:28px;font-weight:800;color:#111827;">
                作業管理システム
            </div>
            <div style="margin-top:8px;font-size:14px;color:#4B5563;line-height:1.8;">
                事業所と職員のログイン情報で入るある。<br>
                通常は自事業所固定、🐝日誌入力だけ必要時に例外送信できる設計ある。
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )