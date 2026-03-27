import streamlit as st


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


def render_sidebar_common():
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
            <div style="display:flex;align-items:center;gap:12px;">
                <div style="font-size:38px;">&#128029;</div>
                <div>
                    <div style="font-weight:bold;font-size:28px;">Sue for Bee</div>
                    <div style="font-size:18px;color:gray;">Assistance System</div>
                </div>
            </div>
            <div style="margin-top:16px;font-size:14px;color:#4B5563;line-height:1.8;">
                事業所と職員のログイン情報で入るある。<br>
                通常は自事業所固定、🐝日誌入力だけ必要時に例外送信できる設計ある。
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


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