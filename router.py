import streamlit as st

from break_room import render_break_room_page
from chat import render_chat_room_page
from archive import render_archive_page
from warehouse import render_warehouse_page
from company_settings import render_company_knowbe_settings_page
from bee_diary import render_bee_diary_page
from task_board import render_task_board_page, render_my_tasks_page
from task_history import render_task_history_page
from manual_page import render_manual_page
from record_status import render_record_status_page
from calendar_page import render_calendar_page
from resident_info_page import render_resident_info_page
from saved_documents_page import render_saved_documents_page
from admin_page import render_admin_page
from search_page import render_search_page


DEFAULT_PAGE = "① 未着手の任務（掲示板）"


def init_page_session():
    if "current_page" not in st.session_state:
        st.session_state.current_page = DEFAULT_PAGE
    if "bee_menu_unlocked" not in st.session_state:
        st.session_state["bee_menu_unlocked"] = False
    if "other_office_register_unlocked" not in st.session_state:
        st.session_state["other_office_register_unlocked"] = False
    if "secret_doc_mode" not in st.session_state:
        st.session_state["secret_doc_mode"] = False
    if "heart_mode" not in st.session_state:
        st.session_state["heart_mode"] = False
    if "secret_bee_cmd" not in st.session_state:
        st.session_state["secret_bee_cmd"] = ""


def set_page(page_name: str):
    st.session_state.current_page = str(page_name)
    st.rerun()


def get_current_page() -> str:
    return str(st.session_state.get("current_page", DEFAULT_PAGE)).strip()


def heart_label(text: str) -> str:
    if not st.session_state.get("heart_mode", False):
        return str(text)

    s = str(text)

    if len(s) >= 2 and s[1] == " ":
        return f"💕 {s[2:]}"
    if len(s) >= 3 and s[2] == " ":
        return f"💕 {s[3:]}"

    if "knowbe" in s.lower():
        return "💕knowbe日誌入力💕"

    return f"💕 {s}"


def process_secret_command():
    cmd = str(st.session_state.get("secret_bee_cmd", "")).strip()

    if cmd == "🐝":
        st.session_state["bee_menu_unlocked"] = True
    elif cmd == "登録💻":
        st.session_state["other_office_register_unlocked"] = True
    elif cmd == "🤫":
        st.session_state["secret_doc_mode"] = True
    elif cmd == "💕":
        st.session_state["heart_mode"] = True

    st.session_state["secret_bee_cmd"] = ""


def render_selected_menu(label: str):
    st.sidebar.markdown(
        f'<div class="menu-selected-wrap"><div class="menu-selected-box">● {label}</div></div>',
        unsafe_allow_html=True
    )


def render_sidebar_navigation():
    main_page_options = [
        ("⓪ 検索", "⑩ 検索"),
        ("① 未着手の任務（掲示板）", "① 未着手の任務（掲示板）"),
        ("② タスクの引き受け・報告", "② タスクの引き受け・報告"),
        ("③ 稼働状況・完了履歴", "③ 稼働状況・完了履歴"),
        ("④ チームチャット", "休憩室_チャットルーム"),
        ("⑤ 業務マニュアル", "④ マニュアル"),
        ("⑥ 日誌入力状況", "⑤ 記録状況"),
        ("⑦ タスクカレンダー", "⑥ カレンダー"),
        ("⑧ 緊急一覧", "⑧ 緊急一覧"),
        ("⑨ 利用者情報", "⑦ 利用者情報"),
        ("⑩ 書類アップロード", "休憩室_書類アップロード"),
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

    for label, target_page in main_page_options:
        is_selected = (get_current_page() == target_page)
        display_label = heart_label(label)

        if is_selected:
            render_selected_menu(display_label)
        else:
            if st.sidebar.button(display_label, key=f"menu_{label}", use_container_width=True):
                set_page(target_page)

    st.sidebar.markdown("### 利用者書類")

    for page_key, label in document_page_options:
        is_selected = (get_current_page() == page_key)
        display_label = heart_label(label)

        if is_selected:
            render_selected_menu(display_label)
        else:
            if st.sidebar.button(display_label, key=f"menu_{page_key}", use_container_width=True):
                set_page(page_key)

    if st.sidebar.button("個人ログアウト", key="sidebar_logout", use_container_width=True):
        for k in [
            "logged_in", "company_id", "company_name", "company_code", "company_login_id",
            "user_id", "user", "user_login_id", "is_admin", "role_type",
            "current_page", "bee_menu_unlocked", "other_office_register_unlocked",
            "secret_doc_mode", "heart_mode", "secret_bee_cmd"
        ]:
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

    if st.sidebar.button("事業所切り替え", key="sidebar_switch_company", use_container_width=True):
        for k in [
            "logged_in", "company_id", "company_name", "company_code", "company_login_id",
            "user_id", "user", "user_login_id", "is_admin", "role_type",
            "current_page", "bee_menu_unlocked", "other_office_register_unlocked",
            "secret_doc_mode", "heart_mode", "secret_bee_cmd"
        ]:
            if k in st.session_state:
                del st.session_state[k]
        st.rerun()

    if st.session_state.get("bee_menu_unlocked", False):
        knowbe_label = "🐝knowbe日誌入力🐝"
        if st.session_state.get("heart_mode", False):
            knowbe_label = "💕knowbe日誌入力💕"

        if st.sidebar.button(knowbe_label, key="knowbe_menu_button", use_container_width=True):
            set_page("🐝knowbe日誌入力🐝")

    if st.session_state.get("other_office_register_unlocked", False):
        if st.sidebar.button("💻他事業所へ登録💻", key="other_office_register_menu_button", use_container_width=True):
            set_page("💻他事業所へ登録💻")

    st.sidebar.text_input(
        "secret command",
        key="secret_bee_cmd",
        on_change=process_secret_command
    )

    if st.session_state.get("is_admin", False):
        if st.sidebar.button("スタッフ登録・削除", key="menu_staff_manage", use_container_width=True):
            set_page("⑨ 管理者")

        if st.sidebar.button("Knowbe情報登録", key="menu_knowbe_settings", use_container_width=True):
            set_page("Knowbe情報登録")

    st.sidebar.caption("System Version 2.0")


def render_placeholder_page(title: str):
    st.title(title)
    st.info("このページはまだ接続途中ある。")


def route_page(
    bee_generate_fn=None,
    bee_send_fn=None,
):
    page = get_current_page()

    if page == "① 未着手の任務（掲示板）":
        render_task_board_page()
        return

    if page == "② タスクの引き受け・報告":
        render_my_tasks_page()
        return

    if page == "③ 稼働状況・完了履歴":
        render_task_history_page()
        return

    if page == "④ マニュアル":
        render_manual_page()
        return

    if page == "⑤ 記録状況":
        render_record_status_page()
        return

    if page == "⑥ カレンダー":
        render_calendar_page()
        return

    if page == "⑦ 利用者情報":
        render_resident_info_page()
        return

    if page == "⑩ 検索":
        render_search_page()
        return

    if page == "休憩室":
        render_break_room_page()
        return

    if page == "休憩室_チャットルーム":
        render_chat_room_page()
        return

    if page == "休憩室_書類アップロード":
        render_archive_page()
        return

    if page == "休憩室_倉庫":
        render_warehouse_page()
        return

    if page == "Knowbe情報登録":
        render_company_knowbe_settings_page()
        return

    if page in ["🐝knowbe日誌入力🐝", "🐝 Knowbe日誌入力"]:
        render_bee_diary_page(
            generate_fn=bee_generate_fn,
            send_fn=bee_send_fn,
        )
        return

    if page in [
        "⑧ 緊急一覧",
        "書類_個別支援計画案",
        "書類_サービス担当者会議",
        "書類_個別支援計画",
        "書類_モニタリング",
        "書類_在宅評価シート",
        "書類_アセスメント",
        "書類_基本シート",
        "書類_就労分野シート",
        "💻他事業所へ登録💻",
    ]:
        render_placeholder_page(page)
        return

    render_placeholder_page(page)