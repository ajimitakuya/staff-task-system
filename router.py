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


def set_page(page_name: str):
    st.session_state.current_page = str(page_name)
    st.rerun()


def get_current_page() -> str:
    return str(st.session_state.get("current_page", DEFAULT_PAGE)).strip()


def _menu_button(label: str, page_name: str, key: str):
    current = get_current_page()
    selected = current == page_name

    if selected:
        st.sidebar.markdown(
            f"""
            <div style="
                background:#EEF2FF;
                border:1px solid #C7D2FE;
                border-radius:12px;
                padding:12px 14px;
                margin-bottom:8px;
                font-weight:700;
                color:#1F2937;
            ">
                {label}
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        if st.sidebar.button(label, key=key, use_container_width=True):
            set_page(page_name)


def render_sidebar_navigation():
    st.sidebar.markdown("### 📚 メニュー")

    _menu_button("① 未着手の任務（掲示板）", "① 未着手の任務（掲示板）", "menu_01")
    _menu_button("② タスクの引き受け・報告", "② タスクの引き受け・報告", "menu_02")
    _menu_button("③ 稼働状況・完了履歴", "③ 稼働状況・完了履歴", "menu_03")
    _menu_button("④ マニュアル", "④ マニュアル", "menu_04")
    _menu_button("⑤ 記録状況", "⑤ 記録状況", "menu_05")
    _menu_button("⑥ カレンダー", "⑥ カレンダー", "menu_06")
    _menu_button("⑦ 利用者情報", "⑦ 利用者情報", "menu_07")
    _menu_button("⑧ 保存書類", "⑧ 保存書類", "menu_08")
    _menu_button("⑨ 管理者", "⑨ 管理者", "menu_09")
    _menu_button("⑩ 検索", "⑩ 検索", "menu_10")

    st.sidebar.divider()

    _menu_button("☕ 休憩室", "休憩室", "menu_break")
    _menu_button("🔐 Knowbe情報登録", "Knowbe情報登録", "menu_knowbe")
    _menu_button("🐝 Knowbe日誌入力", "🐝 Knowbe日誌入力", "menu_bee")


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

    if page == "⑧ 保存書類":
        render_saved_documents_page()
        return

    if page == "⑨ 管理者":
        render_admin_page()
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

    if page == "🐝 Knowbe日誌入力":
        render_bee_diary_page(
            generate_fn=bee_generate_fn,
            send_fn=bee_send_fn,
        )
        return

    render_placeholder_page(page)