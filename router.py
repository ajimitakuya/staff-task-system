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
from saved_documents_page import render_saved_documents_page

DEFAULT_PAGE = "① 未着手の任務（掲示板）"


def init_page_session():
    if "current_page" not in st.session_state:
        st.session_state.current_page = DEFAULT_PAGE


def set_page(page_name: str):
    st.session_state.current_page = str(page_name)
    st.rerun()


def get_current_page() -> str:
    return str(st.session_state.get("current_page", DEFAULT_PAGE)).strip()


def render_sidebar_navigation():
    st.sidebar.markdown("## 📚 メニュー")

    if st.sidebar.button("① 未着手の任務（掲示板）", use_container_width=True):
        set_page("① 未着手の任務（掲示板）")

    if st.sidebar.button("☕ 休憩室", use_container_width=True):
        set_page("休憩室")

    if st.sidebar.button("🔐 Knowbe情報登録", use_container_width=True):
        set_page("Knowbe情報登録")

    if st.sidebar.button("🐝 Knowbe日誌入力", use_container_width=True):
        set_page("🐝 Knowbe日誌入力")


def render_placeholder_page(title: str):
    st.title(title)
    st.info("このページは最後の完成版 app.py で本接続するある。今は分割準備中ある。")


def route_page(
    bee_generate_fn=None,
    bee_send_fn=None,
    custom_pages=None,
):
    """
    custom_pages:
      {
        "ページ名": callable
      }
    """
    page = get_current_page()

    if custom_pages is None:
        custom_pages = {}

    if page in custom_pages:
        custom_pages[page]()
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

    # まだ分割していない既存ページ群は一旦プレースホルダ
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
        render_placeholder_page("⑦ 利用者情報")
        return

    if page == "⑧ 保存書類":
        render_saved_documents_page()
        return
       

    if page == "⑨ 管理者":
        render_placeholder_page("⑨ 管理者")
        return

    if page == "⑩ 検索":
        render_placeholder_page("⑩ 検索")
        return

    render_placeholder_page(page)