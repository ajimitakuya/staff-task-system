import streamlit as st

from auth import (
    init_auth_session,
    is_logged_in,
    render_login_page,
    render_logout_button,
)
from layout import (
    render_app_header,
    render_sidebar_common,
    render_login_page_shell,
)
from router import (
    init_page_session,
    render_sidebar_navigation,
    route_page,
)
from task_board import render_sidebar_task_status


st.set_page_config(
    page_title="作業管理システム",
    layout="wide",
)


def init_app():
    init_auth_session()
    init_page_session()


def bee_generate_hook(payload):
    return {
        "generated_status": "",
        "generated_support": "",
    }


def bee_send_hook(payload):
    return False, "まだ Knowbe 送信本体は未接続ある。最終接続でつなぐある。"


def main():
    init_app()

    if not is_logged_in():
        render_login_page_shell()
        render_login_page()
        return

    # 画面上部
    render_app_header()

    # ログアウトは ex-app っぽく中央に戻す
    render_logout_button()

    # 左サイド
    render_sidebar_common()
    render_sidebar_task_status()
    render_sidebar_navigation()

    # 本体
    route_page(
        bee_generate_fn=bee_generate_hook,
        bee_send_fn=bee_send_hook,
    )


if __name__ == "__main__":
    main()