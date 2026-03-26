import streamlit as st

from auth import (
    init_auth_session,
    is_logged_in,
    render_login_page,
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


# =========================
# ページ設定
# =========================
st.set_page_config(
    page_title="作業管理システム",
    layout="wide",
)


# =========================
# 共通初期化
# =========================
def init_app():
    init_auth_session()
    init_page_session()


# =========================
# Bee diary hook
# あとで既存の Gemini / Selenium 本体へ接続するある
# =========================
def bee_generate_hook(payload):
    """
    ここは最後に既存の Gemini 生成関数へつなぐある。
    今は bee_diary.py 側の default_generate_diary_texts が使われるので、
    None を返して route 側の既定動作でもOKある。
    """
    return {
        "generated_status": "",
        "generated_support": "",
    }


def bee_send_hook(payload):
    """
    ここは最後に既存の Knowbe 送信関数へつなぐある。
    いまは仮実装。
    """
    return False, "まだ Knowbe 送信本体は未接続ある。最終 app.py 完成版でつなぐある。"


# =========================
# まだ分割していない旧ページの仮置き
# あとで順番に移植 or 接続するある
# =========================
def render_old_task_board_placeholder():
    st.title("① 未着手の任務（掲示板）")
    st.info("このページは旧 app.py 側の本体をあとで接続するある。")


def render_custom_pages():
    return {
        "① 未着手の任務（掲示板）": render_old_task_board_placeholder,
    }


# =========================
# メイン
# =========================
def main():
    init_app()

    if not is_logged_in():
        render_login_page_shell()
        render_login_page()
        return

    render_app_header()
    render_sidebar_common()
    render_sidebar_navigation()

    route_page(
        bee_generate_fn=bee_generate_hook,
        bee_send_fn=bee_send_hook,
        custom_pages=render_custom_pages(),
    )


if __name__ == "__main__":
    main()