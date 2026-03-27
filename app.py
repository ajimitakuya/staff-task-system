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
from task_board import render_sidebar_task_status


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
    いまは空返しで、bee_diary.py 側の編集欄を直接使える状態にしておくある。
    """
    return {
        "generated_status": "",
        "generated_support": "",
    }


def bee_send_hook(payload):
    """
    ここは最後に既存の Knowbe 送信関数へつなぐある。
    いまは仮実装ある。
    """
    return False, "まだ Knowbe 送信本体は未接続ある。最終接続でつなぐある。"


# =========================
# メイン
# =========================
def main():
    init_app()

    # -------------------------
    # ログイン前
    # -------------------------
    if not is_logged_in():
        render_login_page_shell()
        render_login_page()
        return

    # -------------------------
    # ログイン後
    # -------------------------
    render_app_header()
    render_sidebar_common()
    render_sidebar_task_status()
    render_sidebar_navigation()

    route_page(
        bee_generate_fn=bee_generate_hook,
        bee_send_fn=bee_send_hook,
    )


if __name__ == "__main__":
    main()