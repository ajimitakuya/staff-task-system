import streamlit as st


def render_break_room_page():
    st.title("☕ 休憩室")
    st.caption("ここから チャットルーム・書類アップロード・倉庫 に入るある。")

    st.markdown(
        f"""
        **現在の事業所**: {st.session_state.get("company_name", "")}  
        **ログイン中**: {st.session_state.get("user", "")}
        """
    )

    st.divider()

    cols = st.columns(3)

    with cols[0]:
        st.markdown("## 🚪 チャットルーム")
        st.caption("全事業所共通の交流・共有スペースある。")
        if st.button("チャットルームへ", key="go_chat_rooms", use_container_width=True):
            st.session_state.current_page = "休憩室_チャットルーム"
            st.rerun()

    with cols[1]:
        st.markdown("## 🚪 書類アップロード")
        st.caption("この事業所だけで共有する資料を登録・閲覧するある。")
        if st.button("書類アップロードへ", key="go_archive_page", use_container_width=True):
            st.session_state.current_page = "休憩室_書類アップロード"
            st.rerun()

    with cols[2]:
        st.markdown("## 🚪 倉庫")
        st.caption("全事業所共通の資料置き場ある。")
        if st.button("倉庫へ", key="go_warehouse_page", use_container_width=True):
            st.session_state.current_page = "休憩室_倉庫"
            st.rerun()