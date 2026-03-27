import streamlit as st


def _go_page(page_name: str):
    st.session_state.current_page = str(page_name)
    st.rerun()


def _room_card(title: str, emoji: str, desc: str, button_label: str, target_page: str, key: str):
    st.markdown(
        f"""
        <div style="
            border:1px solid #E5E7EB;
            border-radius:16px;
            background:#FFFFFF;
            padding:18px 18px 14px 18px;
            box-shadow:0 1px 3px rgba(0,0,0,0.05);
            min-height:180px;
            margin-bottom:10px;
        ">
            <div style="font-size:30px; margin-bottom:6px;">{emoji}</div>
            <div style="font-size:22px; font-weight:800; color:#111827; margin-bottom:10px;">
                {title}
            </div>
            <div style="font-size:14px; line-height:1.8; color:#4B5563;">
                {desc}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    if st.button(button_label, key=key, use_container_width=True):
        _go_page(target_page)


def render_break_room_page():
    company_name = str(st.session_state.get("company_name", "")).strip()
    user_name = str(st.session_state.get("user", "")).strip()

    top_cols = st.columns([1, 2])
    with top_cols[0]:
        if st.button("← 戻る", key="break_room_back", use_container_width=True):
            _go_page("① 未着手の任務（掲示板）")
    with top_cols[1]:
        st.info(f"ログイン中: {company_name} / {user_name}")

    st.markdown(
        """
        <div style="
            border:1px solid #F3E8FF;
            background:linear-gradient(135deg, #FFF7ED 0%, #FFFDF7 100%);
            border-radius:18px;
            padding:22px 22px 18px 22px;
            margin-top:6px;
            margin-bottom:18px;
        ">
            <div style="font-size:15px; font-weight:700; color:#D97706; margin-bottom:6px;">
                ☕ Break Room
            </div>
            <div style="font-size:28px; font-weight:800; color:#111827; margin-bottom:10px;">
                休憩室
            </div>
            <div style="font-size:14px; line-height:1.9; color:#4B5563;">
                ここでは、事業所内外のやりとりや資料共有に使う機能へ入れるある。<br>
                チャット、書類アップロード、倉庫をここから使い分けるある。
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    cols = st.columns(3)

    with cols[0]:
        _room_card(
            title="チャットルーム",
            emoji="💬",
            desc="全体共有や連絡、添付ファイル付きのやりとりをする場所ある。制限ルームにも対応できるある。",
            button_label="チャットルームへ",
            target_page="休憩室_チャットルーム",
            key="break_room_to_chat",
        )

    with cols[1]:
        _room_card(
            title="書類アップロード",
            emoji="📤",
            desc="この事業所だけで使う資料を保存・閲覧する場所ある。事業所内共有用の書庫ある。",
            button_label="書類アップロードへ",
            target_page="休憩室_書類アップロード",
            key="break_room_to_archive",
        )

    with cols[2]:
        _room_card(
            title="倉庫",
            emoji="🏭",
            desc="全事業所共通で資料を共有する場所ある。検索や公開設定つきの共通資料置き場ある。",
            button_label="倉庫へ",
            target_page="休憩室_倉庫",
            key="break_room_to_warehouse",
        )

    st.divider()

    st.markdown("### 使い分け")
    st.markdown(
        """
        - **チャットルーム**：会話・やりとり中心  
        - **書類アップロード**：この事業所の内部共有  
        - **倉庫**：事業所をまたぐ共通共有
        """
    )