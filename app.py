import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="精鋭チーム作戦本部", layout="wide")

# スプレッドシート接続（Secrets設定を参照する設定ある）
conn = st.connection("gsheets", type=GSheetsConnection)

# データ取得関数
def load_tasks(): return conn.read(worksheet="tasks", ttl=0)
def load_chat(): return conn.read(worksheet="chat", ttl=0)

# --- ログイン判定 ---
if 'user' not in st.session_state:
    st.markdown("<style>[data-testid='stSidebarNav'] {display: none;}</style>", unsafe_allow_html=True)
    st.title("🛡️ 基地の入り口")
    st.warning("### ログインしてくれなきゃ泣いちゃうから🥺")
    user = st.selectbox("君は誰だ！", ["--- 選択してね ---", "ゆー", "Aさん", "Bさん", "Cさん"])
    if user != "--- 選択してね ---" and st.button("🚀 出撃するある！"):
        st.session_state.user = user
        st.rerun()
    st.stop()

# --- メイン画面 ---
st.sidebar.title(f"👤 {st.session_state.user} 隊員")
page = st.sidebar.radio("移動先", ["① 未着手の任務", "② チャットルーム"])

if page == "① 未着手の任務":
    st.title("📋 タスク一覧")
    df = load_tasks()
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    with st.expander("➕ 新しい任務を追加"):
        with st.form("add_form"):
            new_task = st.text_input("任務名")
            if st.form_submit_button("登録"):
                new_data = pd.DataFrame([{"id": len(df)+1, "task": new_task, "status": "未着手", "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M')}])
                updated_df = pd.concat([df, new_data], ignore_index=True)
                conn.update(worksheet="tasks", data=updated_df)
                st.success("登録したある！")
                st.rerun()

elif page == "② チャットルーム":
    st.title("💬 会議室")
    chat_df = load_chat()
    with st.form("chat_form", clear_on_submit=True):
        msg = st.text_input("メッセージ")
        if st.form_submit_button("送信"):
            new_msg = pd.DataFrame([{"date": datetime.now().strftime('%Y-%m-%d'), "time": datetime.now().strftime('%H:%M'), "user": st.session_state.user, "message": msg}])
            updated_chat = pd.concat([chat_df, new_msg], ignore_index=True)
            conn.update(worksheet="chat", data=updated_chat)
            st.rerun()
    for _, row in chat_df.sort_index(ascending=False).iterrows():
        st.write(f"**{row['user']}** ({row['time']}): {row['message']}")