import base64
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from streamlit_gsheets import GSheetsConnection

# --- ページ基本設定 ---
st.set_page_config(page_title="作業管理システム", layout="wide")


# --- 🔌 スプレッドシート接続設定（最新の最強版ある！） ---
conn = st.connection("gsheets", type=GSheetsConnection)

def get_sheet_name(file):
    if file == "task":
        return "task"
    elif file == "chat":
        return "chat"
    elif file == "manual":
        return "manual"
    elif file == "record_status":
        return "record_status"
    else:
        raise ValueError(f"未対応のシート名ある: {file}")


def load_db(file):
    s_name = get_sheet_name(file)
    df = conn.read(worksheet=s_name, ttl="0s")

    if df is None:
        df = pd.DataFrame()

    expected_cols = {
        "task": ["id", "task", "status", "user", "limit", "priority", "updated_at"],
        "chat": ["date", "time", "user", "message"],
        "manual": ["id", "title", "content", "image_data", "created_at"],
        "record_status": ["id", "resident_name", "month", "status"],
    }

    for col in expected_cols[file]:
        if col not in df.columns:
            df[col] = ""

    return df


def save_db(df, file):
    s_name = get_sheet_name(file)
    conn.update(worksheet=s_name, data=df)
# ==========================================
# 🔑 ユーザー認証 (ここはそのままある)
# ==========================================
if 'user' not in st.session_state:
    st.markdown("<style>[data-testid='stSidebarNav'] {display: none;}</style>", unsafe_allow_html=True)
    st.title("🛡️ 業務システム・ログイン")
    st.warning("### 名前を選んでログインしてください💻")
    user_list = ["--- 選択してください ---", "木村 由美", "秋吉 幸雄", "安心院 拓也", "粟田 絵利菜", "小宅 正嗣", "土居 容子", "中本 匡", "中本 文代", "伴 法子", "栁川 幸恵", "山口 晴彦"]
    user = st.selectbox("担当者を選択してください", user_list)
    if user != "--- 選択してください ---":
        if st.button("システムへログイン", use_container_width=True):
            st.session_state.user = user
            st.rerun()
    st.stop()

# ==========================================
# 🏠 メインメニュー
# ==========================================
st.sidebar.markdown(f"### 👤 ログイン中：\n## {st.session_state.user}")

page = st.sidebar.radio("メニューを選択してください", 
                        ["① 未着手の任務（掲示板）", "② タスクの引き受け・報告", "③ 稼働状況・完了履歴", 
                         "④ チームチャット", "⑤ 業務マニュアル", "⑥ 日誌入力状況"]) # 👈 ここを追加するある！

if st.sidebar.button("ログアウト"):
    del st.session_state.user
    st.rerun()

st.sidebar.divider()
st.sidebar.caption("System Version 2.0")

# ==========================================
# ① 未着手の任務（掲示板）
# ==========================================
if page == "① 未着手の任務（掲示板）":
    @st.fragment(run_every=60)
    def show_task_board_page():
        st.title("📋 未着手タスク一覧")
        st.write("現在、依頼されている業務の一覧です。新しいタスクを登録することも可能です。")

        with st.expander("➕ 新規タスクを登録する"):
            with st.form("task_form"):
                t_name = st.text_input("タスク名")
                t_prio = st.select_slider("緊急度", options=["通常", "重要", "至急"])
                t_limit = st.date_input("完了期限", datetime.now())
                if st.form_submit_button("タスクを登録"):
                    if t_name:
                        df = load_db("task") 
                        new_task = pd.DataFrame([{"id": len(df)+1, "task": t_name, "status": "未着手", 
                                                 "user": "", "limit": str(t_limit), "priority": t_prio, 
                                                 "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M')}])
                        save_db(pd.concat([df, new_task], ignore_index=True), "task")
                        st.success("タスクを登録しました。")
                        st.rerun()
                    else:
                        st.error("タスク名を入力してください。")

        df = load_db("task")
        todo = df[df["status"] == "未着手"].copy()
        if not todo.empty:
            prio_map = {"至急": 0, "重要": 1, "通常": 2}
            todo["p_val"] = todo["priority"].map(prio_map)
            st.dataframe(todo.sort_values("p_val")[["priority", "limit", "task"]], use_container_width=True, hide_index=True)
        else:
            st.info("現在、未着手のタスクはありません。")
    show_task_board_page()

# ==========================================
# ② タスクの引き受け・報告
# ==========================================
elif page == "② タスクの引き受け・報告":
    @st.fragment(run_every=60)
    def show_my_tasks_page():
        st.title("🎯 タスク管理")
        df = load_db("task")
        
        st.subheader("📦 新しくタスクを引き受ける")
        todo = df[df["status"] == "未着手"]
        if todo.empty:
            st.write("引き受け可能なタスクはありません。")
        for _, row in todo.iterrows():
            p_symbol = "🔴 [至急]" if row['priority'] == "至急" else "🟡 [重要]" if row['priority'] == "重要" else "⚪ [通常]"
            if st.button(f"{p_symbol} {row['task']} (期限:{row['limit']}) を開始する", key=f"get_{row['id']}"):
                df.loc[df["id"] == row["id"], ["status", "user", "updated_at"]] = ["作業中", st.session_state.user, datetime.now().strftime('%Y-%m-%d %H:%M')]
                save_db(df, "task")
                st.rerun()

        st.divider()
        st.subheader("⚡ 現在対応中のタスク")
        my_tasks = df[(df["status"] == "作業中") & (df["user"] == st.session_state.user)]
        if my_tasks.empty:
            st.write("現在、対応中のタスクはありません。")
        for _, row in my_tasks.iterrows():
            if st.button(f"✅ {row['task']} の完了を報告する", key=f"done_{row['id']}", type="primary"):
                df.loc[df["id"] == row["id"], ["status", "updated_at"]] = ["完了", datetime.now().strftime('%Y-%m-%d %H:%M')]
                save_db(df, "task")
                st.rerun()
    show_my_tasks_page()

# ==========================================
# ③ 稼働状況・完了履歴
# ==========================================
elif page == "③ 稼働状況・完了履歴":
    @st.fragment(run_every=60)
    def show_status_page():
        st.title("📊 チーム稼働状況")
        df = load_db("task")
        
        st.subheader("🏃 現在稼働中のメンバー")
        active = df[df["status"] == "作業中"]
        if active.empty:
            st.write("現在稼働中のメンバーはいません。")
        else:
            for _, row in active.iterrows():
                st.info(f"👤 **{row['user']}**： {row['task']} （着手時刻：{row['updated_at']}）")

        st.divider()
        st.subheader("✅ 完了済みのタスク（過去7日間）")
        one_week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        # 完了済みデータのフィルタリング
        done = df[(df["status"] == "完了") & (df.get("updated_at", "").str[:10] >= one_week_ago)]
        if not done.empty:
            st.table(done.sort_values("updated_at", ascending=False)[["updated_at", "user", "task"]])
        else:
            st.write("直近1週間以内の完了履歴はありません。")
    show_status_page()

# ==========================================
# ④ チームチャット
# ==========================================
elif page == "④ チームチャット":
    @st.fragment(run_every=15)
    def show_chat_page():
        st.title("💬 業務連絡チャット")
        chat_df = load_db("chat")
        today = datetime.now().strftime('%Y-%m-%d')
        
        mode = st.radio("表示切り替え", ["本日の連絡", "ログ倉庫（過去分）"], horizontal=True)

        if mode == "本日の連絡":
            with st.form("chat_form", clear_on_submit=True):
                msg = st.text_input("業務連絡を入力してください")
                if st.form_submit_button("送信"):
                    if msg:
                        new_msg = pd.DataFrame([{"date": today, "time": datetime.now().strftime('%H:%M'), 
                                                 "user": st.session_state.user, "message": msg}])
                        save_db(pd.concat([chat_df, new_msg], ignore_index=True), "chat")
                        st.rerun()
            
            st.divider()
            display_df = chat_df[chat_df["date"] == today]
            if display_df.empty:
                st.write("本日の連絡事項はありません。")
            for _, row in display_df.sort_values("time", ascending=False).iterrows():
                st.markdown(f"**{row['user']}** ({row['time']}):  \n{row['message']}")
        else:
            st.subheader("📚 過去の連絡ログ")
            dates = chat_df["date"].unique()
            target_date = st.selectbox("参照する日付を選択してください", sorted(dates, reverse=True))
            if target_date:
                st.table(chat_df[chat_df["date"] == target_date][["time", "user", "message"]])
    show_chat_page()

# ==========================================
# ⑤ 業務マニュアル (画像D&D対応ある！)
# ==========================================
elif page == "⑤ 業務マニュアル":
    @st.fragment(run_every=60)
    def show_manual_page():
        st.title("📚 業務マニュアル")

        with st.expander("📝 新しいマニュアルを作成する"):
            with st.form("manual_form"):
                title = st.text_input("マニュアルのタイトル")
                content = st.text_area("手順の説明")

                # D&D対応
                uploaded_file = st.file_uploader(
                    "写真をドラッグ&ドロップ",
                    type=["png", "jpg", "jpeg"]
                )

                if uploaded_file is not None:
                    st.image(uploaded_file, caption="アップロード画像プレビュー", use_container_width=True)

                if st.form_submit_button("保存する"):
                    if title and content:
                        m_df = load_db("manual")

                        image_data = ""
                        if uploaded_file is not None:
                            file_bytes = uploaded_file.read()
                            image_data = base64.b64encode(file_bytes).decode("utf-8")

                        new_m = pd.DataFrame([{
                            "id": len(m_df) + 1,
                            "title": title,
                            "content": content,
                            "image_data": image_data,
                            "created_at": datetime.now().strftime("%Y-%m-%d")
                        }])

                        save_db(pd.concat([m_df, new_m], ignore_index=True), "manual")
                        st.success("マニュアルを保存したある！")
                        st.rerun()
                    else:
                        st.error("タイトルと説明は必須ある。")

        st.divider()
        m_df = load_db("manual")

        if not m_df.empty:
            for _, row in m_df.iterrows():
                title = row.get("title", "")
                content = row.get("content", "")
                created_at = row.get("created_at", "")
                image_data = row.get("image_data", "")

                with st.expander(f"📖 {title} (作成日: {created_at})"):
                    st.write(content)

                    if isinstance(image_data, str) and image_data.strip():
                        try:
                            image_bytes = base64.b64decode(image_data)
                            st.image(image_bytes, caption="添付画像", use_container_width=True)
                        except Exception:
                            st.warning("画像データの読み込みに失敗したある。")
        else:
            st.info("マニュアルはまだ登録されてないある。")

    show_manual_page()

# ==========================================
# ⑥ 日誌入力状況 (利用者も月も自由自在ある！)
# ==========================================
elif page == "⑥ 日誌入力状況":
    @st.fragment(run_every=60)
    def show_record_status_page():
        st.title("📝 日誌入力状況管理")
        r_df = load_db("record_status")
        
        # 💡 既存のデータからリストを作るある
        existing_months = sorted(r_df["month"].unique().tolist()) if not r_df.empty else ["4月"]
        existing_names = sorted(r_df["resident_name"].unique().tolist()) if not r_df.empty else []

        with st.expander("👤 新しく項目を追加する"):
            # 名前と月の入力方法を選べるようにしたある！
            col_a, col_b = st.columns(2)
            with col_a:
                name_mode = st.radio("名前", ["既存から選ぶ", "新規入力"], horizontal=True)
                new_name = st.selectbox("選択", existing_names) if name_mode == "既存から選ぶ" and existing_names else st.text_input("名前を入力")
            with col_b:
                month_mode = st.radio("対象月", ["既存から選ぶ", "新規入力"], horizontal=True)
                target_month = st.selectbox("月を選択", existing_months) if month_mode == "既存から選ぶ" and existing_months else st.text_input("月を入力 (例: 3月)")

            if st.button("表に追加する"):
                if new_name and target_month:
                    duplicate = r_df[(r_df["resident_name"] == new_name) & (r_df["month"] == target_month)]
                    if not duplicate.empty:
                        st.error("⚠️ 既に登録済みある！")
                    else:
                        new_r = pd.DataFrame([{"id": len(r_df)+1, "resident_name": new_name, "month": target_month, "status": "未入力"}])
                        save_db(pd.concat([r_df, new_r], ignore_index=True), "record_status")
                        st.success("追加したある！")
                        st.rerun()

        st.divider()
        if not r_df.empty:
            for idx, row in r_df.iterrows():
                col1, col2, col3 = st.columns([2, 1, 2])
                with col1: st.write(f"**{row['resident_name']}** ({row['month']})")
                with col2: st.write(f"{'🔴' if row['status'] == '未入力' else '🟢'} {row['status']}")
                with col3:
                    if row['status'] == "未入力":
                        if st.button("✅ 完了", key=f"r_{row['id']}"):
                            r_df.loc[r_df["id"] == row["id"], "status"] = "入力済"
                            save_db(r_df, "record_status")
                            st.rerun()
        else:
            st.info("データがないある。")
    show_record_status_page()