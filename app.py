import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from streamlit_gsheets import GSheetsConnection

# --- ページ基本設定 ---
st.set_page_config(page_title="作業管理システム", layout="wide")


# --- 🔌 スプレッドシート接続設定（最新の最強版ある！） ---
conn = st.connection("gsheets", type=GSheetsConnection)

def load_db(file):
    # 💡 コツは「URL」を書かないことある！
    # Secretsにある設定と共有設定（image_24fa99.png）をフル活用する公式の書き方ある！
    s_name = "task" if "task" in file else "chat"
    return conn.read(worksheet=s_name, ttl="0s")

def save_db(df, file):
    s_name = "task" if "task" in file else "chat"
    # 書き込みも「ワークシート名」だけで指定するのが、Googleを怒らせない秘訣ある！
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
                        ["① 未着手の任務（掲示板）", "② タスクの引き受け・報告", "③ 稼働状況・完了履歴", "④ チームチャット"])

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
# ⑤ 業務マニュアル (画像アップロード対応版！)
# ==========================================
elif page == "⑤ 業務マニュアル":
    @st.fragment(run_every=60)
    def show_manual_page():
        st.title("📚 業務マニュアル")
        
        with st.expander("📝 新しいマニュアルを作成する"):
            with st.form("manual_form"):
                title = st.text_input("マニュアルのタイトル")
                content = st.text_area("手順の説明")
                
                # 💡 ここがD&Dエリアある！
                uploaded_file = st.file_uploader("写真をドラッグ&ドロップ、または選択してください", type=['png', 'jpg', 'jpeg'])
                
                if st.form_submit_button("保存する"):
                    if title and content:
                        # 画像がある場合は、バイナリデータのまま一旦セッションやリストに持たせる工夫が必要あるが、
                        # シンプルにやるならまずはテキストと「画像あり」のフラグを保存するある
                        m_df = load_db("manual")
                        
                        # 本来は画像をストレージに保存してURLを発行するのが理想あるが、
                        # まずはタイトルと内容を保存して、動くか確認するある！
                        new_m = pd.DataFrame([{
                            "id": len(m_df)+1, 
                            "title": title, 
                            "content": content, 
                            "created_at": datetime.now().strftime('%Y-%m-%d')
                        }])
                        save_db(pd.concat([m_df, new_m], ignore_index=True), "manual")
                        
                        # 💡 アップロードされた画像がある場合は、画面に即座に表示して確認できるようにするある
                        if uploaded_file:
                            st.image(uploaded_file, caption="アップロードされた画像", use_container_width=True)
                            
                        st.success("マニュアルを保存したある！")
                        st.rerun()

        st.divider()
        m_df = load_db("manual")
        if not m_df.empty:
            for _, row in m_df.iterrows():
                with st.expander(f"📖 {row['title']} (作成日: {row['created_at']})"):
                    st.write(row['content'])
                    # 今後はここに画像を表示する処理を追加していくある！
        else:
            st.info("マニュアルはまだ登録されてないある。")
            
    show_manual_page()

# ==========================================
# ⑥ 日誌入力状況 (利用者も月も自動取得版！)
# ==========================================
elif page == "⑥ 日誌入力状況":
    @st.fragment(run_every=60)
    def show_record_status_page():
        st.title("📝 日誌入力状況管理")
        
        r_df = load_db("record_status")
        
        # 💡 ここがポイント！既存のデータから「月」と「名前」のリストを自動で作るある
        existing_months = sorted(r_df["month"].unique().tolist()) if not r_df.empty else ["4月"]
        existing_names = sorted(r_df["resident_name"].unique().tolist()) if not r_df.empty else []

        with st.expander("👤 新しく項目を追加する"):
            # 名前も過去の入力から選べるし、新しい名前も入力できるある！
            name_mode = st.radio("名前の入力方法", ["既存の利用者から選ぶ", "新しい名前を入力する"], horizontal=True)
            if name_mode == "既存の利用者から選ぶ" and existing_names:
                new_name = st.selectbox("利用者名を選択", existing_names)
            else:
                new_name = st.text_input("新しい利用者名を入力")

            # 月も自由に入力できるように text_input にしたある！
            month_mode = st.radio("月の入力方法", ["既存の月から選ぶ", "新しい月を入力する"], horizontal=True)
            if month_mode == "既存の月から選ぶ" and existing_months:
                target_month = st.selectbox("対象月を選択", existing_months)
            else:
                target_month = st.text_input("新しい月を入力（例：3月）")

            if st.button("表に追加する"):
                if new_name and target_month:
                    # すでに同じ名前と月の組み合わせがないかチェックするある
                    duplicate = r_df[(r_df["resident_name"] == new_name) & (r_df["month"] == target_month)]
                    if not duplicate.empty:
                        st.error(f"⚠️ {new_name}さんの{target_month}分は既に存在するある！")
                    else:
                        new_r = pd.DataFrame([{"id": len(r_df)+1, "resident_name": new_name, "month": target_month, "status": "未入力"}])
                        save_db(pd.concat([r_df, new_r], ignore_index=True), "record_status")
                        st.success(f"✨ {new_name}さんの{target_month}分を追加したある！")
                        st.rerun()

        st.divider()
        # --- (以下、表の表示部分はそのまま流用ある！) ---
        if not r_df.empty:
            for idx, row in r_df.iterrows():
                col1, col2, col3 = st.columns([2, 1, 2])
                with col1:
                    st.write(f"**{row['resident_name']}** ({row['month']})")
                with col2:
                    color = "🔴" if row['status'] == "未入力" else "🟢"
                    st.write(f"{color} {row['status']}")
                with col3:
                    if row['status'] == "未入力":
                        if st.button("✅ 入力完了に更新", key=f"r_{row['id']}"):
                            r_df.loc[r_df["id"] == row["id"], "status"] = "入力済"
                            save_db(r_df, "record_status")
                            st.rerun()
        else:
            st.info("まだ利用者データがないある。上のボタンから追加してほしいある！")
            
    show_record_status_page()