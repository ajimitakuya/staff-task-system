import streamlit as st
import pandas as pd
import base64
from datetime import datetime, timedelta, timezone
JST = timezone(timedelta(hours=9))

def now_jst():
    return datetime.now(JST)
from streamlit_gsheets import GSheetsConnection
from streamlit_calendar import calendar

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
    elif file == "calendar":
        return "calendar"
    else:
        raise ValueError(f"未対応のシート名ある: {file}")


def load_db(file):
    s_name = get_sheet_name(file)
    df = conn.read(worksheet=s_name, ttl=0)

    if df is None:
        df = pd.DataFrame()

    # record_status の列名を補正するある
    if file == "record_status" and not df.empty:
        cols = list(df.columns)
        fixed_cols = []
        current_year = None

        for col in cols:
            col_str = str(col).strip()

            if col_str == "resident_name":
                fixed_cols.append("resident_name")
                continue

            if "年" in col_str and "月" in col_str:
                current_year = col_str.split("年")[0]
                fixed_cols.append(col_str)
                continue

            # 「2月」「3月」みたいな省略列を直前の年で補完
            if col_str.endswith("月") and current_year is not None:
                fixed_cols.append(f"{current_year}年{col_str}")
            else:
                fixed_cols.append(col_str)

        df.columns = fixed_cols

    record_status_cols = ["resident_name"]
    for year in range(2025, 2027):
        for month in range(1, 13):
            record_status_cols.append(f"{year}年{month}月")

    calendar_cols = ["id", "title", "start", "end", "user", "memo", "source_type", "source_task_id"]

    expected_cols = {
        "task": ["id", "task", "status", "user", "limit", "priority", "updated_at"],
        "chat": ["date", "time", "user", "message", "image_data"],
        "manual": ["id", "title", "content", "image_data", "created_at"],
        "record_status": record_status_cols,
        "calendar": calendar_cols,
    }

    for col in expected_cols[file]:
        if col not in df.columns:
            df[col] = ""

    return df

def save_db(df, file):
    s_name = get_sheet_name(file)
    conn.update(worksheet=s_name, data=df)

def sync_task_events_to_calendar():
    task_df = load_db("task")
    cal_df = load_db("calendar")

    if task_df is None or task_df.empty:
        return

    if cal_df is None or cal_df.empty:
        cal_df = pd.DataFrame(columns=[
            "id", "title", "start", "end", "user", "memo", "source_type", "source_task_id"
        ])
    else:
        for col in ["id", "title", "start", "end", "user", "memo", "source_type", "source_task_id"]:
            if col not in cal_df.columns:
                cal_df[col] = ""

    task_df = task_df.fillna("")
    cal_df = cal_df.fillna("")

    # 既存の task由来イベントだけ一旦消して作り直すある
    cal_df = cal_df[~cal_df["source_type"].isin(["task_deadline", "task_active"])].copy()

    today = datetime.now().date()

    new_events = []

    # 次のID開始値
    if cal_df.empty:
        next_id = 1
    else:
        try:
            next_id = pd.to_numeric(cal_df["id"], errors="coerce").max()
            next_id = 1 if pd.isna(next_id) else int(next_id) + 1
        except Exception:
            next_id = len(cal_df) + 1

    for _, row in task_df.iterrows():
        task_id = str(row.get("id", "")).strip()
        task_name = str(row.get("task", "")).strip()
        status = str(row.get("status", "")).strip()
        user_name = str(row.get("user", "")).strip()
        limit_str = str(row.get("limit", "")).strip()
        updated_at = str(row.get("updated_at", "")).strip()

        # ① 締切イベント
        if limit_str:
            try:
                limit_date = pd.to_datetime(limit_str).date()
                if limit_date > today:
                    new_events.append({
                        "id": next_id,
                        "title": f"締切：{task_name}",
                        "start": str(limit_date),
                        "end": str(limit_date),
                        "user": user_name,
                        "memo": f"タスク期限 / 状態: {status}",
                        "source_type": "task_deadline",
                        "source_task_id": task_id
                    })
                    next_id += 1
            except Exception:
                pass

        # ② 作業中イベント
        if status == "作業中" and updated_at:
            try:
                active_date = pd.to_datetime(updated_at).date()
                new_events.append({
                    "id": next_id,
                    "title": f"作業中：{task_name}",
                    "start": str(active_date),
                    "end": str(active_date),
                    "user": user_name,
                    "memo": f"現在進行中 / 着手: {updated_at}",
                    "source_type": "task_active",
                    "source_task_id": task_id
                })
                next_id += 1
            except Exception:
                pass

    if new_events:
        add_df = pd.DataFrame(new_events)
        cal_df = pd.concat([cal_df, add_df], ignore_index=True)

    save_db(cal_df, "calendar")

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

page = st.sidebar.radio(
    "メニューを選択してください",
    [
        "① 未着手の任務（掲示板）",
        "② タスクの引き受け・報告",
        "③ 稼働状況・完了履歴",
        "④ チームチャット",
        "⑤ 業務マニュアル",
        "⑥ 日誌入力状況",
        "⑦ 勤務カレンダー",
    ]
)

if st.sidebar.button("ログアウト"):
    del st.session_state.user
    st.rerun()

st.sidebar.divider()
st.sidebar.caption("System Version 2.0")

task_df = load_db("task").fillna("")

my_active = task_df[
    (task_df["status"].astype(str).str.strip() == "作業中") &
    (task_df["user"].astype(str).str.strip() == st.session_state.user)
]

my_todo = task_df[
    (task_df["status"].astype(str).str.strip() == "未着手")
]

st.sidebar.divider()
st.sidebar.markdown("### 📌 マイ状況")

if not my_active.empty:
    st.sidebar.write(f"作業中: {len(my_active)}件")
else:
    st.sidebar.write("作業中: 0件")

st.sidebar.write(f"未着手全体: {len(my_todo)}件")

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
                t_limit = st.date_input("完了期限", now_jst().date())

                if st.form_submit_button("タスクを登録"):
                    if t_name:
                        df = load_db("task")

                        # IDが重複しないように最大値+1で作るある
                        if df.empty:
                            next_id = 1
                        else:
                            ids = pd.to_numeric(df["id"], errors="coerce").dropna()
                            next_id = int(ids.max()) + 1 if not ids.empty else 1

                        new_task = pd.DataFrame([{
                            "id": next_id,
                            "task": t_name,
                            "status": "未着手",
                            "user": "",
                            "limit": str(t_limit),
                            "priority": t_prio,
                            "updated_at": now_jst().strftime("%Y-%m-%d %H:%M")
                        }])

                        save_db(pd.concat([df, new_task], ignore_index=True), "task")
                        sync_task_events_to_calendar()
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
                df.loc[df["id"] == row["id"], ["status", "user", "updated_at"]] = ["作業中", st.session_state.user, now_jst().strftime('%Y-%m-%d %H:%M')]
                save_db(df, "task")
                sync_task_events_to_calendar()
                st.rerun()
        st.divider()
        st.subheader("⚡ 現在対応中のタスク")
        my_tasks = df[(df["status"] == "作業中") & (df["user"] == st.session_state.user)]
        if my_tasks.empty:
            st.write("現在、対応中のタスクはありません。")
        for _, row in my_tasks.iterrows():
            if st.button(f"✅ {row['task']} の完了を報告する", key=f"done_{row['id']}", type="primary"):
                df.loc[df["id"] == row["id"], ["status", "updated_at"]] = ["完了", now_jst().strftime('%Y-%m-%d %H:%M')]
                save_db(df, "task")
                sync_task_events_to_calendar()
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

        if df is None or df.empty:
            df = pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])
        else:
            for col in ["id", "task", "status", "user", "limit", "priority", "updated_at"]:
                if col not in df.columns:
                    df[col] = ""

        df = df.fillna("")

        # ------------------------------------------
        # 期限アラート
        # ------------------------------------------
        st.subheader("⏰ 期限アラート")

        today = now_jst().date()
        tomorrow = today + timedelta(days=1)

        deadline_alerts = []

        for _, row in df.iterrows():
            status = str(row.get("status", "")).strip()
            task_name = str(row.get("task", "")).strip()
            limit_str = str(row.get("limit", "")).strip()

            if status == "完了":
                continue

            if limit_str:
                try:
                    limit_date = pd.to_datetime(limit_str).date()

                    if limit_date < today:
                        deadline_alerts.append(("期限切れ", task_name, str(limit_date)))
                    elif limit_date == today:
                        deadline_alerts.append(("今日期限", task_name, str(limit_date)))
                    elif limit_date == tomorrow:
                        deadline_alerts.append(("明日期限", task_name, str(limit_date)))
                except Exception:
                    pass

        if deadline_alerts:
            for kind, task_name, d in deadline_alerts:
                if kind == "期限切れ":
                    st.error(f"🔴 {kind}: {task_name}（{d}）")
                elif kind == "今日期限":
                    st.warning(f"🟠 {kind}: {task_name}（{d}）")
                else:
                    st.info(f"🟡 {kind}: {task_name}（{d}）")
        else:
            st.write("期限アラートはないある。")

        st.divider()

        # ------------------------------------------
        # 現在稼働中のメンバー
        # ------------------------------------------
        st.subheader("🏃 現在稼働中のメンバー")

        active = df[df["status"].astype(str).str.strip() == "作業中"].copy()

        if active.empty:
            st.write("現在稼働中のメンバーはいません。")
        else:
            for _, row in active.iterrows():
                st.info(f"👤 **{row['user']}**： {row['task']} （着手時刻：{row['updated_at']}）")

        st.divider()

        # ------------------------------------------
        # 完了済みのタスク（過去7日間）
        # ------------------------------------------
        st.subheader("✅ 完了済みのタスク（過去7日間）")

        one_week_ago = now_jst() - timedelta(days=7)

        done_rows = []

        for _, row in df.iterrows():
            status = str(row.get("status", "")).strip()
            updated_at = str(row.get("updated_at", "")).strip()

            if status != "完了" or not updated_at:
                continue

            try:
                done_dt = pd.to_datetime(updated_at)
                if done_dt >= one_week_ago:
                    done_rows.append(row)
            except Exception:
                pass

        if done_rows:
            done_df = pd.DataFrame(done_rows)
            done_df = done_df.sort_values("updated_at", ascending=False)
            st.table(done_df[["updated_at", "user", "task"]])
        else:
            st.write("直近1週間以内の完了履歴はありません。")

    show_status_page()

# ==========================================
# ④ チームチャット（画像添付対応ある！）
# ==========================================
elif page == "④ チームチャット":
    @st.fragment(run_every=15)
    def show_chat_page():
        st.title("💬 業務連絡チャット")
        chat_df = load_db("chat")
        today = now_jst().strftime("%Y-%m-%d")

        # 必要列を保証
        if chat_df is None or chat_df.empty:
            chat_df = pd.DataFrame(columns=["date", "time", "user", "message", "image_data"])
        else:
            for col in ["date", "time", "user", "message", "image_data"]:
                if col not in chat_df.columns:
                    chat_df[col] = ""

        mode = st.radio("表示切り替え", ["本日の連絡", "ログ倉庫（過去分）"], horizontal=True)

        if mode == "本日の連絡":
            with st.form("chat_form", clear_on_submit=True):
                msg = st.text_area("業務連絡を入力してください")
                uploaded_image = st.file_uploader(
                    "画像を添付（ドラッグ＆ドロップ可）",
                    type=["png", "jpg", "jpeg"],
                    key="chat_image_uploader"
                )

                if uploaded_image is not None:
                    st.image(uploaded_image, caption="送信前プレビュー", use_container_width=True)

                if st.form_submit_button("送信"):
                    if msg or uploaded_image is not None:
                        image_data = ""

                        if uploaded_image is not None:
                            file_bytes = uploaded_image.read()
                            image_data = base64.b64encode(file_bytes).decode("utf-8")

                        new_msg = pd.DataFrame([{
                            "date": today,
                            "time": datetime.now().strftime("%H:%M"),
                            "user": st.session_state.user,
                            "message": msg,
                            "image_data": image_data
                        }])

                        save_db(pd.concat([chat_df, new_msg], ignore_index=True), "chat")
                        st.success("送信したある！")
                        st.rerun()
                    else:
                        st.error("メッセージか画像のどちらかを入れてほしいある。")

            st.divider()

            display_df = chat_df[chat_df["date"].astype(str) == today].copy()

            if display_df.empty:
                st.write("本日の連絡事項はありません。")

            display_df = display_df.fillna("")

            for _, row in display_df.sort_values("time", ascending=False).iterrows():
                user_name = str(row.get("user", ""))
                msg_text = str(row.get("message", ""))
                time_text = str(row.get("time", ""))
                image_data = str(row.get("image_data", ""))

                st.markdown(f"**{user_name}** ({time_text})")

                if msg_text.strip():
                    st.write(msg_text)

                if image_data.strip():
                    try:
                        image_bytes = base64.b64decode(image_data)
                        st.image(image_bytes, caption="添付画像", use_container_width=True)
                    except Exception:
                        st.warning("画像の読み込みに失敗したある。")

                st.divider()

        else:
            st.subheader("📚 過去の連絡ログ")

            if chat_df.empty:
                st.write("過去ログはありません。")
                return

            dates = sorted(
                [str(d).strip() for d in chat_df["date"].dropna().tolist() if str(d).strip()],
                reverse=True
            )

            if not dates:
                st.write("過去ログはありません。")
                return

            target_date = st.selectbox("参照する日付を選択してください", dates)

            if target_date:
                log_df = chat_df[chat_df["date"].astype(str) == target_date].copy().fillna("")

                for _, row in log_df.sort_values("time", ascending=False).iterrows():
                    user_name = str(row.get("user", ""))
                    msg_text = str(row.get("message", ""))
                    time_text = str(row.get("time", ""))
                    image_data = str(row.get("image_data", ""))

                    st.markdown(f"**{user_name}** ({time_text})")

                    if msg_text.strip():
                        st.write(msg_text)

                    if image_data.strip():
                        try:
                            image_bytes = base64.b64decode(image_data)
                            st.image(image_bytes, caption="添付画像", use_container_width=True)
                        except Exception:
                            st.warning("画像の読み込みに失敗したある。")

                    st.divider()

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
                            "created_at": now_jst().strftime("%Y-%m-%d")
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
# ⑥ 日誌入力状況（年つき横表・Excel風ある！）
# ==========================================
elif page == "⑥ 日誌入力状況":
    @st.fragment(run_every=60)
    def show_record_status_page():
        st.title("📝 日誌入力状況管理")

        # 表示したい年の範囲
        start_year = 2025
        end_year = 2026

        # 年月列を作るある
        month_cols = []
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                month_cols.append(f"{year}年{month}月")

        required_cols = ["resident_name"] + month_cols

        r_df = load_db("record_status")

        # 空でも最低限の形に整えるある
        if r_df is None or r_df.empty:
            r_df = pd.DataFrame(columns=required_cols)
        else:
            for col in required_cols:
                if col not in r_df.columns:
                    r_df[col] = ""
            r_df = r_df[required_cols].copy()

        # ==========================================
        # 利用者一括追加
        # ==========================================
        with st.expander("👤 利用者をまとめて追加する"):
            bulk_names = st.text_area(
                "名前を入力（改行ごとに1人。Excelの縦列コピーもOK）",
                placeholder="荒木 和也\n石田 愛子\n岩城 勝也"
            )

            if st.button("利用者を追加する", key="add_residents_button"):
                names = [
                    n.strip()
                    for n in str(bulk_names).replace("\r\n", "\n").split("\n")
                    if n.strip()
                ]

                if not names:
                    st.error("名前を1人以上入力してほしいある。")
                else:
                    existing_names = set(
                        str(x).strip()
                        for x in r_df["resident_name"].dropna().tolist()
                        if str(x).strip()
                    )

                    new_rows = []
                    skipped = []

                    for name in names:
                        if name in existing_names:
                            skipped.append(name)
                        else:
                            row = {"resident_name": name}
                            for m in month_cols:
                                row[m] = ""
                            new_rows.append(row)

                    if new_rows:
                        add_df = pd.DataFrame(new_rows)
                        r_df = pd.concat([r_df, add_df], ignore_index=True)
                        save_db(r_df, "record_status")

                        if skipped:
                            st.success(
                                f"{len(new_rows)}人追加したある。重複スキップ: {', '.join(skipped)}"
                            )
                        else:
                            st.success(f"{len(new_rows)}人追加したある。")
                        st.rerun()
                    else:
                        st.warning("全員すでに登録済みある。")

        st.divider()
        st.caption("各セルに「未入力」「15日まで」「完了」など自由に入力できるある。")

        # data_editorで落ちないように、全部文字列にそろえるある
        r_df = r_df.fillna("")

        for col in required_cols:
            if col not in r_df.columns:
                r_df[col] = ""
            r_df[col] = r_df[col].astype(str)

        # 列設定を自動生成
        column_config = {
            "resident_name": st.column_config.TextColumn("氏名", width="medium")
        }
        for col in month_cols:
            column_config[col] = st.column_config.TextColumn(col, width="small")

        # 表示用
        edited_df = st.data_editor(
            r_df,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            column_config=column_config,
            key="record_status_editor"
        )

        # 保存ボタン
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("💾 表を保存する", type="primary", key="save_record_status_button"):
                edited_df = edited_df.fillna("")

                # 氏名空欄の行は除外
                edited_df = edited_df[
                    edited_df["resident_name"].astype(str).str.strip() != ""
                ].copy()

                # 必要列だけにそろえる
                for col in required_cols:
                    if col not in edited_df.columns:
                        edited_df[col] = ""
                    edited_df[col] = edited_df[col].astype(str)

                edited_df = edited_df[required_cols]

                save_db(edited_df, "record_status")
                st.success("保存したある！")
                st.rerun()

    show_record_status_page()

# ==========================================
# ⑦ 勤務カレンダー
# ==========================================
elif page == "⑦ 勤務カレンダー":
    sync_task_events_to_calendar()
    @st.fragment(run_every=60)
    def show_calendar_page():
        st.title("📅 勤務カレンダー")

        cal_df = load_db("calendar")
        task_df = load_db("task")

        # calendarシートの最低列を保証
        if cal_df is None or cal_df.empty:
            cal_df = pd.DataFrame(columns=["id", "title", "start", "end", "user", "memo", "source_type", "source_task_id"])
        else:
            for col in ["id", "title", "start", "end", "user", "memo", "source_type", "source_task_id"]:
                if col not in cal_df.columns:
                    cal_df[col] = ""

        # taskシートの最低列を保証
        if task_df is None or task_df.empty:
            task_df = pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])
        else:
            for col in ["id", "task", "status", "user", "limit", "priority", "updated_at"]:
                if col not in task_df.columns:
                    task_df[col] = ""

        with st.expander("➕ 予定を追加する"):
            with st.form("calendar_form"):
                title = st.text_input("予定名")
                start_date = st.date_input("開始日")
                end_date = st.date_input("終了日")
                user_name = st.text_input("担当者", value=st.session_state.user)
                memo = st.text_area("メモ")

                if st.form_submit_button("予定を保存する"):
                    if title:
                        if cal_df.empty:
                            next_id = 1
                        else:
                            try:
                                next_id = pd.to_numeric(cal_df["id"], errors="coerce").max()
                                next_id = 1 if pd.isna(next_id) else int(next_id) + 1
                            except Exception:
                                next_id = len(cal_df) + 1

                        new_row = pd.DataFrame([{
                            "id": next_id,
                            "title": title,
                            "start": str(start_date),
                            "end": str(end_date),
                            "user": user_name,
                            "memo": memo,
                            "source_type": "manual",
                            "source_task_id": ""
                        }])

                        save_db(pd.concat([cal_df, new_row], ignore_index=True), "calendar")
                        st.success("予定を保存したある！")
                        st.rerun()
                    else:
                        st.error("予定名を入れてほしいある。")

        st.divider()

        events = []
        event_ids = set()

        # ------------------------------------------
        # ① 手入力の予定（calendarシート）
        # ------------------------------------------
        display_cal_df = cal_df.fillna("")

        for _, row in display_cal_df.iterrows():
            title = str(row.get("title", "")).strip()
            start = str(row.get("start", "")).strip()
            end = str(row.get("end", "")).strip()
            user_name = str(row.get("user", "")).strip()
            memo = str(row.get("memo", "")).strip()
            source_type = str(row.get("source_type", "")).strip()

            # task由来イベントは下で task_df から作るので、ここでは表示しないある
            if source_type in ["task_deadline", "task_active"]:
                continue

            if title and start:
                short_title = title if len(title) <= 10 else title[:10] + "…"
                event_title = short_title if not user_name else f"{user_name}：{short_title}"
                event_id = f"manual_{row.get('id', '')}"

                if event_id not in event_ids:
                    events.append({
                        "id": event_id,
                        "title": event_title,
                        "start": start,
                        "end": end if end else start,
                        "color": "#3788d8",
                        "extendedProps": {
                            "memo": memo,
                            "user": user_name,
                            "source_type": "manual",
                            "full_title": title,
                        }
                    })
                    event_ids.add(event_id)

        # ------------------------------------------
        # ② taskシートから締切イベントを自動生成
        # ------------------------------------------
        display_task_df = task_df.fillna("")
        today = now_jst().date()

        for _, row in display_task_df.iterrows():
            task_id = str(row.get("id", "")).strip()
            task_name = str(row.get("task", "")).strip()
            status = str(row.get("status", "")).strip()
            user_name = str(row.get("user", "")).strip()
            priority = str(row.get("priority", "")).strip()
            limit_str = str(row.get("limit", "")).strip()
            updated_at = str(row.get("updated_at", "")).strip()

            # 締切が今日以降なら出す
            if limit_str:
                try:
                    limit_date = pd.to_datetime(limit_str).date()
                    if limit_date >= today:
                        event_id = f"deadline_{task_id}"
                        short_title = task_name if len(task_name) <= 10 else task_name[:10] + "…"

                        if event_id not in event_ids:
                            events.append({
                                "id": event_id,
                                "title": f"締切：{short_title}",
                                "start": str(limit_date),
                                "end": str(limit_date),
                                "color": "#e74c3c",
                                "extendedProps": {
                                    "memo": f"優先度: {priority} / 状態: {status}",
                                    "user": user_name,
                                    "source_type": "task_deadline",
                                    "full_title": task_name,
                                }
                            })
                            event_ids.add(event_id)
                except Exception:
                    pass

            # 作業中なら開始日で出す
            if status == "作業中" and updated_at:
                try:
                    active_date = pd.to_datetime(updated_at).date()
                    event_id = f"active_{task_id}"
                    short_title = task_name if len(task_name) <= 10 else task_name[:10] + "…"

                    if event_id not in event_ids:
                        events.append({
                            "id": event_id,
                            "title": f"作業中：{short_title}",
                            "start": str(active_date),
                            "end": str(active_date),
                            "color": "#f39c12",
                            "extendedProps": {
                                "memo": f"着手: {updated_at}",
                                "user": user_name,
                                "source_type": "task_active",
                                "full_title": task_name,
                            }
                        })
                        event_ids.add(event_id)
                except Exception:
                    pass

        calendar_options = {
            "initialView": "dayGridMonth",
            "locale": "ja",
            "height": "auto",
            "dayMaxEvents": 3,
            "moreLinkClick": "popover",
            "displayEventTime": False,
            "eventDisplay": "block",
            "headerToolbar": {
                "left": "prev,next today",
                "center": "title",
                "right": "dayGridMonth,timeGridWeek,listWeek"
            }
        }

        state = calendar(
            events=events,
            options=calendar_options,
            key="work_calendar"
        )



        st.divider()
        st.subheader("📋 登録済み予定一覧")

        if not display_cal_df.empty:
            st.dataframe(
                display_cal_df[["start", "end", "user", "title", "memo"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("手入力の予定はまだないある。")

        # ------------------------------------------
        # 選択日の予定一覧
        # ------------------------------------------
        st.divider()
        st.subheader("🗓️ 選択日の予定一覧")

        selected_date = None

        if state and state.get("dateClick"):
            selected_date = str(state["dateClick"].get("date", ""))[:10]
        elif state and state.get("eventClick"):
            selected_date = str(state["eventClick"]["event"].get("start", ""))[:10]

        if selected_date:
            day_events = []

            for ev in events:
                ev_start = str(ev.get("start", ""))[:10]
                if ev_start == selected_date:
                    ext = ev.get("extendedProps", {})
                    source_type = ext.get("source_type", "")

                    type_label = "手入力予定"
                    if source_type == "task_deadline":
                        type_label = "締切タスク"
                    elif source_type == "task_active":
                        type_label = "作業中タスク"

                    day_events.append({
                        "種類": type_label,
                        "予定名": ext.get("full_title", ev.get("title", "")),
                        "担当者": ext.get("user", ""),
                        "メモ": ext.get("memo", "")
                    })

            if day_events:
                st.dataframe(
                    pd.DataFrame(day_events),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info(f"{selected_date} の予定はないある。")
        else:
            st.caption("日付か予定をクリックすると、その日の一覧を下に表示するある。")

        # ------------------------------------------
        # クリックした予定の詳細
        # ------------------------------------------
        if state and state.get("eventClick"):
            clicked = state["eventClick"]["event"]
            ext = clicked.get("extendedProps", {})

            st.divider()
            st.subheader("🔍 選択中の予定")

            full_title = ext.get("full_title", "") or clicked.get("title", "")
            source_type = ext.get("source_type", "")

            type_label = "手入力予定"
            if source_type == "task_deadline":
                type_label = "締切タスク"
            elif source_type == "task_active":
                type_label = "作業中タスク"

            st.write(f"**種類**: {type_label}")
            st.write(f"**予定名**: {full_title}")
            st.write(f"**開始**: {clicked.get('start', '')}")
            st.write(f"**終了**: {clicked.get('end', '')}")

            if ext.get("user"):
                st.write(f"**担当者**: {ext.get('user')}")

            if ext.get("memo"):
                st.write(f"**メモ**: {ext.get('memo')}")

    show_calendar_page()