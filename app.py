import streamlit as st
import pandas as pd
import base64
import time
import random
from datetime import datetime, timedelta, timezone
from streamlit_gsheets import GSheetsConnection
from streamlit_calendar import calendar
JST = timezone(timedelta(hours=9))
def now_jst():
    return datetime.now(JST)

# --- ページ基本設定 ---
st.set_page_config(page_title="作業管理システム", layout="wide")

# --- 🔌 スプレッドシート接続設定 ---
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
    elif file == "active_users":
        return "active_users"
    elif file == "resident_master":
        return "resident_master"
    elif file == "resident_schedule":
        return "resident_schedule"
    elif file == "resident_notes":
        return "resident_notes"
    elif file == "document_master":
        return "document_master"
    else:
        raise ValueError(f"未対応のシート名ある: {file}")

def load_db(file, retries=3, delay=0.8):
    s_name = get_sheet_name(file)

    last_error = None
    for attempt in range(retries):
        try:
            df = conn.read(worksheet=s_name, ttl=2)

            if df is None:
                df = pd.DataFrame()

            # record_status の列名補正
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
                "active_users": ["user", "login_at", "last_seen"],
                                "resident_master": [
                    "resident_id", "resident_name", "status",
                    "consultant", "consultant_phone",
                    "caseworker", "caseworker_phone",
                    "hospital", "hospital_phone",
                    "nurse", "nurse_phone",
                    "care", "care_phone",
                    "created_at", "updated_at"
                ],
                "resident_schedule": [
                    "id", "resident_id", "weekday", "service_type",
                    "start_time", "end_time", "place", "phone", "person_in_charge", "memo"
                ],
                "resident_notes": [
                    "id", "resident_id", "date", "user", "note"
                ],
                "document_master": [
                    "document_id", "category1", "category2", "category3",
                    "title", "file_type", "url", "summary", "memo",
                    "status", "updated_at", "created_at"
                ],
            }

            for col in expected_cols[file]:
                if col not in df.columns:
                    df[col] = ""

            return df

        except Exception as e:
            last_error = e
            if attempt < retries - 1:
                time.sleep(delay + random.random() * 0.5)
            else:
                raise last_error

def save_db(df, file, retries=3, delay=1.0):
    s_name = get_sheet_name(file)

    last_error = None
    for attempt in range(retries):
        try:
            conn.update(worksheet=s_name, data=df)
            return
        except Exception as e:
            last_error = e
            if attempt < retries - 1:
                time.sleep(delay + random.random() * 0.7)
            else:
                raise last_error

def update_active_user():
    active_df = load_db("active_users")

    if active_df is None or active_df.empty:
        active_df = pd.DataFrame(columns=["user", "login_at", "last_seen"])
    else:
        for col in ["user", "login_at", "last_seen"]:
            if col not in active_df.columns:
                active_df[col] = ""

    now_str = now_jst().strftime("%Y-%m-%d %H:%M")

    keep_rows = []
    now_naive = now_jst().replace(tzinfo=None)

    for _, row in active_df.fillna("").iterrows():
        last_seen = str(row.get("last_seen", "")).strip()
        if not last_seen:
            continue
        try:
            last_dt = pd.to_datetime(last_seen).to_pydatetime()
            if (now_naive - last_dt).total_seconds() <= 15 * 60:
                keep_rows.append(row)
        except Exception:
            pass

    active_df = pd.DataFrame(keep_rows) if keep_rows else pd.DataFrame(columns=["user", "login_at", "last_seen"])

    current_user = st.session_state.user

    if current_user in active_df["user"].astype(str).tolist():
        active_df.loc[active_df["user"] == current_user, "last_seen"] = now_str
    else:
        new_row = pd.DataFrame([{
            "user": current_user,
            "login_at": st.session_state.get("login_at", now_str),
            "last_seen": now_str
        }])
        active_df = pd.concat([active_df, new_row], ignore_index=True)

    save_db(active_df, "active_users")

def heartbeat_active_user():
    now_ts = now_jst().timestamp()
    last_ping = st.session_state.get("last_active_ping", 0)

    # 5分に1回だけ更新
    if now_ts - last_ping >= 300:
        update_active_user()
        st.session_state["last_active_ping"] = now_ts

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

    cal_df = cal_df.fillna("")
    save_db(cal_df, "calendar")

def get_urgent_tasks_df():
    df = load_db("task")

    if df is None or df.empty:
        return pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])

    for col in ["id", "task", "status", "user", "limit", "priority", "updated_at"]:
        if col not in df.columns:
            df[col] = ""

    df = df.fillna("").copy()

    urgent_df = df[
        df["priority"].astype(str).str.strip().isin(["至急", "重要"]) &
        (df["status"].astype(str).str.strip() != "完了")
    ].copy()

    if urgent_df.empty:
        return urgent_df

    prio_map = {"至急": 0, "重要": 1}
    urgent_df["prio_sort"] = urgent_df["priority"].map(prio_map).fillna(9)

    try:
        urgent_df["limit_sort"] = pd.to_datetime(urgent_df["limit"], errors="coerce")
    except Exception:
        urgent_df["limit_sort"] = pd.NaT

    urgent_df = urgent_df.sort_values(["prio_sort", "limit_sort", "updated_at"], ascending=[True, True, False])
    return urgent_df


def start_task(task_id):
    df = load_db("task")
    if df is None or df.empty:
        return

    df = df.fillna("").copy()
    df.loc[df["id"].astype(str) == str(task_id), ["status", "user", "updated_at"]] = [
        "作業中",
        st.session_state.user,
        now_jst().strftime("%Y-%m-%d %H:%M")
    ]
    save_db(df, "task")
    sync_task_events_to_calendar()


def complete_task(task_id):
    df = load_db("task")
    if df is None or df.empty:
        return

    df = df.fillna("").copy()
    df.loc[df["id"].astype(str) == str(task_id), ["status", "updated_at"]] = [
        "完了",
        now_jst().strftime("%Y-%m-%d %H:%M")
    ]
    save_db(df, "task")
    sync_task_events_to_calendar()


def go_to_page(page_name):
    st.session_state.current_page = page_name
    st.rerun()


def render_urgent_banner():
    urgent_df = get_urgent_tasks_df()

    if urgent_df.empty:
        return

    urgent_count = len(urgent_df)
    critical_count = len(urgent_df[urgent_df["priority"].astype(str).str.strip() == "至急"])
    important_count = len(urgent_df[urgent_df["priority"].astype(str).str.strip() == "重要"])

    st.markdown(
        f"""
        <div style="
            background: linear-gradient(90deg, #ff7b54 0%, #ff9f43 100%);
            color: white;
            padding: 14px 18px;
            border-radius: 12px;
            margin-bottom: 16px;
            font-weight: 700;
            box-shadow: 0 2px 8px rgba(0,0,0,0.12);
        ">
            🚨 緊急タスクあり　合計 {urgent_count}件（至急 {critical_count}件 / 重要 {important_count}件）
        </div>
        """,
        unsafe_allow_html=True
    )

    col_a, col_b = st.columns([1, 5])
    with col_a:
        if st.button("緊急一覧を開く", key="open_urgent_page_button", use_container_width=True):
            go_to_page("⑧ 緊急一覧")
    with col_b:
        st.caption("クリックして、至急・重要タスクの一覧を確認できるある。")

# ==========================================
# 🔑 ユーザー認証
# ==========================================
if 'user' not in st.session_state:
    st.markdown("<style>[data-testid='stSidebarNav'] {display: none;}</style>", unsafe_allow_html=True)
    st.title("🛡️ 業務システム・ログイン")
    st.warning("### 名前を選んでログインしてください💻")

    user_list = [
        "木村 由美", "秋吉 幸雄", "安心院 拓也", "粟田 絵利菜", "小宅 正嗣",
        "土居 容子", "中本 匡", "中本 文代", "中本 雄斗", "伴 法子", "栁川 幸恵", "山口 晴彦"
    ]

    user = st.radio(
        "担当者を選択してください",
        user_list,
        index=None
    )

    if st.button("システムへログイン", use_container_width=True):
        if user:
            st.session_state.user = user
            st.session_state.login_at = now_jst().strftime("%Y-%m-%d %H:%M")
            st.session_state.last_active_ping = 0
            st.rerun()
        else:
            st.error("担当者を選択してください。")

    st.stop()

# ==========================================
# 🏠 メインメニュー
# ==========================================
heartbeat_active_user()

st.sidebar.markdown(f"### 👤 ログイン中：\n## {st.session_state.user}")

page_options = [
    "① 未着手の任務（掲示板）",
    "② タスクの引き受け・報告",
    "③ 稼働状況・完了履歴",
    "④ チームチャット",
    "⑤ 業務マニュアル",
    "⑥ 日誌入力状況",
    "⑦ タスクカレンダー",
    "⑧ 緊急一覧",
    "⑨ 利用者情報",
    "⑩ 書類",
]

if "current_page" not in st.session_state or st.session_state.current_page not in page_options:
    st.session_state.current_page = "① 未着手の任務（掲示板）"

st.sidebar.markdown("メニューを選択してください")

st.sidebar.markdown(
    """
    <style>
    /* サイドバー全体 */
    section[data-testid="stSidebar"] .stButton {
        margin-bottom: 0.45rem !important;
    }

    /* 未選択メニューボタン本体 */
    section[data-testid="stSidebar"] .stButton > button {
        width: 100% !important;
        min-height: 56px !important;
        border-radius: 12px !important;
        border: 1px solid #d9d9d9 !important;
        background: #ffffff !important;
        color: #1f2d3d !important;
        font-weight: 700 !important;
        padding: 0 !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;

        display: flex !important;
        align-items: center !important;
        justify-content: flex-start !important;

        text-align: left !important;
    }

    section[data-testid="stSidebar"] .stButton > button:hover {
        border-color: #ff9f43 !important;
        color: #ff7b54 !important;
        background: #fffaf5 !important;
    }

    /* buttonの中の文字コンテナを強制左寄せ */
    section[data-testid="stSidebar"] .stButton > button > div {
        width: 100% !important;
        display: flex !important;
        align-items: center !important;
        justify-content: flex-start !important;
        text-align: left !important;
        padding: 0.78rem 1rem !important;
        box-sizing: border-box !important;
    }

    /* さらに中の文字要素も左寄せ */
    section[data-testid="stSidebar"] .stButton > button > div p,
    section[data-testid="stSidebar"] .stButton > button > div span {
        width: 100% !important;
        margin: 0 !important;
        text-align: left !important;
        justify-content: flex-start !important;
    }

    /* 選択中メニュー */
    .menu-selected-box {
        width: 100%;
        min-height: 56px;
        border-radius: 12px;
        border: 1px solid #ff9f43;
        background: linear-gradient(90deg, #fff1e8 0%, #fff7e6 100%);
        color: #d35400;
        font-weight: 700;
        padding: 12px 14px;
        margin-bottom: 8px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        text-align: center;
        display: flex;
        align-items: center;
        justify-content: center;
        box-sizing: border-box;
    }
    </style>
    """,
    unsafe_allow_html=True
)

for p in page_options:
    is_selected = (st.session_state.current_page == p)

    if is_selected:
        st.sidebar.markdown(
            f'<div class="menu-selected-box">● {p}</div>',
            unsafe_allow_html=True
        )
    else:
        if st.sidebar.button(p, key=f"menu_{p}", use_container_width=True):
            st.session_state.current_page = p
            st.rerun()

page = st.session_state.current_page

if st.sidebar.button("ログアウト"):
    del st.session_state.user
    if "login_at" in st.session_state:
        del st.session_state.login_at
    if "last_active_ping" in st.session_state:
        del st.session_state.last_active_ping
    if "current_page" in st.session_state:
        del st.session_state.current_page
    st.rerun()

st.sidebar.divider()
st.sidebar.caption("System Version 2.0")

render_urgent_banner()

def get_next_numeric_id(df, col_name="id", start=1):
    if df is None or df.empty or col_name not in df.columns:
        return start
    ids = pd.to_numeric(df[col_name], errors="coerce").dropna()
    return int(ids.max()) + 1 if not ids.empty else start


def get_next_resident_id(master_df):
    if master_df is None or master_df.empty or "resident_id" not in master_df.columns:
        return "R0001"

    numbers = []
    for x in master_df["resident_id"].fillna("").astype(str):
        x = x.strip().upper()
        if x.startswith("R"):
            num = x[1:]
            if num.isdigit():
                numbers.append(int(num))

    next_num = max(numbers) + 1 if numbers else 1
    return f"R{next_num:04d}"


def get_resident_master_df():
    df = load_db("resident_master")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "resident_id", "resident_name", "status",
            "consultant", "consultant_phone",
            "caseworker", "caseworker_phone",
            "hospital", "hospital_phone",
            "nurse", "nurse_phone",
            "care", "care_phone",
            "created_at", "updated_at"
        ])
    else:
        for col in [
            "resident_id", "resident_name", "status",
            "consultant", "consultant_phone",
            "caseworker", "caseworker_phone",
            "hospital", "hospital_phone",
            "nurse", "nurse_phone",
            "care", "care_phone",
            "created_at", "updated_at"
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_resident_schedule_df():
    df = load_db("resident_schedule")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "id", "resident_id", "weekday", "service_type",
            "start_time", "end_time", "place", "phone", "person_in_charge", "memo"
        ])
    else:
        for col in [
            "id", "resident_id", "weekday", "service_type",
            "start_time", "end_time", "place", "phone", "person_in_charge", "memo"
        ]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_resident_notes_df():
    df = load_db("resident_notes")
    if df is None or df.empty:
        df = pd.DataFrame(columns=["id", "resident_id", "date", "user", "note"])
    else:
        for col in ["id", "resident_id", "date", "user", "note"]:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


def get_schedule_slot_num(row):
    memo_val = str(row.get("memo", "")).strip()
    if memo_val.startswith("slot:"):
        try:
            num = int(memo_val.replace("slot:", ""))
            return num if 1 <= num <= 4 else 0
        except Exception:
            return 0
    return 0

def build_schedule_form_base(schedule_df, resident_id):
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    service_types = ["病院", "看護", "介護"]

    base = {
        service: {
            "place": "",
            "phone": "",
            "person_in_charge": "",
            "days": {wd: ["", "", "", ""] for wd in weekdays}
        }
        for service in service_types
    }

    if schedule_df is None or schedule_df.empty:
        return base

    view = schedule_df[
        (schedule_df["resident_id"].astype(str) == str(resident_id)) &
        (schedule_df["service_type"].astype(str).isin(service_types))
    ].copy()

    if view.empty:
        return base

    view["_slot_num"] = view.apply(get_schedule_slot_num, axis=1)
    view = view.sort_values(["service_type", "weekday", "_slot_num", "start_time"])

    for _, hit in view.iterrows():
        service_type = str(hit.get("service_type", "")).strip()
        weekday = str(hit.get("weekday", "")).strip()

        if service_type not in base or weekday not in base[service_type]["days"]:
            continue

        if not base[service_type]["place"]:
            base[service_type]["place"] = str(hit.get("place", "")).strip()
        if not base[service_type]["phone"]:
            base[service_type]["phone"] = str(hit.get("phone", "")).strip()
        if not base[service_type]["person_in_charge"]:
            base[service_type]["person_in_charge"] = str(hit.get("person_in_charge", "")).strip()

        slot_num = get_schedule_slot_num(hit)
        if 1 <= slot_num <= 4:
            start_time = str(hit.get("start_time", "")).strip()
            end_time = str(hit.get("end_time", "")).strip()

            time_text = start_time
            if end_time:
                time_text += f"〜{end_time}"

            base[service_type]["days"][weekday][slot_num - 1] = time_text

    return base

def render_resident_schedule_html(schedule_view):
    schedule_view = schedule_view.copy()

    color_map = {
        "病院": "#f6c90e",
        "看護": "#9fd3e6",
        "介護": "#b7e20f",
    }
    col_order = ["月", "火", "水", "木", "金", "土", "日"]

    legend_html = ""
    for name, color in color_map.items():
        legend_html += f'''
        <div style="
            display:inline-block;
            background:{color};
            color:#111;
            font-weight:700;
            padding:8px 28px;
            border:2px solid #111;
            margin-right:10px;
            margin-bottom:10px;
            min-width:120px;
            text-align:center;
        ">{name}</div>
        '''

    table_html = '<table style="width:100%; border-collapse:collapse; table-layout:fixed;">'
    table_html += '<tr>'
    for wd in col_order:
        table_html += f'''
        <th style="
            border:2px solid #111;
            padding:8px;
            text-align:center;
            background:#fafafa;
            width:14.28%;
        ">{wd}</th>
        '''
    table_html += '</tr><tr>'

    for wd in col_order:
        day_df = schedule_view[schedule_view["weekday"].astype(str) == wd].copy()
        items = []

        if not day_df.empty:
            day_df["_slot_num"] = day_df.apply(get_schedule_slot_num, axis=1)
            day_df = day_df.sort_values(["_slot_num", "service_type", "start_time"])

            for _, hit in day_df.iterrows():
                service_type = str(hit.get("service_type", "")).strip()
                bg = color_map.get(service_type, "#eeeeee")

                start_time = str(hit.get("start_time", "")).strip()
                end_time = str(hit.get("end_time", "")).strip()

                time_text = start_time
                if end_time:
                    time_text += f"〜{end_time}"

                items.append(
                    f'''
                    <div style="
                        background:{bg};
                        border:2px solid #222;
                        padding:6px 8px;
                        margin:6px 0;
                        font-weight:700;
                        text-align:left;
                        min-height:34px;
                        box-sizing:border-box;
                        overflow:hidden;
                    ">{time_text}</div>
                    '''
                )

        table_html += f'''
        <td style="
            border:2px solid #111;
            vertical-align:top;
            padding:6px;
            height:190px;
            width:14.28%;
            box-sizing:border-box;
        ">
            {''.join(items)}
        </td>
        '''

    table_html += "</tr></table>"

    st.markdown(legend_html + table_html, unsafe_allow_html=True)


def go_resident_detail(resident_id):
    st.session_state.selected_resident_id = resident_id
    st.rerun()


def back_to_resident_list():
    st.session_state.selected_resident_id = ""
    st.rerun()

# ログイン中メンバー表示
try:
    active_df = load_db("active_users")
except Exception:
    active_df = pd.DataFrame(columns=["user", "login_at", "last_seen"])

st.sidebar.markdown("### 👥 ログイン中メンバー")

if active_df is None or active_df.empty:
    st.sidebar.write("現在ログイン中の人はいないある。")
else:
    active_df = active_df.fillna("")
    now_dt = now_jst().replace(tzinfo=None)
    visible_rows = []

    for _, row in active_df.iterrows():
        last_seen = str(row.get("last_seen", "")).strip()
        if not last_seen:
            continue
        try:
            last_dt = pd.to_datetime(last_seen).to_pydatetime()
            if (now_dt - last_dt).total_seconds() <= 15 * 60:
                visible_rows.append(row)
        except Exception:
            pass

    if visible_rows:
        for row in visible_rows:
            user_name = str(row.get("user", "")).strip()
            login_at = str(row.get("login_at", "")).strip()
            st.sidebar.write(f"**{user_name}**")
            st.sidebar.caption(f"ログイン: {login_at}")
    else:
        st.sidebar.write("現在ログイン中の人はいないある。")

# マイ状況
try:
    task_df = load_db("task").fillna("")
except Exception:
    task_df = pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])

my_active = task_df[
    (task_df["status"].astype(str).str.strip() == "作業中") &
    (task_df["user"].astype(str).str.strip() == st.session_state.user)
]

my_todo = task_df[
    (task_df["status"].astype(str).str.strip() == "未着手")
]

st.sidebar.divider()
st.sidebar.markdown("### 📌 マイ状況")

active_count = len(my_active)
todo_count = len(my_todo)

if st.sidebar.button(f"作業中: {active_count}件", key="go_my_active", use_container_width=True):
    st.session_state.current_page = "② タスクの引き受け・報告"
    st.rerun()

if st.sidebar.button(f"未着手全体: {todo_count}件", key="go_todo_board", use_container_width=True):
    st.session_state.current_page = "① 未着手の任務（掲示板）"
    st.rerun()

# ここを追加するある
page = st.session_state.get("current_page", "① 未着手の任務（掲示板）")
# ==========================================
# ① 未着手の任務（掲示板）
# ==========================================
if page == "① 未着手の任務（掲示板）":
    def show_task_board_page():
        st.title("📋 未着手タスク一覧")
        st.write("現在、依頼されている業務の一覧です。新しいタスクを登録することも可能です。")

        with st.expander("➕ 新規タスクを登録する"):
            with st.form("task_form"):
                bulk_tasks = st.text_area(
                    "タスク名（1行に1件。まとめて貼り付けOK）",
                    placeholder="請求書の確認\n支援記録の見直し\n送迎表の作成"
                )
                t_prio = st.select_slider("緊急度", options=["通常", "重要", "至急"])
                t_limit = st.date_input("完了期限", now_jst().date())

                if st.form_submit_button("タスクを登録"):
                    lines = [x.strip() for x in str(bulk_tasks).replace("\r\n", "\n").split("\n") if x.strip()]

                    if lines:
                        df = load_db("task")

                        if df.empty:
                            next_id = 1
                        else:
                            ids = pd.to_numeric(df["id"], errors="coerce").dropna()
                            next_id = int(ids.max()) + 1 if not ids.empty else 1

                        new_rows = []
                        now_str = now_jst().strftime("%Y-%m-%d %H:%M")

                        for task_name in lines:
                            new_rows.append({
                                "id": next_id,
                                "task": task_name,
                                "status": "未着手",
                                "user": "",
                                "limit": str(t_limit),
                                "priority": t_prio,
                                "updated_at": now_str
                            })
                            next_id += 1

                        add_df = pd.DataFrame(new_rows)
                        merged_df = pd.concat([df, add_df], ignore_index=True)

                        save_db(merged_df, "task")
                        sync_task_events_to_calendar()

                        st.success(f"{len(new_rows)}件のタスクを登録したある！")
                        st.rerun()
                    else:
                        st.error("タスクを1件以上入力してください。")

        df = load_db("task")
        todo = df[df["status"].astype(str).str.strip() == "未着手"].copy()

        if not todo.empty:
            prio_map = {"至急": 0, "重要": 1, "通常": 2}
            todo["p_val"] = todo["priority"].map(prio_map)
            st.dataframe(
                todo.sort_values(["p_val", "limit"])[["priority", "limit", "task"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("現在、未着手のタスクはありません。")

    show_task_board_page()
    
# ==========================================
# ② タスクの引き受け・報告
# ==========================================
elif page == "② タスクの引き受け・報告":
    # @st.fragment(run_every=180)
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
    @st.fragment(run_every=180)
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
# ⑤ 業務マニュアル ver2 (画像D&D + 削除対応)
# ==========================================
elif page == "⑤ 業務マニュアル":
    @st.fragment(run_every=180)
    def show_manual_page():
        st.title("📚 業務マニュアル")

        m_df = load_db("manual")

        if m_df is None or m_df.empty:
            m_df = pd.DataFrame(columns=["id", "title", "content", "image_data", "created_at"])
        else:
            for col in ["id", "title", "content", "image_data", "created_at"]:
                if col not in m_df.columns:
                    m_df[col] = ""

        with st.expander("📝 新しいマニュアルを作成する"):
            with st.form("manual_form"):
                title = st.text_input("マニュアルのタイトル")
                content = st.text_area("手順の説明")

                uploaded_file = st.file_uploader(
                    "写真をドラッグ&ドロップ",
                    type=["png", "jpg", "jpeg"]
                )

                if uploaded_file is not None:
                    st.image(
                        uploaded_file,
                        caption="アップロード画像プレビュー",
                        use_container_width=True
                    )

                if st.form_submit_button("保存する"):
                    if title and content:
                        if m_df.empty:
                            next_id = 1
                        else:
                            ids = pd.to_numeric(m_df["id"], errors="coerce").dropna()
                            next_id = int(ids.max()) + 1 if not ids.empty else 1

                        image_data = ""
                        if uploaded_file is not None:
                            bytes_data = uploaded_file.getvalue()
                            image_data = base64.b64encode(bytes_data).decode("utf-8")

                        new_m = pd.DataFrame([{
                            "id": next_id,
                            "title": title,
                            "content": content,
                            "image_data": image_data,
                            "created_at": now_jst().strftime("%Y-%m-%d %H:%M")
                        }])

                        save_db(pd.concat([m_df, new_m], ignore_index=True), "manual")
                        st.success("マニュアルを保存したある！")
                        st.rerun()
                    else:
                        st.error("タイトルと説明は必須ある。")

        st.divider()

        m_df = load_db("manual")

        if m_df is None or m_df.empty:
            st.info("マニュアルはまだ登録されてないある。")
            return

        for col in ["id", "title", "content", "image_data", "created_at"]:
            if col not in m_df.columns:
                m_df[col] = ""

        m_df = m_df.fillna("")

        try:
            display_df = m_df.sort_values("created_at", ascending=False)
        except Exception:
            display_df = m_df.copy()

        for _, row in display_df.iterrows():
            manual_id = row.get("id", "")
            title = str(row.get("title", "")).strip()
            content = str(row.get("content", "")).strip()
            created_at = str(row.get("created_at", "")).strip()
            image_data = str(row.get("image_data", "")).strip()

            with st.expander(f"📖 {title} (作成日: {created_at})"):
                if content:
                    st.write(content)

                if image_data:
                    try:
                        image_bytes = base64.b64decode(image_data)
                        st.image(
                            image_bytes,
                            caption="添付画像",
                            use_container_width=True
                        )
                    except Exception:
                        st.warning("画像データの読み込みに失敗したある。")

                if st.button("🗑️ このマニュアルを削除する", key=f"delete_manual_{manual_id}"):
                    new_df = m_df[m_df["id"].astype(str) != str(manual_id)].copy()
                    save_db(new_df, "manual")
                    st.success("削除したある。")
                    st.rerun()

    show_manual_page()

# ==========================================
# ⑥ 日誌入力状況（年つき横表・Excel風ある！）
# ==========================================
elif page == "⑥ 日誌入力状況":
    @st.fragment(run_every=180)
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
elif page == "⑦ タスクカレンダー":
    sync_task_events_to_calendar()

    @st.fragment(run_every=180)
    def show_calendar_page():
        st.title("📅 タスクカレンダー")

        try:
            cal_df = load_db("calendar")
            task_df = load_db("task")
        except Exception:
            st.warning("Googleスプレッドシートとの通信が一時的に不安定ある。少し待って再読み込みしてほしいある。")
            return

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

# ==========================================
# ⑧ 緊急一覧
# ==========================================
elif page == "⑧ 緊急一覧":
    def show_urgent_page():
        st.title("🚨 緊急一覧")

        urgent_df = get_urgent_tasks_df()

        if urgent_df.empty:
            st.success("現在、至急・重要タスクはないある。")
            return

        col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 3])

        with col1:
            if st.button("全部", key="urgent_filter_all", use_container_width=True):
                st.session_state.urgent_filter = "全部"
        with col2:
            if st.button("至急", key="urgent_filter_critical", use_container_width=True):
                st.session_state.urgent_filter = "至急"
        with col3:
            if st.button("重要", key="urgent_filter_important", use_container_width=True):
                st.session_state.urgent_filter = "重要"
        with col4:
            if st.button("未着手", key="urgent_filter_todo", use_container_width=True):
                st.session_state.urgent_filter = "未着手"

        current_filter = st.session_state.get("urgent_filter", "全部")

        if current_filter == "至急":
            urgent_df = urgent_df[urgent_df["priority"].astype(str).str.strip() == "至急"]
        elif current_filter == "重要":
            urgent_df = urgent_df[urgent_df["priority"].astype(str).str.strip() == "重要"]
        elif current_filter == "未着手":
            urgent_df = urgent_df[urgent_df["status"].astype(str).str.strip() == "未着手"]

        st.caption(f"現在の表示: {current_filter}")

        if urgent_df.empty:
            st.info("条件に合う緊急タスクはないある。")
            return

        # ===============================
        # 緊急カード表示（2列・期限色対応）
        # ===============================
        cols = st.columns(2)

        for i, (_, row) in enumerate(urgent_df.iterrows()):
            with cols[i % 2]:
                task_id = str(row.get("id", "")).strip()
                task_name = str(row.get("task", "")).strip()
                priority = str(row.get("priority", "")).strip()
                status = str(row.get("status", "")).strip()
                user_name = str(row.get("user", "")).strip()
                limit_str = str(row.get("limit", "")).strip()
                updated_at = str(row.get("updated_at", "")).strip()

                limit_date = None
                try:
                    limit_date = pd.to_datetime(limit_str, errors="coerce").date()
                except:
                    pass

                today = now_jst().date()

                if limit_date and limit_date < today:
                    icon = "🩸"
                    border_color = "#d63031"
                    bg_color = "#ffeaea"
                elif priority == "至急":
                    icon = "🚨"
                    border_color = "#ff4d4f"
                    bg_color = "#fff1f0"
                else:
                    icon = "⚠️"
                    border_color = "#ff9f43"
                    bg_color = "#fff7e6"

                assignee = user_name if user_name else "未割当"

                with st.container(border=True):
                    st.markdown(
                        f"""
                        <div style="
                            border-left: 8px solid {border_color};
                            background-color: {bg_color};
                            padding: 16px;
                            border-radius: 12px;
                            margin-bottom: 12px;
                            min-height: 160px;
                        ">
                        <div style="font-size:20px; font-weight:700; margin-bottom:6px;">
                        {icon} {priority}
                        </div>
                        <div style="font-size:22px; font-weight:700; margin-bottom:10px;">
                        {task_name}
                        </div>
                        <div style="line-height:1.9;">
                        <b>状態:</b> {status}<br>
                        <b>担当:</b> {assignee}<br>
                        <b>期限:</b> {limit_str}<br>
                        <b>更新:</b> {updated_at}
                        </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                    action_cols = st.columns([1, 1, 4])

                    if status == "未着手":
                        with action_cols[0]:
                            if st.button("開始する", key=f"urgent_start_{task_id}", use_container_width=True):
                                start_task(task_id)
                                st.success("タスクを開始したある！")
                                st.rerun()

                    elif status == "作業中":
                        if user_name == st.session_state.user:
                            with action_cols[1]:
                                if st.button("完了する", key=f"urgent_done_{task_id}", use_container_width=True):
                                    complete_task(task_id)
                                    st.success("タスクを完了したある！")
                                    st.rerun()
                        else:
                            with action_cols[2]:
                                st.caption(f"現在 {user_name} さんが対応中ある。")

    show_urgent_page()

# ==========================================
# ⑨ 利用者情報（軽量化版 + 至急アラート連動）
# ==========================================
elif page == "⑨ 利用者情報":
    def show_resident_page():
        st.title("👤 利用者情報")

        master_df = get_resident_master_df()

        if "resident_mode" not in st.session_state:
            st.session_state.resident_mode = "利用中"

        if "selected_resident_id" not in st.session_state:
            st.session_state.selected_resident_id = ""

        if "edit_resident_basic" not in st.session_state:
            st.session_state.edit_resident_basic = False

        if "edit_resident_schedule" not in st.session_state:
            st.session_state.edit_resident_schedule = False

        if "edit_resident_note" not in st.session_state:
            st.session_state.edit_resident_note = False

        def reset_resident_edit_flags():
            st.session_state.edit_resident_basic = False
            st.session_state.edit_resident_schedule = False
            st.session_state.edit_resident_note = False

        def parse_time_range(raw_text: str):
            raw = str(raw_text).strip()
            if not raw:
                return "", ""

            raw = raw.replace("～", "〜").replace("~", "〜").replace("-", "〜")
            if "〜" in raw:
                start_time, end_time = [x.strip() for x in raw.split("〜", 1)]
                return start_time, end_time

            return raw, ""

        # ------------------------------------------
        # 一覧モード
        # ------------------------------------------
        if not st.session_state.selected_resident_id:
            reset_resident_edit_flags()

            top_cols = st.columns([1, 1, 3])

            with top_cols[0]:
                if st.button("利用者一覧", use_container_width=True):
                    st.session_state.resident_mode = "利用中"
                    st.rerun()

            with top_cols[1]:
                if st.button("退所者一覧", use_container_width=True):
                    st.session_state.resident_mode = "退所"
                    st.rerun()

            with top_cols[2]:
                st.caption(f"現在表示: {st.session_state.resident_mode}")

            st.divider()

            with st.expander("➕ 新しい利用者を追加する"):
                weekdays = ["月", "火", "水", "木", "金", "土", "日"]
                service_defs = [
                    ("病院", "#FFD54F"),
                    ("看護", "#9FD3E6"),
                    ("介護", "#B7E20F"),
                ]

                slot_placeholders = [
                    "例 10:00〜11:00",
                    "2つ目があれば入力",
                    "3つ目があれば入力",
                    "4つ目があれば入力",
                ]

                def parse_add_time_range(raw_text: str):
                    raw = str(raw_text).strip()
                    if not raw:
                        return "", ""

                    raw = raw.replace("～", "〜").replace("~", "〜").replace("-", "〜")
                    if "〜" in raw:
                        start_time, end_time = [x.strip() for x in raw.split("〜", 1)]
                        return start_time, end_time

                    return raw, ""

                st.markdown(
                    """
                    <style>
                    .add-svc-title {
                        display:inline-block;
                        min-width:140px;
                        text-align:center;
                        font-weight:700;
                        font-size:20px;
                        color:#111;
                        border:3px solid #111;
                        padding:8px 18px;
                        margin-top:10px;
                        margin-bottom:12px;
                    }
                    .add-week-head {
                        text-align:center;
                        font-weight:700;
                        border:2px solid #111;
                        padding:7px 0;
                        background:#fafafa;
                        margin-bottom:4px;
                    }
                    .add-week-wrap {
                        border:3px solid #111;
                        padding:10px 10px 6px 10px;
                        margin-bottom:18px;
                        background:#fff;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True
                )

                with st.form("resident_add_form"):
                    st.markdown("### 基本情報")

                    basic1 = st.columns(3)
                    with basic1[0]:
                        resident_name = st.text_input("利用者名")
                    with basic1[1]:
                        status = st.selectbox("状態", ["利用中", "退所"])
                    with basic1[2]:
                        consultant = st.text_input("相談員")

                    basic2 = st.columns(3)
                    with basic2[0]:
                        consultant_phone = st.text_input("相談員電話")
                    with basic2[1]:
                        caseworker = st.text_input("ケースワーカー")
                    with basic2[2]:
                        caseworker_phone = st.text_input("ケースワーカー電話")

                    st.markdown("### 病院・看護・介護の週間予定")
                    st.caption("同じ曜日に2回以上ある場合は、同じ曜日の下の2つ目・3つ目・4つ目にもそのまま入力してほしいある。Enterは不要ある。")

                    weekly_inputs = {}

                    for service_name, service_color in service_defs:
                        st.markdown(
                            f'<div class="add-svc-title" style="background:{service_color};">{service_name}</div>',
                            unsafe_allow_html=True
                        )

                        top_info = st.columns([4, 4, 3])
                        with top_info[0]:
                            place_val = st.text_input(
                                f"{service_name}名",
                                key=f"add_{service_name}_place",
                                placeholder=f"{service_name}名"
                            )
                        with top_info[1]:
                            person_val = st.text_input(
                                f"{service_name}担当",
                                key=f"add_{service_name}_person",
                                placeholder="担当"
                            )
                        with top_info[2]:
                            phone_val = st.text_input(
                                f"{service_name}電話",
                                key=f"add_{service_name}_phone",
                                placeholder="電話"
                            )

                        st.markdown('<div class="add-week-wrap">', unsafe_allow_html=True)
                        day_cols = st.columns(7)

                        day_values = {}

                        for i, wd in enumerate(weekdays):
                            with day_cols[i]:
                                st.markdown(f'<div class="add-week-head">{wd}</div>', unsafe_allow_html=True)

                                slots = []
                                for slot_idx in range(4):
                                    slot_val = st.text_input(
                                        f"{service_name}_{wd}_{slot_idx+1}",
                                        key=f"add_{service_name}_{wd}_{slot_idx+1}",
                                        label_visibility="collapsed",
                                        placeholder=slot_placeholders[slot_idx]
                                    )
                                    slots.append(slot_val)

                                day_values[wd] = slots

                        st.markdown("</div>", unsafe_allow_html=True)

                        weekly_inputs[service_name] = {
                            "place": str(place_val).strip(),
                            "person_in_charge": str(person_val).strip(),
                            "phone": str(phone_val).strip(),
                            "days": day_values
                        }

                    submit_cols = st.columns(2)
                    with submit_cols[0]:
                        add_resident = st.form_submit_button("利用者を登録する", use_container_width=True)
                    with submit_cols[1]:
                        cancel_add = st.form_submit_button("入力をやめる", use_container_width=True)

                    if add_resident:
                        if resident_name.strip():
                            next_resident_id = get_next_resident_id(master_df)
                            now_str = now_jst().strftime("%Y-%m-%d %H:%M")

                            new_master_row = pd.DataFrame([{
                                "resident_id": next_resident_id,
                                "resident_name": resident_name.strip(),
                                "status": status,
                                "consultant": consultant.strip(),
                                "consultant_phone": consultant_phone.strip(),
                                "caseworker": caseworker.strip(),
                                "caseworker_phone": caseworker_phone.strip(),
                                "hospital": weekly_inputs["病院"]["place"],
                                "hospital_phone": weekly_inputs["病院"]["phone"],
                                "nurse": weekly_inputs["看護"]["place"],
                                "nurse_phone": weekly_inputs["看護"]["phone"],
                                "care": weekly_inputs["介護"]["place"],
                                "care_phone": weekly_inputs["介護"]["phone"],
                                "created_at": now_str,
                                "updated_at": now_str
                            }])

                            new_master_df = pd.concat([master_df, new_master_row], ignore_index=True)
                            save_db(new_master_df, "resident_master")

                            schedule_df_add = get_resident_schedule_df()
                            next_schedule_id = get_next_numeric_id(schedule_df_add, "id", 1)
                            new_schedule_rows = []

                            for service_type, data in weekly_inputs.items():
                                place_name = str(data["place"]).strip()
                                phone_text = str(data["phone"]).strip()
                                person_text = str(data["person_in_charge"]).strip()

                                for wd, slot_list in data["days"].items():
                                    for slot_index, raw_time in enumerate(slot_list, start=1):
                                        start_time, end_time = parse_add_time_range(raw_time)
                                        if not start_time and not end_time:
                                            continue

                                        new_schedule_rows.append({
                                            "id": next_schedule_id,
                                            "resident_id": next_resident_id,
                                            "weekday": wd,
                                            "service_type": service_type,
                                            "start_time": start_time,
                                            "end_time": end_time,
                                            "place": place_name,
                                            "phone": phone_text,
                                            "person_in_charge": person_text,
                                            "memo": f"slot:{slot_index}"
                                        })
                                        next_schedule_id += 1

                            if new_schedule_rows:
                                add_schedule_df = pd.DataFrame(new_schedule_rows)
                                merged_schedule_df = pd.concat([schedule_df_add, add_schedule_df], ignore_index=True)
                                save_db(merged_schedule_df.fillna(""), "resident_schedule")

                            st.success("利用者を登録したある！")
                            st.rerun()
                        else:
                            st.error("利用者名を入力してほしいある。")

                    if cancel_add:
                        st.rerun()

            st.divider()

            search_word = st.text_input("名前検索", placeholder="利用者名を入力")

            list_df = master_df.copy()
            list_df = list_df[
                list_df["status"].astype(str).str.strip() == st.session_state.resident_mode
            ].copy()

            if search_word.strip():
                list_df = list_df[
                    list_df["resident_name"].astype(str).str.contains(search_word.strip(), case=False, na=False)
                ].copy()

            if not list_df.empty:
                list_df = list_df.sort_values("resident_name")

            if list_df.empty:
                st.info("該当する利用者はいないある。")
                return

            cols = st.columns(2)

            for i, (_, row) in enumerate(list_df.iterrows()):
                with cols[i % 2]:
                    resident_id = str(row.get("resident_id", "")).strip()
                    resident_name = str(row.get("resident_name", "")).strip()
                    status = str(row.get("status", "")).strip()
                    consultant = str(row.get("consultant", "")).strip()

                    if status == "利用中":
                        status_label = "🟢 利用中"
                        bg = "#eefbf0"
                        border = "#2ecc71"
                    else:
                        status_label = "⚫ 退所"
                        bg = "#f3f4f6"
                        border = "#7f8c8d"

                    with st.container(border=True):
                        st.markdown(
                            f"""
                            <div style="
                                border-left: 8px solid {border};
                                background-color: {bg};
                                padding: 14px;
                                border-radius: 10px;
                                min-height: 140px;
                                margin-bottom: 10px;
                            ">
                                <div style="font-size: 20px; font-weight: 700; margin-bottom: 8px;">
                                    {resident_name}
                                </div>
                                <div style="margin-bottom: 6px;">{status_label}</div>
                                <div>相談員: {consultant if consultant else '未登録'}</div>
                                <div>ID: {resident_id}</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                        if st.button("詳細を見る", key=f"open_resident_{resident_id}", use_container_width=True):
                            st.session_state.selected_resident_id = resident_id
                            reset_resident_edit_flags()
                            st.rerun()

            return

        # ------------------------------------------
        # 個人詳細モード
        # ------------------------------------------
        selected_id = str(st.session_state.selected_resident_id).strip()

        detail_df = master_df[master_df["resident_id"].astype(str) == selected_id].copy()
        if detail_df.empty:
            st.warning("利用者情報が見つからないある。")
            if st.button("一覧に戻る", use_container_width=True):
                st.session_state.selected_resident_id = ""
                reset_resident_edit_flags()
                st.rerun()
            return

        row = detail_df.iloc[0]
        resident_name = str(row.get("resident_name", "")).strip()
        status = str(row.get("status", "")).strip()

        schedule_df = get_resident_schedule_df()
        notes_df = get_resident_notes_df()

        try:
            task_df_detail = load_db("task")
            if task_df_detail is None or task_df_detail.empty:
                task_df_detail = pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])
            else:
                for col in ["id", "task", "status", "user", "limit", "priority", "updated_at"]:
                    if col not in task_df_detail.columns:
                        task_df_detail[col] = ""
                task_df_detail = task_df_detail.fillna("")
        except Exception:
            task_df_detail = pd.DataFrame(columns=["id", "task", "status", "user", "limit", "priority", "updated_at"])

        back_cols = st.columns([1, 5])
        with back_cols[0]:
            if st.button("← 一覧に戻る", use_container_width=True):
                st.session_state.selected_resident_id = ""
                reset_resident_edit_flags()
                st.rerun()

        status_label = "🟢 利用中" if status == "利用中" else "⚫ 退所"

        st.subheader(f"{resident_name} 様")
        st.caption(f"{status_label} / ID: {selected_id}")

        # ------------------------------------------
        # この人の至急アラート
        # ------------------------------------------
        st.markdown("### 🚨 この人のために今すぐやること")

        urgent_person_df = task_df_detail[
            task_df_detail["priority"].astype(str).str.strip().isin(["至急", "重要"]) &
            (task_df_detail["status"].astype(str).str.strip() != "完了") &
            task_df_detail["task"].astype(str).str.contains(resident_name, case=False, na=False)
        ].copy()

        if not urgent_person_df.empty:
            prio_map = {"至急": 0, "重要": 1}
            urgent_person_df["prio_sort"] = urgent_person_df["priority"].map(prio_map).fillna(9)
            urgent_person_df["limit_sort"] = pd.to_datetime(urgent_person_df["limit"], errors="coerce")
            urgent_person_df = urgent_person_df.sort_values(
                ["prio_sort", "limit_sort", "updated_at"],
                ascending=[True, True, False]
            )

            for _, trow in urgent_person_df.iterrows():
                t_id = str(trow.get("id", "")).strip()
                t_name = str(trow.get("task", "")).strip()
                t_priority = str(trow.get("priority", "")).strip()
                t_status = str(trow.get("status", "")).strip()
                t_user = str(trow.get("user", "")).strip()
                t_limit = str(trow.get("limit", "")).strip()
                t_updated = str(trow.get("updated_at", "")).strip()

                limit_date = pd.to_datetime(t_limit, errors="coerce")
                today = now_jst().date()

                if pd.notna(limit_date) and limit_date.date() < today:
                    icon = "🩸"
                    border_color = "#d63031"
                    bg_color = "#ffeaea"
                elif t_priority == "至急":
                    icon = "🚨"
                    border_color = "#ff4d4f"
                    bg_color = "#fff1f0"
                else:
                    icon = "⚠️"
                    border_color = "#ff9f43"
                    bg_color = "#fff7e6"

                assignee = t_user if t_user else "未割当"

                with st.container(border=True):
                    st.markdown(
                        f"""
                        <div style="
                            border-left: 8px solid {border_color};
                            background-color: {bg_color};
                            padding: 14px;
                            border-radius: 10px;
                            margin-bottom: 10px;
                        ">
                            <div style="font-size:18px; font-weight:700; margin-bottom:6px;">
                                {icon} {t_priority} - {t_name}
                            </div>
                            <div style="line-height:1.8;">
                                <b>状態:</b> {t_status}<br>
                                <b>担当:</b> {assignee}<br>
                                <b>期限:</b> {t_limit}<br>
                                <b>更新:</b> {t_updated}
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                    btn_cols = st.columns([1, 1, 4])

                    if t_status == "未着手":
                        with btn_cols[0]:
                            if st.button("開始する", key=f"resident_urgent_start_{t_id}", use_container_width=True):
                                start_task(t_id)
                                st.success("タスクを開始したある！")
                                st.rerun()

                    elif t_status == "作業中":
                        if t_user == st.session_state.user:
                            with btn_cols[1]:
                                if st.button("完了する", key=f"resident_urgent_done_{t_id}", use_container_width=True):
                                    complete_task(t_id)
                                    st.success("タスクを完了したある！")
                                    st.rerun()
                        else:
                            with btn_cols[2]:
                                st.caption(f"現在 {t_user} さんが対応中ある。")
        else:
            st.info("この利用者に連動した至急・重要タスクは今のところないある。")

        st.divider()
        st.markdown("### 基本情報")

        info_cols = st.columns(2)
        with info_cols[0]:
            st.write(f"**相談員**: {row.get('consultant', '')}")
            st.write(f"**相談員電話**: {row.get('consultant_phone', '')}")
            st.write(f"**ケースワーカー**: {row.get('caseworker', '')}")
            st.write(f"**ケースワーカー電話**: {row.get('caseworker_phone', '')}")
            st.write(f"**病院**: {row.get('hospital', '')}")
            st.write(f"**病院電話**: {row.get('hospital_phone', '')}")

        with info_cols[1]:
            st.write(f"**看護**: {row.get('nurse', '')}")
            st.write(f"**看護電話**: {row.get('nurse_phone', '')}")
            st.write(f"**介護**: {row.get('care', '')}")
            st.write(f"**介護電話**: {row.get('care_phone', '')}")
            st.write(f"**登録日**: {row.get('created_at', '')}")
            st.write(f"**更新日**: {row.get('updated_at', '')}")

        st.divider()
        st.markdown("### 週間予定")

        schedule_view = schedule_df[schedule_df["resident_id"].astype(str) == selected_id].copy()
        schedule_view = schedule_view[
            schedule_view["service_type"].astype(str).isin(["病院", "看護", "介護"])
        ].copy()

        if schedule_view.empty:
            st.info("週間予定はまだ登録されてないある。")
        else:
            render_resident_schedule_html(schedule_view)

        st.divider()
        st.markdown("### 共有メモ")

        notes_view = notes_df[notes_df["resident_id"].astype(str) == selected_id].copy()
        if not notes_view.empty:
            try:
                notes_view = notes_view.sort_values("date", ascending=False)
            except Exception:
                pass

            for _, note_row in notes_view.iterrows():
                with st.container(border=True):
                    st.write(f"**{note_row.get('date', '')}**  {note_row.get('user', '')}")
                    st.write(note_row.get("note", ""))
        else:
            st.info("共有メモはまだないある。")

        st.divider()
        st.markdown("### 編集・追加")

        edit_cols = st.columns(3)
        with edit_cols[0]:
            if st.button("基本情報を編集", key=f"edit_basic_{selected_id}", use_container_width=True):
                st.session_state.edit_resident_basic = True
                st.session_state.edit_resident_schedule = False
                st.session_state.edit_resident_note = False
                st.rerun()

        with edit_cols[1]:
            if st.button("予定を追加", key=f"edit_schedule_{selected_id}", use_container_width=True):
                st.session_state.edit_resident_basic = False
                st.session_state.edit_resident_schedule = True
                st.session_state.edit_resident_note = False
                st.rerun()

        with edit_cols[2]:
            if st.button("メモを追加", key=f"edit_note_{selected_id}", use_container_width=True):
                st.session_state.edit_resident_basic = False
                st.session_state.edit_resident_schedule = False
                st.session_state.edit_resident_note = True
                st.rerun()

        # ------------------------------------------
        # 基本情報編集
        # ------------------------------------------
        if st.session_state.get("edit_resident_basic", False):
            st.divider()
            st.markdown("#### 基本情報を編集")

            with st.form(f"resident_basic_form_{selected_id}"):
                resident_name = st.text_input("利用者名", value=str(row.get("resident_name", "")))
                status = st.selectbox(
                    "状態",
                    ["利用中", "退所"],
                    index=0 if str(row.get("status", "")).strip() == "利用中" else 1
                )
                consultant = st.text_input("相談員", value=str(row.get("consultant", "")))
                consultant_phone = st.text_input("相談員電話", value=str(row.get("consultant_phone", "")))
                caseworker = st.text_input("ケースワーカー", value=str(row.get("caseworker", "")))
                caseworker_phone = st.text_input("ケースワーカー電話", value=str(row.get("caseworker_phone", "")))
                hospital = st.text_input("病院", value=str(row.get("hospital", "")))
                hospital_phone = st.text_input("病院電話", value=str(row.get("hospital_phone", "")))
                nurse = st.text_input("看護", value=str(row.get("nurse", "")))
                nurse_phone = st.text_input("看護電話", value=str(row.get("nurse_phone", "")))
                care = st.text_input("介護", value=str(row.get("care", "")))
                care_phone = st.text_input("介護電話", value=str(row.get("care_phone", "")))

                save_col1, save_col2 = st.columns(2)
                with save_col1:
                    save_basic = st.form_submit_button("基本情報を保存する", use_container_width=True)
                with save_col2:
                    cancel_basic = st.form_submit_button("キャンセル", use_container_width=True)

                if save_basic:
                    update_df = master_df.copy()
                    now_str = now_jst().strftime("%Y-%m-%d %H:%M")

                    target_mask = update_df["resident_id"].astype(str) == selected_id
                    update_df.loc[target_mask, "resident_name"] = resident_name.strip()
                    update_df.loc[target_mask, "status"] = status
                    update_df.loc[target_mask, "consultant"] = consultant.strip()
                    update_df.loc[target_mask, "consultant_phone"] = consultant_phone.strip()
                    update_df.loc[target_mask, "caseworker"] = caseworker.strip()
                    update_df.loc[target_mask, "caseworker_phone"] = caseworker_phone.strip()
                    update_df.loc[target_mask, "hospital"] = hospital.strip()
                    update_df.loc[target_mask, "hospital_phone"] = hospital_phone.strip()
                    update_df.loc[target_mask, "nurse"] = nurse.strip()
                    update_df.loc[target_mask, "nurse_phone"] = nurse_phone.strip()
                    update_df.loc[target_mask, "care"] = care.strip()
                    update_df.loc[target_mask, "care_phone"] = care_phone.strip()
                    update_df.loc[target_mask, "updated_at"] = now_str

                    save_db(update_df, "resident_master")
                    st.session_state.edit_resident_basic = False
                    st.success("基本情報を保存したある！")
                    st.rerun()

                if cancel_basic:
                    st.session_state.edit_resident_basic = False
                    st.rerun()

        # ------------------------------------------
        # 週間予定編集
        # ------------------------------------------
        if st.session_state.get("edit_resident_schedule", False):
            st.divider()
            st.markdown("#### 病院・看護・介護の週間予定を編集")
            st.caption("同じ曜日に2回以上ある場合は、同じ曜日の下の2つ目・3つ目・4つ目にもそのまま入力してほしいある。Enterは不要ある。")

            schedule_base = build_schedule_form_base(schedule_df, selected_id)
            weekdays = ["月", "火", "水", "木", "金", "土", "日"]
            service_types = ["病院", "看護", "介護"]

            color_map = {
                "病院": "#FFD54F",
                "看護": "#81D4FA",
                "介護": "#AED581",
            }

            slot_placeholders = [
                "例 10:00〜11:00",
                "2つ目があれば入力",
                "3つ目があれば入力",
                "4つ目があれば入力",
            ]

            with st.form(f"resident_schedule_form_{selected_id}"):
                weekly_inputs = {}

                for service in service_types:
                    st.markdown(
                        f"""
                        <div style="
                            background:{color_map[service]};
                            border:2px solid #111;
                            padding:8px 16px;
                            font-weight:700;
                            display:inline-block;
                            min-width:120px;
                            text-align:center;
                            margin-top:8px;
                            margin-bottom:10px;
                        ">{service}</div>
                        """,
                        unsafe_allow_html=True
                    )

                    top_cols = st.columns([3, 3, 3])
                    with top_cols[0]:
                        place_val = st.text_input(
                            f"{service}名",
                            value=schedule_base[service]["place"],
                            key=f"{selected_id}_{service}_place"
                        )
                    with top_cols[1]:
                        phone_val = st.text_input(
                            f"{service}電話",
                            value=schedule_base[service]["phone"],
                            key=f"{selected_id}_{service}_phone"
                        )
                    with top_cols[2]:
                        person_val = st.text_input(
                            f"{service}担当",
                            value=schedule_base[service]["person_in_charge"],
                            key=f"{selected_id}_{service}_person"
                        )

                    st.markdown(
                        """
                        <style>
                        .weekly-head {
                            text-align:center;
                            font-weight:700;
                            padding:6px 0;
                            border:2px solid #111;
                            background:#fafafa;
                            margin-bottom:4px;
                        }
                        </style>
                        """,
                        unsafe_allow_html=True
                    )

                    day_values = {}
                    day_cols = st.columns(7)

                    for i, wd in enumerate(weekdays):
                        with day_cols[i]:
                            st.markdown(f'<div class="weekly-head">{wd}</div>', unsafe_allow_html=True)

                            slots = []
                            for slot_idx in range(4):
                                slot_val = schedule_base[service]["days"][wd][slot_idx]
                                new_val = st.text_input(
                                    f"{wd}{slot_idx+1}枠",
                                    value=slot_val,
                                    key=f"{selected_id}_{service}_{wd}_{slot_idx+1}",
                                    label_visibility="collapsed",
                                    placeholder=slot_placeholders[slot_idx]
                                )
                                slots.append(new_val)

                            day_values[wd] = slots

                    weekly_inputs[service] = {
                        "place": str(place_val).strip(),
                        "phone": str(phone_val).strip(),
                        "person_in_charge": str(person_val).strip(),
                        "days": day_values
                    }

                    st.markdown("<br>", unsafe_allow_html=True)

                save_col1, save_col2 = st.columns(2)
                with save_col1:
                    save_weekly = st.form_submit_button("週間予定を保存する", use_container_width=True)
                with save_col2:
                    cancel_weekly = st.form_submit_button("キャンセル", use_container_width=True)

                if save_weekly:
                    keep_df = schedule_df[
                        ~(
                            (schedule_df["resident_id"].astype(str) == selected_id) &
                            (schedule_df["service_type"].astype(str).isin(["病院", "看護", "介護"]))
                        )
                    ].copy()

                    next_id = get_next_numeric_id(schedule_df, "id", 1)
                    new_rows = []

                    for service_type, data in weekly_inputs.items():
                        place_name = data["place"]
                        phone_text = data["phone"]
                        person_text = data["person_in_charge"]

                        for wd, slot_list in data["days"].items():
                            for slot_index, time_range in enumerate(slot_list, start=1):
                                start_time, end_time = parse_time_range(time_range)
                                if not start_time and not end_time:
                                    continue

                                new_rows.append({
                                    "id": next_id,
                                    "resident_id": selected_id,
                                    "weekday": wd,
                                    "service_type": service_type,
                                    "start_time": start_time,
                                    "end_time": end_time,
                                    "place": place_name,
                                    "phone": phone_text,
                                    "person_in_charge": person_text,
                                    "memo": f"slot:{slot_index}"
                                })
                                next_id += 1

                    if new_rows:
                        add_df = pd.DataFrame(new_rows)
                        save_df = pd.concat([keep_df, add_df], ignore_index=True)
                    else:
                        save_df = keep_df.copy()

                    save_df = save_df.fillna("")
                    save_db(save_df, "resident_schedule")
                    st.session_state.edit_resident_schedule = False
                    st.success("週間予定を保存したある！")
                    st.rerun()

                if cancel_weekly:
                    st.session_state.edit_resident_schedule = False
                    st.rerun()

            st.markdown("#### 現在の週間予定")

            current_view = schedule_df[schedule_df["resident_id"].astype(str) == selected_id].copy()
            current_view = current_view[
                current_view["service_type"].astype(str).isin(["病院", "看護", "介護"])
            ].copy()

            if current_view.empty:
                st.info("週間予定はまだ登録されてないある。")
            else:
                render_resident_schedule_html(current_view)

            delete_target = schedule_df[
                (schedule_df["resident_id"].astype(str) == selected_id) &
                (schedule_df["service_type"].astype(str).isin(["病院", "看護", "介護"]))
            ].copy()

            if not delete_target.empty:
                if st.button("週間予定をすべて削除", key=f"delete_weekly_schedule_{selected_id}", use_container_width=True):
                    new_schedule_df = schedule_df[
                        ~(
                            (schedule_df["resident_id"].astype(str) == selected_id) &
                            (schedule_df["service_type"].astype(str).isin(["病院", "看護", "介護"]))
                        )
                    ].copy()
                    save_db(new_schedule_df, "resident_schedule")
                    st.success("週間予定を削除したある。")
                    st.rerun()
                    
        # ------------------------------------------
        # メモ追加
        # ------------------------------------------
        if st.session_state.get("edit_resident_note", False):
            st.divider()
            st.markdown("#### 共有メモを追加")

            with st.form(f"resident_note_form_{selected_id}"):
                note_date = st.date_input("日付", value=now_jst().date())
                note_text = st.text_area("共有メモ")

                save_col1, save_col2 = st.columns(2)
                with save_col1:
                    add_note = st.form_submit_button("メモを追加する", use_container_width=True)
                with save_col2:
                    cancel_note = st.form_submit_button("キャンセル", use_container_width=True)

                if add_note:
                    if note_text.strip():
                        next_id = get_next_numeric_id(notes_df, "id", 1)
                        new_row = pd.DataFrame([{
                            "id": next_id,
                            "resident_id": selected_id,
                            "date": str(note_date),
                            "user": str(st.session_state.get("user", "")),
                            "note": note_text.strip()
                        }])

                        new_notes_df = pd.concat([notes_df, new_row], ignore_index=True)
                        save_db(new_notes_df, "resident_notes")
                        st.session_state.edit_resident_note = False
                        st.success("共有メモを追加したある！")
                        st.rerun()
                    else:
                        st.error("メモ内容を入力してほしいある。")

                if cancel_note:
                    st.session_state.edit_resident_note = False
                    st.rerun()

            notes_delete_df = notes_df[
                notes_df["resident_id"].astype(str) == selected_id
            ].copy()

            if not notes_delete_df.empty:
                try:
                    notes_delete_df = notes_delete_df.sort_values("date", ascending=False)
                except Exception:
                    pass

                st.caption("登録済みメモを削除する場合は下から選ぶある。")

                for _, nrow in notes_delete_df.iterrows():
                    nid = str(nrow.get("id", "")).strip()

                    with st.container(border=True):
                        st.write(f"**{nrow.get('date', '')}**  {nrow.get('user', '')}")
                        st.write(nrow.get("note", ""))

                        if st.button(
                            "このメモを削除",
                            key=f"delete_note_{selected_id}_{nid}",
                            use_container_width=True
                        ):
                            latest_notes_df = get_resident_notes_df()
                            new_notes_df = latest_notes_df[
                                latest_notes_df["id"].astype(str) != nid
                            ].copy()

                            save_db(new_notes_df, "resident_notes")
                            st.success("メモを削除したある。")
                            st.rerun()

    show_resident_page()

# ==========================================
# ⑩ 書類
# ==========================================
elif page == "⑩ 書類":
    CATEGORY_MAP = {
        "ケアマネ": {
            "書類": ["提出書類", "記入例", "テンプレート", "契約関係", "その他"],
            "連絡先": ["電話", "FAX", "メール", "住所", "担当者", "その他"],
            "情報": ["役割", "支援方針", "注意事項", "面談内容", "その他"],
            "その他": ["その他"]
        },
        "相談員": {
            "書類": ["提出書類", "記入例", "テンプレート", "モニタリング", "個別支援計画", "その他"],
            "連絡先": ["電話", "FAX", "メール", "住所", "担当者", "その他"],
            "情報": ["支援内容", "注意事項", "面談内容", "連携事項", "その他"],
            "その他": ["その他"]
        },
        "介護施設": {
            "書類": ["提出書類", "契約関係", "利用資料", "その他"],
            "連絡先": ["電話", "FAX", "メール", "住所", "担当者", "その他"],
            "情報": ["受入情報", "注意事項", "連携事項", "支援内容", "その他"],
            "その他": ["その他"]
        },
        "役所": {
            "書類": ["申請書類", "提出書類", "更新手続き", "契約関係", "その他"],
            "連絡先": ["電話", "FAX", "メール", "住所", "窓口", "その他"],
            "情報": ["制度情報", "申請手順", "注意事項", "担当部署", "その他"],
            "その他": ["その他"]
        },
        "病院": {
            "書類": ["診断書", "紹介状", "提出書類", "医療関係", "その他"],
            "連絡先": ["電話", "FAX", "メール", "住所", "外来窓口", "担当者", "その他"],
            "情報": ["通院情報", "入院情報", "服薬情報", "注意事項", "医療連携", "その他"],
            "その他": ["その他"]
        },
        "利用者": {
            "書類": ["個別支援計画", "モニタリング", "面談記録", "契約関係", "申請関係", "その他"],
            "連絡先": ["本人連絡先", "家族連絡先", "緊急連絡先", "その他"],
            "情報": ["基本情報", "支援内容", "注意事項", "医療情報", "金銭情報", "生活状況", "その他"],
            "その他": ["その他"]
        },
        "退所者": {
            "書類": ["退所書類", "契約終了", "引継ぎ資料", "その他"],
            "連絡先": ["本人連絡先", "家族連絡先", "関係先", "その他"],
            "情報": ["退所理由", "引継ぎ事項", "注意事項", "その他"],
            "その他": ["その他"]
        },
        "その他": {
            "書類": ["その他"],
            "連絡先": ["その他"],
            "情報": ["その他"],
            "その他": ["その他"]
        }
    }

    CATEGORY1_MASTER = list(CATEGORY_MAP.keys())
    FILE_TYPE_MASTER = ["PDF", "Word", "Excel", "画像", "URL", "その他"]
    STATUS_MASTER = ["利用中", "旧版", "無効"]

    def get_document_master_df():
        df = load_db("document_master")
        if df is None or df.empty:
            df = pd.DataFrame(columns=[
                "document_id", "category1", "category2", "category3",
                "title", "file_type", "url", "summary", "memo",
                "status", "updated_at", "created_at"
            ])
        else:
            for col in [
                "document_id", "category1", "category2", "category3",
                "title", "file_type", "url", "summary", "memo",
                "status", "updated_at", "created_at"
            ]:
                if col not in df.columns:
                    df[col] = ""
        return df.fillna("")

    def get_next_document_id(doc_df):
        if doc_df is None or doc_df.empty or "document_id" not in doc_df.columns:
            return "D0001"

        numbers = []
        for x in doc_df["document_id"].fillna("").astype(str):
            x = x.strip().upper()
            if x.startswith("D"):
                num = x[1:]
                if num.isdigit():
                    numbers.append(int(num))

        next_num = max(numbers) + 1 if numbers else 1
        return f"D{next_num:04d}"

    def get_category2_options(cat1):
        if cat1 in CATEGORY_MAP:
            return list(CATEGORY_MAP[cat1].keys())
        return ["その他"]

    def get_category3_options(cat1, cat2):
        if cat1 in CATEGORY_MAP and cat2 in CATEGORY_MAP[cat1]:
            return CATEGORY_MAP[cat1][cat2]
        return ["その他"]

    def reset_document_flags():
        st.session_state.selected_document_id = ""
        st.session_state.edit_document = False
        st.session_state.doc_view_mode = ""

    st.title("📄 書類")

    if "selected_document_id" not in st.session_state:
        st.session_state.selected_document_id = ""

    if "edit_document" not in st.session_state:
        st.session_state.edit_document = False

    if "doc_view_mode" not in st.session_state:
        st.session_state.doc_view_mode = ""

    doc_df = get_document_master_df()

    # ------------------------------------------
    # 一覧モード
    # ------------------------------------------
    if not st.session_state.selected_document_id:
        st.session_state.edit_document = False

        with st.expander("➕ 新しい書類を登録する"):
            with st.form("document_add_form"):
                add_cat1 = st.selectbox("カテゴリ1", CATEGORY1_MASTER, key="doc_add_cat1")
                add_cat2_options = get_category2_options(add_cat1)
                add_cat2 = st.selectbox("カテゴリ2", add_cat2_options, key="doc_add_cat2")
                add_cat3_options = get_category3_options(add_cat1, add_cat2)
                add_cat3 = st.selectbox("カテゴリ3", add_cat3_options, key="doc_add_cat3")

                title = st.text_input("タイトル")
                file_type = st.selectbox("ファイル種別", FILE_TYPE_MASTER)
                url = st.text_input("URL / 保存先リンク")
                summary = st.text_area("概要")
                memo = st.text_area("メモ")
                status = st.selectbox("状態", STATUS_MASTER)

                if st.form_submit_button("書類を登録する"):
                    if title.strip():
                        next_id = get_next_document_id(doc_df)
                        now_str = now_jst().strftime("%Y-%m-%d %H:%M")

                        new_row = pd.DataFrame([{
                            "document_id": next_id,
                            "category1": add_cat1,
                            "category2": add_cat2,
                            "category3": add_cat3,
                            "title": title.strip(),
                            "file_type": file_type,
                            "url": url.strip(),
                            "summary": summary.strip(),
                            "memo": memo.strip(),
                            "status": status,
                            "updated_at": now_str,
                            "created_at": now_str
                        }])

                        save_db(pd.concat([doc_df, new_row], ignore_index=True), "document_master")
                        st.success("書類を登録したある！")
                        st.rerun()
                    else:
                        st.error("タイトルを入力してほしいある。")

        st.divider()
        st.markdown("### 🔎 書類検索")

        search_col1, search_col2, search_col3, search_col4 = st.columns(4)

        with search_col1:
            search_cat1 = st.selectbox(
                "カテゴリ1",
                ["すべて"] + CATEGORY1_MASTER,
                key="doc_search_cat1"
            )

        if search_cat1 == "すべて":
            search_cat2_options = ["すべて"]
            search_cat3_options = ["すべて"]
        else:
            search_cat2_options = ["すべて"] + get_category2_options(search_cat1)
            current_search_cat2 = st.session_state.get("doc_search_cat2_dynamic", "すべて")
            if current_search_cat2 not in search_cat2_options:
                current_search_cat2 = "すべて"
                st.session_state["doc_search_cat2_dynamic"] = "すべて"

            if current_search_cat2 == "すべて":
                search_cat3_options = ["すべて"]
            else:
                search_cat3_options = ["すべて"] + get_category3_options(search_cat1, current_search_cat2)

        with search_col2:
            search_cat2 = st.selectbox(
                "カテゴリ2",
                search_cat2_options,
                key="doc_search_cat2_dynamic"
            )

        if search_cat1 == "すべて" or search_cat2 == "すべて":
            search_cat3_options = ["すべて"]
        else:
            search_cat3_options = ["すべて"] + get_category3_options(search_cat1, search_cat2)

        current_search_cat3 = st.session_state.get("doc_search_cat3_dynamic", "すべて")
        if current_search_cat3 not in search_cat3_options:
            current_search_cat3 = "すべて"
            st.session_state["doc_search_cat3_dynamic"] = "すべて"

        with search_col3:
            search_cat3 = st.selectbox(
                "カテゴリ3",
                search_cat3_options,
                key="doc_search_cat3_dynamic"
            )

        status_options = ["すべて"] + sorted([x for x in doc_df["status"].astype(str).unique().tolist() if x])

        with search_col4:
            search_status = st.selectbox("状態", status_options, key="doc_search_status")

        keyword = st.text_input("キーワード検索", placeholder="タイトル・概要・メモで検索", key="doc_keyword")

        btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 4])

        with btn_col1:
            if st.button("検索", key="doc_search_button", use_container_width=True):
                all_default = (
                    search_cat1 == "すべて" and
                    search_cat2 == "すべて" and
                    search_cat3 == "すべて" and
                    search_status == "すべて" and
                    not keyword.strip()
                )

                if all_default:
                    st.session_state.doc_view_mode = ""
                    st.warning("検索条件を1つ以上入れてから検索してほしいある。")
                else:
                    st.session_state.doc_view_mode = "search"
                    st.rerun()

        with btn_col2:
            if st.button("一覧", key="doc_list_button", use_container_width=True):
                st.session_state.doc_view_mode = "list"
                st.rerun()

        with btn_col3:
            if st.session_state.doc_view_mode == "":
                st.caption("カテゴリを選んで『検索』、または『一覧』を押してほしいある。")
            elif st.session_state.doc_view_mode == "search":
                st.caption("検索結果を表示中ある。")
            elif st.session_state.doc_view_mode == "list":
                st.caption("一覧を表示中ある。")

        st.divider()
        st.markdown("### 📚 検索結果")

        filtered_df = pd.DataFrame(columns=doc_df.columns)

        if st.session_state.doc_view_mode == "":
            st.info("まだ結果は表示していないある。検索または一覧を押してほしいある。")

        elif st.session_state.doc_view_mode == "list":
            filtered_df = doc_df.copy()

        elif st.session_state.doc_view_mode == "search":
            filtered_df = doc_df.copy()

            if search_cat1 != "すべて":
                filtered_df = filtered_df[filtered_df["category1"].astype(str) == search_cat1]

            if search_cat2 != "すべて":
                filtered_df = filtered_df[filtered_df["category2"].astype(str) == search_cat2]

            if search_cat3 != "すべて":
                filtered_df = filtered_df[filtered_df["category3"].astype(str) == search_cat3]

            if search_status != "すべて":
                filtered_df = filtered_df[filtered_df["status"].astype(str) == search_status]

            if keyword.strip():
                kw = keyword.strip()
                filtered_df = filtered_df[
                    filtered_df["title"].astype(str).str.contains(kw, case=False, na=False) |
                    filtered_df["summary"].astype(str).str.contains(kw, case=False, na=False) |
                    filtered_df["memo"].astype(str).str.contains(kw, case=False, na=False)
                ]

        if st.session_state.doc_view_mode in ["list", "search"]:
            if filtered_df.empty:
                st.warning("該当する書類が見つからなかったある。条件を変えて探してほしいある。")
            else:
                filtered_df = filtered_df.sort_values(["status", "updated_at", "title"], ascending=[True, False, True])

                cols = st.columns(2)

                for i, (_, row) in enumerate(filtered_df.iterrows()):
                    with cols[i % 2]:
                        document_id = str(row.get("document_id", "")).strip()
                        title = str(row.get("title", "")).strip()
                        category1 = str(row.get("category1", "")).strip()
                        category2 = str(row.get("category2", "")).strip()
                        category3 = str(row.get("category3", "")).strip()
                        file_type = str(row.get("file_type", "")).strip()
                        status = str(row.get("status", "")).strip()
                        summary = str(row.get("summary", "")).strip()

                        if status == "利用中":
                            border = "#2ecc71"
                            bg = "#eefbf0"
                        elif status == "旧版":
                            border = "#f39c12"
                            bg = "#fff7e6"
                        else:
                            border = "#95a5a6"
                            bg = "#f3f4f6"

                        with st.container(border=True):
                            st.markdown(
                                f"""
                                <div style="
                                    border-left: 8px solid {border};
                                    background-color: {bg};
                                    padding: 14px;
                                    border-radius: 10px;
                                    min-height: 170px;
                                    margin-bottom: 10px;
                                ">
                                    <div style="font-size: 19px; font-weight: 700; margin-bottom: 8px;">
                                        {title}
                                    </div>
                                    <div style="margin-bottom: 6px;">
                                        {category1} / {category2} / {category3}
                                    </div>
                                    <div style="margin-bottom: 6px;">
                                        種別: {file_type} / 状態: {status}
                                    </div>
                                    <div>
                                        {summary if summary else '概要なし'}
                                    </div>
                                </div>
                                """,
                                unsafe_allow_html=True
                            )

                            if st.button("詳細を見る", key=f"open_document_{document_id}", use_container_width=True):
                                st.session_state.selected_document_id = document_id
                                st.session_state.edit_document = False
                                st.rerun()

    # ------------------------------------------
    # 詳細モード
    # ------------------------------------------
    else:
        selected_id = st.session_state.selected_document_id
        detail_df = doc_df[doc_df["document_id"].astype(str) == str(selected_id)].copy()

        if detail_df.empty:
            st.warning("書類情報が見つからないある。")
            if st.button("一覧に戻る", use_container_width=True):
                reset_document_flags()
                st.rerun()
        else:
            row = detail_df.iloc[0]

            back_cols = st.columns([1, 5])
            with back_cols[0]:
                if st.button("← 一覧に戻る", use_container_width=True):
                    st.session_state.selected_document_id = ""
                    st.session_state.edit_document = False
                    st.rerun()

            st.subheader(str(row.get("title", "")).strip())
            st.caption(f"ID: {selected_id}")

            st.markdown("### 基本情報")
            info_cols = st.columns(2)

            with info_cols[0]:
                st.write(f"**カテゴリ1**: {row.get('category1', '')}")
                st.write(f"**カテゴリ2**: {row.get('category2', '')}")
                st.write(f"**カテゴリ3**: {row.get('category3', '')}")
                st.write(f"**ファイル種別**: {row.get('file_type', '')}")
                st.write(f"**状態**: {row.get('status', '')}")

            with info_cols[1]:
                st.write(f"**作成日**: {row.get('created_at', '')}")
                st.write(f"**更新日**: {row.get('updated_at', '')}")

            st.divider()
            st.markdown("### 概要")
            st.write(row.get("summary", "") if str(row.get("summary", "")).strip() else "概要なし")

            st.divider()
            st.markdown("### メモ")
            st.write(row.get("memo", "") if str(row.get("memo", "")).strip() else "メモなし")

            st.divider()
            st.markdown("### リンク")
            url_val = str(row.get("url", "")).strip()
            if url_val:
                st.link_button("書類を開く", url_val, use_container_width=True)
            else:
                st.info("URLはまだ登録されてないある。")

            st.divider()
            st.markdown("### 編集")

            if st.button("書類情報を編集", use_container_width=True):
                st.session_state.edit_document = True

            if st.session_state.get("edit_document", False):
                current_cat1 = str(row.get("category1", "")).strip()
                if current_cat1 not in CATEGORY1_MASTER:
                    current_cat1 = CATEGORY1_MASTER[0]

                current_cat2_options = get_category2_options(current_cat1)
                current_cat2 = str(row.get("category2", "")).strip()
                if current_cat2 not in current_cat2_options:
                    current_cat2 = current_cat2_options[0]

                current_cat3_options = get_category3_options(current_cat1, current_cat2)
                current_cat3 = str(row.get("category3", "")).strip()
                if current_cat3 not in current_cat3_options:
                    current_cat3 = current_cat3_options[0]

                with st.form(f"document_edit_form_{selected_id}"):
                    new_cat1 = st.selectbox(
                        "カテゴリ1",
                        CATEGORY1_MASTER,
                        index=CATEGORY1_MASTER.index(current_cat1),
                        key="doc_edit_cat1"
                    )

                    new_cat2_options = get_category2_options(new_cat1)
                    new_cat2_default = current_cat2 if current_cat2 in new_cat2_options else new_cat2_options[0]
                    new_cat2 = st.selectbox(
                        "カテゴリ2",
                        new_cat2_options,
                        index=new_cat2_options.index(new_cat2_default),
                        key="doc_edit_cat2"
                    )

                    new_cat3_options = get_category3_options(new_cat1, new_cat2)
                    new_cat3_default = current_cat3 if current_cat3 in new_cat3_options else new_cat3_options[0]
                    new_cat3 = st.selectbox(
                        "カテゴリ3",
                        new_cat3_options,
                        index=new_cat3_options.index(new_cat3_default),
                        key="doc_edit_cat3"
                    )

                    current_file_type = str(row.get("file_type", "")).strip()
                    if current_file_type not in FILE_TYPE_MASTER:
                        current_file_type = FILE_TYPE_MASTER[0]

                    current_status = str(row.get("status", "")).strip()
                    if current_status not in STATUS_MASTER:
                        current_status = STATUS_MASTER[0]

                    new_title = st.text_input("タイトル", value=str(row.get("title", "")))
                    new_file_type = st.selectbox(
                        "ファイル種別",
                        FILE_TYPE_MASTER,
                        index=FILE_TYPE_MASTER.index(current_file_type)
                    )
                    new_url = st.text_input("URL / 保存先リンク", value=str(row.get("url", "")))
                    new_summary = st.text_area("概要", value=str(row.get("summary", "")))
                    new_memo = st.text_area("メモ", value=str(row.get("memo", "")))
                    new_status = st.selectbox(
                        "状態",
                        STATUS_MASTER,
                        index=STATUS_MASTER.index(current_status)
                    )

                    save_col1, save_col2 = st.columns(2)

                    with save_col1:
                        save_doc = st.form_submit_button("保存する", use_container_width=True)
                    with save_col2:
                        cancel_doc = st.form_submit_button("キャンセル", use_container_width=True)

                    if save_doc:
                        if new_title.strip():
                            mask = doc_df["document_id"].astype(str) == str(selected_id)
                            doc_df.loc[mask, "category1"] = new_cat1
                            doc_df.loc[mask, "category2"] = new_cat2
                            doc_df.loc[mask, "category3"] = new_cat3
                            doc_df.loc[mask, "title"] = new_title.strip()
                            doc_df.loc[mask, "file_type"] = new_file_type
                            doc_df.loc[mask, "url"] = new_url.strip()
                            doc_df.loc[mask, "summary"] = new_summary.strip()
                            doc_df.loc[mask, "memo"] = new_memo.strip()
                            doc_df.loc[mask, "status"] = new_status
                            doc_df.loc[mask, "updated_at"] = now_jst().strftime("%Y-%m-%d %H:%M")

                            save_db(doc_df, "document_master")
                            st.session_state.edit_document = False
                            st.success("書類情報を更新したある！")
                            st.rerun()
                        else:
                            st.error("タイトルを入力してほしいある。")

                    if cancel_doc:
                        st.session_state.edit_document = False
                        st.rerun()