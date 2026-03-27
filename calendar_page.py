import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone

from db import get_df

JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


def get_current_company_id():
    return str(st.session_state.get("company_id", "")).strip()


def render_calendar_page():
    st.title("⑥ カレンダー")
    st.caption("タスク期限と日誌入力をまとめて確認できるある。")

    company_id = get_current_company_id()

    # =========================
    # データ取得
    # =========================
    task_df = get_df("task")
    diary_df = get_df("diary_input_rules")

    # =========================
    # 正規化
    # =========================
    if task_df is None or task_df.empty:
        task_df = pd.DataFrame(columns=["company_id", "task", "limit", "status"])
    else:
        task_df = task_df.fillna("")

    if diary_df is None or diary_df.empty:
        diary_df = pd.DataFrame(columns=["company_id", "date", "resident_name"])
    else:
        diary_df = diary_df.fillna("")

    # 事業所フィルタ
    task_df = task_df[task_df["company_id"].astype(str) == company_id]
    diary_df = diary_df[diary_df["company_id"].astype(str) == company_id]

    # =========================
    # 月選択
    # =========================
    today = now_jst().date()

    col1, col2 = st.columns(2)

    with col1:
        year = st.selectbox("年", list(range(today.year - 1, today.year + 2)), index=1)

    with col2:
        month = st.selectbox("月", list(range(1, 13)), index=today.month - 1)

    # =========================
    # カレンダー生成
    # =========================
    first_day = datetime(year, month, 1).date()
    last_day = (datetime(year, month, 1) + timedelta(days=32)).replace(day=1).date() - timedelta(days=1)

    days = (last_day - first_day).days + 1

    # =========================
    # 日別イベント作成
    # =========================
    calendar_data = {}

    for i in range(days):
        d = first_day + timedelta(days=i)
        calendar_data[str(d)] = []

    # タスク（期限）
    for _, row in task_df.iterrows():
        d = str(row.get("limit", "")).strip()
        if d in calendar_data:
            calendar_data[d].append({
                "type": "task",
                "text": f"📋 {row.get('task', '')}",
                "status": row.get("status", "")
            })

    # 日誌
    for _, row in diary_df.iterrows():
        d = str(row.get("date", "")).strip()
        if d in calendar_data:
            calendar_data[d].append({
                "type": "diary",
                "text": f"📝 {row.get('resident_name', '')}",
                "status": row.get("send_status", "")
            })

    # =========================
    # 表示
    # =========================
    st.subheader(f"{year}年 {month}月")

    cols = st.columns(7)
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]

    for i in range(7):
        cols[i].markdown(f"**{weekdays[i]}**")

    start_weekday = first_day.weekday()  # 月=0

    rows = []

    row = [""] * start_weekday

    for i in range(days):
        d = first_day + timedelta(days=i)
        key = str(d)

        cell = f"**{d.day}日**\n"

        for event in calendar_data[key]:
            if event["type"] == "task":
                cell += f"📋 {event['text']}\n"
            else:
                if event["status"] == "sent":
                    cell += f"🟢 {event['text']}\n"
                else:
                    cell += f"🔴 {event['text']}\n"

        row.append(cell)

        if len(row) == 7:
            rows.append(row)
            row = []

    if row:
        row += [""] * (7 - len(row))
        rows.append(row)

    for r in rows:
        cols = st.columns(7)
        for i in range(7):
            cols[i].markdown(r[i])