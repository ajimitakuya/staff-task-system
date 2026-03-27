import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone

from db import get_df

JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


def get_current_company_id():
    return str(st.session_state.get("company_id", "")).strip()


def render_record_status_page():
    st.title("⑤ 記録状況")
    st.caption("日誌入力の状況・送信状況を確認するページある。")

    company_id = get_current_company_id()

    df = get_df("diary_input_rules")

    if df is None or df.empty:
        st.info("まだ記録がないある。")
        return

    # 必須列補完
    required_cols = [
        "record_id", "company_id", "date", "resident_name",
        "staff_name", "service_type",
        "start_time", "end_time",
        "send_status", "sent_at"
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = ""

    df = df.fillna("").copy()

    # 事業所フィルタ
    df = df[df["company_id"].astype(str).str.strip() == company_id]

    if df.empty:
        st.info("この事業所の記録はまだないある。")
        return

    # =========================
    # 📅 フィルタ
    # =========================
    st.subheader("🔍 絞り込み")

    cols = st.columns(3)

    with cols[0]:
        date_filter = st.date_input("日付", value=None)

    with cols[1]:
        staff_filter = st.text_input("職員名")

    with cols[2]:
        status_filter = st.selectbox(
            "送信状態",
            ["すべて", "draft", "sent", "error"]
        )

    work = df.copy()

    # 日付
    if date_filter:
        work = work[work["date"].astype(str) == str(date_filter)]

    # 職員
    if staff_filter.strip():
        work = work[
            work["staff_name"].astype(str).str.contains(
                staff_filter.strip(), case=False, na=False
            )
        ]

    # 状態
    if status_filter != "すべて":
        work = work[
            work["send_status"].astype(str).str.strip() == status_filter
        ]

    # =========================
    # 📊 サマリー
    # =========================
    st.subheader("📊 状況サマリー")

    total = len(work)
    sent = len(work[work["send_status"] == "sent"])
    draft = len(work[work["send_status"] == "draft"])
    error = len(work[work["send_status"] == "error"])

    s_cols = st.columns(4)
    s_cols[0].metric("全体", total)
    s_cols[1].metric("送信済", sent)
    s_cols[2].metric("下書き", draft)
    s_cols[3].metric("エラー", error)

    st.divider()

    # =========================
    # 📋 一覧
    # =========================
    st.subheader("📋 記録一覧")

    if work.empty:
        st.info("該当データなしある。")
        return

    # 並び替え
    try:
        work = work.sort_values(["date", "record_id"], ascending=[False, False])
    except Exception:
        pass

    show_cols = [
        "date",
        "resident_name",
        "staff_name",
        "service_type",
        "start_time",
        "end_time",
        "send_status",
        "sent_at"
    ]

    st.dataframe(
        work[show_cols],
        use_container_width=True,
        hide_index=True
    )

    # =========================
    # ⚠️ 未送信アラート
    # =========================
    st.subheader("⚠️ 未送信チェック")

    unsent = work[work["send_status"] != "sent"]

    if unsent.empty:
        st.success("未送信はないある！完璧ある🔥")
    else:
        for _, row in unsent.iterrows():
            st.warning(
                f"{row['date']} / {row['resident_name']} / {row['staff_name']} → 未送信"
            )