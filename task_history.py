from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from task_board import get_tasks_df


JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


def normalize_task_df(df):
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "company_id",
            "id",
            "task",
            "status",
            "user",
            "limit",
            "priority",
            "updated_at",
        ])

    work = df.copy().fillna("")
    for col in ["company_id", "id", "task", "status", "user", "limit", "priority", "updated_at"]:
        if col not in work.columns:
            work[col] = ""

    return work


def sort_task_df(df):
    if df is None or df.empty:
        return df

    work = df.copy()
    prio_map = {"至急": 0, "重要": 1, "通常": 2}
    work["priority_sort"] = work["priority"].map(prio_map).fillna(9)

    try:
        work = work.sort_values(
            ["priority_sort", "limit", "updated_at"],
            ascending=[True, True, False]
        )
    except Exception:
        pass

    return work


def render_task_history_page():
    st.title("📈 稼働状況・完了履歴")
    st.caption("この事業所の作業中タスクと、完了済みタスクを確認するページある。")

    df = get_tasks_df()
    df = normalize_task_df(df)

    if df.empty:
        st.info("まだタスクがないある。")
        return

    filter_cols = st.columns([2, 1, 1])

    with filter_cols[0]:
        keyword = st.text_input("検索", key="task_history_keyword")

    with filter_cols[1]:
        status_filter = st.selectbox(
            "表示対象",
            ["すべて", "作業中", "完了"],
            key="task_history_status_filter"
        )

    with filter_cols[2]:
        user_filter = st.text_input("担当者で絞る", key="task_history_user_filter")

    work = df.copy()

    if keyword.strip():
        kw = keyword.strip()
        work = work[
            work["task"].astype(str).str.contains(kw, case=False, na=False) |
            work["user"].astype(str).str.contains(kw, case=False, na=False)
        ].copy()

    if status_filter != "すべて":
        work = work[
            work["status"].astype(str).str.strip() == status_filter
        ].copy()

    if user_filter.strip():
        uf = user_filter.strip()
        work = work[
            work["user"].astype(str).str.contains(uf, case=False, na=False)
        ].copy()

    doing_df = work[work["status"].astype(str).str.strip() == "作業中"].copy()
    done_df = work[work["status"].astype(str).str.strip() == "完了"].copy()

    doing_df = sort_task_df(doing_df)
    done_df = sort_task_df(done_df)

    summary_cols = st.columns(3)
    with summary_cols[0]:
        st.metric("未着手", int((df["status"].astype(str).str.strip() == "未着手").sum()))
    with summary_cols[1]:
        st.metric("作業中", int((df["status"].astype(str).str.strip() == "作業中").sum()))
    with summary_cols[2]:
        st.metric("完了", int((df["status"].astype(str).str.strip() == "完了").sum()))

    st.divider()

    st.subheader("⚡ 作業中")
    if doing_df.empty:
        st.info("現在、作業中のタスクはないある。")
    else:
        show_doing = doing_df[["priority", "limit", "task", "user", "updated_at"]].copy()
        show_doing.columns = ["優先度", "期限", "タスク", "担当者", "更新日時"]
        st.dataframe(show_doing, use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("✅ 完了履歴")
    if done_df.empty:
        st.info("まだ完了済みタスクはないある。")
    else:
        show_done = done_df[["priority", "limit", "task", "user", "updated_at"]].copy()
        show_done.columns = ["優先度", "期限", "タスク", "担当者", "更新日時"]
        st.dataframe(show_done, use_container_width=True, hide_index=True)