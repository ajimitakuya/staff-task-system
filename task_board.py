from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from db import get_df, save_db


JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


def get_current_company_id():
    return str(st.session_state.get("company_id", "")).strip()


def normalize_company_scoped_df(df, required_cols):
    if df is None or df.empty:
        return pd.DataFrame(columns=required_cols)

    work = df.copy().fillna("")

    for col in required_cols:
        if col not in work.columns:
            work[col] = ""

    work = work[required_cols].copy()

    if "company_id" in work.columns:
        work["company_id"] = work["company_id"].astype(str).str.strip()

    return work


def filter_by_company_id(df, company_id):
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if df is not None else [])

    if "company_id" not in df.columns:
        return df.copy()

    return df[df["company_id"].astype(str).str.strip() == str(company_id).strip()].copy()


def get_task_required_cols():
    return [
        "company_id",
        "id",
        "task",
        "status",
        "user",
        "limit",
        "priority",
        "updated_at",
    ]


def get_tasks_df(company_id=None):
    if company_id is None:
        company_id = get_current_company_id()

    df = get_df("task")
    required_cols = get_task_required_cols()

    work = normalize_company_scoped_df(df, required_cols)
    return filter_by_company_id(work, company_id)


def get_next_task_id(task_df=None):
    if task_df is None:
        task_df = get_tasks_df()

    if task_df is None or task_df.empty:
        return 1

    ids = pd.to_numeric(task_df["id"], errors="coerce").dropna()
    return int(ids.max()) + 1 if not ids.empty else 1


def sync_task_events_to_calendar(company_id=None):
    """
    旧app.pyでは task → calendar 同期が入っていたが、
    今回はまず安全版として no-op にしておくある。
    あとで calendar.py を戻すときに本接続するある。
    """
    return


def start_task(task_id, company_id=None):
    if company_id is None:
        company_id = get_current_company_id()

    all_df = get_df("task")
    required_cols = get_task_required_cols()
    all_df = normalize_company_scoped_df(all_df, required_cols)

    mask = (
        (all_df["company_id"].astype(str).str.strip() == str(company_id).strip()) &
        (all_df["id"].astype(str).str.strip() == str(task_id).strip())
    )

    if not mask.any():
        return False

    all_df.loc[mask, "status"] = "作業中"
    all_df.loc[mask, "user"] = str(st.session_state.get("user", "")).strip()
    all_df.loc[mask, "updated_at"] = now_jst().strftime("%Y-%m-%d %H:%M")

    save_db(all_df, "task")
    sync_task_events_to_calendar(company_id)
    return True


def complete_task(task_id, company_id=None):
    if company_id is None:
        company_id = get_current_company_id()

    all_df = get_df("task")
    required_cols = get_task_required_cols()
    all_df = normalize_company_scoped_df(all_df, required_cols)

    current_user = str(st.session_state.get("user", "")).strip()

    mask = (
        (all_df["company_id"].astype(str).str.strip() == str(company_id).strip()) &
        (all_df["id"].astype(str).str.strip() == str(task_id).strip()) &
        (all_df["user"].astype(str).str.strip() == current_user)
    )

    if not mask.any():
        return False

    all_df.loc[mask, "status"] = "完了"
    all_df.loc[mask, "updated_at"] = now_jst().strftime("%Y-%m-%d %H:%M")

    save_db(all_df, "task")
    sync_task_events_to_calendar(company_id)
    return True


def render_task_board_page():
    st.title("📋 未着手タスク一覧")
    st.write("現在、依頼されている業務の一覧です。新しいタスクを登録することも可能です。")

    current_company_id = get_current_company_id()

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
                    df = get_df("task")
                    required_cols = get_task_required_cols()
                    df = normalize_company_scoped_df(df, required_cols)

                    next_id = get_next_task_id(filter_by_company_id(df, current_company_id))
                    new_rows = []
                    now_str = now_jst().strftime("%Y-%m-%d %H:%M")

                    for task_name in lines:
                        new_rows.append({
                            "company_id": current_company_id,
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
                    sync_task_events_to_calendar(current_company_id)

                    st.success(f"{len(new_rows)}件のタスクを登録したある！")
                    st.rerun()
                else:
                    st.error("タスクを1件以上入力してください。")

    df = get_tasks_df(current_company_id)
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


def render_my_tasks_page():
    st.title("🎯 タスク管理")

    current_company_id = get_current_company_id()
    df = get_tasks_df(current_company_id)

    st.subheader("📦 新しくタスクを引き受ける")
    todo = df[df["status"].astype(str).str.strip() == "未着手"].copy()

    if todo.empty:
        st.write("引き受け可能なタスクはありません。")

    for _, row in todo.iterrows():
        p_symbol = "🔴 [至急]" if row["priority"] == "至急" else "🟡 [重要]" if row["priority"] == "重要" else "⚪ [通常]"
        if st.button(
            f"{p_symbol} {row['task']} (期限:{row['limit']}) を開始する",
            key=f"get_{current_company_id}_{row['id']}"
        ):
            start_task(row["id"], current_company_id)
            st.rerun()

    st.divider()
    st.subheader("⚡ 現在対応中のタスク")

    my_tasks = df[
        (df["status"].astype(str).str.strip() == "作業中") &
        (df["user"].astype(str).str.strip() == str(st.session_state.get("user", "")).strip())
    ].copy()

    if my_tasks.empty:
        st.write("現在、対応中のタスクはありません。")

    for _, row in my_tasks.iterrows():
        if st.button(
            f"✅ {row['task']} の完了を報告する",
            key=f"done_{current_company_id}_{row['id']}",
            type="primary"
        ):
            complete_task(row["id"], current_company_id)
            st.rerun()


def render_sidebar_task_status():
    try:
        task_df = get_tasks_df().fillna("")
    except Exception:
        task_df = pd.DataFrame(columns=get_task_required_cols())

    current_user = str(st.session_state.get("user", "")).strip()

    my_active = task_df[
        (task_df["status"].astype(str).str.strip() == "作業中") &
        (task_df["user"].astype(str).str.strip() == current_user)
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