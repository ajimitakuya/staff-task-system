import streamlit as st
import pandas as pd
import uuid
from datetime import datetime
from io import BytesIO
from pathlib import Path

from openpyxl import Workbook

from common import now_jst
from data_access import (
    save_db,
    get_users_df,
    get_user_company_permissions_df,
    get_attendance_logs_df,
    get_attendance_display_settings_df,
    get_companies_df,
    get_ic_reader_bridge_df,
    get_ic_card_users_df,
    get_ic_attendance_logs_df,
)

from supabase import create_client

SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

ATTENDANCE_LOG_COLS = [
    "attendance_id",
    "date",
    "user_id",
    "company_id",
    "action",
    "timestamp",
    "device_name",
    "recorded_by",
]

IC_BRIDGE_COLS = ["bridge_id", "device_name", "card_id", "touched_at", "status"]

IC_CARD_USER_COLS = ["card_id", "user_id", "user_name", "company_id", "is_active", "note"]

IC_ATTENDANCE_LOG_COLS = [
    "log_id", "date", "user_id", "user_name", "company_id",
    "action", "action_label", "timestamp", "device_name",
    "card_id", "source", "memo",
]


def _df_with_columns(rows, cols):
    df = pd.DataFrame(rows or [])
    if df.empty:
        df = pd.DataFrame(columns=cols)
    else:
        for col in cols:
            if col not in df.columns:
                df[col] = ""
    return df.fillna("")


@st.cache_data(ttl=2, show_spinner=False)
def _fetch_attendance_logs_for_company_date(company_id: str, date_str: str):
    cid = str(company_id or "").strip()
    d = str(date_str or "").strip()
    if not cid or not d:
        return pd.DataFrame(columns=ATTENDANCE_LOG_COLS)

    res = (
        supabase.table("attendance_logs")
        .select("*")
        .eq("company_id", cid)
        .eq("date", d)
        .order("timestamp", desc=True)
        .limit(2000)
        .execute()
    )
    return _df_with_columns(res.data, ATTENDANCE_LOG_COLS)


def _fetch_ic_reader_bridge_row_live(bridge_id: str):
    bid = str(bridge_id or "").strip()
    if not bid:
        return {}

    res = (
        supabase.table("ic_reader_bridge")
        .select("*")
        .eq("bridge_id", bid)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    if not rows:
        return {}

    row = dict(rows[0])
    for col in IC_BRIDGE_COLS:
        row.setdefault(col, "")
    return row


@st.cache_data(ttl=10, show_spinner=False)
def _fetch_ic_card_users_for_card(company_id: str, card_id: str):
    cid = str(company_id or "").strip()
    card = _normalize_card_id(card_id)
    if not cid or not card:
        return pd.DataFrame(columns=IC_CARD_USER_COLS)

    res = (
        supabase.table("ic_card_users")
        .select("*")
        .eq("company_id", cid)
        .eq("card_id", card)
        .limit(5)
        .execute()
    )
    return _df_with_columns(res.data, IC_CARD_USER_COLS)


@st.cache_data(ttl=30, show_spinner=False)
def _fetch_ic_attendance_logs_for_range(company_id: str, start_date_str: str, end_date_str: str):
    cid = str(company_id or "").strip()
    start = str(start_date_str or "").strip()
    end = str(end_date_str or "").strip()
    if not cid or not start or not end:
        return pd.DataFrame(columns=IC_ATTENDANCE_LOG_COLS)

    res = (
        supabase.table("ic_attendance_logs")
        .select("*")
        .eq("company_id", cid)
        .gte("date", start)
        .lte("date", end)
        .order("timestamp", desc=False)
        .limit(10000)
        .execute()
    )
    return _df_with_columns(res.data, IC_ATTENDANCE_LOG_COLS)


def _clear_attendance_fast_caches():
    for fn in [
        _fetch_attendance_logs_for_company_date,
        _fetch_ic_card_users_for_card,
        _fetch_ic_attendance_logs_for_range,
    ]:
        try:
            fn.clear()
        except Exception:
            pass

# =========================
# 初期化
# =========================
def init_attendance_runtime_state():
    if "attendance_pending_logs" not in st.session_state:
        st.session_state["attendance_pending_logs"] = []

    if "attendance_last_flush_at" not in st.session_state:
        st.session_state["attendance_last_flush_at"] = None

    if "attendance_flush_message" not in st.session_state:
        st.session_state["attendance_flush_message"] = ""

    if "attendance_last_action_message" not in st.session_state:
        st.session_state["attendance_last_action_message"] = ""

    if "attendance_mode" not in st.session_state:
        st.session_state["attendance_mode"] = False

    if "attendance_users_df" not in st.session_state:
        st.session_state["attendance_users_df"] = None

    if "attendance_permissions_df" not in st.session_state:
        st.session_state["attendance_permissions_df"] = None

    if "attendance_logs_df" not in st.session_state:
        st.session_state["attendance_logs_df"] = None

    if "attendance_settings_df" not in st.session_state:
        st.session_state["attendance_settings_df"] = None

    if "attendance_companies_df" not in st.session_state:
        st.session_state["attendance_companies_df"] = None

    if "attendance_loaded_company" not in st.session_state:
        st.session_state["attendance_loaded_company"] = ""

    if "attendance_loaded_date" not in st.session_state:
        st.session_state["attendance_loaded_date"] = ""

    if "attendance_action_mode" not in st.session_state:
        st.session_state["attendance_action_mode"] = "in"

    if "attendance_local_status" not in st.session_state:
        st.session_state["attendance_local_status"] = {}

    # 将来のIC再開用フラグだけ残す
    if "attendance_ic_enabled" not in st.session_state:
        st.session_state["attendance_ic_enabled"] = False

    if "attendance_ic_bridge_id" not in st.session_state:
        st.session_state["attendance_ic_bridge_id"] = "main_reader"

    if "attendance_ic_last_event_key" not in st.session_state:
        st.session_state["attendance_ic_last_event_key"] = ""


# =========================
# 保存
# =========================
def flush_attendance_pending_logs(force: bool = False):

    pending_logs = st.session_state.get("attendance_pending_logs", [])
    if not pending_logs:
        return False

    now_dt = now_jst()
    last_flush_at = st.session_state.get("attendance_last_flush_at")

    should_flush = force
    if not should_flush:
        if last_flush_at is None:
            should_flush = True
        else:
            try:
                delta = (now_dt - last_flush_at).total_seconds()
                should_flush = delta >= 300
            except Exception:
                should_flush = True

    if not should_flush:
        return False

    try:
        # 🔥 insertのみ（IDはDBが作る）
        supabase.table("attendance_logs").insert(pending_logs).execute()
    except Exception as e:
        st.error(f"勤怠保存エラー: {e}")
        return False

    st.session_state["attendance_pending_logs"] = []
    st.session_state["attendance_last_flush_at"] = now_dt
    st.session_state["attendance_flush_message"] = (
        f"{len(pending_logs)}件保存（{now_dt.strftime('%H:%M:%S')}）"
    )

    return True


def flush_attendance_before_page_change():
    """
    勤怠の未保存ログがあれば、ページ移動前に必ず保存する
    """
    pending_logs = st.session_state.get("attendance_pending_logs", [])
    if not pending_logs:
        return True

    try:
        flush_attendance_pending_logs(force=True)
        return True
    except Exception as e:
        st.error(f"ページ移動前の勤怠保存に失敗しました: {e}")
        return False


# =========================
# 打刻処理共通
# =========================
def apply_attendance_action(uid: str, selected_company: str, current_user_id: str, attendance_logs_df, device_name: str = "tablet"):
    now = now_jst()
    action = st.session_state.get("attendance_action_mode", "in")

    action_text_map = {
        "in": "出勤",
        "out": "退勤",
        "break_start": "休憩中",
        "break_end": "出勤",
    }

    result_text_map = {
        "in": "出勤",
        "out": "退勤",
        "break_start": "休憩開始",
        "break_end": "休憩終了",
    }

    st.session_state["attendance_local_status"][uid] = action_text_map.get(action, "退勤")

    pending_logs = st.session_state.get("attendance_pending_logs", [])

    # 🔥 attendance_idは完全削除
    new_log = {
        "date": now.strftime("%Y-%m-%d"),
        "user_id": uid,
        "company_id": selected_company,
        "action": action,
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "device_name": device_name,
        "recorded_by": current_user_id,
    }

    attendance_logs_df = pd.concat(
        [attendance_logs_df, pd.DataFrame([new_log])],
        ignore_index=True
    )

    st.session_state["attendance_logs_df"] = attendance_logs_df

    pending_logs.append(new_log)
    st.session_state["attendance_pending_logs"] = pending_logs

    st.session_state["attendance_last_action_message"] = (
        f"{uid} を {result_text_map.get(action, '打刻')} しました"
    )

    return attendance_logs_df


def _attendance_action_label(action: str) -> str:
    return {
        "in": "出勤",
        "out": "退勤",
        "break_start": "休憩開始",
        "break_end": "休憩終了",
    }.get(str(action or "").strip(), "打刻")


def _is_active_value(value) -> bool:
    s = str(value or "").strip().lower()
    return s not in ["0", "false", "inactive", "無効", "停止"]


def _normalize_card_id(card_id: str) -> str:
    return str(card_id or "").strip().upper()


def _mark_ic_bridge_status(bridge_id: str, card_id: str, status: str):
    try:
        supabase.table("ic_reader_bridge").update({
            "status": status,
            "card_id": _normalize_card_id(card_id),
            "touched_at": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
        }).eq("bridge_id", bridge_id).execute()
    except Exception:
        pass


def _insert_ic_attendance_log(uid: str, user_name: str, selected_company: str, action: str, device_name: str, card_id: str, memo: str = ""):
    now = now_jst()
    row = {
        "log_id": str(uuid.uuid4()),
        "date": now.strftime("%Y-%m-%d"),
        "user_id": str(uid).strip(),
        "user_name": str(user_name or uid).strip(),
        "company_id": str(selected_company).strip(),
        "action": str(action).strip(),
        "action_label": _attendance_action_label(action),
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "device_name": str(device_name or "ic_reader").strip(),
        "card_id": _normalize_card_id(card_id),
        "source": "ic_card",
        "memo": memo,
    }
    supabase.table("ic_attendance_logs").insert(row).execute()
    _clear_attendance_fast_caches()
    return row


def process_ic_bridge_touch(selected_company: str, current_user_id: str, attendance_logs_df):
    bridge_id = str(st.session_state.get("attendance_ic_bridge_id", "main_reader")).strip() or "main_reader"

    bridge_df = get_ic_reader_bridge_df()
    if bridge_df is None or bridge_df.empty:
        return attendance_logs_df, "ic_reader_bridge にブリッジ行がありません。", "warning"

    for col in ["bridge_id", "device_name", "card_id", "touched_at", "status"]:
        if col not in bridge_df.columns:
            bridge_df[col] = ""

    target = bridge_df[
        bridge_df["bridge_id"].astype(str).str.strip() == bridge_id
    ].copy()

    if target.empty:
        return attendance_logs_df, f"bridge_id={bridge_id} が見つかりません。", "warning"

    row = target.iloc[0]
    status = str(row.get("status", "")).strip()
    card_id = _normalize_card_id(row.get("card_id", ""))
    touched_at = str(row.get("touched_at", "")).strip()
    device_name = str(row.get("device_name", "ic_reader")).strip() or "ic_reader"

    if status != "ready" or not card_id:
        return attendance_logs_df, "ICカードの待機中です。カードをタッチしてください。", "info"

    event_key = f"{bridge_id}|{card_id}|{touched_at}"
    if event_key == str(st.session_state.get("attendance_ic_last_event_key", "")):
        return attendance_logs_df, "このICタッチは処理済みです。", "info"

    card_users_df = get_ic_card_users_df()
    if card_users_df is None or card_users_df.empty:
        return attendance_logs_df, "ic_card_users にカード紐づけがありません。", "error"

    for col in ["card_id", "user_id", "user_name", "company_id", "is_active"]:
        if col not in card_users_df.columns:
            card_users_df[col] = ""

    mapped = card_users_df[
        (card_users_df["card_id"].astype(str).str.upper().str.strip() == card_id) &
        (card_users_df["company_id"].astype(str).str.strip() == str(selected_company).strip())
    ].copy()

    if not mapped.empty:
        mapped = mapped[mapped["is_active"].apply(_is_active_value)]

    if mapped.empty:
        return attendance_logs_df, f"未登録カードです: {card_id}", "error"

    user_row = mapped.iloc[0]
    uid = str(user_row.get("user_id", "")).strip()
    user_name = str(user_row.get("user_name", uid)).strip() or uid

    if not uid:
        return attendance_logs_df, f"カード {card_id} の user_id が空です。", "error"

    action = st.session_state.get("attendance_action_mode", "in")

    attendance_logs_df = apply_attendance_action(
        uid=uid,
        selected_company=selected_company,
        current_user_id=current_user_id,
        attendance_logs_df=attendance_logs_df,
        device_name=device_name,
    )

    flush_attendance_pending_logs(force=True)
    _insert_ic_attendance_log(
        uid=uid,
        user_name=user_name,
        selected_company=selected_company,
        action=action,
        device_name=device_name,
        card_id=card_id,
        memo=f"bridge_id={bridge_id}",
    )

    st.session_state["attendance_ic_last_event_key"] = event_key
    _mark_ic_bridge_status(bridge_id, card_id, "processed")

    msg = f"{user_name} を {_attendance_action_label(action)} しました（IC）"
    st.session_state["attendance_last_action_message"] = msg

    return attendance_logs_df, msg, "success"


def process_ic_bridge_touch_fast(selected_company: str, current_user_id: str, attendance_logs_df):
    bridge_id = str(st.session_state.get("attendance_ic_bridge_id", "main_reader")).strip() or "main_reader"

    row = _fetch_ic_reader_bridge_row_live(bridge_id)
    if not row:
        return attendance_logs_df, "ic_reader_bridgeに対象の行がありません。", "warning"
    st.session_state["attendance_ic_last_bridge_row"] = row

    status = str(row.get("status", "")).strip()
    card_id = _normalize_card_id(row.get("card_id", ""))
    touched_at = str(row.get("touched_at", "")).strip()
    device_name = str(row.get("device_name", "ic_reader")).strip() or "ic_reader"

    if status != "ready" or not card_id:
        return attendance_logs_df, "ICカードのタッチ待機中です。", "info"

    event_key = f"{bridge_id}|{card_id}|{touched_at}"
    if event_key == str(st.session_state.get("attendance_ic_last_event_key", "")):
        return attendance_logs_df, "このICタッチは処理済みです。", "info"

    card_users_df = _fetch_ic_card_users_for_card(selected_company, card_id)
    if card_users_df is None or card_users_df.empty:
        return attendance_logs_df, f"未登録カードです: {card_id}", "error"

    mapped = card_users_df.copy()
    if "is_active" in mapped.columns:
        mapped = mapped[mapped["is_active"].apply(_is_active_value)]

    if mapped.empty:
        return attendance_logs_df, f"無効または未登録のカードです: {card_id}", "error"

    user_row = mapped.iloc[0]
    uid = str(user_row.get("user_id", "")).strip()
    user_name = str(user_row.get("user_name", uid)).strip() or uid

    if not uid:
        return attendance_logs_df, f"カード {card_id} の user_id が空です。", "error"

    action = st.session_state.get("attendance_action_mode", "in")

    attendance_logs_df = apply_attendance_action(
        uid=uid,
        selected_company=selected_company,
        current_user_id=current_user_id,
        attendance_logs_df=attendance_logs_df,
        device_name=device_name,
    )

    flush_attendance_pending_logs(force=True)
    _insert_ic_attendance_log(
        uid=uid,
        user_name=user_name,
        selected_company=selected_company,
        action=action,
        device_name=device_name,
        card_id=card_id,
        memo=f"bridge_id={bridge_id}",
    )

    st.session_state["attendance_ic_last_event_key"] = event_key
    _mark_ic_bridge_status(bridge_id, card_id, "processed")
    st.session_state["attendance_ic_last_bridge_row"] = {
        **row,
        "status": "processed",
        "card_id": card_id,
        "touched_at": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
    }

    msg = f"{user_name} を {_attendance_action_label(action)} しました（IC）"
    st.session_state["attendance_last_action_message"] = msg

    return attendance_logs_df, msg, "success"


def _build_daily_rows_from_ic_logs(logs_df, selected_company: str, start_date, end_date):
    cols = [
        "date", "user_id", "user_name", "company_id",
        "clock_in", "break_start", "break_end", "clock_out",
        "break_minutes", "work_minutes", "status", "note",
    ]

    if logs_df is None or logs_df.empty:
        return pd.DataFrame(columns=cols)

    df = logs_df.copy().fillna("")
    for col in ["date", "user_id", "user_name", "company_id", "action", "timestamp"]:
        if col not in df.columns:
            df[col] = ""

    df = df[df["company_id"].astype(str).str.strip() == str(selected_company).strip()].copy()
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp_dt"]).copy()

    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)
    df = df[(df["timestamp_dt"] >= start_ts) & (df["timestamp_dt"] < end_ts)].copy()

    if df.empty:
        return pd.DataFrame(columns=cols)

    df["date"] = df["timestamp_dt"].dt.strftime("%Y-%m-%d")
    rows = []

    for (date_str, uid), g in df.sort_values("timestamp_dt").groupby(["date", "user_id"], dropna=False):
        user_name = str(g.iloc[0].get("user_name", uid)).strip() or str(uid)

        def first_time(action):
            x = g[g["action"].astype(str).str.strip() == action]
            if x.empty:
                return None
            return x.iloc[0]["timestamp_dt"]

        def last_time(action):
            x = g[g["action"].astype(str).str.strip() == action]
            if x.empty:
                return None
            return x.iloc[-1]["timestamp_dt"]

        clock_in = first_time("in")
        break_start = first_time("break_start")
        break_end = last_time("break_end")
        clock_out = last_time("out")

        break_minutes = 0
        if break_start is not None and break_end is not None and break_end >= break_start:
            break_minutes = int((break_end - break_start).total_seconds() // 60)

        work_minutes = ""
        if clock_in is not None and clock_out is not None and clock_out >= clock_in:
            work_minutes = max(0, int((clock_out - clock_in).total_seconds() // 60) - break_minutes)

        missing = []
        if clock_in is None:
            missing.append("出勤")
        if clock_out is None:
            missing.append("退勤")

        status = "OK" if not missing else "未完了"
        note = "" if not missing else "未打刻: " + "・".join(missing)

        def fmt(dt):
            return "" if dt is None else dt.strftime("%H:%M")

        rows.append({
            "date": date_str,
            "user_id": str(uid).strip(),
            "user_name": user_name,
            "company_id": str(selected_company).strip(),
            "clock_in": fmt(clock_in),
            "break_start": fmt(break_start),
            "break_end": fmt(break_end),
            "clock_out": fmt(clock_out),
            "break_minutes": str(break_minutes),
            "work_minutes": "" if work_minutes == "" else str(work_minutes),
            "status": status,
            "note": note,
        })

    return pd.DataFrame(rows, columns=cols).fillna("")


def _replace_ic_daily_rows(daily_df):
    if daily_df is None or daily_df.empty:
        return

    for _, row in daily_df.iterrows():
        data = row.to_dict()
        date_str = str(data.get("date", "")).strip()
        uid = str(data.get("user_id", "")).strip()
        cid = str(data.get("company_id", "")).strip()
        if not date_str or not uid or not cid:
            continue
        try:
            (
                supabase.table("ic_attendance_daily")
                .delete()
                .eq("date", date_str)
                .eq("user_id", uid)
                .eq("company_id", cid)
                .execute()
            )
        except Exception:
            pass
        supabase.table("ic_attendance_daily").insert(data).execute()

    _clear_attendance_fast_caches()


def _build_attendance_workbook_bytes(daily_df, company_name: str, start_date, end_date):
    wb = Workbook()
    ws = wb.active
    ws.title = "出勤簿"

    ws.append([f"出勤簿 {company_name}", "", "", "", "", "", "", "", "", "", "", ""])
    ws.append([f"対象期間 {start_date} 〜 {end_date}", "", "", "", "", "", "", "", "", "", "", ""])
    ws.append([])

    headers = [
        "日付", "利用者ID", "氏名", "出勤", "休憩開始", "休憩終了",
        "退勤", "休憩分", "勤務分", "状態", "備考",
    ]
    ws.append(headers)

    for _, row in daily_df.iterrows():
        ws.append([
            row.get("date", ""),
            row.get("user_id", ""),
            row.get("user_name", ""),
            row.get("clock_in", ""),
            row.get("break_start", ""),
            row.get("break_end", ""),
            row.get("clock_out", ""),
            row.get("break_minutes", ""),
            row.get("work_minutes", ""),
            row.get("status", ""),
            row.get("note", ""),
        ])

    for col in range(1, 12):
        ws.column_dimensions[chr(64 + col)].width = 14
    ws.column_dimensions["C"].width = 20
    ws.column_dimensions["K"].width = 28

    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)
    return bio.getvalue()


# =========================
# メイン画面
# =========================
def render_attendance_page():
    st.header("勤怠管理")

    init_attendance_runtime_state()

    # 5分経過後の「次の操作時」にまとめ保存
    try:
        flush_attendance_pending_logs(force=False)
    except Exception as e:
        st.warning(f"勤怠の自動保存判定でエラーです: {e}")

    top_reload_cols = st.columns([1, 1, 1, 3])

    with top_reload_cols[0]:
        if st.button("勤怠管理表示", key="attendance_mode_button", use_container_width=True):
            st.session_state["attendance_mode"] = True

            st.session_state["attendance_settings_df"] = get_attendance_display_settings_df()
            st.session_state["attendance_companies_df"] = get_companies_df()

            st.session_state["attendance_users_df"] = None
            st.session_state["attendance_permissions_df"] = None
            st.session_state["attendance_logs_df"] = None

    with top_reload_cols[1]:
        if st.button("再読込", key="attendance_reload_button", use_container_width=True):
            try:
                flush_attendance_pending_logs(force=True)
            except Exception as e:
                st.error(f"再読込前の保存に失敗しました: {e}")
                return

            st.session_state["attendance_settings_df"] = get_attendance_display_settings_df()
            st.session_state["attendance_companies_df"] = get_companies_df()

            st.session_state["attendance_users_df"] = None
            st.session_state["attendance_permissions_df"] = None
            st.session_state["attendance_logs_df"] = None
            st.session_state["attendance_loaded_company"] = ""
            st.session_state["attendance_loaded_date"] = ""

    with top_reload_cols[2]:
        if st.button("今すぐ保存", key="attendance_force_flush", use_container_width=True):
            try:
                saved = flush_attendance_pending_logs(force=True)
                if not saved:
                    st.info("未保存の勤怠ログはありません。")
            except Exception as e:
                st.error(f"勤怠保存でエラーです: {e}")

    with top_reload_cols[3]:
        pending_count = len(st.session_state.get("attendance_pending_logs", []))
        flush_msg = str(st.session_state.get("attendance_flush_message", "")).strip()

        if pending_count > 0:
            st.warning(f"未保存ログ: {pending_count}件")
        elif flush_msg:
            st.success(flush_msg)

    if not st.session_state.get("attendance_mode", False):
        st.info("「勤怠管理表示」を押すと読み込みます。")
        return

    settings_df = st.session_state.get("attendance_settings_df")
    companies_df = st.session_state.get("attendance_companies_df")

    if settings_df is None or companies_df is None:
        st.warning("まだ勤怠データが読み込まれていません。『勤怠管理表示』を押してください。")
        return

    if settings_df is None or settings_df.empty:
        settings_df = pd.DataFrame(columns=[
            "setting_id", "group_id", "slot_no", "company_id",
            "status", "created_at", "registered_by"
        ])
    else:
        for col in ["setting_id", "group_id", "slot_no", "company_id", "status", "created_at", "registered_by"]:
            if col not in settings_df.columns:
                settings_df[col] = ""

    current_company_id = str(st.session_state.get("company_id", "")).strip()
    current_user_id = str(st.session_state.get("user_id", "")).strip()

    group_id = None
    if current_company_id and companies_df is not None and not companies_df.empty:
        row = companies_df[companies_df["company_id"].astype(str).str.strip() == current_company_id]
        if not row.empty and "group_id" in row.columns:
            group_id = str(row.iloc[0]["group_id"]).strip()

    if not group_id:
        st.error("group_id が取得できません")
        return

    st.subheader("事業所登録（最大5件）")

    existing_settings = settings_df[
        (settings_df["group_id"].astype(str).str.strip() == group_id) &
        (settings_df["status"].astype(str).str.strip() == "active")
    ].copy()

    for i in range(1, 6):
        slot_row = existing_settings[
            pd.to_numeric(existing_settings["slot_no"], errors="coerce").fillna(0).astype(int) == i
        ]

        registered_company_name = ""
        registered_company_id = ""

        if not slot_row.empty:
            registered_company_id = str(slot_row.iloc[0]["company_id"]).strip()
            comp_row = companies_df[
                companies_df["company_id"].astype(str).str.strip() == registered_company_id
            ]
            if not comp_row.empty:
                registered_company_name = str(comp_row.iloc[0].get("company_name", "")).strip()

        st.markdown(f"### 事業所{i}")

        if registered_company_id:
            col_a, col_b = st.columns([4, 1])
            with col_a:
                st.success(f"登録済み: {registered_company_name}（{registered_company_id}）")
            with col_b:
                if st.button("削除", key=f"delete_slot_{i}", use_container_width=True):
                    idx = slot_row.index
                    settings_df.loc[idx, "status"] = "inactive"
                    save_db(settings_df, "attendance_display_settings")
                    st.rerun()
        else:
            col1, col2, col3 = st.columns([2, 2, 1])

            with col1:
                login_id = st.text_input(f"事業所ID_{i}", key=f"attendance_id_{i}")

            with col2:
                password = st.text_input(f"事業所PASS_{i}", type="password", key=f"attendance_pass_{i}")

            with col3:
                st.write("")
                if st.button(f"登録_{i}", key=f"attendance_register_{i}", use_container_width=True):
                    comp = companies_df[
                        (companies_df["company_login_id"].astype(str).str.strip() == str(login_id).strip()) &
                        (companies_df["company_login_password"].astype(str).str.strip() == str(password).strip()) &
                        (companies_df["status"].astype(str).str.strip() == "active")
                    ]

                    if comp.empty:
                        st.error("認証失敗")
                    else:
                        cid = str(comp.iloc[0]["company_id"]).strip()

                        same_active = settings_df[
                            (settings_df["group_id"].astype(str).str.strip() == group_id) &
                            (settings_df["company_id"].astype(str).str.strip() == cid) &
                            (settings_df["status"].astype(str).str.strip() == "active")
                        ]
                        if not same_active.empty:
                            st.warning("その事業所はもう登録済みです")
                        else:
                            new_row = {
                                "setting_id": f"ADS{len(settings_df) + 1:04}",
                                "group_id": group_id,
                                "slot_no": i,
                                "company_id": cid,
                                "status": "active",
                                "created_at": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
                                "registered_by": current_user_id,
                            }
                            settings_df = pd.concat([settings_df, pd.DataFrame([new_row])], ignore_index=True)
                            save_db(settings_df, "attendance_display_settings")
                            st.success(f"{cid} を登録しました。")
                            st.rerun()

    target_settings = settings_df[
        (settings_df["group_id"].astype(str).str.strip() == group_id) &
        (settings_df["status"].astype(str).str.strip() == "active")
    ].copy()

    if target_settings.empty:
        st.info("登録済み事業所がまだありません。")
        return

    target_settings["slot_no_num"] = pd.to_numeric(target_settings["slot_no"], errors="coerce").fillna(9999)
    target_settings = target_settings.sort_values("slot_no_num")

    company_ids = target_settings["company_id"].astype(str).str.strip().tolist()

    company_name_map = {}
    for cid in company_ids:
        comp_row = companies_df[companies_df["company_id"].astype(str).str.strip() == cid]
        if not comp_row.empty:
            company_name_map[cid] = str(comp_row.iloc[0].get("company_name", cid)).strip()
        else:
            company_name_map[cid] = cid

    selected_company = st.radio(
        "事業所選択",
        options=company_ids,
        format_func=lambda x: company_name_map.get(x, x),
        horizontal=True,
        key="attendance_selected_company"
    )

    current_loaded_company = str(st.session_state.get("attendance_loaded_company", "")).strip()
    today_str = now_jst().strftime("%Y-%m-%d")
    current_loaded_date = str(st.session_state.get("attendance_loaded_date", "")).strip()

    if (
        st.session_state.get("attendance_users_df") is None
        or st.session_state.get("attendance_permissions_df") is None
        or st.session_state.get("attendance_logs_df") is None
        or current_loaded_company != str(selected_company).strip()
        or current_loaded_date != today_str
    ):
        st.session_state["attendance_users_df"] = get_users_df()
        st.session_state["attendance_permissions_df"] = get_user_company_permissions_df()
        st.session_state["attendance_logs_df"] = _fetch_attendance_logs_for_company_date(
            str(selected_company).strip(),
            today_str,
        )
        st.session_state["attendance_loaded_company"] = str(selected_company).strip()
        st.session_state["attendance_loaded_date"] = today_str

    users_df = st.session_state.get("attendance_users_df")
    permissions_df = st.session_state.get("attendance_permissions_df")
    attendance_logs_df = st.session_state.get("attendance_logs_df")

    if attendance_logs_df is None or attendance_logs_df.empty:
        attendance_logs_df = pd.DataFrame(columns=[
            "date",
            "user_id",
            "company_id",
            "action",
            "timestamp",
            "device_name",
            "recorded_by"
        ])
    else:
        for col in ["date", "user_id", "company_id", "action", "timestamp", "device_name", "recorded_by"]:
            if col not in attendance_logs_df.columns:
                attendance_logs_df[col] = ""

    valid_users = users_df
    if "attendance_enabled" in valid_users.columns:
        valid_users = valid_users[
            pd.to_numeric(valid_users["attendance_enabled"], errors="coerce").fillna(0).astype(int) == 1
        ]
    if "status" in valid_users.columns:
        valid_users = valid_users[
            valid_users["status"].astype(str).str.strip() == "active"
        ]

    merged = pd.merge(valid_users, permissions_df, on="user_id", how="inner", suffixes=("", "_perm"))
    merged = merged[
        merged["company_id_perm"].astype(str).str.strip() == selected_company
    ].copy()

    if "can_use_perm" in merged.columns:
        merged = merged[
            pd.to_numeric(merged["can_use_perm"], errors="coerce").fillna(0) == 1
        ]
    elif "can_use" in merged.columns:
        merged = merged[
            merged["can_use"].astype(str).str.strip().isin(["1", "TRUE", "True", "true"])
        ]

    if "display_order" in merged.columns:
        merged["display_order_num"] = pd.to_numeric(merged["display_order"], errors="coerce").fillna(9999)
        merged = merged.sort_values("display_order_num")
    else:
        merged = merged.sort_values("display_name")

    st.markdown("---")

    def get_status(user_id):
        user_id = str(user_id).strip()

        local_map = st.session_state.get("attendance_local_status", {})
        if user_id in local_map:
            return local_map[user_id]

        logs = attendance_logs_df[
            (attendance_logs_df["user_id"].astype(str).str.strip() == user_id) &
            (attendance_logs_df["company_id"].astype(str).str.strip() == selected_company)
        ].copy()

        if logs.empty:
            return "退勤"

        if "timestamp" in logs.columns:
            logs = logs.sort_values("timestamp")

        last = logs.iloc[-1]
        last_action = str(last.get("action", "")).strip()

        action_map = {
            "in": "出勤",
            "out": "退勤",
            "break_start": "休憩中",
            "break_end": "出勤",
        }
        return action_map.get(last_action, "退勤")

    st.markdown("""
    <style>
    div[data-testid="stButton"] > button {
        height: 56px !important;
        width: 100% !important;
        font-size: 18px !important;
        font-weight: 600 !important;
        border-radius: 10px !important;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("### 勤怠モード選択")

    mode_cols = st.columns(4)

    with mode_cols[0]:
        if st.button(
            "出勤",
            key="attendance_mode_in",
            use_container_width=True,
            type="primary" if st.session_state["attendance_action_mode"] == "in" else "secondary",
        ):
            st.session_state["attendance_action_mode"] = "in"

    with mode_cols[1]:
        if st.button(
            "退勤",
            key="attendance_mode_out",
            use_container_width=True,
            type="primary" if st.session_state["attendance_action_mode"] == "out" else "secondary",
        ):
            st.session_state["attendance_action_mode"] = "out"

    with mode_cols[2]:
        if st.button(
            "休憩開始",
            key="attendance_mode_break_start",
            use_container_width=True,
            type="primary" if st.session_state["attendance_action_mode"] == "break_start" else "secondary",
        ):
            st.session_state["attendance_action_mode"] = "break_start"

    with mode_cols[3]:
        if st.button(
            "休憩終了",
            key="attendance_mode_break_end",
            use_container_width=True,
            type="primary" if st.session_state["attendance_action_mode"] == "break_end" else "secondary",
        ):
            st.session_state["attendance_action_mode"] = "break_end"

    mode_label_map = {
        "in": "出勤モード",
        "out": "退勤モード",
        "break_start": "休憩開始モード",
        "break_end": "休憩終了モード",
    }

    st.info(f"現在のモード: {mode_label_map.get(st.session_state['attendance_action_mode'], '出勤モード')}")

    last_action_msg = str(st.session_state.get("attendance_last_action_message", "")).strip()
    if last_action_msg:
        st.caption(f"最新打刻: {last_action_msg}")

    with st.expander("IC打刻", expanded=False):
        st.caption("ICカードリーダー側の ic_bridge_local.py が Supabase の ic_reader_bridge に書き込んだ内容を読み取ります。")
        st.checkbox("IC打刻を使う", key="attendance_ic_enabled")
        st.text_input("bridge_id", key="attendance_ic_bridge_id")

        ic_cols = st.columns([1, 2])
        with ic_cols[0]:
            if st.button("ICタッチ確認", key="attendance_ic_check", use_container_width=True):
                if not st.session_state.get("attendance_ic_enabled", False):
                    st.warning("IC打刻を使うにチェックしてください。")
                else:
                    try:
                        attendance_logs_df, msg, level = process_ic_bridge_touch_fast(
                            selected_company=selected_company,
                            current_user_id=current_user_id,
                            attendance_logs_df=attendance_logs_df,
                        )
                        if level == "success":
                            st.success(msg)
                            st.rerun()
                        elif level == "error":
                            st.error(msg)
                        elif level == "warning":
                            st.warning(msg)
                        else:
                            st.info(msg)
                    except Exception as e:
                        st.error(f"IC打刻エラー: {e}")

        with ic_cols[1]:
            try:
                last_bridge_row = st.session_state.get("attendance_ic_last_bridge_row", {})
                bridge_df = _df_with_columns([last_bridge_row] if last_bridge_row else [], IC_BRIDGE_COLS)
                bridge_id = str(st.session_state.get("attendance_ic_bridge_id", "main_reader")).strip()
                if bridge_df is not None and not bridge_df.empty:
                    view = bridge_df[bridge_df["bridge_id"].astype(str).str.strip() == bridge_id].copy()
                    if not view.empty:
                        row = view.iloc[0]
                        st.caption(
                            f"状態: {row.get('status', '')} / card_id: {row.get('card_id', '')} / touched_at: {row.get('touched_at', '')}"
                        )
                    else:
                        st.caption("指定 bridge_id の行はまだありません。")
                else:
                    st.caption("ic_reader_bridge は空です。")
            except Exception as e:
                st.caption(f"IC状態取得エラー: {e}")

    with st.expander("出勤簿作成", expanded=False):
        today = now_jst().date()
        month_start = today.replace(day=1)
        export_cols = st.columns([1, 1, 1])
        with export_cols[0]:
            start_date = st.date_input("開始日", value=month_start, key="attendance_export_start")
        with export_cols[1]:
            end_date = st.date_input("終了日", value=today, key="attendance_export_end")
        with export_cols[2]:
            st.write("")
            st.write("")
            make_book = st.button("出勤簿作成", key="attendance_make_book", use_container_width=True)

        if make_book:
            try:
                ic_logs_df = _fetch_ic_attendance_logs_for_range(
                    str(selected_company).strip(),
                    str(start_date),
                    str(end_date),
                )
                daily_df = _build_daily_rows_from_ic_logs(
                    ic_logs_df,
                    selected_company=selected_company,
                    start_date=start_date,
                    end_date=end_date,
                )

                if daily_df.empty:
                    st.warning("対象期間のIC勤怠ログがありません。")
                else:
                    _replace_ic_daily_rows(daily_df)

                    company_name = company_name_map.get(selected_company, selected_company)
                    book_bytes = _build_attendance_workbook_bytes(
                        daily_df,
                        company_name=company_name,
                        start_date=start_date,
                        end_date=end_date,
                    )

                    safe_company = "".join(c if c.isalnum() else "_" for c in str(company_name))[:30]
                    out_dir = Path(__file__).resolve().parent / "出勤簿出力"
                    out_dir.mkdir(exist_ok=True)
                    filename = f"出勤簿_{safe_company}_{start_date}_{end_date}.xlsx"
                    out_path = out_dir / filename
                    out_path.write_bytes(book_bytes)

                    st.success(f"出勤簿を作成しました: {out_path}")
                    st.download_button(
                        "出勤簿をダウンロード",
                        data=book_bytes,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="attendance_download_book",
                    )
            except Exception as e:
                st.error(f"出勤簿作成エラー: {e}")

    users_list = merged.to_dict(orient="records")

    for start_idx in range(0, len(users_list), 5):
        cols = st.columns(5)

        for col_idx in range(5):
            data_idx = start_idx + col_idx

            with cols[col_idx]:
                if data_idx >= len(users_list):
                    st.write("")
                    continue

                row = users_list[data_idx]

                uid = str(row["user_id"]).strip()
                name = str(row.get("display_name", uid)).strip()
                status = get_status(uid)

                button_type = "primary" if status in ["出勤", "休憩中"] else "secondary"
                button_label = f"{name}\n{status}"

                if st.button(
                    button_label,
                    key=f"attendance_user_{selected_company}_{uid}",
                    use_container_width=True,
                    type=button_type,
                ):
                    attendance_logs_df = apply_attendance_action(
                        uid=uid,
                        selected_company=selected_company,
                        current_user_id=current_user_id,
                        attendance_logs_df=attendance_logs_df,
                        device_name="tablet",
                    )
                    st.rerun()
