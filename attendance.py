import time
import streamlit as st
import pandas as pd

from common import now_jst
from data_access import (
    load_db,
    save_db,
    get_users_df,
    get_user_company_permissions_df,
    get_attendance_logs_df,
    get_attendance_display_settings_df,
    get_companies_df,
)


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

    if "attendance_action_mode" not in st.session_state:
        st.session_state["attendance_action_mode"] = "in"

    if "attendance_local_status" not in st.session_state:
        st.session_state["attendance_local_status"] = {}

    # ===== IC監視用 =====
    if "attendance_last_ic_card_id" not in st.session_state:
        st.session_state["attendance_last_ic_card_id"] = ""

    if "attendance_last_ic_processed_at" not in st.session_state:
        st.session_state["attendance_last_ic_processed_at"] = 0.0

    if "attendance_ic_message" not in st.session_state:
        st.session_state["attendance_ic_message"] = ""

    if "attendance_ic_monitor_enabled" not in st.session_state:
        st.session_state["attendance_ic_monitor_enabled"] = True

    if "attendance_debug_logs" not in st.session_state:
        st.session_state["attendance_debug_logs"] = []

    if "attendance_show_debug" not in st.session_state:
        st.session_state["attendance_show_debug"] = True

    if "attendance_auto_refresh_seconds" not in st.session_state:
        st.session_state["attendance_auto_refresh_seconds"] = 2


# =========================
# debug
# =========================
def add_attendance_debug_log(message: str):
    logs = st.session_state.get("attendance_debug_logs", [])
    ts = now_jst().strftime("%H:%M:%S")
    logs.append(f"[{ts}] {str(message)}")
    # 多すぎると重いので最新50件
    st.session_state["attendance_debug_logs"] = logs[-50:]


# =========================
# 保存
# =========================
def flush_attendance_pending_logs(force: bool = False):
    """
    pending に積んだ勤怠ログをまとめて attendance_logs へ保存する。
    force=False のときは、前回flushから5分以上経過した場合のみ保存。
    """
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

    attendance_logs_df = st.session_state.get("attendance_logs_df")
    if attendance_logs_df is None or attendance_logs_df.empty:
        attendance_logs_df = pd.DataFrame(columns=[
            "attendance_id", "date", "user_id", "company_id",
            "action", "timestamp", "device_name", "recorded_by"
        ])
    else:
        attendance_logs_df = attendance_logs_df.copy()

    add_df = pd.DataFrame(pending_logs)
    attendance_logs_df = pd.concat([attendance_logs_df, add_df], ignore_index=True)

    save_db(attendance_logs_df, "attendance_logs")

    st.session_state["attendance_logs_df"] = attendance_logs_df
    st.session_state["attendance_pending_logs"] = []
    st.session_state["attendance_last_flush_at"] = now_dt
    st.session_state["attendance_flush_message"] = (
        f"勤怠ログを {len(add_df)} 件まとめて保存しました（{now_dt.strftime('%H:%M:%S')}）"
    )
    add_attendance_debug_log(f"pending {len(add_df)}件を attendance_logs に保存")
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
        add_attendance_debug_log(f"ページ移動前保存エラー: {e}")
        return False


# =========================
# IC bridge
# =========================
def get_ic_reader_bridge_df():
    try:
        df = load_db("ic_reader_bridge")
    except Exception as e:
        add_attendance_debug_log(f"ic_reader_bridge 読込失敗: {e}")
        return pd.DataFrame(columns=["bridge_id", "device_name", "last_card_id", "last_seen_at", "status"])

    if df is None or df.empty:
        return pd.DataFrame(columns=["bridge_id", "device_name", "last_card_id", "last_seen_at", "status"])

    for col in ["bridge_id", "device_name", "last_card_id", "last_seen_at", "status"]:
        if col not in df.columns:
            df[col] = ""

    return df.fillna("")


def get_latest_ic_card_id_from_bridge_for_attendance(bridge_id: str = "main_reader", max_age_seconds: int = 20):
    df = get_ic_reader_bridge_df()
    if df.empty:
        return "", "ic_reader_bridge が空です"

    work = df.copy()
    work["bridge_id"] = work["bridge_id"].astype(str).str.strip()
    row = work[work["bridge_id"] == str(bridge_id).strip()]

    if row.empty:
        return "", f"bridge_id={bridge_id} が見つかりません"

    r = row.iloc[0]
    card_id = str(r.get("last_card_id", "")).strip().upper()
    status = str(r.get("status", "")).strip()
    last_seen_at = str(r.get("last_seen_at", "")).strip()

    add_attendance_debug_log(
        f"bridge読取: bridge_id={bridge_id} card_id={card_id or '(空)'} status={status or '(空)'} last_seen_at={last_seen_at or '(空)'}"
    )

    if not card_id:
        return "", "カードがまだ読まれていません"

    if status not in ["ready", "idle"]:
        return "", f"reader status={status}"

    try:
        seen_dt = pd.to_datetime(last_seen_at)
        now_dt = now_jst().replace(tzinfo=None)
        seen_dt = seen_dt.to_pydatetime().replace(tzinfo=None)
        age = (now_dt - seen_dt).total_seconds()
        if age > max_age_seconds:
            return "", f"カード読取が古いです（{int(age)}秒前）"
    except Exception as e:
        add_attendance_debug_log(f"last_seen_at 解釈エラー: {e}")

    return card_id, ""


# =========================
# カードID→user_id
# =========================
def find_user_id_by_card_id(card_id: str, selected_company: str, users_df, permissions_df):
    card_id = str(card_id or "").strip().upper()
    if not card_id:
        return "", "card_id が空です"

    if users_df is None or users_df.empty:
        return "", "users が空です"

    work_users = users_df.fillna("").copy()

    if "login_card_id" not in work_users.columns:
        return "", "users に login_card_id 列がありません"

    work_users["login_card_id"] = work_users["login_card_id"].astype(str).str.strip().str.upper()
    work_users["user_id"] = work_users["user_id"].astype(str).str.strip()

    hit = work_users[work_users["login_card_id"] == card_id].copy()
    if hit.empty:
        return "", "このカードIDはスタッフに登録されていません"

    if permissions_df is None or permissions_df.empty:
        return "", "権限データがありません"

    work_perm = permissions_df.fillna("").copy()
    work_perm["user_id"] = work_perm["user_id"].astype(str).str.strip()
    work_perm["company_id"] = work_perm["company_id"].astype(str).str.strip()

    hit = pd.merge(hit, work_perm, on="user_id", how="inner", suffixes=("", "_perm"))
    hit = hit[hit["company_id_perm"] == str(selected_company).strip()].copy()

    if "can_use_perm" in hit.columns:
        hit = hit[pd.to_numeric(hit["can_use_perm"], errors="coerce").fillna(0) == 1]
    elif "can_use" in hit.columns:
        hit = hit[hit["can_use"].astype(str).str.strip().isin(["1", "TRUE", "True", "true"])]

    if hit.empty:
        return "", "この事業所では使えないスタッフです"

    uid = str(hit.iloc[0]["user_id"]).strip()
    add_attendance_debug_log(f"card_id={card_id} → user_id={uid}")
    return uid, ""


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
    new_log = {
        "attendance_id": f"A{len(attendance_logs_df) + len(pending_logs) + 1:04}",
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

    msg = f"{uid} を {result_text_map.get(action, '打刻')} しました"
    st.session_state["attendance_last_action_message"] = msg
    add_attendance_debug_log(f"打刻実行: {msg} / device={device_name}")

    return attendance_logs_df


# =========================
# IC自動打刻
# =========================
def handle_ic_attendance(selected_company: str, current_user_id: str, users_df, permissions_df, attendance_logs_df, duplicate_guard_sec: int = 10):
    card_id, err = get_latest_ic_card_id_from_bridge_for_attendance("main_reader", 20)

    if err or not card_id:
        if err:
            st.session_state["attendance_ic_message"] = err
        return attendance_logs_df, False

    now_ts = time.time()
    last_card_id = str(st.session_state.get("attendance_last_ic_card_id", "")).strip().upper()
    last_processed_at = float(st.session_state.get("attendance_last_ic_processed_at", 0.0) or 0.0)

    # 同じカードは10秒以内なら無視
    if card_id == last_card_id and (now_ts - last_processed_at) < duplicate_guard_sec:
        remain = duplicate_guard_sec - int(now_ts - last_processed_at)
        st.session_state["attendance_ic_message"] = f"同一カード待機中（残り約{remain}秒）"
        add_attendance_debug_log(f"同一カード10秒ガード: card_id={card_id}")
        return attendance_logs_df, False

    uid, user_err = find_user_id_by_card_id(card_id, selected_company, users_df, permissions_df)
    if user_err:
        st.session_state["attendance_ic_message"] = user_err
        st.session_state["attendance_last_ic_card_id"] = card_id
        st.session_state["attendance_last_ic_processed_at"] = now_ts
        add_attendance_debug_log(f"user_id特定失敗: {user_err}")
        return attendance_logs_df, False

    attendance_logs_df = apply_attendance_action(
        uid=uid,
        selected_company=selected_company,
        current_user_id=current_user_id,
        attendance_logs_df=attendance_logs_df,
        device_name="ic_reader",
    )

    st.session_state["attendance_ic_message"] = f"IC打刻しました: {card_id}"
    st.session_state["attendance_last_ic_card_id"] = card_id
    st.session_state["attendance_last_ic_processed_at"] = now_ts

    return attendance_logs_df, True


# =========================
# 自動監視フラグメント
# =========================
@st.fragment
def attendance_ic_monitor_fragment(selected_company: str, current_user_id: str, users_df, permissions_df):
    if not st.session_state.get("attendance_ic_monitor_enabled", True):
        return

    interval_sec = int(st.session_state.get("attendance_auto_refresh_seconds", 2) or 2)
    time.sleep(interval_sec)

    attendance_logs_df = st.session_state.get("attendance_logs_df")
    if attendance_logs_df is None or attendance_logs_df.empty:
        attendance_logs_df = pd.DataFrame(columns=[
            "attendance_id", "date", "user_id", "company_id",
            "action", "timestamp", "device_name", "recorded_by"
        ])

    attendance_logs_df, processed = handle_ic_attendance(
        selected_company=selected_company,
        current_user_id=current_user_id,
        users_df=users_df,
        permissions_df=permissions_df,
        attendance_logs_df=attendance_logs_df,
        duplicate_guard_sec=10
    )

    st.session_state["attendance_logs_df"] = attendance_logs_df

    # 何もなくても再実行を続ける
    st.rerun()


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
        add_attendance_debug_log(f"自動保存判定エラー: {e}")

    top_reload_cols = st.columns([1, 1, 1, 3])

    with top_reload_cols[0]:
        if st.button("勤怠管理表示", key="attendance_mode_button", use_container_width=True):
            st.session_state["attendance_mode"] = True

            st.session_state["attendance_settings_df"] = get_attendance_display_settings_df()
            st.session_state["attendance_companies_df"] = get_companies_df()

            st.session_state["attendance_users_df"] = None
            st.session_state["attendance_permissions_df"] = None
            st.session_state["attendance_logs_df"] = None
            add_attendance_debug_log("勤怠管理表示を押下")

    with top_reload_cols[1]:
        if st.button("再読込", key="attendance_reload_button", use_container_width=True):
            try:
                flush_attendance_pending_logs(force=True)
            except Exception as e:
                st.error(f"再読込前の保存に失敗しました: {e}")
                add_attendance_debug_log(f"再読込前保存エラー: {e}")
                return

            st.session_state["attendance_settings_df"] = get_attendance_display_settings_df()
            st.session_state["attendance_companies_df"] = get_companies_df()

            st.session_state["attendance_users_df"] = None
            st.session_state["attendance_permissions_df"] = None
            st.session_state["attendance_logs_df"] = None
            st.session_state["attendance_loaded_company"] = ""
            add_attendance_debug_log("再読込を押下")

    with top_reload_cols[2]:
        if st.button("今すぐ保存", key="attendance_force_flush", use_container_width=True):
            try:
                saved = flush_attendance_pending_logs(force=True)
                if not saved:
                    st.info("未保存の勤怠ログはありません。")
            except Exception as e:
                st.error(f"勤怠保存でエラーです: {e}")
                add_attendance_debug_log(f"今すぐ保存エラー: {e}")

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

    # ===== group_id取得 =====
    group_id = None
    if current_company_id and companies_df is not None and not companies_df.empty:
        row = companies_df[companies_df["company_id"].astype(str).str.strip() == current_company_id]
        if not row.empty and "group_id" in row.columns:
            group_id = str(row.iloc[0]["group_id"]).strip()

    if not group_id:
        st.error("group_id が取得できません")
        add_attendance_debug_log("group_id が取得できません")
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
                    add_attendance_debug_log(f"事業所スロット{i}を削除")
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
                        add_attendance_debug_log(f"事業所登録失敗 slot={i}")
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
                            add_attendance_debug_log(f"事業所登録成功 slot={i} cid={cid}")
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

    if (
        st.session_state.get("attendance_users_df") is None
        or st.session_state.get("attendance_permissions_df") is None
        or st.session_state.get("attendance_logs_df") is None
        or current_loaded_company != str(selected_company).strip()
    ):
        st.session_state["attendance_users_df"] = get_users_df()
        st.session_state["attendance_permissions_df"] = get_user_company_permissions_df()
        st.session_state["attendance_logs_df"] = get_attendance_logs_df()
        st.session_state["attendance_loaded_company"] = str(selected_company).strip()
        add_attendance_debug_log(f"勤怠データ読込 company={selected_company}")

    users_df = st.session_state.get("attendance_users_df")
    permissions_df = st.session_state.get("attendance_permissions_df")
    attendance_logs_df = st.session_state.get("attendance_logs_df")

    if attendance_logs_df is None or attendance_logs_df.empty:
        attendance_logs_df = pd.DataFrame(columns=[
            "attendance_id", "date", "user_id", "company_id",
            "action", "timestamp", "device_name", "recorded_by"
        ])
    else:
        for col in ["attendance_id", "date", "user_id", "company_id", "action", "timestamp", "device_name", "recorded_by"]:
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
            add_attendance_debug_log("モード変更: 出勤")

    with mode_cols[1]:
        if st.button(
            "退勤",
            key="attendance_mode_out",
            use_container_width=True,
            type="primary" if st.session_state["attendance_action_mode"] == "out" else "secondary",
        ):
            st.session_state["attendance_action_mode"] = "out"
            add_attendance_debug_log("モード変更: 退勤")

    with mode_cols[2]:
        if st.button(
            "休憩開始",
            key="attendance_mode_break_start",
            use_container_width=True,
            type="primary" if st.session_state["attendance_action_mode"] == "break_start" else "secondary",
        ):
            st.session_state["attendance_action_mode"] = "break_start"
            add_attendance_debug_log("モード変更: 休憩開始")

    with mode_cols[3]:
        if st.button(
            "休憩終了",
            key="attendance_mode_break_end",
            use_container_width=True,
            type="primary" if st.session_state["attendance_action_mode"] == "break_end" else "secondary",
        ):
            st.session_state["attendance_action_mode"] = "break_end"
            add_attendance_debug_log("モード変更: 休憩終了")

    mode_label_map = {
        "in": "出勤モード",
        "out": "退勤モード",
        "break_start": "休憩開始モード",
        "break_end": "休憩終了モード",
    }

    st.info(f"現在のモード: {mode_label_map.get(st.session_state['attendance_action_mode'], '出勤モード')}")

    # ===== IC監視設定 =====
    ic_cfg_cols = st.columns([1, 1, 1])
    with ic_cfg_cols[0]:
        st.checkbox(
            "IC監視ON",
            key="attendance_ic_monitor_enabled"
        )
    with ic_cfg_cols[1]:
        st.selectbox(
            "監視間隔",
            options=[1, 2, 3, 5],
            key="attendance_auto_refresh_seconds"
        )
    with ic_cfg_cols[2]:
        if st.button("IC今すぐ確認", key="attendance_check_ic_now", use_container_width=True):
            attendance_logs_df, processed = handle_ic_attendance(
                selected_company=selected_company,
                current_user_id=current_user_id,
                users_df=users_df,
                permissions_df=permissions_df,
                attendance_logs_df=attendance_logs_df,
                duplicate_guard_sec=10
            )
            st.session_state["attendance_logs_df"] = attendance_logs_df
            if processed:
                st.rerun()

    ic_msg = str(st.session_state.get("attendance_ic_message", "")).strip()
    last_action_msg = str(st.session_state.get("attendance_last_action_message", "")).strip()

    if ic_msg:
        st.caption(f"IC状態: {ic_msg}")
    if last_action_msg:
        st.caption(f"最新打刻: {last_action_msg}")

    # ===== bridge内容の見える化 =====
    bridge_df = get_ic_reader_bridge_df()
    with st.expander("IC bridge デバッグ情報", expanded=st.session_state.get("attendance_show_debug", True)):
        if bridge_df.empty:
            st.write("ic_reader_bridge は空です。")
        else:
            show_bridge = bridge_df.copy()
            st.dataframe(show_bridge, use_container_width=True)

        st.write("最後に処理したカード:", st.session_state.get("attendance_last_ic_card_id", ""))
        st.write("最後に処理した時刻(epoch):", st.session_state.get("attendance_last_ic_processed_at", 0.0))

        debug_logs = st.session_state.get("attendance_debug_logs", [])
        if debug_logs:
            st.text("\n".join(debug_logs))
        else:
            st.write("debugログはまだありません。")

    # ===== 自動監視 =====
    attendance_ic_monitor_fragment(
        selected_company=selected_company,
        current_user_id=current_user_id,
        users_df=users_df,
        permissions_df=permissions_df,
    )

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