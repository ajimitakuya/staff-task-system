import streamlit as st
import pandas as pd

from common import now_jst
from data_access import load_db, save_db, get_users_df, get_user_company_permissions_df, get_attendance_logs_df, get_attendance_display_settings_df, get_companies_df

def init_attendance_runtime_state():
    if "attendance_pending_logs" not in st.session_state:
        st.session_state["attendance_pending_logs"] = []

    if "attendance_last_flush_at" not in st.session_state:
        st.session_state["attendance_last_flush_at"] = None

    if "attendance_flush_message" not in st.session_state:
        st.session_state["attendance_flush_message"] = ""

    if "attendance_last_action_message" not in st.session_state:
        st.session_state["attendance_last_action_message"] = ""


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

def go_page(page_name: str):
    """
    共通ページ移動。
    勤怠の未保存があれば先に保存してから移動する。
    """
    if not flush_attendance_before_page_change():
        return

    st.session_state.current_page = page_name
    st.rerun()

def render_attendance_page():
    st.header("勤怠管理")

    init_attendance_runtime_state()

    # 5分経過後の「次の操作時」にまとめ保存
    try:
        flush_attendance_pending_logs(force=False)
    except Exception as e:
        st.warning(f"勤怠の自動保存判定でエラーです: {e}") 

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

    top_reload_cols = st.columns([1, 1, 1, 3])

    with top_reload_cols[0]:
        if st.button("勤怠管理表示", key="attendance_mode_button", use_container_width=True):
            st.session_state["attendance_mode"] = True

            # 最初に必要なものだけ
            st.session_state["attendance_settings_df"] = get_attendance_display_settings_df()
            st.session_state["attendance_companies_df"] = get_companies_df()

            # まだ読み込まない
            st.session_state["attendance_users_df"] = None
            st.session_state["attendance_permissions_df"] = None
            st.session_state["attendance_logs_df"] = None

    with top_reload_cols[1]:
        if st.button("再読込", key="attendance_reload_button", use_container_width=True):
            # pendingがあるときは先に保存
            try:
                flush_attendance_pending_logs(force=True)
            except Exception as e:
                st.error(f"再読込前の保存に失敗しました: {e}")
                return

            # 軽いものだけ先に再読込
            st.session_state["attendance_settings_df"] = get_attendance_display_settings_df()
            st.session_state["attendance_companies_df"] = get_companies_df()

            # 重いものは現在の選択事業所に応じて後で読む
            st.session_state["attendance_users_df"] = None
            st.session_state["attendance_permissions_df"] = None
            st.session_state["attendance_logs_df"] = None
            st.session_state["attendance_loaded_company"] = ""

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

    # settingsとcompaniesだけは必須
    if settings_df is None or companies_df is None:
        st.warning("まだ勤怠データが読み込まれていません。『勤怠管理表示』を押してください。")
        return

    # ===== 必須列補完 =====
    if settings_df is None or settings_df.empty:
        settings_df = pd.DataFrame(columns=[
            "setting_id", "group_id", "slot_no", "company_id",
            "status", "created_at", "registered_by"
        ])
    else:
        for col in ["setting_id", "group_id", "slot_no", "company_id", "status", "created_at", "registered_by"]:
            if col not in settings_df.columns:
                settings_df[col] = ""

    # ===== ログイン中情報 =====
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

    if "attendance_action_mode" not in st.session_state:
        st.session_state["attendance_action_mode"] = "in"

    if "attendance_local_status" not in st.session_state:
        st.session_state["attendance_local_status"] = {}

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

                    # 先に画面上の状態を即更新
                    st.session_state["attendance_local_status"][uid] = action_text_map.get(action, "退勤")

                    new_log = {
                        "attendance_id": f"A{len(attendance_logs_df) + len(st.session_state.get('attendance_pending_logs', [])) + 1:04}",
                        "date": now.strftime("%Y-%m-%d"),
                        "user_id": uid,
                        "company_id": selected_company,
                        "action": action,
                        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                        "device_name": "tablet",
                        "recorded_by": current_user_id,
                    }

                    # 画面用のログにも先に積む
                    attendance_logs_df = pd.concat(
                        [attendance_logs_df, pd.DataFrame([new_log])],
                        ignore_index=True
                    )
                    st.session_state["attendance_logs_df"] = attendance_logs_df

                    # スプシ保存は後回し。pendingキューへ積む
                    pending_logs = st.session_state.get("attendance_pending_logs", [])
                    pending_logs.append(new_log)
                    st.session_state["attendance_pending_logs"] = pending_logs

                    # 1回クリックで即色を変えるため、ここで再描画だけする
                    st.rerun()