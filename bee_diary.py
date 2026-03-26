from datetime import datetime, timedelta, timezone
from typing import Callable, Optional, Tuple, Dict, Any

import pandas as pd
import streamlit as st

from db import save_db, get_df
from auth import (
    authenticate_company_login,
    get_company_saved_knowbe_info,
)


JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


# =========================
# 基本データ取得
# =========================
def get_current_company_id() -> str:
    return str(st.session_state.get("company_id", "")).strip()


def get_diary_input_rules_required_cols():
    return [
        "record_id", "company_id", "date", "resident_id", "resident_name",
        "start_time", "end_time", "work_start_time", "work_end_time", "work_break_time",
        "meal_flag", "note",
        "start_memo", "end_memo", "staff_name",
        "generated_status", "generated_support", "created_at",
        "service_type", "knowbe_target", "send_status", "sent_at", "send_error",
        "record_mode"
    ]


def get_diary_input_rules_df(company_id=None):
    if company_id is None:
        company_id = get_current_company_id()

    df = get_df("diary_input_rules")
    if df is None or df.empty:
        df = pd.DataFrame(columns=get_diary_input_rules_required_cols())
    else:
        for col in get_diary_input_rules_required_cols():
            if col not in df.columns:
                df[col] = ""

    df = df.fillna("").copy()
    df["company_id"] = df["company_id"].astype(str).str.strip()
    return df[df["company_id"] == str(company_id).strip()].copy()


def get_resident_master_df(company_id=None):
    if company_id is None:
        company_id = get_current_company_id()

    required_cols = [
        "company_id",
        "resident_id", "resident_name", "status",
        "consultant", "consultant_phone",
        "caseworker", "caseworker_phone",
        "hospital", "hospital_phone",
        "nurse", "nurse_phone",
        "care", "care_phone",
        "created_at", "updated_at"
    ]

    df = get_df("resident_master")
    if df is None or df.empty:
        df = pd.DataFrame(columns=required_cols)
    else:
        for col in required_cols:
            if col not in df.columns:
                df[col] = ""

    df = df.fillna("").copy()
    df["company_id"] = df["company_id"].astype(str).str.strip()
    return df[df["company_id"] == str(company_id).strip()].copy()


def get_staff_example_row(company_id: str, staff_name: str):
    df = get_df("staff_examples")

    if df is None or df.empty:
        return None

    df = df.fillna("").copy()

    for col in [
        "company_id",
        "staff_name",
        "home_start_example", "home_end_example",
        "day_start_example", "day_end_example",
        "outside_start_example", "outside_end_example",
        "updated_at",
    ]:
        if col not in df.columns:
            df[col] = ""

    df["company_id"] = df["company_id"].astype(str).str.strip()
    df["staff_name"] = df["staff_name"].astype(str).str.strip()

    hit = df[
        (df["company_id"] == str(company_id).strip()) &
        (df["staff_name"] == str(staff_name).strip())
    ].copy()

    if hit.empty:
        return None

    return hit.iloc[0].to_dict()


def get_personal_rule_row(company_id: str, staff_name: str):
    df = get_df("personal_rules")

    if df is None or df.empty:
        return None

    df = df.fillna("").copy()

    for col in [
        "company_id",
        "staff_name",
        "rule_text",
        "updated_at",
    ]:
        if col not in df.columns:
            df[col] = ""

    df["company_id"] = df["company_id"].astype(str).str.strip()
    df["staff_name"] = df["staff_name"].astype(str).str.strip()

    hit = df[
        (df["company_id"] == str(company_id).strip()) &
        (df["staff_name"] == str(staff_name).strip())
    ].copy()

    if hit.empty:
        return None

    return hit.iloc[0].to_dict()


# =========================
# diary_input_rules 保存
# =========================
def save_diary_input_record(
    date,
    resident_id,
    resident_name,
    start_time,
    end_time,
    work_start_time,
    work_end_time,
    work_break_time,
    meal_flag,
    note,
    start_memo,
    end_memo,
    staff_name,
    generated_status="",
    generated_support="",
    service_type="在宅",
    knowbe_target="",
    send_status="draft",
    sent_at="",
    send_error="",
    record_mode="gemini",
    company_id=""
):
    all_df = get_df("diary_input_rules")
    if all_df is None or all_df.empty:
        all_df = pd.DataFrame(columns=get_diary_input_rules_required_cols())
    else:
        for col in get_diary_input_rules_required_cols():
            if col not in all_df.columns:
                all_df[col] = ""

    all_df = all_df.fillna("").copy()

    if all_df.empty:
        next_id = 1
    else:
        nums = pd.to_numeric(all_df["record_id"], errors="coerce").dropna()
        next_id = int(nums.max()) + 1 if not nums.empty else 1

    created_at = now_jst().strftime("%Y-%m-%d %H:%M:%S")

    if not company_id:
        company_id = get_current_company_id()

    new_row = pd.DataFrame([{
        "record_id": next_id,
        "company_id": str(company_id),
        "date": str(date),
        "resident_id": str(resident_id),
        "resident_name": str(resident_name),
        "start_time": str(start_time),
        "end_time": str(end_time),
        "work_start_time": str(work_start_time),
        "work_end_time": str(work_end_time),
        "work_break_time": str(work_break_time),
        "meal_flag": str(meal_flag),
        "note": str(note),
        "start_memo": str(start_memo),
        "end_memo": str(end_memo),
        "staff_name": str(staff_name),
        "generated_status": str(generated_status),
        "generated_support": str(generated_support),
        "created_at": created_at,
        "service_type": str(service_type),
        "knowbe_target": str(knowbe_target),
        "send_status": str(send_status),
        "sent_at": str(sent_at),
        "send_error": str(send_error),
        "record_mode": str(record_mode),
    }])

    all_df = pd.concat([all_df, new_row], ignore_index=True)
    save_db(all_df, "diary_input_rules")
    return next_id


def update_diary_input_record_status(record_id, send_status, sent_at="", send_error=""):
    df = get_df("diary_input_rules")

    if df is None or df.empty:
        return False

    for col in get_diary_input_rules_required_cols():
        if col not in df.columns:
            df[col] = ""

    mask = df["record_id"].astype(str) == str(record_id)

    if not mask.any():
        return False

    df.loc[mask, "send_status"] = str(send_status)
    df.loc[mask, "sent_at"] = str(sent_at)
    df.loc[mask, "send_error"] = str(send_error)

    save_db(df, "diary_input_rules")
    return True


# =========================
# 軽い補助関数
# =========================
def _to_minutes(hhmm: str):
    s = str(hhmm).strip()
    if not s or ":" not in s:
        return None
    try:
        h, m = s.split(":", 1)
        return int(h) * 60 + int(m)
    except Exception:
        return None


def normalize_time_text(value: str) -> str:
    s = str(value).strip()
    if not s:
        return ""
    if ":" not in s:
        return s
    try:
        h, m = s.split(":", 1)
        return f"{int(h):02d}:{int(m):02d}"
    except Exception:
        return s


def get_service_example_key(service_type: str, memo_kind: str) -> str:
    """
    memo_kind: start / end
    """
    service_type = str(service_type).strip()

    if service_type == "在宅":
        return "home_start_example" if memo_kind == "start" else "home_end_example"
    if service_type == "通所":
        return "day_start_example" if memo_kind == "start" else "day_end_example"
    return "outside_start_example" if memo_kind == "start" else "outside_end_example"


def build_generation_context(company_id: str, staff_name: str, service_type: str) -> Dict[str, str]:
    example_row = get_staff_example_row(company_id, staff_name) or {}
    rule_row = get_personal_rule_row(company_id, staff_name) or {}

    start_example_key = get_service_example_key(service_type, "start")
    end_example_key = get_service_example_key(service_type, "end")

    return {
        "staff_name": str(staff_name).strip(),
        "service_type": str(service_type).strip(),
        "staff_start_example": str(example_row.get(start_example_key, "")).strip(),
        "staff_end_example": str(example_row.get(end_example_key, "")).strip(),
        "personal_rule_text": str(rule_row.get("rule_text", "")).strip(),
    }


# =========================
# 送信先解決
# =========================
def resolve_bee_target_context(
    use_other_company: bool,
    temp_company_login_id: str = "",
    temp_company_login_password: str = "",
    temp_knowbe_username: str = "",
    temp_knowbe_password: str = "",
) -> Tuple[bool, Dict[str, str], str]:
    """
    戻り値:
      ok, context, error_message

    context keys:
      company_id
      company_name
      knowbe_login_username
      knowbe_login_password
      knowbe_target_label
      target_mode  # own / temporary_other
    """
    if not use_other_company:
        company_id = str(st.session_state.get("company_id", "")).strip()
        company_name = str(st.session_state.get("company_name", "")).strip()
        saved_user, saved_pw = get_company_saved_knowbe_info(company_id)

        if not saved_user or not saved_pw:
            return False, {}, "現在ログイン中の事業所に Knowbe 情報が保存されてないある。"

        return True, {
            "company_id": company_id,
            "company_name": company_name,
            "knowbe_login_username": str(saved_user).strip(),
            "knowbe_login_password": str(saved_pw).strip(),
            "knowbe_target_label": company_name,
            "target_mode": "own",
        }, ""

    company_row = authenticate_company_login(temp_company_login_id, temp_company_login_password)
    if company_row is None:
        return False, {}, "他事業所の事業所IDまたは事業所パスワードが違うある。"

    if not str(temp_knowbe_username).strip() or not str(temp_knowbe_password).strip():
        return False, {}, "一時送信用の Knowbe アカウント名とパスワードを入れてほしいある。"

    return True, {
        "company_id": str(company_row.get("company_id", "")).strip(),
        "company_name": str(company_row.get("company_name", "")).strip(),
        "knowbe_login_username": str(temp_knowbe_username).strip(),
        "knowbe_login_password": str(temp_knowbe_password).strip(),
        "knowbe_target_label": str(company_row.get("company_name", "")).strip(),
        "target_mode": "temporary_other",
    }, ""


# =========================
# hook ベースの生成・送信
# =========================
def default_generate_diary_texts(payload: Dict[str, Any]) -> Dict[str, str]:
    """
    既存Gemini関数につなぐまでの安全な仮実装。
    """
    resident_name = str(payload.get("resident_name", "")).strip()
    service_type = str(payload.get("service_type", "")).strip()
    note = str(payload.get("note", "")).strip()
    start_time = str(payload.get("start_time", "")).strip()
    end_time = str(payload.get("end_time", "")).strip()
    meal_flag = str(payload.get("meal_flag", "")).strip()

    start_memo = f"{resident_name}さんの{service_type}利用について受付したある。開始予定は{start_time}ある。"
    if note:
        start_memo += f" 連絡事項: {note}"

    end_memo = f"{resident_name}さんの記録をまとめたある。終了予定は{end_time}ある。"
    if meal_flag:
        end_memo += f" 食事提供: {meal_flag}"

    return {
        "generated_status": start_memo,
        "generated_support": end_memo,
    }


def default_send_to_knowbe(payload: Dict[str, Any]) -> Tuple[bool, str]:
    """
    既存Selenium送信関数につなぐまでの仮実装。
    app.py 完成版で本物に差し替える前提ある。
    """
    return False, "Knowbe送信本体はまだ接続してないある。最後に app.py 完成版でつなぐある。"


# =========================
# 画面本体
# =========================
def render_bee_diary_page(
    generate_fn: Optional[Callable[[Dict[str, Any]], Dict[str, str]]] = None,
    send_fn: Optional[Callable[[Dict[str, Any]], Tuple[bool, str]]] = None,
):
    """
    generate_fn:
      payload -> {
          "generated_status": "...",
          "generated_support": "..."
      }

    send_fn:
      payload -> (success: bool, message: str)
    """
    if generate_fn is None:
        generate_fn = default_generate_diary_texts
    if send_fn is None:
        send_fn = default_send_to_knowbe

    st.title("🐝 Knowbe日誌入力")
    st.caption("通常は自事業所へ送信、必要時だけ他事業所へ一時送信できるある。")

    current_company_id = get_current_company_id()
    current_company_name = str(st.session_state.get("company_name", "")).strip()
    current_user_name = str(st.session_state.get("user", "")).strip()

    top_cols = st.columns([1, 1])
    with top_cols[0]:
        if st.button("← 戻る", key="back_from_bee_diary", use_container_width=True):
            st.session_state.current_page = "① 未着手の任務（掲示板）"
            st.rerun()
    with top_cols[1]:
        st.info(f"ログイン中: {current_company_name} / {current_user_name}")

    st.divider()

    resident_df = get_resident_master_df(current_company_id)
    resident_options = []
    resident_map = {}

    if resident_df is not None and not resident_df.empty:
        work = resident_df.copy()
        work["resident_name"] = work["resident_name"].astype(str).str.strip()
        work["resident_id"] = work["resident_id"].astype(str).str.strip()
        try:
            work = work.sort_values(["resident_name"], ascending=[True])
        except Exception:
            pass

        for _, row in work.iterrows():
            rid = str(row.get("resident_id", "")).strip()
            rname = str(row.get("resident_name", "")).strip()
            label = f"{rname} ({rid})" if rid else rname
            resident_options.append(label)
            resident_map[label] = {
                "resident_id": rid,
                "resident_name": rname,
            }

    basic_cols = st.columns(3)
    with basic_cols[0]:
        input_date = st.date_input("日付", value=now_jst().date(), key="bee_date")
    with basic_cols[1]:
        service_type = st.selectbox("サービス種別", ["在宅", "通所", "施設外"], key="bee_service_type")
    with basic_cols[2]:
        staff_name = st.text_input("記録者名", value=current_user_name, key="bee_staff_name")

    resident_label = st.selectbox("利用者", options=[""] + resident_options, key="bee_resident_label")
    resident_id = resident_map.get(resident_label, {}).get("resident_id", "")
    resident_name = resident_map.get(resident_label, {}).get("resident_name", "")

    time_cols = st.columns(5)
    with time_cols[0]:
        start_time = normalize_time_text(st.text_input("開始時刻", value="10:00", key="bee_start_time"))
    with time_cols[1]:
        end_time = normalize_time_text(st.text_input("終了時刻", value="12:00", key="bee_end_time"))
    with time_cols[2]:
        work_start_time = normalize_time_text(st.text_input("作業開始", value="", key="bee_work_start_time"))
    with time_cols[3]:
        work_end_time = normalize_time_text(st.text_input("作業終了", value="", key="bee_work_end_time"))
    with time_cols[4]:
        work_break_time = st.text_input("休憩（分）", value="0", key="bee_work_break_time")

    meal_flag = st.selectbox("食事提供", ["", "あり", "なし"], key="bee_meal_flag")
    note = st.text_area("メモ・原文", key="bee_note", height=120)

    st.divider()

    st.markdown("### 送信先")
    use_other_company = st.checkbox("他事業所へ一時送信する", key="bee_use_other_company")

    if not use_other_company:
        st.success(f"通常送信先: {current_company_name}")
    else:
        temp_cols1 = st.columns(2)
        with temp_cols1[0]:
            temp_company_login_id = st.text_input("他事業所ID", key="bee_temp_company_login_id")
        with temp_cols1[1]:
            temp_company_login_password = st.text_input(
                "他事業所パスワード",
                type="password",
                key="bee_temp_company_login_password"
            )

        temp_cols2 = st.columns(2)
        with temp_cols2[0]:
            temp_knowbe_username = st.text_input("一時送信用 Knowbe ID", key="bee_temp_knowbe_username")
        with temp_cols2[1]:
            temp_knowbe_password = st.text_input(
                "一時送信用 Knowbe PASS",
                type="password",
                key="bee_temp_knowbe_password"
            )
    st.divider()

    preview_context = build_generation_context(
        company_id=current_company_id,
        staff_name=staff_name,
        service_type=service_type,
    )

    with st.expander("生成参考（職員例文・個人ルール）"):
        st.write("開始例文:", preview_context.get("staff_start_example", ""))
        st.write("終了例文:", preview_context.get("staff_end_example", ""))
        st.write("個人ルール:", preview_context.get("personal_rule_text", ""))

    if "bee_generated_status" not in st.session_state:
        st.session_state.bee_generated_status = ""
    if "bee_generated_support" not in st.session_state:
        st.session_state.bee_generated_support = ""

    action_cols = st.columns(3)

    payload_base = {
        "date": str(input_date),
        "resident_id": resident_id,
        "resident_name": resident_name,
        "start_time": start_time,
        "end_time": end_time,
        "work_start_time": work_start_time,
        "work_end_time": work_end_time,
        "work_break_time": work_break_time,
        "meal_flag": meal_flag,
        "note": note,
        "staff_name": staff_name,
        "service_type": service_type,
        "generation_context": preview_context,
    }

    with action_cols[0]:
        if st.button("文章生成", key="bee_generate_button", use_container_width=True):
            if not resident_name:
                st.error("利用者を選んでほしいある。")
            elif not staff_name.strip():
                st.error("記録者名を入れてほしいある。")
            else:
                generated = generate_fn(payload_base) or {}
                st.session_state.bee_generated_status = str(generated.get("generated_status", "")).strip()
                st.session_state.bee_generated_support = str(generated.get("generated_support", "")).strip()
                st.success("生成したある。")

    with action_cols[1]:
        if st.button("下書き保存", key="bee_save_draft_button", use_container_width=True):
            if not resident_name:
                st.error("利用者を選んでほしいある。")
            else:
                record_id = save_diary_input_record(
                    date=str(input_date),
                    resident_id=resident_id,
                    resident_name=resident_name,
                    start_time=start_time,
                    end_time=end_time,
                    work_start_time=work_start_time,
                    work_end_time=work_end_time,
                    work_break_time=work_break_time,
                    meal_flag=meal_flag,
                    note=note,
                    start_memo=st.session_state.get("bee_generated_status", ""),
                    end_memo=st.session_state.get("bee_generated_support", ""),
                    staff_name=staff_name,
                    generated_status=st.session_state.get("bee_generated_status", ""),
                    generated_support=st.session_state.get("bee_generated_support", ""),
                    service_type=service_type,
                    knowbe_target="",
                    send_status="draft",
                    sent_at="",
                    send_error="",
                    record_mode="gemini" if st.session_state.get("bee_generated_status", "") or st.session_state.get("bee_generated_support", "") else "manual",
                    company_id=current_company_id,
                )
                st.success(f"下書き保存したある。record_id={record_id}")

    with action_cols[2]:
        if st.button("Knowbeへ送信", key="bee_send_button", use_container_width=True):
            if not resident_name:
                st.error("利用者を選んでほしいある。")
            else:
                ok_ctx, target_ctx, ctx_err = resolve_bee_target_context(
                    use_other_company=use_other_company,
                    temp_company_login_id=st.session_state.get("bee_temp_company_login_id", ""),
                    temp_company_login_password=st.session_state.get("bee_temp_company_login_password", ""),
                    temp_knowbe_username=st.session_state.get("bee_temp_knowbe_username", ""),
                    temp_knowbe_password=st.session_state.get("bee_temp_knowbe_password", ""),
                )

                if not ok_ctx:
                    st.error(ctx_err)
                else:
                    start_memo = st.session_state.get("bee_generated_status", "")
                    end_memo = st.session_state.get("bee_generated_support", "")

                    record_id = save_diary_input_record(
                        date=str(input_date),
                        resident_id=resident_id,
                        resident_name=resident_name,
                        start_time=start_time,
                        end_time=end_time,
                        work_start_time=work_start_time,
                        work_end_time=work_end_time,
                        work_break_time=work_break_time,
                        meal_flag=meal_flag,
                        note=note,
                        start_memo=start_memo,
                        end_memo=end_memo,
                        staff_name=staff_name,
                        generated_status=start_memo,
                        generated_support=end_memo,
                        service_type=service_type,
                        knowbe_target=target_ctx.get("knowbe_target_label", ""),
                        send_status="sending",
                        sent_at="",
                        send_error="",
                        record_mode="gemini" if start_memo or end_memo else "manual",
                        company_id=current_company_id,
                    )

                    send_payload = {
                        **payload_base,
                        "start_memo": start_memo,
                        "end_memo": end_memo,
                        "target_context": target_ctx,
                        "record_id": record_id,
                    }

                    success, message = send_fn(send_payload)

                    if success:
                        update_diary_input_record_status(
                            record_id=record_id,
                            send_status="sent",
                            sent_at=now_jst().strftime("%Y-%m-%d %H:%M:%S"),
                            send_error="",
                        )
                        st.success(message or "送信完了ある。")
                    else:
                        update_diary_input_record_status(
                            record_id=record_id,
                            send_status="error",
                            sent_at="",
                            send_error=str(message),
                        )
                        st.error(message or "送信に失敗したある。")

    st.divider()

    st.markdown("### 生成結果")
    generated_status = st.text_area(
        "利用者状態 / 開始メモ",
        value=st.session_state.get("bee_generated_status", ""),
        key="bee_generated_status_view",
        height=140,
    )
    generated_support = st.text_area(
        "職員考察 / 終了メモ",
        value=st.session_state.get("bee_generated_support", ""),
        key="bee_generated_support_view",
        height=180,
    )

    st.session_state.bee_generated_status = str(generated_status)
    st.session_state.bee_generated_support = str(generated_support)

    st.divider()

    st.markdown("### 最近の下書き・送信履歴")
    hist_df = get_diary_input_rules_df(current_company_id)

    if hist_df is None or hist_df.empty:
        st.info("まだ記録がないある。")
    else:
        work = hist_df.copy()
        try:
            work["record_id_num"] = pd.to_numeric(work["record_id"], errors="coerce")
            work = work.sort_values(["record_id_num"], ascending=[False])
        except Exception:
            pass

        show_cols = [
            "record_id", "date", "resident_name", "service_type",
            "staff_name", "knowbe_target", "send_status", "sent_at", "record_mode"
        ]
        for col in show_cols:
            if col not in work.columns:
                work[col] = ""

        st.dataframe(work[show_cols].head(30), use_container_width=True)