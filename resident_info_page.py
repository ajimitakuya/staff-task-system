import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, timezone

from db import get_df, save_db
from task_board import start_task, complete_task, get_tasks_df


JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


def get_current_company_id():
    return str(st.session_state.get("company_id", "")).strip()


# ==========================================
# 共通
# ==========================================
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


def get_next_numeric_id(df, id_col="id", default_start=1):
    if df is None or df.empty or id_col not in df.columns:
        return default_start

    ids = pd.to_numeric(df[id_col], errors="coerce").dropna()
    return int(ids.max()) + 1 if not ids.empty else default_start


def parse_time_range(raw_text: str):
    raw = str(raw_text).strip()
    if not raw:
        return "", ""

    raw = raw.replace("～", "〜").replace("~", "〜").replace("-", "〜")
    if "〜" in raw:
        start_time, end_time = [x.strip() for x in raw.split("〜", 1)]
        return start_time, end_time

    return raw, ""


# ==========================================
# resident_master
# ==========================================
def get_resident_master_required_cols():
    return [
        "company_id",
        "resident_id", "resident_name", "status",
        "consultant", "consultant_phone",
        "caseworker", "caseworker_phone",
        "hospital", "hospital_phone",
        "nurse", "nurse_phone",
        "care", "care_phone",
        "created_at", "updated_at"
    ]


def get_resident_master_df(company_id=None):
    if company_id is None:
        company_id = get_current_company_id()

    df = get_df("resident_master")
    work = normalize_company_scoped_df(df, get_resident_master_required_cols())

    if "resident_id" in work.columns:
        work["resident_id"] = work["resident_id"].astype(str).str.strip()
    if "resident_name" in work.columns:
        work["resident_name"] = work["resident_name"].astype(str).str.strip()

    return filter_by_company_id(work, company_id)


def get_next_resident_id(company_df=None):
    if company_df is None:
        company_df = get_resident_master_df()

    if company_df is None or company_df.empty:
        return "R0001"

    nums = []
    for x in company_df["resident_id"].fillna("").astype(str):
        x = x.strip().upper()
        if x.startswith("R"):
            num = x[1:]
            if num.isdigit():
                nums.append(int(num))

    next_num = max(nums) + 1 if nums else 1
    return f"R{next_num:04d}"


def save_new_resident(
    resident_name,
    status="利用中",
    consultant="",
    consultant_phone="",
    caseworker="",
    caseworker_phone="",
    hospital="",
    hospital_phone="",
    nurse="",
    nurse_phone="",
    care="",
    care_phone="",
):
    company_id = get_current_company_id()
    all_df = get_df("resident_master")
    all_df = normalize_company_scoped_df(all_df, get_resident_master_required_cols())
    company_df = filter_by_company_id(all_df, company_id)
    now_str = now_jst().strftime("%Y-%m-%d %H:%M")

    new_row = pd.DataFrame([{
        "company_id": company_id,
        "resident_id": get_next_resident_id(company_df),
        "resident_name": str(resident_name).strip(),
        "status": str(status).strip(),
        "consultant": str(consultant).strip(),
        "consultant_phone": str(consultant_phone).strip(),
        "caseworker": str(caseworker).strip(),
        "caseworker_phone": str(caseworker_phone).strip(),
        "hospital": str(hospital).strip(),
        "hospital_phone": str(hospital_phone).strip(),
        "nurse": str(nurse).strip(),
        "nurse_phone": str(nurse_phone).strip(),
        "care": str(care).strip(),
        "care_phone": str(care_phone).strip(),
        "created_at": now_str,
        "updated_at": now_str,
    }])

    merged = pd.concat([all_df, new_row], ignore_index=True)
    save_db(merged, "resident_master")
    return str(new_row.iloc[0]["resident_id"])


# ==========================================
# resident_schedule
# ==========================================
def get_resident_schedule_required_cols():
    return [
        "company_id",
        "id", "resident_id", "weekday", "service_type",
        "start_time", "end_time", "place", "phone",
        "person_in_charge", "memo"
    ]


def get_resident_schedule_df(company_id=None):
    if company_id is None:
        company_id = get_current_company_id()

    df = get_df("resident_schedule")
    work = normalize_company_scoped_df(df, get_resident_schedule_required_cols())

    if "resident_id" in work.columns:
        work["resident_id"] = work["resident_id"].astype(str).str.strip()

    return filter_by_company_id(work, company_id)


def build_schedule_form_base(schedule_df, resident_id):
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    service_types = ["病院", "看護", "介護"]

    base = {}
    target_df = schedule_df[schedule_df["resident_id"].astype(str) == str(resident_id)].copy()

    for service in service_types:
        base[service] = {
            "place": "",
            "phone": "",
            "person_in_charge": "",
            "days": {wd: ["", "", "", ""] for wd in weekdays}
        }

        service_df = target_df[target_df["service_type"].astype(str) == service].copy()
        if service_df.empty:
            continue

        first_row = service_df.iloc[0]
        base[service]["place"] = str(first_row.get("place", "")).strip()
        base[service]["phone"] = str(first_row.get("phone", "")).strip()
        base[service]["person_in_charge"] = str(first_row.get("person_in_charge", "")).strip()

        for _, row in service_df.iterrows():
            wd = str(row.get("weekday", "")).strip()
            if wd not in weekdays:
                continue

            slot_index = 0
            memo = str(row.get("memo", "")).strip()
            if memo.startswith("slot:"):
                try:
                    slot_index = max(int(memo.split(":", 1)[1]) - 1, 0)
                except Exception:
                    slot_index = 0
            else:
                existing = base[service]["days"][wd]
                slot_index = next((i for i, v in enumerate(existing) if not str(v).strip()), 0)

            start_time = str(row.get("start_time", "")).strip()
            end_time = str(row.get("end_time", "")).strip()

            if start_time and end_time:
                value = f"{start_time}〜{end_time}"
            else:
                value = start_time or end_time

            if 0 <= slot_index < 4:
                base[service]["days"][wd][slot_index] = value

    return base


def render_resident_schedule_html(schedule_view):
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    service_types = ["病院", "看護", "介護"]
    color_map = {
        "病院": "#F8E7A1",
        "看護": "#CFEAF6",
        "介護": "#DDEDB7",
    }

    base = build_schedule_form_base(schedule_view, str(schedule_view.iloc[0].get("resident_id", "")).strip())

    for service in service_types:
        service_data = base.get(service, {})
        if not service_data:
            continue

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
            st.write(f"**{service}名**: {service_data.get('place', '')}")
        with top_cols[1]:
            st.write(f"**{service}電話**: {service_data.get('phone', '')}")
        with top_cols[2]:
            st.write(f"**{service}担当**: {service_data.get('person_in_charge', '')}")

        cols = st.columns(7)
        for i, wd in enumerate(weekdays):
            with cols[i]:
                st.markdown(f"**{wd}**")
                for slot in service_data["days"][wd]:
                    if str(slot).strip():
                        st.caption(slot)
                    else:
                        st.caption("―")

        st.markdown("<br>", unsafe_allow_html=True)


# ==========================================
# resident_notes
# ==========================================
def get_resident_notes_required_cols():
    return [
        "company_id",
        "id", "resident_id", "date", "user", "note"
    ]


def get_resident_notes_df(company_id=None):
    if company_id is None:
        company_id = get_current_company_id()

    df = get_df("resident_notes")
    work = normalize_company_scoped_df(df, get_resident_notes_required_cols())

    if "resident_id" in work.columns:
        work["resident_id"] = work["resident_id"].astype(str).str.strip()

    return filter_by_company_id(work, company_id)


# ==========================================
# external_contacts / resident_links
# ==========================================
def get_external_contacts_required_cols():
    return [
        "contact_id", "category1", "category2",
        "name", "organization", "phone", "memo"
    ]


def get_resident_links_required_cols():
    return ["id", "resident_id", "contact_id", "role"]


def get_external_contacts_df():
    df = get_df("external_contacts")
    if df is None or df.empty:
        return pd.DataFrame(columns=get_external_contacts_required_cols())

    work = df.copy().fillna("")
    for col in get_external_contacts_required_cols():
        if col not in work.columns:
            work[col] = ""

    return work[get_external_contacts_required_cols()].copy()


def get_resident_links_df():
    df = get_df("resident_links")
    if df is None or df.empty:
        return pd.DataFrame(columns=get_resident_links_required_cols())

    work = df.copy().fillna("")
    for col in get_resident_links_required_cols():
        if col not in work.columns:
            work[col] = ""

    work["resident_id"] = work["resident_id"].astype(str).str.strip()
    work["contact_id"] = work["contact_id"].astype(str).str.strip()

    return work[get_resident_links_required_cols()].copy()


def get_next_contact_id(contacts_df):
    if contacts_df is None or contacts_df.empty:
        return "C0001"

    nums = []
    for x in contacts_df["contact_id"].fillna("").astype(str):
        x = x.strip().upper()
        if x.startswith("C"):
            num = x[1:]
            if num.isdigit():
                nums.append(int(num))

    next_num = max(nums) + 1 if nums else 1
    return f"C{next_num:04d}"


# ==========================================
# render
# ==========================================
def render_resident_info_page():
    st.title("⑦ 利用者情報")
    st.caption("利用者の基本情報、予定、メモ、連絡先、至急タスクを確認・編集するページある。")

    current_company_id = get_current_company_id()

    master_df = get_resident_master_df(current_company_id)
    schedule_df = get_resident_schedule_df(current_company_id)
    notes_df = get_resident_notes_df(current_company_id)
    task_df_detail = get_tasks_df(current_company_id)
    contacts_df = get_external_contacts_df()
    links_df = get_resident_links_df()

    for flag_key in ["edit_resident_basic", "edit_resident_schedule", "edit_resident_note"]:
        if flag_key not in st.session_state:
            st.session_state[flag_key] = False

    with st.expander("➕ 新しい利用者を登録する"):
        with st.form("new_resident_form"):
            resident_name = st.text_input("利用者名")
            status = st.selectbox("状態", ["利用中", "休止", "終了"])
            consultant = st.text_input("相談員")
            consultant_phone = st.text_input("相談員電話")
            caseworker = st.text_input("ケースワーカー")
            caseworker_phone = st.text_input("ケースワーカー電話")
            hospital = st.text_input("病院")
            hospital_phone = st.text_input("病院電話")
            nurse = st.text_input("看護")
            nurse_phone = st.text_input("看護電話")
            care = st.text_input("介護")
            care_phone = st.text_input("介護電話")

            if st.form_submit_button("登録する"):
                if not str(resident_name).strip():
                    st.error("利用者名を入れてほしいある。")
                else:
                    resident_id = save_new_resident(
                        resident_name=resident_name,
                        status=status,
                        consultant=consultant,
                        consultant_phone=consultant_phone,
                        caseworker=caseworker,
                        caseworker_phone=caseworker_phone,
                        hospital=hospital,
                        hospital_phone=hospital_phone,
                        nurse=nurse,
                        nurse_phone=nurse_phone,
                        care=care,
                        care_phone=care_phone,
                    )
                    st.success(f"登録できたある！ resident_id={resident_id}")
                    st.rerun()

    st.divider()

    if master_df.empty:
        st.info("まだ利用者が登録されてないある。")
        return

    search_cols = st.columns([2, 1])

    with search_cols[0]:
        keyword = st.text_input("利用者名検索", key="resident_search_keyword")

    with search_cols[1]:
        status_filter = st.selectbox("状態", ["すべて", "利用中", "休止", "終了"], key="resident_status_filter")

    view_df = master_df.copy()

    if keyword.strip():
        kw = keyword.strip()
        view_df = view_df[
            view_df["resident_name"].astype(str).str.contains(kw, case=False, na=False) |
            view_df["resident_id"].astype(str).str.contains(kw, case=False, na=False)
        ].copy()

    if status_filter != "すべて":
        view_df = view_df[view_df["status"].astype(str).str.strip() == status_filter].copy()

    if view_df.empty:
        st.info("条件に合う利用者がいないある。")
        return

    try:
        view_df = view_df.sort_values(["resident_name"], ascending=[True])
    except Exception:
        pass

    options = []
    option_map = {}
    for _, row in view_df.iterrows():
        label = f"{row.get('resident_name', '')} ({row.get('resident_id', '')})"
        options.append(label)
        option_map[label] = row.to_dict()

    selected_label = st.selectbox("利用者を選択", options, key="selected_resident_label")
    if not selected_label:
        return

    row = option_map[selected_label]
    selected_id = str(row.get("resident_id", "")).strip()
    resident_name = str(row.get("resident_name", "")).strip()

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
                            start_task(t_id, current_company_id)
                            st.success("タスクを開始したある！")
                            st.rerun()

                elif t_status == "作業中":
                    if t_user == str(st.session_state.get("user", "")).strip():
                        with btn_cols[1]:
                            if st.button("完了する", key=f"resident_urgent_done_{t_id}", use_container_width=True):
                                complete_task(t_id, current_company_id)
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
    st.markdown("### ➕ 関係者を登録する")

    with st.expander("新しい関係者を追加"):
        with st.form(f"resident_contact_add_form_{selected_id}"):
            form_col1, form_col2 = st.columns(2)

            with form_col1:
                category1 = st.selectbox(
                    "大分類",
                    ["医療", "外部連携", "家族", "業者"],
                    key=f"contact_cat1_{selected_id}"
                )

                category2_map = {
                    "医療": ["主治医", "訪問看護", "薬局", "行政", "その他"],
                    "外部連携": ["ケアマネ", "相談員", "行政", "その他"],
                    "家族": ["家族", "成年後見人", "身元引受人", "その他"],
                    "業者": ["配食", "福祉用具", "修理", "その他"],
                }

                category2 = st.selectbox(
                    "分類",
                    category2_map.get(category1, ["その他"]),
                    key=f"contact_cat2_{selected_id}"
                )

                role = st.text_input(
                    "この利用者に対する役割",
                    value=category2,
                    key=f"contact_role_{selected_id}"
                )

                name = st.text_input("氏名", key=f"contact_name_{selected_id}")

            with form_col2:
                organization = st.text_input("事業所名", key=f"contact_org_{selected_id}")
                phone = st.text_input("電話番号", key=f"contact_phone_{selected_id}")
                memo = st.text_area("メモ", key=f"contact_memo_{selected_id}")

            save_contact = st.form_submit_button("関係者を登録する", use_container_width=True)

            if save_contact:
                if name.strip() or organization.strip():
                    latest_contacts_df = get_external_contacts_df()
                    latest_links_df = get_resident_links_df()

                    next_contact_id = get_next_contact_id(latest_contacts_df)
                    next_link_id = get_next_numeric_id(latest_links_df, "id", 1)

                    new_contact_row = pd.DataFrame([{
                        "contact_id": next_contact_id,
                        "category1": category1,
                        "category2": category2,
                        "name": name.strip(),
                        "organization": organization.strip(),
                        "phone": phone.strip(),
                        "memo": memo.strip(),
                    }])

                    new_contacts_df = pd.concat([latest_contacts_df, new_contact_row], ignore_index=True)
                    save_db(new_contacts_df, "external_contacts")

                    new_link_row = pd.DataFrame([{
                        "id": next_link_id,
                        "resident_id": selected_id,
                        "contact_id": next_contact_id,
                        "role": role.strip(),
                    }])

                    new_links_df = pd.concat([latest_links_df, new_link_row], ignore_index=True)
                    save_db(new_links_df, "resident_links")

                    st.success("関係者を登録したある！")
                    st.rerun()
                else:
                    st.error("氏名か事業所名のどちらかは入れてほしいある。")

    linked_view = links_df[links_df["resident_id"].astype(str) == selected_id].copy()
    if not linked_view.empty:
        linked_merge = linked_view.merge(contacts_df, how="left", on="contact_id").fillna("")
        st.markdown("### 登録済み関係者")
        for _, crow in linked_merge.iterrows():
            contact_id = str(crow.get("contact_id", "")).strip()
            with st.container(border=True):
                st.write(f"**{crow.get('role', '')}**")
                st.write(f"{crow.get('name', '')} / {crow.get('organization', '')}")
                if str(crow.get("phone", "")).strip():
                    st.write(f"電話: {crow.get('phone', '')}")
                if str(crow.get("memo", "")).strip():
                    st.write(f"メモ: {crow.get('memo', '')}")

                if st.button(
                    "この紐づきを削除",
                    key=f"delete_contact_link_{selected_id}_{contact_id}",
                    use_container_width=True
                ):
                    latest_links_df = get_resident_links_df()
                    new_links_df = latest_links_df[
                        ~(
                            (latest_links_df["resident_id"].astype(str) == str(selected_id)) &
                            (latest_links_df["contact_id"].astype(str) == str(contact_id))
                        )
                    ].copy()
                    save_db(new_links_df, "resident_links")
                    st.success("この利用者との紐づきを削除したある。")
                    st.rerun()

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
            resident_name_input = st.text_input("利用者名", value=str(row.get("resident_name", "")))
            current_status = str(row.get("status", "利用中")).strip()
            status_options = ["利用中", "休止", "終了"]
            status_index = status_options.index(current_status) if current_status in status_options else 0
            status = st.selectbox("状態", status_options, index=status_index)

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
                update_df.loc[target_mask, "resident_name"] = resident_name_input.strip()
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

                all_master_df = get_df("resident_master")
                all_master_df = normalize_company_scoped_df(all_master_df, get_resident_master_required_cols())
                other_company_df = all_master_df[
                    all_master_df["company_id"].astype(str).str.strip() != current_company_id
                ].copy()

                save_db(pd.concat([other_company_df, update_df], ignore_index=True), "resident_master")
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
            "病院": "#F8E7A1",
            "看護": "#CFEAF6",
            "介護": "#DDEDB7",
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

                day_values = {}
                day_cols = st.columns(7)

                for i, wd in enumerate(weekdays):
                    with day_cols[i]:
                        st.markdown(f"**{wd}**")

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
                all_schedule_df = get_df("resident_schedule")
                all_schedule_df = normalize_company_scoped_df(all_schedule_df, get_resident_schedule_required_cols())

                keep_df = all_schedule_df[
                    ~(
                        (all_schedule_df["company_id"].astype(str).str.strip() == current_company_id) &
                        (all_schedule_df["resident_id"].astype(str) == selected_id) &
                        (all_schedule_df["service_type"].astype(str).isin(["病院", "看護", "介護"]))
                    )
                ].copy()

                next_id = get_next_numeric_id(all_schedule_df, "id", 1)
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
                                "company_id": current_company_id,
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
                all_schedule_df = get_df("resident_schedule")
                all_schedule_df = normalize_company_scoped_df(all_schedule_df, get_resident_schedule_required_cols())

                new_schedule_df = all_schedule_df[
                    ~(
                        (all_schedule_df["company_id"].astype(str).str.strip() == current_company_id) &
                        (all_schedule_df["resident_id"].astype(str) == selected_id) &
                        (all_schedule_df["service_type"].astype(str).isin(["病院", "看護", "介護"]))
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
                    all_notes_df = get_df("resident_notes")
                    all_notes_df = normalize_company_scoped_df(all_notes_df, get_resident_notes_required_cols())
                    next_id = get_next_numeric_id(all_notes_df, "id", 1)

                    new_row = pd.DataFrame([{
                        "company_id": current_company_id,
                        "id": next_id,
                        "resident_id": selected_id,
                        "date": str(note_date),
                        "user": str(st.session_state.get("user", "")),
                        "note": note_text.strip()
                    }])

                    new_notes_df = pd.concat([all_notes_df, new_row], ignore_index=True)
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
                        latest_notes_df = get_df("resident_notes")
                        latest_notes_df = normalize_company_scoped_df(latest_notes_df, get_resident_notes_required_cols())
                        new_notes_df = latest_notes_df[
                            ~(
                                (latest_notes_df["company_id"].astype(str).str.strip() == current_company_id) &
                                (latest_notes_df["id"].astype(str) == nid)
                            )
                        ].copy()

                        save_db(new_notes_df, "resident_notes")
                        st.success("メモを削除したある。")
                        st.rerun()