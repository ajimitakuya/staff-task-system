import base64
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from db import get_archive_files_df, get_users_df, get_admin_logs_df, save_db


JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


def get_next_archive_file_id():
    df = get_archive_files_df()
    if df is None or df.empty:
        return "A0001"

    nums = []
    for x in df["archive_file_id"].fillna("").astype(str):
        x = x.strip().upper()
        if x.startswith("A"):
            num = x[1:]
            if num.isdigit():
                nums.append(int(num))

    next_num = max(nums) + 1 if nums else 1
    return f"A{next_num:04d}"


def get_next_admin_log_id():
    df = get_admin_logs_df()
    if df is None or df.empty:
        return "L0001"

    nums = []
    for x in df["log_id"].fillna("").astype(str):
        x = x.strip().upper()
        if x.startswith("L"):
            num = x[1:]
            if num.isdigit():
                nums.append(int(num))

    next_num = max(nums) + 1 if nums else 1
    return f"L{next_num:04d}"


def create_admin_log(action_type, target_type, target_id, action_detail=""):
    df = get_admin_logs_df()

    new_row = pd.DataFrame([{
        "log_id": get_next_admin_log_id(),
        "company_id": str(st.session_state.get("company_id", "")).strip(),
        "acted_by_user_id": str(st.session_state.get("user_id", "")).strip(),
        "acted_by_display_name": str(st.session_state.get("user", "")).strip(),
        "action_type": str(action_type).strip(),
        "target_type": str(target_type).strip(),
        "target_id": str(target_id).strip(),
        "action_detail": str(action_detail).strip(),
        "created_at": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
    }])

    df = pd.concat([df, new_row], ignore_index=True)
    save_db(df, "admin_logs")


def detect_file_type(file_name: str) -> str:
    lower_name = str(file_name).strip().lower()

    if lower_name.endswith(".xlsx"):
        return "xlsx"
    if lower_name.endswith(".xls"):
        return "xls"
    if lower_name.endswith(".pdf"):
        return "pdf"
    if lower_name.endswith(".docx"):
        return "docx"
    if lower_name.endswith(".doc"):
        return "doc"
    if lower_name.endswith(".jpg") or lower_name.endswith(".jpeg"):
        return "jpg"
    if lower_name.endswith(".png"):
        return "png"
    return "other"


def detect_mime_type(file_name: str) -> str:
    lower_name = str(file_name).strip().lower()

    if lower_name.endswith(".xlsx"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if lower_name.endswith(".xls"):
        return "application/vnd.ms-excel"
    if lower_name.endswith(".pdf"):
        return "application/pdf"
    if lower_name.endswith(".docx"):
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if lower_name.endswith(".doc"):
        return "application/msword"
    if lower_name.endswith(".jpg") or lower_name.endswith(".jpeg"):
        return "image/jpeg"
    if lower_name.endswith(".png"):
        return "image/png"
    return "application/octet-stream"


def save_archive_file(
    title,
    description,
    category_main,
    category_sub,
    tags,
    uploaded_file,
    visibility_type="normal",
    download_password="",
):
    df = get_archive_files_df()

    now_str = now_jst().strftime("%Y-%m-%d %H:%M:%S")
    archive_file_id = get_next_archive_file_id()

    file_bytes = uploaded_file.read()
    file_data_base64 = base64.b64encode(file_bytes).decode("utf-8")
    file_name = str(uploaded_file.name).strip()
    file_type = detect_file_type(file_name)

    new_row = pd.DataFrame([{
        "archive_file_id": archive_file_id,
        "company_id": str(st.session_state.get("company_id", "")).strip(),
        "title": str(title).strip(),
        "description": str(description).strip(),
        "category_main": str(category_main).strip(),
        "category_sub": str(category_sub).strip(),
        "tags": str(tags).strip(),
        "file_name": file_name,
        "file_data": file_data_base64,
        "file_type": file_type,
        "uploaded_by_user_id": str(st.session_state.get("user_id", "")).strip(),
        "visibility_type": str(visibility_type).strip(),
        "download_password": str(download_password).strip(),
        "is_deleted": "0",
        "created_at": now_str,
        "updated_at": now_str,
        "deleted_by_user_id": "",
        "deleted_at": "",
    }])

    df = pd.concat([df, new_row], ignore_index=True)
    save_db(df, "archive_files")
    return archive_file_id


def soft_delete_archive_file(archive_file_id):
    df = get_archive_files_df()
    if df is None or df.empty:
        return False

    mask = df["archive_file_id"].astype(str) == str(archive_file_id).strip()
    if not mask.any():
        return False

    now_str = now_jst().strftime("%Y-%m-%d %H:%M:%S")
    df.loc[mask, "is_deleted"] = "1"
    df.loc[mask, "deleted_by_user_id"] = str(st.session_state.get("user_id", "")).strip()
    df.loc[mask, "deleted_at"] = now_str
    df.loc[mask, "updated_at"] = now_str

    save_db(df, "archive_files")
    return True


def get_archive_download_data(row):
    file_data_base64 = str(row.get("file_data", "")).strip()
    file_name = str(row.get("file_name", "")).strip()

    if not file_data_base64 or not file_name:
        return None, None, None

    file_bytes = base64.b64decode(file_data_base64)
    mime = detect_mime_type(file_name)

    return file_bytes, file_name, mime


def get_uploader_name(users_df, uploaded_by_user_id: str) -> str:
    uploader_name = uploaded_by_user_id
    try:
        target_user = users_df[users_df["user_id"].astype(str) == str(uploaded_by_user_id).strip()]
        if not target_user.empty:
            uploader_name = str(target_user.iloc[0].get("display_name", uploaded_by_user_id)).strip()
    except Exception:
        pass
    return uploader_name


def filter_archive_files(df, company_id="", keyword="", filter_main="", filter_sub=""):
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()
    work = work[
        (work["company_id"].astype(str) == str(company_id).strip()) &
        (work["is_deleted"].astype(str) != "1")
    ].copy()

    if str(keyword).strip():
        kw = str(keyword).strip()
        work = work[
            work["title"].astype(str).str.contains(kw, case=False, na=False) |
            work["description"].astype(str).str.contains(kw, case=False, na=False) |
            work["tags"].astype(str).str.contains(kw, case=False, na=False) |
            work["file_name"].astype(str).str.contains(kw, case=False, na=False)
        ].copy()

    if str(filter_main).strip():
        work = work[
            work["category_main"].astype(str).str.contains(str(filter_main).strip(), case=False, na=False)
        ].copy()

    if str(filter_sub).strip():
        work = work[
            work["category_sub"].astype(str).str.contains(str(filter_sub).strip(), case=False, na=False)
        ].copy()

    try:
        work = work.sort_values(["updated_at", "created_at"], ascending=[False, False])
    except Exception:
        pass

    return work


def render_archive_page():
    st.title("📤 書類アップロード")
    st.caption("この事業所だけで共有する資料置き場ある。")

    current_company_id = str(st.session_state.get("company_id", "")).strip()
    current_user_id = str(st.session_state.get("user_id", "")).strip()
    is_admin = bool(st.session_state.get("is_admin", False))

    top_cols = st.columns([1, 1])

    with top_cols[0]:
        if st.button("← 休憩室へ戻る", key="back_from_archive", use_container_width=True):
            st.session_state.current_page = "休憩室"
            st.rerun()

    with top_cols[1]:
        st.info(f"事業所: {st.session_state.get('company_name', '')}")

    st.divider()

    with st.expander("＋ 新しい資料を登録する"):
        title = st.text_input("タイトル", key="archive_title")
        description = st.text_area("説明", key="archive_description", height=80)
        category_main = st.text_input("カテゴリ大", key="archive_category_main")
        category_sub = st.text_input("カテゴリ小", key="archive_category_sub")
        tags = st.text_input("タグ（カンマ区切りでもOK）", key="archive_tags")
        uploaded_file = st.file_uploader("ファイルを選択", key="archive_uploaded_file")

        if st.button("書庫へ保存", key="save_archive_button", use_container_width=True):
            if not str(title).strip():
                st.error("タイトルを入れてほしいある。")
            elif uploaded_file is None:
                st.error("ファイルを選んでほしいある。")
            else:
                archive_file_id = save_archive_file(
                    title=title,
                    description=description,
                    category_main=category_main,
                    category_sub=category_sub,
                    tags=tags,
                    uploaded_file=uploaded_file,
                    visibility_type="normal",
                    download_password="",
                )
                st.success(f"保存したある！ {archive_file_id}")
                st.rerun()

    st.divider()

    df = get_archive_files_df()

    if df is None or df.empty:
        st.info("まだ書庫に資料がないある。")
        return

    search_cols = st.columns([2, 1, 1])

    with search_cols[0]:
        keyword = st.text_input("検索", key="archive_search_keyword")

    with search_cols[1]:
        filter_main = st.text_input("カテゴリ大で絞る", key="archive_filter_main")

    with search_cols[2]:
        filter_sub = st.text_input("カテゴリ小で絞る", key="archive_filter_sub")

    work = filter_archive_files(
        df=df,
        company_id=current_company_id,
        keyword=keyword,
        filter_main=filter_main,
        filter_sub=filter_sub,
    )

    if work.empty:
        st.info("この事業所の書庫にはまだ資料がないある。")
        return

    st.markdown(f"### 一覧（{len(work)}件）")

    users_df = get_users_df()

    for _, row in work.iterrows():
        archive_file_id = str(row.get("archive_file_id", "")).strip()
        title = str(row.get("title", "")).strip()
        description = str(row.get("description", "")).strip()
        category_main = str(row.get("category_main", "")).strip()
        category_sub = str(row.get("category_sub", "")).strip()
        tags = str(row.get("tags", "")).strip()
        file_name = str(row.get("file_name", "")).strip()
        file_type = str(row.get("file_type", "")).strip()
        uploaded_by_user_id = str(row.get("uploaded_by_user_id", "")).strip()
        created_at = str(row.get("created_at", "")).strip()
        updated_at = str(row.get("updated_at", "")).strip()

        uploader_name = get_uploader_name(users_df, uploaded_by_user_id)

        st.markdown("---")
        st.markdown(f"## {title}")
        st.caption(f"{archive_file_id} / {file_name} / {file_type}")

        meta_cols = st.columns([2, 2, 2])
        with meta_cols[0]:
            st.write(f"カテゴリ: {category_main} / {category_sub}")
        with meta_cols[1]:
            st.write(f"登録者: {uploader_name}")
        with meta_cols[2]:
            st.write(f"更新: {updated_at or created_at}")

        if description:
            st.write(description)
        if tags:
            st.caption(f"タグ: {tags}")

        file_bytes, dl_name, mime = get_archive_download_data(row)

        action_cols = st.columns([1, 1, 1])

        with action_cols[0]:
            if file_bytes is not None:
                st.download_button(
                    label="ダウンロード",
                    data=file_bytes,
                    file_name=dl_name,
                    mime=mime,
                    key=f"archive_download_{archive_file_id}",
                    use_container_width=True
                )

        can_delete = (uploaded_by_user_id == current_user_id) or (
            is_admin and str(row.get("company_id", "")).strip() == current_company_id
        )

        with action_cols[1]:
            if can_delete:
                if st.button("削除", key=f"archive_delete_{archive_file_id}", use_container_width=True):
                    ok = soft_delete_archive_file(archive_file_id)
                    if ok:
                        create_admin_log(
                            action_type="delete_archive_file",
                            target_type="archive_file",
                            target_id=archive_file_id,
                            action_detail=f"title={title}"
                        )
                        st.success("削除したある。")
                        st.rerun()
                    else:
                        st.error("削除に失敗したある。")

        with action_cols[2]:
            st.write("")