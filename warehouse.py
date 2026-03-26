import base64
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from db import get_warehouse_files_df, get_users_df, get_admin_logs_df, save_db


JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


def get_next_warehouse_file_id():
    df = get_warehouse_files_df()
    if df is None or df.empty:
        return "W0001"

    nums = []
    for x in df["file_id"].fillna("").astype(str):
        x = x.strip().upper()
        if x.startswith("W"):
            num = x[1:]
            if num.isdigit():
                nums.append(int(num))

    next_num = max(nums) + 1 if nums else 1
    return f"W{next_num:04d}"


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


def save_warehouse_file(
    title,
    description,
    category_main,
    category_sub,
    tags,
    uploaded_file,
    visibility_type="public",
    download_password="",
    is_searchable="1",
    source_room_id="",
):
    df = get_warehouse_files_df()

    now_str = now_jst().strftime("%Y-%m-%d %H:%M:%S")
    file_id = get_next_warehouse_file_id()

    file_bytes = uploaded_file.read()
    file_data_base64 = base64.b64encode(file_bytes).decode("utf-8")
    file_name = str(uploaded_file.name).strip()
    file_type = detect_file_type(file_name)

    new_row = pd.DataFrame([{
        "file_id": file_id,
        "title": str(title).strip(),
        "description": str(description).strip(),
        "category_main": str(category_main).strip(),
        "category_sub": str(category_sub).strip(),
        "tags": str(tags).strip(),
        "file_name": file_name,
        "file_data": file_data_base64,
        "file_type": file_type,
        "uploaded_by_user_id": str(st.session_state.get("user_id", "")).strip(),
        "uploaded_by_company_id": str(st.session_state.get("company_id", "")).strip(),
        "source_room_id": str(source_room_id).strip(),
        "visibility_type": str(visibility_type).strip(),
        "download_password": str(download_password).strip(),
        "is_searchable": str(is_searchable).strip(),
        "is_deleted": "0",
        "created_at": now_str,
        "updated_at": now_str,
        "deleted_by_user_id": "",
        "deleted_at": "",
    }])

    df = pd.concat([df, new_row], ignore_index=True)
    save_db(df, "warehouse_files")
    return file_id


def soft_delete_warehouse_file(file_id):
    df = get_warehouse_files_df()
    if df is None or df.empty:
        return False

    mask = df["file_id"].astype(str) == str(file_id).strip()
    if not mask.any():
        return False

    now_str = now_jst().strftime("%Y-%m-%d %H:%M:%S")
    df.loc[mask, "is_deleted"] = "1"
    df.loc[mask, "deleted_by_user_id"] = str(st.session_state.get("user_id", "")).strip()
    df.loc[mask, "deleted_at"] = now_str
    df.loc[mask, "updated_at"] = now_str

    save_db(df, "warehouse_files")
    return True


def get_warehouse_download_data(row):
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


def filter_warehouse_files(df, keyword="", filter_main="", filter_sub=""):
    if df is None or df.empty:
        return pd.DataFrame()

    work = df.copy()
    work = work[work["is_deleted"].astype(str) != "1"].copy()
    work = work[work["is_searchable"].astype(str) == "1"].copy()

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


def render_warehouse_page():
    st.title("🏭 倉庫")
    st.caption("全事業所共通の資料置き場ある。検索して、共有して、必要なら限定公開もできるある。")

    current_company_id = str(st.session_state.get("company_id", "")).strip()
    current_user_id = str(st.session_state.get("user_id", "")).strip()
    is_admin = bool(st.session_state.get("is_admin", False))

    if "warehouse_unlocked_files" not in st.session_state:
        st.session_state.warehouse_unlocked_files = []

    top_cols = st.columns([1, 1])

    with top_cols[0]:
        if st.button("← 休憩室へ戻る", key="back_from_warehouse", use_container_width=True):
            st.session_state.current_page = "休憩室"
            st.rerun()

    with top_cols[1]:
        st.info(f"ログイン中: {st.session_state.get('company_name', '')} / {st.session_state.get('user', '')}")

    st.divider()

    with st.expander("＋ 新しい資料を登録する"):
        title = st.text_input("タイトル", key="warehouse_title")
        description = st.text_area("説明", key="warehouse_description", height=80)
        category_main = st.text_input("カテゴリ大", key="warehouse_category_main")
        category_sub = st.text_input("カテゴリ小", key="warehouse_category_sub")
        tags = st.text_input("タグ（カンマ区切りでもOK）", key="warehouse_tags")
        visibility_type = st.selectbox(
            "公開設定",
            ["public", "limited", "private"],
            key="warehouse_visibility_type"
        )
        download_password = st.text_input(
            "ダウンロードパスワード（limited/privateなら設定）",
            key="warehouse_download_password"
        )
        is_searchable = st.selectbox(
            "検索に表示するか",
            ["1", "0"],
            format_func=lambda x: "表示する" if x == "1" else "表示しない",
            key="warehouse_is_searchable"
        )
        uploaded_file = st.file_uploader("ファイルを選択", key="warehouse_uploaded_file")

        if st.button("倉庫へ保存", key="save_warehouse_button", use_container_width=True):
            if not str(title).strip():
                st.error("タイトルを入れてほしいある。")
            elif uploaded_file is None:
                st.error("ファイルを選んでほしいある。")
            elif visibility_type in ["limited", "private"] and not str(download_password).strip():
                st.error("その公開設定ならダウンロードパスワードが必要ある。")
            else:
                file_id = save_warehouse_file(
                    title=title,
                    description=description,
                    category_main=category_main,
                    category_sub=category_sub,
                    tags=tags,
                    uploaded_file=uploaded_file,
                    visibility_type=visibility_type,
                    download_password=download_password,
                    is_searchable=is_searchable,
                    source_room_id="",
                )
                st.success(f"保存したある！ {file_id}")
                st.rerun()

    st.divider()

    df = get_warehouse_files_df()
    if df is None or df.empty:
        st.info("まだ倉庫に資料がないある。")
        return

    keyword_cols = st.columns([2, 1, 1])

    with keyword_cols[0]:
        keyword = st.text_input("検索", key="warehouse_search_keyword")

    with keyword_cols[1]:
        filter_main = st.text_input("カテゴリ大で絞る", key="warehouse_filter_main")

    with keyword_cols[2]:
        filter_sub = st.text_input("カテゴリ小で絞る", key="warehouse_filter_sub")

    work = filter_warehouse_files(
        df=df,
        keyword=keyword,
        filter_main=filter_main,
        filter_sub=filter_sub,
    )

    st.markdown(f"### 一覧（{len(work)}件）")

    if work.empty:
        st.info("条件に合う資料がないある。")
        return

    users_df = get_users_df()

    for _, row in work.iterrows():
        file_id = str(row.get("file_id", "")).strip()
        title = str(row.get("title", "")).strip()
        description = str(row.get("description", "")).strip()
        category_main = str(row.get("category_main", "")).strip()
        category_sub = str(row.get("category_sub", "")).strip()
        tags = str(row.get("tags", "")).strip()
        file_name = str(row.get("file_name", "")).strip()
        file_type = str(row.get("file_type", "")).strip()
        uploaded_by_user_id = str(row.get("uploaded_by_user_id", "")).strip()
        uploaded_by_company_id = str(row.get("uploaded_by_company_id", "")).strip()
        visibility_type = str(row.get("visibility_type", "")).strip()
        created_at = str(row.get("created_at", "")).strip()
        updated_at = str(row.get("updated_at", "")).strip()

        uploader_name = get_uploader_name(users_df, uploaded_by_user_id)

        st.markdown("---")
        st.markdown(f"## {title}")
        st.caption(f"{file_id} / {file_name} / {file_type} / 公開設定: {visibility_type}")

        meta_cols = st.columns([2, 2, 2])
        with meta_cols[0]:
            st.write(f"カテゴリ: {category_main} / {category_sub}")
        with meta_cols[1]:
            st.write(f"登録者: {uploader_name}")
        with meta_cols[2]:
            st.write(f"更新: {updated_at or created_at}")

        st.caption(f"登録事業所: {uploaded_by_company_id}")

        if description:
            st.write(description)
        if tags:
            st.caption(f"タグ: {tags}")

        file_bytes, dl_name, mime = get_warehouse_download_data(row)

        can_delete = (uploaded_by_user_id == current_user_id) or (
            is_admin and uploaded_by_company_id == current_company_id
        )

        is_unlocked = (file_id in st.session_state.warehouse_unlocked_files)

        if visibility_type == "public":
            is_unlocked = True

        if visibility_type in ["limited", "private"] and not is_unlocked:
            pw_cols = st.columns([2, 1])
            with pw_cols[0]:
                input_pw = st.text_input(
                    f"{file_id} のダウンロードパスワード",
                    type="password",
                    key=f"warehouse_pw_{file_id}"
                )
            with pw_cols[1]:
                st.write("")
                if st.button("解除", key=f"unlock_warehouse_{file_id}", use_container_width=True):
                    real_pw = str(row.get("download_password", "")).strip()
                    if str(input_pw).strip() == real_pw:
                        st.session_state.warehouse_unlocked_files.append(file_id)
                        st.success("ダウンロード可能になったある。")
                        st.rerun()
                    else:
                        st.error("パスワードが違うある。")

        action_cols = st.columns([1, 1, 1])

        with action_cols[0]:
            if is_unlocked and file_bytes is not None:
                st.download_button(
                    label="ダウンロード",
                    data=file_bytes,
                    file_name=dl_name,
                    mime=mime,
                    key=f"warehouse_download_{file_id}",
                    use_container_width=True
                )

        with action_cols[1]:
            if can_delete:
                if st.button("削除", key=f"warehouse_delete_{file_id}", use_container_width=True):
                    ok = soft_delete_warehouse_file(file_id)
                    if ok:
                        create_admin_log(
                            action_type="delete_warehouse_file",
                            target_type="warehouse_file",
                            target_id=file_id,
                            action_detail=f"title={title}"
                        )
                        st.success("削除したある。")
                        st.rerun()
                    else:
                        st.error("削除に失敗したある。")

        with action_cols[2]:
            st.write("")