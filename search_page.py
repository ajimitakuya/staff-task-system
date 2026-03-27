import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from db import get_df


JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


def get_current_company_id():
    return str(st.session_state.get("company_id", "")).strip()


def safe_text(v):
    return str(v).strip() if v is not None else ""


def contains_any(text, keyword):
    return keyword.lower() in safe_text(text).lower()


def try_parse_json(raw_text: str):
    raw = safe_text(raw_text)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def search_archive_files(keyword: str, company_id: str):
    df = get_df("archive_files")
    if df is None or df.empty:
        return []

    work = df.copy().fillna("")

    required_cols = [
        "archive_file_id", "company_id", "title", "description",
        "category_main", "category_sub", "tags",
        "file_name", "file_type", "created_at", "updated_at", "is_deleted"
    ]
    for col in required_cols:
        if col not in work.columns:
            work[col] = ""

    work = work[
        (work["company_id"].astype(str).str.strip() == company_id) &
        (work["is_deleted"].astype(str).str.strip() != "1")
    ].copy()

    results = []
    for _, row in work.iterrows():
        hay = " ".join([
            safe_text(row.get("title")),
            safe_text(row.get("description")),
            safe_text(row.get("category_main")),
            safe_text(row.get("category_sub")),
            safe_text(row.get("tags")),
            safe_text(row.get("file_name")),
        ])
        if contains_any(hay, keyword):
            results.append({
                "source": "書類アップロード",
                "title": safe_text(row.get("title")) or safe_text(row.get("file_name")),
                "subtitle": f"{safe_text(row.get('category_main'))} / {safe_text(row.get('category_sub'))}",
                "summary": safe_text(row.get("description")),
                "meta": f"file={safe_text(row.get('file_name'))} / updated={safe_text(row.get('updated_at')) or safe_text(row.get('created_at'))}",
                "raw": row.to_dict(),
            })
    return results


def search_warehouse_files(keyword: str):
    df = get_df("warehouse_files")
    if df is None or df.empty:
        return []

    work = df.copy().fillna("")

    required_cols = [
        "file_id", "title", "description",
        "category_main", "category_sub", "tags",
        "file_name", "file_type", "visibility_type",
        "is_searchable", "is_deleted", "created_at", "updated_at"
    ]
    for col in required_cols:
        if col not in work.columns:
            work[col] = ""

    work = work[
        (work["is_deleted"].astype(str).str.strip() != "1") &
        (work["is_searchable"].astype(str).str.strip() == "1")
    ].copy()

    results = []
    for _, row in work.iterrows():
        hay = " ".join([
            safe_text(row.get("title")),
            safe_text(row.get("description")),
            safe_text(row.get("category_main")),
            safe_text(row.get("category_sub")),
            safe_text(row.get("tags")),
            safe_text(row.get("file_name")),
        ])
        if contains_any(hay, keyword):
            results.append({
                "source": "倉庫",
                "title": safe_text(row.get("title")) or safe_text(row.get("file_name")),
                "subtitle": f"{safe_text(row.get('category_main'))} / {safe_text(row.get('category_sub'))}",
                "summary": safe_text(row.get("description")),
                "meta": f"file={safe_text(row.get('file_name'))} / 公開={safe_text(row.get('visibility_type'))} / updated={safe_text(row.get('updated_at')) or safe_text(row.get('created_at'))}",
                "raw": row.to_dict(),
            })
    return results


def search_saved_documents(keyword: str):
    df = get_df("saved_documents")
    if df is None or df.empty:
        return []

    work = df.copy().fillna("")

    required_cols = [
        "record_id", "resident_id", "resident_name",
        "doc_type", "created_at", "updated_at", "json_data"
    ]
    for col in required_cols:
        if col not in work.columns:
            work[col] = ""

    results = []
    for _, row in work.iterrows():
        json_data = safe_text(row.get("json_data"))
        hay = " ".join([
            safe_text(row.get("resident_name")),
            safe_text(row.get("resident_id")),
            safe_text(row.get("doc_type")),
            json_data,
        ])
        if contains_any(hay, keyword):
            results.append({
                "source": "保存書類",
                "title": f"{safe_text(row.get('resident_name'))} / {safe_text(row.get('doc_type'))}",
                "subtitle": f"resident_id={safe_text(row.get('resident_id'))}",
                "summary": f"updated={safe_text(row.get('updated_at')) or safe_text(row.get('created_at'))}",
                "meta": f"record_id={safe_text(row.get('record_id'))}",
                "raw": row.to_dict(),
            })
    return results


def search_manuals(keyword: str, company_id: str):
    df = get_df("manual")
    if df is None or df.empty:
        return []

    work = df.copy().fillna("")

    required_cols = ["company_id", "id", "title", "content", "created_at"]
    for col in required_cols:
        if col not in work.columns:
            work[col] = ""

    work = work[work["company_id"].astype(str).str.strip() == company_id].copy()

    results = []
    for _, row in work.iterrows():
        hay = " ".join([
            safe_text(row.get("title")),
            safe_text(row.get("content")),
        ])
        if contains_any(hay, keyword):
            results.append({
                "source": "マニュアル",
                "title": safe_text(row.get("title")),
                "subtitle": f"id={safe_text(row.get('id'))}",
                "summary": safe_text(row.get("content"))[:160],
                "meta": f"created_at={safe_text(row.get('created_at'))}",
                "raw": row.to_dict(),
            })
    return results


def render_result_card(item, index: int):
    st.markdown("---")
    st.markdown(f"### {item['title']}")
    st.caption(f"{item['source']} / {item['subtitle']}")
    if item["summary"]:
        st.write(item["summary"])
    if item["meta"]:
        st.caption(item["meta"])

    with st.expander("詳細データを見る", expanded=False):
        raw = item.get("raw", {})
        if item["source"] == "保存書類":
            parsed = try_parse_json(raw.get("json_data", ""))
            if parsed is not None:
                st.json(parsed)
            else:
                st.text_area(
                    "json_data",
                    value=safe_text(raw.get("json_data")),
                    height=220,
                    key=f"search_json_raw_{index}"
                )
        else:
            st.json(raw)


def render_search_page():
    st.title("⓪ 検索")
    st.caption("書類アップロード・倉庫・保存書類・マニュアルを横断検索するある。")

    company_id = get_current_company_id()

    keyword = st.text_input("検索キーワード", key="global_search_keyword")

    source_options = ["すべて", "書類アップロード", "倉庫", "保存書類", "マニュアル"]
    selected_source = st.selectbox("カテゴリー", source_options, key="global_search_source")

    if not keyword.strip():
        st.info("キーワードを入れると検索できるある。")
        return

    results = []

    if selected_source in ["すべて", "書類アップロード"]:
        results.extend(search_archive_files(keyword, company_id))

    if selected_source in ["すべて", "倉庫"]:
        results.extend(search_warehouse_files(keyword))

    if selected_source in ["すべて", "保存書類"]:
        results.extend(search_saved_documents(keyword))

    if selected_source in ["すべて", "マニュアル"]:
        results.extend(search_manuals(keyword, company_id))

    st.divider()
    st.subheader(f"検索結果: {len(results)}件")

    if not results:
        st.warning("該当データは見つからなかったある。")
        return

    for i, item in enumerate(results):
        render_result_card(item, i)