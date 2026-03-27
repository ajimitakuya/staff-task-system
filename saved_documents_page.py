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


def get_saved_documents_required_cols():
    return [
        "record_id",
        "resident_id",
        "resident_name",
        "doc_type",
        "created_at",
        "updated_at",
        "json_data",
    ]


def normalize_saved_documents_df(df):
    required_cols = get_saved_documents_required_cols()

    if df is None or df.empty:
        return pd.DataFrame(columns=required_cols)

    work = df.copy().fillna("")

    for col in required_cols:
        if col not in work.columns:
            work[col] = ""

    return work[required_cols].copy()


def get_saved_documents_df():
    df = get_df("saved_documents")
    return normalize_saved_documents_df(df)


def try_parse_json(json_text: str):
    raw = str(json_text).strip()
    if not raw:
        return None, "空データ"

    try:
        return json.loads(raw), ""
    except Exception as e:
        return None, str(e)


def render_saved_documents_page():
    st.title("⑧ 保存書類")
    st.caption("作成・保存された書類データを確認するページある。")

    df = get_saved_documents_df()

    if df is None or df.empty:
        st.info("まだ保存書類がないある。")
        return

    st.subheader("🔍 絞り込み")

    filter_cols = st.columns(3)

    with filter_cols[0]:
        keyword = st.text_input("利用者名で検索", key="saved_docs_keyword")

    with filter_cols[1]:
        doc_type_options = ["すべて"] + sorted(
            [str(x).strip() for x in df["doc_type"].dropna().tolist() if str(x).strip()]
        )
        doc_type = st.selectbox("書類種別", doc_type_options, key="saved_docs_doc_type")

    with filter_cols[2]:
        sort_order = st.selectbox(
            "並び順",
            ["新しい順", "古い順"],
            key="saved_docs_sort_order"
        )

    work = df.copy()

    if keyword.strip():
        kw = keyword.strip()
        work = work[
            work["resident_name"].astype(str).str.contains(kw, case=False, na=False) |
            work["resident_id"].astype(str).str.contains(kw, case=False, na=False)
        ].copy()

    if doc_type != "すべて":
        work = work[
            work["doc_type"].astype(str).str.strip() == str(doc_type).strip()
        ].copy()

    try:
        ascending = sort_order == "古い順"
        sort_col = "updated_at" if "updated_at" in work.columns else "created_at"
        work = work.sort_values(sort_col, ascending=ascending)
    except Exception:
        pass

    st.divider()

    if work.empty:
        st.info("条件に合う保存書類はないある。")
        return

    st.subheader(f"📚 一覧（{len(work)}件）")

    summary_df = work[[
        "resident_name",
        "doc_type",
        "created_at",
        "updated_at",
    ]].copy()

    summary_df.columns = [
        "利用者名",
        "書類種別",
        "作成日時",
        "更新日時",
    ]

    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("🧾 詳細")

    options = []
    option_map = {}

    for _, row in work.iterrows():
        label = f"{row.get('resident_name', '')} / {row.get('doc_type', '')} / {row.get('updated_at', '') or row.get('created_at', '')}"
        options.append(label)
        option_map[label] = row.to_dict()

    selected_label = st.selectbox("詳細を見る書類を選択", options, key="saved_docs_selected")

    if not selected_label:
        return

    row = option_map[selected_label]

    resident_name = str(row.get("resident_name", "")).strip()
    resident_id = str(row.get("resident_id", "")).strip()
    doc_type = str(row.get("doc_type", "")).strip()
    created_at = str(row.get("created_at", "")).strip()
    updated_at = str(row.get("updated_at", "")).strip()
    json_data = str(row.get("json_data", "")).strip()

    meta_cols = st.columns(2)
    with meta_cols[0]:
        st.write(f"**利用者名**: {resident_name}")
        st.write(f"**利用者ID**: {resident_id}")
    with meta_cols[1]:
        st.write(f"**書類種別**: {doc_type}")
        st.write(f"**更新日時**: {updated_at or created_at}")

    parsed, err = try_parse_json(json_data)

    if parsed is None:
        st.warning(f"JSONを読めなかったある: {err}")
        st.text_area("生データ", value=json_data, height=260, key="saved_docs_raw_json")
        return

    view_mode = st.radio(
        "表示形式",
        ["見やすく表示", "JSONそのまま"],
        horizontal=True,
        key="saved_docs_view_mode"
    )

    if view_mode == "JSONそのまま":
        st.code(json.dumps(parsed, ensure_ascii=False, indent=2), language="json")
        return

    if isinstance(parsed, dict):
        for k, v in parsed.items():
            with st.expander(str(k), expanded=False):
                if isinstance(v, (dict, list)):
                    st.json(v)
                else:
                    st.write(v)
    elif isinstance(parsed, list):
        st.json(parsed)
    else:
        st.write(parsed)