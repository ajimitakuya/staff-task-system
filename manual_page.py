from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from db import get_df, save_db


JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


def get_current_company_id():
    return str(st.session_state.get("company_id", "")).strip()


def get_manual_required_cols():
    return [
        "company_id",
        "id",
        "title",
        "content",
        "image_data",
        "created_at",
    ]


def normalize_manual_df(df):
    required_cols = get_manual_required_cols()

    if df is None or df.empty:
        return pd.DataFrame(columns=required_cols)

    work = df.copy().fillna("")

    for col in required_cols:
        if col not in work.columns:
            work[col] = ""

    work = work[required_cols].copy()
    work["company_id"] = work["company_id"].astype(str).str.strip()
    return work


def get_manual_df(company_id=None):
    if company_id is None:
        company_id = get_current_company_id()

    df = get_df("manual")
    work = normalize_manual_df(df)
    return work[work["company_id"] == str(company_id).strip()].copy()


def get_next_manual_id(manual_df=None):
    if manual_df is None:
        manual_df = get_manual_df()

    if manual_df is None or manual_df.empty:
        return 1

    ids = pd.to_numeric(manual_df["id"], errors="coerce").dropna()
    return int(ids.max()) + 1 if not ids.empty else 1


def save_new_manual(title, content):
    company_id = get_current_company_id()

    all_df = get_df("manual")
    all_df = normalize_manual_df(all_df)

    company_df = all_df[all_df["company_id"] == company_id].copy()
    next_id = get_next_manual_id(company_df)

    new_row = pd.DataFrame([{
        "company_id": company_id,
        "id": next_id,
        "title": str(title).strip(),
        "content": str(content).strip(),
        "image_data": "",
        "created_at": now_jst().strftime("%Y-%m-%d %H:%M"),
    }])

    merged = pd.concat([all_df, new_row], ignore_index=True)
    save_db(merged, "manual")
    return next_id


def update_manual(manual_id, title, content):
    company_id = get_current_company_id()

    all_df = get_df("manual")
    all_df = normalize_manual_df(all_df)

    mask = (
        (all_df["company_id"].astype(str).str.strip() == company_id) &
        (all_df["id"].astype(str).str.strip() == str(manual_id).strip())
    )

    if not mask.any():
        return False

    all_df.loc[mask, "title"] = str(title).strip()
    all_df.loc[mask, "content"] = str(content).strip()

    save_db(all_df, "manual")
    return True


def delete_manual(manual_id):
    company_id = get_current_company_id()

    all_df = get_df("manual")
    all_df = normalize_manual_df(all_df)

    mask = (
        (all_df["company_id"].astype(str).str.strip() == company_id) &
        (all_df["id"].astype(str).str.strip() == str(manual_id).strip())
    )

    if not mask.any():
        return False

    all_df = all_df[~mask].copy()
    save_db(all_df, "manual")
    return True


def render_manual_page():
    st.title("📘 マニュアル")
    st.caption("事業所ごとの手順書や引き継ぎメモを保存・閲覧するページある。")

    df = get_manual_df()

    with st.expander("➕ 新しいマニュアルを登録する"):
        with st.form("new_manual_form"):
            title = st.text_input("タイトル")
            content = st.text_area("内容", height=240)

            if st.form_submit_button("登録する"):
                if not str(title).strip():
                    st.error("タイトルを入れてほしいある。")
                elif not str(content).strip():
                    st.error("内容を入れてほしいある。")
                else:
                    manual_id = save_new_manual(title, content)
                    st.success(f"登録したある！ ID={manual_id}")
                    st.rerun()

    st.divider()

    if df is None or df.empty:
        st.info("まだマニュアルがないある。")
        return

    keyword = st.text_input("検索", key="manual_search_keyword")

    work = df.copy()

    if keyword.strip():
        kw = keyword.strip()
        work = work[
            work["title"].astype(str).str.contains(kw, case=False, na=False) |
            work["content"].astype(str).str.contains(kw, case=False, na=False)
        ].copy()

    try:
        work["id_num"] = pd.to_numeric(work["id"], errors="coerce")
        work = work.sort_values(["id_num"], ascending=[False])
    except Exception:
        pass

    if work.empty:
        st.info("条件に合うマニュアルがないある。")
        return

    for _, row in work.iterrows():
        manual_id = str(row.get("id", "")).strip()
        title = str(row.get("title", "")).strip()
        content = str(row.get("content", "")).strip()
        created_at = str(row.get("created_at", "")).strip()

        st.markdown("---")
        st.markdown(f"## {title}")
        st.caption(f"ID: {manual_id} / 作成日時: {created_at}")

        with st.expander("内容を見る", expanded=False):
            st.write(content)

        edit_key = f"edit_manual_{manual_id}"
        if edit_key not in st.session_state:
            st.session_state[edit_key] = False

        action_cols = st.columns(3)

        with action_cols[0]:
            if st.button("編集", key=f"edit_btn_{manual_id}", use_container_width=True):
                st.session_state[edit_key] = not st.session_state[edit_key]
                st.rerun()

        with action_cols[1]:
            if st.button("削除", key=f"delete_btn_{manual_id}", use_container_width=True):
                ok = delete_manual(manual_id)
                if ok:
                    st.success("削除したある。")
                    st.rerun()
                else:
                    st.error("削除に失敗したある。")

        with action_cols[2]:
            st.write("")

        if st.session_state.get(edit_key, False):
            with st.form(f"edit_manual_form_{manual_id}"):
                new_title = st.text_input("タイトル", value=title, key=f"title_{manual_id}")
                new_content = st.text_area("内容", value=content, height=240, key=f"content_{manual_id}")

                save_clicked = st.form_submit_button("更新する")
                if save_clicked:
                    if not str(new_title).strip():
                        st.error("タイトルを入れてほしいある。")
                    elif not str(new_content).strip():
                        st.error("内容を入れてほしいある。")
                    else:
                        ok = update_manual(manual_id, new_title, new_content)
                        if ok:
                            st.session_state[edit_key] = False
                            st.success("更新したある。")
                            st.rerun()
                        else:
                            st.error("更新に失敗したある。")