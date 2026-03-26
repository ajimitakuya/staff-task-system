from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from db import get_chat_rooms_df, get_chat_messages_df, save_db
from warehouse import save_warehouse_file


JST = timezone(timedelta(hours=9))


def now_jst():
    return datetime.now(JST)


def get_next_room_id():
    df = get_chat_rooms_df()
    if df is None or df.empty:
        return "R0001"

    nums = []
    for x in df["room_id"].fillna("").astype(str):
        x = x.strip().upper()
        if x.startswith("R"):
            num = x[1:]
            if num.isdigit():
                nums.append(int(num))

    next_num = max(nums) + 1 if nums else 1
    return f"R{next_num:04d}"


def get_next_message_id():
    df = get_chat_messages_df()
    if df is None or df.empty:
        return "M0001"

    nums = []
    for x in df["message_id"].fillna("").astype(str):
        x = x.strip().upper()
        if x.startswith("M"):
            num = x[1:]
            if num.isdigit():
                nums.append(int(num))

    next_num = max(nums) + 1 if nums else 1
    return f"M{next_num:04d}"


def create_chat_room(
    room_name,
    room_type,
    room_password="",
    description="",
):
    df = get_chat_rooms_df()

    now_str = now_jst().strftime("%Y-%m-%d %H:%M:%S")
    room_id = get_next_room_id()

    new_row = pd.DataFrame([{
        "room_id": room_id,
        "room_name": str(room_name).strip(),
        "room_type": str(room_type).strip(),
        "room_password": str(room_password).strip(),
        "created_by_user_id": str(st.session_state.get("user_id", "")).strip(),
        "created_by_company_id": str(st.session_state.get("company_id", "")).strip(),
        "description": str(description).strip(),
        "status": "active",
        "created_at": now_str,
        "updated_at": now_str,
    }])

    df = pd.concat([df, new_row], ignore_index=True)
    save_db(df, "chat_rooms")
    return room_id


def create_chat_message(room_id, message_text, attached_file=None):
    df = get_chat_messages_df()

    now_str = now_jst().strftime("%Y-%m-%d %H:%M:%S")
    message_id = get_next_message_id()

    has_attachment = "0"
    attachment_type = ""
    linked_file_id = ""

    if attached_file is not None:
        visibility_type = "public"
        room_df = get_chat_rooms_df()
        room_row = room_df[room_df["room_id"].astype(str) == str(room_id).strip()].copy()

        if not room_row.empty:
            room_type = str(room_row.iloc[0].get("room_type", "")).strip()
            room_pw = str(room_row.iloc[0].get("room_password", "")).strip()

            if room_type == "limited":
                visibility_type = "limited"
            else:
                visibility_type = "public"

            linked_file_id = save_warehouse_file(
                title=f"[チャット添付] {attached_file.name}",
                description=f"チャットルーム {room_id} から自動保存",
                category_main="チャット添付",
                category_sub=str(room_id).strip(),
                tags="チャット添付,自動保存",
                uploaded_file=attached_file,
                visibility_type=visibility_type,
                download_password=room_pw if room_type == "limited" else "",
                is_searchable="1",
                source_room_id=str(room_id).strip(),
            )

            has_attachment = "1"
            lower_name = str(attached_file.name).lower()
            if "." in lower_name:
                attachment_type = lower_name.rsplit(".", 1)[-1]
            else:
                attachment_type = "other"

    new_row = pd.DataFrame([{
        "message_id": message_id,
        "room_id": str(room_id).strip(),
        "user_id": str(st.session_state.get("user_id", "")).strip(),
        "display_name": str(st.session_state.get("user", "")).strip(),
        "company_id": str(st.session_state.get("company_id", "")).strip(),
        "message_text": str(message_text).strip(),
        "has_attachment": has_attachment,
        "attachment_type": attachment_type,
        "linked_file_id": linked_file_id,
        "is_deleted": "0",
        "created_at": now_str,
        "updated_at": now_str,
        "deleted_by_user_id": "",
        "deleted_at": "",
    }])

    df = pd.concat([df, new_row], ignore_index=True)
    save_db(df, "chat_messages")
    return message_id


def get_room_type_label_and_colors(room_type: str):
    room_type = str(room_type).strip().lower()

    if room_type == "public":
        return {
            "label": "公開ルーム",
            "bg_color": "#EAF7EE",
            "line_color": "#2ECC71",
            "dot_color": "#2ECC71",
        }

    return {
        "label": "制限ルーム",
        "bg_color": "#FCEEF5",
        "line_color": "#F3A6C8",
        "dot_color": "#F3A6C8",
    }


def render_room_card(room_id, room_name, room_type, desc, is_selected):
    style = get_room_type_label_and_colors(room_type)
    safe_desc = desc if desc else "説明なし"
    border_style = "2px solid #111827" if is_selected else "1px solid #E5E7EB"

    st.markdown(
        f"""
        <div style="
            background:{style['bg_color']};
            border-left:8px solid {style['line_color']};
            border:{border_style};
            border-radius:14px;
            padding:16px 18px;
            margin-bottom:10px;
            box-shadow:0 1px 3px rgba(0,0,0,0.05);
        ">
            <div style="font-size:24px;font-weight:700;color:#1F2937;line-height:1.2;">
                {room_name}
            </div>

            <div style="margin-top:10px;font-size:15px;color:#374151;">
                <span style="
                    display:inline-block;
                    width:12px;
                    height:12px;
                    border-radius:999px;
                    background:{style['dot_color']};
                    margin-right:8px;
                    vertical-align:middle;
                "></span>
                {style['label']}
            </div>

            <div style="margin-top:10px;font-size:14px;color:#4B5563;">
                説明: {safe_desc}
            </div>

            <div style="margin-top:6px;font-size:14px;color:#4B5563;">
                ID: {room_id}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_chat_room_page():
    st.title("💬 チャットルーム")
    st.caption("ルーム一覧・新規作成・投稿ができるある。")

    rooms_df = get_chat_rooms_df()
    msgs_df = get_chat_messages_df()

    if "selected_room_id" not in st.session_state:
        st.session_state.selected_room_id = ""

    if "pending_room_id" not in st.session_state:
        st.session_state.pending_room_id = ""

    if "pending_room_type" not in st.session_state:
        st.session_state.pending_room_type = ""

    top_cols = st.columns([1, 1])

    with top_cols[0]:
        if st.button("← 休憩室へ戻る", key="back_break_room", use_container_width=True):
            st.session_state.current_page = "休憩室"
            st.rerun()

    with top_cols[1]:
        if st.button("選択中ルームを解除", key="clear_selected_room", use_container_width=True):
            st.session_state.selected_room_id = ""
            st.session_state.pending_room_id = ""
            st.session_state.pending_room_type = ""
            st.rerun()

    st.divider()

    with st.expander("＋ 新しいルームを作る"):
        room_name = st.text_input("ルーム名", key="new_room_name")
        room_type = st.selectbox(
            "公開設定",
            ["public", "limited"],
            format_func=lambda x: "公開ルーム" if x == "public" else "制限ルーム",
            key="new_room_type"
        )
        room_password = st.text_input(
            "ルームパスワード（制限ルーム用）",
            key="new_room_password"
        )
        room_description = st.text_area("説明", key="new_room_description", height=80)

        if st.button("ルームを作成", key="create_new_room_button", use_container_width=True):
            if not str(room_name).strip():
                st.error("ルーム名を入れてほしいある。")
            elif room_type == "limited" and not str(room_password).strip():
                st.error("制限ルームにはパスワードが必要ある。")
            else:
                new_room_id = create_chat_room(
                    room_name=room_name,
                    room_type=room_type,
                    room_password=room_password,
                    description=room_description,
                )
                st.success(f"ルーム作成完了ある！ {new_room_id}")
                st.session_state.selected_room_id = new_room_id
                st.session_state.pending_room_id = ""
                st.session_state.pending_room_type = ""
                st.rerun()

    st.divider()

    left_col, right_col = st.columns([1, 2])

    with left_col:
        st.markdown("### ルーム一覧")

        if rooms_df is None or rooms_df.empty:
            st.info("まだルームがないある。")
        else:
            work = rooms_df.copy()
            work = work[work["status"].astype(str).str.strip().str.lower() == "active"].copy()

            work["room_type"] = work["room_type"].fillna("").astype(str).str.strip().str.lower()
            work = work[work["room_type"].isin(["public", "limited"])].copy()

            try:
                work = work.sort_values(["created_at"], ascending=[False])
            except Exception:
                pass

            for _, row in work.iterrows():
                room_id = str(row.get("room_id", "")).strip()
                room_name = str(row.get("room_name", "")).strip()
                room_type = str(row.get("room_type", "")).strip().lower()
                desc = str(row.get("description", "")).strip()

                is_selected = str(st.session_state.get("selected_room_id", "")).strip() == room_id

                render_room_card(
                    room_id=room_id,
                    room_name=room_name,
                    room_type=room_type,
                    desc=desc,
                    is_selected=is_selected,
                )

                if st.button("詳細を見る", key=f"select_room_{room_id}", use_container_width=True):
                    if room_type == "limited":
                        st.session_state.pending_room_id = room_id
                        st.session_state.pending_room_type = room_type
                    else:
                        st.session_state.selected_room_id = room_id
                        st.session_state.pending_room_id = ""
                        st.session_state.pending_room_type = ""
                    st.rerun()

        if st.session_state.get("pending_room_id"):
            st.divider()
            st.markdown("### パスワード入力")
            pw = st.text_input("ルームパスワード", type="password", key="room_access_password")

            if st.button("入室する", key="enter_limited_room", use_container_width=True):
                room_id = st.session_state.get("pending_room_id", "")
                target = rooms_df[rooms_df["room_id"].astype(str) == str(room_id)].copy()

                if target.empty:
                    st.error("ルームが見つからないある。")
                else:
                    real_pw = str(target.iloc[0].get("room_password", "")).strip()
                    if str(pw).strip() == real_pw:
                        st.session_state.selected_room_id = room_id
                        st.session_state.pending_room_id = ""
                        st.session_state.pending_room_type = ""
                        st.success("入室できたある。")
                        st.rerun()
                    else:
                        st.error("パスワードが違うある。")

    with right_col:
        selected_room_id = str(st.session_state.get("selected_room_id", "")).strip()

        if not selected_room_id:
            st.info("左からルームを選ぶある。")
        else:
            room_row = rooms_df[rooms_df["room_id"].astype(str) == selected_room_id].copy()

            if room_row.empty:
                st.warning("選択中ルームが見つからないある。")
            else:
                room_name = str(room_row.iloc[0].get("room_name", "")).strip()
                room_type = str(room_row.iloc[0].get("room_type", "")).strip()
                room_desc = str(room_row.iloc[0].get("description", "")).strip()

                st.markdown(f"## {room_name}")
                st.caption(f"公開設定: {room_type}")
                if room_desc:
                    st.write(room_desc)

                st.divider()

                post_text = st.text_area("メッセージ", key="chat_post_text", height=100)
                attached_file = st.file_uploader(
                    "添付ファイル（あれば倉庫へ自動保存）",
                    key=f"chat_attach_{selected_room_id}"
                )

                if st.button("投稿する", key="chat_post_button", use_container_width=True):
                    if not str(post_text).strip() and attached_file is None:
                        st.error("メッセージか添付のどちらかを入れてほしいある。")
                    else:
                        create_chat_message(selected_room_id, post_text, attached_file=attached_file)
                        st.success("投稿したある！")
                        st.rerun()

                st.divider()
                st.markdown("### 投稿一覧")

                room_msgs = msgs_df.copy()
                room_msgs = room_msgs[
                    (room_msgs["room_id"].astype(str) == selected_room_id) &
                    (room_msgs["is_deleted"].astype(str) != "1")
                ].copy()

                try:
                    room_msgs = room_msgs.sort_values(["created_at"], ascending=[True])
                except Exception:
                    pass

                if room_msgs.empty:
                    st.info("まだ投稿がないある。")
                else:
                    for _, msg in room_msgs.iterrows():
                        display_name = str(msg.get("display_name", "")).strip()
                        company_id = str(msg.get("company_id", "")).strip()
                        message_text = str(msg.get("message_text", "")).strip()
                        created_at = str(msg.get("created_at", "")).strip()
                        has_attachment = str(msg.get("has_attachment", "")).strip()
                        linked_file_id = str(msg.get("linked_file_id", "")).strip()

                        attach_text = ""
                        if has_attachment == "1" and linked_file_id:
                            attach_text = f"<div style='margin-top:6px;color:#2563eb;'>📎 添付あり（倉庫ID: {linked_file_id}）</div>"

                        st.markdown(
                            f"""
                            <div style="padding:10px 12px;border:1px solid #e5e7eb;border-radius:10px;margin-bottom:10px;background:#fff;">
                                <div style="font-size:13px;color:#666;"><b>{display_name}</b> / {company_id} / {created_at}</div>
                                <div style="margin-top:6px;white-space:pre-wrap;">{message_text}</div>
                                {attach_text}
                            </div>
                            """,
                            unsafe_allow_html=True
                        )