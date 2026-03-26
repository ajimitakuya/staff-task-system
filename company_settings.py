import streamlit as st

from auth import (
    authenticate_company_login,
    get_company_saved_knowbe_info,
    save_company_saved_knowbe_info,
    mask_secret_text,
)
from warehouse import create_admin_log


def render_company_knowbe_settings_page():
    st.title("🔐 Knowbe情報登録")
    st.caption("現在ログイン中の事業所に、Knowbeログイン情報を保存するページある。")

    current_company_id = str(st.session_state.get("company_id", "")).strip()
    current_company_name = str(st.session_state.get("company_name", "")).strip()

    saved_user, saved_pw = get_company_saved_knowbe_info(current_company_id)

    st.info(f"対象事業所: {current_company_name}")

    verify_cols = st.columns(2)
    with verify_cols[0]:
        verify_company_login_id = st.text_input(
            "事業所ID（確認用）",
            key="knowbe_setting_verify_company_login_id"
        )
    with verify_cols[1]:
        verify_company_login_password = st.text_input(
            "事業所パスワード（確認用）",
            type="password",
            key="knowbe_setting_verify_company_login_password"
        )

    input_cols = st.columns(2)
    with input_cols[0]:
        knowbe_login_username = st.text_input(
            "knowbeアカウント名",
            value=saved_user,
            key="knowbe_setting_login_username"
        )
    with input_cols[1]:
        knowbe_login_password = st.text_input(
            "knowbeパスワード",
            type="password",
            value=saved_pw,
            key="knowbe_setting_login_password"
        )

    st.caption(f"現在保存中のアカウント名: {mask_secret_text(saved_user)}")

    btn_cols = st.columns([1, 4])
    with btn_cols[0]:
        if st.button("登録・更新", key="save_company_knowbe_settings", use_container_width=True):
            row = authenticate_company_login(verify_company_login_id, verify_company_login_password)

            if row is None:
                st.error("事業所IDまたは事業所パスワードが違うある。")
            else:
                auth_company_id = str(row.get("company_id", "")).strip()

                if auth_company_id != current_company_id:
                    st.error("現在ログイン中の事業所と一致しないある。")
                elif not str(knowbe_login_username).strip() or not str(knowbe_login_password).strip():
                    st.error("knowbeアカウント名とknowbeパスワードを両方入れてほしいある。")
                else:
                    ok = save_company_saved_knowbe_info(
                        company_id=current_company_id,
                        knowbe_login_username=knowbe_login_username,
                        knowbe_login_password=knowbe_login_password,
                    )

                    if ok:
                        create_admin_log(
                            action_type="save_company_knowbe_settings",
                            target_type="company",
                            target_id=current_company_id,
                            action_detail=f"company_name={current_company_name}"
                        )
                        st.success("Knowbe情報を保存したある！")
                        st.rerun()
                    else:
                        st.error("保存に失敗したある。")