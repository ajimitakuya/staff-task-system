import time
from datetime import timedelta, date

import streamlit as st
from data_access import load_db
from run_assistance import (
    build_chrome_driver,
    get_knowbe_login_credentials,
    manual_login_wait,
    goto_report_daily,
    update_report_note_only,
)

# ===================================
# 共通
# ===================================
def _iter_dates(start_date: date, end_date: date):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)

# ===================================
# 利用中利用者一覧
# ===================================
def _get_active_residents(company_id: str):
    df = load_db("resident_master")
    if df is None or df.empty:
        return []

    work = df.copy().fillna("")

    if "company_id" in work.columns:
        work = work[
            work["company_id"].astype(str).str.strip() == str(company_id).strip()
        ].copy()

    if "resident_name" not in work.columns:
        return []

    if "status" in work.columns:
        status_col = work["status"].astype(str).str.strip()
        work = work[status_col.isin(["利用中", "active", "有効"])].copy()

    work["resident_name"] = work["resident_name"].astype(str).str.strip()
    work = work[work["resident_name"] != ""].copy()

    return sorted(work["resident_name"].unique().tolist())

# ===================================
# 期間一括
# ===================================
def run_set_home_flag_period(driver, resident_names, start_date: date, end_date: date, remark_text="在宅利用", status_box=None):
    done = []
    failed = []

    all_dates = list(_iter_dates(start_date, end_date))

    for d in all_dates:
        day_str = d.strftime("%Y-%m-%d")

        for resident_name in resident_names:
            if status_box is not None:
                status_box.info(f"処理中: {day_str} / {resident_name}")

            ok = update_report_note_only(
                driver=driver,
                target_date=day_str,
                resident_name=resident_name,
                note_text=remark_text,
            )

            if ok:
                done.append(f"{resident_name}: {day_str} 完了")
            else:
                failed.append(f"{resident_name}: {day_str} 失敗")

    return done, failed

# ===================================
# ページUI
# ===================================
def render_knowbe_home_flag_page():
    st.title("🐝 在宅利用一括入力")
    st.caption("利用実績ページの備考欄へ「在宅利用」を期間指定で一括入力します。")

    if not st.session_state.get("is_admin", False):
        st.error("このページは管理者専用です。")
        return

    company_id = str(st.session_state.get("company_id", "")).strip()
    resident_options = _get_active_residents(company_id)

    if not resident_options:
        st.warning("利用中の利用者が見つからないか、resident_master が空です。")
        return

    selected_residents = st.multiselect(
        "対象利用者を選択",
        resident_options,
        default=[],
        key="knowbe_home_flag_residents",
    )

    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input(
            "開始日",
            value=date.today(),
            key="knowbe_home_flag_start_date",
        )

    with col2:
        end_date = st.date_input(
            "終了日",
            value=date.today(),
            key="knowbe_home_flag_end_date",
        )

    remark_text = st.text_input(
        "備考欄へ入れる文字",
        value="在宅利用",
        key="knowbe_home_flag_remark",
    )

    login_username, login_password = get_knowbe_login_credentials()

    if login_username:
        st.caption(f"使用するKnowbe ID: {login_username}")

    if start_date and end_date:
        days_count = (end_date - start_date).days + 1
        if days_count > 0:
            st.info(f"対象期間: {start_date} ～ {end_date}（{days_count}日間）")

    live_box = st.empty()

    if st.button("在宅利用を一括入力する", key="run_knowbe_home_flag", use_container_width=True):
        if not selected_residents:
            st.warning("対象利用者を1人以上選んでください。")
            return

        if not login_username or not login_password:
            st.error("Knowbeのログイン情報が取得できません。")
            return

        if start_date > end_date:
            st.error("開始日は終了日以前にしてください。")
            return

        driver = None
        try:
            live_box.info("Knowbeを起動しています...")
            driver = build_chrome_driver()

            # 先にKnowbeへ飛ぶ
            goto_report_daily(driver)
            time.sleep(1.5)

            manual_login_wait(driver, login_username, login_password)

            done, failed = run_set_home_flag_period(
                driver=driver,
                resident_names=selected_residents,
                start_date=start_date,
                end_date=end_date,
                remark_text=remark_text,
                status_box=live_box,
            )

            live_box.empty()

            if done:
                st.success(f"完了件数: {len(done)}件")
                with st.expander("完了一覧を見る"):
                    for x in done:
                        st.write(x)

            if failed:
                st.error(f"失敗件数: {len(failed)}件")
                with st.expander("失敗一覧を見る"):
                    for x in failed:
                        st.write(x)

            if not done and not failed:
                st.warning("処理対象がありませんでした。")

        except Exception as e:
            live_box.empty()
            st.error(f"実行中にエラーが発生しました: {e}")

        finally:
            try:
                if driver is not None:
                    driver.quit()
            except Exception:
                pass