import time
import re
from datetime import timedelta, date

import streamlit as st
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from data_access import load_db
from run_assistance import (
    build_chrome_driver,
    get_knowbe_login_credentials,
    manual_login_wait,
    goto_report_daily,
    goto_report_date,
    safe_click,
    set_input_value,
    click_pencil_in_row,
    wait_table_stable_after_date_change,
)

# ===================================
# 共通
# ===================================
def _norm_name(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "").strip())

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
# 利用実績一覧の対象行探し
# ===================================
def _find_usage_row_by_name(driver, resident_name: str, timeout: int = 15):
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))

    target = _norm_name(resident_name)

    try:
        rows = driver.find_elements(By.XPATH, "//tbody/tr")
    except Exception:
        rows = []

    # まず完全一致寄り
    for row in rows:
        try:
            txt = _norm_name((row.text or "").replace("\n", " "))
            if txt == target or txt.startswith(target):
                return row
        except Exception:
            continue

    # 次に部分一致
    for row in rows:
        try:
            txt = _norm_name((row.text or "").replace("\n", " "))
            if target and target in txt:
                return row
        except Exception:
            continue

    return None

# ===================================
# 備考欄
# ===================================
def _find_remark_input(driver, timeout: int = 10):
    wait = WebDriverWait(driver, timeout)

    candidates = [
        (By.XPATH, "//textarea"),
        (By.XPATH, "//input[@type='text']"),
    ]

    for by, selector in candidates:
        try:
            el = wait.until(EC.presence_of_element_located((by, selector)))
            return el
        except Exception:
            continue

    return None

# ===================================
# 保存
# ===================================
def _save_dialog(driver, timeout: int = 10):
    wait = WebDriverWait(driver, timeout)

    xpaths = [
        "//button[contains(normalize-space(.),'保存')]",
        "//button[contains(normalize-space(.),'更新')]",
        "//span[contains(normalize-space(.),'保存')]/ancestor::button[1]",
        "//span[contains(normalize-space(.),'更新')]/ancestor::button[1]",
    ]

    for xp in xpaths:
        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, xp)))
            if safe_click(driver, btn):
                time.sleep(0.8)
                return True
        except Exception:
            continue

    return False

# ===================================
# 1人・1日処理
# ===================================
def _apply_home_flag_one(driver, target_date: date, resident_name: str, remark_text="在宅利用"):
    try:
        goto_report_date(driver, target_date.year, target_date.month, target_date.day)
        wait_table_stable_after_date_change(driver, timeout=20)
        time.sleep(0.8)

        row = _find_usage_row_by_name(driver, resident_name)
        if row is None:
            return False, f"{resident_name}: {target_date} 一覧で見つからない"

        # run_assistance 側の最新版に合わせる
        if not click_pencil_in_row(driver, row):
            return False, f"{resident_name}: {target_date} 鉛筆ボタンを押せない"

        time.sleep(1.0)

        remark_el = _find_remark_input(driver)
        if remark_el is None:
            return False, f"{resident_name}: {target_date} 備考欄が見つからない"

        set_input_value(driver, remark_el, remark_text)
        time.sleep(0.3)

        if not _save_dialog(driver):
            return False, f"{resident_name}: {target_date} 保存ボタンが見つからない"

        time.sleep(1.0)
        return True, f"{resident_name}: {target_date} 完了"

    except Exception as e:
        return False, f"{resident_name}: {target_date} {e}"

# ===================================
# 期間一括
# ===================================
def run_set_home_flag_period(driver, resident_names, start_date: date, end_date: date, remark_text="在宅利用", status_box=None):
    done = []
    failed = []

    goto_report_daily(driver)
    wait_table_stable_after_date_change(driver, timeout=20)
    time.sleep(1.0)

    all_dates = list(_iter_dates(start_date, end_date))

    for d in all_dates:
        for resident_name in resident_names:
            if status_box is not None:
                status_box.info(f"処理中: {d} / {resident_name}")

            ok, msg = _apply_home_flag_one(
                driver=driver,
                target_date=d,
                resident_name=resident_name,
                remark_text=remark_text,
            )

            if ok:
                done.append(msg)
            else:
                failed.append(msg)

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