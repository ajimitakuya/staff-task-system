import time
import pandas as pd
import streamlit as st
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from data_access import load_db
from run_assistance import (
    build_chrome_driver,
    manual_login_wait,
    goto_report_daily,
    safe_click,
    set_input_value,
)

def _get_active_residents(company_id: str):
    df = load_db("resident_master")
    if df is None or df.empty:
        return []

    work = df.copy().fillna("")

    if "company_id" in work.columns:
        work = work[work["company_id"].astype(str).str.strip() == str(company_id).strip()].copy()

    if "resident_name" not in work.columns:
        return []

    if "status" in work.columns:
        status_col = work["status"].astype(str).str.strip()
        work = work[status_col.isin(["利用中", "active", "有効"])].copy()

    work["resident_name"] = work["resident_name"].astype(str).str.strip()
    work = work[work["resident_name"] != ""].copy()

    return sorted(work["resident_name"].unique().tolist())

def _find_usage_row_by_name(driver, resident_name: str, timeout: int = 15):
    wait = WebDriverWait(driver, timeout)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))

    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    for row in rows:
        text = (row.text or "").strip()
        if resident_name in text:
            return row
    return None

def _open_row_pencil(row):
    buttons = row.find_elements(By.TAG_NAME, "button")
    for btn in buttons:
        if safe_click(row.parent, btn):
            return True
    return False

def _click_row_edit_button(driver, row):
    try:
        last_td = row.find_elements(By.TAG_NAME, "td")[-1]
    except Exception:
        last_td = row

    try:
        btns = last_td.find_elements(By.TAG_NAME, "button")
        for btn in btns:
            if safe_click(driver, btn):
                return True
    except Exception:
        pass

    try:
        svgs = last_td.find_elements(By.TAG_NAME, "svg")
        for svg in svgs:
            try:
                btn = svg.find_element(By.XPATH, "./ancestor::button[1]")
                if safe_click(driver, btn):
                    return True
            except Exception:
                continue
    except Exception:
        pass

    try:
        btn = row.find_element(By.XPATH, ".//button")
        if safe_click(driver, btn):
            return True
    except Exception:
        pass

    return False

def _find_remark_textarea(driver, timeout: int = 10):
    wait = WebDriverWait(driver, timeout)

    candidates = [
        (By.TAG_NAME, "textarea"),
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

def _save_dialog(driver, timeout: int = 10):
    wait = WebDriverWait(driver, timeout)

    xpaths = [
        "//button[contains(normalize-space(.),'保存')]",
        "//span[contains(normalize-space(.),'保存')]/ancestor::button[1]",
    ]

    for xp in xpaths:
        try:
            btn = wait.until(EC.element_to_be_clickable((By.XPATH, xp)))
            if safe_click(driver, btn):
                return True
        except Exception:
            continue

    return False

def run_set_home_flag(driver, resident_names, remark_text="在宅利用", status_box=None):
    done = []
    failed = []

    goto_report_daily(driver)
    time.sleep(2)

    for resident_name in resident_names:
        try:
            if status_box is not None:
                status_box.info(f"処理中: {resident_name}")

            row = _find_usage_row_by_name(driver, resident_name)
            if row is None:
                failed.append(f"{resident_name}: 一覧で見つからない")
                continue

            if not _click_row_edit_button(driver, row):
                failed.append(f"{resident_name}: 鉛筆ボタンを押せない")
                continue

            time.sleep(1.0)

            textarea = _find_remark_textarea(driver)
            if textarea is None:
                failed.append(f"{resident_name}: 備考欄が見つからない")
                continue

            set_input_value(driver, textarea, remark_text)
            time.sleep(0.3)

            if not _save_dialog(driver):
                failed.append(f"{resident_name}: 保存ボタンが見つからない")
                continue

            time.sleep(1.2)
            done.append(resident_name)

            goto_report_daily(driver)
            time.sleep(1.5)

        except Exception as e:
            failed.append(f"{resident_name}: {e}")
            try:
                goto_report_daily(driver)
                time.sleep(1.5)
            except Exception:
                pass

    return done, failed

def render_knowbe_home_flag_page():
    st.header("🐝 Knowbe在宅利用 一括入力")
    st.caption("利用中の利用者を選び、利用実績ページの備考欄へ「在宅利用」を順番に入力します。")

    if not st.session_state.get("is_admin", False):
        st.error("このページは管理者専用です。")
        return

    company_id = str(st.session_state.get("company_id", "")).strip()
    resident_options = _get_active_residents(company_id)

    if not resident_options:
        st.warning("利用中の利用者が見つからないか、resident_master が空です。")
        return

    selected_residents = st.multiselect(
        "対象利用者",
        resident_options,
        key="knowbe_home_flag_residents"
    )

    remark_text = st.text_input(
        "備考欄に入れる文字",
        value="在宅利用",
        key="knowbe_home_flag_remark"
    )

    username = ""
    password = ""
    try:
        companies_df = load_db("companies")
        if companies_df is not None and not companies_df.empty:
            companies_df = companies_df.fillna("")
            hit = companies_df[companies_df["company_id"].astype(str).str.strip() == company_id]
            if not hit.empty:
                row = hit.iloc[0]
                username = str(row.get("knowbe_login_username", "")).strip()
                password = str(row.get("knowbe_login_password", "")).strip()
    except Exception:
        pass

    if username:
        st.caption(f"Knowbe ID: {username}")

    live_box = st.empty()

    if st.button("一括で在宅利用を入力する", use_container_width=True):
        if not selected_residents:
            st.warning("対象利用者を選んでください。")
            return

        if not username or not password:
            st.error("companies シートに Knowbe のログイン情報がありません。")
            return

        driver = None
        try:
            live_box.info("Knowbeを起動しています...")
            driver = build_chrome_driver()
            manual_login_wait(driver, username, password)

            done, failed = run_set_home_flag(
                driver=driver,
                resident_names=selected_residents,
                remark_text=remark_text,
                status_box=live_box,
            )

            live_box.empty()

            if done:
                st.success("入力完了: " + "、".join(done))
            if failed:
                st.error("失敗あり:\n\n" + "\n".join(failed))

        except Exception as e:
            live_box.empty()
            st.error(f"実行中にエラーが発生しました: {e}")
        finally:
            try:
                if driver is not None:
                    driver.quit()
            except Exception:
                pass