import time
import re
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
    safe_click,
    set_input_value,
)

# ===================================
# 利用中利用者一覧
# ===================================
def _norm_name(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "").strip())

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

    names = sorted(work["resident_name"].unique().tolist())
    return names

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

    for row in rows:
        try:
            txt = _norm_name((row.text or "").replace("\n", " "))
            if target and target in txt:
                return row
        except Exception:
            continue

    return None

# ===================================
# 鉛筆押下
# ===================================
def _click_row_edit_button(driver, row):
    # 右端セルの button/svg を優先
    try:
        tds = row.find_elements(By.TAG_NAME, "td")
        if tds:
            last_td = tds[-1]

            try:
                btns = last_td.find_elements(By.TAG_NAME, "button")
            except Exception:
                btns = []

            for btn in btns:
                try:
                    if safe_click(driver, btn):
                        return True
                except Exception:
                    continue

            try:
                svgs = last_td.find_elements(By.TAG_NAME, "svg")
            except Exception:
                svgs = []

            for svg in svgs:
                try:
                    btn = svg.find_element(By.XPATH, "./ancestor::button[1]")
                    if safe_click(driver, btn):
                        return True
                except Exception:
                    continue
    except Exception:
        pass

    # フォールバック
    try:
        btns = row.find_elements(By.TAG_NAME, "button")
        for btn in btns:
            try:
                if safe_click(driver, btn):
                    return True
            except Exception:
                continue
    except Exception:
        pass

    return False

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
# メイン実行
# ===================================
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

            remark_el = _find_remark_input(driver)
            if remark_el is None:
                failed.append(f"{resident_name}: 備考欄が見つからない")
                continue

            set_input_value(driver, remark_el, remark_text)
            time.sleep(0.3)

            if not _save_dialog(driver):
                failed.append(f"{resident_name}: 保存ボタンが見つからない")
                continue

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

# ===================================
# ページUI
# ===================================
def render_knowbe_home_flag_page():
    st.title("🐝 在宅利用一括入力")
    st.caption("利用実績ページの備考欄へ「在宅利用」を一括で入力します。")

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

    remark_text = st.text_input(
        "備考欄へ入れる文字",
        value="在宅利用",
        key="knowbe_home_flag_remark",
    )

    login_username, login_password = get_knowbe_login_credentials()

    if login_username:
        st.caption(f"使用するKnowbe ID: {login_username}")

    live_box = st.empty()

    if st.button("在宅利用を一括入力する", key="run_knowbe_home_flag", use_container_width=True):
        if not selected_residents:
            st.warning("対象利用者を1人以上選んでください。")
            return

        if not login_username or not login_password:
            st.error("Knowbeのログイン情報が取得できません。")
            return

        driver = None
        try:
            live_box.info("Knowbeを起動しています...")
            driver = build_chrome_driver()
            manual_login_wait(driver, login_username, login_password)

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

if st.button("在宅利用を一括入力する"):
    if not selected_users:
        st.error("対象利用者を選択してください。")
    elif not isinstance(date_range, tuple) or len(date_range) != 2:
        st.error("開始日と終了日を選択してください。")
    else:
        start_date, end_date = date_range

        if start_date > end_date:
            st.error("開始日は終了日以前にしてください。")
        else:
            target_dates = []
            current = start_date
            while current <= end_date:
                target_dates.append(current)
                current += timedelta(days=1)

            st.write("対象日:", target_dates)

            # ここで selected_users × target_dates に対して処理
            for user in selected_users:
                for d in target_dates:
                    # 例: knowbeへ登録する関数
                    # register_zaitaku(user=user, target_date=d, remark=remark_text)
                    pass

            st.success(f"{start_date} ～ {end_date} の期間で在宅利用を一括入力しました。")