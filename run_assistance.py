# -*- coding: utf-8 -*-
"""
knowbe 日誌入力スクリプト（安定統合版・2026-02-23 / dropdown強化fix）
- Excelは必ずTEMPにコピーしてから openpyxl で読む（Y:直読み破損対策）
- 手動ログイン（2FA/Googleログイン変動に強い）
- ログイン後、必ず「利用実績（日ごと）」で指定日付へ移動
- 日付変更後はテーブル安定待ち
- 編集(鉛筆)は「行の右端セル button/svg」優先
- 条約準拠：
    * B列が「通所」「施設外就労」のみ処理（それ以外は触らない）
    * 「通所」は E列=提供なし → 在宅扱い(備考=在宅利用), E列=提供あり → 来所扱い(備考=食事摂取量などExcel優先)
    * 実績入力は B〜F（サービス/開始/終了/食事/備考）を入れて保存まで完了
- API：キーが無いなら絶対に進めない（条約）
"""

import os
import streamlit as st
import time
import datetime
import re
import math
import time
import shutil
import tempfile

from dataclasses import dataclass
from typing import List, Tuple, Optional

import random
import google.generativeai as genai
import openpyxl

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.action_chains import ActionChains


import os
import streamlit as st
import google.generativeai as genai

# =========================
# API
# =========================
GEMINI_API_KEY = ""

try:
    GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", "")
except Exception:
    GEMINI_API_KEY = ""

if not GEMINI_API_KEY:
    try:
        if "gemini" in st.secrets and "api_key" in st.secrets["gemini"]:
            GEMINI_API_KEY = st.secrets["gemini"]["api_key"]
    except Exception:
        pass

if not GEMINI_API_KEY:
    GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

USE_GEMINI = bool(GEMINI_API_KEY)

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

# =========================
# 設定
# =========================
def get_knowbe_login_credentials():
    username = ""
    password = ""

    try:
        username = st.secrets.get("KB_LOGIN_USERNAME", "")
        password = st.secrets.get("KB_LOGIN_PASSWORD", "")
    except Exception:
        username = ""
        password = ""

    if not username:
        username = os.environ.get("KB_LOGIN_USERNAME", "")
    if not password:
        password = os.environ.get("KB_LOGIN_PASSWORD", "")

    print(f"[SECRETS CHECK NOW] LOGIN_USERNAME exists={bool(username)}", flush=True)
    print(f"[SECRETS CHECK NOW] LOGIN_PASSWORD exists={bool(password)}", flush=True)

    return username, password



def build_chrome_driver():
    options = webdriver.ChromeOptions()
    options.binary_location = "/usr/bin/chromium"

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1400,900")
    options.add_argument("--remote-debugging-port=9222")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")

    driver = webdriver.Chrome(
        service=ChromeService("/usr/bin/chromedriver"),
        options=options
    )
    driver.set_page_load_timeout(20)
    driver.set_script_timeout(20)
    driver.implicitly_wait(2)
    return driver

BASE_URL = "https://mgr.knowbe.jp/v2/"
REPORT_DAILY_URL = "https://mgr.knowbe.jp/v2/#/report/daily"
EXCEL_PATH_DEFAULT = os.path.join(os.path.dirname(__file__), "日誌基本情報サポート.xlsx")

SHEET_MAIN = "基本情報"
SHEET_TREATY = "条約"

WAIT_SHORT = 5
WAIT_NORMAL = 20

# 鉛筆SVG path(d)（フォールバック）
PENCIL_D_PREFIX = "M3 17.25V21"


# =========================
# データ構造
# =========================
@dataclass
class PersonItem:
    name: str
    service: str      # B列（通所/施設外就労/その他）
    start: str        # C列
    end: str          # D列
    meal: str         # E列（提供あり/提供なし）
    note: str         # F列（在宅利用/食事摂取量 x/10/空欄など）
    user_state: str   # H列（今回未使用）
    staff_note: str   # I列（今回未使用）
    staff_name: str   # K列（今回未使用）
    staff_mark: str   # L列（今回未使用）


# =========================
# ログ/デバッグ
# =========================
def log(msg: str):
    print(msg, flush=True)

def dump_debug(driver, tag: str):
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    outdir = os.path.join(os.path.dirname(__file__), "dbg")
    os.makedirs(outdir, exist_ok=True)
    png = os.path.join(outdir, f"debug_{tag}_{ts}.png")
    html = os.path.join(outdir, f"debug_{tag}_{ts}.html")
    try:
        driver.save_screenshot(png)
    except Exception:
        pass
    try:
        with open(html, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
    except Exception:
        pass
    log(f"[DEBUG] dump: {png}, {html}")


# =========================
# Excel: 安定読み取り（TEMPへコピー）
# =========================
def stage_excel_local(src_path: str) -> str:
    src_path = os.path.abspath(src_path)
    base = os.path.basename(src_path)
    if base.startswith("~$"):
        raise RuntimeError("[FATAL] Excelの一時ファイル(~$)を読もうとしてるある。元のxlsxを指定してくれある")
    if not os.path.exists(src_path):
        raise RuntimeError(f"[FATAL] Excelが見つからないある: {src_path}")

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    staged = os.path.join(tempfile.gettempdir(), f"knowbe_staged_{ts}.xlsx")
    shutil.copy2(src_path, staged)

    if os.path.getsize(staged) < 1000:
        raise RuntimeError(f"[FATAL] staged excel が小さすぎるある: {staged}")
    return staged

def norm(v) -> str:
    return "" if v is None else str(v).strip()

def read_treaty_check(excel_path: str):
    staged = stage_excel_local(excel_path)
    wb = openpyxl.load_workbook(staged, data_only=True)
    if SHEET_TREATY not in wb.sheetnames:
        raise RuntimeError(f"[FATAL] 条約シートが無いある: {SHEET_TREATY}")
    ws = wb[SHEET_TREATY]
    a1 = (ws["A1"].value or "")
    b1 = (ws["B1"].value or "")
    c1 = (ws["C1"].value or "")
    log(f"[DEBUG] 条約シート確認: A1={a1} / B1={b1} / C1={c1}")
    wb.close()

def normalize_hhmm(v) -> str:
    """
    Excel由来の時刻が
      - datetime.time / datetime.datetime
      - '10:01:00' / '10:01'
    どれでも最終的に 'HH:MM' にする
    """
    if v is None:
        return ""
    if isinstance(v, datetime.time):
        return f"{v.hour:02d}:{v.minute:02d}"
    if isinstance(v, datetime.datetime):
        return f"{v.hour:02d}:{v.minute:02d}"

    s = str(v).strip()
    m = re.match(r"^\s*(\d{1,2}):(\d{2})(?::\d{2})?\s*$", s)
    if m:
        hh = int(m.group(1)); mm = int(m.group(2))
        return f"{hh:02d}:{mm:02d}"
    return ""  # ★None返し事故を防ぐ

def read_sheet_data(excel_path: str) -> Tuple[int, int, int, List[PersonItem]]:
    staged = stage_excel_local(excel_path)
    wb = openpyxl.load_workbook(staged, data_only=True)

    if SHEET_MAIN not in wb.sheetnames:
        raise RuntimeError(f"[FATAL] 基本情報シートが無いある: {SHEET_MAIN}")
    ws = wb[SHEET_MAIN]

    y = int(ws["A1"].value)
    m = int(ws["B1"].value)
    d = int(ws["C1"].value)

    items: List[PersonItem] = []
    row = 3
    while True:
        name = norm(ws[f"A{row}"].value)
        if not name:
            break

        start = normalize_hhmm(ws[f"C{row}"].value)
        end   = normalize_hhmm(ws[f"D{row}"].value)

        items.append(PersonItem(
            name=name,
            service=norm(ws[f"B{row}"].value),
            start=start,   # ← 空欄なら空欄のまま
            end=end,       # ← 空欄なら空欄のまま
            meal=norm(ws[f"E{row}"].value),
            note=norm(ws[f"F{row}"].value),
            user_state=norm(ws[f"H{row}"].value),
            staff_note=norm(ws[f"I{row}"].value),
            staff_name=norm(ws[f"K{row}"].value),
            staff_mark=norm(ws[f"L{row}"].value),
        ))
        row += 1

    wb.close()
    return y, m, d, items

# =========================
# Selenium ユーティリティ
# =========================
def safe_click(driver, el) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.05)
    except Exception:
        pass
    try:
        el.click()
        return True
    except Exception:
        pass
    try:
        ActionChains(driver).move_to_element(el).pause(0.05).click(el).perform()
        return True
    except Exception:
        pass
    try:
        driver.execute_script("arguments[0].click();", el)
        return True
    except Exception:
        return False

def manual_login_wait(driver, login_username, login_password):
    login_username = "" if login_username is None else str(login_username).strip()
    login_password = "" if login_password is None else str(login_password).strip()

    if not login_username or not login_password:
        raise RuntimeError("[FATAL] login_username / login_password が空ある")

    last_error = ""
    start_ts = time.time()

    while time.time() - start_ts < 30:
        url = driver.current_url or ""

        if "login" not in url and "mgr.knowbe.jp" in url:
            return

        try:
            user_el = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.ID, "username"))
            )
            pass_el = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.ID, "password"))
            )

            set_input_value(driver, user_el, login_username)
            time.sleep(0.2)
            set_input_value(driver, pass_el, login_password)
            time.sleep(0.3)

            span = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//span[contains(normalize-space(.),'上記に同意してログイン')]")
                )
            )
            btn = span.find_element(By.XPATH, "./ancestor::button[1]")

            if not safe_click(driver, btn):
                driver.execute_script("arguments[0].click();", btn)

            t0 = time.time()
            while time.time() - t0 < 10:
                cur = driver.current_url or ""
                if "login" not in cur and "mgr.knowbe.jp" in cur:
                    time.sleep(1.0)
                    return
                time.sleep(0.3)

        except Exception as e:
            last_error = str(e)

        time.sleep(1)

    raise RuntimeError(
        f"[FATAL] 自動ログインが30秒でタイムアウトしたある。"
        f" current_url={driver.current_url!r} last_error={last_error!r}"
    )

def get_top_dialog(driver):
    ds = driver.find_elements(By.CSS_SELECTOR, "[role='dialog']")
    return ds[-1] if ds else None

def close_dialog_if_open(driver):
    dlg = get_top_dialog(driver)
    if not dlg:
        return
    for xp in [
        ".//button[contains(.,'キャンセル')]",
        ".//button[contains(.,'閉じる')]",
        ".//button[contains(.,'戻る')]",
    ]:
        try:
            b = dlg.find_element(By.XPATH, xp)
            if safe_click(driver, b):
                try:
                    WebDriverWait(driver, 10).until(EC.invisibility_of_element(dlg))
                except Exception:
                    pass
                return
        except Exception:
            pass
    try:
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
    except Exception:
        pass

def wait_table_stable_after_date_change(driver, timeout=20):
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table"))
    )
    time.sleep(0.8)
    t0 = time.time()
    while time.time() - t0 < 10:
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        if rows:
            for r in rows[:6]:
                try:
                    tds = r.find_elements(By.TAG_NAME, "td")
                    if not tds:
                        continue
                    last = tds[-1]
                    if last.find_elements(By.TAG_NAME, "button") or last.find_elements(By.TAG_NAME, "svg"):
                        return
                except Exception:
                    continue
        time.sleep(0.25)


# =========================
# 日付移動（ピッカー）
# =========================
def goto_report_daily(driver):
    driver.get("https://mgr.knowbe.jp/v2/#/report/daily")
    time.sleep(1.2)

def parse_header_date_text(s: str) -> Optional[Tuple[int, int, int]]:
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", s)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))

def get_current_header_date(driver) -> Optional[Tuple[int, int, int]]:
    try:
        header = driver.find_element(By.CSS_SELECTOR, "#reportDailyHeader")
        txt = (header.text or "").replace("\n", " ").strip()
        got = parse_header_date_text(txt)
        if got:
            return got
    except Exception:
        pass
    return None

def open_date_picker(driver) -> bool:
    try:
        header = WebDriverWait(driver, WAIT_NORMAL).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#reportDailyHeader"))
        )
    except Exception:
        return False

    btns = header.find_elements(By.TAG_NAME, "button")
    for b in btns:
        if safe_click(driver, b):
            time.sleep(0.4)
            if driver.find_elements(By.CSS_SELECTOR, "[role='dialog']"):
                return True
    return False

def _get_dialog(driver):
    ds = driver.find_elements(By.CSS_SELECTOR, "[role='dialog']")
    return ds[-1] if ds else None

def _get_scroll_container(driver, dialog):
    try:
        return dialog.find_element(By.CSS_SELECTOR, ".Cal__MonthList__root")
    except Exception:
        pass
    return None

def _visible_date_range(dialog) -> Optional[Tuple[datetime.date, datetime.date]]:
    try:
        els = dialog.find_elements(By.CSS_SELECTOR, "[data-date]")
        ds = []
        for el in els:
            dd = el.get_attribute("data-date") or ""
            if re.match(r"^\d{4}-\d{2}-\d{2}$", dd):
                y, m, d = dd.split("-")
                ds.append(datetime.date(int(y), int(m), int(d)))
        if not ds:
            return None
        return min(ds), max(ds)
    except Exception:
        return None

def _click_confirm_if_any(driver, dialog) -> bool:
    for xp in [
        ".//button[contains(.,'確定する')]",
        ".//button[contains(.,'確定')]",
        ".//button[contains(.,'OK')]",
        ".//button[contains(.,'決定')]",
    ]:
        try:
            b = dialog.find_element(By.XPATH, xp)
            if safe_click(driver, b):
                time.sleep(0.2)
                return True
        except Exception:
            pass
    return False

def set_date_in_picker(driver, y: int, m: int, d: int) -> bool:
    target_date = datetime.date(y, m, d)
    target_iso = f"{y:04d}-{m:02d}-{d:02d}"
    sel_any = f"[data-date='{target_iso}']"

    def try_click_target() -> bool:
        dialog = _get_dialog(driver)
        if not dialog:
            return False
        try:
            el = dialog.find_element(By.CSS_SELECTOR, sel_any)
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.05)
            driver.execute_script("arguments[0].click();", el)
            time.sleep(0.12)
            _click_confirm_if_any(driver, dialog)
            return True
        except Exception:
            return False

    if try_click_target():
        return True

    def month_index(dt: datetime.date) -> int:
        return dt.year * 12 + (dt.month - 1)

    for _ in range(50):
        dialog = _get_dialog(driver)
        if not dialog:
            return False
        sc = _get_scroll_container(driver, dialog)
        if not sc:
            return False

        rng = _visible_date_range(dialog)
        if not rng:
            driver.execute_script("arguments[0].scrollTop += 800;", sc)
            time.sleep(0.06)
            if try_click_target():
                return True
            continue

        vmin, vmax = rng
        if vmin <= target_date <= vmax:
            return try_click_target()

        if target_date > vmax:
            diff_m = month_index(target_date) - month_index(vmax)
            jump = min(max(diff_m * 260, 800), 24000)
            driver.execute_script("arguments[0].scrollTop += arguments[1];", sc, jump)
        else:
            diff_m = month_index(vmin) - month_index(target_date)
            jump = min(max(diff_m * 260, 800), 24000)
            driver.execute_script("arguments[0].scrollTop -= arguments[1];", sc, jump)
        time.sleep(0.06)

        if try_click_target():
            return True

    return False

def goto_report_date(driver, y: int, m: int, d: int):
    goto_report_daily(driver)

    cur = get_current_header_date(driver)
    if cur == (y, m, d):
        wait_table_stable_after_date_change(driver)
        return

    if not open_date_picker(driver):
        dump_debug(driver, "open_date_picker_fail")
        raise RuntimeError("[FATAL] 日付ピッカーが開けなかったある")

    if not set_date_in_picker(driver, y, m, d):
        dump_debug(driver, "set_date_in_picker_fail")
        raise RuntimeError("[FATAL] ピッカー内で指定日を選べなかったある（dbg参照）")

    for _ in range(100):
        cur = get_current_header_date(driver)
        if cur == (y, m, d):
            wait_table_stable_after_date_change(driver)
            return
        time.sleep(0.2)

    dump_debug(driver, "goto_report_date_fail")
    raise RuntimeError("[FATAL] 指定日へ移動できなかったある（dbg参照）")


# =========================
# テーブル行検索＆鉛筆クリック
# =========================
def normalize_name(name: str) -> str:
    return str(name).replace(" ", "").replace("　", "").replace("柳", "栁")

def _get_report_scroll_container(driver):
    """
    table を含む一番近いスクロール可能親を取る
    """
    script = r"""
    const table = document.querySelector("table");
    if (!table) return null;

    let el = table.parentElement;
    while (el) {
        const st = getComputedStyle(el);
        const oy = st.overflowY;
        if ((oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight) {
            return el;
        }
        el = el.parentElement;
    }

    return document.scrollingElement || document.documentElement || document.body;
    """
    return driver.execute_script(script)


from selenium.webdriver.common.action_chains import ActionChains

def find_row_by_name(driver, name: str):
    target = normalize_name(name)

    def scan_rows():
        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
        visible = []
        for r in rows:
            try:
                txt = normalize_name((r.text or "").replace("\n", " "))
                visible.append(txt)
                if target in txt:
                    return r, visible
            except Exception:
                continue
        return None, visible

    row, visible = scan_rows()

    log(f"[DEBUG] 探索対象名(normalized): {target}")

    # 先頭10件
    for i, txt in enumerate(visible[:10], 1):
        log(f"[DEBUG] visible row {i}: {txt}")

    # 末尾5件（10件以上あるときだけ）
    if len(visible) > 10:
        for i, txt in enumerate(visible[-5:], 1):
            log(f"[DEBUG] tail row {i}: {txt}")

    if row is not None:
        return row

    # 表の中央あたりを基点にホイールスクロール
    try:
        table = driver.find_element(By.CSS_SELECTOR, "table")
    except Exception:
        log("[DEBUG] table not found")
        return None

    last_sig = ""
    stuck = 0

    for step in range(30):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", table)
            time.sleep(0.2)
        except Exception:
            pass

        # Selenium 4 の wheel アクション
        try:
            ActionChains(driver).move_to_element(table).pause(0.1).scroll_by_amount(0, 500).perform()
        except Exception:
            # 保険：JSスクロール
            try:
                driver.execute_script("window.scrollBy(0, 500);")
            except Exception:
                pass

        time.sleep(0.8)

        row, visible = scan_rows()

        sig = " | ".join(visible)
        log(f"[DEBUG] step={step+1} rows={len(visible)}")
        for i, txt in enumerate(visible[:10], 1):
            log(f"[DEBUG] visible row {i}: {txt}")

        if row is not None:
            return row

        if sig == last_sig:
            stuck += 1
        else:
            stuck = 0
            last_sig = sig

        if stuck >= 3:
            log("[DEBUG] scrolling appears stuck")
            break

    return None
def click_pencil_in_row(driver, row) -> bool:
    try:
        tds = row.find_elements(By.TAG_NAME, "td")
        if tds:
            last = tds[-1]
            for b in last.find_elements(By.TAG_NAME, "button"):
                if safe_click(driver, b):
                    return True
            for svg in last.find_elements(By.TAG_NAME, "svg"):
                if safe_click(driver, svg):
                    return True
    except Exception:
        pass
    return False


# =========================
# 入力：強制クリア＆JS注入
# =========================
def _js_set_value_and_fire(driver, el, value: str):
    driver.execute_script("""
        const el = arguments[0];
        const v  = arguments[1];

        el.focus();
        const proto = Object.getPrototypeOf(el);
        const desc = Object.getOwnPropertyDescriptor(proto, 'value');
        if (desc && desc.set) desc.set.call(el, v);
        else el.value = v;

        el.dispatchEvent(new Event('input',  {bubbles:true}));
        el.dispatchEvent(new Event('change', {bubbles:true}));
        el.dispatchEvent(new Event('blur',   {bubbles:true}));
    """, el, value)

def _clear_input_strong(driver, el):
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    except Exception:
        pass
    safe_click(driver, el)
    time.sleep(0.05)

    try:
        el.send_keys(Keys.CONTROL, "a")
        el.send_keys(Keys.DELETE)
    except Exception:
        pass
    try:
        for _ in range(10):
            el.send_keys(Keys.BACKSPACE)
    except Exception:
        pass
    try:
        _js_set_value_and_fire(driver, el, "")
    except Exception:
        pass

def set_input_value(driver, el, value: str):
    value = "" if value is None else str(value)
    _clear_input_strong(driver, el)
    time.sleep(0.05)
    try:
        el.send_keys(value)
        el.send_keys(Keys.TAB)
    except Exception:
        pass
    try:
        _js_set_value_and_fire(driver, el, value)
    except Exception:
        pass


# =========================
# ドロップダウン選択（強化版）
# =========================
def _text_norm(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "")).strip()

def _find_field_container_by_label(root, label_text: str):
    """
    ラベルの for 属性から、対応するフィールド本体を安定取得する。
    jss*** のような毎回変わる class 名には依存しない。
    """
    label_text = (label_text or "").strip()
    if not label_text:
        return None

    # まずラベル本文で候補を探す
    xps = [
        f".//label[normalize-space(.)='{label_text}']",
        f".//label[contains(normalize-space(.), '{label_text}')]",
        f".//*[self::label or @for][contains(normalize-space(.), '{label_text}')]",
    ]

    labels = []
    for xp in xps:
        try:
            labels.extend(root.find_elements(By.XPATH, xp))
        except Exception:
            pass

    for lab in labels:
        try:
            if not lab.is_displayed():
                continue
        except Exception:
            pass

        # もっとも安定：for="initial.status" → id="select-initial.status"
        try:
            target_for = (lab.get_attribute("for") or "").strip()
            if target_for:
                try:
                    el = root.find_element(By.ID, f"select-{target_for}")
                    if el.is_displayed():
                        return el
                except Exception:
                    pass
                try:
                    el = root.find_element(By.ID, target_for)
                    if el.is_displayed():
                        return el
                except Exception:
                    pass
        except Exception:
            pass

        # 保険：近い親の中の role=button を取る
        cur = lab
        for _ in range(6):
            try:
                cur = cur.find_element(By.XPATH, "..")
            except Exception:
                break

            try:
                btns = cur.find_elements(By.XPATH, ".//*[@role='button' and @aria-haspopup='true']")
            except Exception:
                btns = []

            for b in btns:
                try:
                    if b.is_displayed():
                        return b
                except Exception:
                    pass

    return None

def _open_dropdown(driver, container) -> bool:
    """
    select本体（role=button）を確実に開く。
    """
    candidates = []

    try:
        if container.get_attribute("role") == "button":
            candidates.append(container)
    except Exception:
        pass

    xps = [
        ".//*[@role='button' and @aria-haspopup='true']",
        ".//*[@role='button']",
        ".//*[@aria-haspopup='true']",
        ".//*[@aria-haspopup='listbox']",
        ".//div",
    ]

    for xp in xps:
        try:
            candidates.extend(container.find_elements(By.XPATH, xp))
        except Exception:
            pass

    seen = set()
    uniq = []
    for el in candidates:
        try:
            key = el.id
        except Exception:
            key = None
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        uniq.append(el)

    for el in uniq[:30]:
        try:
            if not el.is_displayed():
                continue
        except Exception:
            pass

        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.05)
        except Exception:
            pass

        if safe_click(driver, el):
            time.sleep(0.3)
            # メニューが出たか確認
            try:
                if (
                    driver.find_elements(By.XPATH, "//ul[@role='listbox']") or
                    driver.find_elements(By.XPATH, "//div[@role='listbox']") or
                    driver.find_elements(By.XPATH, "//li[@role='option']") or
                    driver.find_elements(By.XPATH, "//body//li")
                ):
                    return True
            except Exception:
                pass

    return False

def _get_open_menu(driver):
    """
    開いている Material-UI のメニュー本体を拾う
    """
    xps = [
        "//ul[@role='listbox']",
        "//div[@role='listbox']",
        "//ul[contains(@class,'MuiMenu-list')]",
        "//div[contains(@class,'MuiPopover-root')]",
        "//div[contains(@class,'MuiDialog-root')]//ul",
        "//body",
    ]

    found = []
    for xp in xps:
        try:
            found.extend(driver.find_elements(By.XPATH, xp))
        except Exception:
            pass

    for el in reversed(found):
        try:
            if el.is_displayed():
                return el
        except Exception:
            continue

    return None

def _choose_option_from_open_menu(driver, value_text: str) -> bool:
    """
    開いているメニューから表示文字で選ぶ
    """
    value_text = (value_text or "").strip()
    if not value_text:
        return False

    menu = _get_open_menu(driver)
    if not menu:
        return False

    option_xps = [
        f".//li[normalize-space(.)='{value_text}']",
        f".//li[contains(normalize-space(.), '{value_text}')]",
        f".//*[@role='option'][normalize-space(.)='{value_text}']",
        f".//*[@role='option'][contains(normalize-space(.), '{value_text}')]",
        f".//*[self::span or self::div][normalize-space(.)='{value_text}']",
        f".//*[self::span or self::div][contains(normalize-space(.), '{value_text}')]",
    ]

    for xp in option_xps:
        try:
            elems = menu.find_elements(By.XPATH, xp)
        except Exception:
            elems = []

        for el in elems[:50]:
            try:
                if not el.is_displayed():
                    continue
            except Exception:
                pass

            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.05)
            except Exception:
                pass

            if safe_click(driver, el):
                time.sleep(0.2)
                return True

            try:
                parent_li = el.find_element(By.XPATH, "./ancestor::li[1]")
                if safe_click(driver, parent_li):
                    time.sleep(0.2)
                    return True
            except Exception:
                pass

    return False

def _norm_name_for_match(s: str) -> str:
    s = norm(s)
    s = s.replace("　", "").replace(" ", "")
    return s


def select_dropdown_skip_if_same(driver, root, label_text: str, value_text: str) -> bool:
    """
    jss番号ではなく、label for -> select-initial.xxx の流れで安定選択する。
    """
    value_text = _text_norm(value_text)
    if not value_text:
        return False

    cont = _find_field_container_by_label(root, label_text)
    if not cont:
        return False

    # 現在値確認
    try:
        now = _text_norm(cont.text or "")
        if value_text == now:
            return True
    except Exception:
        pass

    if not _open_dropdown(driver, cont):
        return False

    if _choose_option_from_open_menu(driver, value_text):
        return True

    # 保険：ESCして再試行
    try:
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(0.2)
    except Exception:
        pass

    if not _open_dropdown(driver, cont):
        return False

    return _choose_option_from_open_menu(driver, value_text)


# =========================
# フィールド探索（時間・備考）
# =========================
def _find_time_input(root, kind: str):
    """
    上側の
      開始時間 / 終了時間
    だけを拾う。

    下側の
      作業開始時間 / 作業終了時間
    は絶対に拾わない。
    """
    if kind == "start":
        target_labels = ["開始時間", "利用開始時間", "サービス開始時間"]
        ng_words = ["作業開始時間"]
    else:
        target_labels = ["終了時間", "利用終了時間", "サービス終了時間"]
        ng_words = ["作業終了時間"]

    # まず「作業開始時間」「作業終了時間」を含む領域を除外しつつ、
    # 上側ラベルに最も近い input を拾う
    for label_text in target_labels:
        try:
            labels = root.find_elements(
                By.XPATH,
                f".//*[normalize-space(text())='{label_text}' or contains(normalize-space(.), '{label_text}')]"
            )
        except Exception:
            labels = []

        for lab in labels:
            try:
                txt = (lab.text or "").strip()
                if not txt:
                    continue
                if any(ng in txt for ng in ng_words):
                    continue
                if "作業" in txt:
                    continue
            except Exception:
                continue

            # 近い親要素まで上がって、そのブロック内の input を優先
            cur = lab
            for _ in range(6):
                try:
                    cur = cur.find_element(By.XPATH, "..")
                except Exception:
                    break

                try:
                    area_text = (cur.text or "").strip()
                except Exception:
                    area_text = ""

                # 「作業開始時間」「作業終了時間」を含むブロックは除外
                if "作業開始時間" in area_text or "作業終了時間" in area_text:
                    continue

                try:
                    inputs = cur.find_elements(By.XPATH, ".//input")
                except Exception:
                    inputs = []

                visible_inputs = []
                for inp in inputs:
                    try:
                        if inp.is_displayed():
                            visible_inputs.append(inp)
                    except Exception:
                        pass

                if len(visible_inputs) == 1:
                    return visible_inputs[0]

                if len(visible_inputs) >= 2:
                    # 念のため、開始は左寄り、終了は右寄りを取る
                    try:
                        visible_inputs = sorted(visible_inputs, key=lambda e: e.location["x"])
                    except Exception:
                        pass
                    return visible_inputs[0] if kind == "start" else visible_inputs[-1]

            # ラベル直後の input を拾う保険
            try:
                cand = lab.find_element(
                    By.XPATH,
                    "./following::input[not(ancestor::*[contains(., '作業開始時間') or contains(., '作業終了時間')])][1]"
                )
                if cand and cand.is_displayed():
                    return cand
            except Exception:
                pass

    # 最後の保険：
    # 画面内の input を総当たりして、上側2つだけを選ぶ
    try:
        all_inputs = root.find_elements(By.XPATH, ".//input")
    except Exception:
        all_inputs = []

    top_candidates = []
    for inp in all_inputs:
        try:
            if not inp.is_displayed():
                continue
        except Exception:
            continue

        try:
            # 近くの親ブロックに「作業開始時間/作業終了時間」があれば除外
            parent = inp.find_element(By.XPATH, "./ancestor::*[self::div or self::td][1]")
            ptxt = (parent.text or "").strip()
            if "作業開始時間" in ptxt or "作業終了時間" in ptxt:
                continue
        except Exception:
            pass

        try:
            x = inp.location["x"]
            y = inp.location["y"]
        except Exception:
            x, y = 0, 0

        top_candidates.append((y, x, inp))

    if top_candidates:
        top_candidates.sort(key=lambda t: (t[0], t[1]))
        # 上から見て最初の2つが上段の開始/終了である前提
        first_two = [t[2] for t in top_candidates[:2]]
        if len(first_two) == 1:
            return first_two[0]
        if len(first_two) >= 2:
            try:
                first_two = sorted(first_two, key=lambda e: e.location["x"])
            except Exception:
                pass
            return first_two[0] if kind == "start" else first_two[-1]

    return None

def _find_remark_area(root):
    for ta in root.find_elements(By.TAG_NAME, "textarea"):
        try:
            if ta.is_displayed():
                return ta
        except Exception:
            continue
    # inputしか無い場合の保険
    try:
        node = root.find_element(By.XPATH, ".//*[contains(normalize-space(.),'備考')]")
        cand = node.find_element(By.XPATH, ".//following::textarea[1] | .//following::input[1]")
        if cand and cand.is_displayed():
            return cand
    except Exception:
        pass
    return None


def process_report_edit(driver, it: PersonItem) -> bool:
    s = (it.service or "").strip()
    if s not in ("通所", "施設外就労"):
        log(f"⏭️ 対象外なのでスキップ（編集しない）: {it.name} / B列={s!r}")
        return True

    close_dialog_if_open(driver)

    row = find_row_by_name(driver, it.name)
    if row is None:
        log(f"⚠️ {it.name} 行が見つからないある")
        return False

    if not click_pencil_in_row(driver, row):
        log(f"⚠️ {it.name} 編集(鉛筆)ボタンが押せないある")
        return False

    try:
        WebDriverWait(driver, 10).until(lambda d: get_top_dialog(d) is not None)
    except Exception:
        log(f"⚠️ {it.name} モーダルが開かないある")
        return False

    dlg = get_top_dialog(driver)
    if not dlg:
        return False

    try:
        # B: サービス提供の状況
        if it.service:
            ok = select_dropdown_skip_if_same(driver, dlg, "サービス提供の状況", it.service)
            if not ok:
                ok = select_dropdown_skip_if_same(driver, dlg, "サービス提供の状況 *", it.service)
            if not ok:
                ok = select_dropdown_skip_if_same(driver, dlg, "サービス提供", it.service)
            if not ok:
                dump_debug(driver, f"service_dropdown_fail_{it.name}")
                log(f"⚠️ {it.name} サービス提供の選択に失敗ある → dbg参照")
                close_dialog_if_open(driver)
                return False

        # E: 食事提供（後で時間分岐にも使うので先に確定）
        meal = (it.meal or "").strip()
        if not meal:
            log(f"⚠️ {it.name} 食事提供(E列)が空欄ある → 条約違反防止で保存せず閉じるある")
            close_dialog_if_open(driver)
            return False

        # C/D: 上側の開始/終了のみ
        # Excelで時間未入力なら、時間欄は触らずにスキップする
        has_start = bool((it.start or "").strip())
        has_end = bool((it.end or "").strip())

        if has_start or has_end:
            inp_start = _find_time_input(dlg, "start")
            inp_end = _find_time_input(dlg, "end")

            if not inp_start or not inp_end:
                dump_debug(driver, f"time_input_not_found_{it.name}")
                log(f"⚠️ {it.name} 上側の開始/終了inputが拾えないある → dbg参照")
                close_dialog_if_open(driver)
                return False

            # 施設外就労 と 通所(食事提供あり) は時間をずらさない
            no_jitter = (s == "施設外就労") or (s == "通所" and meal == "提供あり")

            if has_start:
                start_t = it.start
                set_input_value(driver, inp_start, start_t)

            if has_end:
                end_t = it.end
                set_input_value(driver, inp_end, end_t)

        # E: 食事提供
        ok = select_dropdown_skip_if_same(driver, dlg, "食事提供", meal)
        if not ok:
            dump_debug(driver, f"meal_dropdown_fail_{it.name}")
            log(f"⚠️ {it.name} 食事提供の選択に失敗ある → dbg参照")
            close_dialog_if_open(driver)
            return False

        # F: 備考
        note_src = (it.note or "").strip()
        if s == "施設外就労":
            final_note = "施設外就労(実施報告書等添付)"
        else:
            if meal == "提供なし":
                final_note = "在宅利用"
            else:
                final_note = note_src

        if final_note:
            area = _find_remark_area(dlg)
            if area:
                set_input_value(driver, area, final_note)
            else:
                dump_debug(driver, f"remark_not_found_{it.name}")
                log(f"⚠️ {it.name} 備考欄が見つからないある → dbg参照")
                close_dialog_if_open(driver)
                return False

        # 保存
        save_btn = None
        for xp in [
            ".//button[contains(.,'保存する')]",
            ".//button[contains(.,'保存')]",
            ".//button[contains(.,'登録')]",
            ".//button[contains(.,'更新')]",
        ]:
            try:
                save_btn = dlg.find_element(By.XPATH, xp)
                break
            except Exception:
                continue

        if not save_btn:
            dump_debug(driver, f"save_btn_not_found_{it.name}")
            log(f"⚠️ {it.name} 保存ボタンが見つからないある → dbg参照")
            close_dialog_if_open(driver)
            return False

        for _ in range(25):
            disabled = save_btn.get_attribute("disabled")
            aria_disabled = (save_btn.get_attribute("aria-disabled") or "").lower()
            if disabled is None and aria_disabled != "true":
                break
            time.sleep(0.12)

        if not safe_click(driver, save_btn):
            dump_debug(driver, f"save_click_fail_{it.name}")
            log(f"⚠️ {it.name} 保存ボタンが押せないある → dbg参照")
            close_dialog_if_open(driver)
            return False

        try:
            WebDriverWait(driver, 15).until(EC.invisibility_of_element(dlg))
        except Exception:
            close_dialog_if_open(driver)

        return True

    except Exception as e:
        dump_debug(driver, f"exception_{it.name}")
        log(f"⚠️ {it.name} 例外ある: {e} → モーダル閉じて次へ")
        close_dialog_if_open(driver)
        return False

# =========================
# 日々の記録 + Gemini
# =========================
def _daily_record_category(it: PersonItem) -> str:
    """
    スタッフ例文シート用のカテゴリ
      在宅 / 通所 / 施設外
    """
    s = (it.service or "").strip()
    meal = (it.meal or "").strip()

    if s == "施設外就労":
        return "施設外"
    if s == "通所" and meal == "提供なし":
        return "在宅"
    return "通所"


def _daily_record_work_label(it: PersonItem) -> str:
    """
    日々の記録ページの「作業」欄
      通所（提供あり/なし）→ 内職
      施設外就労           → 清掃
    """
    s = (it.service or "").strip()
    if s == "施設外就労":
        return "清掃"
    return "内職"


def _read_treaty_and_staff_examples(excel_path: str):
    """
    条約シート全文と、スタッフ例文シートを読む
    戻り値:
      treaty_text: str
      examples: {
        "スタッフ名": {
          "在宅":   {"利用者状態": "...", "職員考察": "..."},
          "通所":   {"利用者状態": "...", "職員考察": "..."},
          "施設外": {"利用者状態": "...", "職員考察": "..."},
        }
      }
    """
    staged = stage_excel_local(excel_path)
    wb = openpyxl.load_workbook(staged, data_only=True)

    # 条約
    treaty_text = ""
    if "条約" in wb.sheetnames:
        ws = wb["条約"]
        chunks = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            a = norm(row[0] if len(row) > 0 else "")
            b = norm(row[1] if len(row) > 1 else "")
            c = norm(row[2] if len(row) > 2 else "")
            if a or b or c:
                chunks.append(f"{a} | {b} | {c}")
        treaty_text = "\n".join(chunks)

    # スタッフ例文
    examples = {}
    if "スタッフ例文" in wb.sheetnames:
        ws = wb["スタッフ例文"]
        row = 3
        while True:
            staff_name = norm(ws[f"A{row}"].value)
            if not staff_name:
                break

            examples[staff_name] = {
                "在宅": {
                    "利用者状態": norm(ws[f"B{row}"].value),
                    "職員考察": norm(ws[f"C{row}"].value),
                },
                "通所": {
                    "利用者状態": norm(ws[f"D{row}"].value),
                    "職員考察": norm(ws[f"E{row}"].value),
                },
                "施設外": {
                    "利用者状態": norm(ws[f"F{row}"].value),
                    "職員考察": norm(ws[f"G{row}"].value),
                },
            }
            row += 1

    wb.close()
    return treaty_text, examples


def _choose_daily_recorder(excel_path: str) -> str:
    """
    基本情報シートの「スタッフ名 / 日誌を書く人を[〇]」表から、
    〇 の付いたスタッフを1人選ぶ（今回は1日分固定）
    """
    staged = stage_excel_local(excel_path)
    wb = openpyxl.load_workbook(staged, data_only=True)

    if SHEET_MAIN not in wb.sheetnames:
        wb.close()
        return ""

    ws = wb[SHEET_MAIN]

    # K列=スタッフ名, L列=〇/空欄 の前提
    candidates = []
    row = 3
    while True:
        staff_name = norm(ws[f"K{row}"].value)
        flag = norm(ws[f"L{row}"].value)

        # KもLも空なら終端
        if not staff_name and not flag:
            break

        if staff_name and flag == "〇":
            candidates.append(staff_name)

        row += 1

    wb.close()

    if not candidates:
        return ""

    # 今回は1日分固定なので最初の1人を採用
    return candidates[0]

def _get_style_examples_for_staff(examples: dict, staff_name: str, category: str):
    """
    category: 在宅 / 通所 / 施設外
    """
    x = examples.get(staff_name, {})
    c = x.get(category, {})
    return c.get("利用者状態", ""), c.get("職員考察", "")

def _replace_placeholder_name(text: str, full_name: str) -> str:
    """
    スタッフ例文中の「〇〇さん」を利用者の苗字に置き換える
    例:
      荒木 和也 → 荒木さん
    """
    text = norm(text)
    full_name = norm(full_name)

    if not text or not full_name:
        return text

    surname = re.split(r"[ 　]+", full_name)[0].strip()
    if not surname:
        return text

    return text.replace("〇〇さん", f"{surname}さん")


def _build_gemini_prompt(
    field_kind: str,           # "利用者状態" or "職員考察"
    base_memo: str,            # H列 or I列
    category: str,             # 在宅 / 通所 / 施設外
    staff_name: str,
    style_example: str,
    treaty_text: str
) -> str:
    """
    Geminiへ渡す最終プロンプト
    """
    return f"""あなたは就労継続支援B型の支援記録作成アシスタントです。
以下の「条約」を絶対遵守して、{field_kind}欄に入れる日本語文を1段落で作成してください。

【条約】
{treaty_text}

【今回の欄】
{field_kind}

【利用形態】
{category}

【参照するスタッフ文体】
スタッフ名: {staff_name}

【文体参考例】
{style_example}

【元メモ】
{base_memo}

【厳守事項】

- 日誌を書く人と電話を受けた人は同一人物のため、
  「〜と伺った」「〜と聞いた」などの二重伝聞は禁止。
  「連絡があった」「話していた」「報告があった」などの直接表現を使う。
- 在宅利用の場合のみ、体調や生活状況の説明部分は
  「〜とのことです」「〜と話していた」などの伝聞調を使用してよい。
- 通所・施設外就労の場合は、実際に見た文体で書く。
- 出力は本文のみ。見出し、箇条書き、引用符、注釈は不要。
- 事実を歪めず、元メモの内容を必ず盛り込む。
- 不適切表現、子ども扱い表現は禁止。
- 支援記録として自然で丁寧な文章にする。
- 3文程度でまとめること。
- 100〜150文字程度を目安にすること。
- 長すぎず短すぎず、Knowbeへそのまま貼れる長さにする。
- スタッフ例文に「〇〇さん」とある場合は
  そこに利用者の苗字を入れる。
"""


def _gemini_generate_text(client, prompt: str) -> str:
    """
    Geminiで本文生成
    """
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )
    text = getattr(response, "text", "") or ""
    return text.strip()


def click_daily_edit_button(driver) -> bool:
    """
    右上の「編集」ボタンを押す
    """
    try:
        btn = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//button[.//span[contains(normalize-space(.),'編集')] or contains(normalize-space(.),'編集')]"
            ))
        )
    except Exception:
        dump_debug(driver, "click_daily_edit_button_not_found")
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.1)
    except Exception:
        pass

    if not safe_click(driver, btn):
        dump_debug(driver, "click_daily_edit_button_click_fail")
        return False

    # 編集後に「保存する」が見えたら成功
    t0 = time.time()
    while time.time() - t0 < 10:
        try:
            saves = driver.find_elements(
                By.XPATH,
                "//button[.//span[contains(normalize-space(.),'保存する')] or contains(normalize-space(.),'保存する')]"
            )
            for s in saves:
                try:
                    if s.is_displayed():
                        time.sleep(0.5)
                        return True
                except Exception:
                    pass
        except Exception:
            pass
        time.sleep(0.3)

    dump_debug(driver, "click_daily_edit_button_after_click_fail")
    return False
    
def _wait_daily_save_complete(driver, timeout=15) -> bool:
    """
    日々の記録の保存完了待ち
    どれか1つでも満たせば成功とみなす
      - 「編集」ボタンが再表示される
      - 「保存する」ボタンが消える
      - DOM上で編集状態っぽい要素が減る
    """
    t0 = time.time()
    while time.time() - t0 < timeout:
        # 編集ボタンが戻ってきたら保存完了扱い
        try:
            edits = driver.find_elements(
                By.XPATH,
                "//button[.//span[contains(normalize-space(.),'編集')] or contains(normalize-space(.),'編集')]"
            )
            for b in edits:
                try:
                    if b.is_displayed():
                        return True
                except Exception:
                    pass
        except Exception:
            pass

        # 保存するボタンが見えなくなっても完了扱い
        try:
            saves = driver.find_elements(
                By.XPATH,
                "//button[.//span[contains(normalize-space(.),'保存する')] or contains(normalize-space(.),'保存する')]"
            )
            visible = False
            for s in saves:
                try:
                    if s.is_displayed():
                        visible = True
                        break
                except Exception:
                    pass
            if not visible:
                return True
        except Exception:
            pass

        time.sleep(0.4)

    return False

def _find_daily_record_row_by_name(driver, name: str):
    """
    支援記録テーブルの行を、利用者名で探す
    """
    rows = driver.find_elements(By.XPATH, "//tbody/tr")
    for r in rows:
        try:
            txt = (r.text or "").replace("\n", " ")
            if name in txt:
                return r
        except Exception:
            pass
    return None


def _choose_option_from_open_listbox_text(driver, value_text: str) -> bool:
    """
    開いているリストボックス/メニューから、文字で選ぶ
    スタッフ名の全角/半角スペース揺れにも対応
    """
    value_text = norm(value_text)
    if not value_text:
        return False

    target_norm = _norm_name_for_match(value_text)

    # まず見えている候補を総当たり
    xps = [
        "//li[@role='option']",
        "//li",
        "//*[@role='option']",
    ]

    for xp in xps:
        try:
            els = driver.find_elements(By.XPATH, xp)
        except Exception:
            els = []

        for el in els:
            try:
                if not el.is_displayed():
                    continue
            except Exception:
                continue

            try:
                txt = norm(el.text)
            except Exception:
                txt = ""

            if not txt:
                continue

            txt_norm = _norm_name_for_match(txt)

            # 完全一致 or 含有一致
            if txt_norm == target_norm or target_norm in txt_norm:
                if safe_click(driver, el):
                    time.sleep(0.3)
                    return True

    return False

def _set_daily_work_for_row(driver, row, work_label: str) -> bool:
    """
    その行の作業欄を、内職/清掃にする
    """
    if not work_label:
        return True

    # まず現在値確認
    try:
        row_text = row.text or ""
        if work_label in row_text:
            return True
    except Exception:
        pass

    # 「内容」列の最初の role=button を狙う
    btn = None
    xps = [
        ".//*[@role='button' and contains(@id, 'work_history')]",
        ".//*[@role='button']",
    ]
    for xp in xps:
        try:
            els = row.find_elements(By.XPATH, xp)
        except Exception:
            els = []
        for el in els:
            try:
                if not el.is_displayed():
                    continue
            except Exception:
                pass
            btn = el
            break
        if btn:
            break

    if not btn:
        dump_debug(driver, "daily_work_button_not_found")
        return False

    if not safe_click(driver, btn):
        return False
    time.sleep(0.4)

    if not _choose_option_from_open_listbox_text(driver, work_label):
        dump_debug(driver, f"daily_work_option_fail_{work_label}")
        return False

    # メニューを閉じる
    try:
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(0.2)
    except Exception:
        pass
    return True


def _set_daily_textareas_for_row(driver, row, user_status_text: str, staff_comment_text: str) -> bool:
    """
    その行の 利用者状態 / 職員考察 textarea に入力
    """
    try:
        user_area = row.find_element(By.XPATH, ".//textarea[contains(@name,'user_status')]")
        staff_area = row.find_element(By.XPATH, ".//textarea[contains(@name,'staff_comment')]")
    except Exception:
        dump_debug(driver, "daily_textarea_not_found")
        return False

    set_input_value(driver, user_area, user_status_text)
    time.sleep(0.1)
    set_input_value(driver, staff_area, staff_comment_text)
    time.sleep(0.1)
    return True


def _set_daily_recorder_for_row(driver, row, recorder_name: str) -> bool:
    """
    その行の 記録者 select を選ぶ
    """
    if not recorder_name:
        return False

    # すでに入っていればOK
    try:
        row_text = norm(row.text)
        if _norm_name_for_match(recorder_name) in _norm_name_for_match(row_text):
            return True
    except Exception:
        pass

    btn = None

    # staff_id 専用のボタンを最優先
    xps = [
        ".//*[@role='button' and contains(@id, 'staff_id')]",
        ".//*[@role='button' and contains(normalize-space(.), '選択してください')]",
        ".//*[@role='button']",
    ]

    for xp in xps:
        try:
            els = row.find_elements(By.XPATH, xp)
        except Exception:
            els = []

        for el in els:
            try:
                if not el.is_displayed():
                    continue
            except Exception:
                continue

            btn = el
            break

        if btn:
            break

    if not btn:
        dump_debug(driver, "daily_recorder_button_not_found")
        return False

    if not safe_click(driver, btn):
        dump_debug(driver, "daily_recorder_button_click_fail")
        return False

    time.sleep(0.5)

    if not _choose_option_from_open_listbox_text(driver, recorder_name):
        dump_debug(driver, f"daily_recorder_option_fail_{recorder_name}")
        return False

    time.sleep(0.3)
    return True


def click_daily_save_button(driver) -> bool:
    """
    右上の「保存する」ボタンを押して、保存完了まで待つ
    """
    try:
        span = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//span[contains(normalize-space(.),'保存する')]"
            ))
        )
    except Exception:
        dump_debug(driver, "click_daily_save_button_span_not_found")
        return False

    try:
        btn = span.find_element(By.XPATH, "./ancestor::button[1]")
    except Exception:
        dump_debug(driver, "click_daily_save_button_button_not_found")
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        time.sleep(0.1)
    except Exception:
        pass

    # disabled解除待ち
    t0 = time.time()
    while time.time() - t0 < 10:
        try:
            disabled = btn.get_attribute("disabled")
            aria_disabled = (btn.get_attribute("aria-disabled") or "").lower()
            if disabled is None and aria_disabled != "true":
                break
        except Exception:
            pass
        time.sleep(0.2)

    if not safe_click(driver, btn):
        try:
            driver.execute_script("arguments[0].click();", btn)
        except Exception:
            dump_debug(driver, "click_daily_save_button_click_fail")
            return False

    if not _wait_daily_save_complete(driver, timeout=15):
        dump_debug(driver, "click_daily_save_button_wait_fail")
        return False

    time.sleep(0.5)
    return True


def process_one_daily_record(driver, client, treaty_text: str, examples: dict, recorder_name: str, it: PersonItem) -> bool:
    """
    1人分だけ:
      編集 → 対象行 → 作業 → 利用者状態 → 職員考察 → 記録者 → 保存
    """
    if not click_daily_edit_button(driver):
        return False

    row = _find_daily_record_row_by_name(driver, it.name)
    if row is None:
        dump_debug(driver, f"daily_row_not_found_{it.name}")
        log(f"⚠️ 日々の記録で行が見つからないある: {it.name}")
        return False

    category = _daily_record_category(it)
    work_label = _daily_record_work_label(it)

    # スタッフ例文
    user_ex, staff_ex = _get_style_examples_for_staff(examples, recorder_name, category)

    # Gemini 生成
    user_prompt = _build_gemini_prompt(
        field_kind="利用者状態",
        base_memo=it.user_state,
        category=category,
        staff_name=recorder_name,
        style_example=user_ex,
        treaty_text=treaty_text,
    )
    staff_prompt = _build_gemini_prompt(
        field_kind="職員考察",
        base_memo=it.staff_note,
        category=category,
        staff_name=recorder_name,
        style_example=staff_ex,
        treaty_text=treaty_text,
    )

    user_text = _gemini_generate_text(client, user_prompt)
    staff_text = _gemini_generate_text(client, staff_prompt)

    user_text = _replace_placeholder_name(user_text, it.name)
    staff_text = _replace_placeholder_name(staff_text, it.name)
    
    if not _set_daily_work_for_row(driver, row, work_label):
        log(f"⚠️ 作業欄の選択失敗ある: {it.name}")
        return False

    if not _set_daily_textareas_for_row(driver, row, user_text, staff_text):
        log(f"⚠️ 日々の記録 textarea 入力失敗ある: {it.name}")
        return False

    if not _set_daily_recorder_for_row(driver, row, recorder_name):
        log(f"⚠️ 記録者選択失敗ある: {it.name}")
        return False

    if not click_daily_save_button(driver):
        log(f"⚠️ 日々の記録 保存失敗ある: {it.name}")
        return False

    log(f"✅ 日々の記録 保存成功ある: {it.name}")
    return True

def open_daily_record_page(driver, y: int, m: int, d: int) -> bool:
    """
    左メニューの「日々の記録」へ移動し、対象日ページを開く
    """
    try:
        btn = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//p[contains(normalize-space(.),'日々の記録')]")
            )
        )
        safe_click(driver, btn)
        time.sleep(1.0)
    except Exception:
        pass

    target_url = f"https://mgr.knowbe.jp/v2/?_page=record/daily/#/record/daily/{y:04d}{m:02d}{d:02d}"
    driver.get(target_url)
    time.sleep(1.5)

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.XPATH, "//*[contains(normalize-space(.),'支援記録')]")
            )
        )
        return True
    except Exception:
        dump_debug(driver, "open_daily_record_page_fail")
        return False


def run_daily_records(driver, excel_path: str, items: List[PersonItem], targets: List[PersonItem], y: int, m: int, d: int):
    """
    利用実績入力のあとに呼ぶ本体
    """
    if not USE_GEMINI or not GEMINI_API_KEY:
        raise RuntimeError("[FATAL] Gemini APIキーが無いある")

    client = genai.Client(api_key=GEMINI_API_KEY)

    treaty_text, examples = _read_treaty_and_staff_examples(excel_path)
    recorder_name = _choose_daily_recorder(excel_path)

    if not recorder_name:
        raise RuntimeError("[FATAL] 日々の記録の記録者を決められないある")

    log(f"📝 日々の記録の記録者: {recorder_name}")

    if not open_daily_record_page(driver, y, m, d):
        raise RuntimeError("[FATAL] 日々の記録ページへ行けないある")

    for it in targets:
        s = norm(it.service)
        if s not in ("通所", "施設外就労"):
            continue

        log(f"🧾 日々の記録入力: {it.name}")
        ok = process_one_daily_record(
            driver=driver,
            client=client,
            treaty_text=treaty_text,
            examples=examples,
            recorder_name=recorder_name,
            it=it,
        )
        if not ok:
            dump_debug(driver, f"daily_record_fail_{it.name}")
            log(f"[WARN] 日々の記録失敗→次へ: {it.name}")

def _normalize_service_for_app(service_type: str, knowbe_target: str) -> str:
    s = norm(service_type)
    k = norm(knowbe_target)

    if s in ("通所", "施設外就労"):
        return s

    if k in ("facility_outside", "outside", "施設外", "施設外就労"):
        return "施設外就労"

    return "通所"


def _normalize_meal_for_app(meal_flag: str) -> str:
    m = norm(meal_flag)
    if m in ("あり", "提供あり", "有"):
        return "提供あり"
    return "提供なし"


def _build_single_item_from_app(
    resident_name: str,
    service_type: str,
    start_time: str,
    end_time: str,
    meal_flag: str,
    note_text: str,
    generated_status: str,
    generated_support: str,
    staff_name: str,
    knowbe_target: str
) -> PersonItem:
    return PersonItem(
        name=norm(resident_name),
        service=_normalize_service_for_app(service_type, knowbe_target),
        start=normalize_hhmm(start_time),
        end=normalize_hhmm(end_time),
        meal=_normalize_meal_for_app(meal_flag),
        note=norm(note_text),
        user_state=norm(generated_status),
        staff_note=norm(generated_support),
        staff_name=norm(staff_name),
        staff_mark="〇",
    )


def process_one_daily_record_direct(
    driver,
    it: PersonItem,
    recorder_name: str,
    user_text: str,
    staff_text: str
) -> bool:
    """
    app用：Gemini生成済みの本文をそのまま1人分だけ送る
    """
    if not click_daily_edit_button(driver):
        return False

    row = _find_daily_record_row_by_name(driver, it.name)
    if row is None:
        dump_debug(driver, f"daily_row_not_found_{it.name}")
        log(f"⚠️ 日々の記録で行が見つからないある: {it.name}")
        return False

    work_label = _daily_record_work_label(it)

    if not _set_daily_work_for_row(driver, row, work_label):
        log(f"⚠️ 作業欄の選択失敗ある: {it.name}")
        return False

    if not _set_daily_textareas_for_row(driver, row, user_text, staff_text):
        log(f"⚠️ 日々の記録 textarea 入力失敗ある: {it.name}")
        return False

    if not _set_daily_recorder_for_row(driver, row, recorder_name):
        log(f"⚠️ 記録者選択失敗ある: {it.name}")
        return False

    if not click_daily_save_button(driver):
        log(f"⚠️ 日々の記録 保存失敗ある: {it.name}")
        return False

    log(f"✅ 日々の記録 保存成功ある: {it.name}")
    return True



def send_one_record_from_app(
    target_date,
    resident_name,
    service_type,
    start_time,
    end_time,
    meal_flag,
    note_text,
    generated_status,
    generated_support,
    staff_name,
    knowbe_target,
    login_username,
    login_password,
):
    """
    appから1件だけ渡されたデータを Knowbe に送る
    """
    if not target_date:
        raise RuntimeError("[FATAL] target_date が空ある")
    if not resident_name:
        raise RuntimeError("[FATAL] resident_name が空ある")
    if not start_time:
        raise RuntimeError("[FATAL] start_time が空ある")
    if not end_time:
        raise RuntimeError("[FATAL] end_time が空ある")
    if not staff_name:
        raise RuntimeError("[FATAL] staff_name が空ある")

    m = re.match(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$", str(target_date))
    if not m:
        raise RuntimeError(f"[FATAL] target_date形式が不正ある: {target_date}")

    y = int(m.group(1))
    mo = int(m.group(2))
    d = int(m.group(3))

    it = _build_single_item_from_app(
        resident_name=resident_name,
        service_type=service_type,
        start_time=start_time,
        end_time=end_time,
        meal_flag=meal_flag,
        note_text=note_text,
        generated_status=generated_status,
        generated_support=generated_support,
        staff_name=staff_name,
        knowbe_target=knowbe_target,
    )

    print("[STEP] build_chrome_driver start", flush=True)
    driver = build_chrome_driver()
    print("[STEP] driver created", flush=True)

    try:
        print("[STEP] goto_report_daily start", flush=True)
        goto_report_daily(driver)
        print("[STEP] goto_report_daily done", flush=True)

        print("[STEP] manual_login_wait start", flush=True)
        manual_login_wait(driver, login_username, login_password)
        print("[STEP] manual_login_wait done", flush=True)

        print("[STEP] goto_report_date start", flush=True)
        goto_report_date(driver, y, mo, d)
        print("[STEP] goto_report_date done", flush=True)

        log(f"🏃 app単発 実績処理: {it.name}")
        ok = process_report_edit(driver, it)
        if not ok:
            raise RuntimeError(f"[FATAL] 利用実績の入力失敗ある: {it.name}")

        print("[STEP] open_daily_record_page start", flush=True)
        if not open_daily_record_page(driver, y, mo, d):
            raise RuntimeError("[FATAL] 日々の記録ページへ行けないある")
        print("[STEP] open_daily_record_page done", flush=True)

        log(f"🧾 app単発 日々の記録入力: {it.name}")
        ok = process_one_daily_record_direct(
            driver=driver,
            it=it,
            recorder_name=staff_name,
            user_text=generated_status,
            staff_text=generated_support,
        )
        if not ok:
            raise RuntimeError(f"[FATAL] 日々の記録の入力失敗ある: {it.name}")

        log("🎊 app単発送信 完了ある！")
        return True

    finally:
        try:
            driver.quit()
        except Exception:
            pass


def main():
    # =========================================
    # app単発モード
    # =========================================
    if os.environ.get("KB_SINGLE_MODE", "") == "1":
        login_username, login_password = get_knowbe_login_credentials()

        if not login_username or not login_password:
            raise RuntimeError("[FATAL] KB_LOGIN_USERNAME / KB_LOGIN_PASSWORD が空ある")

        send_one_record_from_app(
            target_date=os.environ.get("KB_TARGET_DATE", ""),
            resident_name=os.environ.get("KB_RESIDENT_NAME", ""),
            service_type=os.environ.get("KB_SERVICE_TYPE", ""),
            start_time=os.environ.get("KB_START_TIME", ""),
            end_time=os.environ.get("KB_END_TIME", ""),
            meal_flag=os.environ.get("KB_MEAL_FLAG", ""),
            note_text=os.environ.get("KB_NOTE_TEXT", ""),
            generated_status=os.environ.get("KB_GENERATED_STATUS", ""),
            generated_support=os.environ.get("KB_GENERATED_SUPPORT", ""),
            staff_name=os.environ.get("KB_STAFF_NAME", ""),
            knowbe_target=os.environ.get("KB_KNOWBE_TARGET", ""),
            login_username=login_username,
            login_password=login_password,
        )
        return

    # =========================================
    # 旧：Excel一括モード
    # =========================================
    excel_path = os.environ.get("EXCEL_PATH", EXCEL_PATH_DEFAULT)
    if not os.path.exists(excel_path):
        raise RuntimeError(f"[FATAL] Excelが見つからないある: {excel_path}")

    log("エクセルを熟読中ある...")
    y, m, d, items = read_sheet_data(excel_path)

    targets = [
        it for it in items
        if (it.service or "").strip() in ("通所", "施設外就労")
        and (it.start or "").strip()
        and (it.end or "").strip()
    ]
    log(f"🚀 {len(targets)}名の処理を開始するある！ ターゲット: {y}/{m}/{d}")

    read_treaty_check(excel_path)

    driver = build_chrome_driver()

    try:
        login_username, login_password = get_knowbe_login_credentials()

        if not login_username or not login_password:
            raise RuntimeError("[FATAL] KB_LOGIN_USERNAME / KB_LOGIN_PASSWORD が空ある")

        goto_report_daily(driver)
        manual_login_wait(driver, login_username, login_password)
        goto_report_date(driver, y, m, d)

        any_ok = False
        for it in targets:
            read_treaty_check(excel_path)
            log(f"🏃 {it.name} 実績処理")
            ok = process_report_edit(driver, it)
            if not ok:
                log(f"[WARN] 実績入力失敗→次へ: {it.name}")
            else:
                any_ok = True

        if not any_ok:
            log("⚠️ 実績が1件も成功してないある（dbgを確認してくれある）")
            dump_debug(driver, "no_success_report")

        if any_ok:
            run_daily_records(driver, excel_path, items, targets, y, m, d)

        log("🎊 全行程完了ある！")

    finally:
        try:
            driver.quit()
        except Exception:
            pass
