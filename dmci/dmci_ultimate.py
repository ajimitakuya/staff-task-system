import os
import re
import json
import time
import shutil
import traceback
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

import pdfplumber
import pandas as pd
import gspread

from openpyxl import load_workbook
from selenium import webdriver
from selenium.webdriver import ChromeOptions, EdgeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


# =========================================================
# 設定読込
# =========================================================
CONFIG_PATH = Path("config_local.json")


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError("config_local.json が見つからないある")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


CFG = load_config()

BASE_URL = CFG["base_url"]
USER_ID = CFG["user_id"]
PASSWORD = CFG["password"]
BROWSER = CFG.get("browser", "chrome").lower()
PDF_WAIT = int(CFG.get("download_wait_sec", 600))
TASKS = CFG["tasks"]

TODAY_STR = datetime.now().strftime("%Y%m%d")
STAMP_STR = datetime.now().strftime("%Y%m%d_%H%M%S")

DESKTOP = Path.home() / "Desktop"
WORK_DIR = DESKTOP / f"{CFG.get('desktop_output_dir_name_prefix', 'DMCI_SUPER_AUTORUN')}_{STAMP_STR}"
PDF_DIR = WORK_DIR / "pdf"
XLSX_DIR = WORK_DIR / "xlsx"
DEBUG_DIR = WORK_DIR / "debug"
LOG_DIR = WORK_DIR / "logs"
HTML_DIR = DEBUG_DIR / "html"
SHOT_DIR = DEBUG_DIR / "screenshots"
JSON_DIR = DEBUG_DIR / "json"

RUN_LOG = LOG_DIR / "run.log"
RESULT_JSON = JSON_DIR / "result_summary.json"
FINAL_BOOK_PATH = WORK_DIR / f"{TODAY_STR}.xlsx"


# =========================================================
# 基本
# =========================================================
def ensure_dirs():
    for p in [PDF_DIR, XLSX_DIR, DEBUG_DIR, LOG_DIR, HTML_DIR, SHOT_DIR, JSON_DIR]:
        p.mkdir(parents=True, exist_ok=True)


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(RUN_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def dump_json(obj, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)


def safe_filename(s: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", str(s))


def now_tag() -> str:
    return datetime.now().strftime("%H%M%S")


def save_screenshot(driver, name: str):
    path = SHOT_DIR / f"{safe_filename(name)}_{now_tag()}.png"
    try:
        driver.save_screenshot(str(path))
    except Exception:
        pass


def save_html(driver, name: str):
    path = HTML_DIR / f"{safe_filename(name)}_{now_tag()}.html"
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
    except Exception:
        pass


def debug_dump(driver, name: str):
    save_screenshot(driver, name)
    save_html(driver, name)
    try:
        meta = {
            "url": driver.current_url,
            "title": driver.title,
            "timestamp": datetime.now().isoformat(),
            "window_handles": driver.window_handles,
        }
        dump_json(meta, JSON_DIR / f"{safe_filename(name)}_{now_tag()}.json")
    except Exception:
        pass


def retry(func, retries=3, wait_sec=3, label="retry_target"):
    last_err = None
    for i in range(1, retries + 1):
        try:
            log(f"{label}: {i}/{retries} 回目")
            return func()
        except Exception as e:
            last_err = e
            log(f"{label}: 失敗 / {e}")
            time.sleep(wait_sec)
    raise last_err


# =========================================================
# Google Sheets
# =========================================================
def get_gspread_client():
    sa_file = CFG["google_service_account_file"]
    gc = gspread.service_account(filename=sa_file)
    return gc


def get_spreadsheet():
    gc = get_gspread_client()
    ss = gc.open_by_key(CFG["google_spreadsheet_id"])
    return ss


def ensure_worksheet(ss, title: str, rows=2000, cols=30):
    try:
        return ss.worksheet(title)
    except Exception:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


def write_df_to_gsheet(ss, sheet_name: str, df: pd.DataFrame):
    ws_name = CFG["google_sheet_names"].get(sheet_name, sheet_name)
    ws = ensure_worksheet(ss, ws_name, rows=max(len(df) + 100, 2000), cols=max(len(df.columns) + 10, 20))
    ws.clear()

    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    ws.update("A1", values)
    log(f"Google Sheets書込完了: {ws_name} / {len(df)}件")


def append_run_log(ss, level: str, message: str):
    ws = ensure_worksheet(ss, "run_log", rows=5000, cols=10)
    ws.append_row([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), level, message], value_input_option="USER_ENTERED")


def write_run_summary(ss, summary: Dict[str, Any]):
    ws = ensure_worksheet(ss, "run_summary", rows=2000, cols=30)
    ws.clear()

    rows = [["key", "value"]]
    for k, v in summary.items():
        if isinstance(v, (dict, list)):
            rows.append([k, json.dumps(v, ensure_ascii=False)])
        else:
            rows.append([k, str(v)])
    ws.update("A1", rows)


# =========================================================
# ブラウザ
# =========================================================
def build_driver(download_dir: Path):
    if BROWSER == "edge":
        options = EdgeOptions()
    else:
        options = ChromeOptions()

    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "safebrowsing.enabled": True,
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--start-maximized")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--lang=en-US")

    if BROWSER == "edge":
        driver = webdriver.Edge(options=options)
    else:
        driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(PDF_WAIT)
    return driver


# =========================================================
# Selenium 共通
# =========================================================
def wait_visible(driver, by, value, timeout=30):
    return WebDriverWait(driver, timeout).until(
        EC.visibility_of_element_located((by, value))
    )


def wait_present(driver, by, value, timeout=30):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, value))
    )


def wait_clickable(driver, by, value, timeout=30):
    return WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((by, value))
    )


def js_click(driver, elem):
    driver.execute_script("arguments[0].click();", elem)


def robust_click(driver, elem):
    try:
        elem.click()
        return
    except Exception:
        pass
    js_click(driver, elem)


def robust_click_xpath(driver, xpaths: List[str], timeout=10):
    last_err = None
    for xp in xpaths:
        try:
            elem = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.XPATH, xp))
            )
            robust_click(driver, elem)
            return True
        except Exception as e:
            last_err = e
    if last_err:
        raise last_err
    return False


def hover(driver, elem):
    ActionChains(driver).move_to_element(elem).pause(1.0).perform()


# =========================================================
# ログインと遷移
# =========================================================
def open_login(driver):
    driver.get(BASE_URL)
    debug_dump(driver, "login_page")

    uid = wait_visible(driver, By.ID, "txtUserId", timeout=120)
    pw = wait_visible(driver, By.ID, "txtPassword", timeout=120)

    uid.clear()
    uid.send_keys(USER_ID)
    pw.clear()
    pw.send_keys(PASSWORD)
    pw.send_keys(Keys.ENTER)

    time.sleep(3)
    debug_dump(driver, "after_login_submit")


def click_entity_code(driver, entity_code: str):
    xpaths = [
        f"//td[normalize-space()='{entity_code}']",
        f"//td[contains(normalize-space(), '{entity_code}')]",
    ]
    robust_click_xpath(driver, xpaths, timeout=60)
    time.sleep(3)
    debug_dump(driver, f"after_entity_{entity_code}")


def open_unit_availability(driver):
    prop_xpaths = [
        "//div[@id='sidebar-menu-title' and contains(., 'Properties')]",
        "//*[normalize-space()='Properties']",
        "//nav//*[contains(., 'Properties')]",
    ]

    moved = False
    for xp in prop_xpaths:
        try:
            prop = wait_present(driver, By.XPATH, xp, timeout=10)
            hover(driver, prop)
            time.sleep(1.5)
            robust_click_xpath(
                driver,
                [
                    "//a[contains(., 'Unit Availability')]",
                    "//*[normalize-space()='Unit Availability']",
                    "//*[contains(@href, 'propertyunit')]",
                ],
                timeout=8,
            )
            moved = True
            break
        except Exception:
            continue

    if not moved:
        driver.get("https://seller.dmcihomes.com/propertyunit?filtercategory=0&filterstatus=1")

    time.sleep(3)
    debug_dump(driver, "unit_availability_opened")


# =========================================================
# 検索条件
# =========================================================
def select_by_value_or_text(driver, select_id: str, value: Optional[str], text_hint: Optional[str]):
    sel = Select(wait_visible(driver, By.ID, select_id, timeout=20))

    if value:
        try:
            sel.select_by_value(value)
            return
        except Exception:
            pass

    if text_hint:
        for opt in sel.options:
            if text_hint.lower() in (opt.text or "").lower():
                sel.select_by_visible_text(opt.text)
                return

    options = [{"value": o.get_attribute("value"), "text": o.text} for o in sel.options]
    dump_json(options, JSON_DIR / f"{select_id}_options_{now_tag()}.json")
    raise RuntimeError(f"{select_id} の選択ができなかったある")


def run_search_if_needed(driver, task: Dict):
    if not task.get("need_search", False):
        return

    select_by_value_or_text(driver, "project", task.get("project_value"), task.get("project_text_hint"))
    select_by_value_or_text(driver, "category", task.get("category_value"), task.get("category_text_hint"))
    debug_dump(driver, f"after_select_{task['sheet_name']}")

    robust_click_xpath(
        driver,
        [
            "//button[@id='btnSearch']",
            "//button[contains(., 'Search')]",
        ],
        timeout=15,
    )
    time.sleep(5)
    debug_dump(driver, f"after_search_{task['sheet_name']}")


def set_view_by_100(driver):
    sel = Select(wait_visible(driver, By.ID, "selectedcount", timeout=20))
    sel.select_by_value("100")
    time.sleep(4)
    debug_dump(driver, "after_view_by_100")


# =========================================================
# Summary PDF
# =========================================================
def cleanup_pdf_dir():
    for p in PDF_DIR.glob("*"):
        try:
            p.unlink()
        except Exception:
            pass


def click_summary_pdf(driver):
    export_candidates = [
        "//img[contains(@src, 'exportpdf')]",
        "//div[@id='viewbyindex']//img",
        "//img[contains(@style, 'margin-top')]",
    ]

    export_elem = None
    for xp in export_candidates:
        try:
            export_elem = wait_present(driver, By.XPATH, xp, timeout=10)
            hover(driver, export_elem)
            time.sleep(2)
            break
        except Exception:
            continue

    if export_elem is None:
        raise RuntimeError("DOWNLOAD PDFアイコンが見つからないある")

    for _ in range(3):
        try:
            robust_click_xpath(
                driver,
                [
                    "//a[contains(., 'Summary')]",
                    "//*[self::a or self::button][contains(., 'Summary')]",
                ],
                timeout=8,
            )
            time.sleep(3)
            debug_dump(driver, "after_summary_click")
            return
        except Exception:
            hover(driver, export_elem)
            time.sleep(2)

    raise RuntimeError("Summary がクリックできなかったある")


def wait_download_complete(timeout=600) -> Path:
    start = time.time()
    last_pdf = None

    while time.time() - start < timeout:
        pdfs = list(PDF_DIR.glob("*.pdf"))
        crs = list(PDF_DIR.glob("*.crdownload"))

        if pdfs:
            last_pdf = max(pdfs, key=lambda p: p.stat().st_mtime)

        if last_pdf and not crs:
            time.sleep(2)
            return last_pdf

        time.sleep(2)

    raise TimeoutException("PDFダウンロード待ちタイムアウトある")


def close_extra_windows(driver, base_handle: str):
    for h in driver.window_handles:
        if h != base_handle:
            try:
                driver.switch_to.window(h)
                driver.close()
            except Exception:
                pass
    driver.switch_to.window(base_handle)


def download_summary_pdf(driver, logical_name: str) -> Path:
    cleanup_pdf_dir()

    base_handle = driver.current_window_handle
    before_handles = set(driver.window_handles)

    click_summary_pdf(driver)
    time.sleep(5)

    after_handles = set(driver.window_handles)
    new_handles = list(after_handles - before_handles)
    if new_handles:
        try:
            driver.switch_to.window(new_handles[-1])
            debug_dump(driver, f"new_window_{logical_name}")
        except Exception:
            pass

    pdf_path = wait_download_complete(timeout=PDF_WAIT)
    target = PDF_DIR / f"{safe_filename(logical_name)}.pdf"
    if target.exists():
        target.unlink()
    shutil.move(str(pdf_path), str(target))

    close_extra_windows(driver, base_handle)
    return target


# =========================================================
# テーブル直読み fallback
# =========================================================
def extract_html_table_rows(driver) -> List[Dict[str, str]]:
    rows = []
    tr_list = driver.find_elements(By.XPATH, "//table//tr")
    for tr in tr_list:
        tds = tr.find_elements(By.TAG_NAME, "td")
        vals = [td.text.strip() for td in tds]
        if not vals:
            continue
        if len(vals) >= 8:
            rows.append({
                "col1": vals[0] if len(vals) > 0 else "",
                "col2": vals[1] if len(vals) > 1 else "",
                "col3": vals[2] if len(vals) > 2 else "",
                "col4": vals[3] if len(vals) > 3 else "",
                "col5": vals[4] if len(vals) > 4 else "",
                "col6": vals[5] if len(vals) > 5 else "",
                "col7": vals[6] if len(vals) > 6 else "",
                "col8": vals[7] if len(vals) > 7 else "",
                "raw": " | ".join(vals),
            })
    return rows


def collect_all_pages_table(driver, logical_name: str) -> pd.DataFrame:
    all_rows = []
    page_no = 1

    while True:
        time.sleep(2)
        page_rows = extract_html_table_rows(driver)
        for r in page_rows:
            r["page_no"] = page_no
        all_rows.extend(page_rows)

        debug_dump(driver, f"table_page_{logical_name}_{page_no}")

        next_candidates = [
            "//a[contains(., 'Next')]",
            "//button[contains(., 'Next')]",
            "//*[contains(@class,'paginate')]//*[contains(., 'Next')]",
        ]

        moved = False
        for xp in next_candidates:
            try:
                elems = driver.find_elements(By.XPATH, xp)
                for e in elems:
                    txt = (e.text or "").strip().lower()
                    cls = (e.get_attribute("class") or "").lower()
                    if "disabled" in cls:
                        continue
                    if e.is_displayed():
                        robust_click(driver, e)
                        moved = True
                        page_no += 1
                        time.sleep(3)
                        break
                if moved:
                    break
            except Exception:
                continue

        if not moved:
            break

    if not all_rows:
        raise RuntimeError(f"一覧テーブル直読みも0件だったある: {logical_name}")

    return pd.DataFrame(all_rows)


# =========================================================
# PDF解析
# =========================================================
HEADER_KEYWORDS = [
    "DMCI Seller's Portal",
    "Unit Availability List",
    "As of ",
    "Page:",
    "Generated Date:",
    "Generated by:",
]

ROW_PATTERN_STRICT = re.compile(
    r"""
    ^
    (?P<Property>\S+)\s+
    (?P<Building>.+?)\s+
    (?P<Unit>\S+)\s+
    (?P<Tower>\S+)\s+
    (?P<Floor>\S+)\s+
    (?P<Status>\S+)\s+
    (?P<Category>\S+)\s+
    (?P<Type>\S+)\s+
    (?P<GrossArea>\d+(?:\.\d+)?)\s+
    (?P<Location>.+?)\s+
    (?P<PropertyUnit>\S+)\s+
    (?P<RFODate>\d{1,2}/\d{1,2}/\d{4})
    (?:\s+(?P<TandemPackage>.*?))?
    \s+(?P<ListPrice>Php\s*[\d,]+\.\d{2})
    $
    """,
    re.VERBOSE,
)

ROW_PATTERN_RELAXED = re.compile(
    r"""
    ^
    (?P<Property>\S+)\s+
    (?P<Building>.+?)\s+
    (?P<Unit>\S+)\s+
    (?P<Tower>\S+)\s+
    (?P<Floor>\S+)\s+
    (?P<Status>\S+)\s+
    (?P<Category>\S+)\s+
    (?P<Type>\S+)\s+
    (?P<GrossArea>\d+(?:\.\d+)?)\s+
    (?P<Location>.+?)\s+
    (?P<PropertyUnit>\S+)
    (?:\s+(?P<RFODate>\d{1,2}/\d{1,2}/\d{4}))?
    (?:\s+(?P<TandemPackage>.*?))?
    \s+(?P<ListPrice>Php\s*[\d,]+\.\d{2})
    $
    """,
    re.VERBOSE,
)


def is_header_line(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return True
    if s.startswith("Property Building Unit Tower Floor Status"):
        return True
    for kw in HEADER_KEYWORDS:
        if s.startswith(kw):
            return True
    return False


def parse_line(line: str) -> Optional[Dict[str, str]]:
    s = line.strip()
    if is_header_line(s):
        return None
    m = ROW_PATTERN_STRICT.match(s)
    if m:
        return m.groupdict()
    m = ROW_PATTERN_RELAXED.match(s)
    if m:
        return m.groupdict()
    return None


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if "Property" in df.columns:
        df = df.drop(columns=["Property"])

    if "RFODate" in df.columns:
        df["RFODate"] = pd.to_datetime(df["RFODate"], errors="coerce")

    if "GrossArea" in df.columns:
        df["GrossArea"] = pd.to_numeric(df["GrossArea"], errors="coerce")

    if "ListPrice" in df.columns:
        df["ListPrice"] = (
            df["ListPrice"]
            .astype(str)
            .str.replace("Php", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        df["ListPrice"] = pd.to_numeric(df["ListPrice"], errors="coerce")

    ordered_cols = [
        "Building",
        "Unit",
        "Tower",
        "Floor",
        "Status",
        "Category",
        "Type",
        "GrossArea",
        "Location",
        "PropertyUnit",
        "RFODate",
        "TandemPackage",
        "ListPrice",
    ]
    return df[[c for c in ordered_cols if c in df.columns]]


def parse_pdf_to_dataframe(pdf_path: Path, logical_name: str) -> pd.DataFrame:
    rows = []
    unmatched = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            lines = text.splitlines()
            for line in lines:
                parsed = parse_line(line)
                if parsed:
                    rows.append(parsed)
                else:
                    s = line.strip()
                    if s and not is_header_line(s):
                        unmatched.append({"page": i, "line": s})

    dump_json(
        {
            "sheet": logical_name,
            "rows": len(rows),
            "unmatched_sample": unmatched[:200]
        },
        JSON_DIR / f"pdf_parse_{safe_filename(logical_name)}.json"
    )

    if not rows:
        raise RuntimeError(f"PDFから1件も取れなかったある: {logical_name}")

    df = pd.DataFrame(rows)
    return normalize_dataframe(df)


# =========================================================
# Excel
# =========================================================
def format_excel(path: Path):
    wb = load_workbook(path)
    for ws in wb.worksheets:
        ws.freeze_panes = "A2"

        header_map = {}
        for c in ws[1]:
            header_map[c.value] = c.column_letter

        if "RFODate" in header_map:
            col = header_map["RFODate"]
            for cell in ws[col][1:]:
                cell.number_format = 'yyyy"年"mm"月"dd"日"'

        if "ListPrice" in header_map:
            col = header_map["ListPrice"]
            for cell in ws[col][1:]:
                cell.number_format = '#,##0.00'

        widths = {}
        for row in ws.iter_rows():
            for cell in row:
                val = "" if cell.value is None else str(cell.value)
                widths[cell.column_letter] = max(widths.get(cell.column_letter, 0), len(val))

        for col, width in widths.items():
            ws.column_dimensions[col].width = min(max(width + 2, 12), 42)

    wb.save(path)


def save_single_excel(df: pd.DataFrame, sheet_name: str, path: Path):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    format_excel(path)


def save_final_excel(items: List[Dict[str, pd.DataFrame]], out_path: Path):
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for item in items:
            item["df"].to_excel(writer, sheet_name=item["sheet_name"], index=False)
    format_excel(out_path)


# =========================================================
# 1タスク
# =========================================================
def process_task(driver, task: Dict, ss) -> Dict[str, Any]:
    name = task["sheet_name"]
    result = {
        "sheet_name": name,
        "entity_code": task["entity_code"],
        "status": "running",
        "row_count": 0,
        "source_mode": None,
        "pdf_path": None,
        "xlsx_path": None,
        "error": None,
    }

    try:
        append_run_log(ss, "INFO", f"開始: {name}")

        retry(lambda: open_login(driver), retries=3, wait_sec=5, label=f"{name}_open_login")
        retry(lambda: click_entity_code(driver, task["entity_code"]), retries=3, wait_sec=3, label=f"{name}_entity")
        retry(lambda: open_unit_availability(driver), retries=3, wait_sec=3, label=f"{name}_unit_availability")
        retry(lambda: run_search_if_needed(driver, task), retries=3, wait_sec=3, label=f"{name}_run_search")
        retry(lambda: set_view_by_100(driver), retries=3, wait_sec=3, label=f"{name}_viewby")

        # まずPDF
        try:
            pdf_path = retry(lambda: download_summary_pdf(driver, name), retries=2, wait_sec=5, label=f"{name}_pdf_download")
            df = parse_pdf_to_dataframe(pdf_path, name)
            result["source_mode"] = "pdf"
            result["pdf_path"] = str(pdf_path)
        except Exception as pdf_err:
            log(f"{name}: PDF方式失敗 -> テーブル直読みにfallback / {pdf_err}")
            append_run_log(ss, "WARN", f"{name}: PDF方式失敗 -> fallback")
            df = collect_all_pages_table(driver, name)
            result["source_mode"] = "html_table"

        result["row_count"] = int(len(df))

        xlsx_path = XLSX_DIR / f"{safe_filename(name)}.xlsx"
        save_single_excel(df, name, xlsx_path)
        result["xlsx_path"] = str(xlsx_path)

        write_df_to_gsheet(ss, name, df)

        result["status"] = "success"
        append_run_log(ss, "INFO", f"成功: {name} / {len(df)}件 / mode={result['source_mode']}")
        return {"result": result, "df": df}

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        debug_dump(driver, f"task_failed_{name}")
        append_run_log(ss, "ERROR", f"失敗: {name} / {e}")
        return {"result": result, "df": None}


# =========================================================
# main
# =========================================================
def main():
    ensure_dirs()
    log(f"作業開始: {WORK_DIR}")

    summary = {
        "started_at": datetime.now().isoformat(),
        "work_dir": str(WORK_DIR),
        "browser": BROWSER,
        "tasks": [],
        "final_excel": None,
    }

    ss = get_spreadsheet()
    append_run_log(ss, "INFO", "全体開始")

    driver = None
    final_items = []

    try:
        driver = build_driver(PDF_DIR)

        for task in TASKS:
            out = process_task(driver, task, ss)
            summary["tasks"].append(out["result"])

            if out["df"] is not None:
                final_items.append({
                    "sheet_name": task["sheet_name"],
                    "df": out["df"],
                })

            dump_json(summary, RESULT_JSON)
            write_run_summary(ss, summary)

        if final_items:
            save_final_excel(final_items, FINAL_BOOK_PATH)
            summary["final_excel"] = str(FINAL_BOOK_PATH)

        summary["finished_at"] = datetime.now().isoformat()
        dump_json(summary, RESULT_JSON)
        write_run_summary(ss, summary)
        append_run_log(ss, "INFO", "全体終了")

        log("全処理完了ある")
        if summary["final_excel"]:
            log(f"最終Excel: {FINAL_BOOK_PATH}")

    except Exception as e:
        summary["fatal_error"] = str(e)
        summary["finished_at"] = datetime.now().isoformat()
        dump_json(summary, RESULT_JSON)
        try:
            write_run_summary(ss, summary)
            append_run_log(ss, "ERROR", f"main異常終了: {e}")
        except Exception:
            pass
        traceback.print_exc()

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        log("終了ある")


if __name__ == "__main__":
    main()