from datetime import datetime
import calendar
import pandas as pd
import calendar
from datetime import datetime

# ===== 時刻 =====
def now_jst():
    return datetime.now()

# ===== シート名 =====
def get_sheet_name_candidates(file):
    return [
        file,
        file.lower(),
        file.upper()
    ]

def get_sheet_name(file):
    return get_sheet_name_candidates(file)[0]

# ===== 時間処理 =====
def parse_time_range(raw_text: str):
    if not raw_text:
        return "", ""
    parts = raw_text.replace("〜", "-").split("-")
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return "", ""

def _to_minutes(hhmm: str):
    if not hhmm or ":" not in hhmm:
        return None
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)

def _normalize_weekday_label(dt_value):
    if not dt_value:
        return ""
    return dt_value.strftime("%a")

def is_time_overlap(start1, end1, start2, end2):
    s1 = _to_minutes(start1)
    e1 = _to_minutes(end1)
    s2 = _to_minutes(start2)
    e2 = _to_minutes(end2)

    if None in (s1, e1, s2, e2):
        return False

    return max(s1, s2) < min(e1, e2)

# ===== テキスト =====
def mask_secret_text(value: str) -> str:
    if not value:
        return ""
    return "*" * len(value)

def safe_text(v):
    return str(v or "").strip()

def heart_label(text: str) -> str:
    return f"💕 {text}"

# ===== 日付 =====
def get_saturday_dates_for_month(year: int, month: int):
    cal = calendar.monthcalendar(year, month)
    saturdays = []
    for week in cal:
        if week[calendar.SATURDAY] != 0:
            saturdays.append(week[calendar.SATURDAY])
    return saturdays

def get_next_numeric_id(df, col_name="id", start=1):
    if df is None or df.empty or col_name not in df.columns:
        return start
    ids = pd.to_numeric(df[col_name], errors="coerce").dropna()
    return int(ids.max()) + 1 if not ids.empty else start

def normalize_company_scoped_df(df, required_cols):
    if df is None or df.empty:
        return pd.DataFrame(columns=required_cols)

    work = df.copy()
    for col in required_cols:
        if col not in work.columns:
            work[col] = ""

    work["company_id"] = work["company_id"].fillna("").astype(str).str.strip()
    return work.fillna("")

def filter_by_company_id(df, company_id):
    if df is None or df.empty:
        return df

    if "company_id" not in df.columns:
        return df

    return df[
        df["company_id"].astype(str).str.strip() == str(company_id).strip()
    ].copy()


