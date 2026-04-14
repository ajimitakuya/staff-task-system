import time
import uuid
import pandas as pd
import streamlit as st

from common import now_jst
from data_access import load_db, save_db

# =========================================
# 📝 ログ書き込み
# =========================================
def append_journal_log(row_dict):
    df = load_db("journal_rewrite_logs")
    if df is None or df.empty:
        df = pd.DataFrame(columns=[
            "exec_id", "exec_time", "user", "company",
            "resident_name", "target_month",
            "result", "count", "message"
        ])

    df = pd.concat([df, pd.DataFrame([row_dict])], ignore_index=True)
    save_db(df, "journal_rewrite_logs")


# =========================================
# 🤖 Gemini処理（黄金ルール）
# =========================================
def generate_month_diary_with_gemini(text):
    from app import generate_json_with_gemini

    prompt = f"""
以下は支援記録ページを丸ごとコピーしたテキストです。

【絶対ルール】
・嘘を書かない
・書いていないことは絶対に補完しない
・事実をそのまま丁寧な文章にする
・利用者状態と職員考察を必ず分ける
・各日ごとに生成する
・日付ごとにJSONで出力する

出力形式：
{{
  "2026-04-01": {{
    "user_state": "...",
    "staff_note": "..."
  }},
  "2026-04-02": {{
    "user_state": "...",
    "staff_note": "..."
  }}
}}

本文：
{text}
"""
    return generate_json_with_gemini(prompt)

# =========================================
# 📅 1ヶ月処理
# =========================================
def process_one_month(driver, resident_name, year, month, exec_id, user, company):
    from run_assistance import goto_support_record_month, fetch_support_record_page_text

    ym = f"{year}-{month:02d}"

    try:
        goto_support_record_month(driver, year, month)
        time.sleep(2)

        page_text = fetch_support_record_page_text(driver)

        if not str(page_text).strip():
            append_journal_log({
                "exec_id": exec_id,
                "exec_time": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
                "user": user,
                "company": company,
                "resident_name": resident_name,
                "target_month": ym,
                "result": "なし",
                "count": 0,
                "message": "日誌なし"
            })
            return

        result_json = generate_month_diary_with_gemini(page_text)

        success_count = 0

        for date_str, content in result_json.items():
            try:
                from run_assistance import open_day_edit_modal, update_day_fields, save_day

                open_day_edit_modal(driver, date_str)
                update_day_fields(
                    driver,
                    content.get("user_state", ""),
                    content.get("staff_note", "")
                )
                save_day(driver)
                success_count += 1

            except Exception as e:
                print("日付処理失敗:", date_str, e)

        append_journal_log({
            "exec_id": exec_id,
            "exec_time": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
            "user": user,
            "company": company,
            "resident_name": resident_name,
            "target_month": ym,
            "result": "成功",
            "count": success_count,
            "message": f"{success_count}件修正"
        })

    except Exception as e:
        append_journal_log({
            "exec_id": exec_id,
            "exec_time": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
            "user": user,
            "company": company,
            "resident_name": resident_name,
            "target_month": ym,
            "result": "エラー",
            "count": 0,
            "message": str(e)
        })

# =========================================
# 🔁 メイン処理
# =========================================
def run_bulk_rewrite(driver, residents, start_y, start_m, end_y, end_m):
    exec_id = str(uuid.uuid4())

    user = st.session_state.get("user_id", "")
    company = st.session_state.get("company_id", "")

    for resident in residents:
        from run_assistance import go_to_user_and_open_support_record

        go_to_user_and_open_support_record(driver, resident)

        y, m = start_y, start_m

        while (y < end_y) or (y == end_y and m <= end_m):
            process_one_month(
                driver,
                resident,
                y,
                m,
                exec_id,
                user,
                company
            )

            if m == 12:
                y += 1
                m = 1
            else:
                m += 1

def render_journal_rewrite_page():
    st.header("過去日誌参照（自動上書き）")
    st.caption("Knowbeの支援記録を月単位で取得し、Geminiで利用者状態と職員考察を再生成して上書きします。")

    st.info("ここに利用者選択UIと開始年月・終了年月UIを置くある。")