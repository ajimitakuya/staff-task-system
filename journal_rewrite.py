import time
import uuid
import json
import pandas as pd
import streamlit as st
import google.generativeai as genai

from common import now_jst
from data_access import load_db, save_db, get_resident_master_df


# =========================================
# Gemini JSON生成
# =========================================
def _get_gemini_api_key():
    api_key = ""
    try:
        api_key = st.secrets.get("GEMINI_API_KEY", "")
    except Exception:
        api_key = ""

    if not api_key:
        import os
        api_key = os.environ.get("GEMINI_API_KEY", "")

    return str(api_key or "").strip()


def generate_json_with_gemini_local(prompt: str):
    api_key = _get_gemini_api_key()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY が見つかりません")

    genai.configure(api_key=api_key)

    model_candidates = ["gemini-2.5-flash"]
    last_error = None
    last_text = ""

    for model_name in model_candidates:
        for attempt in range(3):
            try:
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(prompt)
                text = str(getattr(response, "text", "")).strip()
                last_text = text

                if not text:
                    last_error = RuntimeError(f"{model_name} empty response")
                    continue

                text = text.replace("```json", "").replace("```", "").strip()

                start = text.find("{")
                end = text.rfind("}")
                if start != -1 and end != -1 and end > start:
                    text = text[start:end + 1].strip()

                return json.loads(text)

            except Exception as e:
                last_error = e
                msg = str(e)
                if "429" in msg:
                    continue
                break

    preview = (last_text or "")[:1000]
    raise RuntimeError(
        f"Gemini JSON生成失敗: {last_error}\n"
        f"--- preview start ---\n{preview}\n--- preview end ---"
    )


# =========================================
# ログ書き込み
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
# 月単位の日誌再生成
# =========================================
def generate_month_diary_with_gemini(text: str):
    prompt = f"""
以下は就労継続支援B型の支援記録ページを丸ごとコピーしたテキストです。

【絶対ルール】
・嘘を書かない
・書いていないことは絶対に補完しない
・事実をそのまま丁寧な文章にする
・利用者状態と職員考察を必ず分ける
・各日ごとに生成する
・本文がない日は出力しない
・出力はJSONのみ

出力形式：
{{
  "2026-04-01": {{
    "user_state": "利用者状態の本文",
    "staff_note": "職員考察の本文"
  }},
  "2026-04-02": {{
    "user_state": "利用者状態の本文",
    "staff_note": "職員考察の本文"
  }}
}}

本文：
{text}
"""
    return generate_json_with_gemini_local(prompt)


# =========================================
# 1ヶ月処理
# =========================================
def process_one_month(driver, resident_name, year, month, exec_id, user, company):
    from run_assistance import goto_support_record_month, fetch_support_record_page_text
    from run_assistance import open_day_edit_modal, update_day_fields, save_day

    ym = f"{year}-{month:02d}"

    try:
        ok = goto_support_record_month(driver, year, month)
        if not ok:
            append_journal_log({
                "exec_id": exec_id,
                "exec_time": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
                "user": user,
                "company": company,
                "resident_name": resident_name,
                "target_month": ym,
                "result": "なし",
                "count": 0,
                "message": "対象月へ移動できませんでした"
            })
            return

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
                open_day_edit_modal(driver, date_str)
                update_day_fields(
                    driver,
                    str(content.get("user_state", "")).strip(),
                    str(content.get("staff_note", "")).strip()
                )
                save_day(driver)
                success_count += 1
                time.sleep(0.8)
            except Exception as e:
                print("日付処理失敗:", date_str, e, flush=True)

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
# メイン処理
# =========================================
def run_bulk_rewrite(driver, residents, start_y, start_m, end_y, end_m):
    from run_assistance import goto_users_summary
    from run_assistance import apply_users_summary_filter_show_expired
    from run_assistance import open_support_record_for_resident

    exec_id = str(uuid.uuid4())
    user = st.session_state.get("user_id", "")
    company = st.session_state.get("company_id", "")

    for resident in residents:
        ok = goto_users_summary(driver)
        if not ok:
            append_journal_log({
                "exec_id": exec_id,
                "exec_time": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
                "user": user,
                "company": company,
                "resident_name": resident,
                "target_month": "",
                "result": "エラー",
                "count": 0,
                "message": "利用者ごと一覧へ移動できませんでした"
            })
            continue

        apply_users_summary_filter_show_expired(driver)

        ok = open_support_record_for_resident(driver, resident)
        if not ok:
            append_journal_log({
                "exec_id": exec_id,
                "exec_time": now_jst().strftime("%Y-%m-%d %H:%M:%S"),
                "user": user,
                "company": company,
                "resident_name": resident,
                "target_month": "",
                "result": "エラー",
                "count": 0,
                "message": "対象利用者の支援記録を開けませんでした"
            })
            continue

        y, m = int(start_y), int(start_m)

        while (y < int(end_y)) or (y == int(end_y) and m <= int(end_m)):
            process_one_month(
                driver=driver,
                resident_name=resident,
                year=y,
                month=m,
                exec_id=exec_id,
                user=user,
                company=company,
            )

            if m == 12:
                y += 1
                m = 1
            else:
                m += 1


# =========================================
# ページUI
# =========================================
def render_journal_rewrite_page():
    from run_assistance import build_chrome_driver, get_knowbe_login_credentials, manual_login_wait

    st.header("過去日誌訂正（自動上書き）")
    st.caption("Knowbeの支援記録を月単位で取得し、Geminiで利用者状態と職員考察を再生成して上書きします。")

    if not st.session_state.get("is_admin", False):
        st.error("このページは管理者専用です。")
        return

    company_id = str(st.session_state.get("company_id", "")).strip()
    master_df = get_resident_master_df(company_id)

    if master_df is None or master_df.empty or "resident_name" not in master_df.columns:
        st.warning("利用者マスタが見つかりません。")
        return

    work = master_df.copy()
    work["resident_name"] = work["resident_name"].fillna("").astype(str).str.strip()
    work = work[work["resident_name"] != ""].copy()

    resident_options = sorted(work["resident_name"].unique().tolist())

    selected_residents = st.multiselect(
        "対象利用者（複数選択可）",
        resident_options,
        key="journal_rewrite_residents"
    )

    c1, c2 = st.columns(2)
    with c1:
        start_y = st.number_input("開始年", min_value=2024, max_value=2035, value=2025, step=1, key="jr_start_y")
        start_m = st.number_input("開始月", min_value=1, max_value=12, value=8, step=1, key="jr_start_m")
    with c2:
        end_y = st.number_input("終了年", min_value=2024, max_value=2035, value=2026, step=1, key="jr_end_y")
        end_m = st.number_input("終了月", min_value=1, max_value=12, value=3, step=1, key="jr_end_m")

    if st.button("自動上書きを実行", key="run_journal_rewrite", use_container_width=True):
        if not selected_residents:
            st.error("利用者を1人以上選んでください。")
            return

        if (int(start_y), int(start_m)) > (int(end_y), int(end_m)):
            st.error("開始年月が終了年月より後になっています。")
            return

        login_username, login_password = get_knowbe_login_credentials()

        driver = None
        try:
            with st.spinner("Knowbeへ接続して自動上書きを実行中です…"):
                driver = build_chrome_driver()
                driver.get("https://mgr.knowbe.jp/v2/")
                time.sleep(2.0)
                manual_login_wait(driver, login_username, login_password)

                run_bulk_rewrite(
                    driver=driver,
                    residents=selected_residents,
                    start_y=int(start_y),
                    start_m=int(start_m),
                    end_y=int(end_y),
                    end_m=int(end_m),
                )

            st.success("自動上書き処理が完了しました。")
        except Exception as e:
            st.error(f"実行中にエラーが発生しました: {e}")
        finally:
            if driver is not None:
                try:
                    driver.quit()
                except Exception:
                    pass

    log_df = load_db("journal_rewrite_logs")
    if log_df is not None and not log_df.empty:
        st.markdown("### 実行ログ")
        st.dataframe(log_df.tail(50), use_container_width=True)