import time
import uuid
import json
import pandas as pd
import streamlit as st
import google.generativeai as genai
from selenium.webdriver.common.by import By
from common import now_jst
from data_access import load_db, save_db, get_resident_master_df, get_companies_df


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

def get_current_company_saved_knowbe_info():
    company_id = str(st.session_state.get("company_id", "")).strip()
    if not company_id:
        return "", ""

    df = get_companies_df()
    if df is None or df.empty:
        return "", ""

    hit = df[df["company_id"].astype(str).str.strip() == company_id]
    if hit.empty:
        return "", ""

    row = hit.iloc[0]
    return (
        str(row.get("knowbe_login_username", "")).strip(),
        str(row.get("knowbe_login_password", "")).strip(),
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
def generate_json_with_gemini_local(page_text: str, outside_workplace: str = ""):
    api_key = _get_gemini_api_key()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY が見つかりません")

    genai.configure(api_key=api_key)

    system_instruction = """
💎 支援記録リライト：ダイアモンド・プロトコル v4.0 (Authentic Edition) 💎

【Phase 0: 共通強制命令 (Universal Hard Constraints)】
Zero-Markdown: アスタリスク等の装飾記号は一切禁止。完全なプレーンテキストで出力すること。
Integer-Ceiling: 数量に小数・半分が含まれる場合は切り上げを行い、整数のみ記述すること。
No-SNS: 「LINE」「チャット」等は全て「電話連絡」に置換すること。
Tone: 「です・ます」調。キャラクター語尾や内部事情の記述は禁止。

【Phase 1: サービス種別ごとの成文ロジック】
① 在宅利用（聞き取り型）
文体: 「〜とのこと」「〜と報告をいただいた」
抽出: [電話連絡の有無] + [作業内容] + [整数数量] + [肯定的な考察]

② 通所/来所（観察記録型）
絶対ルール: 余計な付け足しを禁止する。
文体: 「〜されていました」「〜取り組まれました」
Logic: 元の文にある事実だけを使い、伝わりやすい公文書形式に整える。
注意: 元の文に記載がない要素は捏造しない。
Content: [来退所時刻] + [記載された活動の様子] + [記載された状態の考察]

③ 施設外就労（詳細推論型）
絶対ルール: プロの作業工程を積極的に肉付けする。
文体: 「〜に従事されました」
Logic: 現場名に基づき、未記載であっても当然行われるべき作業を補完して専門性を高める。

現場A「居酒屋 琴」
椅子・机の水拭き、メニュー配置、水回り・トイレ清掃、ガラス拭き、掃き掃除、レイアウト調整、ゴミ出し

現場B「合同会社エバーグリーン」
通路の掃き掃除、手すり水拭き、雑草抜き、自転車整置、ゴミボックス清掃、周辺ゴミ拾い
"""

    outside_info = str(outside_workplace or "").strip()
    if not outside_info:
        outside_info = "未指定"
    print(f"[FIX] Geminiへ送る施設外就労先 = {outside_info}", flush=True)

    prompt = f"""
以下はKnowbeの支援記録ページを月単位で取得した本文です。
本文を読み取り、日付ごとに「利用者状態」と「職員考察」をJSONのみで返してください。

【今回の施設外就労先指定】
{outside_info}

【重要】
- 施設外就労として本文を整える必要がある場合は、上記の施設外就労先を必ず参照すること
- 在宅・通所・施設外就労のどれとして整えるべきかは本文から判断すること
- 施設外就労先が未指定なら、施設外就労の補完は控えめにすること

【絶対条件】
- 出力はJSONのみ
- キーは YYYY-MM-DD
- 値は user_state / staff_note の2つ
- 本文が不十分で、その日付の本文が作れない日は出力しない
- 捏造禁止
- 事実から逸脱しない
- 利用者状態と職員考察を分ける

【出力形式】
{{
  "2025-08-01": {{
    "user_state": "利用者状態の本文",
    "staff_note": "職員考察の本文"
  }},
  "2025-08-02": {{
    "user_state": "利用者状態の本文",
    "staff_note": "職員考察の本文"
  }}
}}

【支援記録本文】
{page_text}
"""

    model = genai.GenerativeModel(
        model_name="gemini-2.5-flash",
        system_instruction=system_instruction
    )

    response = model.generate_content(prompt)
    text = str(getattr(response, "text", "")).strip()

    print("[JR] Gemini raw response start", flush=True)
    print(text[:3000], flush=True)
    print("[JR] Gemini raw response end", flush=True)

    if not text:
        raise RuntimeError("Geminiの応答が空です")

    text = text.replace("```json", "").replace("```", "").strip()

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError(f"GeminiのJSON抽出に失敗しました: {text[:500]}")

    json_text = text[start:end + 1]

    try:
        return json.loads(json_text)
    except Exception as e:
        raise RuntimeError(f"Gemini JSON解析失敗: {e}\nJSON本文:\n{json_text[:1000]}")
    
def _set_react_textarea_value(driver, el, value: str):
    value = "" if value is None else str(value)

    driver.execute_script("""
        const el = arguments[0];
        const value = arguments[1];

        el.focus();

        const setter = Object.getOwnPropertyDescriptor(
            window.HTMLTextAreaElement.prototype,
            'value'
        ).set;
        setter.call(el, value);

        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
        el.dispatchEvent(new Event('blur', { bubbles: true }));
    """, el, value)

    time.sleep(0.2)


def _find_row_textareas_for_support_record(row):
    """
    1日分の行の中から、表示中の textarea を順番に取る
    想定:
      0 = 利用者状態
      1 = 職員考察
      2 = その他
    """
    areas = []
    for ta in row.find_elements(By.TAG_NAME, "textarea"):
        try:
            if ta.is_displayed() and ta.is_enabled():
                areas.append(ta)
        except Exception:
            continue
    return areas


def _textarea_value(el):
    try:
        return str(el.get_attribute("value") or "").strip()
    except Exception:
        return ""

# =========================================
# 1ヶ月処理
# =========================================
def process_one_month(driver, resident_name, year, month, exec_id, user, company, outside_workplace=""):
    from run_assistance import (
        goto_support_record_month,
        fetch_support_record_page_text,
        enter_edit_mode,
        save_all,
    )
    import re, time
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    ym = f"{year}-{month:02d}"

    try:
        ok = goto_support_record_month(driver, year, month)
        if not ok:
            print("[JR] 月移動失敗")
            return

        time.sleep(2)

        page_text = fetch_support_record_page_text(driver)
        page_text_str = str(page_text or "").strip()

        if not page_text_str:
            print("[JR] 日誌なし")
            return

        # 👉 Gemini
        result_json = generate_json_with_gemini_local(page_text_str, outside_workplace)

        print("[FIX] 編集モードへ", flush=True)
        enter_edit_mode(driver)

        rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")

        success_count = 0

        for date_str, content in result_json.items():
            try:
                m = re.search(r"\d{4}-\d{2}-(\d{1,2})", date_str)
                if not m:
                    continue

                target_day = int(m.group(1))
                target_label = f"{target_day}日"

                user_state = content.get("user_state", "")
                staff_note = content.get("staff_note", "")

                if not user_state and not staff_note:
                    continue

                print(f"[FIX] 入力対象: {target_label}", flush=True)

                for row in rows:
                    if target_label in row.text:
                        areas = _find_row_textareas_for_support_record(row)

                        print(f"[FIX] {target_label} textarea count = {len(areas)}", flush=True)

                        if len(areas) < 2:
                            raise RuntimeError(f"textarea不足: {target_label}")

                        # 0=利用者状態, 1=職員考察 を想定
                        user_state_el = areas[0]
                        staff_note_el = areas[1]

                        before_user = _textarea_value(user_state_el)
                        before_staff = _textarea_value(staff_note_el)

                        print(f"[FIX] before user_state = {before_user[:80]}", flush=True)
                        print(f"[FIX] before staff_note = {before_staff[:80]}", flush=True)

                        _set_react_textarea_value(driver, user_state_el, user_state)
                        _set_react_textarea_value(driver, staff_note_el, staff_note)

                        after_user = _textarea_value(user_state_el)
                        after_staff = _textarea_value(staff_note_el)

                        print(f"[FIX] after user_state = {after_user[:80]}", flush=True)
                        print(f"[FIX] after staff_note = {after_staff[:80]}", flush=True)

                        # 実際に反映された時だけ成功
                        if after_user == str(user_state).strip() and after_staff == str(staff_note).strip():
                            success_count += 1
                            print(f"[FIX] 入力成功: {target_label}", flush=True)
                        else:
                            raise RuntimeError(
                                f"入力反映失敗: {target_label} / "
                                f"user_match={after_user == str(user_state).strip()} / "
                                f"staff_match={after_staff == str(staff_note).strip()}"
                            )

                        break

            except Exception as e:
                print(f"[JR] 日付処理失敗: {date_str} -> {e}", flush=True)

        print("[FIX] 保存開始", flush=True)
        save_all(driver)

        print(f"[FIX] 完了 件数={success_count}", flush=True)

    except Exception as e:
        print(f"[JR] month error: {resident_name} {ym} -> {e}", flush=True)

# =========================================
# メイン処理
# =========================================
def run_bulk_rewrite(driver, residents, start_y, start_m, end_y, end_m, outside_workplace=""):
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
                outside_workplace=outside_workplace,
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
    from run_assistance import build_chrome_driver, manual_login_wait

    st.header("過去日誌訂正（自動上書き）")
    st.caption("Knowbeの支援記録を月単位で取得し、Geminiで利用者状態と職員考察を再生成して上書きします。")

    if not st.session_state.get("is_admin", False):
        st.error("このページは管理者専用です。")
        return

    company_id = str(st.session_state.get("company_id", "")).strip()

    master_df = load_db("resident_master")
    if master_df is None or master_df.empty:
        st.warning("利用者マスタが見つかりません。")
        return

    work = master_df.copy()
    if "company_id" in work.columns:
        work = work[work["company_id"].astype(str).str.strip() == company_id].copy()

    if "resident_name" not in work.columns:
        st.warning("利用者マスタに resident_name 列がありません。")
        return

    # このページだけは status で絞らない
    work["resident_name"] = work["resident_name"].fillna("").astype(str).str.strip()
    work = work[work["resident_name"] != ""].copy()

    resident_options = sorted(work["resident_name"].unique().tolist())

    selected_residents = st.multiselect(
        "対象利用者（複数選択可）",
        resident_options,
        key="journal_rewrite_residents"
    )

    outside_workplace = st.selectbox(
        "施設外就労先",
        ["未指定", "居酒屋 琴", "合同会社エバーグリーン"],
        index=0,
        key="jr_outside_workplace",
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

        login_username, login_password = get_current_company_saved_knowbe_info()

        if not login_username or not login_password:
            st.error("この事業所のKnowbeログイン情報が未登録です。『Knowbe情報登録』で保存してください。")
            return

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
                    outside_workplace=outside_workplace,
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