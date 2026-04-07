import json
import gspread

with open("config_local.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

gc = gspread.service_account(filename="service_account.json")
ss = gc.open_by_key(cfg["google_spreadsheet_id"])

ws = ss.worksheet("run_log")
ws.append_row(["みー成功ある", "🔥"], value_input_option="USER_ENTERED")

print("OKある！")
