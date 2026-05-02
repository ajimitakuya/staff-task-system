import csv
import gspread

SPREADSHEET_ID = "1UZ0O6Rtfu127YCIAYrAoU3us8aneudnO-gFVkjndtaQ"
SHEET_NAME = "resident_master"
OUTPUT_CSV = "resident_master.csv"

gc = gspread.service_account(filename="service_account.json")
ss = gc.open_by_key(SPREADSHEET_ID)
ws = ss.worksheet(SHEET_NAME)

rows = ws.get_all_values()

with open(OUTPUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(rows)

print(f"OK: {SHEET_NAME} -> {OUTPUT_CSV}")
print(f"rows: {len(rows)}")