import time
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
from smartcard.System import readers


# ===== 設定ここだけ =====
SPREADSHEET_ID = "1UZ0O6Rtfu127YCIAYrAoU3us8aneudnO-gFVkjndtaQ"
SERVICE_ACCOUNT_FILE = r"Y:\作業管理\service_account.json"
SHEET_NAME = "ic_reader_bridge"
BRIDGE_ID = "main_reader"
DEVICE_NAME = "front_desk"

POLL_INTERVAL_SEC = 0.8
DUPLICATE_GUARD_SEC = 10.0          # 同じカードは10秒以内なら再送しない
READY_HOLD_SEC = 10.0               # 読み取り後、readyを最低これだけ維持
IDLE_WRITE_INTERVAL_SEC = 5.0       # idleを書き直すのは最短5秒ごと
# ======================


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_gc():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    return gspread.authorize(creds)


def get_ws(gc):
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(SHEET_NAME)


def ensure_bridge_row(ws):
    records = ws.get_all_records()
    for idx, row in enumerate(records, start=2):
        if str(row.get("bridge_id", "")).strip() == BRIDGE_ID:
            return idx

    ws.append_row([BRIDGE_ID, DEVICE_NAME, "", "", "idle"])
    records = ws.get_all_records()
    for idx, row in enumerate(records, start=2):
        if str(row.get("bridge_id", "")).strip() == BRIDGE_ID:
            return idx

    raise RuntimeError("ic_reader_bridge の bridge row が作れませんでした")


def write_status(ws, row_no, card_id="", status="idle"):
    ws.update(
        values=[[
            BRIDGE_ID,
            DEVICE_NAME,
            str(card_id).strip().upper(),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            status
        ]],
        range_name=f"A{row_no}:E{row_no}"
    )


def get_uid_from_first_reader():
    rlist = readers()
    if not rlist:
        raise RuntimeError("カードリーダーが見つかりません")

    r = rlist[0]
    conn = r.createConnection()
    conn.connect()

    # UID取得 APDU
    cmd = [0xFF, 0xCA, 0x00, 0x00, 0x00]
    data, sw1, sw2 = conn.transmit(cmd)

    if (sw1, sw2) != (0x90, 0x00):
        raise RuntimeError(f"UID取得失敗 sw={sw1:02X}{sw2:02X}")

    return "".join(f"{b:02X}" for b in data)


def main():
    gc = get_gc()
    ws = get_ws(gc)
    row_no = ensure_bridge_row(ws)

    print("ICブリッジ開始")
    print(f"bridge_id={BRIDGE_ID} device={DEVICE_NAME}")

    last_card_id = ""
    last_card_sent_ts = 0.0
    last_idle_write_ts = 0.0
    ready_until_ts = 0.0

    # 起動時だけ初期 idle
    write_status(ws, row_no, "", "idle")
    last_idle_write_ts = time.time()

    while True:
        now_ts = time.time()

        try:
            card_id = get_uid_from_first_reader()
            card_id = str(card_id).strip().upper()

            # 同じカードは10秒以内なら再送しない
            if card_id == last_card_id and (now_ts - last_card_sent_ts) < DUPLICATE_GUARD_SEC:
                # ready維持時間中は何もしない
                time.sleep(POLL_INTERVAL_SEC)
                continue

            last_card_id = card_id
            last_card_sent_ts = now_ts
            ready_until_ts = now_ts + READY_HOLD_SEC

            print(f"[{now_str()}] card_id={card_id}")
            write_status(ws, row_no, card_id, "ready")

            time.sleep(POLL_INTERVAL_SEC)
            continue

        except Exception:
            # カード未タッチや一時失敗時
            # ただし、ready維持時間中は last_card_id を消さず、readyのまま保持
            if now_ts < ready_until_ts:
                time.sleep(POLL_INTERVAL_SEC)
                continue

            # idleは頻繁に書きすぎない
            if (now_ts - last_idle_write_ts) >= IDLE_WRITE_INTERVAL_SEC:
                try:
                    # ここが重要:
                    # card_id は空に戻さず、最後に読んだカードを残す
                    write_status(ws, row_no, last_card_id, "idle")
                    last_idle_write_ts = now_ts
                except Exception:
                    pass

            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()