import os
import re
import time
from datetime import datetime
from pathlib import Path

from smartcard.System import readers
from supabase import create_client


# ===== 設定ここだけ =====
BRIDGE_ID = "main_reader"
DEVICE_NAME = "front_desk"
TABLE_NAME = "ic_reader_bridge"

POLL_INTERVAL_SEC = 0.1
DUPLICATE_GUARD_SEC = 10.0
READY_HOLD_SEC = 10.0
IDLE_WRITE_INTERVAL_SEC = 5.0
# ======================


BASE_DIR = Path(__file__).resolve().parent


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _parse_key_value_text(path: Path):
    if not path.exists():
        return {}

    text = path.read_text(encoding="utf-8", errors="ignore")
    out = {}

    for key in ["SUPABASE_URL", "SUPABASE_KEY"]:
        m = re.search(rf"{key}\s*[:=]\s*[\"']?([^\"'\r\n]+)", text)
        if m:
            out[key] = m.group(1).strip()

    return out


def load_supabase_config():
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_KEY", "").strip()

    if url and key:
        return url, key

    candidates = [
        BASE_DIR / "supabase.txt",
        BASE_DIR / ".streamlit" / "secrets.toml",
        Path.home() / ".streamlit" / "secrets.toml",
    ]

    for path in candidates:
        vals = _parse_key_value_text(path)
        url = url or vals.get("SUPABASE_URL", "")
        key = key or vals.get("SUPABASE_KEY", "")
        if url and key:
            return url, key

    raise RuntimeError("SUPABASE_URL / SUPABASE_KEY が見つかりません")


def get_supabase():
    url, key = load_supabase_config()
    return create_client(url, key)


def write_status(sb, card_id="", status="idle"):
    row = {
        "bridge_id": BRIDGE_ID,
        "device_name": DEVICE_NAME,
        "card_id": str(card_id or "").strip().upper(),
        "touched_at": now_str(),
        "status": status,
    }

    try:
        updated = (
            sb.table(TABLE_NAME)
            .update(row)
            .eq("bridge_id", BRIDGE_ID)
            .execute()
        )
        if getattr(updated, "data", None):
            return
    except Exception:
        pass

    try:
        sb.table(TABLE_NAME).insert(row).execute()
    except Exception:
        sb.table(TABLE_NAME).upsert(row).execute()


def get_uid_from_first_reader():
    rlist = readers()
    if not rlist:
        raise RuntimeError("カードリーダーが見つかりません")

    reader = rlist[0]
    conn = reader.createConnection()
    conn.connect()

    cmd = [0xFF, 0xCA, 0x00, 0x00, 0x00]
    data, sw1, sw2 = conn.transmit(cmd)

    if (sw1, sw2) != (0x90, 0x00):
        raise RuntimeError(f"UID取得失敗 sw={sw1:02X}{sw2:02X}")

    return "".join(f"{b:02X}" for b in data)


def main():
    sb = get_supabase()

    print("ICブリッジ開始（Supabase）")
    print(f"bridge_id={BRIDGE_ID} device={DEVICE_NAME}")

    last_card_id = ""
    last_card_sent_ts = 0.0
    last_idle_write_ts = 0.0
    ready_until_ts = 0.0

    write_status(sb, "", "idle")
    last_idle_write_ts = time.time()

    while True:
        now_ts = time.time()

        try:
            card_id = get_uid_from_first_reader()
            card_id = str(card_id).strip().upper()

            if card_id == last_card_id and (now_ts - last_card_sent_ts) < DUPLICATE_GUARD_SEC:
                time.sleep(POLL_INTERVAL_SEC)
                continue

            last_card_id = card_id
            last_card_sent_ts = now_ts
            ready_until_ts = now_ts + READY_HOLD_SEC

            print(f"[{now_str()}] card_id={card_id}")
            write_status(sb, card_id, "ready")

            time.sleep(POLL_INTERVAL_SEC)
            continue

        except Exception:
            if now_ts < ready_until_ts:
                time.sleep(POLL_INTERVAL_SEC)
                continue

            if (now_ts - last_idle_write_ts) >= IDLE_WRITE_INTERVAL_SEC:
                try:
                    write_status(sb, last_card_id, "idle")
                    last_idle_write_ts = now_ts
                except Exception:
                    pass

            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
