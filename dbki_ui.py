import os
import sys
import json
import time
import signal
import threading
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

from flask import Flask, request, redirect, url_for, render_template_string

# =========================================================
# DBKI (どぶんけえいんたーふえーす)
# ローカルPCで動かす簡易Web UI
# =========================================================

APP = Flask(__name__)
if getattr(sys, "frozen", False):
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    BASE_DIR = Path(__file__).resolve().parent
DMCI_SCRIPT = BASE_DIR / "dmci_ultimate.py"
CONFIG_FILE = BASE_DIR / "config_local.json"
DEFAULT_OUTPUT_DIR = BASE_DIR / "保存"
STATE_FILE = BASE_DIR / "dbki_state.json"
LOG_FILE = BASE_DIR / "dbki_ui.log"

LOCK = threading.RLock()
RUNNER_THREAD: Optional[threading.Thread] = None
STOP_EVENT = threading.Event()
CURRENT_PROCESS: Optional[subprocess.Popen] = None

STATE = {
    "auto_enabled": False,
    "interval_hours": 4.0,
    "last_run_started_at": None,
    "last_run_finished_at": None,
    "last_run_result": None,
    "last_run_returncode": None,
    "next_run_at": None,
    "current_status": "待機中",
    "current_pid": None,
    "save_dir": str(DEFAULT_OUTPUT_DIR),
}

HTML = """
<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <title>DBKI - DMCI 実行サイト</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; background: #f6f7fb; color: #222; }
    .wrap { max-width: 980px; margin: 0 auto; }
    .card { background: #fff; border-radius: 16px; padding: 18px 20px; box-shadow: 0 4px 16px rgba(0,0,0,.08); margin-bottom: 16px; }
    h1 { margin: 0 0 14px 0; font-size: 28px; }
    h2 { margin: 0 0 10px 0; font-size: 20px; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }
    .status { font-size: 18px; font-weight: bold; }
    .ok { color: #0a7a24; }
    .ng { color: #b11a1a; }
    .warn { color: #9a6b00; }
    .muted { color: #666; }
    button { border: 0; border-radius: 12px; padding: 12px 16px; font-size: 16px; cursor: pointer; }
    .run { background: #1677ff; color: white; }
    .stop { background: #d7263d; color: white; }
    .open { background: #2a9d8f; color: white; }
    .save { background: #6c5ce7; color: white; }
    .ghost { background: #eceff7; color: #222; }
    input[type='number'], input[type='text'] { padding: 10px 12px; font-size: 16px; border-radius: 10px; border: 1px solid #cfd5e3; }
    label { font-weight: bold; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    pre { background: #111827; color: #e5e7eb; padding: 14px; border-radius: 12px; overflow: auto; white-space: pre-wrap; word-break: break-word; }
    .small { font-size: 14px; }
    @media (max-width: 700px) { .grid { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
<div class="wrap">
  <div class="card">
    <h1>DBKI（どぶんけえいんたーふえーす(￣ｍ￣〃)ぷぷっ!）</h1>
    <div class="status {{ status_class }}">{{ state.current_status }}</div>
    <div class="muted small">dmci_ultimate.py をボタン1つで実行するローカルサイトある🥳🥳🥳。</div>
  </div>

  <div class="card">
    <h2>今の状態🤔</h2>
    <div class="grid small">
      <div><b>自動実行:</b> {{ 'ON' if state.auto_enabled else 'OFF' }}</div>
      <div><b>実行間隔:</b> {{ state.interval_hours }} 時間ごとに実行するある👀</div>
      <div><b>前回開始:</b> {{ state.last_run_started_at or 'まだないある😩' }}</div>
      <div><b>前回終了:</b> {{ state.last_run_finished_at or 'まだないある😩' }}</div>
      <div><b>前回結果:</b> {{ state.last_run_result or 'まだないある😩' }}</div>
      <div><b>返り値:</b> {{ state.last_run_returncode if state.last_run_returncode is not none else 'まだない' }}</div>
      <div><b>次回予定:</b> {{ state.next_run_at or '未設定' }}</div>
      <div><b>PID:</b> {{ state.current_pid or 'なし' }}</div>
      <div><b>保存先:</b> {{ state.save_dir }}</div>
      <div><b>スクリプト:</b> {{ script_path }}</div>
    </div>
  </div>

  <div class="card">
    <h2>手動実行</h2>
    <div class="row">
      <form method="post" action="/run"><button class="run" type="submit">今すぐ実行</button></form>
      <form method="post" action="/stop"><button class="stop" type="submit">中断</button></form>
      <form method="post" action="/open_save"><button class="open" type="submit">保存先フォルダを開く</button></form>
      <form method="post" action="/open_script"><button class="ghost" type="submit">dmciフォルダを開く</button></form>
    </div>
  </div>

  <div class="card">
    <h2>自動実行設定</h2>
    <form method="post" action="/save_settings">
      <div class="row">
        <label for="interval_hours">〇時間ごとに実行</label>
        <input id="interval_hours" name="interval_hours" type="number" min="0.1" step="0.1" value="{{ state.interval_hours }}">
      </div>
      <div class="row" style="margin-top: 12px;">
        <label for="save_dir">保存先フォルダ</label>
        <input id="save_dir" name="save_dir" type="text" style="min-width: 620px; max-width: 100%;" value="{{ state.save_dir }}">
      </div>
      <div class="row" style="margin-top: 14px;">
        <button class="save" type="submit">設定保存</button>
      </div>
    </form>

    <div class="row" style="margin-top: 16px;">
      <form method="post" action="/auto_start"><button class="run" type="submit">自動実行ON</button></form>
      <form method="post" action="/auto_stop"><button class="stop" type="submit">自動実行OFF</button></form>
    </div>
  </div>

  <div class="card">
    <h2>最近のログ</h2>
    <pre>{{ log_text }}</pre>
  </div>
</div>
</body>
</html>
"""


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")



def load_state() -> None:
    global STATE
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            STATE.update(data)
        except Exception as e:
            log(f"state読込失敗: {e}")



def save_state() -> None:
    try:
        STATE_FILE.write_text(json.dumps(STATE, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        log(f"state保存失敗: {e}")



def get_status_class() -> str:
    s = str(STATE.get("current_status", ""))
    if "実行中" in s:
        return "warn"
    if "失敗" in s or "エラー" in s:
        return "ng"
    if "完了" in s or "待機中" in s:
        return "ok"
    return "muted"



def tail_log(lines: int = 80) -> str:
    if not LOG_FILE.exists():
        return "まだログはないある。"
    try:
        text = LOG_FILE.read_text(encoding="utf-8", errors="ignore")
        return "\n".join(text.splitlines()[-lines:])
    except Exception as e:
        return f"ログ読込失敗: {e}"



def ensure_save_dir(path_str: str) -> Path:
    p = Path(path_str)
    p.mkdir(parents=True, exist_ok=True)
    return p



def update_dmci_output_dir_in_config(save_dir: str) -> None:
    if not CONFIG_FILE.exists():
        return
    try:
        cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        cfg["desktop_output_dir_name_prefix"] = "DMCI_SUPER_AUTORUN"
        # UI側で保存先を直接使うよう、別キーも一応保存
        cfg["fixed_save_dir"] = save_dir
        CONFIG_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        log(f"config_local.json更新失敗: {e}")



def patch_environment_for_run(env: dict, save_dir: str) -> dict:
    new_env = dict(env)
    new_env["DMCI_FIXED_SAVE_DIR"] = save_dir
    return new_env



def start_run() -> bool:
    global CURRENT_PROCESS
    with LOCK:
        if CURRENT_PROCESS and CURRENT_PROCESS.poll() is None:
            log("すでに実行中ある")
            return False

        if not DMCI_SCRIPT.exists():
            STATE["current_status"] = "失敗: dmci_ultimate.py がないある"
            save_state()
            return False

        save_dir = ensure_save_dir(STATE["save_dir"])
        update_dmci_output_dir_in_config(str(save_dir))

        cmd = [sys.executable, str(DMCI_SCRIPT)]
        log(f"実行開始: {' '.join(cmd)}")

        CURRENT_PROCESS = subprocess.Popen(
            cmd,
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
            env=patch_environment_for_run(os.environ, str(save_dir)),
        )

        STATE["current_status"] = "実行中ある"
        STATE["current_pid"] = CURRENT_PROCESS.pid
        STATE["last_run_started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STATE["last_run_result"] = None
        STATE["last_run_returncode"] = None
        save_state()

        threading.Thread(target=watch_process_output, daemon=True).start()
        return True



def watch_process_output() -> None:
    global CURRENT_PROCESS
    proc = CURRENT_PROCESS
    if proc is None:
        return

    try:
        if proc.stdout:
            for line in proc.stdout:
                line = line.rstrip("\n")
                if line.strip():
                    log(f"DMCI: {line}")
    except Exception as e:
        log(f"標準出力監視失敗: {e}")

    rc = None
    try:
        rc = proc.wait(timeout=10)
    except Exception:
        pass

    with LOCK:
        if rc == 0:
            STATE["current_status"] = "完了ある"
            STATE["last_run_result"] = "成功"
        else:
            STATE["current_status"] = "失敗または中断ある"
            STATE["last_run_result"] = "失敗/中断"
        STATE["last_run_returncode"] = rc
        STATE["last_run_finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STATE["current_pid"] = None
        CURRENT_PROCESS = None

        if STATE.get("auto_enabled"):
            next_dt = datetime.now() + timedelta(hours=float(STATE.get("interval_hours", 4.0)))
            STATE["next_run_at"] = next_dt.strftime("%Y-%m-%d %H:%M:%S")

        save_state()



def stop_run() -> bool:
    global CURRENT_PROCESS
    with LOCK:
        if not CURRENT_PROCESS or CURRENT_PROCESS.poll() is not None:
            STATE["current_status"] = "停止対象なしある"
            save_state()
            return False

        try:
            if os.name == "nt":
                CURRENT_PROCESS.terminate()
            else:
                CURRENT_PROCESS.send_signal(signal.SIGTERM)
            STATE["current_status"] = "中断指示を送ったある"
            save_state()
            log("中断指示送信")
            return True
        except Exception as e:
            STATE["current_status"] = f"中断失敗: {e}"
            save_state()
            log(f"中断失敗: {e}")
            return False



def scheduler_loop() -> None:
    log("自動実行スレッド開始")
    while not STOP_EVENT.is_set():
        try:
            with LOCK:
                enabled = bool(STATE.get("auto_enabled"))
                next_run_at = STATE.get("next_run_at")
                running = CURRENT_PROCESS is not None and CURRENT_PROCESS.poll() is None

            if enabled and not running:
                now = datetime.now()
                due = False

                if not next_run_at:
                    due = True
                else:
                    try:
                        due = now >= datetime.strptime(next_run_at, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        due = True

                if due:
                    ok = start_run()
                    if ok:
                        log("自動実行で開始したある")
                    else:
                        time.sleep(5)

            time.sleep(2)
        except Exception as e:
            log(f"scheduler_loop失敗: {e}")
            time.sleep(5)

    log("自動実行スレッド終了")



def ensure_scheduler_started() -> None:
    global RUNNER_THREAD
    if RUNNER_THREAD and RUNNER_THREAD.is_alive():
        return
    STOP_EVENT.clear()
    RUNNER_THREAD = threading.Thread(target=scheduler_loop, daemon=True)
    RUNNER_THREAD.start()



def open_folder(path_str: str) -> None:
    path = ensure_save_dir(path_str)
    os.startfile(str(path))



def open_script_folder() -> None:
    os.startfile(str(BASE_DIR))



@APP.route("/", methods=["GET"])
def index():
    return render_template_string(
        HTML,
        state=STATE,
        status_class=get_status_class(),
        log_text=tail_log(),
        script_path=str(DMCI_SCRIPT),
    )


@APP.route("/run", methods=["POST"])
def run_now():
    start_run()
    return redirect(url_for("index"))


@APP.route("/stop", methods=["POST"])
def stop_now():
    stop_run()
    return redirect(url_for("index"))


@APP.route("/open_save", methods=["POST"])
def open_save():
    try:
        open_folder(STATE["save_dir"])
    except Exception as e:
        log(f"保存先フォルダを開く失敗: {e}")
    return redirect(url_for("index"))


@APP.route("/open_script", methods=["POST"])
def open_script():
    try:
        open_script_folder()
    except Exception as e:
        log(f"dmciフォルダを開く失敗: {e}")
    return redirect(url_for("index"))


@APP.route("/save_settings", methods=["POST"])
def save_settings():
    try:
        interval_hours = float(request.form.get("interval_hours", "4"))
        save_dir = request.form.get("save_dir", str(DEFAULT_OUTPUT_DIR)).strip()
        if interval_hours <= 0:
            interval_hours = 1.0
        if not save_dir:
            save_dir = str(DEFAULT_OUTPUT_DIR)

        STATE["interval_hours"] = interval_hours
        STATE["save_dir"] = save_dir

        if STATE.get("auto_enabled"):
            next_dt = datetime.now() + timedelta(hours=interval_hours)
            STATE["next_run_at"] = next_dt.strftime("%Y-%m-%d %H:%M:%S")

        save_state()
        log(f"設定保存: interval={interval_hours}, save_dir={save_dir}")
    except Exception as e:
        log(f"設定保存失敗: {e}")
    return redirect(url_for("index"))


@APP.route("/auto_start", methods=["POST"])
def auto_start():
    try:
        STATE["auto_enabled"] = True
        next_dt = datetime.now() + timedelta(hours=float(STATE.get("interval_hours", 4.0)))
        STATE["next_run_at"] = next_dt.strftime("%Y-%m-%d %H:%M:%S")
        STATE["current_status"] = "自動実行ONある"
        save_state()
        ensure_scheduler_started()
        log("自動実行ON")
    except Exception as e:
        log(f"自動実行ON失敗: {e}")
    return redirect(url_for("index"))


@APP.route("/auto_stop", methods=["POST"])
def auto_stop():
    try:
        STATE["auto_enabled"] = False
        STATE["next_run_at"] = None
        STATE["current_status"] = "自動実行OFFある"
        save_state()
        log("自動実行OFF")
    except Exception as e:
        log(f"自動実行OFF失敗: {e}")
    return redirect(url_for("index"))


if __name__ == "__main__":
    ensure_save_dir(str(DEFAULT_OUTPUT_DIR))
    load_state()
    ensure_scheduler_started()
    log("DBKI起動")
    APP.run(host="127.0.0.1", port=5050, debug=False)
