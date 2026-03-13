import os
import json
import re
from datetime import datetime

import gspread
import requests
from flask import Flask, request
from google.oauth2.service_account import Credentials

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")

DEFAULT_TIMEZONE = "Asia/Kuala_Lumpur"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def get_client():
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_spreadsheet():
    client = get_client()
    return client.open_by_key(SPREADSHEET_ID)


def get_sheet(name):
    return get_spreadsheet().worksheet(name)


def get_settings():
    ws = get_sheet("SETTINGS")
    rows = ws.get_all_values()
    data = {}

    for row in rows[1:]:
        key = str(row[0]).strip() if len(row) > 0 else ""
        val = str(row[1]).strip() if len(row) > 1 else ""
        if key:
            data[key] = val

    return data


def log_message(level, message, raw=""):
    try:
        settings = get_settings()
        timezone = settings.get("TIMEZONE", DEFAULT_TIMEZONE)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ws = get_sheet("LOG")
        ws.append_row([now, level, message, raw])
    except Exception:
        pass


def get_bank_codes():
    ws = get_sheet("BANK_LIST")
    rows = ws.get_all_values()
    banks = []

    for row in rows[1:]:
        code = str(row[0]).strip().upper() if len(row) > 0 else ""
        active = str(row[2]).strip().upper() if len(row) > 2 else ""
        if code and active == "YES":
            banks.append(code)

    return banks


def get_bank_name(bank_code):
    ws = get_sheet("BANK_LIST")
    rows = ws.get_all_values()

    for row in rows[1:]:
        code = str(row[0]).strip().upper() if len(row) > 0 else ""
        name = str(row[1]).strip() if len(row) > 1 else ""
        active = str(row[2]).strip().upper() if len(row) > 2 else ""
        if code == bank_code and active == "YES":
            return name or code

    return None


def generate_tx_id(tx_type):
    ws = get_sheet("TRANSAKSI")
    rows = ws.get_all_values()
    today = datetime.now().strftime("%Y%m%d")
    prefix = f"{tx_type}{today}"
    max_num = 0

    for row in rows[1:]:
        txid = str(row[2]).strip() if len(row) > 2 else ""
        if txid.startswith(prefix):
            tail = txid.replace(prefix, "")
            if tail.isdigit():
                max_num = max(max_num, int(tail))

    next_num = str(max_num + 1).zfill(4)
    return f"{prefix}{next_num}"


def send_message(chat_id, text, thread_id=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text
    }

    if thread_id:
        payload["message_thread_id"] = int(thread_id)

    try:
        res = requests.post(url, json=payload, timeout=30)
        log_message("INFO", "sendMessage", res.text)
    except Exception as e:
        log_message("ERROR", "sendMessage_failed", str(e))


def build_summary():
    settings = get_settings()
    timezone = settings.get("TIMEZONE", DEFAULT_TIMEZONE)

    ws_tx = get_sheet("TRANSAKSI")
    ws_bank = get_sheet("BANK_LIST")

    rows = ws_tx.get_all_values()
    bank_rows = ws_bank.get_all_values()

    today = datetime.now().strftime("%Y-%m-%d")

    summary = {}
    for row in bank_rows[1:]:
        code = str(row[0]).strip().upper() if len(row) > 0 else ""
        name = str(row[1]).strip() if len(row) > 1 else code
        active = str(row[2]).strip().upper() if len(row) > 2 else ""
        if code and active == "YES":
            summary[code] = {
                "name": name,
                "in": 0.0,
                "out": 0.0,
                "in_count": 0,
                "out_count": 0
            }

    total_in = 0.0
    total_out = 0.0

    for row in rows[1:]:
        row_date = str(row[0]).strip() if len(row) > 0 else ""
        tx_type = str(row[3]).strip().upper() if len(row) > 3 else ""
        amount = float(row[5]) if len(row) > 5 and str(row[5]).strip() else 0.0
        bank_code = str(row[6]).strip().upper() if len(row) > 6 else ""
        status = str(row[7]).strip() if len(row) > 7 else ""

        if row_date != today:
            continue
        if status != "Success":
            continue
        if bank_code not in summary:
            continue

        if tx_type == "IN":
            summary[bank_code]["in"] += amount
            summary[bank_code]["in_count"] += 1
            total_in += amount
        elif tx_type == "OUT":
            summary[bank_code]["out"] += amount
            summary[bank_code]["out_count"] += 1
            total_out += amount

    lines = ["📊 DAILY SUMMARY", "", f"Date: {today}", ""]

    for bank_code, item in summary.items():
        remainder = item["in"] - item["out"]
        lines.append(
            f"{item['name']} | In({item['in_count']}#): {item['in']} | "
            f"Out({item['out_count']}#): {item['out']} | Remainder: {remainder}"
        )

    lines.append("")
    lines.append(f"Total In : {total_in}")
    lines.append(f"Total Out : {total_out}")
    lines.append(f"Remainder : {total_in - total_out}")

    return "\n".join(lines)


@app.route("/", methods=["GET"])
def home():
    return "Bot running", 200


@app.route("/", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True, silent=True) or {}
        log_message("INFO", "incoming", json.dumps(data))

        message = data.get("message") or data.get("edited_message")
        if not message:
            return "ok", 200

        settings = get_settings()

        allowed_chat_id = str(settings.get("ALLOWED_CHAT_ID", "")).strip()
        depo_topic_id = str(settings.get("DEPO_TOPIC_ID", "")).strip()
        wd_topic_id = str(settings.get("WD_TOPIC_ID", "")).strip()
        report_topic_id = str(settings.get("REPORT_TOPIC_ID", "")).strip()
        auto_summary = str(settings.get("AUTO_SUMMARY", "NO")).strip().upper()

        chat_id = str(message.get("chat", {}).get("id", ""))
        thread_id = str(message.get("message_thread_id", ""))
        text = str(message.get("text", "")).strip()

        log_message("INFO", "text", text)

        if allowed_chat_id and chat_id != allowed_chat_id:
            log_message("WARN", "unauthorized_chat", chat_id)
            return "ok", 200

        if not text:
            return "ok", 200

        if text.lower() == "where":
            reply = f"CHAT_ID: {chat_id}\nTOPIC_ID: {thread_id}\nTEXT: {text}"
            send_message(chat_id, reply, thread_id)
            return "ok", 200

        if text.lower() == "summary":
            summary_text = build_summary()
            if report_topic_id:
                send_message(chat_id, summary_text, report_topic_id)
            else:
                send_message(chat_id, summary_text, thread_id)
            return "ok", 200

        if thread_id == depo_topic_id:
            m = re.match(r"^\+\s*(\d+(?:\.\d+)?)\s+([A-Za-z0-9_]+)$", text)
            tx_type = "IN"
        elif thread_id == wd_topic_id:
            m = re.match(r"^\-\s*(\d+(?:\.\d+)?)\s+([A-Za-z0-9_]+)$", text)
            tx_type = "OUT"
        else:
            log_message("INFO", "ignored_topic", thread_id)
            return "ok", 200

        if not m:
            log_message("WARN", "command_no_match", text)
            return "ok", 200

        if "reply_to_message" not in message:
            send_message(chat_id, "Reply ke pesan nama member dulu.", thread_id)
            return "ok", 200

        amount = float(m.group(1))
        bank_code = m.group(2).upper()

        bank_name = get_bank_name(bank_code)
        if not bank_name:
            send_message(chat_id, "Bank tidak valid.", thread_id)
            return "ok", 200

        reply_text = str(message["reply_to_message"].get("text", "")).strip()
        full_name = reply_text.split("\n")[0].strip()

        if not full_name:
            send_message(chat_id, "Nama member tidak ditemukan di pesan reply.", thread_id)
            return "ok", 200

        now = datetime.now()
        date_str = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H:%M:%S")
        tx_id = generate_tx_id(tx_type)

        tx_ws = get_sheet("TRANSAKSI")
        tx_ws.append_row([
            date_str,
            time_str,
            tx_id,
            tx_type,
            full_name,
            amount,
            bank_code,
            "Success"
        ])

        success_text = (
            f"✅ {tx_type} SUCCESS\n\n"
            f"TX_ID: {tx_id}\n"
            f"Name: {full_name}\n"
            f"Amount: {amount}\n"
            f"Bank: {bank_name}\n"
            f"Status: Success"
        )
        send_message(chat_id, success_text, thread_id)

        if auto_summary == "YES":
            summary_text = build_summary()
            if report_topic_id:
                send_message(chat_id, summary_text, report_topic_id)

        return "ok", 200

    except Exception as e:
        log_message("ERROR", "webhook_error", str(e))
        return "ok", 200
