import os
import json
import re
import csv
import threading
import time
from io import BytesIO
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
import requests
from flask import Flask, request
from google.oauth2.service_account import Credentials

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "").strip()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
DEFAULT_TIMEZONE = "Asia/Kuala_Lumpur"


def now_local():
    try:
        return datetime.now(ZoneInfo(DEFAULT_TIMEZONE))
    except:
        return datetime.now()


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
    data = {}
    try:
        ws = get_sheet("SETTINGS")
        rows = ws.get_all_values()
        for r in rows[1:]:
            data[r[0]] = r[1]
    except:
        pass
    return data


def log_message(level, msg, raw=""):
    print(level, msg, raw)

    if level != "ERROR":
        return

    try:
        ws = get_sheet("LOG")
        ws.append_row([
            now_local().strftime("%Y-%m-%d %H:%M:%S"),
            level,
            msg,
            raw
        ])
    except:
        pass


def send_message(chat, text, topic=None):

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": chat,
        "text": text
    }

    if topic:
        payload["message_thread_id"] = int(topic)

    try:
        requests.post(url, json=payload, timeout=30)
    except Exception as e:
        log_message("ERROR", "sendMessage", str(e))


def send_document(chat, file_bytes, filename, topic=None):

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"

    files = {
        "document": (filename, file_bytes)
    }

    data = {
        "chat_id": chat
    }

    if topic:
        data["message_thread_id"] = int(topic)

    try:
        requests.post(url, data=data, files=files, timeout=60)
    except Exception as e:
        log_message("ERROR", "sendDocument", str(e))


def extract_message_text(msg):

    if not msg:
        return ""

    return (msg.get("text") or msg.get("caption") or "").strip()


def parse_amount(v):

    try:
        return float(str(v).replace(",", ""))
    except:
        return 0


def get_bank_rows():

    ws = get_sheet("BANK_LIST")
    rows = ws.get_all_values()

    banks = []

    for r in rows[1:]:

        code = r[0].strip().upper()
        name = r[1].strip()
        active = r[2].strip().upper()

        opening = 0

        if len(r) > 3:
            opening = parse_amount(r[3])

        if active == "YES":

            banks.append({
                "bankCode": code,
                "bankName": name,
                "openingBalance": opening
            })

    return banks


def get_bank_info(code):

    for b in get_bank_rows():
        if b["bankCode"] == code:
            return b

    return None


def generate_tx_id(tx_type):

    ws = get_sheet("TRANSAKSI")
    rows = ws.get_all_values()

    prefix = tx_type + now_local().strftime("%Y%m%d")

    num = 0

    for r in rows[1:]:

        tx = r[2]

        if tx.startswith(prefix):

            tail = tx.replace(prefix, "")

            if tail.isdigit():
                num = max(num, int(tail))

    return prefix + str(num+1).zfill(4)


def get_bank_total_balance(bank):

    opening = 0

    for b in get_bank_rows():
        if b["bankCode"] == bank:
            opening = b["openingBalance"]

    ws = get_sheet("TRANSAKSI")
    rows = ws.get_all_values()

    bal = opening

    for r in rows[1:]:

        if r[7] != "Success":
            continue

        if r[6] != bank:
            continue

        amt = parse_amount(r[5])

        if r[3] == "IN":
            bal += amt
        else:
            bal -= amt

    return bal


def parse_tx_command(text):

    m = re.match(r"^([+-])\s*(\d+(?:\.\d+)?)\s+([A-Za-z0-9_]+)$", text)

    if not m:
        return None

    return {
        "type": "IN" if m.group(1) == "+" else "OUT",
        "amount": float(m.group(2)),
        "bankCode": m.group(3).upper()
    }


def parse_cancel_command(text):

    m = re.match(r"^cancel\s+([A-Za-z0-9]+)$", text, re.IGNORECASE)

    if not m:
        return None

    return m.group(1)


def cancel_transaction(tx_id):

    ws = get_sheet("TRANSAKSI")
    rows = ws.get_all_values()

    for i, r in enumerate(rows[1:], start=2):

        if r[2] == tx_id:

            if r[7] == "Cancelled":
                return "already"

            ws.update_cell(i, 8, "Cancelled")

            return "ok"

    return "notfound"


def build_summary_text():

    ws = get_sheet("TRANSAKSI")
    rows = ws.get_all_values()

    today = now_local().strftime("%Y-%m-%d")

    total_in = 0
    total_out = 0

    count_in = 0
    count_out = 0

    for r in rows[1:]:

        if r[0] != today:
            continue

        if r[7] != "Success":
            continue

        amt = parse_amount(r[5])

        if r[3] == "IN":
            total_in += amt
            count_in += 1
        else:
            total_out += amt
            count_out += 1

    return f"""📊 DAILY SUMMARY
📅 {today}

📥 IN ({count_in})
{total_in:,.2f}

📤 OUT ({count_out})
{total_out:,.2f}

💰 REMAIN
{(total_in-total_out):,.2f}
"""


def build_closing_csv():

    ws = get_sheet("TRANSAKSI")
    rows = ws.get_all_values()

    today = now_local().strftime("%Y-%m-%d")

    banks = {}

    for b in get_bank_rows():
        banks[b["bankCode"]] = {
            "name": b["bankName"],
            "IN": [],
            "OUT": []
        }

    for r in rows[1:]:

        if r[0] != today:
            continue

        if r[7] != "Success":
            continue

        code = r[6]

        if code not in banks:
            continue

        user = r[4]
        amt = parse_amount(r[5])

        banks[code][r[3]].append((user, amt))

    output = BytesIO()
    writer = csv.writer(output)

    for code, data in banks.items():

        if not data["IN"] and not data["OUT"]:
            continue

        writer.writerow(["BANK", data["name"]])
        writer.writerow(["TYPE", "USERNAME", "AMOUNT"])

        for u, a in data["IN"]:
            writer.writerow(["IN", u, a])

        for u, a in data["OUT"]:
            writer.writerow(["OUT", u, a])

        writer.writerow([])

    output.seek(0)

    return output


def run_daily_closing():

    settings = get_settings()

    chat = settings.get("ALLOWED_CHAT_ID")
    topic = settings.get("REPORT_TOPIC_ID")

    send_message(chat, "📊 DAILY CLOSING\n\n" + build_summary_text(), topic)

    csv_file = build_closing_csv()

    filename = "closing_" + now_local().strftime("%Y-%m-%d") + ".csv"

    send_document(chat, csv_file, filename, topic)


def closing_scheduler():

    while True:

        try:

            now = now_local()

            if now.hour == 23 and now.minute == 59:

                run_daily_closing()

                time.sleep(60)

        except Exception as e:
            log_message("ERROR", "closing", str(e))

        time.sleep(20)


@app.route("/", methods=["GET"])
def home():
    return "Bot running"


@app.route("/", methods=["POST"])
def webhook():

    try:

        data = request.get_json()

        message = data.get("message") or data.get("edited_message")

        if not message:
            return "ok"

        chat_id = str(message["chat"]["id"])
        text = extract_message_text(message)
        topic = message.get("message_thread_id")

        if text.lower() == "summary":

            send_message(chat_id, build_summary_text(), topic)

            return "ok"

        cancel_id = parse_cancel_command(text)

        if cancel_id:

            result = cancel_transaction(cancel_id)

            if result == "ok":
                send_message(chat_id, "❌ TX Cancelled\n" + cancel_id, topic)
            elif result == "already":
                send_message(chat_id, "TX already cancelled", topic)
            else:
                send_message(chat_id, "TX not found", topic)

            return "ok"

        cmd = parse_tx_command(text)

        if not cmd:
            return "ok"

        reply_msg = message.get("reply_to_message")

        if not reply_msg:
            send_message(chat_id, "Reply ke pesan member", topic)
            return "ok"

        name = extract_message_text(reply_msg).split("\n")[0]

        bank = get_bank_info(cmd["bankCode"])

        if not bank:
            send_message(chat_id, "Bank tidak valid", topic)
            return "ok"

        tx_id = generate_tx_id(cmd["type"])

        ws = get_sheet("TRANSAKSI")

        ws.append_row([
            now_local().strftime("%Y-%m-%d"),
            now_local().strftime("%H:%M:%S"),
            tx_id,
            cmd["type"],
            name,
            cmd["amount"],
            cmd["bankCode"],
            "Success"
        ])

        balance = get_bank_total_balance(cmd["bankCode"])

        send_message(chat_id, f"""✅ {cmd['type']} SUCCESS

TX_ID: {tx_id}
Name: {name}
Amount: {cmd['amount']}
Bank: {bank['bankName']}
Bank Balance: {balance:,.2f}
""", topic)

        return "ok"

    except Exception as e:

        log_message("ERROR", "webhook", str(e))

        return "ok"


if __name__ == "__main__":

    scheduler = threading.Thread(target=closing_scheduler)
    scheduler.daemon = True
    scheduler.start()

    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False
    )
