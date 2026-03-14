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
    except Exception:
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
        for row in rows[1:]:
            key = str(row[0]).strip()
            val = str(row[1]).strip()
            if key:
                data[key] = val
    except:
        pass
    return data


def log_message(level, message, raw=""):

    print(f"[{level}] {message} {raw}", flush=True)

    if level != "ERROR":
        return

    try:
        ws = get_sheet("LOG")
        ws.append_row([
            now_local().strftime("%Y-%m-%d %H:%M:%S"),
            level,
            message,
            raw
        ])
    except:
        pass


def send_message(chat_id, text, thread_id=None):

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": str(chat_id),
        "text": text
    }

    if thread_id:
        payload["message_thread_id"] = int(thread_id)

    try:
        requests.post(url, json=payload, timeout=30)
    except Exception as e:
        log_message("ERROR", "sendMessage_failed", str(e))


def send_document(chat_id, file_bytes, filename, thread_id=None):

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"

    files = {
        "document": (filename, file_bytes)
    }

    data = {
        "chat_id": str(chat_id)
    }

    if thread_id:
        data["message_thread_id"] = int(thread_id)

    try:
        requests.post(url, data=data, files=files, timeout=60)
    except Exception as e:
        log_message("ERROR", "sendDocument_failed", str(e))


def extract_message_text(msg):

    if not msg:
        return ""

    return (msg.get("text") or msg.get("caption") or "").strip()


def parse_amount(value):

    s = str(value).replace(",", "").strip()

    try:
        return float(s)
    except:
        return 0


def get_bank_rows():

    ws = get_sheet("BANK_LIST")
    rows = ws.get_all_values()

    out = []

    for row in rows[1:]:

        code = str(row[0]).strip().upper()
        name = str(row[1]).strip()
        active = str(row[2]).strip().upper()

        opening = 0

        if len(row) > 3:
            opening = parse_amount(row[3])

        if code and active == "YES":

            out.append({
                "bankCode": code,
                "bankName": name,
                "openingBalance": opening
            })

    return out


def get_bank_info(code):

    for b in get_bank_rows():
        if b["bankCode"] == code:
            return b

    return None


def generate_tx_id(tx_type):

    ws = get_sheet("TRANSAKSI")

    rows = ws.get_all_values()

    prefix = f"{tx_type}{now_local().strftime('%Y%m%d')}"

    num = 0

    for r in rows[1:]:

        tx = str(r[2]).strip()

        if tx.startswith(prefix):

            n = tx.replace(prefix, "")

            if n.isdigit():
                num = max(num, int(n))

    return f"{prefix}{str(num+1).zfill(4)}"


def get_bank_total_balance(bank_code):

    opening = 0

    for b in get_bank_rows():
        if b["bankCode"] == bank_code:
            opening = b["openingBalance"]

    ws = get_sheet("TRANSAKSI")
    rows = ws.get_all_values()

    bal = opening

    for r in rows[1:]:

        status = str(r[7]).strip()

        if status != "Success":
            continue

        code = str(r[6]).strip()

        if code != bank_code:
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

    send_message(chat, "📊 DAILY CLOSING\n\n"+build_summary_text(), topic)

    csv_file = build_closing_csv()

    filename = f"closing_{now_local().strftime('%Y-%m-%d')}.csv"

    send_document(chat, csv_file, filename, topic)


def closing_scheduler():

    while True:

        try:

            now = now_local()

            if now.hour == 23 and now.minute == 59:

                run_daily_closing()

                time.sleep(60)

        except Exception as e:
            log_message("ERROR","closing_scheduler",str(e))

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

        settings = get_settings()

        chat_id = str(message["chat"]["id"])
        text = extract_message_text(message)

        thread_id = message.get("message_thread_id")

        if text.lower() == "summary":

            send_message(chat_id, build_summary_text(), thread_id)

            return "ok"

        cmd = parse_tx_command(text)

        if not cmd:
            return "ok"

        reply_msg = message.get("reply_to_message")

        if not reply_msg:
            send_message(chat_id,"Reply ke nama member",thread_id)
            return "ok"

        name = extract_message_text(reply_msg).split("\n")[0]

        bank = get_bank_info(cmd["bankCode"])

        if not bank:
            send_message(chat_id,"Bank tidak valid",thread_id)
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

        send_message(chat_id,f"""✅ {cmd['type']} SUCCESS

TX_ID: {tx_id}
Name: {name}
Amount: {cmd['amount']}
Bank: {bank['bankName']}
Bank Balance: {balance:,.2f}
""",thread_id)

        return "ok"

    except Exception as e:

        log_message("ERROR","webhook_error",str(e))

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
