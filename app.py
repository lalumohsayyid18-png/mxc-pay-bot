import os
import json
import re
import csv
import threading
import time
from io import StringIO, BytesIO
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
        for r in rows[1:]:
            key = str(r[0]).strip() if len(r) > 0 else ""
            val = str(r[1]).strip() if len(r) > 1 else ""
            if key:
                data[key] = val
    except Exception:
        pass
    return data


def log_message(level, msg, raw=""):
    print(f"[{level}] {msg} {raw}", flush=True)

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
    except Exception:
        pass


def send_message(chat, text, topic=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": str(chat),
        "text": text
    }

    if topic not in (None, "", 0, "0"):
        payload["message_thread_id"] = int(topic)

    try:
        requests.post(url, json=payload, timeout=30)
    except Exception as e:
        log_message("ERROR", "sendMessage_failed", str(e))


def send_document(chat, file_bytes, filename, topic=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"

    data = {
        "chat_id": str(chat)
    }

    if topic not in (None, "", 0, "0"):
        data["message_thread_id"] = int(topic)

    if isinstance(file_bytes, StringIO):
        content = file_bytes.getvalue().encode("utf-8-sig")
    elif isinstance(file_bytes, BytesIO):
        content = file_bytes.getvalue()
    else:
        content = str(file_bytes).encode("utf-8-sig")

    files = {
        "document": (filename, content)
    }

    try:
        requests.post(url, data=data, files=files, timeout=60)
    except Exception as e:
        log_message("ERROR", "sendDocument_failed", str(e))


def extract_message_text(msg):
    if not msg:
        return ""
    return (msg.get("text") or msg.get("caption") or "").strip()


def parse_amount(v):
    s = str(v).strip()
    if not s:
        return 0.0

    s = s.replace(" ", "")

    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except Exception:
        return 0.0


def get_bank_rows():
    ws = get_sheet("BANK_LIST")
    rows = ws.get_all_values()

    banks = []

    for r in rows[1:]:
        code = str(r[0]).strip().upper() if len(r) > 0 else ""
        name = str(r[1]).strip() if len(r) > 1 else code
        active = str(r[2]).strip().upper() if len(r) > 2 else ""
        opening = parse_amount(r[3]) if len(r) > 3 else 0.0

        if code and active == "YES":
            banks.append({
                "bankCode": code,
                "bankName": name or code,
                "openingBalance": opening
            })

    return banks


def get_bank_info(code):
    code = str(code).strip().upper()
    for b in get_bank_rows():
        if b["bankCode"] == code:
            return b
    return None


def get_active_bank_codes():
    return [b["bankCode"] for b in get_bank_rows()]


def generate_tx_id(tx_type):
    ws = get_sheet("TRANSAKSI")
    rows = ws.get_all_values()

    prefix = f"{tx_type}{now_local().strftime('%Y%m%d')}"
    num = 0

    for r in rows[1:]:
        tx = str(r[2]).strip() if len(r) > 2 else ""
        if tx.startswith(prefix):
            tail = tx.replace(prefix, "")
            if tail.isdigit():
                num = max(num, int(tail))

    return prefix + str(num + 1).zfill(4)


def get_bank_total_balance(bank_code):
    bank_code = str(bank_code).strip().upper()
    opening = 0.0

    for b in get_bank_rows():
        if b["bankCode"] == bank_code:
            opening = b["openingBalance"]
            break

    ws = get_sheet("TRANSAKSI")
    rows = ws.get_all_values()

    balance = opening

    for r in rows[1:]:
        status = str(r[7]).strip() if len(r) > 7 else ""
        if status != "Success":
            continue

        code = str(r[6]).strip().upper() if len(r) > 6 else ""
        if code != bank_code:
            continue

        amt = parse_amount(r[5]) if len(r) > 5 else 0.0
        tx_type = str(r[3]).strip().upper() if len(r) > 3 else ""

        if tx_type == "IN":
            balance += amt
        elif tx_type == "OUT":
            balance -= amt

    return balance


def parse_tx_command(text):
    m = re.match(r"^([+-])\s*(\d+(?:[.,]\d+)?)\s+([A-Za-z0-9_]+)$", text.strip())
    if not m:
        return None

    return {
        "type": "IN" if m.group(1) == "+" else "OUT",
        "amount": parse_amount(m.group(2)),
        "bankCode": m.group(3).upper()
    }


def parse_cancel_command(text):
    m = re.match(r"^cancel\s+([A-Za-z0-9]+)$", text.strip(), re.IGNORECASE)
    if not m:
        return None
    return m.group(1).upper()


def cancel_transaction(tx_id):
    ws = get_sheet("TRANSAKSI")
    rows = ws.get_all_values()

    for i, r in enumerate(rows[1:], start=2):
        row_tx_id = str(r[2]).strip().upper() if len(r) > 2 else ""

        if row_tx_id == tx_id:
            status = str(r[7]).strip() if len(r) > 7 else ""

            if status == "Cancelled":
                return "already"

            if status != "Success":
                return "invalid"

            ws.update_cell(i, 8, "Cancelled")
            return "ok"

    return "notfound"


def build_summary_text():
    ws = get_sheet("TRANSAKSI")
    rows = ws.get_all_values()
    today = now_local().strftime("%Y-%m-%d")

    banks = get_bank_rows()
    summary = {}

    total_in = 0.0
    total_out = 0.0
    count_in = 0
    count_out = 0

    for b in banks:
        summary[b["bankCode"]] = {
            "name": b["bankName"],
            "in": 0.0,
            "out": 0.0,
            "in_count": 0,
            "out_count": 0,
            "balance": get_bank_total_balance(b["bankCode"])
        }

    for r in rows[1:]:
        row_date = str(r[0]).strip() if len(r) > 0 else ""
        tx_type = str(r[3]).strip().upper() if len(r) > 3 else ""
        amount = parse_amount(r[5]) if len(r) > 5 else 0.0
        bank_code = str(r[6]).strip().upper() if len(r) > 6 else ""
        status = str(r[7]).strip() if len(r) > 7 else ""

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
            count_in += 1
        elif tx_type == "OUT":
            summary[bank_code]["out"] += amount
            summary[bank_code]["out_count"] += 1
            total_out += amount
            count_out += 1

    lines = [
        "📊 DAILY SUMMARY",
        f"📅 {today}",
        ""
    ]

    for _, item in summary.items():
        lines.append(f"🏦 {item['name']}")
        lines.append(f"📥 IN ({item['in_count']}#): {item['in']:,.2f}")
        lines.append(f"📤 OUT ({item['out_count']}#): {item['out']:,.2f}")
        lines.append(f"💰 TOTAL BALANCE: {item['balance']:,.2f}")
        lines.append("")

    lines.append("━━━━━━━━━━━━━━")
    lines.append(f"📈 TOTAL IN ({count_in}#): {total_in:,.2f}")
    lines.append(f"📉 TOTAL OUT ({count_out}#): {total_out:,.2f}")
    lines.append(f"💵 REMAINDER: {(total_in - total_out):,.2f}")

    return "\n".join(lines)


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
        if str(r[0]).strip() != today:
            continue

        if str(r[7]).strip() != "Success":
            continue

        code = str(r[6]).strip().upper() if len(r) > 6 else ""
        if code not in banks:
            continue

        user = str(r[4]).strip() if len(r) > 4 else ""
        amt = parse_amount(r[5]) if len(r) > 5 else 0.0
        tx_type = str(r[3]).strip().upper() if len(r) > 3 else ""

        if tx_type in ("IN", "OUT"):
            banks[code][tx_type].append((user, amt))

    output = StringIO()
    writer = csv.writer(output)

    for _, data in banks.items():
        if not data["IN"] and not data["OUT"]:
            continue

        writer.writerow(["BANK", data["name"]])
        writer.writerow(["TYPE", "USERNAME", "AMOUNT"])

        for u, a in data["IN"]:
            writer.writerow(["IN", u, f"{a:.2f}"])

        for u, a in data["OUT"]:
            writer.writerow(["OUT", u, f"{a:.2f}"])

        writer.writerow([])

    output.seek(0)
    return output


def send_auto_summary():
    settings = get_settings()
    auto_summary = str(settings.get("AUTO_SUMMARY", "")).strip().upper()
    chat = str(settings.get("ALLOWED_CHAT_ID", "")).strip()
    topic = str(settings.get("REPORT_TOPIC_ID", "")).strip()

    if auto_summary != "YES":
        return

    if not chat or not topic:
        return

    send_message(chat, build_summary_text(), topic)


def run_daily_closing():
    settings = get_settings()
    chat = str(settings.get("ALLOWED_CHAT_ID", "")).strip()
    topic = str(settings.get("REPORT_TOPIC_ID", "")).strip()

    if not chat or not topic:
        return

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
            log_message("ERROR", "closing_scheduler_error", str(e))

        time.sleep(20)


@app.route("/", methods=["GET"])
def home():
    return "Bot running", 200


@app.route("/", methods=["POST"])
def webhook():
    try:
        data = request.get_json(silent=True) or {}
        message = data.get("message") or data.get("edited_message")

        if not message:
            return "ok", 200

        chat_id = str(message.get("chat", {}).get("id", "")).strip()
        text = extract_message_text(message)
        topic = message.get("message_thread_id")

        if not text:
            return "ok", 200

        if text.lower() == "where":
            send_message(chat_id, f"CHAT_ID: {chat_id}\nTOPIC_ID: {topic}\nTEXT: {text}", topic)
            return "ok", 200

        if text.lower() == "summary":
            send_message(chat_id, build_summary_text(), topic)
            return "ok", 200

        cancel_id = parse_cancel_command(text)
        if cancel_id:
            result = cancel_transaction(cancel_id)

            if result == "ok":
                send_message(chat_id, "❌ TX Cancelled\n" + cancel_id, topic)
                try:
                    send_auto_summary()
                except Exception as e:
                    log_message("ERROR", "auto_summary_after_cancel_failed", str(e))
            elif result == "already":
                send_message(chat_id, "TX already cancelled", topic)
            elif result == "invalid":
                send_message(chat_id, "TX status not valid for cancel", topic)
            else:
                send_message(chat_id, "TX not found", topic)

            return "ok", 200

        cmd = parse_tx_command(text)
        if not cmd:
            return "ok", 200

        reply_msg = message.get("reply_to_message")
        if not reply_msg:
            send_message(chat_id, "Reply ke pesan member", topic)
            return "ok", 200

        name = extract_message_text(reply_msg).split("\n")[0].strip()
        if not name:
            send_message(chat_id, "Nama member tidak ditemukan", topic)
            return "ok", 200

        bank = get_bank_info(cmd["bankCode"])
        if not bank:
            send_message(chat_id, "Bank tidak valid", topic)
            return "ok", 200

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

        send_message(
            chat_id,
            f"""✅ {cmd['type']} SUCCESS

TX_ID: {tx_id}
Name: {name}
Amount: {cmd['amount']:,.2f}
Bank: {bank['bankName']}
Bank Balance: {balance:,.2f}
""",
            topic
        )

        try:
            send_auto_summary()
        except Exception as e:
            log_message("ERROR", "auto_summary_after_tx_failed", str(e))

        return "ok", 200

    except Exception as e:
        log_message("ERROR", "webhook_error", str(e))
        return "ok", 200


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
