import os
import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
import requests
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "").strip()
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "").strip()
BOT_REPORT_CHAT_ID = os.environ.get("BOT_REPORT_CHAT_ID", "").strip()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
DEFAULT_TIMEZONE = os.environ.get("DEFAULT_TIMEZONE", "Asia/Kuala_Lumpur")

MAIN_SHEET_NAME = "TRANSAKSI"
LOG_SHEET_NAME = "LOG"
BANK_LIST_SHEET_NAME = "BANK_LIST"

MAIN_HEADERS = ["DATE", "TIME", "TX_ID", "TYPE", "FULL_NAME", "AMOUNT", "BANK_CODE", "STATUS"]
BANK_HEADERS = ["DATE", "FULL_NAME", "AMOUNT"]


def now_local():
    try:
        return datetime.now(ZoneInfo(DEFAULT_TIMEZONE))
    except Exception:
        return datetime.now()


def today_str():
    return now_local().strftime("%Y-%m-%d")


def time_str():
    return now_local().strftime("%H:%M:%S")


def get_client():
    creds_dict = json.loads(GOOGLE_CREDENTIALS)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def get_spreadsheet():
    client = get_client()
    return client.open_by_key(SPREADSHEET_ID)


def safe_sheet_title(name: str) -> str:
    name = (name or "").strip().upper()
    name = re.sub(r"[\[\]\*\?\/\\:]", "", name)
    return name[:100] if name else "UNKNOWN"


def get_or_create_sheet(spreadsheet, title, rows=1000, cols=20):
    try:
        ws = spreadsheet.worksheet(title)
    except Exception:
        ws = spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))
    return ws


def ensure_headers(ws, headers):
    current = ws.row_values(1)
    if current != headers:
        if not current:
            ws.append_row(headers, value_input_option="USER_ENTERED")
        else:
            ws.update("A1", [headers])


def get_main_sheet(spreadsheet):
    ws = get_or_create_sheet(spreadsheet, MAIN_SHEET_NAME, rows=5000, cols=20)
    ensure_headers(ws, MAIN_HEADERS)
    return ws


def get_log_sheet(spreadsheet):
    ws = get_or_create_sheet(spreadsheet, LOG_SHEET_NAME, rows=2000, cols=10)
    if not ws.row_values(1):
        ws.append_row(["DATE", "TIME", "LEVEL", "MESSAGE"], value_input_option="USER_ENTERED")
    return ws


def get_bank_sheet(spreadsheet, bank_code):
    title = safe_sheet_title(bank_code)
    ws = get_or_create_sheet(spreadsheet, title, rows=3000, cols=5)
    ensure_headers(ws, BANK_HEADERS)
    return ws


def log_message(level, message):
    try:
        spreadsheet = get_spreadsheet()
        ws = get_log_sheet(spreadsheet)
        ws.append_row([today_str(), time_str(), level, str(message)], value_input_option="USER_ENTERED")
    except Exception:
        pass


def telegram_api(method, payload):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
    return requests.post(url, json=payload, timeout=30)


def send_message(chat_id, text, reply_to_message_id=None):
    if not BOT_TOKEN or not chat_id:
        return None
    payload = {
        "chat_id": str(chat_id),
        "text": text,
        "parse_mode": "HTML"
    }
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    return telegram_api("sendMessage", payload)


def format_amount(value):
    try:
        num = float(value)
        return f"{num:,.2f}"
    except Exception:
        return str(value)


def parse_amount(raw):
    text = str(raw).strip().replace(" ", "")
    if not text:
        return 0.0

    if "," in text and "." not in text:
        parts = text.split(",")
        if len(parts) == 2 and len(parts[1]) in (1, 2):
            text = text.replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text and "." in text:
        text = text.replace(",", "")

    return float(text)


def generate_tx_id(tx_type):
    prefix = "IN" if tx_type == "IN" else "OUT"
    return f"{prefix}{now_local().strftime('%Y%m%d%H%M%S%f')[:-3]}"


def normalize_bank_code(text):
    return safe_sheet_title(text)


def parse_reply_transaction_input(text):
    text = (text or "").strip()
    if not text:
        return None

    m = re.match(r"^([+-])\s*([\d.,]+)\s+([A-Za-z0-9_\-]+)$", text, re.IGNORECASE)
    if not m:
        return None

    sign, amount_raw, bank_code = m.groups()
    tx_type = "IN" if sign == "+" else "OUT"

    return {
        "type": tx_type,
        "amount": parse_amount(amount_raw),
        "bank_code": normalize_bank_code(bank_code)
    }


def extract_full_name_from_replied_message(message):
    reply = message.get("reply_to_message")
    if not reply:
        return ""
    return (reply.get("text") or reply.get("caption") or "").strip()


def append_main_transaction(spreadsheet, tx_id, tx_type, full_name, amount, bank_code, status="Success"):
    ws = get_main_sheet(spreadsheet)
    row = [
        today_str(),
        time_str(),
        tx_id,
        tx_type,
        full_name,
        amount,
        bank_code,
        status
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")
    return row


def append_bank_transaction(spreadsheet, bank_code, full_name, amount, tx_type):
    ws = get_bank_sheet(spreadsheet, bank_code)

    signed_amount = abs(float(amount))
    if tx_type == "OUT":
        signed_amount = -signed_amount

    ws.append_row(
        [today_str(), full_name, signed_amount],
        value_input_option="USER_ENTERED"
    )


def find_tx_row_by_id(ws, tx_id):
    col_values = ws.col_values(3)
    for idx, value in enumerate(col_values, start=1):
        if str(value).strip() == str(tx_id).strip():
            return idx
    return None


def get_row_dict_by_index(ws, row_index):
    headers = ws.row_values(1)
    values = ws.row_values(row_index)
    data = {}
    for i, key in enumerate(headers):
        data[key] = values[i] if i < len(values) else ""
    return data


def cancel_transaction(spreadsheet, tx_id):
    ws = get_main_sheet(spreadsheet)
    row_index = find_tx_row_by_id(ws, tx_id)
    if not row_index or row_index == 1:
        return False, "TX_ID not found."

    data = get_row_dict_by_index(ws, row_index)
    current_status = str(data.get("STATUS", "")).strip().lower()
    if current_status == "cancelled":
        return False, "This TX_ID has already been cancelled."

    ws.update_cell(row_index, 8, "Cancelled")

    try:
        tx_type = str(data.get("TYPE", "")).strip().upper()
        amount = parse_amount(data.get("AMOUNT", "0"))
        bank_code = normalize_bank_code(data.get("BANK_CODE", "UNKNOWN"))
        full_name = str(data.get("FULL_NAME", "")).strip()

        reverse_type = "OUT" if tx_type == "IN" else "IN"
        reversal_name = f"CANCEL {full_name}"
        append_bank_transaction(spreadsheet, bank_code, reversal_name, amount, reverse_type)
    except Exception as e:
        log_message("ERROR", f"Failed to append bank reversal for {tx_id}: {e}")

    return True, data


def get_all_main_records(spreadsheet):
    ws = get_main_sheet(spreadsheet)
    rows = ws.get_all_records()
    cleaned = []
    for row in rows:
        cleaned.append({
            "DATE": str(row.get("DATE", "")).strip(),
            "TIME": str(row.get("TIME", "")).strip(),
            "TX_ID": str(row.get("TX_ID", "")).strip(),
            "TYPE": str(row.get("TYPE", "")).strip().upper(),
            "FULL_NAME": str(row.get("FULL_NAME", "")).strip(),
            "AMOUNT": row.get("AMOUNT", 0),
            "BANK_CODE": normalize_bank_code(str(row.get("BANK_CODE", "")).strip()),
            "STATUS": str(row.get("STATUS", "")).strip()
        })
    return cleaned


def get_opening_balances(spreadsheet):
    try:
        ws = spreadsheet.worksheet(BANK_LIST_SHEET_NAME)
    except Exception:
        return {}

    rows = ws.get_all_records()
    balances = {}

    for row in rows:
        bank = normalize_bank_code(str(row.get("BANK_CODE", "")).strip())
        active = str(row.get("ACTIVE", "")).strip().upper()

        if not bank or active != "YES":
            continue

        try:
            bal = parse_amount(row.get("OPENING_BALANCE", 0))
        except Exception:
            bal = 0.0

        balances[bank] = bal

    return balances


def calculate_bank_balances(spreadsheet, target_date=None):
    target_date = target_date or today_str()
    rows = get_all_main_records(spreadsheet)
    opening_balances = get_opening_balances(spreadsheet)

    summary = {}
    total_in = 0.0
    total_out = 0.0
    success_count = 0
    cancelled_count = 0

    for bank_code in opening_balances.keys():
        summary[bank_code] = {"IN": 0.0, "OUT": 0.0, "COUNT": 0}

    for row in rows:
        row_date = str(row.get("DATE", "")).strip()
        if row_date != target_date:
            continue

        bank = normalize_bank_code(str(row.get("BANK_CODE", "")).strip()) or "UNKNOWN"
        if bank not in summary:
            summary[bank] = {"IN": 0.0, "OUT": 0.0, "COUNT": 0}

        status = str(row.get("STATUS", "")).strip().lower()
        tx_type = str(row.get("TYPE", "")).strip().upper()

        try:
            amount = parse_amount(row.get("AMOUNT", 0))
        except Exception:
            amount = 0.0

        if status == "success":
            success_count += 1
            summary[bank]["COUNT"] += 1

            if tx_type == "IN":
                summary[bank]["IN"] += amount
                total_in += amount
            elif tx_type == "OUT":
                summary[bank]["OUT"] += amount
                total_out += amount

        elif status == "cancelled":
            cancelled_count += 1

    balances = {}
    for bank in summary.keys():
        bank_in = float(summary[bank]["IN"])
        bank_out = float(summary[bank]["OUT"])
        cnt = int(summary[bank]["COUNT"])
        opening = float(opening_balances.get(bank, 0.0))
        current_balance = opening + bank_in - bank_out

        balances[bank] = {
            "BAL": current_balance,
            "IN": bank_in,
            "OUT": bank_out,
            "TX": cnt,
            "OPENING": opening
        }

    meta = {
        "success_count": success_count,
        "cancelled_count": cancelled_count,
        "total_in": total_in,
        "total_out": total_out,
        "net": total_in - total_out
    }

    return balances, meta


def build_daily_summary(spreadsheet, target_date=None):
    target_date = target_date or today_str()
    balances, meta = calculate_bank_balances(spreadsheet, target_date)

    lines = [f"<b>SUMMARY {target_date}</b>", ""]
    lines.append(f"Success TX: <b>{meta['success_count']}</b>")
    lines.append(f"Cancelled TX: <b>{meta['cancelled_count']}</b>")
    lines.append(f"Total IN: <b>{format_amount(meta['total_in'])}</b>")
    lines.append(f"Total OUT: <b>{format_amount(meta['total_out'])}</b>")
    lines.append(f"Net: <b>{format_amount(meta['net'])}</b>")
    lines.append("")

    for bank in sorted(balances.keys()):
        item = balances[bank]
        lines.append(
            f"<b>{bank}</b>\n"
            f"BAL: {format_amount(item['BAL'])} | "
            f"IN: {format_amount(item['IN'])} | "
            f"OUT: {format_amount(item['OUT'])} | "
            f"TX: {item['TX']}"
        )

    return "\n".join(lines)


def get_single_bank_balance(spreadsheet, bank_code, target_date=None):
    bank_code = normalize_bank_code(bank_code)
    balances, _ = calculate_bank_balances(spreadsheet, target_date or today_str())
    return balances.get(bank_code, {
        "BAL": 0.0,
        "IN": 0.0,
        "OUT": 0.0,
        "TX": 0,
        "OPENING": 0.0
    })


def send_auto_report(spreadsheet):
    if not BOT_REPORT_CHAT_ID:
        log_message("WARNING", "BOT_REPORT_CHAT_ID is empty, auto summary skipped.")
        return

    try:
        text = build_daily_summary(spreadsheet, today_str())
        resp = send_message(BOT_REPORT_CHAT_ID, text)
        if resp is not None and not resp.ok:
            log_message("ERROR", f"Auto summary send failed: {resp.text}")
    except Exception as e:
        log_message("ERROR", f"Failed to send auto report: {e}")


def handle_new_reply_transaction(chat_id, message, text):
    parsed = parse_reply_transaction_input(text)
    if not parsed:
        return False

    reply = message.get("reply_to_message")
    if not reply:
        send_message(
            chat_id,
            "Please reply to the member name first.\n\nExample:\n<code>Walter jay</code>\nThen reply with:\n<code>+100 TERRI</code>",
            reply_to_message_id=message.get("message_id")
        )
        return True

    full_name = extract_full_name_from_replied_message(message)
    if not full_name:
        send_message(
            chat_id,
            "The replied message has no readable name.",
            reply_to_message_id=message.get("message_id")
        )
        return True

    spreadsheet = get_spreadsheet()
    tx_id = generate_tx_id(parsed["type"])

    append_main_transaction(
        spreadsheet=spreadsheet,
        tx_id=tx_id,
        tx_type=parsed["type"],
        full_name=full_name,
        amount=parsed["amount"],
        bank_code=parsed["bank_code"],
        status="Success"
    )

    append_bank_transaction(
        spreadsheet=spreadsheet,
        bank_code=parsed["bank_code"],
        full_name=full_name,
        amount=parsed["amount"],
        tx_type=parsed["type"]
    )

    bank_balance = get_single_bank_balance(spreadsheet, parsed["bank_code"], today_str())

    send_message(
        chat_id,
        f"✅ <b>TRANSACTION SUCCESS</b>\n\n"
        f"Member  : <b>{full_name}</b>\n"
        f"Bank    : <b>{parsed['bank_code']}</b>\n"
        f"Type    : <b>{parsed['type']}</b>\n"
        f"Amount  : <b>{format_amount(parsed['amount'])}</b>\n"
        f"Balance : <b>{format_amount(bank_balance['BAL'])}</b>\n\n"
        f"TX_ID   : <code>{tx_id}</code>",
        reply_to_message_id=message.get("message_id")
    )

    send_auto_report(spreadsheet)
    return True


def handle_cancel(chat_id, text, reply_to_message_id=None):
    m = re.match(r"^(?:/cancel|cancel)\s+([A-Za-z0-9]+)$", text.strip(), re.IGNORECASE)
    if not m:
        send_message(
            chat_id,
            "Invalid cancel format.\nExample: <code>/cancel IN20260314123000123</code>",
            reply_to_message_id=reply_to_message_id
        )
        return

    tx_id = m.group(1).strip()
    spreadsheet = get_spreadsheet()
    ok, result = cancel_transaction(spreadsheet, tx_id)

    if not ok:
        send_message(chat_id, f"❌ {result}", reply_to_message_id=reply_to_message_id)
        return

    bank_code = normalize_bank_code(result.get("BANK_CODE", ""))
    bank_balance = get_single_bank_balance(spreadsheet, bank_code, today_str())

    send_message(
        chat_id,
        f"✅ <b>TRANSACTION CANCELLED</b>\n\n"
        f"Member  : <b>{result.get('FULL_NAME', '')}</b>\n"
        f"Bank    : <b>{bank_code}</b>\n"
        f"Type    : <b>{result.get('TYPE', '')}</b>\n"
        f"Amount  : <b>{format_amount(parse_amount(result.get('AMOUNT', 0)))}</b>\n"
        f"Balance : <b>{format_amount(bank_balance['BAL'])}</b>\n\n"
        f"TX_ID   : <code>{tx_id}</code>",
        reply_to_message_id=reply_to_message_id
    )

    send_auto_report(spreadsheet)


def handle_summary(chat_id, text, reply_to_message_id=None):
    spreadsheet = get_spreadsheet()
    m = re.match(r"^(?:/summary|summary)(?:\s+(\d{4}-\d{2}-\d{2}))?$", text.strip(), re.IGNORECASE)
    target_date = today_str()
    if m and m.group(1):
        target_date = m.group(1)

    summary_text = build_daily_summary(spreadsheet, target_date)
    send_message(chat_id, summary_text, reply_to_message_id=reply_to_message_id)


def handle_help(chat_id, reply_to_message_id=None):
    text = (
        "<b>BOT FORMAT</b>\n\n"
        "1. Send member name\n"
        "<code>Walter jay</code>\n\n"
        "2. Reply to that message with:\n"
        "<code>+100 TERRI</code>\n"
        "<code>-100 NEXA</code>\n"
        "<code>-500.27 HWD</code>\n\n"
        "Cancel transaction:\n"
        "<code>/cancel TX_ID</code>\n\n"
        "Summary:\n"
        "<code>/summary</code>\n"
        "<code>/summary 2026-03-15</code>"
    )
    send_message(chat_id, text, reply_to_message_id=reply_to_message_id)


def process_telegram_update(update):
    message = update.get("message") or update.get("edited_message") or {}
    if not message:
        return

    chat = message.get("chat", {})
    chat_id = chat.get("id")
    text = (message.get("text") or message.get("caption") or "").strip()
    message_id = message.get("message_id")

    if not chat_id or not text:
        return

    try:
        if re.match(r"^/(start|help)$", text, re.IGNORECASE):
            handle_help(chat_id, reply_to_message_id=message_id)
            return

        if re.match(r"^(?:/summary|summary)(?:\s+\d{4}-\d{2}-\d{2})?$", text, re.IGNORECASE):
            handle_summary(chat_id, text, reply_to_message_id=message_id)
            return

        if re.match(r"^(?:/cancel|cancel)\s+[A-Za-z0-9]+$", text, re.IGNORECASE):
            handle_cancel(chat_id, text, reply_to_message_id=message_id)
            return

        if parse_reply_transaction_input(text):
            handle_new_reply_transaction(chat_id, message, text)
            return

        return

    except Exception as e:
        log_message("ERROR", f"process_telegram_update error: {e}")
        send_message(chat_id, f"❌ Error: <code>{str(e)}</code>", reply_to_message_id=message_id)


@app.route("/", methods=["GET"])
def home():
    return jsonify({"ok": True, "message": "Telegram payment gateway bot running"}), 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        update = request.get_json(force=True, silent=True) or {}
        process_telegram_update(update)
        return jsonify({"ok": True}), 200
    except Exception as e:
        log_message("ERROR", f"webhook error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/set_webhook", methods=["GET"])
def set_webhook():
    webhook_url = os.environ.get("WEBHOOK_URL", "").strip()
    if not webhook_url:
        return jsonify({"ok": False, "error": "WEBHOOK_URL is not set"}), 400

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
    resp = requests.post(url, json={"url": webhook_url}, timeout=30)
    return jsonify(resp.json()), 200


if __name__ == "__main__":
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN is not set.")
    if not SPREADSHEET_ID:
        raise ValueError("SPREADSHEET_ID is not set.")
    if not GOOGLE_CREDENTIALS:
        raise ValueError("GOOGLE_CREDENTIALS is not set.")

    try:
        ss = get_spreadsheet()
        get_main_sheet(ss)
        get_log_sheet(ss)
    except Exception as e:
        raise RuntimeError(f"Failed to access Google Sheet: {e}")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
