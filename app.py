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
        return
    payload = {
        "chat_id": str(chat_id),
        "text": text,
        "parse_mode": "HTML"
    }
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
    telegram_api("sendMessage", payload)


def format_amount(value):
    try:
        num = float(str(value).replace(",", "").strip())
        return f"{num:,.2f}"
    except Exception:
        return str(value)


def parse_amount(raw):
    text = str(raw).strip().replace(",", "")
    return float(text)


def generate_tx_id(tx_type):
    prefix = "IN" if tx_type == "IN" else "OUT"
    return f"{prefix}{now_local().strftime('%Y%m%d%H%M%S%f')[:-3]}"


def normalize_bank_code(text):
    return safe_sheet_title(text)


def parse_reply_transaction_input(text):
    """
    Format:
    +100 TERRI
    -100 TERRI
    """
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

    full_name = (reply.get("text") or reply.get("caption") or "").strip()
    return full_name


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
    """
    DATE | FULL_NAME | AMOUNT
    IN  => positif
    OUT => negatif
    """
    ws = get_bank_sheet(spreadsheet, bank_code)

    signed_amount = float(amount)
    if tx_type == "OUT":
        signed_amount = -abs(signed_amount)
    else:
        signed_amount = abs(signed_amount)

    ws.append_row(
        [today_str(), full_name, signed_amount],
        value_input_option="USER_ENTERED"
    )


def find_tx_row_by_id(ws, tx_id):
    col_values = ws.col_values(3)  # TX_ID col C
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
        return False, "TX_ID tidak ditemukan."

    data = get_row_dict_by_index(ws, row_index)
    current_status = str(data.get("STATUS", "")).strip().lower()
    if current_status == "cancelled":
        return False, "TX_ID ini sudah pernah dicancel."

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
        log_message("ERROR", f"Gagal append reversal bank sheet untuk {tx_id}: {e}")

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


def build_daily_summary(spreadsheet, target_date=None):
    target_date = target_date or today_str()
    rows = get_all_main_records(spreadsheet)

    summary = {}
    total_in = 0.0
    total_out = 0.0
    success_count = 0
    cancelled_count = 0

    for row in rows:
        if row["DATE"] != target_date:
            continue

        bank = row["BANK_CODE"] or "UNKNOWN"
        if bank not in summary:
            summary[bank] = {"IN": 0.0, "OUT": 0.0, "COUNT": 0}

        try:
            amount = parse_amount(row["AMOUNT"])
        except Exception:
            amount = 0.0

        status = row["STATUS"].strip().lower()
        if status == "success":
            success_count += 1
            summary[bank]["COUNT"] += 1
            if row["TYPE"] == "IN":
                summary[bank]["IN"] += amount
                total_in += amount
            elif row["TYPE"] == "OUT":
                summary[bank]["OUT"] += amount
                total_out += amount
        elif status == "cancelled":
            cancelled_count += 1

    lines = [f"<b>SUMMARY {target_date}</b>", ""]
    lines.append(f"Success TX: <b>{success_count}</b>")
    lines.append(f"Cancelled TX: <b>{cancelled_count}</b>")
    lines.append(f"Total IN: <b>{format_amount(total_in)}</b>")
    lines.append(f"Total OUT: <b>{format_amount(total_out)}</b>")
    lines.append(f"Net: <b>{format_amount(total_in - total_out)}</b>")
    lines.append("")

    if summary:
        for bank in sorted(summary.keys()):
            bank_in = summary[bank]["IN"]
            bank_out = summary[bank]["OUT"]
            bank_net = bank_in - bank_out
            cnt = summary[bank]["COUNT"]
            lines.append(
                f"<b>{bank}</b>\n"
                f"IN: {format_amount(bank_in)} | "
                f"OUT: {format_amount(bank_out)} | "
                f"NET: {format_amount(bank_net)} | "
                f"TX: {cnt}"
            )
    else:
        lines.append("Belum ada transaksi.")

    return "\n".join(lines)


def send_auto_report(spreadsheet):
    if not BOT_REPORT_CHAT_ID:
        return
    try:
        text = build_daily_summary(spreadsheet, today_str())
        send_message(BOT_REPORT_CHAT_ID, text)
    except Exception as e:
        log_message("ERROR", f"Gagal kirim auto report: {e}")


def handle_new_reply_transaction(chat_id, message, text):
    parsed = parse_reply_transaction_input(text)
    if not parsed:
        return False

    reply = message.get("reply_to_message")
    if not reply:
        send_message(
            chat_id,
            "Format transaksi harus reply ke pesan nama.\n\n"
            "Contoh:\n"
            "1. kirim nama: <code>abog boba</code>\n"
            "2. lalu reply: <code>+100 TERRI</code>"
        )
        return True

    full_name = extract_full_name_from_replied_message(message)
    if not full_name:
        send_message(chat_id, "Nama di pesan yang direply kosong / tidak terbaca.")
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

    signed_preview = parsed["amount"] if parsed["type"] == "IN" else -abs(parsed["amount"])

    send_message(
        chat_id,
        f"✅ <b>TRANSAKSI BERHASIL</b>\n\n"
        f"TX_ID: <code>{tx_id}</code>\n"
        f"Type: <b>{parsed['type']}</b>\n"
        f"Name: <b>{full_name}</b>\n"
        f"Bank: <b>{parsed['bank_code']}</b>\n"
        f"Amount: <b>{format_amount(parsed['amount'])}</b>\n"
        f"Catatan bank: <b>{parsed['bank_code']}</b> → {format_amount(signed_preview)}",
        reply_to_message_id=message.get("message_id")
    )

    send_auto_report(spreadsheet)
    return True


def handle_cancel(chat_id, text):
    m = re.match(r"^(?:/cancel|cancel)\s+([A-Za-z0-9]+)$", text.strip(), re.IGNORECASE)
    if not m:
        send_message(chat_id, "Format cancel salah.\nContoh: <code>/cancel IN20260314123000123</code>")
        return

    tx_id = m.group(1).strip()
    spreadsheet = get_spreadsheet()
    ok, result = cancel_transaction(spreadsheet, tx_id)

    if not ok:
        send_message(chat_id, f"❌ {result}")
        return

    send_message(
        chat_id,
        f"✅ <b>TRANSAKSI DICANCEL</b>\n\n"
        f"TX_ID: <code>{tx_id}</code>\n"
        f"Name: <b>{result.get('FULL_NAME', '')}</b>\n"
        f"Bank: <b>{result.get('BANK_CODE', '')}</b>\n"
        f"Type asal: <b>{result.get('TYPE', '')}</b>\n"
        f"Amount asal: <b>{format_amount(result.get('AMOUNT', 0))}</b>\n\n"
        f"Reversal sudah ditambahkan ke sheet bank."
    )

    send_auto_report(spreadsheet)


def handle_summary(chat_id, text):
    spreadsheet = get_spreadsheet()
    m = re.match(r"^(?:/summary|summary)(?:\s+(\d{4}-\d{2}-\d{2}))?$", text.strip(), re.IGNORECASE)
    target_date = today_str()
    if m and m.group(1):
        target_date = m.group(1)

    summary_text = build_daily_summary(spreadsheet, target_date)
    send_message(chat_id, summary_text)


def handle_help(chat_id):
    text = (
        "<b>FORMAT BOT</b>\n\n"
        "Flow input transaksi:\n"
        "1. kirim nama dulu\n"
        "<code>abog boba</code>\n\n"
        "2. lalu reply ke pesan nama itu:\n"
        "<code>+100 TERRI</code>\n"
        "<code>-100 NEXA</code>\n\n"
        "Cancel transaksi:\n"
        "<code>/cancel TX_ID</code>\n\n"
        "Summary hari ini:\n"
        "<code>/summary</code>\n\n"
        "Summary tanggal tertentu:\n"
        "<code>/summary 2026-03-14</code>"
    )
    send_message(chat_id, text)


def process_telegram_update(update):
    message = update.get("message") or update.get("edited_message") or {}
    if not message:
        return

    chat = message.get("chat", {})
    chat_id = chat.get("id")
    text = (message.get("text") or message.get("caption") or "").strip()

    if not chat_id or not text:
        return

    try:
        if re.match(r"^/(start|help)$", text, re.IGNORECASE):
            handle_help(chat_id)
            return

        if re.match(r"^(?:/summary|summary)(?:\s+\d{4}-\d{2}-\d{2})?$", text, re.IGNORECASE):
            handle_summary(chat_id, text)
            return

        if re.match(r"^(?:/cancel|cancel)\s+[A-Za-z0-9]+$", text, re.IGNORECASE):
            handle_cancel(chat_id, text)
            return

        if handle_new_reply_transaction(chat_id, message, text):
            return

    except Exception as e:
        log_message("ERROR", f"process_telegram_update error: {e}")
        send_message(chat_id, f"❌ Error: <code>{str(e)}</code>")



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
        return jsonify({"ok": False, "error": "WEBHOOK_URL belum diisi"}), 400

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
    resp = requests.post(url, json={"url": webhook_url}, timeout=30)
    return jsonify(resp.json()), 200


if __name__ == "__main__":
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN belum diisi.")
    if not SPREADSHEET_ID:
        raise ValueError("SPREADSHEET_ID belum diisi.")
    if not GOOGLE_CREDENTIALS:
        raise ValueError("GOOGLE_CREDENTIALS belum diisi.")

    try:
        ss = get_spreadsheet()
        get_main_sheet(ss)
        get_log_sheet(ss)
    except Exception as e:
        raise RuntimeError(f"Gagal akses Google Sheet: {e}")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
