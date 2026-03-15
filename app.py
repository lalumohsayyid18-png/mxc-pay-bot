import os
import json
import re
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
key = str(row[0]).strip() if len(row) > 0 else ""
val = str(row[1]).strip() if len(row) > 1 else ""
if key:
data[key] = val
except Exception:
pass
return data


def log_message(level, message, raw=""):
print(f"[{level}] {message} {raw}", flush=True)

# hanya ERROR yang disimpan ke Google Sheet
if level != "ERROR":
return

try:
ws = get_sheet("LOG")
t = now_local().strftime("%Y-%m-%d %H:%M:%S")
ws.append_row([t, level, message, raw])
except Exception:
pass


def send_message(chat_id, text, thread_id=None):
url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
payload = {
chat_id: str(chat_id),
text: text
}

if thread_id not in (None, "", "0"):
payload["message_thread_id"] = int(thread_id)

try:
res = requests.post(url, json=payload, timeout=30)
print(f"[TG] sendMessage {res.text}", flush=True)
except Exception as e:
log_message("ERROR", "sendMessage_failed", str(e))


def extract_message_text(msg):
if not msg:
return ""
return (msg.get("text") or msg.get("caption") or "").strip()


def parse_amount(value):
s = str(value).strip()
if not s:
return 0.0

s = s.replace(" ", "")

if "," in s and "." in s:
if s.rfind(",") > s.rfind("."):
# contoh: 1.234,56
s = s.replace(".", "").replace(",", ".")
else:
# contoh: 1,234.56
s = s.replace(",", "")
elif "," in s:
# contoh: 646,02
s = s.replace(",", ".")

try:
return float(s)
except Exception:
return 0.0


def get_bank_rows():
ws = get_sheet("BANK_LIST")
rows = ws.get_all_values()
out = []

for row in rows[1:]:
code = str(row[0]).strip().upper() if len(row) > 0 else ""
name = str(row[1]).strip() if len(row) > 1 else code
active = str(row[2]).strip().upper() if len(row) > 2 else ""
opening_balance = 0.0

if len(row) > 3 and str(row[3]).strip():
opening_balance = parse_amount(row[3])

if code and active == "YES":
out.append({
bankCode: code,
bankName: name or code,
openingBalance: opening_balance
})

return out


def get_bank_info(bank_code):
bank_code = str(bank_code).strip().upper()
for bank in get_bank_rows():
if bank["bankCode"] == bank_code:
return bank
return None


def get_active_bank_codes():
return [b["bankCode"] for b in get_bank_rows()]


def generate_tx_id(tx_type):
ws = get_sheet("TRANSAKSI")
rows = ws.get_all_values()
date_part = now_local().strftime("%Y%m%d")
prefix = f"{tx_type}{date_part}"
max_num = 0

for row in rows[1:]:
tx_id = str(row[2]).strip() if len(row) > 2 else ""
if tx_id.startswith(prefix):
tail = tx_id.replace(prefix, "")
if tail.isdigit():
max_num = max(max_num, int(tail))

return f"{prefix}{str(max_num + 1).zfill(4)}"


def get_bank_total_balance(bank_code):
bank_code = str(bank_code).strip().upper()

opening_balance = 0.0
for bank in get_bank_rows():
if bank["bankCode"] == bank_code:
opening_balance = float(bank.get("openingBalance", 0.0))
break

tx_ws = get_sheet("TRANSAKSI")
rows = tx_ws.get_all_values()

balance = opening_balance

for row in rows[1:]:
tx_type = str(row[3]).strip().upper() if len(row) > 3 else ""
amount = parse_amount(row[5]) if len(row) > 5 else 0.0
row_bank_code = str(row[6]).strip().upper() if len(row) > 6 else ""
status = str(row[7]).strip() if len(row) > 7 else ""

if status != "Success":
continue
if row_bank_code != bank_code:
continue

if tx_type == "IN":
balance += amount
elif tx_type == "OUT":
balance -= amount

return balance


def parse_tx_command(text):
m = re.match(r"^([+-])\s*(\d+(?:[.,]\d+)?)\s+([A-Za-z0-9_]+)$", text.strip())
if not m:
return None

return {
type: "IN" if m.group(1) == "+" else "OUT",
amount: parse_amount(m.group(2)),
bankCode: m.group(3).upper()
}


def parse_cancel_command(text):
m = re.match(r"^(?:cancel|batal)\s+([A-Za-z0-9]+)$", text.strip(), re.IGNORECASE)
if not m:
return None
return m.group(1).upper()


def cancel_transaction(tx_id):
tx_id = str(tx_id).strip().upper()
ws = get_sheet("TRANSAKSI")
rows = ws.get_all_values()

for idx, row in enumerate(rows[1:], start=2):
row_tx_id = str(row[2]).strip().upper() if len(row) > 2 else ""
if row_tx_id != tx_id:
continue

tx_date = str(row[0]).strip() if len(row) > 0 else ""
tx_time = str(row[1]).strip() if len(row) > 1 else ""
tx_type = str(row[3]).strip().upper() if len(row) > 3 else ""
member_name = str(row[4]).strip() if len(row) > 4 else ""
amount = parse_amount(row[5]) if len(row) > 5 else 0.0
bank_code = str(row[6]).strip().upper() if len(row) > 6 else ""
status = str(row[7]).strip() if len(row) > 7 else ""

if status == "Cancelled":
return {"ok": False, "reason": "already_cancelled"}

if status != "Success":
return {"ok": False, "reason": f"invalid_status_{status}"}

ws.update_cell(idx, 8, "Cancelled")

return {
ok: True,
tx_id: row_tx_id,
date: tx_date,
time: tx_time,
type: tx_type,
name: member_name,
amount: amount,
bank_code: bank_code,
status: "Cancelled"
}

return {"ok": False, "reason": "not_found"}


def build_summary_text():
tx_ws = get_sheet("TRANSAKSI")
rows = tx_ws.get_all_values()
banks = get_bank_rows()
today = now_local().strftime("%Y-%m-%d")

summary = {}
for b in banks:
summary[b["bankCode"]] = {
name: b["bankName"],
today_in: 0.0,
today_out: 0.0,
opening_balance: float(b.get("openingBalance", 0.0)),
total_balance: float(b.get("openingBalance", 0.0)),
today_in_count: 0,
today_out_count: 0
}

total_today_in = 0.0
total_today_out = 0.0
total_today_in_count = 0
total_today_out_count = 0

for row in rows[1:]:
row_date = str(row[0]).strip() if len(row) > 0 else ""
tx_type = str(row[3]).strip().upper() if len(row) > 3 else ""
amount = parse_amount(row[5]) if len(row) > 5 else 0.0
bank_code = str(row[6]).strip().upper() if len(row) > 6 else ""
status = str(row[7]).strip() if len(row) > 7 else ""

if status != "Success":
continue
if bank_code not in summary:
continue

if tx_type == "IN":
summary[bank_code]["total_balance"] += amount
elif tx_type == "OUT":
summary[bank_code]["total_balance"] -= amount

if row_date == today:
if tx_type == "IN":
summary[bank_code]["today_in"] += amount
summary[bank_code]["today_in_count"] += 1
total_today_in += amount
total_today_in_count += 1
elif tx_type == "OUT":
summary[bank_code]["today_out"] += amount
summary[bank_code]["today_out_count"] += 1
total_today_out += amount
total_today_out_count += 1

def fmt(n):
return f"{n:,.2f}"

lines = ["📊 DAILY SUMMARY", f"📅 Date: {today}", ""]

for _, item in summary.items():
lines.append(f"🏦 {item['name']}")
lines.append(f"📥 IN ({item['today_in_count']}#): {fmt(item['today_in'])}")
lines.append(f"📤 OUT ({item['today_out_count']}#): {fmt(item['today_out'])}")
lines.append(f"💰 TOTAL BALANCE: {fmt(item['total_balance'])}")
lines.append("")

lines.append("━━━━━━━━━━━━━━")
lines.append(f"📈 TOTAL IN ({total_today_in_count}#): {fmt(total_today_in)}")
lines.append(f"📉 TOTAL OUT ({total_today_out_count}#): {fmt(total_today_out)}")
lines.append(f"💵 REMAINDER: {fmt(total_today_in - total_today_out)}")

return "\n".join(lines)


def send_report_summary(settings):
report_chat_id = str(settings.get("REPORT_CHAT_ID", "")).strip()
report_topic_id = str(settings.get("REPORT_TOPIC_ID", "")).strip()
allowed_chat_id = str(settings.get("ALLOWED_CHAT_ID", "")).strip()

target_chat_id = report_chat_id or allowed_chat_id
target_thread_id = report_topic_id or None

if not target_chat_id:
return

summary_text = build_summary_text()
send_message(target_chat_id, summary_text, target_thread_id)


@app.route("/", methods=["GET"])
def home():
return "Bot running", 200


@app.route("/", methods=["POST"])
def webhook():
try:
data = request.get_json(silent=True) or {}
print(f"[INFO] incoming {json.dumps(data)}", flush=True)

message = data.get("message") or data.get("edited_message")
if not message:
return "ok", 200

settings = get_settings()
allowed_chat_id = str(settings.get("ALLOWED_CHAT_ID", "")).strip()

chat_id = str(message.get("chat", {}).get("id", "")).strip()
thread_id = message.get("message_thread_id")
text = extract_message_text(message)

print(f"[INFO] text {text}", flush=True)

if allowed_chat_id and chat_id != allowed_chat_id:
print(f"[WARN] unauthorized_chat {chat_id}", flush=True)
return "ok", 200

if not text:
return "ok", 200

if text.lower() == "where":
reply = f"CHAT_ID: {chat_id}\nTOPIC_ID: {thread_id}\nTEXT: {text}"
send_message(chat_id, reply, thread_id)
return "ok", 200

if text.lower() == "summary":
send_message(chat_id, build_summary_text(), thread_id)
return "ok", 200

cancel_tx_id = parse_cancel_command(text)
if cancel_tx_id:
result = cancel_transaction(cancel_tx_id)

if not result["ok"]:
if result["reason"] == "not_found":
send_message(chat_id, f"TX_ID {cancel_tx_id} tidak ditemukan.", thread_id)
elif result["reason"] == "already_cancelled":
send_message(chat_id, f"TX_ID {cancel_tx_id} sudah pernah dibatalkan.", thread_id)
else:
send_message(chat_id, f"TX_ID {cancel_tx_id} gagal dibatalkan.", thread_id)
return "ok", 200

bank = get_bank_info(result["bank_code"])
bank_name = bank["bankName"] if bank else result["bank_code"]
bank_balance = get_bank_total_balance(result["bank_code"])

cancel_text = (
f"❌ TRANSACTION CANCELLED\n\n"
f"TX_ID: {result['tx_id']}\n"
f"Date: {result['date']} {result['time']}\n"
f"Type: {result['type']}\n"
f"Name: {result['name']}\n"
f"Amount: {result['amount']:,.2f}\n"
f"Bank: {bank_name}\n"
f"New Bank Balance: {bank_balance:,.2f}\n"
f"Status: Cancelled"
)

send_message(chat_id, cancel_text, thread_id)

try:
send_report_summary(settings)
except Exception as e:
log_message("ERROR", "send_report_summary_cancel_failed", str(e))

return "ok", 200

cmd = parse_tx_command(text)
if not cmd:
print(f"[INFO] ignored_text {text}", flush=True)
return "ok", 200

reply_msg = message.get("reply_to_message")
if not reply_msg:
send_message(chat_id, "Reply ke pesan nama member dulu.", thread_id)
return "ok", 200

raw_name = extract_message_text(reply_msg)
full_name = raw_name.split("\n")[0].strip()

if not full_name:
send_message(chat_id, "Nama member tidak ditemukan di pesan reply.", thread_id)
return "ok", 200

bank = get_bank_info(cmd["bankCode"])
if not bank:
send_message(
chat_id,
Bank tidak valid.\nValid:  + ", ".join(get_active_bank_codes()),
thread_id
)
return "ok", 200

tx_id = generate_tx_id(cmd["type"])
now = now_local()

tx_ws = get_sheet("TRANSAKSI")
tx_ws.append_row([
now.strftime("%Y-%m-%d"),
now.strftime("%H:%M:%S"),
tx_id,
cmd["type"],
full_name,
cmd["amount"],
cmd["bankCode"],
Success
])

bank_balance = get_bank_total_balance(cmd["bankCode"])

success_text = (
f"✅ {cmd['type']} SUCCESS\n\n"
f"TX_ID: {tx_id}\n"
f"Name: {full_name}\n"
f"Amount: {cmd['amount']:,.2f}\n"
f"Bank: {bank['bankName']}\n"
f"Bank Balance: {bank_balance:,.2f}\n"
f"Status: Success"
)

send_message(chat_id, success_text, thread_id)

try:
send_report_summary(settings)
except Exception as e:
log_message("ERROR", "send_report_summary_success_failed", str(e))

return "ok", 200

except Exception as e:
log_message("ERROR", "webhook_error", str(e))
return "ok", 200


if __name__ == "__main__":
port = int(os.environ.get("PORT", 10000))
app.run(host="0.0.0.0", port=port)
