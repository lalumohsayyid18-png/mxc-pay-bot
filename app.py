import os
import json
import re
from flask import Flask, request
import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime

BOT_TOKEN = os.environ.get("BOT_TOKEN")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")

app = Flask(**name**)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS"))
creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
client = gspread.authorize(creds)

sheet = client.open_by_key(SPREADSHEET_ID)
tx_sheet = sheet.worksheet("TRANSAKSI")
bank_sheet = sheet.worksheet("BANK_LIST")

def send_message(chat_id, text, thread_id=None):
url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
payload = {"chat_id": chat_id, "text": text}

```
if thread_id:
    payload["message_thread_id"] = thread_id

requests.post(url, json=payload)
```

def get_bank_codes():
rows = bank_sheet.get_all_values()[1:]
codes = []
for r in rows:
if r[2].upper() == "YES":
codes.append(r[0].upper())
return codes

@app.route("/", methods=["POST"])
def webhook():
data = request.json

```
if "message" not in data:
    return "ok"

msg = data["message"]
text = msg.get("text", "")
chat_id = msg["chat"]["id"]
thread_id = msg.get("message_thread_id")

if text.lower() == "summary":
    rows = tx_sheet.get_all_values()[1:]

    total_in = 0
    total_out = 0

    for r in rows:
        if r[3] == "IN":
            total_in += float(r[5])
        if r[3] == "OUT":
            total_out += float(r[5])

    summary = f"""
```

DAILY SUMMARY

Total IN : {total_in}
Total OUT : {total_out}
BALANCE : {total_in-total_out}
"""

```
    send_message(chat_id, summary, thread_id)
    return "ok"

match = re.match(r"^([+-])(\\d+(?:\\.\\d+)?)\\s+(\\w+)", text)

if not match:
    return "ok"

sign = match.group(1)
amount = float(match.group(2))
bank = match.group(3).upper()

banks = get_bank_codes()

if bank not in banks:
    send_message(chat_id, "BANK CODE INVALID", thread_id)
    return "ok"

tx_type = "IN" if sign == "+" else "OUT"

now = datetime.now()

row = [
    now.strftime("%Y-%m-%d"),
    now.strftime("%H:%M:%S"),
    "",
    tx_type,
    "",
    amount,
    bank,
    "SUCCESS"
]

tx_sheet.append_row(row)

send_message(chat_id, f"{tx_type} {amount} {bank} SUCCESS", thread_id)

return "ok"
```

@app.route("/", methods=["GET"])
def home():
return "Bot running"
