import os
import requests
from flask import Flask, request

app = Flask(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

def send_message(chat_id, text, thread_id=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text
    }
    if thread_id:
        payload["message_thread_id"] = int(thread_id)

    r = requests.post(url, json=payload, timeout=30)
    print("SEND MESSAGE:", r.status_code, r.text, flush=True)

@app.route("/", methods=["GET"])
def home():
    return "Bot running", 200

@app.route("/", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    print("INCOMING:", data, flush=True)

    message = data.get("message") or data.get("edited_message")
    if not message:
        return "ok", 200

    chat_id = message.get("chat", {}).get("id")
    thread_id = message.get("message_thread_id")
    text = (message.get("text") or "").strip()

    print("TEXT:", text, "CHAT_ID:", chat_id, "THREAD_ID:", thread_id, flush=True)

    if text:
        send_message(chat_id, f"ECHO: {text}", thread_id)

    return "ok", 200
