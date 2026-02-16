#!/usr/bin/env python3
"""
Single-file Telegram File Bot (webhook) + Flask dashboard
- python-telegram-bot v20+
- pg8000 for PostgreSQL
- Flask for health/dashboard + webhook endpoint
"""

import os
import sys
import time
import ssl
import logging
import threading
import asyncio
import urllib.parse
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import pg8000
from contextlib import suppress
from flask import Flask, request, jsonify, render_template_string

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ----------------- Configuration & Logging -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("telegram-file-bot")
log.setLevel(logging.INFO)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
DATABASE_URL = os.environ.get("DATABASE_URL", "")
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
WEBHOOK_PATH = "/telegram-webhook"
WEBHOOK_URL = f"{RENDER_EXTERNAL_URL}{WEBHOOK_PATH}" if RENDER_EXTERNAL_URL else ""
PORT = int(os.environ.get("PORT", "5000"))
BOT_USERNAME = os.environ.get("BOT_USERNAME", "file_bot")  # optional fallback

DELETE_AFTER = int(os.environ.get("DELETE_AFTER", str(600)))  # seconds
PLAYABLE_EXTS = {"mp4", "mov", "m4v", "mpeg", "mpg"}
ALL_VIDEO_EXTS = {
    "mp4", "mkv", "mov", "avi", "webm", "flv", "m4v", "3gp", "wmv", "mpg", "mpeg"
}

if not BOT_TOKEN or not ADMIN_ID or not DATABASE_URL or not RENDER_EXTERNAL_URL:
    log.error("Missing required environment variables. Please set BOT_TOKEN, ADMIN_ID, DATABASE_URL, and RENDER_EXTERNAL_URL.")
    sys.exit(1)

# --------------- Flask App (dashboard + webhook) ----------------
app = Flask(__name__)
start_time = time.time()

# ----------------- Database helper (sync calls via to_thread) -----------------
class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.conn = None
        self.connection_params = self.parse_db_url(db_url)
        self._lock = threading.Lock()
        self.initialized = False
        log.info(f"Parsed DB params: {self.connection_params}")

    def parse_db_url(self, db_url: str):
        s = db_url
        s = s.replace("postgresql://", "").replace("postgres://", "")
        try:
            user_pass, host_port_db = s.split("@", 1)
            user, password = user_pass.split(":", 1)
            # URL decode
            password = urllib.parse.unquote(password)
            if "/" in host_port_db:
                host_port, database = host_port_db.split("/", 1)
            else:
                host_port = host_port_db
                database = "postgres"
            if ":" in host_port:
                host, port = host_port.split(":", 1)
                port = int(port)
            else:
                host = host_port
                port = 5432
            return {"user": user, "password": password, "host": host, "port": port, "database": database}
        except Exception as e:
            log.error("Failed parsing DATABASE_URL", exc_info=True)
            raise

    def create_ssl_context(self):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def connect_sync(self, database: Optional[str] = None):
        params = self.connection_params.copy()
        if database:
            params["database"] = database
        ctx = self.create_ssl_context()
        return pg8000.connect(
            user=params["user"],
            password=params["password"],
            host=params["host"],
            port=params["port"],
            database=params["database"],
            ssl_context=ctx,
            timeout=30
        )

    async def ensure_database(self):
        # ensure database exists; create if necessary
        def _ensure():
            params = self.connection_params
            target = params["database"]
            try:
                conn = self.connect_sync(target)
                conn.close()
                return True
            except Exception as e:
                err = str(e).lower()
                # Try to connect to postgres and create
                try:
                    conn = self.connect_sync("postgres")
                    cur = conn.cursor()
                    conn.autocommit = True
                    cur.execute(f'CREATE DATABASE "{target}"')
                    conn.close()
                    return True
                except Exception as e2:
                    log.error("Could not create or access database", exc_info=True)
                    raise

        return await asyncio.to_thread(_ensure)

    async def get_connection(self):
        with self._lock:
            if self.conn is None:
                # create a persistent connection (sync)
                self.conn = await asyncio.to_thread(self.connect_sync, self.connection_params.get("database"))
        return self.conn

    async def init_db(self):
        conn = await self.get_connection()
        def _init():
            cur = conn.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS files (
                    id SERIAL PRIMARY KEY,
                    file_id TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    mime_type TEXT,
                    is_video INTEGER DEFAULT 0,
                    file_size BIGINT DEFAULT 0,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    access_count INTEGER DEFAULT 0
                )
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS membership_cache (
                    user_id BIGINT,
                    channel TEXT,
                    is_member INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, channel)
                )
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_deletions (
                    chat_id BIGINT NOT NULL,
                    message_id INTEGER NOT NULL,
                    scheduled_time TIMESTAMP NOT NULL,
                    delete_after INTEGER DEFAULT 600,
                    PRIMARY KEY (chat_id, message_id)
                )
            ''')
            cur.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_interactions INTEGER DEFAULT 1,
                    total_files_accessed INTEGER DEFAULT 0,
                    last_file_accessed TIMESTAMP
                )
            ''')
            conn.commit()
        await asyncio.to_thread(_init)
        self.initialized = True

    # DB helpers (save/get/delete)
    async def save_file(self, file_id: str, file_info: dict) -> str:
        conn = await self.get_connection()
        def _save():
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO files (file_id, file_name, mime_type, is_video, file_size, access_count) VALUES (%s, %s, %s, %s, %s, 0) RETURNING id",
                (file_id, file_info.get("file_name", ""), file_info.get("mime_type", ""), 1 if file_info.get("is_video") else 0, file_info.get("size", 0))
            )
            new_id = cur.fetchone()[0]
            conn.commit()
            return str(new_id)
        return await asyncio.to_thread(_save)

    async def get_file(self, file_key: str) -> Optional[dict]:
        try:
            key = int(file_key)
        except:
            return None
        conn = await self.get_connection()
        def _get():
            cur = conn.cursor()
            cur.execute("UPDATE files SET access_count = access_count + 1 WHERE id = %s RETURNING file_id, file_name, mime_type, is_video, file_size, TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as ts, access_count", (key,))
            row = cur.fetchone()
            if row:
                conn.commit()
                return {
                    "file_id": row[0],
                    "file_name": row[1],
                    "mime_type": row[2],
                    "is_video": bool(row[3]),
                    "size": row[4],
                    "timestamp": row[5],
                    "access_count": row[6]
                }
            return None
        return await asyncio.to_thread(_get)

    async def delete_file(self, file_key: str) -> bool:
        try:
            key = int(file_key)
        except:
            return False
        conn = await self.get_connection()
        def _del():
            cur = conn.cursor()
            cur.execute("DELETE FROM files WHERE id = %s", (key,))
            rc = cur.rowcount
            conn.commit()
            return rc > 0
        return await asyncio.to_thread(_del)

    async def get_file_count(self) -> int:
        conn = await self.get_connection()
        def _cnt():
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM files")
            return cur.fetchone()[0]
        return await asyncio.to_thread(_cnt)

    async def get_user_count(self) -> int:
        conn = await self.get_connection()
        def _cnt():
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            return cur.fetchone()[0]
        return await asyncio.to_thread(_cnt)

    async def schedule_deletion(self, chat_id: int, message_id: int):
        conn = await self.get_connection()
        scheduled = datetime.now() + timedelta(seconds=DELETE_AFTER)
        def _sched():
            cur = conn.cursor()
            cur.execute("INSERT INTO scheduled_deletions (chat_id, message_id, scheduled_time, delete_after) VALUES (%s,%s,%s,%s) ON CONFLICT (chat_id, message_id) DO UPDATE SET scheduled_time=EXCLUDED.scheduled_time", (chat_id, message_id, scheduled, DELETE_AFTER))
            conn.commit()
        return await asyncio.to_thread(_sched)

    async def get_due_messages(self):
        conn = await self.get_connection()
        def _get():
            cur = conn.cursor()
            cur.execute("SELECT chat_id, message_id FROM scheduled_deletions WHERE scheduled_time <= CURRENT_TIMESTAMP")
            rows = cur.fetchall()
            return rows
        return await asyncio.to_thread(_get)

    async def remove_scheduled(self, chat_id: int, message_id: int):
        conn = await self.get_connection()
        def _r():
            cur = conn.cursor()
            cur.execute("DELETE FROM scheduled_deletions WHERE chat_id=%s AND message_id=%s", (chat_id, message_id))
            conn.commit()
        return await asyncio.to_thread(_r)

    async def update_user_interaction(self, user_id:int, username:Optional[str]=None, first_name:Optional[str]=None, last_name:Optional[str]=None, file_accessed:bool=False):
        conn = await self.get_connection()
        def _up():
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM users WHERE user_id=%s", (user_id,))
            if cur.fetchone():
                cur.execute("UPDATE users SET last_active=CURRENT_TIMESTAMP, total_interactions=total_interactions+1, username=COALESCE(%s, username), first_name=COALESCE(%s, first_name), last_name=COALESCE(%s, last_name) WHERE user_id=%s", (username, first_name, last_name, user_id))
                if file_accessed:
                    cur.execute("UPDATE users SET total_files_accessed = total_files_accessed+1, last_file_accessed = CURRENT_TIMESTAMP WHERE user_id=%s", (user_id,))
            else:
                cur.execute("INSERT INTO users (user_id, username, first_name, last_name) VALUES (%s,%s,%s,%s)", (user_id, username, first_name, last_name))
            conn.commit()
        return await asyncio.to_thread(_up)

    async def get_all_user_ids(self, exclude_admin: bool=True) -> List[int]:
        conn = await self.get_connection()
        def _get():
            cur = conn.cursor()
            if exclude_admin:
                cur.execute("SELECT user_id FROM users WHERE user_id != %s", (ADMIN_ID,))
            else:
                cur.execute("SELECT user_id FROM users")
            return [r[0] for r in cur.fetchall()]
        return await asyncio.to_thread(_get)

# Instantiate DB
db = Database(DATABASE_URL)

# ---------------- Telegram application & handlers ----------------
application: Optional[Application] = None
app_loop: Optional[asyncio.AbstractEventLoop] = None

# Helper: schedule deletion using job_queue (when available)
async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id:int, message_id:int):
    try:
        await db.schedule_deletion(chat_id, message_id)
        if context.job_queue:
            context.job_queue.run_once(_delete_job, DELETE_AFTER, data=message_id, chat_id=chat_id)
    except Exception as e:
        log.error("schedule_message_deletion failed", exc_info=True)

async def _delete_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.chat_id
    message_id = job.data
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        await db.remove_scheduled(chat_id, message_id)
        log.info(f"Deleted message {message_id} in {chat_id}")
    except Exception as e:
        # if not found / can't be deleted, remove scheduled row
        await db.remove_scheduled(chat_id, message_id)
        log.warning(f"Delete job: {e}")

# Membership check using bot.get_chat_member
async def check_user_in_channel(bot, channel: str, user_id: int) -> bool:
    if not channel:
        return True
    ch = channel if channel.startswith("@") else f"@{channel}"
    try:
        mem = await bot.get_chat_member(chat_id=ch, user_id=user_id)
        return mem.status in ("member", "administrator", "creator")
    except Exception as e:
        log.warning(f"Membership check for {ch} failed: {e}")
        # avoid locking out users when API throws unexpected errors
        return True

# ---------------- Handlers (kept similar to your original) ----------------
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message:
            return
        user = update.effective_user
        await db.update_user_interaction(user.id, user.username, user.first_name, user.last_name)
        args = context.args
        if not args:
            keyboard = [
                [InlineKeyboardButton("🔄 Check Membership", callback_data="check_membership")]
            ]
            sent = await update.message.reply_text(
                "🤖 Welcome to the File Bot.\nUse admin-uploaded links to access files.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            return

        key = args[0]
        file_info = await db.get_file(key)
        if not file_info:
            sent = await update.message.reply_text("❌ File not found")
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            return

        result = True  # optionally check membership if you have CHANNEL_1 etc.
        if not result:
            sent = await update.message.reply_text("🔒 Join required channels.")
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            return

        # send file
        try:
            filename = file_info["file_name"]
            ext = filename.lower().split(".")[-1] if "." in filename else ""
            if file_info["is_video"] and ext in PLAYABLE_EXTS:
                sent = await context.bot.send_video(chat_id=update.effective_chat.id, video=file_info["file_id"], caption=f"{filename}\nAuto-delete in {DELETE_AFTER//60}m")
            else:
                sent = await context.bot.send_document(chat_id=update.effective_chat.id, document=file_info["file_id"], caption=f"{filename}\nAuto-delete in {DELETE_AFTER//60}m")
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        except Exception as e:
            log.exception("Failed to send file")
            sent = await update.message.reply_text("❌ Failed to send file")
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)

    except Exception as e:
        log.exception("start_handler error")

async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        msg = update.message
        if not msg:
            return
        video = msg.video
        doc = msg.document
        if not video and not doc:
            sent = await msg.reply_text("❌ Send a video or document")
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            return

        if video:
            file_id = video.file_id
            fname = video.file_name or f"video_{int(time.time())}.mp4"
            mime = video.mime_type or "video/mp4"
            size = video.file_size or 0
            is_video = True
        else:
            file_id = doc.file_id
            fname = doc.file_name or f"doc_{int(time.time())}"
            mime = doc.mime_type or ""
            size = doc.file_size or 0
            ext = fname.lower().split(".")[-1] if "." in fname else ""
            is_video = ext in ALL_VIDEO_EXTS

        file_info = {"file_name": fname, "mime_type": mime, "is_video": is_video, "size": int(size)}
        key = await db.save_file(file_id, file_info)
        link = f"https://t.me/{BOT_USERNAME}?start={key}"
        sent = await msg.reply_text(f"✅ Saved file `{fname}`\nKey: `{key}`\nLink: {link}", parse_mode="Markdown")
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
    except Exception as e:
        log.exception("upload_handler")

async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    uptime = str(timedelta(seconds=int(time.time() - start_time)))
    file_count = await db.get_file_count()
    user_count = await db.get_user_count()
    await update.message.reply_text(f"Uptime: {uptime}\nFiles: {file_count}\nUsers: {user_count}")

# minimal callback handler for demo
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("Callback received!")

# ---------------- Bot startup in background thread ----------------
async def start_bot_async(app_obj: Application):
    """Initialize DB, add handlers, set webhook, start application tasks."""
    # 1) ensure DB exists and initialized
    await db.ensure_database()
    await db.init_db()
    log.info("Database ready.")

    # 2) add handlers
    app_obj.add_handler(CommandHandler("start", start_handler))
    app_obj.add_handler(CommandHandler("upload", upload_handler))
    app_obj.add_handler(CommandHandler("stats", stats_handler))
    app_obj.add_handler(CallbackQueryHandler(callback_handler))
    # Add more handlers as needed...

    # initialize and set webhook
    await app_obj.initialize()
    # remove any previous webhook
    await app_obj.bot.delete_webhook(drop_pending_updates=True)
    await app_obj.bot.set_webhook(url=WEBHOOK_URL)
    log.info(f"Webhook set to: {WEBHOOK_URL}")

    # start application (starts internal tasks/jobs)
    await app_obj.start()
    log.info("Application started inside background event loop.")
    # keep running
    await asyncio.Event().wait()

def bot_thread_target(loop, app_obj):
    """Run the bot's async main inside a dedicated thread and loop."""
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(start_bot_async(app_obj))
    except Exception as e:
        log.exception("Bot thread died")
    finally:
        # Attempt graceful shutdown if we exit
        with suppress(Exception):
            loop.run_until_complete(app_obj.stop())
        loop.close()

def start_bot_in_thread():
    global application, app_loop
    application = Application.builder().token(BOT_TOKEN).build()
    # create dedicated loop and thread
    app_loop = asyncio.new_event_loop()
    t = threading.Thread(target=bot_thread_target, args=(app_loop, application), daemon=True)
    t.start()
    # Wait briefly until webhook is set (best-effort)
    time.sleep(1)
    log.info("Bot thread launched.")

# ---------------- Flask webhook route -> forward to application ----------------
@app.route(WEBHOOK_PATH, methods=["POST"])
def telegram_webhook():
    global application, app_loop
    if application is None or app_loop is None:
        return "bot not ready", 503
    try:
        data = request.get_json(force=True)
        update = Update.de_json(data, application.bot)
        # schedule processing in bot event loop
        fut = asyncio.run_coroutine_threadsafe(application.process_update(update), app_loop)
        # optional: wait for processing to complete (small timeout)
        with suppress(Exception):
            fut.result(timeout=10)
        return "ok", 200
    except Exception as e:
        log.exception("Webhook processing failed")
        return "error", 500

# ---------------- Dashboard & Health routes ----------------
@app.route("/", methods=["GET"])
def home():
    uptime = str(timedelta(seconds=int(time.time() - start_time)))
    file_count = 0
    user_count = 0
    try:
        if db.initialized:
            file_count = asyncio.run(db.get_file_count())
            user_count = asyncio.run(db.get_user_count())
    except Exception:
        pass

    html = """
    <!doctype html>
    <html>
      <head><meta name="viewport" content="width=device-width, initial-scale=1"><title>Telegram File Bot</title></head>
      <body style="font-family:Arial,Helvetica,sans-serif;background:#111;color:#eee;padding:20px;">
        <h2>🤖 Telegram File Bot (Webhook)</h2>
        <p>Status: <strong>Running</strong></p>
        <p>Uptime: {{ uptime }}</p>
        <p>Files: {{ files }} | Users: {{ users }}</p>
        <p>Webhook: <code>{{ webhook }}</code></p>
      </body>
    </html>
    """
    return render_template_string(html, uptime=uptime, files=file_count or 0, users=user_count or 0, webhook=WEBHOOK_URL)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "uptime_seconds": int(time.time() - start_time),
        "webhook_url": WEBHOOK_URL,
        "db_initialized": db.initialized
    }), 200

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

# ---------------- Main startup ----------------
def main():
    log.info("Starting bot + flask server...")
    start_bot_in_thread()
    log.info(f"Starting Flask on 0.0.0.0:{PORT}")
    # Note: use_reloader=False is critical in Render to avoid double-starting
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
