#!/usr/bin/env python3
"""
Telegram File Bot — single-file webhook + Flask + PostgreSQL
Compatible with Python 3.14.3, python-telegram-bot >=21.7, Flask >=2.3.3, pg8000 >=1.30.5
"""

import os
import sys
import time
import ssl
import logging
import threading
import asyncio
import urllib.parse
import signal
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from contextlib import suppress

import pg8000
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

# ---------- Config & Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("telegram-file-bot")
log.setLevel(logging.INFO)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
WEBHOOK_PATH = "/telegram-webhook"
WEBHOOK_URL = f"{RENDER_EXTERNAL_URL}{WEBHOOK_PATH}" if RENDER_EXTERNAL_URL else ""
PORT = int(os.environ.get("PORT", "5000"))
BOT_USERNAME = os.environ.get("BOT_USERNAME", "file_bot")  # optional helpful fallback

DELETE_AFTER = int(os.environ.get("DELETE_AFTER", "600"))  # seconds
PLAYABLE_EXTS = {"mp4", "mov", "m4v", "mpeg", "mpg"}
ALL_VIDEO_EXTS = {
    "mp4", "mkv", "mov", "avi", "webm", "flv", "m4v", "3gp", "wmv", "mpg", "mpeg"
}

if not (BOT_TOKEN and ADMIN_ID and DATABASE_URL and RENDER_EXTERNAL_URL):
    log.error("Missing one of required env vars: BOT_TOKEN, ADMIN_ID, DATABASE_URL, RENDER_EXTERNAL_URL")
    sys.exit(1)

# ---------- Flask app ----------
app = Flask(__name__)
start_time = time.time()

# ---------- Database helper ----------
class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.conn = None
        self._lock = threading.Lock()
        self.initialized = False
        self.params = self._parse_db_url(db_url)
        log.info(f"Parsed DB params: {self.params}")

    def _parse_db_url(self, db_url: str) -> Dict[str, Any]:
        s = db_url.replace("postgresql://", "").replace("postgres://", "")
        user_pass, host_port_db = s.split("@", 1)
        user, password = user_pass.split(":", 1)
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

    def _ssl_ctx(self):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def connect_sync(self, database: Optional[str] = None):
        params = self.params.copy()
        if database:
            params["database"] = database
        return pg8000.connect(
            user=params["user"],
            password=params["password"],
            host=params["host"],
            port=params["port"],
            database=params["database"],
            ssl_context=self._ssl_ctx(),
            timeout=30,
        )

    async def ensure_database(self):
        def _ensure():
            target = self.params["database"]
            try:
                conn = self.connect_sync(target)
                conn.close()
                return True
            except Exception:
                # try to create using postgres
                try:
                    conn = self.connect_sync("postgres")
                    cur = conn.cursor()
                    conn.autocommit = True
                    cur.execute(f'CREATE DATABASE "{target}"')
                    conn.close()
                    return True
                except Exception:
                    raise
        return await asyncio.to_thread(_ensure)

    async def get_connection(self):
        with self._lock:
            if self.conn is None:
                self.conn = await asyncio.to_thread(self.connect_sync, self.params.get("database"))
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

    async def save_file(self, file_id: str, file_info: dict) -> str:
        conn = await self.get_connection()
        def _save():
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO files (file_id, file_name, mime_type, is_video, file_size, access_count) VALUES (%s,%s,%s,%s,%s,0) RETURNING id",
                (file_id, file_info.get("file_name",""), file_info.get("mime_type",""), 1 if file_info.get("is_video") else 0, int(file_info.get("size",0)))
            )
            nid = cur.fetchone()[0]
            conn.commit()
            return str(nid)
        return await asyncio.to_thread(_save)

    async def get_file(self, key: str) -> Optional[dict]:
        try:
            kid = int(key)
        except:
            return None
        conn = await self.get_connection()
        def _get():
            cur = conn.cursor()
            cur.execute("UPDATE files SET access_count = access_count + 1 WHERE id = %s RETURNING file_id, file_name, mime_type, is_video, file_size, TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS'), access_count", (kid,))
            row = cur.fetchone()
            if row:
                conn.commit()
                return {"file_id": row[0], "file_name": row[1], "mime_type": row[2], "is_video": bool(row[3]), "size": row[4], "timestamp": row[5], "access_count": row[6]}
            return None
        return await asyncio.to_thread(_get)

    async def delete_file(self, key: str) -> bool:
        try:
            kid = int(key)
        except:
            return False
        conn = await self.get_connection()
        def _del():
            cur = conn.cursor()
            cur.execute("DELETE FROM files WHERE id = %s", (kid,))
            rc = cur.rowcount
            conn.commit()
            return rc > 0
        return await asyncio.to_thread(_del)

    async def get_file_count(self) -> int:
        conn = await self.get_connection()
        def _c():
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM files")
            return cur.fetchone()[0]
        return await asyncio.to_thread(_c)

    async def get_user_count(self) -> int:
        conn = await self.get_connection()
        def _c():
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users")
            return cur.fetchone()[0]
        return await asyncio.to_thread(_c)

    async def schedule_deletion(self, chat_id: int, message_id: int):
        conn = await self.get_connection()
        st = datetime.now() + timedelta(seconds=DELETE_AFTER)
        def _s():
            cur = conn.cursor()
            cur.execute("INSERT INTO scheduled_deletions (chat_id, message_id, scheduled_time, delete_after) VALUES (%s,%s,%s,%s) ON CONFLICT (chat_id, message_id) DO UPDATE SET scheduled_time=EXCLUDED.scheduled_time", (chat_id, message_id, st, DELETE_AFTER))
            conn.commit()
        return await asyncio.to_thread(_s)

    async def get_due_messages(self):
        conn = await self.get_connection()
        def _g():
            cur = conn.cursor()
            cur.execute("SELECT chat_id, message_id FROM scheduled_deletions WHERE scheduled_time <= CURRENT_TIMESTAMP")
            return cur.fetchall()
        return await asyncio.to_thread(_g)

    async def remove_scheduled(self, chat_id: int, message_id: int):
        conn = await self.get_connection()
        def _r():
            cur = conn.cursor()
            cur.execute("DELETE FROM scheduled_deletions WHERE chat_id=%s AND message_id=%s", (chat_id, message_id))
            conn.commit()
        return await asyncio.to_thread(_r)

    async def update_user_interaction(self, user_id:int, username:Optional[str]=None, first_name:Optional[str]=None, last_name:Optional[str]=None, file_accessed:bool=False):
        conn = await self.get_connection()
        def _u():
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM users WHERE user_id=%s", (user_id,))
            if cur.fetchone():
                cur.execute("UPDATE users SET last_active=CURRENT_TIMESTAMP, total_interactions=total_interactions+1, username=COALESCE(%s,username), first_name=COALESCE(%s,first_name), last_name=COALESCE(%s,last_name) WHERE user_id=%s", (username, first_name, last_name, user_id))
                if file_accessed:
                    cur.execute("UPDATE users SET total_files_accessed=total_files_accessed+1, last_file_accessed=CURRENT_TIMESTAMP WHERE user_id=%s", (user_id,))
            else:
                cur.execute("INSERT INTO users (user_id, username, first_name, last_name) VALUES (%s,%s,%s,%s)", (user_id, username, first_name, last_name))
            conn.commit()
        return await asyncio.to_thread(_u)

    async def get_all_user_ids(self, exclude_admin: bool=True) -> List[int]:
        conn = await self.get_connection()
        def _g():
            cur = conn.cursor()
            if exclude_admin:
                cur.execute("SELECT user_id FROM users WHERE user_id != %s", (ADMIN_ID,))
            else:
                cur.execute("SELECT user_id FROM users")
            return [r[0] for r in cur.fetchall()]
        return await asyncio.to_thread(_g)

db = Database(DATABASE_URL)

# ---------- Bot & handlers ----------
application: Optional[Application] = None
bot_loop: Optional[asyncio.AbstractEventLoop] = None

async def _delete_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.chat_id
    message_id = job.data
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        await db.remove_scheduled(chat_id, message_id)
    except Exception:
        await db.remove_scheduled(chat_id, message_id)

async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    try:
        await db.schedule_deletion(chat_id, message_id)
        if context.job_queue:
            context.job_queue.run_once(_delete_job, DELETE_AFTER, data=message_id, chat_id=chat_id)
    except Exception as e:
        log.warning(f"schedule_message_deletion failed: {e}")

async def check_user_in_channel(bot, channel: str, user_id: int) -> bool:
    if not channel:
        return True
    ch = channel if channel.startswith("@") else f"@{channel}"
    try:
        mem = await bot.get_chat_member(chat_id=ch, user_id=user_id)
        return mem.status in ("member","administrator","creator")
    except Exception as e:
        log.warning(f"Membership check failed for {ch}: {e}")
        return True

# Handlers — keeping full feature set
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    await db.update_user_interaction(user.id, user.username, user.first_name, user.last_name)
    args = context.args
    if not args:
        keyboard = [
            [InlineKeyboardButton("🔄 Check Membership", callback_data="check_membership")]
        ]
        sent = await update.message.reply_text("🤖 Welcome — use admin-uploaded links to access files.", reply_markup=InlineKeyboardMarkup(keyboard))
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        return
    key = args[0]
    info = await db.get_file(key)
    if not info:
        sent = await update.message.reply_text("❌ File not found")
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        return
    # membership logic placeholder — adapt CHANNEL_1/CHANNEL_2 if you set them
    # send file
    try:
        fname = info["file_name"]
        ext = fname.lower().split(".")[-1] if "." in fname else ""
        if info["is_video"] and ext in PLAYABLE_EXTS:
            sent = await context.bot.send_video(chat_id=update.effective_chat.id, video=info["file_id"], caption=f"{fname}\nAuto-delete in {DELETE_AFTER//60}m")
        else:
            sent = await context.bot.send_document(chat_id=update.effective_chat.id, document=info["file_id"], caption=f"{fname}\nAuto-delete in {DELETE_AFTER//60}m")
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
    except Exception as e:
        log.exception("send file failed")
        sent = await update.message.reply_text("❌ Failed to send file")
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)

async def upload_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
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
        fid = video.file_id
        fname = video.file_name or f"video_{int(time.time())}.mp4"
        mime = video.mime_type or "video/mp4"
        size = video.file_size or 0
        is_video = True
    else:
        fid = doc.file_id
        fname = doc.file_name or f"doc_{int(time.time())}"
        mime = doc.mime_type or ""
        size = doc.file_size or 0
        ext = fname.lower().split(".")[-1] if "." in fname else ""
        is_video = ext in ALL_VIDEO_EXTS
    fi = {"file_name": fname, "mime_type": mime, "is_video": is_video, "size": int(size)}
    key = await db.save_file(fid, fi)
    link = f"https://t.me/{BOT_USERNAME}?start={key}"
    sent = await msg.reply_text(f"✅ Saved `{fname}`\nKey: `{key}`\nLink: {link}", parse_mode="Markdown")
    await schedule_message_deletion(context, sent.chat_id, sent.message_id)

async def listfiles_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    conn = await db.get_connection()
    def _list():
        cur = conn.cursor()
        cur.execute("SELECT id, file_name, file_size, access_count, TO_CHAR(timestamp,'YYYY-MM-DD HH24:MI:SS') FROM files ORDER BY timestamp DESC LIMIT 100")
        return cur.fetchall()
    rows = await asyncio.to_thread(_list)
    if not rows:
        sent = await update.message.reply_text("📁 No files stored")
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        return
    msg = "📁 Files:\n"
    for r in rows[:50]:
        fid, name, size, access, ts = r
        msg += f"`{fid}` - {name[:30]} - {size//1024}KB - {access} accesses\n"
    sent = await update.message.reply_text(msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent.chat_id, sent.message_id)

async def deletefile_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        sent = await update.message.reply_text("❌ Usage: /deletefile <key>")
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        return
    key = context.args[0]
    ok = await db.delete_file(key)
    sent = await update.message.reply_text(f"✅ Deleted {key}" if ok else f"❌ Not found {key}")
    await schedule_message_deletion(context, sent.chat_id, sent.message_id)

async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    uptime = str(timedelta(seconds=int(time.time()-start_time)))
    files = await db.get_file_count()
    users = await db.get_user_count()
    await update.message.reply_text(f"Uptime: {uptime}\nFiles: {files}\nUsers: {users}")

async def broadcast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args and not update.message.reply_to_message:
        sent = await update.message.reply_text("❌ Usage: /broadcast <message> or reply with /broadcast")
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        return
    if update.message.reply_to_message:
        text = update.message.reply_to_message.text or update.message.reply_to_message.caption or ""
    else:
        text = " ".join(context.args)
    if not text:
        sent = await update.message.reply_text("❌ Message empty")
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        return
    uids = await db.get_all_user_ids()
    status = await update.message.reply_text(f"Broadcasting to {len(uids)} (preview up to 100)")
    success = failed = 0
    for uid in uids[:100]:
        try:
            await context.bot.send_message(chat_id=uid, text=text, parse_mode="Markdown")
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
    await status.edit_text(f"✅ Done\nSent: {success}\nFailed: {failed}")
    await schedule_message_deletion(context, status.chat_id, status.message_id)

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("Callback received!")

# ---------- Bot startup inside thread ----------
async def start_bot_async(app_obj: Application):
    # DB init
    await db.ensure_database()
    await db.init_db()
    log.info("DB ready")

    # add handlers
    app_obj.add_handler(CommandHandler("start", start_handler))
    app_obj.add_handler(CommandHandler("upload", upload_handler))
    app_obj.add_handler(CommandHandler("listfiles", listfiles_handler))
    app_obj.add_handler(CommandHandler("deletefile", deletefile_handler))
    app_obj.add_handler(CommandHandler("stats", stats_handler))
    app_obj.add_handler(CommandHandler("broadcast", broadcast_handler))
    app_obj.add_handler(CallbackQueryHandler(callback_handler))
    # initialize and set webhook
    await app_obj.initialize()
    await app_obj.bot.delete_webhook(drop_pending_updates=True)
    await app_obj.bot.set_webhook(url=WEBHOOK_URL)
    log.info(f"Webhook set: {WEBHOOK_URL}")
    await app_obj.start()
    log.info("Application started")
    await asyncio.Event().wait()

def bot_thread_target(loop: asyncio.BaseEventLoop, app_obj: Application):
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(start_bot_async(app_obj))
    except Exception:
        log.exception("Bot thread crashed")
    finally:
        with suppress(Exception):
            loop.run_until_complete(app_obj.stop())
        loop.close()

def start_bot_in_thread():
    global application, bot_loop
    application = Application.builder().token(BOT_TOKEN).build()
    bot_loop = asyncio.new_event_loop()
    t = threading.Thread(target=bot_thread_target, args=(bot_loop, application), daemon=True)
    t.start()
    # wait brief
    time.sleep(1)
    log.info("Bot thread started")

# ---------- Flask webhook route (forward to bot loop) ----------
@app.route(WEBHOOK_PATH, methods=["POST"])
def telegram_webhook():
    global application, bot_loop
    if application is None or bot_loop is None:
        return "not ready", 503
    try:
        data = request.get_json(force=True)
        upd = Update.de_json(data, application.bot)
        fut = asyncio.run_coroutine_threadsafe(application.process_update(upd), bot_loop)
        # Optionally wait short to ensure error surfaced
        with suppress(Exception):
            fut.result(timeout=10)
        return "ok", 200
    except Exception:
        log.exception("Webhook forward failed")
        return "error", 500

# ---------- Dashboard / health ----------
@app.route("/", methods=["GET"])
def home():
    uptime = str(timedelta(seconds=int(time.time()-start_time)))
    file_count = user_count = 0
    try:
        if db.initialized:
            file_count = asyncio.run(db.get_file_count())
            user_count = asyncio.run(db.get_user_count())
    except Exception:
        pass
    html = """
    <html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Telegram File Bot</title></head>
    <body style="font-family:Arial;background:#111;color:#eee;padding:20px;">
    <h2>🤖 Telegram File Bot (Webhook)</h2>
    <p>Uptime: {{uptime}}</p>
    <p>Files: {{files}} | Users: {{users}}</p>
    <p>Webhook: <code>{{webhook}}</code></p>
    </body></html>
    """
    return render_template_string(html, uptime=uptime, files=file_count, users=user_count, webhook=WEBHOOK_URL)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status":"ok","uptime_seconds":int(time.time()-start_time),"webhook":WEBHOOK_URL,"db_initialized":db.initialized}), 200

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

# ---------- Graceful shutdown (try best-effort) ----------
def _term_handler(signum, frame):
    log.info("SIGTERM received — shutting down")
    # Stop application if running
    with suppress(Exception):
        if bot_loop and application:
            fut = asyncio.run_coroutine_threadsafe(application.stop(), bot_loop)
            fut.result(timeout=10)
    # exit process
    sys.exit(0)

signal.signal(signal.SIGTERM, _term_handler)
signal.signal(signal.SIGINT, _term_handler)

# ---------- Main ----------
def main():
    log.info("Starting bot + Flask server")
    start_bot_in_thread()
    log.info(f"Starting Flask on 0.0.0.0:{PORT}")
    # use_reloader=False important on Render
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
