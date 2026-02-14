import os
import asyncio
import logging
import secrets
import threading
import time

from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, Tuple

import pg8000
from flask import Flask, jsonify, render_template_string
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,   # ✅ ADD HERE
    filters,
    ContextTypes,
)


# ==================== CONFIGURATION ====================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_1 = os.environ.get("CHANNEL_USERNAME1")  # e.g. @channel1
CHANNEL_2 = os.environ.get("CHANNEL_USERNAME2")  # e.g. @channel2
ADMIN_ID = int(os.environ.get("ADMIN_ID", 0))
DATABASE_URL = os.environ.get("DATABASE_URL")
PORT = int(os.environ.get("PORT", 5000))  # Render sets this

if not all([BOT_TOKEN, CHANNEL_1, CHANNEL_2, ADMIN_ID, DATABASE_URL]):
    raise ValueError("Missing required environment variables")

# Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== DATABASE HELPERS ====================
import ssl
import urllib.parse as urlparse

def get_db_connection():
    try:
        # Normalize URL
        db_url = DATABASE_URL
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)

        url = urlparse.urlparse(db_url)

        conn = pg8000.connect(
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port,
            database=url.path.lstrip("/"),
            ssl_context=ssl.create_default_context()  # Proper SSL
        )
        return conn

    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise


def init_db():
    """Create tables if they don't exist."""
    conn = get_db_connection()
    cur = conn.cursor()
    # Files table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id SERIAL PRIMARY KEY,
            file_key TEXT UNIQUE NOT NULL,
            file_id TEXT NOT NULL,
            file_type TEXT NOT NULL,
            mime_type TEXT,
            file_name TEXT,
            file_size BIGINT,
            upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            access_count INTEGER DEFAULT 0
        )
    """)
    # Users table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            username TEXT,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            interactions INTEGER DEFAULT 0,
            files_accessed INTEGER DEFAULT 0
        )
    """)
    # Membership cache table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS membership_cache (
            user_id BIGINT PRIMARY KEY,
            status1 BOOLEAN,
            status2 BOOLEAN,
            last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Scheduled deletions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scheduled_deletions (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            message_id INTEGER,
            delete_time TIMESTAMP,
            file_key TEXT
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Database initialized.")

# ----- File operations -----
def add_file(file_key: str, file_id: str, file_type: str, mime: str, name: str, size: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO files (file_key, file_id, file_type, mime_type, file_name, file_size) VALUES (%s, %s, %s, %s, %s, %s)",
        (file_key, file_id, file_type, mime, name, size),
    )
    conn.commit()
    cur.close()
    conn.close()

def get_file(file_key: str) -> Optional[Tuple]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT file_id, file_type, file_name FROM files WHERE file_key = %s", (file_key,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def increment_file_access(file_key: str):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE files SET access_count = access_count + 1 WHERE file_key = %s", (file_key,))
    conn.commit()
    cur.close()
    conn.close()

def list_files():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT file_key, file_name, access_count, upload_time FROM files ORDER BY upload_time DESC")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def delete_file(file_key: str) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM files WHERE file_key = %s", (file_key,))
    deleted = cur.rowcount > 0
    conn.commit()
    cur.close()
    conn.close()
    return deleted

# ----- User tracking -----
def update_user(user_id: int, first_name: str, last_name: str = "", username: str = ""):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO users (user_id, first_name, last_name, username, last_active, interactions)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, 1)
        ON CONFLICT (user_id) DO UPDATE SET
            last_active = CURRENT_TIMESTAMP,
            interactions = users.interactions + 1,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            username = EXCLUDED.username
        """,
        (user_id, first_name, last_name or "", username or ""),
    )
    conn.commit()
    cur.close()
    conn.close()

def increment_user_files(user_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE users SET files_accessed = files_accessed + 1 WHERE user_id = %s", (user_id,))
    conn.commit()
    cur.close()
    conn.close()

def get_user_stats():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE last_active > CURRENT_TIMESTAMP - INTERVAL '24 hours'")
    active_24h = cur.fetchone()[0]
    cur.execute("SELECT user_id, first_name, username, interactions, files_accessed FROM users ORDER BY interactions DESC LIMIT 10")
    top = cur.fetchall()
    cur.close()
    conn.close()
    return total, active_24h, top

def get_all_users():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users")
    users = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return users

# ----- Membership cache -----
def get_cached_membership(user_id: int) -> Optional[Tuple[bool, bool]]:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT status1, status2, last_checked FROM membership_cache WHERE user_id = %s",
        (user_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        status1, status2, last_checked = row
        # Check if cache is still valid (5 minutes)
        if datetime.now() - last_checked < timedelta(minutes=5):
            return status1, status2
    return None

def update_membership_cache(user_id: int, status1: bool, status2: bool):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO membership_cache (user_id, status1, status2, last_checked)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (user_id) DO UPDATE SET
            status1 = EXCLUDED.status1,
            status2 = EXCLUDED.status2,
            last_checked = CURRENT_TIMESTAMP
        """,
        (user_id, status1, status2),
    )
    conn.commit()
    cur.close()
    conn.close()

def clear_membership_cache():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM membership_cache")
    conn.commit()
    cur.close()
    conn.close()

# ----- Scheduled deletions -----
def schedule_deletion(chat_id: int, message_id: int, file_key: str = None):
    delete_time = datetime.now() + timedelta(minutes=10)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO scheduled_deletions (chat_id, message_id, delete_time, file_key) VALUES (%s, %s, %s, %s)",
        (chat_id, message_id, delete_time, file_key),
    )
    conn.commit()
    cur.close()
    conn.close()

def get_due_deletions():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, chat_id, message_id FROM scheduled_deletions WHERE delete_time <= CURRENT_TIMESTAMP"
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def remove_deletion_record(delete_id: int):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM scheduled_deletions WHERE id = %s", (delete_id,))
    conn.commit()
    cur.close()
    conn.close()

# ==================== MEMBERSHIP VERIFICATION ====================
async def is_user_member(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> bool:
    """Check if user is member of both required channels (with caching)."""
    cached = get_cached_membership(user_id)
    if cached:
        return cached[0] and cached[1]

    try:
        member1 = await context.bot.get_chat_member(chat_id=CHANNEL_1, user_id=user_id)
        member2 = await context.bot.get_chat_member(chat_id=CHANNEL_2, user_id=user_id)
        status1 = member1.status in ("member", "administrator", "creator")
        status2 = member2.status in ("member", "administrator", "creator")
    except Exception as e:
        logger.error(f"Membership check failed for {user_id}: {e}")
        status1 = status2 = False

    update_membership_cache(user_id, status1, status2)
    return status1 and status2

# ==================== BOT HANDLERS ====================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    update_user(user.id, user.first_name, user.last_name, user.username)

    args = context.args
    if args and args[0]:
        # Deep link with file key
        file_key = args[0]
        file_info = await asyncio.to_thread(get_file, file_key)
        if not file_info:
            await update.message.reply_text("❌ File not found or expired.")
            return

        # Check membership
        if not await is_user_member(context, user.id):
            # Show join buttons
            keyboard = [
                [InlineKeyboardButton(f"Join {CHANNEL_1}", url=f"https://t.me/{CHANNEL_1.lstrip('@')}")],
                [InlineKeyboardButton(f"Join {CHANNEL_2}", url=f"https://t.me/{CHANNEL_2.lstrip('@')}")],
                [InlineKeyboardButton("✅ I've Joined", callback_data=f"check_{file_key}")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "⚠️ To access this file, you must join both channels below.\n"
                "After joining, click 'I've Joined'.",
                reply_markup=reply_markup,
            )
            return

        # Send file
        file_id, file_type, file_name = file_info
        if file_type == "video":
            msg = await update.message.reply_video(video=file_id, caption=f"📁 {file_name}")
        else:
            msg = await update.message.reply_document(document=file_id, caption=f"📁 {file_name}")

        # Schedule deletion
        schedule_deletion(update.effective_chat.id, msg.message_id, file_key)

        # Update stats
        await asyncio.to_thread(increment_file_access, file_key)
        await asyncio.to_thread(increment_user_files, user.id)
    else:
        # Welcome message
        keyboard = [
            [InlineKeyboardButton(f"Join {CHANNEL_1}", url=f"https://t.me/{CHANNEL_1.lstrip('@')}")],
            [InlineKeyboardButton(f"Join {CHANNEL_2}", url=f"https://t.me/{CHANNEL_2.lstrip('@')}")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"👋 Welcome {user.first_name}!\n"
            "I can store and share files. Use /start <key> to retrieve a file.\n"
            "You must join both channels below to use me.",
            reply_markup=reply_markup,
        )

async def check_membership_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    file_key = query.data.replace("check_", "")

    if await is_user_member(context, user.id):
        # Trigger file send again
        context.args = [file_key]
        await start(update, context)
    else:
        await query.edit_message_text(
            "❌ You haven't joined both channels yet. Please join and try again."
        )

async def handle_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin only: store file and generate key."""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Unauthorized.")
        return

    # Determine if it's video or document
    file_obj = None
    file_type = None
    if update.message.video:
        file_obj = update.message.video
        file_type = "video"
    elif update.message.document:
        file_obj = update.message.document
        file_type = "document"
    else:
        await update.message.reply_text("Please send a video or document.")
        return

    # Generate unique key
    file_key = secrets.token_urlsafe(8)
    file_id = file_obj.file_id
    mime = file_obj.mime_type
    name = file_obj.file_name if hasattr(file_obj, "file_name") else f"file_{file_key}"
    size = file_obj.file_size

    await asyncio.to_thread(add_file, file_key, file_id, file_type, mime, name, size)

 bot_username = (await context.bot.get_me()).username
 share_link = f"https://t.me/{bot_username}?start={file_key}"

    await update.message.reply_text(
        f"✅ File stored!\nKey: `{file_key}`\nShare link: {share_link}",
        parse_mode="Markdown",
    )

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    total_files = len(await asyncio.to_thread(list_files))
    total_users, active_24h, _ = await asyncio.to_thread(get_user_stats)
    await update.message.reply_text(
        f"📊 **Bot Statistics**\n"
        f"Total files: {total_files}\n"
        f"Total users: {total_users}\n"
        f"Active (24h): {active_24h}",
        parse_mode="Markdown",
    )

async def listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    files = await asyncio.to_thread(list_files)
    if not files:
        await update.message.reply_text("No files stored.")
        return
    msg = "📁 **Stored Files:**\n\n"
    for key, name, acc, ts in files[:20]:  # limit to 20 to avoid long message
        msg += f"`{key}` - {name} (accesses: {acc})\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def deletefile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Usage: /deletefile <key>")
        return
    key = context.args[0]
    deleted = await asyncio.to_thread(delete_file, key)
    if deleted:
        await update.message.reply_text(f"✅ File {key} deleted.")
    else:
        await update.message.reply_text("❌ File not found.")

async def users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    total, active, top = await asyncio.to_thread(get_user_stats)
    msg = f"👥 **Users:** total {total}, active 24h {active}\n\n**Top users:**\n"
    for uid, name, uname, inter, files_acc in top:
        msg += f"• {name or uid} (@{uname}) – {inter} interactions, {files_acc} files\n"
    await update.message.reply_text(msg, parse_mode="Markdown")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    message = " ".join(context.args)
    users = await asyncio.to_thread(get_all_users)
    sent = 0
    failed = 0
    status_msg = await update.message.reply_text(f"Broadcasting to {len(users)} users...")
    for i, uid in enumerate(users):
        try:
            await context.bot.send_message(chat_id=uid, text=message)
            sent += 1
        except Exception as e:
            failed += 1
            logger.warning(f"Broadcast failed to {uid}: {e}")
        if i % 500 == 0:  # avoid flood
            await asyncio.sleep(1)
    await status_msg.edit_text(f"✅ Broadcast finished. Sent: {sent}, Failed: {failed}")

async def clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await asyncio.to_thread(clear_membership_cache)
    await update.message.reply_text("✅ Membership cache cleared.")

async def cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    # Manual cleanup trigger (auto cleanup runs every 5 min)
    await perform_cleanup(context)
    await update.message.reply_text("✅ Cleanup job executed.")

async def perform_cleanup(context: ContextTypes.DEFAULT_TYPE):
    """Delete old messages and clean scheduled_deletions."""
    due = await asyncio.to_thread(get_due_deletions)
    for delete_id, chat_id, message_id in due:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception as e:
            logger.debug(f"Could not delete message {message_id}: {e}")
        await asyncio.to_thread(remove_deletion_record, delete_id)
    logger.info(f"Cleanup: processed {len(due)} deletions.")

async def periodic_cleanup(context: ContextTypes.DEFAULT_TYPE):
    """Job to run cleanup every 5 minutes."""
    await perform_cleanup(context)

# ==================== FLASK DASHBOARD ====================
flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    """Simple status dashboard."""
    total_files = len(list_files())
    total_users, active_24h, _ = get_user_stats()
    uptime_seconds = time.time() - start_time
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head><title>xiomovies_bot Dashboard</title></head>
    <body>
        <h1>🤖 Bot Status</h1>
        <ul>
            <li>Uptime: {{ uptime }}</li>
            <li>Total files: {{ total_files }}</li>
            <li>Total users: {{ total_users }}</li>
            <li>Active users (24h): {{ active_24h }}</li>
        </ul>
        <p>Health check: <a href="/health">/health</a></p>
    </body>
    </html>
    """, uptime=str(timedelta(seconds=int(uptime_seconds))),
         total_files=total_files, total_users=total_users, active_24h=active_24h)

@flask_app.route("/health")
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})

@flask_app.route("/ping")
def ping():
    return "pong"

# ==================== MAIN ====================
async def run_bot():
    """Set up and start the bot."""
    # Initialize DB
    await asyncio.to_thread(init_db)

    # Build application
    app = (Application.builder()
           .token(BOT_TOKEN)
           .build())

    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("upload", handle_upload))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("listfiles", listfiles))
    app.add_handler(CommandHandler("deletefile", deletefile))
    app.add_handler(CommandHandler("users", users))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("clearcache", clearcache))
    app.add_handler(CommandHandler("cleanup", cleanup))
    app.add_handler(CallbackQueryHandler(check_membership_callback, pattern="^check_"))

    # Periodic cleanup job (every 5 minutes)
    app.job_queue.run_repeating(periodic_cleanup, interval=300, first=10)

    logger.info("Bot started polling...")
    await app.run_polling()

def run_flask():
    """Start Flask server (blocking, runs in thread)."""
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    # Record start time for uptime
    start_time = time.time()

    # Start Flask in a background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Run bot (blocking)
    asyncio.run(run_bot())
