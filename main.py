import asyncio
import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import threading
import sqlite3
from contextlib import contextmanager

db_lock = threading.Lock()

# ================= HEALTH SERVER FOR RENDER =================
from flask import Flask, render_template_string, jsonify
app = Flask(__name__)

# Global variables for web dashboard
start_time = time.time()
bot_username = "xiomovies_bot"

# ===========================================================
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    JobQueue
)

# ================= CONFIG =================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

# Channel usernames (without @)
CHANNEL_1 = os.environ.get("CHANNEL_1", "A_Knight_of_the_Seven_Kingdoms_t").replace("@", "")
CHANNEL_2 = os.environ.get("CHANNEL_2", "your_movies_web").replace("@", "")

# SQLite database for persistent storage
DB_PATH = Path("file_bot.db")
DELETE_AFTER = 20  # 10 minutes - media disappears after this
MAX_STORED_FILES = 1000
AUTO_CLEANUP_DAYS = 0  # Set to 0 to NEVER auto-cleanup files

# Playable formats
PLAYABLE_EXTS = {"mp4", "mov", "m4v", "mpeg", "mpg"}

# All video extensions
ALL_VIDEO_EXTS = {
    "mp4", "mkv", "mov", "avi", "webm", "flv", "m4v", 
    "3gp", "wmv", "mpg", "mpeg"
}

# =========================================

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# ================= DATABASE =================

class Database:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize database with required tables"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS files (
                    id TEXT PRIMARY KEY,
                    file_id TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    mime_type TEXT,
                    is_video INTEGER DEFAULT 0,
                    file_size INTEGER DEFAULT 0,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    access_count INTEGER DEFAULT 0
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS membership_cache (
                    user_id INTEGER,
                    channel TEXT,
                    is_member INTEGER,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, channel)
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON membership_cache(timestamp)')
            conn.commit()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=30,
            check_same_thread=False
        )
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            yield conn
        finally:
            conn.close()

    def save_file(self, file_id: str, file_info: dict) -> str:
        """Save file info and return generated ID"""
        with db_lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    "SELECT COALESCE(MAX(CAST(id AS INTEGER)), 0) FROM files"
                )
                max_id = cursor.fetchone()[0]
                new_id = str(max_id + 1)

                cursor.execute(
                    '''
                    INSERT INTO files
                    (id, file_id, file_name, mime_type, is_video, file_size, access_count)
                    VALUES (?, ?, ?, ?, ?, ?, 0)
                    ''',
                    (
                        new_id,
                        file_id,
                        file_info.get('file_name', ''),
                        file_info.get('mime_type', ''),
                        1 if file_info.get('is_video', False) else 0,
                        file_info.get('size', 0)
                    )
                )

                conn.commit()
                return new_id

    def get_file(self, file_id: str) -> Optional[dict]:
        """Get file info by ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT file_id, file_name, mime_type, is_video, file_size, timestamp, access_count
                FROM files WHERE id = ?
            ''', (file_id,))
            row = cursor.fetchone()
            
            if row:
                # Increment access count
                cursor.execute('UPDATE files SET access_count = access_count + 1 WHERE id = ?', (file_id,))
                conn.commit()
                
                return {
                    'file_id': row[0],
                    'file_name': row[1],
                    'mime_type': row[2],
                    'is_video': bool(row[3]),
                    'size': row[4],
                    'timestamp': row[5],
                    'access_count': row[6] + 1
                }
            return None
    
    def cleanup_old_files(self):
        if AUTO_CLEANUP_DAYS <= 0:
            log.info("Auto-cleanup DISABLED (AUTO_CLEANUP_DAYS = 0). Files will be kept forever.")
            return
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM files 
                WHERE timestamp < datetime('now', ?)
            ''', (f'-{AUTO_CLEANUP_DAYS} days',))
            
            deleted = cursor.rowcount
            if deleted > 0:
                log.info(f"Auto-cleanup removed {deleted} old files from database")
            
            cursor.execute('''
                DELETE FROM files 
                WHERE id NOT IN (
                    SELECT id FROM files 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                )
            ''', (MAX_STORED_FILES,))
            
            if cursor.rowcount > 0:
                log.info(f"Limited files to {MAX_STORED_FILES} in database")
            
            conn.commit()
    
    def get_file_count(self) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM files")
            return cursor.fetchone()[0]
    
    def cache_membership(self, user_id: int, channel: str, is_member: bool):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO membership_cache (user_id, channel, is_member, timestamp)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, channel, 1 if is_member else 0))
            conn.commit()
    
    def get_cached_membership(self, user_id: int, channel: str) -> Optional[bool]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT is_member FROM membership_cache 
                WHERE user_id = ? AND channel = ? 
                AND timestamp > datetime('now', '-5 minutes')
            ''', (user_id, channel))
            row = cursor.fetchone()
            return bool(row[0]) if row else None

    def clear_membership_cache(self, user_id: Optional[int] = None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if user_id:
                cursor.execute("DELETE FROM membership_cache WHERE user_id = ?", (user_id,))
                log.info(f"Cleared cache for user {user_id}")
            else:
                cursor.execute("DELETE FROM membership_cache")
                log.info("Cleared all membership cache")
            conn.commit()

    def delete_file(self, file_id: str) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))
            deleted = cursor.rowcount > 0
            conn.commit()
            return deleted

    def get_all_files(self) -> list:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, file_name, is_video, file_size, timestamp, access_count 
                FROM files ORDER BY timestamp DESC
            ''')
            return cursor.fetchall()

# Initialize database
db = Database()

# ============ HELPER FUNCTION TO SCHEDULE MESSAGE DELETION ============
async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    if not context.job_queue:
        log.warning(f"Job queue not available - cannot schedule deletion for message {message_id}")
        return
    
    try:
        context.job_queue.run_once(
            delete_message_job,
            DELETE_AFTER,
            data=message_id,
            chat_id=chat_id,
            name=f"delete_msg_{chat_id}_{message_id}_{int(time.time())}"
        )
        log.info(f"Scheduled deletion of message {message_id} from chat {chat_id} in {DELETE_AFTER} seconds")
    except Exception as e:
        log.error(f"Failed to schedule deletion for message {message_id}: {e}")

async def delete_message_job(context):
    try:
        job = context.job
        chat_id = job.chat_id
        message_id = job.data
        
        if not chat_id or not message_id:
            log.warning(f"Invalid delete job data: chat_id={chat_id}, message_id={message_id}")
            return
        
        log.info(f"Attempting to delete message {message_id} from chat {chat_id}")
        
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            log.info(f"✅ Successfully deleted message {message_id} from chat {chat_id}")
        except Exception as e:
            error_msg = str(e).lower()
            if "message to delete not found" in error_msg:
                log.info(f"Message {message_id} already deleted from chat {chat_id}")
            elif "message can't be deleted" in error_msg:
                log.warning(f"Can't delete message {message_id} - insufficient permissions in chat {chat_id}")
            elif "chat not found" in error_msg:
                log.info(f"Chat {chat_id} not found - message probably already deleted")
            else:
                log.error(f"Failed to delete message {message_id} from chat {chat_id}: {e}")
                
    except Exception as e:
        log.error(f"Error in delete_message_job: {e}", exc_info=True)

# ============ FIXED MEMBERSHIP CHECK ============
async def check_user_in_channel(bot, channel: str, user_id: int, force_check: bool = False) -> bool:
    if not force_check:
        cached = db.get_cached_membership(user_id, channel)
        if cached is not None:
            log.info(f"Cache hit for user {user_id} in @{channel}: {cached}")
            return cached
    
    try:
        channel_username = f"@{channel}" if not channel.startswith("@") else channel
        
        log.info(f"Checking user {user_id} in {channel_username}")
        
        member = await bot.get_chat_member(
            chat_id=channel_username,
            user_id=user_id
        )
        
        is_member = member.status in ["member", "administrator", "creator"]
        
        log.info(f"User {user_id} in {channel_username}: status={member.status}, is_member={is_member}")
        
        db.cache_membership(user_id, channel.replace("@", ""), is_member)
        
        return is_member
        
    except Exception as e:
        error_msg = str(e).lower()
        log.warning(f"Failed to check user {user_id} in @{channel}: {e}")
        
        if "user not found" in error_msg or "user not participant" in error_msg:
            db.cache_membership(user_id, channel.replace("@", ""), False)
            return False
        elif "chat not found" in error_msg:
            log.error(f"Channel @{channel} not found!")
            return True
        elif "forbidden" in error_msg:
            log.error(f"Bot can't access @{channel}. Might be private or bot not admin.")
            return True
        else:
            return True

async def check_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE, force_check: bool = False) -> Dict[str, Any]:
    bot = context.bot
    
    result = {
        "channel1": False,
        "channel2": False,
        "all_joined": False,
        "missing_channels": []
    }
    
    if force_check:
        db.clear_membership_cache(user_id)
    
    try:
        ch1_result = await check_user_in_channel(bot, CHANNEL_1, user_id, force_check)
        result["channel1"] = ch1_result
        if not ch1_result:
            result["missing_channels"].append(f"@{CHANNEL_1}")
    except Exception as e:
        log.error(f"Error checking channel 1: {e}")
        result["channel1"] = True
    
    try:
        ch2_result = await check_user_in_channel(bot, CHANNEL_2, user_id, force_check)
        result["channel2"] = ch2_result
        if not ch2_result:
            result["missing_channels"].append(f"@{CHANNEL_2}")
    except Exception as e:
        log.error(f"Error checking channel 2: {e}")
        result["channel2"] = True
    
    result["all_joined"] = result["channel1"] and result["channel2"]
    
    log.info(f"Membership check for {user_id}: ch1={result['channel1']}, ch2={result['channel2']}, all={result['all_joined']}")
    return result

# ============ WEB ROUTES ============
@app.route('/')
def home():
    html_content = """
    <!DOCTYPE html>
<html>
<head>
    <title>🤖 Telegram File Bot</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
            margin: 0; 
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            min-height: 100vh;
        }
        .container { 
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2);
        }
        h1 { color: white; margin-top: 0; font-size: 1.5rem; }
        .status { 
            background: rgba(0, 255, 0, 0.2); 
            padding: 10px; 
            border-radius: 8px; 
            margin: 10px 0;
            border-left: 4px solid #00ff00;
        }
        .info { 
            background: rgba(255, 255, 255, 0.1);
            padding: 10px;
            border-radius: 8px;
            margin: 10px 0;
        }
        a { 
            color: #FFD700; 
            text-decoration: none; 
        }
        .btn {
            display: inline-block;
            background: #4CAF50;
            color: white;
            padding: 8px 16px;
            border-radius: 6px;
            margin: 5px;
            font-size: 0.9rem;
        }
        .warning {
            background: rgba(255, 165, 0, 0.2);
            border-left: 4px solid #ffa500;
            padding: 10px;
            border-radius: 8px;
            margin: 10px 0;
            font-size: 0.9rem;
        }
        code {
            background: rgba(0, 0, 0, 0.3);
            padding: 2px 4px;
            border-radius: 3px;
            font-family: monospace;
            font-size: 0.9rem;
        }
        ul { padding-left: 20px; }
        li { margin: 5px 0; }
        .error {
            background: rgba(255, 0, 0, 0.2);
            border-left: 4px solid #ff0000;
            padding: 10px;
            border-radius: 8px;
            margin: 10px 0;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Telegram File Bot</h1>
        <div class="status">
            <h3>✅ Status: <strong>ACTIVE</strong></h3>
            <p>Bot is running on Render</p>
            <p>Uptime: {{ uptime }}</p>
            <p>Files in DB: {{ file_count }}</p>
            <p>📁 Storage: PERMANENT (no auto-delete)</p>
        </div>
        
        <div class="info">
            <h3>📊 Bot Information</h3>
            <ul>
                <li>Service: <strong>Render Web Service</strong></li>
                <li>Bot: <strong>@{{ bot_username }}</strong></li>
                <li>Channels: <strong>@{{ channel1 }}, @{{ channel2 }}</strong></li>
                <li>File Storage: <strong>PERMANENT</strong></li>
                <li>Media Auto-disappear: <strong>{{ delete_minutes }} minutes</strong></li>
            </ul>
        </div>
        
        <div class="warning">
            <h3>⚠️ Important Notes</h3>
            <ul>
                <li>Files are stored <strong>PERMANENTLY</strong> in database</li>
                <li>Media disappears after {{ delete_minutes }} minutes (message stays)</li>
                <li>Users can access same file multiple times forever</li>
                <li>Admin must manually delete files if needed</li>
            </ul>
        </div>
        
        <div class="info">
            <h3>📞 Start Bot</h3>
            <p><a href="https://t.me/{{ bot_username }}" target="_blank" class="btn">Start @{{ bot_username }}</a></p>
        </div>
        
        <footer style="margin-top: 20px; border-top: 1px solid rgba(255,255,255,0.2); padding-top: 10px; font-size: 0.8rem;">
            <small>Render • {{ current_time }} • v1.0 • Permanent Storage</small>
        </footer>
    </div>
</body>
</html>
    """
    
    uptime_seconds = time.time() - start_time
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))
    
    file_count = 0
    try:
        file_count = db.get_file_count()
    except:
        pass
    
    return render_template_string(html_content, 
                                  bot_username=bot_username,
                                  uptime=uptime_str,
                                  current_time=datetime.now().strftime("%H:%M:%S"),
                                  file_count=file_count,
                                  channel1=CHANNEL_1,
                                  channel2=CHANNEL_2,
                                  delete_minutes=DELETE_AFTER//60)

@app.route('/health')
def health():
    return jsonify({
        "status": "OK", 
        "timestamp": datetime.now().isoformat(),
        "service": "telegram-file-bot",
        "uptime": str(timedelta(seconds=int(time.time() - start_time))),
        "database": "sqlite",
        "storage": "permanent",
        "file_count": db.get_file_count()
    }), 200

@app.route('/ping')
def ping():
    return "pong", 200

def run_flask_thread():
    port = int(os.environ.get('PORT', 10000))
    
    import warnings
    warnings.filterwarnings("ignore")
    
    import logging as flask_logging
    flask_logging.getLogger('werkzeug').setLevel(flask_logging.ERROR)
    flask_logging.getLogger('flask').setLevel(flask_logging.ERROR)
    
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error(f"Error: {context.error}", exc_info=True)

async def cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    days = 30
    if context.args:
        try:
            days = int(context.args[0])
            days = max(1, min(days, 365))
        except ValueError:
            await update.message.reply_text("Usage: /cleanup [days=30]\nSet days=0 to cancel")
            return
    
    if days == 0:
        await update.message.reply_text("✅ Cleanup cancelled. Files will be kept permanently.")
        return
    
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM files 
                WHERE timestamp < datetime('now', ?)
            ''', (f'-{days} days',))
            
            deleted = cursor.rowcount
            conn.commit()
        
        file_count = db.get_file_count()
        
        msg = f"🧹 Manual database cleanup complete\n"
        msg += f"📁 Files retained in database: {file_count}\n"
        msg += f"🗑️ Files older than {days} days removed: {deleted}\n\n"
        msg += f"⚠️ Note: Auto-cleanup is DISABLED. Files are kept permanently by default."
        
        sent_msg = await update.message.reply_text(msg)
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Cleanup failed: {str(e)[:100]}")

async def deletefile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        sent_msg = await update.message.reply_text(
            "❌ Usage: /deletefile <file_key>\n\n"
            "Example: /deletefile 123\n\n"
            "Use /listfiles to see all files"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    key = context.args[0]
    
    file_info = db.get_file(key)
    if not file_info:
        sent_msg = await update.message.reply_text(f"❌ File with key '{key}' not found in database")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    filename = file_info.get('file_name', 'Unknown')
    
    if db.delete_file(key):
        sent_msg = await update.message.reply_text(
            f"✅ File deleted from database\n\n"
            f"🔑 Key: {key}\n"
            f"📁 Name: {filename}\n\n"
            f"⚠️ This file can no longer be accessed by users"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    else:
        sent_msg = await update.message.reply_text(f"❌ Failed to delete file '{key}'")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        files = db.get_all_files()
        
        if not files:
            sent_msg = await update.message.reply_text("📁 Database is empty. No files stored.")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
        
        total_size = 0
        total_access = 0
        message_parts = []
        
        for i, file in enumerate(files[:50]):
            file_id, filename, is_video, size, timestamp, access_count = file
            total_size += size if size else 0
            total_access += access_count
            
            size_mb = size / (1024 * 1024) if size else 0
            
            try:
                date_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                date_str = date_obj.strftime("%b %d, %Y")
            except:
                date_str = timestamp
            
            message_parts.append(
                f"🔑 `{file_id}`\n"
                f"📁 {filename[:30]}{'...' if len(filename) > 30 else ''}\n"
                f"🎬 {'Video' if is_video else 'Doc'} • {size_mb:.1f}MB • 📅 {date_str} • 👥 {access_count}x\n"
            )
        
        summary = (
            f"📊 Database Summary:\n"
            f"• Total files: {len(files)}\n"
            f"• Total size: {total_size/(1024*1024*1024):.2f} GB\n"
            f"• Total accesses: {total_access}\n"
            f"• Storage: PERMANENT (no auto-delete)\n\n"
            f"📋 Files (showing {min(50, len(files))} of {len(files)}):\n"
        )
        
        full_message = summary + "\n".join(message_parts)
        
        if len(full_message) > 4000:
            sent_msg1 = await update.message.reply_text(full_message[:4000], parse_mode="Markdown")
            sent_msg2 = await update.message.reply_text(full_message[4000:], parse_mode="Markdown")
            await schedule_message_deletion(context, sent_msg1.chat_id, sent_msg1.message_id)
            await schedule_message_deletion(context, sent_msg2.chat_id, sent_msg2.message_id)
        else:
            sent_msg = await update.message.reply_text(full_message, parse_mode="Markdown")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            
    except Exception as e:
        log.error(f"Error listing files: {e}")
        sent_msg = await update.message.reply_text(f"❌ Error listing files: {str(e)[:200]}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    uptime_seconds = time.time() - start_time
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))

    file_count = db.get_file_count()
    db_size = DB_PATH.stat().st_size / 1024 if DB_PATH.exists() else 0
    
    total_access = 0
    try:
        files = db.get_all_files()
        total_access = sum(file[5] for file in files)
    except:
        pass

    sent_msg = await update.message.reply_text(
        f"📊 Bot Statistics\n\n"
        f"🤖 Bot: @{bot_username}\n"
        f"⏱ Uptime: {uptime_str}\n"
        f"📁 Files in database: {file_count}\n"
        f"👥 Total accesses: {total_access}\n"
        f"💾 DB Size: {db_size:.1f} KB\n"
        f"🧹 Auto-cleanup: DISABLED (permanent storage)\n"
        f"⏰ Media auto-disappear: {DELETE_AFTER//60} minutes\n\n"
        f"📢 Channels:\n"
        f"1. @{CHANNEL_1}\n"
        f"2. @{CHANNEL_2}\n\n"
        f"⚙️ Admin commands:\n"
        f"/listfiles - View all files\n"
        f"/deletefile <key> - Delete specific file\n"
        f"/cleanup [days] - Manual cleanup (optional)"
    )
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    user_id = None
    if context.args:
        try:
            user_id = int(context.args[0])
        except ValueError:
            sent_msg = await update.message.reply_text("Usage: /clearcache [user_id]")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
    
    db.clear_membership_cache(user_id)
    
    if user_id:
        sent_msg = await update.message.reply_text(f"✅ Cleared cache for user {user_id}")
    else:
        sent_msg = await update.message.reply_text("✅ Cleared all membership cache")
    
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def testchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    
    user_id = update.effective_user.id
    
    try:
        try:
            member1 = await context.bot.get_chat_member(f"@{CHANNEL_1}", user_id)
            ch1_status = f"✅ Accessible - Your status: {member1.status}"
        except Exception as e:
            ch1_status = f"❌ Error: {str(e)[:100]}"
        
        try:
            member2 = await context.bot.get_chat_member(f"@{CHANNEL_2}", user_id)
            ch2_status = f"✅ Accessible - Your status: {member2.status}"
        except Exception as e:
            ch2_status = f"❌ Error: {str(e)[:100]}"
        
        sent_msg = await update.message.reply_text(
            f"🔍 Channel Access Test\n\n"
            f"Channel 1 (@{CHANNEL_1}):\n{ch1_status}\n\n"
            f"Channel 2 (@{CHANNEL_2}):\n{ch2_status}"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    except Exception as e:
        sent_msg = await update.message.reply_text(f"❌ Test failed: {e}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message:
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        args = context.args

        if not args:
            keyboard = []
            keyboard.append([InlineKeyboardButton("📢 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")])
            keyboard.append([InlineKeyboardButton("📢 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")])
            keyboard.append([InlineKeyboardButton("🔄 Check Membership", callback_data="check_membership")])

            sent_msg = await update.message.reply_text(
                "🤖 *Welcome to File Sharing Bot*\n\n"
                "🔗 *How to use:*\n"
                "1️⃣ Use admin-provided links\n"
                "2️⃣ Join both channels below\n"
                "3️⃣ Click 'Check Membership' after joining\n\n"
                f"⚠️ *Note:* Media disappears after {DELETE_AFTER//60} minutes — forward immediately to keep!\n"
                "💾 *Storage:* Files are stored permanently in database",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        key = args[0]
        file_info = db.get_file(key)
        
        if not file_info:
            sent_msg = await update.message.reply_text("❌ File not found. It may have been manually deleted by admin.")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        result = await check_membership(user_id, context, force_check=True)
        
        if not result["all_joined"]:
            missing_count = len(result["missing_channels"])
            
            message_text = "🔒 *Access Required*\n\n"
            
            if missing_count == 2:
                message_text += "⚠️ *You need to join both channels to access this file:*\n"
                keyboard = [
                    [InlineKeyboardButton(f"📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                    [InlineKeyboardButton(f"📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                    [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                ]
            elif missing_count == 1:
                missing_channel = result["missing_channels"][0].replace("@", "")
                channel_name = "Channel 1" if CHANNEL_1 in missing_channel else "Channel 2"
                
                message_text += f"⚠️ *You need to join {channel_name} to access this file:*\n"
                
                keyboard = [
                    [InlineKeyboardButton(f"📥 Join {channel_name}", url=f"https://t.me/{missing_channel}")],
                    [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                ]
            
            message_text += "\n👉 *Join the channel(s) and then click '✅ Check Again'*"

            sent_msg = await update.message.reply_text(
                message_text,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        # Send file with disappearing timer
        try:
            filename = file_info['file_name']
            ext = filename.lower().split('.')[-1] if '.' in filename else ""
            
            warning_msg = (
                f"\n\n⚠️ *This { 'video' if file_info['is_video'] else 'file' } will disappear in {DELETE_AFTER//60} minutes*\n"
                f"📤 *Forward to Saved Messages right now to keep it forever*\n"
                f"💾 *File is stored permanently in database*"
            )
            
            chat_id = update.effective_chat.id
            
            if file_info['is_video'] and ext in PLAYABLE_EXTS:
                sent = await context.bot.send_video(
                    chat_id=chat_id,
                    video=file_info["file_id"],
                    caption=f"🎬 *{filename}*\n📥 Accessed {file_info.get('access_count', 0)} times{warning_msg}",
                    parse_mode="Markdown",
                    supports_streaming=True,
                    message_auto_delete_timer=DELETE_AFTER,   # <--- THIS MAKES MEDIA DISAPPEAR
                    protect_content=True                          # prevents save/forward
                )
            else:
                sent = await context.bot.send_document(
                    chat_id=chat_id,
                    document=file_info["file_id"],
                    caption=f"📁 *{filename}*\n📥 Accessed {file_info.get('access_count', 0)} times{warning_msg}",
                    parse_mode="Markdown",
                    message_auto_delete_timer=DELETE_AFTER,   # <--- THIS MAKES MEDIA DISAPPEAR
                    protect_content=True
                )
            
            # We do NOT schedule message deletion here → caption stays visible
            
        except Exception as e:
            log.error(f"Error sending file: {e}", exc_info=True)
            error_msg = str(e).lower()
            
            if "file is too big" in error_msg or "too large" in error_msg:
                text = "❌ File is too large. Maximum size is 50MB for videos."
            elif "file not found" in error_msg or "invalid file id" in error_msg:
                text = "❌ File expired from Telegram servers. Please contact admin."
            elif "forbidden" in error_msg:
                text = "❌ Bot can't send messages here."
            else:
                text = "❌ Failed to send file. Please try again."
            
            sent_msg = await update.message.reply_text(text)
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.error(f"Start error: {e}", exc_info=True)
        if update.message:
            sent_msg = await update.message.reply_text("❌ Error processing request")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        if not query:
            return
        await query.answer()
        
        user_id = query.from_user.id
        data = query.data
        
        if data == "check_membership":
            result = await check_membership(user_id, context, force_check=True)
            
            if result["all_joined"]:
                await query.edit_message_text(
                    f"✅ *Great! You've joined both channels!*\n\n"
                    "Now you can use file links shared by the admin.\n"
                    f"⚠️ *Note:* Media disappears after {DELETE_AFTER//60} minutes — forward immediately!",
                    parse_mode="Markdown"
                )
            else:
                message_text = "❌ *Membership Check Failed*\n\n"
                missing_count = len(result["missing_channels"])
                
                if missing_count == 2:
                    message_text += "You're not a member of either channel.\n"
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                        [InlineKeyboardButton(f"📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                        [InlineKeyboardButton("🔄 Check Again", callback_data="check_membership")]
                    ]
                elif missing_count == 1:
                    missing_channel = result["missing_channels"][0].replace("@", "")
                    channel_name = "Channel 1" if CHANNEL_1 in missing_channel else "Channel 2"
                    
                    message_text += f"You're missing {channel_name}.\n"
                    
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join {channel_name}", url=f"https://t.me/{missing_channel}")],
                        [InlineKeyboardButton("🔄 Check Again", callback_data="check_membership")]
                    ]
                
                message_text += "\nJoin the channel(s) and check again."

                await query.edit_message_text(
                    message_text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            return
        
        if data.startswith("check|"):
            _, key = data.split("|", 1)
            
            file_info = db.get_file(key)
            if not file_info:
                await query.edit_message_text("❌ File not found. It may have been manually deleted by admin.")
                return
            
            result = await check_membership(user_id, context, force_check=True)
            
            if not result['all_joined']:
                text = "❌ *Still Not Joined*\n\n"
                missing_count = len(result["missing_channels"])
                
                if missing_count == 2:
                    text += "You need to join both channels:\n"
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                        [InlineKeyboardButton(f"📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                        [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                    ]
                elif missing_count == 1:
                    missing_channel = result["missing_channels"][0].replace("@", "")
                    channel_name = "Channel 1" if CHANNEL_1 in missing_channel else "Channel 2"
                    
                    text += f"You need to join {channel_name}:\n"
                    
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join {channel_name}", url=f"https://t.me/{missing_channel}")],
                        [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                    ]
                
                text += "\nJoin and click 'Check Again'"
                
                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return
            
            # Send file with disappearing timer
            try:
                filename = file_info.get('file_name', 'file')
                ext = filename.lower().split('.')[-1] if '.' in filename else ""
                
                warning_msg = (
                    f"\n\n⚠️ *This { 'video' if file_info['is_video'] else 'file' } will disappear in {DELETE_AFTER//60} minutes*\n"
                    f"📤 *Forward to Saved Messages right now to keep it forever*\n"
                    f"💾 *File is stored permanently in database*"
                )
                
                chat_id = query.message.chat_id
                
                if file_info['is_video'] and ext in PLAYABLE_EXTS:
                    sent_msg = await context.bot.send_video(
                        chat_id=chat_id,
                        video=file_info["file_id"],
                        caption=f"🎬 *{filename}*\n📥 Accessed {file_info.get('access_count', 0)} times{warning_msg}",
                        parse_mode="Markdown",
                        supports_streaming=True,
                        message_auto_delete_timer=DELETE_AFTER,
                        protect_content=True
                    )
                else:
                    sent_msg = await context.bot.send_document(
                        chat_id=chat_id,
                        document=file_info["file_id"],
                        caption=f"📁 *{filename}*\n📥 Accessed {file_info.get('access_count', 0)} times{warning_msg}",
                        parse_mode="Markdown",
                        message_auto_delete_timer=DELETE_AFTER,
                        protect_content=True
                    )
                
                await query.edit_message_text("✅ *Access granted! File sent below.*", parse_mode="Markdown")
                # No schedule_message_deletion here → caption stays
                
            except Exception as e:
                log.error(f"Failed to send file in callback: {e}", exc_info=True)
                error_msg = str(e).lower()
                
                if "file is too big" in error_msg or "too large" in error_msg:
                    text = "❌ File is too large (max 50MB)."
                elif "file not found" in error_msg or "invalid file id" in error_msg:
                    text = "❌ File expired from Telegram servers."
                elif "forbidden" in error_msg:
                    text = "❌ Bot can't send files here."
                else:
                    text = "❌ Failed to send file. Please try again."
                
                await query.edit_message_text(text, parse_mode="Markdown")
        
    except Exception as e:
        log.error(f"Callback error: {e}", exc_info=True)
        if update.callback_query:
            try:
                await update.callback_query.answer("An error occurred. Please try again.", show_alert=True)
            except:
                pass

async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        msg = update.message
        video = msg.video
        document = msg.document

        file_id = None
        filename = None
        mime_type = None
        file_size = 0
        is_video = False

        if video:
            file_id = video.file_id
            filename = video.file_name or f"video_{int(time.time())}.mp4"
            mime_type = video.mime_type or "video/mp4"
            file_size = video.file_size or 0
            is_video = True
        elif document:
            filename = document.file_name or f"document_{int(time.time())}"
            file_id = document.file_id
            mime_type = document.mime_type or ""
            file_size = document.file_size or 0
            ext = filename.lower().split('.')[-1] if '.' in filename else ""
            
            if ext in ALL_VIDEO_EXTS:
                is_video = True
        else:
            sent_msg = await msg.reply_text("❌ Please send a video or document")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        file_info = {
            "file_name": filename,
            "mime_type": mime_type,
            "is_video": is_video,
            "size": int(file_size) if file_size else 0
        }

        key = db.save_file(file_id, file_info)
        link = f"https://t.me/{bot_username}?start={key}"

        sent_msg = await msg.reply_text(
            f"✅ *Upload Successful*\n\n"
            f"📁 *Name:* `{filename}`\n"
            f"🎬 *Type:* {'Video' if is_video else 'Document'}\n"
            f"📦 *Size:* {file_size/1024/1024:.1f} MB\n"
            f"🔑 *Key:* `{key}`\n"
            f"⏰ *Media disappears after:* {DELETE_AFTER//60} minutes\n"
            f"💾 *Storage:* PERMANENT in database\n\n"
            f"🔗 *Link:*\n`{link}`\n\n"
            f"⚠️ *Note:* File will be stored FOREVER unless manually deleted",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.exception("Upload error")
        sent_msg = await update.message.reply_text(f"❌ Upload failed: {str(e)[:200]}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

def start_bot():
    if not BOT_TOKEN:
        print("❌ ERROR: BOT_TOKEN is not set!")
        return
    
    if not ADMIN_ID or ADMIN_ID == 0:
        print("❌ ERROR: ADMIN_ID is not set or invalid!")
        return
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    if application.job_queue:
        print("🟢 Job queue initialized")
    else:
        print("⚠️ Job queue not available - some deletion features limited")

    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cleanup", cleanup))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("clearcache", clearcache))
    application.add_handler(CommandHandler("testchannel", testchannel))
    application.add_handler(CommandHandler("listfiles", listfiles))
    application.add_handler(CommandHandler("deletefile", deletefile))
    
    application.add_handler(CallbackQueryHandler(check_join, pattern=r"^check_membership$"))
    application.add_handler(CallbackQueryHandler(check_join, pattern=r"^check\|"))

    upload_filter = filters.VIDEO | filters.Document.ALL
    application.add_handler(
        MessageHandler(upload_filter & filters.User(ADMIN_ID) & filters.ChatType.PRIVATE, upload)
    )

    print("🟢 Bot is running and listening...")
    print(f"🟢 Bot username: @{bot_username}")
    print(f"🟢 Admin ID: {ADMIN_ID}")
    print(f"🟢 Channels: @{CHANNEL_1}, @{CHANNEL_2}")
    print(f"🟢 Media disappears after: {DELETE_AFTER//60} minutes (message stays)")
    print(f"🟢 Database auto-cleanup: DISABLED (files stored permanently)")
    print(f"🟢 Max stored files: {MAX_STORED_FILES}")
    
    db.clear_membership_cache()
    
    try:
        file_count = db.get_file_count()
        print(f"🟢 Database initialized. Files in database: {file_count}")
    except Exception as e:
        print(f"⚠️ Database initialization failed: {e}")
    
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )

def main():
    print("\n" + "=" * 50)
    print("🤖 TELEGRAM FILE BOT - PERMANENT STORAGE + MEDIA AUTO-DISAPPEAR")
    print("=" * 50)

    if not BOT_TOKEN:
        print("❌ ERROR: BOT_TOKEN is not set!")
        return

    if not ADMIN_ID or ADMIN_ID == 0:
        print("❌ ERROR: ADMIN_ID is not set or invalid!")
        return

    print(f"🟢 Admin ID: {ADMIN_ID}")
    print(f"🟢 Channels: @{CHANNEL_1}, @{CHANNEL_2}")
    print(f"🟢 Media disappears after: {DELETE_AFTER//60} minutes")
    print(f"🟢 Database storage: PERMANENT (no auto-cleanup)")
    
    print("\n🟢 Starting Flask web dashboard...")
    flask_thread = threading.Thread(target=run_flask_thread, daemon=True)
    flask_thread.start()
    time.sleep(1)
    print(f"🟢 Flask running on port {os.environ.get('PORT', 10000)}")

    start_bot()

if __name__ == "__main__":
    main()
