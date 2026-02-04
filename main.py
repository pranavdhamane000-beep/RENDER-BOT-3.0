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
        </div>
        
        <div class="info">
            <h3>📊 Bot Information</h3>
            <ul>
                <li>Service: <strong>Render Web Service</strong></li>
                <li>Bot: <strong>@{{ bot_username }}</strong></li>
                <li>Storage: <strong>SQLite Database</strong></li>
            </ul>
        </div>
        
        <div class="warning">
            <h3>⚠️ Important Notes</h3>
            <ul>
                <li><strong>🎥 Playable:</strong> MP4, MOV, M4V (sent as videos)</li>
                <li><strong>📁 Download only:</strong> MKV, AVI, WEBM (sent as documents)</li>
                <li><strong>💾 Storage:</strong> Database persists between deployments</li>
            </ul>
        </div>
        
        <div class="info">
            <h3>📞 Start Bot</h3>
            <p><a href="https://t.me/{{ bot_username }}" target="_blank" class="btn">Start @{{ bot_username }}</a></p>
        </div>
        
        {% if error %}
        <div class="error">
            <h3>⚠️ System Notice</h3>
            <p>{{ error }}</p>
        </div>
        {% endif %}
        
        <footer style="margin-top: 20px; border-top: 1px solid rgba(255,255,255,0.2); padding-top: 10px; font-size: 0.8rem;">
            <small>Render • {{ current_time }} • v1.0</small>
        </footer>
    </div>
</body>
</html>
    """
    
    uptime_seconds = time.time() - start_time
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))
    
    # Get file count from database
    file_count = 0
    try:
        file_count = db.get_file_count()
    except:
        pass
    
    # Check for channel access issues
    error = None
    try:
        import os
        if not os.environ.get("BOT_TOKEN"):
            error = "BOT_TOKEN not set in environment"
    except:
        pass
    
    return render_template_string(html_content, 
                                  bot_username=bot_username,
                                  uptime=uptime_str,
                                  current_time=datetime.now().strftime("%H:%M:%S"),
                                  file_count=file_count,
                                  error=error)

@app.route('/health')
def health():
    return jsonify({
        "status": "OK", 
        "timestamp": datetime.now().isoformat(),
        "service": "telegram-file-bot",
        "uptime": str(timedelta(seconds=int(time.time() - start_time))),
        "database": "sqlite"
    }), 200

@app.route('/ping')
def ping():
    return "pong", 200

def run_flask_thread():
    """Run Flask server in a thread for Render"""
    port = int(os.environ.get('PORT', 10000))
    
    # Disable verbose logging
    import warnings
    warnings.filterwarnings("ignore")
    
    import logging as flask_logging
    flask_logging.getLogger('werkzeug').setLevel(flask_logging.ERROR)
    flask_logging.getLogger('flask').setLevel(flask_logging.ERROR)
    
    # Use threaded=True for Render compatibility
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)
# ===========================================================

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes
)

# ================= CONFIG =================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

CHANNEL_1 = os.environ.get("CHANNEL_1", "A_Knight_of_the_Seven_Kingdoms_t")
CHANNEL_2 = os.environ.get("CHANNEL_2", "your_movies_web")

# SQLite database for persistent storage
DB_PATH = Path("file_bot.db")
DELETE_AFTER = 600  # 10 minutes
TIMEOUT = 30
MAX_STORED_FILES = 100
AUTO_CLEANUP_DAYS = 7

# Playable formats
PLAYABLE_EXTS = {"mp4", "mov", "m4v", "mpeg", "mpg"}
PLAYABLE_MIME = {"video/mp4", "video/quicktime", "video/mpeg"}

# All video extensions
ALL_VIDEO_EXTS = {
    "mp4", "mkv", "mov", "avi", "webm", "flv", "m4v", 
    "3gp", "wmv", "mpg", "mpeg"
}

# MIME types for video detection
VIDEO_MIME_TYPES = {
    "video/mp4", "video/x-matroska", "video/quicktime",
    "video/x-msvideo", "video/webm", "video/x-flv", "video/3gpp",
    "video/x-ms-wmv", "video/mpeg"
}
# =========================================

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
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
                    (id, file_id, file_name, mime_type, is_video, file_size)
                    VALUES (?, ?, ?, ?, ?, ?)
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
                SELECT file_id, file_name, mime_type, is_video, file_size, timestamp
                FROM files WHERE id = ?
            ''', (file_id,))
            row = cursor.fetchone()
            
            if row:
                return {
                    'file_id': row[0],
                    'file_name': row[1],
                    'mime_type': row[2],
                    'is_video': bool(row[3]),
                    'size': row[4],
                    'timestamp': row[5]
                }
            return None
    
    def file_exists(self, file_id: str) -> bool:
        """Check if file exists"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM files WHERE id = ?", (file_id,))
            return cursor.fetchone() is not None
    
    def cleanup_old_files(self):
        """Remove files older than AUTO_CLEANUP_DAYS"""
        if AUTO_CLEANUP_DAYS <= 0:
            return
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM files 
                WHERE timestamp < datetime('now', ?)
            ''', (f'-{AUTO_CLEANUP_DAYS} days',))
            
            deleted = cursor.rowcount
            if deleted > 0:
                log.info(f"Auto-cleanup removed {deleted} old files")
            
            # Also limit total files
            cursor.execute('''
                DELETE FROM files 
                WHERE id NOT IN (
                    SELECT id FROM files 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                )
            ''', (MAX_STORED_FILES,))
            
            if cursor.rowcount > 0:
                log.info(f"Limited files to {MAX_STORED_FILES}")
            
            conn.commit()
    
    def get_file_count(self) -> int:
        """Get total number of files"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM files")
            return cursor.fetchone()[0]
    
    def cache_membership(self, user_id: int, channel: str, is_member: bool):
        """Cache membership check result"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO membership_cache (user_id, channel, is_member, timestamp)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, channel, 1 if is_member else 0))
            conn.commit()
    
    def get_cached_membership(self, user_id: int, channel: str) -> Optional[bool]:
        """Get cached membership result (valid for 5 minutes)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT is_member FROM membership_cache 
                WHERE user_id = ? AND channel = ? 
                AND timestamp > datetime('now', '-5 minutes')
            ''', (user_id, channel))
            row = cursor.fetchone()
            return bool(row[0]) if row else None

# Initialize database
db = Database()

def get_file_count():
    """For web dashboard compatibility"""
    return db.get_file_count()

# ============ MEMBERSHIP CHECK ============
async def is_member_async(bot, channel: str, user_id: int) -> Optional[bool]:
    """ASYNCHRONOUS membership check with caching"""
    # Check cache first
    cached = db.get_cached_membership(user_id, channel)
    if cached is not None:
        return cached
    
    chat_id = f"@{channel}" if not channel.startswith("@") else channel
    
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        is_member = member.status in ("member", "administrator", "creator")
        db.cache_membership(user_id, channel, is_member)
        return is_member
    except Exception as e:
        err = str(e).lower()
        if "user not found" in err or "chat not found" in err:
            db.cache_membership(user_id, channel, False)
            return False
        elif "forbidden" in err:
            # Bot doesn't have permission to check membership
            log.warning(f"Bot cannot check membership in {channel}: {e}")
            return None
        else:
            log.warning(f"Membership check failed for {channel}: {e}")
            return None

async def check_membership_async(bot, user_id: int) -> Dict[str, Any]:
    """ASYNCHRONOUS membership check for both channels"""
    result = {
        'channel1': False,
        'channel2': False,
        'all_joined': False,
        'errors': []
    }
    
    try:
        # Check first channel
        ch1 = await is_member_async(bot, CHANNEL_1, user_id)
        if ch1 is None:
            result['errors'].append(f"Cannot check @{CHANNEL_1}")
            ch1 = False
        
        # Check second channel
        ch2 = await is_member_async(bot, CHANNEL_2, user_id)
        if ch2 is None:
            result['errors'].append(f"Cannot check @{CHANNEL_2}")
            ch2 = False
        
        result['channel1'] = ch1
        result['channel2'] = ch2
        result['all_joined'] = ch1 and ch2
        
    except Exception as e:
        log.error(f"Membership check error: {e}")
        result['errors'].append(str(e))
    
    return result

# ============ VIDEO DETECTION ============
def is_video_file(document) -> bool:
    """Check if document is a video file"""
    if not document:
        return False
    
    # Check mime type
    mime = getattr(document, 'mime_type', '').lower()
    if mime:
        for video_mime in VIDEO_MIME_TYPES:
            if video_mime in mime:
                return True
    
    # Check file extension
    filename = getattr(document, 'file_name', '').lower()
    if filename:
        ext = filename.split('.')[-1] if '.' in filename else ''
        if ext in ALL_VIDEO_EXTS:
            return True
    
    return False

def should_send_as_video(file_info: Dict[str, Any]) -> Tuple[bool, bool]:
    """
    Determine if file should be sent as playable video
    Returns: (send_as_video, supports_streaming)
    """
    filename = file_info.get('file_name', '').lower()
    mime_type = file_info.get('mime_type', '').lower()
    
    # Check by mime type
    if mime_type:
        if 'video/mp4' in mime_type:
            return True, True
        elif 'video/quicktime' in mime_type or 'video/mpeg' in mime_type:
            return True, True
    
    # Check by extension
    ext = filename.split('.')[-1] if '.' in filename else ''
    if ext in PLAYABLE_EXTS:
        return True, True
    
    return False, False

# ============ DELETE JOB ============
async def delete_job(context):
    """Delete message"""
    job = context.job
    chat_id = job.data.get("chat")
    message_id = job.data.get("msg")
    
    if not chat_id or not message_id:
        return
    
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

# ============ ERROR HANDLER ============
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Error handler"""
    log.error(f"Error: {context.error}", exc_info=False)

# ============ CLEANUP COMMAND ============
async def cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cleanup command - only command besides start/upload"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    days = 7
    if context.args:
        try:
            days = int(context.args[0])
            days = max(1, min(days, 30))
        except ValueError:
            await update.message.reply_text("Usage: /cleanup [days=7]")
            return
    
    try:
        # Run cleanup
        db.cleanup_old_files()
        
        # Get new count
        file_count = db.get_file_count()
        
        msg = f"🧹 Cleanup complete\n"
        msg += f"Files retained: {file_count}\n"
        msg += f"Old files (> {days} days) removed"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Cleanup failed: {str(e)[:100]}")

# ============ STATS ============

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    uptime_seconds = time.time() - start_time
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))

    file_count = db.get_file_count()
    db_size = DB_PATH.stat().st_size / 1024 if DB_PATH.exists() else 0

    await update.message.reply_text(
        "📊 *Bot Statistics*\n\n"
        f"🤖 Bot: @{bot_username}\n"
        f"⏱ Uptime: {uptime_str}\n"
        f"📁 Files: {file_count}\n"
        f"💾 DB Size: {db_size:.1f} KB\n"
        f"🧹 Auto-cleanup: {AUTO_CLEANUP_DAYS} days\n"
        f"⏰ Auto-delete: {DELETE_AFTER//60} minutes\n\n"
        f"📢 Channels:\n"
        f"1. @{CHANNEL_1}\n"
        f"2. @{CHANNEL_2}",
        parse_mode="Markdown"
    )

# ============ START COMMAND ============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message:
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        args = context.args

        # ❗ No file key → just show join info
        if not args:
            keyboard = [
                [InlineKeyboardButton("📢 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                [InlineKeyboardButton("📢 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")]
            ]

            await update.message.reply_text(
                "🤖 *File Sharing Bot*\n\n"
                "🔗 Use admin-provided links\n"
                "📢 Join both channels to access files",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
            return

        # ✅ File key exists
        key = args[0]

        file_info = db.get_file(key)
        if not file_info:
            await update.message.reply_text("❌ File not found or expired")
            return

        # Membership check
        result = await check_membership_async(context.bot, user_id)

        if not result["all_joined"]:
            keyboard = [
                [InlineKeyboardButton("📢 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                [InlineKeyboardButton("📢 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
            ]

            await update.message.reply_text(
                "🔒 *Access Locked*\n\n"
                "Please join both channels to unlock this file 👇",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )
            return

        # ✅ Send file
        send_as_video, supports_streaming = should_send_as_video(file_info)

        if send_as_video and file_info["is_video"]:
            sent = await context.bot.send_video(
                chat_id=chat_id,
                video=file_info["file_id"],
                caption=f"📹 {file_info['file_name']}",
                supports_streaming=supports_streaming
            )
        else:
            sent = await context.bot.send_document(
                chat_id=chat_id,
                document=file_info["file_id"],
                caption=f"📁 {file_info['file_name']}"
            )

        context.job_queue.run_once(
            delete_job,
            DELETE_AFTER,
            data={"chat": chat_id, "msg": sent.message_id}
        )

    except Exception as e:
        log.error(f"Start error: {e}")
        await update.message.reply_text("❌ Error processing request")



# ============ CALLBACK ============
async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle check membership callback"""
    try:
        query = update.callback_query
        if not query:
            return
        await query.answer()
        
        user_id = query.from_user.id
        data_parts = query.data.split("|")
        
        if len(data_parts) != 2:
            return
        
        _, key = data_parts
        
        # Check if file exists
        file_info = db.get_file(key)
        if not file_info:
            await query.edit_message_text("❌ File expired")
            return
        
        # Check membership (ASYNC)
        result = await check_membership_async(context.bot, user_id)
        
        if not result['all_joined']:
            # Update the message with current status
            text = "❌ Still not joined:\n"
            if not result['channel1']:
                text += f"\n• @{CHANNEL_1}"
            if not result['channel2']:
                text += f"\n• @{CHANNEL_2}"
            
            if result['errors']:
                text += f"\n\n⚠️ {', '.join(result['errors'])}"
            
            keyboard = []
            if not result['channel1']:
                keyboard.append([InlineKeyboardButton("Join Channel 1", url=f"https://t.me/{CHANNEL_1.replace('@', '')}")])
            if not result['channel2']:
                keyboard.append([InlineKeyboardButton("Join Channel 2", url=f"https://t.me/{CHANNEL_2.replace('@', '')}")])
            keyboard.append([InlineKeyboardButton("🔄 Check Again", callback_data=f"check|{key}")])
            
            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
            return
        
        # Send file
        filename = file_info.get('file_name', 'file')
        send_as_video, supports_streaming = should_send_as_video(file_info)
        
        try:
            if send_as_video and file_info.get('is_video'):
                sent_msg = await context.bot.send_video(
                    chat_id=query.message.chat_id,
                    video=file_info["file_id"],
                    caption=f"📹 {filename}",
                    supports_streaming=supports_streaming
                )
            else:
                sent_msg = await context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file_info["file_id"],
                    caption=f"📁 {filename}"
                )
        except Exception as e:
            await query.edit_message_text("❌ Failed to send file")
            return
        
        await query.edit_message_text("✅ Access granted! File sent.")
        
        # Schedule deletion
        context.job_queue.run_once(
            delete_job,
            DELETE_AFTER,
            data={"chat": query.message.chat_id, "msg": sent_msg.message_id}
        )
        
    except Exception as e:
        log.error(f"Callback error: {e}")
        if update.callback_query:
            await update.callback_query.answer("Error occurred", show_alert=True)

# ============ UPLOAD ============
# ============ UPLOAD HANDLER (ADMIN ONLY) ============
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

        # 🎥 VIDEO MESSAGE (MP4 etc.)
        if video:
            file_id = video.file_id
            filename = video.file_name or f"video_{int(time.time())}.mp4"
            mime_type = video.mime_type or "video/mp4"
            file_size = video.file_size or 0
            is_video = True

        # 📁 DOCUMENT (MKV, AVI, ZIP, etc.)
        elif document:
            file_id = document.file_id
            filename = document.file_name or f"file_{int(time.time())}"
            mime_type = document.mime_type or ""
            file_size = document.file_size or 0

            ext = filename.lower().split('.')[-1]

            # ❗ Non-streamable formats
            if ext in {"mkv", "avi", "webm", "flv"}:
                is_video = False
                await msg.reply_text(
                    " Enjoy \n"
                    f"Format: {ext.upper()}\n"
                    "Users will download this file"
                )
            else:
                is_video = mime_type.startswith("video/")

        else:
            await msg.reply_text("❌ Please send a video or document")
            return

        # 💾 SAVE TO DATABASE
        file_info = {
            "file_name": filename,
            "mime_type": mime_type,
            "is_video": is_video,
            "size": int(file_size)
        }

        key = db.save_file(file_id, file_info)
        # 🔗 GENERATE LINK
        
        link = f"https://t.me/{bot_username}?start={key}"

        await msg.reply_text(
            "✅ *Upload Successful*\n\n"
            f"📁 Name: `{filename}`\n"
            f"🎬 Type: {'Video' if is_video else 'Document'}\n"
            f"📦 Size: {file_size/1024/1024:.1f} MB\n\n"
            f"🔗 Link:\n{link}",
            parse_mode="Markdown"
        )

    except Exception as e:
        log.exception("Upload error")
        await update.message.reply_text(
            f"❌ Upload failed:\n{str(e)[:200]}"
        )


# ============ MAIN FUNCTION ============



# ========== DEFINE ENVIRONMENT VARIABLES ==========


# ========== PLACEHOLDER HANDLER FUNCTIONS ==========



# ---------------- FLASK SERVER ----------------
# def run_flask_thread():
#     from flask import Flask, jsonify
#     from werkzeug.serving import make_server

#     app = Flask(__name__)

#     @app.route("/")
#     def index():
#         return "Bot Web Dashboard ✅"

#     @app.route("/health")
#     def health():
#         return jsonify({"status": "ok", "service": "telegram-bot"}), 200

#     @app.route("/ping")
#     def ping():
#         return "pong", 200

#     port = int(os.environ.get("PORT", 10000))
#     server = make_server("0.0.0.0", port, app)
#     server.serve_forever()


# ---------------- TELEGRAM BOT ----------------
def start_bot():
    # ✅ CRITICAL FIX: Check for BOT_TOKEN here too
    if not BOT_TOKEN:
        print("❌ ERROR: BOT_TOKEN is not set!")
        return
    
    application = Application.builder().token(BOT_TOKEN).build()

    # Handlers (must be async functions!)
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cleanup", cleanup))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CallbackQueryHandler(check_join, pattern=r"^check\|"))

    upload_filter = filters.VIDEO | filters.Document.ALL


    application.add_handler(
        #MessageHandler(upload_filter & filters.User(ADMIN_ID), upload)
       MessageHandler(upload_filter & filters.User(ADMIN_ID) & filters.ChatType.PRIVATE,
    upload) 
    )

    print("🟢 Bot is running and listening...")
    print("🟢 Press Ctrl+C to stop")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )


# ---------------- MAIN ----------------
def main():
    print("\n" + "=" * 50)
    print("🤖 TELEGRAM FILE BOT")
    print("=" * 50)

    # ✅ ADD THESE CHECKS
    if not BOT_TOKEN:
        print("❌ ERROR: BOT_TOKEN is not set!")
        return

    if not ADMIN_ID or ADMIN_ID == 0:
        print("❌ ERROR: ADMIN_ID is not set or invalid!")
        return

    print(f"🟢 Admin ID: {ADMIN_ID}")
    print(f"🟢 Channels: @{CHANNEL_1}, @{CHANNEL_2}")

    # Start Flask in a separate thread
    print("🟢 Starting Flask web dashboard...")
    flask_thread = threading.Thread(target=run_flask_thread, daemon=True)
    flask_thread.start()
    time.sleep(1)  # Let Flask initialize
    print(f"🟢 Flask running on port {os.environ.get('PORT', 10000)}")

    # Start Telegram bot in main thread
    start_bot()


if __name__ == "__main__":
    main()
