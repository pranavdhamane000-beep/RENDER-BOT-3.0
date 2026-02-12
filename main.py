import asyncio
import json
import logging
import os
import sys
import time
import traceback
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
import threading
from contextlib import contextmanager
import ssl

# Database setup with pg8000 (Standard interface)
import pg8000
from pg8000 import Connection, Cursor

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
)

# ================= CONFIG =================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

# Channel usernames (without @)
CHANNEL_1 = os.environ.get("CHANNEL_1", "A_knight_of_the_seven_kingdoms_r").replace("@", "")
CHANNEL_2 = os.environ.get("CHANNEL_2", "A_knight_of_the_seven_kingdoms_y").replace("@", "")

# PostgreSQL database URL from Render
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Delete bot messages after (seconds)
DELETE_AFTER = 600  # 10 minutes
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

# ================= DATABASE WITH PG8000 =================

def parse_database_url():
    """Parse DATABASE_URL into connection parameters"""
    if not DATABASE_URL:
        log.error("DATABASE_URL environment variable is not set!")
        raise ValueError("DATABASE_URL is required for PostgreSQL")
    
    # Handle postgres:// URL scheme
    url_str = DATABASE_URL
    if url_str.startswith("postgres://"):
        url_str = url_str.replace("postgres://", "postgresql://", 1)
    
    url = urllib.parse.urlparse(url_str)
    
    params = {
        'host': url.hostname,
        'port': url.port or 5432,
        'database': url.path[1:],
        'user': url.username,
        'password': url.password,
    }
    
    log.info(f"Database connection: {params['user']}@{params['host']}:{params['port']}/{params['database']}")
    return params

# Parse connection parameters
db_params = parse_database_url()

# Create SSL context that doesn't verify certificates (for Render)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

@contextmanager
def get_cursor():
    """Get a database cursor with SSL disabled for Render"""
    conn = None
    cursor = None
    try:
        conn = pg8000.connect(
            user=db_params['user'],
            password=db_params['password'],
            host=db_params['host'],
            port=db_params['port'],
            database=db_params['database'],
            ssl_context=ssl_context,
            timeout=30
        )
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        log.error(f"Database error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

class Database:
    def __init__(self):
        self.init_db()
    
    def init_db(self):
        """Initialize database with required tables"""
        try:
            with get_cursor() as cursor:
                # Create files table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS files (
                        id SERIAL PRIMARY KEY,
                        file_id TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        mime_type TEXT,
                        is_video BOOLEAN DEFAULT FALSE,
                        file_size INTEGER DEFAULT 0,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        access_count INTEGER DEFAULT 0
                    )
                ''')
                
                # Create membership_cache table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS membership_cache (
                        user_id INTEGER,
                        channel TEXT,
                        is_member BOOLEAN,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (user_id, channel)
                    )
                ''')
                
                # Table to track scheduled deletions
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS scheduled_deletions (
                        chat_id BIGINT NOT NULL,
                        message_id INTEGER NOT NULL,
                        scheduled_time TIMESTAMP NOT NULL,
                        delete_after INTEGER DEFAULT 600,
                        PRIMARY KEY (chat_id, message_id)
                    )
                ''')
                
                # Users table for tracking user interactions
                cursor.execute('''
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
                
                # Create indexes
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON membership_cache(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_deletions_time ON scheduled_deletions(scheduled_time)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_first_seen ON users(first_seen)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_access ON files(access_count)')
                
                log.info("✅ Database tables initialized successfully")
        except Exception as e:
            log.error(f"❌ Failed to initialize database: {e}")
            raise

    def save_file(self, file_id: str, file_info: dict) -> str:
        """Save file info and return generated ID"""
        with get_cursor() as cursor:
            cursor.execute(
                '''
                INSERT INTO files (file_id, file_name, mime_type, is_video, file_size, access_count)
                VALUES (%s, %s, %s, %s, %s, 0)
                RETURNING id
                ''',
                (file_id,
                file_info.get('file_name', ''),
                file_info.get('mime_type', ''),
                file_info.get('is_video', False),
                file_info.get('size', 0))
            )
            result = cursor.fetchone()
            return str(result[0])

    def get_file(self, file_id: str) -> Optional[dict]:
        """Get file info by ID"""
        with get_cursor() as cursor:
            cursor.execute('''
                SELECT id, file_id, file_name, mime_type, is_video, file_size, 
                       timestamp, access_count
                FROM files WHERE id = %s
            ''', (file_id,))
            row = cursor.fetchone()
            
            if row:
                file_data = {
                    'id': str(row[0]),
                    'file_id': row[1],
                    'file_name': row[2],
                    'mime_type': row[3],
                    'is_video': bool(row[4]),
                    'size': row[5],
                    'timestamp': row[6].isoformat() if row[6] else None,
                    'access_count': row[7]
                }
                
                cursor.execute('UPDATE files SET access_count = access_count + 1 WHERE id = %s', (file_id,))
                cursor.connection.commit()
                file_data['access_count'] += 1
                
                return file_data
            return None
    
    def get_file_count(self) -> int:
        """Get total number of files"""
        with get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM files")
            result = cursor.fetchone()
            return result[0] if result else 0
    
    def cache_membership(self, user_id: int, channel: str, is_member: bool):
        """Cache membership check result"""
        with get_cursor() as cursor:
            cursor.execute('''
                INSERT INTO membership_cache (user_id, channel, is_member, timestamp)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id, channel) 
                DO UPDATE SET is_member = EXCLUDED.is_member, timestamp = EXCLUDED.timestamp
            ''', (user_id, channel, is_member))
    
    def get_cached_membership(self, user_id: int, channel: str) -> Optional[bool]:
        """Get cached membership result (valid for 5 minutes)"""
        with get_cursor() as cursor:
            cursor.execute('''
                SELECT is_member FROM membership_cache 
                WHERE user_id = %s AND channel = %s 
                AND timestamp > CURRENT_TIMESTAMP - INTERVAL '5 minutes'
            ''', (user_id, channel))
            row = cursor.fetchone()
            return bool(row[0]) if row else None

    def clear_membership_cache(self, user_id: Optional[int] = None):
        """Clear membership cache for a user or all users"""
        with get_cursor() as cursor:
            if user_id:
                cursor.execute("DELETE FROM membership_cache WHERE user_id = %s", (user_id,))
                log.info(f"Cleared cache for user {user_id}")
            else:
                cursor.execute("DELETE FROM membership_cache")
                log.info("Cleared all membership cache")

    def delete_file(self, file_id: str) -> bool:
        """Manually delete a file from database (admin only)"""
        with get_cursor() as cursor:
            cursor.execute("DELETE FROM files WHERE id = %s", (file_id,))
            return cursor.rowcount > 0

    def get_all_files(self) -> list:
        """Get all files for admin view"""
        with get_cursor() as cursor:
            cursor.execute('''
                SELECT id, file_name, is_video, file_size, timestamp, access_count 
                FROM files ORDER BY timestamp DESC LIMIT 100
            ''')
            rows = cursor.fetchall()
            return [
                (row[0], row[1], row[2], row[3], 
                 row[4].isoformat() if row[4] else '', 
                 row[5])
                for row in rows
            ]
    
    def schedule_message_deletion(self, chat_id: int, message_id: int):
        """Schedule a message for deletion in database"""
        scheduled_time = datetime.now() + timedelta(seconds=DELETE_AFTER)
        with get_cursor() as cursor:
            cursor.execute('''
                INSERT INTO scheduled_deletions (chat_id, message_id, scheduled_time, delete_after)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (chat_id, message_id) 
                DO UPDATE SET scheduled_time = EXCLUDED.scheduled_time, delete_after = EXCLUDED.delete_after
            ''', (chat_id, message_id, scheduled_time, DELETE_AFTER))
    
    def get_due_messages(self):
        """Get messages that are due for deletion"""
        with get_cursor() as cursor:
            cursor.execute('''
                SELECT chat_id, message_id FROM scheduled_deletions 
                WHERE scheduled_time <= CURRENT_TIMESTAMP
            ''')
            rows = cursor.fetchall()
            return [{'chat_id': row[0], 'message_id': row[1]} for row in rows]
    
    def remove_scheduled_message(self, chat_id: int, message_id: int):
        """Remove message from scheduled deletions"""
        with get_cursor() as cursor:
            cursor.execute('DELETE FROM scheduled_deletions WHERE chat_id = %s AND message_id = %s', 
                          (chat_id, message_id))

    def update_user_interaction(self, user_id: int, username: str = None, 
                               first_name: str = None, last_name: str = None,
                               file_accessed: bool = False):
        """Update user interaction timestamp and count"""
        with get_cursor() as cursor:
            cursor.execute("SELECT 1 FROM users WHERE user_id = %s", (user_id,))
            exists = cursor.fetchone()
            
            if exists:
                update_query = '''
                    UPDATE users 
                    SET last_active = CURRENT_TIMESTAMP,
                        total_interactions = total_interactions + 1,
                        username = COALESCE(%s, username),
                        first_name = COALESCE(%s, first_name),
                        last_name = COALESCE(%s, last_name)
                    WHERE user_id = %s
                '''
                cursor.execute(update_query, (username, first_name, last_name, user_id))
                
                if file_accessed:
                    cursor.execute('''
                        UPDATE users 
                        SET total_files_accessed = total_files_accessed + 1,
                            last_file_accessed = CURRENT_TIMESTAMP
                        WHERE user_id = %s
                    ''', (user_id,))
            else:
                cursor.execute('''
                    INSERT INTO users 
                    (user_id, username, first_name, last_name, first_seen, last_active, total_interactions)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                    ON CONFLICT (user_id) DO NOTHING
                ''', (user_id, username, first_name, last_name))
    
    def get_user_count(self) -> int:
        """Get total number of users"""
        with get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM users")
            result = cursor.fetchone()
            return result[0] if result else 0

# Initialize database
try:
    db = Database()
    log.info("✅ Database initialized successfully with SSL disabled for Render")
except Exception as e:
    log.error(f"❌ Failed to initialize database: {e}")
    sys.exit(1)

# ============ MESSAGE DELETION SYSTEM ============
async def delete_message_job(context):
    """Delete message after timer"""
    try:
        job = context.job
        chat_id = job.chat_id
        message_id = job.data
        
        if not chat_id or not message_id:
            return
        
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            db.remove_scheduled_message(chat_id, message_id)
        except:
            db.remove_scheduled_message(chat_id, message_id)
    except Exception as e:
        log.error(f"Error in delete_message_job: {e}")

async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    """Schedule a message for deletion after DELETE_AFTER seconds"""
    try:
        db.schedule_message_deletion(chat_id, message_id)
        
        if context.job_queue:
            context.job_queue.run_once(
                delete_message_job,
                DELETE_AFTER,
                data=message_id,
                chat_id=chat_id,
                name=f"delete_msg_{chat_id}_{message_id}"
            )
    except Exception as e:
        log.error(f"Failed to schedule deletion: {e}")

async def cleanup_overdue_messages(context: ContextTypes.DEFAULT_TYPE):
    """Clean up any overdue messages from database"""
    try:
        due_messages = db.get_due_messages()
        for msg in due_messages:
            try:
                await context.bot.delete_message(chat_id=msg['chat_id'], message_id=msg['message_id'])
            except:
                pass
            finally:
                db.remove_scheduled_message(msg['chat_id'], msg['message_id'])
    except Exception as e:
        log.error(f"Error in cleanup_overdue_messages: {e}")

# ============ MEMBERSHIP CHECK ============
async def check_user_in_channel(bot, channel: str, user_id: int, force_check: bool = False) -> bool:
    """Check if user is in channel"""
    if not force_check:
        cached = db.get_cached_membership(user_id, channel)
        if cached is not None:
            return cached
    
    try:
        channel_username = f"@{channel}" if not channel.startswith("@") else channel
        member = await bot.get_chat_member(chat_id=channel_username, user_id=user_id)
        is_member = member.status in ["member", "administrator", "creator"]
        db.cache_membership(user_id, channel.replace("@", ""), is_member)
        return is_member
    except Exception as e:
        error_msg = str(e).lower()
        if "user not found" in error_msg or "user not participant" in error_msg:
            db.cache_membership(user_id, channel.replace("@", ""), False)
            return False
        return True

async def check_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE, force_check: bool = False) -> Dict[str, Any]:
    """Check if user is member of both channels"""
    bot = context.bot
    result = {"channel1": False, "channel2": False, "all_joined": False, "missing_channels": []}
    
    if force_check:
        db.clear_membership_cache(user_id)
    
    ch1_result = await check_user_in_channel(bot, CHANNEL_1, user_id, force_check)
    result["channel1"] = ch1_result
    if not ch1_result:
        result["missing_channels"].append(f"@{CHANNEL_1}")
    
    ch2_result = await check_user_in_channel(bot, CHANNEL_2, user_id, force_check)
    result["channel2"] = ch2_result
    if not ch2_result:
        result["missing_channels"].append(f"@{CHANNEL_2}")
    
    result["all_joined"] = result["channel1"] and result["channel2"]
    return result

# ============ WEB ROUTES ============
@app.route('/')
def home():
    uptime_seconds = time.time() - start_time
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))
    
    try:
        file_count = db.get_file_count()
        user_count = db.get_user_count()
    except:
        file_count = 0
        user_count = 0
    
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>🤖 Telegram File Bot</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; min-height: 100vh; }
            .container { background: rgba(255, 255, 255, 0.1); backdrop-filter: blur(10px); padding: 20px; border-radius: 10px; box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2); }
            h1 { color: white; margin-top: 0; font-size: 1.5rem; }
            .status { background: rgba(0, 255, 0, 0.2); padding: 10px; border-radius: 8px; margin: 10px 0; border-left: 4px solid #00ff00; }
            .info { background: rgba(255, 255, 255, 0.1); padding: 10px; border-radius: 8px; margin: 10px 0; }
            a { color: #FFD700; text-decoration: none; }
            .btn { display: inline-block; background: #4CAF50; color: white; padding: 8px 16px; border-radius: 6px; margin: 5px; font-size: 0.9rem; }
            .warning { background: rgba(255, 165, 0, 0.2); border-left: 4px solid #ffa500; padding: 10px; border-radius: 8px; margin: 10px 0; font-size: 0.9rem; }
            ul { padding-left: 20px; }
            li { margin: 5px 0; }
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
                <p>Users in DB: {{ user_count }}</p>
                <p>📁 Storage: PERMANENT (no auto-delete)</p>
                <p>💾 Database: PostgreSQL</p>
            </div>
            <div class="info">
                <h3>📊 Bot Information</h3>
                <ul>
                    <li>Service: <strong>Render Web Service</strong></li>
                    <li>Bot: <strong>@{{ bot_username }}</strong></li>
                    <li>Channels: <strong>@{{ channel1 }}, @{{ channel2 }}</strong></li>
                    <li>File Storage: <strong>PERMANENT</strong></li>
                    <li>Database: <strong>PostgreSQL</strong></li>
                    <li>Message Auto-delete: <strong>{{ delete_minutes }} minutes</strong></li>
                    <li>Total Users: <strong>{{ user_count }}</strong></li>
                </ul>
            </div>
            <div class="warning">
                <h3>⚠️ Important Notes</h3>
                <ul>
                    <li>Files are stored <strong>PERMANENTLY</strong> in PostgreSQL database</li>
                    <li>Only chat messages auto-delete after {{ delete_minutes }} minutes</li>
                    <li>Users can access same file multiple times forever</li>
                    <li>Admin must manually delete files if needed</li>
                </ul>
            </div>
            <div class="info">
                <h3>📞 Start Bot</h3>
                <p><a href="https://t.me/{{ bot_username }}" target="_blank" class="btn">Start @{{ bot_username }}</a></p>
            </div>
            <footer style="margin-top: 20px; border-top: 1px solid rgba(255,255,255,0.2); padding-top: 10px; font-size: 0.8rem;">
                <small>Render • {{ current_time }} • PostgreSQL • User Tracking</small>
            </footer>
        </div>
    </body>
    </html>
    """
    
    return render_template_string(html_content, 
                                  bot_username=bot_username,
                                  uptime=uptime_str,
                                  current_time=datetime.now().strftime("%H:%M:%S"),
                                  file_count=file_count,
                                  user_count=user_count,
                                  channel1=CHANNEL_1,
                                  channel2=CHANNEL_2,
                                  delete_minutes=DELETE_AFTER//60)

@app.route('/health')
def health():
    try:
        return jsonify({
            "status": "OK", 
            "timestamp": datetime.now().isoformat(),
            "service": "telegram-file-bot",
            "uptime": str(timedelta(seconds=int(time.time() - start_time))),
            "database": "postgresql",
            "storage": "permanent",
            "file_count": db.get_file_count() if db else 0,
            "user_count": db.get_user_count() if db else 0
        }), 200
    except:
        return jsonify({"status": "OK", "service": "telegram-file-bot"}), 200

@app.route('/ping')
def ping():
    return "pong", 200

def run_flask():
    """Run Flask server in a separate thread"""
    port = int(os.environ.get('PORT', 10000))
    log.info(f"Starting Flask on port {port} binding to 0.0.0.0")
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)

# ============ BOT HANDLERS ============
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Error handler"""
    log.error(f"Error: {context.error}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    try:
        if not update.message:
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        args = context.args

        user = update.effective_user
        db.update_user_interaction(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )

        if not args:
            keyboard = [
                [InlineKeyboardButton("📢 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                [InlineKeyboardButton("📢 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                [InlineKeyboardButton("🔄 Check Membership", callback_data="check_membership")]
            ]
            sent_msg = await update.message.reply_text(
                "🤖 *Welcome to File Sharing Bot*\n\n"
                "🔗 *How to use:*\n"
                "1️⃣ Use admin-provided links\n"
                "2️⃣ Join both channels below\n"
                "3️⃣ Click 'Check Membership' after joining\n\n"
                f"⚠️ *Note:* All bot messages auto-delete after {DELETE_AFTER//60} minutes\n"
                "💾 *Storage:* Files are stored permanently in PostgreSQL database",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        key = args[0]
        file_info = db.get_file(key)
        
        if not file_info:
            sent_msg = await update.message.reply_text("❌ File not found.")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        result = await check_membership(user_id, context, force_check=True)
        
        if not result["all_joined"]:
            missing_count = len(result["missing_channels"])
            message_text = "🔒 *Access Required*\n\n"
            
            if missing_count == 2:
                message_text += "⚠️ *You need to join both channels:*\n"
                keyboard = [
                    [InlineKeyboardButton(f"📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                    [InlineKeyboardButton(f"📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                    [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                ]
            else:
                missing_channel = result["missing_channels"][0].replace("@", "")
                channel_name = "Channel 1" if CHANNEL_1 in missing_channel else "Channel 2"
                message_text += f"⚠️ *You need to join {channel_name}:*\n"
                keyboard = [
                    [InlineKeyboardButton(f"📥 Join {channel_name}", url=f"https://t.me/{missing_channel}")],
                    [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                ]
            
            message_text += "\n👉 *Join and click 'Check Again'*"
            sent_msg = await update.message.reply_text(
                message_text,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        db.update_user_interaction(user_id=user_id, file_accessed=True)
        
        filename = file_info['file_name']
        ext = filename.lower().split('.')[-1] if '.' in filename else ""
        
        warning_msg = f"\n\n⚠️ *Auto-deletes in {DELETE_AFTER//60} minutes*"
        
        if file_info['is_video'] and ext in PLAYABLE_EXTS:
            sent = await context.bot.send_video(
                chat_id=chat_id,
                video=file_info["file_id"],
                caption=f"🎬 *{filename}*{warning_msg}",
                parse_mode="Markdown"
            )
        else:
            sent = await context.bot.send_document(
                chat_id=chat_id,
                document=file_info["file_id"],
                caption=f"📁 *{filename}*{warning_msg}",
                parse_mode="Markdown"
            )
        
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        
    except Exception as e:
        log.error(f"Start error: {e}")

async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle check membership callback"""
    try:
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        data = query.data
        
        user = query.from_user
        db.update_user_interaction(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )
        
        if data == "check_membership":
            result = await check_membership(user_id, context, force_check=True)
            
            if result["all_joined"]:
                await query.edit_message_text(
                    "✅ *You've joined both channels!*\n\nNow you can use file links.",
                    parse_mode="Markdown"
                )
            else:
                text = "❌ *Membership Check Failed*\n\n"
                if len(result["missing_channels"]) == 2:
                    text += "You're not in either channel."
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                        [InlineKeyboardButton(f"📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                        [InlineKeyboardButton("🔄 Check Again", callback_data="check_membership")]
                    ]
                else:
                    missing = result["missing_channels"][0].replace("@", "")
                    name = "Channel 1" if CHANNEL_1 in missing else "Channel 2"
                    text += f"You're missing {name}."
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join {name}", url=f"https://t.me/{missing}")],
                        [InlineKeyboardButton("🔄 Check Again", callback_data="check_membership")]
                    ]
                
                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            return
        
        if data.startswith("check|"):
            _, key = data.split("|")
            file_info = db.get_file(key)
            
            if not file_info:
                await query.edit_message_text("❌ File not found.")
                return
            
            result = await check_membership(user_id, context, force_check=True)
            
            if not result['all_joined']:
                text = "❌ *Still Not Joined*\n\n"
                if len(result["missing_channels"]) == 2:
                    text += "You need to join both channels."
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                        [InlineKeyboardButton(f"📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                        [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                    ]
                else:
                    missing = result["missing_channels"][0].replace("@", "")
                    name = "Channel 1" if CHANNEL_1 in missing else "Channel 2"
                    text += f"You need to join {name}."
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join {name}", url=f"https://t.me/{missing}")],
                        [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                    ]
                
                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return
            
            db.update_user_interaction(user_id=user_id, file_accessed=True)
            
            filename = file_info.get('file_name', 'file')
            ext = filename.lower().split('.')[-1] if '.' in filename else ""
            warning_msg = f"\n\n⚠️ *Auto-deletes in {DELETE_AFTER//60} minutes*"
            
            if file_info['is_video'] and ext in PLAYABLE_EXTS:
                sent_msg = await context.bot.send_video(
                    chat_id=query.message.chat_id,
                    video=file_info["file_id"],
                    caption=f"🎬 *{filename}*{warning_msg}",
                    parse_mode="Markdown"
                )
            else:
                sent_msg = await context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=file_info["file_id"],
                    caption=f"📁 *{filename}*{warning_msg}",
                    parse_mode="Markdown"
                )
            
            await query.edit_message_text("✅ *Access granted!*", parse_mode="Markdown")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        
    except Exception as e:
        log.error(f"Callback error: {e}")

async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Upload file handler"""
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
            f"📦 *Size:* {file_size/1024/1024:.1f} MB\n"
            f"🔑 *Key:* `{key}`\n"
            f"🔗 *Link:* `{link}`",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.error(f"Upload error: {e}")

# ============ ADMIN COMMANDS ============
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show bot statistics"""
    if update.effective_user.id != ADMIN_ID:
        return

    uptime_str = str(timedelta(seconds=int(time.time() - start_time)))
    file_count = db.get_file_count()
    user_count = db.get_user_count()
    
    sent_msg = await update.message.reply_text(
        f"📊 *Bot Statistics*\n\n"
        f"🤖 Bot: @{bot_username}\n"
        f"⏱ Uptime: {uptime_str}\n"
        f"📁 Files: {file_count}\n"
        f"👥 Users: {user_count}\n"
        f"💾 Database: PostgreSQL\n"
        f"⏰ Auto-delete: {DELETE_AFTER//60} min",
        parse_mode="Markdown"
    )
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List files in database"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    files = db.get_all_files()
    if not files:
        sent_msg = await update.message.reply_text("📁 No files stored.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    text = f"📁 *Total Files: {len(files)}*\n\n"
    for f in files[:5]:
        text += f"🔑 `{f[0]}` • {f[1][:20]} • {f[3]/(1024*1024):.1f}MB • 👥 {f[5]}x\n"
    
    sent_msg = await update.message.reply_text(text, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def deletefile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete a file"""
    if update.effective_user.id != ADMIN_ID or not context.args:
        return
    
    key = context.args[0]
    if db.delete_file(key):
        sent_msg = await update.message.reply_text(f"✅ File {key} deleted")
    else:
        sent_msg = await update.message.reply_text(f"❌ File {key} not found")
    
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def testchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test channel access"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    user_id = update.effective_user.id
    status = []
    
    try:
        await context.bot.get_chat_member(f"@{CHANNEL_1}", user_id)
        status.append(f"✅ Channel 1: @{CHANNEL_1}")
    except Exception as e:
        status.append(f"❌ Channel 1: {str(e)[:30]}")
    
    try:
        await context.bot.get_chat_member(f"@{CHANNEL_2}", user_id)
        status.append(f"✅ Channel 2: @{CHANNEL_2}")
    except Exception as e:
        status.append(f"❌ Channel 2: {str(e)[:30]}")
    
    sent_msg = await update.message.reply_text("\n".join(status), parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear membership cache"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    db.clear_membership_cache()
    sent_msg = await update.message.reply_text("✅ Cache cleared")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

# ============ MAIN ============
def main():
    print("\n" + "=" * 50)
    print("🤖 TELEGRAM FILE BOT - RENDER POSTGRESQL")
    print("=" * 50)

    if not BOT_TOKEN:
        print("❌ ERROR: BOT_TOKEN is not set!")
        return

    if not ADMIN_ID or ADMIN_ID == 0:
        print("❌ ERROR: ADMIN_ID is not set!")
        return

    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL is not set!")
        return

    print(f"✅ Bot: @{bot_username}")
    print(f"✅ Admin ID: {ADMIN_ID}")
    print(f"✅ Channels: @{CHANNEL_1}, @{CHANNEL_2}")
    print(f"✅ Database: PostgreSQL")
    
    # Start Flask in a separate thread
    print("\n🟢 Starting Flask web server...")
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    time.sleep(2)
    
    # Create and run bot in asyncio event loop
    print("🟢 Starting Telegram bot...")
    
    # Create application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add job queue if available
    if application.job_queue:
        application.job_queue.run_repeating(cleanup_overdue_messages, interval=300, first=10)
        print("✅ Job queue initialized")
    else:
        print("⚠️ Job queue not available - install with [job-queue] extra")
    
    # Add handlers
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("listfiles", listfiles))
    application.add_handler(CommandHandler("deletefile", deletefile))
    application.add_handler(CommandHandler("testchannel", testchannel))
    application.add_handler(CommandHandler("clearcache", clearcache))
    application.add_handler(CallbackQueryHandler(check_join, pattern=r"^check"))
    
    upload_filter = filters.VIDEO | filters.Document.ALL
    application.add_handler(
        MessageHandler(upload_filter & filters.User(ADMIN_ID) & filters.ChatType.PRIVATE, upload)
    )
    
    # Clear cache on startup
    db.clear_membership_cache()
    
    # Run the bot (this will block)
    print("✅ Bot is running. Press Ctrl+C to stop.\n")
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
    
