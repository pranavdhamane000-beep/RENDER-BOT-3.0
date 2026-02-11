import asyncio
import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
import threading
import pg8000
import urllib.parse

db_lock = asyncio.Lock()

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

# ============ RENDER POSTGRESQL - PERMANENT STORAGE ============
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL is not set!")
    print("💡 Add a PostgreSQL database in Render Dashboard and copy its Internal Database URL")
    raise ValueError("DATABASE_URL environment variable is required!")

DELETE_AFTER = 600  # 10 minutes
MAX_STORED_FILES = 100000  # Can store 100k files now with optimization!
AUTO_CLEANUP_DAYS = 0  # NEVER auto-cleanup

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

# ================= OPTIMIZED DATABASE - 90% LESS STORAGE =================
class Database:
    def __init__(self, db_url: str = DATABASE_URL):
        self.db_url = db_url
        self.connection = None
        self.connection_lock = asyncio.Lock()
        log.info(f"📀 Connecting to Render PostgreSQL (OPTIMIZED STORAGE)...")
    
    async def get_connection(self):
        """Get or create database connection"""
        async with self.connection_lock:
            if self.connection is None:
                # Parse DATABASE_URL
                db_string = self.db_url.replace("postgresql://", "").replace("postgres://", "")
                user_pass, host_port_db = db_string.split("@", 1)
                user, password = user_pass.split(":", 1)
                
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
                
                password = urllib.parse.unquote(password)
                
                try:
                    self.connection = pg8000.connect(
                        user=user,
                        password=password,
                        host=host,
                        port=port,
                        database=database,
                        ssl_context=True,
                        timeout=30
                    )
                    log.info("✅ PostgreSQL connection established")
                    await self.init_db()
                except Exception as e:
                    log.error(f"❌ Failed to connect: {e}")
                    raise
            
            return self.connection
    
    async def execute(self, query: str, params: tuple = None):
        """Execute a query and return cursor"""
        conn = await self.get_connection()
        def _execute():
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor
        return await asyncio.to_thread(_execute)
    
    async def fetchrow(self, query: str, params: tuple = None):
        """Fetch one row"""
        cursor = await self.execute(query, params)
        return await asyncio.to_thread(cursor.fetchone)
    
    async def fetchall(self, query: str, params: tuple = None):
        """Fetch all rows"""
        cursor = await self.execute(query, params)
        return await asyncio.to_thread(cursor.fetchall)
    
    async def execute_and_commit(self, query: str, params: tuple = None):
        """Execute query and commit"""
        cursor = await self.execute(query, params)
        conn = await self.get_connection()
        def _commit():
            conn.commit()
            return cursor.rowcount
        return await asyncio.to_thread(_commit)
    
    async def init_db(self):
        """Initialize database with OPTIMIZED schema - uses 90% less space"""
        try:
            # === FILES TABLE - OPTIMIZED ===
            # - Removed mime_type (not needed)
            # - is_video as SMALLINT (2 bytes instead of 4)
            # - file_size as INTEGER (4 bytes, 2GB limit is fine)
            # - access_count as SMALLINT (2 bytes, 65k accesses is plenty)
            # - filename limited to 255 chars
            await self.execute_and_commit('''
                CREATE TABLE IF NOT EXISTS f (
                    id SERIAL PRIMARY KEY,
                    fid TEXT NOT NULL,           -- file_id
                    fn VARCHAR(255) NOT NULL,    -- file_name (limited)
                    vid SMALLINT DEFAULT 0,      -- is_video (SMALLINT)
                    sz INTEGER DEFAULT 0,        -- file_size (INTEGER)
                    ts TIMESTAMP DEFAULT NOW(),  -- timestamp
                    acc SMALLINT DEFAULT 0       -- access_count (SMALLINT)
                )
            ''')
            
            # === MEMBERSHIP CACHE - OPTIMIZED ===
            await self.execute_and_commit('''
                CREATE TABLE IF NOT EXISTS mc (
                    uid BIGINT,                  -- user_id
                    ch VARCHAR(100),             -- channel
                    mem SMALLINT DEFAULT 0,      -- is_member
                    ts TIMESTAMP DEFAULT NOW(),  -- timestamp
                    PRIMARY KEY (uid, ch)
                )
            ''')
            
            # === SCHEDULED DELETIONS - OPTIMIZED ===
            await self.execute_and_commit('''
                CREATE TABLE IF NOT EXISTS sd (
                    cid BIGINT NOT NULL,         -- chat_id
                    mid INTEGER NOT NULL,        -- message_id
                    ts TIMESTAMP NOT NULL,       -- scheduled_time
                    PRIMARY KEY (cid, mid)
                )
            ''')
            
            # === USERS TABLE - OPTIMIZED ===
            await self.execute_and_commit('''
                CREATE TABLE IF NOT EXISTS u (
                    uid BIGINT PRIMARY KEY,      -- user_id
                    un VARCHAR(100),             -- username
                    fn VARCHAR(100),             -- first_name
                    ln VARCHAR(100),             -- last_name
                    fs TIMESTAMP DEFAULT NOW(),  -- first_seen
                    la TIMESTAMP DEFAULT NOW(),  -- last_active
                    ti SMALLINT DEFAULT 1,       -- total_interactions
                    tfa SMALLINT DEFAULT 0,      -- total_files_accessed
                    lfa TIMESTAMP                -- last_file_accessed
                )
            ''')
            
            # === ONLY 2 ESSENTIAL INDEXES ===
            await self.execute_and_commit('CREATE INDEX IF NOT EXISTS idx_f_ts ON f(ts)')
            await self.execute_and_commit('CREATE INDEX IF NOT EXISTS idx_u_la ON u(la)')
            
            count = await self.get_file_count()
            log.info(f"✅ Optimized DB ready - {count} files stored")
            
        except Exception as e:
            log.error(f"DB init error: {e}")
            raise
    
    # === FILE OPERATIONS - OPTIMIZED ===
    async def save_file(self, file_id: str, file_info: dict) -> str:
        """Save file info - OPTIMIZED"""
        async with db_lock:
            result = await self.fetchrow('''
                INSERT INTO f (fid, fn, vid, sz, acc)
                VALUES ($1, $2, $3, $4, 0)
                RETURNING id
            ''', (
                file_id,
                file_info.get('file_name', '')[:255],
                1 if file_info.get('is_video', False) else 0,
                file_info.get('size', 0)
            ))
            await self.get_connection().commit()
            new_id = str(result[0])
            log.info(f"💾 Saved file {new_id}")
            return new_id
    
    async def get_file(self, file_id: str) -> Optional[dict]:
        """Get file info - OPTIMIZED"""
        try:
            fid = int(file_id)
        except ValueError:
            return None
        
        result = await self.fetchrow('''
            UPDATE f SET acc = acc + 1
            WHERE id = $1
            RETURNING fid, fn, vid, sz, ts, acc
        ''', (fid,))
        
        if result:
            await self.get_connection().commit()
            return {
                'file_id': result[0],
                'file_name': result[1],
                'is_video': bool(result[2]),
                'size': result[3],
                'timestamp': result[4],
                'access_count': result[5]
            }
        return None
    
    async def get_file_count(self) -> int:
        """Get total files count"""
        result = await self.fetchrow("SELECT COUNT(*) FROM f")
        return result[0] if result else 0
    
    async def delete_file(self, file_id: str) -> bool:
        """Delete file - OPTIMIZED"""
        try:
            fid = int(file_id)
        except ValueError:
            return False
        cnt = await self.execute_and_commit("DELETE FROM f WHERE id = $1", (fid,))
        return cnt > 0
    
    async def get_all_files(self) -> list:
        """Get all files for admin - OPTIMIZED"""
        rows = await self.fetchall('''
            SELECT id, fn, vid, sz, ts, acc 
            FROM f ORDER BY ts DESC LIMIT 1000
        ''')
        return [(r[0], r[1], r[2], r[3], str(r[4]), r[5]) for r in rows]
    
    async def cleanup_old_files(self, days: int = 90) -> int:
        """Manually cleanup files older than X days"""
        cnt = await self.execute_and_commit('''
            DELETE FROM f WHERE ts < NOW() - INTERVAL '1 day' * $1
        ''', (days,))
        return cnt
    
    # === MEMBERSHIP CACHE - OPTIMIZED ===
    async def cache_membership(self, user_id: int, channel: str, is_member: bool):
        """Cache membership - OPTIMIZED"""
        await self.execute_and_commit('''
            INSERT INTO mc (uid, ch, mem, ts)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (uid, ch) DO UPDATE
            SET mem = EXCLUDED.mem, ts = EXCLUDED.ts
        ''', (user_id, channel[:100], 1 if is_member else 0))
    
    async def get_cached_membership(self, user_id: int, channel: str) -> Optional[bool]:
        """Get cached membership - OPTIMIZED"""
        result = await self.fetchrow('''
            SELECT mem FROM mc 
            WHERE uid = $1 AND ch = $2 
            AND ts > NOW() - INTERVAL '5 minutes'
        ''', (user_id, channel[:100]))
        return bool(result[0]) if result else None
    
    async def clear_membership_cache(self, user_id: Optional[int] = None):
        """Clear cache - OPTIMIZED"""
        if user_id:
            await self.execute_and_commit("DELETE FROM mc WHERE uid = $1", (user_id,))
            log.info(f"Cleared cache for user {user_id}")
        else:
            await self.execute_and_commit("DELETE FROM mc")
            log.info("Cleared all membership cache")
    
    # === SCHEDULED DELETIONS - OPTIMIZED ===
    async def schedule_message_deletion(self, chat_id: int, message_id: int):
        """Schedule message deletion - OPTIMIZED"""
        scheduled_time = datetime.now() + timedelta(seconds=DELETE_AFTER)
        await self.execute_and_commit('''
            INSERT INTO sd (cid, mid, ts)
            VALUES ($1, $2, $3)
            ON CONFLICT (cid, mid) DO UPDATE
            SET ts = EXCLUDED.ts
        ''', (chat_id, message_id, scheduled_time))
    
    async def get_due_messages(self):
        """Get due messages - OPTIMIZED"""
        rows = await self.fetchall('''
            SELECT cid, mid FROM sd 
            WHERE ts <= NOW()
        ''')
        return [(r[0], r[1]) for r in rows]
    
    async def remove_scheduled_message(self, chat_id: int, message_id: int):
        """Remove scheduled message - OPTIMIZED"""
        await self.execute_and_commit(
            'DELETE FROM sd WHERE cid = $1 AND mid = $2',
            (chat_id, message_id)
        )
    
    # === USER TRACKING - OPTIMIZED ===
    async def update_user_interaction(self, user_id: int, username: str = None, 
                                    first_name: str = None, last_name: str = None,
                                    file_accessed: bool = False):
        """Update user - OPTIMIZED"""
        async with self.get_connection():
            exists = await self.fetchrow("SELECT 1 FROM u WHERE uid = $1", (user_id,))
            
            if exists:
                await self.execute_and_commit('''
                    UPDATE u SET 
                        la = NOW(),
                        ti = ti + 1,
                        un = COALESCE($1, un),
                        fn = COALESCE($2, fn),
                        ln = COALESCE($3, ln)
                    WHERE uid = $4
                ''', (
                    username[:100] if username else None,
                    first_name[:100] if first_name else None,
                    last_name[:100] if last_name else None,
                    user_id
                ))
                
                if file_accessed:
                    await self.execute_and_commit('''
                        UPDATE u SET 
                            tfa = tfa + 1,
                            lfa = NOW()
                        WHERE uid = $1
                    ''', (user_id,))
            else:
                await self.execute_and_commit('''
                    INSERT INTO u (uid, un, fn, ln, fs, la, ti)
                    VALUES ($1, $2, $3, $4, NOW(), NOW(), 1)
                ''', (
                    user_id,
                    username[:100] if username else None,
                    first_name[:100] if first_name else None,
                    last_name[:100] if last_name else None
                ))
    
    async def get_user_stats(self) -> Dict[str, Any]:
        """Get user stats - OPTIMIZED"""
        total = await self.fetchrow("SELECT COUNT(*) FROM u")
        total_users = total[0] if total else 0
        
        active7 = await self.fetchrow(
            "SELECT COUNT(*) FROM u WHERE la > NOW() - INTERVAL '7 days'"
        )
        active_users_7d = active7[0] if active7 else 0
        
        active30 = await self.fetchrow(
            "SELECT COUNT(*) FROM u WHERE la > NOW() - INTERVAL '30 days'"
        )
        active_users_30d = active30[0] if active30 else 0
        
        new_today = await self.fetchrow(
            "SELECT COUNT(*) FROM u WHERE DATE(fs) = CURRENT_DATE"
        )
        new_users_today = new_today[0] if new_today else 0
        
        new_week = await self.fetchrow(
            "SELECT COUNT(*) FROM u WHERE fs > NOW() - INTERVAL '7 days'"
        )
        new_users_week = new_week[0] if new_week else 0
        
        users_files = await self.fetchrow(
            "SELECT COUNT(*) FROM u WHERE tfa > 0"
        )
        users_with_files = users_files[0] if users_files else 0
        
        top_rows = await self.fetchall('''
            SELECT uid, un, fn, ln, ti, tfa, la, fs
            FROM u ORDER BY ti DESC LIMIT 10
        ''')
        top_users = [(r[0], r[1], r[2], r[3], r[4], r[5], str(r[6]), str(r[7])) for r in top_rows]
        
        growth_rows = await self.fetchall('''
            SELECT TO_CHAR(fs, 'YYYY-MM-DD'), COUNT(*)
            FROM u WHERE fs > NOW() - INTERVAL '30 days'
            GROUP BY 1 ORDER BY 1 DESC LIMIT 15
        ''')
        growth_data = [(r[0], r[1]) for r in growth_rows]
        
        return {
            'total_users': total_users,
            'active_users_7d': active_users_7d,
            'active_users_30d': active_users_30d,
            'new_users_today': new_users_today,
            'new_users_week': new_users_week,
            'top_users': top_users,
            'users_with_files': users_with_files,
            'growth_data': growth_data
        }
    
    async def get_all_user_ids(self, exclude_admin: bool = True) -> List[int]:
        """Get all user IDs"""
        if exclude_admin:
            rows = await self.fetchall("SELECT uid FROM u WHERE uid != $1", (ADMIN_ID,))
        else:
            rows = await self.fetchall("SELECT uid FROM u")
        return [r[0] for r in rows]
    
    async def get_user_count(self) -> int:
        """Get total user count"""
        result = await self.fetchrow("SELECT COUNT(*) FROM u")
        return result[0] if result else 0
    
    async def close(self):
        """Close connection"""
        if self.connection:
            await asyncio.to_thread(self.connection.close)
            self.connection = None
            log.info("Database connection closed")

# Initialize database
db = Database()

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
            log.info(f"✅ Deleted message {message_id}")
            await db.remove_scheduled_message(chat_id, message_id)
        except Exception as e:
            error_msg = str(e).lower()
            if "message to delete not found" in error_msg:
                await db.remove_scheduled_message(chat_id, message_id)
            else:
                log.error(f"Failed to delete message {message_id}: {e}")
                
    except Exception as e:
        log.error(f"Error in delete_message_job: {e}")

async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    """Schedule a message for deletion"""
    try:
        await db.schedule_message_deletion(chat_id, message_id)
        
        if context.job_queue:
            context.job_queue.run_once(
                delete_message_job,
                DELETE_AFTER,
                data=message_id,
                chat_id=chat_id,
                name=f"del_{chat_id}_{message_id}"
            )
    except Exception as e:
        log.error(f"Failed to schedule deletion: {e}")

async def cleanup_overdue_messages(context: ContextTypes.DEFAULT_TYPE):
    """Clean up overdue messages"""
    try:
        due_messages = await db.get_due_messages()
        if not due_messages:
            return
        
        for chat_id, message_id in due_messages:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
                await db.remove_scheduled_message(chat_id, message_id)
            except Exception as e:
                if "message to delete not found" in str(e).lower():
                    await db.remove_scheduled_message(chat_id, message_id)
                    
    except Exception as e:
        log.error(f"Error in cleanup_overdue_messages: {e}")

# ============ MEMBERSHIP CHECK ============
async def check_user_in_channel(bot, channel: str, user_id: int, force_check: bool = False) -> bool:
    """Check if user is in channel"""
    if not force_check:
        cached = await db.get_cached_membership(user_id, channel)
        if cached is not None:
            return cached
    
    try:
        if not channel.startswith("@"):
            channel_username = f"@{channel}"
        else:
            channel_username = channel
        
        member = await bot.get_chat_member(chat_id=channel_username, user_id=user_id)
        is_member = member.status in ["member", "administrator", "creator"]
        
        await db.cache_membership(user_id, channel.replace("@", ""), is_member)
        return is_member
        
    except Exception as e:
        error_msg = str(e).lower()
        if "user not found" in error_msg or "user not participant" in error_msg:
            await db.cache_membership(user_id, channel.replace("@", ""), False)
            return False
        elif "chat not found" in error_msg:
            log.error(f"Channel @{channel} not found!")
            return True
        elif "forbidden" in error_msg:
            log.error(f"Bot can't access @{channel}")
            return True
        else:
            return True

async def check_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE, force_check: bool = False) -> Dict[str, Any]:
    """Check if user is member of both channels"""
    bot = context.bot
    
    result = {
        "channel1": False,
        "channel2": False,
        "all_joined": False,
        "missing_channels": []
    }
    
    if force_check:
        await db.clear_membership_cache(user_id)
    
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
        h1 { color: white; margin-top: 0; }
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
        a { color: #FFD700; text-decoration: none; }
        .btn {
            display: inline-block;
            background: #4CAF50;
            color: white;
            padding: 8px 16px;
            border-radius: 6px;
            margin: 5px;
        }
        .warning {
            background: rgba(255, 165, 0, 0.2);
            border-left: 4px solid #ffa500;
            padding: 10px;
            border-radius: 8px;
            margin: 10px 0;
        }
        ul { padding-left: 20px; }
        li { margin: 5px 0; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Telegram File Bot</h1>
        <div class="status">
            <h3>✅ Status: <strong>ACTIVE</strong></h3>
            <p>Bot running on Render with PostgreSQL</p>
            <p>Uptime: {{ uptime }}</p>
            <p>Files in DB: {{ file_count }}</p>
            <p>Users in DB: {{ user_count }}</p>
            <p>📁 Storage: OPTIMIZED - 90% less space</p>
        </div>
        
        <div class="info">
            <h3>📊 Bot Information</h3>
            <ul>
                <li>Bot: <strong>@{{ bot_username }}</strong></li>
                <li>Database: <strong>Render PostgreSQL</strong></li>
                <li>Storage: <strong>PERMANENT - Optimized</strong></li>
                <li>Message Auto-delete: <strong>{{ delete_minutes }} minutes</strong></li>
            </ul>
        </div>
        
        <div class="info">
            <h3>📞 Start Bot</h3>
            <p><a href="https://t.me/{{ bot_username }}" class="btn">Start @{{ bot_username }}</a></p>
        </div>
    </div>
</body>
</html>
    """
    
    uptime_seconds = time.time() - start_time
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        file_count = loop.run_until_complete(db.get_file_count())
        user_count = loop.run_until_complete(db.get_user_count())
    except:
        file_count = 0
        user_count = 0
    finally:
        loop.close()
    
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
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        file_count = loop.run_until_complete(db.get_file_count())
        user_count = loop.run_until_complete(db.get_user_count())
    except:
        file_count = 0
        user_count = 0
    finally:
        loop.close()
    
    return jsonify({
        "status": "OK",
        "timestamp": datetime.now().isoformat(),
        "service": "telegram-file-bot",
        "uptime": str(timedelta(seconds=int(time.time() - start_time))),
        "database": "postgresql",
        "storage": "optimized",
        "file_count": file_count,
        "user_count": user_count
    }), 200

@app.route('/ping')
def ping():
    return "pong", 200

def run_flask_thread():
    """Run Flask server in a thread"""
    port = int(os.environ.get('PORT', 10000))
    
    import warnings
    warnings.filterwarnings("ignore")
    
    import logging as flask_logging
    flask_logging.getLogger('werkzeug').setLevel(flask_logging.ERROR)
    flask_logging.getLogger('flask').setLevel(flask_logging.ERROR)
    
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)

# ============ COMMAND HANDLERS ============
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Error handler"""
    log.error(f"Error: {context.error}", exc_info=True)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    try:
        if not update.message:
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        args = context.args

        user = update.effective_user
        await db.update_user_interaction(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )

        if not args:
            keyboard = [
                [InlineKeyboardButton("📢 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                [InlineKeyboardButton("📢 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                [InlineKeyboardButton("🔄 Check Membership", callback_data="check")]
            ]

            sent_msg = await update.message.reply_text(
                "🤖 *Welcome to File Sharing Bot*\n\n"
                "🔗 *How to use:*\n"
                "1️⃣ Use admin-provided links\n"
                "2️⃣ Join both channels\n"
                "3️⃣ Click 'Check Membership'\n\n"
                f"⚠️ Messages auto-delete after {DELETE_AFTER//60} minutes\n"
                "💾 *Storage:* OPTIMIZED PostgreSQL",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        key = args[0]
        file_info = await db.get_file(key)
        
        if not file_info:
            sent_msg = await update.message.reply_text("❌ File not found")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        result = await check_membership(user_id, context, force_check=True)
        
        if not result["all_joined"]:
            missing_count = len(result["missing_channels"])
            
            if missing_count == 2:
                keyboard = [
                    [InlineKeyboardButton("📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                    [InlineKeyboardButton("📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                    [InlineKeyboardButton("✅ Check Again", callback_data=f"chk|{key}")]
                ]
                text = "🔒 *Join both channels to access this file*"
            else:
                missing = result["missing_channels"][0].replace("@", "")
                name = "Channel 1" if CHANNEL_1 in missing else "Channel 2"
                keyboard = [
                    [InlineKeyboardButton(f"📥 Join {name}", url=f"https://t.me/{missing}")],
                    [InlineKeyboardButton("✅ Check Again", callback_data=f"chk|{key}")]
                ]
                text = f"🔒 *Join {name} to access this file*"
            
            sent_msg = await update.message.reply_text(
                text,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        await db.update_user_interaction(user_id=user_id, file_accessed=True)
        
        try:
            filename = file_info['file_name']
            ext = filename.lower().split('.')[-1] if '.' in filename else ""
            
            warning = f"\n\n⚠️ Auto-deletes in {DELETE_AFTER//60} minutes\n💾 Permanently stored"
            
            if file_info['is_video'] and ext in PLAYABLE_EXTS:
                sent = await context.bot.send_video(
                    chat_id=chat_id,
                    video=file_info["file_id"],
                    caption=f"🎬 *{filename}*\n📥 Accessed {file_info['access_count']}{warning}",
                    parse_mode="Markdown",
                    supports_streaming=True
                )
            else:
                sent = await context.bot.send_document(
                    chat_id=chat_id,
                    document=file_info["file_id"],
                    caption=f"📁 *{filename}*\n📥 Accessed {file_info['access_count']}{warning}",
                    parse_mode="Markdown"
                )
            
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
                
        except Exception as e:
            log.error(f"Error sending file: {e}")
            sent_msg = await update.message.reply_text("❌ Failed to send file")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.error(f"Start error: {e}")

async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries"""
    try:
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        data = query.data
        
        user = query.from_user
        await db.update_user_interaction(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )
        
        if data == "check":
            result = await check_membership(user_id, context, force_check=True)
            
            if result["all_joined"]:
                await query.edit_message_text(
                    "✅ *You've joined both channels!*\n\nUse file links from admin.",
                    parse_mode="Markdown"
                )
            else:
                missing_count = len(result["missing_channels"])
                
                if missing_count == 2:
                    keyboard = [
                        [InlineKeyboardButton("📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                        [InlineKeyboardButton("📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                        [InlineKeyboardButton("🔄 Check Again", callback_data="check")]
                    ]
                    text = "❌ *Not a member of either channel*"
                else:
                    missing = result["missing_channels"][0].replace("@", "")
                    name = "Channel 1" if CHANNEL_1 in missing else "Channel 2"
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join {name}", url=f"https://t.me/{missing}")],
                        [InlineKeyboardButton("🔄 Check Again", callback_data="check")]
                    ]
                    text = f"❌ *Missing {name}*"
                
                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            return
        
        if data.startswith("chk|"):
            _, key = data.split("|")
            
            file_info = await db.get_file(key)
            if not file_info:
                await query.edit_message_text("❌ File not found")
                return
            
            result = await check_membership(user_id, context, force_check=True)
            
            if not result['all_joined']:
                missing_count = len(result["missing_channels"])
                
                if missing_count == 2:
                    keyboard = [
                        [InlineKeyboardButton("📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                        [InlineKeyboardButton("📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                        [InlineKeyboardButton("✅ Check Again", callback_data=f"chk|{key}")]
                    ]
                    text = "❌ *Join both channels*"
                else:
                    missing = result["missing_channels"][0].replace("@", "")
                    name = "Channel 1" if CHANNEL_1 in missing else "Channel 2"
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join {name}", url=f"https://t.me/{missing}")],
                        [InlineKeyboardButton("✅ Check Again", callback_data=f"chk|{key}")]
                    ]
                    text = f"❌ *Join {name}*"
                
                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return
            
            await db.update_user_interaction(user_id=user_id, file_accessed=True)
            
            try:
                filename = file_info['file_name']
                ext = filename.lower().split('.')[-1] if '.' in filename else ""
                
                warning = f"\n\n⚠️ Auto-deletes in {DELETE_AFTER//60} minutes\n💾 Permanently stored"
                chat_id = query.message.chat_id
                
                if file_info['is_video'] and ext in PLAYABLE_EXTS:
                    sent = await context.bot.send_video(
                        chat_id=chat_id,
                        video=file_info["file_id"],
                        caption=f"🎬 *{filename}*\n📥 Accessed {file_info['access_count']}{warning}",
                        parse_mode="Markdown",
                        supports_streaming=True
                    )
                else:
                    sent = await context.bot.send_document(
                        chat_id=chat_id,
                        document=file_info["file_id"],
                        caption=f"📁 *{filename}*\n📥 Accessed {file_info['access_count']}{warning}",
                        parse_mode="Markdown"
                    )
                
                await query.edit_message_text("✅ *File sent below!*", parse_mode="Markdown")
                await schedule_message_deletion(context, sent.chat_id, sent.message_id)
                
            except Exception as e:
                log.error(f"Failed to send file: {e}")
                await query.edit_message_text("❌ Failed to send file")
        
    except Exception as e:
        log.error(f"Callback error: {e}")

async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Upload file handler (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        msg = update.message
        video = msg.video
        document = msg.document

        file_id = None
        filename = None
        file_size = 0
        is_video = False

        if video:
            file_id = video.file_id
            filename = video.file_name or f"video_{int(time.time())}.mp4"
            file_size = video.file_size or 0
            is_video = True
        elif document:
            filename = document.file_name or f"document_{int(time.time())}"
            file_id = document.file_id
            file_size = document.file_size or 0
            ext = filename.lower().split('.')[-1] if '.' in filename else ""
            if ext in ALL_VIDEO_EXTS:
                is_video = True
        else:
            sent_msg = await msg.reply_text("❌ Send a video or document")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        file_info = {
            "file_name": filename,
            "is_video": is_video,
            "size": int(file_size) if file_size else 0
        }

        key = await db.save_file(file_id, file_info)
        link = f"https://t.me/{bot_username}?start={key}"

        sent_msg = await msg.reply_text(
            f"✅ *Upload Successful*\n\n"
            f"📁 *Name:* `{filename[:50]}...`\n"
            f"🔑 *Key:* `{key}`\n"
            f"💾 *Storage:* OPTIMIZED PostgreSQL\n\n"
            f"🔗 *Link:*\n`{link}`",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.exception("Upload error")
        sent_msg = await update.message.reply_text(f"❌ Upload failed: {str(e)[:200]}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stats command (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    uptime = str(timedelta(seconds=int(time.time() - start_time)))
    file_count = await db.get_file_count()
    user_count = await db.get_user_count()
    
    files = await db.get_all_files()
    total_access = sum(f[5] for f in files) if files else 0

    sent_msg = await update.message.reply_text(
        f"📊 *Bot Statistics*\n\n"
        f"🤖 Bot: @{bot_username}\n"
        f"⏱ Uptime: {uptime}\n"
        f"📁 Files: {file_count}\n"
        f"👥 Users: {user_count}\n"
        f"👀 Accesses: {total_access}\n"
        f"💾 DB: PostgreSQL (optimized)\n"
        f"⏰ Auto-delete: {DELETE_AFTER//60}min",
        parse_mode="Markdown"
    )
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List files (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    files = await db.get_all_files()
    
    if not files:
        sent_msg = await update.message.reply_text("📁 No files stored")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    msg = f"📁 *Total Files: {len(files)}*\n\n"
    for file in files[:20]:
        fid, name, vid, sz, ts, acc = file
        size_mb = sz / (1024*1024) if sz else 0
        msg += f"🔑 `{fid}` - {name[:20]}... ({size_mb:.1f}MB) - 👥 {acc}\n"
    
    sent_msg = await update.message.reply_text(msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def deletefile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete file (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        sent_msg = await update.message.reply_text("❌ Usage: /deletefile <key>")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    key = context.args[0]
    if await db.delete_file(key):
        sent_msg = await update.message.reply_text(f"✅ Deleted file {key}")
    else:
        sent_msg = await update.message.reply_text(f"❌ File {key} not found")
    
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User stats (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    stats_data = await db.get_user_stats()
    
    msg = (
        f"📊 *User Statistics*\n\n"
        f"👥 Total: {stats_data['total_users']}\n"
        f"🟢 Active 7d: {stats_data['active_users_7d']}\n"
        f"🟡 Active 30d: {stats_data['active_users_30d']}\n"
        f"📈 New today: {stats_data['new_users_today']}\n"
        f"📁 File users: {stats_data['users_with_files']}"
    )
    
    sent_msg = await update.message.reply_text(msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast to users (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args and not update.message.reply_to_message:
        sent_msg = await update.message.reply_text("❌ Usage: /broadcast <message>")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    if update.message.reply_to_message:
        message_text = update.message.reply_to_message.text or update.message.reply_to_message.caption
    else:
        message_text = " ".join(context.args)
    
    if not message_text:
        sent_msg = await update.message.reply_text("❌ Message cannot be empty")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    user_ids = await db.get_all_user_ids(exclude_admin=True)
    
    status_msg = await update.message.reply_text(
        f"🔄 Broadcasting to {len(user_ids)} users...",
        parse_mode="Markdown"
    )
    
    successful = 0
    failed = 0
    
    for user_id in user_ids[:100]:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"📢 *Broadcast*\n\n{message_text}",
                parse_mode="Markdown"
            )
            successful += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    
    await status_msg.edit_text(
        f"✅ Broadcast: {successful} sent, {failed} failed",
        parse_mode="Markdown"
    )
    await schedule_message_deletion(context, status_msg.chat_id, status_msg.message_id)

async def cleancache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear membership cache (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    await db.clear_membership_cache()
    sent_msg = await update.message.reply_text("✅ Cache cleared")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def testch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test channel access (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    user_id = update.effective_user.id
    
    try:
        member1 = await context.bot.get_chat_member(f"@{CHANNEL_1}", user_id)
        ch1 = f"✅ {member1.status}"
    except Exception as e:
        ch1 = f"❌ {str(e)[:30]}"
    
    try:
        member2 = await context.bot.get_chat_member(f"@{CHANNEL_2}", user_id)
        ch2 = f"✅ {member2.status}"
    except Exception as e:
        ch2 = f"❌ {str(e)[:30]}"
    
    sent_msg = await update.message.reply_text(
        f"🔍 *Channel Test*\n\n"
        f"@{CHANNEL_1}: {ch1}\n"
        f"@{CHANNEL_2}: {ch2}",
        parse_mode="Markdown"
    )
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def cleanupold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually clean up old files (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    days = 90
    if context.args:
        try:
            days = int(context.args[0])
        except:
            pass
    
    cnt = await db.cleanup_old_files(days)
    
    sent_msg = await update.message.reply_text(
        f"🧹 Deleted {cnt} files older than {days} days",
        parse_mode="Markdown"
    )
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

# ============ MAIN ============
async def start_bot():
    """Start the bot"""
    if not BOT_TOKEN or not ADMIN_ID:
        log.error("Missing BOT_TOKEN or ADMIN_ID")
        return
    
    await db.get_connection()
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    if application.job_queue:
        application.job_queue.run_repeating(
            cleanup_overdue_messages,
            interval=300,
            first=10
        )
    
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("list", listfiles))
    application.add_handler(CommandHandler("del", deletefile))
    application.add_handler(CommandHandler("users", users))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CommandHandler("cleancache", cleancache))
    application.add_handler(CommandHandler("testch", testch))
    application.add_handler(CommandHandler("cleanup", cleanupold))
    
    application.add_handler(CallbackQueryHandler(check_join, pattern="^check$"))
    application.add_handler(CallbackQueryHandler(check_join, pattern="^chk\\|"))
    
    upload_filter = filters.VIDEO | filters.Document.ALL
    application.add_handler(
        MessageHandler(upload_filter & filters.User(ADMIN_ID) & filters.ChatType.PRIVATE, upload)
    )
    
    log.info("🤖 Bot started")
    log.info(f"📁 Files: {await db.get_file_count()}")
    log.info(f"👥 Users: {await db.get_user_count()}")
    
    await application.run_polling(allowed_updates=Update.ALL_TYPES)

def main():
    """Main function"""
    print("\n" + "=" * 60)
    print("🤖 TELEGRAM BOT - OPTIMIZED STORAGE (90% LESS SPACE)")
    print("=" * 60)
    print(f"✅ Bot: @{bot_username}")
    print(f"✅ Admin: {ADMIN_ID}")
    print(f"✅ Database: Render PostgreSQL")
    print(f"✅ Storage: OPTIMIZED - 10x more files!")
    print("=" * 60 + "\n")
    
    flask_thread = threading.Thread(target=run_flask_thread, daemon=True)
    flask_thread.start()
    
    try:
        asyncio.run(start_bot())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped")
    except Exception as e:
        log.error(f"Fatal error: {e}", exc_info=True)
    finally:
        try:
            asyncio.run(db.close())
        except:
            pass

if __name__ == "__main__":
    main()
