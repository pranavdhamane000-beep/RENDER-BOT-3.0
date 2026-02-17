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
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from contextlib import asynccontextmanager
import urllib.parse

# ================= HEALTH SERVER FOR RENDER =================
from flask import Flask, render_template_string, jsonify, request
app = Flask(__name__)

# Global variables for web dashboard
start_time = time.time()
bot_username = "xiomovies_bot"
# Global variable to store bot application instance for webhook
bot_app = None
bot_loop = None
bot_initialized = False

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
from telegram.request import HTTPXRequest

# ================= CONFIG =================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

# Channel usernames (without @)
CHANNEL_1 = os.environ.get("CHANNEL_1", "A_Knight_of_the_Seven_Kingdoms_t").replace("@", "")
CHANNEL_2 = os.environ.get("CHANNEL_2", "your_movies_web").replace("@", "")

# ============ RENDER POSTGRESQL WITH PSYCOPG2 ============
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL is not set!")
    print("💡 Add a PostgreSQL database in Render Dashboard and copy its Internal Database URL")
    raise ValueError("DATABASE_URL environment variable is required!")

DELETE_AFTER = 600  # 10 minutes
MAX_STORED_FILES = 10000
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
logging.getLogger("psycopg2").setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# ================= DATABASE (Render PostgreSQL with psycopg2) =================

class Database:
    def __init__(self, db_url: str = DATABASE_URL):
        self.db_url = db_url
        # Use a simple connection pool for thread safety and efficiency
        self.pool = None
        self._pool_initialized = False
        log.info(f"📀 Connecting to Render PostgreSQL with psycopg2...")

    def _get_pool_sync(self):
        """Synchronous pool initialization - called only once"""
        if self.pool is None:
            # Parse DATABASE_URL for psycopg2's DSN
            result = urllib.parse.urlparse(self.db_url)
            user = result.username
            password = urllib.parse.unquote(result.password) if result.password else ''
            database = result.path[1:]
            host = result.hostname
            port = result.port or 5432

            dsn = f"dbname='{database}' user='{user}' password='{password}' host='{host}' port='{port}'"
            log.info(f"🔌 Creating connection pool to Render PostgreSQL at {host}:{port}/{database}")

            try:
                # Create a connection pool
                self.pool = psycopg2.pool.SimpleConnectionPool(
                    1, 20, dsn=dsn, connect_timeout=30,
                    sslmode='require'
                )
                log.info("✅ Render PostgreSQL connection pool created (SSL enabled)")

                # Initialize tables
                conn = self.pool.getconn()
                try:
                    with conn.cursor() as cur:
                        self._init_db(conn, cur)
                finally:
                    self.pool.putconn(conn)

                self._pool_initialized = True
                log.info("✅ Database tables initialized/verified.")

            except Exception as e:
                log.error(f"❌ Failed to create connection pool to Render PostgreSQL: {e}")
                raise
        return self.pool

    async def _get_pool_async(self):
        """Async wrapper for pool initialization"""
        if self.pool is None:
            await asyncio.to_thread(self._get_pool_sync)
        return self.pool

    def _init_db(self, conn, cur):
        """Initialize database tables (synchronous)"""
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
        cur.execute('CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON membership_cache(timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_deletions_time ON scheduled_deletions(scheduled_time)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_users_first_seen ON users(first_seen)')
        conn.commit()

    @asynccontextmanager
    async def get_db_connection(self):
        """Asynchronous context manager to get and return a connection from the pool."""
        pool = await self._get_pool_async()
        conn = await asyncio.to_thread(pool.getconn)
        try:
            yield conn
        finally:
            await asyncio.to_thread(pool.putconn, conn)

    async def execute(self, query: str, params: tuple = None):
        """Execute a query and return cursor"""
        async with self.get_db_connection() as conn:
            def _execute():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(query, params)
                    return cur
            return await asyncio.to_thread(_execute)

    async def fetchrow(self, query: str, params: tuple = None):
        """Fetch one row as a dictionary."""
        async with self.get_db_connection() as conn:
            def _fetch():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(query, params)
                    return cur.fetchone()
            return await asyncio.to_thread(_fetch)

    async def fetchall(self, query: str, params: tuple = None):
        """Fetch all rows as a list of dictionaries."""
        async with self.get_db_connection() as conn:
            def _fetch():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(query, params)
                    return cur.fetchall()
            return await asyncio.to_thread(_fetch)

    async def execute_and_commit(self, query: str, params: tuple = None):
        """Execute query and commit."""
        async with self.get_db_connection() as conn:
            def _execute():
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    conn.commit()
                    return cur.rowcount
            return await asyncio.to_thread(_execute)

    async def save_file(self, file_id: str, file_info: dict) -> str:
        """Save file info and return generated ID."""
        async with self.get_db_connection() as conn:
            def _save():
                with conn.cursor() as cur:
                    cur.execute('''
                        INSERT INTO files
                        (file_id, file_name, mime_type, is_video, file_size, access_count)
                        VALUES (%s, %s, %s, %s, %s, 0)
                        RETURNING id
                    ''', (
                        file_id,
                        file_info.get('file_name', ''),
                        file_info.get('mime_type', ''),
                        1 if file_info.get('is_video', False) else 0,
                        file_info.get('size', 0)
                    ))
                    new_id = cur.fetchone()[0]
                    conn.commit()
                    log.info(f"💾 Saved file {new_id}: {file_info.get('file_name', '')}")
                    return str(new_id)
            return await asyncio.to_thread(_save)

    async def get_file(self, file_id: str) -> Optional[dict]:
        """Get file info by ID."""
        try:
            file_id_int = int(file_id)
        except ValueError:
            return None

        async with self.get_db_connection() as conn:
            def _get():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute('''
                        UPDATE files
                        SET access_count = access_count + 1
                        WHERE id = %s
                        RETURNING file_id, file_name, mime_type, is_video, file_size,
                                  TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as timestamp,
                                  access_count
                    ''', (file_id_int,))
                    row = cur.fetchone()
                    if row:
                        conn.commit()
                        return dict(row)
                    return None
            return await asyncio.to_thread(_get)

    async def get_file_count(self) -> int:
        """Get total number of files."""
        result = await self.fetchrow("SELECT COUNT(*) as count FROM files")
        return result['count'] if result else 0

    async def cache_membership(self, user_id: int, channel: str, is_member: bool):
        """Cache membership check result."""
        await self.execute_and_commit('''
            INSERT INTO membership_cache (user_id, channel, is_member, timestamp)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, channel) DO UPDATE
            SET is_member = EXCLUDED.is_member,
                timestamp = EXCLUDED.timestamp
        ''', (user_id, channel, 1 if is_member else 0))

    async def get_cached_membership(self, user_id: int, channel: str) -> Optional[bool]:
        """Get cached membership result (valid for 5 minutes)."""
        result = await self.fetchrow('''
            SELECT is_member FROM membership_cache
            WHERE user_id = %s AND channel = %s
            AND timestamp > CURRENT_TIMESTAMP - INTERVAL '5 minutes'
        ''', (user_id, channel))
        return bool(result['is_member']) if result else None

    async def clear_membership_cache(self, user_id: Optional[int] = None):
        """Clear membership cache for a user or all users."""
        if user_id:
            await self.execute_and_commit("DELETE FROM membership_cache WHERE user_id = %s", (user_id,))
            log.info(f"Cleared cache for user {user_id}")
        else:
            await self.execute_and_commit("DELETE FROM membership_cache")
            log.info("Cleared all membership cache")

    async def delete_file(self, file_id: str) -> bool:
        """Manually delete a file from database."""
        try:
            file_id_int = int(file_id)
        except ValueError:
            return False

        rowcount = await self.execute_and_commit("DELETE FROM files WHERE id = %s", (file_id_int,))
        deleted = rowcount > 0
        if deleted:
            log.info(f"🗑️ Deleted file {file_id}")
        return deleted

    async def get_all_files(self) -> list:
        """Get all files for admin view."""
        rows = await self.fetchall('''
            SELECT id, file_name, is_video, file_size,
                   TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as timestamp,
                   access_count
            FROM files
            ORDER BY timestamp DESC
        ''')
        # Return as list of tuples for compatibility with original code
        return [(row['id'], row['file_name'], row['is_video'], row['file_size'], row['timestamp'], row['access_count']) for row in rows]

    async def schedule_message_deletion(self, chat_id: int, message_id: int):
        """Schedule a message for deletion."""
        scheduled_time = datetime.now() + timedelta(seconds=DELETE_AFTER)
        await self.execute_and_commit('''
            INSERT INTO scheduled_deletions (chat_id, message_id, scheduled_time, delete_after)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chat_id, message_id) DO UPDATE
            SET scheduled_time = EXCLUDED.scheduled_time,
                delete_after = EXCLUDED.delete_after
        ''', (chat_id, message_id, scheduled_time, DELETE_AFTER))
        log.info(f"Scheduled deletion for message {message_id} in chat {chat_id}")

    async def get_due_messages(self):
        """Get messages that are due for deletion."""
        rows = await self.fetchall('''
            SELECT chat_id, message_id FROM scheduled_deletions
            WHERE scheduled_time <= CURRENT_TIMESTAMP
        ''')
        return [(row['chat_id'], row['message_id']) for row in rows]

    async def remove_scheduled_message(self, chat_id: int, message_id: int):
        """Remove message from scheduled deletions."""
        await self.execute_and_commit(
            'DELETE FROM scheduled_deletions WHERE chat_id = %s AND message_id = %s',
            (chat_id, message_id)
        )
        log.info(f"Removed scheduled deletion for message {message_id}")

    async def update_user_interaction(self, user_id: int, username: str = None,
                                    first_name: str = None, last_name: str = None,
                                    file_accessed: bool = False):
        """Update user interaction timestamp and count."""
        async with self.get_db_connection() as conn:
            def _update():
                with conn.cursor() as cur:
                    # Check if user exists
                    cur.execute("SELECT 1 FROM users WHERE user_id = %s", (user_id,))
                    exists = cur.fetchone()

                    if exists:
                        # Update existing user
                        cur.execute('''
                            UPDATE users
                            SET last_active = CURRENT_TIMESTAMP,
                                total_interactions = total_interactions + 1,
                                username = COALESCE(%s, username),
                                first_name = COALESCE(%s, first_name),
                                last_name = COALESCE(%s, last_name)
                            WHERE user_id = %s
                        ''', (username, first_name, last_name, user_id))

                        if file_accessed:
                            cur.execute('''
                                UPDATE users
                                SET total_files_accessed = total_files_accessed + 1,
                                    last_file_accessed = CURRENT_TIMESTAMP
                                WHERE user_id = %s
                            ''', (user_id,))
                    else:
                        # Insert new user
                        cur.execute('''
                            INSERT INTO users
                            (user_id, username, first_name, last_name, first_seen, last_active, total_interactions)
                            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
                        ''', (user_id, username, first_name, last_name))
                    conn.commit()
            await asyncio.to_thread(_update)

    async def get_user_stats(self) -> Dict[str, Any]:
        """Get comprehensive user statistics."""
        async def fetch_one(query, params=None):
            return await self.fetchrow(query, params)

        total_users_task = fetch_one("SELECT COUNT(*) as count FROM users")
        active_7d_task = fetch_one('''
            SELECT COUNT(*) as count FROM users
            WHERE last_active > CURRENT_TIMESTAMP - INTERVAL '7 days'
        ''')
        active_30d_task = fetch_one('''
            SELECT COUNT(*) as count FROM users
            WHERE last_active > CURRENT_TIMESTAMP - INTERVAL '30 days'
        ''')
        new_today_task = fetch_one('''
            SELECT COUNT(*) as count FROM users
            WHERE DATE(first_seen) = CURRENT_DATE
        ''')
        new_week_task = fetch_one('''
            SELECT COUNT(*) as count FROM users
            WHERE first_seen > CURRENT_TIMESTAMP - INTERVAL '7 days'
        ''')
        users_files_task = fetch_one('''
            SELECT COUNT(DISTINCT user_id) as count FROM users
            WHERE total_files_accessed > 0
        ''')
        top_users_task = self.fetchall('''
            SELECT user_id, username, first_name, last_name,
                   total_interactions, total_files_accessed,
                   TO_CHAR(last_active, 'YYYY-MM-DD HH24:MI:SS') as last_active,
                   TO_CHAR(first_seen, 'YYYY-MM-DD HH24:MI:SS') as first_seen
            FROM users
            ORDER BY total_interactions DESC
            LIMIT 10
        ''')
        growth_task = self.fetchall('''
            SELECT
                TO_CHAR(first_seen, 'YYYY-MM-DD') as date,
                COUNT(*) as new_users
            FROM users
            WHERE first_seen > CURRENT_TIMESTAMP - INTERVAL '30 days'
            GROUP BY date
            ORDER BY date DESC
            LIMIT 15
        ''')

        total_users, active_7d, active_30d, new_today, new_week, users_files, top_users, growth_data = await asyncio.gather(
            total_users_task, active_7d_task, active_30d_task, new_today_task, new_week_task, users_files_task, top_users_task, growth_task
        )

        return {
            'total_users': total_users['count'] if total_users else 0,
            'active_users_7d': active_7d['count'] if active_7d else 0,
            'active_users_30d': active_30d['count'] if active_30d else 0,
            'new_users_today': new_today['count'] if new_today else 0,
            'new_users_week': new_week['count'] if new_week else 0,
            'top_users': [(row['user_id'], row['username'], row['first_name'], row['last_name'], row['total_interactions'], row['total_files_accessed'], row['last_active'], row['first_seen']) for row in top_users],
            'users_with_files': users_files['count'] if users_files else 0,
            'growth_data': [(row['date'], row['new_users']) for row in growth_data]
        }

    async def get_all_user_ids(self, exclude_admin: bool = True) -> List[int]:
        """Get all user IDs for broadcasting."""
        if exclude_admin:
            rows = await self.fetchall("SELECT user_id FROM users WHERE user_id != %s", (ADMIN_ID,))
        else:
            rows = await self.fetchall("SELECT user_id FROM users")
        return [row['user_id'] for row in rows]

    async def get_user_count(self) -> int:
        """Get total number of users."""
        result = await self.fetchrow("SELECT COUNT(*) as count FROM users")
        return result['count'] if result else 0

    async def close_pool(self):
        """Close all connections in the pool."""
        if self.pool:
            self.pool.closeall()
            log.info("Database connection pool closed")

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

        log.info(f"🗑️ Attempting to delete message {message_id} from chat {chat_id}")

        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            log.info(f"✅ Successfully deleted message {message_id}")
            await db.remove_scheduled_message(chat_id, message_id)
        except Exception as e:
            error_msg = str(e).lower()
            if "message to delete not found" in error_msg:
                await db.remove_scheduled_message(chat_id, message_id)
            elif "message can't be deleted" in error_msg:
                log.warning(f"Can't delete message {message_id}")
            else:
                log.error(f"Failed to delete message {message_id}: {e}")

    except Exception as e:
        log.error(f"Error in delete_message_job: {e}", exc_info=True)

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
                name=f"delete_msg_{chat_id}_{message_id}_{int(time.time())}"
            )
            log.info(f"Scheduled deletion of message {message_id} in {DELETE_AFTER} seconds")
    except Exception as e:
        log.error(f"Failed to schedule deletion: {e}")

async def cleanup_overdue_messages(context: ContextTypes.DEFAULT_TYPE):
    """Clean up overdue messages"""
    try:
        due_messages = await db.get_due_messages()
        if not due_messages:
            return

        log.info(f"Found {len(due_messages)} overdue messages")

        for chat_id, message_id in due_messages:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
                log.info(f"✅ Cleanup: Deleted overdue message {message_id}")
                await db.remove_scheduled_message(chat_id, message_id)
            except Exception as e:
                error_msg = str(e).lower()
                if "message to delete not found" in error_msg:
                    await db.remove_scheduled_message(chat_id, message_id)
                else:
                    log.error(f"Cleanup failed for {message_id}: {e}")

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

    # Check channel 1
    ch1_result = await check_user_in_channel(bot, CHANNEL_1, user_id, force_check)
    result["channel1"] = ch1_result
    if not ch1_result:
        result["missing_channels"].append(f"@{CHANNEL_1}")

    # Check channel 2
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
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Telegram File Bot</h1>
        <div class="status">
            <h3>✅ Status: <strong>ACTIVE</strong></h3>
            <p>Bot is running on Render with PostgreSQL (psycopg2)</p>
            <p>Uptime: {{ uptime }}</p>
            <p>Files in DB: {{ file_count }}</p>
            <p>Users in DB: {{ user_count }}</p>
            <p>📁 Storage: PERMANENT PostgreSQL</p>
        </div>

        <div class="info">
            <h3>📊 Bot Information</h3>
            <ul>
                <li>Bot: <strong>@{{ bot_username }}</strong></li>
                <li>Database: <strong>Render PostgreSQL</strong></li>
                <li>Driver: <strong>psycopg2-binary (C-based, fast)</strong></li>
                <li>Storage: <strong>PERMANENT - Survives restarts!</strong></li>
                <li>Message Auto-delete: <strong>{{ delete_minutes }} minutes</strong></li>
            </ul>
        </div>

        <div class="info">
            <h3>📞 Start Bot</h3>
            <p><a href="https://t.me/{{ bot_username }}" target="_blank" class="btn">Start @{{ bot_username }}</a></p>
        </div>
    </div>
</body>
</html>
    """

    uptime_seconds = time.time() - start_time
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))

    # Use asyncio.run() with proper error handling
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        file_count = loop.run_until_complete(db.get_file_count())
        user_count = loop.run_until_complete(db.get_user_count())
        loop.close()
    except Exception as e:
        log.error(f"Error fetching counts for home route: {e}")
        file_count = 0
        user_count = 0

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
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        file_count = loop.run_until_complete(db.get_file_count())
        user_count = loop.run_until_complete(db.get_user_count())
        loop.close()
    except Exception as e:
        log.error(f"Error in health check: {e}")
        file_count = 0
        user_count = 0

    return jsonify({
        "status": "OK",
        "timestamp": datetime.now().isoformat(),
        "service": "telegram-file-bot",
        "uptime": str(timedelta(seconds=int(time.time() - start_time))),
        "database": "postgresql",
        "driver": "psycopg2-binary",
        "storage": "permanent",
        "file_count": file_count,
        "user_count": user_count,
        "bot_initialized": bot_initialized
    }), 200

@app.route('/ping')
def ping():
    return "pong", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle Telegram webhook updates"""
    global bot_app, bot_loop, bot_initialized
    
    if not bot_initialized or bot_app is None or bot_loop is None:
        log.error("Bot application not fully initialized for webhook")
        return "Bot not ready", 503

    update_data = request.get_json()
    if not update_data:
        return "Invalid request", 400

    # Process update in bot's event loop
    future = asyncio.run_coroutine_threadsafe(
        process_update(update_data, bot_app),
        bot_loop
    )
    
    try:
        # Wait a bit for the update to be queued
        future.result(timeout=1)
    except asyncio.TimeoutError:
        # Update is queued but not completed - that's fine
        pass
    except Exception as e:
        log.error(f"Error queueing update: {e}")

    return "OK", 200

async def process_update(update_data, application):
    """Process Telegram update"""
    try:
        update = Update.de_json(update_data, application.bot)
        await application.process_update(update)
    except Exception as e:
        log.error(f"Error processing update: {e}", exc_info=True)

def run_flask_thread():
    """Run Flask server in a thread"""
    port = int(os.environ.get('PORT', 10000))

    import warnings
    warnings.filterwarnings("ignore")

    import logging as flask_logging
    flask_logging.getLogger('werkzeug').setLevel(flask_logging.ERROR)
    flask_logging.getLogger('flask').setLevel(flask_logging.ERROR)

    # Important for Python 3.14: Disable asyncio debug in Flask thread
    os.environ['PYTHONASYNCIODEBUG'] = '0'

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

        # Update user interaction
        user = update.effective_user
        await db.update_user_interaction(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )

        # No file key - show welcome
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
                "2️⃣ Join both channels\n"
                "3️⃣ Click 'Check Membership'\n\n"
                f"⚠️ Messages auto-delete after {DELETE_AFTER//60} minutes\n"
                "💾 *Storage:* PERMANENT PostgreSQL",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        # File key exists
        key = args[0]
        file_info = await db.get_file(key)

        if not file_info:
            sent_msg = await update.message.reply_text("❌ File not found")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        # Check membership
        result = await check_membership(user_id, context, force_check=True)

        if not result["all_joined"]:
            missing_count = len(result["missing_channels"])

            if missing_count == 2:
                keyboard = [
                    [InlineKeyboardButton("📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                    [InlineKeyboardButton("📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                    [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                ]
                text = "🔒 *Join both channels to access this file*"
            else:
                missing_channel = result["missing_channels"][0].replace("@", "")
                channel_name = "Channel 1" if CHANNEL_1 in missing_channel else "Channel 2"
                keyboard = [
                    [InlineKeyboardButton(f"📥 Join {channel_name}", url=f"https://t.me/{missing_channel}")],
                    [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                ]
                text = f"🔒 *Join {channel_name} to access this file*"

            sent_msg = await update.message.reply_text(
                text,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        # User has joined - send file
        await db.update_user_interaction(user_id=user_id, file_accessed=True)

        try:
            filename = file_info['file_name']
            ext = filename.lower().split('.')[-1] if '.' in filename else ""

            warning = f"\n\n⚠️ Auto-deletes in {DELETE_AFTER//60} minutes\n💾 Permanently stored"

            if file_info['is_video'] and ext in PLAYABLE_EXTS:
                sent = await context.bot.send_video(
                    chat_id=chat_id,
                    video=file_info["file_id"],
                    caption=f"🎬 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                    parse_mode="Markdown",
                    supports_streaming=True
                )
            else:
                sent = await context.bot.send_document(
                    chat_id=chat_id,
                    document=file_info["file_id"],
                    caption=f"📁 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                    parse_mode="Markdown"
                )

            await schedule_message_deletion(context, sent.chat_id, sent.message_id)

        except Exception as e:
            log.error(f"Error sending file: {e}")
            sent_msg = await update.message.reply_text("❌ Failed to send file")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.error(f"Start error: {e}", exc_info=True)

async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries"""
    try:
        query = update.callback_query
        await query.answer()

        user_id = query.from_user.id
        data = query.data

        # Update user interaction
        user = query.from_user
        await db.update_user_interaction(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )

        if data == "check_membership":
            result = await check_membership(user_id, context, force_check=True)

            if result["all_joined"]:
                await query.edit_message_text(
                    "✅ *You've joined both channels!*\n\n"
                    "Now you can use file links from admin.",
                    parse_mode="Markdown"
                )
            else:
                missing_count = len(result["missing_channels"])

                if missing_count == 2:
                    keyboard = [
                        [InlineKeyboardButton("📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],
                        [InlineKeyboardButton("📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],
                        [InlineKeyboardButton("🔄 Check Again", callback_data="check_membership")]
                    ]
                    text = "❌ *Not a member of either channel*"
                else:
                    missing = result["missing_channels"][0].replace("@", "")
                    name = "Channel 1" if CHANNEL_1 in missing else "Channel 2"
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join {name}", url=f"https://t.me/{missing}")],
                        [InlineKeyboardButton("🔄 Check Again", callback_data="check_membership")]
                    ]
                    text = f"❌ *Missing {name}*"

                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            return

        if data.startswith("check|"):
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
                        [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                    ]
                    text = "❌ *Join both channels*"
                else:
                    missing = result["missing_channels"][0].replace("@", "")
                    name = "Channel 1" if CHANNEL_1 in missing else "Channel 2"
                    keyboard = [
                        [InlineKeyboardButton(f"📥 Join {name}", url=f"https://t.me/{missing}")],
                        [InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]
                    ]
                    text = f"❌ *Join {name}*"

                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return

            # Send file
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
                        caption=f"🎬 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                        parse_mode="Markdown",
                        supports_streaming=True
                    )
                else:
                    sent = await context.bot.send_document(
                        chat_id=chat_id,
                        document=file_info["file_id"],
                        caption=f"📁 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                        parse_mode="Markdown"
                    )

                await query.edit_message_text("✅ *File sent below!*", parse_mode="Markdown")
                await schedule_message_deletion(context, sent.chat_id, sent.message_id)

            except Exception as e:
                log.error(f"Failed to send file: {e}")
                await query.edit_message_text("❌ Failed to send file")

    except Exception as e:
        log.error(f"Callback error: {e}", exc_info=True)

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
            sent_msg = await msg.reply_text("❌ Send a video or document")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        file_info = {
            "file_name": filename,
            "mime_type": mime_type,
            "is_video": is_video,
            "size": int(file_size) if file_size else 0
        }

        key = await db.save_file(file_id, file_info)
        link = f"https://t.me/{bot_username}?start={key}"

        sent_msg = await msg.reply_text(
            f"✅ *Upload Successful*\n\n"
            f"📁 *Name:* `{filename}`\n"
            f"🔑 *Key:* `{key}`\n"
            f"💾 *Storage:* PERMANENT PostgreSQL\n\n"
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

    # Get total accesses
    files = await db.get_all_files()
    total_access = sum(f[5] for f in files) if files else 0

    sent_msg = await update.message.reply_text(
        f"📊 *Bot Statistics*\n\n"
        f"🤖 Bot: @{bot_username}\n"
        f"⏱ Uptime: {uptime}\n"
        f"📁 Files: {file_count}\n"
        f"👥 Users: {user_count}\n"
        f"👀 Accesses: {total_access}\n"
        f"💾 Database: PostgreSQL (permanent)\n"
        f"⏰ Auto-delete: {DELETE_AFTER//60} minutes",
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
    for file in files[:20]:  # Show first 20
        file_id, name, is_video, size, ts, access = file
        size_mb = size / (1024*1024) if size else 0
        msg += f"🔑 `{file_id}` - {name[:30]}... ({size_mb:.1f}MB) - 👥 {access}\n"

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
        f"👥 Total Users: {stats_data['total_users']}\n"
        f"🟢 Active (7d): {stats_data['active_users_7d']}\n"
        f"🟡 Active (30d): {stats_data['active_users_30d']}\n"
        f"📈 New Today: {stats_data['new_users_today']}\n"
        f"📁 File Accessors: {stats_data['users_with_files']}\n"
    )

    sent_msg = await update.message.reply_text(msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast to users (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args and not update.message.reply_to_message:
        sent_msg = await update.message.reply_text("❌ Usage: /broadcast <message> or reply with /broadcast")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    # Get message text
    if update.message.reply_to_message:
        message_text = update.message.reply_to_message.text or update.message.reply_to_message.caption
    else:
        message_text = " ".join(context.args)

    if not message_text:
        sent_msg = await update.message.reply_text("❌ Message cannot be empty")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    # Get all users
    user_ids = await db.get_all_user_ids(exclude_admin=True)

    status_msg = await update.message.reply_text(
        f"🔄 Broadcasting to {len(user_ids)} users...",
        parse_mode="Markdown"
    )

    successful = 0
    failed = 0

    for user_id in user_ids[:100]:  # Limit to 100 for free tier
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"📢 *Broadcast*\n\n{message_text}",
                parse_mode="Markdown"
            )
            successful += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            log.warning(f"Failed to send to {user_id}: {e}")

    await status_msg.edit_text(
        f"✅ Broadcast complete\n✅ Sent: {successful}\n❌ Failed: {failed}",
        parse_mode="Markdown"
    )
    await schedule_message_deletion(context, status_msg.chat_id, status_msg.message_id)

async def clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear membership cache (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    await db.clear_membership_cache()
    sent_msg = await update.message.reply_text("✅ Cache cleared")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def testchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test channel access (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    user_id = update.effective_user.id

    try:
        member1 = await context.bot.get_chat_member(f"@{CHANNEL_1}", user_id)
        ch1 = f"✅ {member1.status}"
    except Exception as e:
        ch1 = f"❌ {str(e)[:50]}"

    try:
        member2 = await context.bot.get_chat_member(f"@{CHANNEL_2}", user_id)
        ch2 = f"✅ {member2.status}"
    except Exception as e:
        ch2 = f"❌ {str(e)[:50]}"

    sent_msg = await update.message.reply_text(
        f"🔍 *Channel Test*\n\n"
        f"@{CHANNEL_1}: {ch1}\n"
        f"@{CHANNEL_2}: {ch2}",
        parse_mode="Markdown"
    )
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual cleanup (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    days = 30
    if context.args:
        try:
            days = int(context.args[0])
        except:
            pass

    result = await db.execute_and_commit('''
        DELETE FROM files
        WHERE timestamp < CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
    ''', (days,))

    file_count = await db.get_file_count()

    sent_msg = await update.message.reply_text(
        f"🧹 Cleaned files older than {days} days\n"
        f"📁 Files remaining: {file_count}",
        parse_mode="Markdown"
    )
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)


# ============ MAIN ============
async def initialize_bot():
    """Initialize bot application"""
    global bot_app, bot_loop, bot_initialized

    if not BOT_TOKEN or not ADMIN_ID:
        log.error("Missing BOT_TOKEN or ADMIN_ID")
        return None

    # Initialize database connection pool synchronously first
    log.info("Initializing database connection pool...")
    try:
        # This will run the synchronous pool initialization
        db._get_pool_sync()
        log.info("Database pool initialized.")
    except Exception as e:
        log.error(f"Failed to initialize database: {e}", exc_info=True)
        return None

    # Create application with a custom request for better timeout handling
    request = HTTPXRequest(connection_pool_size=40)
    application = Application.builder().token(BOT_TOKEN).request(request).build()
    
    # Initialize the application
    await application.initialize()
    
    bot_loop = asyncio.get_running_loop()
    bot_app = application

    # Add job queue for cleanup
    if application.job_queue:
        application.job_queue.run_repeating(
            cleanup_overdue_messages,
            interval=300,
            first=10
        )

    # Add handlers
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("listfiles", listfiles))
    application.add_handler(CommandHandler("deletefile", deletefile))
    application.add_handler(CommandHandler("users", users))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CommandHandler("clearcache", clearcache))
    application.add_handler(CommandHandler("testchannel", testchannel))
    application.add_handler(CommandHandler("cleanup", cleanup))

    application.add_handler(CallbackQueryHandler(check_join, pattern="^check_membership$"))
    application.add_handler(CallbackQueryHandler(check_join, pattern="^check\\|"))

    upload_filter = filters.VIDEO | filters.Document.ALL
    application.add_handler(
        MessageHandler(upload_filter & filters.User(ADMIN_ID) & filters.ChatType.PRIVATE, upload)
    )

    # Set webhook
    render_url = os.environ.get('RENDER_EXTERNAL_URL')
    if not render_url:
        render_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME', 'localhost')}"

    webhook_url = f"{render_url}/webhook"
    log.info(f"Setting webhook to: {webhook_url}")

    try:
        # Delete any existing webhook
        await application.bot.delete_webhook(drop_pending_updates=True)
        # Set new webhook
        await application.bot.set_webhook(
            url=webhook_url,
            allowed_updates=Update.ALL_TYPES,
            max_connections=40
        )
        log.info("✅ Webhook set successfully")
    except Exception as e:
        log.error(f"Failed to set webhook: {e}", exc_info=True)
        return None

    # Mark as initialized
    bot_initialized = True
    
    log.info("🤖 Bot initialized and ready via webhook")
    log.info(f"📁 Files in database: {await db.get_file_count()}")
    log.info(f"👥 Users in database: {await db.get_user_count()}")

    return application

async def main_async():
    """Async main function"""
    global bot_app
    
    bot_app = await initialize_bot()
    
    if bot_app is None:
        log.error("Failed to initialize bot. Exiting.")
        return

    log.info("Bot is running. Waiting for webhook events...")
    
    # Keep the application running
    while True:
        await asyncio.sleep(3600)  # Sleep for an hour

def main():
    """Main function"""
    print("\n" + "=" * 60)
    print("🤖 TELEGRAM FILE BOT - Python 3.14+ Compatibility Mode (Webhook)")
    print("=" * 60)
    print(f"✅ Bot: @{bot_username}")
    print(f"✅ Admin: {ADMIN_ID}")
    print(f"✅ Database: Render PostgreSQL with psycopg2-binary")
    print(f"✅ Python Version: {sys.version}")
    print("=" * 60 + "\n")

    # Start Flask in a separate thread
    flask_thread = threading.Thread(target=run_flask_thread, daemon=True)
    flask_thread.start()
    log.info("Flask thread started")

    # Give Flask a moment to start
    time.sleep(2)

    # Run the async main function
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
    except Exception as e:
        log.error(f"Fatal error in main loop: {e}", exc_info=True)
    finally:
        log.info("Shutting down...")
        if bot_app:
            asyncio.run(bot_app.shutdown())
        asyncio.run(db.close_pool())
        print("Shutdown complete.")

if __name__ == "__main__":
    main()
