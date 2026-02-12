
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

# ============ 🔥 PERSISTENT DISK STORAGE - SURVIVES RESTARTS 🔥 ============
# Create persistent disk in Render Dashboard first!
# Mount path: /opt/render/project/src/data
PERSISTENT_PATH = Path("/opt/render/project/src/data")
PERSISTENT_PATH.mkdir(parents=True, exist_ok=True)

# SQLite database on persistent disk - FILES WILL NEVER VANISH!
DB_PATH = PERSISTENT_PATH / "file_bot.db"
# ===========================================================================

DELETE_AFTER = 600  # 10 minutes - DELETE ALL BOT MESSAGES
MAX_STORED_FILES = 10000  # Increased since we have persistent storage
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

# ================= OPTIMIZED DATABASE (STORAGE EFFICIENT) =================
# - Integer timestamps (Unix epoch) instead of DATETIME strings
# - Channel names replaced by IDs (0/1) in membership_cache
# - mime_type column removed (video detection via extension)
# - User name fields truncated to save space
# - Schema versioning & automatic migration

class Database:
    SCHEMA_VERSION = 2

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        log.info(f"📀 Database path: {self.db_path}")
        log.info(f"📀 Database exists: {self.db_path.exists()}")
        self.init_db()

    def _now(self) -> int:
        """Return current Unix timestamp"""
        return int(time.time())

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
            # Additional optimizations
            conn.execute("PRAGMA auto_vacuum=FULL;")
            conn.execute("PRAGMA encoding='UTF-8';")
            yield conn
        finally:
            conn.close()

    def _get_schema_version(self, conn) -> int:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'")
        if not cursor.fetchone():
            return 0  # No version table -> old schema
        cursor.execute("SELECT version FROM schema_version LIMIT 1")
        row = cursor.fetchone()
        return row[0] if row else 0

    def _upgrade_schema(self, conn):
        """Migrate from old schema (v0/v1) to v2 (optimized)"""
        current_version = self._get_schema_version(conn)
        if current_version >= self.SCHEMA_VERSION:
            return

        log.info(f"🔄 Upgrading database schema from v{current_version} to v{self.SCHEMA_VERSION}...")

        if current_version == 0:
            # No schema_version table -> assume v1 schema exists or new DB
            # Check if old tables exist
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'")
            old_tables_exist = cursor.fetchone() is not None

            if old_tables_exist:
                # Existing v1 database, migrate
                self._migrate_v1_to_v2(conn)
            else:
                # Fresh database, create v2 directly
                self._create_v2_tables(conn)
        elif current_version == 1:
            self._migrate_v1_to_v2(conn)

        # Set schema version
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS schema_version")
        cursor.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        cursor.execute("INSERT INTO schema_version (version) VALUES (?)", (self.SCHEMA_VERSION,))
        conn.commit()
        log.info("✅ Schema upgrade completed")

        # VACUUM to reclaim space after migration
        conn.execute("VACUUM")
        log.info("🧹 VACUUM performed to reclaim space")

    def _migrate_v1_to_v2(self, conn):
        """Migrate from old schema (v1) to v2"""
        cursor = conn.cursor()

        # ----- files table migration -----
        # Old schema: id, file_id, file_name, mime_type, is_video, file_size, timestamp (TEXT), access_count
        # New schema: id, file_id, file_name, is_video, file_size, timestamp (INTEGER), access_count
        cursor.execute('''
            CREATE TABLE files_v2 (
                id TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                file_name TEXT NOT NULL,
                is_video INTEGER DEFAULT 0,
                file_size INTEGER DEFAULT 0,
                timestamp INTEGER DEFAULT (strftime('%s','now')),
                access_count INTEGER DEFAULT 0
            )
        ''')
        # Convert timestamp strings to Unix epoch
        cursor.execute('''
            INSERT INTO files_v2 (id, file_id, file_name, is_video, file_size, timestamp, access_count)
            SELECT 
                id,
                file_id,
                file_name,
                is_video,
                file_size,
                strftime('%s', timestamp) AS timestamp,
                access_count
            FROM files
        ''')
        cursor.execute("DROP TABLE files")
        cursor.execute("ALTER TABLE files_v2 RENAME TO files")

        # ----- membership_cache migration -----
        # Old: user_id, channel (TEXT), is_member, timestamp (TEXT)
        # New: user_id, channel_id (INTEGER 0/1), is_member, timestamp (INTEGER)
        cursor.execute('''
            CREATE TABLE membership_cache_v2 (
                user_id INTEGER,
                channel_id INTEGER,
                is_member INTEGER,
                timestamp INTEGER DEFAULT (strftime('%s','now')),
                PRIMARY KEY (user_id, channel_id)
            )
        ''')
        # Map channel usernames to IDs (0 = CHANNEL_1, 1 = CHANNEL_2)
        cursor.execute('''
            INSERT INTO membership_cache_v2 (user_id, channel_id, is_member, timestamp)
            SELECT 
                user_id,
                CASE 
                    WHEN channel = ? THEN 0
                    WHEN channel = ? THEN 1
                    ELSE -1
                END,
                is_member,
                strftime('%s', timestamp)
            FROM membership_cache
        ''', (CHANNEL_1, CHANNEL_2))
        cursor.execute("DROP TABLE membership_cache")
        cursor.execute("ALTER TABLE membership_cache_v2 RENAME TO membership_cache")

        # ----- scheduled_deletions migration -----
        # Old: chat_id, message_id, scheduled_time (TEXT), delete_after
        # New: chat_id, message_id, scheduled_time (INTEGER), delete_after
        cursor.execute('''
            CREATE TABLE scheduled_deletions_v2 (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                scheduled_time INTEGER NOT NULL,
                delete_after INTEGER DEFAULT 600,
                PRIMARY KEY (chat_id, message_id)
            )
        ''')
        cursor.execute('''
            INSERT INTO scheduled_deletions_v2 (chat_id, message_id, scheduled_time, delete_after)
            SELECT 
                chat_id,
                message_id,
                strftime('%s', scheduled_time),
                delete_after
            FROM scheduled_deletions
        ''')
        cursor.execute("DROP TABLE scheduled_deletions")
        cursor.execute("ALTER TABLE scheduled_deletions_v2 RENAME TO scheduled_deletions")

        # ----- users migration -----
        # Old: user_id, username, first_name, last_name, first_seen (TEXT), last_active (TEXT),
        #      total_interactions, total_files_accessed, last_file_accessed (TEXT)
        # New: same columns but timestamps as INTEGER
        cursor.execute('''
            CREATE TABLE users_v2 (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                first_seen INTEGER DEFAULT (strftime('%s','now')),
                last_active INTEGER DEFAULT (strftime('%s','now')),
                total_interactions INTEGER DEFAULT 1,
                total_files_accessed INTEGER DEFAULT 0,
                last_file_accessed INTEGER
            )
        ''')
        cursor.execute('''
            INSERT INTO users_v2 (
                user_id, username, first_name, last_name,
                first_seen, last_active, total_interactions,
                total_files_accessed, last_file_accessed
            )
            SELECT
                user_id,
                username,
                first_name,
                last_name,
                strftime('%s', first_seen),
                strftime('%s', last_active),
                total_interactions,
                total_files_accessed,
                strftime('%s', last_file_accessed)
            FROM users
        ''')
        cursor.execute("DROP TABLE users")
        cursor.execute("ALTER TABLE users_v2 RENAME TO users")

        conn.commit()

    def _create_v2_tables(self, conn):
        """Create tables from scratch with v2 schema"""
        cursor = conn.cursor()
        # files
        cursor.execute('''
            CREATE TABLE files (
                id TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                file_name TEXT NOT NULL,
                is_video INTEGER DEFAULT 0,
                file_size INTEGER DEFAULT 0,
                timestamp INTEGER DEFAULT (strftime('%s','now')),
                access_count INTEGER DEFAULT 0
            )
        ''')
        # membership_cache
        cursor.execute('''
            CREATE TABLE membership_cache (
                user_id INTEGER,
                channel_id INTEGER,
                is_member INTEGER,
                timestamp INTEGER DEFAULT (strftime('%s','now')),
                PRIMARY KEY (user_id, channel_id)
            )
        ''')
        # scheduled_deletions
        cursor.execute('''
            CREATE TABLE scheduled_deletions (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                scheduled_time INTEGER NOT NULL,
                delete_after INTEGER DEFAULT 600,
                PRIMARY KEY (chat_id, message_id)
            )
        ''')
        # users
        cursor.execute('''
            CREATE TABLE users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                first_seen INTEGER DEFAULT (strftime('%s','now')),
                last_active INTEGER DEFAULT (strftime('%s','now')),
                total_interactions INTEGER DEFAULT 1,
                total_files_accessed INTEGER DEFAULT 0,
                last_file_accessed INTEGER
            )
        ''')
        # indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON membership_cache(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_deletions_time ON scheduled_deletions(scheduled_time)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_first_seen ON users(first_seen)')
        conn.commit()

    def init_db(self):
        """Initialize/upgrade database"""
        with self.get_connection() as conn:
            self._upgrade_schema(conn)

            # Create indexes if missing (after migration they exist, but for fresh install we create them)
            cursor = conn.cursor()
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON membership_cache(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_deletions_time ON scheduled_deletions(scheduled_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_first_seen ON users(first_seen)')
            conn.commit()

            # Get file count to verify persistence
            try:
                cursor.execute("SELECT COUNT(*) FROM files")
                file_count = cursor.fetchone()[0]
                log.info(f"📊 Database initialized with {file_count} existing files")
            except sqlite3.OperationalError:
                # Table may not exist yet (fresh DB) – ignore
                pass

    # ============ FILE OPERATIONS ============
    def save_file(self, file_id: str, file_info: dict) -> str:
        """Save file info and return generated ID"""
        with db_lock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COALESCE(MAX(CAST(id AS INTEGER)), 0) FROM files")
                max_id = cursor.fetchone()[0]
                new_id = str(max_id + 1)

                cursor.execute('''
                    INSERT INTO files
                    (id, file_id, file_name, is_video, file_size, access_count)
                    VALUES (?, ?, ?, ?, ?, 0)
                ''', (
                    new_id,
                    file_id,
                    file_info.get('file_name', ''),
                    1 if file_info.get('is_video', False) else 0,
                    file_info.get('size', 0)
                ))
                conn.commit()
                log.info(f"💾 Saved file {new_id}: {file_info.get('file_name', '')}")
                return new_id

    def get_file(self, file_id: str) -> Optional[dict]:
        """Get file info by ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT file_id, file_name, is_video, file_size, timestamp, access_count
                FROM files WHERE id = ?
            ''', (file_id,))
            row = cursor.fetchone()
            if row:
                cursor.execute('UPDATE files SET access_count = access_count + 1 WHERE id = ?', (file_id,))
                conn.commit()
                return {
                    'file_id': row[0],
                    'file_name': row[1],
                    'is_video': bool(row[2]),
                    'size': row[3],
                    'timestamp': row[4],
                    'access_count': row[5] + 1
                }
            return None

    def delete_file(self, file_id: str) -> bool:
        """Manually delete a file from database"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM files WHERE id = ?", (file_id,))
            deleted = cursor.rowcount > 0
            conn.commit()
            if deleted:
                log.info(f"🗑️ Deleted file {file_id}")
            return deleted

    def get_all_files(self) -> list:
        """Get all files for admin view"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, file_name, is_video, file_size, timestamp, access_count
                FROM files ORDER BY timestamp DESC
            ''')
            return cursor.fetchall()

    def get_file_count(self) -> int:
        """Get total number of files"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM files")
            return cursor.fetchone()[0]

    def cleanup_old_files(self):
        """Remove files older than AUTO_CLEANUP_DAYS - DISABLED when AUTO_CLEANUP_DAYS = 0"""
        if AUTO_CLEANUP_DAYS <= 0:
            log.info("Auto-cleanup DISABLED (AUTO_CLEANUP_DAYS = 0). Files kept forever.")
            return

        cutoff = int(time.time()) - AUTO_CLEANUP_DAYS * 86400
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM files WHERE timestamp < ?', (cutoff,))
            deleted = cursor.rowcount
            if deleted > 0:
                log.info(f"Auto-cleanup removed {deleted} old files")

            # Limit to MAX_STORED_FILES
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

    # ============ MEMBERSHIP CACHE ============
    def cache_membership(self, user_id: int, channel: str, is_member: bool):
        """Cache membership check result (channel is username without @)"""
        # Map channel to ID (0 or 1)
        channel_id = 0 if channel == CHANNEL_1 else (1 if channel == CHANNEL_2 else -1)
        if channel_id == -1:
            log.warning(f"Unknown channel: {channel}, not caching")
            return
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO membership_cache (user_id, channel_id, is_member, timestamp)
                VALUES (?, ?, ?, ?)
            ''', (user_id, channel_id, 1 if is_member else 0, self._now()))
            conn.commit()

    def get_cached_membership(self, user_id: int, channel: str) -> Optional[bool]:
        """Get cached membership result (valid for 5 minutes)"""
        channel_id = 0 if channel == CHANNEL_1 else (1 if channel == CHANNEL_2 else -1)
        if channel_id == -1:
            return None
        cutoff = self._now() - 300  # 5 minutes
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT is_member FROM membership_cache
                WHERE user_id = ? AND channel_id = ? AND timestamp > ?
            ''', (user_id, channel_id, cutoff))
            row = cursor.fetchone()
            return bool(row[0]) if row else None

    def clear_membership_cache(self, user_id: Optional[int] = None):
        """Clear membership cache for a user or all users"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if user_id:
                cursor.execute("DELETE FROM membership_cache WHERE user_id = ?", (user_id,))
                log.info(f"Cleared cache for user {user_id}")
            else:
                cursor.execute("DELETE FROM membership_cache")
                log.info("Cleared all membership cache")
            conn.commit()

    # ============ SCHEDULED DELETIONS ============
    def schedule_message_deletion(self, chat_id: int, message_id: int):
        """Schedule a message for deletion"""
        scheduled_time = self._now() + DELETE_AFTER
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO scheduled_deletions (chat_id, message_id, scheduled_time, delete_after)
                VALUES (?, ?, ?, ?)
            ''', (chat_id, message_id, scheduled_time, DELETE_AFTER))
            conn.commit()
            log.info(f"Scheduled deletion for msg {message_id} in chat {chat_id} at {scheduled_time}")

    def get_due_messages(self):
        """Get messages that are due for deletion"""
        now = self._now()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT chat_id, message_id FROM scheduled_deletions
                WHERE scheduled_time <= ?
            ''', (now,))
            return cursor.fetchall()

    def remove_scheduled_message(self, chat_id: int, message_id: int):
        """Remove message from scheduled deletions"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM scheduled_deletions WHERE chat_id = ? AND message_id = ?',
                           (chat_id, message_id))
            conn.commit()
            log.info(f"Removed scheduled deletion for msg {message_id}")

    # ============ USER TRACKING ============
    def _truncate_str(self, s: Optional[str], max_len: int) -> Optional[str]:
        """Truncate string to save space"""
        if s and len(s) > max_len:
            return s[:max_len]
        return s

    def update_user_interaction(self, user_id: int, username: str = None,
                                first_name: str = None, last_name: str = None,
                                file_accessed: bool = False):
        """Update user interaction timestamp and count"""
        username = self._truncate_str(username, 32)
        first_name = self._truncate_str(first_name, 64)
        last_name = self._truncate_str(last_name, 64)
        now = self._now()

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
            exists = cursor.fetchone()

            if exists:
                cursor.execute('''
                    UPDATE users
                    SET last_active = ?,
                        total_interactions = total_interactions + 1,
                        username = COALESCE(?, username),
                        first_name = COALESCE(?, first_name),
                        last_name = COALESCE(?, last_name)
                    WHERE user_id = ?
                ''', (now, username, first_name, last_name, user_id))

                if file_accessed:
                    cursor.execute('''
                        UPDATE users
                        SET total_files_accessed = total_files_accessed + 1,
                            last_file_accessed = ?
                        WHERE user_id = ?
                    ''', (now, user_id))
            else:
                cursor.execute('''
                    INSERT INTO users
                    (user_id, username, first_name, last_name, first_seen, last_active, total_interactions)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                ''', (user_id, username, first_name, last_name, now, now))
            conn.commit()

    def get_user_stats(self) -> Dict[str, Any]:
        """Get comprehensive user statistics"""
        now = self._now()
        week_ago = now - 7 * 86400
        month_ago = now - 30 * 86400
        today_start = now - (now % 86400)  # midnight today

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            total_users = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM users WHERE last_active > ?", (week_ago,))
            active_users_7d = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM users WHERE last_active > ?", (month_ago,))
            active_users_30d = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM users WHERE first_seen > ?", (today_start,))
            new_users_today = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM users WHERE first_seen > ?", (week_ago,))
            new_users_week = cursor.fetchone()[0]

            cursor.execute('''
                SELECT user_id, username, first_name, last_name,
                       total_interactions, total_files_accessed,
                       last_active, first_seen
                FROM users
                ORDER BY total_interactions DESC
                LIMIT 10
            ''')
            top_users = cursor.fetchall()

            cursor.execute('''
                SELECT COUNT(DISTINCT user_id) FROM users
                WHERE total_files_accessed > 0
            ''')
            users_with_files = cursor.fetchone()[0]

            # Growth data: group by day using integer division
            cursor.execute('''
                SELECT
                    date(datetime(first_seen, 'unixepoch')) as date,
                    COUNT(*) as new_users
                FROM users
                WHERE first_seen > ?
                GROUP BY date
                ORDER BY date DESC
                LIMIT 15
            ''', (month_ago,))
            growth_data = cursor.fetchall()

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

    def get_all_user_ids(self, exclude_admin: bool = True) -> List[int]:
        """Get all user IDs for broadcasting"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if exclude_admin:
                cursor.execute("SELECT user_id FROM users WHERE user_id != ?", (ADMIN_ID,))
            else:
                cursor.execute("SELECT user_id FROM users")
            return [row[0] for row in cursor.fetchall()]

    def get_user_count(self) -> int:
        """Get total number of users"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0]

# Initialize database
db = Database()

# ============ FIXED MESSAGE DELETION SYSTEM ============
async def delete_message_job(context):
    """Delete message after timer"""
    try:
        job = context.job
        chat_id = job.chat_id
        message_id = job.data

        if not chat_id or not message_id:
            log.warning(f"Invalid delete job data: chat_id={chat_id}, message_id={message_id}")
            return

        log.info(f"🗑️ Attempting to delete message {message_id} from chat {chat_id}")

        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            log.info(f"✅ Successfully deleted message {message_id} from chat {chat_id}")
            db.remove_scheduled_message(chat_id, message_id)
        except Exception as e:
            error_msg = str(e).lower()
            if "message to delete not found" in error_msg:
                log.info(f"Message {message_id} already deleted")
                db.remove_scheduled_message(chat_id, message_id)
            elif "message can't be deleted" in error_msg:
                log.warning(f"Can't delete message {message_id} - insufficient permissions")
            elif "chat not found" in error_msg:
                log.info(f"Chat {chat_id} not found")
                db.remove_scheduled_message(chat_id, message_id)
            else:
                log.error(f"Failed to delete message {message_id}: {e}")
    except Exception as e:
        log.error(f"Error in delete_message_job: {e}", exc_info=True)

async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    """Schedule a message for deletion after DELETE_AFTER seconds"""
    try:
        db.schedule_message_deletion(chat_id, message_id)

        if not context.job_queue:
            log.warning(f"Job queue not available - will use database backup for message {message_id}")
            return

        context.job_queue.run_once(
            delete_message_job,
            DELETE_AFTER,
            data=message_id,
            chat_id=chat_id,
            name=f"delete_msg_{chat_id}_{message_id}_{int(time.time())}"
        )
        log.info(f"Scheduled deletion of message {message_id} in {DELETE_AFTER} seconds")
    except Exception as e:
        log.error(f"Failed to schedule deletion for message {message_id}: {e}")

async def cleanup_overdue_messages(context: ContextTypes.DEFAULT_TYPE):
    """Clean up any overdue messages from database"""
    try:
        due_messages = db.get_due_messages()
        if not due_messages:
            return

        log.info(f"Found {len(due_messages)} overdue messages to clean up")

        for chat_id, message_id in due_messages:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
                log.info(f"✅ Cleanup: Deleted overdue message {message_id} from chat {chat_id}")
                db.remove_scheduled_message(chat_id, message_id)
            except Exception as e:
                error_msg = str(e).lower()
                if "message to delete not found" in error_msg:
                    log.info(f"Cleanup: Message {message_id} already deleted")
                    db.remove_scheduled_message(chat_id, message_id)
                elif "message can't be deleted" in error_msg:
                    log.warning(f"Cleanup: Can't delete message {message_id} - insufficient permissions")
                else:
                    log.error(f"Cleanup: Failed to delete message {message_id}: {e}")
    except Exception as e:
        log.error(f"Error in cleanup_overdue_messages: {e}")

# ============ MEMBERSHIP CHECK ============
async def check_user_in_channel(bot, channel: str, user_id: int, force_check: bool = False) -> bool:
    """Check if user is in channel"""
    if not force_check:
        cached = db.get_cached_membership(user_id, channel)
        if cached is not None:
            log.info(f"Cache hit for user {user_id} in @{channel}: {cached}")
            return cached

    try:
        if not channel.startswith("@"):
            channel_username = f"@{channel}"
        else:
            channel_username = channel

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
    """Check if user is member of both channels"""
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
            <p>Bot is running on Render with Persistent Disk</p>
            <p>Uptime: {{ uptime }}</p>
            <p>Files in DB: {{ file_count }}</p>
            <p>Users in DB: {{ user_count }}</p>
            <p>📁 Storage: PERSISTENT DISK (survives restarts)</p>
        </div>

        <div class="info">
            <h3>📊 Bot Information</h3>
            <ul>
                <li>Service: <strong>Render Web Service</strong></li>
                <li>Bot: <strong>@{{ bot_username }}</strong></li>
                <li>Storage: <strong>Persistent Disk - NEVER VANISHES</strong></li>
                <li>File Storage: <strong>PERMANENT</strong></li>
                <li>Message Auto-delete: <strong>{{ delete_minutes }} minutes</strong></li>
                <li>Total Users: <strong>{{ user_count }}</strong></li>
            </ul>
        </div>

        <div class="warning">
            <h3>⚠️ Important Notes</h3>
            <ul>
                <li>Files are stored on <strong>PERSISTENT DISK</strong> - survives restarts!</li>
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
            <small>Render • {{ current_time }} • v1.1 • Persistent Storage • User Tracking</small>
        </footer>
    </div>
</body>
</html>
    """

    uptime_seconds = time.time() - start_time
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))

    file_count = 0
    user_count = 0
    try:
        file_count = db.get_file_count()
        user_count = db.get_user_count()
    except:
        pass

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
    user_count = 0
    try:
        user_count = db.get_user_count()
    except:
        pass

    return jsonify({
        "status": "OK",
        "timestamp": datetime.now().isoformat(),
        "service": "telegram-file-bot",
        "uptime": str(timedelta(seconds=int(time.time() - start_time))),
        "database": "sqlite",
        "storage": "persistent_disk",
        "file_count": db.get_file_count(),
        "user_count": user_count
    }), 200

@app.route('/ping')
def ping():
    return "pong", 200

def run_flask_thread():
    """Run Flask server in a thread for Render"""
    port = int(os.environ.get('PORT', 10000))

    import warnings
    warnings.filterwarnings("ignore")

    import logging as flask_logging
    flask_logging.getLogger('werkzeug').setLevel(flask_logging.ERROR)
    flask_logging.getLogger('flask').setLevel(flask_logging.ERROR)

    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Error handler"""
    log.error(f"Error: {context.error}", exc_info=True)

# ============ ADMIN COMMANDS ============
async def cleanup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cleanup command - MANUAL cleanup only (optional)"""
    if update.effective_user.id != ADMIN_ID:
        return

    days = 30
    if context.args:
        try:
            days = int(context.args[0])
            days = max(1, min(days, 365))
        except ValueError:
            sent_msg = await update.message.reply_text("Usage: /cleanup [days=30]\nSet days=0 to cancel")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

    if days == 0:
        sent_msg = await update.message.reply_text("✅ Cleanup cancelled. Files will be kept permanently.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    try:
        cutoff = int(time.time()) - days * 86400
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM files WHERE timestamp < ?', (cutoff,))
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
        sent_msg = await update.message.reply_text(f"❌ Cleanup failed: {str(e)[:100]}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def deletefile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually delete a specific file from database"""
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
    """List all files in database"""
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

            # timestamp is now integer epoch
            date_str = datetime.fromtimestamp(timestamp).strftime("%b %d, %Y") if timestamp else "Unknown"

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
            f"• Storage: PERMANENT (persistent disk)\n\n"
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
    user_count = db.get_user_count()
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
        f"👥 Total users: {user_count}\n"
        f"👥 Total accesses: {total_access}\n"
        f"💾 DB Size: {db_size:.1f} KB\n"
        f"💿 Storage: Persistent Disk - SURVIVES RESTARTS\n"
        f"🧹 Auto-cleanup: DISABLED (permanent storage)\n"
        f"⏰ Message auto-delete: {DELETE_AFTER//60} minutes\n\n"
        f"📢 Channels:\n"
        f"1. @{CHANNEL_1}\n"
        f"2. @{CHANNEL_2}\n\n"
        f"⚙️ Admin commands:\n"
        f"/listfiles - View all files\n"
        f"/deletefile <key> - Delete specific file\n"
        f"/cleanup [days] - Manual cleanup (optional)\n"
        f"/users - User statistics\n"
        f"/broadcast - Send message to all users"
    )
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear membership cache"""
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
    """Test if bot can access channels"""
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

async def users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user statistics and top users"""
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        stats_data = db.get_user_stats()

        top_users_text = ""
        for i, user in enumerate(stats_data['top_users'], 1):
            user_id, username, first_name, last_name, interactions, files_accessed, last_active, first_seen = user

            if first_name and last_name:
                name = f"{first_name} {last_name}"
            elif first_name:
                name = first_name
            elif username:
                name = f"@{username}"
            else:
                name = f"User {user_id}"

            # Convert timestamps
            last_active_str = datetime.fromtimestamp(last_active).strftime("%b %d") if last_active else "Unknown"
            top_users_text += f"{i}. {name[:20]}{'...' if len(name) > 20 else ''}\n"
            top_users_text += f"   👤 ID: {user_id} | 🔢 {interactions} int | 📁 {files_accessed} files\n"
            top_users_text += f"   🕐 Last active: {last_active_str}\n"

        growth_text = ""
        for date_str, count in stats_data['growth_data'][:7]:
            growth_text += f"📅 {date_str}: +{count} users\n"

        message = (
            f"📊 *USER STATISTICS*\n\n"
            f"👥 *Total Users:* {stats_data['total_users']}\n"
            f"🟢 *Active (7 days):* {stats_data['active_users_7d']}\n"
            f"🟡 *Active (30 days):* {stats_data['active_users_30d']}\n"
            f"📈 *New Today:* {stats_data['new_users_today']}\n"
            f"📈 *New This Week:* {stats_data['new_users_week']}\n"
            f"📁 *Users Who Accessed Files:* {stats_data['users_with_files']}\n\n"
            f"🏆 *TOP 10 USERS BY INTERACTIONS:*\n{top_users_text}\n"
            f"📈 *RECENT GROWTH (Last 7 days):*\n{growth_text}\n"
            f"💡 *Tip:* Use /broadcast to message all users"
        )

        sent_msg = await update.message.reply_text(message, parse_mode="Markdown")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.error(f"Error in users command: {e}")
        sent_msg = await update.message.reply_text(f"❌ Error getting user statistics: {str(e)[:200]}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast message to all users"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args and not (update.message.reply_to_message and update.message.reply_to_message.text):
        sent_msg = await update.message.reply_text(
            "📢 *Broadcast Message to All Users*\n\n"
            "*Usage:*\n"
            "1. `/broadcast your message here`\n"
            "2. Reply to a message with `/broadcast`\n\n"
            "*Options:*\n"
            "`/broadcast -silent your message` - Send silently\n"
            "`/broadcast -test your message` - Test with yourself only",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    args = context.args or []
    silent_mode = False
    test_mode = False

    if args and args[0] in ['-silent', '-s']:
        silent_mode = True
        args = args[1:]
    elif args and args[0] in ['-test', '-t']:
        test_mode = True
        args = args[1:]

    if update.message.reply_to_message:
        if update.message.reply_to_message.text:
            message_text = update.message.reply_to_message.text
        elif update.message.reply_to_message.caption:
            message_text = update.message.reply_to_message.caption
        else:
            sent_msg = await update.message.reply_text("❌ Replied message must have text or caption")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
    else:
        message_text = " ".join(args)

    if not message_text.strip():
        sent_msg = await update.message.reply_text("❌ Message cannot be empty")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    if not silent_mode:
        message_text = f"📢 *Broadcast from @{bot_username}*\n\n{message_text}"

    if test_mode:
        user_ids = [update.effective_user.id]
        sent_msg = await update.message.reply_text("🔄 *TEST MODE:* Sending to yourself only...", parse_mode="Markdown")
    else:
        user_ids = db.get_all_user_ids(exclude_admin=True)
        sent_msg = await update.message.reply_text(
            f"🔄 *BROADCAST STARTED*\n\n"
            f"📤 Sending to: {len(user_ids)} users\n"
            f"📝 Message length: {len(message_text)} chars\n"
            f"⏳ Please wait...",
            parse_mode="Markdown"
        )

    status_msg = sent_msg
    total_users = len(user_ids)
    successful = 0
    failed = 0
    blocked = 0

    for i, user_id in enumerate(user_ids):
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=message_text,
                parse_mode="Markdown" if not silent_mode else None,
                disable_notification=silent_mode
            )
            successful += 1

            if (i + 1) % 20 == 0 and not test_mode:
                try:
                    await status_msg.edit_text(
                        f"🔄 *BROADCAST PROGRESS*\n\n"
                        f"✅ Successful: {successful}\n"
                        f"❌ Failed: {failed}\n"
                        f"🚫 Blocked: {blocked}\n"
                        f"📤 Total: {i+1}/{total_users}\n"
                        f"⏳ {((i+1)/total_users*100):.1f}% complete",
                        parse_mode="Markdown"
                    )
                except:
                    pass

            await asyncio.sleep(0.1)

        except Exception as e:
            error_msg = str(e).lower()
            if "blocked" in error_msg or "forbidden" in error_msg:
                blocked += 1
            else:
                failed += 1
            log.warning(f"Failed to send broadcast to {user_id}: {e}")

    report = (
        f"✅ *BROADCAST COMPLETED*\n\n"
        f"📊 *Statistics:*\n"
        f"✅ Successful: {successful}\n"
        f"❌ Failed: {failed}\n"
        f"🚫 Blocked/Deleted: {blocked}\n"
        f"📤 Total Attempted: {total_users}\n\n"
    )

    if test_mode:
        report += f"🔧 *Test Mode:* Only sent to yourself\n"
    elif silent_mode:
        report += f"🔕 *Silent Mode:* No notification sound\n"

    report += f"\n📝 *Message Preview:*\n{message_text[:200]}{'...' if len(message_text) > 200 else ''}"

    await status_msg.edit_text(report, parse_mode="Markdown")
    await schedule_message_deletion(context, status_msg.chat_id, status_msg.message_id)

# ============ USER COMMANDS ============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
                f"⚠️ *Note:* All bot messages auto-delete after {DELETE_AFTER//60} minutes\n"
                "💾 *Storage:* Files are stored permanently on persistent disk",
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

        db.update_user_interaction(user_id=user_id, file_accessed=True)

        try:
            filename = file_info['file_name']
            ext = filename.lower().split('.')[-1] if '.' in filename else ""

            warning_msg = f"\n\n⚠️ *This message will auto-delete in {DELETE_AFTER//60} minutes*\n"
            warning_msg += f"📤 *Forward to saved messages to keep it*\n"
            warning_msg += f"💾 *File is stored permanently on persistent disk*"

            if file_info['is_video'] and ext in PLAYABLE_EXTS:
                sent = await context.bot.send_video(
                    chat_id=chat_id,
                    video=file_info["file_id"],
                    caption=f"🎬 *{filename}*\n📥 Accessed {file_info.get('access_count', 0)} times{warning_msg}",
                    parse_mode="Markdown",
                    supports_streaming=True
                )
            else:
                sent = await context.bot.send_document(
                    chat_id=chat_id,
                    document=file_info["file_id"],
                    caption=f"📁 *{filename}*\n📥 Accessed {file_info.get('access_count', 0)} times{warning_msg}",
                    parse_mode="Markdown"
                )

            await schedule_message_deletion(context, sent.chat_id, sent.message_id)

        except Exception as e:
            log.error(f"Error sending file: {e}", exc_info=True)

            error_msg = str(e).lower()

            if "file is too big" in error_msg or "too large" in error_msg:
                sent_msg = await update.message.reply_text("❌ File is too large. Maximum size is 50MB for videos.")
            elif "file not found" in error_msg or "invalid file id" in error_msg:
                sent_msg = await update.message.reply_text("❌ File expired from Telegram servers. Please contact admin.")
            elif "forbidden" in error_msg:
                sent_msg = await update.message.reply_text("❌ Bot can't send messages here.")
            else:
                sent_msg = await update.message.reply_text("❌ Failed to send file. Please try again.")

            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            log.error(f"File send failed for {key}: {traceback.format_exc()}")

    except Exception as e:
        log.error(f"Start error: {e}", exc_info=True)
        if update.message:
            sent_msg = await update.message.reply_text("❌ Error processing request")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle check membership callback"""
    try:
        query = update.callback_query
        if not query:
            return
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
                    f"✅ *Great! You've joined both channels!*\n\n"
                    "Now you can use file links shared by the admin.\n"
                    f"⚠️ *Note:* All bot messages auto-delete after {DELETE_AFTER//60} minutes\n"
                    "💾 *Storage:* Files are stored permanently on persistent disk",
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
            data_parts = data.split("|")
            if len(data_parts) != 2:
                return

            _, key = data_parts

            try:
                file_info = db.get_file(key)
                if not file_info:
                    await query.edit_message_text("❌ File not found. It may have been manually deleted by admin.")
                    return
            except Exception as e:
                log.error(f"Database error when getting file {key}: {e}")
                await query.edit_message_text("❌ Error accessing file. Please try again.")
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

            db.update_user_interaction(user_id=user_id, file_accessed=True)

            try:
                filename = file_info.get('file_name', 'file')
                ext = filename.lower().split('.')[-1] if '.' in filename else ""

                warning_msg = f"\n\n⚠️ *This message will auto-delete in {DELETE_AFTER//60} minutes*\n"
                warning_msg += f"📤 *Forward to saved messages to keep it*\n"
                warning_msg += f"💾 *File is stored permanently on persistent disk*"

                chat_id = query.message.chat_id

                if file_info['is_video'] and ext in PLAYABLE_EXTS:
                    sent_msg = await context.bot.send_video(
                        chat_id=chat_id,
                        video=file_info["file_id"],
                        caption=f"🎬 *{filename}*\n📥 Accessed {file_info.get('access_count', 0)} times{warning_msg}",
                        parse_mode="Markdown",
                        supports_streaming=True
                    )
                else:
                    sent_msg = await context.bot.send_document(
                        chat_id=chat_id,
                        document=file_info["file_id"],
                        caption=f"📁 *{filename}*\n📥 Accessed {file_info.get('access_count', 0)} times{warning_msg}",
                        parse_mode="Markdown"
                    )

                await query.edit_message_text("✅ *Access granted! File sent below.*", parse_mode="Markdown")
                await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

            except Exception as e:
                log.error(f"Failed to send file in callback: {e}", exc_info=True)
                error_msg = str(e).lower()

                if "file is too big" in error_msg or "too large" in error_msg:
                    await query.edit_message_text("❌ File is too large (max 50MB).")
                elif "file not found" in error_msg or "invalid file id" in error_msg:
                    await query.edit_message_text("❌ File expired from Telegram servers.")
                elif "forbidden" in error_msg:
                    await query.edit_message_text("❌ Bot can't send files here.")
                else:
                    await query.edit_message_text("❌ Failed to send file. Please try again.")

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
            sent_msg = await msg.reply_text("❌ Please send a video or document")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        file_info = {
            "file_name": filename,
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
            f"⏰ *Message auto-delete:* {DELETE_AFTER//60} minutes\n"
            f"💾 *Storage:* PERMANENT on persistent disk\n\n"
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
        application.job_queue.run_repeating(
            cleanup_overdue_messages,
            interval=300,
            first=10
        )
        print("🟢 Periodic message cleanup scheduled")
    else:
        print("⚠️ Job queue not available - auto-delete feature will not work")

    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("cleanup", cleanup))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("clearcache", clearcache))
    application.add_handler(CommandHandler("testchannel", testchannel))
    application.add_handler(CommandHandler("listfiles", listfiles))
    application.add_handler(CommandHandler("deletefile", deletefile))
    application.add_handler(CommandHandler("users", users))
    application.add_handler(CommandHandler("broadcast", broadcast))

    application.add_handler(CallbackQueryHandler(check_join, pattern=r"^check_membership$"))
    application.add_handler(CallbackQueryHandler(check_join, pattern=r"^check\|"))

    upload_filter = filters.VIDEO | filters.Document.ALL
    application.add_handler(
        MessageHandler(upload_filter & filters.User(ADMIN_ID) & filters.ChatType.PRIVATE, upload)
    )

    print("\n" + "=" * 60)
    print("🤖 TELEGRAM FILE BOT - PERSISTENT DISK STORAGE (OPTIMIZED)")
    print("=" * 60)
    print(f"🟢 Bot username: @{bot_username}")
    print(f"🟢 Admin ID: {ADMIN_ID}")
    print(f"🟢 Channels: @{CHANNEL_1}, @{CHANNEL_2}")
    print(f"🟢 Message auto-delete: {DELETE_AFTER//60} minutes")
    print(f"🟢 Database path: {DB_PATH}")
    print(f"🟢 Database exists: {DB_PATH.exists()}")
    print(f"🟢 Storage: PERSISTENT DISK - SURVIVES RESTARTS!")
    print("🟢 Schema version: v2 (optimized, integer timestamps, channel IDs, no mime)")
    print("=" * 60)

    try:
        file_count = db.get_file_count()
        user_count = db.get_user_count()
        print(f"📊 Files in database: {file_count}")
        print(f"👥 Users in database: {user_count}")
    except Exception as e:
        print(f"⚠️ Database check failed: {e}")

    db.clear_membership_cache()

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )

def main():
    if not BOT_TOKEN:
        print("❌ ERROR: BOT_TOKEN is not set!")
        return

    if not ADMIN_ID or ADMIN_ID == 0:
        print("❌ ERROR: ADMIN_ID is not set or invalid!")
        return

    print("\n🟢 Starting Flask web dashboard...")
    flask_thread = threading.Thread(target=run_flask_thread, daemon=True)
    flask_thread.start()
    time.sleep(1)
    print(f"🟢 Flask running on port {os.environ.get('PORT', 10000)}")

    start_bot()

if __name__ == "__main__":
    main()
    
