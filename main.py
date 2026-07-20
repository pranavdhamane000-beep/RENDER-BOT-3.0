import asyncio
import json
import logging
import os
import sys
import time
import traceback
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
import threading
import psycopg2
import psycopg2.extras
from psycopg2 import pool, sql
from contextlib import asynccontextmanager
import urllib.parse
import csv
import io
import html

# ================= HEALTH SERVER FOR RENDER =================
from flask import Flask, render_template_string, jsonify, request
app = Flask(__name__)

# Global variables for web dashboard
start_time = time.time()
bot_username = "xaiomovie_bot"
# Global variable to store bot application instance for webhook
bot_app = None
bot_loop = None
bot_initialized = False
BOT_INIT_WAIT_SECONDS = 25
WEBHOOK_PROCESS_TIMEOUT_SECONDS = float(os.environ.get("WEBHOOK_PROCESS_TIMEOUT_SECONDS", "1"))
BOT_READY_POLL_INTERVAL_SECONDS = 0.1
KEEPALIVE_INTERVAL_SECONDS = int(os.environ.get("KEEPALIVE_INTERVAL_SECONDS", "240"))
BLOCK_ON_CHANNEL_VERIFY_ERROR = os.environ.get("BLOCK_ON_CHANNEL_VERIFY_ERROR", "false").lower() in {"1", "true", "yes", "on"}

def normalize_channel_username(value: Any) -> str:
    """Normalize a channel identifier to a plain Telegram username."""
    text = str(value or "").strip()
    if not text:
        return ""

    text = text.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
    text = text.lstrip("@").strip()
    text = text.split("?", 1)[0].split("#", 1)[0].strip()

    if text.startswith("c/"):
        parts = text.split("/")
        if len(parts) > 1 and parts[1].isdigit():
            return f"-100{parts[1]}"

    if text.startswith("+"):
        return ""

    return text


def extract_chat_from_forward(forwarded: Any) -> Optional[Any]:
    """Helper to safely extract the source chat from a forwarded message across API versions."""
    if hasattr(forwarded, 'forward_origin') and forwarded.forward_origin:
        origin = forwarded.forward_origin
        if hasattr(origin, 'chat'):
            return origin.chat
        if hasattr(origin, 'sender_chat'):
            return origin.sender_chat
    if getattr(forwarded, 'forward_from_chat', None):
        return forwarded.forward_from_chat
    if getattr(forwarded, 'sender_chat', None):
        return forwarded.sender_chat
    return None

def escape_markdown(text: str) -> str:
    """Escape markdown characters for Telegram v1."""
    if not text:
        return ""
    for char in ['_', '*', '[', ']', '`']:
        text = text.replace(char, f'\\{char}')
    return text


def telegram_chat_ref(channel: Any):
    """Return a Telegram API chat reference for a username or numeric chat id."""
    clean_channel = normalize_channel_username(channel)
    if re.fullmatch(r"-?\d+", clean_channel):
        return int(clean_channel)
    return f"@{clean_channel}" if clean_channel else None

# ===========================================================
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMemberUpdated
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    JobQueue,
    ChatMemberHandler
)
from telegram.request import HTTPXRequest
from telegram.error import RetryAfter, TimedOut, NetworkError

# Global semaphore to limit concurrent Telegram API calls (prevents flood errors)
_telegram_semaphore = asyncio.Semaphore(10)

# ================= CONFIG =================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))

# Default channels (will be added to database on first run)
DEFAULT_CHANNELS = [
    normalize_channel_username(os.environ.get("CHANNEL_1", "A_Knight_of_the_Seven_Kingdoms_t")),
    normalize_channel_username(os.environ.get("CHANNEL_2", "your_movies_web"))
]

# ============ RENDER POSTGRESQL WITH PSYCOPG2 ============
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL is not set!")
    print("💡 Add a PostgreSQL database in Render Dashboard and copy its Internal Database URL")
    raise ValueError("DATABASE_URL environment variable is required!")

DELETE_AFTER = 600  # 10 minutes
LISTFILES_MESSAGE_LIMIT = 3600
UPLOAD_BATCH_WINDOW_SECONDS = float(os.environ.get("UPLOAD_BATCH_WINDOW_SECONDS", "3"))
BROADCAST_CHUNK_SIZE = 1000
BROADCAST_MAX_BUTTONS = 20
BROADCAST_TEXT_LIMIT = 4096
BROADCAST_PHOTO_CAPTION_LIMIT = 1024
MAX_STORED_FILES = 10000
AUTO_CLEANUP_DAYS = 0  # DISABLED - No auto cleanup

# Playable formats
PLAYABLE_EXTS = {"mp4", "mov", "m4v", "mpeg", "mpg"}

# All video extensions
ALL_VIDEO_EXTS = {
    "mp4", "mkv", "mov", "avi", "webm", "flv", "m4v",
    "3gp", "wmv", "mpg", "mpeg"
}

# Friendly channel names (for UI) - You can customize these!
CHANNEL_NAMES = {
    "A_Knight_of_the_Seven_Kingdoms_t": "Channel 1",
    "A_Knight_of_the_Seven_Kingdoms_r": "Main Channel",
    "A_Knight_of_the_Seven_Kingdoms_y": "Backup Channel",
    "your_movies_web": "Movies Channel",
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
        self.pool = None
        self._pool_initialized = False
        log.info(f"📀 Connecting to Render PostgreSQL with psycopg2...")

    def _get_pool_sync(self):
        """Synchronous pool initialization - called only once"""
        if self.pool is None:
            result = urllib.parse.urlparse(self.db_url)
            user = result.username
            password = urllib.parse.unquote(result.password) if result.password else ''
            database = result.path[1:]
            host = result.hostname
            port = result.port or 5432

            dsn = f"dbname='{database}' user='{user}' password='{password}' host='{host}' port='{port}'"
            log.info(f"🔌 Creating connection pool to Render PostgreSQL at {host}:{port}/{database}")

            try:
                self.pool = psycopg2.pool.ThreadedConnectionPool(
                    1, 50, dsn=dsn, connect_timeout=30,
                    sslmode='require'
                )
                log.info("✅ Render PostgreSQL connection pool created (SSL enabled)")

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

    def _get_valid_connection_sync(self):
        """Get a live connection, replacing stale idle connections if needed."""
        pool = self._get_pool_sync()
        last_error = None

        for attempt in range(3):
            conn = pool.getconn()
            try:
                if conn.closed:
                    raise psycopg2.InterfaceError("Connection is already closed")

                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                conn.rollback()
                return conn
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last_error = e
                log.warning(f"Discarding stale PostgreSQL connection from pool (attempt {attempt + 1}/3): {e}")
                try:
                    pool.putconn(conn, close=True)
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass

        raise last_error or psycopg2.OperationalError("Unable to get a live PostgreSQL connection")

    def _init_db(self, conn, cur):
        """Initialize database tables (synchronous)"""
        # Files table
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
            CREATE TABLE IF NOT EXISTS file_groups (
                id SERIAL PRIMARY KEY,
                title TEXT,
                file_count INTEGER DEFAULT 0,
                total_size BIGINT DEFAULT 0,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                access_count INTEGER DEFAULT 0
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS file_group_items (
                group_id INTEGER NOT NULL REFERENCES file_groups(id) ON DELETE CASCADE,
                file_db_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                position INTEGER NOT NULL,
                PRIMARY KEY (group_id, position)
            )
        ''')
        
        # Membership cache table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS membership_cache (
                user_id BIGINT,
                channel TEXT,
                is_member INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, channel)
            )
        ''')
        
        # Scheduled deletions table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_deletions (
                chat_id BIGINT NOT NULL,
                message_id INTEGER NOT NULL,
                scheduled_time TIMESTAMP NOT NULL,
                delete_after INTEGER DEFAULT 600,
                PRIMARY KEY (chat_id, message_id)
            )
        ''')
        
        # Users table
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
        
        # Required channels table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS required_channels (
                id SERIAL PRIMARY KEY,
                channel_username TEXT UNIQUE NOT NULL,
                channel_name TEXT,
                channel_type TEXT DEFAULT 'public',
                invite_link TEXT,
                added_by BIGINT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                position INTEGER DEFAULT 0
            )
        ''')
        
        # Private channel requests table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS private_channel_requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                channel_id INTEGER NOT NULL REFERENCES required_channels(id) ON DELETE CASCADE,
                file_key TEXT NOT NULL,
                requested BOOLEAN DEFAULT TRUE,
                requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, channel_id)
            )
        ''')
        
        # Pending file delivery table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS pending_file_delivery (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                file_key TEXT NOT NULL,
                missing_channels TEXT[],
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, file_key)
            )
        ''')
        
        # Create indexes
        cur.execute('CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_file_groups_timestamp ON file_groups(timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_file_group_items_group ON file_group_items(group_id, position)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_file_group_items_file ON file_group_items(file_db_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON membership_cache(timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_deletions_time ON scheduled_deletions(scheduled_time)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_users_first_seen ON users(first_seen)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_channels_active ON required_channels(is_active)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_private_requests_user ON private_channel_requests(user_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_pending_delivery_user ON pending_file_delivery(user_id)')
        
        # ---- Migrations for older databases ----
        # Add missing columns to required_channels if they don't exist
        for col_name, col_def in [
            ('channel_type', "TEXT DEFAULT 'public'"),
            ('invite_link', 'TEXT'),
            ('position', 'INTEGER DEFAULT 0'),
            ('added_by', 'BIGINT'),
            ('added_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'),
        ]:
            cur.execute('''
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'required_channels' AND column_name = %s
            ''', (col_name,))
            if not cur.fetchone():
                cur.execute(f'ALTER TABLE required_channels ADD COLUMN {col_name} {col_def}')
                log.info(f"Migration: added column '{col_name}' to required_channels")
        
        # Insert default channels if table is empty
        cur.execute("SELECT COUNT(*) FROM required_channels")
        count = cur.fetchone()[0]
        
        if count == 0 and DEFAULT_CHANNELS:
            for i, channel in enumerate(DEFAULT_CHANNELS):
                if channel:
                    friendly_name = CHANNEL_NAMES.get(channel, f"Channel {i+1}")
                    cur.execute('''
                        INSERT INTO required_channels (channel_username, channel_name, channel_type, position, is_active)
                        VALUES (%s, %s, 'public', %s, 1)
                        ON CONFLICT (channel_username) DO NOTHING
                    ''', (channel, friendly_name, i))
                    log.info(f"Added default channel: {channel} as '{friendly_name}'")
        
        conn.commit()

    @asynccontextmanager
    async def get_db_connection(self):
        """Asynchronous context manager to get and return a connection from the pool."""
        await self._get_pool_async()
        
        conn = None
        for attempt in range(15):
            try:
                conn = await asyncio.to_thread(self._get_valid_connection_sync)
                break
            except pool.PoolError:
                if attempt == 14:
                    log.error("Database connection pool exhausted after multiple retries.")
                    raise
                await asyncio.sleep(0.5)
                
        try:
            yield conn
        finally:
            if conn is not None:
                await asyncio.to_thread(self.pool.putconn, conn)

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

    # ============ Get database storage usage ============
    async def get_db_storage_usage(self) -> Dict[str, Any]:
        """Get PostgreSQL database storage usage"""
        try:
            result = await self.fetchrow('''
                SELECT 
                    pg_database_size(current_database()) as total_bytes,
                    (SELECT COALESCE(SUM(pg_total_relation_size(relid)), 0) 
                     FROM pg_stat_user_tables) as table_bytes,
                    (SELECT COALESCE(SUM(pg_indexes_size(relid)), 0) 
                     FROM pg_stat_user_tables) as index_bytes
            ''')
            
            if result:
                total_bytes = result['total_bytes'] or 0
                table_bytes = result['table_bytes'] or 0
                index_bytes = result['index_bytes'] or 0
                
                def format_bytes(bytes_val):
                    if bytes_val < 1024:
                        return f"{bytes_val} B"
                    elif bytes_val < 1024 * 1024:
                        return f"{bytes_val/1024:.2f} KB"
                    elif bytes_val < 1024 * 1024 * 1024:
                        return f"{bytes_val/(1024*1024):.2f} MB"
                    else:
                        return f"{bytes_val/(1024*1024*1024):.2f} GB"
                
                return {
                    'total': format_bytes(total_bytes),
                    'total_bytes': total_bytes,
                    'tables': format_bytes(table_bytes),
                    'indexes': format_bytes(index_bytes),
                    'tables_bytes': table_bytes,
                    'indexes_bytes': index_bytes
                }
        except Exception as e:
            log.error(f"Error getting DB storage: {e}")
        
        return {
            'total': 'Unknown',
            'total_bytes': 0,
            'tables': 'Unknown',
            'indexes': 'Unknown'
        }

    # ============ Get metadata storage info ============
    async def get_metadata_storage_info(self) -> Dict[str, Any]:
        """Get detailed metadata storage info"""
        try:
            files_count = await self.get_file_count()
            users_count = await self.get_user_count()
            
            cache_result = await self.fetchrow("SELECT COUNT(*) as count FROM membership_cache")
            cache_count = cache_result['count'] if cache_result else 0
            
            channels_count = await self.get_channel_count()
            
            estimated_metadata_bytes = (files_count * 200) + (users_count * 150) + (cache_count * 50) + (channels_count * 100)
            
            def format_bytes(bytes_val):
                if bytes_val < 1024:
                    return f"{bytes_val} B"
                elif bytes_val < 1024 * 1024:
                    return f"{bytes_val/1024:.2f} KB"
                else:
                    return f"{bytes_val/(1024*1024):.2f} MB"
            
            return {
                'files_count': files_count,
                'users_count': users_count,
                'cache_entries': cache_count,
                'channels_count': channels_count,
                'estimated_metadata': format_bytes(estimated_metadata_bytes),
                'estimated_bytes': estimated_metadata_bytes
            }
        except Exception as e:
            log.error(f"Error getting metadata info: {e}")
            return {
                'files_count': 0,
                'users_count': 0,
                'cache_entries': 0,
                'channels_count': 0,
                'estimated_metadata': 'Unknown',
                'estimated_bytes': 0
            }

    # ============ Get total size of files uploaded ============
    async def get_total_uploaded_size(self) -> int:
        """Get total size of all files uploaded"""
        result = await self.fetchrow("SELECT COALESCE(SUM(file_size), 0) as total FROM files")
        return result['total'] if result else 0

    # ============ Channel management methods ============
    async def get_required_channels(self, active_only: bool = True) -> List[Dict]:
        """Get list of all required channels with details"""
        if active_only:
            rows = await self.fetchall("SELECT * FROM required_channels WHERE is_active = 1 ORDER BY position, id")
        else:
            rows = await self.fetchall("SELECT * FROM required_channels ORDER BY position, id")
        
        return [dict(row) for row in rows]
    
    async def get_channels_with_details(self) -> List[Dict]:
        """Get channels with all details for listing"""
        rows = await self.fetchall('''
            SELECT id, channel_username, channel_name, channel_type, invite_link, added_at, is_active, position
            FROM required_channels
            ORDER BY position, id
        ''')
        return [dict(row) for row in rows]
    
    async def add_channel(self, channel_username: str, added_by: int, channel_name: str = None, 
                          channel_type: str = 'public', invite_link: str = None) -> bool:
        """Add a new required channel"""
        clean_username = normalize_channel_username(channel_username)
        
        if not clean_username:
            return False
        
        friendly_name = channel_name or CHANNEL_NAMES.get(clean_username, clean_username)
        
        result = await self.fetchrow("SELECT COALESCE(MAX(position), -1) + 1 as next_pos FROM required_channels")
        next_pos = result['next_pos'] if result else 0
        
        try:
            await self.execute_and_commit('''
                INSERT INTO required_channels (channel_username, channel_name, channel_type, invite_link, added_by, position, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, 1)
                ON CONFLICT (channel_username) DO UPDATE
                SET is_active = 1,
                    channel_type = COALESCE(EXCLUDED.channel_type, required_channels.channel_type),
                    invite_link = COALESCE(EXCLUDED.invite_link, required_channels.invite_link),
                    added_by = EXCLUDED.added_by,
                    channel_name = COALESCE(EXCLUDED.channel_name, required_channels.channel_name)
            ''', (clean_username, friendly_name, channel_type, invite_link, added_by, next_pos))
            
            log.info(f"Channel added: @{clean_username} as '{friendly_name}' (type: {channel_type}) by user {added_by}")
            return True
        except Exception as e:
            log.error(f"Error adding channel: {e}")
            return False
    
    async def remove_channel(self, channel_username: str) -> bool:
        """Remove a required channel (soft delete by setting inactive)"""
        clean_username = normalize_channel_username(channel_username)
        
        rowcount = await self.execute_and_commit('''
            UPDATE required_channels SET is_active = 0
            WHERE channel_username = %s
        ''', (clean_username,))
        
        if rowcount > 0:
            log.info(f"Channel removed: @{clean_username}")
            await self.execute_and_commit("DELETE FROM membership_cache WHERE channel = %s", (clean_username,))
            return True
        return False
    
    async def update_channel_name(self, channel_username: str, new_name: str) -> bool:
        """Update friendly name for a channel"""
        clean_username = normalize_channel_username(channel_username)
        
        rowcount = await self.execute_and_commit('''
            UPDATE required_channels SET channel_name = %s
            WHERE channel_username = %s
        ''', (new_name, clean_username))
        
        return rowcount > 0
    
    async def get_channel_count(self) -> int:
        """Get number of active required channels"""
        result = await self.fetchrow("SELECT COUNT(*) as count FROM required_channels WHERE is_active = 1")
        return result['count'] if result else 0

    # ============ Private channel request methods ============
    async def add_private_request(self, user_id: int, channel_id: int, file_key: str) -> bool:
        """Add a private channel request for a user"""
        try:
            await self.execute_and_commit('''
                INSERT INTO private_channel_requests (user_id, channel_id, file_key, requested)
                VALUES (%s, %s, %s, TRUE)
                ON CONFLICT (user_id, channel_id) DO UPDATE
                SET requested = TRUE, file_key = EXCLUDED.file_key
            ''', (user_id, channel_id, file_key))
            return True
        except Exception as e:
            log.error(f"Error adding private request: {e}")
            return False
    
    async def has_private_request(self, user_id: int, channel_id: int) -> bool:
        """Check if user has requested a private channel"""
        result = await self.fetchrow('''
            SELECT requested FROM private_channel_requests
            WHERE user_id = %s AND channel_id = %s
        ''', (user_id, channel_id))
        return bool(result['requested']) if result else False
    
    async def reset_private_request(self, user_id: int, channel_id: int) -> bool:
        """Reset a private channel request"""
        rowcount = await self.execute_and_commit('''
            DELETE FROM private_channel_requests
            WHERE user_id = %s AND channel_id = %s
        ''', (user_id, channel_id))
        return rowcount > 0
    
    async def get_pending_requests_for_channel(self, channel_id: int) -> List[Dict]:
        """Get all pending requests for a specific channel"""
        rows = await self.fetchall('''
            SELECT user_id, file_key FROM private_channel_requests
            WHERE channel_id = %s AND requested = TRUE
        ''', (channel_id,))
        return [dict(row) for row in rows]
    
    async def clear_user_requests(self, user_id: int, channel_id: int = None):
        """Clear requests for a user"""
        if channel_id:
            await self.execute_and_commit('''
                DELETE FROM private_channel_requests
                WHERE user_id = %s AND channel_id = %s
            ''', (user_id, channel_id))
        else:
            await self.execute_and_commit('''
                DELETE FROM private_channel_requests
                WHERE user_id = %s
            ''', (user_id,))

    # ============ Pending file delivery methods ============
    async def add_pending_delivery(self, user_id: int, file_key: str, missing_channels: List[str]):
        """Add a pending file delivery for a user"""
        try:
            await self.execute_and_commit('''
                INSERT INTO pending_file_delivery (user_id, file_key, missing_channels)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, file_key) DO UPDATE
                SET missing_channels = EXCLUDED.missing_channels,
                    created_at = CURRENT_TIMESTAMP
            ''', (user_id, file_key, missing_channels))
        except Exception as e:
            log.error(f"Error adding pending delivery: {e}")
    
    async def get_pending_deliveries(self, user_id: int) -> List[Dict]:
        """Get all pending deliveries for a user"""
        rows = await self.fetchall('''
            SELECT file_key, missing_channels FROM pending_file_delivery
            WHERE user_id = %s
            ORDER BY created_at DESC
        ''', (user_id,))
        return [dict(row) for row in rows]

    async def get_pending_deliveries_for_channels(self, channels: List[str]) -> List[Dict]:
        """Get pending deliveries waiting on any of the given channel identifiers."""
        clean_channels = [normalize_channel_username(ch) for ch in channels if normalize_channel_username(ch)]
        if not clean_channels:
            return []

        rows = await self.fetchall('''
            SELECT DISTINCT user_id, file_key, missing_channels
            FROM pending_file_delivery
            WHERE missing_channels && %s::text[]
            ORDER BY user_id
        ''', (clean_channels,))
        return [dict(row) for row in rows]
    
    async def remove_pending_delivery(self, user_id: int, file_key: str = None):
        """Remove pending delivery for a user"""
        if file_key:
            await self.execute_and_commit('''
                DELETE FROM pending_file_delivery
                WHERE user_id = %s AND file_key = %s
            ''', (user_id, file_key))
        else:
            await self.execute_and_commit('''
                DELETE FROM pending_file_delivery
                WHERE user_id = %s
            ''', (user_id,))
    
    async def get_file_key_for_pending(self, user_id: int, channel_username: str) -> Optional[str]:
        """Get the file key for a user who joined a channel"""
        rows = await self.fetchall('''
            SELECT file_key FROM pending_file_delivery
            WHERE user_id = %s AND %s = ANY(missing_channels)
        ''', (user_id, channel_username))
        return rows[0]['file_key'] if rows else None

    # ============ Existing database methods ============
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

    async def save_file_group(self, files_info: List[dict]) -> str:
        """Save multiple file records under one shareable group key."""
        if not files_info:
            raise ValueError("Cannot save an empty file group")

        async with self.get_db_connection() as conn:
            def _save_group():
                try:
                    with conn.cursor() as cur:
                        title = files_info[0].get('file_name', 'File group')
                        total_size = sum(int(item.get('size') or 0) for item in files_info)

                        cur.execute('''
                            INSERT INTO file_groups (title, file_count, total_size, access_count)
                            VALUES (%s, %s, %s, 0)
                            RETURNING id
                        ''', (title, len(files_info), total_size))
                        group_id = cur.fetchone()[0]

                        for position, item in enumerate(files_info, start=1):
                            cur.execute('''
                                INSERT INTO files
                                (file_id, file_name, mime_type, is_video, file_size, access_count)
                                VALUES (%s, %s, %s, %s, %s, 0)
                                RETURNING id
                            ''', (
                                item.get('file_id', ''),
                                item.get('file_name', ''),
                                item.get('mime_type', ''),
                                1 if item.get('is_video', False) else 0,
                                int(item.get('size') or 0)
                            ))
                            file_db_id = cur.fetchone()[0]

                            cur.execute('''
                                INSERT INTO file_group_items (group_id, file_db_id, position)
                                VALUES (%s, %s, %s)
                            ''', (group_id, file_db_id, position))

                        conn.commit()
                        log.info(f"Saved file group {group_id} with {len(files_info)} files")
                        return str(group_id)
                except Exception:
                    conn.rollback()
                    raise

            return await asyncio.to_thread(_save_group)

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

    async def get_file_group(self, group_key: str) -> Optional[dict]:
        """Get grouped file info by a g_<id> key and increment access counters."""
        normalized_key = str(group_key or "").strip().lower()
        if normalized_key.startswith("g_"):
            normalized_key = normalized_key[2:]
        elif normalized_key.startswith("group_"):
            normalized_key = normalized_key[6:]

        try:
            group_id_int = int(normalized_key)
        except ValueError:
            return None

        async with self.get_db_connection() as conn:
            def _get_group():
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute('''
                        UPDATE file_groups
                        SET access_count = access_count + 1
                        WHERE id = %s
                        RETURNING id, title, file_count, total_size,
                                  TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as timestamp,
                                  access_count
                    ''', (group_id_int,))
                    group_row = cur.fetchone()
                    if not group_row:
                        return None

                    cur.execute('''
                        UPDATE files
                        SET access_count = access_count + 1
                        WHERE id IN (
                            SELECT file_db_id FROM file_group_items WHERE group_id = %s
                        )
                    ''', (group_id_int,))

                    cur.execute('''
                        SELECT f.file_id, f.file_name, f.mime_type, f.is_video,
                               f.file_size, f.access_count, i.position
                        FROM file_group_items i
                        JOIN files f ON f.id = i.file_db_id
                        WHERE i.group_id = %s
                        ORDER BY i.position
                    ''', (group_id_int,))
                    rows = cur.fetchall()
                    conn.commit()

                    group = dict(group_row)
                    group['files'] = [dict(row) for row in rows]
                    return group

            return await asyncio.to_thread(_get_group)

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

    async def clear_membership_cache(self, user_id: Optional[int] = None, channel: Optional[str] = None):
        """Clear membership cache for a user, channel, or all."""
        if user_id and channel:
            await self.execute_and_commit(
                "DELETE FROM membership_cache WHERE user_id = %s AND channel = %s",
                (user_id, channel.replace("@", ""))
            )
        elif user_id:
            await self.execute_and_commit("DELETE FROM membership_cache WHERE user_id = %s", (user_id,))
        elif channel:
            await self.execute_and_commit(
                "DELETE FROM membership_cache WHERE channel = %s",
                (channel.replace("@", ""),)
            )
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

    async def delete_file_group(self, group_key: str) -> bool:
        """Delete a file group and its member file records."""
        normalized_key = str(group_key or "").strip().lower()
        if normalized_key.startswith("g_"):
            normalized_key = normalized_key[2:]
        elif normalized_key.startswith("group_"):
            normalized_key = normalized_key[6:]

        try:
            group_id_int = int(normalized_key)
        except ValueError:
            return False

        async with self.get_db_connection() as conn:
            def _delete_group():
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT file_db_id FROM file_group_items WHERE group_id = %s",
                            (group_id_int,)
                        )
                        file_ids = [row[0] for row in cur.fetchall()]

                        cur.execute("DELETE FROM file_groups WHERE id = %s", (group_id_int,))
                        deleted_group = cur.rowcount > 0

                        if deleted_group and file_ids:
                            cur.execute("DELETE FROM files WHERE id = ANY(%s)", (file_ids,))

                        conn.commit()
                        if deleted_group:
                            log.info(f"Deleted file group g_{group_id_int}")
                        return deleted_group
                except Exception:
                    conn.rollback()
                    raise

            return await asyncio.to_thread(_delete_group)

    async def get_all_files(self) -> list:
        """Get standalone files for admin view."""
        rows = await self.fetchall('''
            SELECT id, file_name, is_video, file_size,
                   TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as timestamp,
                   access_count
            FROM files
            WHERE NOT EXISTS (
                SELECT 1 FROM file_group_items i WHERE i.file_db_id = files.id
            )
            ORDER BY timestamp DESC
        ''')
        return [(row['id'], row['file_name'], row['is_video'], row['file_size'], row['timestamp'], row['access_count']) for row in rows]

    async def get_all_file_groups(self) -> list:
        """Get all grouped file links for admin view."""
        rows = await self.fetchall('''
            SELECT id, title, file_count, total_size,
                   TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as timestamp,
                   access_count
            FROM file_groups
            ORDER BY timestamp DESC
        ''')
        return [
            (
                row['id'],
                row['title'],
                row['file_count'],
                row['total_size'],
                row['timestamp'],
                row['access_count']
            )
            for row in rows
        ]

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

    async def purge_overdue_scheduled_deletions(self) -> int:
        """Drop stale deletion jobs left over from previous deployments."""
        return await self.execute_and_commit(
            'DELETE FROM scheduled_deletions WHERE scheduled_time <= CURRENT_TIMESTAMP'
        )

    async def update_user_interaction(self, user_id: int, username: str = None,
                                    first_name: str = None, last_name: str = None,
                                    file_accessed: bool = False):
        """Update user interaction timestamp and count."""
        async with self.get_db_connection() as conn:
            def _update():
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM users WHERE user_id = %s", (user_id,))
                    exists = cur.fetchone()

                    if exists:
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

async def delete_message_after_delay(bot, chat_id: int, message_id: int):
    """Fallback deletion task for deployments without python-telegram-bot JobQueue."""
    await asyncio.sleep(DELETE_AFTER)

    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        log.info(f"Fallback deleted message {message_id}")
        await db.remove_scheduled_message(chat_id, message_id)
    except Exception as e:
        error_msg = str(e).lower()
        if "message to delete not found" in error_msg:
            await db.remove_scheduled_message(chat_id, message_id)
        elif "message can't be deleted" in error_msg:
            log.warning(f"Fallback can't delete message {message_id}")
        else:
            log.error(f"Fallback failed to delete message {message_id}: {e}")

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
        else:
            asyncio.create_task(delete_message_after_delay(context.bot, chat_id, message_id))
            log.info(f"Scheduled fallback deletion of message {message_id} in {DELETE_AFTER} seconds")
    except Exception as e:
        log.error(f"Failed to schedule deletion: {e}")

def is_group_share_key(key: str) -> bool:
    """Return True when a /start key points to a grouped upload."""
    normalized_key = str(key or "").strip().lower()
    return normalized_key.startswith("g_") or normalized_key.startswith("group_")


async def get_shared_content(key: str) -> Optional[dict]:
    """Fetch either a single file or a grouped upload for a share key."""
    if is_group_share_key(key):
        group = await db.get_file_group(key)
        if group:
            group["kind"] = "group"
        return group

    file_info = await db.get_file(key)
    if file_info:
        file_info["kind"] = "file"
    return file_info


async def send_file_record(context: ContextTypes.DEFAULT_TYPE, chat_id: int, file_info: dict, caption: str):
    """Send one stored Telegram file as video when playable, otherwise document.
    
    Includes retry logic for Telegram rate limits (RetryAfter), timeouts, and
    network errors. Uses a global semaphore to limit concurrent API calls.
    """
    filename = file_info.get('file_name', 'file')
    ext = filename.lower().split('.')[-1] if '.' in filename else ""
    max_retries = 5

    for attempt in range(max_retries):
        try:
            async with _telegram_semaphore:
                if file_info.get('is_video') and ext in PLAYABLE_EXTS:
                    return await context.bot.send_video(
                        chat_id=chat_id,
                        video=file_info["file_id"],
                        caption=caption,
                        parse_mode="Markdown",
                        supports_streaming=True
                    )

                return await context.bot.send_document(
                    chat_id=chat_id,
                    document=file_info["file_id"],
                    caption=caption,
                    parse_mode="Markdown"
                )

        except RetryAfter as e:
            wait = e.retry_after + 1
            log.warning(f"Rate limited sending file to {chat_id}. Waiting {wait}s (attempt {attempt+1}/{max_retries})")
            await asyncio.sleep(wait)
        except TimedOut:
            wait = 2 * (attempt + 1)
            log.warning(f"Timeout sending file to {chat_id}. Retrying in {wait}s (attempt {attempt+1}/{max_retries})")
            await asyncio.sleep(wait)
        except NetworkError as e:
            if attempt == max_retries - 1:
                raise
            wait = 2 * (attempt + 1)
            log.warning(f"Network error sending file to {chat_id}: {e}. Retrying in {wait}s (attempt {attempt+1}/{max_retries})")
            await asyncio.sleep(wait)

    raise RuntimeError(f"Failed to send file to {chat_id} after {max_retries} retries")


async def send_shared_content(context: ContextTypes.DEFAULT_TYPE, chat_id: int, content: dict) -> List[Any]:
    """Send the file or all files represented by a share key."""
    warning = f"\n\nAuto-deletes in {DELETE_AFTER//60} minutes\nForward file to saved messages"
    sent_messages = []

    if content.get("kind") == "group":
        files = content.get("files") or []
        total = len(files)
        if total == 0:
            raise ValueError("File group has no files")

        access_count = content.get('access_count') or 0
        access_word = "time" if access_count == 1 else "times"

        for index, file_info in enumerate(files, start=1):
            filename = clean_single_line(file_info.get('file_name'), 'file')
            caption = (
                f"{index}/{total} {markdown_code(filename)}\n"
                f"Group accessed {access_count} {access_word}{warning}"
            )
            sent = await send_file_record(context, chat_id, file_info, caption)
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            sent_messages.append(sent)
            await asyncio.sleep(0.3)

        return sent_messages

    filename = clean_single_line(content.get('file_name'), 'file')
    access_count = content.get('access_count') or 0
    access_word = "time" if access_count == 1 else "times"
    caption = (
        f"{markdown_code(filename)}\n"
        f"Accessed {access_count} {access_word}{warning}"
    )
    sent = await send_file_record(context, chat_id, content, caption)
    await schedule_message_deletion(context, sent.chat_id, sent.message_id)
    return [sent]

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

async def cleanup_overdue_messages_loop(bot):
    """Fallback cleanup loop for deployments without python-telegram-bot JobQueue."""
    while True:
        try:
            due_messages = await db.get_due_messages()
            for chat_id, message_id in due_messages:
                try:
                    await bot.delete_message(chat_id=chat_id, message_id=message_id)
                    log.info(f"Fallback cleanup deleted overdue message {message_id}")
                    await db.remove_scheduled_message(chat_id, message_id)
                except Exception as e:
                    error_msg = str(e).lower()
                    if "message to delete not found" in error_msg:
                        await db.remove_scheduled_message(chat_id, message_id)
                    else:
                        log.error(f"Fallback cleanup failed for {message_id}: {e}")
        except Exception as e:
            log.error(f"Error in fallback cleanup loop: {e}")

        await asyncio.sleep(300)

# ============ DYNAMIC MEMBERSHIP CHECK ============
async def check_user_in_channel(bot, channel: str, user_id: int, force_check: bool = False) -> Tuple[bool, Optional[str]]:
    """Check if user is in channel.

    Returns a tuple of (is_member, verification_error). verification_error is
    populated when Telegram membership lookup itself fails, which is useful for
    diagnosing broken channel permissions/configuration.
    """
    clean_channel = normalize_channel_username(channel)
    if not clean_channel:
        log.error(f"Invalid channel identifier: {channel!r}")
        return False, "invalid channel identifier"
    
    if not force_check:
        cached = await db.get_cached_membership(user_id, clean_channel)
        if cached is not None:
            log.info(f"✅ CACHE HIT: User {user_id} in {clean_channel}: {cached}")
            return cached, None
        else:
            log.info(f"🔄 CACHE MISS: User {user_id} in {clean_channel}")

    try:
        channel_ref = telegram_chat_ref(clean_channel)
        if channel_ref is None:
            log.error(f"Invalid channel identifier: {channel!r}")
            return False, "invalid channel identifier"

        log.info(f"🔍 Checking user {user_id} in channel {channel_ref}")
        
        # Use semaphore + retry to handle rate limits under high concurrency
        member = None
        for attempt in range(4):
            try:
                async with _telegram_semaphore:
                    member = await bot.get_chat_member(chat_id=channel_ref, user_id=user_id)
                break
            except RetryAfter as e:
                wait = e.retry_after + 1
                log.warning(f"Rate limited checking {user_id} in {clean_channel}. Waiting {wait}s (attempt {attempt+1}/4)")
                await asyncio.sleep(wait)
            except TimedOut:
                if attempt == 3:
                    raise
                await asyncio.sleep(1 * (attempt + 1))
        
        if member is None:
            return False, "rate limited after retries"
        
        is_member = member.status in ["member", "administrator", "creator"]
        if not is_member and member.status == "restricted":
            is_member = bool(getattr(member, "is_member", False))
        log.info(f"✅ User {user_id} in {clean_channel}: {is_member} (status: {member.status})")

        await db.cache_membership(user_id, clean_channel, is_member)
        return is_member, None

    except Exception as e:
        error_msg = str(e).lower()
        log.error(f"❌ Error checking user {user_id} in {clean_channel}: {error_msg}")
        
        if "user not found" in error_msg or "user not participant" in error_msg:
            await db.cache_membership(user_id, clean_channel, False)
            return False, None
        elif "chat not found" in error_msg:
            log.error(f"Channel @{clean_channel} not found!")
            return False, error_msg
        elif "forbidden" in error_msg:
            log.error(f"Bot can't access @{clean_channel}")
            return False, error_msg
        else:
            return False, error_msg

async def check_membership(
    user_id: int,
    context: ContextTypes.DEFAULT_TYPE,
    force_check: bool = False,
    allow_private_requests: bool = False,
    prefetched_channels: list = None
) -> Dict[str, Any]:
    """Check if user is member of all required channels.
    
    Args:
        prefetched_channels: Optional pre-fetched list from get_channels_with_details()
                             to avoid a duplicate DB query.
    """
    bot = context.bot

    result = {
        "all_joined": False,
        "missing_channels": [],
        "missing_channel_names": [],
        "missing_channel_ids": [],
        "missing_channel_types": [],
        "channel_status": {},
        "verification_errors": []
    }

    if prefetched_channels is not None:
        active_channels = [c for c in prefetched_channels if c['is_active'] == 1]
    else:
        channels_data = await db.get_channels_with_details()
        active_channels = [c for c in channels_data if c['is_active'] == 1]
    
    log.info(f"📋 Found {len(active_channels)} active channels for user {user_id}")
    
    if not active_channels:
        result["all_joined"] = True
        return result

    if force_check:
        await db.clear_membership_cache(user_id)

    # --- Check ALL channels in parallel for speed ---
    async def _check_one(channel_data):
        channel = channel_data['channel_username']
        channel_name = channel_data['channel_name'] or channel
        channel_id = channel_data['id']
        channel_type = channel_data.get('channel_type', 'public')
        
        is_member, verification_error = await check_user_in_channel(bot, channel, user_id, force_check)
        
        # Check for private channel request
        if allow_private_requests and not is_member and channel_type == 'private':
            has_request = await db.has_private_request(user_id, channel_id)
            if has_request:
                log.info(f"User {user_id} has requested to join private {channel_name}. Treating as member for file access.")
                is_member = True
                verification_error = None
        
        return channel, channel_name, channel_id, channel_type, is_member, verification_error

    check_results = await asyncio.gather(*[_check_one(cd) for cd in active_channels])

    for channel, channel_name, channel_id, channel_type, is_member, verification_error in check_results:
        result["channel_status"][channel] = {
            'is_member': is_member,
            'name': channel_name,
            'type': channel_type,
            'verification_error': verification_error
        }
        
        if verification_error:
            result["verification_errors"].append({
                "channel": channel,
                "name": channel_name,
                "error": verification_error
            })
            if not BLOCK_ON_CHANNEL_VERIFY_ERROR:
                log.warning(f"Skipping unverifiable channel @{channel} for user {user_id}: {verification_error}")
                continue
        
        if not is_member:
            log.info(f"❌ User {user_id} NOT in channel @{channel}")
            result["missing_channels"].append(channel)
            result["missing_channel_names"].append(channel_name)
            result["missing_channel_ids"].append(channel_id)
            result["missing_channel_types"].append(channel_type)
        else:
            log.info(f"✅ User {user_id} IS in channel @{channel}")

    result["all_joined"] = len(result["missing_channels"]) == 0
    log.info(f"📊 Final result for user {user_id}: all_joined={result['all_joined']}, missing={result['missing_channel_names']}")
    
    return result

# ============ CHAT MEMBER HANDLER - Auto detect joins ============
async def chat_member_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle chat member updates - auto send files when user joins channels"""
    try:
        chat_member_update = update.chat_member
        
        if not chat_member_update:
            return
        
        # Get the user who joined
        user = chat_member_update.new_chat_member.user
        user_id = user.id
        chat = chat_member_update.chat
        
        # Check if this is a channel or group
        if chat.type not in ['channel', 'group', 'supergroup']:
            return
        
        # Get the channel/group identifier
        chat_id = str(chat.id)
        if chat.type == 'channel':
            chat_username = chat.username or chat_id
        else:
            chat_username = chat_id
        
        # Check if this is a required channel
        channels = await db.get_channels_with_details()
        required_channel = None
        for ch in channels:
            if ch['is_active'] == 1:
                normalized_ch = normalize_channel_username(ch['channel_username'])
                normalized_joined = normalize_channel_username(chat_username)
                if normalized_ch == normalized_joined or str(chat.id) == normalized_ch:
                    required_channel = ch
                    break
        
        if not required_channel:
            return
        
        # Check if user is now a member
        new_status = chat_member_update.new_chat_member.status
        is_member = new_status in ['member', 'administrator', 'creator']
        
        if not is_member:
            return
        
        log.info(f"✅ User {user_id} joined required channel: {chat_username}")
        
        # Clear membership cache for this user
        await db.clear_membership_cache(user_id)
        
        # Check if this user has any pending deliveries
        pending_deliveries = await db.get_pending_deliveries(user_id)
        
        if not pending_deliveries:
            log.info(f"No pending deliveries for user {user_id}")
            return
        
        # For each pending delivery, check if all channels are now satisfied
        for delivery in pending_deliveries:
            file_key = delivery['file_key']
            missing_channels = delivery['missing_channels']
            
            # Check if this channel was in the missing list
            if chat_username not in missing_channels and str(chat.id) not in missing_channels:
                continue
            
            # Check membership again fresh
            membership_result = await check_membership(user_id, context, force_check=True, allow_private_requests=True)
            
            if membership_result['all_joined']:
                log.info(f"🎯 User {user_id} now has ALL channels! Sending file: {file_key}")
                
                # Get file content
                shared_content = await get_shared_content(file_key)
                if shared_content:
                    try:
                        if context and hasattr(context, 'user_data') and 'force_sub_msg_id' in context.user_data:
                            try:
                                await context.bot.delete_message(chat_id=user_id, message_id=context.user_data['force_sub_msg_id'])
                                del context.user_data['force_sub_msg_id']
                            except Exception as e:
                                log.error(f"Failed to delete force sub message: {e}")
                                
                        sending_msg = await context.bot.send_message(chat_id=user_id, text="⏳ Bot sending files...")
                        
                        # Send the file
                        await send_shared_content(context, user_id, shared_content)
                        log.info(f"✅ Auto-sent file {file_key} to user {user_id}")
                        
                        try:
                            await sending_msg.delete()
                        except Exception as e:
                            log.warning(f"Could not delete sending message: {e}")
                        
                        # Update user interaction
                        await db.update_user_interaction(
                            user_id=user_id,
                            username=user.username,
                            first_name=user.first_name,
                            last_name=user.last_name,
                            file_accessed=True
                        )
                        
                        # Remove pending delivery
                        await db.remove_pending_delivery(user_id, file_key)
                        
                        # Clear private channel requests for this user
                        await db.clear_user_requests(user_id)
                        
                    except Exception as e:
                        log.error(f"❌ Failed to auto-send file to user {user_id}: {e}")
        
    except Exception as e:
        log.error(f"Error in chat_member_handler: {e}", exc_info=True)


# ============ CHAT JOIN REQUEST HANDLER ============
async def chat_join_request_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle chat join requests - auto send files when user requests to join private channels"""
    try:
        request = update.chat_join_request
        if not request:
            return
            
        user = request.from_user
        user_id = user.id
        chat = request.chat
        
        # Check if this is a channel or group
        if chat.type not in ['channel', 'group', 'supergroup']:
            return
            
        chat_id = str(chat.id)
        if chat.type == 'channel':
            chat_username = chat.username or chat_id
        else:
            chat_username = chat_id
            
        log.info(f"🔔 User {user_id} requested to join channel: {chat_username}")
            
        # Check if this is a required channel
        channels = await db.get_channels_with_details()
        required_channel = None
        db_channel_id = None
        for ch in channels:
            if ch['is_active'] == 1:
                normalized_ch = normalize_channel_username(ch['channel_username'])
                normalized_joined = normalize_channel_username(chat_username)
                if normalized_ch == normalized_joined or str(chat.id) == normalized_ch:
                    required_channel = ch
                    db_channel_id = ch['id']
                    break
                    
        if not required_channel or not db_channel_id:
            return
            
        # Register the private request in the database
        pending_deliveries = await db.get_pending_deliveries(user_id)
        file_key = ""
        if pending_deliveries:
            file_key = pending_deliveries[0]['file_key']
            
        await db.add_private_request(user_id, db_channel_id, file_key)
        
        # Check if this user has any pending deliveries
        if not pending_deliveries:
            return
            
        # For each pending delivery, check if all channels are now satisfied
        for delivery in pending_deliveries:
            f_key = delivery['file_key']
            missing_channels = delivery['missing_channels']
            
            # Check if this channel was in the missing list
            if chat_username not in missing_channels and str(chat.id) not in missing_channels:
                continue
            
            # Check membership again fresh
            membership_result = await check_membership(user_id, context, force_check=True, allow_private_requests=True)
            
            if membership_result['all_joined']:
                log.info(f"🎯 User {user_id} now has ALL channels (via request)! Sending file: {f_key}")
                
                # Get file content
                shared_content = await get_shared_content(f_key)
                if shared_content:
                    try:
                        if context and hasattr(context, 'user_data') and 'force_sub_msg_id' in context.user_data:
                            try:
                                await context.bot.delete_message(chat_id=user_id, message_id=context.user_data['force_sub_msg_id'])
                                del context.user_data['force_sub_msg_id']
                            except Exception as e:
                                log.error(f"Failed to delete force sub message: {e}")
                                
                        sending_msg = await context.bot.send_message(chat_id=user_id, text="⏳ Bot sending files...")
                        
                        # Send the file
                        await send_shared_content(context, user_id, shared_content)
                        log.info(f"✅ Auto-sent file {f_key} to user {user_id}")
                        
                        try:
                            await sending_msg.delete()
                        except Exception as e:
                            log.warning(f"Could not delete sending message: {e}")
                        
                        # Update user interaction
                        await db.update_user_interaction(
                            user_id=user_id,
                            username=user.username,
                            first_name=user.first_name,
                            last_name=user.last_name,
                            file_accessed=True
                        )
                        
                        # Remove pending delivery
                        await db.remove_pending_delivery(user_id, f_key)
                        
                    except Exception as e:
                        log.error(f"❌ Failed to auto-send file to user {user_id}: {e}")
                        
    except Exception as e:
        log.error(f"Error in chat_join_request_handler: {e}", exc_info=True)

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
            <p>Required Channels: {{ channel_count }}</p>
            <p>📁 Storage: Metadata only (files stored on Telegram)</p>
        </div>

        <div class="info">
            <h3>📊 Bot Information</h3>
            <ul>
                <li>Bot: <strong>@{{ bot_username }}</strong></li>
                <li>Database: <strong>Render PostgreSQL</strong></li>
                <li>Driver: <strong>psycopg2-binary</strong></li>
                <li>Storage: <strong>Metadata only - Files on Telegram</strong></li>
                <li>Message Auto-delete: <strong>{{ delete_minutes }} minutes</strong></li>
                <li>Dynamic Channels: <strong>Yes (Add/Remove anytime)</strong></li>
                <li>Private Channels: <strong>Supported (auto-invite + auto-approve)</strong></li>
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

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        file_count = loop.run_until_complete(db.get_file_count())
        user_count = loop.run_until_complete(db.get_user_count())
        channel_count = loop.run_until_complete(db.get_channel_count())
        loop.close()
    except Exception as e:
        log.error(f"Error fetching counts for home route: {e}")
        file_count = 0
        user_count = 0
        channel_count = 0

    return render_template_string(html_content,
                                  bot_username=bot_username,
                                  uptime=uptime_str,
                                  current_time=datetime.now().strftime("%H:%M:%S"),
                                  file_count=file_count,
                                  user_count=user_count,
                                  channel_count=channel_count,
                                  delete_minutes=DELETE_AFTER//60)

@app.route('/health')
def health():
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        file_count = loop.run_until_complete(db.get_file_count())
        user_count = loop.run_until_complete(db.get_user_count())
        channel_count = loop.run_until_complete(db.get_channel_count())
        loop.close()
    except Exception as e:
        log.error(f"Error in health check: {e}")
        file_count = 0
        user_count = 0
        channel_count = 0

    return jsonify({
        "status": "OK",
        "timestamp": datetime.now().isoformat(),
        "service": "telegram-file-bot",
        "uptime": str(timedelta(seconds=int(time.time() - start_time))),
        "database": "postgresql",
        "driver": "psycopg2-binary",
        "storage": "metadata_only",
        "auto_cleanup": False,
        "file_count": file_count,
        "user_count": user_count,
        "channel_count": channel_count,
        "dynamic_channels": True,
        "private_channels": True,
        "bot_initialized": bot_initialized
    }), 200

@app.route('/ping')
def ping():
    return "pong", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle Telegram webhook updates"""
    global bot_app, bot_loop, bot_initialized

    update_data = request.get_json(silent=True)
    if not update_data:
        log.warning("Webhook received an empty or invalid request")
        return "OK", 200

    if not bot_initialized or bot_app is None or bot_loop is None:
        log.warning("Webhook received before bot was ready; waiting for initialization")
        deadline = time.monotonic() + BOT_INIT_WAIT_SECONDS
        while time.monotonic() < deadline:
            if bot_initialized and bot_app is not None and bot_loop is not None:
                break
            time.sleep(BOT_READY_POLL_INTERVAL_SECONDS)

    if not bot_initialized or bot_app is None or bot_loop is None:
        log.error("Bot application still not initialized after waiting; acknowledging update")
        return "OK", 200

    future = asyncio.run_coroutine_threadsafe(
        process_update(update_data, bot_app),
        bot_loop
    )
    
    try:
        future.result(timeout=WEBHOOK_PROCESS_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
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

    os.environ['PYTHONASYNCIODEBUG'] = '0'

    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)

# ============ DATABASE BACKUP & EXPORT FEATURE ============

BACKUP_TABLE_COLUMNS = {
    "files": ["id", "file_id", "file_name", "mime_type", "is_video",
              "file_size", "timestamp", "access_count"],
    "file_groups": ["id", "title", "file_count", "total_size", "timestamp", "access_count"],
    "file_group_items": ["group_id", "file_db_id", "position"],
    "users": ["user_id", "username", "first_name", "last_name",
              "first_seen", "last_active", "total_interactions",
              "total_files_accessed", "last_file_accessed"],
    "membership_cache": ["user_id", "channel", "is_member", "timestamp"],
    "required_channels": ["id", "channel_username", "channel_name", "channel_type", "invite_link",
                          "added_by", "added_at", "is_active", "position"],
    "scheduled_deletions": ["chat_id", "message_id", "scheduled_time", "delete_after"],
    "private_channel_requests": ["id", "user_id", "channel_id", "file_key", "requested", "requested_at"],
    "pending_file_delivery": ["id", "user_id", "file_key", "missing_channels", "created_at"]
}

IMPORT_TABLE_ORDER = [
    "required_channels",
    "users",
    "files",
    "file_groups",
    "file_group_items",
    "membership_cache",
    "scheduled_deletions",
    "private_channel_requests",
    "pending_file_delivery"
]

REQUIRED_IMPORT_FILES = ["files.csv", "users.csv", "required_channels.csv"]
OPTIONAL_IMPORT_FILES = [
    "file_groups.csv",
    "file_group_items.csv",
    "membership_cache.csv",
    "scheduled_deletions.csv",
    "private_channel_requests.csv",
    "pending_file_delivery.csv",
    "metadata.json"
]
CANONICAL_BACKUP_FILES = REQUIRED_IMPORT_FILES + OPTIONAL_IMPORT_FILES

INTEGER_IMPORT_COLUMNS = {
    "id", "is_video", "access_count", "total_interactions",
    "total_files_accessed", "is_active", "position", "delete_after",
    "added_by", "message_id", "group_id", "file_db_id", "file_count",
    "channel_id", "requested"
}

BIGINT_IMPORT_COLUMNS = {"user_id", "chat_id", "file_size", "total_size"}


def decode_backup_bytes(file_content: bytes) -> str:
    """Decode admin-uploaded backup files without breaking on UTF-8 BOM files."""
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return file_content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return file_content.decode("utf-8", errors="replace")


def canonical_backup_filename(filename: str) -> Optional[str]:
    """Map exact/timestamped backup filenames to the canonical import filename."""
    if not filename:
        return None

    clean_name = Path(filename).name.strip().lower()

    for canonical in CANONICAL_BACKUP_FILES:
        if clean_name == canonical:
            return canonical

        stem, extension = canonical.rsplit(".", 1)
        pattern = rf"(?:^|[_\-\s]){re.escape(stem)}(?:\s*\(\d+\))?\.{re.escape(extension)}$"
        if re.search(pattern, clean_name):
            return canonical

    return None


def normalize_backup_file_map(files_data: Dict[str, str]) -> Tuple[Dict[str, str], List[str]]:
    """Normalize received backup filenames and return unmatched CSV/JSON names."""
    normalized = {}
    unmatched = []

    for filename, content in files_data.items():
        canonical = canonical_backup_filename(filename)
        if canonical:
            if canonical in normalized:
                log.info(f"Replacing duplicate backup file for {canonical} with {filename}")
            normalized[canonical] = content
        else:
            unmatched.append(filename)

    return normalized, unmatched


def convert_import_value(column: str, value: Any) -> Any:
    """Convert CSV string values into values PostgreSQL can insert cleanly."""
    if value is None:
        return None

    if not isinstance(value, str):
        return value

    stripped = value.strip()
    if stripped == "" or stripped.upper() in {"NULL", "NONE"}:
        return None

    if column in INTEGER_IMPORT_COLUMNS or column in BIGINT_IMPORT_COLUMNS:
        return int(stripped)

    return value


def markdown_code(value: Any) -> str:
    """Return a safe Telegram Markdown inline-code value."""
    text = str(value).replace("\\", "\\\\").replace("`", "\\`")
    return f"`{text}`"


def clean_single_line(value: Any, fallback: str = "") -> str:
    """Return text that is safe to show as one Telegram message line."""
    text = str(value or fallback)
    return text.replace("\r", " ").replace("\n", " ").strip() or fallback


def get_command_body(message_text: Optional[str], command_name: str) -> str:
    """Return everything after a bot command, preserving line breaks."""
    if not message_text:
        return ""

    stripped = message_text.strip()
    if not stripped:
        return ""

    parts = stripped.split(maxsplit=1)
    command = parts[0].split("@", 1)[0].lower()
    if command != f"/{command_name.lower()}":
        return ""

    return parts[1] if len(parts) > 1 else ""


def is_valid_button_url(url: str) -> bool:
    """Validate URL buttons before Telegram rejects a broadcast."""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme in {"http", "https"}:
        return bool(parsed.netloc)
    if parsed.scheme == "tg":
        return bool(parsed.netloc or parsed.path)
    return False


def parse_broadcast_content(raw_text: str) -> Tuple[str, List[Tuple[str, str]], Optional[str]]:
    """Split broadcast text from an optional BUTTONS section."""
    normalized = (raw_text or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")

    buttons_index = None
    for index, line in enumerate(lines):
        if line.strip().lower() == "buttons:":
            buttons_index = index
            break

    if buttons_index is None:
        return normalized.strip(), [], None

    message_text = "\n".join(lines[:buttons_index]).strip()
    button_lines = [line.strip() for line in lines[buttons_index + 1:] if line.strip()]

    if not button_lines:
        return "", [], "Add at least one button after BUTTONS: or remove the BUTTONS: section."

    if len(button_lines) > BROADCAST_MAX_BUTTONS:
        return "", [], f"Too many buttons. Maximum allowed is {BROADCAST_MAX_BUTTONS}."

    buttons = []
    for line_number, line in enumerate(button_lines, start=1):
        if "|" not in line:
            return "", [], f"Button line {line_number} must be: Button Name | https://example.com"

        label, url = [part.strip() for part in line.split("|", 1)]
        if not label:
            return "", [], f"Button line {line_number} is missing the button name."
        if not url:
            return "", [], f"Button line {line_number} is missing the URL."
        if len(label) > 64:
            return "", [], f"Button line {line_number} name is too long. Keep it under 64 characters."
        if not is_valid_button_url(url):
            return "", [], f"Button line {line_number} has an invalid URL. Use http://, https://, or tg://."

        buttons.append((label, url))

    return message_text, buttons, None


def build_url_button_rows(buttons: List[Tuple[str, str]]) -> List[List[InlineKeyboardButton]]:
    """Build URL button rows with a maximum of two buttons per row."""
    rows = []
    for index in range(0, len(buttons), 2):
        rows.append([
            InlineKeyboardButton(label, url=url)
            for label, url in buttons[index:index + 2]
        ])
    return rows


def build_broadcast_reply_markup(
    buttons: List[Tuple[str, str]],
    include_actions: bool = False,
    broadcast_id: Optional[str] = None
) -> Optional[InlineKeyboardMarkup]:
    rows = build_url_button_rows(buttons)
    if include_actions:
        confirm_data = f"confirm_broadcast|{broadcast_id}" if broadcast_id else "confirm_broadcast"
        cancel_data = f"cancel_broadcast|{broadcast_id}" if broadcast_id else "cancel_broadcast"
        rows.append([
            InlineKeyboardButton("✅ Confirm Broadcast", callback_data=confirm_data),
            InlineKeyboardButton("❌ Cancel", callback_data=cancel_data)
        ])

    return InlineKeyboardMarkup(rows) if rows else None


def validate_broadcast_payload(text: str, photo_file_id: Optional[str]) -> Optional[str]:
    if not text and not photo_file_id:
        return "Message cannot be empty."

    if photo_file_id and len(text) > BROADCAST_PHOTO_CAPTION_LIMIT:
        return f"Photo captions can be at most {BROADCAST_PHOTO_CAPTION_LIMIT} characters. Please shorten the text."

    if not photo_file_id and len(text) > BROADCAST_TEXT_LIMIT:
        return f"Text broadcasts can be at most {BROADCAST_TEXT_LIMIT} characters. Please shorten the text."

    return None

async def export_table_to_csv(table_name: str, columns: list) -> str:
    """Export a table to CSV format and return CSV content"""
    try:
        rows = await db.fetchall(f"SELECT * FROM {table_name}")
        
        if not rows:
            return None
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        writer.writerow(columns)
        
        for row in rows:
            row_dict = dict(row)
            row_data = [row_dict.get(col, '') for col in columns]
            writer.writerow(row_data)
        
        return output.getvalue()
        
    except Exception as e:
        log.error(f"Error exporting {table_name}: {e}")
        return None

async def export_database_backup(update: Update = None, context: ContextTypes.DEFAULT_TYPE = None, send_to_admin: bool = True) -> Dict[str, Any]:
    """Export entire database to CSV files and return as dictionary of file contents"""
    
    backup_data = {}
    backup_info = {
        "export_time": datetime.now().isoformat(),
        "tables_exported": [],
        "row_counts": {}
    }
    
    for table_name, columns in BACKUP_TABLE_COLUMNS.items():
        try:
            csv_content = await export_table_to_csv(table_name, columns)
            
            if csv_content:
                backup_data[f"{table_name}.csv"] = csv_content
                row_count = len(csv_content.splitlines()) - 1
                backup_info["tables_exported"].append(table_name)
                backup_info["row_counts"][table_name] = max(0, row_count)
                log.info(f"✅ Exported {table_name}: {row_count} rows")
            else:
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(columns)
                backup_data[f"{table_name}.csv"] = output.getvalue()
                backup_info["tables_exported"].append(table_name)
                backup_info["row_counts"][table_name] = 0
                log.info(f"📭 Table {table_name} is empty")
                
        except Exception as e:
            log.error(f"❌ Failed to export {table_name}: {e}")
    
    metadata = {
        "export_info": backup_info,
        "bot_config": {
            "bot_username": bot_username,
            "delete_after_seconds": DELETE_AFTER,
            "auto_cleanup_days": AUTO_CLEANUP_DAYS,
            "export_timestamp": datetime.now().isoformat()
        }
    }
    
    backup_data["metadata.json"] = json.dumps(metadata, indent=2)
    backup_info["metadata_created"] = True
    
    if send_to_admin and context:
        await send_backup_to_admin(context, backup_data, backup_info)
    
    return backup_data

async def send_backup_to_admin(context: ContextTypes.DEFAULT_TYPE, backup_data: Dict[str, str], backup_info: Dict[str, Any]):
    """Send backup files to admin"""
    try:
        summary = f"📦 *Database Backup Created*\n\n"
        summary += f"⏰ Time: {backup_info['export_time']}\n"
        summary += f"📊 Tables exported: {len(backup_info['tables_exported'])}\n\n"
        summary += f"📈 *Row Counts:*\n"
        
        for table, count in backup_info['row_counts'].items():
            summary += f"   • {table}: {count} rows\n"
        
        summary += f"\n💾 *Total backup size:* {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB\n"
        summary += f"\n📁 *Files included:*\n"
        for filename in backup_data.keys():
            size_kb = len(backup_data[filename]) / 1024
            summary += f"   • {filename} ({size_kb:.1f} KB)\n"
        
        summary += f"\n⚠️ *Important:* Save these files immediately!\n"
        summary += f"Your Render PostgreSQL data will be lost after 1 month.\n\n"
        summary += f"💡 *To restore:* Forward ALL files back to bot and use `/import`\n"
        summary += f"📌 The bot now accepts both exact filenames and timestamped filenames"
        
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=summary
        )
        
        for filename, content in backup_data.items():
            if content and len(content) > 0:
                file_bytes = io.BytesIO(content.encode('utf-8'))
                file_bytes.seek(0)
                
                file_emoji = "📋" if filename.endswith('.json') else "📄"
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                send_filename = f"backup_{timestamp}_{filename}"
                
                await context.bot.send_document(
                    chat_id=ADMIN_ID,
                    document=file_bytes,
                    filename=send_filename,
                    caption=f"{file_emoji} {filename} - {len(content.splitlines())} lines"
                )
                
                await asyncio.sleep(0.5)
        
        instructions = f"""
✅ *Backup Complete!*

📋 *To Restore on New Database:*

1. Create new PostgreSQL database on Render
2. Update DATABASE_URL environment variable
3. Restart bot
4. Forward ALL backup files (CSV + JSON) to bot
5. Use `/import` to restore
6. Confirm import
7. All users and files restored! ✅

🔧 *Commands:*
• `/backup` - Create new backup
• `/backup_status` - Check database health
• `/import` - Restore from backup files
• `/import_status` - Check collected files

⚠️ *Your users and broadcasts will work after restore!*
📌 The bot now supports both exact and timestamped filenames
        """
        
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=instructions
        )
        
        log.info(f"✅ Database backup sent to admin (ID: {ADMIN_ID})")
        
    except Exception as e:
        log.error(f"❌ Failed to send backup to admin: {e}")
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"❌ Backup created but failed to send files: {str(e)[:200]}\n\nBackup data size: {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB"
            )
        except:
            pass

# ============ IMPORT/RESTORE FUNCTIONS ============

async def import_csv_to_table(table_name: str, csv_content: str, truncate_first: bool = True) -> Dict[str, Any]:
    """Import CSV data to a specific PostgreSQL table."""
    result = {
        "success": False,
        "rows_imported": 0,
        "errors": [],
        "table": table_name
    }

    try:
        if table_name not in BACKUP_TABLE_COLUMNS:
            raise ValueError(f"Unsupported import table: {table_name}")

        csv_text = csv_content.lstrip('\ufeff')
        csv_reader = csv.DictReader(io.StringIO(csv_text))

        if not csv_reader.fieldnames:
            raise ValueError(f"{table_name}.csv has no header row")

        original_headers = [header for header in csv_reader.fieldnames if header]
        normalized_headers = [header.strip().lstrip('\ufeff') for header in original_headers]
        expected_columns = BACKUP_TABLE_COLUMNS[table_name]
        columns = [column for column in expected_columns if column in normalized_headers]

        if not columns:
            raise ValueError(
                f"{table_name}.csv does not contain any valid columns. "
                f"Found: {', '.join(normalized_headers)}"
            )

        missing_columns = [column for column in expected_columns if column not in normalized_headers]
        if missing_columns:
            log.warning(f"{table_name}.csv missing columns: {missing_columns}")

        rows = []
        for raw_row in csv_reader:
            normalized_row = {}
            for original, normalized in zip(original_headers, normalized_headers):
                normalized_row[normalized] = raw_row.get(original)
            rows.append(normalized_row)

        async with db.get_db_connection() as conn:
            def _import():
                try:
                    with conn.cursor() as cur:
                        if truncate_first:
                            truncate_query = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(
                                sql.Identifier(table_name)
                            )
                            cur.execute(truncate_query)
                            log.info(f"Truncated table {table_name}")

                        if not rows:
                            conn.commit()
                            return 0

                        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                            sql.Identifier(table_name),
                            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                            sql.SQL(", ").join(sql.Placeholder() for _ in columns)
                        )

                        imported = 0
                        for row_number, row in enumerate(rows, start=2):
                            try:
                                values = [convert_import_value(column, row.get(column)) for column in columns]
                            except Exception as e:
                                raise ValueError(f"Row {row_number}: value conversion failed: {e}") from e

                            cur.execute(insert_query, values)
                            imported += 1

                        conn.commit()
                        return imported
                except Exception:
                    conn.rollback()
                    raise

            result["rows_imported"] = await asyncio.to_thread(_import)
            result["success"] = True
            log.info(f"Imported {result['rows_imported']} rows to {table_name}")

    except Exception as e:
        log.error(f"Failed to import {table_name}: {e}")
        result["errors"].append(str(e))

    return result


async def reset_sequences():
    """Reset PostgreSQL sequences after import"""
    try:
        async with db.get_db_connection() as conn:
            def _reset():
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT setval(
                            pg_get_serial_sequence('files', 'id'),
                            COALESCE((SELECT MAX(id) FROM files), 1),
                            EXISTS(SELECT 1 FROM files)
                        )
                    """)
                    cur.execute("""
                        SELECT setval(
                            pg_get_serial_sequence('required_channels', 'id'),
                            COALESCE((SELECT MAX(id) FROM required_channels), 1),
                            EXISTS(SELECT 1 FROM required_channels)
                        )
                    """)
                    cur.execute("""
                        SELECT setval(
                            pg_get_serial_sequence('file_groups', 'id'),
                            COALESCE((SELECT MAX(id) FROM file_groups), 1),
                            EXISTS(SELECT 1 FROM file_groups)
                        )
                    """)
                    conn.commit()
                    log.info("✅ Sequences reset successfully")
            await asyncio.to_thread(_reset)
    except Exception as e:
        log.error(f"Failed to reset sequences: {e}")

async def restore_from_backup(files_data: Dict[str, str]) -> Dict[str, Any]:
    """Restore entire database from backup files"""
    
    restore_result = {
        "success": False,
        "tables_restored": [],
        "total_rows": 0,
        "errors": [],
        "warnings": [],
        "timestamp": datetime.now().isoformat()
    }
    
    for table_name in IMPORT_TABLE_ORDER:
        csv_filename = f"{table_name}.csv"
        
        if csv_filename in files_data:
            log.info(f"📥 Importing {table_name}...")
            
            result = await import_csv_to_table(table_name, files_data[csv_filename], truncate_first=True)
            
            if result["success"]:
                restore_result["tables_restored"].append({
                    "table": table_name,
                    "rows": result["rows_imported"]
                })
                restore_result["total_rows"] += result["rows_imported"]
            else:
                restore_result["errors"].append(f"{table_name}: {', '.join(result['errors'])}")
        else:
            log.warning(f"⚠️ No CSV file found for {table_name}")
            if csv_filename in REQUIRED_IMPORT_FILES:
                restore_result["errors"].append(f"Missing {csv_filename}")
            else:
                restore_result["warnings"].append(f"Missing {csv_filename}")
    
    await reset_sequences()
    
    restore_result["success"] = len(restore_result["errors"]) == 0
    
    return restore_result

# ============ COMMAND HANDLERS ============

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Error handler"""
    log.error(f"Error: {context.error}", exc_info=True)

# ============ FORWARDED FILE HANDLER (CSV + JSON) ============
async def handle_forwarded_backup_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Detect and process forwarded CSV and JSON backup files"""
    try:
        msg = update.message
        
        if not msg.document:
            return
        
        doc = msg.document
        
        doc_name = doc.file_name or ""
        lower_doc_name = doc_name.lower()
        is_csv = lower_doc_name.endswith('.csv')
        is_json = lower_doc_name.endswith('.json')
        
        if not (is_csv or is_json):
            if update.effective_user.id == ADMIN_ID:
                return
            return
        
        if update.effective_user.id == ADMIN_ID:
            file_type = "CSV" if is_csv else "JSON"
            log.info(f"📥 Admin sent {file_type} file: {doc.file_name}")
            
            if 'pending_backup_files' not in context.user_data:
                context.user_data['pending_backup_files'] = {}
            
            try:
                file = await context.bot.get_file(doc.file_id)
                file_content = await file.download_as_bytearray()
                file_text = decode_backup_bytes(bytes(file_content))
                
                context.user_data['pending_backup_files'][doc.file_name] = file_text
                
                if is_csv:
                    lines = len(file_text.splitlines()) - 1
                    record_info = f"📊 Records: {lines}"
                else:
                    try:
                        json.loads(file_text)
                        record_info = f"📋 JSON metadata file"
                    except:
                        record_info = f"📋 JSON file"
                
                collected_files = list(context.user_data['pending_backup_files'].keys())
                log.info(f"📦 Collected backup files: {collected_files}")
                
                file_emoji = "📄" if is_csv else "📋"
                sent_msg = await msg.reply_text(
                    f"✅ *{file_type} File Received*\n\n"
                    f"{file_emoji} File: `{doc.file_name}`\n"
                    f"{record_info}\n"
                    f"💾 Size: {doc.file_size / 1024:.1f} KB\n\n"
                    f"📦 Files collected: {len(context.user_data['pending_backup_files'])}\n\n"
                    f"💡 When ready, use `/import` to restore all collected files.\n"
                    f"🔍 Use `/import_status` to check collected files.\n\n"
                    f"⚠️ *Note:* Forwarded backup files are automatically collected.",
                    parse_mode="Markdown"
                )
                
                await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                
            except Exception as e:
                log.error(f"Error downloading {file_type} file: {e}")
                sent_msg = await msg.reply_text(f"❌ Error downloading {file_type} file: {str(e)[:200]}")
                await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            
        else:
            file_type = "CSV" if is_csv else "JSON"
            log.info(f"ℹ️ Non-admin user {update.effective_user.id} sent {file_type} file (ignored)")
            
    except Exception as e:
        log.error(f"Error handling backup file: {e}", exc_info=True)

# ============ START COMMAND - With auto-send on join ============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler - Shows buttons for missing channels and auto-sends when joined"""
    try:
        if not update.message:
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        args = context.args
        username = update.effective_user.username
        first_name = update.effective_user.first_name

        log.info(f"🚀 /start command from user {user_id} (@{username}) with args: {args}")

        await db.update_user_interaction(
            user_id=user_id,
            username=username,
            first_name=first_name,
            last_name=update.effective_user.last_name
        )

        channels_data = await db.get_channels_with_details()
        active_channels = [c for c in channels_data if c['is_active'] == 1]
        
        log.info(f"📋 Found {len(active_channels)} active channels for user {user_id}")
        
        # No file key - show welcome
        if not args:
            log.info(f"👋 Showing welcome menu to user {user_id}")
            keyboard = []
            
            for channel_data in active_channels:
                channel = normalize_channel_username(channel_data['channel_username'])
                channel_name = channel_data['channel_name'] or f"Channel"
                channel_type = channel_data.get('channel_type', 'public')
                
                if channel_type == 'private' and channel_data.get('invite_link'):
                    keyboard.append([InlineKeyboardButton(
                        f"📢 Join {channel_name}", 
                        url=channel_data['invite_link']
                    )])
                else:
                    keyboard.append([InlineKeyboardButton(
                        f"📢 Join {channel_name}", 
                        url=f"https://t.me/{channel}"
                    )])

            if active_channels:
                channel_list = "\n".join([f"{i+1}. {escape_markdown(c['channel_name'] or f'Channel {i+1}')} ({c.get('channel_type', 'public')})" for i, c in enumerate(active_channels)])
            else:
                channel_list = "No channels required!"

            sent_msg = await update.message.reply_text(
                "🤖 *Welcome to Bot*\n\n"
                "🔗 *How to use:*\n"
                "1️⃣ Use admin-provided links\n"
                "2️⃣ Join the required channels:\n"
                "3️⃣ After joining, the file will be sent automatically!",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        # ============ FILE KEY EXISTS - CHECK MEMBERSHIP ============
        key = args[0]
        log.info(f"🔑 User {user_id} accessing file key: {key}")
        
        # Run file lookup and membership check concurrently for speed
        log.info(f"🔍 Checking membership for user {user_id}")
        shared_content_task = get_shared_content(key)
        membership_task = check_membership(
            user_id, context, force_check=True,
            allow_private_requests=True, prefetched_channels=active_channels
        )
        shared_content, result = await asyncio.gather(shared_content_task, membership_task)

        if not shared_content:
            log.warning(f"❌ File key {key} not found for user {user_id}")
            sent_msg = await update.message.reply_text("❌ File not found")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        if shared_content.get("kind") == "group":
            log.info(f"File group found: {key} ({len(shared_content.get('files') or [])} files)")
        else:
            log.info(f"File found: {shared_content['file_name']}")

        log.info(f"📊 Membership result: all_joined={result['all_joined']}, missing={result['missing_channel_names']}")

        if not result["all_joined"]:
            missing_channels = result["missing_channels"]
            missing_names = result["missing_channel_names"]
            missing_ids = result["missing_channel_ids"]
            missing_types = result["missing_channel_types"]
            verification_errors = result.get("verification_errors", [])
            
            log.info(f"🔒 User {user_id} missing {len(missing_names)} channels: {missing_names}")
            
            # Store pending delivery
            await db.add_pending_delivery(user_id, key, missing_channels)
            
            # Create keyboard with buttons for missing channels
            keyboard = []
            
            for i, channel in enumerate(missing_channels):
                channel = normalize_channel_username(channel)
                channel_name = missing_names[i] if i < len(missing_names) else f"Channel {i+1}"
                channel_type = missing_types[i] if i < len(missing_types) else 'public'
                channel_id = missing_ids[i] if i < len(missing_ids) else None
                
                if channel_type == 'private':
                    # Get invite link from database
                    channel_data = await db.fetchrow(
                        "SELECT invite_link FROM required_channels WHERE channel_username = %s",
                        (channel,)
                    )
                    if channel_data and channel_data['invite_link']:
                        keyboard.append([InlineKeyboardButton(
                            f"📢 Join {channel_name}", 
                            url=channel_data['invite_link']
                        )])
                    else:
                        log.error(f"No invite link found for private channel: {channel}")
                        keyboard.append([InlineKeyboardButton(
                            f"❌ {channel_name} (Contact Admin)", 
                            callback_data="noop"
                        )])
                else:
                    keyboard.append([InlineKeyboardButton(
                        f"📢 Join {channel_name}", 
                        url=f"https://t.me/{channel}"
                    )])
            
            # Create appropriate message
            text = "Join all the channels\nBot will send file"

            log.info(f"📨 Sending restriction message to user {user_id} with {len(keyboard)} buttons")
            
            sent_msg = await update.message.reply_text(
                text,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
            if context and hasattr(context, 'user_data'):
                context.user_data['force_sub_msg_id'] = sent_msg.message_id
                
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        # User has joined all channels - send file immediately
        log.info(f"✅ User {user_id} has joined all channels. Sending file...")
        await db.update_user_interaction(user_id=user_id, file_accessed=True)

        sending_msg = await update.message.reply_text("⏳ Bot sending files...")

        try:
            sent_items = await send_shared_content(context, chat_id, shared_content)
            
            try:
                await sending_msg.delete()
            except Exception as e:
                log.warning(f"Could not delete sending message: {e}")
            log.info(f"Sent {len(sent_items)} file(s) successfully to user {user_id}")
            
            # Clean up any pending deliveries
            await db.remove_pending_delivery(user_id, key)

        except Exception as e:
            log.error(f"❌ Error sending file to user {user_id}: {e}", exc_info=True)
            sent_msg = await update.message.reply_text("❌ Failed to send file")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.error(f"❌ Start error: {e}", exc_info=True)
        try:
            if update.message:
                await update.message.reply_text(f"❌ Start error: {e}")
        except Exception:
            pass

# ============ CALLBACK HANDLER ============
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries"""
    try:
        query = update.callback_query
        await query.answer()

        user_id = query.from_user.id
        data = query.data
        username = query.from_user.username

        log.info(f"🔄 Callback query from user {user_id} (@{username}): {data}")

        user = query.from_user
        await db.update_user_interaction(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )

        if data.startswith("status|"):
            _, key = data.split("|")
            log.info(f"📊 Status check for file {key} from user {user_id}")

            shared_content = await get_shared_content(key)
            if not shared_content:
                await query.edit_message_text("❌ File not found")
                return

            result = await check_membership(
                user_id,
                context,
                force_check=True,
                allow_private_requests=True
            )

            if result['all_joined']:
                # User now has all channels - send file
                log.info(f"✅ User {user_id} now has all channels! Sending file...")
                await db.update_user_interaction(user_id=user_id, file_accessed=True)
                
                try:
                    chat_id = query.message.chat_id
                    sent_items = await send_shared_content(context, chat_id, shared_content)
                    await query.edit_message_text("✅ *File sent below!*", parse_mode="Markdown")
                    log.info(f"Sent {len(sent_items)} file(s) successfully to user {user_id}")
                    
                    # Clean up
                    await db.remove_pending_delivery(user_id, key)
                    
                except Exception as e:
                    log.error(f"❌ Failed to send file to user {user_id}: {e}", exc_info=True)
                    await query.edit_message_text("❌ Failed to send file")
            else:
                # Still missing some channels
                missing_names = result["missing_channel_names"]
                
                if len(missing_names) == 1:
                    text = f"⏳ *Still missing: {missing_names[0]}*\n\n"
                elif len(missing_names) == 2:
                    text = f"⏳ *Still missing: {missing_names[0]} and {missing_names[1]}*\n\n"
                else:
                    channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                    text = f"⏳ *Still missing: {channels_text}*\n\n"
                
                text += "Please join the channels and the file will be sent automatically!"
                
                await query.edit_message_text(text, parse_mode="Markdown")

        elif data == "noop":
            await query.edit_message_text("⚠️ Please contact the admin to fix this channel.")

    except Exception as e:
        log.error(f"❌ Callback error: {e}", exc_info=True)

# ============ CHANNEL MANAGEMENT COMMANDS ============

async def addchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new required channel (admin only) - FIXED"""
    if update.effective_user.id != ADMIN_ID:
        return

    # Check if replying to a forwarded message
    if update.message.reply_to_message:
        forwarded = update.message.reply_to_message
        
        # Check if it's a forwarded message from a channel
        chat = extract_chat_from_forward(forwarded)
        if chat:
            
            # Get channel info
            channel_id = str(chat.id)
            channel_title = chat.title or "Unknown Channel"
            chat_username = chat.username or None
            
            log.info(f"📝 Adding channel from forwarded message: {channel_title} (ID: {channel_id}, Username: {chat_username})")
            
            # Get friendly name from command args if provided
            friendly_name = None
            if context.args:
                friendly_name = " ".join(context.args)
            
            # Determine if private channel (no username)
            is_private = chat_username is None
            
            if is_private:
                # Private channel - need to create invite link
                try:
                    # Get channel reference
                    channel_ref = int(channel_id) if channel_id.lstrip('-').isdigit() else channel_id
                    
                    # Check if bot is admin
                    try:
                        bot_member = await context.bot.get_chat_member(channel_ref, context.bot.id)
                        if bot_member.status not in ["administrator", "creator"]:
                            sent_msg = await update.message.reply_text(
                                f"❌ Bot is not an admin in {channel_title}.\n\n"
                                "Make the bot an admin and try again."
                            )
                            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                            return
                    except Exception as e:
                        log.error(f"Error checking bot membership: {e}")
                        sent_msg = await update.message.reply_text(
                            f"❌ Bot cannot access {channel_title}. Make sure the bot is an admin in the channel."
                        )
                        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                        return
                    
                    # Create invite link (no expiry)
                    invite_link = await context.bot.create_chat_invite_link(
                        chat_id=channel_ref,
                        creates_join_request=True
                    )
                    
                    # Save to database
                    success = await db.add_channel(
                        channel_username=channel_id,  # Use numeric ID for private channels
                        added_by=ADMIN_ID,
                        channel_name=friendly_name or channel_title,
                        channel_type='private',
                        invite_link=invite_link.invite_link
                    )
                    
                    if success:
                        # Get updated channel list
                        channels = await db.get_channels_with_details()
                        active_channels = [c for c in channels if c['is_active'] == 1]
                        channel_list = "\n".join([f"{i+1}. {c['channel_name'] or c['channel_username']} ({c.get('channel_type', 'public')})" for i, c in enumerate(active_channels)])
                        
                        sent_msg = await update.message.reply_text(
                            f"✅ *Private Channel Added Successfully!*\n\n"
                            f"📢 Channel: {channel_title}\n"
                            f"🔑 ID: {channel_id}\n"
                            f"🔗 Invite Link: [Click Here]({invite_link.invite_link})\n"
                            f"📝 Type: Private\n\n"
                            f"Users can now request to join this channel.\n"
                            f"Use `/approve` to approve pending requests.\n\n"
                            f"📋 *Current required channels:*\n{channel_list}",
                            parse_mode="Markdown",
                            disable_web_page_preview=True
                        )
                    else:
                        sent_msg = await update.message.reply_text("❌ Failed to add channel. It may already exist.")
                    
                    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                    
                except Exception as e:
                    log.error(f"Error adding private channel: {e}", exc_info=True)
                    sent_msg = await update.message.reply_text(
                        f"❌ Failed to create invite link: {str(e)[:200]}\n\n"
                        "Make sure the bot is an admin in the channel."
                    )
                    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                
                return
            
            else:
                # Public channel - use username
                channel_username = chat_username
                success = await db.add_channel(
                    channel_username=channel_username,
                    added_by=ADMIN_ID,
                    channel_name=friendly_name or channel_title,
                    channel_type='public'
                )
                
                if success:
                    # Get updated channel list
                    channels = await db.get_channels_with_details()
                    active_channels = [c for c in channels if c['is_active'] == 1]
                    channel_list = "\n".join([f"{i+1}. {c['channel_name'] or c['channel_username']} ({c.get('channel_type', 'public')})" for i, c in enumerate(active_channels)])
                    
                    sent_msg = await update.message.reply_text(
                        f"✅ *Public Channel Added Successfully!*\n\n"
                        f"📢 Channel: {channel_title}\n"
                        f"@: @{channel_username}\n"
                        f"📝 Type: Public\n\n"
                        f"Users must join this channel to access files.\n\n"
                        f"📋 *Current required channels:*\n{channel_list}",
                        parse_mode="Markdown"
                    )
                else:
                    sent_msg = await update.message.reply_text("❌ Failed to add channel. It may already exist.")
                
                await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                return
        
        else:
            # Not a forwarded message from a channel
            sent_msg = await update.message.reply_text(
                "❌ *Please forward a message from the channel and reply with /addchannel*\n\n"
                "Usage: Forward a message from the channel, then reply to it with:\n"
                "/addchannel [friendly name]\n\n"
                "💡 The bot will automatically detect if it's a public or private channel.",
                parse_mode="Markdown"
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
    
    else:
        # No reply - try old method for public channels
        if not context.args:
            sent_msg = await update.message.reply_text(
                "❌ *Usage:*\n\n"
                "For public channels:\n"
                "/addchannel @username [friendly name]\n\n"
                "For private channels:\n"
                "Forward a message from the channel and reply with:\n"
                "/addchannel [friendly name]",
                parse_mode="Markdown"
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
        
        # Old method - public channel with @username
        channel = normalize_channel_username(context.args[0])
        friendly_name = None
        
        if len(context.args) > 1:
            friendly_name = " ".join(context.args[1:])
        
        if not channel:
            sent_msg = await update.message.reply_text("❌ Invalid channel username. Use @username.")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
        
        try:
            channel_ref = telegram_chat_ref(channel)
            if channel_ref is None:
                sent_msg = await update.message.reply_text("❌ Invalid channel. Use a public @username.")
                await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                return
            
            # Verify bot is admin
            try:
                bot_member = await context.bot.get_chat_member(channel_ref, context.bot.id)
                if bot_member.status not in ["administrator", "creator"]:
                    sent_msg = await update.message.reply_text(
                        f"⚠️ *Bot is not an admin in @{channel}*\n\n"
                        "Make sure to add the bot as admin to check memberships!",
                        parse_mode="Markdown"
                    )
                    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                    return
            except Exception as e:
                log.warning(f"Could not verify bot in channel {channel}: {e}")
                sent_msg = await update.message.reply_text(
                    f"❌ Could not verify @{channel}.\n\n"
                    "Add the bot as admin in that channel, then run /addchannel again.",
                    parse_mode="Markdown"
                )
                await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                return
            
            # Get channel info
            chat = await context.bot.get_chat(channel_ref)
            channel_title = chat.title or channel
            
            success = await db.add_channel(
                channel_username=channel,
                added_by=ADMIN_ID,
                channel_name=friendly_name or channel_title,
                channel_type='public'
            )
            
            if success:
                channels = await db.get_channels_with_details()
                active_channels = [c for c in channels if c['is_active'] == 1]
                channel_list = "\n".join([f"{i+1}. {c['channel_name'] or c['channel_username']} ({c.get('channel_type', 'public')})" for i, c in enumerate(active_channels)])
                
                sent_msg = await update.message.reply_text(
                    f"✅ *Channel added successfully!*\n\n"
                    f"Added: {friendly_name or f'@{channel.replace('@', '')}'}\n\n"
                    f"📋 *Current required channels:*\n{channel_list}",
                    parse_mode="Markdown"
                )
            else:
                sent_msg = await update.message.reply_text(f"❌ Failed to add channel. It might already exist.")
            
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            
        except Exception as e:
            log.error(f"Error adding channel: {e}", exc_info=True)
            sent_msg = await update.message.reply_text(f"❌ Error: {str(e)[:200]}")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def approve(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Approve all pending requests for a private channel (admin only) - FIXED"""
    if update.effective_user.id != ADMIN_ID:
        return

    # Check if replying to a forwarded message
    if not update.message.reply_to_message:
        sent_msg = await update.message.reply_text(
            "❌ *Usage:* Forward a message from the private channel and reply with /approve",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    forwarded = update.message.reply_to_message
    
    # Check if it's a forwarded message from a channel
    chat = extract_chat_from_forward(forwarded)
    if not chat:
        sent_msg = await update.message.reply_text(
            "❌ Please forward a message from the channel you want to approve requests for."
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    if chat.type not in ['channel', 'group', 'supergroup']:
        sent_msg = await update.message.reply_text("❌ Please forward a message from a channel or group.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    channel_id = str(chat.id)
    channel_title = chat.title or "Unknown Channel"
    
    log.info(f"📝 Approving requests for channel: {channel_title} (ID: {channel_id})")
    
    # Get channel from database - try exact match first
    channel_data = await db.fetchrow(
        "SELECT id, channel_username, channel_name, invite_link FROM required_channels WHERE channel_username = %s AND is_active = 1",
        (channel_id,)
    )
    
    if not channel_data:
        # Try without leading dash
        clean_id = channel_id.lstrip('-')
        channel_data = await db.fetchrow(
            "SELECT id, channel_username, channel_name, invite_link FROM required_channels WHERE channel_username = %s AND is_active = 1",
            (clean_id,)
        )
    
    if not channel_data:
        sent_msg = await update.message.reply_text(
            f"❌ Channel '{channel_title}' is not in the required channels list.\n"
            "Add it first with /addchannel (reply to a forwarded message)."
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    db_channel_id = channel_data['id']
    channel_name = channel_data['channel_name'] or channel_title
    channel_identifiers = [
        channel_data['channel_username'],
        channel_id,
        channel_id.lstrip('-'),
    ]
    
    # Get all pending requests for this channel
    pending_requests = await db.get_pending_requests_for_channel(db_channel_id)
    using_delivery_fallback = False
    
    if not pending_requests:
        fallback_deliveries = await db.get_pending_deliveries_for_channels(channel_identifiers)
        if fallback_deliveries:
            using_delivery_fallback = True
            pending_requests = [
                {
                    'user_id': delivery['user_id'],
                    'file_key': delivery['file_key'],
                    'from_pending_delivery': True,
                }
                for delivery in fallback_deliveries
            ]
            log.info(
                f"No saved join-request rows for {channel_name}; using "
                f"{len(pending_requests)} pending deliveries as approval fallback."
            )
        else:
            sent_msg = await update.message.reply_text(
                f"✅ No pending requests for {channel_name}\n\n"
                "No users are waiting for this private channel in the bot database.",
                parse_mode="Markdown"
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

    log.info(f"📋 Found {len(pending_requests)} pending requests for {channel_name}")
    
    status_msg = await update.message.reply_text(
        f"🔄 Processing {len(pending_requests)} pending requests for {channel_name}..."
    )
    
    approved_count = 0
    join_approved_count = 0
    join_already_member_count = 0
    failed_count = 0
    failed_reasons = []
    already_sent_count = 0
    waiting_count = 0
    approve_chat_id = int(channel_id) if channel_id.lstrip('-').isdigit() else channel_id
    
    # Pre-fetch channels ONCE to avoid repeated DB queries per user
    all_channels_data = await db.get_channels_with_details()
    
    for idx, request in enumerate(pending_requests):
        user_id = request['user_id']
        file_key = request['file_key']
        
        try:
            # Approve the Telegram join request with retry for rate limits
            try:
                for attempt in range(4):
                    try:
                        async with _telegram_semaphore:
                            await context.bot.approve_chat_join_request(
                                chat_id=approve_chat_id,
                                user_id=int(user_id)
                            )
                        break
                    except RetryAfter as e:
                        wait = e.retry_after + 1
                        log.warning(f"Rate limited approving user {user_id}. Waiting {wait}s")
                        await asyncio.sleep(wait)
                    except TimedOut:
                        if attempt == 3:
                            raise
                        await asyncio.sleep(1 * (attempt + 1))
                
                join_approved_count += 1
                log.info(f"Approved Telegram join request for user {user_id} in {channel_name}")
            except Exception as approve_error:
                actual_member, _ = await check_user_in_channel(
                    context.bot,
                    channel_id,
                    user_id,
                    force_check=True
                )
                if actual_member:
                    join_already_member_count += 1
                    log.info(f"User {user_id} is already a member of {channel_name}; continuing delivery checks.")
                else:
                    error_msg = str(approve_error)
                    log.error(f"Failed to approve Telegram join request for user {user_id} in {channel_name}: {error_msg}")
                    failed_count += 1
                    if error_msg not in failed_reasons:
                        failed_reasons.append(error_msg)
                    continue

            if using_delivery_fallback or request.get('from_pending_delivery'):
                await db.add_private_request(user_id, db_channel_id, file_key)

            if not file_key:
                pending_deliveries = await db.get_pending_deliveries(user_id)
                if pending_deliveries:
                    file_key = pending_deliveries[0]['file_key']
                else:
                    already_sent_count += 1
                    # We still need to clear their join request so they aren't processed again
                    await db.clear_user_requests(user_id, db_channel_id)
                    continue

            # Check if user is now a member (reuse prefetched channels)
            result = await check_membership(
                user_id,
                context,
                force_check=True,
                allow_private_requests=True,
                prefetched_channels=all_channels_data
            )
            
            if result['all_joined']:
                # User has all channels - send file
                log.info(f"✅ User {user_id} now has all channels. Sending file: {file_key}")
                
                shared_content = await get_shared_content(file_key)
                if shared_content:
                    try:
                        # Check if user is still pending
                        pending_deliveries = await db.get_pending_deliveries(user_id)
                        if pending_deliveries:
                            for delivery in pending_deliveries:
                                if delivery['file_key'] == file_key:
                                    await send_shared_content(context, user_id, shared_content)
                                    log.info(f"✅ Sent file {file_key} to user {user_id}")
                                    
                                    # Update user interaction
                                    await db.update_user_interaction(
                                        user_id=user_id,
                                        file_accessed=True
                                    )
                                    
                                    # Clean up
                                    await db.remove_pending_delivery(user_id, file_key)
                                    await db.clear_user_requests(user_id, db_channel_id)
                                    
                                    approved_count += 1
                                    break
                            else:
                                already_sent_count += 1
                        else:
                            already_sent_count += 1
                    except Exception as e:
                        log.error(f"❌ Failed to send file to user {user_id}: {e}")
                        failed_count += 1
                else:
                    log.error(f"❌ File {file_key} not found for user {user_id}")
                    failed_count += 1
            else:
                # Keep the private request recorded. A pending/manual approval
                # request counts as satisfied for private channels; any missing
                # public channels can still be joined later.
                log.info(f"User {user_id} still missing other channels. Keeping private request for {channel_name}.")
                waiting_count += 1
                
        except Exception as e:
            log.error(f"❌ Error processing user {user_id}: {e}")
            failed_count += 1
        
        # Delay between users to avoid Telegram flood errors
        await asyncio.sleep(0.5)
        
        # Update progress every 10 users
        processed = idx + 1
        if processed % 10 == 0 and processed < len(pending_requests):
            try:
                await status_msg.edit_text(
                    f"🔄 Processing {channel_name}... {processed}/{len(pending_requests)}\n"
                    f"✅ Sent: {approved_count} | ⏳ Waiting: {waiting_count} | ❌ Failed: {failed_count}"
                )
            except Exception:
                pass
    
    # Send completion message
    sent_msg = await update.message.reply_text(
        f"✅ *Approval Complete*\n\n"
        f"📢 Channel: {channel_name}\n"
        f"✅ Join requests approved: {join_approved_count} users\n"
        f"👤 Already members: {join_already_member_count} users\n"
        f"📁 Files sent: {approved_count} users\n"
        f"⏳ Waiting for other channels: {waiting_count} users\n"
        f"❌ Failed: {failed_count} users\n"
        f"⏭️ Already processed: {already_sent_count} users\n"
        f"📊 Total processed: {len(pending_requests)}",
        parse_mode="Markdown"
    )
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    
    # Delete the status message
    try:
        await status_msg.delete()
    except Exception as e:
        log.warning(f"Could not delete status message: {e}")

async def removechannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a required channel (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        # Check if replying to a forwarded message
        if update.message.reply_to_message:
            forwarded = update.message.reply_to_message
            chat = extract_chat_from_forward(forwarded)
            if chat:
                channel_id = str(chat.id)
                chat_username = chat.username or channel_id
                
                success = await db.remove_channel(chat_username)
                if not success and chat_username != channel_id:
                    success = await db.remove_channel(channel_id)
                    
                if success:
                    channels = await db.get_channels_with_details()
                    active_channels = [c for c in channels if c['is_active'] == 1]
                    if active_channels:
                        channel_list = "\n".join([f"{i+1}. {c['channel_name'] or c['channel_username']} ({c.get('channel_type', 'public')})" for i, c in enumerate(active_channels)])
                    else:
                        channel_list = "No channels required (all access granted)"
                    
                    sent_msg = await update.message.reply_text(
                        f"✅ *Channel removed successfully!*\n\n"
                        f"Removed: @{chat_username.replace('@', '')}\n\n"
                        f"📋 *Current required channels:*\n{channel_list}",
                        parse_mode="Markdown"
                    )
                    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                    return

        sent_msg = await update.message.reply_text(
            "❌ Usage: /removechannel <channel username>\n"
            "Example: /removechannel @my_channel\n"
            "Or reply to a forwarded message from the channel with /removechannel"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    channel = normalize_channel_username(context.args[0])
    
    success = await db.remove_channel(channel)
    
    if success:
        channels = await db.get_channels_with_details()
        active_channels = [c for c in channels if c['is_active'] == 1]
        
        if active_channels:
            channel_list = "\n".join([f"{i+1}. {c['channel_name'] or c['channel_username']} ({c.get('channel_type', 'public')})" for i, c in enumerate(active_channels)])
        else:
            channel_list = "No channels required (all access granted)"
        
        sent_msg = await update.message.reply_text(
            f"✅ *Channel removed successfully!*\n\n"
            f"Removed: @{channel.replace('@', '')}\n\n"
            f"📋 *Current required channels:*\n{channel_list}",
            parse_mode="Markdown"
        )
    else:
        sent_msg = await update.message.reply_text(f"❌ Channel not found or already removed.")
    
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def listchannels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all required channels (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    channels = await db.get_channels_with_details()
    
    if not channels:
        sent_msg = await update.message.reply_text(
            "📋 <b>No channels configured</b>\n\n"
            "Use /addchannel to add required channels.",
            parse_mode="HTML"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    active_channels = [c for c in channels if c['is_active'] == 1]
    inactive_channels = [c for c in channels if c['is_active'] == 0]
    
    msg = f"📋 <b>Channel Management</b>\n\n"
    msg += f"📢 <b>Active Channels ({len(active_channels)}):</b>\n"
    
    for i, ch in enumerate(active_channels):
        added_at = ch['added_at']
        if added_at:
            added_date = added_at.strftime('%Y-%m-%d') if hasattr(added_at, 'strftime') else str(added_at)[:10]
        else:
            added_date = 'Unknown'
        channel_type = ch.get('channel_type', 'public')
        type_emoji = "🔒" if channel_type == 'private' else "📢"
        display_name = html.escape(str(ch['channel_name'] or ch['channel_username']))
        channel_username = html.escape(str(ch['channel_username']))
        msg += f"{i+1}. {type_emoji} {display_name}\n"
        msg += f"   └ Username/ID: {channel_username}\n"
        msg += f"   └ Type: {html.escape(str(channel_type))}\n"
        msg += f"   └ Added: {added_date}\n"
        if channel_type == 'private' and ch.get('invite_link'):
            invite_link = html.escape(str(ch['invite_link']), quote=True)
            msg += f"   └ Invite Request: <a href=\"{invite_link}\">Link</a>\n"
    
    if inactive_channels:
        msg += f"\n⏸️ <b>Inactive Channels ({len(inactive_channels)}):</b>\n"
        for i, ch in enumerate(inactive_channels):
            display_name = html.escape(str(ch['channel_name'] or ch['channel_username']))
            channel_username = html.escape(str(ch['channel_username']))
            msg += f"{i+1}. {display_name} ({channel_username})\n"
    
    msg += f"\n💡 <b>Commands:</b>\n"
    msg += f"/addchannel - Add channel (reply to forwarded message for private)\n"
    msg += f"/approve - Approve pending requests (reply to forwarded message)\n"
    msg += f"/removechannel @channel - Remove channel\n"
    msg += f"/testchannels - Test bot access to all channels"
    
    sent_msg = await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def testchannels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test bot access to all required channels (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    channels_data = await db.get_channels_with_details()
    active_channels = [c for c in channels_data if c['is_active'] == 1]
    
    if not active_channels:
        sent_msg = await update.message.reply_text("📋 No channels configured.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    status_msg = await update.message.reply_text("🔍 Testing channel access...")
    
    results = []
    for ch in active_channels:
        channel = normalize_channel_username(ch['channel_username'])
        display_name = ch['channel_name'] or channel
        channel_type = ch.get('channel_type', 'public')
        
        try:
            channel_ref = telegram_chat_ref(channel)
            if channel_ref is None:
                results.append(f"❌ {display_name} - Invalid channel")
                continue

            chat = await context.bot.get_chat(channel_ref)
            bot_member = await context.bot.get_chat_member(channel_ref, context.bot.id)
            
            if bot_member.status in ["administrator", "creator"]:
                results.append(f"✅ {display_name} ({channel_type}) - Bot is admin")
            else:
                results.append(f"⚠️ {display_name} ({channel_type}) - Bot is member (not admin)")
                
        except Exception as e:
            error_msg = str(e)
            if "chat not found" in error_msg.lower():
                results.append(f"❌ {display_name} ({channel_type}) - Channel not found")
            elif "forbidden" in error_msg.lower():
                results.append(f"❌ {display_name} ({channel_type}) - Bot not in channel")
            else:
                results.append(f"❌ {display_name} ({channel_type}) - Error: {error_msg[:50]}")
    
    result_text = "🔍 *Channel Access Test*\n\n" + "\n".join(results)
    
    await status_msg.edit_text(result_text, parse_mode="Markdown")
    await schedule_message_deletion(context, status_msg.chat_id, status_msg.message_id)

# ============ EXISTING COMMAND HANDLERS ============

pending_upload_batches: Dict[Tuple[int, int], Dict[str, Any]] = {}
pending_upload_lock = asyncio.Lock()


def extract_upload_file_info(msg) -> Optional[dict]:
    """Return Telegram file metadata from a video/document message."""
    video = msg.video
    document = msg.document

    if video:
        filename = video.file_name or f"video_{int(time.time())}.mp4"
        return {
            "file_id": video.file_id,
            "file_name": filename,
            "mime_type": video.mime_type or "video/mp4",
            "is_video": True,
            "size": int(video.file_size or 0)
        }

    if document:
        filename = document.file_name or f"document_{int(time.time())}"
        ext = filename.lower().split('.')[-1] if '.' in filename else ""
        return {
            "file_id": document.file_id,
            "file_name": filename,
            "mime_type": document.mime_type or "",
            "is_video": ext in ALL_VIDEO_EXTS,
            "size": int(document.file_size or 0)
        }

    return None


def build_group_file_preview(files_info: List[dict], limit: int = 8) -> str:
    """Build a compact Markdown list of files in an upload batch."""
    lines = []
    for index, item in enumerate(files_info[:limit], start=1):
        name = clean_single_line(item.get("file_name"), "file")
        lines.append(f"{index}. {markdown_code(name)}")

    remaining = len(files_info) - limit
    if remaining > 0:
        lines.append(f"... and {remaining} more")

    return "\n".join(lines)


async def finalize_upload_batch(batch_key: Tuple[int, int], context: ContextTypes.DEFAULT_TYPE):
    """Save a pending admin upload batch after the quiet window expires."""
    try:
        await asyncio.sleep(UPLOAD_BATCH_WINDOW_SECONDS)
    except asyncio.CancelledError:
        return

    async with pending_upload_lock:
        batch = pending_upload_batches.pop(batch_key, None)

    if not batch:
        return

    files_info = batch["files"]
    reply_msg = batch["reply_msg"]

    try:
        if len(files_info) == 1:
            file_info = files_info[0]
            key = await db.save_file(file_info["file_id"], file_info)
            link = f"https://t.me/{bot_username}?start={key}"

            sent_msg = await reply_msg.reply_text(
                "Upload Successful\n\n"
                f"Name: {markdown_code(file_info['file_name'])}\n"
                f"Key: {markdown_code(key)}\n"
                "Storage: Metadata only (file stored on Telegram)\n\n"
                f"Link:\n{markdown_code(link)}",
                parse_mode="Markdown"
            )
        else:
            group_id = await db.save_file_group(files_info)
            group_key = f"g_{group_id}"
            link = f"https://t.me/{bot_username}?start={group_key}"
            preview = build_group_file_preview(files_info)

            sent_msg = await reply_msg.reply_text(
                "Batch Upload Successful\n\n"
                f"Files: {len(files_info)}\n"
                f"Key: {markdown_code(group_key)}\n"
                "Storage: Metadata only (files stored on Telegram)\n\n"
                f"{preview}\n\n"
                f"Single Link:\n{markdown_code(link)}",
                parse_mode="Markdown"
            )

        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.exception("Upload batch error")
        sent_msg = await reply_msg.reply_text(f"Upload failed: {str(e)[:200]}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)


async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Upload file handler (admin only), batching files sent together."""
    if update.effective_user.id != ADMIN_ID:
        return

    try:
        msg = update.message
        file_info = extract_upload_file_info(msg)

        if not file_info:
            sent_msg = await msg.reply_text("Send a video or document")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        batch_key = (msg.chat_id, update.effective_user.id)

        async with pending_upload_lock:
            batch = pending_upload_batches.setdefault(
                batch_key,
                {"files": [], "reply_msg": msg, "task": None}
            )
            batch["files"].append(file_info)
            batch["reply_msg"] = msg

            task = batch.get("task")
            if task and not task.done():
                task.cancel()

            batch["task"] = asyncio.create_task(finalize_upload_batch(batch_key, context))
            queued_count = len(batch["files"])

        log.info(f"Queued upload batch for chat {msg.chat_id}: {queued_count} file(s)")

    except Exception as e:
        log.exception("Upload error")
        sent_msg = await update.message.reply_text(f"Upload failed: {str(e)[:200]}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stats command (admin only) - Shows REAL database storage"""
    if update.effective_user.id != ADMIN_ID:
        return

    uptime = str(timedelta(seconds=int(time.time() - start_time)))
    file_count = await db.get_file_count()
    user_count = await db.get_user_count()
    channel_count = await db.get_channel_count()

    db_storage = await db.get_db_storage_usage()
    metadata_info = await db.get_metadata_storage_info()
    total_uploaded_bytes = await db.get_total_uploaded_size()
    
    def format_bytes(bytes_val):
        if bytes_val < 1024:
            return f"{bytes_val} B"
        elif bytes_val < 1024 * 1024:
            return f"{bytes_val/1024:.2f} KB"
        elif bytes_val < 1024 * 1024 * 1024:
            return f"{bytes_val/(1024*1024):.2f} MB"
        else:
            return f"{bytes_val/(1024*1024*1024):.2f} GB"
    
    total_uploaded = format_bytes(total_uploaded_bytes)

    files = await db.get_all_files()
    groups = await db.get_all_file_groups()
    total_access = (sum(f[5] for f in files) if files else 0) + (sum(g[5] for g in groups) if groups else 0)

    # Get pending request count
    pending_result = await db.fetchrow("SELECT COUNT(*) as count FROM private_channel_requests WHERE requested = TRUE")
    pending_requests = pending_result['count'] if pending_result else 0

    escaped_bot_username = bot_username.replace("_", "\\_")

    try:
        sent_msg = await update.message.reply_text(
            f"📊 *Bot Statistics*\n\n"
            f"🤖 Bot: @{escaped_bot_username}\n"
            f"⏱ Uptime: {uptime}\n\n"
            f"📁 *Files:* {file_count}\n"
            f"📦 *Total Uploaded:* {total_uploaded} (on Telegram)\n"
            f"👥 *Users:* {user_count}\n"
            f"📢 *Required Channels:* {channel_count}\n"
            f"👀 *Total Accesses:* {total_access}\n"
            f"🔄 *Pending Requests:* {pending_requests}\n\n"
            f"💾 *PostgreSQL Storage (REAL):*\n"
            f"   ├─ Total DB: {db_storage['total']}\n"
            f"   ├─ Tables: {db_storage['tables']}\n"
            f"   └─ Indexes: {db_storage['indexes']}\n\n"
            f"📊 *Metadata Stats:*\n"
            f"   ├─ Cache Entries: {metadata_info['cache_entries']}\n"
            f"   └─ Est. Metadata: {metadata_info['estimated_metadata']}\n\n"
            f"⏰ Auto-delete: {DELETE_AFTER//60} minutes\n"
            f"🧹 Auto Cleanup: DISABLED (Permanent storage)\n"
            f"📅 Auto Backup: Every 3 days\n"
            f"🔒 Private Channels: Supported",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    except Exception as e:
        log.error(f"Error in stats command: {e}", exc_info=True)
        try:
            sent_msg = await update.message.reply_text(
                f"📊 Bot Statistics\n\n"
                f"🤖 Bot: @{bot_username}\n"
                f"⏱ Uptime: {uptime}\n"
                f"📁 Files: {file_count}\n"
                f"📦 Total Uploaded: {total_uploaded} (on Telegram)\n"
                f"👥 Users: {user_count}\n"
                f"📢 Required Channels: {channel_count}\n"
                f"👀 Accesses: {total_access}\n"
                f"🔄 Pending Requests: {pending_requests}\n"
                f"💾 PostgreSQL: {db_storage['total']}\n"
                f"⏰ Auto-delete: {DELETE_AFTER//60} minutes\n"
                f"🧹 Auto Cleanup: DISABLED\n"
                f"📅 Auto Backup: Every 3 days\n"
                f"🔒 Private Channels: Supported"
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        except Exception as e2:
            log.error(f"Even fallback failed: {e2}")

async def listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List standalone and grouped share links (admin only)."""
    if update.effective_user.id != ADMIN_ID:
        return

    groups = await db.get_all_file_groups()
    files = await db.get_all_files()

    if not groups and not files:
        sent_msg = await update.message.reply_text("No files stored")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    total_links = len(groups) + len(files)
    chunks = [
        f"*Total Links: {total_links}*\n"
        f"Bundles: {len(groups)} | Single files: {len(files)}\n\n"
    ]

    for index, group in enumerate(groups, start=1):
        group_id, title, file_count, total_size, ts, access = group
        display_name = clean_single_line(title, f"Bundle {group_id}")
        group_key = f"g_{group_id}"
        link = f"https://t.me/{bot_username}?start={group_key}"
        access_count = access or 0
        access_word = "time" if access_count == 1 else "times"
        file_word = "file" if file_count == 1 else "files"

        entry = (
            f"*B{index}.* Bundle: {markdown_code(display_name)}\n"
            f"Files: {file_count} {file_word}\n"
            f"Link: [Open]({link}) | {markdown_code(link)}\n"
            f"Accessed: {access_count} {access_word}\n\n"
        )

        if len(chunks[-1]) + len(entry) > LISTFILES_MESSAGE_LIMIT:
            chunks.append(f"*Links Continued* ({total_links} total)\n\n")

        chunks[-1] += entry

    for index, file in enumerate(files, start=1):
        file_id, name, is_video, size, ts, access = file
        display_name = clean_single_line(name, "Unnamed file")
        link = f"https://t.me/{bot_username}?start={file_id}"
        access_count = access or 0
        access_word = "time" if access_count == 1 else "times"

        entry = (
            f"*{index}.* {markdown_code(display_name)}\n"
            f"Link: [Open]({link}) | {markdown_code(link)}\n"
            f"Accessed: {access_count} {access_word}\n\n"
        )

        if len(chunks[-1]) + len(entry) > LISTFILES_MESSAGE_LIMIT:
            chunks.append(f"*Links Continued* ({total_links} total)\n\n")

        chunks[-1] += entry

    for chunk in chunks:
        sent_msg = await update.message.reply_text(
            chunk,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        await asyncio.sleep(0.05)


async def deletefile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete file (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        sent_msg = await update.message.reply_text("❌ Usage: /deletefile <key>")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    key = context.args[0]
    deleted = await db.delete_file_group(key) if is_group_share_key(key) else await db.delete_file(key)

    if deleted:
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

# ============ BROADCAST FEATURE ============
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Preview and broadcast text, optional photo, and optional URL buttons."""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args and not update.message.reply_to_message:
        sent_msg = await update.message.reply_text(
            "❌ Usage:\n"
            "/broadcast your message\n\n"
            "Optional buttons:\n"
            "BUTTONS:\n"
            "Button Name | https://example.com\n\n"
            "For photo broadcasts, reply to a photo with /broadcast and your text."
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    command_body = get_command_body(update.message.text or update.message.caption or "", "broadcast")
    reply = update.message.reply_to_message
    photo_file_id = None
    reply_fallback_text = ""

    if reply:
        if reply.photo:
            photo_file_id = reply.photo[-1].file_id
            reply_fallback_text = reply.caption or ""
        elif reply.document and (reply.document.mime_type or "").startswith("image/"):
            sent_msg = await update.message.reply_text(
                "❌ Please send the image as a Telegram photo, then reply to that photo with /broadcast."
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
        elif not command_body:
            reply_fallback_text = reply.text or reply.caption or ""

    text_source = command_body or reply_fallback_text
    message_text, buttons, parse_error = parse_broadcast_content(text_source)

    if parse_error:
        sent_msg = await update.message.reply_text(f"❌ {parse_error}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    if command_body and not message_text and reply_fallback_text:
        message_text = reply_fallback_text.strip()

    validation_error = validate_broadcast_payload(message_text, photo_file_id)
    if validation_error:
        sent_msg = await update.message.reply_text(f"❌ {validation_error}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    status_msg = await update.message.reply_text(
        "📊 Fetching user list...",
    )

    user_ids = await db.get_all_user_ids(exclude_admin=True)
    total_users = len(user_ids)

    if total_users == 0:
        await status_msg.edit_text("❌ No users found to broadcast")
        return

    payload = {
        "text": message_text,
        "photo_file_id": photo_file_id,
        "buttons": buttons
    }
    broadcast_id = f"{int(time.time() * 1000)}_{update.message.message_id}"
    broadcast_payloads = context.chat_data.setdefault('broadcast_payloads', {})
    broadcast_payloads[broadcast_id] = payload

    try:
        await send_broadcast_preview(update, context, payload, broadcast_id)
        chunks_count = (total_users + BROADCAST_CHUNK_SIZE - 1) // BROADCAST_CHUNK_SIZE
        await status_msg.edit_text(
            f"🔍 Preview ready for {total_users} users.\n"
            f"📦 Will send in {chunks_count} chunk(s).\n"
            "Confirm or cancel using the buttons on the preview."
        )
    except Exception as e:
        broadcast_payloads.pop(broadcast_id, None)
        log.error(f"Error creating broadcast preview: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Failed to create preview: {str(e)[:150]}")

async def send_broadcast_preview(update: Update, context: ContextTypes.DEFAULT_TYPE, payload: dict, broadcast_id: str):
    """Send an admin preview with final content plus confirm/cancel buttons."""
    reply_markup = build_broadcast_reply_markup(
        payload["buttons"],
        include_actions=True,
        broadcast_id=broadcast_id
    )

    if payload["photo_file_id"]:
        return await context.bot.send_photo(
            chat_id=update.effective_chat.id,
            photo=payload["photo_file_id"],
            caption=payload["text"] or None,
            reply_markup=reply_markup
        )

    return await update.message.reply_text(
        payload["text"],
        reply_markup=reply_markup,
        disable_web_page_preview=True
    )

async def send_single_broadcast(context: ContextTypes.DEFAULT_TYPE, user_id: int, payload: dict):
    """Send one broadcast payload to one user."""
    reply_markup = build_broadcast_reply_markup(payload["buttons"])

    if payload["photo_file_id"]:
        await context.bot.send_photo(
            chat_id=user_id,
            photo=payload["photo_file_id"],
            caption=payload["text"] or None,
            reply_markup=reply_markup
        )
        return

    await context.bot.send_message(
        chat_id=user_id,
        text=payload["text"],
        reply_markup=reply_markup,
        disable_web_page_preview=True
    )

async def process_broadcast_chunks(context: ContextTypes.DEFAULT_TYPE, user_ids: list, payload: dict, status_msg):
    """Process broadcast in chunks."""
    total_users = len(user_ids)
    total_chunks = (total_users + BROADCAST_CHUNK_SIZE - 1) // BROADCAST_CHUNK_SIZE
    
    successful = 0
    failed = 0
    blocked = 0
    chunk_results = []
    
    start_time = time.time()
    
    for chunk_num in range(total_chunks):
        chunk_start = chunk_num * BROADCAST_CHUNK_SIZE
        chunk_end = min((chunk_num + 1) * BROADCAST_CHUNK_SIZE, total_users)
        chunk_users = user_ids[chunk_start:chunk_end]
        
        chunk_success = 0
        chunk_failed = 0
        chunk_blocked = 0
        
        await status_msg.edit_text(
            f"📦 *Processing Chunk {chunk_num + 1}/{total_chunks}*\n"
            f"👥 Users in this chunk: {len(chunk_users)}\n"
            f"✅ Sent so far: {successful}\n"
            f"❌ Failed: {failed}\n"
            f"🚫 Blocked: {blocked}\n"
            f"⏱️ Chunk {chunk_num + 1} starting...",
            parse_mode="Markdown"
        )
        
        for i, user_id in enumerate(chunk_users):
            try:
                await send_single_broadcast(context, user_id, payload)
                chunk_success += 1
                successful += 1
                
                if (i + 1) % 100 == 0:
                    await status_msg.edit_text(
                        f"📦 *Chunk {chunk_num + 1}/{total_chunks}* - {i + 1}/{len(chunk_users)} users\n"
                        f"✅ Sent: {successful}\n"
                        f"❌ Failed: {failed}\n"
                        f"🚫 Blocked: {blocked}",
                        parse_mode="Markdown"
                    )
                
                await asyncio.sleep(0.05)
                
            except Exception as e:
                error_str = str(e).lower()
                if "blocked" in error_str or "forbidden" in error_str or "deactivated" in error_str or "bot was blocked" in error_str:
                    chunk_blocked += 1
                    blocked += 1
                else:
                    chunk_failed += 1
                    failed += 1
                
                log.warning(f"Failed to send to {user_id}: {e}")
        
        chunk_results.append({
            'chunk': chunk_num + 1,
            'users': len(chunk_users),
            'success': chunk_success,
            'failed': chunk_failed,
            'blocked': chunk_blocked
        })
        
        await status_msg.edit_text(
            f"✅ *Chunk {chunk_num + 1}/{total_chunks} Complete*\n"
            f"📊 *Results for this chunk:*\n"
            f"✅ Sent: {chunk_success}\n"
            f"❌ Failed: {chunk_failed}\n"
            f"🚫 Blocked: {chunk_blocked}\n\n"
            f"📈 *Overall Progress:*\n"
            f"✅ Total Sent: {successful}\n"
            f"❌ Total Failed: {failed}\n"
            f"🚫 Total Blocked: {blocked}\n"
            f"📊 Completion: {(successful + failed + blocked)/total_users*100:.1f}%",
            parse_mode="Markdown"
        )
        
        if chunk_num < total_chunks - 1:
            await asyncio.sleep(2)
    
    elapsed_time = time.time() - start_time
    avg_speed = successful / elapsed_time if elapsed_time > 0 else 0
    
    summary = f"✅ *Broadcast Complete!*\n\n"
    summary += f"📊 *Final Statistics:*\n"
    summary += f"👥 Total Users: {total_users}\n"
    summary += f"✅ Successfully Sent: {successful}\n"
    summary += f"❌ Failed: {failed}\n"
    summary += f"🚫 Blocked/Deactivated: {blocked}\n"
    summary += f"📦 Chunks Processed: {total_chunks}\n"
    summary += f"⏱️ Time Taken: {elapsed_time:.1f} seconds\n"
    summary += f"⚡ Avg Speed: {avg_speed:.1f} users/sec\n\n"
    
    summary += f"📋 *Chunk Details:*\n"
    for chunk in chunk_results:
        summary += f"Chunk {chunk['chunk']}: {chunk['success']}✅/{chunk['failed']}❌/{chunk['blocked']}🚫\n"
    
    success_rate = (successful / total_users * 100) if total_users > 0 else 0
    summary += f"\n📈 Success Rate: {success_rate:.1f}%"
    
    await status_msg.edit_text(summary, parse_mode="Markdown")
    
    log.info(f"Broadcast completed: {successful}/{total_users} successful, {failed} failed, {blocked} blocked")

async def broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle broadcast confirmation callbacks"""
    query = update.callback_query
    if query.from_user.id != ADMIN_ID:
        await query.answer("Admin only", show_alert=True)
        return

    await query.answer()
    
    data = query.data
    action, _, broadcast_id = data.partition("|")
    broadcast_payloads = context.chat_data.get('broadcast_payloads', {})
    payload = broadcast_payloads.get(broadcast_id) if broadcast_id else None
    
    if action == "cancel_broadcast":
        try:
            await query.edit_message_reply_markup(
                reply_markup=build_broadcast_reply_markup(payload["buttons"]) if payload else None
            )
        except Exception as e:
            log.warning(f"Could not remove broadcast action buttons: {e}")

        if broadcast_id:
            broadcast_payloads.pop(broadcast_id, None)
        await query.message.reply_text("❌ Broadcast cancelled")
        return
    
    if action == "confirm_broadcast":
        try:
            if not payload:
                await query.message.reply_text("❌ This broadcast preview expired. Please create it again.")
                return

            try:
                await query.edit_message_reply_markup(
                    reply_markup=build_broadcast_reply_markup(payload["buttons"])
                )
            except Exception as e:
                log.warning(f"Could not remove broadcast action buttons: {e}")

            user_ids = await db.get_all_user_ids(exclude_admin=True)
            total_users = len(user_ids)

            if total_users == 0:
                if broadcast_id:
                    broadcast_payloads.pop(broadcast_id, None)
                await query.message.reply_text("❌ No users found to broadcast")
                return

            status_msg = await query.message.reply_text(
                f"🔄 Starting broadcast to {total_users} users...\n"
                f"📦 Processing in chunks of {BROADCAST_CHUNK_SIZE} users"
            )
            
            asyncio.create_task(process_broadcast_chunks(
                context, user_ids, payload, status_msg
            ))
            
            if broadcast_id:
                broadcast_payloads.pop(broadcast_id, None)
            
        except Exception as e:
            log.error(f"Error in broadcast confirmation: {e}")
            await query.message.reply_text(f"❌ Error starting broadcast: {str(e)[:100]}")

async def clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear membership cache (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if context.args:
        channel = normalize_channel_username(context.args[0])
        await db.clear_membership_cache(channel=channel)
        sent_msg = await update.message.reply_text(f"✅ Cache cleared for channel {channel}")
    else:
        await db.clear_membership_cache()
        sent_msg = await update.message.reply_text("✅ All cache cleared")
    
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

# ============ BACKUP AND IMPORT COMMANDS ============

async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manual backup command - Export database and send to admin"""
    if update.effective_user.id != ADMIN_ID:
        sent_msg = await update.message.reply_text("⛔ Admin only command")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    status_msg = await update.message.reply_text("🔄 Creating database backup... This may take a moment...")
    
    try:
        backup_data = await export_database_backup(update=update, context=context, send_to_admin=False)
        
        await status_msg.edit_text(f"✅ Backup created!\n📦 Total size: {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB\n\nSending files now...")
        
        for filename, content in backup_data.items():
            if content:
                file_bytes = io.BytesIO(content.encode('utf-8'))
                file_bytes.seek(0)
                
                file_emoji = "📋" if filename.endswith('.json') else "📄"
                
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                send_filename = f"backup_{timestamp}_{filename}"
                
                await context.bot.send_document(
                    chat_id=ADMIN_ID,
                    document=file_bytes,
                    filename=send_filename,
                    caption=f"{file_emoji} {filename}"
                )
                await asyncio.sleep(0.5)
        
        await status_msg.delete()
        
        summary = f"✅ *Full Database Backup Complete*\n\n"
        summary += f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        summary += f"💾 Total size: {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB\n\n"
        summary += f"💡 To restore: Send all backup files (CSV + JSON) and use `/import`\n"
        summary += f"📌 Bot accepts both exact filenames and timestamped filenames"
        
        sent_msg = await update.message.reply_text(summary, parse_mode="Markdown")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        
    except Exception as e:
        log.error(f"Backup error: {e}")
        await status_msg.edit_text(f"❌ Backup failed: {str(e)[:200]}")

async def backup_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check backup status and database health"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    file_count = await db.get_file_count()
    user_count = await db.get_user_count()
    channel_count = await db.get_channel_count()
    db_storage = await db.get_db_storage_usage()
    
    pending_result = await db.fetchrow("SELECT COUNT(*) as count FROM private_channel_requests WHERE requested = TRUE")
    pending_requests = pending_result['count'] if pending_result else 0
    
    status_msg = f"""
📊 *Database Status*

📈 *Data Summary:*
• Files: {file_count}
• Users: {user_count}
• Channels: {channel_count}
• DB Size: {db_storage.get('total', 'Unknown')}
• Pending Requests: {pending_requests}

💾 *Backup Ready:* Yes
• Use `/backup` to create backup
• Use `/import` to restore from backup
• Auto-backup: Every 3 days

🔒 *Private Channels:* Supported
• Users request to join via invite link
• Admin approves using `/approve`
• Files sent automatically after approval

⚠️ *Remember:* Free tier PostgreSQL expires after 30 days!
• Run `/backup` regularly
• Save backup files (CSV + JSON) to cloud storage
• Bot accepts both exact and timestamped filenames
"""
    
    sent_msg = await update.message.reply_text(status_msg, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def import_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check status of collected backup files for import"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    pending_files = context.user_data.get('pending_backup_files', {})
    if not pending_files:
        pending_files = context.user_data.get('pending_csv_files', {})
    
    if not pending_files:
        sent_msg = await update.message.reply_text(
            "📋 *No backup files collected*\n\n"
            "Send backup files (CSV + JSON) to start the import process.\n"
            "Required files: files.csv, users.csv, required_channels.csv\n"
            "Optional: metadata.json, membership_cache.csv, scheduled_deletions.csv, private_channel_requests.csv, pending_file_delivery.csv\n\n"
            "💡 *Tip:* Forward backup files directly to bot and they'll be auto-collected\n"
            "📌 Bot accepts both exact filenames and timestamped filenames",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    status = f"📋 *Collected Backup Files* ({len(pending_files)})\n\n"
    
    csv_files = {k: v for k, v in pending_files.items() if k.lower().endswith('.csv')}
    json_files = {k: v for k, v in pending_files.items() if k.lower().endswith('.json')}
    
    if csv_files:
        status += f"📄 *CSV Files:*\n"
        for filename, content in csv_files.items():
            lines = len(content.splitlines()) - 1
            status += f"✅ {markdown_code(filename)}: {lines} records\n"
    
    if json_files:
        status += f"\n📋 *JSON Files:*\n"
        for filename in json_files:
            status += f"✅ {markdown_code(filename)}\n"
    
    found_required = []
    missing_required = []
    
    for required in REQUIRED_IMPORT_FILES:
        found = False
        for filename in pending_files.keys():
            if canonical_backup_filename(filename) == required:
                found_required.append(required)
                found = True
                break
        if not found:
            missing_required.append(required)
    
    if missing_required:
        status += f"\n⚠️ *Missing required files:* {', '.join(missing_required)}\n"
    else:
        status += f"\n✅ All required files collected!\n"
    
    status += f"\n💡 Use `/import` to restore all collected files"
    
    sent_msg = await update.message.reply_text(status, parse_mode="Markdown")
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def import_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Import database from backup files - Admin only"""
    if update.effective_user.id != ADMIN_ID:
        sent_msg = await update.message.reply_text("⛔ Admin only command")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    collected_files = context.user_data.get('pending_backup_files', {})
    if not collected_files:
        collected_files = context.user_data.get('pending_csv_files', {})
    
    log.info(f"Import command - collected files: {list(collected_files.keys())}")
    
    if not update.message.reply_to_message and not collected_files:
        sent_msg = await update.message.reply_text(
            "📥 *Import Database from Backup*\n\n"
            "**Two ways to import:**\n\n"
            "1️⃣ *Forward backup files* directly to bot\n"
            "   • Bot will automatically collect them\n"
            "   • Supports CSV and JSON files\n"
            "   • Then use `/import` to restore\n\n"
            "2️⃣ *Reply to backup files* with `/import`\n"
            "   • Send all backup files (CSV + JSON)\n"
            "   • Reply to that message with `/import`\n\n"
            "**Required files:**\n"
            "• files.csv (or backup_*_files.csv)\n"
            "• users.csv (or backup_*_users.csv)\n"
            "• required_channels.csv (or backup_*_required_channels.csv)\n"
            "• metadata.json (recommended)\n\n"
            "📌 Bot accepts both exact and timestamped filenames\n"
            "⚠️ **Warning:** This will replace ALL existing data!",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    backup_files = {}
    
    if collected_files:
        backup_files = collected_files.copy()
        log.info(f"✅ Using {len(backup_files)} collected backup files for import: {list(backup_files.keys())}")
    elif update.message.reply_to_message:
        replied_msg = update.message.reply_to_message
        
        if replied_msg.document:
            doc = replied_msg.document
            doc_name = doc.file_name or ""
            lower_doc_name = doc_name.lower()
            if doc_name and (lower_doc_name.endswith('.csv') or lower_doc_name.endswith('.json')):
                try:
                    file = await context.bot.get_file(doc.file_id)
                    content = await file.download_as_bytearray()
                    backup_files[doc.file_name] = decode_backup_bytes(bytes(content))
                    log.info(f"Found backup file in replied message: {doc.file_name}")
                except Exception as e:
                    log.error(f"Error downloading backup file: {e}")
    
    if not backup_files:
        sent_msg = await update.message.reply_text(
            "❌ No backup files found.\n\n"
            "Please send backup files first (CSV + JSON), then use `/import`\n\n"
            f"📋 Currently collected files: {list(collected_files.keys()) if collected_files else 'None'}\n"
            f"Required: files.csv (or backup_*_files.csv), users.csv, required_channels.csv\n\n"
            f"💡 Tip: Forward backup files directly to bot\n"
            f"📌 Bot accepts both exact and timestamped filenames"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    required_table_patterns = {
        'files.csv': ['files'],
        'users.csv': ['users'],
        'required_channels.csv': ['required_channels'],
        'membership_cache.csv': ['membership_cache'],
        'scheduled_deletions.csv': ['scheduled_deletions'],
        'private_channel_requests.csv': ['private_channel_requests'],
        'pending_file_delivery.csv': ['pending_file_delivery'],
        'metadata.json': ['metadata']
    }
    
    normalized_files = {}
    missing_files = []
    
    for required_file, patterns in required_table_patterns.items():
        found = False
        for filename, content in backup_files.items():
            if canonical_backup_filename(filename) == required_file:
                normalized_files[required_file] = content
                found = True
                log.info(f"✅ Matched {filename} -> {required_file}")
                break
        
        if not found:
            if required_file in REQUIRED_IMPORT_FILES:
                missing_files.append(required_file)
    
    normalized_files, unmatched_files = normalize_backup_file_map(backup_files)
    missing_files = [filename for filename in REQUIRED_IMPORT_FILES if filename not in normalized_files]
    if unmatched_files:
        log.info(f"Ignoring unmatched backup files: {unmatched_files}")

    if missing_files:
        found_files = list(backup_files.keys())
        sent_msg = await update.message.reply_text(
            f"❌ Missing required files: {', '.join(missing_files)}\n\n"
            f"📁 Files found: {', '.join(markdown_code(name) for name in found_files) if found_files else 'None'}\n"
            f"📋 Required: files.csv, users.csv, required_channels.csv\n\n"
            f"💡 *Tip:* Make sure your backup includes all required CSV files.\n"
            f"📌 The bot supports both exact filenames (files.csv) and timestamped filenames (backup_*_files.csv)",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    backup_files = normalized_files
    log.info(f"✅ Normalized backup files: {list(backup_files.keys())}")
    
    required_files = REQUIRED_IMPORT_FILES
    missing_files = [f for f in required_files if f not in backup_files]
    
    if missing_files:
        found_files = list(backup_files.keys())
        sent_msg = await update.message.reply_text(
            f"❌ Missing required files: {', '.join(missing_files)}\n\n"
            f"📁 Files found: {', '.join(markdown_code(name) for name in found_files) if found_files else 'None'}\n"
            f"📋 Required: {', '.join(required_files)}\n\n"
            f"Please make sure your backup includes all required CSV files."
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    confirm_keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ YES, Import Now", callback_data="confirm_import"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel_import")
    ]])
    
    csv_files = {k: v for k, v in backup_files.items() if k.lower().endswith('.csv')}
    json_files = {k: v for k, v in backup_files.items() if k.lower().endswith('.json')}
    
    summary = f"📊 *Backup Files Found:*\n\n"
    
    if csv_files:
        summary += f"📄 *CSV Files ({len(csv_files)}):*\n"
        for filename, content in csv_files.items():
            lines = len(content.splitlines()) - 1
            summary += f"• {markdown_code(filename)}: {lines} records\n"
    
    if json_files:
        summary += f"\n📋 *JSON Files ({len(json_files)}):*\n"
        for filename in json_files:
            summary += f"• {markdown_code(filename)}\n"
    
    summary += f"\n⚠️ *WARNING:* This will REPLACE all existing data in your database!\n"
    summary += f"✅ Make sure this is the correct backup before proceeding."
    
    sent_msg = await update.message.reply_text(
        summary,
        parse_mode="Markdown",
        reply_markup=confirm_keyboard
    )
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    
    context.chat_data['import_backup_files'] = backup_files
    log.info(f"Stored {len(backup_files)} normalized backup files in chat_data for import confirmation")

async def import_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle import confirmation"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "cancel_import":
        await query.edit_message_text("❌ Import cancelled. No changes were made.")
        return
    
    if data == "confirm_import":
        backup_files = context.chat_data.get('import_backup_files', {})
        if not backup_files:
            backup_files = context.chat_data.get('import_csv_files', {})
            
        if not backup_files:
            backup_files = context.user_data.get('pending_backup_files', {})
            if not backup_files:
                backup_files = context.user_data.get('pending_csv_files', {})
            
        if not backup_files:
            await query.edit_message_text("❌ No backup files found. Please try again.")
            return
        
        await query.edit_message_text("🔄 Importing data... This may take a few moments...")
        
        try:
            backup_files, unmatched_files = normalize_backup_file_map(backup_files)
            missing_files = [filename for filename in REQUIRED_IMPORT_FILES if filename not in backup_files]
            if missing_files:
                await query.edit_message_text(
                    f"Missing required files: {', '.join(missing_files)}\n\n"
                    f"Please send all required CSV files again and run /import."
                )
                return

            csv_only_files = {k: v for k, v in backup_files.items() if k.lower().endswith('.csv')}
            
            result = await restore_from_backup(csv_only_files)
            
            if result["success"]:
                context.user_data.pop('pending_backup_files', None)
                context.user_data.pop('pending_csv_files', None)
                context.chat_data.pop('import_backup_files', None)
                context.chat_data.pop('import_csv_files', None)
                
                success_msg = f"✅ *Database Import Successful!*\n\n"
                success_msg += f"📊 *Import Summary:*\n"
                
                for table in result["tables_restored"]:
                    success_msg += f"• {table['table']}: {table['rows']} rows restored\n"
                
                success_msg += f"\n📦 *Total rows restored:* {result['total_rows']}\n"
                success_msg += f"🕐 *Completed at:* {result['timestamp']}\n\n"
                
                if result.get("warnings"):
                    success_msg += f"*Skipped optional files:*\n"
                    for warning in result["warnings"][:5]:
                        success_msg += f"- {warning}\n"
                    success_msg += "\n"

                if 'metadata.json' in backup_files:
                    success_msg += f"📋 *Metadata:* JSON file was included in backup\n"
                
                success_msg += f"\n💡 *Next steps:*\n"
                success_msg += f"• Run `/stats` to verify data\n"
                success_msg += f"• Run `/listchannels` to check channels\n"
                success_msg += f"• Broadcast will work with all restored users! ✅\n\n"
                success_msg += f"⚠️ *Remember:* Your database will still expire. Run `/backup` regularly!\n"
                success_msg += f"📅 Auto-backup runs every 3 days"
                
                await query.edit_message_text(success_msg)
                
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"🎉 Database restored from backup! {result['total_rows']} rows imported. All users restored for broadcasts!"
                )
                
            else:
                error_msg = f"❌ *Import Completed with Errors*\n\n"
                error_msg += f"⚠️ {len(result['errors'])} errors occurred:\n"
                for error in result['errors'][:10]:
                    error_msg += f"• {error}\n"
                
                if result["tables_restored"]:
                    error_msg += f"\n✅ Successfully restored tables:\n"
                    for table in result["tables_restored"]:
                        error_msg += f"• {table['table']}: {table['rows']} rows\n"
                
                await query.edit_message_text(error_msg)
                
        except Exception as e:
            log.error(f"Import callback error: {e}", exc_info=True)
            await query.edit_message_text(f"❌ Import failed: {str(e)[:200]}")

async def auto_backup_job(context: ContextTypes.DEFAULT_TYPE):
    """Automated backup job - runs every 3 days"""
    log.info("🔄 Running scheduled auto-backup (every 3 days)...")
    
    try:
        backup_data = await export_database_backup(update=None, context=context, send_to_admin=True)
        log.info(f"✅ Auto-backup completed. Size: {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB")
    except Exception as e:
        log.error(f"❌ Auto-backup failed: {e}")
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"⚠️ Auto-backup failed: {str(e)[:200]}\n\nPlease run manual backup with /backup"
            )
        except:
            pass

async def keepalive_check(bot):
    """Keep Telegram and PostgreSQL connections warm while Render is kept awake."""
    try:
        await asyncio.gather(
            bot.get_me(),
            db.get_file_count()
        )
        log.debug("Keepalive check completed")
    except Exception as e:
        log.warning(f"Keepalive check failed: {e}")

async def keepalive_job(context: ContextTypes.DEFAULT_TYPE):
    """JobQueue keepalive wrapper."""
    await keepalive_check(context.bot)

class BotOnlyContext:
    """Small context shim for fallback background tasks that only need context.bot."""
    def __init__(self, bot):
        self.bot = bot

async def keepalive_loop(bot):
    """Fallback keepalive loop for deployments without python-telegram-bot JobQueue."""
    await asyncio.sleep(60)

    while True:
        await keepalive_check(bot)
        await asyncio.sleep(KEEPALIVE_INTERVAL_SECONDS)

async def auto_backup_loop(bot):
    """Fallback auto-backup loop for deployments without python-telegram-bot JobQueue."""
    await asyncio.sleep(3600)

    while True:
        log.info("Running fallback scheduled auto-backup (every 3 days)...")

        try:
            backup_data = await export_database_backup(
                update=None,
                context=BotOnlyContext(bot),
                send_to_admin=True
            )
            log.info(f"Fallback auto-backup completed. Size: {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB")
        except Exception as e:
            log.error(f"Fallback auto-backup failed: {e}")
            try:
                await bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"Auto-backup failed: {str(e)[:200]}\n\nPlease run manual backup with /backup"
                )
            except Exception:
                pass

        await asyncio.sleep(259200)

# ============ MAIN ============
async def initialize_bot():
    """Initialize bot application"""
    global bot_app, bot_loop, bot_initialized

    if not BOT_TOKEN or not ADMIN_ID:
        log.error("Missing BOT_TOKEN or ADMIN_ID")
        return None

    log.info("Initializing database connection pool...")
    try:
        db._get_pool_sync()
        log.info("Database pool initialized.")
    except Exception as e:
        log.error(f"Failed to initialize database: {e}", exc_info=True)
        return None

    request = HTTPXRequest(connection_pool_size=40)
    application = Application.builder().token(BOT_TOKEN).request(request).build()
    
    await application.initialize()
    
    bot_loop = asyncio.get_running_loop()
    bot_app = application

    if application.job_queue:
        application.job_queue.run_repeating(
            cleanup_overdue_messages,
            interval=300,
            first=10
        )

        application.job_queue.run_repeating(
            keepalive_job,
            interval=KEEPALIVE_INTERVAL_SECONDS,
            first=60
        )
        log.info(f"Keepalive scheduled (every {KEEPALIVE_INTERVAL_SECONDS} seconds)")
        
        application.job_queue.run_repeating(
            auto_backup_job,
            interval=259200,
            first=3600
        )
        log.info("📅 Auto-backup scheduled (every 3 days)")

    else:
        asyncio.create_task(cleanup_overdue_messages_loop(application.bot))
        asyncio.create_task(keepalive_loop(application.bot))
        asyncio.create_task(auto_backup_loop(application.bot))
        log.warning("JobQueue unavailable; using fallback asyncio cleanup and auto-backup loops")

    application.add_error_handler(error_handler)
    
    # Add backup file handler (CSV + JSON)
    application.add_handler(
        MessageHandler(
            (filters.Document.FileExtension("csv") | filters.Document.FileExtension("json")) & filters.ChatType.PRIVATE,
            handle_forwarded_backup_files
        )
    )
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("listfiles", listfiles))
    application.add_handler(CommandHandler("deletefile", deletefile))
    application.add_handler(CommandHandler("users", users))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CommandHandler("clearcache", clearcache))
    
    # Channel management commands
    application.add_handler(CommandHandler("addchannel", addchannel))
    application.add_handler(CommandHandler("approve", approve))
    application.add_handler(CommandHandler("removechannel", removechannel))
    application.add_handler(CommandHandler("listchannels", listchannels))
    application.add_handler(CommandHandler("testchannels", testchannels))
    
    # Backup and import commands
    application.add_handler(CommandHandler("backup", backup_command))
    application.add_handler(CommandHandler("backup_status", backup_status))
    application.add_handler(CommandHandler("import", import_command))
    application.add_handler(CommandHandler("import_status", import_status))

    # Add callback handlers
    application.add_handler(CallbackQueryHandler(callback_handler, pattern="^(status\\|)|^(noop)$"))
    application.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^(confirm_broadcast|cancel_broadcast)(\\|.+)?$"))
    application.add_handler(CallbackQueryHandler(import_callback, pattern="^(confirm_import|cancel_import)$"))

    # Add upload handler (admin only)
    upload_filter = (filters.VIDEO | (filters.Document.ALL & ~filters.Document.FileExtension("csv") & ~filters.Document.FileExtension("json")))
    application.add_handler(
        MessageHandler(upload_filter & filters.User(ADMIN_ID) & filters.ChatType.PRIVATE, upload)
    )

    # Add chat member handler for auto-send on join
    application.add_handler(ChatMemberHandler(
        chat_member_handler,
        ChatMemberHandler.CHAT_MEMBER
    ))
    
    # Add chat join request handler
    from telegram.ext import ChatJoinRequestHandler
    application.add_handler(ChatJoinRequestHandler(chat_join_request_handler))

    # Set webhook
    render_url = os.environ.get('RENDER_EXTERNAL_URL')
    if not render_url:
        render_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME', 'localhost')}"

    webhook_url = f"{render_url}/webhook"
    log.info(f"Setting webhook to: {webhook_url}")

    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
        await application.bot.set_webhook(
            url=webhook_url,
            allowed_updates=Update.ALL_TYPES,
            max_connections=40,
            drop_pending_updates=True
        )
        log.info("✅ Webhook set successfully")
    except Exception as e:
        log.error(f"Failed to set webhook: {e}", exc_info=True)
        return None

    await application.start()
    await cleanup_overdue_messages(BotOnlyContext(application.bot))

    bot_initialized = True
    
    log.info("🤖 Bot initialized and ready via webhook")
    log.info(f"📁 Files in database: {await db.get_file_count()}")
    log.info(f"👥 Users in database: {await db.get_user_count()}")
    log.info(f"📢 Required channels: {await db.get_channel_count()}")
    log.info(f"🧹 Auto cleanup: DISABLED (Permanent storage)")
    log.info(f"📅 Auto backup: Enabled (every 3 days)")
    log.info(f"🔒 Private channels: Supported (auto-invite + auto-approve)")
    log.info(f"📋 Backup file support: CSV + JSON (exact and timestamped filenames)")
    log.info(f"✅ Python Version: {sys.version}")

    return application

async def main_async():
    """Async main function"""
    global bot_app
    
    bot_app = await initialize_bot()
    
    if bot_app is None:
        log.error("Failed to initialize bot. Exiting.")
        return

    log.info("Bot is running. Waiting for webhook events...")
    
    while True:
        await asyncio.sleep(3600)

def main():
    """Main function"""
    print("\n" + "=" * 60)
    print("🤖 TELEGRAM FILE BOT - COMPLETE VERSION")
    print("=" * 60)
    print(f"✅ Bot: @{bot_username}")
    print(f"✅ Admin: {ADMIN_ID}")
    print(f"✅ Database: Render PostgreSQL")
    print(f"✅ Auto Cleanup: DISABLED (Permanent storage)")
    print(f"✅ Storage: Metadata only (Files on Telegram)")
    print(f"✅ Backup: Enabled (manual + auto every 3 days)")
    print(f"✅ Import: Enabled (restore from CSV + JSON)")
    print(f"✅ File Handler: Auto-detect CSV and JSON backup files")
    print(f"✅ Filename Support: Exact and timestamped filenames")
    print(f"✅ Private Channels: Supported (auto-invite + auto-approve)")
    print(f"✅ Auto-Send: Files sent automatically after joining all channels")
    print(f"✅ Python Version: {sys.version}")
    print("=" * 60 + "\n")

    flask_thread = threading.Thread(target=run_flask_thread, daemon=True)
    flask_thread.start()
    log.info("Flask thread started")

    time.sleep(2)

    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
    except Exception as e:
        log.error(f"Fatal error in main loop: {e}", exc_info=True)
    finally:
        log.info("Shutting down...")
        if bot_app:
            asyncio.run(bot_app.stop())
            asyncio.run(bot_app.shutdown())
        asyncio.run(db.close_pool())
        print("Shutdown complete.")

if __name__ == "__main__":
    main()
