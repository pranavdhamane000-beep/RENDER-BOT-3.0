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

# ================= HEALTH SERVER FOR RENDER =================
from flask import Flask, render_template_string, jsonify, request
app = Flask(__name__)

# Global variables for web dashboard
start_time = time.time()
bot_username = "itcchiuchiha_bot"
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


def telegram_chat_ref(channel: Any):
    """Return a Telegram API chat reference for a username or numeric chat id."""
    clean_channel = normalize_channel_username(channel)
    if re.fullmatch(r"-?\d+", clean_channel):
        return int(clean_channel)
    return f"@{clean_channel}" if clean_channel else None

# ===========================================================
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    MessageHandler,
    filters,
    ContextTypes,
    JobQueue
)
from telegram.request import HTTPXRequest

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
# REMOVED AUTO CLEANUP - Set to 0 (disabled)
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
    # Add friendly names for your channels here
    # Format: "channel_username": "Display Name"
    "A_Knight_of_the_Seven_Kingdoms_t": "Channel 1",
    "A_Knight_of_the_Seven_Kingdoms_r": "Main Channel",
    "A_Knight_of_the_Seven_Kingdoms_y": "Backup Channel",
    "your_movies_web": "Movies Channel",
    # Add more as needed
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
                self.pool = psycopg2.pool.ThreadedConnectionPool(
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
                added_by BIGINT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                position INTEGER DEFAULT 0
            )
        ''')

        cur.execute("ALTER TABLE required_channels ADD COLUMN IF NOT EXISTS channel_type TEXT DEFAULT 'normal'")
        cur.execute("ALTER TABLE required_channels ADD COLUMN IF NOT EXISTS chat_id BIGINT")
        cur.execute("ALTER TABLE required_channels ADD COLUMN IF NOT EXISTS invite_link TEXT")
        cur.execute("UPDATE required_channels SET channel_type = 'normal' WHERE channel_type IS NULL")

        cur.execute('''
            CREATE TABLE IF NOT EXISTS join_request_satisfactions (
                user_id BIGINT NOT NULL,
                channel TEXT NOT NULL,
                requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, channel)
            )
        ''')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS pending_share_requests (
                user_id BIGINT PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                share_key TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        cur.execute('CREATE INDEX IF NOT EXISTS idx_channels_type ON required_channels(channel_type)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_join_request_channel ON join_request_satisfactions(channel)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_pending_share_timestamp ON pending_share_requests(timestamp)')
        
        # Insert default channels if table is empty
        cur.execute("SELECT COUNT(*) FROM required_channels")
        count = cur.fetchone()[0]
        
        if count == 0 and DEFAULT_CHANNELS:
            for i, channel in enumerate(DEFAULT_CHANNELS):
                if channel:  # Only if not empty
                    # Get friendly name from dictionary or use default
                    friendly_name = CHANNEL_NAMES.get(channel, f"Channel {i+1}")
                    cur.execute('''
                        INSERT INTO required_channels (channel_username, channel_name, position, is_active)
                        VALUES (%s, %s, %s, 1)
                        ON CONFLICT (channel_username) DO NOTHING
                    ''', (channel, friendly_name, i))
                    log.info(f"Added default channel: {channel} as '{friendly_name}'")
        
        conn.commit()

    @asynccontextmanager
    async def get_db_connection(self):
        """Asynchronous context manager to get and return a connection from the pool."""
        await self._get_pool_async()
        conn = await asyncio.to_thread(self._get_valid_connection_sync)
        try:
            yield conn
        finally:
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

    # ============ Get database storage usage (REAL PostgreSQL size) ============
    async def get_db_storage_usage(self) -> Dict[str, Any]:
        """Get PostgreSQL database storage usage (REAL disk usage)"""
        try:
            # Query to get database size
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
                
                # Convert to human readable format
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
        """Get detailed metadata storage info (what's actually in DB)"""
        try:
            # Get row counts for main tables
            files_count = await self.get_file_count()
            users_count = await self.get_user_count()
            
            # Get cache size
            cache_result = await self.fetchrow("SELECT COUNT(*) as count FROM membership_cache")
            cache_count = cache_result['count'] if cache_result else 0
            
            # Get channels count
            channels_count = await self.get_channel_count()
            
            # Estimate metadata size (approximate)
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

    # ============ Get total size of files uploaded (for info only) ============
    async def get_total_uploaded_size(self) -> int:
        """Get total size of all files uploaded (sum of file_size column)"""
        result = await self.fetchrow("SELECT COALESCE(SUM(file_size), 0) as total FROM files")
        return result['total'] if result else 0

    # ============ Channel management methods ============
    async def get_required_channels(self, active_only: bool = True) -> List[str]:
        """Get list of all required channels"""
        if active_only:
            rows = await self.fetchall("SELECT channel_username FROM required_channels WHERE is_active = 1 ORDER BY position, id")
        else:
            rows = await self.fetchall("SELECT channel_username FROM required_channels ORDER BY position, id")
        
        return [row['channel_username'] for row in rows]
    
    async def get_channels_with_details(self) -> List[Dict]:
        """Get channels with all details for listing"""
        rows = await self.fetchall('''
            SELECT id, channel_username, channel_name, added_at, is_active, position,
                   COALESCE(channel_type, 'normal') as channel_type, chat_id, invite_link
            FROM required_channels
            ORDER BY position, id
        ''')
        return [dict(row) for row in rows]
    
    async def add_channel(self, channel_username: str, added_by: int, channel_name: str = None) -> bool:
        """Add a new required channel"""
        clean_username = normalize_channel_username(channel_username)
        
        if not clean_username:
            return False
        
        # Use friendly name from dictionary if available
        friendly_name = channel_name or CHANNEL_NAMES.get(clean_username, clean_username)
        
        # Get max position for new channel
        result = await self.fetchrow("SELECT COALESCE(MAX(position), -1) + 1 as next_pos FROM required_channels")
        next_pos = result['next_pos'] if result else 0
        
        try:
            await self.execute_and_commit('''
                INSERT INTO required_channels (channel_username, channel_name, added_by, position, is_active)
                VALUES (%s, %s, %s, %s, 1)
                ON CONFLICT (channel_username) DO UPDATE
                SET is_active = 1,
                    added_by = EXCLUDED.added_by,
                    channel_name = COALESCE(EXCLUDED.channel_name, required_channels.channel_name)
            ''', (clean_username, friendly_name, added_by, next_pos))
            
            log.info(f"Channel added: @{clean_username} as '{friendly_name}' by user {added_by}")
            return True
        except Exception as e:
            log.error(f"Error adding channel: {e}")
            return False

    async def add_request_channel(self, chat_id: int, button_name: str, invite_link: str, added_by: int, username: str = None) -> bool:
        """Add a required channel that is satisfied by submitting a join request."""
        channel_key = str(chat_id)
        friendly_name = clean_single_line(button_name, "Join Channel")
        clean_username = normalize_channel_username(username) if username else channel_key

        result = await self.fetchrow("SELECT COALESCE(MAX(position), -1) + 1 as next_pos FROM required_channels")
        next_pos = result['next_pos'] if result else 0

        try:
            if clean_username and clean_username != channel_key:
                rowcount = await self.execute_and_commit('''
                    UPDATE required_channels
                    SET is_active = 1,
                        channel_name = %s,
                        added_by = %s,
                        channel_type = 'request',
                        chat_id = %s,
                        invite_link = %s
                    WHERE channel_username = %s
                ''', (friendly_name, added_by, int(chat_id), invite_link, clean_username))

                if rowcount > 0:
                    await self.execute_and_commit('''
                        UPDATE required_channels
                        SET is_active = 0
                        WHERE channel_username = %s
                    ''', (channel_key,))
                    log.info(f"Converted normal channel @{clean_username} to request mode")
                    return True

            await self.execute_and_commit('''
                INSERT INTO required_channels
                    (channel_username, channel_name, added_by, position, is_active, channel_type, chat_id, invite_link)
                VALUES (%s, %s, %s, %s, 1, 'request', %s, %s)
                ON CONFLICT (channel_username) DO UPDATE
                SET is_active = 1,
                    channel_name = EXCLUDED.channel_name,
                    added_by = EXCLUDED.added_by,
                    channel_type = 'request',
                    chat_id = EXCLUDED.chat_id,
                    invite_link = EXCLUDED.invite_link
            ''', (channel_key, friendly_name, added_by, next_pos, int(chat_id), invite_link))

            if clean_username != channel_key:
                await self.execute_and_commit('''
                    UPDATE required_channels
                    SET is_active = 0,
                        chat_id = %s
                    WHERE channel_username = %s AND channel_type = 'normal'
                ''', (int(chat_id), clean_username))

            log.info(f"Request channel added: {channel_key} as '{friendly_name}' by user {added_by}")
            return True
        except Exception as e:
            log.error(f"Error adding request channel: {e}")
            return False
    
    async def remove_channel(self, channel_username: str) -> bool:
        """Remove a required channel by username, row id, chat id, or button name."""
        identifier = str(channel_username or "").strip()
        clean_username = normalize_channel_username(identifier)

        row = await self.fetchrow('''
            SELECT channel_username FROM required_channels
            WHERE channel_username = %s
               OR (%s ~ '^[0-9]+$' AND id = %s::integer)
               OR LOWER(channel_name) = LOWER(%s)
            ORDER BY is_active DESC, id
            LIMIT 1
        ''', (clean_username, identifier, identifier if identifier.isdigit() else None, identifier))

        if not row:
            return False

        channel_key = row['channel_username']

        rowcount = await self.execute_and_commit('''
            UPDATE required_channels SET is_active = 0
            WHERE channel_username = %s
        ''', (channel_key,))
        
        if rowcount > 0:
            log.info(f"Channel removed: {channel_key}")
            
            # Also clear cached/satisfied state for this channel
            await self.execute_and_commit("DELETE FROM membership_cache WHERE channel = %s", (channel_key,))
            await self.execute_and_commit("DELETE FROM join_request_satisfactions WHERE channel = %s", (channel_key,))
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

    async def get_request_channel_by_chat_id(self, chat_id: int) -> Optional[Dict]:
        """Find an active request-mode channel by Telegram chat id."""
        row = await self.fetchrow('''
            SELECT id, channel_username, channel_name, chat_id, invite_link
            FROM required_channels
            WHERE is_active = 1 AND COALESCE(channel_type, 'normal') = 'request' AND chat_id = %s
            LIMIT 1
        ''', (int(chat_id),))
        return dict(row) if row else None

    async def mark_join_request_satisfied(self, user_id: int, channel: str):
        """Persist that a user submitted a join request for a request-mode channel."""
        await self.execute_and_commit('''
            INSERT INTO join_request_satisfactions (user_id, channel, requested_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, channel) DO UPDATE
            SET requested_at = EXCLUDED.requested_at
        ''', (int(user_id), str(channel)))

    async def has_join_request_satisfaction(self, user_id: int, channel: str) -> bool:
        """Return whether the user has submitted a join request for this channel."""
        row = await self.fetchrow('''
            SELECT 1 FROM join_request_satisfactions
            WHERE user_id = %s AND channel = %s
        ''', (int(user_id), str(channel)))
        return bool(row)

    async def save_pending_share_request(self, user_id: int, chat_id: int, share_key: str):
        """Remember the share key a user is trying to unlock."""
        await self.execute_and_commit('''
            INSERT INTO pending_share_requests (user_id, chat_id, share_key, timestamp)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO UPDATE
            SET chat_id = EXCLUDED.chat_id,
                share_key = EXCLUDED.share_key,
                timestamp = EXCLUDED.timestamp
        ''', (int(user_id), int(chat_id), str(share_key)))

    async def get_pending_share_request(self, user_id: int) -> Optional[Dict]:
        """Get the latest pending share key for a user."""
        row = await self.fetchrow('''
            SELECT user_id, chat_id, share_key, timestamp
            FROM pending_share_requests
            WHERE user_id = %s
        ''', (int(user_id),))
        return dict(row) if row else None

    async def clear_pending_share_request(self, user_id: int):
        """Clear a user's pending share key."""
        await self.execute_and_commit(
            "DELETE FROM pending_share_requests WHERE user_id = %s",
            (int(user_id),)
        )

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
        # Return as list of tuples for compatibility with original code
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
    """Send one stored Telegram file as video when playable, otherwise document."""
    filename = file_info.get('file_name', 'file')
    ext = filename.lower().split('.')[-1] if '.' in filename else ""

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
            await asyncio.sleep(0.15)

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
        member = await bot.get_chat_member(chat_id=channel_ref, user_id=user_id)
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
            # Fail closed so a verification problem cannot bypass force-join.
            return False, error_msg
        elif "forbidden" in error_msg:
            log.error(f"Bot can't access @{clean_channel}")
            return False, error_msg
        else:
            # Any unexpected verification error should block access rather
            # than silently bypass the force-join requirement.
            return False, error_msg

async def check_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE, force_check: bool = False) -> Dict[str, Any]:
    """Check if user is member of all required channels"""
    bot = context.bot

    result = {
        "all_joined": False,
        "missing_channels": [],
        "missing_channel_names": [],
        "missing_channel_data": [],
        "channel_status": {},
        "verification_errors": []
    }

    # Get all active required channels with details (ALWAYS fetch fresh from DB)
    log.info(f"📋 Fetching active channels from database for user {user_id}")
    channels_data = await db.get_channels_with_details()
    active_channels = [c for c in channels_data if c['is_active'] == 1]
    request_chat_ids = {
        c.get('chat_id')
        for c in active_channels
        if (c.get('channel_type') or 'normal') == 'request' and c.get('chat_id') is not None
    }
    active_channels = [
        c for c in active_channels
        if not (
            (c.get('channel_type') or 'normal') == 'normal'
            and c.get('chat_id') in request_chat_ids
        )
    ]
    
    log.info(f"📋 Found {len(active_channels)} active channels: {[c['channel_username'] for c in active_channels]}")
    
    if not active_channels:
        # No channels required - auto approve
        log.info(f"✅ No channels required for user {user_id} - auto approving")
        result["all_joined"] = True
        return result

    if force_check:
        log.info(f"🔄 Force check - clearing cache for user {user_id}")
        await db.clear_membership_cache(user_id)

    # Check each channel
    for channel_data in active_channels:
        channel = channel_data['channel_username']
        channel_name = channel_data['channel_name'] or channel
        channel_type = channel_data.get('channel_type') or 'normal'

        if channel_type == "request":
            is_satisfied = await db.has_join_request_satisfaction(user_id, channel)
            result["channel_status"][channel] = {
                'is_member': is_satisfied,
                'name': channel_name,
                'type': channel_type,
                'verification_error': None
            }

            if not is_satisfied:
                log.info(f"User {user_id} has not submitted join request for {channel}")
                result["missing_channels"].append(channel)
                result["missing_channel_names"].append(channel_name)
                result["missing_channel_data"].append(channel_data)
            else:
                log.info(f"User {user_id} satisfied request channel {channel}")

            continue
        
        log.info(f"🔍 Checking user {user_id} in channel @{channel} ({channel_name})")
        is_member, verification_error = await check_user_in_channel(bot, channel, user_id, force_check)
        
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
                log.warning(
                    f"Skipping unverifiable channel @{channel} for user {user_id}: {verification_error}"
                )
                continue
        
        if not is_member:
            log.info(f"❌ User {user_id} NOT in channel @{channel}")
            result["missing_channels"].append(channel)
            result["missing_channel_names"].append(channel_name)
            result["missing_channel_data"].append(channel_data)
        else:
            log.info(f"✅ User {user_id} IS in channel @{channel}")

    result["all_joined"] = len(result["missing_channels"]) == 0
    log.info(f"📊 Final result for user {user_id}: all_joined={result['all_joined']}, missing={result['missing_channel_names']}")
    
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

    # Process update in bot's event loop
    future = asyncio.run_coroutine_threadsafe(
        process_update(update_data, bot_app),
        bot_loop
    )
    
    try:
        # Wait a bit for the update to be queued
        future.result(timeout=WEBHOOK_PROCESS_TIMEOUT_SECONDS)
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
    "required_channels": ["id", "channel_username", "channel_name", "added_by",
                          "added_at", "is_active", "position", "channel_type",
                          "chat_id", "invite_link"],
    "join_request_satisfactions": ["user_id", "channel", "requested_at"],
    "pending_share_requests": ["user_id", "chat_id", "share_key", "timestamp"],
    "scheduled_deletions": ["chat_id", "message_id", "scheduled_time", "delete_after"]
}

IMPORT_TABLE_ORDER = [
    "required_channels",
    "users",
    "files",
    "file_groups",
    "file_group_items",
    "membership_cache",
    "join_request_satisfactions",
    "pending_share_requests",
    "scheduled_deletions"
]

REQUIRED_IMPORT_FILES = ["files.csv", "users.csv", "required_channels.csv"]
OPTIONAL_IMPORT_FILES = [
    "file_groups.csv",
    "file_group_items.csv",
    "membership_cache.csv",
    "join_request_satisfactions.csv",
    "pending_share_requests.csv",
    "scheduled_deletions.csv",
    "metadata.json"
]
CANONICAL_BACKUP_FILES = REQUIRED_IMPORT_FILES + OPTIONAL_IMPORT_FILES

INTEGER_IMPORT_COLUMNS = {
    "id", "is_video", "access_count", "total_interactions",
    "total_files_accessed", "is_active", "position", "delete_after",
    "added_by", "message_id", "group_id", "file_db_id", "file_count"
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


def channel_button_url(channel_data: Dict[str, Any]) -> Optional[str]:
    """Return the URL users should open for a required channel."""
    channel_type = channel_data.get("channel_type") or "normal"
    if channel_type == "request":
        return channel_data.get("invite_link")

    channel = normalize_channel_username(channel_data.get("channel_username"))
    if not channel or re.fullmatch(r"-?\d+", channel):
        return None
    return f"https://t.me/{channel}"


def build_join_keyboard(
    missing_channel_data: List[Dict[str, Any]],
    callback_data: Optional[str] = None,
    include_check_button: bool = False
) -> InlineKeyboardMarkup:
    """Build join buttons, optionally with a manual check button."""
    keyboard = []

    for index, channel_data in enumerate(missing_channel_data, start=1):
        channel_name = channel_data.get("channel_name") or f"Channel {index}"
        url = channel_button_url(channel_data)
        if url:
            keyboard.append([InlineKeyboardButton(
                f"Join {channel_name}",
                url=url
            )])

    if include_check_button and callback_data:
        keyboard.append([InlineKeyboardButton(
            "Check Again",
            callback_data=callback_data
        )])

    return InlineKeyboardMarkup(keyboard)


def extract_forwarded_channel(message) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """Extract channel id, title, and username from a forwarded channel message."""
    if not message:
        return None, None, None

    origin = getattr(message, "forward_origin", None)
    origin_chat = getattr(origin, "chat", None)
    if origin_chat:
        return origin_chat.id, getattr(origin_chat, "title", None), getattr(origin_chat, "username", None)

    legacy_chat = getattr(message, "forward_from_chat", None)
    if legacy_chat:
        return legacy_chat.id, getattr(legacy_chat, "title", None), getattr(legacy_chat, "username", None)

    return None, None, None


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
        # Fetch all data from table
        rows = await db.fetchall(f"SELECT * FROM {table_name}")
        
        if not rows:
            return None
        
        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write headers
        writer.writerow(columns)
        
        # Write data rows
        for row in rows:
            # Convert row dict to list in column order
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
    
    # Export each table
    for table_name, columns in BACKUP_TABLE_COLUMNS.items():
        try:
            csv_content = await export_table_to_csv(table_name, columns)
            
            if csv_content:
                backup_data[f"{table_name}.csv"] = csv_content
                row_count = len(csv_content.splitlines()) - 1  # Subtract header row
                backup_info["tables_exported"].append(table_name)
                backup_info["row_counts"][table_name] = max(0, row_count)
                log.info(f"✅ Exported {table_name}: {row_count} rows")
            else:
                # Create empty CSV with headers for tables that exist but have no data
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(columns)
                backup_data[f"{table_name}.csv"] = output.getvalue()
                backup_info["tables_exported"].append(table_name)
                backup_info["row_counts"][table_name] = 0
                log.info(f"📭 Table {table_name} is empty")
                
        except Exception as e:
            log.error(f"❌ Failed to export {table_name}: {e}")
    
    # Create metadata file
    metadata = {
        "export_info": backup_info,
        "bot_config": {
            "bot_username": bot_username,
            "delete_after_seconds": DELETE_AFTER,
            "auto_cleanup_days": AUTO_CLEANUP_DAYS,
            "export_timestamp": datetime.now().isoformat()
        }
    }
    
    # Add metadata as JSON
    backup_data["metadata.json"] = json.dumps(metadata, indent=2)
    backup_info["metadata_created"] = True
    
    if send_to_admin and context:
        # Send backup to admin in multiple messages if needed
        await send_backup_to_admin(context, backup_data, backup_info)
    
    return backup_data

async def send_backup_to_admin(context: ContextTypes.DEFAULT_TYPE, backup_data: Dict[str, str], backup_info: Dict[str, Any]):
    """Send backup files to admin"""
    try:
        # First, send summary message
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
        summary += f"📌 The bot now accepts both exact filenames (files.csv) and timestamped filenames"
        
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=summary
        )
        
        # Send each file as a document (both CSV and JSON)
        for filename, content in backup_data.items():
            if content and len(content) > 0:
                # Create file in memory
                file_bytes = io.BytesIO(content.encode('utf-8'))
                file_bytes.seek(0)
                
                # Determine file type emoji
                file_emoji = "📋" if filename.endswith('.json') else "📄"
                
                # Use timestamped filename for uniqueness but keep base name recognizable
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                send_filename = f"backup_{timestamp}_{filename}"
                
                # Send file
                await context.bot.send_document(
                    chat_id=ADMIN_ID,
                    document=file_bytes,
                    filename=send_filename,
                    caption=f"{file_emoji} {filename} - {len(content.splitlines())} lines"
                )
                
                # Small delay to avoid rate limiting
                await asyncio.sleep(0.5)
        
        # Send final instructions
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

async def import_csv_to_table_legacy(table_name: str, csv_content: str, truncate_first: bool = True) -> Dict[str, Any]:
    """Import CSV data to a specific table"""
    result = {
        "success": False,
        "rows_imported": 0,
        "errors": [],
        "table": table_name
    }
    
    try:
        # Parse CSV content
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(csv_reader)
        
        if not rows:
            result["success"] = True
            result["rows_imported"] = 0
            return result
        
        async with db.get_db_connection() as conn:
            def _import():
                with conn.cursor() as cur:
                    # Optionally truncate table first
                    if truncate_first:
                        cur.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
                        log.info(f"🗑️ Truncated table {table_name}")
                    
                    # Get column names from CSV header
                    columns = list(rows[0].keys())
                    placeholders = ','.join(['%s'] * len(columns))
                    columns_str = ','.join(columns)
                    
                    # Prepare INSERT statement
                    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                    
                    # Insert each row
                    imported = 0
                    for row in rows:
                        try:
                            # Convert values to appropriate types
                            values = []
                            for col in columns:
                                val = row[col]
                                # Handle NULL values
                                if val == '' or val == 'NULL':
                                    values.append(None)
                                else:
                                    # Try to convert numeric values
                                    if col in ['id', 'is_video', 'access_count', 'total_interactions', 
                                              'total_files_accessed', 'is_active', 'position', 
                                              'delete_after', 'added_by']:
                                        try:
                                            values.append(int(val) if val else None)
                                        except:
                                            values.append(None)
                                    elif col in ['file_size']:
                                        try:
                                            values.append(int(val) if val else 0)
                                        except:
                                            values.append(0)
                                    else:
                                        values.append(val)
                            
                            cur.execute(insert_query, values)
                            imported += 1
                            
                            # Commit every 1000 rows
                            if imported % 1000 == 0:
                                conn.commit()
                                
                        except Exception as e:
                            log.warning(f"Error importing row in {table_name}: {e}")
                            result["errors"].append(f"Row {imported+1}: {str(e)[:100]}")
                    
                    conn.commit()
                    return imported
            
            result["rows_imported"] = await asyncio.to_thread(_import)
            result["success"] = True
            log.info(f"✅ Imported {result['rows_imported']} rows to {table_name}")
            
    except Exception as e:
        log.error(f"Failed to import {table_name}: {e}")
        result["errors"].append(str(e))
        result["success"] = False
    
    return result

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
                    # Reset files id sequence
                    cur.execute("""
                        SELECT setval(
                            pg_get_serial_sequence('files', 'id'),
                            COALESCE((SELECT MAX(id) FROM files), 1),
                            EXISTS(SELECT 1 FROM files)
                        )
                    """)
                    # Reset required_channels id sequence
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
    
    # Import tables in correct order
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
    
    # Reset sequences
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
        
        # Check if message contains documents
        if not msg.document:
            return  # Not a document, let other handlers process
        
        doc = msg.document
        
        # Check if it's a backup file (CSV or JSON)
        doc_name = doc.file_name or ""
        lower_doc_name = doc_name.lower()
        is_csv = lower_doc_name.endswith('.csv')
        is_json = lower_doc_name.endswith('.json')
        
        if not (is_csv or is_json):
            # Not a backup file - let normal upload handler process for admin
            if update.effective_user.id == ADMIN_ID:
                return  # Will be handled by upload handler
            return
        
        # If it's from admin, handle as potential import
        if update.effective_user.id == ADMIN_ID:
            file_type = "CSV" if is_csv else "JSON"
            log.info(f"📥 Admin sent {file_type} file: {doc.file_name}")
            
            # Initialize pending backup files if not exists
            if 'pending_backup_files' not in context.user_data:
                context.user_data['pending_backup_files'] = {}
            
            try:
                # Download the file content
                file = await context.bot.get_file(doc.file_id)
                file_content = await file.download_as_bytearray()
                file_text = decode_backup_bytes(bytes(file_content))
                
                # Store with proper filename
                context.user_data['pending_backup_files'][doc.file_name] = file_text
                
                # Count records based on file type
                if is_csv:
                    lines = len(file_text.splitlines()) - 1  # Exclude header
                    record_info = f"📊 Records: {lines}"
                else:  # JSON
                    try:
                        json_data = json.loads(file_text)
                        record_info = f"📋 JSON metadata file"
                    except:
                        record_info = f"📋 JSON file"
                
                # Log what we have collected
                collected_files = list(context.user_data['pending_backup_files'].keys())
                log.info(f"📦 Collected backup files: {collected_files}")
                
                # Send acknowledgment
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
                
                # Schedule auto-deletion of this message
                await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
                
            except Exception as e:
                log.error(f"Error downloading {file_type} file: {e}")
                sent_msg = await msg.reply_text(f"❌ Error downloading {file_type} file: {str(e)[:200]}")
                await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            
        else:
            # Non-admin sent backup file - ignore politely
            file_type = "CSV" if is_csv else "JSON"
            log.info(f"ℹ️ Non-admin user {update.effective_user.id} sent {file_type} file (ignored)")
            
    except Exception as e:
        log.error(f"Error handling backup file: {e}", exc_info=True)
        if update.effective_user.id == ADMIN_ID:
            try:
                sent_msg = await update.message.reply_text(f"❌ Error processing backup file: {str(e)[:200]}")
                await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            except:
                pass

# ============ FIXED START COMMAND - Shows ALL missing channels ============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler - FIXED: Shows buttons for ALL missing channels"""
    try:
        if not update.message:
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        args = context.args
        username = update.effective_user.username
        first_name = update.effective_user.first_name

        log.info(f"🚀 /start command from user {user_id} (@{username}) with args: {args}")

        # Update user interaction
        await db.update_user_interaction(
            user_id=user_id,
            username=username,
            first_name=first_name,
            last_name=update.effective_user.last_name
        )

        # ALWAYS fetch fresh channel list from database
        log.info(f"📋 Fetching channels for user {user_id}")
        channels_data = await db.get_channels_with_details()
        active_channels = [c for c in channels_data if c['is_active'] == 1]
        
        log.info(f"📋 Found {len(active_channels)} active channels: {[c['channel_username'] for c in active_channels]}")
        
        # No file key - show welcome
        if not args:
            log.info(f"👋 Showing welcome menu to user {user_id}")
            keyboard = []
            
            # Add buttons for each required channel with friendly names
            for channel_data in active_channels:
                channel_name = channel_data['channel_name'] or f"Channel"
                url = channel_button_url(channel_data)
                if not url:
                    continue
                keyboard.append([InlineKeyboardButton(
                    f"📢 Join {channel_name}", 
                    url=url
                )])
            
            # Add check membership button
            keyboard.append([InlineKeyboardButton(
                "🔄 Check Membership", 
                callback_data="check_membership"
            )])

            if active_channels:
                channel_list = "\n".join([f"{i+1}. {c['channel_name'] or f'Channel {i+1}'}" for i, c in enumerate(active_channels)])
            else:
                channel_list = "No channels required!"

            sent_msg = await update.message.reply_text(
                "🤖 *Welcome to File Sharing Bot*\n\n"
                "🔗 *How to use:*\n"
                "1️⃣ Use admin-provided links\n"
                "2️⃣ Join the required channels:\n"
                f"{channel_list}\n"
                "3️⃣ Click 'Check Membership'\n\n",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        # ============ FILE KEY EXISTS - CHECK MEMBERSHIP ============
        key = args[0]
        log.info(f"🔑 User {user_id} accessing file key: {key}")
        
        shared_content = await get_shared_content(key)

        if not shared_content:
            log.warning(f"❌ File key {key} not found for user {user_id}")
            sent_msg = await update.message.reply_text("❌ File not found")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        if shared_content.get("kind") == "group":
            log.info(f"File group found: {key} ({len(shared_content.get('files') or [])} files)")
        else:
            log.info(f"File found: {shared_content['file_name']}")

        # FORCE CHECK membership with latest channels (ignore cache)
        log.info(f"🔍 Checking membership for user {user_id} (force=True)")
        result = await check_membership(user_id, context, force_check=True)

        log.info(f"📊 Membership result: all_joined={result['all_joined']}, missing={result['missing_channel_names']}")

        if not result["all_joined"]:
            missing_channels = result["missing_channels"]  # List of channel usernames
            missing_names = result["missing_channel_names"]  # List of friendly names
            missing_channel_data = result.get("missing_channel_data", [])
            verification_errors = result.get("verification_errors", [])
            
            log.info(f"🔒 User {user_id} missing {len(missing_names)} channels: {missing_names}")
            
            # Create keyboard with buttons for EACH missing channel
            keyboard = []
            
            # Add a button for EVERY missing channel
            for i, channel in enumerate(missing_channels):
                channel = normalize_channel_username(channel)
                channel_name = missing_names[i] if i < len(missing_names) else f"Channel {i+1}"
                keyboard.append([InlineKeyboardButton(
                    f"📥 Join {channel_name}", 
                    url=f"https://t.me/{channel}"
                )])
            
            # Add check again button
            keyboard.append([InlineKeyboardButton(
                "✅ Check Again", 
                callback_data=f"check|{key}"
            )])
            await db.save_pending_share_request(user_id, chat_id, key)
            keyboard_markup = build_join_keyboard(missing_channel_data, f"check|{key}")
            
            # Create appropriate message based on number of missing channels
            if len(missing_names) == 1:
                text = f"🔒 *Join {missing_names[0]} to access this file*"
            elif len(missing_names) == 2:
                text = f"🔒 *Join {missing_names[0]} and {missing_names[1]} to access this file*"
            else:
                channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                text = f"🔒 *Join {channels_text} to access this file*"

            if verification_errors:
                unresolved = ", ".join(markdown_code(item["name"]) for item in verification_errors)
                text += f"\n\n⚠️ *I couldn't verify:* {unresolved}\nPlease make sure the bot is an admin in those channels and try again."

            log.info(f"📨 Sending restriction message to user {user_id} with {len(keyboard)} buttons")
            
            sent_msg = await update.message.reply_text(
                text,
                parse_mode="Markdown",
                reply_markup=keyboard_markup
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        # User has joined all channels - send file
        log.info(f"✅ User {user_id} has joined all channels. Sending file...")
        await db.update_user_interaction(user_id=user_id, file_accessed=True)

        try:
            sent_items = await send_shared_content(context, chat_id, shared_content)
            await db.clear_pending_share_request(user_id)
            log.info(f"Sent {len(sent_items)} file(s) successfully to user {user_id}")

        except Exception as e:
            log.error(f"❌ Error sending file to user {user_id}: {e}", exc_info=True)
            sent_msg = await update.message.reply_text("❌ Failed to send file")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.error(f"❌ Start error: {e}", exc_info=True)

# ============ FIXED CHECK JOIN - Shows ALL missing channels ============
async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries - FIXED: Shows buttons for ALL missing channels"""
    try:
        query = update.callback_query
        await query.answer()

        user_id = query.from_user.id
        data = query.data
        username = query.from_user.username

        log.info(f"🔄 Callback query from user {user_id} (@{username}): {data}")

        # Update user interaction
        user = query.from_user
        await db.update_user_interaction(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )

        if data == "check_membership":
            log.info(f"🔍 Checking membership for user {user_id} from callback")
            result = await check_membership(user_id, context, force_check=True)

            if result["all_joined"]:
                # Get channels with details
                channels_data = await db.get_channels_with_details()
                active_channels = [c for c in channels_data if c['is_active'] == 1]
                
                channel_list = "\n".join([f"✅ {c['channel_name'] or f'Channel {i+1}'}" for i, c in enumerate(active_channels)])
                
                log.info(f"✅ User {user_id} has joined all channels")
                await query.edit_message_text(
                    f"✅ *You've joined all required channels!*\n\n"
                    f"{channel_list}\n\n"
                    f"Now you can use file links from admin.",
                    parse_mode="Markdown"
                )
            else:
                missing_channels = result["missing_channels"]
                missing_names = result["missing_channel_names"]
                missing_channel_data = result.get("missing_channel_data", [])
                verification_errors = result.get("verification_errors", [])
                
                log.info(f"❌ User {user_id} missing channels: {missing_names}")
                
                keyboard = []
                
                # Add button for EVERY missing channel
                for i, channel in enumerate(missing_channels):
                    channel = normalize_channel_username(channel)
                    channel_name = missing_names[i] if i < len(missing_names) else f"Channel {i+1}"
                    keyboard.append([InlineKeyboardButton(
                        f"📥 Join {channel_name}", 
                        url=f"https://t.me/{channel}"
                    )])
                
                # Add check again button
                keyboard.append([InlineKeyboardButton(
                    "🔄 Check Again", 
                    callback_data="check_membership"
                )])
                keyboard_markup = build_join_keyboard(
                    missing_channel_data,
                    "check_membership",
                    include_check_button=True
                )
                
                if len(missing_names) == 1:
                    text = f"❌ *Missing {missing_names[0]}*"
                elif len(missing_names) == 2:
                    text = f"❌ *Missing {missing_names[0]} and {missing_names[1]}*"
                else:
                    channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                    text = f"❌ *Missing {channels_text}*"

                if verification_errors:
                    unresolved = ", ".join(markdown_code(item["name"]) for item in verification_errors)
                    text += f"\n\n⚠️ *I couldn't verify:* {unresolved}\nPlease make sure the bot is an admin in those channels and try again."

                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=keyboard_markup
                )
            return

        if data.startswith("check|"):
            _, key = data.split("|")
            log.info(f"🔑 Check again for file {key} from user {user_id}")

            shared_content = await get_shared_content(key)
            if not shared_content:
                log.warning(f"❌ File {key} not found")
                await query.edit_message_text("❌ File not found")
                return

            result = await check_membership(user_id, context, force_check=True)

            if not result['all_joined']:
                missing_channels = result["missing_channels"]
                missing_names = result["missing_channel_names"]
                missing_channel_data = result.get("missing_channel_data", [])
                verification_errors = result.get("verification_errors", [])
                
                log.info(f"❌ User {user_id} still missing channels: {missing_names}")
                
                keyboard = []
                
                # Add button for EVERY missing channel
                for i, channel in enumerate(missing_channels):
                    channel = normalize_channel_username(channel)
                    channel_name = missing_names[i] if i < len(missing_names) else f"Channel {i+1}"
                    keyboard.append([InlineKeyboardButton(
                        f"📥 Join {channel_name}", 
                        url=f"https://t.me/{channel}"
                    )])
                
                # Add check again button
                keyboard.append([InlineKeyboardButton(
                    "✅ Check Again", 
                    callback_data=f"check|{key}"
                )])
                chat_id = query.message.chat_id
                await db.save_pending_share_request(user_id, chat_id, key)
                keyboard_markup = build_join_keyboard(missing_channel_data, f"check|{key}")
                
                if len(missing_names) == 1:
                    text = f"❌ *Join {missing_names[0]}*"
                elif len(missing_names) == 2:
                    text = f"❌ *Join {missing_names[0]} and {missing_names[1]}*"
                else:
                    channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                    text = f"❌ *Join {channels_text}*"

                if verification_errors:
                    unresolved = ", ".join(markdown_code(item["name"]) for item in verification_errors)
                    text += f"\n\n⚠️ *I couldn't verify:* {unresolved}\nPlease make sure the bot is an admin in those channels and try again."

                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=keyboard_markup
                )
                return

            # User has joined all channels - send file
            log.info(f"✅ User {user_id} now joined all channels. Sending file...")
            await db.update_user_interaction(user_id=user_id, file_accessed=True)

            try:
                chat_id = query.message.chat_id
                sent_items = await send_shared_content(context, chat_id, shared_content)
                await db.clear_pending_share_request(user_id)

                await query.edit_message_text("✅ *File sent below!*", parse_mode="Markdown")
                log.info(f"Sent {len(sent_items)} file(s) successfully to user {user_id}")

            except Exception as e:
                log.error(f"❌ Failed to send file to user {user_id}: {e}", exc_info=True)
                await query.edit_message_text("❌ Failed to send file")

    except Exception as e:
        log.error(f"❌ Callback error: {e}", exc_info=True)

async def send_pending_share_if_ready(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Send a user's pending share when every required channel is satisfied."""
    pending = await db.get_pending_share_request(user_id)
    if not pending:
        log.info(f"No pending share request for user {user_id}")
        return False

    result = await check_membership(user_id, context, force_check=True)
    if not result["all_joined"]:
        log.info(f"User {user_id} still missing channels after join request: {result['missing_channel_names']}")
        return False

    shared_content = await get_shared_content(pending["share_key"])
    if not shared_content:
        log.warning(f"Pending share key {pending['share_key']} no longer exists for user {user_id}")
        await db.clear_pending_share_request(user_id)
        return False

    chat_id = int(pending["chat_id"])
    sent_items = await send_shared_content(context, chat_id, shared_content)
    await db.update_user_interaction(user_id=user_id, file_accessed=True)
    await db.clear_pending_share_request(user_id)
    log.info(f"Auto-sent {len(sent_items)} pending file(s) to user {user_id} after join request")
    return True


async def handle_chat_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mark request-mode channels satisfied as soon as Telegram emits chat_join_request."""
    join_request = update.chat_join_request
    if not join_request:
        return

    user = join_request.from_user
    chat = join_request.chat

    try:
        channel = await db.get_request_channel_by_chat_id(chat.id)
        if not channel:
            log.info(f"Join request in chat {chat.id}, but no active request channel is configured")
            return

        await db.mark_join_request_satisfied(user.id, channel["channel_username"])
        await db.cache_membership(user.id, channel["channel_username"], True)
        await db.update_user_interaction(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )
        await send_pending_share_if_ready(user.id, context)

    except Exception as e:
        log.error(f"Error handling chat_join_request for user {getattr(user, 'id', None)}: {e}", exc_info=True)


# ============ CHANNEL MANAGEMENT COMMANDS ============
async def addchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new required channel (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        sent_msg = await update.message.reply_text(
            "❌ Usage: /addchannel <channel username> [friendly name]\n"
            "Example: /addchannel @my_channel \"My Channel\""
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    channel = normalize_channel_username(context.args[0])
    friendly_name = None
    
    # Check if friendly name provided
    if len(context.args) > 1:
        friendly_name = " ".join(context.args[1:])
    
    user_id = update.effective_user.id
    
    # Try to verify bot is admin in the channel
    try:
        clean_channel = normalize_channel_username(channel)
        channel_ref = telegram_chat_ref(clean_channel)
        if channel_ref is None:
            sent_msg = await update.message.reply_text(
                "❌ Invalid channel. Use a public @username or numeric -100... channel id."
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        chat = await context.bot.get_chat(channel_ref)
        
        # Check if bot is admin
        bot_member = await context.bot.get_chat_member(channel_ref, context.bot.id)
        if bot_member.status not in ["administrator", "creator"]:
            keyboard = []
            if not re.fullmatch(r"-?\d+", clean_channel):
                keyboard = [[InlineKeyboardButton(
                    "🤖 Add Bot to Channel",
                    url=f"https://t.me/{clean_channel}?startchannel=bot"
                )]]
            
            sent_msg = await update.message.reply_text(
                f"⚠️ *Bot is not an admin in @{clean_channel}*\n\n"
                f"Make sure to add the bot as admin to check memberships!",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
            
    except Exception as e:
        log.warning(f"Could not verify bot in channel {channel}: {e}")
        sent_msg = await update.message.reply_text(
            f"❌ Could not verify {markdown_code(channel)}.\n\n"
            "Add the bot as admin in that channel, then run /addchannel again.",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    # Add channel to database
    success = await db.add_channel(channel, user_id, friendly_name)
    
    if success:
        channels = await db.get_channels_with_details()
        active_channels = [c for c in channels if c['is_active'] == 1]
        channel_list = "\n".join([f"{i+1}. {c['channel_name'] or c['channel_username']}" for i, c in enumerate(active_channels)])
        
        sent_msg = await update.message.reply_text(
            f"✅ *Channel added successfully!*\n\n"
            f"Added: {friendly_name or f'@{channel.replace("@", "")}'}\n\n"
            f"📋 *Current required channels:*\n{channel_list}",
            parse_mode="Markdown"
        )
    else:
        sent_msg = await update.message.reply_text(
            f"❌ Failed to add channel. It might already exist."
        )
    
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def addchannelreq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a join-request required channel from a replied forwarded channel message."""
    if update.effective_user.id != ADMIN_ID:
        return

    if not update.message or not update.message.reply_to_message:
        sent_msg = await update.message.reply_text(
            "Usage: forward a message from the target channel, then reply to it with:\n"
            "/addchannelreq Join Channel"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    source_chat_id, source_title, source_username = extract_forwarded_channel(update.message.reply_to_message)
    if not source_chat_id:
        sent_msg = await update.message.reply_text(
            "I couldn't detect the source channel from that forwarded message.\n\n"
            "Forwarding may be protected/hidden. Try forwarding another channel post."
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    button_name = clean_single_line(" ".join(context.args), source_title or "Join Channel")
    if len(button_name) > 64:
        button_name = button_name[:64].strip()

    try:
        bot_member = await context.bot.get_chat_member(source_chat_id, context.bot.id)
        if bot_member.status not in ["administrator", "creator"]:
            sent_msg = await update.message.reply_text(
                "Bot is not an admin in that channel.\n\n"
                "Add the bot as admin with invite-link permission, then run /addchannelreq again."
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        invite = await context.bot.create_chat_invite_link(
            chat_id=source_chat_id,
            name=f"force-join-{int(time.time())}",
            creates_join_request=True
        )
    except Exception as e:
        log.warning(f"Could not create join-request invite for {source_chat_id}: {e}")
        sent_msg = await update.message.reply_text(
            "Could not create a join-request invite link.\n\n"
            "Make sure the bot can invite users / manage invite links in that channel."
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    success = await db.add_request_channel(
        chat_id=source_chat_id,
        button_name=button_name,
        invite_link=invite.invite_link,
        added_by=update.effective_user.id,
        username=source_username
    )

    if success:
        sent_msg = await update.message.reply_text(
            f"Join-request channel added successfully!\n\n"
            f"Button: {button_name}\n"
            f"Chat ID: {source_chat_id}\n"
            f"Users will satisfy this channel as soon as they submit the join request."
        )
    else:
        sent_msg = await update.message.reply_text("Failed to add join-request channel.")

    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def removechannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a required channel (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        sent_msg = await update.message.reply_text(
            "❌ Usage: /removechannel <channel username>\n"
            "Example: /removechannel @my_channel"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    channel = " ".join(context.args).strip()
    
    success = await db.remove_channel(channel)
    
    if success:
        channels = await db.get_channels_with_details()
        active_channels = [c for c in channels if c['is_active'] == 1]
        
        if active_channels:
            channel_list = "\n".join([f"{i+1}. {c['channel_name'] or c['channel_username']}" for i, c in enumerate(active_channels)])
        else:
            channel_list = "No channels required (all access granted)"
        
        sent_msg = await update.message.reply_text(
            f"✅ *Channel removed successfully!*\n\n"
            f"Removed: @{channel.replace('@', '')}\n\n"
            f"📋 *Current required channels:*\n{channel_list}",
            parse_mode="Markdown"
        )
    else:
        sent_msg = await update.message.reply_text(
            f"❌ Channel not found or already removed."
        )
    
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def listchannels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all required channels (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    channels = await db.get_channels_with_details()
    
    if not channels:
        sent_msg = await update.message.reply_text(
            "📋 *No channels configured*\n\n"
            "Use /addchannel to add required channels.",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    active_channels = [c for c in channels if c['is_active'] == 1]
    inactive_channels = [c for c in channels if c['is_active'] == 0]
    
    msg = f"📋 *Channel Management*\n\n"
    msg += f"📢 *Active Channels ({len(active_channels)}):*\n"
    
    for i, ch in enumerate(active_channels):
        added_date = ch['added_at'].strftime('%Y-%m-%d') if ch['added_at'] else 'Unknown'
        display_name = ch['channel_name'] or ch['channel_username']
        channel_type = ch.get('channel_type') or 'normal'
        msg += f"{i+1}. {display_name}\n"
        msg += f"   Type: {channel_type}\n"
        if channel_type == "request":
            msg += f"   Chat ID: {ch['chat_id'] or ch['channel_username']}\n"
        msg += f"   └ Username: @{ch['channel_username']}\n"
        msg += f"   └ Added: {added_date}\n"
    
    if inactive_channels:
        msg += f"\n⏸️ *Inactive Channels ({len(inactive_channels)}):*\n"
        for i, ch in enumerate(inactive_channels):
            display_name = ch['channel_name'] or ch['channel_username']
            msg += f"{i+1}. {display_name} (@{ch['channel_username']})\n"
    
    msg += f"\n💡 *Commands:*\n"
    msg += f"/addchannel @channel [name] - Add channel\n"
    msg += f"/addchannelreq [button] - Add join-request channel\n"
    msg += f"/removechannel <id, username, or button> - Remove channel\n"
    msg += f"/testchannels - Test bot access to all channels"
    
    sent_msg = await update.message.reply_text(msg, parse_mode="Markdown")
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
        
        try:
            channel_ref = telegram_chat_ref(channel)
            if channel_ref is None:
                results.append(f"❌ {display_name} - Invalid channel")
                continue

            chat = await context.bot.get_chat(channel_ref)
            bot_member = await context.bot.get_chat_member(channel_ref, context.bot.id)
            
            if bot_member.status in ["administrator", "creator"]:
                results.append(f"✅ {display_name} - Bot is admin")
            else:
                results.append(f"⚠️ {display_name} - Bot is member (not admin)")
                
        except Exception as e:
            error_msg = str(e)
            if "chat not found" in error_msg.lower():
                results.append(f"❌ {display_name} - Channel not found")
            elif "forbidden" in error_msg.lower():
                results.append(f"❌ {display_name} - Bot not in channel")
            else:
                results.append(f"❌ {display_name} - Error: {error_msg[:50]}")
    
    result_text = "🔍 *Channel Access Test*\n\n" + "\n".join(results)
    
    await status_msg.edit_text(result_text, parse_mode="Markdown")
    await schedule_message_deletion(context, status_msg.chat_id, status_msg.message_id)

# ============ EXISTING COMMAND HANDLERS ============
async def upload_immediate_legacy(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            f"💾 *Storage:* Metadata only (file stored on Telegram)\n\n"
            f"🔗 *Link:*\n`{link}`",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.exception("Upload error")
        sent_msg = await update.message.reply_text(f"❌ Upload failed: {str(e)[:200]}")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

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

    # Get database storage usage (REAL PostgreSQL size)
    db_storage = await db.get_db_storage_usage()
    
    # Get metadata storage info
    metadata_info = await db.get_metadata_storage_info()
    
    # Get total size of files uploaded (for information only)
    total_uploaded_bytes = await db.get_total_uploaded_size()
    
    # Format bytes to human readable
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

    # Get total accesses
    files = await db.get_all_files()
    groups = await db.get_all_file_groups()
    total_access = (sum(f[5] for f in files) if files else 0) + (sum(g[5] for g in groups) if groups else 0)

    # Escape underscores in bot_username for Markdown
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
            f"👀 *Total Accesses:* {total_access}\n\n"
            f"💾 *PostgreSQL Storage (REAL):*\n"
            f"   ├─ Total DB: {db_storage['total']}\n"
            f"   ├─ Tables: {db_storage['tables']}\n"
            f"   └─ Indexes: {db_storage['indexes']}\n\n"
            f"📊 *Metadata Stats:*\n"
            f"   ├─ Cache Entries: {metadata_info['cache_entries']}\n"
            f"   └─ Est. Metadata: {metadata_info['estimated_metadata']}\n\n"
            f"⏰ Auto-delete: {DELETE_AFTER//60} minutes\n"
            f"🧹 Auto Cleanup: DISABLED (Permanent storage)\n"
            f"📅 Auto Backup: Every 3 days",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
    except Exception as e:
        log.error(f"Error in stats command: {e}", exc_info=True)
        # Fallback to plain text
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
                f"💾 PostgreSQL: {db_storage['total']}\n"
                f"⏰ Auto-delete: {DELETE_AFTER//60} minutes\n"
                f"🧹 Auto Cleanup: DISABLED\n"
                f"📅 Auto Backup: Every 3 days"
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        except Exception as e2:
            log.error(f"Even fallback failed: {e2}")

async def listfiles_legacy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all files with access links and access counts (admin only)."""
    if update.effective_user.id != ADMIN_ID:
        return

    groups = await db.get_all_file_groups()
    files = await db.get_all_files()

    if not groups and not files:
        sent_msg = await update.message.reply_text("📁 No files stored")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    header = f"📁 *Total Files: {len(files)}*\n\n"
    chunks = [header]

    for index, file in enumerate(files, start=1):
        file_id, name, is_video, size, ts, access = file
        display_name = clean_single_line(name, "Unnamed file")
        link = f"https://t.me/{bot_username}?start={file_id}"
        access_count = access or 0
        access_word = "time" if access_count == 1 else "times"

        entry = (
            f"*{index}.* {markdown_code(display_name)}\n"
            f"🔗 Link: [Open]({link}) | {markdown_code(link)}\n"
            f"👀 Accessed: {access_count} {access_word}\n\n"
        )

        if len(chunks[-1]) + len(entry) > LISTFILES_MESSAGE_LIMIT:
            chunks.append(f"📁 *Files Continued* ({len(files)} total)\n\n")

        chunks[-1] += entry

    for chunk in chunks:
        sent_msg = await update.message.reply_text(
            chunk,
            parse_mode="Markdown",
            disable_web_page_preview=True
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        await asyncio.sleep(0.05)

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

# ============ ENHANCED BROADCAST FEATURE ============
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
        
        # Update status for current chunk
        await status_msg.edit_text(
            f"📦 *Processing Chunk {chunk_num + 1}/{total_chunks}*\n"
            f"👥 Users in this chunk: {len(chunk_users)}\n"
            f"✅ Sent so far: {successful}\n"
            f"❌ Failed: {failed}\n"
            f"🚫 Blocked: {blocked}\n"
            f"⏱️ Chunk {chunk_num + 1} starting...",
            parse_mode="Markdown"
        )
        
        # Process current chunk
        for i, user_id in enumerate(chunk_users):
            try:
                await send_single_broadcast(context, user_id, payload)
                chunk_success += 1
                successful += 1
                
                # Update progress every 100 users within chunk
                if (i + 1) % 100 == 0:
                    await status_msg.edit_text(
                        f"📦 *Chunk {chunk_num + 1}/{total_chunks}* - {i + 1}/{len(chunk_users)} users\n"
                        f"✅ Sent: {successful}\n"
                        f"❌ Failed: {failed}\n"
                        f"🚫 Blocked: {blocked}",
                        parse_mode="Markdown"
                    )
                
                # Small delay to avoid hitting rate limits
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
        
        # Store chunk result
        chunk_results.append({
            'chunk': chunk_num + 1,
            'users': len(chunk_users),
            'success': chunk_success,
            'failed': chunk_failed,
            'blocked': chunk_blocked
        })
        
        # Update status after chunk completion
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
        
        # Delay between chunks (2 seconds to avoid hitting limits)
        if chunk_num < total_chunks - 1:
            await asyncio.sleep(2)
    
    # Final summary
    elapsed_time = time.time() - start_time
    avg_speed = successful / elapsed_time if elapsed_time > 0 else 0
    
    # Create detailed summary
    summary = f"✅ *Broadcast Complete!*\n\n"
    summary += f"📊 *Final Statistics:*\n"
    summary += f"👥 Total Users: {total_users}\n"
    summary += f"✅ Successfully Sent: {successful}\n"
    summary += f"❌ Failed: {failed}\n"
    summary += f"🚫 Blocked/Deactivated: {blocked}\n"
    summary += f"📦 Chunks Processed: {total_chunks}\n"
    summary += f"⏱️ Time Taken: {elapsed_time:.1f} seconds\n"
    summary += f"⚡ Avg Speed: {avg_speed:.1f} users/sec\n\n"
    
    # Add chunk details
    summary += f"📋 *Chunk Details:*\n"
    for chunk in chunk_results:
        summary += f"Chunk {chunk['chunk']}: {chunk['success']}✅/{chunk['failed']}❌/{chunk['blocked']}🚫\n"
    
    # Calculate success rate
    success_rate = (successful / total_users * 100) if total_users > 0 else 0
    summary += f"\n📈 Success Rate: {success_rate:.1f}%"
    
    await status_msg.edit_text(summary, parse_mode="Markdown")
    
    # Log the broadcast
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

            # Get all users
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
            
            # Start the broadcast process
            asyncio.create_task(process_broadcast_chunks(
                context, user_ids, payload, status_msg
            ))
            
            # Clear stored payload after handing it to the background task
            if broadcast_id:
                broadcast_payloads.pop(broadcast_id, None)
            
        except Exception as e:
            log.error(f"Error in broadcast confirmation: {e}")
            await query.message.reply_text(f"❌ Error starting broadcast: {str(e)[:100]}")

async def clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear membership cache (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    # Optional: clear cache for specific channel
    if context.args:
        channel = normalize_channel_username(context.args[0])
        await db.clear_membership_cache(channel=channel)
        sent_msg = await update.message.reply_text(f"✅ Cache cleared for channel {channel}")
    else:
        await db.clear_membership_cache()
        sent_msg = await update.message.reply_text("✅ All cache cleared")
    
    await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

async def testchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test channel access (admin only) - Updated for dynamic channels"""
    if update.effective_user.id != ADMIN_ID:
        return

    user_id = update.effective_user.id
    channels_data = await db.get_channels_with_details()
    active_channels = [c for c in channels_data if c['is_active'] == 1]
    
    if not active_channels:
        sent_msg = await update.message.reply_text("📋 No channels configured.")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    results = []
    for ch in active_channels:
        channel = normalize_channel_username(ch['channel_username'])
        display_name = ch['channel_name'] or channel
        
        try:
            channel_ref = telegram_chat_ref(channel)
            if channel_ref is None:
                status = "❌ Invalid channel"
            else:
                member = await context.bot.get_chat_member(channel_ref, user_id)
                status = f"✅ {member.status}"
        except Exception as e:
            status = f"❌ {str(e)[:50]}"
        
        results.append(f"{display_name}: {status}")

    result_text = "🔍 *Channel Access Test*\n\n" + "\n".join(results)
    
    sent_msg = await update.message.reply_text(result_text, parse_mode="Markdown")
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
        # Create backup
        backup_data = await export_database_backup(update=update, context=context, send_to_admin=False)
        
        # Send files directly
        await status_msg.edit_text(f"✅ Backup created!\n📦 Total size: {sum(len(v) for v in backup_data.values()) / 1024:.2f} KB\n\nSending files now...")
        
        # Send each file (both CSV and JSON)
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
        
        # Delete status message
        await status_msg.delete()
        
        # Send summary
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
    
    # Get database stats
    file_count = await db.get_file_count()
    user_count = await db.get_user_count()
    channel_count = await db.get_channel_count()
    
    # Get database size
    db_storage = await db.get_db_storage_usage()
    
    status_msg = f"""
📊 *Database Status*

📈 *Data Summary:*
• Files: {file_count}
• Users: {user_count}
• Channels: {channel_count}
• DB Size: {db_storage.get('total', 'Unknown')}

💾 *Backup Ready:* Yes
• Use `/backup` to create backup
• Use `/import` to restore from backup
• Auto-backup: Every 3 days

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
    
    # Check both old and new key names
    pending_files = context.user_data.get('pending_backup_files', {})
    if not pending_files:
        # Fallback to old key name for backward compatibility
        pending_files = context.user_data.get('pending_csv_files', {})
    
    if not pending_files:
        sent_msg = await update.message.reply_text(
            "📋 *No backup files collected*\n\n"
            "Send backup files (CSV + JSON) to start the import process.\n"
            "Required files: files.csv, users.csv, required_channels.csv\n"
            "Optional: metadata.json, membership_cache.csv, scheduled_deletions.csv\n\n"
            "💡 *Tip:* Forward backup files directly to bot and they'll be auto-collected\n"
            "📌 Bot accepts both exact filenames and timestamped filenames",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    status = f"📋 *Collected Backup Files* ({len(pending_files)})\n\n"
    
    # Separate CSV and JSON files
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
    
    # Check for required files by pattern matching
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
    """Import database from backup files - Admin only - FIXED for timestamped filenames"""
    if update.effective_user.id != ADMIN_ID:
        sent_msg = await update.message.reply_text("⛔ Admin only command")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    # Check for collected backup files first (check both old and new key names)
    collected_files = context.user_data.get('pending_backup_files', {})
    if not collected_files:
        # Fallback to old key name
        collected_files = context.user_data.get('pending_csv_files', {})
    
    # Log what we have
    log.info(f"Import command - collected files: {list(collected_files.keys())}")
    
    # Check if replying to a message
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
            "• metadata.json (recommended)\n"
            "• membership_cache.csv (optional)\n"
            "• scheduled_deletions.csv (optional)\n\n"
            "📌 Bot accepts both exact and timestamped filenames\n"
            "⚠️ **Warning:** This will replace ALL existing data!",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return
    
    # Determine which files to use
    backup_files = {}
    
    if collected_files:
        # Use collected files directly
        backup_files = collected_files.copy()
        log.info(f"✅ Using {len(backup_files)} collected backup files for import: {list(backup_files.keys())}")
    elif update.message.reply_to_message:
        # Try to get files from replied message
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
    
    # Check if we actually have files after all attempts
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
    
    # ===== FIX: Map timestamped filenames to required table names =====
    # Define the mapping of required table names to possible filename patterns
    required_table_patterns = {
        'files.csv': ['files'],
        'users.csv': ['users'],
        'required_channels.csv': ['required_channels'],
        'membership_cache.csv': ['membership_cache'],
        'scheduled_deletions.csv': ['scheduled_deletions'],
        'metadata.json': ['metadata']
    }
    
    # Create normalized backup files dictionary
    normalized_files = {}
    missing_files = []
    
    for required_file, patterns in required_table_patterns.items():
        found = False
        for filename, content in backup_files.items():
            # Check if filename matches any pattern
            for pattern in patterns:
                # Match if pattern is in filename (handles both exact and timestamped names)
                if canonical_backup_filename(filename) == required_file:
                    normalized_files[required_file] = content
                    found = True
                    log.info(f"✅ Matched {filename} -> {required_file}")
                    break
            if found:
                break
        
        if not found:
            # Check if the required file is mandatory
            if required_file in REQUIRED_IMPORT_FILES:
                missing_files.append(required_file)
    
    normalized_files, unmatched_files = normalize_backup_file_map(backup_files)
    missing_files = [filename for filename in REQUIRED_IMPORT_FILES if filename not in normalized_files]
    if unmatched_files:
        log.info(f"Ignoring unmatched backup files: {unmatched_files}")

    # If there are missing required files, show error
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
    
    # Use the normalized files for the rest of the import process
    backup_files = normalized_files
    log.info(f"✅ Normalized backup files: {list(backup_files.keys())}")
    
    # Verify required CSV files again with normalized names
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
    
    # Confirm before proceeding
    confirm_keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ YES, Import Now", callback_data="confirm_import"),
        InlineKeyboardButton("❌ Cancel", callback_data="cancel_import")
    ]])
    
    # Show summary with file types
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
    
    # Store normalized backup files for the callback
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
        # Get stored backup files (check both old and new key names)
        backup_files = context.chat_data.get('import_backup_files', {})
        if not backup_files:
            backup_files = context.chat_data.get('import_csv_files', {})
            
        if not backup_files:
            # Try to get from user_data as fallback
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

            # Perform the restore (only CSV files are used for database import)
            csv_only_files = {k: v for k, v in backup_files.items() if k.lower().endswith('.csv')}
            
            result = await restore_from_backup(csv_only_files)
            
            if result["success"]:
                # Clear collected files after successful import
                context.user_data.pop('pending_backup_files', None)
                context.user_data.pop('pending_csv_files', None)  # Clear old key too
                context.chat_data.pop('import_backup_files', None)
                context.chat_data.pop('import_csv_files', None)  # Clear old key too
                
                # Generate success report
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

                # Check if JSON metadata was included
                if 'metadata.json' in backup_files:
                    success_msg += f"📋 *Metadata:* JSON file was included in backup\n"
                
                success_msg += f"\n💡 *Next steps:*\n"
                success_msg += f"• Run `/stats` to verify data\n"
                success_msg += f"• Run `/listchannels` to check channels\n"
                success_msg += f"• Broadcast will work with all restored users! ✅\n\n"
                success_msg += f"⚠️ *Remember:* Your database will still expire. Run `/backup` regularly!\n"
                success_msg += f"📅 Auto-backup runs every 3 days"
                
                await query.edit_message_text(success_msg)
                
                # Also send as new message
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"🎉 Database restored from backup! {result['total_rows']} rows imported. All users restored for broadcasts!"
                )
                
            else:
                # Show errors
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
        # Create backup
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

    # Add job queue for cleanup (only message deletion, not file cleanup)
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
        
        # Add auto-backup job (every 3 days = 259200 seconds)
        application.job_queue.run_repeating(
            auto_backup_job,
            interval=259200,  # 3 days (72 hours)
            first=3600  # Start after 1 hour
        )
        log.info("📅 Auto-backup scheduled (every 3 days)")

    else:
        asyncio.create_task(cleanup_overdue_messages_loop(application.bot))
        asyncio.create_task(keepalive_loop(application.bot))
        asyncio.create_task(auto_backup_loop(application.bot))
        log.warning("JobQueue unavailable; using fallback asyncio cleanup and auto-backup loops")

    # Add error handler
    application.add_error_handler(error_handler)
    
    # Add backup file handler (CSV + JSON) - MUST be before upload handler
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
    application.add_handler(CommandHandler("testchannel", testchannel))
    
    # Channel management commands
    application.add_handler(CommandHandler("addchannel", addchannel))
    application.add_handler(CommandHandler("addchannelreq", addchannelreq))
    application.add_handler(CommandHandler("removechannel", removechannel))
    application.add_handler(CommandHandler("listchannels", listchannels))
    application.add_handler(CommandHandler("testchannels", testchannels))
    application.add_handler(ChatJoinRequestHandler(handle_chat_join_request))
    
    # Backup and import commands
    application.add_handler(CommandHandler("backup", backup_command))
    application.add_handler(CommandHandler("backup_status", backup_status))
    application.add_handler(CommandHandler("import", import_command))
    application.add_handler(CommandHandler("import_status", import_status))

    # Add callback handlers
    application.add_handler(CallbackQueryHandler(check_join, pattern="^check_membership$"))
    application.add_handler(CallbackQueryHandler(check_join, pattern="^check\\|"))
    application.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^(confirm_broadcast|cancel_broadcast)(\\|.+)?$"))
    application.add_handler(CallbackQueryHandler(import_callback, pattern="^(confirm_import|cancel_import)$"))

    # Add upload handler (admin only) - Make sure CSV and JSON files are excluded
    upload_filter = (filters.VIDEO | (filters.Document.ALL & ~filters.Document.FileExtension("csv") & ~filters.Document.FileExtension("json")))
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
        # Preserve any updates Telegram queued while Render was restarting.
        await application.bot.delete_webhook(drop_pending_updates=False)
        # Set new webhook
        await application.bot.set_webhook(
            url=webhook_url,
            allowed_updates=Update.ALL_TYPES,
            max_connections=40,
            drop_pending_updates=False
        )
        log.info("✅ Webhook set successfully")
    except Exception as e:
        log.error(f"Failed to set webhook: {e}", exc_info=True)
        return None

    await application.start()
    await cleanup_overdue_messages(BotOnlyContext(application.bot))

    # Mark as initialized
    bot_initialized = True
    
    log.info("🤖 Bot initialized and ready via webhook")
    log.info(f"📁 Files in database: {await db.get_file_count()}")
    log.info(f"👥 Users in database: {await db.get_user_count()}")
    log.info(f"📢 Required channels: {await db.get_channel_count()}")
    log.info(f"🧹 Auto cleanup: DISABLED (Permanent storage)")
    log.info(f"📅 Auto backup: Enabled (every 3 days)")
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
    
    # Keep the application running
    while True:
        await asyncio.sleep(3600)  # Sleep for an hour

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
            asyncio.run(bot_app.stop())
            asyncio.run(bot_app.shutdown())
        asyncio.run(db.close_pool())
        print("Shutdown complete.")

if __name__ == "__main__":
    main()
