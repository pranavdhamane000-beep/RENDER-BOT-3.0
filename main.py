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

# Default channels (will be added to database on first run)
DEFAULT_CHANNELS = [
    os.environ.get("CHANNEL_1", "A_Knight_of_the_Seven_Kingdoms_t").replace("@", ""),
    os.environ.get("CHANNEL_2", "your_movies_web").replace("@", "")
]

# ============ RENDER POSTGRESQL WITH PSYCOPG2 ============
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL is not set!")
    print("💡 Add a PostgreSQL database in Render Dashboard and copy its Internal Database URL")
    raise ValueError("DATABASE_URL environment variable is required!")

DELETE_AFTER = 600  # 10 minutes
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
        
        # Create indexes
        cur.execute('CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON membership_cache(timestamp)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_deletions_time ON scheduled_deletions(scheduled_time)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_users_first_seen ON users(first_seen)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_channels_active ON required_channels(is_active)')
        
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
            # Each file record ~200 bytes, each user ~150 bytes, each cache entry ~50 bytes
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
            SELECT id, channel_username, channel_name, added_at, is_active, position
            FROM required_channels
            ORDER BY position, id
        ''')
        return [dict(row) for row in rows]
    
    async def add_channel(self, channel_username: str, added_by: int, channel_name: str = None) -> bool:
        """Add a new required channel"""
        # Clean username (remove @ if present)
        clean_username = channel_username.replace("@", "").strip()
        
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
    
    async def remove_channel(self, channel_username: str) -> bool:
        """Remove a required channel (soft delete by setting inactive)"""
        clean_username = channel_username.replace("@", "").strip()
        
        rowcount = await self.execute_and_commit('''
            UPDATE required_channels SET is_active = 0
            WHERE channel_username = %s
        ''', (clean_username,))
        
        if rowcount > 0:
            log.info(f"Channel removed: @{clean_username}")
            
            # Also clear cache for this channel
            await self.execute_and_commit("DELETE FROM membership_cache WHERE channel = %s", (clean_username,))
            return True
        return False
    
    async def update_channel_name(self, channel_username: str, new_name: str) -> bool:
        """Update friendly name for a channel"""
        clean_username = channel_username.replace("@", "").strip()
        
        rowcount = await self.execute_and_commit('''
            UPDATE required_channels SET channel_name = %s
            WHERE channel_username = %s
        ''', (new_name, clean_username))
        
        return rowcount > 0
    
    async def get_channel_count(self) -> int:
        """Get number of active required channels"""
        result = await self.fetchrow("SELECT COUNT(*) as count FROM required_channels WHERE is_active = 1")
        return result['count'] if result else 0

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

# ============ DYNAMIC MEMBERSHIP CHECK ============
async def check_user_in_channel(bot, channel: str, user_id: int, force_check: bool = False) -> bool:
    """Check if user is in channel"""
    clean_channel = channel.replace("@", "")
    
    if not force_check:
        cached = await db.get_cached_membership(user_id, clean_channel)
        if cached is not None:
            log.info(f"✅ CACHE HIT: User {user_id} in {clean_channel}: {cached}")
            return cached
        else:
            log.info(f"🔄 CACHE MISS: User {user_id} in {clean_channel}")

    try:
        if not channel.startswith("@"):
            channel_username = f"@{channel}"
        else:
            channel_username = channel

        log.info(f"🔍 Checking user {user_id} in channel {channel_username}")
        member = await bot.get_chat_member(chat_id=channel_username, user_id=user_id)
        is_member = member.status in ["member", "administrator", "creator"]
        log.info(f"✅ User {user_id} in {clean_channel}: {is_member} (status: {member.status})")

        await db.cache_membership(user_id, clean_channel, is_member)
        return is_member

    except Exception as e:
        error_msg = str(e).lower()
        log.error(f"❌ Error checking user {user_id} in {clean_channel}: {error_msg}")
        
        if "user not found" in error_msg or "user not participant" in error_msg:
            await db.cache_membership(user_id, clean_channel, False)
            return False
        elif "chat not found" in error_msg:
            log.error(f"Channel @{clean_channel} not found!")
            # Still return True to allow access if channel doesn't exist
            return True
        elif "forbidden" in error_msg:
            log.error(f"Bot can't access @{clean_channel}")
            return True
        else:
            return True

async def check_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE, force_check: bool = False) -> Dict[str, Any]:
    """Check if user is member of all required channels"""
    bot = context.bot

    result = {
        "all_joined": False,
        "missing_channels": [],
        "missing_channel_names": [],
        "channel_status": {}
    }

    # Get all active required channels with details (ALWAYS fetch fresh from DB)
    log.info(f"📋 Fetching active channels from database for user {user_id}")
    channels_data = await db.get_channels_with_details()
    active_channels = [c for c in channels_data if c['is_active'] == 1]
    
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
        
        log.info(f"🔍 Checking user {user_id} in channel @{channel} ({channel_name})")
        is_member = await check_user_in_channel(bot, channel, user_id, force_check)
        
        result["channel_status"][channel] = {
            'is_member': is_member,
            'name': channel_name
        }
        
        if not is_member:
            log.info(f"❌ User {user_id} NOT in channel @{channel}")
            result["missing_channels"].append(channel)
            result["missing_channel_names"].append(channel_name)
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
                channel = channel_data['channel_username']
                channel_name = channel_data['channel_name'] or f"Channel"
                keyboard.append([InlineKeyboardButton(
                    f"📢 Join {channel_name}", 
                    url=f"https://t.me/{channel}"
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
                "3️⃣ Click 'Check Membership'\n\n"
                f"⚠️ Messages auto-delete after {DELETE_AFTER//60} minutes\n"
                "💾 *Storage:* Metadata only (files stored on Telegram)\n"
                "📢 *Dynamic Channels:* Add/remove anytime by admin",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        # ============ FILE KEY EXISTS - CHECK MEMBERSHIP ============
        key = args[0]
        log.info(f"🔑 User {user_id} accessing file key: {key}")
        
        file_info = await db.get_file(key)

        if not file_info:
            log.warning(f"❌ File key {key} not found for user {user_id}")
            sent_msg = await update.message.reply_text("❌ File not found")
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        log.info(f"📁 File found: {file_info['file_name']}")

        # FORCE CHECK membership with latest channels (ignore cache)
        log.info(f"🔍 Checking membership for user {user_id} (force=True)")
        result = await check_membership(user_id, context, force_check=True)

        log.info(f"📊 Membership result: all_joined={result['all_joined']}, missing={result['missing_channel_names']}")

        if not result["all_joined"]:
            missing_channels = result["missing_channels"]  # List of channel usernames
            missing_names = result["missing_channel_names"]  # List of friendly names
            
            log.info(f"🔒 User {user_id} missing {len(missing_names)} channels: {missing_names}")
            
            # Create keyboard with buttons for EACH missing channel
            keyboard = []
            
            # Add a button for EVERY missing channel
            for i, channel in enumerate(missing_channels):
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
            
            # Create appropriate message based on number of missing channels
            if len(missing_names) == 1:
                text = f"🔒 *Join {missing_names[0]} to access this file*"
            elif len(missing_names) == 2:
                text = f"🔒 *Join {missing_names[0]} and {missing_names[1]} to access this file*"
            else:
                channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                text = f"🔒 *Join {channels_text} to access this file*"

            log.info(f"📨 Sending restriction message to user {user_id} with {len(keyboard)} buttons")
            
            sent_msg = await update.message.reply_text(
                text,
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return

        # User has joined all channels - send file
        log.info(f"✅ User {user_id} has joined all channels. Sending file...")
        await db.update_user_interaction(user_id=user_id, file_accessed=True)

        try:
            filename = file_info['file_name']
            ext = filename.lower().split('.')[-1] if '.' in filename else ""

            warning = f"\n\n⚠️ Auto-deletes in {DELETE_AFTER//60} minutes\n💾 Metadata stored in DB (file on Telegram)"

            if file_info['is_video'] and ext in PLAYABLE_EXTS:
                log.info(f"🎬 Sending video to user {user_id}: {filename}")
                sent = await context.bot.send_video(
                    chat_id=chat_id,
                    video=file_info["file_id"],
                    caption=f"🎬 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                    parse_mode="Markdown",
                    supports_streaming=True
                )
            else:
                log.info(f"📁 Sending document to user {user_id}: {filename}")
                sent = await context.bot.send_document(
                    chat_id=chat_id,
                    document=file_info["file_id"],
                    caption=f"📁 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                    parse_mode="Markdown"
                )

            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            log.info(f"✅ File sent successfully to user {user_id}")

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
                
                log.info(f"❌ User {user_id} missing channels: {missing_names}")
                
                keyboard = []
                
                # Add button for EVERY missing channel
                for i, channel in enumerate(missing_channels):
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
                
                if len(missing_names) == 1:
                    text = f"❌ *Missing {missing_names[0]}*"
                elif len(missing_names) == 2:
                    text = f"❌ *Missing {missing_names[0]} and {missing_names[1]}*"
                else:
                    channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                    text = f"❌ *Missing {channels_text}*"

                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            return

        if data.startswith("check|"):
            _, key = data.split("|")
            log.info(f"🔑 Check again for file {key} from user {user_id}")

            file_info = await db.get_file(key)
            if not file_info:
                log.warning(f"❌ File {key} not found")
                await query.edit_message_text("❌ File not found")
                return

            result = await check_membership(user_id, context, force_check=True)

            if not result['all_joined']:
                missing_channels = result["missing_channels"]
                missing_names = result["missing_channel_names"]
                
                log.info(f"❌ User {user_id} still missing channels: {missing_names}")
                
                keyboard = []
                
                # Add button for EVERY missing channel
                for i, channel in enumerate(missing_channels):
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
                
                if len(missing_names) == 1:
                    text = f"❌ *Join {missing_names[0]}*"
                elif len(missing_names) == 2:
                    text = f"❌ *Join {missing_names[0]} and {missing_names[1]}*"
                else:
                    channels_text = ", ".join(missing_names[:-1]) + f" and {missing_names[-1]}"
                    text = f"❌ *Join {channels_text}*"

                await query.edit_message_text(
                    text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                return

            # User has joined all channels - send file
            log.info(f"✅ User {user_id} now joined all channels. Sending file...")
            await db.update_user_interaction(user_id=user_id, file_accessed=True)

            try:
                filename = file_info['file_name']
                ext = filename.lower().split('.')[-1] if '.' in filename else ""

                warning = f"\n\n⚠️ Auto-deletes in {DELETE_AFTER//60} minutes\n💾 Metadata stored in DB (file on Telegram)"
                chat_id = query.message.chat_id

                if file_info['is_video'] and ext in PLAYABLE_EXTS:
                    log.info(f"🎬 Sending video to user {user_id}: {filename}")
                    sent = await context.bot.send_video(
                        chat_id=chat_id,
                        video=file_info["file_id"],
                        caption=f"🎬 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                        parse_mode="Markdown",
                        supports_streaming=True
                    )
                else:
                    log.info(f"📁 Sending document to user {user_id}: {filename}")
                    sent = await context.bot.send_document(
                        chat_id=chat_id,
                        document=file_info["file_id"],
                        caption=f"📁 *{filename}*\n📥 Accessed {file_info['access_count']} times{warning}",
                        parse_mode="Markdown"
                    )

                await query.edit_message_text("✅ *File sent below!*", parse_mode="Markdown")
                await schedule_message_deletion(context, sent.chat_id, sent.message_id)

            except Exception as e:
                log.error(f"❌ Failed to send file to user {user_id}: {e}", exc_info=True)
                await query.edit_message_text("❌ Failed to send file")

    except Exception as e:
        log.error(f"❌ Callback error: {e}", exc_info=True)

# ============ CHANNEL MANAGEMENT COMMANDS ============
async def addchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a new required channel (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        sent_msg = await update.message.reply_text(
            "❌ Usage: /addchannel <channel username> [friendly name]\n"
            "Example: /addchannel @my_channel \"1\""
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    channel = context.args[0]
    friendly_name = None
    
    # Check if friendly name provided
    if len(context.args) > 1:
        friendly_name = " ".join(context.args[1:])
    
    user_id = update.effective_user.id
    
    # Try to verify bot is admin in the channel
    try:
        clean_channel = channel.replace("@", "")
        chat = await context.bot.get_chat(f"@{clean_channel}")
        
        # Check if bot is admin
        bot_member = await context.bot.get_chat_member(f"@{clean_channel}", context.bot.id)
        if bot_member.status not in ["administrator", "creator"]:
            keyboard = [[InlineKeyboardButton(
                "🤖 Add Bot to Channel",
                url=f"https://t.me/{clean_channel}?startchannel=bot"
            )]]
            
            sent_msg = await update.message.reply_text(
                f"⚠️ *Bot is not an admin in @{clean_channel}*\n\n"
                f"Make sure to add the bot as admin to check memberships!",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
            return
            
    except Exception as e:
        log.warning(f"Could not verify bot in channel {channel}: {e}")
        # Continue anyway - maybe channel exists but bot not added yet

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

    channel = context.args[0]
    
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
        msg += f"{i+1}. {display_name}\n"
        msg += f"   └ Username: @{ch['channel_username']}\n"
        msg += f"   └ Added: {added_date}\n"
    
    if inactive_channels:
        msg += f"\n⏸️ *Inactive Channels ({len(inactive_channels)}):*\n"
        for i, ch in enumerate(inactive_channels):
            display_name = ch['channel_name'] or ch['channel_username']
            msg += f"{i+1}. {display_name} (@{ch['channel_username']})\n"
    
    msg += f"\n💡 *Commands:*\n"
    msg += f"/addchannel @channel [name] - Add channel\n"
    msg += f"/removechannel @channel - Remove channel\n"
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
        channel = ch['channel_username']
        display_name = ch['channel_name'] or channel
        
        try:
            chat = await context.bot.get_chat(f"@{channel}")
            bot_member = await context.bot.get_chat_member(f"@{channel}", context.bot.id)
            
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
            f"💾 *Storage:* Metadata only (file stored on Telegram)\n\n"
            f"🔗 *Link:*\n`{link}`",
            parse_mode="Markdown"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)

    except Exception as e:
        log.exception("Upload error")
        sent_msg = await update.message.reply_text(f"❌ Upload failed: {str(e)[:200]}")
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
    total_access = sum(f[5] for f in files) if files else 0

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
            f"🧹 Auto Cleanup: DISABLED (Permanent storage)",
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
                f"🧹 Auto Cleanup: DISABLED"
            )
            await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        except Exception as e2:
            log.error(f"Even fallback failed: {e2}")

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

# ============ ENHANCED BROADCAST FEATURE ============
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast to users (admin only) - Auto chunk processing with 1000 users per batch"""
    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args and not update.message.reply_to_message:
        sent_msg = await update.message.reply_text(
            "❌ Usage: /broadcast <message> or reply with /broadcast\n"
            "Optional: /broadcast --preview to see preview only\n"
            "Bot will auto-send in chunks of 1000 users"
        )
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    # Check for preview mode
    preview_mode = False
    args_list = context.args if context.args else []
    
    if args_list and args_list[0] == "--preview":
        preview_mode = True
        message_text = " ".join(args_list[1:]) if len(args_list) > 1 else ""
    else:
        # Get message text
        if update.message.reply_to_message:
            message_text = update.message.reply_to_message.text or update.message.reply_to_message.caption
        else:
            message_text = " ".join(args_list) if args_list else ""

    if not message_text:
        sent_msg = await update.message.reply_text("❌ Message cannot be empty")
        await schedule_message_deletion(context, sent_msg.chat_id, sent_msg.message_id)
        return

    # Get all users
    status_msg = await update.message.reply_text(
        "📊 Fetching user list...",
        parse_mode="Markdown"
    )

    user_ids = await db.get_all_user_ids(exclude_admin=True)
    total_users = len(user_ids)

    if total_users == 0:
        await status_msg.edit_text("❌ No users found to broadcast")
        return

    # Preview mode - show sample
    if preview_mode:
        preview_text = f"🔍 *BROADCAST PREVIEW*\n\n"
        preview_text += f"📝 *Message:*\n{message_text[:200]}{'...' if len(message_text) > 200 else ''}\n\n"
        preview_text += f"👥 *Total users:* {total_users}\n"
        preview_text += f"📦 *Chunks:* {(total_users + 999) // 1000} chunks of 1000\n\n"
        preview_text += f"*First 5 users:*\n"
        
        for i, uid in enumerate(user_ids[:5]):
            preview_text += f"{i+1}. `{uid}`\n"
        
        keyboard = [[
            InlineKeyboardButton("✅ Confirm Broadcast", callback_data=f"confirm_broadcast|{total_users}"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_broadcast")
        ]]
        
        await status_msg.edit_text(
            preview_text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        # Store message text in context for later use
        context.chat_data['broadcast_message'] = message_text
        return

    # Direct broadcast without preview
    await status_msg.edit_text(
        f"🔄 Starting broadcast to {total_users} users...\n"
        f"📦 Processing in chunks of 1000 users"
    )
    
    # Start the broadcast process
    asyncio.create_task(process_broadcast_chunks(context, user_ids, message_text, status_msg))

async def process_broadcast_chunks(context: ContextTypes.DEFAULT_TYPE, user_ids: list, message_text: str, status_msg: Message):
    """Process broadcast in chunks of 1000 users"""
    CHUNK_SIZE = 1000
    total_users = len(user_ids)
    total_chunks = (total_users + CHUNK_SIZE - 1) // CHUNK_SIZE
    
    successful = 0
    failed = 0
    blocked = 0
    chunk_results = []
    
    start_time = time.time()
    
    for chunk_num in range(total_chunks):
        chunk_start = chunk_num * CHUNK_SIZE
        chunk_end = min((chunk_num + 1) * CHUNK_SIZE, total_users)
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
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"📢 *Broadcast Message*\n\n{message_text}",
                    parse_mode="Markdown"
                )
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
    await query.answer()
    
    data = query.data
    
    if data == "cancel_broadcast":
        await query.edit_message_text("❌ Broadcast cancelled")
        return
    
    if data.startswith("confirm_broadcast"):
        try:
            total_users = int(data.split("|")[1])
            
            # Get the message text from context
            message_text = context.chat_data.get('broadcast_message', '')
            
            if not message_text:
                await query.edit_message_text("❌ Could not retrieve message. Please try again.")
                return
            
            await query.edit_message_text(
                f"🔄 Starting broadcast to {total_users} users...\n"
                f"📦 Processing in chunks of 1000 users"
            )
            
            # Get all users
            user_ids = await db.get_all_user_ids(exclude_admin=True)
            
            # Start the broadcast process
            asyncio.create_task(process_broadcast_chunks(
                context, user_ids, message_text, query.message
            ))
            
            # Clear stored message
            context.chat_data.pop('broadcast_message', None)
            
        except Exception as e:
            log.error(f"Error in broadcast confirmation: {e}")
            await query.edit_message_text(f"❌ Error starting broadcast: {str(e)[:100]}")

async def clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear membership cache (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return

    # Optional: clear cache for specific channel
    if context.args:
        channel = context.args[0]
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
        channel = ch['channel_username']
        display_name = ch['channel_name'] or channel
        
        try:
            member = await context.bot.get_chat_member(f"@{channel}", user_id)
            status = f"✅ {member.status}"
        except Exception as e:
            status = f"❌ {str(e)[:50]}"
        
        results.append(f"{display_name}: {status}")

    result_text = "🔍 *Channel Access Test*\n\n" + "\n".join(results)
    
    sent_msg = await update.message.reply_text(result_text, parse_mode="Markdown")
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

    # Add job queue for cleanup (only message deletion, not file cleanup)
    if application.job_queue:
        application.job_queue.run_repeating(
            cleanup_overdue_messages,
            interval=300,
            first=10
        )

    # Add error handler
    application.add_error_handler(error_handler)
    
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
    application.add_handler(CommandHandler("removechannel", removechannel))
    application.add_handler(CommandHandler("listchannels", listchannels))
    application.add_handler(CommandHandler("testchannels", testchannels))

    # Add callback handlers
    application.add_handler(CallbackQueryHandler(check_join, pattern="^check_membership$"))
    application.add_handler(CallbackQueryHandler(check_join, pattern="^check\\|"))
    application.add_handler(CallbackQueryHandler(broadcast_callback, pattern="^(confirm_broadcast|cancel_broadcast)$"))

    # Add upload handler (admin only)
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
    log.info(f"📢 Required channels: {await db.get_channel_count()}")
    log.info(f"🧹 Auto cleanup: DISABLED (Permanent storage)")

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
    print("🤖 TELEGRAM FILE BOT - DEBUG VERSION - Shows ALL Missing Channels")
    print("=" * 60)
    print(f"✅ Bot: @{bot_username}")
    print(f"✅ Admin: {ADMIN_ID}")
    print(f"✅ Database: Render PostgreSQL")
    print(f"✅ Auto Cleanup: DISABLED (Permanent storage)")
    print(f"✅ Storage: Metadata only (Files on Telegram)")
    print(f"✅ DEBUG MODE: Extensive logging enabled")
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
