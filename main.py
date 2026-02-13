"""
Optimized Telegram File Bot
- PostgreSQL persistent storage (pg8000)
- Flask health endpoint (threaded)
- Membership check caching
- Auto-delete messages (JobQueue + scheduled_deletions table)
- Admin commands: upload, listfiles, deletefile, stats, users, broadcast, clearcache, testchannel, cleanup

Environment variables expected:
- BOT_TOKEN
- ADMIN_ID
- DATABASE_URL (postgresql://user:pass@host:port/db)
- CHANNEL_1, CHANNEL_2 (channel usernames without @)
- PORT (optional for Flask)

Run: python telegram_file_bot.py
"""

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
import ssl
import urllib.parse
from contextlib import asynccontextmanager

import pg8000

from flask import Flask, render_template_string, jsonify

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ---------------- Configuration & Defaults ----------------
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN not set in environment")
    sys.exit(1)

try:
    ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
except ValueError:
    ADMIN_ID = 0

CHANNEL_1 = os.environ.get("CHANNEL_1", "channel1").replace("@", "")
CHANNEL_2 = os.environ.get("CHANNEL_2", "channel2").replace("@", "")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL is required")
    sys.exit(1)

DELETE_AFTER = int(os.environ.get("DELETE_AFTER", 600))  # seconds
MAX_STORED_FILES = int(os.environ.get("MAX_STORED_FILES", 10000))
AUTO_CLEANUP_DAYS = int(os.environ.get("AUTO_CLEANUP_DAYS", 0))
PLAYABLE_EXTS = {"mp4", "mov", "m4v", "mpeg", "mpg"}
ALL_VIDEO_EXTS = {
    "mp4", "mkv", "mov", "avi", "webm", "flv", "m4v", "3gp", "wmv", "mpg", "mpeg"
}
BROADCAST_LIMIT = int(os.environ.get("BROADCAST_LIMIT", 5000))
bot_username = os.environ.get("BOT_USERNAME", "xiomovies_bot")

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# ---------------- Flask Health Server ----------------
app = Flask(__name__)
start_time = time.time()

# ---------------- Database Helper ----------------

class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.connection = None
        self.connection_lock = asyncio.Lock()
        self._parsed = None

    def _parse_url(self):
        if self._parsed:
            return self._parsed
        # Use urllib.parse to safely parse
        parsed = urllib.parse.urlparse(self.db_url)
        user = parsed.username or ""
        password = urllib.parse.unquote(parsed.password or "")
        host = parsed.hostname or "localhost"
        port = parsed.port or 5432
        database = parsed.path.lstrip("/") or "postgres"
        self._parsed = (user, password, host, port, database)
        return self._parsed

    async def get_connection(self):
        """Return an open pg8000 connection (blocking operations run in thread)."""
        async with self.connection_lock:
            if self.connection is not None:
                try:
                    # quick ping
                    await asyncio.to_thread(self.connection.rollback)
                    return self.connection
                except Exception:
                    log.warning("Database connection appears dead; reconnecting")
                    try:
                        await asyncio.to_thread(self.connection.close)
                    except Exception:
                        pass
                    self.connection = None

            user, password, host, port, database = self._parse_url()
            log.info(f"Connecting to Postgres at {host}:{port}/{database}")

            # Create SSL context (do not verify certs if required)
            ssl_ctx = ssl.create_default_context()
            # Optional: allow disabling verification if explicit env var set
            if os.environ.get("DB_SSL_NO_VERIFY", "true").lower() in ("1", "true", "yes"):
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

            try:
                # pg8000.connect is blocking -> run in thread
                def _connect():
                    return pg8000.connect(
                        user=user,
                        password=password,
                        host=host,
                        port=port,
                        database=database,
                        ssl_context=ssl_ctx,
                        timeout=30,
                    )

                self.connection = await asyncio.to_thread(_connect)
                log.info("Postgres connection established")
                await self.init_db()
                return self.connection

            except Exception as e:
                log.exception("Failed to connect to Postgres")
                raise

    async def execute(self, query: str, params: tuple = None):
        conn = await self.get_connection()

        def _exec():
            cur = conn.cursor()
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            return cur

        return await asyncio.to_thread(_exec)

    async def fetchrow(self, query: str, params: tuple = None):
        cur = await self.execute(query, params)
        return await asyncio.to_thread(cur.fetchone)

    async def fetchall(self, query: str, params: tuple = None):
        cur = await self.execute(query, params)
        return await asyncio.to_thread(cur.fetchall)

    async def execute_and_commit(self, query: str, params: tuple = None):
        cur = await self.execute(query, params)
        conn = await self.get_connection()

        def _commit():
            conn.commit()
            return cur.rowcount

        return await asyncio.to_thread(_commit)

    async def init_db(self):
        """Create required tables if not exist."""
        try:
            await self.execute_and_commit('''
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

            await self.execute_and_commit('''
                CREATE TABLE IF NOT EXISTS membership_cache (
                    user_id BIGINT,
                    channel TEXT,
                    is_member INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, channel)
                )
            ''')

            await self.execute_and_commit('''
                CREATE TABLE IF NOT EXISTS scheduled_deletions (
                    chat_id BIGINT NOT NULL,
                    message_id INTEGER NOT NULL,
                    scheduled_time TIMESTAMP NOT NULL,
                    delete_after INTEGER DEFAULT %s,
                    PRIMARY KEY (chat_id, message_id)
                )
            ''', (DELETE_AFTER,))

            await self.execute_and_commit('''
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

            # Indexes
            await self.execute_and_commit('CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)')
            await self.execute_and_commit('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON membership_cache(timestamp)')
            await self.execute_and_commit('CREATE INDEX IF NOT EXISTS idx_deletions_time ON scheduled_deletions(scheduled_time)')
            await self.execute_and_commit('CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)')

        except Exception:
            log.exception("Failed to initialize DB schema")
            raise

    # -------- CRUD & helpers --------
    async def save_file(self, file_id: str, file_info: dict) -> str:
        result = await self.fetchrow('''
            INSERT INTO files (file_id, file_name, mime_type, is_video, file_size, access_count)
            VALUES (%s, %s, %s, %s, %s, 0)
            RETURNING id
        ''', (
            file_id,
            file_info.get('file_name', ''),
            file_info.get('mime_type', ''),
            1 if file_info.get('is_video', False) else 0,
            file_info.get('size', 0),
        ))
        await (await self.get_connection()).commit()
        new_id = str(result[0])
        log.info(f"Saved file {new_id}: {file_info.get('file_name')}")
        return new_id

    async def get_file(self, file_id: str) -> Optional[dict]:
        try:
            fid = int(file_id)
        except ValueError:
            return None

        result = await self.fetchrow('''
            UPDATE files
            SET access_count = access_count + 1
            WHERE id = %s
            RETURNING file_id, file_name, mime_type, is_video, file_size,
                      TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as timestamp, access_count
        ''', (fid,))

        if result:
            await (await self.get_connection()).commit()
            return {
                'file_id': result[0],
                'file_name': result[1],
                'mime_type': result[2],
                'is_video': bool(result[3]),
                'size': result[4],
                'timestamp': result[5],
                'access_count': result[6],
            }
        return None

    async def get_file_count(self) -> int:
        row = await self.fetchrow('SELECT COUNT(*) FROM files')
        return int(row[0]) if row else 0

    async def cache_membership(self, user_id: int, channel: str, is_member: bool):
        await self.execute_and_commit('''
            INSERT INTO membership_cache (user_id, channel, is_member, timestamp)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id, channel) DO UPDATE
            SET is_member = EXCLUDED.is_member, timestamp = EXCLUDED.timestamp
        ''', (user_id, channel, 1 if is_member else 0))

    async def get_cached_membership(self, user_id: int, channel: str) -> Optional[bool]:
        row = await self.fetchrow('''
            SELECT is_member FROM membership_cache
            WHERE user_id = %s AND channel = %s
              AND timestamp > CURRENT_TIMESTAMP - INTERVAL '5 minutes'
        ''', (user_id, channel))
        return bool(row[0]) if row else None

    async def clear_membership_cache(self, user_id: Optional[int] = None):
        if user_id:
            await self.execute_and_commit('DELETE FROM membership_cache WHERE user_id = %s', (user_id,))
        else:
            await self.execute_and_commit('DELETE FROM membership_cache')

    async def delete_file(self, file_id: str) -> bool:
        try:
            fid = int(file_id)
        except ValueError:
            return False
        rc = await self.execute_and_commit('DELETE FROM files WHERE id = %s', (fid,))
        return rc > 0

    async def get_all_files(self) -> list:
        rows = await self.fetchall('''
            SELECT id, file_name, is_video, file_size, TO_CHAR(timestamp, 'YYYY-MM-DD HH24:MI:SS') as timestamp, access_count
            FROM files
            ORDER BY timestamp DESC
        ''')
        return [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]

    async def schedule_message_deletion(self, chat_id: int, message_id: int):
        scheduled_time = datetime.now() + timedelta(seconds=DELETE_AFTER)
        await self.execute_and_commit('''
            INSERT INTO scheduled_deletions (chat_id, message_id, scheduled_time, delete_after)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chat_id, message_id) DO UPDATE
            SET scheduled_time = EXCLUDED.scheduled_time, delete_after = EXCLUDED.delete_after
        ''', (chat_id, message_id, scheduled_time, DELETE_AFTER))

    async def get_due_messages(self):
        rows = await self.fetchall(
            "SELECT chat_id, message_id FROM scheduled_deletions WHERE scheduled_time <= CURRENT_TIMESTAMP"
        )
        return [(r[0], r[1]) for r in rows]

    async def remove_scheduled_message(self, chat_id: int, message_id: int):
        await self.execute_and_commit('DELETE FROM scheduled_deletions WHERE chat_id = %s AND message_id = %s', (chat_id, message_id))

    async def update_user_interaction(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None, file_accessed: bool = False):
        exists = await self.fetchrow('SELECT 1 FROM users WHERE user_id = %s', (user_id,))
        if exists:
            await self.execute_and_commit('''
                UPDATE users
                SET last_active = CURRENT_TIMESTAMP,
                    total_interactions = total_interactions + 1,
                    username = COALESCE(%s, username),
                    first_name = COALESCE(%s, first_name),
                    last_name = COALESCE(%s, last_name)
                WHERE user_id = %s
            ''', (username, first_name, last_name, user_id))
            if file_accessed:
                await self.execute_and_commit('''
                    UPDATE users
                    SET total_files_accessed = total_files_accessed + 1,
                        last_file_accessed = CURRENT_TIMESTAMP
                    WHERE user_id = %s
                ''', (user_id,))
        else:
            await self.execute_and_commit('''
                INSERT INTO users (user_id, username, first_name, last_name, first_seen, last_active, total_interactions)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
            ''', (user_id, username, first_name, last_name))

    async def get_user_stats(self) -> Dict[str, Any]:
        total_users = (await self.fetchrow('SELECT COUNT(*) FROM users'))[0]
        active_7d = (await self.fetchrow("SELECT COUNT(*) FROM users WHERE last_active > CURRENT_TIMESTAMP - INTERVAL '7 days'"))[0]
        active_30d = (await self.fetchrow("SELECT COUNT(*) FROM users WHERE last_active > CURRENT_TIMESTAMP - INTERVAL '30 days'"))[0]
        new_today = (await self.fetchrow("SELECT COUNT(*) FROM users WHERE DATE(first_seen) = CURRENT_DATE"))[0]
        new_week = (await self.fetchrow("SELECT COUNT(*) FROM users WHERE first_seen > CURRENT_TIMESTAMP - INTERVAL '7 days'"))[0]

        top_rows = await self.fetchall('''
            SELECT user_id, username, first_name, last_name, total_interactions, total_files_accessed,
                   TO_CHAR(last_active, 'YYYY-MM-DD HH24:MI:SS') as last_active,
                   TO_CHAR(first_seen, 'YYYY-MM-DD HH24:MI:SS') as first_seen
            FROM users
            ORDER BY total_interactions DESC
            LIMIT 10
        ''')
        top_users = [tuple(r) for r in top_rows]

        users_files = (await self.fetchrow('SELECT COUNT(DISTINCT user_id) FROM users WHERE total_files_accessed > 0'))[0]

        growth_rows = await self.fetchall('''
            SELECT TO_CHAR(first_seen, 'YYYY-MM-DD') as date, COUNT(*) as new_users
            FROM users
            WHERE first_seen > CURRENT_TIMESTAMP - INTERVAL '30 days'
            GROUP BY date
            ORDER BY date DESC
            LIMIT 15
        ''')
        growth_data = [(r[0], r[1]) for r in growth_rows]

        return {
            'total_users': total_users,
            'active_users_7d': active_7d,
            'active_users_30d': active_30d,
            'new_users_today': new_today,
            'new_users_week': new_week,
            'top_users': top_users,
            'users_with_files': users_files,
            'growth_data': growth_data,
        }

    async def get_all_user_ids(self, exclude_admin: bool = True) -> List[int]:
        if exclude_admin:
            rows = await self.fetchall('SELECT user_id FROM users WHERE user_id != %s', (ADMIN_ID,))
        else:
            rows = await self.fetchall('SELECT user_id FROM users')
        return [r[0] for r in rows]

    async def get_user_count(self) -> int:
        row = await self.fetchrow('SELECT COUNT(*) FROM users')
        return int(row[0]) if row else 0

    async def close(self):
        if self.connection:
            try:
                await asyncio.to_thread(self.connection.close)
            except Exception:
                pass
            self.connection = None

# Initialize DB
db = Database(DATABASE_URL)

# ---------------- Message deletion jobs ----------------

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    chat_id = job.chat_id
    message_id = job.data
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        await db.remove_scheduled_message(chat_id, message_id)
        log.info(f"Deleted message {message_id} from {chat_id}")
    except Exception as e:
        err = str(e).lower()
        if 'message to delete not found' in err:
            await db.remove_scheduled_message(chat_id, message_id)
        elif "can't be deleted" in err or "message can't be deleted" in err:
            log.warning(f"Message {message_id} can't be deleted: {e}")
        else:
            log.exception(f"Failed to delete scheduled message {message_id}")

async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    try:
        await db.schedule_message_deletion(chat_id, message_id)
        if context.job_queue:
            context.job_queue.run_once(delete_message_job, DELETE_AFTER, data=message_id, chat_id=chat_id,
                                       name=f"delete_msg_{chat_id}_{message_id}_{int(time.time())}")
            log.info(f"Scheduled deletion of {message_id} in {DELETE_AFTER}s")
    except Exception:
        log.exception("Failed to schedule message deletion")

async def cleanup_overdue_messages(context: ContextTypes.DEFAULT_TYPE):
    try:
        due = await db.get_due_messages()
        if not due:
            return
        for chat_id, message_id in due:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
                await db.remove_scheduled_message(chat_id, message_id)
                log.info(f"Cleanup deleted {message_id}")
            except Exception as e:
                err = str(e).lower()
                if 'message to delete not found' in err:
                    await db.remove_scheduled_message(chat_id, message_id)
                else:
                    log.exception(f"Cleanup failed for {message_id}")
    except Exception:
        log.exception("Error in cleanup_overdue_messages")

# ---------------- Membership helpers ----------------

async def check_user_in_channel(bot, channel: str, user_id: int, force_check: bool = False) -> bool:
    if not force_check:
        cached = await db.get_cached_membership(user_id, channel)
        if cached is not None:
            return cached

    try:
        channel_username = f"@{channel}" if not channel.startswith('@') else channel
        member = await bot.get_chat_member(chat_id=channel_username, user_id=user_id)
        is_member = member.status in ["member", "administrator", "creator"]
        await db.cache_membership(user_id, channel.replace('@', ''), is_member)
        return is_member
    except Exception as e:
        err = str(e).lower()
        if 'user not found' in err or 'user not participant' in err:
            await db.cache_membership(user_id, channel.replace('@', ''), False)
            return False
        elif 'chat not found' in err:
            log.error(f"Channel @{channel} not found")
            return True
        elif 'forbidden' in err:
            log.error(f"Bot forbidden to access @{channel}")
            return True
        else:
            log.exception('Unknown error checking membership')
            return True

async def check_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE, force_check: bool = False) -> Dict[str, Any]:
    bot = context.bot
    if force_check:
        await db.clear_membership_cache(user_id)

    res = {"channel1": False, "channel2": False, "all_joined": False, "missing_channels": []}

    ch1 = await check_user_in_channel(bot, CHANNEL_1, user_id, force_check)
    res['channel1'] = ch1
    if not ch1:
        res['missing_channels'].append(f"@{CHANNEL_1}")

    ch2 = await check_user_in_channel(bot, CHANNEL_2, user_id, force_check)
    res['channel2'] = ch2
    if not ch2:
        res['missing_channels'].append(f"@{CHANNEL_2}")

    res['all_joined'] = ch1 and ch2
    return res

# ---------------- Flask routes ----------------

HOME_TEMPLATE = """
<!DOCTYPE html>
<html>
<head><meta name="viewport" content="width=device-width, initial-scale=1"><title>Telegram File Bot</title></head>
<body style="font-family:Arial,Helvetica,sans-serif;background:#f6f8fb;color:#222;padding:20px;">
  <div style="max-width:850px;margin:auto;background:#fff;padding:20px;border-radius:8px;box-shadow:0 6px 24px rgba(0,0,0,0.08);">
    <h2>Telegram File Bot</h2>
    <p>Bot: <strong>@{{ bot_username }}</strong></p>
    <p>Uptime: <strong>{{ uptime }}</strong></p>
    <p>Files: <strong>{{ file_count }}</strong> • Users: <strong>{{ user_count }}</strong></p>
    <p>Auto-delete (mins): <strong>{{ delete_minutes }}</strong></p>
    <p><a href="https://t.me/{{ bot_username }}" target="_blank">Start Bot</a></p>
  </div>
</body>
</html>
"""

@app.route('/')
def home():
    uptime = str(timedelta(seconds=int(time.time() - start_time)))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        file_count = loop.run_until_complete(db.get_file_count())
        user_count = loop.run_until_complete(db.get_user_count())
    except Exception:
        file_count = 0
        user_count = 0
    finally:
        loop.close()
    return render_template_string(HOME_TEMPLATE, bot_username=bot_username, uptime=uptime, file_count=file_count, user_count=user_count, delete_minutes=DELETE_AFTER//60)

@app.route('/health')
def health():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        file_count = loop.run_until_complete(db.get_file_count())
        user_count = loop.run_until_complete(db.get_user_count())
    except Exception:
        file_count = 0
        user_count = 0
    finally:
        loop.close()
    return jsonify({
        'status': 'OK',
        'timestamp': datetime.now().isoformat(),
        'uptime': str(timedelta(seconds=int(time.time() - start_time))),
        'file_count': file_count,
        'user_count': user_count,
        'broadcast_limit': BROADCAST_LIMIT,
    }), 200

@app.route('/ping')
def ping():
    return 'pong', 200

def run_flask():
    port = int(os.environ.get('PORT', 10000))
    log.info(f"Starting Flask on port {port}")
    # quieter logs
    import logging as _l
    _l.getLogger('werkzeug').setLevel(_l.ERROR)
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)

# ---------------- Telegram handlers ----------------

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error(f"Error: {context.error}", exc_info=True)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message:
            return
        user = update.effective_user
        chat_id = update.effective_chat.id
        await db.update_user_interaction(user.id, user.username, user.first_name, user.last_name)

        args = context.args
        if not args:
            keyboard = [[InlineKeyboardButton("📢 Join Channel 1", url=f"https://t.me/{CHANNEL_1}" )], [InlineKeyboardButton("📢 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")], [InlineKeyboardButton("🔄 Check Membership", callback_data="check_membership")]]
            sent = await update.message.reply_text(
                "🤖 *Welcome to File Bot*\n\n1. Join both channels\n2. Use admin-provided links\n3. Press Check Membership\n\n⚠️ Messages auto-delete after {} minutes".format(DELETE_AFTER//60),
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            return

        key = args[0]
        file_info = await db.get_file(key)
        if not file_info:
            sent = await update.message.reply_text("❌ File not found")
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            return

        # check membership
        res = await check_membership(user.id, context, force_check=True)
        if not res['all_joined']:
            missing = res['missing_channels']
            if len(missing) == 2:
                kb = [[InlineKeyboardButton("📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}")],[InlineKeyboardButton("📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}")],[InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]]
                txt = "🔒 *Join both channels to access this file*"
            else:
                ch = missing[0].replace('@','')
                name = 'Channel 1' if CHANNEL_1 in ch else 'Channel 2'
                kb = [[InlineKeyboardButton(f"📥 Join {name}", url=f"https://t.me/{ch}" )],[InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]]
                txt = f"🔒 *Join {name} to access this file*"
            sent = await update.message.reply_text(txt, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            return

        # send file
        await db.update_user_interaction(user.id, user.username, user.first_name, user.last_name, file_accessed=True)

        try:
            filename = file_info['file_name']
            ext = filename.lower().split('.')[-1] if '.' in filename else ''
            warning = f"\n\n⚠️ Auto-deletes in {DELETE_AFTER//60} minutes\n💾 Permanently stored"

            if file_info['is_video'] and ext in PLAYABLE_EXTS:
                sent = await context.bot.send_video(chat_id=chat_id, video=file_info['file_id'], caption=f"🎬 *{filename}*{warning}", parse_mode='Markdown', supports_streaming=True)
            else:
                sent = await context.bot.send_document(chat_id=chat_id, document=file_info['file_id'], caption=f"📁 *{filename}*{warning}", parse_mode='Markdown')

            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        except Exception:
            log.exception("Failed to send file")
            sent = await update.message.reply_text("❌ Failed to send file")
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)

    except Exception:
        log.exception('start handler failed')

async def check_join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user = query.from_user
        await db.update_user_interaction(user.id, user.username, user.first_name, user.last_name)
        data = query.data

        if data == 'check_membership':
            res = await check_membership(user.id, context, force_check=True)
            if res['all_joined']:
                await query.edit_message_text("✅ *You've joined both channels!*\nNow you can use admin links.", parse_mode='Markdown')
            else:
                missing = res['missing_channels']
                if len(missing) == 2:
                    kb = [[InlineKeyboardButton("📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}" )],[InlineKeyboardButton("📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}" )],[InlineKeyboardButton("🔄 Check Again", callback_data='check_membership')]]
                    txt = "❌ *Not a member of either channel*"
                else:
                    ch = missing[0].replace('@','')
                    name = 'Channel 1' if CHANNEL_1 in ch else 'Channel 2'
                    kb = [[InlineKeyboardButton(f"📥 Join {name}", url=f"https://t.me/{ch}" )],[InlineKeyboardButton("🔄 Check Again", callback_data='check_membership')]]
                    txt = f"❌ *Missing {name}*"
                await query.edit_message_text(txt, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
            return

        if data.startswith('check|'):
            _, key = data.split('|', 1)
            file_info = await db.get_file(key)
            if not file_info:
                await query.edit_message_text('❌ File not found')
                return
            res = await check_membership(user.id, context, force_check=True)
            if not res['all_joined']:
                missing = res['missing_channels']
                if len(missing) == 2:
                    kb = [[InlineKeyboardButton("📥 Join Channel 1", url=f"https://t.me/{CHANNEL_1}" )],[InlineKeyboardButton("📥 Join Channel 2", url=f"https://t.me/{CHANNEL_2}" )],[InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]]
                    txt = "❌ *Join both channels*"
                else:
                    ch = missing[0].replace('@','')
                    name = 'Channel 1' if CHANNEL_1 in ch else 'Channel 2'
                    kb = [[InlineKeyboardButton(f"📥 Join {name}", url=f"https://t.me/{ch}" )],[InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")]]
                    txt = f"❌ *Join {name}*"
                await query.edit_message_text(txt, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
                return

            # send file
            await db.update_user_interaction(user.id, user.username, user.first_name, user.last_name, file_accessed=True)
            try:
                filename = file_info['file_name']
                ext = filename.lower().split('.')[-1] if '.' in filename else ''
                warning = f"\n\n⚠️ Auto-deletes in {DELETE_AFTER//60} minutes\n💾 Permanently stored"
                chat_id = query.message.chat_id
                if file_info['is_video'] and ext in PLAYABLE_EXTS:
                    sent = await context.bot.send_video(chat_id=chat_id, video=file_info['file_id'], caption=f"🎬 *{filename}*{warning}", parse_mode='Markdown', supports_streaming=True)
                else:
                    sent = await context.bot.send_document(chat_id=chat_id, document=file_info['file_id'], caption=f"📁 *{filename}*{warning}", parse_mode='Markdown')
                await query.edit_message_text('✅ *File sent below!*', parse_mode='Markdown')
                await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            except Exception:
                log.exception('Failed to send file from callback')
                await query.edit_message_text('❌ Failed to send file')

    except Exception:
        log.exception('check_join failed')

async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        msg = update.message
        video = msg.video
        document = msg.document

        if not (video or document):
            sent = await msg.reply_text('❌ Send a video or document')
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            return

        file_id = None
        filename = None
        mime_type = None
        file_size = 0
        is_video = False

        if video:
            file_id = video.file_id
            filename = video.file_name or f"video_{int(time.time())}.mp4"
            mime_type = video.mime_type or 'video/mp4'
            file_size = video.file_size or 0
            is_video = True
        else:
            file_id = document.file_id
            filename = document.file_name or f"document_{int(time.time())}"
            mime_type = document.mime_type or ''
            file_size = document.file_size or 0
            ext = filename.lower().split('.')[-1] if '.' in filename else ''
            if ext in ALL_VIDEO_EXTS:
                is_video = True

        info = {'file_name': filename, 'mime_type': mime_type, 'is_video': is_video, 'size': int(file_size)}
        key = await db.save_file(file_id, info)
        link = f"https://t.me/{bot_username}?start={key}"
        sent = await msg.reply_text(f"✅ Upload successful\n📁 Name: `{filename}`\n🔑 Key: `{key}`\nLink: `{link}`", parse_mode='Markdown')
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
    except Exception:
        log.exception('upload failed')
        sent = await update.message.reply_text('❌ Upload failed')
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        uptime = str(timedelta(seconds=int(time.time() - start_time)))
        file_count = await db.get_file_count()
        user_count = await db.get_user_count()
        files = await db.get_all_files()
        total_access = sum(f[5] for f in files) if files else 0
        sent = await update.message.reply_text(
            f"📊 Bot Stats\nUptime: {uptime}\nFiles: {file_count}\nUsers: {user_count}\nAccesses: {total_access}\nAuto-delete: {DELETE_AFTER//60} minutes",
            parse_mode='Markdown')
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
    except Exception:
        log.exception('stats failed')

async def listfiles(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    files = await db.get_all_files()
    if not files:
        sent = await update.message.reply_text('📁 No files stored')
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        return
    msg = f"📁 Total Files: {len(files)}\n\n"
    for f in files[:50]:
        fid, name, is_video, size, ts, access = f
        size_mb = size/(1024*1024) if size else 0
        msg += f"`{fid}` - {name[:40]} ({size_mb:.1f}MB) - {access} accesses\n"
    sent = await update.message.reply_text(msg, parse_mode='Markdown')
    await schedule_message_deletion(context, sent.chat_id, sent.message_id)

async def deletefile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        sent = await update.message.reply_text('❌ Usage: /deletefile <key>')
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        return
    key = context.args[0]
    ok = await db.delete_file(key)
    sent = await update.message.reply_text(f"✅ Deleted" if ok else "❌ File not found")
    await schedule_message_deletion(context, sent.chat_id, sent.message_id)

async def users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    stats = await db.get_user_stats()
    msg = (
        f"👥 Total Users: {stats['total_users']}\n"
        f"🟢 Active (7d): {stats['active_users_7d']}\n"
        f"🟡 Active (30d): {stats['active_users_30d']}\n"
        f"📈 New Today: {stats['new_users_today']}\n"
        f"📁 File Accessors: {stats['users_with_files']}\n"
        f"📢 Broadcast Limit: {BROADCAST_LIMIT}"
    )
    sent = await update.message.reply_text(msg, parse_mode='Markdown')
    await schedule_message_deletion(context, sent.chat_id, sent.message_id)

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args and not update.message.reply_to_message:
        sent = await update.message.reply_text('❌ Usage: /broadcast <message> or reply with /broadcast')
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        return
    if update.message.reply_to_message:
        message_text = update.message.reply_to_message.text or update.message.reply_to_message.caption
    else:
        message_text = ' '.join(context.args)
    if not message_text:
        sent = await update.message.reply_text('❌ Message cannot be empty')
        await schedule_message_deletion(context, sent.chat_id, sent.message_id)
        return
    user_ids = await db.get_all_user_ids(exclude_admin=True)
    users_to_send = user_ids[:BROADCAST_LIMIT]
    status = await update.message.reply_text(f"🔄 Broadcasting to {len(users_to_send)} users...")
    success = 0
    failed = 0
    for uid in users_to_send:
        try:
            await context.bot.send_message(chat_id=uid, text=f"📢 Broadcast\n\n{message_text}")
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            log.warning(f"Broadcast failed to {uid}: {e}")
    if len(user_ids) > BROADCAST_LIMIT:
        remaining = len(user_ids) - BROADCAST_LIMIT
        result_text = f"✅ Partial broadcast\nSent: {success}\nFailed: {failed}\nNot processed: {remaining} (limit: {BROADCAST_LIMIT})"
    else:
        result_text = f"✅ Broadcast complete\nSent: {success}\nFailed: {failed}"
    await status.edit_text(result_text)
    await schedule_message_deletion(context, status.chat_id, status.message_id)

async def clearcache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await db.clear_membership_cache()
    sent = await update.message.reply_text('✅ Cache cleared')
    await schedule_message_deletion(context, sent.chat_id, sent.message_id)

async def testchannel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    uid = update.effective_user.id
    try:
        m1 = await context.bot.get_chat_member(f"@{CHANNEL_1}", uid)
        ch1 = f"✅ {m1.status}"
    except Exception as e:
        ch1 = f"❌ {str(e)[:50]}"
    try:
        m2 = await context.bot.get_chat_member(f"@{CHANNEL_2}", uid)
        ch2 = f"✅ {m2.status}"
    except Exception as e:
        ch2 = f"❌ {str(e)[:50]}"
    sent = await update.message.reply_text(f"@{CHANNEL_1}: {ch1}\n@{CHANNEL_2}: {ch2}", parse_mode='Markdown')
    await schedule_message_deletion(context, sent.chat_id, sent.message_id)

async def cleanup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    days = 30
    if context.args:
        try:
            days = int(context.args[0])
        except Exception:
            pass
    # Delete files older than days
    await db.execute_and_commit("DELETE FROM files WHERE timestamp < CURRENT_TIMESTAMP - INTERVAL '1 day' * %s", (days,))
    fc = await db.get_file_count()
    sent = await update.message.reply_text(f"🧹 Cleaned files older than {days} days. Remaining: {fc}")
    await schedule_message_deletion(context, sent.chat_id, sent.message_id)

# ---------------- Main bot runner ----------------

async def run_bot():
    try:
        await db.get_connection()

        application = Application.builder().token(BOT_TOKEN).build()

        # Job queue for periodic cleanup
        if application.job_queue:
            application.job_queue.run_repeating(cleanup_overdue_messages, interval=300, first=10)

        # Handlers
        application.add_error_handler(error_handler)
        application.add_handler(CommandHandler('start', start))
        application.add_handler(CommandHandler('stats', stats))
        application.add_handler(CommandHandler('listfiles', listfiles))
        application.add_handler(CommandHandler('deletefile', deletefile))
        application.add_handler(CommandHandler('users', users))
        application.add_handler(CommandHandler('broadcast', broadcast))
        application.add_handler(CommandHandler('clearcache', clearcache))
        application.add_handler(CommandHandler('testchannel', testchannel))
        application.add_handler(CommandHandler('cleanup', cleanup_cmd))

        application.add_handler(CallbackQueryHandler(check_join, pattern='^check_membership$'))
        application.add_handler(CallbackQueryHandler(check_join, pattern='^check\\|'))

        upload_filter = filters.VIDEO | filters.Document.ALL
        application.add_handler(MessageHandler(upload_filter & filters.User(ADMIN_ID) & filters.ChatType.PRIVATE, upload))

        log.info('Bot starting polling...')
        await application.run_polling(allowed_updates=Update.ALL_TYPES)

    except Exception:
        log.exception('Fatal error in run_bot')
        raise
    finally:
        await db.close()

# ---------------- Entrypoint ----------------

def main():
    print('='*60)
    print('Telegram File Bot')
    print('='*60)

    flask_t = threading.Thread(target=run_flask, daemon=True)
    flask_t.start()
    log.info('Flask thread started')

    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        log.info('Interrupted by user')
    except Exception:
        log.exception('Unhandled exception')
    finally:
        log.info('Shutdown complete')

if __name__ == '__main__':
    main()
