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
from flask import Flask, render_template_string, jsonify

# Flask app for web dashboard
app = Flask(__name__)

# Global variables
starttime = time.time()
botusername = "xoticcroissant_bot"

# CONFIG - Set these as environment variables
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMIN_ID = int(os.environ.get('ADMIN_ID', 0))
CHANNEL1 = os.environ.get('CHANNEL1', 'AKnightoftheSevenKingdoms').replace('@', '')
CHANNEL2 = os.environ.get('CHANNEL2', 'yourmoviesweb').replace('@', '')

DB_PATH = Path('filebot.db')
DELETE_AFTER = 20  # 10 minutes - DELETE ALL BOT MESSAGES
MAX_STORED_FILES = 1000
AUTO_CLEANUP_DAYS = 0  # Set to 0 to NEVER auto-cleanup files

PLAYABLE_EXTS = {'mp4', 'mov', 'm4v', 'mpeg', 'mpg'}
ALL_VIDEO_EXTS = {'mp4', 'mkv', 'mov', 'avi', 'webm', 'flv', 'm4v', '3gp', 'wmv', 'mpg', 'mpeg'}

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
log = logging.getLogger(__name__)

class Database:
    def __init__(self, dbpath: Path):
        self.dbpath = dbpath
        self.init_db()
        self.dblock = threading.Lock()

    def init_db(self):
        """Initialize database with required tables"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS files (
                    id TEXT PRIMARY KEY,
                    fileid TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    mimetype TEXT,
                    isvideo INTEGER DEFAULT 0,
                    filesize INTEGER DEFAULT 0,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    accesscount INTEGER DEFAULT 0
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS membershipcache (
                    userid INTEGER,
                    channel TEXT,
                    ismember INTEGER,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (userid, channel)
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_timestamp ON files(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON membershipcache(timestamp)')
            conn.commit()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(str(self.dbpath), timeout=30, check_same_thread=False)
        try:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            yield conn
        finally:
            conn.close()

    def save_file(self, fileid: str, fileinfo: dict) -> str:
        """Save file info and return generated ID"""
        with self.dblock:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COALESCE(MAX(CAST(id AS INTEGER)), 0) FROM files')
                maxid = cursor.fetchone()[0]
                newid = str(maxid + 1)
                cursor.execute('''
                    INSERT INTO files (id, fileid, filename, mimetype, isvideo, filesize, accesscount)
                    VALUES (?, ?, ?, ?, ?, ?, 0)
                ''', (
                    newid, fileid, fileinfo.get('filename', ''),
                    fileinfo.get('mimetype', ''), 
                    1 if fileinfo.get('isvideo', False) else 0,
                    fileinfo.get('size', 0)
                ))
                conn.commit()
                return newid

    def get_file(self, fileid: str) -> Optional[dict]:
        """Get file info by ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT fileid, filename, mimetype, isvideo, filesize, timestamp, accesscount
                FROM files WHERE id = ?
            ''', (fileid,))
            row = cursor.fetchone()
            if row:
                cursor.execute('UPDATE files SET accesscount = accesscount + 1 WHERE id = ?', (fileid,))
                conn.commit()
                return {
                    'fileid': row[0], 'filename': row[1], 'mimetype': row[2],
                    'isvideo': bool(row[3]), 'size': row[4], 'timestamp': row[5],
                    'accesscount': row[6] + 1
                }
            return None

    def delete_file(self, fileid: str) -> bool:
        """Manually delete a file from database (admin only)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM files WHERE id = ?', (fileid,))
            deleted = cursor.rowcount > 0
            conn.commit()
            return deleted

    def get_all_files(self) -> list:
        """Get all files for admin view"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, filename, isvideo, filesize, timestamp, accesscount
                FROM files ORDER BY timestamp DESC
            ''')
            return cursor.fetchall()

    def get_file_count(self) -> int:
        """Get total number of files"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM files')
            return cursor.fetchone()[0]

    def cache_membership(self, userid: int, channel: str, ismember: bool):
        """Cache membership check result"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO membershipcache (userid, channel, ismember, timestamp)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (userid, channel.replace('@', ''), 1 if ismember else 0))
            conn.commit()

    def get_cached_membership(self, userid: int, channel: str) -> Optional[bool]:
        """Get cached membership result (valid for 5 minutes)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT ismember FROM membershipcache
                WHERE userid = ? AND channel = ? AND timestamp > datetime('now', '-5 minutes')
            ''', (userid, channel.replace('@', '')))
            row = cursor.fetchone()
            return bool(row[0]) if row else None

    def clear_membership_cache(self, userid: Optional[int] = None):
        """Clear membership cache for a user or all users"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if userid:
                cursor.execute('DELETE FROM membershipcache WHERE userid = ?', (userid,))
                log.info(f'Cleared cache for user {userid}')
            else:
                cursor.execute('DELETE FROM membershipcache')
                log.info('Cleared all membership cache')
            conn.commit()

# Initialize database
db = Database(DB_PATH)

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# FIXED: Proper Application with JobQueue
application = (
    ApplicationBuilder()
    .token(BOT_TOKEN)
    .job_queue(True)  # ✅ FIXED: Enable JobQueue for auto-delete
    .build()
)

async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    """Schedule a message for deletion after DELETE_AFTER seconds"""
    if not context.job_queue:
        log.warning(f'Job queue not available - cannot schedule deletion for message {message_id}')
        return
    
    try:
        # FIXED: Proper job data structure
        context.job_queue.run_once(
            delete_message_job,
            when=DELETE_AFTER,
            data={'chat_id': chat_id, 'message_id': message_id},
            name=f'delete_msg_{chat_id}_{message_id}_{int(time.time())}'
        )
        log.info(f'Scheduled deletion of message {message_id} from chat {chat_id} in {DELETE_AFTER} seconds')
    except Exception as e:
        log.error(f'Failed to schedule deletion for message {message_id}: {e}')

async def delete_message_job(context: ContextTypes.DEFAULT_TYPE):
    """Delete message after timer"""
    try:
        job = context.job
        chat_id = job.data['chat_id']
        message_id = job.data['message_id']
        
        if not chat_id or not message_id:
            log.warning(f'Invalid delete job data: chat_id={chat_id}, message_id={message_id}')
            return

        log.info(f'Attempting to delete message {message_id} from chat {chat_id}')
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        log.info(f'Successfully deleted message {message_id} from chat {chat_id}')
        
    except Exception as e:
        errormsg = str(e).lower()
        if 'message to delete not found' in errormsg:
            log.info(f'Message {message_id} already deleted from chat {chat_id}')
        elif 'message can\'t be deleted' in errormsg:
            log.warning(f"Can't delete message {message_id} - insufficient permissions in chat {chat_id}")
        elif 'chat not found' in errormsg:
            log.info(f'Chat {chat_id} not found - message probably already deleted')
        else:
            log.error(f'Failed to delete message {message_id} from chat {chat_id}: {e}')

async def check_user_in_channel(bot, channel: str, userid: int, forcecheck: bool = False) -> bool:
    """Check if user is in channel. Returns True if user is member, False if not or can't check"""
    if not forcecheck:
        cached = db.get_cached_membership(userid, channel)
        if cached is not None:
            log.info(f'Cache hit for user {userid} in {channel}: {cached}')
            return cached

    try:
        channel_username = f'@{channel}' if not channel.startswith('@') else channel
        log.info(f'Checking user {userid} in {channel_username}')
        member = await bot.get_chat_member(chat_id=channel_username, user_id=userid)
        ismember = member.status in ('member', 'administrator', 'creator')
        log.info(f'User {userid} in {channel_username} status={member.status}, ismember={ismember}')
        db.cache_membership(userid, channel, ismember)
        return ismember
    except Exception as e:
        errormsg = str(e).lower()
        log.warning(f'Failed to check user {userid} in {channel}: {e}')
        if 'user not found' in errormsg or 'user not participant' in errormsg:
            db.cache_membership(userid, channel.replace('@', ''), False)
            return False
        elif 'chat not found' in errormsg:
            log.error(f'Channel {channel} not found!')
            return True  # Assume member if channel not found
        elif 'forbidden' in errormsg:
            log.error(f'Bot can\'t access channel. Might be private or bot not admin.')
            return True  # Assume member to avoid blocking
        else:
            return True  # For other errors, don't cache and assume True to avoid blocking

async def check_membership(userid: int, context: ContextTypes.DEFAULT_TYPE, forcecheck: bool = False) -> Dict[str, Any]:
    """Check if user is member of both channels"""
    if forcecheck:
        db.clear_membership_cache(userid)

    result = {'channel1': False, 'channel2': False, 'all_joined': False, 'missing_channels': []}
    
    try:
        ch1_result = await check_user_in_channel(context.bot, CHANNEL1, userid, forcecheck)
        result['channel1'] = ch1_result
        if not ch1_result:
            result['missing_channels'].append(f'{CHANNEL1}')
    except Exception as e:
        log.error(f'Error checking channel 1: {e}')
        result['channel1'] = True  # Assume true on error to not block

    try:
        ch2_result = await check_user_in_channel(context.bot, CHANNEL2, userid, forcecheck)
        result['channel2'] = ch2_result
        if not ch2_result:
            result['missing_channels'].append(f'{CHANNEL2}')
    except Exception as e:
        log.error(f'Error checking channel 2: {e}')
        result['channel2'] = True  # Assume true on error to not block

    result['all_joined'] = result['channel1'] and result['channel2']
    log.info(f'Membership check for {userid}: ch1={result["channel1"]}, ch2={result["channel2"]}, all={result["all_joined"]}')
    return result

# Web dashboard routes
@app.route('/')
def home():
    uptime_seconds = int(time.time() - starttime)
    uptime_str = str(timedelta(seconds=uptime_seconds))
    filecount = 0
    try:
        filecount = db.get_file_count()
    except:
        pass
    
    html_content = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Telegram File Bot</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif; margin: 0; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; min-height: 100vh; }
            .container { background: rgba(255, 255, 255, 0.1); backdrop-filter: blur(10px); padding: 20px; border-radius: 10px; box-shadow: 0 4px 16px rgba(0, 0, 0, 0.2); }
            h1 { color: white; margin-top: 0; font-size: 1.5rem; }
            .status { background: rgba(0, 255, 0, 0.2); padding: 10px; border-radius: 8px; margin: 10px 0; border-left: 4px solid #00ff00; }
            .info { background: rgba(255, 255, 255, 0.1); padding: 10px; border-radius: 8px; margin: 10px 0; }
            a { color: #FFD700; text-decoration: none; }
            .btn { display: inline-block; background: #4CAF50; color: white; padding: 8px 16px; border-radius: 6px; margin: 5px; font-size: 0.9rem; }
            .warning { background: rgba(255, 165, 0, 0.2); border-left: 4px solid #ffa500; padding: 10px; border-radius: 8px; margin: 10px 0; font-size: 0.9rem; }
            code { background: rgba(0, 0, 0, 0.3); padding: 2px 4px; border-radius: 3px; font-family: monospace; font-size: 0.9rem; }
            ul { padding-left: 20px; }
            li { margin: 5px 0; }
            .error { background: rgba(255, 0, 0, 0.2); border-left: 4px solid #ff0000; padding: 10px; border-radius: 8px; margin: 10px 0; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Telegram File Bot</h1>
            <div class="status">
                <h3>Status: <strong>ACTIVE</strong></h3>
                <p>Bot is running on Render</p>
                <p>Uptime: {{ uptime }}</p>
                <p>Files in DB: {{ filecount }}</p>
                <p>Storage: PERMANENT (no auto-delete)</p>
            </div>
            <div class="info">
                <h3>Bot Information</h3>
                <ul>
                    <li>Service: <strong>Render Web Service</strong></li>
                    <li>Bot: <strong>{{ botusername }}</strong></li>
                    <li>Channels: <strong>{{ channel1 }}, {{ channel2 }}</strong></li>
                    <li>File Storage: <strong>PERMANENT</strong></li>
                    <li>Message Auto-delete: <strong>{{ deleteminutes }} minutes</strong></li>
                </ul>
            </div>
            <div class="warning">
                <h3>Important Notes</h3>
                <ul>
                    <li>Files are stored <strong>PERMANENTLY</strong> in database</li>
                    <li>Only chat messages auto-delete after {{ deleteminutes }} minutes</li>
                    <li>Users can access same file multiple times (forever)</li>
                    <li>Admin must manually delete files if needed</li>
                </ul>
            </div>
            <div class="info">
                <h3>Start Bot</h3>
                <p><a href="https://t.me/{{ botusername }}" target="_blank" class="btn">Start {{ botusername }}</a></p>
            </div>
            <footer style="margin-top: 20px; border-top: 1px solid rgba(255,255,255,0.2); padding-top: 10px; font-size: 0.8rem;">
                <small>Render {{ currenttime }} v1.0 Permanent Storage</small>
            </footer>
        </div>
    </body>
    </html>
    '''
    deleteminutes = DELETE_AFTER // 60
    return render_template_string(html_content, 
                                botusername=botusername, 
                                uptime=uptime_str,
                                currenttime=datetime.now().strftime('%H:%M:%S'),
                                filecount=filecount,
                                channel1=CHANNEL1, 
                                channel2=CHANNEL2, 
                                deleteminutes=deleteminutes)

@app.route('/health')
def health():
    return jsonify({
        'status': 'OK',
        'timestamp': datetime.now().isoformat(),
        'service': 'telegram-file-bot',
        'uptime': str(timedelta(seconds=int(time.time() - starttime))),
        'database': 'sqlite',
        'storage': 'permanent',
        'filecount': db.get_file_count()
    }), 200

@app.route('/ping')
def ping():
    return 'pong', 200

def run_flask_thread():
    """Run Flask server in a thread for Render"""
    port = int(os.environ.get('PORT', 10000))
    import warnings
    warnings.filterwarnings('ignore')
    import logging as flask_logging
    flask_logging.getLogger('werkzeug').setLevel(flask_logging.ERROR)
    flask_logging.getLogger('flask').setLevel(flask_logging.ERROR)
    
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False, threaded=True)

# Admin commands
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
            await update.message.reply_text('Usage: /cleanup [days=30]\n(days=0 to cancel)')
            return
    
    if days == 0:
        await update.message.reply_text('Cleanup cancelled. Files will be kept permanently.')
        return
    
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM files WHERE timestamp < datetime(?, ?) ', (f'-{days} days',))
            deleted = cursor.rowcount
            conn.commit()
        
        filecount = db.get_file_count()
        msg = (f'Manual database cleanup complete!\n'
               f'Files retained in database: {filecount}\n'
               f'Files older than {days} days removed: {deleted}\n'
               f'Note: Auto-cleanup is DISABLED. Files are kept permanently by default.')
        
        sentmsg = await update.message.reply_text(msg)
        await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
    except Exception as e:
        await update.message.reply_text(f'Cleanup failed: {str(e)[:100]}')

async def delete_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually delete a specific file from database"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    if not context.args:
        sentmsg = await update.message.reply_text('Usage: /deletefile filekey\nExample: /deletefile 123\nUse /listfiles to see all files')
        await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
        return
    
    key = context.args[0]
    fileinfo = db.get_file(key)
    
    if not fileinfo:
        sentmsg = await update.message.reply_text(f'File with key `{key}` not found in database')
        await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
        return
    
    filename = fileinfo.get('filename', 'Unknown')
    if db.delete_file(key):
        sentmsg = await update.message.reply_text(
            f'File deleted from database!\n'
            f'Key: `{key}`\n'
            f'Name: {filename}\n'
            f'This file can no longer be accessed by users'
        )
        await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
    else:
        sentmsg = await update.message.reply_text(f'Failed to delete file `{key}`')
        await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)

async def list_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all files in database"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        files = db.get_all_files()
        if not files:
            sentmsg = await update.message.reply_text('Database is empty. No files stored.')
            await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
            return
        
        totalsize = 0
        totalaccess = 0
        messageparts = []
        for i, file in enumerate(files[:50]):  # Show first 50 files
            fileid, filename, isvideo, size, timestamp, accesscount = file
            totalsize += size if size else 0
            totalaccess += accesscount
            sizemb = size / 1024 / 1024 if size else 0
            try:
                dateobj = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                datestr = dateobj.strftime('%b %d, %Y')
            except:
                datestr = timestamp
            
            messageparts.append(f'`{fileid}` `{filename[:30]}...` if len(filename) > 30 else f"`{fileid}` `{filename}`"')
            messageparts.append(f"Video{'✓' if isvideo else ''} {sizemb:.1f}MB {datestr} {accesscount}x")
        
        summary = (f'Database Summary\n'
                  f'Total files: {len(files)}\n'
                  f'Total size: {totalsize/1024/1024/1024:.2f} GB\n'
                  f'Total accesses: {totalaccess}\n'
                  f'Storage: PERMANENT (no auto-delete)\n'
                  f'Files showing: min(50, {len(files)}) of {len(files)}')
        
        fullmessage = summary + '\n\n' + '\n'.join(messageparts)
        if len(fullmessage) > 4000:
            fullmessage = fullmessage[:4000]
        
        if len(fullmessage) > 4000:
            sentmsg1 = await update.message.reply_text(fullmessage[:4000], parse_mode='Markdown')
            sentmsg2 = await update.message.reply_text(fullmessage[4000:8000][:4000], parse_mode='Markdown')
            await schedule_message_deletion(context, sentmsg1.chat_id, sentmsg1.message_id)
            await schedule_message_deletion(context, sentmsg2.chat_id, sentmsg2.message_id)
        else:
            sentmsg = await update.message.reply_text(fullmessage, parse_mode='Markdown')
            await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
    except Exception as e:
        log.error(f'Error listing files: {e}')
        sentmsg = await update.message.reply_text(f'Error listing files: {str(e)[:200]}')
        await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Bot statistics"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    uptime_seconds = time.time() - starttime
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))
    filecount = db.get_file_count()
    dbsize = DB_PATH.stat().st_size / 1024 if DB_PATH.exists() else 0
    
    totalaccess = 0
    try:
        files = db.get_all_files()
        totalaccess = sum(file[5] for file in files)
    except:
        pass
    
    msg = (f'Bot Statistics\n'
           f'Bot: `{botusername}`\n'
           f'Uptime: {uptime_str}\n'
           f'Files in database: {filecount}\n'
           f'Total accesses: {totalaccess}\n'
           f'DB Size: {dbsize:.1f} KB\n'
           f'Auto-cleanup: DISABLED (permanent storage)\n'
           f'Message auto-delete: {DELETE_AFTER//60} minutes\n'
           f'Channels:\n1. `{CHANNEL1}`\n2. `{CHANNEL2}`\n'
           f'Admin commands:\n'
           f'`/listfiles` - View all files\n'
           f'`/deletefile key` - Delete specific file\n'
           f'`/cleanup days` - Manual cleanup (optional)')
    
    sentmsg = await update.message.reply_text(msg, parse_mode='Markdown')
    await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)

async def clear_cache(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear membership cache"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    userid = None
    if context.args:
        try:
            userid = int(context.args[0])
        except ValueError:
            sentmsg = await update.message.reply_text('Usage: /clearcache [userid]')
            await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
            return
    
    db.clear_membership_cache(userid)
    if userid:
        sentmsg = await update.message.reply_text(f'Cleared cache for user `{userid}`')
    else:
        sentmsg = await update.message.reply_text('Cleared all membership cache')
    
    await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)

async def test_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test if bot can access channels"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    userid = update.effective_user.id
    try:
        member1 = await context.bot.get_chat_member(f'@{CHANNEL1}', userid)
        ch1status = f'Accessible - Your status: {member1.status}'
    except Exception as e:
        ch1status = f'Error: {str(e)[:100]}'
    
    try:
        member2 = await context.bot.get_chat_member(f'@{CHANNEL2}', userid)
        ch2status = f'Accessible - Your status: {member2.status}'
    except Exception as e:
        ch2status = f'Error: {str(e)[:100]}'
    
    sentmsg = await update.message.reply_text(
        f'Channel Access Test\n'
        f'Channel 1 (`{CHANNEL1}`): {ch1status}\n'
        f'Channel 2 (`{CHANNEL2}`): {ch2status}'
    )
    await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)

# Main handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command"""
    try:
        if not update.message:
            return
        
        userid = update.effective_user.id
        chat_id = update.effective_chat.id
        args = context.args
        
        if not args:
            # No file key - show join info
            keyboard = []
            keyboard.append([InlineKeyboardButton("Join Channel 1", url=f'https://t.me/{CHANNEL1}')])
            keyboard.append([InlineKeyboardButton("Join Channel 2", url=f'https://t.me/{CHANNEL2}')])
            keyboard.append([InlineKeyboardButton("Check Membership", callback_data='checkmembership')])
            
            sentmsg = await update.message.reply_text(
                'Welcome to File Sharing Bot!\n\n'
                'How to use:\n'
                '1. Use admin-provided links\n'
                '2. Join both channels below\n'
                '3. Click Check Membership after joining\n\n'
                f'Note: All bot messages auto-delete after {DELETE_AFTER//60} minutes\n'
                'Storage: Files are stored permanently in database',
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
            return
        
        # File key provided
        key = args[0]
        fileinfo = db.get_file(key)
        
        if not fileinfo:
            sentmsg = await update.message.reply_text('File not found. It may have been manually deleted by admin.')
            await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
            return
        
        # Check membership (force fresh check for start command)
        result = await check_membership(userid, context, forcecheck=True)
        if not result['all_joined']:
            missingcount = len(result['missing_channels'])
            
            if missingcount == 2:
                messagetext = 'Access Required\n\nYou need to join both channels to access this file'
                keyboard = [
                    [InlineKeyboardButton("Join Channel 1", url=f'https://t.me/{CHANNEL1}')],
                    [InlineKeyboardButton("Join Channel 2", url=f'https://t.me/{CHANNEL2}')],
                    [InlineKeyboardButton("Check Again", callback_data=f'check{key}')]
                ]
            elif missingcount == 1:
                missingchannel = result['missing_channels'][0].replace('@', '')
                channelname = 'Channel 1' if CHANNEL1 in missingchannel else 'Channel 2'
                messagetext = f'You need to join {channelname} to access this file'
                keyboard = [
                    [InlineKeyboardButton(f"Join {channelname}", url=f'https://t.me/{missingchannel}')],
                    [InlineKeyboardButton("Check Again", callback_data=f'check{key}')]
                ]
            else:
                messagetext = 'Join the channels and then click Check Again'
                keyboard = [
                    [InlineKeyboardButton("Join Channels", url=f'https://t.me/{CHANNEL1}')],
                    [InlineKeyboardButton("Check Again", callback_data=f'check{key}')]
                ]
            
            sentmsg = await update.message.reply_text(
                messagetext, 
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
            return
        
        # User has joined both channels - send the file
        filename = fileinfo['filename']
        ext = filename.lower().split('.')[-1] if '.' in filename else ''
        warningmsg = (
            f'This message will auto-delete in {DELETE_AFTER//60} minutes\n'
            f'Forward to saved messages to keep it\n'
            f'File is stored permanently in database'
        )
        
        try:
            if fileinfo['isvideo'] and ext in PLAYABLE_EXTS:
                sent = await context.bot.send_video(
                    chat_id=chat_id,
                    video=fileinfo['fileid'],
                    caption=f'{filename}\nAccessed {fileinfo.get("accesscount", 0)} times\n\n{warningmsg}',
                    parse_mode='Markdown',
                    supports_streaming=True
                )
            else:
                sent = await context.bot.send_document(
                    chat_id=chat_id,
                    document=fileinfo['fileid'],
                    caption=f'{filename}\nAccessed {fileinfo.get("accesscount", 0)} times\n\n{warningmsg}',
                    parse_mode='Markdown'
                )
            log.info(f'File send successful for key {key}')
            await schedule_message_deletion(context, sent.chat_id, sent.message_id)
            
        except Exception as e:
            errormsg = str(e).lower()
            if 'file is too big' in errormsg or 'too large' in errormsg:
                sentmsg = await update.message.reply_text('File is too large. Maximum size is 50MB for videos.')
            elif 'file not found' in errormsg or 'invalid file id' in errormsg:
                sentmsg = await update.message.reply_text('File expired from Telegram servers. Please contact admin.')
            elif 'forbidden' in errormsg:
                sentmsg = await update.message.reply_text('Bot can\'t send messages here.')
            else:
                sentmsg = await update.message.reply_text('Failed to send file. Please try again.')
            
            await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
            
    except Exception as e:
        log.error(f'Start error: {e}', exc_info=True)
        if update.message:
            sentmsg = await update.message.reply_text('Error processing request')
            await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)

async def checkjoin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle check membership callback"""
    try:
        query = update.callback_query
        if not query:
            return
        
        await query.answer()
        userid = query.from_user.id
        data = query.data
        
        if data == 'checkmembership':
            result = await check_membership(userid, context, forcecheck=True)
            if result['all_joined']:
                await query.edit_message_text(
                    f'Great! You\'ve joined both channels! Now you can use file links shared by the admin.\n\n'
                    f'Note: All bot messages auto-delete after {DELETE_AFTER//60} minutes\n'
                    f'Storage: Files are stored permanently in database',
                    parse_mode='Markdown'
                )
            else:
                messagetext = 'Membership Check Failed'
                missingcount = len(result['missing_channels'])
                if missingcount == 2:
                    messagetext = 'You\'re not a member of either channel.'
                elif missingcount == 1:
                    missingchannel = result['missing_channels'][0].replace('@', '')
                    channelname = 'Channel 1' if CHANNEL1 in missingchannel else 'Channel 2'
                    messagetext = f'You\'re missing {channelname}.'
                
                keyboard = [
                    [InlineKeyboardButton("Join Channel 1", url=f'https://t.me/{CHANNEL1}')],
                    [InlineKeyboardButton("Join Channel 2", url=f'https://t.me/{CHANNEL2}')],
                    [InlineKeyboardButton("Check Again", callback_data='checkmembership')]
                ]
                await query.edit_message_text(
                    messagetext + '\n\nJoin the channels and check again.',
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            return
        
        # File key check
        key = data.replace('check', '')
        fileinfo = db.get_file(key)
        if not fileinfo:
            await query.edit_message_text('File not found. It may have been manually deleted by admin.')
            return
        
        result = await check_membership(userid, context, forcecheck=True)
        if result['all_joined']:
            filename = fileinfo['filename']
            ext = filename.lower().split('.')[-1] if '.' in filename else ''
            warningmsg = (
                f'This message will auto-delete in {DELETE_AFTER//60} minutes\n'
                f'Forward to saved messages to keep it\n'
                f'File is stored permanently in database'
            )
            chat_id = query.message.chat_id
            
            try:
                if fileinfo['isvideo'] and ext in PLAYABLE_EXTS:
                    sentmsg = await context.bot.send_video(
                        chat_id=chat_id,
                        video=fileinfo['fileid'],
                        caption=f'{filename}\nAccessed {fileinfo.get("accesscount", 0)} times\n\n{warningmsg}',
                        parse_mode='Markdown',
                        supports_streaming=True
                    )
                else:
                    sentmsg = await context.bot.send_document(
                        chat_id=chat_id,
                        document=fileinfo['fileid'],
                        caption=f'{filename}\nAccessed {fileinfo.get("accesscount", 0)} times\n\n{warningmsg}',
                        parse_mode='Markdown'
                    )
                await query.edit_message_text('Access granted! File sent below.', parse_mode='Markdown')
                await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
            except Exception as e:
                log.error(f'Failed to send file in callback: {e}', exc_info=True)
                errormsg = str(e).lower()
                if 'file is too big' in errormsg or 'too large' in errormsg:
                    await query.edit_message_text('File is too large (max 50MB).')
                elif 'file not found' in errormsg or 'invalid file id' in errormsg:
                    await query.edit_message_text('File expired from Telegram servers.')
                elif 'forbidden' in errormsg:
                    await query.edit_message_text('Bot can\'t send files here.')
                else:
                    await query.edit_message_text('Failed to send file. Please try again.')
        else:
            missingcount = len(result['missing_channels'])
            text = 'Still Not Joined'
            if missingcount == 2:
                text = 'You need to join both channels'
            
            keyboard = [
                [InlineKeyboardButton("Join Channels", url=f'https://t.me/{CHANNEL1}')],
                [InlineKeyboardButton("Check Again", callback_data=f'check{key}')]
            ]
            await query.edit_message_text(
                text,
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
    except Exception as e:
        log.error(f'Callback error: {e}', exc_info=True)
        try:
            await update.callback_query.answer('An error occurred. Please try again.', show_alert=True)
        except:
            pass

async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin file upload handler"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        msg = update.message
        video = msg.video
        document = msg.document
        
        fileid = None
        filename = None
        mimetype = None
        filesize = 0
        isvideo = False
        
        if video:
            fileid = video.file_id
            filename = video.file_name or f'video_{int(time.time())}.mp4'
            mimetype = video.mime_type or 'video/mp4'
            filesize = video.file_size or 0
            isvideo = True
        elif document:
            filename = document.file_name or f'document_{int(time.time())}'
            fileid = document.file_id
            mimetype = document.mime_type
            filesize = document.file_size or 0
            ext = filename.lower().split('.')[-1] if '.' in filename else ''
            if ext in ALL_VIDEO_EXTS:
                isvideo = True
        else:
            sentmsg = await msg.reply_text('Please send a video or document')
            await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
            return
        
        fileinfo = {
            'filename': filename,
            'mimetype': mimetype,
            'isvideo': isvideo,
            'size': int(filesize) if filesize else 0
        }
        
        key = db.save_file(fileid, fileinfo)
        link = f'https://t.me/{botusername}?start={key}'
        
        sentmsg = await msg.reply_text(
            f'Upload Successful!\n'
            f'Name: {filename}\n'
            f'Type: Video{'✓' if isvideo else ''} / Document\n'
            f'Size: {filesize/1024/1024:.1f} MB\n'
            f'Key: `{key}`\n'
            f'Message auto-delete: {DELETE_AFTER//60} minutes\n'
            f'Storage: PERMANENT in database\n'
            f'Link: {link}\n\n'
            f'Note: File will be stored FOREVER unless manually deleted',
            parse_mode='Markdown'
        )
        await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)
        
    except Exception as e:
        log.exception('Upload error')
        sentmsg = await update.message.reply_text(f'Upload failed: {str(e)[:200]}')
        await schedule_message_deletion(context, sentmsg.chat_id, sentmsg.message_id)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Error handler"""
    log.error(f'Error: {context.error}', exc_info=True)

def start_bot():
    """Start the bot"""
    if not BOT_TOKEN:
        print('ERROR: BOT_TOKEN is not set!')
        return
    
    if not ADMIN_ID or ADMIN_ID == 0:
        print('ERROR: ADMIN_ID is not set or invalid!')
        return
    
    # Start Flask web dashboard
    print('Starting Flask web dashboard...')
    flask_thread = threading.Thread(target=run_flask_thread, daemon=True)
    flask_thread.start()
    time.sleep(1)
    print(f'Flask running on port {os.environ.get("PORT", 10000)}')
    
    # Check job queue
    if application.job_queue:
        print('✅ Job queue initialized - auto-delete feature enabled')
    else:
        print('❌ Job queue not available - auto-delete feature will not work')
    
    try:
        filecount = db.get_file_count()
        print(f'Database initialized. Files in database: {filecount}')
        print('Files will be kept FOREVER in database')
    except Exception as e:
        print(f'Database initialization failed: {e}')
    
    # Add handlers
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('cleanup', cleanup))
    application.add_handler(CommandHandler('stats', stats))
    application.add_handler(CommandHandler('clearcache', clear_cache))
    application.add_handler(CommandHandler('testchannel', test_channel))
    application.add_handler(CommandHandler('listfiles', list_files))
    application.add_handler(CommandHandler('deletefile', delete_file))
    application.add_handler(CallbackQueryHandler(checkjoin, pattern=r'^check(membership|[0-9]+)$'))
    
    upload_filter = filters.VIDEO | filters.Document.ALL
    application.add_handler(MessageHandler(upload_filter & filters.User(user_id=ADMIN_ID) & filters.ChatType.PRIVATE, upload))
    
    print('Bot is running and listening...')
    print(f'Bot username: {botusername}')
    print(f'Admin ID: {ADMIN_ID}')
    print(f'Channels: {CHANNEL1}, {CHANNEL2}')
    print(f'ALL bot messages auto-delete after {DELETE_AFTER//60} minutes')
    print('Database auto-cleanup: DISABLED (files stored permanently)')
    print(f'Max stored files: {MAX_STORED_FILES}')
    print('IMPORTANT: Files are stored PERMANENTLY in database!')
    print('Use /listfiles to see all files')
    print('Use /deletefile <key> to delete specific files')
    print('Use /cleanup <days> for manual cleanup (optional)')
    print('Channels must be PUBLIC for membership check!')
    print('Use /testchannel to test channel access')
    print('Use /clearcache to clear membership cache')
    
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

def main():
    print('='*50)
    print('TELEGRAM FILE BOT - PERMANENT STORAGE')
    print('='*50)
    
    if not BOT_TOKEN:
        print('ERROR: BOT_TOKEN is not set!')
        print('Set it as environment variable or in .env file')
        return
    
    if not ADMIN_ID or ADMIN_ID == 0:
        print('ERROR: ADMIN_ID is not set or invalid!')
        print('Get your Telegram ID from @userinfobot')
        return
    
    print(f'Admin ID: {ADMIN_ID}')
    print(f'Channels: {CHANNEL1}, {CHANNEL2}')
    print(f'ALL bot messages auto-delete after {DELETE_AFTER//60} minutes')
    print('Database storage: PERMANENT (no auto-cleanup)')
    print(f'Max files: {MAX_STORED_FILES}')
    print('FILES WILL BE STORED FOREVER IN DATABASE!')
    print('Use /deletefile or /cleanup to manually remove files')
    
    start_bot()

if __name__ == '__main__':
    main()
