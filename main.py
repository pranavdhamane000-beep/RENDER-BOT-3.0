import asyncio
import json
import logging
import os
import sys
import time
import traceback
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import signal
import threading

# ================= HEALTH SERVER FOR RENDER =================
from flask import Flask, render_template_string, jsonify
app = Flask(__name__)

# Global variables for web dashboard
start_time = time.time()
bot_username = "TelegramBot"  # Will be updated when bot starts

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
                <p>Bot is running on Render</p>
                <p>Uptime: {{ uptime }}</p>
            </div>
            
            <div class="info">
                <h3>📊 Bot Information</h3>
                <ul>
                    <li>Service: <strong>Render Web Service</strong></li>
                    <li>Bot: <strong>@{{ bot_username }}</strong></li>
                </ul>
            </div>
            
            <div class="warning">
                <h3>⚠️ Video Formats</h3>
                <ul>
                    <li><strong>🎥 Playable:</strong> MP4, MOV, M4V</li>
                    <li><strong>📁 Download only:</strong> MKV, AVI, WEBM</li>
                </ul>
            </div>
            
            <div class="info">
                <h3>📞 Start Bot</h3>
                <p><a href="https://t.me/{{ bot_username }}" target="_blank" class="btn">Start @{{ bot_username }}</a></p>
            </div>
            
            <footer style="margin-top: 20px; border-top: 1px solid rgba(255,255,255,0.2); padding-top: 10px; font-size: 0.8rem;">
                <small>Render • {{ current_time }}</small>
            </footer>
        </div>
    </body>
    </html>
    """
    
    # Calculate uptime
    uptime_seconds = time.time() - start_time
    uptime_str = str(timedelta(seconds=int(uptime_seconds)))
    
    return render_template_string(html_content, 
                                  bot_username=bot_username,
                                  uptime=uptime_str,
                                  current_time=datetime.now().strftime("%H:%M:%S"))

@app.route('/health')
def health():
    return jsonify({
        "status": "OK", 
        "timestamp": datetime.now().isoformat(),
        "service": "telegram-file-bot",
        "uptime": str(timedelta(seconds=int(time.time() - start_time)))
    }), 200

@app.route('/ping')
def ping():
    return "pong", 200

def run_flask_thread():
    """Run Flask server in a thread (not a process) for Render"""
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
    ContextTypes,
    filters,
)

# ================= CONFIG =================
BOT_TOKEN = os.environ.get("8426456863:AAH986HdotZusBc385uNoUTobBVxtPYoS5A", "")
ADMIN_ID = int(os.environ.get("6234222988", "0"))

CHANNEL_1 = os.environ.get("CHANNEL_1", "A_Knight_of_the_Seven_Kingdoms_t")
CHANNEL_2 = os.environ.get("CHANNEL_2", "your_movies_web")

# Use /tmp for persistence on Render (survives container restarts)
DATA_FILE = Path("/tmp/data.json")
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

# Simple membership cache
class MembershipCache:
    def __init__(self, ttl_seconds=300):
        self.cache = {}
        self.ttl = ttl_seconds
    
    def get(self, user_id: int, channel: str) -> Optional[bool]:
        key = f"{user_id}_{channel}"
        if key in self.cache:
            timestamp, value = self.cache[key]
            if time.time() - timestamp < self.ttl:
                return value
            else:
                del self.cache[key]
        return None
    
    def set(self, user_id: int, channel: str, value: bool):
        key = f"{user_id}_{channel}"
        self.cache[key] = (time.time(), value)
    
    def clean(self):
        """Fixed: Properly delete expired items"""
        current = time.time()
        expired_keys = []
        
        # First collect keys to delete
        for k, (t, _) in self.cache.items():
            if current - t > self.ttl:
                expired_keys.append(k)
        
        # Then delete them
        for k in expired_keys:
            del self.cache[k]

membership_cache = MembershipCache()

# ============ STORAGE MANAGEMENT ============
async def load_data() -> Dict[str, Any]:
    """Load data from JSON file"""
    try:
        if not DATA_FILE.exists():
            return {}
        
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                return {}
            data = json.loads(content)
        
        # Auto cleanup old files
        cleaned = await auto_cleanup_old_files(data)
        if len(cleaned) < len(data):
            await save_data(cleaned)
            return cleaned
        
        return data
        
    except json.JSONDecodeError:
        log.error("JSON decode error, resetting data file")
        DATA_FILE.unlink(missing_ok=True)
        return {}
    except Exception as e:
        log.error(f"Failed to load data: {e}")
        return {}

async def save_data(data: Dict[str, Any]) -> bool:
    """Save data to JSON file"""
    try:
        # Enforce file limit
        if len(data) > MAX_STORED_FILES:
            data = await remove_oldest_files(data, MAX_STORED_FILES)
        
        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, separators=(',', ':'))  # Compact format
        
        return True
    except Exception as e:
        log.error(f"Failed to save data: {e}")
        return False

async def auto_cleanup_old_files(data: Dict[str, Any]) -> Dict[str, Any]:
    """Remove files older than AUTO_CLEANUP_DAYS"""
    if AUTO_CLEANUP_DAYS <= 0 or not data:
        return data
    
    cutoff = datetime.now() - timedelta(days=AUTO_CLEANUP_DAYS)
    cleaned = {}
    
    for key, info in data.items():
        ts = info.get('timestamp')
        if ts:
            try:
                file_date = datetime.fromisoformat(ts)
                if file_date > cutoff:
                    cleaned[key] = info
            except:
                cleaned[key] = info
        else:
            cleaned[key] = info
    
    removed = len(data) - len(cleaned)
    if removed > 0:
        log.info(f"Auto-cleanup removed {removed} old files")
    
    return cleaned

async def remove_oldest_files(data: Dict[str, Any], max_files: int) -> Dict[str, Any]:
    """Remove oldest files"""
    if len(data) <= max_files:
        return data
    
    # Sort by timestamp
    items = list(data.items())
    items.sort(key=lambda x: x[1].get('timestamp', ''))
    
    removed = len(data) - max_files
    log.info(f"Removing {removed} oldest files")
    
    return dict(items[-max_files:])

def next_id(data: Dict[str, Any]) -> str:
    """Generate next ID"""
    if not data:
        return "1"
    
    max_num = 0
    for k in data:
        try:
            num = int(k)
            if num > max_num:
                max_num = num
        except ValueError:
            continue
    
    return str(max_num + 1)

# ============ MEMBERSHIP CHECK ============
async def is_member(bot, channel: str, user_id: int) -> Optional[bool]:
    """Check if user is member of channel with cache"""
    cached = membership_cache.get(user_id, channel)
    if cached is not None:
        return cached
    
    chat_id = f"@{channel}" if not channel.startswith("@") else channel
    
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        is_member = member.status in ("member", "administrator", "creator")
        membership_cache.set(user_id, channel, is_member)
        return is_member
    except Exception as e:
        err = str(e).lower()
        if "user not found" in err:
            membership_cache.set(user_id, channel, False)
            return False
        elif "chat not found" in err or "forbidden" in err:
            return None
        log.warning(f"Membership check failed for {channel}: {e}")
        return None

async def check_membership(bot, user_id: int) -> Dict[str, Any]:
    """Check membership for both channels"""
    membership_cache.clean()
    
    result = {
        'channel1': False,
        'channel2': False,
        'all_joined': False
    }
    
    try:
        ch1 = await is_member(bot, CHANNEL_1, user_id)
        await asyncio.sleep(0.5)  # Small delay
        ch2 = await is_member(bot, CHANNEL_2, user_id)
        
        result['channel1'] = ch1 if ch1 is not None else False
        result['channel2'] = ch2 if ch2 is not None else False
        result['all_joined'] = result['channel1'] and result['channel2']
        
    except Exception:
        pass
    
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
async def delete_job(context: ContextTypes.DEFAULT_TYPE):
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
        data = await load_data()
        initial = len(data)
        
        # Clean old entries
        cutoff = datetime.now() - timedelta(days=days)
        cleaned = {}
        for k, v in data.items():
            ts = v.get('timestamp')
            if ts:
                try:
                    if datetime.fromisoformat(ts) > cutoff:
                        cleaned[k] = v
                except:
                    cleaned[k] = v
            else:
                cleaned[k] = v
        
        await save_data(cleaned)
        
        msg = f"🧹 Cleanup complete\n"
        msg += f"Files: {initial} → {len(cleaned)}\n"
        msg += f"Removed: {initial - len(cleaned)} files"
        
        await update.message.reply_text(msg)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Cleanup failed: {str(e)[:100]}")

# ============ START COMMAND ============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start"""
    try:
        if not update.message:
            return
        
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        args = context.args
        
        if not args:
            await update.message.reply_text(
                "🤖 File Sharing Bot\n\n"
                "🔗 Use admin-provided links\n"
                "📢 Join channels to access files"
            )
            return
        
        key = args[0]
        data = await load_data()
        
        if key not in data:
            await update.message.reply_text("❌ File not found")
            return
        
        # Check membership
        result = await check_membership(context.bot, user_id)
        
        if not result['all_joined']:
            missing = []
            if not result['channel1']:
                missing.append(f"@{CHANNEL_1}")
            if not result['channel2']:
                missing.append(f"@{CHANNEL_2}")
            
            keyboard = []
            if missing:
                text = "📢 Join to access:\n" + "\n".join(missing)
                if not result['channel1']:
                    keyboard.append([InlineKeyboardButton("Join Channel 1", url=f"https://t.me/A_Knight_of_the_Seven_Kingdoms_t")])
                if not result['channel2']:
                    keyboard.append([InlineKeyboardButton("Join Channel 2", url=f"https://t.me/your_movies_web")])
                keyboard.append([InlineKeyboardButton("✅ Check Again", callback_data=f"check|{key}")])
                
                await update.message.reply_text(
                    text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            return
        
        # Send file
        record = data[key]
        filename = record.get("file_name", "file")
        
        send_as_video, supports_streaming = should_send_as_video(record)
        
        try:
            if send_as_video:
                sent_msg = await context.bot.send_video(
                    chat_id=chat_id,
                    video=record["file_id"],
                    caption=f"📹 {filename}",
                    supports_streaming=supports_streaming,
                    read_timeout=TIMEOUT,
                    write_timeout=TIMEOUT
                )
            else:
                sent_msg = await context.bot.send_document(
                    chat_id=chat_id,
                    document=record["file_id"],
                    caption=f"📁 {filename}",
                    read_timeout=TIMEOUT,
                    write_timeout=TIMEOUT
                )
        except Exception as e:
            await update.message.reply_text("❌ Failed to send file")
            log.error(f"Send failed: {e}")
            return
        
        # Schedule deletion
        context.job_queue.run_once(
            delete_job,
            DELETE_AFTER,
            data={"chat": chat_id, "msg": sent_msg.message_id}
        )
        
        # Send auto-delete warning
        warn = await update.message.reply_text(
            f"✅ File sent\n⚠️ Auto-deletes in {DELETE_AFTER//60} min"
        )
        
        context.job_queue.run_once(
            delete_job,
            DELETE_AFTER + 30,
            data={"chat": chat_id, "msg": warn.message_id}
        )
        
    except Exception as e:
        log.error(f"Start error: {e}")
        if update.message:
            await update.message.reply_text("❌ Error")

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
        data = await load_data()
        
        if key not in data:
            await query.edit_message_text("❌ File expired")
            return
        
        result = await check_membership(context.bot, user_id)
        
        if not result['all_joined']:
            await query.answer("Join all channels first!", show_alert=True)
            return
        
        # Send file
        record = data[key]
        filename = record.get("file_name", "file")
        send_as_video, supports_streaming = should_send_as_video(record)
        
        try:
            if send_as_video:
                sent_msg = await context.bot.send_video(
                    chat_id=query.message.chat_id,
                    video=record["file_id"],
                    caption=f"📹 {filename}",
                    supports_streaming=supports_streaming
                )
            else:
                sent_msg = await context.bot.send_document(
                    chat_id=query.message.chat_id,
                    document=record["file_id"],
                    caption=f"📁 {filename}"
                )
        except Exception as e:
            await query.edit_message_text("❌ Failed to send")
            return
        
        await query.edit_message_reply_markup(None)
        
        # Schedule deletion
        context.job_queue.run_once(
            delete_job,
            DELETE_AFTER,
            data={"chat": query.message.chat_id, "msg": sent_msg.message_id}
        )
        
    except Exception as e:
        log.error(f"Callback error: {e}")
        if update.callback_query:
            await update.callback_query.answer("Error", show_alert=True)

# ============ UPLOAD ============
async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle file uploads (admin only)"""
    if update.effective_user.id != ADMIN_ID:
        return
    
    try:
        video = update.message.video
        document = update.message.document
        
        file_id = None
        filename = None
        mime_type = None
        file_size = 0
        is_video = False
        
        if video:
            # Video message (always playable)
            file_id = video.file_id
            filename = getattr(video, 'file_name', f'video_{int(time.time())}.mp4')
            mime_type = getattr(video, 'mime_type', 'video/mp4')
            file_size = getattr(video, 'file_size', 0)
            is_video = True
            
        elif document and is_video_file(document):
            file_id = document.file_id
            filename = getattr(document, 'file_name', f'file_{int(time.time())}')
            mime_type = getattr(document, 'mime_type', '')
            file_size = getattr(document, 'file_size', 0)
            
            # Check if playable (no MKV conversion on Render free tier)
            filename_lower = filename.lower()
            
            if filename_lower.endswith('.mkv') or 'matroska' in (mime_type or '').lower():
                # MKV files are documents (not playable)
                is_video = False
                await update.message.reply_text(
                    "⚠️ MKV file (sent as document)\n"
                    "Note: No MKV conversion on Render free tier"
                )
            else:
                # Check if playable format
                send_as_video, _ = should_send_as_video({
                    'file_name': filename,
                    'mime_type': mime_type
                })
                is_video = send_as_video
                
        else:
            await update.message.reply_text(
                "❌ Send a video file\n\n"
                "🎥 Playable: MP4, MOV, M4V\n"
                "📁 Download: MKV, AVI, WEBM\n"
                "📦 Max: 2GB (Telegram limit)"
            )
            return
        
        # Save to database
        data = await load_data()
        key = next_id(data)
        
        data[key] = {
            "file_id": file_id,
            "is_video": is_video,
            "mime_type": mime_type,
            "file_name": filename,
            "timestamp": datetime.now().isoformat(),
            "size": file_size
        }
        
        await save_data(data)
        
        # Generate link
        try:
            bot_user = await context.bot.get_me()
            global bot_username
            bot_username = bot_user.username
            link = f"https://t.me/{bot_username}?start={key}"
            
            file_type = "🎥 Video" if is_video else "📁 Document"
            await update.message.reply_text(
                f"✅ Uploaded\n"
                f"ID: <code>{key}</code>\n"
                f"Name: {filename}\n"
                f"Type: {file_type}\n"
                f"Size: {file_size/1024/1024:.1f} MB\n\n"
                f"🔗 Link:\n<code>{link}</code>",
                parse_mode='HTML'
            )
            
        except Exception as e:
            await update.message.reply_text(f"✅ Saved as {key}\nError: {str(e)[:100]}")
            
    except Exception as e:
        log.error(f"Upload error: {e}")
        await update.message.reply_text("❌ Upload failed")

# ============ MAIN FUNCTION ============
def main():
    """Main function for Render"""
    if not BOT_TOKEN:
        print("❌ ERROR: BOT_TOKEN is not set!")
        print("\n📋 SETUP FOR RENDER:")
        print("1. Create Web Service on Render")
        print("2. Add Environment Variables:")
        print("   - BOT_TOKEN: from @BotFather")
        print("   - ADMIN_ID: your Telegram ID")
        print("   - CHANNEL_1: first channel username")
        print("   - CHANNEL_2: second channel username")
        print("3. Build Command: pip install -r requirements.txt")
        print("4. Start Command: python main.py")
        sys.exit(1)
    
    # Start Flask in a thread (not process) for Render compatibility
    flask_thread = threading.Thread(target=run_flask_thread, daemon=True)
    flask_thread.start()
    
    print("\n" + "="*50)
    print("🤖 TELEGRAM FILE BOT")
    print("="*50)
    print(f"👤 Admin: {ADMIN_ID}")
    print(f"📺 Channels: @{CHANNEL_1}, @{CHANNEL_2}")
    print(f"💾 Storage: /tmp/data.json")
    print(f"🧹 Auto-cleanup: {AUTO_CLEANUP_DAYS} days")
    print("="*50 + "\n")
    
    try:
        # Create Telegram application
        app = (
            Application.builder()
            .token(BOT_TOKEN)
            .read_timeout(TIMEOUT)
            .write_timeout(TIMEOUT)
            .build()
        )
        
        # Add handlers (only essential ones)
        app.add_error_handler(error_handler)
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("cleanup", cleanup))  # Only other command
        app.add_handler(CallbackQueryHandler(check_join, pattern=r"^check\|"))
        
        # Upload handler (admin only)
        upload_filter = filters.VIDEO | filters.Document.ALL
        app.add_handler(MessageHandler(upload_filter & filters.User(ADMIN_ID), upload))
        
        # Start polling
        print("🚀 Starting bot...")
        app.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )
        
    except KeyboardInterrupt:
        print("\n👋 Bot stopped")
    except Exception as e:
        print(f"\n💥 Bot crashed: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

# ==============================
# requirements.txt
# ==============================
"""
python-telegram-bot==20.7
flask==3.0.0
"""

# ==============================
# SETUP INSTRUCTIONS
# ==============================
"""
1. Files needed:
   - main.py (this file)
   - requirements.txt (above)

2. On Render:
   - Create new Web Service
   - Connect GitHub repo
   - Set Environment Variables:
        BOT_TOKEN=your_token_here
        ADMIN_ID=your_user_id
        CHANNEL_1=channel1_username
        CHANNEL_2=channel2_username
   - Build Command: pip install -r requirements.txt
   - Start Command: python main.py

3. Features:
   • /start <key> - Access files
   • /cleanup [days] - Clean old files (admin only)
   • Video upload - Admin uploads files
   • Channel membership check
   • Auto-delete sent files after 10 min
   • Health endpoints for Render

4. Notes:
   • Uses /tmp/data.json for persistent storage
   • No FFmpeg conversion on Render free tier
   • MKV files sent as documents
   • MP4/MOV/M4V sent as playable videos
   • Simple and reliable for Render
"""
