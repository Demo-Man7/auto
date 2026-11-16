import telebot
import re
import unicodedata
import time
import datetime
import json
import os
import pymongo
import random
import html
import threading
from collections import deque
from telebot.types import ChatPermissions, InlineKeyboardMarkup, InlineKeyboardButton
from telebot.apihelper import ApiTelegramException
from dotenv import load_dotenv

# မြန်မာစံတော်ချိန် (UTC+6:30) ရဖို့ Helper Function
def get_mm_time():
    utc_now = datetime.datetime.now(datetime.timezone.utc)
    mm_time = utc_now + datetime.timedelta(hours=6, minutes=30)
    return mm_time.strftime('%Y-%m-%d %H:%M:%S')

load_dotenv()

# ======================
# CONFIG
# ======================
TOKEN = os.getenv("TOKEN")
BOT_USERNAME = os.getenv("BOT_USERNAME")
ADMIN_IDS = [int(i.strip()) for i in os.getenv("ADMIN_IDS").split(",")]
DATA_FILE = os.getenv("DATA_FILE")
FORCE_JOIN_CHANNEL = os.getenv("FORCE_JOIN_CHANNEL")
MONGO_URI = os.getenv("MONGO_URI")

# Logger & Button URLs
LOGGER_ID = os.getenv("LOGGER_ID")
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/myanmarbot_music")
SUPPORT_URL = os.getenv("SUPPORT_URL", "https://t.me/myanmar_music_Bot2027")
BOT_999_URL = os.getenv("BOT_999_URL", "tg://resolve?domain=cores_999")
ALL_BOT_URL = os.getenv("ALL_BOT_URL", "https://t.me/myanmarbot_music/53")

try:
    LOGGER_ID = int(LOGGER_ID) if LOGGER_ID else None
except (ValueError, TypeError):
    LOGGER_ID = None

bot = telebot.TeleBot(TOKEN, parse_mode="HTML")

# ======================
# 🔥 QUEUE SYSTEM (Drop မရှိ + Flood မဖြစ်)
# ======================
reply_queue = deque()
_queue_lock = threading.Lock()

MIN_GAP = [0.3]              # စာ ကြား အနည်းဆုံး (dynamic)
CHAT_COOLDOWN = 0.5          # chat တစ်ခုကို 0.5s ခြား
QUEUE_MAX = 10000            # queue အများဆုံး
_worker_started = [False]

chat_last_reply = {}
_global_last = [0.0]
_bot_warn_cooldown = {} 


def can_queue(chat_id):
    """Queue ထဲ ထည့်ခင် rate limit စစ်"""
    with _queue_lock:
        now = time.time()
        if now - chat_last_reply.get(chat_id, 0) < CHAT_COOLDOWN:
            return False
        if now - _global_last[0] < MIN_GAP[0]:
            return False
        chat_last_reply[chat_id] = now
        _global_last[0] = now
        # Memory cleanup
        if len(chat_last_reply) > 5000:
            cutoff = now - 300
            for k in [k for k, v in chat_last_reply.items() if v < cutoff]:
                chat_last_reply.pop(k, None)
        return True


def queue_worker():
    """Queue ထဲက message တွေကို တစ်ခုချင်း ပို့ — drop မရှိ"""
    while True:
        if not reply_queue:
            time.sleep(0.3)
            continue

        with _queue_lock:
            if not reply_queue:
                continue
            try:
                message, text = reply_queue.popleft()
            except IndexError:
                continue

        try:
            bot.reply_to(message, text, parse_mode=None)
            # အောင်မြင်ရင် gap ကို နည်းနည်း လျှော့ (မြန်လာအောင်)
            if MIN_GAP[0] > 0.3:
                MIN_GAP[0] = max(0.3, MIN_GAP[0] - 0.02)
        except ApiTelegramException as e:
            if e.error_code == 429:
                # Flood ဖြစ်ရင် gap တင် (နှေးလာအောင်)
                MIN_GAP[0] = min(2.0, MIN_GAP[0] + 0.2)
                try:
                    retry_after = int(e.result_json.get("parameters", {}).get("retry_after", 5))
                except Exception:
                    retry_after = 5
                time.sleep(retry_after + 1)
                # တစ်ခါပဲ ပြန်ကြိုး
                try:
                    bot.reply_to(message, text, parse_mode=None)
                except Exception:
                    pass
            elif e.error_code in (400, 403):
                pass  # silent skip
        except Exception:
            pass

        time.sleep(MIN_GAP[0])


def ensure_worker():
    """Worker ကို တစ်ခါပဲ စတင်"""
    if not _worker_started[0]:
        _worker_started[0] = True
        t = threading.Thread(target=queue_worker, daemon=True)
        t.start()


# ======================
# DATABASE
# ======================
client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client['bot_database']
brain_collection = db['brain']

def init_db():
    try:
        client.admin.command('ping')
        print("✅ MongoDB Connected")
        try:
            brain_collection.create_index("input_text")
        except Exception:
            pass
    except Exception:
        print("❌ MongoDB Connection Error")

# ======================
# BRAIN CACHE
# ======================
_brain_cache = {}
_brain_cache_lock = threading.Lock()
CACHE_TTL = 300

def _invalidate_cache(key):
    with _brain_cache_lock:
        _brain_cache.pop(key, None)

def save_to_brain(input_text, reply_text):
    try:
        key = input_text.lower().strip()
        query = {"input_text": key, "reply_text": reply_text}
        if not brain_collection.find_one(query, {"_id": 1}):
            brain_collection.insert_one(query)
            _invalidate_cache(key)
    except Exception:
        pass

def get_reply(text):
    if not text:
        return None
    user_input = text.lower().strip()
    now = time.time()
    with _brain_cache_lock:
        cached = _brain_cache.get(user_input)
    if cached and now - cached[1] < CACHE_TTL:
        replies = cached[0]
    else:
        try:
            cursor = brain_collection.find(
                {"input_text": user_input}, {"reply_text": 1, "_id": 0}
            ).limit(50)
            replies = [r.get("reply_text") for r in cursor if r.get("reply_text")]
        except Exception:
            return None
        with _brain_cache_lock:
            if len(_brain_cache) > 20000:
                _brain_cache.clear()
            _brain_cache[user_input] = (replies, now)
    return random.choice(replies) if replies else None

# ======================
# HELPERS
# ======================
def is_joined(user_id):
    if user_id in ADMIN_IDS:
        return True
    try:
        member = bot.get_chat_member(FORCE_JOIN_CHANNEL, user_id)
        if member.status not in ['left', 'kicked']:
            return True
    except Exception:
        pass
    return False

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {}
    data.setdefault("mute_time", 30)
    data.setdefault("strikes", {})
    data.setdefault("extra_words", [])
    data.setdefault("groups", [])
    data.setdefault("users", [])
    data.setdefault("reply_on", True)
    data.setdefault("reply_on_chats", {})
    data.setdefault("warning_text", None)
    data.setdefault("last_ts", {})
    return data

def save_data():
    data["groups"] = list(set(data.get("groups", [])))
    data["users"] = list(set(data.get("users", [])))
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception:
        pass

data = load_data()
BOT_START_TIME = int(time.time())

def is_clean_text(text):
    if not text: return False
    if len(text) < 1 or len(text) > 150: return False
    if re.search(r'http[s]?://', text) or '@' in text: return False
    return True

def mention(user):
    return f"<a href='tg://user?id={user.id}'>{html.escape(user.first_name)}</a>"

def strike_key(chat_id, user_id):
    return f"{chat_id}:{user_id}"

def is_admin(chat_id, user_id):
    try:
        if user_id in ADMIN_IDS: return True
        admins = bot.get_chat_administrators(chat_id)
        return any(a.user.id == user_id for a in admins)
    except: return False

def parse_time(text):
    unit = text[-1].lower()
    try:
        if unit == 's': return int(text[:-1])
        if unit == 'm': return int(text[:-1]) * 60
        if unit == 'h': return int(text[:-1]) * 3600
        if unit == 'd': return int(text[:-1]) * 86400
        return int(text)
    except: return None

def main_buttons():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(
        "𝐀𝐝𝐝 𝐓𝐨 𝐘𝐨𝐮𝐫 𝐆𝐫𝐨𝐮𝐩",
        url=f"https://t.me/{BOT_USERNAME}?startgroup=s&delete_message+manage_video_chats_message+invite_users"))
    kb.row(InlineKeyboardButton("𝐂𝐇𝐀𝐍𝐍𝐄𝐋", url=CHANNEL_URL),
           InlineKeyboardButton(" 𝐒𝐔𝐏𝐏𝐎𝐑𝐓 ", url=SUPPORT_URL))
    kb.row(InlineKeyboardButton("𝐎𝐖𝐍𝐄𝐑", url=BOT_999_URL),
           InlineKeyboardButton("𝐀𝐥𝐥 𝐁𝐎𝐓", url=ALL_BOT_URL))
    return kb

# ======================
# LOGGER HELPERS
# ======================
def send_to_logger(text, **kwargs):
    if not LOGGER_ID:
        return
    try:
        bot.send_message(LOGGER_ID, text, parse_mode="HTML", **kwargs)
    except ApiTelegramException as e:
        if e.error_code in (429, 400, 403):
            return
        print(f"⚠️ Logger send error: {e.error_code}")
    except Exception:
        pass

def log_start_event(message):
    if not LOGGER_ID:
        return
    try:
        user = message.from_user
        user_name = html.escape(user.first_name or "Unknown")
        user_username = f"@{user.username}" if user.username else "No Username"
        user_id = user.id

        if message.chat.type == "private":
            chat_type = "🔒 𝐏𝐫𝐢𝐯𝐚𝐭𝐞"
            chat_id = message.chat.id
            chat_title = "Private Chat"
        else:
            chat_type = "👥 𝐆𝐫𝐨𝐮𝐩" if message.chat.type == "group" else "👥 𝐒𝐮𝐩𝐞𝐫𝐠𝐫𝐨𝐮𝐩"
            chat_id = message.chat.id
            chat_title = html.escape(message.chat.title or "Unknown Group")

        log_text = (
            f"🔔 <b>#𝐍𝐄𝐖_𝐒𝐓𝐀𝐑𝐓</b>\n"
            f"<blockquote>"
            f"👤 <b>𝐔𝐬𝐞𝐫:</b> {user_name}\n"
            f"🔗 <b>𝐔𝐬𝐞𝐫𝐧𝐚𝐦𝐞:</b> {user_username}\n"
            f"🆔 <b>𝐔𝐬𝐞𝐫 𝐈𝐃:</b> <code>{user_id}</code>"
            f"</blockquote>\n"
            f"<blockquote>"
            f"📌 <b>𝐓𝐲𝐩𝐞:</b> {chat_type}\n"
            f"📝 <b>𝐂𝐡𝐚𝐭:</b> {chat_title}\n"
            f"🆔 <b>𝐂𝐡𝐚𝐭 𝐈𝐃:</b> <code>{chat_id}</code>"
            f"</blockquote>\n"
            f"<blockquote>"
            f"🕐 <b>𝐓𝐢𝐦𝐞:</b> <code>{get_mm_time()}</code>"
            f"</blockquote>"
        )
        send_to_logger(log_text)
    except Exception:
        pass

def log_startup():
    if not LOGGER_ID:
        return
    try:
        bot_info = bot.get_me()
        log_text = (
            f"🚀 <b>#𝐁𝐎𝐓_𝐒𝐓𝐀𝐑𝐓𝐄𝐃</b>\n"
            f"<blockquote>"
            f"🤖 <b>𝐁𝐨𝐭:</b> @{bot_info.username}\n"
            f"🆔 <b>𝐁𝐨𝐭 𝐈𝐃:</b> <code>{bot_info.id}</code>"
            f"</blockquote>\n"
            f"<blockquote>"
            f"✅ <b>𝐒𝐭𝐚𝐭𝐮𝐬:</b> Online & Ready\n"
            f"💾 <b>𝐃𝐚𝐭𝐚𝐛𝐚𝐬𝐞:</b> MongoDB Connected\n"
            f"👥 <b>𝐆𝐫𝐨𝐮𝐩𝐬:</b> {len(data.get('groups', []))}\n"
            f"👤 <b>𝐔𝐬𝐞𝐫𝐬:</b> {len(data.get('users', []))}\n"
            f"🔥 <b>𝐐𝐮𝐞𝐮𝐞:</b> Active (no drop)"
            f"</blockquote>\n"
            f"<blockquote>"
            f"🕐 <b>𝐓𝐢𝐦𝐞:</b> <code>{get_mm_time()}</code>"
            f"</blockquote>"
        )
        send_to_logger(log_text)
    except Exception:
        pass

def log_bot_added(message, adder):
    if not LOGGER_ID:
        return
    try:
        adder_name = html.escape(adder.first_name) if adder and adder.first_name else "Unknown"
        adder_username = f"@{adder.username}" if adder and adder.username else "No Username"
        adder_id = adder.id if adder else 0
        group_title = html.escape(message.chat.title or "Unknown Group")
        group_id = message.chat.id
        group_type = "👥 𝐆𝐫𝐨𝐮𝐩" if message.chat.type == "group" else "👥 𝐒𝐮𝐩𝐞𝐫𝐠𝐫𝐨𝐮𝐩"

        log_text = (
            f"➕ <b>#𝐁𝐎𝐓_𝐀𝐃𝐃𝐄𝐃_𝐓𝐎_𝐆𝐑𝐎𝐔𝐏</b>\n"
            f"<blockquote>"
            f"📌 <b>𝐓𝐲𝐩𝐞:</b> {group_type}\n"
            f"📝 <b>𝐆𝐫𝐨𝐮𝐩:</b> {group_title}\n"
            f"🆔 <b>𝐆𝐫𝐨𝐮𝐩 𝐈𝐃:</b> <code>{group_id}</code>"
            f"</blockquote>\n"
            f"<blockquote>"
            f"👤 <b>𝐀𝐝𝐝𝐞𝐝 𝐁𝐲:</b> {adder_name}\n"
            f"🔗 <b>𝐔𝐬𝐞𝐫𝐧𝐚𝐦𝐞:</b> {adder_username}\n"
            f"🆔 <b>𝐔𝐬𝐞𝐫 𝐈𝐃:</b> <code>{adder_id}</code>"
            f"</blockquote>\n"
            f"<blockquote>"
            f"🕐 <b>𝐓𝐢𝐦𝐞:</b> <code>{get_mm_time()}</code>"
            f"</blockquote>"
        )
        send_to_logger(log_text)
    except Exception:
        pass

def log_chat_created(message):
    if not LOGGER_ID:
        return
    try:
        creator = message.from_user
        creator_name = html.escape(creator.first_name) if creator and creator.first_name else "Unknown"
        creator_username = f"@{creator.username}" if creator and creator.username else "No Username"
        creator_id = creator.id if creator else 0

        log_text = (
            f"🆕 <b>#𝐍𝐄𝐖_𝐂𝐇𝐀𝐓_𝐂𝐑𝐄𝐀𝐓𝐄𝐃</b>\n"
            f"<blockquote>"
            f"📝 <b>𝐓𝐢𝐭𝐥𝐞:</b> {html.escape(message.chat.title or 'Unknown')}\n"
            f"🆔 <b>𝐂𝐡𝐚𝐭 𝐈𝐃:</b> <code>{message.chat.id}</code>\n"
            f"📌 <b>𝐓𝐲𝐩𝐞:</b> {message.chat.type}"
            f"</blockquote>\n"
            f"<blockquote>"
            f"👤 <b>𝐂𝐫𝐞𝐚𝐭𝐞𝐝 𝐁𝐲:</b> {creator_name}\n"
            f"🔗 <b>𝐔𝐬𝐞𝐫𝐧𝐚𝐦𝐞:</b> {creator_username}\n"
            f"🆔 <b>𝐔𝐬𝐞𝐫 𝐈𝐃:</b> <code>{creator_id}</code>"
            f"</blockquote>\n"
            f"<blockquote>"
            f"🕐 <b>𝐓𝐢𝐦𝐞:</b> <code>{get_mm_time()}</code>"
            f"</blockquote>"
        )
        send_to_logger(log_text)
    except Exception:
        pass

# ======================
# SAFE SEND HELPERS
# ======================
def safe_send(chat_id, text, **kwargs):
    try:
        return bot.send_message(chat_id, text, **kwargs)
    except ApiTelegramException as e:
        if e.error_code in (429, 400, 403):
            return None
        print(f"⚠️ Send error {e.error_code}")
    except Exception:
        pass
    return None

def safe_delete(chat_id, msg_id):
    try:
        bot.delete_message(chat_id, msg_id)
    except Exception:
        pass

# ======================
# CALLBACKS
# ======================
@bot.callback_query_handler(func=lambda call: call.data.startswith("unmute:"))
def handle_unmute(call):
    chat_id = call.message.chat.id
    if not is_admin(chat_id, call.from_user.id):
        bot.answer_callback_query(call.id, "❌ Admin များသာ Unmute လုပ်နိုင်ပါသည်။", show_alert=True)
        return
    target_id = int(call.data.split(":")[1])
    try:
        bot.restrict_chat_member(chat_id, target_id,
            permissions=ChatPermissions(can_send_messages=True, can_send_media_messages=True,
                                        can_send_other_messages=True, can_add_web_page_previews=True))
        bot.edit_message_text(f"✅ {mention(call.from_user)} မှ User ကို Unmute လုပ်ပေးလိုက်ပါပြီ။",
                              chat_id, call.message.message_id)
    except Exception:
        pass

@bot.callback_query_handler(func=lambda call: call.data == "toggle_reply")
def toggle_reply_callback(call):
    chat_id = call.message.chat.id
    if not is_admin(chat_id, call.from_user.id):
        return bot.answer_callback_query(call.id, "Baby ကိုယ်နဲ့မဆိုင်ရင် မနှိပ်ရဘူးလေ။", show_alert=True)
    key = str(chat_id)
    current = data.get("reply_on_chats", {}).get(key, data.get("reply_on", True))
    new_status = not current
    data.setdefault("reply_on_chats", {})[key] = new_status
    save_data()
    btn_text = "🟢 𝐁𝐨𝐭 စာပြန်ခြင်း: ON" if new_status else "🔴 𝐁𝐨𝐭 စာပြန်ခြင်း: OFF"
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton(btn_text, callback_data="toggle_reply"))
    try:
        bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=kb)
    except Exception:
        pass
    bot.answer_callback_query(call.id, f"𝐁𝐨𝐭 စာပြန်ခြင်းကို {'ဖွင့်' if new_status else 'ပိတ်'} လိုက်ပါပြီ။")

# ======================
# ADMIN COMMANDS
# ======================
@bot.message_handler(commands=["setmute"])
def set_mute_time(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    if message.chat.type == "private":
        if user_id not in ADMIN_IDS: return
    else:
        try:
            member = bot.get_chat_member(chat_id, user_id)
            if member.status not in ["administrator", "creator"] and user_id not in ADMIN_IDS:
                return bot.reply_to(message, "Baby ဒီလုပ်ဆောင်ချက်က Group Admin သီးသန့်တွက်ပဲ။")
        except Exception:
            return
    args = message.text.split()
    if len(args) < 2:
        return bot.reply_to(message, "Usage: /setmute 30s (or 5m, 1d)")
    seconds = parse_time(args[1])
    if seconds is not None:
        if seconds < 30:
            return bot.reply_to(message, "⚠️ Mute time ကို အနည်းဆုံး 30s ထားပေးပါ။")
        data.setdefault("group_mute_times", {})[str(chat_id)] = seconds
        save_data()
        bot.reply_to(message, f"✅ Mute time သတ်မှတ်ပြီ: {args[1]} ({seconds}s)")
    else:
        bot.reply_to(message, "❌ Invalid format! Use: 30s, 5m, 1h")

@bot.message_handler(commands=["addword"])
def add_word(message):
    if message.chat.type != "private" or message.from_user.id not in ADMIN_IDS: return
    raw = message.text.replace("/addword", "").strip()
    if not raw: return bot.reply_to(message, "Usage: /addword <word>")
    norm = _normalize_text_for_match(raw)
    if not norm or len(norm) < 1:
        return bot.reply_to(message, "⚠️ စကားလုံး အနည်းဆုံး 2 လုံး ရှိရပါမည်။")
    if norm not in data.get("extra_words", []):
        data.setdefault("extra_words", []).append(norm)
        save_data()
        bot.reply_to(message, f"✅ Added: {raw}")
    else:
        bot.reply_to(message, f"ℹ️ Already: {raw}")

@bot.message_handler(commands=["setwarn"])
def set_warning_text(message):
    if not is_admin(message.chat.id, message.from_user.id): return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return bot.reply_to(message, "Usage: /setwarn <message>")
    data["warning_text"] = parts[1].strip()
    save_data()
    bot.reply_to(message, "✅ Warning text updated.")

@bot.message_handler(commands=["setword"])
def set_chat_word(message):
    if message.chat.type not in ["group", "supergroup"]: return
    if not is_admin(message.chat.id, message.from_user.id):
        return bot.reply_to(message, "❌ Admins only")
    raw = message.text.replace("/setword", "").strip()
    if not raw: return bot.reply_to(message, "Usage: /setword <word>")
    norm = _normalize_text_for_match(raw)
    if not norm or len(norm) < 2:
        return bot.reply_to(message, "⚠️ စကားလုံး အနည်းဆုံး 2 လုံး ရှိရပါမည်။")
    lst = data.setdefault("chat_words", {}).setdefault(str(message.chat.id), [])
    if norm not in lst:
        lst.append(norm)
        save_data()
        bot.reply_to(message, f"✅ Added '{raw}'")
    else:
        bot.reply_to(message, f"ℹ️ Already")

@bot.message_handler(commands=["getwarn"])
def get_warning_text(message):
    if not is_admin(message.chat.id, message.from_user.id): return
    warn = data.get("warning_text") or "(default)"
    bot.reply_to(message, f"Current warning:\n{warn}")

@bot.message_handler(commands=["delword"])
def del_word(message):
    chat_id = message.chat.id
    if message.chat.type == "private":
        if message.from_user.id not in ADMIN_IDS: return
        target = data.setdefault("extra_words", [])
    else:
        if not is_admin(chat_id, message.from_user.id): return
        target = data.setdefault("chat_words", {}).setdefault(str(chat_id), [])
    raw = message.text.replace("/delword", "").strip()
    if not raw: return bot.reply_to(message, "Usage: /delword <word>")
    norm = _normalize_text_for_match(raw)
    if norm in target:
        target.remove(norm)
        save_data()
        bot.reply_to(message, f"✅ Removed: {raw}")
    else:
        bot.reply_to(message, "❌ Not found.")

@bot.message_handler(commands=["list"])
def list_brain(message):
    if message.from_user.id not in ADMIN_IDS: return
    args = message.text.split(maxsplit=1)
    mode = args[1].strip().lower() if len(args) > 1 else "words"
    if mode == "brain":
        try:
            all_entries = list(brain_collection.find({}, {"_id": 0}).limit(200))
        except Exception:
            all_entries = []
        if not all_entries:
            return safe_send(message.chat.id, "❌ မှတ်ဉာဏ် မရှိသေးပါ။")
        msg = f"<b>📚 Entries ({len(all_entries)})</b>\n\n"
        for idx, doc in enumerate(all_entries, 1):
            inp = html.escape((doc.get("input_text") or "")[:40])
            rp = html.escape((doc.get("reply_text") or "")[:35])
            msg += f"<b>{idx}.</b> 💬 <code>{inp}</code>\n   ➜ {rp}\n\n"
            if len(msg) > 3500:
                safe_send(message.chat.id, msg, parse_mode="HTML")
                msg = ""
        if msg:
            safe_send(message.chat.id, msg, parse_mode="HTML")
        return

    gw = data.get("extra_words", [])
    cw = data.get("chat_words", {}).get(str(message.chat.id), [])
    msg = f"<b>📋 Ban Words</b>\n\n<b>Global ({len(gw)}):</b>\n"
    msg += "".join(f"• <code>{html.escape(w)}</code>\n" for w in gw) or "(none)\n"
    msg += f"\n<b>Chat ({len(cw)}):</b>\n"
    msg += "".join(f"• <code>{html.escape(w)}</code>\n" for w in cw) or "(none)\n"
    msg += "\n/d <id> or /d <word> ဖျက်ရန်"
    safe_send(message.chat.id, msg, parse_mode="HTML")

@bot.message_handler(commands=["d"])
def delete_brain_entry(message):
    target = ""
    
    # နည်းလမ်း ၁ - ဖျက်ချင်တဲ့ စာကို Reply ထောက်ပြီး /d ရိုက်တာ
    if message.reply_to_message and message.reply_to_message.text:
        target = message.reply_to_message.text.strip()
    # နည်းလမ်း ၂ - /d စာသား ရိုက်တာ
    elif len(message.text.split(maxsplit=1)) > 1:
        target = message.text.split(maxsplit=1)[1].strip()
    else:
        return bot.reply_to(message, "Usage:\n/d စာသား (ဒါမှမဟုတ်) ဖျက်ချင်တဲ့စာကို Reply ထောက်ပြီး /d ရိုက်ပါ။")

    if not target:
        return bot.reply_to(message, "❌ ဖျက်ရန် စာသား မတွေ့ပါ။")

    target_lower = target.lower()
    try:
        # အဲ့ဒီမေးခွန်းအတွက် သိမ်းထားတဲ့ အဖြေအားလုံးကို ဖျက်မယ်
        result = brain_collection.delete_many({"input_text": target_lower})
        
        if result.deleted_count > 0:
            _invalidate_cache(target_lower) # Cache ကို ရှင်းမယ်
            bot.reply_to(message, f"✅ '{target}' အတွက် အဖြေ {result.deleted_count} ခု ဖျက်ပြီးပါပြီ။")
        else:
            bot.reply_to(message, f"❌ '{target}' ဆိုတဲ့ မှတ်ဉာဏ် မတွေ့ပါ။")
    except Exception as e:
        bot.reply_to(message, f"⚠️ Error: {e}")

@bot.message_handler(commands=["rp"])
def reply_toggle(message):
    chat_id = message.chat.id
    if not is_admin(chat_id, message.from_user.id):
        return bot.reply_to(message, "❌ Admin များသာ။")
    current = data.get("reply_on_chats", {}).get(str(chat_id), True)
    new_status = not current
    data.setdefault("reply_on_chats", {})[str(chat_id)] = new_status
    save_data()
    btn_text = "🟢 𝐁𝐨𝐭 စာပြန်ခြင်း: ON" if new_status else "🔴 𝐁𝐨𝐭 စာပြန်ခြင်း: OFF"
    kb = InlineKeyboardMarkup().add(InlineKeyboardButton(btn_text, callback_data="toggle_reply"))
    safe_send(chat_id,
        f"<blockquote>🤖 <b>𝐀𝐮𝐭𝐨 𝐑𝐞𝐩𝐥𝐲 𝐒𝐲𝐬𝐭𝐞𝐦</b>\n\n"
        f"{'🟢 ဖွင့်ပြီ' if new_status else '🔴 ပိတ်ပြီ'}\n"
        f"🕐 <b>𝐓𝐢𝐦𝐞:</b> <code>{get_mm_time()}</code></blockquote>",
        reply_markup=kb, parse_mode="HTML")

@bot.message_handler(commands=["id"])
def show_chat_id(message):
    bot.reply_to(message, f"Chat ID: {message.chat.id}")

@bot.message_handler(commands=["broadcast"])
def broadcast(message):
    if message.from_user.id not in ADMIN_IDS:
        return bot.reply_to(message, "Baby ကိုယ်နဲ့မဆိုင်တာကို ဘာတွေပို့နေတာ")
    if not message.reply_to_message:
        return bot.reply_to(message, "⚠️ Message ကို Reply ထောက်ပြီး သုံးပါ။")
    target_msg = message.reply_to_message
    targets = list(set(data.get("groups", []) + data.get("users", [])))
    if not targets:
        return bot.reply_to(message, "⚠️ Target မရှိ။")
    status_msg = bot.reply_to(message, f"🚀 {len(targets)} နေရာကို ပို့နေပါပြီ...")
    success = fail = 0
    for tid in targets:
        try:
            bot.forward_message(chat_id=tid, from_chat_id=message.chat.id,
                                message_id=target_msg.message_id)
            success += 1
            time.sleep(0.08)
        except ApiTelegramException as e:
            if e.error_code == 429:
                time.sleep(int(e.result_json.get("parameters", {}).get("retry_after", 5)))
            fail += 1
        except Exception:
            fail += 1
    try:
        bot.edit_message_text(
            f"✅ <b>Broadcast Done</b>\n\n🟢 Success: {success}\n🔴 Fail: {fail}",
            chat_id=message.chat.id, message_id=status_msg.message_id, parse_mode="HTML")
    except Exception:
        pass

@bot.message_handler(commands=["delgp"])
def del_group(message):
    if message.from_user.id not in ADMIN_IDS:
        return bot.reply_to(message, "❌ Owner only.")
    parts = message.text.split()
    if len(parts) == 1:
        gid = message.chat.id
    elif len(parts) == 2:
        try: gid = int(parts[1])
        except ValueError: return bot.reply_to(message, "⚠️ Chat ID က ကိန်း ဖြစ်ရမည်။")
    else:
        return bot.reply_to(message, "Usage: /delgp or /delgp <chat_id>")
    if gid not in data.get("groups", []):
        return bot.reply_to(message, "ℹ️ ဒီ Group မရှိပါ။")
    data["groups"].remove(gid)
    data.get("reply_on_chats", {}).pop(str(gid), None)
    save_data()
    bot.reply_to(message, f"✅ Group {gid} ဖယ်ပြီ။")

# ======================
# START & HELP
# ======================
@bot.message_handler(commands=["start"])
def start(message):
    user_id = message.from_user.id
    if message.chat.type == "private":
        if message.chat.id not in data["users"]:
            data["users"].append(message.chat.id)
            save_data()

        if not is_joined(user_id):
            join_kb = InlineKeyboardMarkup()
            clean_channel = FORCE_JOIN_CHANNEL.replace('@', '')
            join_kb.add(InlineKeyboardButton("𝐂𝐡𝐚𝐧𝐧𝐞𝐥 𝐣𝐨𝐢𝐧", url=f"https://t.me/{clean_channel}"))
            join_kb.add(InlineKeyboardButton("𝐣𝐨𝐢𝐧 (စစ်ဆေးမည်)", url=f"https://t.me/{BOT_USERNAME}?start=start"))
            return safe_send(message.chat.id,
                "<blockquote>❌ <b>အသုံးပြုခွင့်မရှိသေးပါ!</b>\n\n"
                "ဒီ 𝐁𝐨𝐭 သုံးဖို့ Channel Join ပေးပါ။</blockquote>",
                reply_markup=join_kb)

    log_start_event(message)

    safe_send(message.chat.id,
        "🤖 <b>𝐀𝐮𝐭𝐨 𝐑𝐞𝐩𝐥𝐲 & 𝐒𝐩𝐚𝐦 𝐇𝐞𝐥𝐩 𝐁𝐨𝐭</b>\n"
        "<blockquote>"
        "⚠️ <b>𝐅𝐞𝐚𝐭𝐮𝐫𝐞𝐬:</b>\n"
        "🚫 𝐁𝐢𝐨 / 𝐉𝐨𝐢𝐧 / 𝐋𝐢𝐧𝐤 𝐬𝐩𝐚𝐦 𝐚𝐮𝐭𝐨 𝐝𝐞𝐥𝐞𝐭𝐞\n"
        "⚠️ 𝟑 𝐖𝐚𝐫𝐧𝐢𝐧𝐠𝐬 = 𝐀𝐮𝐭𝐨 𝐌𝐮𝐭𝐞\n"
        "💬 𝐀𝐮𝐭𝐨 𝐑𝐞𝐩𝐥𝐲 𝐒𝐲𝐬𝐭𝐞𝐦"
        "</blockquote>\n"
        "<blockquote>"
        "💕 <b>𝐇𝐨𝐰 𝐓𝐨 𝐔𝐬𝐞:</b>\n"
        "▸ /rp ဖြင့် Group မှာ Auto စာပြန်ဖွင့်ပါ\n"
        "▸ 𝐁𝐨𝐭 ကို Group ထဲထည့်ပြီး 𝐀𝐝𝐦𝐢𝐧 ပေးပါ"
        "</blockquote>",
        reply_markup=main_buttons())

@bot.message_handler(commands=["help"])
def help_command(message):
    help_text = ("Owner Commands:\n/setmute\n/addword\n/delword\n/broadcast\n/list\n"
                 "/setwarn\n/getwarn\nGroup admin: /setword /delword")
    if message.from_user.id in ADMIN_IDS:
        try:
            bot.send_message(message.from_user.id, help_text, parse_mode=None)
            bot.reply_to(message, "✅ Sent to DM")
        except Exception:
            try: bot.reply_to(message, help_text, parse_mode=None)
            except Exception: pass
    else:
        try: bot.reply_to(message, "❌ Owner only.")
        except Exception: pass

# ======================
# PATTERNS
# ======================
BASE_PATTERNS = [
    r"b[\W_]*i[\W_]*o", r"j[\W_]*o[\W_]*i[\W_]*n", r"t[\W_]*\.?[\W_]*m[\W_]*e",
    r"http", r"www", r"link", r"ဂျိုင်း",
    r"ဂျိုင်း[\s\S]*လင့်|ဂျိုင်း[\s\S]*လင့်|ဂျိုင်း[\s\S]*လင်ခ်",
    r"လင့်ခ်|လင့်|လင့်|လင်ခ်", r"ဘိုင်[\W_]*အို", r"ဘိုင်ယို"
]
PATTERN_NAMES = {0:"bio_en",1:"join_en",2:"telegram",3:"http",4:"www",5:"link_en",
                 6:"join_mm",7:"join_mm_link",8:"link_mm",9:"bio_mm",10:"bio_mm_alt"}

def _normalize_text_for_match(s: str) -> str:
    if not s: return s
    nk = unicodedata.normalize('NFKC', s).lower()
    return ''.join(c for c in unicodedata.normalize('NFKD', nk) if not unicodedata.category(c).startswith('M'))

def matches_word_in_text(word: str, text: str) -> bool:
    if not word or not text: return False
    return bool(re.search(r'\b' + re.escape(word) + r'\b', text, flags=re.UNICODE))

if isinstance(data.get("extra_words", None), list):
    normalized = []
    for w in data.get("extra_words", []):
        if not w: continue
        n = _normalize_text_for_match(w)
        if n and n not in normalized: normalized.append(n)
    data["extra_words"] = normalized
    save_data()

def strip_accents(s: str) -> str:
    nkfd = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in nkfd if not unicodedata.category(c).startswith('M'))

def detect_reason(text):
    if not text: return "spam"
    try:
        t = strip_accents(unicodedata.normalize("NFKC", text).lower())
        for idx, pattern in enumerate(BASE_PATTERNS):
            if re.search(pattern, t):
                return PATTERN_NAMES.get(idx, "spam")
        for word in data.get("extra_words", []):
            if matches_word_in_text(word.lower(), t): return word
    except Exception: pass
    return "spam"

# ======================
# WELCOME (Bot Added + Logger)
# ======================
@bot.message_handler(content_types=["new_chat_members"])
def welcome_group(message):
    if not message.new_chat_members: return
    for user in message.new_chat_members:
        try:
            if user.id == bot.get_me().id:
                log_bot_added(message, message.from_user)
                safe_send(message.chat.id,
                    "🤖 <b>𝐀𝐮𝐭𝐨 𝐑𝐞𝐩𝐥𝐲 & 𝐒𝐩𝐚𝐦 𝐇𝐞𝐥𝐩 𝐁𝐨𝐭 𝐀𝐜𝐭𝐢𝐯𝐞!</b>\n"
                    "<blockquote>"
                    "❤️ <b>𝐀𝐝𝐦𝐢𝐧 ပေးထားဖို့ မမေ့ပါနဲ့</b>\n"
                    "💕 /rp ဖြင့် Auto စာပြန်ဖွင့်ပေးပါ"
                    "</blockquote>",
                    reply_markup=main_buttons())
        except Exception:
            pass

@bot.message_handler(content_types=["group_chat_created", "supergroup_chat_created", "channel_chat_created"])
def handle_chat_creation(message):
    log_chat_created(message)
    safe_send(message.chat.id,
        "🤖 <b>𝐇𝐞𝐥𝐩 𝐁𝐨𝐭 𝐀𝐜𝐭𝐢𝐯𝐞!</b>\n"
        "<blockquote>𝐀𝐝𝐦𝐢𝐧 ပေးထားဖို့ လိုအပ်ပါတယ်။</blockquote>",
        reply_markup=main_buttons())

# ======================
# MAIN MESSAGE HANDLER
# ======================
@bot.message_handler(content_types=["text", "photo", "sticker", "story", "video", "animation"])
def handle_all(message):
    # Worker ကို စတင်
    ensure_worker()

    if hasattr(message, 'date'):
        msg_ts = message.date
        if isinstance(msg_ts, datetime.datetime):
            msg_ts = int(msg_ts.timestamp())
        try: msg_ts = int(msg_ts)
        except Exception: msg_ts = None
        if msg_ts is not None:
            if msg_ts < BOT_START_TIME: return
            chat_key = str(message.chat.id)
            if "last_ts" not in data: data["last_ts"] = {}
            last = data.get("last_ts", {}).get(chat_key, 0)
            if msg_ts <= last: return
            data["last_ts"][chat_key] = msg_ts

    chat_id = message.chat.id
    user = message.from_user
    user_id = user.id if user else 0

    if message.chat.type in ["group", "supergroup"]:
        if chat_id not in data.get("groups", []):
            data.setdefault("groups", []).append(chat_id)
            data.setdefault("reply_on_chats", {})[str(chat_id)] = False
            save_data()

    if message.content_type == 'text' and message.text and message.text.startswith('/'):
        return

    if message.chat.type == "private" and user_id not in ADMIN_IDS:
        if not is_joined(user_id):
            join_kb = InlineKeyboardMarkup()
            clean_channel = FORCE_JOIN_CHANNEL.replace('@', '')
            join_kb.add(InlineKeyboardButton("𝐂𝐡𝐚𝐧𝐧𝐞𝐥 𝐣𝐨𝐢𝐧", url=f"https://t.me/{clean_channel}"))
            join_kb.add(InlineKeyboardButton(" 𝐣𝐨𝐢𝐧 (စစ်ဆေးမည်)", url=f"https://t.me/{BOT_USERNAME}?start=start"))
            return safe_send(message.chat.id,
                "<blockquote>❌ <b>အသုံးပြုခွင့်မရှိသေးပါ!</b>\n\nChannel Join ပေးပါ။</blockquote>",
                reply_markup=join_kb)

    # ── Bot spam protection ──
    is_bot_sender = False
    if user:
        if getattr(user, 'is_bot', False):
            is_bot_sender = True
        elif user.username and user.username.lower().endswith('bot'):
            is_bot_sender = True

    if is_bot_sender:
        bot_check = message.text or message.caption or ""
        has_link = any(x in bot_check.lower() for x in ['http', 't.me/', 'www', 'link'])
        is_long = len(bot_check) > 50

        if has_link or is_long:
            safe_delete(chat_id, message.message_id)
            warn_key = str(chat_id)
            now = time.time()
            if now - _bot_warn_cooldown.get(warn_key, 0) > 60:
                _bot_warn_cooldown[warn_key] = now
                if len(_bot_warn_cooldown) > 2000:
                    cutoff = now - 300
                    for k in [k for k, v in _bot_warn_cooldown.items() if v < cutoff]:
                        _bot_warn_cooldown.pop(k, None)

                safe_send(chat_id,
                    f"⚠️ <b>𝐁𝐨𝐭 𝐒𝐩𝐚𝐦 𝐃𝐞𝐭𝐞𝐜𝐭𝐞𝐝</b>\n"
                    f"<blockquote>"
                    f"အက်မင်မဟုတ်တဲ့ ဘော့ကနေ လင့်/စာ ပို့ခြင်းကို ခွင့်မပြုပါ။\n\n"
                    f"👤 𝐁𝐨𝐭: {mention(user)}"
                    f"</blockquote>",
                    parse_mode="HTML")
        return

    is_story = (message.content_type == 'story')
    is_forwarded = (message.forward_from is not None
                    or message.forward_from_chat is not None
                    or message.forward_sender_name is not None)

    if (is_story or is_forwarded) and not is_admin(chat_id, user_id):
        safe_delete(chat_id, message.message_id)
        warn = "⚠️ <b>𝐖𝐚𝐫𝐧𝐢𝐧𝐠</b>\n<blockquote>"
        warn += "❌𝐒𝐭𝐨𝐫𝐲 ပို့ခြင်းကို ခွင့်မပြုပါ။\n" if is_story else "❌𝐅𝐨𝐫𝐰𝐚𝐫𝐝 𝐦𝐞𝐬𝐬𝐚𝐠𝐞 ကို ခွင့်မပြုပါ။\n"
        if user: warn += f"\n👤 𝐔𝐬𝐞𝐫: {mention(user)}"
        warn += "</blockquote>"
        safe_send(chat_id, warn, parse_mode="HTML")
        return

    check_text = message.text if message.text else message.caption
    if check_text:
        norm_text = strip_accents(unicodedata.normalize('NFKC', check_text).lower())
    else:
        norm_text = check_text

    found = False
    matched_chat_word = None
    if check_text:
        if any(re.search(p, norm_text) for p in BASE_PATTERNS):
            found = True
        if not found:
            for w in data.get("extra_words", []):
                if matches_word_in_text(w, norm_text):
                    found = True; break
        if not found:
            for w in data.get("chat_words", {}).get(str(chat_id), []):
                if matches_word_in_text(w, norm_text):
                    found = True; matched_chat_word = w; break

    if found or is_forwarded:
        if is_admin(chat_id, user_id):
            return

        if matched_chat_word:
            warn_msg = (f"⚠️ <b>𝐁𝐚𝐧𝐧𝐞𝐝 𝐖𝐨𝐫𝐝</b>\n"
                        f"<blockquote>Group မှပိတ်ထားသော စကားပါနေသည်။\n"
                        f"🚫 <code>{html.escape(matched_chat_word)}</code></blockquote>")
        else:
            warn_msg = (f"⚠️ <b>𝐖𝐚𝐫𝐧𝐢𝐧𝐠</b>\n"
                        f"<blockquote>{data.get('warning_text') or 'ဂျိုင်း/ဘိုင်အို/လင့် ပါသည်။'}</blockquote>")
        safe_send(chat_id, warn_msg, parse_mode="HTML")

        key = strike_key(chat_id, user_id)
        strikes = data["strikes"].get(key, 0) + 1
        data["strikes"][key] = strikes
        save_data()

        if strikes >= 3:
            mute_dur = data.get("group_mute_times", {}).get(str(chat_id), data.get("mute_time", 60))
            until = int(time.time()) + mute_dur
            try:
                bot.restrict_chat_member(chat_id, user_id, permissions=ChatPermissions(), until_date=until)
            except Exception: pass

            kb = InlineKeyboardMarkup().add(
                InlineKeyboardButton("🔓 Unmute", callback_data=f"unmute:{user_id}"))
            safe_send(chat_id,
                f"❌ <b>𝐀𝐮𝐭𝐨 𝐌𝐮𝐭𝐞</b> ❌\n"
                f"<blockquote>"
                f"👤 𝐔𝐬𝐞𝐫: {mention(user)}\n"
                f"ℹ️ အကြောင်း: ဘိုင်အို/ဂျိုင်း/လင့် ၃ ကြိမ်\n"
                f"⏱️ ကြာချိန်: {mute_dur} စက္ကန့်"
                f"</blockquote>\n"
                f"<blockquote>"
                f"⏳ သတ်မှတ်ချိန်ပြည့်ရင် Auto Unmute ဖြစ်မည်။"
                f"</blockquote>",
                reply_markup=kb, parse_mode="HTML")
            data["strikes"][key] = 0
            save_data()
        else:
            safe_send(chat_id,
                f"❌ <b>𝐖𝐚𝐫𝐧𝐢𝐧𝐠 ({strikes}/3)</b>\n"
                f"<blockquote>"
                f"👤 {mention(user)}\n"
                f"🚫 (ဘိုင်အို / ဂျိုင်း / လင့်) ခွင့်မပြု။"
                f"</blockquote>\n"
                f"<blockquote>"
                f"⚠️ ၃ ကြိမ်ပြုလုပ်ပါက 𝐀𝐮𝐭𝐨 𝐌𝐮𝐭𝐞 ဖြစ်မည်။"
                f"</blockquote>",
                parse_mode="HTML")
        safe_delete(chat_id, message.message_id)
        return

    # ── learn from reply ──
    if message.reply_to_message and message.reply_to_message.content_type == 'text':
        parent = message.reply_to_message.text
        if parent:
            parent_norm = parent.strip().lower()
            if parent_norm:
                if message.content_type == 'text':
                    current_reply = getattr(message, 'html_text', message.text) or message.text
                    if is_clean_text(current_reply):
                        save_to_brain(parent_norm, current_reply)
                        return

    # ── 🔥 Queue ထဲ ထည့် (Drop မရှိ) ──
    if not message.text or not message.text.startswith("/"):
        user_input = message.text.lower() if message.text else None
        chat_reply_on = data.get("reply_on_chats", {}).get(str(chat_id), True)

        if user_input and chat_reply_on and message.content_type == 'text':
            reply_text = get_reply(user_input)
            if reply_text and can_queue(chat_id):
                with _queue_lock:
                    if len(reply_queue) < QUEUE_MAX:
                        reply_queue.append((message, reply_text))

# ======================
# RUN
# ======================
if __name__ == '__main__':
    init_db()
    print("🤖 Bot starting...")

    # Worker ကို စတင်
    ensure_worker()
    print("🔥 Queue worker started")

    try:
        log_startup()
    except Exception:
        pass

    while True:
        try:
            print("🚀 Bot online.")
            bot.infinity_polling(timeout=90, long_polling_timeout=20, skip_pending=True)
        except Exception as e:
            print(f"⚠️ Connection: {type(e).__name__}")
            time.sleep(5)
