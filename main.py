#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""KroMusic - lightweight single-file Telegram music bot for Railway."""
import os, sys, time, logging, sqlite3, base64, asyncio, urllib.parse, re, glob, shutil, random
from pathlib import Path
from types import SimpleNamespace
from pyrogram import Client, filters, idle
from pyrogram import __version__ as PYROGRAM_VERSION
from pyrogram.types import *
from pyrogram.enums import *
from pyrogram.errors import *
try:
    from pytgcalls import PyTgCalls, StreamType
    from pytgcalls.types import AudioPiped, HighQualityAudio, Update
    from pytgcalls.exceptions import NoActiveGroupCall, TelegramServerError, UnMuteNeeded
except Exception:
    PyTgCalls = None
import yt_dlp

load_dotenv = None
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
CACHE_DIR = BASE_DIR / "cache"
DOWNLOADS_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)
DB_PATH = BASE_DIR / os.getenv("DB_PATH", "kromusic.db")
COOKIES_FILE = BASE_DIR / os.getenv("YTDLP_COOKIES_FILE", "cookies.txt")

def _required(name):
    value=os.getenv(name, "").strip()
    if not value: raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def _int_required(name):
    try: return int(_required(name))
    except ValueError as e: raise RuntimeError(f"Environment variable {name} must be an integer.") from e

def _int_list(value):
    result=[]
    for item in value.replace(",", " ").split():
        try: result.append(int(item))
        except ValueError: raise RuntimeError(f"SUDO_USERS contains invalid Telegram user ID: {item}")
    return result

API_ID=_int_required("API_ID")
API_HASH=_required("API_HASH")
BOT_TOKEN=_required("BOT_TOKEN")
OWNER_ID=_int_required("OWNER_ID")
SESSION=os.getenv("SESSION", "").strip()
BOT_NAME="KroMusic"
DEVELOPER_USERNAME=os.getenv("DEVELOPER_USERNAME", "krofullpower").lstrip("@")
DEVELOPER_CHANNEL=os.getenv("DEVELOPER_CHANNEL", "dlxfullpower").lstrip("@")
DURATION_LIMIT=int(os.getenv("DURATION_LIMIT", "90"))
PING_IMG=""; START_IMG=""; FAILED=""
SUPPORT_CHAT=os.getenv("SUPPORT_CHAT", f"https://t.me/{DEVELOPER_CHANNEL}").strip()
SUPPORT_CHANNEL=os.getenv("SUPPORT_CHANNEL", f"https://t.me/{DEVELOPER_CHANNEL}").strip()
SUDO_USERS=_int_list(os.getenv("SUDO_USERS", ""))
AUTO_JOIN_CHATS=[x.lstrip("@").strip() for x in os.getenv("AUTO_JOIN_CHATS", "").replace(",", " ").split() if x.strip()]
YTDLP_COOKIES_B64=os.getenv("YTDLP_COOKIES_B64", "").strip()
if YTDLP_COOKIES_B64:
    try:
        COOKIES_FILE.write_bytes(base64.b64decode(YTDLP_COOKIES_B64, validate=True)); COOKIES_FILE.chmod(0o600)
    except Exception as exc: raise RuntimeError("YTDLP_COOKIES_B64 is not valid base64.") from exc

config=SimpleNamespace(**{k:v for k,v in globals().items() if k.isupper()})
config.DB_PATH=DB_PATH; config.DOWNLOADS_DIR=DOWNLOADS_DIR; config.CACHE_DIR=CACHE_DIR; config.COOKIES_FILE=COOKIES_FILE
fm=sys.modules[__name__]
StartTime=time.time()
logging.basicConfig(format="[%(asctime)s - %(levelname)s] %(name)s: %(message)s", datefmt="%d-%b-%y %H:%M:%S", level=logging.INFO, stream=sys.stdout)
logging.getLogger("pyrogram").setLevel(logging.ERROR); logging.getLogger("pytgcalls").setLevel(logging.ERROR)
LOGGER=logging.getLogger("KroMusic")
app=Client("KroMusic", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
app2=None; pytgcalls=None
SUDOERS=filters.user(); CURRENT_FILES={}; fallendb={}
ASS_ID=ASS_NAME=ASS_USERNAME=ASS_MENTION=None
BOT_ID=BOT_NAME_VALUE=BOT_USERNAME=BOT_MENTION=None
BOT_NAME="KroMusic"

def _db():
    conn=sqlite3.connect(DB_PATH); conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"); return conn

def get_saved_session():
    if SESSION: return SESSION
    conn=_db()
    try:
        row=conn.execute("SELECT value FROM settings WHERE key='assistant_session'").fetchone(); return row[0] if row else ""
    finally: conn.close()

def save_session(session):
    conn=_db(); conn.execute("INSERT INTO settings(key,value) VALUES('assistant_session',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (session,)); conn.commit(); conn.close()

def delete_saved_session():
    conn=_db(); conn.execute("DELETE FROM settings WHERE key='assistant_session'"); conn.commit(); conn.close()

async def start_assistant(session_string=None):
    global app2,pytgcalls,ASS_ID,ASS_NAME,ASS_USERNAME,ASS_MENTION
    session_string=session_string or get_saved_session()
    if not session_string: return False
    if app2 is not None:
        try:
            if app2.is_connected: return True
        except Exception: pass
    app2=Client("KroAssistant", api_id=API_ID, api_hash=API_HASH, session_string=session_string)
    await app2.start(); me=await app2.get_me()
    ASS_ID=me.id; ASS_NAME=(me.first_name+" "+(me.last_name or "")).strip(); ASS_USERNAME=me.username; ASS_MENTION=me.mention
    if PyTgCalls:
        pytgcalls=PyTgCalls(app2); await pytgcalls.start()
    return True

async def stop_assistant():
    global app2,pytgcalls,ASS_ID,ASS_NAME,ASS_USERNAME,ASS_MENTION
    if pytgcalls:
        try: await pytgcalls.stop()
        except Exception: pass
    if app2:
        try: await app2.stop()
        except Exception: pass
    app2=None; pytgcalls=None; ASS_ID=ASS_NAME=ASS_USERNAME=ASS_MENTION=None

async def fallen_startup():
    global BOT_ID,BOT_USERNAME,BOT_MENTION
    await app.start(); me=await app.get_me(); BOT_ID=me.id; BOT_USERNAME=me.username; BOT_MENTION=me.mention
    for sudoer in SUDO_USERS: SUDOERS.add(sudoer)
    SUDOERS.add(OWNER_ID)
    session=get_saved_session()
    if session:
        try: await start_assistant(session); LOGGER.info("KroMusic assistant started: @%s", ASS_USERNAME or "-")
        except Exception as exc: LOGGER.error("Assistant session failed: %s", type(exc).__name__); await stop_assistant()


# ===== HELPER: active.py =====
active = []
stream = {}

async def is_active_chat(chat_id: int) -> bool:
    if chat_id not in active:
        return False
    else:
        return True

async def add_active_chat(chat_id: int):
    if chat_id not in active:
        active.append(chat_id)

async def remove_active_chat(chat_id: int):
    if chat_id in active:
        active.remove(chat_id)

async def get_active_chats() -> list:
    return active

async def is_streaming(chat_id: int) -> bool:
    run = stream.get(chat_id)
    if not run:
        return False
    return run

async def stream_on(chat_id: int):
    stream[chat_id] = True

async def stream_off(chat_id: int):
    stream[chat_id] = False

# ===== HELPER: admins.py =====
from typing import Callable
from pyrogram.enums import ChatMemberStatus
from pyrogram.types import CallbackQuery, Message

def admin_check(func: Callable) -> Callable:

    async def non_admin(_, message: Message):
        if message.from_user.id in SUDOERS:
            return await func(_, message)
        try:
            member = await app.get_chat_member(message.chat.id, message.from_user.id)
        except:
            return
        if member.status in [ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR]:
            return await func(_, message)
        else:
            return
    return non_admin

def admin_check_cb(func: Callable) -> Callable:

    async def cb_non_admin(_, query: CallbackQuery):
        if query.from_user.id in SUDOERS:
            return await func(_, query)
        try:
            member = await app.get_chat_member(query.message.chat.id, query.from_user.id)
        except:
            return
        if member.status in [ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR]:
            return await func(_, query)
        else:
            return
    return cb_non_admin

# ===== HELPER: dossier.py =====
PM_START_TEXT = '\nأهلًا {0}.\n\nأنا {1}، بوت تشغيل موسيقى خفيف للمجموعات.\nأرسل اسم المقطع أو رابطه للبدء.\n'
START_TEXT = '\nأهلًا {0}.\n\n{1} جاهز لتشغيل الموسيقى في {2}.\n\nقناة المطور: {3}\n'
HELP_TEXT = f'\nأوامر {BOT_NAME}:\n\nشغل / تشغيل: تشغيل مقطع صوتي.\nاوكف: إيقاف مؤقت.\nكمل: استكمال التشغيل.\nتخطي: تخطي المقطع الحالي.\nانهاء: إنهاء التشغيل والقائمة.\nاغنية: تحميل مقطع وإرساله.\nبحث: البحث في يوتيوب.\nping: فحص الاستجابة.\n'
HELP_SUDO = f'\nأوامر الإدارة في {BOT_NAME}:\n\nالمساعد: إدارة حساب المساعد.\nتغيير الاسم: تغيير اسم حساب المساعد.\nبايو مساعد: تغيير البايو.\nضيف صورة: تغيير الصورة.\nمسح صورة: حذف الصورة.\n'
HELP_DEV = f'\nلوحة مالك {BOT_NAME}:\n\nالمساعد: إضافة أو حذف أو فحص حساب المساعد.\nمسح الكاش: حذف الملفات المؤقتة.\n'

# ===== HELPER: errors.py =====
class DurationLimitError(Exception):
    pass

class FFmpegReturnCodeError(Exception):
    pass

# ===== HELPER: formatters.py =====
def get_readable_time(seconds: int) -> str:
    count = 0
    ping_time = ''
    time_list = []
    time_suffix_list = ['ث', 'د', 'س', 'يوم']
    while count < 4:
        count += 1
        if count < 3:
            remainder, result = divmod(seconds, 60)
        else:
            remainder, result = divmod(seconds, 24)
        if seconds == 0 and remainder == 0:
            break
        time_list.append(int(result))
        seconds = int(remainder)
    for i in range(len(time_list)):
        time_list[i] = str(time_list[i]) + time_suffix_list[i]
    if len(time_list) == 4:
        ping_time += time_list.pop() + ', '
    time_list.reverse()
    ping_time += ':'.join(time_list)
    return ping_time

# ===== HELPER: gets.py =====
from typing import Union
from pyrogram.enums import MessageEntityType
from pyrogram.types import Audio, Message, Voice

def get_url(message_1: Message) -> Union[str, None]:
    messages = [message_1]
    if message_1.reply_to_message:
        messages.append(message_1.reply_to_message)
    text = ''
    offset = None
    length = None
    for message in messages:
        if offset:
            break
        if message.entities:
            for entity in message.entities:
                if entity.type == MessageEntityType.URL:
                    text = message.text or message.caption
                    offset, length = (entity.offset, entity.length)
                    break
    if offset in (None,):
        return None
    return text[offset:offset + length]

def get_file_name(audio: Union[Audio, Voice]):
    return f"{audio.file_unique_id}.{(audio.file_name.split('.')[-1] if not isinstance(audio, Voice) else 'ogg')}"

# ===== HELPER: inline.py =====
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
close_key = InlineKeyboardMarkup([[InlineKeyboardButton('إغلاق', callback_data='close')]])
buttons = InlineKeyboardMarkup([[InlineKeyboardButton('استكمال', callback_data='resume_cb'), InlineKeyboardButton('تخطي', callback_data='skip_cb'), InlineKeyboardButton('إيقاف', callback_data='pause_cb')], [InlineKeyboardButton('إنهاء', callback_data='end_cb')]])

def _support_rows():
    rows = []
    if config.DEVELOPER_CHANNEL:
        rows.append([InlineKeyboardButton('قناة المطور', url=f'https://t.me/{config.DEVELOPER_CHANNEL}')])
    if config.DEVELOPER_USERNAME:
        rows.append([InlineKeyboardButton('المطور', url=f'https://t.me/{config.DEVELOPER_USERNAME}')])
    return rows
pm_buttons = [[InlineKeyboardButton('إضافة إلى مجموعة', url=f'https://t.me/{BOT_USERNAME}?startgroup=true')], [InlineKeyboardButton('الأوامر', callback_data='fallen_cb help')], *_support_rows()]
gp_buttons = [[InlineKeyboardButton('إضافة إلى مجموعة', url=f'https://t.me/{BOT_USERNAME}?startgroup=true')], *_support_rows()]
helpmenu = [[InlineKeyboardButton('الأوامر', callback_data='fallen_cb help')], [InlineKeyboardButton('أوامر الإدارة', callback_data='fallen_cb sudo')], [InlineKeyboardButton('لوحة المالك', callback_data='fallen_cb owner')], [InlineKeyboardButton('إغلاق', callback_data='close')]]
help_back = []
if config.DEVELOPER_CHANNEL:
    help_back.append([InlineKeyboardButton('قناة المطور', url=f'https://t.me/{config.DEVELOPER_CHANNEL}')])
help_back.append([InlineKeyboardButton('رجوع', callback_data='fallen_help')])
help_back.append([InlineKeyboardButton('إغلاق', callback_data='close')])

# ===== HELPER: queue.py =====
async def put(chat_id, title, duration, videoid, file_path, ruser, user_id):
    put_f = {'title': title, 'duration': duration, 'file_path': file_path, 'videoid': videoid, 'req': ruser, 'user_id': user_id}
    get = fallendb.get(chat_id)
    if get:
        fallendb[chat_id].append(put_f)
    else:
        fallendb[chat_id] = []
        fallendb[chat_id].append(put_f)

# ===== HELPER: thumbnails.py =====
import asyncio

def _thumbnail_from_info(info: dict) -> str | None:
    return info.get('thumbnail')

async def _get_thumbnail(videoid: str) -> str | None:
    if not videoid or videoid == 'fuckitstgaudio':
        return None
    try:
        info = await asyncio.to_thread(video_info, f'https://www.youtube.com/watch?v={videoid}')
        return _thumbnail_from_info(info)
    except Exception:
        return None

async def gen_thumb(videoid, user_id):
    return await _get_thumbnail(videoid) or FAILED

async def gen_qthumb(videoid, user_id):
    return await _get_thumbnail(videoid) or FAILED

# ===== HELPER: downloaders.py =====
import os
import urllib.parse
from pathlib import Path
import yt_dlp

def clean_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(parsed.query)
    if parsed.netloc in {'youtu.be', 'www.youtu.be'}:
        video_id = parsed.path.strip('/')
        return f'https://www.youtube.com/watch?v={video_id}' if video_id else url
    if 'youtube.com' in parsed.netloc and query.get('v'):
        return f"https://www.youtube.com/watch?v={query['v'][0]}"
    return url

def _common_opts() -> dict:
    opts = {'quiet': True, 'no_warnings': True, 'noplaylist': True, 'retries': 2}
    if config.COOKIES_FILE.is_file():
        opts['cookiefile'] = str(config.COOKIES_FILE)
    return opts

def _audio_opts() -> dict:
    opts = _common_opts()
    opts.update({'format': 'bestaudio[ext=m4a]/bestaudio', 'outtmpl': str(config.DOWNLOADS_DIR / '%(id)s.%(ext)s'), 'overwrites': True})
    return opts

def audio_dl(url: str) -> str:
    cleaned_url = clean_url(url)
    with yt_dlp.YoutubeDL(_audio_opts()) as ydl:
        info = ydl.extract_info(cleaned_url, download=True)
        path = Path(ydl.prepare_filename(info))
        if not path.is_file():
            candidates = list(config.DOWNLOADS_DIR.glob(f"{info['id']}.*"))
            if not candidates:
                raise FileNotFoundError('yt-dlp downloaded the media but no file was found.')
            path = candidates[0]
        return str(path)

def video_info(url: str) -> dict:
    cleaned_url = clean_url(url)
    opts = _common_opts()
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(cleaned_url, download=False)
    return info

def search_youtube(query: str, max_results: int=4) -> list[dict]:
    opts = _common_opts()
    opts['extract_flat'] = True
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(f'ytsearch{max_results}:{query}', download=False)
    entries = []
    for entry in (info or {}).get('entries') or []:
        if entry:
            entries.append(entry)
    return entries[:max_results]

def cleanup_file(path: str | None) -> None:
    if not path:
        return
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except OSError:
        pass

# ===== HELPER: clear.py =====
async def _clear_(chat_id):
    try:
        for item in fallendb.get(chat_id, []):
            cleanup_file(item.get('file_path'))
        fallendb[chat_id] = []
        current = CURRENT_FILES.pop(chat_id, None)
        cleanup_file(current)
        await remove_active_chat(chat_id)
    except Exception:
        return

# ===== MODULE: assistant.py =====
import re
from pyrogram import filters
from pyrogram.errors import SessionPasswordNeeded
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, CallbackQuery
LOGIN_STATE = {}

def clean_code(value: str) -> str:
    return re.sub('[^0-9]', '', value or '')

def owner_panel():
    return InlineKeyboardMarkup([[InlineKeyboardButton('إضافة حساب المساعد', callback_data='kro_add_assistant')], [InlineKeyboardButton('حالة حساب المساعد', callback_data='kro_assistant_status')], [InlineKeyboardButton('حذف حساب المساعد', callback_data='kro_remove_assistant')]])

@app.on_message(filters.command('المساعد') & filters.user(config.OWNER_ID))
async def assistant_panel(_, message: Message):
    await message.reply_text('إدارة حساب المساعد KroMusic:', reply_markup=owner_panel())

@app.on_callback_query(filters.regex('^kro_add_assistant$'))
async def add_assistant_start(_, query: CallbackQuery):
    if query.from_user.id != config.OWNER_ID:
        return await query.answer('هذا الخيار متاح للمالك فقط.', show_alert=True)
    LOGIN_STATE[query.from_user.id] = {'step': 'phone'}
    await query.answer()
    await query.message.edit_text('إضافة حساب المساعد\n\nأرسل رقم الهاتف بصيغة دولية.\nمثال: +9647XXXXXXXXX\n\nلإلغاء العملية أرسل /cancel')

@app.on_callback_query(filters.regex('^kro_assistant_status$'))
async def assistant_status(_, query: CallbackQuery):
    if query.from_user.id != config.OWNER_ID:
        return await query.answer('هذا الخيار متاح للمالك فقط.', show_alert=True)
    if fm.app2 is None:
        text = 'حساب المساعد: غير مسجل دخول.'
    else:
        text = f"حساب المساعد: متصل\n{fm.ASS_MENTION or 'بدون اسم مستخدم'}"
    await query.answer()
    await query.message.edit_text(text, reply_markup=owner_panel())

@app.on_callback_query(filters.regex('^kro_remove_assistant$'))
async def remove_assistant(_, query: CallbackQuery):
    if query.from_user.id != config.OWNER_ID:
        return await query.answer('هذا الخيار متاح للمالك فقط.', show_alert=True)
    await stop_assistant()
    delete_saved_session()
    LOGIN_STATE.pop(query.from_user.id, None)
    await query.answer('تم حذف جلسة المساعد.', show_alert=True)
    await query.message.edit_text('تم حذف جلسة حساب المساعد. يمكنك تسجيل حساب جديد.', reply_markup=owner_panel())

@app.on_message(filters.command('cancel') & filters.user(config.OWNER_ID))
async def cancel_login(_, message: Message):
    LOGIN_STATE.pop(message.from_user.id, None)
    await message.reply_text('تم إلغاء تسجيل الدخول.')

@app.on_message(filters.text & filters.user(config.OWNER_ID), group=20)
async def assistant_login_flow(_, message: Message):
    state = LOGIN_STATE.get(message.from_user.id)
    if not state or message.text.startswith('/'):
        return
    try:
        if state['step'] == 'phone':
            phone = message.text.strip().replace(' ', '')
            if not re.fullmatch('\\+?[0-9]{7,15}', phone):
                return await message.reply_text('رقم الهاتف غير صحيح. أرسله بصيغة دولية.')
            from pyrogram import Client
            client = Client('KroAssistantLogin', api_id=config.API_ID, api_hash=config.API_HASH, in_memory=True)
            await client.connect()
            sent = await client.send_code(phone)
            state.update({'step': 'code', 'client': client, 'phone': phone, 'hash': sent.phone_code_hash})
            return await message.reply_text('تم إرسال رمز التحقق.\n\nأرسل الرمز كما وصلك. يمكنك وضع نقاط بين الأرقام، مثل: 12.345.\n\nلإلغاء العملية أرسل /cancel')
        if state['step'] == 'code':
            code = clean_code(message.text)
            if len(code) < 4:
                return await message.reply_text('رمز التحقق غير صحيح.')
            client = state['client']
            try:
                await client.sign_in(state['phone'], state['hash'], code)
            except SessionPasswordNeeded:
                state['step'] = 'password'
                return await message.reply_text('الحساب محمي بالمصادقة الثنائية.\nأرسل كلمة مرور المصادقة الثنائية.\n\nلإلغاء العملية أرسل /cancel')
            return await _finish_login(message, client)
        if state['step'] == 'password':
            client = state['client']
            await client.check_password(message.text)
            return await _finish_login(message, client)
    except Exception as exc:
        LOGGER.error('Assistant login failed: %s', exc)
        client = state.get('client')
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        LOGIN_STATE.pop(message.from_user.id, None)
        await message.reply_text(f'فشل تسجيل حساب المساعد.\nالسبب: `{type(exc).__name__}`')

async def _finish_login(message: Message, client):
    session = await client.export_session_string()
    await client.disconnect()
    save_session(session)
    LOGIN_STATE.pop(message.from_user.id, None)
    try:
        await start_assistant(session)
        await message.reply_text(f"تم تسجيل حساب المساعد بنجاح.\nالحساب: {fm.ASS_MENTION or 'بدون معرف'}\n\nالجلسة محفوظة محليًا في قاعدة بيانات البوت ولا يتم إرسالها في رسالة.")
    except Exception as exc:
        delete_saved_session()
        await message.reply_text(f'تم تسجيل الدخول لكن تعذر تشغيل المساعد.\nالسبب: `{type(exc).__name__}`')

@app.on_message(filters.text & filters.regex('^(asspfp|setpfp|ضيف صورة)$') & SUDOERS)
async def set_pfp(_, message: Message):
    if fm.app2 is None:
        return await message.reply_text('حساب المساعد غير مسجل الدخول.')
    if message.reply_to_message and message.reply_to_message.photo:
        fuk = await message.reply_text('جاري تغيير صورة المساعد...')
        img = await message.reply_to_message.download()
        try:
            await fm.app2.set_profile_photo(photo=img)
            return await fuk.edit_text('تم تغيير صورة المساعد.')
        except Exception:
            return await fuk.edit_text('فشل تغيير صورة المساعد.')
    await message.reply_text('قم بالرد على صورة لتعيينها للمساعد.')

@app.on_message(filters.text & filters.regex('^(delpfp|delasspfp|مسح صورة)$') & SUDOERS)
async def del_pfp(_, message: Message):
    if fm.app2 is None:
        return await message.reply_text('حساب المساعد غير مسجل الدخول.')
    try:
        pfp = [p async for p in fm.app2.get_chat_photos('me')]
        if pfp:
            await fm.app2.delete_profile_photos(pfp[0].file_id)
        await message.reply_text('تم حذف صورة المساعد.')
    except Exception as ex:
        LOGGER.error(ex)
        await message.reply_text('فشل حذف صورة المساعد.')

@app.on_message(filters.text & filters.regex('^(assbio|setbio|بايو مساعد)') & SUDOERS)
async def set_bio(_, message: Message):
    if fm.app2 is None:
        return await message.reply_text('حساب المساعد غير مسجل الدخول.')
    msg = message.reply_to_message
    text = msg.text if msg and msg.text else message.text.split(None, 1)[1] if len(message.text.split()) > 1 else ''
    if not text:
        return await message.reply_text('أرسل نصًا أو رد على رسالة لتعيين البايو.')
    await fm.app2.update_profile(bio=text)
    await message.reply_text('تم تحديث بايو المساعد.')

@app.on_message(filters.text & filters.regex('^(assname|setname|تغيير الاسم)') & SUDOERS)
async def set_name(_, message: Message):
    if fm.app2 is None:
        return await message.reply_text('حساب المساعد غير مسجل الدخول.')
    msg = message.reply_to_message
    name = msg.text if msg and msg.text else message.text.split(None, 1)[1] if len(message.text.split()) > 1 else ''
    if not name:
        return await message.reply_text('أرسل الاسم الجديد أو رد على رسالة.')
    await fm.app2.update_profile(first_name=name, last_name='')
    await message.reply_text('تم تحديث اسم المساعد.')

# ===== MODULE: callback.py =====
from pyrogram import filters
from pyrogram.types import CallbackQuery, InlineKeyboardMarkup
from pytgcalls.types import AudioPiped, HighQualityAudio

@app.on_callback_query(filters.regex('forceclose'))
async def close_(_, CallbackQuery):
    callback_data = CallbackQuery.data.strip()
    callback_request = callback_data.split(None, 1)[1]
    query, user_id = callback_request.split('|')
    if CallbackQuery.from_user.id != int(user_id):
        try:
            return await CallbackQuery.answer('‹ : يرجى عدم العبث بما لا يخصك .', show_alert=True)
        except:
            return
    await CallbackQuery.message.delete()
    try:
        await CallbackQuery.answer()
    except:
        return

@app.on_callback_query(filters.regex('close'))
async def forceclose_command(_, CallbackQuery):
    try:
        await CallbackQuery.message.delete()
    except:
        return
    try:
        await CallbackQuery.answer()
    except:
        pass

@app.on_callback_query(filters.regex(pattern='^(resume_cb|pause_cb|skip_cb|end_cb)$'))
@admin_check_cb
async def admin_cbs(_, query: CallbackQuery):
    try:
        await query.answer()
    except:
        pass
    data = query.matches[0].group(1)
    if data == 'resume_cb':
        if await is_streaming(query.message.chat.id):
            return await query.answer('‹ : البث لم يكن موقوف أصلاً .', show_alert=True)
        await stream_on(query.message.chat.id)
        await pytgcalls.resume_stream(query.message.chat.id)
        await query.message.reply_text(text=f'‹ : تم استئناف البث من قبل : {query.from_user.mention} .', reply_markup=close_key)
    elif data == 'pause_cb':
        if not await is_streaming(query.message.chat.id):
            return await query.answer('‹ : البث متوقف بالفعل .', show_alert=True)
        await stream_off(query.message.chat.id)
        await pytgcalls.pause_stream(query.message.chat.id)
        await query.message.reply_text(text=f'‹ : تم إيقاف البث مؤقتاً من قبل : {query.from_user.mention} .', reply_markup=close_key)
    elif data == 'end_cb':
        try:
            await _clear_(query.message.chat.id)
            await pytgcalls.leave_group_call(query.message.chat.id)
        except:
            pass
        await query.message.reply_text(text=f'‹ : تم إنهاء البث من قبل : {query.from_user.mention} .', reply_markup=close_key)
        await query.message.delete()
    elif data == 'skip_cb':
        get = fallendb.get(query.message.chat.id)
        if not get:
            try:
                await _clear_(query.message.chat.id)
                await pytgcalls.leave_group_call(query.message.chat.id)
                await query.message.reply_text(text=f'‹ : تم تخطي البث من قبل : {query.from_user.mention} .\n\n‹ : لا توجد مقاطع أخرى في قائمة التشغيل في {query.message.chat.title}، جاري مغادرة المكالمة الصوتية.', reply_markup=close_key)
                return await query.message.delete()
            except:
                return
        else:
            title = get[0]['title']
            duration = get[0]['duration']
            videoid = get[0]['videoid']
            file_path = get[0]['file_path']
            req_by = get[0]['req']
            user_id = get[0]['user_id']
            get.pop(0)
            stream = AudioPiped(file_path, audio_parameters=HighQualityAudio())
            old_file = CURRENT_FILES.get(query.message.chat.id)
            try:
                await pytgcalls.change_stream(query.message.chat.id, stream)
            except Exception as ex:
                LOGGER.error(ex)
                await _clear_(query.message.chat.id)
                return await pytgcalls.leave_group_call(query.message.chat.id)
            CURRENT_FILES[query.message.chat.id] = file_path
            cleanup_file(old_file)
            img = await gen_thumb(videoid, user_id)
            await query.edit_message_text(text=f'‹ : تم تخطي المقطع من قبل : {query.from_user.mention} .', reply_markup=close_key)
            return await query.message.reply_photo(photo=img, caption=f'‹ : بدء البث الجديد\n\n‹ : العنوان : [{title[:27]}](https://t.me/{BOT_USERNAME}?start=info_{videoid})\n‹ : المدة : `{duration}` دقيقة\n‹ : بواسطة : {req_by}', reply_markup=buttons)

@app.on_callback_query(filters.regex('unban_ass'))
async def unban_ass(_, CallbackQuery):
    callback_data = CallbackQuery.data.strip()
    callback_request = callback_data.split(None, 1)[1]
    chat_id, user_id = callback_request.split('|')
    umm = (await app.get_chat_member(int(chat_id), BOT_ID)).privileges
    if umm.can_restrict_members:
        try:
            await app.unban_chat_member(int(chat_id), ASS_ID)
        except:
            return await CallbackQuery.answer('‹ : فشل إلغاء الحظر عن المساعد.', show_alert=True)
        return await CallbackQuery.edit_message_text(f'‹ : تم إلغاء الحظر عن {ASS_NAME} من قبل {CallbackQuery.from_user.mention}، جرب تشغيل شيء الآن ...')
    else:
        return await CallbackQuery.answer('‹ : لا أملك صلاحية إلغاء الحظر في هذه المجموعة.', show_alert=True)

@app.on_callback_query(filters.regex('fallen_help'))
async def help_menu(_, query: CallbackQuery):
    try:
        await query.answer()
    except:
        pass
    try:
        await query.edit_message_text(text=f'‹ : أهلًا {query.from_user.first_name} .\n\n‹ : اضغط على الزر أدناه لاختيار نوع المساعدة التي تريدها.', reply_markup=InlineKeyboardMarkup(helpmenu))
    except Exception as e:
        LOGGER.error(e)

@app.on_callback_query(filters.regex('fallen_cb'))
async def open_hmenu(_, query: CallbackQuery):
    callback_data = query.data.strip()
    cb = callback_data.split(None, 1)[1]
    keyboard = InlineKeyboardMarkup(help_back)
    try:
        await query.answer()
    except:
        pass
    if cb == 'help':
        await query.edit_message_text(HELP_TEXT, reply_markup=keyboard)
    elif cb == 'sudo':
        await query.edit_message_text(HELP_SUDO, reply_markup=keyboard)
    elif cb == 'owner':
        from KroMusic.Modules.assistant import owner_panel
        await query.edit_message_text(HELP_DEV, reply_markup=owner_panel())

@app.on_callback_query(filters.regex('fallen_home'))
async def home_fallen(_, query: CallbackQuery):
    try:
        await query.answer()
    except:
        pass
    try:
        await query.edit_message_text(text=PM_START_TEXT.format(query.from_user.first_name, BOT_MENTION), reply_markup=InlineKeyboardMarkup(pm_buttons))
    except:
        pass

# ===== MODULE: cleaner.py =====
import shutil
from pyrogram import filters
from pyrogram.types import Message

@app.on_message(filters.command(['clearcache', 'rmdownloads']) & filters.user(OWNER_ID))
async def clear_misc(_, message: Message):
    try:
        await message.delete()
    except Exception:
        pass
    for directory in (DOWNLOADS_DIR, CACHE_DIR):
        directory.mkdir(parents=True, exist_ok=True)
        for item in directory.iterdir():
            try:
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
            except OSError:
                pass
    await message.reply_text('تم تنظيف الملفات المؤقتة.')

# ===== MODULE: inline.py =====
import asyncio
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResultPhoto

@app.on_inline_query()
async def inline_query_handler(_, query):
    text = query.query.strip()
    if not text:
        try:
            await app.answer_inline_query(query.id, results=[], switch_pm_text='اكتب شي حتى ابحث باليوتيوب', cache_time=10)
        except Exception:
            pass
        return
    try:
        result = await asyncio.to_thread(search_youtube, text, 15)
    except Exception:
        return
    answers = []
    for item in result:
        video_id = item.get('id')
        if not video_id:
            continue
        title = item.get('title') or 'بدون عنوان'
        duration = item.get('duration_string') or item.get('duration') or 'غير معروف'
        channel = item.get('channel') or item.get('uploader') or 'غير معروف'
        link = item.get('webpage_url') or f'https://www.youtube.com/watch?v={video_id}'
        thumbnail = item.get('thumbnail') or f'https://i.ytimg.com/vi/{video_id}/hqdefault.jpg'
        description = f'{duration} | {channel}'
        buttons = InlineKeyboardMarkup([[InlineKeyboardButton(text='‹ يوتيوب ›', url=link)]])
        caption = f'‹ : **العنوان :** [{title}]({link})\n\n‹ : **المدة :** `{duration}`\n‹ : **القناة :** `{channel}`\n\n<u>‹ : **البحث تم بواسطة {BOT_NAME}**</u>'
        answers.append(InlineQueryResultPhoto(photo_url=thumbnail, title=title, thumb_url=thumbnail, description=description, caption=caption, reply_markup=buttons))
    try:
        await app.answer_inline_query(query.id, results=answers, cache_time=10)
    except Exception:
        pass

# ===== MODULE: pause.py =====
from pyrogram import filters
from pyrogram.types import Message

@app.on_message(filters.text & filters.regex('^(pause|اوكف)$') & filters.group)
@admin_check
async def pause_str(_, message: Message):
    try:
        await message.delete()
    except:
        pass
    if not await is_streaming(message.chat.id):
        return await message.reply_text('هل تذكر إنك شغلت البث؟ يبدو أنه موقّف بالفعل.')
    await pytgcalls.pause_stream(message.chat.id)
    await stream_off(message.chat.id)
    return await message.reply_text(text=f'‹ : البث موقف\n \n‹ : بواسطة {message.from_user.mention} .', reply_markup=close_key)

# ===== MODULE: ping.py =====
import time
from pyrogram import filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

@app.on_message(filters.command('ping'))
async def ping_fallen(_, message):
    started = time.perf_counter()
    msg = await message.reply_text('KroMusic: جارِ الفحص...')
    latency = (time.perf_counter() - started) * 1000
    uptime = get_readable_time(int(time.time() - StartTime))
    buttons = []
    if config.DEVELOPER_CHANNEL:
        buttons.append([InlineKeyboardButton('قناة المطور', url=f'https://t.me/{config.DEVELOPER_CHANNEL}')])
    if config.DEVELOPER_USERNAME:
        buttons.append([InlineKeyboardButton('المطور', url=f'https://t.me/{config.DEVELOPER_USERNAME}')])
    await msg.edit_text(f'{BOT_NAME}\n\nزمن الاستجابة: `{latency:.0f} ms`\nمدة التشغيل: `{uptime}`', reply_markup=InlineKeyboardMarkup(buttons) if buttons else None)

# ===== MODULE: play.py =====
import asyncio
import os
from pyrogram import filters
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import ChatAdminRequired, UserAlreadyParticipant, UserNotParticipant
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from pytgcalls import StreamType
from pytgcalls.exceptions import NoActiveGroupCall, TelegramServerError, UnMuteNeeded
from pytgcalls.types import AudioPiped, HighQualityAudio

def _duration_seconds(duration) -> int:
    if isinstance(duration, (int, float)):
        return int(duration)
    if not duration:
        return 0
    parts = [int(x) for x in str(duration).split(':')]
    result = 0
    for part in parts:
        result = result * 60 + part
    return result

async def _metadata(url: str) -> tuple[str, str, str, int]:
    info = await asyncio.to_thread(video_info, url)
    title = info.get('title') or 'YouTube Audio'
    duration_seconds = int(info.get('duration') or 0)
    videoid = info.get('id') or 'unknown'
    duration = f'{duration_seconds // 60}:{duration_seconds % 60:02d}'
    return (title, duration, videoid, duration_seconds)

@app.on_message(filters.group & ~filters.forwarded & ~filters.via_bot & filters.regex('^(play|vplay|p|شغل|تشغيل)( .*)?$'))
async def play(_, message: Message):
    if app2 is None or pytgcalls is None or ASS_ID is None:
        return await message.reply_text('حساب المساعد غير مسجل الدخول. يمكن للمالك إضافة الحساب من /المساعد.')
    fallen = await message.reply_text('يرجى الانتظار...')
    try:
        await message.delete()
    except Exception:
        pass
    try:
        try:
            member = await app.get_chat_member(message.chat.id, ASS_ID)
        except ChatAdminRequired:
            return await fallen.edit_text('‹ : ماعندي صلاحيات حتى اضيف المساعد .')
        if member.status == ChatMemberStatus.BANNED:
            key = InlineKeyboardMarkup([[InlineKeyboardButton(text=f'الغاء الحظر عن {ASS_NAME}', callback_data=f'unban_assistant {message.chat.id}|{ASS_ID}')]])
            return await fallen.edit_text(text=f'‹ : المساعد محظور\n\n‹ : 𝖨𝖣 : `{ASS_ID}`\n‹ : 𝖭𝖠𝖬𝖤 : {ASS_MENTION}\n‹ : 𝖴𝖲𝖤𝖱𝖭𝖠𝖬𝖤 : @{ASS_USERNAME}\n\n‹ : افتح الحظر وحاول مرة ثانية .', reply_markup=key)
    except UserNotParticipant:
        if message.chat.username:
            invitelink = message.chat.username
        else:
            try:
                invitelink = await app.export_chat_invite_link(message.chat.id)
            except ChatAdminRequired:
                return await fallen.edit_text('‹ : ماعندي صلاحيات حتى اضيف المساعد .')
            except Exception as exc:
                return await fallen.edit_text(f'فشل اضافة {BOT_NAME} المساعد الى {message.chat.title}.\n\nالسبب: `{exc}`')
        if invitelink.startswith('https://t.me/+'):
            invitelink = invitelink.replace('https://t.me/+', 'https://t.me/joinchat/')
        try:
            await app2.join_chat(invitelink)
            await asyncio.sleep(1)
        except UserAlreadyParticipant:
            pass
        except Exception as exc:
            return await fallen.edit_text(f'فشل اضافة {BOT_NAME} المساعد {message.chat.title}.\n\nالسبب: `{exc}`')
    ruser = message.from_user.first_name
    audio = message.reply_to_message.audio or message.reply_to_message.voice if message.reply_to_message else None
    url = get_url(message)
    if audio:
        if round(audio.duration / 60) > DURATION_LIMIT:
            raise DurationLimitError('‹ : المقطع طويل جداً .')
        file_name = get_file_name(audio)
        file_path = str(DOWNLOADS_DIR / file_name)
        title = file_name
        duration = f'{audio.duration // 60}:{audio.duration % 60:02d}'
        if not os.path.isfile(file_path):
            file_path = await message.reply_to_message.download(file_path)
        videoid = 'telegram'
        duration_seconds = int(audio.duration)
    else:
        if url:
            target_url = url
        else:
            if len(message.text.split()) < 2:
                return await fallen.edit_text('‹ : شتريد عبيبي ؟')
            query = message.text.split(None, 1)[1]
            try:
                results = await asyncio.to_thread(search_youtube, query, 1)
                if not results:
                    return await fallen.edit_text('‹ : ما لكيت نتيجة على يوتيوب .')
                target_url = results[0].get('webpage_url') or f"https://www.youtube.com/watch?v={results[0].get('id')}"
            except Exception as exc:
                LOGGER.error('YouTube search failed: %s', exc)
                return await fallen.edit_text(f'صارت مشكلة\n\nالخطأ: `{exc}`')
        try:
            title, duration, videoid, duration_seconds = await _metadata(target_url)
        except Exception as exc:
            LOGGER.error('YouTube metadata failed: %s', exc)
            return await fallen.edit_text(f'صارت مشكلة\n\nالخطأ: `{exc}`')
        if duration_seconds / 60 > DURATION_LIMIT:
            return await fallen.edit_text('‹ : المقطع طويل جداً .')
        try:
            file_path = await asyncio.to_thread(audio_dl, target_url)
        except Exception as exc:
            LOGGER.error('YouTube download failed: %s', exc)
            return await fallen.edit_text(f'‹ : فشل تحميل المقطع من يوتيوب .\n\nالخطأ: `{exc}`')
    try:
        if await is_active_chat(message.chat.id):
            await put(message.chat.id, title, duration, videoid, file_path, ruser, message.from_user.id)
            position = len(fallendb.get(message.chat.id))
            qimg = await gen_qthumb(videoid, message.from_user.id)
            await message.reply_photo(photo=qimg, caption=f'**‹ : تم اضافتها الى قائمة الانتضار : {position}**\n\n**‹ : العنوان :** [{title[:27]}](https://t.me/{BOT_USERNAME}?start=info_{videoid})\n**‹ : المدة :** `{duration}`\n**‹ : بواسطة :** {ruser}', reply_markup=buttons)
            return await fallen.delete()
        stream = AudioPiped(file_path, audio_parameters=HighQualityAudio())
        try:
            await pytgcalls.join_group_call(message.chat.id, stream, stream_type=StreamType().pulse_stream)
        except NoActiveGroupCall:
            return await fallen.edit_text('**‹ : ماكو اتصال .**\n\n‹ : افتح الاتصال وشغل من جديد .')
        except TelegramServerError:
            return await fallen.edit_text('‹ : اكو بعض المشاكل افتح الاتصال من جديد')
        except UnMuteNeeded:
            return await fallen.edit_text(f'‹ : {BOT_NAME} المساعد مكتوم,\n\n‹ : فك الكتم عن المساعد {ASS_MENTION} .')
        CURRENT_FILES[message.chat.id] = file_path
        await stream_on(message.chat.id)
        await add_active_chat(message.chat.id)
        imgt = await gen_thumb(videoid, message.from_user.id)
        await message.reply_photo(photo=imgt, caption=f'**‹ : بدء البث**\n\n**‹ : العنوان :** [{title[:27]}](https://t.me/{BOT_USERNAME}?start=info_{videoid})\n**‹ : المدة :** `{duration}`\n**‹ : بواسطة :** {ruser}', reply_markup=buttons)
    except Exception as exc:
        LOGGER.error('Playback failed: %s', exc)
        try:
            from KroMusic.Helpers.downloaders import cleanup_file
            cleanup_file(file_path)
        except Exception:
            pass
        return await fallen.edit_text(f'‹ : فشل تشغيل المقطع .\n\nالخطأ: `{exc}`')
    return await fallen.delete()

# ===== MODULE: resume.py =====
from pyrogram import filters
from pyrogram.types import Message

@app.on_message(filters.text & filters.regex('^(resume|كمل)$') & filters.group)
@admin_check
async def res_str(_, message: Message):
    try:
        await message.delete()
    except:
        pass
    if await is_streaming(message.chat.id):
        return await message.reply_text('تريد اذكرك انت موقفة ؟')
    await stream_on(message.chat.id)
    await pytgcalls.resume_stream(message.chat.id)
    return await message.reply_text(text=f'‹ : تشغيل الاغنية\n\n‹ : بواسطة : {message.from_user.mention} .', reply_markup=close_key)

# ===== MODULE: search.py =====
import asyncio
from pyrogram import filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

@app.on_message(filters.text & filters.regex('^(بحث|search)\\s+'))
async def ytsearch(_, message: Message):
    try:
        await message.delete()
    except Exception:
        pass
    try:
        query = message.text.split(None, 1)[1]
    except IndexError:
        return await message.reply_text('‹ : ارسل شي ياحبيبي .')
    m = await message.reply_text('‹ : جاري البحث ...')
    try:
        results = await asyncio.to_thread(search_youtube, query, 4)
        if not results:
            return await m.edit_text('‹ : ما لكيت نتائج على يوتيوب .')
        lines = []
        for entry in results:
            title = entry.get('title') or 'بدون عنوان'
            duration = entry.get('duration_string') or entry.get('duration') or 'غير معروف'
            channel = entry.get('channel') or entry.get('uploader') or 'غير معروف'
            url = entry.get('webpage_url') or f"https://www.youtube.com/watch?v={entry.get('id')}"
            lines.append(f'‹ : العنوان : {title}\n‹ : المدة : `{duration}`\n‹ : القناة : {channel}\n‹ : الرابط : {url}\n')
        key = InlineKeyboardMarkup([[InlineKeyboardButton(text='‹ اغلاق ›', callback_data=f'forceclose abc|{message.from_user.id}')]])
        await m.edit_text(text='\n'.join(lines), reply_markup=key, disable_web_page_preview=True)
    except Exception as exc:
        await m.edit_text(f'‹ : صار خطأ أثناء البحث\n\n**{exc}**')

# ===== MODULE: skip.py =====
from pyrogram import filters
from pyrogram.types import Message
from pytgcalls.types import AudioPiped, HighQualityAudio

@app.on_message(filters.text & filters.regex('^(skip|next|تخطي)$') & filters.group)
@admin_check
async def skip_str(_, message: Message):
    try:
        await message.delete()
    except:
        pass
    get = fallendb.get(message.chat.id)
    if not get:
        try:
            await _clear_(message.chat.id)
            await pytgcalls.leave_group_call(message.chat.id)
        except:
            pass
        try:
            await message.reply_text(text=f'‹ : تم التخطي \n\n‹ : بواسطة : {message.from_user.mention} .\n\n**‹ : لا شيء في القائمة :** {message.chat.title}, **‹ : غادر الاتصال .**', reply_markup=close_key)
        except:
            pass
        return
    title = get[0]['title']
    duration = get[0]['duration']
    file_path = get[0]['file_path']
    videoid = get[0]['videoid']
    req_by = get[0]['req']
    user_id = get[0]['user_id']
    get.pop(0)
    stream = AudioPiped(file_path, audio_parameters=HighQualityAudio())
    try:
        await pytgcalls.change_stream(message.chat.id, stream)
    except:
        await _clear_(message.chat.id)
        await pytgcalls.leave_group_call(message.chat.id)
        return
    try:
        await message.reply_text(text=f'‹ : تم التخطي بواسطة : \n\n‹ : بواسطة : {message.from_user.mention} .', reply_markup=close_key)
    except:
        pass
    img = await gen_thumb(videoid, user_id)
    await message.reply_photo(photo=img, caption=f'**‹ : بدء البث :**\n\n**‹ : العنوان :** [{title[:27]}](https://t.me/{BOT_USERNAME}?start=info_{videoid})\n**‹ : المدة :** `{duration}` دقيقة .\n**‹ : بواسطة ** {req_by}', reply_markup=buttons)

# ===== MODULE: song.py =====
import asyncio
from pyrogram import filters
from pyrogram.enums import ChatType
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

@app.on_message(filters.text & filters.regex('^(song|vsong|video|music|الاغنية|الاغنيه|المقطع|فيديو|الفيديو)'))
async def song(_, message: Message):
    try:
        await message.delete()
    except Exception:
        pass
    m = await message.reply_text('‹ : جاري البحث ...')
    query = ' '.join(message.command[1:]).strip()
    if not query:
        return await m.edit_text('‹ : اكتب اسم الاغنية بعد الامر .')
    try:
        results = await asyncio.to_thread(search_youtube, query, 1)
        if not results:
            return await m.edit_text('‹ : ما لكيت نتيجة على يوتيوب .')
        entry = results[0]
        link = entry.get('webpage_url') or f"https://www.youtube.com/watch?v={entry.get('id')}"
        title = (entry.get('title') or 'YouTube Audio')[:40]
        duration_seconds = int(entry.get('duration') or 0)
        duration = f'{duration_seconds // 60}:{duration_seconds % 60:02d}'
    except Exception as exc:
        LOGGER.error('YouTube search failed: %s', exc)
        return await m.edit_text(f'‹ : فشل جلب المقطع من اليوتيوب\n\nالنتيجة: `{exc}`')
    await m.edit_text('‹ : تحميل الاغنية,\n\n‹ : انتظر ...')
    audio_file = None
    try:
        audio_file = await asyncio.to_thread(audio_dl, link)
        rep = f'‹ : **العنوان :** [{title[:23]}]({link})\n‹ : **المدة :** `{duration}`\n‹ : **رفع بواسطة :** {BOT_MENTION}'
        visit_butt = InlineKeyboardMarkup([[InlineKeyboardButton(text='‹ يوتيوب ›', url=link)]])
        await app.send_audio(chat_id=message.from_user.id, audio=audio_file, caption=rep, title=title, duration=duration_seconds, reply_markup=visit_butt)
        if message.chat.type != ChatType.PRIVATE:
            await message.reply_text('‹ : تم ارسال الاغنية .')
        await m.delete()
    except Exception as exc:
        LOGGER.error('Song delivery failed: %s', exc)
        await m.edit_text(f'‹ : فشل الأرسال .\n\nالخطأ: `{exc}`')
    finally:
        from KroMusic.Helpers.downloaders import cleanup_file
        cleanup_file(audio_file)

# ===== MODULE: start.py =====
import asyncio
import random
from pyrogram import filters
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.types import ChatMemberUpdated, InlineKeyboardMarkup, Message

@app.on_message(filters.command(['start']) & ~filters.forwarded)
@app.on_edited_message(filters.command(['start']) & ~filters.forwarded)
async def fallen_st(_, message: Message):
    if message.chat.type == ChatType.PRIVATE and len(message.text.split()) > 1:
        cmd = message.text.split(None, 1)[1]
        if cmd.startswith('info_'):
            videoid = cmd.replace('info_', '', 1)
            m = await message.reply_text('جاري جلب معلومات المقطع ...')
            try:
                info = await asyncio.to_thread(video_info, f'https://www.youtube.com/watch?v={videoid}')
                title = info.get('title') or 'غير معروف'
                duration = int(info.get('duration') or 0)
                duration_text = f'{duration // 60}:{duration % 60:02d}'
                views = info.get('view_count') or 0
                published = info.get('upload_date') or 'غير معروف'
                link = info.get('webpage_url') or f'https://www.youtube.com/watch?v={videoid}'
                channel = info.get('uploader') or 'غير معروف'
                thumbnail = info.get('thumbnail')
                text = f'**معلومات المقطع الصوتي**\n\n**العنوان:** {title}\n**المدة:** `{duration_text}`\n**عدد المشاهدات:** `{views}`\n**تاريخ النشر:** `{published}`\n**الرابط:** {link}\n**القناة:** {channel}\n\nتم البحث بواسطة {BOT_NAME}'
                await m.delete()
                if thumbnail:
                    return await app.send_photo(message.chat.id, photo=thumbnail, caption=text, reply_markup=InlineKeyboardMarkup(pm_buttons))
                return await app.send_message(message.chat.id, text=text, reply_markup=InlineKeyboardMarkup(pm_buttons), disable_web_page_preview=True)
            except Exception as exc:
                return await m.edit_text(f'فشل جلب المعلومات.\n\n`{exc}`')
    if message.chat.type == ChatType.PRIVATE:
        caption = PM_START_TEXT.format(message.from_user.first_name, BOT_MENTION)
        if config.START_IMG:
            return await message.reply_photo(photo=config.START_IMG, caption=caption, reply_markup=InlineKeyboardMarkup(pm_buttons))
        return await message.reply_text(caption, reply_markup=InlineKeyboardMarkup(pm_buttons))
    caption = START_TEXT.format(message.from_user.first_name, BOT_MENTION, message.chat.title, config.SUPPORT_CHAT)
    if config.START_IMG:
        return await message.reply_photo(photo=config.START_IMG, caption=caption, reply_markup=InlineKeyboardMarkup(gp_buttons))
    return await message.reply_text(caption, reply_markup=InlineKeyboardMarkup(gp_buttons))
bot_replies = ['نعم، تفضل.', 'هلا، شتريد؟', 'تفضل.', 'نعم؟']

@app.on_message(filters.text & filters.group)
async def group_text_handler(_, message: Message):
    text = message.text.strip().lower()
    if text in ['الاوامر', 'مساعدة', 'help']:
        await message.reply_text('اختر نوع الأوامر من الأزرار التالية.', reply_markup=InlineKeyboardMarkup(helpmenu))
        return
    if text == 'بوت':
        await message.reply_text(random.choice(bot_replies))

@app.on_chat_member_updated()
async def on_bot_promoted(client, event: ChatMemberUpdated):
    me = await client.get_me()
    if event.new_chat_member.user.id == me.id:
        if event.new_chat_member.status == ChatMemberStatus.ADMINISTRATOR:
            await client.send_message(event.chat.id, 'تم التفعيل تلقائياً')

# ===== MODULE: stop.py =====
from pyrogram import filters
from pyrogram.types import Message

@app.on_message(filters.text & filters.group)
@admin_check
async def stop_str(_, message: Message):
    text = message.text.lower()
    if text in ['stop', 'end', 'توقف', 'اسكت', 'انهاء']:
        try:
            await message.delete()
        except:
            pass
        try:
            await _clear_(message.chat.id)
            await pytgcalls.leave_group_call(message.chat.id)
        except:
            pass
        return await message.reply_text(text=f'‹ : **تم ايقاف الاغنية .**\n‹ : بواسطة : {message.from_user.mention} .', reply_markup=close_key)

# ===== MODULE: sudoers.py =====
from pyrogram import filters
from pyrogram.types import Message

@app.on_message(filters.command(['addsudo', 'رفع ادمن']) & filters.user(OWNER_ID))
async def sudoadd(_, message: Message):
    try:
        await message.delete()
    except:
        pass
    if not message.reply_to_message:
        if len(message.command) != 2:
            return await message.reply_text('‹ : وجه لي رسالة من العضو او ارسل لي اليوزر او الايدي .')
        user = message.text.split(None, 1)[1]
        if '@' in user:
            user = user.replace('@', '')
        user = await app.get_users(user)
        if int(user.id) in SUDOERS:
            return await message.reply_text(f'‹ : {user.mention} ما موجود بقائمة الادمنية .')
        try:
            SUDOERS.add(int(user.id))
            await message.reply_text(f'‹ : تم اضافة {user.mention} الى قائمة الادمنية .')
        except:
            return await message.reply_text('‹ : فشل اضافة العضو الى الادمنية .')
    if message.reply_to_message.from_user.id in SUDOERS:
        return await message.reply_text(f'‹ : {message.reply_to_message.from_user.mention} ما موجود بقائمة الادمنية .')
    try:
        SUDOERS.add(message.reply_to_message.from_user.id)
        await message.reply_text(f'‹ : تم اضافة {message.reply_to_message.from_user.mention} الى قائمة الادمنية .')
    except:
        return await message.reply_text('‹ : فشل اضافة العضو الى الادمنية .')

@app.on_message(filters.command(['delsudo', 'rmsudo', 'تنزيل ادمن']) & filters.user(OWNER_ID))
async def sudodel(_, message: Message):
    try:
        await message.delete()
    except:
        pass
    if not message.reply_to_message:
        if len(message.command) != 2:
            return await message.reply_text('‹ : وجه لي رسالة من العضو او ارسل لي اليوزر او الايدي .')
        user = message.text.split(None, 1)[1]
        if '@' in user:
            user = user.replace('@', '')
        user = await app.get_users(user)
        if int(user.id) not in SUDOERS:
            return await message.reply_text(f'‹ : {user.mention} ما موجود بقائمة الادمنية .')
        try:
            SUDOERS.remove(int(user.id))
            return await message.reply_text(f'‹ : تم حذف {user.mention} من قائمة الادمنية .')
        except:
            return await message.reply_text(f'‹ : فشل حذفة من قائمة الادمنية .')
    else:
        user_id = message.reply_to_message.from_user.id
        if int(user_id) not in SUDOERS:
            return await message.reply_text(f'‹ : {message.reply_to_message.from_user.mention} ما موجود بقائمة الادمنية .')
        try:
            SUDOERS.remove(int(user_id))
            return await message.reply_text(f'‹ : تم حذف {message.reply_to_message.from_user.mention} من قائمة الادمنية .')
        except:
            return await message.reply_text(f'‹ : فشل حذفة من قائمة الادمنية .')

@app.on_message(filters.command(['sudolist', 'sudoers', 'sudo', 'الادمنية']))
async def sudoers_list(_, message: Message):
    hehe = await message.reply_text('‹ : جار جلب قائمة الادمنية ..')
    text = '<u>**المالك**</u>\n'
    count = 0
    user = await app.get_users(OWNER_ID)
    user = user.first_name if not user.mention else user.mention
    count += 1
    text += f'{count}➤ {user}\n'
    smex = 0
    for user_id in SUDOERS:
        if user_id != OWNER_ID:
            try:
                user = await app.get_users(user_id)
                user = user.first_name if not user.mention else user.mention
                if smex == 0:
                    smex += 1
                    text += '\n<u> **الادمن :**</u>\n'
                count += 1
                text += f'{count}➤ {user}\n'
            except Exception:
                continue
    if not text:
        await message.reply_text('‹ : مالكيت ادمنية .')
    else:
        await hehe.edit_text(text)

# ===== MODULE: watcher.py =====
from pyrogram import filters
from pyrogram.types import Message
from pytgcalls.types import AudioPiped, HighQualityAudio, Update
welcome = 20
close = 30

@app.on_message(filters.video_chat_started, group=welcome)
@app.on_message(filters.video_chat_ended, group=close)
async def welcome(_, message: Message):
    try:
        await _clear_(message.chat.id)
        await pytgcalls.leave_group_call(message.chat.id)
    except:
        pass

@app.on_message(filters.left_chat_member)
async def ub_leave(_, message: Message):
    if message.left_chat_member.id == BOT_ID:
        try:
            await _clear_(message.chat.id)
            await pytgcalls.leave_group_call(message.chat.id)
        except:
            pass
        try:
            await app2.leave_chat(message.chat.id)
        except:
            pass

@pytgcalls.on_left()
@pytgcalls.on_kicked()
@pytgcalls.on_closed_voice_chat()
async def swr_handler(_, chat_id: int):
    try:
        await _clear_(chat_id)
    except:
        pass

@pytgcalls.on_stream_end()
async def on_stream_end(pytgcalls, update: Update):
    chat_id = update.chat_id
    get = fallendb.get(chat_id)
    if not get:
        try:
            await _clear_(chat_id)
            return await pytgcalls.leave_group_call(chat_id)
        except:
            return
    else:
        process = await app.send_message(chat_id=chat_id, text='‹ : جارٍ تحميل المقطع التالي من قائمة الانتظار ...')
        old_file = CURRENT_FILES.get(chat_id)
        title = get[0]['title']
        duration = get[0]['duration']
        file_path = get[0]['file_path']
        videoid = get[0]['videoid']
        req_by = get[0]['req']
        user_id = get[0]['user_id']
        get.pop(0)
        stream = AudioPiped(file_path, audio_parameters=HighQualityAudio())
        try:
            await pytgcalls.change_stream(chat_id, stream)
        except Exception:
            await _clear_(chat_id)
            return await pytgcalls.leave_group_call(chat_id)
        CURRENT_FILES[chat_id] = file_path
        cleanup_file(old_file)
        img = await gen_thumb(videoid, user_id)
        await process.delete()
        await app.send_photo(chat_id=chat_id, photo=img, caption=f'**‹ : بدء التشغيل**\n\n**‹ : الاسم :** [{title[:27]}](https://t.me/{BOT_USERNAME}?start=info_{videoid})\n**‹ : المدة :** `{duration}` دقيقة\n**‹ : بواسطة :** {req_by}', reply_markup=buttons)


async def run():
    await fallen_startup()
    LOGGER.info("KroMusic started")
    await idle()

if __name__ == "__main__":
    asyncio.run(run())
