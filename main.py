from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import signal
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import yt_dlp
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.errors import RPCError, SessionPasswordNeeded, UserNotParticipant
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, CallbackQuery

try:
    from pytgcalls import PyTgCalls, StreamType
    from pytgcalls.types import AudioPiped, HighQualityAudio, Update
    from pytgcalls.exceptions import NoActiveGroupCall, TelegramServerError, UnMuteNeeded
except Exception as exc:
    PyTgCalls = None
    StreamType = AudioPiped = HighQualityAudio = Update = None
    NoActiveGroupCall = TelegramServerError = UnMuteNeeded = Exception
    PYTGCALLS_IMPORT_ERROR = repr(exc)
else:
    PYTGCALLS_IMPORT_ERROR = None

BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
CACHE_DIR = BASE_DIR / "cache"
DB_PATH = BASE_DIR / os.getenv("DB_PATH", "kromusic.db")
for d in (DOWNLOADS_DIR, CACHE_DIR):
    d.mkdir(parents=True, exist_ok=True)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
SESSION = os.getenv("SESSION", "").strip()
SUDO_USERS = {int(x) for x in re.split(r"[ ,]+", os.getenv("SUDO_USERS", "").strip()) if x}
SUDO_USERS.add(OWNER_ID)

BOT_NAME = os.getenv("BOT_NAME", "KroMusic")
DEVELOPER_USERNAME = os.getenv("DEVELOPER_USERNAME", "krofullpower").lstrip("@")
DEVELOPER_CHANNEL = os.getenv("DEVELOPER_CHANNEL", "dlxfullpower").lstrip("@")
DURATION_LIMIT = int(os.getenv("DURATION_LIMIT", "90"))
MAX_AUDIO_MB = int(os.getenv("MAX_AUDIO_MB", "25"))
STALE_FILE_MINUTES = int(os.getenv("STALE_FILE_MINUTES", "30"))
LOGIN_TIMEOUT = int(os.getenv("LOGIN_TIMEOUT", "300"))
MAX_QUEUE_PER_CHAT = int(os.getenv("MAX_QUEUE_PER_CHAT", "100"))
PLAY_COOLDOWN = float(os.getenv("PLAY_COOLDOWN", "4"))
SEARCH_COOLDOWN = float(os.getenv("SEARCH_COOLDOWN", "5"))
SONG_COOLDOWN = float(os.getenv("SONG_COOLDOWN", "10"))
SEARCH_MAX_LEN = int(os.getenv("SEARCH_MAX_LEN", "80"))
DOWNLOAD_CONCURRENCY = max(1, int(os.getenv("DOWNLOAD_CONCURRENCY", "1")))
SEARCH_CONCURRENCY = max(1, int(os.getenv("SEARCH_CONCURRENCY", "2")))

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
)
LOGGER = logging.getLogger(BOT_NAME)

# Pyrogram binds internal futures/tasks to the event loop that exists when the
# Client is created. Keep one dedicated loop for the whole process so the
# Client is never created on one loop and started on another (Railway/Python
# 3.11 can otherwise raise "Future attached to a different loop").
MAIN_LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(MAIN_LOOP)

app = Client("KroMusic", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
app2: Client | None = None
pytgcalls = None
BOT_ID = 0
BOT_USERNAME = ""
BOT_MENTION = ""
ASS_ID = 0
ASS_USERNAME = ""
ASS_MENTION = ""
START_TIME = time.time()

DOWNLOAD_SEMAPHORE = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
SEARCH_SEMAPHORE = asyncio.Semaphore(SEARCH_CONCURRENCY)
LOGIN_STATE: dict[int, dict[str, Any]] = {}
CHAT_LOCKS: dict[int, asyncio.Lock] = {}
CURRENT_FILES: dict[int, str] = {}
NOW_PLAYING: dict[int, int] = {}
RATE_LIMITS: dict[tuple[int, str], float] = {}


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=20000")
    return conn


def db_init() -> None:
    conn = db_connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                url TEXT NOT NULL,
                video_id TEXT,
                title TEXT,
                duration INTEGER NOT NULL DEFAULT 0,
                requester TEXT,
                user_id INTEGER,
                position INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_queue_chat_status_position
            ON queue(chat_id, status, position);
            CREATE TABLE IF NOT EXISTS authorized_chats (
                chat_id INTEGER PRIMARY KEY,
                chat_type TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def db_get_setting(key: str) -> str:
    conn = db_connect()
    try:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else ""
    finally:
        conn.close()


def db_set_setting(key: str, value: str) -> None:
    conn = db_connect()
    try:
        conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


def db_delete_setting(key: str) -> None:
    conn = db_connect()
    try:
        conn.execute("DELETE FROM settings WHERE key=?", (key,))
        conn.commit()
    finally:
        conn.close()


def queue_count(chat_id: int) -> int:
    conn = db_connect()
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM queue WHERE chat_id=? AND status='queued'", (chat_id,)
        ).fetchone()
        return int(row["c"])
    finally:
        conn.close()


def queue_add(chat_id: int, url: str, video_id: str, title: str, duration: int, requester: str, user_id: int | None) -> int:
    conn = db_connect()
    try:
        row = conn.execute(
            "SELECT COALESCE(MAX(position), 0) + 1 AS p FROM queue WHERE chat_id=? AND status='queued'",
            (chat_id,),
        ).fetchone()
        position = int(row["p"])
        cur = conn.execute(
            "INSERT INTO queue(chat_id,url,video_id,title,duration,requester,user_id,position,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (chat_id, url, video_id, title, duration, requester, user_id, position, "queued", int(time.time())),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def queue_next(chat_id: int) -> sqlite3.Row | None:
    conn = db_connect()
    try:
        return conn.execute(
            "SELECT * FROM queue WHERE chat_id=? AND status='queued' ORDER BY position ASC, id ASC LIMIT 1",
            (chat_id,),
        ).fetchone()
    finally:
        conn.close()


def queue_mark_playing(row_id: int) -> None:
    conn = db_connect()
    try:
        conn.execute("UPDATE queue SET status='playing' WHERE id=?", (row_id,))
        conn.commit()
    finally:
        conn.close()


def queue_finish(row_id: int) -> None:
    conn = db_connect()
    try:
        conn.execute("DELETE FROM queue WHERE id=?", (row_id,))
        conn.commit()
    finally:
        conn.close()


def queue_clear(chat_id: int) -> None:
    conn = db_connect()
    try:
        conn.execute("DELETE FROM queue WHERE chat_id=?", (chat_id,))
        conn.commit()
    finally:
        conn.close()


def chat_lock(chat_id: int) -> asyncio.Lock:
    return CHAT_LOCKS.setdefault(chat_id, asyncio.Lock())


def cleanup_file(path: str | Path | None) -> None:
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
    except OSError as exc:
        LOGGER.warning("Could not remove temporary file: %s", type(exc).__name__)


def cleanup_temp_files(older_than_seconds: int = STALE_FILE_MINUTES * 60) -> int:
    removed = 0
    now = time.time()
    for directory in (DOWNLOADS_DIR, CACHE_DIR):
        for path in directory.iterdir():
            try:
                if not path.is_file():
                    continue
                if now - path.stat().st_mtime >= older_than_seconds:
                    path.unlink()
                    removed += 1
            except OSError:
                continue
    return removed


def cleanup_all_temp_files() -> None:
    for directory in (DOWNLOADS_DIR, CACHE_DIR):
        for path in directory.iterdir():
            try:
                if path.is_file() or path.is_symlink():
                    path.unlink()
                elif path.is_dir():
                    shutil.rmtree(path)
            except OSError:
                continue


def readable_time(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


def requester_name(message: Message) -> tuple[str, int | None]:
    user = message.from_user
    if user:
        name = user.mention or user.first_name or str(user.id)
        return name, user.id
    # Channel posts have no normal from_user. Do not invent an identity.
    return "منشور قناة", None


def is_admin_user(user_id: int | None) -> bool:
    return bool(user_id and user_id in SUDO_USERS)


async def bot_is_admin(chat_id: int) -> bool:
    try:
        member = await app.get_chat_member(chat_id, BOT_ID)
        return member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}
    except RPCError as exc:
        LOGGER.warning("Bot admin check failed: %s", type(exc).__name__)
        return False


async def assistant_is_admin(chat_id: int) -> bool:
    if not app2:
        return False
    try:
        member = await app2.get_chat_member(chat_id, "me")
        return member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}
    except UserNotParticipant:
        return False
    except RPCError as exc:
        LOGGER.warning("Assistant admin check failed: %s", type(exc).__name__)
        return False


async def authorize_chat(chat_id: int, chat_type: str) -> bool:
    conn = db_connect()
    try:
        row = conn.execute("SELECT enabled FROM authorized_chats WHERE chat_id=?", (chat_id,)).fetchone()
        if row:
            return bool(row["enabled"])
        # Existing behavior remains usable without an explicit allow-list.
        # An allow-list row can be added later for stricter deployments.
        return True
    finally:
        conn.close()


def save_session(session: str) -> None:
    db_set_setting("assistant_session", session)


def saved_session() -> str:
    return SESSION or db_get_setting("assistant_session")


def delete_session() -> None:
    db_delete_setting("assistant_session")


async def start_assistant(session: str | None = None) -> bool:
    global app2, pytgcalls, ASS_ID, ASS_USERNAME, ASS_MENTION
    session = session or saved_session()
    if not session:
        return False
    if app2:
        try:
            if app2.is_connected:
                return True
        except Exception:
            pass
    app2 = Client("KroAssistant", api_id=API_ID, api_hash=API_HASH, session_string=session)
    try:
        await app2.start()
        me = await app2.get_me()
        ASS_ID = me.id
        ASS_USERNAME = me.username or ""
        ASS_MENTION = me.mention
        if PyTgCalls is None:
            LOGGER.error("PyTgCalls is unavailable: %s", PYTGCALLS_IMPORT_ERROR)
            return True
        pytgcalls = PyTgCalls(app2)
        await pytgcalls.start()
        register_pytgcalls_handlers()
        return True
    except Exception:
        LOGGER.exception("Assistant startup failed")
        try:
            await app2.stop()
        except Exception:
            pass
        app2 = None
        pytgcalls = None
        return False


async def stop_assistant() -> None:
    global app2, pytgcalls, ASS_ID, ASS_USERNAME, ASS_MENTION
    if pytgcalls:
        try:
            await pytgcalls.stop()
        except Exception:
            pass
    if app2:
        try:
            await app2.stop()
        except Exception:
            pass
    app2 = None
    pytgcalls = None
    ASS_ID = 0
    ASS_USERNAME = ""
    ASS_MENTION = ""


YOUTUBE_CLIENTS = ("web_safari", "android_vr", "web_embedded", "tv", "default")


def ytdlp_base(client: str) -> dict[str, Any]:
    opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "retries": 2,
        "socket_timeout": 20,
        "extractor_args": {"youtube": {"player_client": [client]}},
    }
    cookiefile = Path(os.getenv("YTDLP_COOKIES_FILE", "cookies.txt"))
    if cookiefile.is_file():
        opts["cookiefile"] = str(cookiefile)
    return opts


def youtube_info(url: str) -> dict[str, Any]:
    last: Exception | None = None
    for client in YOUTUBE_CLIENTS:
        try:
            opts = ytdlp_base(client)
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(url, download=False)
        except Exception as exc:
            last = exc
            LOGGER.warning("YouTube metadata failed client=%s error=%s", client, str(exc)[:250])
    raise last or RuntimeError("YouTube metadata unavailable")


def youtube_search(query: str, limit: int = 4) -> list[dict[str, Any]]:
    last: Exception | None = None
    for client in YOUTUBE_CLIENTS:
        try:
            opts = ytdlp_base(client)
            opts["extract_flat"] = True
            with yt_dlp.YoutubeDL(opts) as ydl:
                data = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
            entries = [x for x in (data.get("entries") or []) if x]
            if entries:
                return entries[:limit]
        except Exception as exc:
            last = exc
            LOGGER.warning("YouTube search failed client=%s error=%s", client, str(exc)[:250])
    raise last or RuntimeError("YouTube search unavailable")


def choose_audio_format() -> str:
    # A moderate bitrate is enough for voice-chat music and avoids unnecessarily
    # large temporary files. yt-dlp will fall back if the exact filter is absent.
    return "bestaudio[abr<=160]/bestaudio/best"


def download_audio(url: str) -> str:
    last: Exception | None = None
    unique = uuid.uuid4().hex
    attempts = ("web_safari", "android_vr", "web_embedded", "tv", "default")
    for client in attempts:
        try:
            opts = ytdlp_base(client)
            opts.update(
                {
                    "format": choose_audio_format(),
                    "outtmpl": str(DOWNLOADS_DIR / f"{unique}.%(ext)s"),
                    "overwrites": True,
                }
            )
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.extract_info(url, download=True)
            files = list(DOWNLOADS_DIR.glob(f"{unique}.*"))
            if not files:
                raise FileNotFoundError("Downloaded file was not found")
            path = files[0]
            if path.stat().st_size > MAX_AUDIO_MB * 1024 * 1024:
                cleanup_file(path)
                raise RuntimeError(f"الملف أكبر من الحد المسموح ({MAX_AUDIO_MB} MB).")
            return str(path)
        except Exception as exc:
            last = exc
            for p in DOWNLOADS_DIR.glob(f"{unique}.*"):
                cleanup_file(p)
            LOGGER.warning("YouTube download failed client=%s error=%s", client, str(exc)[:300])
    raise last or RuntimeError("Audio download failed")


async def resolve_query(query: str) -> tuple[str, str, str, int]:
    if re.match(r"^https?://", query, re.I):
        url = query
    else:
        async with SEARCH_SEMAPHORE:
            results = await asyncio.to_thread(youtube_search, query, 1)
        if not results:
            raise RuntimeError("لم يتم العثور على نتيجة.")
        item = results[0]
        url = item.get("webpage_url") or f"https://www.youtube.com/watch?v={item.get('id')}"
    async with SEARCH_SEMAPHORE:
        info = await asyncio.to_thread(youtube_info, url)
    title = info.get("title") or "بدون عنوان"
    video_id = info.get("id") or ""
    duration = int(info.get("duration") or 0)
    if duration > DURATION_LIMIT * 60:
        raise RuntimeError(f"المقطع يتجاوز الحد المسموح وهو {DURATION_LIMIT} دقيقة.")
    return url, title, video_id, duration


async def download_one(url: str) -> str:
    async with DOWNLOAD_SEMAPHORE:
        return await asyncio.to_thread(download_audio, url)


def format_error(exc: Exception) -> str:
    text = str(exc)
    low = text.lower()
    if "requested format is not available" in low:
        return "يوتيوب لم يعرض صيغة صوت مناسبة لهذا الاتصال."
    if "sign in" in low or "not a bot" in low or "cookies" in low:
        return "يوتيوب رفض طلب خادم Railway. قد تحتاج جلسة cookies أو معالجة PO Token حسب حالة الخادم."
    return text[:700]


def control_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("استكمال", callback_data="resume_cb"), InlineKeyboardButton("تخطي", callback_data="skip_cb"), InlineKeyboardButton("إيقاف", callback_data="pause_cb")],
            [InlineKeyboardButton("إنهاء", callback_data="end_cb")],
        ]
    )


def support_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("إغلاق", callback_data="close")]]
    if DEVELOPER_CHANNEL:
        rows.insert(0, [InlineKeyboardButton("قناة المطور", url=f"https://t.me/{DEVELOPER_CHANNEL}")])
    return InlineKeyboardMarkup(rows)


def now_playing_text(row: sqlite3.Row) -> str:
    return (
        "KroMusic\n\n"
        f"العنوان: {row['title'] or 'بدون عنوان'}\n"
        f"المدة: `{readable_time(row['duration'])}`\n"
        f"بواسطة: {row['requester'] or 'منشور قناة'}"
    )


async def update_now_playing(chat_id: int, row: sqlite3.Row) -> None:
    text = now_playing_text(row)
    old_id = NOW_PLAYING.get(chat_id)
    if old_id:
        try:
            await app.edit_message_text(chat_id, old_id, text, reply_markup=control_keyboard())
            return
        except RPCError:
            NOW_PLAYING.pop(chat_id, None)
    try:
        msg = await app.send_message(chat_id, text, reply_markup=control_keyboard())
        NOW_PLAYING[chat_id] = msg.id
    except RPCError as exc:
        LOGGER.warning("Now playing message failed: %s", type(exc).__name__)


async def clear_now_playing(chat_id: int) -> None:
    msg_id = NOW_PLAYING.pop(chat_id, None)
    if msg_id:
        try:
            await app.delete_messages(chat_id, msg_id)
        except RPCError:
            pass


async def leave_and_clear(chat_id: int) -> None:
    old_file = CURRENT_FILES.pop(chat_id, None)
    cleanup_file(old_file)
    queue_clear(chat_id)
    await clear_now_playing(chat_id)
    if pytgcalls:
        try:
            await pytgcalls.leave_group_call(chat_id)
        except Exception:
            pass


async def start_next(chat_id: int) -> bool:
    """Download and start the next queued item without recursive retries.

    A single per-chat lock prevents concurrent stream transitions. Failed items
    are marked finished and the loop continues, avoiding recursion/deadlocks.
    """
    if not pytgcalls or not app2:
        return False

    lock = chat_lock(chat_id)
    async with lock:
        while True:
            row = queue_next(chat_id)
            if not row:
                await leave_and_clear(chat_id)
                return False

            row_id = int(row["id"])
            queue_mark_playing(row_id)
            file_path: str | None = None

            try:
                file_path = await download_one(row["url"])
                stream = AudioPiped(file_path, audio_parameters=HighQualityAudio())
                had_stream = chat_id in CURRENT_FILES

                try:
                    if had_stream:
                        await pytgcalls.change_stream(chat_id, stream)
                    else:
                        await pytgcalls.join_group_call(
                            chat_id, stream, stream_type=StreamType.pulse_stream
                        )
                except (NoActiveGroupCall, TelegramServerError, UnMuteNeeded) as exc:
                    queue_finish(row_id)
                    cleanup_file(file_path)
                    await clear_now_playing(chat_id)
                    LOGGER.warning("Voice call failed: %s", type(exc).__name__)
                    try:
                        await app.send_message(
                            chat_id,
                            "تعذر تشغيل المكالمة. تأكد من فتح Voice Chat أو Live Stream ومن صلاحيات المساعد.",
                        )
                    except Exception:
                        pass
                    return False

                old = CURRENT_FILES.get(chat_id)
                CURRENT_FILES[chat_id] = file_path
                if old and old != file_path:
                    cleanup_file(old)
                await update_now_playing(chat_id, row)
                return True

            except Exception as exc:
                queue_finish(row_id)
                cleanup_file(file_path)
                LOGGER.exception("start_next failed: %s", type(exc).__name__)
                try:
                    await app.send_message(
                        chat_id,
                        f"فشل تشغيل المقطع.\n\nالخطأ: `{format_error(exc)}`",
                    )
                except Exception:
                    pass
                # Continue to the next queued item instead of recursively calling
                # start_next while its own lock is still held.
                continue


async def ensure_call_permissions(message: Message) -> tuple[bool, str]:
    if not app2 or not pytgcalls:
        return False, "حساب المساعد غير متصل."
    if not await bot_is_admin(message.chat.id):
        return False, "يجب أن يكون البوت مشرفًا في هذه المحادثة."
    if not await assistant_is_admin(message.chat.id):
        return False, f"أضف حساب المساعد {ASS_MENTION or ASS_USERNAME or 'المساعد'} كمشرف ثم أعد المحاولة."
    return True, ""


@app.on_message(filters.command("start") & ~filters.forwarded)
async def start_handler(_, message: Message):
    await message.reply_text(
        f"أهلًا {message.from_user.mention if message.from_user else 'بك'}.\n\n"
        f"أنا {BOT_NAME}. أرسل اسم المقطع أو رابطه لتشغيله.\n\n"
        f"قناة المطور: @{DEVELOPER_CHANNEL}",
        reply_markup=support_keyboard(),
    )


@app.on_message(filters.command("ping"))
async def ping_handler(_, message: Message):
    started = time.perf_counter()
    msg = await message.reply_text("جارِ الفحص...")
    latency = (time.perf_counter() - started) * 1000
    await msg.edit_text(f"{BOT_NAME}\n\nزمن الاستجابة: `{latency:.0f} ms`\nمدة التشغيل: `{readable_time(int(time.time() - START_TIME))}`")


PLAY_REGEX = r"^(play|vplay|p|شغل|تشغيل)(?:\s+(.+))?$"


@app.on_message((filters.group | filters.channel) & filters.regex(PLAY_REGEX, re.I))
async def play_handler(_, message: Message):
    chat_id = message.chat.id
    if not await authorize_chat(chat_id, str(message.chat.type)):
        return
    requester, user_id = requester_name(message)
    if user_id and not is_admin_user(user_id):
        key = (user_id, "play")
        now = time.monotonic()
        if now - RATE_LIMITS.get(key, 0) < PLAY_COOLDOWN:
            return await message.reply_text("تمهل قليلًا ثم أرسل الطلب مرة أخرى.")
        RATE_LIMITS[key] = now
    ok, reason = await ensure_call_permissions(message)
    if not ok:
        return await message.reply_text(reason)
    if queue_count(chat_id) >= MAX_QUEUE_PER_CHAT:
        return await message.reply_text(f"قائمة الانتظار ممتلئة. الحد الحالي {MAX_QUEUE_PER_CHAT} مقطع.")
    query = (message.matches[0].group(2) or "").strip() if message.matches else ""
    if not query and message.reply_to_message:
        query = (message.reply_to_message.text or message.reply_to_message.caption or "").strip()
    if not query:
        return await message.reply_text("أرسل اسم المقطع أو رابطه بعد الأمر.")
    status = await message.reply_text("جاري تجهيز الطلب...")
    try:
        url, title, video_id, duration = await resolve_query(query)
        queue_id = queue_add(chat_id, url, video_id, title, duration, requester, user_id)
        current = CURRENT_FILES.get(chat_id)
        if current:
            pos = queue_count(chat_id)
            await status.edit_text(f"تمت إضافة المقطع إلى قائمة الانتظار.\nالترتيب: `{pos}`")
        else:
            await status.delete()
            await start_next(chat_id)
    except Exception as exc:
        await status.edit_text(f"فشل تجهيز المقطع.\n\nالخطأ: `{format_error(exc)}`")


async def is_admin_for_command(message: Message) -> bool:
    if message.from_user and message.from_user.id in SUDO_USERS:
        return True
    # For channel posts there is no reliable actor identity. Authorization is
    # based on bot/assistant administrator status for the configured chat.
    if message.chat.type == ChatType.CHANNEL:
        return await bot_is_admin(message.chat.id) and await assistant_is_admin(message.chat.id)
    if not message.from_user:
        return False
    try:
        member = await app.get_chat_member(message.chat.id, message.from_user.id)
        return member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}
    except RPCError as exc:
        LOGGER.warning("Command admin check failed: %s", type(exc).__name__)
        return False


async def handle_control(message: Message, action: str) -> None:
    if not await is_admin_for_command(message):
        return await message.reply_text("هذا الأمر متاح للمشرفين فقط.")
    chat_id = message.chat.id
    if not pytgcalls:
        return await message.reply_text("حساب المساعد غير متصل.")
    try:
        if action == "pause":
            await pytgcalls.pause_stream(chat_id)
            return await message.reply_text("تم إيقاف التشغيل مؤقتًا.")
        if action == "resume":
            await pytgcalls.resume_stream(chat_id)
            return await message.reply_text("تم استكمال التشغيل.")
        if action == "skip":
            # Keep the current stream alive while the next item is downloaded.
            # start_next() will use change_stream() and then remove the old file.
            conn = db_connect()
            try:
                conn.execute("DELETE FROM queue WHERE chat_id=? AND status='playing'", (chat_id,))
                conn.commit()
            finally:
                conn.close()
            await start_next(chat_id)
            return
        if action == "stop":
            await leave_and_clear(chat_id)
            return await message.reply_text("تم إنهاء التشغيل ومسح قائمة الانتظار.")
    except Exception as exc:
        LOGGER.exception("Control action failed: %s", type(exc).__name__)
        await message.reply_text(f"تعذر تنفيذ الأمر.\n\n`{format_error(exc)}`")


@app.on_message((filters.group | filters.channel) & filters.regex(r"^(pause|اوكف)$", re.I))
async def pause_handler(_, message: Message):
    await handle_control(message, "pause")


@app.on_message((filters.group | filters.channel) & filters.regex(r"^(resume|كمل)$", re.I))
async def resume_handler(_, message: Message):
    await handle_control(message, "resume")


@app.on_message((filters.group | filters.channel) & filters.regex(r"^(skip|next|تخطي)$", re.I))
async def skip_handler(_, message: Message):
    await handle_control(message, "skip")


@app.on_message((filters.group | filters.channel) & filters.regex(r"^(stop|end|توقف|اسكت|انهاء)$", re.I))
async def stop_handler(_, message: Message):
    await handle_control(message, "stop")


@app.on_message(filters.command("song"))
async def song_handler(_, message: Message):
    if not message.from_user:
        return await message.reply_text("هذا الأمر يجب إرساله من حساب مستخدم أو من محادثة خاصة.")
    user_id = message.from_user.id
    now = time.monotonic()
    key = (user_id, "song")
    if user_id not in SUDO_USERS and now - RATE_LIMITS.get(key, 0) < SONG_COOLDOWN:
        return await message.reply_text("تمهل قليلًا ثم أرسل الطلب مرة أخرى.")
    RATE_LIMITS[key] = now
    query = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else ""
    if not query:
        return await message.reply_text("اكتب اسم الأغنية بعد الأمر.")
    status = await message.reply_text("جاري تجهيز الملف...")
    path = None
    try:
        url, title, video_id, duration = await resolve_query(query)
        path = await download_one(url)
        await app.send_audio(
            message.from_user.id,
            path,
            title=title,
            duration=duration,
            caption=f"العنوان: {title}\nالمدة: `{readable_time(duration)}`\nالرابط: {url}",
        )
        await status.delete()
    except Exception as exc:
        await status.edit_text(f"فشل إرسال المقطع.\n\nالخطأ: `{format_error(exc)}`")
    finally:
        cleanup_file(path)


@app.on_message(filters.command("بحث"))
async def search_handler(_, message: Message):
    if not message.from_user:
        return
    query = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else ""
    if not query:
        return await message.reply_text("اكتب كلمة البحث.")
    query = query[:SEARCH_MAX_LEN]
    now = time.monotonic()
    key = (message.from_user.id, "search")
    if message.from_user.id not in SUDO_USERS and now - RATE_LIMITS.get(key, 0) < SEARCH_COOLDOWN:
        return await message.reply_text("تمهل قليلًا ثم أعد البحث.")
    RATE_LIMITS[key] = now
    status = await message.reply_text("جاري البحث...")
    try:
        async with SEARCH_SEMAPHORE:
            results = await asyncio.to_thread(youtube_search, query, 4)
        if not results:
            return await status.edit_text("لم يتم العثور على نتائج.")
        lines = []
        for i, item in enumerate(results, 1):
            title = item.get("title") or "بدون عنوان"
            duration = item.get("duration_string") or item.get("duration") or "غير معروف"
            url = item.get("webpage_url") or f"https://www.youtube.com/watch?v={item.get('id')}"
            lines.append(f"{i}. {title}\nالمدة: `{duration}`\nالرابط: {url}")
        await status.edit_text("\n\n".join(lines), disable_web_page_preview=True)
    except Exception as exc:
        await status.edit_text(f"فشل البحث.\n\nالخطأ: `{format_error(exc)}`")


@app.on_callback_query(filters.regex(r"^(pause_cb|resume_cb|skip_cb|end_cb|close)$"))
async def callback_controls(_, query: CallbackQuery):
    if query.data == "close":
        await query.answer()
        try:
            await query.message.delete()
        except RPCError:
            pass
        return
    if not query.message:
        return await query.answer()
    mapping = {"pause_cb": "pause", "resume_cb": "resume", "skip_cb": "skip", "end_cb": "stop"}
    action = mapping[query.data]
    # Callback has a real actor, so the normal admin check can be used.
    fake = query.message
    fake.from_user = query.from_user
    if not await is_admin_for_command(fake):
        return await query.answer("هذا الخيار للمشرفين فقط.", show_alert=True)
    await query.answer()
    await handle_control(fake, action)


@app.on_message(filters.command("المساعد") & filters.user(OWNER_ID))
async def assistant_panel(_, message: Message):
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("إضافة حساب المساعد", callback_data="assistant_add")],
            [InlineKeyboardButton("حالة المساعد", callback_data="assistant_status")],
            [InlineKeyboardButton("حذف جلسة المساعد", callback_data="assistant_delete")],
        ]
    )
    await message.reply_text("إدارة حساب المساعد:", reply_markup=keyboard)


@app.on_callback_query(filters.regex(r"^assistant_(add|status|delete)$"))
async def assistant_callbacks(_, query: CallbackQuery):
    if query.from_user.id != OWNER_ID:
        return await query.answer("هذا الخيار للمالك فقط.", show_alert=True)
    action = query.data.split("_", 1)[1]
    if action == "status":
        text = f"المساعد: متصل\nالحساب: {ASS_MENTION or 'غير مسجل'}" if app2 else "المساعد: غير متصل"
        return await query.message.edit_text(text)
    if action == "delete":
        LOGIN_STATE.pop(OWNER_ID, None)
        await stop_assistant()
        if not SESSION:
            delete_session()
        return await query.message.edit_text("تم حذف جلسة المساعد.")
    LOGIN_STATE[OWNER_ID] = {"step": "phone", "created": time.monotonic()}
    await query.message.edit_text("أرسل رقم الهاتف بصيغة دولية.\n\nلإلغاء العملية: /cancel")


@app.on_message(filters.command("cancel") & filters.user(OWNER_ID))
async def cancel_login(_, message: Message):
    state = LOGIN_STATE.pop(OWNER_ID, None)
    if state and state.get("client"):
        try:
            await state["client"].disconnect()
        except Exception:
            pass
    await message.reply_text("تم إلغاء تسجيل الدخول.")


@app.on_message(filters.text & filters.user(OWNER_ID), group=50)
async def login_flow(_, message: Message):
    state = LOGIN_STATE.get(OWNER_ID)
    if not state or message.text.startswith("/"):
        return
    if time.monotonic() - state.get("created", time.monotonic()) > LOGIN_TIMEOUT:
        LOGIN_STATE.pop(OWNER_ID, None)
        client = state.get("client")
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        return await message.reply_text("انتهت مهلة تسجيل الدخول. ابدأ العملية من /المساعد.")
    try:
        if state["step"] == "phone":
            phone = message.text.strip().replace(" ", "")
            if not re.fullmatch(r"\+?[0-9]{7,15}", phone):
                return await message.reply_text("رقم الهاتف غير صحيح.")
            client = Client(f"KroLogin_{OWNER_ID}", api_id=API_ID, api_hash=API_HASH, in_memory=True)
            await client.connect()
            sent = await client.send_code(phone)
            state.update({"step": "code", "client": client, "phone": phone, "hash": sent.phone_code_hash})
            return await message.reply_text("تم إرسال رمز التحقق. أرسله الآن.")
        if state["step"] == "code":
            code = re.sub(r"\D", "", message.text)
            try:
                await state["client"].sign_in(state["phone"], state["hash"], code)
            except SessionPasswordNeeded:
                state["step"] = "password"
                return await message.reply_text("أرسل كلمة مرور المصادقة الثنائية.")
            return await finish_login(message, state)
        if state["step"] == "password":
            await state["client"].check_password(message.text)
            return await finish_login(message, state)
    except Exception as exc:
        LOGGER.exception("Login failed: %s", type(exc).__name__)
        client = state.get("client")
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
        LOGIN_STATE.pop(OWNER_ID, None)
        await message.reply_text(f"فشل تسجيل الدخول.\n\n`{type(exc).__name__}`")


async def finish_login(message: Message, state: dict[str, Any]) -> None:
    client = state["client"]
    session = await client.export_session_string()
    await client.disconnect()
    LOGIN_STATE.pop(OWNER_ID, None)
    save_session(session)
    if await start_assistant(session):
        await message.reply_text(f"تم تسجيل حساب المساعد بنجاح.\nالحساب: {ASS_MENTION or ASS_USERNAME or ASS_ID}")
    else:
        delete_session()
        await message.reply_text("تم إنشاء الجلسة لكن تعذر تشغيل حساب المساعد.")


def register_pytgcalls_handlers() -> None:
    if not pytgcalls:
        return
    try:
        pytgcalls.on_stream_end()(on_stream_end)
        pytgcalls.on_left()(on_call_closed)
        pytgcalls.on_kicked()(on_call_closed)
        pytgcalls.on_closed_voice_chat()(on_call_closed)
    except Exception as exc:
        LOGGER.warning("Could not register PyTgCalls handlers: %s", type(exc).__name__)


async def on_stream_end(_, update: Update) -> None:
    chat_id = update.chat_id
    old = CURRENT_FILES.pop(chat_id, None)
    cleanup_file(old)
    conn = db_connect()
    try:
        conn.execute("DELETE FROM queue WHERE chat_id=? AND status='playing'", (chat_id,))
        conn.commit()
    finally:
        conn.close()
    await start_next(chat_id)


async def on_call_closed(_, update: Any) -> None:
    chat_id = getattr(update, "chat_id", None)
    if chat_id is None:
        return
    old = CURRENT_FILES.pop(chat_id, None)
    cleanup_file(old)
    # Keep queued URLs so a transient call close does not erase the user's list.
    await clear_now_playing(chat_id)


async def startup() -> None:
    global BOT_ID, BOT_USERNAME, BOT_MENTION
    db_init()
    removed = cleanup_temp_files()
    LOGGER.info("Startup cleanup removed %s stale files", removed)
    await app.start()
    me = await app.get_me()
    BOT_ID = me.id
    BOT_USERNAME = me.username or ""
    BOT_MENTION = me.mention
    session = saved_session()
    if session:
        await start_assistant(session)
    LOGGER.info("%s started as @%s", BOT_NAME, BOT_USERNAME or "-")


async def shutdown() -> None:
    LOGGER.info("Shutting down...")
    for state in list(LOGIN_STATE.values()):
        client = state.get("client")
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass
    LOGIN_STATE.clear()
    for chat_id in list(CURRENT_FILES):
        old = CURRENT_FILES.pop(chat_id, None)
        cleanup_file(old)
    await stop_assistant()
    try:
        await app.stop()
    except Exception:
        pass
    cleanup_all_temp_files()


async def main() -> None:
    await startup()
    try:
        await idle()
    finally:
        await shutdown()


if __name__ == "__main__":
    try:
        MAIN_LOOP.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        try:
            MAIN_LOOP.run_until_complete(asyncio.sleep(0))
        except Exception:
            pass
        asyncio.set_event_loop(None)
        MAIN_LOOP.close()
