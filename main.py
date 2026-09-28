from __future__ import annotations

import asyncio
import logging
import os
import random
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
from pyrogram.errors import (
    RPCError,
    SessionPasswordNeeded,
    UserNotParticipant,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    PhoneNumberInvalid,
)
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

try:
    from pytgcalls.types import AudioVideoPiped, HighQualityVideo, LowQualityVideo, MediumQualityVideo
except Exception as _video_exc:  # noqa: BLE001
    AudioVideoPiped = HighQualityVideo = LowQualityVideo = MediumQualityVideo = None
    VIDEO_IMPORT_ERROR = repr(_video_exc)
else:
    VIDEO_IMPORT_ERROR = None

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
# Chats where the assistant is currently joined to the voice chat. Tracked
# separately from CURRENT_FILES: the file entry is removed when a track ends
# while the assistant is still in the call, which used to make the bot try to
# "join" again and fail with "Already joined into group call".
IN_CALL: set[int] = set()
_BG_TASKS: set[asyncio.Future] = set()

# Auto-clean settings (seconds). Set AUTO_DELETE_COMMANDS=0 to keep user commands.
TEMP_MESSAGE_SECONDS = int(os.getenv("TEMP_MESSAGE_SECONDS", "10"))
ERROR_MESSAGE_SECONDS = int(os.getenv("ERROR_MESSAGE_SECONDS", "20"))
SEARCH_RESULT_SECONDS = int(os.getenv("SEARCH_RESULT_SECONDS", "90"))
AUTO_DELETE_COMMANDS = os.getenv("AUTO_DELETE_COMMANDS", "1").strip().lower() not in {"0", "false", "no"}
JANITOR_INTERVAL = int(os.getenv("JANITOR_INTERVAL", "600"))


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


def queue_requeue(row_id: int) -> None:
    conn = db_connect()
    try:
        conn.execute("UPDATE queue SET status='queued' WHERE id=?", (row_id,))
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
        p = Path(path)
        p.unlink(missing_ok=True)
        if p.suffix != ".mkv":
            # merged audio+video file created for the owner's looping video
            p.with_suffix(".vid.mkv").unlink(missing_ok=True)
    except OSError as exc:
        LOGGER.warning("Could not remove temporary file: %s", type(exc).__name__)


def cleanup_temp_files(older_than_seconds: int = STALE_FILE_MINUTES * 60, protected: set[Path] | None = None) -> int:
    removed = 0
    now = time.time()
    protected = protected or set()
    for directory in (DOWNLOADS_DIR, CACHE_DIR):
        for path in directory.iterdir():
            try:
                if not path.is_file():
                    continue
                if path.resolve() in protected:
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


def spawn(coro) -> asyncio.Future:
    """Run a coroutine in the background and keep a reference until it ends."""
    task = asyncio.ensure_future(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task


async def _delete_after(chat_id: int, msg_id: int, delay: float) -> None:
    await asyncio.sleep(delay)
    try:
        await app.delete_messages(chat_id, msg_id)
    except Exception:  # noqa: BLE001 - already deleted / no permission
        pass


def _is_private(message: Message) -> bool:
    return message.chat.type == ChatType.PRIVATE


def schedule_delete(msg: Message | None, delay: float) -> None:
    if msg is None or getattr(msg, "chat", None) is None or _is_private(msg):
        return
    spawn(_delete_after(msg.chat.id, msg.id, delay))


def delete_command(message: Message, delay: float = 1.0) -> None:
    """Remove the user's command message (needs the bot's delete-messages right)."""
    if AUTO_DELETE_COMMANDS:
        schedule_delete(message, delay)


async def reply_temp(message: Message, text: str, delay: float | None = None, **kwargs) -> Message:
    """Reply, then auto-delete the reply in groups/channels (private chats untouched)."""
    msg = await message.reply_text(text, **kwargs)
    schedule_delete(msg, delay if delay is not None else TEMP_MESSAGE_SECONDS)
    return msg


async def send_temp(chat_id: int, text: str, delay: float = TEMP_MESSAGE_SECONDS) -> None:
    try:
        msg = await app.send_message(chat_id, text)
        spawn(_delete_after(chat_id, msg.id, delay))
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Temp message failed: %s", type(exc).__name__)


def _is_already_joined(exc: Exception) -> bool:
    return "already joined" in str(exc).lower() or type(exc).__name__ == "AlreadyJoinedError"


async def janitor() -> None:
    """Periodic housekeeping so nothing piles up on disk or in memory."""
    while True:
        await asyncio.sleep(JANITOR_INTERVAL)
        try:
            protected = set()
            for f in CURRENT_FILES.values():
                try:
                    protected.add(Path(f).resolve())
                    protected.add(Path(f).with_suffix(".vid.mkv").resolve())
                except OSError:
                    pass
            removed = cleanup_temp_files(protected=protected)
            now = time.monotonic()
            for k in [k for k, v in RATE_LIMITS.items() if now - v > 3600]:
                RATE_LIMITS.pop(k, None)
            if removed:
                LOGGER.info("Janitor removed %s stale files", removed)
        except Exception:  # noqa: BLE001
            LOGGER.exception("Janitor failed")


async def purge_stale_now_playing() -> None:
    """Delete now-playing panels left over from a previous run (their buttons are dead)."""
    conn = db_connect()
    try:
        rows = conn.execute("SELECT key, value FROM settings WHERE key LIKE 'np:%'").fetchall()
    finally:
        conn.close()
    for row in rows:
        try:
            chat_id = int(row["key"].split(":", 1)[1])
            await app.delete_messages(chat_id, int(row["value"]))
        except Exception:  # noqa: BLE001
            pass
        db_delete_setting(row["key"])


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
            try:
                await app2.stop()
            except Exception:
                pass
            app2 = None
            return False
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


# YouTube extraction: the "mweb" client alone often returns NO audio formats
# (SABR / PO Token restrictions), which produces "Requested format is not
# available". We now try a short, ordered list of clients and only move to the
# next one when the current one fails with a format/availability error.
# Cookies are never used.
YTDLP_POT_URL = os.getenv("YTDLP_POT_URL", "http://127.0.0.1:4416").strip()
# Optional: residential proxy, e.g. http://user:pass@host:port
YTDLP_PROXY = os.getenv("YTDLP_PROXY", "").strip()
# Optional: several proxies separated by comma or newline; one is picked at random per request.
YTDLP_PROXIES = [x.strip() for x in re.split(r"[,\n]", os.getenv("YTDLP_PROXIES", "")) if x.strip()]
if YTDLP_PROXY and YTDLP_PROXY not in YTDLP_PROXIES:
    YTDLP_PROXIES.append(YTDLP_PROXY)
# Optional: Netscape cookies.txt exported from a THROWAWAY Google account.
YTDLP_COOKIES_FILE = os.getenv("YTDLP_COOKIES_FILE", "").strip()
# Optional: the same cookies.txt content encoded as base64 (easiest on Railway).
_cookies_b64 = os.getenv("YTDLP_COOKIES_B64", "").strip()
if _cookies_b64 and not YTDLP_COOKIES_FILE:
    try:
        import base64
        _cp = "/tmp/yt_cookies.txt"
        with open(_cp, "wb") as _f:
            _f.write(base64.b64decode(_cookies_b64))
        YTDLP_COOKIES_FILE = _cp
    except Exception as _e:  # noqa: BLE001
        LOGGER.warning("Invalid YTDLP_COOKIES_B64: %s", _e)
YOUTUBE_CLIENT_PROFILES: list[list[str]] = [
    ["default"],      # yt-dlp's own recommended clients
    ["android_vr"],   # does not need a PO Token for audio
    ["tv"],
    ["mweb"],         # last resort (needs the bgutil POT provider)
]
AUDIO_FORMAT = "bestaudio[ext=m4a]/bestaudio/ba*/best"


def ytdlp_base(clients: list[str] | None = None) -> dict[str, Any]:
    """Return a cookie-free yt-dlp profile for Railway."""
    clients = clients or YOUTUBE_CLIENT_PROFILES[0]
    opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "retries": 2,
        "fragment_retries": 2,
        "socket_timeout": 20,
        "noprogress": True,
        "js_runtimes": {"node": {}},
        "extractor_args": {
            "youtube": {"player_client": clients},
            "youtubepot-bgutilhttp": {"base_url": YTDLP_POT_URL},
        },
    }
    if YTDLP_PROXIES:
        opts["proxy"] = random.choice(YTDLP_PROXIES)
    if YTDLP_COOKIES_FILE and os.path.isfile(YTDLP_COOKIES_FILE):
        opts["cookiefile"] = YTDLP_COOKIES_FILE
    return opts


def _retryable_youtube_error(exc: Exception) -> bool:
    low = str(exc).lower()
    return any(
        k in low
        for k in ("requested format is not available", "no video formats found")
    )


def _is_bot_check(exc: Exception) -> bool:
    low = str(exc).lower()
    return "not a bot" in low or "sign in" in low or "403" in low


def _with_clients(fn):
    """Try each proxy (if several are configured), and inside it each client profile."""
    attempts = min(3, len(YTDLP_PROXIES)) if len(YTDLP_PROXIES) > 1 else 1
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            return _with_clients_once(fn)
        except Exception as exc:  # noqa: BLE001
            last = exc
            if not _is_bot_check(exc):
                raise
            if attempt + 1 < attempts:
                LOGGER.warning("Bot check hit; retrying with another proxy")
            else:
                LOGGER.warning("Bot check hit (YouTube blocked this IP)")
    assert last is not None
    raise last


def _with_clients_once(fn):
    """Run fn(clients) across the client profiles until one succeeds."""
    last_exc: Exception | None = None
    for clients in YOUTUBE_CLIENT_PROFILES:
        try:
            return fn(clients)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            LOGGER.warning("yt-dlp failed with clients=%s: %s", clients, str(exc)[:200])
            if not _retryable_youtube_error(exc):
                raise
    assert last_exc is not None
    raise last_exc


def youtube_info(url: str) -> dict[str, Any]:
    def run(clients: list[str]) -> dict[str, Any]:
        opts = ytdlp_base(clients)
        opts["format"] = AUDIO_FORMAT
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    return _with_clients(run)


def youtube_search(query: str, limit: int = 4) -> list[dict[str, Any]]:
    opts = ytdlp_base()
    opts["extract_flat"] = True
    with yt_dlp.YoutubeDL(opts) as ydl:
        data = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
    entries = [x for x in (data.get("entries") or []) if x]
    if not entries:
        raise RuntimeError("YouTube search returned no results")
    return entries[:limit]


def download_audio(url: str) -> str:
    unique = uuid.uuid4().hex

    def run(clients: list[str]) -> None:
        if is_soundcloud_url(url):
            opts = {"quiet": True, "no_warnings": True, "noprogress": True, "noplaylist": True, "retries": 2, "socket_timeout": 20}
        else:
            opts = ytdlp_base(clients)
        opts.update(
            {
                "format": "bestaudio/best" if is_soundcloud_url(url) else AUDIO_FORMAT,
                "outtmpl": str(DOWNLOADS_DIR / f"{unique}.%(ext)s"),
                "overwrites": True,
            }
        )
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.extract_info(url, download=True)

    try:
        if is_soundcloud_url(url):
            run([])
        else:
            _with_clients(run)
        files = [f for f in DOWNLOADS_DIR.glob(f"{unique}.*") if not f.name.endswith(".part")]
        if not files:
            raise FileNotFoundError("Downloaded file was not found")
        path = files[0]
        if path.stat().st_size > MAX_AUDIO_MB * 1024 * 1024:
            cleanup_file(path)
            raise RuntimeError(f"الملف أكبر من الحد المسموح ({MAX_AUDIO_MB} MB).")
        return str(path)
    except Exception:
        for p in DOWNLOADS_DIR.glob(f"{unique}.*"):
            cleanup_file(p)
        raise


# ---------------------------------------------------------------------------
# SoundCloud fallback: used automatically when YouTube blocks the server.
# Disable with SOUNDCLOUD_FALLBACK=0
# ---------------------------------------------------------------------------
SOUNDCLOUD_FALLBACK = os.getenv("SOUNDCLOUD_FALLBACK", "1").strip().lower() not in {"0", "false", "no"}
YOUTUBE_URL_RE = re.compile(r"^https?://(?:www\.|m\.|music\.)?(?:youtube\.com|youtu\.be)/", re.I)


def is_soundcloud_url(url: str) -> bool:
    return bool(re.match(r"^https?://(?:[\w-]+\.)?soundcloud\.com/", url, re.I))


def soundcloud_search(query: str, limit: int = 1) -> list[dict[str, Any]]:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "socket_timeout": 20,
        "extract_flat": True,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        data = ydl.extract_info(f"scsearch{limit}:{query}", download=False)
    entries = [x for x in (data.get("entries") or []) if x]
    if not entries:
        raise RuntimeError("SoundCloud search returned no results")
    return entries[:limit]


def item_url(item: dict[str, Any]) -> str:
    return item.get("webpage_url") or item.get("url") or f"https://www.youtube.com/watch?v={item.get('id')}"


def youtube_oembed_title(url: str) -> str:
    """Get a video's title without yt-dlp (used only to search SoundCloud)."""
    import json as _json
    import urllib.parse
    import urllib.request

    api = "https://www.youtube.com/oembed?format=json&url=" + urllib.parse.quote(url, safe="")
    with urllib.request.urlopen(api, timeout=8) as r:  # noqa: S310
        return (_json.loads(r.read().decode("utf-8")).get("title") or "").strip()


async def _yt_lookup(query: str) -> tuple[str, dict[str, Any]]:
    if re.match(r"^https?://", query, re.I):
        url = query
    else:
        async with SEARCH_SEMAPHORE:
            results = await asyncio.to_thread(youtube_search, query, 1)
        if not results:
            raise RuntimeError("لم يتم العثور على نتيجة.")
        url = item_url(results[0])
    if is_soundcloud_url(url):
        return url, {}
    async with SEARCH_SEMAPHORE:
        info = await asyncio.to_thread(youtube_info, url)
    return url, info


def search_variants(title: str) -> list[str]:
    """Build progressively simpler search texts from a (YouTube) title.

    Long titles with "(Official Video)", emojis, "|" etc. often return nothing
    on SoundCloud, so we also try a cleaned title and shorter prefixes.
    """
    raw = re.sub(r"\s+", " ", title or "").strip()
    t = re.sub(r"[\(\[\{【（].*?[\)\]\}】）]", " ", raw)
    t = re.sub(
        r"(?i)\b(official|video|music|lyrics?|audio|mv|hd|4k|hq|clip|remastered|"
        r"كلمات|حصريا|حصرياً|فيديو كليب|كليب)\b",
        " ",
        t,
    )
    t = re.sub(r"[^\w\s\-&.]", " ", t, flags=re.UNICODE)  # drops emojis, |, quotes...
    t = re.sub(r"\s+", " ", t).strip(" -–—._")
    words = t.split()
    variants: list[str] = []
    for cand in (raw, t, " ".join(words[:6]), " ".join(words[:4])):
        cand = cand.strip()
        if len(cand) >= 2 and cand not in variants:
            variants.append(cand)
    return variants


async def _sc_lookup(text: str) -> tuple[str, dict[str, Any]]:
    last_exc: Exception | None = None
    for variant in search_variants(text):
        try:
            async with SEARCH_SEMAPHORE:
                results = await asyncio.to_thread(soundcloud_search, variant, 1)
            item = results[0]
            return item_url(item), item
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
    raise last_exc or RuntimeError("SoundCloud search returned no results")


async def resolve_query(query: str) -> tuple[str, str, str, int]:
    try:
        url, info = await _yt_lookup(query)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("YouTube lookup failed: %s", str(exc)[:200])
        if not SOUNDCLOUD_FALLBACK:
            raise
        search_text = query
        if re.match(r"^https?://", query, re.I):
            search_text = ""
            if YOUTUBE_URL_RE.match(query):
                try:
                    search_text = await asyncio.to_thread(youtube_oembed_title, query)
                except Exception as oe:  # noqa: BLE001
                    LOGGER.warning("oEmbed title lookup failed: %s", oe)
        if not search_text:
            raise exc
        try:
            url, info = await _sc_lookup(search_text)
            LOGGER.info("Using SoundCloud fallback: %s", url)
        except Exception as sc_exc:  # noqa: BLE001
            LOGGER.warning("SoundCloud fallback failed: %s", str(sc_exc)[:200])
            raise exc
    title = info.get("title") or "بدون عنوان"
    video_id = str(info.get("id") or "")
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
    if "403" in low or "forbidden" in low:
        return "يوتيوب رفض تنزيل الملف من خادم Railway (403). تم استخدام مسار PO Token بدون Cookies؛ جرّب المحاولة مرة أخرى."
    if "sign in" in low or "not a bot" in low or "cookies" in low or "po token" in low:
        return "يوتيوب حظر عنوان IP الخاص بالخادم (Sign in to confirm you're not a bot). الحل: ضبط YTDLP_PROXY (بروكسي سكني) أو YTDLP_COOKIES_FILE على Railway."
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
        db_set_setting(f"np:{chat_id}", str(msg.id))
    except RPCError as exc:
        LOGGER.warning("Now playing message failed: %s", type(exc).__name__)


async def clear_now_playing(chat_id: int) -> None:
    msg_id = NOW_PLAYING.pop(chat_id, None)
    db_delete_setting(f"np:{chat_id}")
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
    IN_CALL.discard(chat_id)


# ---------------------------------------------------------------------------
# Owner-defined looping video / GIF / image shown in the voice chat while a
# track plays. The owner sends the media to the bot in private with /setvideo.
# The media is converted once to a small looping clip; for every track it is
# looped (stream copy, no re-encoding) and merged with the track's audio.
# ---------------------------------------------------------------------------
VIDEO_DIR = BASE_DIR / "video_loop"  # not touched by the temp-file cleaners
VIDEO_DIR.mkdir(parents=True, exist_ok=True)
VIDEO_LOOP_FILE = VIDEO_DIR / "loop.mp4"
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg")
VIDEO_QUALITY = os.getenv("VIDEO_QUALITY", "low").strip().lower()  # low | medium | high
VIDEO_SOURCE_MAX_MB = int(os.getenv("VIDEO_SOURCE_MAX_MB", "30"))
VIDEO_LOOP_MAX_SECONDS = int(os.getenv("VIDEO_LOOP_MAX_SECONDS", "20"))
VIDEO_MAX_TRACK_MINUTES = int(os.getenv("VIDEO_MAX_TRACK_MINUTES", "40"))
_VIDEO_SIZES = {"low": (640, 360, 15), "medium": (854, 480, 20), "high": (1280, 720, 24)}
NO_VIDEO_ROWS: set[int] = set()  # tracks that already failed once in video mode


def video_enabled() -> bool:
    return (
        AudioVideoPiped is not None
        and db_get_setting("video_enabled") == "1"
        and VIDEO_LOOP_FILE.exists()
    )


def _video_params():
    cls = {"low": LowQualityVideo, "medium": MediumQualityVideo, "high": HighQualityVideo}.get(VIDEO_QUALITY) or LowQualityVideo
    return cls()


async def _run_ffmpeg(args: list[str], timeout: int) -> None:
    proc = await asyncio.create_subprocess_exec(
        FFMPEG_BIN, "-hide_banner", "-loglevel", "error", "-nostdin", "-y", *args,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, err = await asyncio.wait_for(proc.communicate(), timeout)
    except asyncio.TimeoutError:
        proc.kill()
        raise RuntimeError("ffmpeg timed out")
    if proc.returncode != 0:
        raise RuntimeError("ffmpeg failed: " + err.decode(errors="replace")[-300:].strip())


async def make_loop_video(source: str, is_image: bool) -> None:
    """Convert the owner's media into a small H.264 loop clip (done once)."""
    w, h, fps = _VIDEO_SIZES.get(VIDEO_QUALITY, _VIDEO_SIZES["low"])
    if is_image:
        fps = 5
    vf = (
        f"scale={w}:{h}:force_original_aspect_ratio=decrease,"
        f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2:black,setsar=1,fps={fps},format=yuv420p"
    )
    tmp = VIDEO_DIR / "loop.tmp.mp4"
    pre = ["-loop", "1", "-framerate", str(fps)] if is_image else []
    dur = "4" if is_image else str(VIDEO_LOOP_MAX_SECONDS)
    enc = ["-an", "-c:v", "libx264", "-preset", "veryfast", "-crf", "28", "-g", str(fps * 2),
           "-movflags", "+faststart", str(tmp)]
    try:
        try:
            await _run_ffmpeg([*pre, "-i", source, "-t", dur, "-vf", vf, *enc], timeout=180)
        except RuntimeError:
            if not is_image:
                raise
            # Fallback for images: repeat the single frame with a filter instead of -loop.
            vf2 = vf.replace(f",fps={fps},format=yuv420p", f",format=yuv420p,loop=loop=-1:size=1:start=0,fps={fps}")
            await _run_ffmpeg(["-i", source, "-t", dur, "-vf", vf2, *enc], timeout=180)
        if not tmp.exists() or tmp.stat().st_size == 0:
            raise RuntimeError("empty output")
        os.replace(tmp, VIDEO_LOOP_FILE)
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


async def build_video_media(audio_path: str, duration: int) -> str | None:
    """Loop the owner's clip for the whole track and merge it with the audio.

    Stream copy only (no re-encoding), so it takes about a second. The result
    ends exactly when the audio ends, so the stream-end event still works.
    """
    if not video_enabled():
        return None
    if duration and duration > VIDEO_MAX_TRACK_MINUTES * 60:
        return None
    out = Path(audio_path).with_suffix(".vid.mkv")
    try:
        await _run_ffmpeg(
            ["-stream_loop", "-1", "-i", str(VIDEO_LOOP_FILE), "-i", audio_path,
             "-map", "0:v:0", "-map", "1:a:0", "-c:v", "copy", "-c:a", "copy",
             "-shortest", "-f", "matroska", str(out)],
            timeout=180,
        )
        if out.exists() and out.stat().st_size > 0:
            return str(out)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Video merge failed, using audio only: %s", str(exc)[:200])
    try:
        out.unlink(missing_ok=True)
    except OSError:
        pass
    return None


def _extract_media(m: Message | None):
    """Return (kind, size, file_id) for an image / GIF / video message, else None."""
    if not m:
        return None
    if m.animation:
        return "video", m.animation.file_size or 0, m.animation.file_id
    if m.video:
        return "video", m.video.file_size or 0, m.video.file_id
    if m.video_note:
        return "video", m.video_note.file_size or 0, m.video_note.file_id
    if m.photo:
        return "image", m.photo.file_size or 0, m.photo.file_id
    d = m.document
    if d and d.mime_type:
        if d.mime_type.startswith("video/") or d.mime_type == "image/gif":
            return "video", d.file_size or 0, d.file_id
        if d.mime_type.startswith("image/"):
            return "image", d.file_size or 0, d.file_id
    return None


async def restore_loop_video() -> None:
    """After a redeploy the local file is gone; rebuild it from the saved Telegram file_id."""
    if db_get_setting("video_enabled") != "1" or VIDEO_LOOP_FILE.exists():
        return
    file_id = db_get_setting("video_file_id")
    if not file_id:
        return
    src = None
    try:
        src = await app.download_media(file_id, file_name=str(VIDEO_DIR / f"src_{uuid.uuid4().hex}"))
        await make_loop_video(str(src), db_get_setting("video_kind") == "image")
        LOGGER.info("Restored the owner's loop video")
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Could not restore loop video: %s", str(exc)[:200])
    finally:
        cleanup_file(src)


@app.on_message(filters.private & filters.user(OWNER_ID) & filters.command(["setvideo", "تعيين_فيديو"]))
async def setvideo_handler(_, message: Message):
    if AudioVideoPiped is None:
        return await message.reply_text(
            "نسخة pytgcalls المثبتة لا تدعم بث الفيديو.\n" f"`{VIDEO_IMPORT_ERROR}`"
        )
    src = message if _extract_media(message) else message.reply_to_message
    media = _extract_media(src)
    if not media:
        return await message.reply_text(
            "أرسل صورة أو GIF أو فيديو قصيرًا مع التعليق /setvideo\n"
            "أو ردّ على وسيط موجود بالأمر /setvideo"
        )
    kind, size, file_id = media
    if size and size > VIDEO_SOURCE_MAX_MB * 1024 * 1024:
        return await message.reply_text(f"الملف كبير جدًا. الحد الأقصى {VIDEO_SOURCE_MAX_MB} MB.")
    status = await message.reply_text("جاري تجهيز الفيديو...")
    tmp = None
    try:
        tmp = await app.download_media(src, file_name=str(VIDEO_DIR / f"src_{uuid.uuid4().hex}"))
        await make_loop_video(str(tmp), kind == "image")
        db_set_setting("video_file_id", file_id)
        db_set_setting("video_kind", kind)
        db_set_setting("video_enabled", "1")
        await status.edit_text(
            "تم تعيين الفيديو. سيظهر في المكالمة مع كل أغنية ويُعاد حتى تنتهي.\n"
            "لإلغائه أرسل /delvideo"
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("setvideo failed: %s", str(exc)[:300])
        await status.edit_text(f"تعذر تجهيز الفيديو.\n\n`{str(exc)[:400]}`")
    finally:
        cleanup_file(tmp)


@app.on_message(filters.private & filters.user(OWNER_ID) & filters.command(["delvideo", "حذف_فيديو"]))
async def delvideo_handler(_, message: Message):
    for key in ("video_enabled", "video_file_id", "video_kind"):
        db_delete_setting(key)
    try:
        VIDEO_LOOP_FILE.unlink(missing_ok=True)
    except OSError:
        pass
    await message.reply_text("تم حذف الفيديو. سيعود التشغيل بالصوت فقط.")


@app.on_message(filters.private & filters.user(OWNER_ID) & filters.command(["videoinfo"]))
async def videoinfo_handler(_, message: Message):
    if AudioVideoPiped is None:
        return await message.reply_text("بث الفيديو غير مدعوم في نسخة pytgcalls الحالية.")
    if not video_enabled():
        return await message.reply_text("لا يوجد فيديو معيّن. أرسل وسيطًا مع /setvideo")
    kind = "صورة" if db_get_setting("video_kind") == "image" else "فيديو/GIF"
    size_kb = VIDEO_LOOP_FILE.stat().st_size // 1024
    w, h, fps = _VIDEO_SIZES.get(VIDEO_QUALITY, _VIDEO_SIZES["low"])
    await message.reply_text(
        f"الفيديو مفعّل\nالنوع: {kind}\nالجودة: {VIDEO_QUALITY} ({w}x{h})\nحجم المقطع: {size_kb} KB"
    )


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
            video_used = False

            try:
                file_path = await download_one(row["url"])
                stream = None
                if row_id not in NO_VIDEO_ROWS:
                    merged = await build_video_media(file_path, int(row["duration"] or 0))
                    if merged:
                        try:
                            stream = AudioVideoPiped(
                                merged,
                                audio_parameters=HighQualityAudio(),
                                video_parameters=_video_params(),
                            )
                            video_used = True
                        except Exception as vexc:  # noqa: BLE001
                            LOGGER.warning("Could not build video stream: %s", str(vexc)[:200])
                            cleanup_file(merged)
                if stream is None:
                    stream = AudioPiped(file_path, audio_parameters=HighQualityAudio())
                had_stream = chat_id in IN_CALL

                try:
                    if had_stream:
                        # Telegram can briefly lose the participant state after a
                        # 503/timeout from phone.JoinGroupCall. In that state
                        # change_stream raises PARTICIPANT_JOIN_MISSING. Rejoin
                        # the call instead of treating the track as permanently
                        # broken.
                        changed = False
                        last_exc = None
                        for attempt in range(3):
                            try:
                                await pytgcalls.change_stream(chat_id, stream)
                                changed = True
                                break
                            except Exception as exc:
                                last_exc = exc
                                msg = str(exc)
                                low_msg = msg.lower()
                                participant_missing = (
                                    "PARTICIPANT_JOIN_MISSING" in msg
                                    or "not in group call" in low_msg
                                    or "not joined" in low_msg
                                    or type(exc).__name__ == "NotInGroupCallError"
                                )
                                if not participant_missing or attempt >= 2:
                                    raise
                                LOGGER.warning(
                                    "Telegram lost the voice-chat participant state; "
                                    "rejoining before changing stream (attempt %s/3)",
                                    attempt + 1,
                                )
                                try:
                                    await pytgcalls.leave_group_call(chat_id)
                                except Exception:
                                    pass
                                await asyncio.sleep(1.5)
                                await pytgcalls.join_group_call(
                                    chat_id,
                                    stream,
                                    stream_type=StreamType().pulse_stream,
                                )
                                await asyncio.sleep(0.8)

                        if not changed and last_exc is not None:
                            raise last_exc
                    else:
                        # A transient Telegram 503 may be returned while the
                        # server is still completing the join. Retry the join
                        # without downloading the media again.
                        last_exc = None
                        joined = False
                        for attempt in range(3):
                            try:
                                await pytgcalls.join_group_call(
                                    chat_id,
                                    stream,
                                    stream_type=StreamType().pulse_stream,
                                )
                                joined = True
                                # Give Telegram a short moment to commit the
                                # participant state before the next transition.
                                await asyncio.sleep(0.8)
                                break
                            except Exception as exc:
                                last_exc = exc
                                if _is_already_joined(exc):
                                    # Assistant is already in the call: just switch the stream.
                                    try:
                                        await pytgcalls.change_stream(chat_id, stream)
                                        joined = True
                                        break
                                    except Exception as exc2:  # noqa: BLE001
                                        last_exc = exc2
                                LOGGER.warning(
                                    "Voice-chat join failed (attempt %s/3): %s",
                                    attempt + 1,
                                    type(exc).__name__,
                                )
                                if attempt < 2:
                                    await asyncio.sleep(1.2 * (attempt + 1))
                        if not joined:
                            raise last_exc

                except (NoActiveGroupCall, TelegramServerError, UnMuteNeeded) as exc:
                    queue_finish(row_id)
                    cleanup_file(file_path)
                    await clear_now_playing(chat_id)
                    IN_CALL.discard(chat_id)
                    LOGGER.warning("Voice call failed: %s", type(exc).__name__)
                    await send_temp(
                        chat_id,
                        "تعذر تشغيل المكالمة. تأكد من فتح Voice Chat أو Live Stream ومن صلاحيات المساعد.",
                        ERROR_MESSAGE_SECONDS,
                    )
                    return False

                IN_CALL.add(chat_id)
                old = CURRENT_FILES.get(chat_id)
                CURRENT_FILES[chat_id] = file_path
                if old and old != file_path:
                    cleanup_file(old)
                await update_now_playing(chat_id, row)
                return True

            except Exception as exc:
                if video_used and row_id not in NO_VIDEO_ROWS:
                    # Video mode failed: play the same track again, audio only.
                    NO_VIDEO_ROWS.add(row_id)
                    LOGGER.warning("Video stream failed (%s); retrying audio-only", type(exc).__name__)
                    cleanup_file(file_path)
                    queue_requeue(row_id)
                    await send_temp(chat_id, "تعذر بث الفيديو، سيتم التشغيل بالصوت فقط.", ERROR_MESSAGE_SECONDS)
                    continue
                queue_finish(row_id)
                cleanup_file(file_path)
                LOGGER.exception("start_next failed: %s", type(exc).__name__)
                await send_temp(
                    chat_id,
                    f"فشل تشغيل المقطع.\n\nالخطأ: `{format_error(exc)}`",
                    ERROR_MESSAGE_SECONDS,
                )
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
    delete_command(message, 2)
    await reply_temp(
        message,
        (
            f"أهلًا {message.from_user.mention if message.from_user else 'بك'}.\n\n"
            f"أنا {BOT_NAME}. أرسل اسم المقطع أو رابطه لتشغيله.\n\n"
            f"قناة المطور: @{DEVELOPER_CHANNEL}"
        ),
        60,
        reply_markup=support_keyboard(),
    )


@app.on_message(filters.command("ping"))
async def ping_handler(_, message: Message):
    started = time.perf_counter()
    msg = await message.reply_text("جارِ الفحص...")
    latency = (time.perf_counter() - started) * 1000
    await msg.edit_text(f"{BOT_NAME}\n\nزمن الاستجابة: `{latency:.0f} ms`\nمدة التشغيل: `{readable_time(int(time.time() - START_TIME))}`")
    schedule_delete(msg, 30)
    delete_command(message, 2)


PLAY_REGEX = r"^(play|vplay|p|شغل|تشغيل)(?:\s+(.+))?$"


@app.on_message((filters.group | filters.channel) & filters.regex(PLAY_REGEX, re.I))
async def play_handler(_, message: Message):
    try:
        await _play_impl(message)
    finally:
        delete_command(message)


async def _play_impl(message: Message) -> None:
    chat_id = message.chat.id
    if not await authorize_chat(chat_id, str(message.chat.type)):
        return
    requester, user_id = requester_name(message)
    if user_id and not is_admin_user(user_id):
        key = (user_id, "play")
        now = time.monotonic()
        if now - RATE_LIMITS.get(key, 0) < PLAY_COOLDOWN:
            return await reply_temp(message, "تمهل قليلًا ثم أرسل الطلب مرة أخرى.")
        RATE_LIMITS[key] = now
    ok, reason = await ensure_call_permissions(message)
    if not ok:
        return await reply_temp(message, reason)
    if queue_count(chat_id) >= MAX_QUEUE_PER_CHAT:
        return await reply_temp(message, f"قائمة الانتظار ممتلئة. الحد الحالي {MAX_QUEUE_PER_CHAT} مقطع.")
    query = (message.matches[0].group(2) or "").strip() if message.matches else ""
    if not query and message.reply_to_message:
        query = (message.reply_to_message.text or message.reply_to_message.caption or "").strip()
    if not query:
        return await reply_temp(message, "أرسل اسم المقطع أو رابطه بعد الأمر.")
    status = await reply_temp(message, "جاري تجهيز الطلب...", 120)
    try:
        url, title, video_id, duration = await resolve_query(query)
        queue_id = queue_add(chat_id, url, video_id, title, duration, requester, user_id)
        current = CURRENT_FILES.get(chat_id) or chat_lock(chat_id).locked()
        if current:
            pos = queue_count(chat_id)
            await status.edit_text(f"تمت إضافة المقطع إلى قائمة الانتظار.\nالترتيب: `{pos}`")
            schedule_delete(status, TEMP_MESSAGE_SECONDS)
        else:
            await status.delete()
            await start_next(chat_id)
    except Exception as exc:
        await status.edit_text(f"فشل تجهيز المقطع.\n\nالخطأ: `{format_error(exc)}`")
        schedule_delete(status, ERROR_MESSAGE_SECONDS)


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
    try:
        await _handle_control_impl(message, action)
    finally:
        delete_command(message)


async def _handle_control_impl(message: Message, action: str) -> None:
    if not await is_admin_for_command(message):
        return await reply_temp(message, "هذا الأمر متاح للمشرفين فقط.")
    chat_id = message.chat.id
    if not pytgcalls:
        return await reply_temp(message, "حساب المساعد غير متصل.")
    try:
        if action == "pause":
            await pytgcalls.pause_stream(chat_id)
            return await reply_temp(message, "تم إيقاف التشغيل مؤقتًا.")
        if action == "resume":
            await pytgcalls.resume_stream(chat_id)
            return await reply_temp(message, "تم استكمال التشغيل.")
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
            return await reply_temp(message, "تم إنهاء التشغيل ومسح قائمة الانتظار.")
    except Exception as exc:
        LOGGER.exception("Control action failed: %s", type(exc).__name__)
        await reply_temp(message, f"تعذر تنفيذ الأمر.\n\n`{format_error(exc)}`")


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
        return await reply_temp(message, "هذا الأمر يجب إرساله من حساب مستخدم أو من محادثة خاصة.")
    user_id = message.from_user.id
    now = time.monotonic()
    key = (user_id, "song")
    if user_id not in SUDO_USERS and now - RATE_LIMITS.get(key, 0) < SONG_COOLDOWN:
        return await reply_temp(message, "تمهل قليلًا ثم أرسل الطلب مرة أخرى.")
    RATE_LIMITS[key] = now
    query = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else ""
    if not query:
        return await reply_temp(message, "اكتب اسم الأغنية بعد الأمر.")
    status = await reply_temp(message, "جاري تجهيز الملف...", 120)
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
        schedule_delete(status, ERROR_MESSAGE_SECONDS)
    finally:
        cleanup_file(path)
        delete_command(message, 2)


@app.on_message(filters.command("بحث"))
async def search_handler(_, message: Message):
    if not message.from_user:
        return
    query = message.text.split(None, 1)[1].strip() if len(message.text.split(None, 1)) > 1 else ""
    if not query:
        return await reply_temp(message, "اكتب كلمة البحث.")
    query = query[:SEARCH_MAX_LEN]
    now = time.monotonic()
    key = (message.from_user.id, "search")
    if message.from_user.id not in SUDO_USERS and now - RATE_LIMITS.get(key, 0) < SEARCH_COOLDOWN:
        return await reply_temp(message, "تمهل قليلًا ثم أعد البحث.")
    RATE_LIMITS[key] = now
    status = await reply_temp(message, "جاري البحث...", 120)
    try:
        source_note = ""
        try:
            async with SEARCH_SEMAPHORE:
                results = await asyncio.to_thread(youtube_search, query, 4)
        except Exception as yt_exc:  # noqa: BLE001
            if not SOUNDCLOUD_FALLBACK:
                raise
            LOGGER.warning("YouTube search failed, using SoundCloud: %s", str(yt_exc)[:200])
            try:
                async with SEARCH_SEMAPHORE:
                    results = await asyncio.to_thread(soundcloud_search, query, 4)
            except Exception:  # noqa: BLE001
                raise yt_exc
            source_note = "المصدر: SoundCloud (يوتيوب غير متاح حاليًا)\n\n"
        if not results:
            return await status.edit_text("لم يتم العثور على نتائج.")
        lines = []
        for i, item in enumerate(results, 1):
            title = item.get("title") or "بدون عنوان"
            duration = item.get("duration_string") or item.get("duration") or "غير معروف"
            url = item_url(item)
            lines.append(f"{i}. {title}\nالمدة: `{duration}`\nالرابط: {url}")
        await status.edit_text(source_note + "\n\n".join(lines), disable_web_page_preview=True)
        schedule_delete(status, SEARCH_RESULT_SECONDS)
    except Exception as exc:
        await status.edit_text(f"فشل البحث.\n\nالخطأ: `{format_error(exc)}`")
        schedule_delete(status, ERROR_MESSAGE_SECONDS)
    finally:
        delete_command(message, 2)


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
    # Callback queries carry the real actor separately; do not mutate the
    # Pyrogram Message object (its from_user field is not a safe writable field).
    chat_id = query.message.chat.id
    actor_id = query.from_user.id
    allowed = actor_id in SUDO_USERS
    if not allowed:
        try:
            member = await app.get_chat_member(chat_id, actor_id)
            allowed = member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}
        except RPCError:
            allowed = False
    if not allowed:
        return await query.answer("هذا الخيار للمشرفين فقط.", show_alert=True)
    await query.answer()

    if not pytgcalls:
        return await send_temp(chat_id, "حساب المساعد غير متصل.")
    try:
        if action == "pause":
            await pytgcalls.pause_stream(chat_id)
            text = "تم إيقاف التشغيل مؤقتًا."
        elif action == "resume":
            await pytgcalls.resume_stream(chat_id)
            text = "تم استكمال التشغيل."
        elif action == "skip":
            conn = db_connect()
            try:
                conn.execute("DELETE FROM queue WHERE chat_id=? AND status='playing'", (chat_id,))
                conn.commit()
            finally:
                conn.close()
            await start_next(chat_id)
            text = "تم تخطي المقطع الحالي."
        else:
            await leave_and_clear(chat_id)
            text = "تم إنهاء التشغيل ومسح قائمة الانتظار."
        await send_temp(chat_id, text)
    except Exception as exc:
        LOGGER.exception("Callback control failed: %s", type(exc).__name__)
        await send_temp(chat_id, f"تعذر تنفيذ الأمر.\n\n`{format_error(exc)}`", ERROR_MESSAGE_SECONDS)


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
            if len(code) < 4:
                return await message.reply_text("رمز التحقق غير صحيح.")
            try:
                await state["client"].sign_in(state["phone"], state["hash"], code)
            except SessionPasswordNeeded:
                state["step"] = "password"
                return await message.reply_text("أرسل كلمة مرور المصادقة الثنائية.")
            except PhoneCodeExpired:
                # Keep the login flow alive and request a fresh code instead of
                # forcing the owner to restart the whole process.
                try:
                    sent = await state["client"].send_code(state["phone"])
                    state["hash"] = sent.phone_code_hash
                    state["created"] = time.monotonic()
                    return await message.reply_text("انتهت صلاحية الرمز. تم إرسال رمز جديد، أرسله الآن.")
                except Exception as resend_exc:
                    LOGGER.warning("Code resend failed: %s", type(resend_exc).__name__)
                    return await message.reply_text("انتهت صلاحية الرمز. أعد العملية من /المساعد.")
            except PhoneCodeInvalid:
                return await message.reply_text("رمز التحقق غير صحيح. أرسل الرمز الأخير الذي وصلك.")
            return await finish_login(message, state)
        if state["step"] == "password":
            await state["client"].check_password(message.text)
            return await finish_login(message, state)
    except PhoneNumberInvalid:
        return await message.reply_text("رقم الهاتف غير صحيح أو غير مقبول من Telegram.")
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
    IN_CALL.discard(chat_id)
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
    await purge_stale_now_playing()
    await restore_loop_video()
    spawn(janitor())
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
    for task in list(_BG_TASKS):
        task.cancel()
    IN_CALL.clear()
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
