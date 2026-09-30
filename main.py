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
from html import escape as _esc
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import yt_dlp
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ChatType, ParseMode
from pyrogram.errors import (
    RPCError,
    SessionPasswordNeeded,
    UserNotParticipant,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    PhoneNumberInvalid,
)
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, CallbackQuery
try:  # newer Pyrogram forks deprecate disable_web_page_preview
    from pyrogram.types import LinkPreviewOptions
except ImportError:  # pragma: no cover
    LinkPreviewOptions = None  # type: ignore[assignment]

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
# Several assistant accounts: SESSION (legacy, one) and/or SESSIONS (many, separated by
# comma / space / newline). Accounts added from the /المساعد panel are stored in the DB.
SESSIONS_ENV = [x for x in re.split(r"[\s,]+", os.getenv("SESSIONS", "")) if x]
if SESSION and SESSION not in SESSIONS_ENV:
    SESSIONS_ENV.insert(0, SESSION)
MAX_ASSISTANTS = max(1, int(os.getenv("MAX_ASSISTANTS", "10")))
# Owner-only alerts (private message). The same alert is not repeated inside this window.
OWNER_ALERT_COOLDOWN = int(os.getenv("OWNER_ALERT_COOLDOWN", "1800"))
# After YouTube blocks the server IP, go straight to SoundCloud for this many seconds
# before trying YouTube again.
YT_BLOCK_COOLDOWN = int(os.getenv("YT_BLOCK_COOLDOWN", "600"))

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
BOT_ID = 0
BOT_USERNAME = ""
BOT_MENTION = ""


class UserError(RuntimeError):
    """An error whose (Arabic) message is safe to show to everybody in a chat."""


class Assistant:
    """One assistant (userbot) account with its own Pyrogram client and PyTgCalls."""

    def __init__(self, key: str, session: str, source: str, db_id: int | None = None,
                 user_id: int = 0, username: str = "") -> None:
        self.key = key              # "env:1" or "db:7"
        self.session = session
        self.source = source        # "env" (host variables) or "db" (added from the panel)
        self.db_id = db_id
        self.user_id = user_id
        self.username = username
        self.name = ""
        self.mention = ""
        self.client: Client | None = None
        self.calls: Any = None
        self.ok = False
        self.error = ""

    @property
    def ready(self) -> bool:
        return bool(self.ok and self.client is not None and self.calls is not None)

    def plain(self) -> str:
        return f"@{self.username}" if self.username else (self.name or self.key)

    def display(self) -> str:
        return f"@{self.username}" if self.username else (self.mention or self.name or self.key)


ASSISTANTS: list[Assistant] = []
CHAT_ASSISTANT: dict[int, Assistant] = {}   # chat_id -> assistant serving that chat
OWNER_ALERTS: dict[str, float] = {}
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
PAUSED: set[int] = set()              # chats whose stream is currently paused
PANEL_INPUT: dict[int, dict[str, Any]] = {}   # owner's pending text input for the panel
CHAT_TITLES: dict[int, str] = {}
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
            CREATE TABLE IF NOT EXISTS assistants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session TEXT NOT NULL UNIQUE,
                user_id INTEGER NOT NULL DEFAULT 0,
                username TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS authorized_chats (
                chat_id INTEGER PRIMARY KEY,
                chat_type TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL
            );
            """
        )
        conn.commit()
        # Migrate the old single-assistant session into the new table.
        legacy = conn.execute("SELECT value FROM settings WHERE key='assistant_session'").fetchone()
        if legacy and legacy["value"]:
            conn.execute(
                "INSERT OR IGNORE INTO assistants(session,user_id,username,created_at) VALUES(?,?,?,?)",
                (legacy["value"], 0, "", int(time.time())),
            )
        conn.execute("DELETE FROM settings WHERE key='assistant_session'")
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
def db_assistants() -> list[sqlite3.Row]:
    conn = db_connect()
    try:
        return conn.execute("SELECT * FROM assistants ORDER BY id ASC").fetchall()
    finally:
        conn.close()


def db_add_assistant(session: str, user_id: int, username: str) -> int:
    conn = db_connect()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO assistants(session,user_id,username,created_at) VALUES(?,?,?,?)",
            (session, user_id, username, int(time.time())),
        )
        conn.commit()
        row = conn.execute("SELECT id FROM assistants WHERE session=?", (session,)).fetchone()
        return int(row["id"])
    finally:
        conn.close()


def db_update_assistant(row_id: int, user_id: int, username: str) -> None:
    conn = db_connect()
    try:
        conn.execute("UPDATE assistants SET user_id=?, username=? WHERE id=?", (user_id, username, row_id))
        conn.commit()
    finally:
        conn.close()


def db_delete_assistant(row_id: int) -> None:
    conn = db_connect()
    try:
        conn.execute("DELETE FROM assistants WHERE id=?", (row_id,))
        conn.commit()
    finally:
        conn.close()


# authorized_chats.enabled: 1 = allowed, 0 = blocked, 2 = pending (seen while allow-list mode was on)
def db_chats() -> list[sqlite3.Row]:
    conn = db_connect()
    try:
        return conn.execute(
            "SELECT * FROM authorized_chats ORDER BY (enabled=2) DESC, created_at DESC"
        ).fetchall()
    finally:
        conn.close()


def db_chat(chat_id: int) -> sqlite3.Row | None:
    conn = db_connect()
    try:
        return conn.execute("SELECT * FROM authorized_chats WHERE chat_id=?", (chat_id,)).fetchone()
    finally:
        conn.close()


def db_set_chat(chat_id: int, chat_type: str, enabled: int) -> None:
    conn = db_connect()
    try:
        conn.execute(
            "INSERT INTO authorized_chats(chat_id,chat_type,enabled,created_at) VALUES(?,?,?,?) "
            "ON CONFLICT(chat_id) DO UPDATE SET enabled=excluded.enabled",
            (chat_id, chat_type, enabled, int(time.time())),
        )
        conn.commit()
    finally:
        conn.close()


def db_del_chat(chat_id: int) -> None:
    conn = db_connect()
    try:
        conn.execute("DELETE FROM authorized_chats WHERE chat_id=?", (chat_id,))
        conn.commit()
    finally:
        conn.close()


def db_playing_row(chat_id: int) -> sqlite3.Row | None:
    conn = db_connect()
    try:
        return conn.execute(
            "SELECT title, requester FROM queue WHERE chat_id=? AND status='playing' ORDER BY id DESC LIMIT 1",
            (chat_id,),
        ).fetchone()
    finally:
        conn.close()


def db_queue_totals() -> tuple[int, int]:
    conn = db_connect()
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(status='queued'),0) AS q, COALESCE(SUM(status='playing'),0) AS p FROM queue"
        ).fetchone()
        return int(row["q"]), int(row["p"])
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
    if msg is None or isinstance(msg, MutedMessage) or getattr(msg, "chat", None) is None or _is_private(msg):
        return
    spawn(_delete_after(msg.chat.id, msg.id, delay))


def delete_command(message: Message, delay: float = 1.0) -> None:
    """Remove the user's command message (needs the bot's delete-messages right)."""
    if get_flag("auto_delete"):
        schedule_delete(message, delay)


def no_preview() -> dict[str, Any]:
    """Keyword arguments that switch off link previews on any Pyrogram version."""
    if LinkPreviewOptions is not None:
        return {"link_preview_options": LinkPreviewOptions(is_disabled=True)}
    return {"disable_web_page_preview": True}


class MutedMessage:
    """Stand-in returned when the bot is not allowed to write in a chat.
    Every call is a harmless no-op, so the flow (and the music) carries on silently."""

    id = 0
    chat = None

    async def edit_text(self, *args: Any, **kwargs: Any) -> "MutedMessage":
        return self

    edit = edit_caption = edit_reply_markup = edit_text

    async def delete(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def reply_text(self, *args: Any, **kwargs: Any) -> "MutedMessage":
        return self


async def report_cannot_send(chat_id: int, exc: Exception) -> None:
    """The bot cannot post in this chat (e.g. a channel where it is admin without the
    'Post messages' right). Tell the OWNER privately, once in a while - never the chat."""
    LOGGER.warning("Cannot send to chat %s: %s", chat_id, type(exc).__name__)
    title = await chat_title(chat_id)
    await notify_owner(
        f"cant_send:{chat_id}",
        "🔇 تنبيه للمالك\n\n"
        f"لا يستطيع البوت إرسال رسائل في: {title}\n({chat_id})\n"
        f"السبب: {type(exc).__name__}\n\n"
        "الحل: من إعدادات المشرفين في تلك المحادثة فعّل للبوت صلاحية إرسال الرسائل "
        "(Post Messages). التشغيل الصوتي يستمر بصمت في هذه الأثناء.",
        21600,
    )


async def safe_reply(message: Message, text: str, **kwargs: Any) -> Any:
    try:
        return await message.reply_text(text, **kwargs)
    except RPCError as exc:
        spawn(report_cannot_send(message.chat.id, exc))
        return MutedMessage()


async def reply_temp(message: Message, text: str, delay: float | None = None, **kwargs) -> Message:
    """Reply, then auto-delete the reply in groups/channels (private chats untouched)."""
    msg = await safe_reply(message, text, **kwargs)
    schedule_delete(msg, delay if delay is not None else TEMP_MESSAGE_SECONDS)
    return msg


async def send_temp(chat_id: int, text: str, delay: float = TEMP_MESSAGE_SECONDS) -> None:
    try:
        msg = await app.send_message(chat_id, text)
        spawn(_delete_after(chat_id, msg.id, delay))
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Temp message failed: %s", type(exc).__name__)
        if isinstance(exc, RPCError):
            spawn(report_cannot_send(chat_id, exc))


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


def assistant_load(a: Assistant) -> int:
    """Number of chats this assistant is currently streaming in."""
    return sum(1 for c, x in CHAT_ASSISTANT.items() if x is a and c in IN_CALL)


def ready_assistants() -> list[Assistant]:
    return [a for a in ASSISTANTS if a.ready]


async def assistant_is_admin(a: Assistant, chat_id: int) -> bool:
    if not a.ready:
        return False
    try:
        member = await a.client.get_chat_member(chat_id, "me")
        return member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER}
    except UserNotParticipant:
        return False
    except RPCError as exc:
        LOGGER.warning("Assistant admin check failed: %s", type(exc).__name__)
        return False


async def admin_assistants(chat_id: int, candidates: list[Assistant]) -> list[Assistant]:
    if not candidates:
        return []
    results = await asyncio.gather(
        *(assistant_is_admin(a, chat_id) for a in candidates), return_exceptions=True
    )
    return [a for a, ok in zip(candidates, results) if ok is True]


async def any_assistant_admin(chat_id: int) -> bool:
    return bool(await admin_assistants(chat_id, ready_assistants()))


async def pick_assistant(chat_id: int) -> tuple[Assistant | None, str]:
    """Choose the assistant that serves a chat: sticky while a call is active,
    otherwise the least busy assistant that is an admin of the chat."""
    ready = ready_assistants()
    if not ready:
        return None, "حساب المساعد غير متصل."
    cur = CHAT_ASSISTANT.get(chat_id)
    if cur is not None and not cur.ready:
        CHAT_ASSISTANT.pop(chat_id, None)
        IN_CALL.discard(chat_id)
        cur = None
    if cur is not None and (chat_id in IN_CALL or chat_lock(chat_id).locked()):
        return cur, ""
    admins = await admin_assistants(chat_id, ready)
    if not admins:
        names = "، ".join(a.display() for a in ready[:3])
        if len(ready) == 1:
            return None, f"أضف حساب المساعد {names} كمشرف ثم أعد المحاولة."
        return None, f"أضف أحد حسابات المساعد ({names}) كمشرف ثم أعد المحاولة."
    chosen = min(admins, key=lambda x: (assistant_load(x), ASSISTANTS.index(x)))
    CHAT_ASSISTANT[chat_id] = chosen
    return chosen, ""


async def next_assistant(chat_id: int, tried: list[Assistant]) -> Assistant | None:
    """Another ready assistant (admin in the chat) that was not tried yet."""
    others = [a for a in ready_assistants() if a not in tried]
    admins = await admin_assistants(chat_id, others)
    if not admins:
        return None
    return min(admins, key=lambda x: (assistant_load(x), ASSISTANTS.index(x)))


async def chat_title(chat_id: int) -> str:
    cached = CHAT_TITLES.get(chat_id)
    if cached:
        return cached
    try:
        chat = await app.get_chat(chat_id)
        title = chat.title or chat.first_name or str(chat_id)
        CHAT_TITLES[chat_id] = title
        return title
    except Exception:  # noqa: BLE001 - bot is not in the chat / chat unknown
        return str(chat_id)


async def alert_unauthorized(chat_id: int) -> None:
    title = await chat_title(chat_id)
    await notify_owner(
        f"unauth:{chat_id}",
        f"🔒 محاولة تشغيل من محادثة غير مسموحة\n\n{title}\n{chat_id}\n\n"
        "للسماح: أرسل /allow داخل المحادثة، أو افتح /panel ثم المحادثات.",
        86400,
    )


async def authorize_chat(chat_id: int, chat_type: str) -> bool:
    """Open mode: everybody is allowed except blocked chats.
    Allow-list mode: only chats marked allowed; new chats are recorded as pending
    and the owner is told privately."""
    allowlist = get_flag("allowlist")
    conn = db_connect()
    try:
        row = conn.execute("SELECT enabled FROM authorized_chats WHERE chat_id=?", (chat_id,)).fetchone()
        if row is None:
            enabled = 2 if allowlist else 1
            conn.execute(
                "INSERT OR IGNORE INTO authorized_chats(chat_id,chat_type,enabled,created_at) VALUES(?,?,?,?)",
                (chat_id, chat_type, enabled, int(time.time())),
            )
            conn.commit()
        else:
            enabled = int(row["enabled"])
    finally:
        conn.close()
    if enabled == 1:
        return True
    if enabled == 0:
        return False
    if not allowlist:
        return True
    spawn(alert_unauthorized(chat_id))
    return False


# ---------------------------------------------------------------------------
# Owner-only alerts. Problems such as a YouTube IP block are reported to the
# owner in a private message ONLY - never in a group or channel.
# ---------------------------------------------------------------------------
async def notify_owner(key: str, text: str, cooldown: float | None = None) -> None:
    if not OWNER_ID or not get_flag("owner_alerts"):
        return
    cooldown = OWNER_ALERT_COOLDOWN if cooldown is None else cooldown
    now = time.monotonic()
    last = OWNER_ALERTS.get(key)
    if last is not None and now - last < cooldown:
        return
    OWNER_ALERTS[key] = now
    try:
        await app.send_message(OWNER_ID, text, parse_mode=ParseMode.DISABLED, **no_preview())
    except Exception as exc:  # noqa: BLE001 - owner never opened the bot / blocked it
        LOGGER.warning("Owner alert failed: %s", type(exc).__name__)


def notify_owner_threadsafe(key: str, text: str, cooldown: float | None = None) -> None:
    """Same as notify_owner, callable from worker threads (yt-dlp runs in threads)."""
    try:
        asyncio.run_coroutine_threadsafe(notify_owner(key, text, cooldown), MAIN_LOOP)
    except Exception:  # noqa: BLE001
        pass


_YT_BLOCKED_UNTIL = 0.0
_YT_BLOCK_REPORTED = False


def youtube_blocked() -> bool:
    """True while we know YouTube is refusing this server (circuit breaker)."""
    return time.monotonic() < _YT_BLOCKED_UNTIL


def youtube_block_remaining() -> int:
    return max(0, int(_YT_BLOCKED_UNTIL - time.monotonic()))


def reset_youtube_block() -> None:
    global _YT_BLOCKED_UNTIL
    _YT_BLOCKED_UNTIL = 0.0


def note_youtube_block(exc: Exception, where: str) -> None:
    global _YT_BLOCKED_UNTIL, _YT_BLOCK_REPORTED
    _YT_BLOCKED_UNTIL = time.monotonic() + YT_BLOCK_COOLDOWN
    _YT_BLOCK_REPORTED = True
    notify_owner_threadsafe(
        "yt_block",
        "🚫 تنبيه للمالك\n\n"
        "يوتيوب حظر عنوان IP الخاص بالخادم (طلب تسجيل الدخول / Bot check).\n"
        f"البوت يستخدم SoundCloud تلقائيًا لمدة {max(1, YT_BLOCK_COOLDOWN // 60)} دقيقة ثم يجرّب يوتيوب مجددًا. "
        "لا يظهر أي شيء عن ذلك في المجموعات أو القنوات.\n\n"
        f"المكان: {where}\n"
        f"الخطأ: {str(exc)[:300]}\n\n"
        "للحل الدائم: أضف YTDLP_COOKIES_B64 (كوكيز حساب جوجل وهمي، ويمكن عدة حسابات) "
        "أو YTDLP_PROXIES (بروكسي سكني).",
    )


def note_youtube_ok() -> None:
    global _YT_BLOCKED_UNTIL, _YT_BLOCK_REPORTED
    _YT_BLOCKED_UNTIL = 0.0
    if _YT_BLOCK_REPORTED:
        _YT_BLOCK_REPORTED = False
        OWNER_ALERTS.pop("yt_block", None)  # so a future block alerts immediately again
        notify_owner_threadsafe("yt_ok", "✅ عاد يوتيوب للعمل، سيستخدمه البوت كمصدر أول من جديد.", 0)


# ---------------------------------------------------------------------------
# Assistant accounts (any number). Sources: SESSION / SESSIONS variables and
# accounts added from the /المساعد panel (stored in the database).
# ---------------------------------------------------------------------------
def load_assistant_records() -> None:
    ASSISTANTS.clear()
    seen: set[str] = set()
    for i, session in enumerate(SESSIONS_ENV, 1):
        ASSISTANTS.append(Assistant(f"env:{i}", session, "env"))
        seen.add(session)
    for row in db_assistants():
        if row["session"] in seen:
            continue
        ASSISTANTS.append(
            Assistant(
                f"db:{row['id']}", row["session"], "db", db_id=int(row["id"]),
                user_id=int(row["user_id"] or 0), username=row["username"] or "",
            )
        )


async def start_assistant(a: Assistant) -> bool:
    if a.ready:
        return True
    if PyTgCalls is None:
        a.error = "PyTgCalls unavailable"
        LOGGER.error("PyTgCalls is unavailable: %s", PYTGCALLS_IMPORT_ERROR)
        return False
    client = Client(
        f"KroAssistant_{a.key.replace(':', '_')}",
        api_id=API_ID, api_hash=API_HASH, session_string=a.session,
    )
    try:
        await client.start()
        me = await client.get_me()
        if any(o is not a and o.ready and o.user_id == me.id for o in ASSISTANTS):
            a.error = "duplicate account"
            LOGGER.warning("Assistant %s is the same account as another assistant; skipped", a.key)
            try:
                await client.stop()
            except Exception:  # noqa: BLE001
                pass
            return False
        calls = PyTgCalls(client)
        await calls.start()
        a.client, a.calls = client, calls
        a.user_id = me.id
        a.username = me.username or ""
        a.name = me.first_name or ""
        a.mention = me.mention
        a.ok = True
        a.error = ""
        register_pytgcalls_handlers(a)
        if a.db_id:
            db_update_assistant(a.db_id, a.user_id, a.username)
        LOGGER.info("Assistant %s started as %s", a.key, a.plain())
        return True
    except Exception as exc:  # noqa: BLE001
        a.error = type(exc).__name__
        LOGGER.exception("Assistant %s startup failed", a.key)
        try:
            await client.stop()
        except Exception:  # noqa: BLE001
            pass
        a.client = None
        a.calls = None
        a.ok = False
        return False


async def stop_assistant(a: Assistant) -> None:
    # Whatever this assistant was streaming is over.
    for chat_id, x in list(CHAT_ASSISTANT.items()):
        if x is a:
            CHAT_ASSISTANT.pop(chat_id, None)
            IN_CALL.discard(chat_id)
            cleanup_file(CURRENT_FILES.pop(chat_id, None))
            await clear_now_playing(chat_id)
    calls, client = a.calls, a.client
    a.ok = False
    a.calls = None
    a.client = None
    if calls:
        try:
            await calls.stop()
        except Exception:  # noqa: BLE001
            pass
    if client:
        try:
            await client.stop()
        except Exception:  # noqa: BLE001
            pass


async def remove_assistant(a: Assistant) -> None:
    await stop_assistant(a)
    if a.db_id:
        db_delete_assistant(a.db_id)
    if a in ASSISTANTS:
        ASSISTANTS.remove(a)


async def start_all_assistants() -> None:
    load_assistant_records()
    for a in list(ASSISTANTS):
        if not await start_assistant(a):
            await notify_owner(
                f"assistant_start:{a.key}",
                f"⚠️ تنبيه للمالك\n\nتعذر تشغيل حساب المساعد ({a.plain()}). السبب: {a.error or 'غير معروف'}.",
            )
    LOGGER.info("Assistants ready: %s/%s", len(ready_assistants()), len(ASSISTANTS))


async def stop_all_assistants() -> None:
    for a in list(ASSISTANTS):
        await stop_assistant(a)


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
# Several accounts are supported: separate the base64 values with a comma or a new line.
# One is picked at random per request, which spreads the load and survives a burned account.
YTDLP_COOKIE_FILES: list[str] = [YTDLP_COOKIES_FILE] if YTDLP_COOKIES_FILE else []
for _i, _b64 in enumerate(x for x in re.split(r"[\s,]+", os.getenv("YTDLP_COOKIES_B64", "")) if x):
    try:
        import base64
        _cp = f"/tmp/yt_cookies_{_i}.txt"
        with open(_cp, "wb") as _f:
            _f.write(base64.b64decode(_b64))
        YTDLP_COOKIE_FILES.append(_cp)
    except Exception as _e:  # noqa: BLE001
        LOGGER.warning("Invalid YTDLP_COOKIES_B64 entry %s: %s", _i + 1, _e)
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
    cookie_files = [p for p in YTDLP_COOKIE_FILES if os.path.isfile(p)]
    if cookie_files:
        opts["cookiefile"] = random.choice(cookie_files)
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
            result = _with_clients_once(fn)
            note_youtube_ok()
            return result
        except Exception as exc:  # noqa: BLE001
            last = exc
            if not _is_bot_check(exc):
                raise
            if attempt + 1 < attempts:
                LOGGER.warning("Bot check hit; retrying with another proxy")
            else:
                LOGGER.warning("Bot check hit (YouTube blocked this IP)")
                note_youtube_block(exc, "yt-dlp")
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
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(f"ytsearch{limit}:{query}", download=False)
    except Exception as exc:  # noqa: BLE001
        if _is_bot_check(exc):
            note_youtube_block(exc, "search")
        raise
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
            raise UserError(f"الملف أكبر من الحد المسموح ({MAX_AUDIO_MB} MB).")
        return str(path)
    except Exception:
        for p in DOWNLOADS_DIR.glob(f"{unique}.*"):
            cleanup_file(p)
        raise


# ---------------------------------------------------------------------------
# SoundCloud fallback: used automatically when YouTube blocks the server, both
# when looking a track up AND when downloading it.
# Disable with SOUNDCLOUD_FALLBACK=0
# ---------------------------------------------------------------------------
SOUNDCLOUD_FALLBACK = os.getenv("SOUNDCLOUD_FALLBACK", "1").strip().lower() not in {"0", "false", "no"}

# Runtime switches, changeable from the owner panel and saved in the database.
# The environment variables only provide the first default.
_FLAG_DEFAULTS: dict[str, bool] = {
    "soundcloud": SOUNDCLOUD_FALLBACK,
    "auto_delete": AUTO_DELETE_COMMANDS,
    "owner_alerts": True,
    "allowlist": False,
}
_FLAG_CACHE: dict[str, bool] = {}


def get_flag(key: str) -> bool:
    if key in _FLAG_CACHE:
        return _FLAG_CACHE[key]
    raw = db_get_setting(f"flag:{key}")
    value = _FLAG_DEFAULTS[key] if raw == "" else raw == "1"
    _FLAG_CACHE[key] = value
    return value


def set_flag(key: str, value: bool) -> None:
    db_set_setting(f"flag:{key}", "1" if value else "0")
    _FLAG_CACHE[key] = value


def soundcloud_enabled() -> bool:
    return get_flag("soundcloud")
YOUTUBE_URL_RE = re.compile(r"^https?://(?:www\.|m\.|music\.)?(?:youtube\.com|youtu\.be)/", re.I)
YT_ID_RE = re.compile(r"(?:v=|youtu\.be/|shorts/|embed/|live/)([A-Za-z0-9_-]{11})")


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
    last: Exception | None = None
    for _ in range(2):  # one retry for transient network errors
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                data = ydl.extract_info(f"scsearch{limit}:{query}", download=False)
            entries = [x for x in (data.get("entries") or []) if x]
            if entries:
                return entries[:limit]
            last = RuntimeError("SoundCloud search returned no results")
            break  # an empty result will not change on retry
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(0.6)
    assert last is not None
    raise last


def item_url(item: dict[str, Any]) -> str:
    return item.get("webpage_url") or item.get("url") or f"https://www.youtube.com/watch?v={item.get('id')}"


def youtube_title_lookup(url: str) -> str:
    """Get a video's title WITHOUT yt-dlp, so it still works while yt-dlp is blocked.

    Tries YouTube oEmbed, then the watch page's og:title, then noembed.com.
    """
    import html as _html
    import json as _json
    import urllib.parse
    import urllib.request

    m = YT_ID_RE.search(url)
    canon = f"https://www.youtube.com/watch?v={m.group(1)}" if m else url
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.8",
    }

    def get(u: str) -> str:
        req = urllib.request.Request(u, headers=headers)
        with urllib.request.urlopen(req, timeout=8) as r:  # noqa: S310
            return r.read(400_000).decode("utf-8", "ignore")

    quoted = urllib.parse.quote(canon, safe="")
    last: Exception | None = None
    for attempt in ("oembed", "page", "noembed"):
        try:
            if attempt == "oembed":
                title = (_json.loads(get("https://www.youtube.com/oembed?format=json&url=" + quoted)).get("title") or "").strip()
            elif attempt == "page":
                page = get(canon)
                mt = re.search(r'<meta property="og:title" content="([^"]+)"', page) or re.search(r"<title>(.*?)</title>", page, re.S)
                title = _html.unescape(mt.group(1)).replace(" - YouTube", "").strip() if mt else ""
                if title.lower() in {"youtube", "before you continue to youtube"}:
                    title = ""
            else:
                title = (_json.loads(get("https://noembed.com/embed?url=" + quoted)).get("title") or "").strip()
            if title:
                return title
        except Exception as exc:  # noqa: BLE001
            last = exc
    raise last or RuntimeError("title lookup failed")


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
    on SoundCloud, so we also try a cleaned title, its parts and shorter prefixes.
    """
    raw = re.sub(r"\s+", " ", title or "").strip()
    t0 = re.sub(r"[\(\[\{【（].*?[\)\]\}】）]", " ", raw)
    t0 = re.sub(r"#\S+", " ", t0)
    t0 = re.sub(r"(?i)\s-\s*topic\b", " ", t0)
    t0 = re.sub(r"(?i)\b(?:ft|feat|featuring)\b\.?", " ", t0)
    t0 = re.sub(
        r"(?i)\b(official|video|music|lyrics?|audio|mv|hd|4k|hq|clip|remastered|"
        r"كلمات|حصريا|حصرياً|فيديو كليب|كليب)\b",
        " ",
        t0,
    )

    def clean(s: str) -> str:
        s = re.sub(r"[^\w\s\-&.]", " ", s, flags=re.UNICODE)  # drops emojis, |, quotes...
        return re.sub(r"\s+", " ", s).strip(" -–—._")

    t = clean(t0)
    words = t.split()
    parts = [clean(p) for p in re.split(r"\s[-–—]\s|[|｜•·/]", t0)]
    variants: list[str] = []
    candidates = [raw, t, " ".join(words[:6]), " ".join(words[:4])]
    candidates += [p for p in parts if len(p.split()) >= 2][:3]
    for cand in candidates:
        cand = cand.strip()
        if len(cand) >= 2 and cand not in variants:
            variants.append(cand)
    return variants


def _norm_text(s: str) -> str:
    return re.sub(r"[^\w]+", " ", (s or "").lower(), flags=re.UNICODE).strip()


def _pick_sc_result(results: list[dict[str, Any]], wanted: str, expected_duration: int = 0) -> dict[str, Any]:
    """Pick the best SoundCloud hit: similar title, plausible length, not a 30s preview."""
    import difflib

    def dur(x: dict[str, Any]) -> int:
        try:
            return int(x.get("duration") or 0)
        except (TypeError, ValueError):
            return 0

    pool = list(results)
    if expected_duration > 0:
        close = [x for x in pool if dur(x) == 0 or abs(dur(x) - expected_duration) <= max(45, expected_duration * 0.4)]
        if close:
            pool = close
        else:
            pool = [x for x in pool if dur(x) == 0 or dur(x) > 35] or pool
    target = _norm_text(wanted)

    def score(pair: tuple[int, dict[str, Any]]) -> float:
        idx, x = pair
        return difflib.SequenceMatcher(None, _norm_text(x.get("title") or ""), target).ratio() - 0.05 * idx

    return max(enumerate(pool), key=score)[1]


async def _sc_lookup(text: str, expected_duration: int = 0) -> tuple[str, dict[str, Any]]:
    last_exc: Exception | None = None
    for variant in search_variants(text):
        try:
            async with SEARCH_SEMAPHORE:
                results = await asyncio.to_thread(soundcloud_search, variant, 5)
            item = _pick_sc_result(results, text, expected_duration)
            return item_url(item), item
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
    raise last_exc or RuntimeError("SoundCloud search returned no results")


async def _resolve_via_soundcloud(query: str) -> tuple[str, dict[str, Any]] | None:
    """Find the same track on SoundCloud. Returns None when nothing suitable is found."""
    search_text = query
    if re.match(r"^https?://", query, re.I):
        search_text = ""
        if YOUTUBE_URL_RE.match(query):
            try:
                search_text = await asyncio.to_thread(youtube_title_lookup, query)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Title lookup failed: %s", str(exc)[:160])
    if not search_text:
        return None
    try:
        url, info = await _sc_lookup(search_text)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("SoundCloud fallback failed for %r: %s", search_text[:80], str(exc)[:200])
        return None
    LOGGER.info("Using SoundCloud fallback: %s", url)
    return url, info


def _finish_resolve(url: str, info: dict[str, Any]) -> tuple[str, str, str, int]:
    title = info.get("title") or "بدون عنوان"
    video_id = str(info.get("id") or "")
    duration = int(info.get("duration") or 0)
    if duration > DURATION_LIMIT * 60:
        raise UserError(f"المقطع يتجاوز الحد المسموح وهو {DURATION_LIMIT} دقيقة.")
    return url, title, video_id, duration


async def resolve_query(query: str) -> tuple[str, str, str, int]:
    sc_tried = False
    # Circuit breaker: YouTube recently blocked us, so do not hammer it - use SoundCloud first.
    if soundcloud_enabled() and youtube_blocked() and not is_soundcloud_url(query):
        sc_tried = True
        found = await _resolve_via_soundcloud(query)
        if found:
            return _finish_resolve(*found)
    try:
        url, info = await _yt_lookup(query)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("YouTube lookup failed: %s", str(exc)[:200])
        if not soundcloud_enabled() or sc_tried:
            raise
        found = await _resolve_via_soundcloud(query)
        if not found:
            raise
        url, info = found
    return _finish_resolve(url, info)


async def search_results(query: str, limit: int = 4) -> tuple[list[dict[str, Any]], str]:
    """Search for the /بحث command. Returns (results, note shown above the list)."""
    note = "المصدر: SoundCloud\n\n"
    if soundcloud_enabled() and youtube_blocked():
        try:
            async with SEARCH_SEMAPHORE:
                return await asyncio.to_thread(soundcloud_search, query, limit), note
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("SoundCloud search failed: %s", str(exc)[:200])
    try:
        async with SEARCH_SEMAPHORE:
            return await asyncio.to_thread(youtube_search, query, limit), ""
    except Exception as yt_exc:  # noqa: BLE001
        if not soundcloud_enabled():
            raise
        LOGGER.warning("YouTube search failed, using SoundCloud: %s", str(yt_exc)[:200])
        try:
            async with SEARCH_SEMAPHORE:
                return await asyncio.to_thread(soundcloud_search, query, limit), note
        except Exception:  # noqa: BLE001
            raise yt_exc


async def download_one(url: str, title: str = "", duration: int = 0) -> str:
    async with DOWNLOAD_SEMAPHORE:
        try:
            return await asyncio.to_thread(download_audio, url)
        except Exception as exc:  # noqa: BLE001
            # YouTube refused the download (block / 403 / no formats): play the same
            # track from SoundCloud instead of failing.
            if isinstance(exc, UserError) or not soundcloud_enabled() or not YOUTUBE_URL_RE.match(url):
                raise
            LOGGER.warning("YouTube download failed (%s); trying SoundCloud", str(exc)[:200])
            wanted = title
            if not wanted:
                try:
                    wanted = await asyncio.to_thread(youtube_title_lookup, url)
                except Exception:  # noqa: BLE001
                    wanted = ""
            if not wanted:
                raise
            try:
                sc_url, _info = await _sc_lookup(wanted, duration)
                LOGGER.info("Using SoundCloud fallback for download: %s", sc_url)
                return await asyncio.to_thread(download_audio, sc_url)
            except Exception as sc_exc:  # noqa: BLE001
                LOGGER.warning("SoundCloud download fallback failed: %s", str(sc_exc)[:200])
                raise exc


# ---------------------------------------------------------------------------
# Errors: what users see in a chat vs. what the owner is told privately.
# ---------------------------------------------------------------------------
BLOCK_MARKERS = (
    "sign in", "not a bot", "confirm you", "cookies", "po token", "403", "forbidden",
    "429", "too many requests", "bot check",
)
PUBLIC_GENERIC = "المقطع غير متاح حاليًا. جرّب اسمًا أو رابطًا آخر، أو أعد المحاولة بعد قليل."


def is_block_error(exc: Exception) -> bool:
    low = str(exc).lower()
    return any(k in low for k in BLOCK_MARKERS)


async def public_error(exc: Exception, where: str = "", fallback: str | None = None) -> str:
    """Return a short message that is safe for a group/channel (never mentions a block,
    proxies or cookies) and send the real details to the owner in private."""
    if isinstance(exc, UserError):
        return str(exc)
    raw = str(exc) or type(exc).__name__
    block = is_block_error(exc)
    key = "req_failed:block" if block else f"req_failed:{type(exc).__name__}:{raw[:60]}"
    detail = "يوتيوب محظور ولم يُعثر على بديل في SoundCloud." if block else "خطأ غير متوقع."
    spawn(
        notify_owner(
            key,
            f"⚠️ تنبيه للمالك\n\nفشل طلب من أحد المستخدمين. {detail}\n"
            f"المكان: {where or '-'}\nالخطأ: {raw[:500]}",
        )
    )
    return fallback or PUBLIC_GENERIC


def _btn(text: str, data: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text, callback_data=data)


def control_keyboard(chat_id: int | None = None) -> InlineKeyboardMarkup:
    paused = chat_id is not None and chat_id in PAUSED
    return InlineKeyboardMarkup(
        [
            [
                _btn("▶️ استكمال" if paused else "⏸ إيقاف مؤقت", "toggle_cb"),
                _btn("⏭ تخطي", "skip_cb"),
            ],
            [_btn("📋 القائمة", "queue_cb"), _btn("⏹ إنهاء", "end_cb")],
        ]
    )


def support_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("✖️ إغلاق", callback_data="close")]]
    if DEVELOPER_CHANNEL:
        rows.insert(0, [InlineKeyboardButton("📢 قناة المطور", url=f"https://t.me/{DEVELOPER_CHANNEL}")])
    return InlineKeyboardMarkup(rows)


def now_playing_text(row: sqlite3.Row) -> str:
    source = "SoundCloud" if is_soundcloud_url(row["url"]) else "YouTube"
    waiting = queue_count(row["chat_id"])
    lines = [
        "🎵 KroMusic",
        "",
        f"العنوان: {row['title'] or 'بدون عنوان'}",
        f"المدة: `{readable_time(row['duration'])}`",
        f"بواسطة: {row['requester'] or 'منشور قناة'}",
        f"المصدر: {source}",
    ]
    if waiting:
        lines.append(f"في الانتظار: {waiting}")
    return "\n".join(lines)


async def update_now_playing(chat_id: int, row: sqlite3.Row) -> None:
    text = now_playing_text(row)
    old_id = NOW_PLAYING.get(chat_id)
    if old_id:
        try:
            await app.edit_message_text(chat_id, old_id, text, reply_markup=control_keyboard(chat_id))
            return
        except RPCError:
            NOW_PLAYING.pop(chat_id, None)
    try:
        msg = await app.send_message(chat_id, text, reply_markup=control_keyboard(chat_id))
        NOW_PLAYING[chat_id] = msg.id
        db_set_setting(f"np:{chat_id}", str(msg.id))
    except RPCError as exc:
        LOGGER.warning("Now playing message failed: %s", type(exc).__name__)
        spawn(report_cannot_send(chat_id, exc))


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
    PAUSED.discard(chat_id)
    a = CHAT_ASSISTANT.pop(chat_id, None)
    if a is not None and a.calls:
        try:
            await a.calls.leave_group_call(chat_id)
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


async def set_paused(chat_id: int, paused: bool) -> None:
    a = CHAT_ASSISTANT.get(chat_id)
    if a is None or not a.ready:
        raise UserError("لا يوجد تشغيل حالي في هذه المحادثة.")
    if paused:
        await a.calls.pause_stream(chat_id)
        PAUSED.add(chat_id)
    else:
        await a.calls.resume_stream(chat_id)
        PAUSED.discard(chat_id)


async def refresh_np_keyboard(chat_id: int) -> None:
    msg_id = NOW_PLAYING.get(chat_id)
    if not msg_id:
        return
    try:
        await app.edit_message_reply_markup(chat_id, msg_id, control_keyboard(chat_id))
    except RPCError:
        pass


async def action_skip(chat_id: int) -> None:
    # Keep the current stream alive while the next item is downloaded;
    # start_next() then switches the stream and removes the old file.
    conn = db_connect()
    try:
        conn.execute("DELETE FROM queue WHERE chat_id=? AND status='playing'", (chat_id,))
        conn.commit()
    finally:
        conn.close()
    await start_next(chat_id)


def queue_preview(chat_id: int) -> str:
    conn = db_connect()
    try:
        rows = conn.execute(
            "SELECT title FROM queue WHERE chat_id=? AND status='queued' ORDER BY position ASC, id ASC LIMIT 5",
            (chat_id,),
        ).fetchall()
    finally:
        conn.close()
    total = queue_count(chat_id)
    if not total:
        return "قائمة الانتظار فارغة."
    lines = [f"قائمة الانتظار ({total}):"]
    lines += [f"{i}. {(r['title'] or 'بدون عنوان')[:32]}" for i, r in enumerate(rows, 1)]
    if total > len(rows):
        lines.append(f"... و{total - len(rows)} أخرى")
    return "\n".join(lines)[:190]


def _participant_missing(exc: Exception) -> bool:
    msg = str(exc)
    low = msg.lower()
    return (
        "PARTICIPANT_JOIN_MISSING" in msg
        or "not in group call" in low
        or "not joined" in low
        or type(exc).__name__ == "NotInGroupCallError"
    )


async def _change_stream(a: Assistant, chat_id: int, stream) -> None:
    """Switch the stream. Telegram can briefly lose the participant state after a
    503/timeout from phone.JoinGroupCall (change_stream then raises
    PARTICIPANT_JOIN_MISSING), so rejoin the call instead of failing the track."""
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            await a.calls.change_stream(chat_id, stream)
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if not _participant_missing(exc) or attempt >= 2:
                raise
            LOGGER.warning(
                "Telegram lost the voice-chat participant state; rejoining before changing stream (attempt %s/3)",
                attempt + 1,
            )
            try:
                await a.calls.leave_group_call(chat_id)
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(1.5)
            await a.calls.join_group_call(chat_id, stream, stream_type=StreamType().pulse_stream)
            await asyncio.sleep(0.8)
    if last_exc is not None:
        raise last_exc


async def _join_call(a: Assistant, chat_id: int, stream) -> None:
    """Join the voice chat, retrying transient Telegram 503s without re-downloading."""
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            await a.calls.join_group_call(chat_id, stream, stream_type=StreamType().pulse_stream)
            # Give Telegram a short moment to commit the participant state.
            await asyncio.sleep(0.8)
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if _is_already_joined(exc):
                # Assistant is already in the call: just switch the stream.
                try:
                    await a.calls.change_stream(chat_id, stream)
                    return
                except Exception as exc2:  # noqa: BLE001
                    last_exc = exc2
            LOGGER.warning("Voice-chat join failed (attempt %s/3): %s", attempt + 1, type(exc).__name__)
            if attempt < 2:
                await asyncio.sleep(1.2 * (attempt + 1))
    assert last_exc is not None
    raise last_exc
async def start_next(chat_id: int) -> bool:
    """Download and start the next queued item without recursive retries.

    A single per-chat lock prevents concurrent stream transitions. Failed items
    are marked finished and the loop continues, avoiding recursion/deadlocks.
    """
    a = CHAT_ASSISTANT.get(chat_id)
    if a is None or not a.ready:
        a, _reason = await pick_assistant(chat_id)
        if a is None:
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
                file_path = await download_one(row["url"], row["title"] or "", int(row["duration"] or 0))
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
                        await _change_stream(a, chat_id, stream)
                    else:
                        # Try the chosen assistant; if it cannot join, fail over to
                        # another assistant that is an admin of this chat.
                        tried = [a]
                        while True:
                            try:
                                await _join_call(a, chat_id, stream)
                                break
                            except (NoActiveGroupCall, UnMuteNeeded):
                                raise
                            except Exception as join_exc:  # noqa: BLE001
                                alt = await next_assistant(chat_id, tried)
                                if alt is None:
                                    raise
                                LOGGER.warning(
                                    "Assistant %s could not join (%s); switching to %s",
                                    a.plain(), type(join_exc).__name__, alt.plain(),
                                )
                                spawn(
                                    notify_owner(
                                        f"assistant_join:{a.key}",
                                        f"⚠️ تنبيه للمالك\n\nتعذر على المساعد {a.plain()} دخول المكالمة "
                                        f"({type(join_exc).__name__})، تم التحويل تلقائيًا إلى {alt.plain()}.",
                                    )
                                )
                                a = alt
                                tried.append(alt)
                                CHAT_ASSISTANT[chat_id] = a

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
                PAUSED.discard(chat_id)
                CHAT_ASSISTANT[chat_id] = a
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
                # Users only see a neutral message; the real reason goes to the owner in private.
                await send_temp(
                    chat_id,
                    f"فشل تشغيل المقطع.\n\n{await public_error(exc, 'start_next')}",
                    ERROR_MESSAGE_SECONDS,
                )
                # Continue to the next queued item instead of recursively calling
                # start_next while its own lock is still held.
                continue


async def ensure_call_permissions(message: Message) -> tuple[bool, str]:
    if not ready_assistants():
        return False, "حساب المساعد غير متصل."
    if not await bot_is_admin(message.chat.id):
        return False, "يجب أن يكون البوت مشرفًا في هذه المحادثة."
    a, reason = await pick_assistant(message.chat.id)
    if a is None:
        return False, reason
    return True, ""


def _start_markup(message: Message) -> InlineKeyboardMarkup:
    markup = support_keyboard()
    if message.from_user and message.from_user.id == OWNER_ID and _is_private(message):
        return InlineKeyboardMarkup([[_btn("🎛 لوحة التحكم", "pn:home")]] + list(markup.inline_keyboard))
    return markup


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
        reply_markup=_start_markup(message),
    )


@app.on_message(filters.command("ping"))
async def ping_handler(_, message: Message):
    started = time.perf_counter()
    msg = await safe_reply(message, "جارِ الفحص...")
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
        await status.edit_text(f"فشل تجهيز المقطع.\n\n{await public_error(exc, 'play')}")
        schedule_delete(status, ERROR_MESSAGE_SECONDS)


async def is_admin_for_command(message: Message) -> bool:
    if message.from_user and message.from_user.id in SUDO_USERS:
        return True
    # For channel posts there is no reliable actor identity. Authorization is
    # based on bot/assistant administrator status for the configured chat.
    if message.chat.type == ChatType.CHANNEL:
        return await bot_is_admin(message.chat.id) and await any_assistant_admin(message.chat.id)
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
    if not ready_assistants():
        return await reply_temp(message, "حساب المساعد غير متصل.")
    a = CHAT_ASSISTANT.get(chat_id)
    if action in {"pause", "resume"} and (a is None or not a.ready):
        return await reply_temp(message, "لا يوجد تشغيل حالي في هذه المحادثة.")
    try:
        if action == "pause":
            await set_paused(chat_id, True)
            await refresh_np_keyboard(chat_id)
            return await reply_temp(message, "⏸ تم إيقاف التشغيل مؤقتًا.")
        if action == "resume":
            await set_paused(chat_id, False)
            await refresh_np_keyboard(chat_id)
            return await reply_temp(message, "▶️ تم استكمال التشغيل.")
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
        await reply_temp(message, f"تعذر تنفيذ الأمر.\n\n{await public_error(exc, 'control', 'حاول مرة أخرى بعد قليل.')}")


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
        await status.edit_text(f"فشل إرسال المقطع.\n\n{await public_error(exc, 'song')}")
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
        results, source_note = await search_results(query, 4)
        if not results:
            return await status.edit_text("لم يتم العثور على نتائج.")
        lines = []
        for i, item in enumerate(results, 1):
            title = item.get("title") or "بدون عنوان"
            duration = item.get("duration_string") or item.get("duration") or "غير معروف"
            url = item_url(item)
            lines.append(f"{i}. {title}\nالمدة: `{duration}`\nالرابط: {url}")
        await status.edit_text(source_note + "\n\n".join(lines), **no_preview())
        schedule_delete(status, SEARCH_RESULT_SECONDS)
    except Exception as exc:
        await status.edit_text(f"فشل البحث.\n\n{await public_error(exc, 'search')}")
        schedule_delete(status, ERROR_MESSAGE_SECONDS)
    finally:
        delete_command(message, 2)


async def safe_answer(query: CallbackQuery, text: str = "", alert: bool = False) -> None:
    try:
        await query.answer(text, show_alert=alert)
    except Exception:  # noqa: BLE001 - already answered / expired
        pass


@app.on_callback_query(filters.regex(r"^(pause_cb|resume_cb|toggle_cb|skip_cb|end_cb|queue_cb|close)$"))
async def callback_controls(_, query: CallbackQuery):
    data = query.data
    if data == "close":
        await query.answer()
        try:
            await query.message.delete()
        except RPCError:
            pass
        return
    if not query.message:
        return await query.answer()
    chat_id = query.message.chat.id
    if data == "queue_cb":  # anybody may look at the queue
        return await safe_answer(query, queue_preview(chat_id), True)
    # Callback queries carry the real actor separately; do not mutate the
    # Pyrogram Message object (its from_user field is not a safe writable field).
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
    if not ready_assistants():
        return await safe_answer(query, "حساب المساعد غير متصل.", True)
    try:
        if data in {"toggle_cb", "pause_cb", "resume_cb"}:
            want = {"pause_cb": True, "resume_cb": False}.get(data, chat_id not in PAUSED)
            await set_paused(chat_id, want)
            await refresh_np_keyboard(chat_id)
            return await safe_answer(query, "⏸ تم الإيقاف المؤقت" if want else "▶️ تم الاستكمال")
        if data == "skip_cb":
            await safe_answer(query, "⏭ جارٍ التخطي...")
            return await action_skip(chat_id)
        await safe_answer(query, "⏹ تم إنهاء التشغيل")
        await leave_and_clear(chat_id)
    except Exception as exc:
        LOGGER.exception("Callback control failed: %s", type(exc).__name__)
        await safe_answer(query, await public_error(exc, "callback", "حاول مرة أخرى بعد قليل."), True)


# ===========================================================================
#  OWNER CONTROL PANEL  (/panel)  - private chat with the owner only.
#  One message that is edited in place. Callback data: "pn:<action>[:<arg>]".
# ===========================================================================
_TEST_VIDEO_URL = "https://www.youtube.com/watch?v=jNQXAC9IVRE"
PANEL_PAGE_SIZE = 8


def _onoff(value: bool) -> str:
    return "✅ مفعّل" if value else "⛔ معطّل"


def _mb(size: int) -> str:
    return f"{size / (1024 * 1024):.1f} MB"


def _yt_state() -> str:
    if youtube_blocked():
        mins = max(1, (youtube_block_remaining() + 59) // 60)
        return f"🚫 محظور (إعادة التجربة بعد {mins} د)"
    return "✅ لا يوجد حظر معروف"


def _find_assistant(key: str) -> Assistant | None:
    return next((a for a in ASSISTANTS if a.key == key), None)


def _temp_stats() -> tuple[int, int]:
    count = size = 0
    for directory in (DOWNLOADS_DIR, CACHE_DIR):
        for p in directory.iterdir():
            try:
                if p.is_file():
                    count += 1
                    size += p.stat().st_size
            except OSError:
                continue
    return count, size


def _video_state() -> str:
    if not VIDEO_LOOP_FILE.exists():
        return "غير محدد"
    return _onoff(db_get_setting("video_enabled") == "1")


Screen = tuple[str, list[list[InlineKeyboardButton]]]


async def panel_show(query: CallbackQuery, text: str, rows: list[list[InlineKeyboardButton]]) -> None:
    try:
        await query.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=ParseMode.HTML,
            **no_preview(),
        )
    except RPCError as exc:
        if "MESSAGE_NOT_MODIFIED" not in str(exc).upper():
            LOGGER.warning("Panel edit failed: %s", str(exc)[:160])


async def screen_home() -> Screen:
    ready, total = len(ready_assistants()), len(ASSISTANTS)
    warn = "\n⚠️ <b>لا يوجد مساعد متصل، لن يعمل التشغيل.</b>" if ready == 0 else ""
    text = (
        f"<b>🎛 لوحة تحكم {_esc(BOT_NAME)}</b>\n\n"
        f"🤖 المساعدون: <b>{ready}/{total}</b> متصل\n"
        f"🎧 تشغيل نشط: <b>{len(IN_CALL)}</b> محادثة\n"
        f"▶️ يوتيوب: {_yt_state()}\n"
        f"☁️ SoundCloud: {_onoff(soundcloud_enabled())}"
        f"{warn}\n\nاختر قسمًا:"
    )
    rows = [
        [_btn("📊 الحالة", "pn:status"), _btn("🎧 التشغيل الآن", "pn:calls")],
        [_btn("🤖 المساعدون", "pn:as"), _btn("🌐 المصادر", "pn:src")],
        [_btn("💬 المحادثات", "pn:chats"), _btn("⚙️ الإعدادات", "pn:set")],
        [_btn("🛠 الصيانة", "pn:maint"), _btn("✖️ إغلاق", "pn:close")],
    ]
    return text, rows


async def screen_status() -> Screen:
    queued, playing = db_queue_totals()
    files, size = _temp_stats()
    text = (
        "<b>📊 حالة البوت</b>\n\n"
        f"⏱ مدة التشغيل: <b>{readable_time(int(time.time() - START_TIME))}</b>\n"
        f"🤖 المساعدون: <b>{len(ready_assistants())}/{len(ASSISTANTS)}</b> متصل\n"
        f"🎧 محادثات نشطة: <b>{len(IN_CALL)}</b>\n"
        f"📋 في الانتظار: <b>{queued}</b> | قيد التشغيل: <b>{playing}</b>\n\n"
        f"▶️ يوتيوب: {_yt_state()}\n"
        f"☁️ SoundCloud: {_onoff(soundcloud_enabled())}\n"
        f"🍪 كوكيز: <b>{len([p for p in YTDLP_COOKIE_FILES if os.path.isfile(p)])}</b> | "
        f"🌐 بروكسي: <b>{len(YTDLP_PROXIES)}</b>\n\n"
        f"🎬 فيديو العرض: {_video_state()}\n"
        f"🗂 ملفات مؤقتة: <b>{files}</b> ({_mb(size)})\n"
        f"🔒 الوضع: {'المسموح فقط' if get_flag('allowlist') else 'مفتوح للجميع'}"
    )
    return text, [[_btn("🔄 تحديث", "pn:status"), _btn("🔙 رجوع", "pn:home")]]


async def screen_calls() -> Screen:
    chats = sorted(IN_CALL)[:PANEL_PAGE_SIZE]
    titles = await asyncio.gather(*(chat_title(c) for c in chats))
    blocks = ["<b>🎧 التشغيل الآن</b>"]
    rows: list[list[InlineKeyboardButton]] = []
    if not chats:
        blocks.append("لا يوجد تشغيل نشط حاليًا.")
    for i, (c, title) in enumerate(zip(chats, titles), 1):
        cur = db_playing_row(c)
        a = CHAT_ASSISTANT.get(c)
        track = _esc((cur["title"] if cur else "") or "—")
        paused = c in PAUSED
        blocks.append(
            f"<b>{i}. {_esc(title)}</b>\n{'⏸' if paused else '▶️'} {track}\n"
            f"🤖 {_esc(a.plain() if a else '-')} • ⏳ {queue_count(c)} بالانتظار"
        )
        rows.append(
            [
                _btn(f"{'▶️' if paused else '⏸'} {i}", f"pn:cpause:{c}"),
                _btn(f"⏭ {i}", f"pn:cskip:{c}"),
                _btn(f"⏹ {i}", f"pn:cstop:{c}"),
            ]
        )
    if len(IN_CALL) > len(chats):
        blocks.append(f"... و{len(IN_CALL) - len(chats)} محادثات أخرى")
    rows.append([_btn("🔄 تحديث", "pn:calls"), _btn("🔙 رجوع", "pn:home")])
    return "\n\n".join(blocks), rows


async def screen_assistants(note: str = "") -> Screen:
    lines = ["<b>🤖 حسابات المساعد</b>", ""]
    if not ASSISTANTS:
        lines.append("لا توجد حسابات. أضف حسابًا ليعمل التشغيل.")
    else:
        lines.append(f"المتصل: <b>{len(ready_assistants())}/{len(ASSISTANTS)}</b>. اضغط على حساب لإدارته.")
    if note:
        lines += ["", note]
    rows = [
        [_btn(f"{'🟢' if a.ready else '🔴'} {a.plain()} • {assistant_load(a)} نشط", f"pn:asv:{a.key}")]
        for a in ASSISTANTS
    ]
    rows.append([_btn("➕ إضافة حساب", "pn:asadd"), _btn("🔄 إعادة اتصال الكل", "pn:asre")])
    rows.append([_btn("🔙 رجوع", "pn:home")])
    return "\n".join(lines), rows


async def screen_assistant(key: str) -> Screen:
    a = _find_assistant(key)
    if a is None:
        return await screen_assistants("الحساب غير موجود.")
    active = [c for c, x in CHAT_ASSISTANT.items() if x is a and c in IN_CALL]
    titles = await asyncio.gather(*(chat_title(c) for c in active))
    state = "🟢 متصل" if a.ready else f"🔴 غير متصل{f' ({_esc(a.error)})' if a.error else ''}"
    src = "متغيرات الاستضافة" if a.source == "env" else "أُضيف من اللوحة"
    lines = [
        f"<b>🤖 {_esc(a.display())}</b>",
        "",
        f"الحالة: {state}",
        f"الاسم: {_esc(a.name or '-')}",
        f"المعرّف: <code>{a.user_id or '-'}</code>",
        f"المصدر: {src}",
        f"محادثات نشطة: <b>{len(active)}</b>",
    ]
    lines += [f"• {_esc(t)}" for t in titles[:6]]
    rows = [[_btn("🔄 إعادة اتصال", f"pn:asre1:{key}")]]
    if a.source == "db":
        rows[0].append(_btn("🗑 حذف", f"pn:asdel:{key}"))
    else:
        lines += ["", "ℹ️ هذا الحساب من متغيرات الاستضافة، يُحذف من إعدادات Railway."]
    rows.append([_btn("🔙 الحسابات", "pn:as")])
    return "\n".join(lines), rows


async def screen_sources(note: str = "") -> Screen:
    cookies = len([p for p in YTDLP_COOKIE_FILES if os.path.isfile(p)])
    text = (
        "<b>🌐 مصادر التشغيل</b>\n\n"
        f"▶️ يوتيوب: {_yt_state()}\n"
        f"☁️ SoundCloud (بديل تلقائي): {_onoff(soundcloud_enabled())}\n"
        f"🍪 حسابات الكوكيز: <b>{cookies}</b>\n"
        f"🌐 البروكسيات: <b>{len(YTDLP_PROXIES)}</b>\n\n"
        "عند حظر يوتيوب يتحول البوت تلقائيًا إلى SoundCloud دون أي إشعار في المجموعات."
    )
    if cookies == 0 and not YTDLP_PROXIES:
        text += "\n\n💡 للحل الدائم أضف YTDLP_COOKIES_B64 أو YTDLP_PROXIES في Railway."
    if note:
        text += f"\n\n<b>نتيجة الاختبار:</b>\n{note}"
    rows = [
        [_btn(f"☁️ SoundCloud: {'تعطيل' if soundcloud_enabled() else 'تفعيل'}", "pn:togs:soundcloud")],
        [_btn("🧪 اختبار يوتيوب", "pn:testyt"), _btn("🧪 اختبار SoundCloud", "pn:testsc")],
        [_btn("🔁 إعادة تجربة يوتيوب الآن", "pn:ytreset")],
        [_btn("🔙 رجوع", "pn:home")],
    ]
    return text, rows


_TOGGLE_LABELS = {
    "auto_delete": "🧹 حذف أوامر المستخدمين",
    "owner_alerts": "🔔 تنبيهات المالك",
    "soundcloud": "☁️ بديل SoundCloud",
    "allowlist": "🔒 المحادثات المسموحة فقط",
}


async def screen_settings() -> Screen:
    text = (
        "<b>⚙️ الإعدادات</b>\n\n"
        "اضغط على أي إعداد لتبديله. تُحفظ التغييرات وتبقى بعد إعادة التشغيل.\n\n"
        "🧹 حذف أوامر المستخدمين: يمسح رسالة الأمر بعد تنفيذه.\n"
        "🔔 تنبيهات المالك: رسائل الأعطال التي تصلك في الخاص.\n"
        "🔒 المسموحة فقط: لا يعمل البوت إلا في المحادثات التي توافق عليها.\n"
        "🎬 فيديو العرض: يُعيَّن بإرسال وسيط مع /setvideo."
    )
    rows = [[_btn(f"{label}: {'✅' if get_flag(key) else '⛔'}", f"pn:tog:{key}")] for key, label in _TOGGLE_LABELS.items()]
    rows.append([_btn(f"🎬 فيديو العرض: {_video_state()}", "pn:tog:video")])
    rows.append([_btn("🔙 رجوع", "pn:home")])
    return text, rows


_CHAT_ICON = {1: "✅", 0: "🚫", 2: "⏳"}
_CHAT_LABEL = {1: "مسموحة", 0: "محظورة", 2: "بانتظار الموافقة"}


async def screen_chats() -> Screen:
    rows_db = db_chats()
    pending = sum(1 for r in rows_db if int(r["enabled"]) == 2)
    mode = get_flag("allowlist")
    text = (
        "<b>💬 المحادثات</b>\n\n"
        f"الوضع: {'🔒 المسموحة فقط' if mode else '🔓 مفتوح للجميع'}\n"
        f"المسجّلة: <b>{len(rows_db)}</b>" + (f" | بانتظار الموافقة: <b>{pending}</b>" if pending else "")
    )
    shown = rows_db[:PANEL_PAGE_SIZE]
    titles = await asyncio.gather(*(chat_title(int(r["chat_id"])) for r in shown))
    rows = [
        [_btn(f"{_CHAT_ICON.get(int(r['enabled']), '❔')} {t[:30]}", f"pn:chat:{r['chat_id']}")]
        for r, t in zip(shown, titles)
    ]
    if len(rows_db) > len(shown):
        text += f"\n\nيظهر أحدث {len(shown)} من {len(rows_db)}."
    if not rows_db:
        text += "\n\nلا توجد محادثات مسجّلة بعد. تُسجَّل تلقائيًا عند أول استخدام."
    text += "\n\nتلميح: أرسل /allow أو /deny داخل أي مجموعة للتحكم بها مباشرة."
    rows.append([_btn("➕ إضافة بالمعرّف", "pn:chadd"), _btn("🔒 تبديل الوضع" if not mode else "🔓 تبديل الوضع", "pn:chmode")])
    rows.append([_btn("🔙 رجوع", "pn:home")])
    return text, rows


async def screen_chat(chat_id: int) -> Screen:
    row = db_chat(chat_id)
    if row is None:
        return await screen_chats()
    enabled = int(row["enabled"])
    title = await chat_title(chat_id)
    text = (
        f"<b>💬 {_esc(title)}</b>\n\n"
        f"المعرّف: <code>{chat_id}</code>\n"
        f"الحالة: {_CHAT_ICON.get(enabled, '❔')} {_CHAT_LABEL.get(enabled, '-')}\n"
        f"تشغيل نشط الآن: {'نعم' if chat_id in IN_CALL else 'لا'}"
    )
    rows = [
        [_btn("✅ سماح", f"pn:chset:{chat_id}:1"), _btn("🚫 حظر", f"pn:chset:{chat_id}:0")],
        [_btn("🗑 حذف من السجل", f"pn:chdel:{chat_id}")],
        [_btn("🔙 المحادثات", "pn:chats")],
    ]
    return text, rows


async def screen_maintenance(note: str = "") -> Screen:
    queued, _playing = db_queue_totals()
    files, size = _temp_stats()
    text = (
        "<b>🛠 الصيانة</b>\n\n"
        f"🗂 ملفات مؤقتة: <b>{files}</b> ({_mb(size)})\n"
        f"📋 مقاطع في الانتظار: <b>{queued}</b>"
    )
    if note:
        text += f"\n\n{note}"
    rows = [
        [_btn("🧹 تنظيف الملفات المؤقتة", "pn:mclean")],
        [_btn("🗑 مسح كل قوائم الانتظار", "pn:mclearq")],
        [_btn("🔄 إعادة اتصال كل المساعدين", "pn:asre")],
        [_btn("🔙 رجوع", "pn:home")],
    ]
    return text, rows


def _confirm(text: str, yes: str, no: str) -> Screen:
    return text, [[_btn("✅ نعم، تأكيد", yes), _btn("❌ إلغاء", no)]]


async def _cancel_owner_flows() -> None:
    PANEL_INPUT.pop(OWNER_ID, None)
    state = LOGIN_STATE.pop(OWNER_ID, None)
    if state and state.get("client"):
        try:
            await state["client"].disconnect()
        except Exception:  # noqa: BLE001
            pass


async def _test_youtube() -> str:
    t0 = time.monotonic()
    try:
        await asyncio.wait_for(asyncio.to_thread(youtube_info, _TEST_VIDEO_URL), 75)
        note_youtube_ok()
        return f"✅ يوتيوب يعمل ({time.monotonic() - t0:.1f} ث)"
    except Exception as exc:  # noqa: BLE001
        if _is_bot_check(exc):
            return "🚫 يوتيوب يرفض هذا الخادم (Bot check). التحويل إلى SoundCloud فعّال."
        return f"⚠️ فشل الاختبار: {_esc(str(exc)[:160])}"


async def _test_soundcloud() -> str:
    t0 = time.monotonic()
    try:
        results = await asyncio.wait_for(asyncio.to_thread(soundcloud_search, "music", 1), 40)
        return f"✅ SoundCloud يعمل ({time.monotonic() - t0:.1f} ث) • {_esc((results[0].get('title') or '')[:40])}"
    except Exception as exc:  # noqa: BLE001
        return f"⚠️ فشل الاختبار: {_esc(str(exc)[:160])}"


async def _reconnect(a: Assistant) -> bool:
    await stop_assistant(a)
    return await start_assistant(a)


async def panel_dispatch(query: CallbackQuery, action: str, arg: str) -> str | None:
    """Run one panel action, render the resulting screen, return an optional toast."""
    toast: str | None = None
    screen: Screen

    if action == "home":
        screen = await screen_home()
    elif action == "close":
        try:
            await query.message.delete()
        except RPCError:
            pass
        return None
    elif action == "status":
        screen = await screen_status()
    elif action == "calls":
        screen = await screen_calls()
    elif action in {"cpause", "cskip", "cstop"}:
        chat_id = int(arg)
        if action == "cpause":
            await set_paused(chat_id, chat_id not in PAUSED)
            await refresh_np_keyboard(chat_id)
            toast = "⏸ تم الإيقاف المؤقت" if chat_id in PAUSED else "▶️ تم الاستكمال"
        elif action == "cskip":
            await panel_show(query, "⏳ جارٍ التخطي...", [])
            await action_skip(chat_id)
            toast = "⏭ تم التخطي"
        else:
            await leave_and_clear(chat_id)
            toast = "⏹ تم إنهاء التشغيل"
        screen = await screen_calls()
    elif action == "as":
        screen = await screen_assistants()
    elif action == "asv":
        screen = await screen_assistant(arg)
    elif action == "asadd":
        if len(ASSISTANTS) >= MAX_ASSISTANTS:
            return f"وصلت للحد الأقصى ({MAX_ASSISTANTS})"
        LOGIN_STATE[OWNER_ID] = {"step": "phone", "created": time.monotonic()}
        screen = (
            "<b>➕ إضافة حساب مساعد</b>\n\nأرسل رقم هاتف الحساب بصيغة دولية، مثل <code>+9665XXXXXXXX</code>.\n"
            "سيصلك رمز التحقق على الحساب نفسه.",
            [[_btn("❌ إلغاء", "pn:ascancel")]],
        )
    elif action == "ascancel":
        await _cancel_owner_flows()
        screen = await screen_assistants("تم إلغاء العملية.")
    elif action == "asre":
        await panel_show(query, "⏳ جارٍ إعادة اتصال المساعدين...", [])
        results = [await _reconnect(a) for a in list(ASSISTANTS)]
        toast = f"تم: {sum(results)}/{len(results)} متصل"
        screen = await screen_assistants()
    elif action == "asre1":
        a = _find_assistant(arg)
        if a is None:
            screen = await screen_assistants("الحساب غير موجود.")
        else:
            await panel_show(query, f"⏳ جارٍ إعادة اتصال {_esc(a.plain())}...", [])
            toast = "✅ تم الاتصال" if await _reconnect(a) else "❌ فشل الاتصال"
            screen = await screen_assistant(arg)
    elif action == "asdel":
        a = _find_assistant(arg)
        if a is None or a.source != "db":
            screen = await screen_assistants("لا يمكن حذف هذا الحساب من هنا.")
        else:
            screen = _confirm(
                f"هل تريد حذف الحساب <b>{_esc(a.plain())}</b>؟\nسيتوقف أي تشغيل يعتمد عليه وتُحذف جلسته.",
                f"pn:asdelok:{arg}",
                f"pn:asv:{arg}",
            )
    elif action == "asdelok":
        a = _find_assistant(arg)
        if a is not None and a.source == "db":
            name = a.plain()
            await remove_assistant(a)
            screen = await screen_assistants(f"🗑 تم حذف {_esc(name)}.")
        else:
            screen = await screen_assistants("الحساب غير موجود.")
    elif action == "src":
        screen = await screen_sources()
    elif action == "togs":
        set_flag("soundcloud", not get_flag("soundcloud"))
        toast = f"SoundCloud: {'مفعّل' if get_flag('soundcloud') else 'معطّل'}"
        screen = await screen_sources()
    elif action == "testyt":
        await panel_show(query, "⏳ جارٍ اختبار يوتيوب (قد يستغرق دقيقة)...", [])
        screen = await screen_sources(await _test_youtube())
    elif action == "testsc":
        await panel_show(query, "⏳ جارٍ اختبار SoundCloud...", [])
        screen = await screen_sources(await _test_soundcloud())
    elif action == "ytreset":
        reset_youtube_block()
        toast = "سيُجرَّب يوتيوب في الطلب القادم"
        screen = await screen_sources()
    elif action == "set":
        screen = await screen_settings()
    elif action == "tog":
        if arg == "video":
            if not VIDEO_LOOP_FILE.exists():
                return "لم يُعيَّن فيديو بعد. أرسل وسيطًا مع /setvideo"
            db_set_setting("video_enabled", "0" if db_get_setting("video_enabled") == "1" else "1")
        elif arg in _FLAG_DEFAULTS:
            set_flag(arg, not get_flag(arg))
            if arg == "allowlist" and get_flag(arg):
                toast = "المحادثات الجديدة ستحتاج موافقتك"
        screen = await screen_settings()
    elif action == "chats":
        screen = await screen_chats()
    elif action == "chmode":
        set_flag("allowlist", not get_flag("allowlist"))
        toast = "🔒 المسموحة فقط" if get_flag("allowlist") else "🔓 مفتوح للجميع"
        screen = await screen_chats()
    elif action == "chadd":
        PANEL_INPUT[OWNER_ID] = {"kind": "chat_add", "created": time.monotonic()}
        screen = (
            "<b>➕ إضافة محادثة</b>\n\nأرسل معرّف المحادثة (رقم يبدأ عادةً بـ <code>-100</code>).\n"
            "أو أرسل /allow داخل المجموعة مباشرة.",
            [[_btn("❌ إلغاء", "pn:chats_cancel")]],
        )
    elif action == "chats_cancel":
        PANEL_INPUT.pop(OWNER_ID, None)
        screen = await screen_chats()
    elif action == "chat":
        screen = await screen_chat(int(arg))
    elif action == "chset":
        cid, val = arg.rsplit(":", 1)
        chat_id, enabled = int(cid), int(val)
        row = db_chat(chat_id)
        db_set_chat(chat_id, row["chat_type"] if row else "group", enabled)
        if enabled == 0 and chat_id in IN_CALL:
            await leave_and_clear(chat_id)
        toast = "✅ تم السماح" if enabled else "🚫 تم الحظر"
        screen = await screen_chat(chat_id)
    elif action == "chdel":
        db_del_chat(int(arg))
        toast = "🗑 تم الحذف من السجل"
        screen = await screen_chats()
    elif action == "maint":
        screen = await screen_maintenance()
    elif action == "mclean":
        protected: set[Path] = set()
        for f in CURRENT_FILES.values():
            try:
                protected.add(Path(f).resolve())
                protected.add(Path(f).with_suffix(".vid.mkv").resolve())
            except OSError:
                pass
        removed = await asyncio.to_thread(cleanup_temp_files, 120, protected)
        screen = await screen_maintenance(f"🧹 تم حذف <b>{removed}</b> ملف مؤقت.")
    elif action == "mclearq":
        queued, _p = db_queue_totals()
        screen = _confirm(
            f"سيتم مسح <b>{queued}</b> مقطعًا من قوائم الانتظار في كل المحادثات. المقطع الجاري تشغيله لا يتأثر.",
            "pn:mclearqok",
            "pn:maint",
        )
    elif action == "mclearqok":
        conn = db_connect()
        try:
            cur = conn.execute("DELETE FROM queue WHERE status='queued'")
            conn.commit()
            cleared = cur.rowcount
        finally:
            conn.close()
        screen = await screen_maintenance(f"🗑 تم مسح <b>{cleared}</b> مقطعًا من قوائم الانتظار.")
    else:
        screen = await screen_home()

    await panel_show(query, *screen)
    return toast


@app.on_callback_query(filters.regex(r"^pn:"))
async def panel_callbacks(_, query: CallbackQuery):
    if query.from_user.id != OWNER_ID:
        return await query.answer("هذا الخيار للمالك فقط.", show_alert=True)
    parts = query.data.split(":", 2)
    action = parts[1] if len(parts) > 1 else "home"
    arg = parts[2] if len(parts) > 2 else ""
    toast: str | None = None
    try:
        toast = await panel_dispatch(query, action, arg)
    except UserError as exc:
        toast = str(exc)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Panel action %s failed: %s", action, type(exc).__name__)
        toast = f"تعذر التنفيذ: {type(exc).__name__}"
    await safe_answer(query, toast or "")


@app.on_message(
    filters.command(["panel", "لوحة", "لوحة_التحكم", "المساعد", "assistants", "admin"])
    & filters.user(OWNER_ID)
    & filters.private
)
async def panel_command(_, message: Message):
    await _cancel_owner_flows()
    text, rows = await screen_home()
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)


@app.on_message(filters.text & filters.user(OWNER_ID) & filters.private, group=51)
async def panel_input(_, message: Message):
    state = PANEL_INPUT.get(OWNER_ID)
    if not state or message.text.startswith("/"):
        return
    if time.monotonic() - state.get("created", time.monotonic()) > LOGIN_TIMEOUT:
        PANEL_INPUT.pop(OWNER_ID, None)
        return await message.reply_text("انتهت المهلة. افتح /panel من جديد.")
    if state.get("kind") == "chat_add":
        raw = message.text.strip()
        if not re.fullmatch(r"-?\d{5,20}", raw):
            return await message.reply_text("المعرّف غير صحيح. أرسل رقمًا مثل -1001234567890 أو /cancel للإلغاء.")
        chat_id = int(raw)
        PANEL_INPUT.pop(OWNER_ID, None)
        db_set_chat(chat_id, "group", 1)
        text, rows = await screen_chat(chat_id)
        await message.reply_text(
            "✅ تمت إضافة المحادثة وتفعيلها.\n\n" + text,
            reply_markup=InlineKeyboardMarkup(rows),
            parse_mode=ParseMode.HTML,
        )


@app.on_message(filters.command(["allow", "سماح", "deny", "حظر_المحادثة"]) & filters.group)
async def chat_access_command(_, message: Message):
    """Owner / sudo users control the current group directly: /allow or /deny."""
    try:
        if not message.from_user or not is_admin_user(message.from_user.id):
            return
        allow = message.command[0].lower() in {"allow", "سماح"}
        db_set_chat(message.chat.id, str(message.chat.type), 1 if allow else 0)
        if message.chat.title:
            CHAT_TITLES[message.chat.id] = message.chat.title
        if not allow and message.chat.id in IN_CALL:
            await leave_and_clear(message.chat.id)
        await reply_temp(message, "✅ تم السماح للمحادثة." if allow else "🚫 تم حظر المحادثة.")
    finally:
        delete_command(message)


@app.on_message(filters.command("cancel") & filters.user(OWNER_ID))
async def cancel_login(_, message: Message):
    PANEL_INPUT.pop(OWNER_ID, None)
    state = LOGIN_STATE.pop(OWNER_ID, None)
    if state and state.get("client"):
        try:
            await state["client"].disconnect()
        except Exception:
            pass
    await message.reply_text("تم إلغاء العملية.")


@app.on_message(filters.text & filters.user(OWNER_ID) & filters.private, group=50)
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
        return await message.reply_text("انتهت مهلة تسجيل الدخول. ابدأ العملية من /panel.")
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
                    return await message.reply_text("انتهت صلاحية الرمز. أعد العملية من /panel.")
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
    try:
        me = await client.get_me()
        session = await client.export_session_string()
    finally:
        try:
            await client.disconnect()
        except Exception:  # noqa: BLE001
            pass
    LOGIN_STATE.pop(OWNER_ID, None)
    if any(a.user_id == me.id for a in ASSISTANTS if a.user_id):
        return await message.reply_text("هذا الحساب مضاف مسبقًا كمساعد.")
    db_id = db_add_assistant(session, me.id, me.username or "")
    a = Assistant(f"db:{db_id}", session, "db", db_id=db_id, user_id=me.id, username=me.username or "")
    ASSISTANTS.append(a)
    if await start_assistant(a):
        await message.reply_text(
            f"تمت إضافة حساب المساعد: {a.display()}\n"
            f"الحسابات المتصلة الآن: {len(ready_assistants())}\n\n"
            "أضفه كمشرف في المجموعات أو القنوات التي تريد التشغيل فيها.\n"
            "لإدارة الحسابات: /panel"
        )
    else:
        await remove_assistant(a)
        await message.reply_text("تم إنشاء الجلسة لكن تعذر تشغيل حساب المساعد.")


def register_pytgcalls_handlers(a: Assistant) -> None:
    calls = a.calls
    if not calls:
        return

    async def _stream_end(_, update: Any) -> None:
        await on_stream_end(a, update)

    async def _closed(_, update: Any) -> None:
        await on_call_closed(a, update)

    try:
        calls.on_stream_end()(_stream_end)
        calls.on_left()(_closed)
        calls.on_kicked()(_closed)
        calls.on_closed_voice_chat()(_closed)
    except Exception as exc:
        LOGGER.warning("Could not register PyTgCalls handlers: %s", type(exc).__name__)


async def on_stream_end(a: Assistant, update: Any) -> None:
    chat_id = update.chat_id
    cur = CHAT_ASSISTANT.get(chat_id)
    if cur is not None and cur is not a:
        return  # another assistant serves this chat now
    old = CURRENT_FILES.pop(chat_id, None)
    cleanup_file(old)
    conn = db_connect()
    try:
        conn.execute("DELETE FROM queue WHERE chat_id=? AND status='playing'", (chat_id,))
        conn.commit()
    finally:
        conn.close()
    await start_next(chat_id)


async def on_call_closed(a: Assistant, update: Any) -> None:
    chat_id = getattr(update, "chat_id", None)
    if chat_id is None:
        return
    cur = CHAT_ASSISTANT.get(chat_id)
    if cur is not None and cur is not a:
        return
    old = CURRENT_FILES.pop(chat_id, None)
    cleanup_file(old)
    IN_CALL.discard(chat_id)
    PAUSED.discard(chat_id)
    CHAT_ASSISTANT.pop(chat_id, None)
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
    await start_all_assistants()
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
    await stop_all_assistants()
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
