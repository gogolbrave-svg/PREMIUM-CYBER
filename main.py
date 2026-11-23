import asyncio
import datetime
import logging
import os
import random
import re
import sqlite3
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    Message,
)
from aiohttp import ClientSession, web
from dotenv import load_dotenv

try:
  import asyncpg
except ImportError:
  asyncpg = None

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
CRYPTO_BOT_TOKEN = os.getenv("CRYPTO_BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID_env = os.getenv("ADMIN_ID")
ADMIN_ID = int(ADMIN_ID_env) if ADMIN_ID_env and ADMIN_ID_env.isdigit() else 0
MANAGER_ID_env = os.getenv("MANAGER_ID")
MANAGER_ID = int(MANAGER_ID_env) if MANAGER_ID_env and MANAGER_ID_env.isdigit() else 0

if not BOT_TOKEN:
  raise ValueError("❌ BOT_TOKEN не найден!")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PG_POOL = None


class OrderForm(StatesGroup):
  service = State()
  target_username = State()
  special_link = State()
  receipt = State()


class SupportForm(StatesGroup):
  reason = State()
  photo = State()


class ClientThreadReplyForm(StatesGroup):
  waiting_reply = State()


class StaffQuickReplyForm(StatesGroup):
  waiting_reply = State()


# Словари и кэши в памяти (синхронизируются с БД)
KNOWN_USERS = {}
LAST_BROADCAST = {}
USER_MAPPING = {}
ABANDONED_CHECKOUTS = {}
CARD_DETAILS = []
CARD_POINTERS = {}
CRYPTO_WALLETS = []
CRYPTO_POINTERS = {}
BANNER_CACHE = {}
PENDING_BILLS = {}

# Активные треды / обращения и отслеживание шагов клиентов
ACTIVE_SEND_THREADS = {}  # client_id -> {"task": asyncio.Task, "expires_at": datetime.datetime, "closed": bool}
CLIENT_JOURNEY = {}  # user_id -> {"max_step": str, "step_rank": int, "service_name": str, "price": float|int, "target": str, "updated_at": str}
UNPAID_FOLLOWUP_TASKS = {}  # user_id -> asyncio.Task
CLOSED_TICKETS = set()  # (client_id, ticket_kind)

STEP_RANK = {
    "catalog": 1,
    "service_selected": 2,
    "target_entered": 3,
    "summary": 4,
    "payment_menu": 5,
    "payment_method_selected": 6,
    "receipt_requested": 7,
}

STEP_LABELS = {
    "catalog": "Каталог услуг",
    "service_selected": "Выбор услуги / описание задачи",
    "target_entered": "Ввод данных цели / ссылки",
    "summary": "Сводка заказа",
    "payment_menu": "Переход к оплате",
    "payment_method_selected": "Выбор способа / валюты / сети оплаты",
    "receipt_requested": "Ожидание отправки чека",
}

router = Router()


# === ИНИЦИАЛИЗАЦИЯ БД (PostgreSQL с fallback на SQLite) ===
def _init_sqlite_db():
  with sqlite3.connect("bot_database.db") as conn:
    cursor = conn.cursor()
    cursor.execute(
        """
          CREATE TABLE IF NOT EXISTS orders (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER,
              service_name TEXT,
              total_price REAL,
              status TEXT,
              created_at TEXT,
              target_info TEXT
          )
      """
    )
    cursor.execute(
        """
          CREATE TABLE IF NOT EXISTS banned_users (
              user_id INTEGER PRIMARY KEY
          )
      """
    )
    cursor.execute(
        """
          CREATE TABLE IF NOT EXISTS users (
              user_id BIGINT PRIMARY KEY,
              username TEXT,
              last_seen TEXT,
              last_broadcast TEXT
          )
      """
    )
    cols = [row[1] for row in cursor.execute("PRAGMA table_info(users)").fetchall()]
    if "last_broadcast" not in cols:
      cursor.execute("ALTER TABLE users ADD COLUMN last_broadcast TEXT")

    cursor.execute(
        """
          CREATE TABLE IF NOT EXISTS card_details (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              details TEXT,
              bank TEXT,
              country TEXT,
              frozen_until TEXT
          )
      """
    )
    cursor.execute(
        """
          CREATE TABLE IF NOT EXISTS crypto_wallets (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              address TEXT,
              asset TEXT,
              network TEXT,
              frozen_until TEXT
          )
      """
    )
    cursor.execute(
        """
          CREATE TABLE IF NOT EXISTS pending_bills (
              user_id BIGINT PRIMARY KEY,
              service_name TEXT,
              total_price REAL,
              target_username TEXT
          )
      """
    )
    cursor.execute(
        """
          CREATE TABLE IF NOT EXISTS abandoned_reminders (
              user_id BIGINT PRIMARY KEY,
              remind_at TEXT
          )
      """
    )
    conn.commit()


async def init_db():
  global PG_POOL
  _init_sqlite_db()
  if DATABASE_URL and asyncpg:
    try:
      PG_POOL = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=1, max_size=5)
      async with PG_POOL.acquire() as conn:
        await conn.execute(
            """
              CREATE TABLE IF NOT EXISTS orders (
                  id SERIAL PRIMARY KEY,
                  user_id BIGINT,
                  service_name TEXT,
                  total_price DOUBLE PRECISION,
                  status TEXT,
                  created_at TEXT,
                  target_info TEXT
              )
          """
        )
        await conn.execute(
            """
              CREATE TABLE IF NOT EXISTS banned_users (
                  user_id BIGINT PRIMARY KEY
              )
          """
        )
        await conn.execute(
            """
              CREATE TABLE IF NOT EXISTS users (
                  user_id BIGINT PRIMARY KEY,
                  username TEXT,
                  last_seen TEXT,
                  last_broadcast TEXT
              )
          """
        )
        await conn.execute(
            """
              ALTER TABLE users ADD COLUMN IF NOT EXISTS last_broadcast TEXT
          """
        )
        await conn.execute(
            """
              CREATE TABLE IF NOT EXISTS card_details (
                  id SERIAL PRIMARY KEY,
                  details TEXT,
                  bank TEXT,
                  country TEXT,
                  frozen_until TEXT
              )
          """
        )
        await conn.execute(
            """
              CREATE TABLE IF NOT EXISTS crypto_wallets (
                  id SERIAL PRIMARY KEY,
                  address TEXT,
                  asset TEXT,
                  network TEXT,
                  frozen_until TEXT
              )
          """
        )
        await conn.execute(
            """
              CREATE TABLE IF NOT EXISTS pending_bills (
                  user_id BIGINT PRIMARY KEY,
                  service_name TEXT,
                  total_price DOUBLE PRECISION,
                  target_username TEXT
              )
          """
        )
        await conn.execute(
            """
              CREATE TABLE IF NOT EXISTS abandoned_reminders (
                  user_id BIGINT PRIMARY KEY,
                  remind_at TEXT
              )
          """
        )
      logger.info("✅ PostgreSQL подключена и таблицы инициализированы.")
    except Exception as e:
      logger.error(f"❌ Ошибка подключения к PostgreSQL, используется SQLite: {e}")
      PG_POOL = None


async def load_persistent_state(bot: Bot | None = None):
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      u_rows = await conn.fetch("SELECT user_id, username, last_seen, last_broadcast FROM users")
      for r in u_rows:
        uid = int(r["user_id"])
        USER_MAPPING[uid] = r["username"]
        if r["username"]:
          USER_MAPPING[r["username"].lower().lstrip("@")] = uid
        if r["last_seen"]:
          try:
            KNOWN_USERS[uid] = datetime.datetime.fromisoformat(r["last_seen"])
          except Exception:
            KNOWN_USERS[uid] = datetime.datetime.now()
        if r["last_broadcast"]:
          try:
            LAST_BROADCAST[uid] = datetime.datetime.fromisoformat(r["last_broadcast"])
          except Exception:
            pass

      c_rows = await conn.fetch("SELECT details, bank, country, frozen_until FROM card_details")
      CARD_DETAILS.clear()
      for r in c_rows:
        CARD_DETAILS.append({
            "details": r["details"],
            "bank": r["bank"],
            "country": r["country"],
            "frozen_until": datetime.datetime.fromisoformat(r["frozen_until"]) if r["frozen_until"] else None,
        })

      w_rows = await conn.fetch("SELECT address, asset, network, frozen_until FROM crypto_wallets")
      CRYPTO_WALLETS.clear()
      for r in w_rows:
        CRYPTO_WALLETS.append({
            "address": r["address"],
            "asset": r["asset"],
            "network": r["network"],
            "frozen_until": datetime.datetime.fromisoformat(r["frozen_until"]) if r["frozen_until"] else None,
        })

      b_rows = await conn.fetch("SELECT user_id, service_name, total_price, target_username FROM pending_bills")
      PENDING_BILLS.clear()
      for r in b_rows:
        PENDING_BILLS[int(r["user_id"])] = {
            "service_name": r["service_name"],
            "total_price": float(r["total_price"]),
            "target_username": r["target_username"],
        }

      rem_rows = await conn.fetch("SELECT user_id, remind_at FROM abandoned_reminders")
      reminders_data = [(int(r["user_id"]), r["remind_at"]) for r in rem_rows]
  else:
    with sqlite3.connect("bot_database.db") as conn:
      cursor = conn.cursor()
      for uid, uname, last_seen, last_broadcast in cursor.execute(
          "SELECT user_id, username, last_seen, last_broadcast FROM users"
      ).fetchall():
        USER_MAPPING[uid] = uname
        if uname:
          USER_MAPPING[uname.lower().lstrip("@")] = uid
        if last_seen:
          try:
            KNOWN_USERS[uid] = datetime.datetime.fromisoformat(last_seen)
          except Exception:
            KNOWN_USERS[uid] = datetime.datetime.now()
        if last_broadcast:
          try:
            LAST_BROADCAST[uid] = datetime.datetime.fromisoformat(last_broadcast)
          except Exception:
            pass

      CARD_DETAILS.clear()
      for details, bank, country, frozen_until in cursor.execute(
          "SELECT details, bank, country, frozen_until FROM card_details"
      ).fetchall():
        CARD_DETAILS.append({
            "details": details,
            "bank": bank,
            "country": country,
            "frozen_until": datetime.datetime.fromisoformat(frozen_until) if frozen_until else None,
        })

      CRYPTO_WALLETS.clear()
      for address, asset, network, frozen_until in cursor.execute(
          "SELECT address, asset, network, frozen_until FROM crypto_wallets"
      ).fetchall():
        CRYPTO_WALLETS.append({
            "address": address,
            "asset": asset,
            "network": network,
            "frozen_until": datetime.datetime.fromisoformat(frozen_until) if frozen_until else None,
        })

      PENDING_BILLS.clear()
      for uid, sname, price, tuser in cursor.execute(
          "SELECT user_id, service_name, total_price, target_username FROM pending_bills"
      ).fetchall():
        PENDING_BILLS[uid] = {
            "service_name": sname,
            "total_price": float(price),
            "target_username": tuser,
        }

      reminders_data = [
          (int(r[0]), r[1])
          for r in cursor.execute("SELECT user_id, remind_at FROM abandoned_reminders").fetchall()
      ]

  if bot:
    now = datetime.datetime.now()
    for uid, remind_at_str in reminders_data:
      try:
        remind_at = datetime.datetime.fromisoformat(remind_at_str)
        delay = max(0.0, (remind_at - now).total_seconds())
      except Exception:
        delay = 0.0
      old_task = ABANDONED_CHECKOUTS.pop(uid, None)
      if old_task:
        old_task.cancel()
      ABANDONED_CHECKOUTS[uid] = asyncio.create_task(
          delayed_abandoned_cart_reminder(bot, uid, delay_seconds=delay)
      )


async def save_user_to_db(user):
  if not user:
    return
  now_dt = datetime.datetime.now()
  now_iso = now_dt.isoformat()
  KNOWN_USERS[user.id] = now_dt
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      await conn.execute(
          """
            INSERT INTO users (user_id, username, last_seen, last_broadcast)
            VALUES ($1, $2, $3, NULL)
            ON CONFLICT (user_id) DO UPDATE
            SET username = COALESCE(EXCLUDED.username, users.username),
                last_seen = EXCLUDED.last_seen
        """,
          user.id,
          user.username,
          now_iso,
      )
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute(
          """
            INSERT INTO users (user_id, username, last_seen, last_broadcast)
            VALUES (?, ?, ?, NULL)
            ON CONFLICT(user_id) DO UPDATE SET
                username = COALESCE(excluded.username, users.username),
                last_seen = excluded.last_seen
        """,
          (user.id, user.username, now_iso),
      )
      conn.commit()


async def save_last_broadcast_db(user_id: int, dt: datetime.datetime):
  LAST_BROADCAST[user_id] = dt
  iso_str = dt.isoformat()
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      await conn.execute(
          """
            INSERT INTO users (user_id, username, last_seen, last_broadcast)
            VALUES ($1, NULL, $2, $2)
            ON CONFLICT (user_id) DO UPDATE
            SET last_broadcast = EXCLUDED.last_broadcast
        """,
          user_id,
          iso_str,
      )
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute(
          """
            INSERT INTO users (user_id, username, last_seen, last_broadcast)
            VALUES (?, NULL, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                last_broadcast = excluded.last_broadcast
        """,
          (user_id, iso_str, iso_str),
      )
      conn.commit()


async def save_abandoned_reminder_db(user_id: int, remind_at: datetime.datetime):
  iso_str = remind_at.isoformat()
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      await conn.execute(
          """
            INSERT INTO abandoned_reminders (user_id, remind_at)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET remind_at = EXCLUDED.remind_at
        """,
          user_id,
          iso_str,
      )
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute(
          """
            INSERT INTO abandoned_reminders (user_id, remind_at)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET remind_at = excluded.remind_at
        """,
          (user_id, iso_str),
      )
      conn.commit()


async def delete_abandoned_reminder_db(user_id: int):
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      await conn.execute("DELETE FROM abandoned_reminders WHERE user_id = $1", user_id)
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute("DELETE FROM abandoned_reminders WHERE user_id = ?", (user_id,))
      conn.commit()


async def add_order_to_db(
    user_id: int, service_name: str, total_price: float, status: str, target_info: str
):
  date_str = datetime.datetime.now().strftime("%d %B")
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      row = await conn.fetchrow(
          """
            INSERT INTO orders (user_id, service_name, total_price, status, created_at, target_info)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
        """,
          user_id,
          service_name,
          total_price,
          status,
          date_str,
          target_info,
      )
      return row["id"] if row else None
  with sqlite3.connect("bot_database.db") as conn:
    cursor = conn.cursor()
    cursor.execute(
        """
          INSERT INTO orders (user_id, service_name, total_price, status, created_at, target_info)
          VALUES (?, ?, ?, ?, ?, ?)
      """,
        (user_id, service_name, total_price, status, date_str, target_info),
    )
    conn.commit()
    return cursor.lastrowid


async def set_user_orders_in_work_db(user_id: int) -> int:
  new_status = "⚙️ В работе"
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      result = await conn.execute(
          "UPDATE orders SET status = $1 WHERE user_id = $2",
          new_status,
          user_id,
      )
      try:
        return int(result.split()[-1])
      except Exception:
        return 0
  else:
    with sqlite3.connect("bot_database.db") as conn:
      cursor = conn.cursor()
      cursor.execute(
          "UPDATE orders SET status = ? WHERE user_id = ?",
          (new_status, user_id),
      )
      conn.commit()
      return cursor.rowcount


async def get_user_orders_from_db(user_id: int):
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      rows = await conn.fetch(
          """
            SELECT id, service_name, total_price, status, created_at, target_info
            FROM orders WHERE user_id = $1 ORDER BY id DESC
        """,
          user_id,
      )
      return [
          {
              "id": r["id"],
              "service_name": r["service_name"],
              "total_price": r["total_price"],
              "status": r["status"],
              "created_at": r["created_at"],
              "target_info": r["target_info"],
          }
          for r in rows
      ]

  with sqlite3.connect("bot_database.db") as conn:
    cursor = conn.cursor()
    cursor.execute(
        """
          SELECT id, service_name, total_price, status, created_at, target_info 
          FROM orders WHERE user_id = ? ORDER BY id DESC
      """,
        (user_id,),
    )
    rows = cursor.fetchall()
    return [
        {
            "id": row[0],
            "service_name": row[1],
            "total_price": row[2],
            "status": row[3],
            "created_at": row[4],
            "target_info": row[5],
        }
        for row in rows
    ]


async def is_user_banned(user_id: int, username: str | None = None) -> bool:
  target_ids = {user_id}
  if username:
    mapped_id = USER_MAPPING.get(username.lower().lstrip("@"))
    if mapped_id:
      target_ids.add(int(mapped_id))

  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      for tid in target_ids:
        row = await conn.fetchrow("SELECT 1 FROM banned_users WHERE user_id = $1", tid)
        if row is not None:
          return True
      return False
  with sqlite3.connect("bot_database.db") as conn:
    cursor = conn.cursor()
    for tid in target_ids:
      cursor.execute("SELECT 1 FROM banned_users WHERE user_id = ?", (tid,))
      if cursor.fetchone() is not None:
        return True
    return False


async def ban_user_in_db(user_id: int):
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      await conn.execute("INSERT INTO banned_users (user_id) VALUES ($1) ON CONFLICT DO NOTHING", user_id)
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute("INSERT OR IGNORE INTO banned_users (user_id) VALUES (?)", (user_id,))
      conn.commit()


async def unban_user_in_db(user_id: int):
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      await conn.execute("DELETE FROM banned_users WHERE user_id = $1", user_id)
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute("DELETE FROM banned_users WHERE user_id = ?", (user_id,))
      conn.commit()


async def save_pending_bill_db(user_id: int, bill: dict):
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      await conn.execute(
          """
            INSERT INTO pending_bills (user_id, service_name, total_price, target_username)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id) DO UPDATE SET
                service_name = EXCLUDED.service_name,
                total_price = EXCLUDED.total_price,
                target_username = EXCLUDED.target_username
        """,
          user_id,
          bill["service_name"],
          bill["total_price"],
          bill["target_username"],
      )
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute(
          """
            INSERT INTO pending_bills (user_id, service_name, total_price, target_username)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                service_name = excluded.service_name,
                total_price = excluded.total_price,
                target_username = excluded.target_username
        """,
          (user_id, bill["service_name"], bill["total_price"], bill["target_username"]),
      )
      conn.commit()


async def delete_pending_bill_db(user_id: int):
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      await conn.execute("DELETE FROM pending_bills WHERE user_id = $1", user_id)
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute("DELETE FROM pending_bills WHERE user_id = ?", (user_id,))
      conn.commit()


async def add_card_db(details: str, bank: str, country: str):
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      await conn.execute(
          "INSERT INTO card_details (details, bank, country, frozen_until) VALUES ($1, $2, $3, NULL)",
          details,
          bank,
          country,
      )
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute(
          "INSERT INTO card_details (details, bank, country, frozen_until) VALUES (?, ?, ?, NULL)",
          (details, bank, country),
      )
      conn.commit()


async def add_crypto_wallet_db(address: str, asset: str, network: str):
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      await conn.execute(
          "INSERT INTO crypto_wallets (address, asset, network, frozen_until) VALUES ($1, $2, $3, NULL)",
          address,
          asset,
          network,
      )
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute(
          "INSERT INTO crypto_wallets (address, asset, network, frozen_until) VALUES (?, ?, ?, NULL)",
          (address, asset, network),
      )
      conn.commit()


async def sync_crypto_wallets_db():
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      async with conn.transaction():
        await conn.execute("DELETE FROM crypto_wallets")
        for w in CRYPTO_WALLETS:
          await conn.execute(
              "INSERT INTO crypto_wallets (address, asset, network, frozen_until) VALUES ($1, $2, $3, $4)",
              w["address"],
              w["asset"],
              w["network"],
              w["frozen_until"].isoformat() if w["frozen_until"] else None,
          )
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute("DELETE FROM crypto_wallets")
      for w in CRYPTO_WALLETS:
        conn.execute(
            "INSERT INTO crypto_wallets (address, asset, network, frozen_until) VALUES (?, ?, ?, ?)",
            (
                w["address"],
                w["asset"],
                w["network"],
                w["frozen_until"].isoformat() if w["frozen_until"] else None,
            ),
        )
      conn.commit()


async def sync_card_details_db():
  if PG_POOL:
    async with PG_POOL.acquire() as conn:
      async with conn.transaction():
        await conn.execute("DELETE FROM card_details")
        for c in CARD_DETAILS:
          await conn.execute(
              "INSERT INTO card_details (details, bank, country, frozen_until) VALUES ($1, $2, $3, $4)",
              c["details"],
              c["bank"],
              c["country"],
              c["frozen_until"].isoformat() if c["frozen_until"] else None,
          )
  else:
    with sqlite3.connect("bot_database.db") as conn:
      conn.execute("DELETE FROM card_details")
      for c in CARD_DETAILS:
        conn.execute(
            "INSERT INTO card_details (details, bank, country, frozen_until) VALUES (?, ?, ?, ?)",
            (
                c["details"],
                c["bank"],
                c["country"],
                c["frozen_until"].isoformat() if c["frozen_until"] else None,
            ),
        )
      conn.commit()


# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===
def is_admin_authorized(user_id: int) -> bool:
  return bool(ADMIN_ID) and user_id == ADMIN_ID


def is_manager_authorized(user_id: int) -> bool:
  allowed_ids = {uid for uid in (MANAGER_ID, ADMIN_ID) if uid}
  if not allowed_ids:
    return False
  return user_id in allowed_ids


def is_pure_manager(user_id: int) -> bool:
  return bool(MANAGER_ID) and user_id == MANAGER_ID and user_id != ADMIN_ID


def _get_staff_notify_ids() -> list[int]:
  ids = []
  for uid in (ADMIN_ID, MANAGER_ID):
    if uid and uid not in ids:
      ids.append(uid)
  return ids


def _format_user_badge(user_id: int, username: str | None = None) -> str:
  uname = username or USER_MAPPING.get(user_id)
  if uname:
    return f"@{uname.lstrip('@')} (<code>{user_id}</code>)"
  return f"ID: <code>{user_id}</code>"


def _resolve_user_target(raw: str) -> int | None:
  cleaned = raw.strip()
  if not cleaned:
    return None
  if cleaned.isdigit():
    return int(cleaned)
  uname_key = cleaned.lstrip("@").lower()
  mapped = USER_MAPPING.get(uname_key)
  return int(mapped) if mapped else None


def _format_price(amount: float | int) -> str:
  if isinstance(amount, float) and amount.is_integer():
    return str(int(amount))
  return str(amount)


# === ПРОВЕРКА ЗАПРЕЩЁННЫХ РЕКВИЗИТОВ ДЛЯ МЕНЕДЖЕРА (/send и /bill) ===
_WALLET_PATTERNS = [
    r"\bT[1-9A-HJ-NP-Za-km-z]{33}\b",  # TRON / TRC-20
    r"\b0x[a-fA-F0-9]{40}\b",  # EVM (BEP-20 / ERC-20)
    r"\b(?:UQ|EQ|kQ|0Q)[A-Za-z0-9_-]{46}\b",  # TON base64url
    r"\b(?:bc1|[13])[a-zA-HJ-NP-Z0-9]{25,62}\b",  # Bitcoin
    r"\b[LM3][a-km-zA-HJ-NP-Z1-9]{26,33}\b",  # Litecoin
    r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b",  # Solana / Base58 адреса
]
_CARD_PATTERN = re.compile(r"(?<!\d)(?:\d[ -]?){13,19}(?!\d)")
_PHONE_PATTERN = re.compile(r"(?<!\w)(?:\+?\d[\d\s().-]{7,16}\d)(?!\w)")


def _check_manager_forbidden_requisites(text: str) -> str | None:
  """Проверяет текст на номера карт, номера телефонов и адреса криптокошельков.
  Названия банков и обычный текст разрешены."""
  if not text:
    return None

  for pattern in _WALLET_PATTERNS:
    if re.search(pattern, text):
      return "обнаружен адрес криптокошелька"

  for m in _CARD_PATTERN.finditer(text):
    digits = re.sub(r"\D", "", m.group(0))
    if 13 <= len(digits) <= 19:
      return "обнаружен номер карты"

  for m in _PHONE_PATTERN.finditer(text):
    start, end = m.span()
    prefix = text[max(0, start - 3) : start].lower()
    if any(tag in prefix for tag in ("id:", "id ", "код ", "#")):
      continue
    digits = re.sub(r"\D", "", m.group(0))
    if 10 <= len(digits) <= 15:
      return "обнаружен номер телефона"

  return None


# === ОТСЛЕЖИВАНИЕ ШАГОВ КЛИЕНТА И НЕОПЛАЧЕННЫХ ЗАКАЗОВ ===
async def _unpaid_followup_job(bot: Bot, user_id: int, delay_seconds: float = 900.0):
  try:
    if delay_seconds > 0:
      await asyncio.sleep(delay_seconds)
    UNPAID_FOLLOWUP_TASKS.pop(user_id, None)

    # Проверяем, не оплатил ли уже клиент
    orders = await get_user_orders_from_db(user_id)
    if orders:
      return

    journey = CLIENT_JOURNEY.get(user_id)
    if not journey or journey.get("step_rank", 0) < STEP_RANK["summary"]:
      return

    summary_card = _build_client_journey_summary(user_id)
    msg_text = (
        f"⚠️ <b>Неоплаченный заказ (клиент остановился на этапе: {journey.get('step_label')})</b>\n\n"
        f"{summary_card}\n\n"
        f"💡 Проведите клиента дальше: напишите ему или отмените/закройте лид."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✍️ Написать", callback_data=f"staff_reply_unpaid_{user_id}", style="primary"),
                InlineKeyboardButton(text="❌ Отмена", callback_data=f"staff_cancel_unpaid_{user_id}", style="danger"),
            ]
        ]
    )
    for uid in _get_staff_notify_ids():
      try:
        await bot.send_message(chat_id=uid, text=msg_text, reply_markup=kb, parse_mode="HTML")
      except Exception as e:
        logger.warning(f"Не удалось отправить уведомление о неоплаченном заказе ({uid}): {e}")
  except asyncio.CancelledError:
    pass


def _cancel_unpaid_followup(user_id: int):
  task = UNPAID_FOLLOWUP_TASKS.pop(user_id, None)
  if task:
    task.cancel()


def _schedule_unpaid_followup(bot: Bot, user_id: int, delay_seconds: float = 900.0):
  _cancel_unpaid_followup(user_id)
  UNPAID_FOLLOWUP_TASKS[user_id] = asyncio.create_task(
      _unpaid_followup_job(bot, user_id, delay_seconds=delay_seconds)
  )


async def track_client_step(
    bot: Bot,
    user,
    step_key: str,
    service_name: str | None = None,
    price: float | int | None = None,
    target: str | None = None,
):
  if not user:
    return
  uid = user.id
  rank = STEP_RANK.get(step_key, 0)
  prev = CLIENT_JOURNEY.get(uid) or {}
  max_rank = max(prev.get("step_rank", 0), rank)
  max_step_key = step_key if rank >= prev.get("step_rank", 0) else prev.get("max_step", step_key)

  CLIENT_JOURNEY[uid] = {
      "user_id": uid,
      "username": user.username or prev.get("username"),
      "step": step_key,
      "step_label": STEP_LABELS.get(step_key, step_key),
      "max_step": max_step_key,
      "max_step_label": STEP_LABELS.get(max_step_key, max_step_key),
      "step_rank": max_rank,
      "service_name": service_name if service_name is not None else prev.get("service_name", "—"),
      "price": price if price is not None else prev.get("price", 0),
      "target": target if target is not None else prev.get("target", "—"),
      "updated_at": datetime.datetime.now().strftime("%d.%m %H:%M"),
  }

  if rank >= STEP_RANK["summary"]:
    _schedule_unpaid_followup(bot, uid, delay_seconds=900.0)


def _build_client_journey_summary(user_id: int) -> str:
  j = CLIENT_JOURNEY.get(user_id) or {}
  orders_count = 0
  try:
    with sqlite3.connect("bot_database.db") as conn:
      row = conn.execute("SELECT COUNT(*) FROM orders WHERE user_id = ?", (user_id,)).fetchone()
      if row:
        orders_count = int(row[0])
  except Exception:
    pass

  badge = _format_user_badge(user_id, j.get("username"))
  price_val = j.get("price", 0)
  price_str = f"{_format_price(price_val)}$" if price_val else "По расчёту"
  max_label = j.get("max_step_label") or "Вход в бота"
  cur_label = j.get("step_label") or max_label
  srv = j.get("service_name") or "—"
  tgt = j.get("target") or "—"
  upd = j.get("updated_at") or datetime.datetime.now().strftime("%d.%m %H:%M")

  return (
      f"📊 <b>Сводка по клиенту:</b>\n"
      f"• Клиент: {badge}\n"
      f"• ID: <code>{user_id}</code>\n"
      f"• Выбранная услуга: <b>{srv}</b> ({price_str})\n"
      f"• Цель / детали: <b>{tgt}</b>\n"
      f"• Текущий этап: <b>{cur_label}</b>\n"
      f"• Максимум дошёл до: <b>{max_label}</b>\n"
      f"• Заказов в истории: <b>{orders_count}</b>\n"
      f"• Обновлено: <i>{upd}</i>"
  )


# === ТРЕДЫ /send С КНОПКАМИ И АВТОЗАКРЫТИЕМ ЧЕРЕЗ 1 ЧАС ===
def _client_send_thread_keyboard(client_id: int) -> InlineKeyboardMarkup:
  return InlineKeyboardMarkup(
      inline_keyboard=[
          [InlineKeyboardButton(text="💬 Ответить менеджеру", callback_data=f"client_reply_mgr_{client_id}", style="primary")],
          [InlineKeyboardButton(text="🔒 Закрыть обращение", callback_data=f"client_close_thr_{client_id}", style="danger")],
      ]
  )


def _staff_ticket_keyboard(client_id: int, ticket_kind: str) -> InlineKeyboardMarkup:
  return InlineKeyboardMarkup(
      inline_keyboard=[
          [
              InlineKeyboardButton(
                  text="💬 Ответить",
                  callback_data=f"staff_ans_ticket_{ticket_kind}_{client_id}",
                  style="primary",
              ),
              InlineKeyboardButton(
                  text="🔒 Закрыть обращение",
                  callback_data=f"staff_close_ticket_{ticket_kind}_{client_id}",
                  style="danger",
              ),
          ]
      ]
  )


async def _auto_close_send_thread(bot: Bot, client_id: int, delay_seconds: float = 3600.0):
  try:
    await asyncio.sleep(delay_seconds)
    thread = ACTIVE_SEND_THREADS.get(client_id)
    if not thread or thread.get("closed"):
      return
    thread["closed"] = True
    ACTIVE_SEND_THREADS.pop(client_id, None)
    try:
      await bot.send_message(
          chat_id=client_id,
          text="🔒 <b>Обращение автоматически закрыто</b> по истечении 1 часа без активности.",
          parse_mode="HTML",
      )
    except Exception as e:
      logger.warning(f"Не удалось отправить уведомление об автозакрытии треда ({client_id}): {e}")
  except asyncio.CancelledError:
    pass


async def _activate_send_thread(bot: Bot, client_id: int):
  old = ACTIVE_SEND_THREADS.pop(client_id, None)
  if old and old.get("task"):
    old["task"].cancel()
  expires_at = datetime.datetime.now() + datetime.timedelta(hours=1)
  task = asyncio.create_task(_auto_close_send_thread(bot, client_id, delay_seconds=3600.0))
  ACTIVE_SEND_THREADS[client_id] = {
      "task": task,
      "expires_at": expires_at,
      "closed": False,
  }


async def _close_send_thread(bot: Bot, client_id: int, notify_client: bool = False, custom_text: str | None = None):
  thread = ACTIVE_SEND_THREADS.pop(client_id, None)
  if thread and thread.get("task"):
    thread["task"].cancel()
  if notify_client:
    try:
      await bot.send_message(
          chat_id=client_id,
          text=custom_text or "🔒 <b>Обращение закрыто.</b>",
          parse_mode="HTML",
      )
    except Exception:
      pass


async def _deliver_to_client_safely(bot: Bot, target_user_id: int, text: str, reply_markup: InlineKeyboardMarkup | None = None):
  """Надёжная доставка сообщения клиенту с учётом кэша и повторной попыткой при сбое."""
  if target_user_id not in KNOWN_USERS:
    KNOWN_USERS[target_user_id] = datetime.datetime.now()
  try:
    return await bot.send_message(
        chat_id=target_user_id,
        text=text,
        reply_markup=reply_markup,
        parse_mode="HTML",
    )
  except Exception:
    await asyncio.sleep(0.4)
    return await bot.send_message(
        chat_id=target_user_id,
        text=text,
        reply_markup=reply_markup,
        parse_mode="HTML",
    )


def _build_catalog_keyboard() -> InlineKeyboardMarkup:
  return InlineKeyboardMarkup(
      inline_keyboard=[
          [InlineKeyboardButton(text="🔍 Доксинг — 30$", callback_data="srv_Доксинг_30", style="primary")],
          [InlineKeyboardButton(text="💣 Снос Telegram аккаунта — 50$", callback_data="srv_Снос_Telegram_аккаунта_50", style="danger")],
          [InlineKeyboardButton(text="⚡️ DDos-атака сайта — 70$", callback_data="srv_DDos-атака_сайта_70", style="danger")],
          [InlineKeyboardButton(text="📸 Взлом Instagram — 100$", callback_data="srv_Взлом_Instagram_100", style="primary")],
          [InlineKeyboardButton(text="💬 Взлом Telegram аккаунта — 120$", callback_data="srv_Взлом_Telegram_аккаунта_120", style="danger")],
          [InlineKeyboardButton(text="🪪 Слив базы сайта — 160$", callback_data="srv_Слив_базы_сайта_160", style="danger")],
          [InlineKeyboardButton(text="💣 Снос TG канала или бота — 200$", callback_data="srv_Снос_TG_канала_или_бота_200", style="primary")],
          [InlineKeyboardButton(text="🌐 Снос сайта — 300$", callback_data="srv_Снос_сайта_300", style="danger")],
          [InlineKeyboardButton(text="🛠 Особый заказ — Рассчитать стоимость", callback_data="srv_special_order", style="primary")],
          [InlineKeyboardButton(text="📖 О услугах", url="https://telegra.ph/Podrobnyj-gajd-po-uslugam-i-oformleniyu-zakaza-11-20-2")],
          [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_home", style="danger")],
      ]
  )


async def _get_payment_context(user_id: int, state: FSMContext) -> dict:
  data = await state.get_data()
  bill = PENDING_BILLS.get(user_id, {})
  total_price = data.get("total_price")
  if total_price is None:
    total_price = bill.get("total_price", 10)
  service_name = data.get("service_name") or bill.get("service_name") or "Особый заказ"
  target_username = data.get("target_username") or bill.get("target_username") or "По счету менеджера"
  payment_method = data.get("payment_method") or "card"
  return {
      "total_price": total_price,
      "service_name": service_name,
      "target_username": target_username,
      "payment_method": payment_method,
  }


def _build_payment_keyboard() -> InlineKeyboardMarkup:
  return InlineKeyboardMarkup(
      inline_keyboard=[
          [
              InlineKeyboardButton(text="💳 Рубли", callback_data="pay_currency_RUB", style="primary"),
              InlineKeyboardButton(text="💳 BYN", callback_data="pay_currency_BYN", style="danger"),
          ],
          [
              InlineKeyboardButton(text="💳 Гривны (UAH)", callback_data="pay_currency_UAH", style="primary"),
              InlineKeyboardButton(text="💳 Тенге (KZT)", callback_data="pay_currency_KZT", style="danger"),
          ],
          [
              InlineKeyboardButton(text="💳 Суммы (UZS)", callback_data="pay_currency_UZS", style="primary"),
              InlineKeyboardButton(text="💳 Леи (MDL)", callback_data="pay_currency_MDL", style="danger"),
          ],
          [InlineKeyboardButton(text="🤖 Оплата через CryptoBot", callback_data="pay_cryptobot", style="primary")],
          [InlineKeyboardButton(text="🪙 Реквизиты (Крипта)", callback_data="crypto_expert", style="danger")],
          [InlineKeyboardButton(text="💬 Нужна помощь", callback_data="payment_help_manager", style="primary")],
          [InlineKeyboardButton(text="⬅️ Назад", callback_data="catalog_services", style="danger")],
      ]
  )


# === MIDDLEWARE ДЛЯ ПРОВЕРКИ БАНА ===
@router.message.middleware()
async def ban_middleware(handler, event, data):
  if event.from_user:
    if is_admin_authorized(event.from_user.id) and event.text and event.text.strip().startswith("/unban"):
      return await handler(event, data)
    if await is_user_banned(event.from_user.id, event.from_user.username):
      return
  return await handler(event, data)


@router.callback_query.middleware()
async def ban_callback_middleware(handler, event, data):
  if event.from_user and await is_user_banned(event.from_user.id, event.from_user.username):
    return
  return await handler(event, data)


async def track_user(user):
  if user:
    USER_MAPPING[user.id] = user.username
    if user.username:
      USER_MAPPING[user.username.lower().lstrip("@")] = user.id
    await save_user_to_db(user)


# === ВАЛЮТЫ ДЛЯ ОПЛАТЫ КАРТОЙ / СБП / НА НОМЕР ===
CURRENCY_CONFIG = {
    "RUB": {"country": "Россия", "name": "Рубли (RUB)", "symbol": "₽"},
    "BYN": {"country": "Беларусь", "name": "BYN", "symbol": "BYN"},
    "UAH": {"country": "Украина", "name": "Гривны (UAH)", "symbol": "грн."},
    "KZT": {"country": "Казахстан", "name": "Тенге (KZT)", "symbol": "₸"},
    "UZS": {"country": "Узбекистан", "name": "Суммах (UZS)", "symbol": "сум"},
    "MDL": {"country": "Молдова", "name": "Мд. леях (MDL)", "symbol": "лей"},
}

# === КРИПТОСЕТИ ===
CRYPTO_NETWORKS = {
    "trc20": {
        "name": "USDT (TRC-20)",
        "asset": "USDT",
        "network": "TRC-20",
    },
    "bep20": {
        "name": "USDT (BEP-20)",
        "asset": "USDT",
        "network": "BEP-20",
    },
    "ton": {
        "name": "USDT (TON)",
        "asset": "USDT",
        "network": "TON",
    },
    "btc": {
        "name": "Bitcoin (BTC)",
        "asset": "BTC",
        "network": "Bitcoin",
    },
}


def _normalize_network(net_str: str) -> str:
  return re.sub(r"[^a-z0-9]", "", net_str.lower())


def get_next_card(country: str):
  now = datetime.datetime.now()
  active = []
  for card in CARD_DETAILS:
    if card["country"].lower() == country.lower():
      if card["frozen_until"] and card["frozen_until"] > now:
        continue
      if card["frozen_until"] and card["frozen_until"] <= now:
        card["frozen_until"] = None
      active.append(card)
  if not active:
    return None
  if country not in CARD_POINTERS:
    CARD_POINTERS[country] = 0
  index = CARD_POINTERS[country] % len(active)
  chosen_card = active[index]
  CARD_POINTERS[country] = (index + 1) % len(active)
  return chosen_card


def _find_matching_wallets(net_key: str):
  net_info = CRYPTO_NETWORKS.get(net_key)
  if not net_info:
    return []
  target_net = _normalize_network(net_info["network"])
  target_asset = net_info["asset"].upper()
  now = datetime.datetime.now()
  active = []
  for wallet in CRYPTO_WALLETS:
    w_net = _normalize_network(wallet["network"])
    w_asset = wallet["asset"].upper()
    matched = (w_net == target_net and w_asset == target_asset) or (
        net_key == "ton" and w_net == "ton" and w_asset == "USDT"
    )
    if matched:
      if wallet["frozen_until"] and wallet["frozen_until"] > now:
        continue
      if wallet["frozen_until"] and wallet["frozen_until"] <= now:
        wallet["frozen_until"] = None
      active.append(wallet)
  return active


def get_next_crypto_wallet(net_key: str):
  active = _find_matching_wallets(net_key)
  if not active:
    return None
  if net_key not in CRYPTO_POINTERS:
    CRYPTO_POINTERS[net_key] = 0
  index = CRYPTO_POINTERS[net_key] % len(active)
  chosen = active[index]
  CRYPTO_POINTERS[net_key] = (index + 1) % len(active)
  return chosen


async def get_exchange_rate(target_currency: str) -> float:
  url = "https://open.er-api.com/v6/latest/USD"
  try:
    async with ClientSession() as session:
      async with session.get(url, timeout=5) as response:
        if response.status == 200:
          data = await response.json()
          rates = data.get("rates", {})
          if target_currency in rates:
            return float(rates[target_currency])
  except Exception as e:
    logger.warning(f"Не удалось получить курс для {target_currency}: {e}")

  fallback_rates = {
      "RUB": 95.0,
      "BYN": 3.3,
      "KZT": 480.0,
      "UAH": 41.5,
      "UZS": 12500.0,
      "MDL": 18.0,
  }
  return fallback_rates.get(target_currency, 1.0)


# === РЕПУТАЦИЯ ===
def get_reputation_stats():
  current_date = datetime.date.today()
  base_date = datetime.date(2026, 1, 1)
  days_passed = max(0, (current_date - base_date).days)
  completed = 3752 + days_passed * 25
  rng = random.Random(current_date.toordinal())
  in_progress = rng.randint(170, 500)
  new_requests = rng.randint(30, 80)
  return completed, in_progress, new_requests


# === ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ПОИСКА БАННЕРА ===
def get_banner_path(banner_name: str):
  base_name = os.path.splitext(banner_name)[0]
  if os.path.exists("banners"):
    for filename in os.listdir("banners"):
      name_without_ext = os.path.splitext(filename)[0]
      if name_without_ext.lower() == base_name.lower():
        return os.path.join("banners", filename)
  return None


# === РЕНДЕРИНГ ЭКРАНОВ ===
async def render_step(
    event: Message | CallbackQuery,
    state: FSMContext,
    banner_name: str,
    text: str,
    reply_markup: InlineKeyboardMarkup,
):
  bot = event.bot
  chat_id = event.chat.id if isinstance(event, Message) else event.message.chat.id

  data = await state.get_data()
  bot_msg_id = data.get("bot_msg_id")

  photo_source = None
  banner_path = get_banner_path(banner_name)

  if banner_name in BANNER_CACHE:
    photo_source = BANNER_CACHE[banner_name]
  elif banner_path and os.path.exists(banner_path):
    photo_source = FSInputFile(banner_path)

  if isinstance(event, CallbackQuery):
    try:
      await event.answer()
    except Exception:
      pass
    msg = event.message
    try:
      if photo_source:
        try:
          sent_msg = await msg.edit_media(
              media=InputMediaPhoto(media=photo_source, caption=text, parse_mode="HTML"),
              reply_markup=reply_markup,
          )
          if banner_name not in BANNER_CACHE and hasattr(sent_msg, "photo") and sent_msg.photo:
            BANNER_CACHE[banner_name] = sent_msg.photo[-1].file_id
          await state.update_data(bot_msg_id=msg.message_id)
          return
        except Exception:
          try:
            await msg.delete()
          except Exception:
            pass
      else:
        try:
          await msg.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
          await state.update_data(bot_msg_id=msg.message_id)
          return
        except Exception:
          try:
            await msg.delete()
          except Exception:
            pass
    except Exception:
      pass

    try:
      if photo_source:
        new_msg = await bot.send_photo(
            chat_id=chat_id, photo=photo_source, caption=text, reply_markup=reply_markup, parse_mode="HTML"
        )
        if banner_name not in BANNER_CACHE and new_msg.photo:
          BANNER_CACHE[banner_name] = new_msg.photo[-1].file_id
      else:
        new_msg = await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode="HTML")
      await state.update_data(bot_msg_id=new_msg.message_id)
    except Exception as e:
      logger.error(f"Ошибка отправки в render_step (callback fallback): {e}")
  else:
    try:
      await event.delete()
    except Exception:
      pass

    edited_successfully = False
    if bot_msg_id:
      try:
        if photo_source:
          sent_msg = await bot.edit_message_media(
              chat_id=chat_id,
              message_id=bot_msg_id,
              media=InputMediaPhoto(media=photo_source, caption=text, parse_mode="HTML"),
              reply_markup=reply_markup,
          )
          if banner_name not in BANNER_CACHE and hasattr(sent_msg, "photo") and sent_msg.photo:
            BANNER_CACHE[banner_name] = sent_msg.photo[-1].file_id
          edited_successfully = True
        else:
          await bot.edit_message_text(
              chat_id=chat_id,
              message_id=bot_msg_id,
              text=text,
              reply_markup=reply_markup,
              parse_mode="HTML",
          )
          edited_successfully = True
      except Exception:
        pass

    if not edited_successfully:
      try:
        if photo_source:
          new_msg = await bot.send_photo(
              chat_id=chat_id, photo=photo_source, caption=text, reply_markup=reply_markup, parse_mode="HTML"
          )
          if banner_name not in BANNER_CACHE and new_msg.photo:
            BANNER_CACHE[banner_name] = new_msg.photo[-1].file_id
        else:
          new_msg = await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode="HTML")
        await state.update_data(bot_msg_id=new_msg.message_id)
      except Exception as e:
        logger.error(f"Ошибка отправки нового сообщения в render_step (message fallback): {e}")


# === ГЛАВНОЕ МЕНЮ ===
async def show_main_menu(event: Message | CallbackQuery, state: FSMContext):
  await state.clear()
  user_id = event.from_user.id
  if user_id not in KNOWN_USERS:
    KNOWN_USERS[user_id] = datetime.datetime.now()

  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[
          [InlineKeyboardButton(text="🛒 Заказать", callback_data="catalog_services", style="primary")],
          [
              InlineKeyboardButton(text="📦 В работе", callback_data="my_orders", style="danger"),
              InlineKeyboardButton(text="📊 Репутация", callback_data="about_service", style="danger"),
          ],
          [InlineKeyboardButton(text="💬 Поддержка", callback_data="contact_admin", style="primary")],
      ]
  )
  text = (
      "💻 <b>ARENA SHOP — КИБЕР</b>\n\n"
      "<i>Добро пожаловать в сервис. Все конфиденциально, быстро и безопасно. Работаем 24/7.</i>\n\n"
      "<b>👇 Выберите пункт меню ниже:</b>"
  )
  await render_step(event, state, "main.jpg", text, keyboard)


# === АДМИНСКИЕ И МЕНЕДЖЕРСКИЕ КОМАНДЫ ===
@router.message(Command("bill"))
async def cmd_manager_bill(message: Message):
  if not is_manager_authorized(message.from_user.id):
    return
  args = message.text.split(maxsplit=1)
  if len(args) < 2:
    await message.reply(
        "❌ Использование: `/bill <user_id> <услуга> <сумма$>`\nПример: `/bill 282810030 особый заказ 220$`",
        parse_mode="Markdown",
    )
    return

  if is_pure_manager(message.from_user.id):
    forbidden_reason = _check_manager_forbidden_requisites(args[1])
    if forbidden_reason:
      await message.reply(f"❌ Отправка запрещена: {forbidden_reason}.")
      return

  parts = args[1].strip().split()
  if len(parts) < 2 or not parts[0].isdigit():
    await message.reply("❌ Неверный формат. Пример: `/bill 282810030 особый заказ 220$`", parse_mode="Markdown")
    return

  target_user_id = int(parts[0])
  raw_amount = parts[-1].rstrip("$").strip()
  try:
    amount = float(raw_amount)
  except ValueError:
    await message.reply("❌ Неверная сумма. Пример: `/bill 282810030 особый заказ 220$`", parse_mode="Markdown")
    return

  service_name = " ".join(parts[1:-1]).strip() if len(parts) > 2 else "Особый заказ"
  if not service_name:
    service_name = "Особый заказ"

  bill_obj = {
      "service_name": service_name,
      "total_price": amount,
      "target_username": "По счету менеджера",
  }
  PENDING_BILLS[target_user_id] = bill_obj
  await save_pending_bill_db(target_user_id, bill_obj)

  price_str = _format_price(amount)
  bill_text = (
      f"🧾 <b>Вам выставлен счет на {service_name}</b>\n\n"
      f"• Услуга: <b>{service_name}</b>\n"
      f"• К оплате: <b>{price_str} USDT</b>\n\n"
      f"Выберите способ оплаты ниже, чтобы получить реквизиты:"
  )
  try:
    await _deliver_to_client_safely(
        message.bot,
        target_user_id=target_user_id,
        text=bill_text,
        reply_markup=_build_payment_keyboard(),
    )
    await message.reply(
        f"✅ Счет на <b>{service_name}</b> (<b>{price_str} USDT</b>) отправлен пользователю <code>{target_user_id}</code>.",
        parse_mode="HTML",
    )
  except Exception as e:
    await message.reply(f"❌ Не удалось отправить счет пользователю <code>{target_user_id}</code>: {e}", parse_mode="HTML")


@router.message(Command("send"))
async def cmd_manager_send(message: Message):
  if not is_manager_authorized(message.from_user.id):
    return
  args = message.text.split(maxsplit=2)
  if len(args) < 3:
    await message.reply(
        "❌ Использование: `/send <user_id или @username> <сообщение>`\nПример: `/send 282810030 Привет! Счет готов.`",
        parse_mode="Markdown",
    )
    return

  target_user_id = _resolve_user_target(args[1].strip())
  if not target_user_id:
    await message.reply("❌ Неверный ID или @username пользователя.")
    return

  out_text = args[2].strip()
  if not out_text:
    await message.reply("❌ Сообщение не может быть пустым.")
    return

  if is_pure_manager(message.from_user.id):
    forbidden_reason = _check_manager_forbidden_requisites(out_text)
    if forbidden_reason:
      await message.reply(f"❌ Отправка запрещена: {forbidden_reason}.")
      return

  try:
    await _activate_send_thread(message.bot, target_user_id)
    await _deliver_to_client_safely(
        message.bot,
        target_user_id=target_user_id,
        text=f"💬 <b>Сообщение от менеджера:</b>\n\n{out_text}",
        reply_markup=_client_send_thread_keyboard(target_user_id),
    )
    await message.reply(f"✅ Сообщение отправлено пользователю <code>{target_user_id}</code>.", parse_mode="HTML")
  except Exception as e:
    await message.reply(f"❌ Ошибка отправки пользователю <code>{target_user_id}</code>: {e}", parse_mode="HTML")


# === КНОПКИ КЛИЕНТА ПОД /send И ОБРАТНАЯ СВЯЗЬ МЕНЕДЖЕРУ ===
@router.callback_query(F.data.startswith("client_reply_mgr_"))
async def cb_client_reply_to_manager(callback: CallbackQuery, state: FSMContext):
  try:
    client_id = int(callback.data.split("_")[-1])
  except ValueError:
    return
  if callback.from_user.id != client_id:
    await callback.answer("Эта кнопка не для вас.", show_alert=True)
    return

  thread = ACTIVE_SEND_THREADS.get(client_id)
  if not thread or thread.get("closed"):
    await callback.answer("Обращение уже закрыто.", show_alert=True)
    return

  await callback.answer()
  await state.set_state(ClientThreadReplyForm.waiting_reply)
  await state.update_data(reply_thread_client_id=client_id)
  kb = InlineKeyboardMarkup(
      inline_keyboard=[
          [InlineKeyboardButton(text="🔒 Закрыть обращение", callback_data=f"client_close_thr_{client_id}", style="danger")]
      ]
  )
  await callback.message.reply(
      "✍️ Напишите ваш вопрос или сообщение менеджеру (можно прикрепить фото):",
      reply_markup=kb,
  )


@router.message(ClientThreadReplyForm.waiting_reply, F.text | F.photo)
async def process_client_thread_reply(message: Message, state: FSMContext):
  await track_user(message.from_user)
  client_id = message.from_user.id
  thread = ACTIVE_SEND_THREADS.get(client_id)
  if not thread or thread.get("closed"):
    await state.clear()
    await message.reply("🔒 Это обращение уже закрыто.")
    return

  # Продлеваем таймер автозакрытия на 1 час при активности клиента
  await _activate_send_thread(message.bot, client_id)

  user_badge = _format_user_badge(client_id, message.from_user.username)
  body_text = (message.caption or message.text or "").strip()
  header = (
      f"📩 <b>Ответ клиента по /send</b>\n"
      f"• От кого: {user_badge}\n"
      f"• Сообщение: {body_text if body_text else '<i>[Фото]</i>'}"
  )
  staff_kb = _staff_ticket_keyboard(client_id, "send")

  for uid in _get_staff_notify_ids():
    try:
      if message.photo:
        await message.bot.send_photo(
            chat_id=uid,
            photo=message.photo[-1].file_id,
            caption=header,
            reply_markup=staff_kb,
            parse_mode="HTML",
        )
      else:
        await message.bot.send_message(
            chat_id=uid,
            text=header,
            reply_markup=staff_kb,
            parse_mode="HTML",
        )
    except Exception as e:
      logger.warning(f"Не удалось переслать ответ клиента ({uid}): {e}")

  await state.clear()
  await message.reply("✅ Ваше сообщение отправлено менеджеру.")


@router.callback_query(F.data.startswith("client_close_thr_"))
async def cb_client_close_send_thread(callback: CallbackQuery, state: FSMContext):
  try:
    client_id = int(callback.data.split("_")[-1])
  except ValueError:
    return
  if callback.from_user.id != client_id:
    await callback.answer("Эта кнопка не для вас.", show_alert=True)
    return

  await _close_send_thread(callback.bot, client_id, notify_client=False)
  for uid in _get_staff_notify_ids():
    try:
      await callback.bot.send_message(
          chat_id=uid,
          text=f"🔒 Клиент {_format_user_badge(client_id, callback.from_user.username)} закрыл обращение по /send.",
          parse_mode="HTML",
      )
    except Exception:
      pass

  await callback.answer("Обращение закрыто.")
  await show_main_menu(callback, state)


# === МЕНЮ ТИКЕТОВ У МЕНЕДЖЕРА / АДМИНА (ПОДДЕРЖКА / ОСОБЫЙ ЗАКАЗ / ОПЛАТА / /send) ===
@router.callback_query(F.data.startswith("staff_ans_ticket_"))
async def cb_staff_answer_ticket(callback: CallbackQuery, state: FSMContext):
  if not is_manager_authorized(callback.from_user.id):
    return
  parts = callback.data.split("_")
  # формат: staff_ans_ticket_{kind}_{client_id}
  if len(parts) < 5:
    return
  kind = parts[3]
  try:
    client_id = int(parts[4])
  except ValueError:
    return

  await callback.answer()
  await state.set_state(StaffQuickReplyForm.waiting_reply)
  await state.update_data(target_client_id=client_id, ticket_kind=kind)
  await callback.message.reply(
      f"✍️ Введите ответ клиенту <code>{client_id}</code> одним сообщением (или отправьте /cancel для отмены):",
      parse_mode="HTML",
  )


@router.message(StaffQuickReplyForm.waiting_reply, Command("cancel"))
async def cmd_cancel_staff_reply(message: Message, state: FSMContext):
  if not is_manager_authorized(message.from_user.id):
    return
  await state.clear()
  await message.reply("❌ Ответ отменён.")


@router.message(StaffQuickReplyForm.waiting_reply, F.text)
async def process_staff_quick_reply(message: Message, state: FSMContext):
  if not is_manager_authorized(message.from_user.id):
    return
  data = await state.get_data()
  target_client_id = data.get("target_client_id")
  kind = data.get("ticket_kind") or "support"
  reply_text = message.text.strip()

  if not target_client_id:
    await state.clear()
    await message.reply("❌ Не найден ID клиента.")
    return

  if is_pure_manager(message.from_user.id):
    forbidden_reason = _check_manager_forbidden_requisites(reply_text)
    if forbidden_reason:
      await message.reply(f"❌ Отправка запрещена: {forbidden_reason}.")
      return

  try:
    if kind == "send":
      await _activate_send_thread(message.bot, int(target_client_id))
      await _deliver_to_client_safely(
          message.bot,
          target_user_id=int(target_client_id),
          text=f"💬 <b>Сообщение от менеджера:</b>\n\n{reply_text}",
          reply_markup=_client_send_thread_keyboard(int(target_client_id)),
      )
    else:
      await _deliver_to_client_safely(
          message.bot,
          target_user_id=int(target_client_id),
          text=f"💬 <b>Сообщение от службы поддержки:</b>\n\n{reply_text}",
      )
    await state.clear()
    await message.reply(f"✅ Ответ отправлен клиенту <code>{target_client_id}</code>.", parse_mode="HTML")
  except Exception as e:
    await message.reply(f"❌ Ошибка отправки клиенту <code>{target_client_id}</code>: {e}", parse_mode="HTML")


@router.callback_query(F.data.startswith("staff_close_ticket_"))
async def cb_staff_close_ticket(callback: CallbackQuery):
  if not is_manager_authorized(callback.from_user.id):
    return
  parts = callback.data.split("_")
  # формат: staff_close_ticket_{kind}_{client_id}
  if len(parts) < 5:
    return
  kind = parts[3]
  try:
    client_id = int(parts[4])
  except ValueError:
    return

  CLOSED_TICKETS.add((client_id, kind))
  if kind == "send":
    await _close_send_thread(callback.bot, client_id, notify_client=True)
  else:
    try:
      await callback.bot.send_message(
          chat_id=client_id,
          text="🔒 <b>Ваше обращение закрыто.</b> Если появятся новые вопросы — напишите нам снова.",
          parse_mode="HTML",
      )
    except Exception:
      pass

  await callback.answer("Обращение закрыто.")
  try:
    await callback.message.edit_reply_markup(reply_markup=None)
  except Exception:
    pass
  await callback.message.reply(f"🔒 Обращение клиента <code>{client_id}</code> закрыто.", parse_mode="HTML")


# === КНОПКИ УПРАВЛЕНИЯ НЕОПЛАЧЕННЫМИ ЗАКАЗАМИ У МЕНЕДЖЕРА / АДМИНА ===
@router.callback_query(F.data.startswith("staff_reply_unpaid_"))
async def cb_staff_reply_unpaid(callback: CallbackQuery, state: FSMContext):
  if not is_manager_authorized(callback.from_user.id):
    return
  try:
    client_id = int(callback.data.split("_")[-1])
  except ValueError:
    return
  await callback.answer()
  await state.set_state(StaffQuickReplyForm.waiting_reply)
  await state.update_data(target_client_id=client_id, ticket_kind="unpaid")
  await callback.message.reply(
      f"✍️ Введите сообщение для клиента <code>{client_id}</code> по неоплаченному заказу (или /cancel):",
      parse_mode="HTML",
  )


@router.callback_query(F.data.startswith("staff_cancel_unpaid_"))
async def cb_staff_cancel_unpaid(callback: CallbackQuery):
  if not is_manager_authorized(callback.from_user.id):
    return
  try:
    client_id = int(callback.data.split("_")[-1])
  except ValueError:
    return
  _cancel_unpaid_followup(client_id)
  PENDING_BILLS.pop(client_id, None)
  await delete_pending_bill_db(client_id)
  await callback.answer("Отменено.")
  try:
    await callback.message.edit_reply_markup(reply_markup=None)
  except Exception:
    pass
  await callback.message.reply(f"❌ Неоплаченный заказ / лид клиента <code>{client_id}</code> отменён.", parse_mode="HTML")


@router.message(Command("work"))
async def cmd_manager_work(message: Message):
  if not is_manager_authorized(message.from_user.id):
    return
  args = message.text.split(maxsplit=1)
  if len(args) < 2:
    await message.reply(
        "❌ Использование: `/work <user_id или @username>`\nПример: `/work 4324324`",
        parse_mode="Markdown",
    )
    return

  target_user_id = _resolve_user_target(args[1])
  if not target_user_id:
    await message.reply("❌ Пользователь не найден.")
    return

  updated_count = await set_user_orders_in_work_db(target_user_id)
  if updated_count <= 0:
    await message.reply(
        f"❌ У пользователя <code>{target_user_id}</code> не найдено заказов для перевода в статус «В работе».",
        parse_mode="HTML",
    )
    return

  try:
    await message.bot.send_message(
        chat_id=target_user_id,
        text="⚙️ <b>Статус вашего заказа обновлен: В работе.</b>",
        parse_mode="HTML",
    )
  except Exception as e:
    logger.warning(f"Не удалось уведомить пользователя {target_user_id} о статусе заказа: {e}")

  await message.reply(
      f"✅ Статус заказа(ов) пользователя <code>{target_user_id}</code> переведен в <b>В работе</b> (обновлено записей: {updated_count}).",
      parse_mode="HTML",
  )


@router.message(Command("ban"))
async def cmd_ban_user(message: Message):
  if not is_admin_authorized(message.from_user.id):
    return
  args = message.text.split(maxsplit=1)
  if len(args) < 2:
    await message.reply("❌ Использование: `/ban <user_id или @username>`", parse_mode="Markdown")
    return
  target_user_id = _resolve_user_target(args[1])
  if target_user_id:
    await ban_user_in_db(target_user_id)
    await message.reply(f"✅ Пользователь с ID <code>{target_user_id}</code> заблокирован.", parse_mode="HTML")
  else:
    await message.reply("❌ Пользователь не найден.")


@router.message(Command("unban"))
async def cmd_unban_user(message: Message):
  if not is_admin_authorized(message.from_user.id):
    return
  args = message.text.split(maxsplit=1)
  if len(args) < 2:
    await message.reply("❌ Использование: `/unban <user_id или @username>`", parse_mode="Markdown")
    return
  target_user_id = _resolve_user_target(args[1])
  if target_user_id:
    await unban_user_in_db(target_user_id)
    await message.reply(f"✅ Пользователь с ID <code>{target_user_id}</code> разблокирован.", parse_mode="HTML")
  else:
    await message.reply("❌ Пользователь не найден.")


@router.message(Command("ans"))
async def cmd_admin_answer(message: Message):
  if not is_admin_authorized(message.from_user.id):
    return
  args = message.text.split(maxsplit=2)
  if len(args) < 3:
    await message.reply("❌ Использование: `/ans <user_id> <текст ответа>`", parse_mode="Markdown")
    return
  target_user_id_str = args[1].strip()
  reply_text = args[2].strip()
  if not target_user_id_str.isdigit():
    await message.reply("❌ Неверный ID пользователя.")
    return
  target_user_id = int(target_user_id_str)
  try:
    await message.bot.send_message(
        chat_id=target_user_id,
        text=f"💬 <b>Сообщение от службы поддержки:</b>\n\n{reply_text}",
        parse_mode="HTML",
    )
    await message.reply(f"✅ Ответ успешно отправлен пользователю <code>{target_user_id}</code>.", parse_mode="HTML")
  except Exception as e:
    await message.reply(f"❌ Ошибка отправки: {e}")


@router.message(F.from_user.id == ADMIN_ID, F.reply_to_message)
async def admin_reply_handler(message: Message):
  if not is_admin_authorized(message.from_user.id):
    return
  reply_msg = message.reply_to_message
  text_to_parse = (reply_msg.text or "") + (reply_msg.caption or "")

  match = re.search(r"\(?<code>(\d+)<\/code>\)?", text_to_parse)
  if not match:
    match = re.search(r"ID:\s*(\d+)", text_to_parse)
  if not match:
    match = re.search(r"\b(\d{6,12})\b", text_to_parse)

  if match:
    target_user_id = int(match.group(1))
    try:
      await message.bot.send_message(
          chat_id=target_user_id,
          text=f"💬 <b>Сообщение от службы поддержки:</b>\n\n{message.text}",
          parse_mode="HTML",
      )
      await message.reply(f"✅ Ответ отправлен пользователю <code>{target_user_id}</code>.", parse_mode="HTML")
    except Exception as e:
      await message.reply(f"❌ Не удалось отправить сообщение пользователю: {e}")
  else:
    await message.reply("❌ Не удалось определить ID пользователя из сообщения, на которое вы ответили.")


@router.message(Command("clear_cache"))
async def cmd_clear_cache(message: Message):
  if not is_admin_authorized(message.from_user.id):
    return
  BANNER_CACHE.clear()
  await message.reply("✅ Кеш баннеров успешно очищен.")


def _parse_wallet_args(raw: str):
  parts = raw.strip().split()
  if len(parts) < 3:
    return None
  address = parts[0]
  asset = parts[1].upper()
  network = " ".join(parts[2:])
  return address, asset, network


def _is_crypto_asset_and_network(asset: str, network: str) -> bool:
  known_assets = {"USDT", "TON", "BTC", "BITCOIN", "ETH", "LTC"}
  norm_net = _normalize_network(network)
  known_nets = {"trc20", "bep20", "ton", "btc", "bitcoin", "erc20", "sol", "solana"}
  return asset.upper() in known_assets or norm_net in known_nets


@router.message(Command("add"))
async def cmd_add_requisites(message: Message):
  if not is_admin_authorized(message.from_user.id):
    return
  args = message.text.split(maxsplit=1)
  if len(args) < 2:
    await message.reply(
        "❌ Использование:\n"
        "• Крипта: `/add UQDwk7PZoWr0zHv-BItBEGxnq4uyj81z6BlIw74heDgF-Shd USDT TON`\n"
        "• Крипта: `/add TYd1ekJgTHnFfgAVYXsi7689GaG5oZN3Xe USDT TRC-20`\n"
        "• Карта/СБП: `/add +79990001122 ОзонБанк Россия`",
        parse_mode="Markdown",
    )
    return

  parsed = _parse_wallet_args(args[1])
  if not parsed:
    await message.reply("❌ Неверный формат.")
    return
  first, second, rest = parsed

  if _is_crypto_asset_and_network(second, rest):
    CRYPTO_WALLETS.append({
        "address": first,
        "asset": second,
        "network": rest,
        "frozen_until": None,
    })
    await add_crypto_wallet_db(first, second, rest)
    await message.reply(
        f"✅ Криптореквизиты добавлены!\n🪙 {second} | 🌐 {rest}\n<code>{first}</code>",
        parse_mode="HTML",
    )
  else:
    parts = args[1].split()
    country = parts[-1]
    bank = parts[-2]
    details = " ".join(parts[:-2])
    CARD_DETAILS.append({
        "details": details,
        "bank": bank,
        "country": country,
        "frozen_until": None,
    })
    await add_card_db(details, bank, country)
    await message.reply(
        f"✅ Реквизиты добавлены!\n🌍 {country} | 🏦 {bank}\n<code>{details}</code>",
        parse_mode="HTML",
    )


@router.message(Command("delete"))
async def cmd_delete_requisites(message: Message):
  if not is_admin_authorized(message.from_user.id):
    return
  global CARD_DETAILS, CRYPTO_WALLETS
  args = message.text.split(maxsplit=1)
  if len(args) < 2:
    await message.reply("❌ Использование: `/delete <реквизиты/адрес> <валюта/банк> <сеть/страна>`", parse_mode="Markdown")
    return
  parsed = _parse_wallet_args(args[1])
  if not parsed:
    await message.reply("❌ Неверный формат.")
    return
  first, second, rest = parsed

  if _is_crypto_asset_and_network(second, rest):
    target_net = _normalize_network(rest)
    initial_len = len(CRYPTO_WALLETS)
    CRYPTO_WALLETS = [
        w
        for w in CRYPTO_WALLETS
        if not (
            w["address"] == first
            and w["asset"].upper() == second
            and _normalize_network(w["network"]) == target_net
        )
    ]
    if len(CRYPTO_WALLETS) < initial_len:
      await sync_crypto_wallets_db()
      await message.reply("✅ Криптореквизиты удалены.")
      return

  parts = args[1].split()
  country = parts[-1]
  bank = parts[-2]
  details = " ".join(parts[:-2])
  initial_card_len = len(CARD_DETAILS)
  CARD_DETAILS = [
      c
      for c in CARD_DETAILS
      if not (c["details"] == details and c["bank"] == bank and c["country"].lower() == country.lower())
  ]
  if len(CARD_DETAILS) < initial_card_len:
    await sync_card_details_db()
    await message.reply("✅ Удалено.")
  else:
    await message.reply("❌ Не найдено.")


@router.message(Command("freeze"))
async def cmd_freeze_requisites(message: Message):
  if not is_admin_authorized(message.from_user.id):
    return
  args = message.text.split(maxsplit=1)
  if len(args) < 2:
    await message.reply("❌ Использование: `/freeze <реквизиты/адрес> <валюта/банк> <сеть/страна>`", parse_mode="Markdown")
    return
  parsed = _parse_wallet_args(args[1])
  if not parsed:
    await message.reply("❌ Неверный формат.")
    return
  first, second, rest = parsed
  until_dt = datetime.datetime.now() + datetime.timedelta(hours=24)

  if _is_crypto_asset_and_network(second, rest):
    target_net = _normalize_network(rest)
    found = False
    for w in CRYPTO_WALLETS:
      if (
          w["address"] == first
          and w["asset"].upper() == second
          and _normalize_network(w["network"]) == target_net
      ):
        w["frozen_until"] = until_dt
        found = True
        break
    if found:
      await sync_crypto_wallets_db()
      await message.reply("✅ Криптореквизиты зафрижены на 24 часа.")
      return

  parts = args[1].split()
  country = parts[-1]
  bank = parts[-2]
  details = " ".join(parts[:-2])
  found_card = False
  for card in CARD_DETAILS:
    if card["details"] == details and card["bank"] == bank and card["country"].lower() == country.lower():
      card["frozen_until"] = until_dt
      found_card = True
      break
  if found_card:
    await sync_card_details_db()
    await message.reply("✅ Зафрижено на 24 часа.")
  else:
    await message.reply("❌ Не найдено.")


@router.message(Command("list"))
async def cmd_list_requisites(message: Message):
  if not is_admin_authorized(message.from_user.id):
    return

  now = datetime.datetime.now()
  cards_changed = False
  wallets_changed = False

  active_cards = []
  frozen_cards = []
  for card in CARD_DETAILS:
    frozen_until = card.get("frozen_until")
    if frozen_until and frozen_until > now:
      frozen_cards.append((card, frozen_until))
    else:
      if frozen_until is not None:
        card["frozen_until"] = None
        cards_changed = True
      active_cards.append(card)

  active_wallets = []
  frozen_wallets = []
  for wallet in CRYPTO_WALLETS:
    frozen_until = wallet.get("frozen_until")
    if frozen_until and frozen_until > now:
      frozen_wallets.append((wallet, frozen_until))
    else:
      if frozen_until is not None:
        wallet["frozen_until"] = None
        wallets_changed = True
      active_wallets.append(wallet)

  if cards_changed:
    await sync_card_details_db()
  if wallets_changed:
    await sync_crypto_wallets_db()

  lines = ["📋 <b>Список актуальных реквизитов:</b>\n"]

  lines.append("💳 <b>Карты / СБП / Номера (актуальные):</b>")
  if active_cards:
    for idx, c in enumerate(active_cards, start=1):
      lines.append(f"{idx}. 🌍 {c['country']} | 🏦 {c['bank']} — <code>{c['details']}</code>")
  else:
    lines.append("<i>Нет активных реквизитов.</i>")

  lines.append("\n🪙 <b>Крипта (актуальные):</b>")
  if active_wallets:
    for idx, w in enumerate(active_wallets, start=1):
      lines.append(f"{idx}. 🪙 {w['asset']} | 🌐 {w['network']} — <code>{w['address']}</code>")
  else:
    lines.append("<i>Нет активных кошельков.</i>")

  lines.append("\n❄️ <b>Зафриженные на 24 часа (недоступны):</b>")
  has_frozen = bool(frozen_cards or frozen_wallets)
  if not has_frozen:
    lines.append("<i>Нет зафриженных реквизитов.</i>")
  else:
    if frozen_cards:
      lines.append("💳 <i>Карты / СБП / Номера:</i>")
      for idx, (c, until_dt) in enumerate(frozen_cards, start=1):
        rem = until_dt - now
        rem_hours = max(1, int(rem.total_seconds() // 3600))
        rem_mins = int((rem.total_seconds() % 3600) // 60)
        lines.append(
            f"{idx}. 🌍 {c['country']} | 🏦 {c['bank']} — <code>{c['details']}</code> "
            f"(до {until_dt.strftime('%d.%m %H:%M')}, ост. {rem_hours}ч {rem_mins}м)"
        )
    if frozen_wallets:
      lines.append("🪙 <i>Крипта:</i>")
      for idx, (w, until_dt) in enumerate(frozen_wallets, start=1):
        rem = until_dt - now
        rem_hours = max(1, int(rem.total_seconds() // 3600))
        rem_mins = int((rem.total_seconds() % 3600) // 60)
        lines.append(
            f"{idx}. 🪙 {w['asset']} | 🌐 {w['network']} — <code>{w['address']}</code> "
            f"(до {until_dt.strftime('%d.%m %H:%M')}, ост. {rem_hours}ч {rem_mins}м)"
        )

  await message.reply("\n".join(lines), parse_mode="HTML")


# === CRYPTOBOT ИНТЕГРАЦИЯ ===
async def create_cryptobot_invoice(amount_usd: float, description: str):
  if not CRYPTO_BOT_TOKEN:
    return "", None
  url = "https://pay.crypt.bot/api/createInvoice"
  headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
  payload = {
      "asset": "USDT",
      "amount": str(amount_usd),
      "description": description,
      "payload": "order_payment",
  }
  try:
    async with ClientSession() as session:
      async with session.post(url, json=payload, headers=headers, timeout=10) as resp:
        if resp.status == 200:
          data = await resp.json()
          if data.get("ok"):
            res = data["result"]
            return res["pay_url"], res["invoice_id"]
  except Exception as e:
    logger.error(f"Ошибка CryptoBot: {e}")
  return "", None


async def check_cryptobot_invoice_status(invoice_id: int) -> bool:
  if not CRYPTO_BOT_TOKEN or not invoice_id:
    return True
  url = "https://pay.crypt.bot/api/getInvoices"
  headers = {"Crypto-Pay-API-Token": CRYPTO_BOT_TOKEN}
  params = {"invoice_ids": str(invoice_id)}
  try:
    async with ClientSession() as session:
      async with session.get(url, params=params, headers=headers, timeout=10) as resp:
        if resp.status == 200:
          data = await resp.json()
          if data.get("ok"):
            items = data.get("result", {}).get("items", [])
            if items:
              return items[0].get("status") == "paid"
  except Exception as e:
    logger.error(f"Ошибка проверки инвойса: {e}")
  return False


# === ФОНОВЫЕ ЗАДАЧИ ===
async def scheduled_broadcast(bot: Bot):
  while True:
    await asyncio.sleep(3600)
    now = datetime.datetime.now()
    banner_path = get_banner_path("special_offer.jpg")
    for user_id, last_time in list(KNOWN_USERS.items()):
      if await is_user_banned(user_id, USER_MAPPING.get(user_id)):
        continue
      ref_time = LAST_BROADCAST.get(user_id) or last_time
      if ref_time is None or (now - ref_time).total_seconds() >= 24 * 3600:
        try:
          today = datetime.date.today()
          target_date = today + datetime.timedelta(days=3)
          months_ru = [
              "",
              "января",
              "февраля",
              "марта",
              "апреля",
              "мая",
              "июня",
              "июля",
              "августа",
              "сентября",
              "октября",
              "ноября",
              "декабря",
          ]
          text = (
              "✨ <b>Специальное предложение</b>\n\n"
              f"Действует персональная скидка <b>20%</b> до <b>{target_date.day} {months_ru[target_date.month]}</b>.\n\n"
              "Ждем вас в боте 👇"
          )
          keyboard = InlineKeyboardMarkup(
              inline_keyboard=[
                  [InlineKeyboardButton(text="✨ Оформить со скидкой", callback_data="catalog_services", style="primary")],
                  [InlineKeyboardButton(text="🏠 На главную", callback_data="back_home", style="danger")],
              ]
          )

          if "special_offer.jpg" in BANNER_CACHE:
            photo = BANNER_CACHE["special_offer.jpg"]
            await bot.send_photo(chat_id=user_id, photo=photo, caption=text, reply_markup=keyboard, parse_mode="HTML")
          elif banner_path and os.path.exists(banner_path):
            photo = FSInputFile(banner_path)
            sent = await bot.send_photo(chat_id=user_id, photo=photo, caption=text, reply_markup=keyboard, parse_mode="HTML")
            if sent.photo:
              BANNER_CACHE["special_offer.jpg"] = sent.photo[-1].file_id
          else:
            await bot.send_message(chat_id=user_id, text=text, reply_markup=keyboard, parse_mode="HTML")

          await save_last_broadcast_db(user_id, now)
        except Exception as e:
          logger.error(f"Ошибка рассылки: {e}")


async def delayed_abandoned_cart_reminder(bot: Bot, user_id: int, delay_seconds: float = 1800.0):
  try:
    if delay_seconds > 0:
      await asyncio.sleep(delay_seconds)
    if await is_user_banned(user_id, USER_MAPPING.get(user_id)):
      ABANDONED_CHECKOUTS.pop(user_id, None)
      await delete_abandoned_reminder_db(user_id)
      return
    if user_id in ABANDONED_CHECKOUTS:
      reminder_text = (
          "👋 <b>Завершить заказ со скидкой -20%?</b>\n\nВы остановились на этапе оплаты. Возвращайтесь к оформлению 👇"
      )
      keyboard = InlineKeyboardMarkup(
          inline_keyboard=[
              [InlineKeyboardButton(text="🛒 Вернуться к заказу", callback_data="catalog_services", style="primary")],
              [InlineKeyboardButton(text="💬 Поддержка", callback_data="contact_admin", style="primary")],
          ]
      )

      banner_path = get_banner_path("abandoned_cart.jpg")
      if "abandoned_cart.jpg" in BANNER_CACHE:
        photo = BANNER_CACHE["abandoned_cart.jpg"]
        await bot.send_photo(chat_id=user_id, photo=photo, caption=reminder_text, reply_markup=keyboard, parse_mode="HTML")
      elif banner_path and os.path.exists(banner_path):
        photo = FSInputFile(banner_path)
        sent = await bot.send_photo(
            chat_id=user_id, photo=photo, caption=reminder_text, reply_markup=keyboard, parse_mode="HTML"
        )
        if sent.photo:
          BANNER_CACHE["abandoned_cart.jpg"] = sent.photo[-1].file_id
      else:
        await bot.send_message(chat_id=user_id, text=reminder_text, reply_markup=keyboard, parse_mode="HTML")

      ABANDONED_CHECKOUTS.pop(user_id, None)
      await delete_abandoned_reminder_db(user_id)
  except asyncio.CancelledError:
    pass


def get_order_details_text(data):
  return f"• Цель: <b>{data.get('target_username')}</b>\n"


# === ОБРАБОТЧИКИ КОМАНД ===
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
  await track_user(message.from_user)
  await show_main_menu(message, state)


@router.message(Command("restart"))
async def cmd_restart(message: Message, state: FSMContext):
  await message.reply("🔄 Бот перезапущен, состояние очищено.")
  await show_main_menu(message, state)


@router.message(Command("order"))
async def cmd_order(message: Message, state: FSMContext):
  await track_user(message.from_user)
  user_id = message.from_user.id
  if user_id not in KNOWN_USERS:
    KNOWN_USERS[user_id] = datetime.datetime.now()
  await track_client_step(message.bot, message.from_user, "catalog")
  await state.set_state(OrderForm.service)
  text = "🛒 <b>Каталог услуг</b>\n\nВыберите подходящий вариант из списка ниже:"
  await render_step(message, state, "catalog.jpg", text, _build_catalog_keyboard())


@router.message(Command("my_orders"))
async def cmd_my_orders_msg(message: Message, state: FSMContext):
  await track_user(message.from_user)
  user_id = message.from_user.id
  orders = await get_user_orders_from_db(user_id)
  if not orders:
    text = "📦 <b>Мои заказы</b>\n\nУ вас пока нет активных или завершенных заказов."
  else:
    text = "📦 <b>История ваших заказов:</b>\n\n"
    for ord_data in orders:
      text += (
          f"<b>Заказ #{ord_data['id']}</b>\n"
          f"• Услуга: {ord_data['service_name']}\n"
          f"• Сумма: {_format_price(ord_data['total_price'])}$\n"
          f"• Статус: {ord_data['status']}\n\n"
      )
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_home", style="danger")]]
  )
  await render_step(message, state, "my_orders.jpg", text, keyboard)


@router.message(Command("reputation"))
async def cmd_reputation_msg(message: Message, state: FSMContext):
  await track_user(message.from_user)
  completed, in_progress, new_requests = get_reputation_stats()
  text = (
      "📊 <b>Репутация сервиса</b>\n\n"
      f"• Успешно выполнено задач: <b>{completed:,}</b>\n"
      f"• В активной работе: <b>{in_progress}</b>\n"
      f"• Новых заявок сегодня: <b>{new_requests}</b>\n\n"
      "<i>Конфиденциальность и безопасность гарантированы.</i>"
  )
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_home", style="danger")]]
  )
  await render_step(message, state, "reputation.jpg", text, keyboard)


@router.callback_query(F.data == "back_home")
async def back_home(callback: CallbackQuery, state: FSMContext):
  user_id = callback.from_user.id
  task = ABANDONED_CHECKOUTS.pop(user_id, None)
  if task:
    task.cancel()
  await delete_abandoned_reminder_db(user_id)
  await show_main_menu(callback, state)


@router.callback_query(F.data == "about_service")
async def show_reputation(callback: CallbackQuery, state: FSMContext):
  completed, in_progress, new_requests = get_reputation_stats()
  text = (
      "📊 <b>Репутация сервиса</b>\n\n"
      f"• Успешно выполнено задач: <b>{completed:,}</b>\n"
      f"• В активной работе: <b>{in_progress}</b>\n"
      f"• Новых заявок сегодня: <b>{new_requests}</b>\n\n"
      "<i>Конфиденциальность и безопасность гарантированы.</i>"
  )
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_home", style="danger")]]
  )
  await render_step(callback, state, "reputation.jpg", text, keyboard)


@router.callback_query(F.data == "my_orders")
async def show_my_orders(callback: CallbackQuery, state: FSMContext):
  user_id = callback.from_user.id
  orders = await get_user_orders_from_db(user_id)
  if not orders:
    text = "📦 <b>Мои заказы</b>\n\nУ вас пока нет активных или завершенных заказов."
  else:
    text = "📦 <b>История ваших заказов:</b>\n\n"
    for ord_data in orders:
      text += (
          f"<b>Заказ #{ord_data['id']}</b>\n"
          f"• Услуга: {ord_data['service_name']}\n"
          f"• Сумма: {_format_price(ord_data['total_price'])}$\n"
          f"• Статус: {ord_data['status']}\n\n"
      )
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_home", style="danger")]]
  )
  await render_step(callback, state, "my_orders.jpg", text, keyboard)


@router.callback_query(F.data == "contact_admin")
async def contact_admin_info(callback: CallbackQuery, state: FSMContext):
  await state.set_state(SupportForm.reason)
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="back_home", style="primary")]]
  )
  text = (
      "💬 <b>Служба поддержки</b>\n\n"
      "Опишите вашу ситуацию или задайте вопрос (оплата, статус заказа и т.д.):"
  )
  await render_step(callback, state, "support.jpg", text, keyboard)


@router.message(SupportForm.reason, F.text)
async def process_support_reason(message: Message, state: FSMContext):
  await track_user(message.from_user)
  await state.update_data(reason=message.text.strip())
  await state.set_state(SupportForm.photo)
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[
          [InlineKeyboardButton(text="⏩ Пропустить", callback_data="skip_support_photo", style="danger")],
          [InlineKeyboardButton(text="⬅️ Отмена", callback_data="back_home", style="primary")],
      ]
  )
  text = "📸 Отправьте фото при необходимости или нажмите пропустить:"
  await render_step(message, state, "support.jpg", text, keyboard)


async def _send_support_ticket_to_staff(bot: Bot, user, reason_text: str, photo_id: str | None = None):
  user_badge = _format_user_badge(user.id, user.username)
  journey_summary = _build_client_journey_summary(user.id)
  ticket_text = (
      f"📩 <b>Новое обращение в поддержку</b>\n\n"
      f"• От кого: {user_badge}\n"
      f"• Вопрос: {reason_text}\n\n"
      f"{journey_summary}"
  )
  kb = _staff_ticket_keyboard(user.id, "support")
  for uid in _get_staff_notify_ids():
    try:
      if photo_id:
        await bot.send_photo(chat_id=uid, photo=photo_id, caption=ticket_text, reply_markup=kb, parse_mode="HTML")
      else:
        await bot.send_message(chat_id=uid, text=ticket_text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
      logger.error(f"Ошибка отправки обращения в поддержку ({uid}): {e}")


@router.message(SupportForm.photo, F.photo)
async def process_support_photo(message: Message, state: FSMContext):
  await track_user(message.from_user)
  photo_id = message.photo[-1].file_id
  data = await state.get_data()
  reason_text = data.get("reason") or "Без описания"
  await _send_support_ticket_to_staff(message.bot, message.from_user, reason_text, photo_id=photo_id)
  text = "✅ Обращение отправлено! Специалист скоро свяжется с вами."
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="🏠 На главную", callback_data="back_home", style="primary")]]
  )
  await render_step(message, state, "main.jpg", text, keyboard)
  await state.clear()


@router.message(SupportForm.photo, F.text)
async def process_support_photo_text_fallback(message: Message, state: FSMContext):
  """Если пользователь в шаге фото написал текст вместо фото — дописываем его к обращению и отправляем."""
  await track_user(message.from_user)
  data = await state.get_data()
  prev_reason = data.get("reason") or ""
  extra = message.text.strip()
  combined = f"{prev_reason}\n{extra}".strip() if prev_reason else extra
  await _send_support_ticket_to_staff(message.bot, message.from_user, combined, photo_id=None)
  text = "✅ Обращение отправлено! Специалист скоро свяжется с вами."
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="🏠 На главную", callback_data="back_home", style="primary")]]
  )
  await state.clear()
  await render_step(message, state, "main.jpg", text, keyboard)


@router.callback_query(SupportForm.photo, F.data == "skip_support_photo")
async def skip_support_photo(callback: CallbackQuery, state: FSMContext):
  await track_user(callback.from_user)
  data = await state.get_data()
  reason_text = (
      data.get("reason")
      or (callback.message.caption if callback.message and callback.message.caption else "Обращение без текста")
  )
  await _send_support_ticket_to_staff(callback.bot, callback.from_user, reason_text, photo_id=None)
  text = "✅ Обращение отправлено! Специалист скоро свяжется с вами."
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="🏠 На главную", callback_data="back_home", style="primary")]]
  )
  await state.clear()
  await render_step(callback, state, "main.jpg", text, keyboard)


# === КАТАЛОГ И ОФОРМЛЕНИЕ ЗАКАЗА ===
@router.callback_query(F.data.in_(["catalog_services", "start_order"]))
async def process_start_order(callback: CallbackQuery, state: FSMContext):
  user_id = callback.from_user.id
  task = ABANDONED_CHECKOUTS.pop(user_id, None)
  if task:
    task.cancel()
  await delete_abandoned_reminder_db(user_id)
  await track_client_step(callback.bot, callback.from_user, "catalog")

  await state.set_state(OrderForm.service)
  text = "🛒 <b>Каталог услуг</b>\n\nВыберите подходящий вариант из списка ниже:"
  await render_step(callback, state, "catalog.jpg", text, _build_catalog_keyboard())


@router.callback_query(OrderForm.service, F.data == "srv_special_order")
async def process_special_order_service(callback: CallbackQuery, state: FSMContext):
  service_name = "Особый заказ — Рассчитать стоимость"
  await state.update_data(service_name=service_name, total_price=0)
  await state.set_state(OrderForm.target_username)
  await track_client_step(callback.bot, callback.from_user, "service_selected", service_name=service_name, price=0)

  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="catalog_services", style="danger")]]
  )
  text = (
      f"🛠 Услуга: <b>{service_name}</b>\n\n"
      f"📝 <b>Описание задачи</b>\n\n"
      f"Опишите задачу следующим сообщением:"
  )
  await render_step(callback, state, "target_data.jpg", text, keyboard)


@router.callback_query(OrderForm.service, F.data.startswith("srv_"))
async def process_service(callback: CallbackQuery, state: FSMContext):
  parts = callback.data.split("_")
  price = int(parts[-1])
  service_name = " ".join(parts[1:-1]).replace("_", " ")

  await state.update_data(service_name=service_name, total_price=price)
  await state.set_state(OrderForm.target_username)
  await track_client_step(callback.bot, callback.from_user, "service_selected", service_name=service_name, price=price)

  link_services = ["снос сайта", "снос tg канала или бота", "ddos-атака сайта", "слив базы сайта"]
  if service_name.lower() in link_services:
    target_prompt = "Введите ссылку на ресурс цели следующим сообщением:"
  else:
    target_prompt = "Введите юзернейм или ссылку на профиль цели следующим сообщением:"

  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="catalog_services", style="danger")]]
  )
  text = (
      f"⚡️ Услуга: <b>{service_name} ({price}$)</b>\n\n"
      f"🎯 <b>Данные цели</b>\n\n"
      f"{target_prompt}"
  )
  await render_step(callback, state, "target_data.jpg", text, keyboard)


@router.message(OrderForm.target_username, F.text)
async def process_target_username(message: Message, state: FSMContext):
  await track_user(message.from_user)
  data = await state.get_data()
  price = data.get("total_price", 0)

  if not price:
    special_task_text = message.text.strip()
    await state.update_data(special_task=special_task_text)
    await state.set_state(OrderForm.special_link)
    await track_client_step(
        message.bot,
        message.from_user,
        "target_entered",
        service_name=data.get("service_name"),
        price=0,
        target=special_task_text,
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="catalog_services", style="danger")]]
    )
    text = (
        f"🛠 Услуга: <b>{data.get('service_name')}</b>\n\n"
        f"🔗 <b>Ссылка на цель</b>\n\n"
        f"Отправьте ссылку на цель следующим сообщением:"
    )
    await render_step(message, state, "target_data.jpg", text, keyboard)
    return

  target_val = message.text.strip()
  await state.update_data(target_username=target_val)
  updated_data = await state.get_data()
  await track_client_step(
      message.bot,
      message.from_user,
      "summary",
      service_name=updated_data.get("service_name"),
      price=price,
      target=target_val,
  )
  summary_text = (
      "✨ <b>Сводка заказа:</b>\n\n"
      f"• Услуга: <b>{updated_data.get('service_name')}</b>\n"
      f"{get_order_details_text(updated_data)}"
      f"💵 <b>К оплате: {_format_price(price)} USDT</b>"
  )
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[
          [InlineKeyboardButton(text="💳 Перейти к оплате", callback_data="go_to_payment", style="primary")],
          [InlineKeyboardButton(text="⬅️ Назад", callback_data="catalog_services", style="danger")],
      ]
  )
  await render_step(message, state, "summary.jpg", summary_text, keyboard)


@router.message(OrderForm.special_link, F.text)
async def process_special_order_link(message: Message, state: FSMContext):
  await track_user(message.from_user)
  link = message.text.strip()
  data = await state.get_data()
  task_desc = data.get("special_task", "")
  user = message.from_user
  user_badge = _format_user_badge(user.id, user.username)
  await track_client_step(
      message.bot,
      user,
      "target_entered",
      service_name=data.get("service_name") or "Особый заказ",
      price=0,
      target=f"{task_desc} | {link}",
  )
  journey_summary = _build_client_journey_summary(user.id)

  ticket_text = (
      f"🛠 <b>Заявка на особый заказ (Рассчитать стоимость)</b>\n\n"
      f"• Клиент: {user_badge}\n"
      f"• Задача: {task_desc}\n"
      f"• Ссылка: {link}\n\n"
      f"{journey_summary}\n\n"
      f"Выставить счёт: <code>/bill {user.id} особый заказ 220$</code>"
  )
  kb = _staff_ticket_keyboard(user.id, "special")
  for uid in _get_staff_notify_ids():
    try:
      await message.bot.send_message(chat_id=uid, text=ticket_text, reply_markup=kb, parse_mode="HTML")
    except Exception:
      pass

  text = "✅ Заявка отправлена менеджеру! В течение 25 минут с вами свяжется специалист для расчёта стоимости."
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="🏠 На главную", callback_data="back_home", style="primary")]]
  )
  await state.clear()
  await render_step(message, state, "main.jpg", text, keyboard)


@router.callback_query(F.data == "go_to_payment")
async def process_go_to_payment(callback: CallbackQuery, state: FSMContext):
  user_id = callback.from_user.id
  ctx = await _get_payment_context(user_id, state)
  total_price = ctx["total_price"]
  await track_client_step(
      callback.bot,
      callback.from_user,
      "payment_menu",
      service_name=ctx["service_name"],
      price=total_price,
      target=ctx["target_username"],
  )

  if user_id in ABANDONED_CHECKOUTS:
    task = ABANDONED_CHECKOUTS.pop(user_id, None)
    if task:
      task.cancel()
  remind_at = datetime.datetime.now() + datetime.timedelta(seconds=1800)
  await save_abandoned_reminder_db(user_id, remind_at)
  ABANDONED_CHECKOUTS[user_id] = asyncio.create_task(
      delayed_abandoned_cart_reminder(callback.bot, user_id, delay_seconds=1800.0)
  )

  payment_text = (
      "⏳ <b>Оплата заказа</b>\n"
      "Время на завершение: <b><u>20:00</u></b>\n\n"
      f"💳 <b>Выберите способ оплаты</b>\n"
      f"К оплате: <b>{_format_price(total_price)} USDT</b>"
  )
  await render_step(callback, state, "payment.jpg", payment_text, _build_payment_keyboard())


@router.callback_query(F.data.startswith("pay_currency_"))
async def process_pay_currency(callback: CallbackQuery, state: FSMContext):
  await state.update_data(payment_method="card")
  curr_code = callback.data.split("_")[2]
  config = CURRENCY_CONFIG.get(curr_code, {"country": "Россия", "name": "Рубли", "symbol": "₽"})
  country = config["country"]

  ctx = await _get_payment_context(callback.from_user.id, state)
  total_price = ctx["total_price"]
  await track_client_step(
      callback.bot,
      callback.from_user,
      "payment_method_selected",
      service_name=ctx["service_name"],
      price=total_price,
      target=ctx["target_username"],
  )
  rate = await get_exchange_rate(curr_code)
  local_amount = round(total_price * rate)

  card = get_next_card(country)
  if not card:
    text = (
        f"❌ <b>Реквизиты временно недоступны</b>\n\n"
        f"Сейчас реквизиты для оплаты в <b>{config['name']}</b> временно недоступны, мы решаем эту проблему. "
        f"Оплатите другим способом (CryptoBot или криптовалюта) 👇"
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Оплата через CryptoBot", callback_data="pay_cryptobot", style="primary")],
            [InlineKeyboardButton(text="🪙 Реквизиты (Крипта)", callback_data="crypto_expert", style="danger")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="go_to_payment", style="danger")],
        ]
    )
    await render_step(callback, state, "payment.jpg", text, keyboard)
    return

  sbp_line = "<b>Метод:</b> Карта / СБП / Перевод по номеру\n" if curr_code == "RUB" else "<b>Метод:</b> Карта / Перевод по номеру\n"
  text = (
      f"💳 <b>Оплата ({config['name']})</b>\n\n"
      f"• Итого к оплате: <b>{local_amount:,} {config['symbol']}</b> <i>({_format_price(total_price)} USDT)</i>\n\n"
      f"⚠️ <b>Переводите строго на реквизиты ниже:</b>\n\n"
      f"{sbp_line}"
      f"<b>Банк:</b> {card['bank']}\n"
      f"<b>Реквизиты:</b> <code>{card['details']}</code>\n\n"
      "💡 <i>После перевода нажмите кнопку ниже для отправки чека или квитанции.</i>"
  )
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[
          [InlineKeyboardButton(text="✅ Прикрепить чек", callback_data="ask_for_receipt", style="primary")],
          [InlineKeyboardButton(text="⬅️ Назад", callback_data="go_to_payment", style="danger")],
      ]
  )
  await render_step(callback, state, "payment.jpg", text, keyboard)


@router.callback_query(F.data == "payment_help_manager")
async def process_payment_help_manager(callback: CallbackQuery):
  user = callback.from_user
  user_badge = _format_user_badge(user.id, user.username)
  journey_summary = _build_client_journey_summary(user.id)
  help_text = (
      f"🚨 <b>Проблема / запрос помощи с оплатой</b>\n\n"
      f"• Клиент: {user_badge}\n\n"
      f"{journey_summary}"
  )
  kb = _staff_ticket_keyboard(user.id, "payment")
  for uid in _get_staff_notify_ids():
    try:
      await callback.bot.send_message(chat_id=uid, text=help_text, reply_markup=kb, parse_mode="HTML")
    except Exception:
      pass
  await callback.answer("✅ Менеджер уведомлен, скоро свяжется с вами.", show_alert=True)


@router.callback_query(F.data == "pay_cryptobot")
async def process_pay_cryptobot(callback: CallbackQuery, state: FSMContext):
  ctx = await _get_payment_context(callback.from_user.id, state)
  total_price = ctx["total_price"]
  await track_client_step(
      callback.bot,
      callback.from_user,
      "payment_method_selected",
      service_name=ctx["service_name"],
      price=total_price,
      target=ctx["target_username"],
  )

  pay_url, inv_id = await create_cryptobot_invoice(total_price, "Оплата услуг")
  if not pay_url:
    await callback.answer("❌ Сервис недоступен.", show_alert=True)
    return

  await state.update_data(invoice_id=inv_id)
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[
          [InlineKeyboardButton(text="🔗 Оплатить счет", url=pay_url, style="primary")],
          [InlineKeyboardButton(text="✅ Проверить оплату", callback_data="cryptobot_check_success", style="danger")],
          [InlineKeyboardButton(text="⬅️ Назад", callback_data="go_to_payment", style="danger")],
      ]
  )
  text = (
      f"🤖 <b>Оплата через CryptoBot</b>\n\n"
      f"К оплате: <b>{_format_price(total_price)} USDT</b>\n\n"
      f"После оплаты нажмите кнопку проверки:"
  )
  await render_step(callback, state, "payment.jpg", text, keyboard)


@router.callback_query(F.data == "cryptobot_check_success")
async def process_cryptobot_check(callback: CallbackQuery, state: FSMContext):
  data = await state.get_data()
  inv_id = data.get("invoice_id")

  is_paid = await check_cryptobot_invoice_status(inv_id)
  if not is_paid:
    await callback.answer("⏳ Платеж еще не поступил. Попробуйте через минуту.", show_alert=True)
    return

  user = callback.from_user
  _cancel_unpaid_followup(user.id)
  task = ABANDONED_CHECKOUTS.pop(user.id, None)
  if task:
    task.cancel()
  await delete_abandoned_reminder_db(user.id)

  ctx = await _get_payment_context(user.id, state)
  service_name = ctx["service_name"]
  total_price = ctx["total_price"]
  target_username = ctx["target_username"]
  target_info = f"Цель: {target_username}"

  await add_order_to_db(user.id, service_name, total_price, "✅ Оплачено через CryptoBot", target_info)
  PENDING_BILLS.pop(user.id, None)
  await delete_pending_bill_db(user.id)

  notify_ids = {uid for uid in (ADMIN_ID, MANAGER_ID) if uid}
  for uid in notify_ids:
    try:
      await callback.bot.send_message(
          chat_id=uid,
          text=(
              f"🤖 Успешная оплата CryptoBot от @{user.username} (<code>{user.id}</code>)\n"
              f"• Услуга: {service_name}\n"
              f"• Цель: {target_username}"
          ),
          parse_mode="HTML",
      )
    except Exception:
      pass

  text = "✅ <b>Оплата подтверждена!</b>\n\nВаша заявка принята в работу."
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="🏠 На главную", callback_data="back_home", style="primary")]]
  )
  await render_step(callback, state, "main.jpg", text, keyboard)
  await state.clear()


@router.callback_query(F.data == "crypto_expert")
async def process_crypto_expert(callback: CallbackQuery, state: FSMContext):
  await state.update_data(payment_method="crypto")
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[
          [InlineKeyboardButton(text="💎 USDT (TRC-20)", callback_data="pay_crypto_trc20", style="primary")],
          [InlineKeyboardButton(text="💵 USDT (BEP-20)", callback_data="pay_crypto_bep20", style="danger")],
          [InlineKeyboardButton(text="🪙 USDT (TON)", callback_data="pay_crypto_ton", style="primary")],
          [InlineKeyboardButton(text="🪙 Bitcoin (BTC)", callback_data="pay_crypto_btc", style="danger")],
          [InlineKeyboardButton(text="⬅️ Назад", callback_data="go_to_payment", style="danger")],
      ]
  )
  text = "🪙 <b>Реквизиты (Крипта)</b>\n\nВыберите криптовалютную сеть для перевода:"
  await render_step(callback, state, "payment.jpg", text, keyboard)


@router.callback_query(F.data.startswith("pay_crypto_"))
async def process_pay_crypto(callback: CallbackQuery, state: FSMContext):
  await state.update_data(payment_method="crypto")
  net_key = callback.data.split("_")[2]
  net_info = CRYPTO_NETWORKS.get(net_key)
  if not net_info:
    await callback.answer("Сеть не найдена", show_alert=True)
    return

  ctx = await _get_payment_context(callback.from_user.id, state)
  total_price = ctx["total_price"]
  await track_client_step(
      callback.bot,
      callback.from_user,
      "payment_method_selected",
      service_name=ctx["service_name"],
      price=total_price,
      target=ctx["target_username"],
  )
  wallet = get_next_crypto_wallet(net_key)

  if not wallet:
    text = (
        "❌ <b>Реквизиты временно недоступны</b>\n\n"
        "Сейчас реквизиты временно недоступны, мы решаем эту проблему. "
        "Оплатите другим способом (CryptoBot или карта/валюта) 👇"
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🤖 Оплата через CryptoBot", callback_data="pay_cryptobot", style="primary")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_expert", style="danger")],
        ]
    )
    await render_step(callback, state, "payment.jpg", text, keyboard)
    return

  text = (
      f"💳 <b>Оплата {net_info['name']}</b>\n\n"
      f"• К оплате: <b>{_format_price(total_price)} USDT</b>\n"
      f"• Монета: <b>{wallet['asset']}</b>\n"
      f"• Сеть: <b>{wallet['network']}</b>\n"
      f"• Реквизиты:\n<code>{wallet['address']}</code>\n\n"
      "💡 <i>После перевода прикрепите скриншот оплаты перевода.</i>"
  )
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[
          [InlineKeyboardButton(text="✅ Прикрепить чек", callback_data="ask_for_receipt", style="primary")],
          [InlineKeyboardButton(text="⬅️ Назад", callback_data="crypto_expert", style="danger")],
      ]
  )
  await render_step(callback, state, "payment.jpg", text, keyboard)


@router.callback_query(F.data == "ask_for_receipt")
async def ask_for_receipt(callback: CallbackQuery, state: FSMContext):
  await state.set_state(OrderForm.receipt)
  ctx = await _get_payment_context(callback.from_user.id, state)
  await track_client_step(
      callback.bot,
      callback.from_user,
      "receipt_requested",
      service_name=ctx["service_name"],
      price=ctx["total_price"],
      target=ctx["target_username"],
  )
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="go_to_payment", style="danger")]]
  )
  if ctx["payment_method"] == "crypto":
    text = "🧾 Отправьте фото скриншот оплаты перевода следующим сообщением:"
  else:
    text = "🧾 Отправьте фото чека или документ квитанции об оплате следующим сообщением:"
  await render_step(callback, state, "payment.jpg", text, keyboard)


@router.message(OrderForm.receipt, F.photo)
async def process_receipt_photo(message: Message, state: FSMContext):
  user = message.from_user
  _cancel_unpaid_followup(user.id)
  task = ABANDONED_CHECKOUTS.pop(user.id, None)
  if task:
    task.cancel()
  await delete_abandoned_reminder_db(user.id)

  ctx = await _get_payment_context(user.id, state)
  service_name = ctx["service_name"]
  total_price = ctx["total_price"]
  target_username = ctx["target_username"]
  target_info = f"Цель: {target_username}"

  await add_order_to_db(user.id, service_name, total_price, "⏳ Ожидает проверки чека", target_info)
  PENDING_BILLS.pop(user.id, None)
  await delete_pending_bill_db(user.id)

  notify_ids = {uid for uid in (ADMIN_ID, MANAGER_ID) if uid}
  for uid in notify_ids:
    try:
      await message.bot.send_photo(
          chat_id=uid,
          photo=message.photo[-1].file_id,
          caption=(
              f"🛒 Новый чек от @{user.username} (<code>{user.id}</code>)\n"
              f"• Услуга: {service_name}\n"
              f"• Цель: {target_username}\n"
              f"• К оплате: {_format_price(total_price)} USDT"
          ),
          parse_mode="HTML",
      )
    except Exception:
      pass

  text = "✅ Чек получен! Менеджер проверит платеж в течение 25 минут."
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="🏠 На главную", callback_data="back_home", style="primary")]]
  )
  await render_step(message, state, "main.jpg", text, keyboard)
  await state.clear()


@router.message(OrderForm.receipt, F.document)
async def process_receipt_document(message: Message, state: FSMContext):
  ctx = await _get_payment_context(message.from_user.id, state)
  if ctx["payment_method"] == "crypto":
    await message.reply("⚠️ Для оплаты криптой отправьте, пожалуйста, фото скриншот оплаты перевода.")
    return

  user = message.from_user
  _cancel_unpaid_followup(user.id)
  task = ABANDONED_CHECKOUTS.pop(user.id, None)
  if task:
    task.cancel()
  await delete_abandoned_reminder_db(user.id)

  service_name = ctx["service_name"]
  total_price = ctx["total_price"]
  target_username = ctx["target_username"]
  target_info = f"Цель: {target_username}"

  await add_order_to_db(user.id, service_name, total_price, "⏳ Ожидает проверки чека", target_info)
  PENDING_BILLS.pop(user.id, None)
  await delete_pending_bill_db(user.id)

  notify_ids = {uid for uid in (ADMIN_ID, MANAGER_ID) if uid}
  for uid in notify_ids:
    try:
      await message.bot.send_document(
          chat_id=uid,
          document=message.document.file_id,
          caption=(
              f"🛒 Новая квитанция (документ) от @{user.username} (<code>{user.id}</code>)\n"
              f"• Услуга: {service_name}\n"
              f"• Цель: {target_username}\n"
              f"• К оплате: {_format_price(total_price)} USDT"
          ),
          parse_mode="HTML",
      )
    except Exception:
      pass

  text = "✅ Квитанция получена! Менеджер проверит платеж в течение 25 минут."
  keyboard = InlineKeyboardMarkup(
      inline_keyboard=[[InlineKeyboardButton(text="🏠 На главную", callback_data="back_home", style="primary")]]
  )
  await render_step(message, state, "main.jpg", text, keyboard)
  await state.clear()


# === ЗАЩИТНЫЕ (FALLBACK) ХЕНДЛЕРЫ ===
@router.message(OrderForm.target_username)
async def fallback_target_username(message: Message):
  await message.reply("⚠️ Пожалуйста, отправьте текстовое сообщение с описанием задачи.")


@router.message(OrderForm.special_link)
async def fallback_special_link(message: Message):
  await message.reply("⚠️ Пожалуйста, отправьте текстовое сообщение со ссылкой на цель.")


@router.message(OrderForm.receipt)
async def fallback_order_receipt(message: Message, state: FSMContext):
  ctx = await _get_payment_context(message.from_user.id, state)
  if ctx["payment_method"] == "crypto":
    await message.reply("⚠️ Пожалуйста, отправьте фото скриншот оплаты перевода.")
  else:
    await message.reply("⚠️ Пожалуйста, отправьте фото чека или документ квитанции об оплате.")


@router.message(SupportForm.reason)
async def fallback_support_message(message: Message):
  await message.reply("⚠️ Пожалуйста, отправьте ваше сообщение текстовым форматом.")


# === WEB SERVER FOR RENDER / RAILWAY ===
async def handle_ping(request):
  return web.Response(text="Bot is running!")


async def start_web_server():
  app = web.Application()
  app.router.add_get("/", handle_ping)
  runner = web.AppRunner(app)
  await runner.setup()
  port = int(os.getenv("PORT", 10000))
  site = web.TCPSite(runner, "0.0.0.0", port)
  await site.start()


async def main():
  await init_db()
  bot = Bot(token=BOT_TOKEN)
  await load_persistent_state(bot=bot)
  dp = Dispatcher()
  dp.include_router(router)

  try:
    await bot.set_my_commands([
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="restart", description="🔄 Перезапуск / Сброс бота"),
        BotCommand(command="order", description="Заказать услугу"),
        BotCommand(command="my_orders", description="Мои заказы"),
        BotCommand(command="reputation", description="Репутация"),
    ])
  except Exception as e:
    logger.warning(f"Не удалось установить команды: {e}")

  asyncio.create_task(start_web_server())
  asyncio.create_task(scheduled_broadcast(bot))
  await bot.delete_webhook(drop_pending_updates=True)

  while True:
    try:
      logger.info("Запуск поллинга кибер-бота...")
      await dp.start_polling(bot)
    except Exception as e:
      logger.error(f"Ошибка поллинга: {e}. Переподключение через 5 секунд...")
      await asyncio.sleep(5)


if __name__ == "__main__":
  asyncio.run(main())
