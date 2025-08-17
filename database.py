import asyncio
import logging
import os
import json
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date
from contextlib import asynccontextmanager

# Імпорти для PostgreSQL та SQLite
try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False
    print("⚠️ asyncpg не встановлено. Використовується SQLite")

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False
    print("⚠️ aiosqlite не встановлено")

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.use_postgres = os.getenv('DATABASE_URL') and ASYNCPG_AVAILABLE
        self.pool = None
        self.sqlite_path = "bot_database.db"

        if self.use_postgres:
            logger.info("🐘 Використовується PostgreSQL")
        else:
            logger.info("📁 Використовується SQLite")

    async def init_pool(self):
        """Ініціалізація пулу з'єднань"""
        if self.use_postgres:
            database_url = os.getenv('DATABASE_URL')
            self.pool = await asyncpg.create_pool(
                database_url,
                min_size=2,
                max_size=10,
                command_timeout=30
            )
            await self._init_postgres_schema()
        else:
            # SQLite ініціалізується при кожному з'єднанні
            await self._init_sqlite_schema()

    async def close(self):
        """Закриття пулу"""
        if self.pool:
            await self.pool.close()

    @asynccontextmanager
    async def get_connection(self):
        """Контекстний менеджер для з'єднання"""
        if self.use_postgres:
            async with self.pool.acquire() as conn:
                yield conn
        else:
            async with aiosqlite.connect(self.sqlite_path) as conn:
                conn.row_factory = aiosqlite.Row
                await conn.execute("PRAGMA foreign_keys=ON")
                yield conn

    async def _init_postgres_schema(self):
        """Створення PostgreSQL схеми"""
        schema_sql = """
        -- Users table
        CREATE TABLE IF NOT EXISTS users (
            id               BIGSERIAL PRIMARY KEY,
            tg_id            BIGINT UNIQUE NOT NULL,
            username         TEXT,
            first_name       TEXT,
            lang             TEXT NOT NULL DEFAULT 'uk',
            last_city        TEXT,
            is_active        BOOLEAN NOT NULL DEFAULT TRUE,
            is_blocked       BOOLEAN NOT NULL DEFAULT FALSE,
            created_at       timestamptz NOT NULL DEFAULT now(),
            updated_at       timestamptz NOT NULL DEFAULT now(),
            last_seen_at     timestamptz,
            utm_source       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_users_tg_id ON users(tg_id);
        CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active);
        CREATE INDEX IF NOT EXISTS idx_users_last_seen_at ON users(last_seen_at DESC);

        -- Sessions (FSM)
        CREATE TABLE IF NOT EXISTS sessions (
            id           BIGSERIAL PRIMARY KEY,
            user_id      BIGINT REFERENCES users(id) ON DELETE CASCADE,
            state        TEXT NOT NULL,
            payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
            updated_at   timestamptz NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);

        -- Cities
        CREATE TABLE IF NOT EXISTS cities (
            code          TEXT PRIMARY KEY,
            name_uk       TEXT NOT NULL,
            channel_url   TEXT,
            is_active     BOOLEAN NOT NULL DEFAULT TRUE,
            created_at    timestamptz NOT NULL DEFAULT now(),
            updated_at    timestamptz NOT NULL DEFAULT now()
        );

        -- City aliases
        CREATE TABLE IF NOT EXISTS city_aliases (
            id        BIGSERIAL PRIMARY KEY,
            city_code TEXT REFERENCES cities(code) ON DELETE CASCADE,
            alias     TEXT NOT NULL,
            UNIQUE(city_code, alias)
        );

        CREATE INDEX IF NOT EXISTS idx_city_alias ON city_aliases(alias);

        -- User city history
        CREATE TABLE IF NOT EXISTS user_city_history (
            id             BIGSERIAL PRIMARY KEY,
            user_id        BIGINT REFERENCES users(id) ON DELETE CASCADE,
            city_code      TEXT REFERENCES cities(code),
            city_name_uk   TEXT NOT NULL,
            selected_at    timestamptz NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_uch_user_id ON user_city_history(user_id, selected_at DESC);

        -- Rental requests
        CREATE TABLE IF NOT EXISTS rental_requests (
            id            BIGSERIAL PRIMARY KEY,
            user_id       BIGINT REFERENCES users(id) ON DELETE SET NULL,
            city_code     TEXT REFERENCES cities(code),
            contact       TEXT,
            description   TEXT,
            status        TEXT NOT NULL DEFAULT 'new',
            created_at    timestamptz NOT NULL DEFAULT now(),
            updated_at    timestamptz NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_rr_status ON rental_requests(status);

        -- Broadcasts
        CREATE TABLE IF NOT EXISTS broadcasts (
            id              BIGSERIAL PRIMARY KEY,
            title           TEXT,
            body_markdown   TEXT,
            buttons_json    JSONB,
            segment_query   JSONB,
            status          TEXT NOT NULL DEFAULT 'draft',
            created_by      BIGINT REFERENCES users(id),
            created_at      timestamptz NOT NULL DEFAULT now(),
            started_at      timestamptz,
            finished_at     timestamptz,
            stats_json      JSONB
        );

        -- Deliveries
        CREATE TABLE IF NOT EXISTS deliveries (
            id              BIGSERIAL PRIMARY KEY,
            broadcast_id    BIGINT REFERENCES broadcasts(id) ON DELETE CASCADE,
            user_id         BIGINT REFERENCES users(id) ON DELETE CASCADE,
            status          TEXT NOT NULL DEFAULT 'queued',
            attempts        INT NOT NULL DEFAULT 0,
            error_code      TEXT,
            sent_at         timestamptz,
            UNIQUE (broadcast_id, user_id)
        );

        CREATE INDEX IF NOT EXISTS idx_deliv_bcast_status ON deliveries(broadcast_id, status);
        CREATE INDEX IF NOT EXISTS idx_deliv_queued ON deliveries(broadcast_id) WHERE status='queued';

        -- Unsubscriptions
        CREATE TABLE IF NOT EXISTS unsubscriptions (
            id           BIGSERIAL PRIMARY KEY,
            user_id      BIGINT REFERENCES users(id) ON DELETE CASCADE,
            reason       TEXT,
            created_at   timestamptz NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_unsub_user ON unsubscriptions(user_id);

        -- Admin actions
        CREATE TABLE IF NOT EXISTS admin_actions (
            id            BIGSERIAL PRIMARY KEY,
            admin_tg_id   BIGINT NOT NULL,
            action        TEXT NOT NULL,
            payload_json  JSONB,
            created_at    timestamptz NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_admin_actions_time ON admin_actions(created_at DESC);

        -- Daily metrics
        CREATE TABLE IF NOT EXISTS metrics_daily (
            date              date PRIMARY KEY,
            new_users         INT NOT NULL DEFAULT 0,
            active_users      INT NOT NULL DEFAULT 0,
            blocked_users     INT NOT NULL DEFAULT 0,
            sent_messages     INT NOT NULL DEFAULT 0,
            errors_count      INT NOT NULL DEFAULT 0,
            unsubs            INT NOT NULL DEFAULT 0
        );
        """

        async with self.get_connection() as conn:
            await conn.execute(schema_sql)
            logger.info("✅ PostgreSQL схема ініціалізована")

    async def _init_sqlite_schema(self):
        """Створення SQLite схеми"""
        schema_sql = """
        -- Users table
        CREATE TABLE IF NOT EXISTS users (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id            INTEGER UNIQUE NOT NULL,
            username         TEXT,
            first_name       TEXT,
            lang             TEXT NOT NULL DEFAULT 'uk',
            last_city        TEXT,
            is_active        BOOLEAN NOT NULL DEFAULT 1,
            is_blocked       BOOLEAN NOT NULL DEFAULT 0,
            created_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_seen_at     TEXT,
            utm_source       TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_users_tg_id ON users(tg_id);
        CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active);

        -- Sessions (FSM)
        CREATE TABLE IF NOT EXISTS sessions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER REFERENCES users(id) ON DELETE CASCADE,
            state        TEXT NOT NULL,
            payload      TEXT NOT NULL DEFAULT '{}',
            updated_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);

        -- Cities
        CREATE TABLE IF NOT EXISTS cities (
            code          TEXT PRIMARY KEY,
            name_uk       TEXT NOT NULL,
            channel_url   TEXT,
            is_active     BOOLEAN NOT NULL DEFAULT 1,
            created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        -- City aliases
        CREATE TABLE IF NOT EXISTS city_aliases (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            city_code TEXT REFERENCES cities(code) ON DELETE CASCADE,
            alias     TEXT NOT NULL,
            UNIQUE(city_code, alias)
        );

        CREATE INDEX IF NOT EXISTS idx_city_alias ON city_aliases(alias);

        -- User city history
        CREATE TABLE IF NOT EXISTS user_city_history (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id        INTEGER REFERENCES users(id) ON DELETE CASCADE,
            city_code      TEXT REFERENCES cities(code),
            city_name_uk   TEXT NOT NULL,
            selected_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        -- Rental requests
        CREATE TABLE IF NOT EXISTS rental_requests (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id       INTEGER REFERENCES users(id) ON DELETE SET NULL,
            city_code     TEXT REFERENCES cities(code),
            contact       TEXT,
            description   TEXT,
            status        TEXT NOT NULL DEFAULT 'new',
            created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        -- Broadcasts
        CREATE TABLE IF NOT EXISTS broadcasts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            title           TEXT,
            body_markdown   TEXT,
            buttons_json    TEXT,
            segment_query   TEXT,
            status          TEXT NOT NULL DEFAULT 'draft',
            created_by      INTEGER REFERENCES users(id),
            created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at      TEXT,
            finished_at     TEXT,
            stats_json      TEXT
        );

        -- Deliveries
        CREATE TABLE IF NOT EXISTS deliveries (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            broadcast_id    INTEGER REFERENCES broadcasts(id) ON DELETE CASCADE,
            user_id         INTEGER REFERENCES users(id) ON DELETE CASCADE,
            status          TEXT NOT NULL DEFAULT 'queued',
            attempts        INTEGER NOT NULL DEFAULT 0,
            error_code      TEXT,
            sent_at         TEXT,
            UNIQUE (broadcast_id, user_id)
        );

        -- Unsubscriptions
        CREATE TABLE IF NOT EXISTS unsubscriptions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER REFERENCES users(id) ON DELETE CASCADE,
            reason       TEXT,
            created_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        -- Admin actions
        CREATE TABLE IF NOT EXISTS admin_actions (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_tg_id   INTEGER NOT NULL,
            action        TEXT NOT NULL,
            payload_json  TEXT,
            created_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        -- Daily metrics
        CREATE TABLE IF NOT EXISTS metrics_daily (
            date              TEXT PRIMARY KEY,
            new_users         INTEGER NOT NULL DEFAULT 0,
            active_users      INTEGER NOT NULL DEFAULT 0,
            blocked_users     INTEGER NOT NULL DEFAULT 0,
            sent_messages     INTEGER NOT NULL DEFAULT 0,
            errors_count      INTEGER NOT NULL DEFAULT 0,
            unsubs            INTEGER NOT NULL DEFAULT 0
        );
        """

        async with self.get_connection() as conn:
            await conn.executescript(schema_sql)
            await conn.commit()
            logger.info("✅ SQLite схема ініціалізована")

    # ===== КОРИСТУВАЧІ =====
    async def save_user(self, tg_id: int, username: str = None, first_name: str = None, 
                       utm_source: str = None) -> int:
        """Збереження/оновлення користувача"""
        now = datetime.utcnow().isoformat() if not self.use_postgres else None

        if self.use_postgres:
            query = """
            INSERT INTO users (tg_id, username, first_name, utm_source, last_seen_at)
            VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (tg_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_seen_at = now(),
                updated_at = now()
            RETURNING id
            """
        else:
            query = """
            INSERT INTO users (tg_id, username, first_name, utm_source, last_seen_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (tg_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_seen_at = ?,
                updated_at = ?
            """

        async with self.get_connection() as conn:
            if self.use_postgres:
                result = await conn.fetchval(query, tg_id, username, first_name, utm_source)
                return result
            else:
                await conn.execute(query, (tg_id, username, first_name, utm_source, now, now, now, now))
                await conn.commit()
                cursor = await conn.execute("SELECT id FROM users WHERE tg_id = ?", (tg_id,))
                row = await cursor.fetchone()
                return row[0] if row else None

    async def get_user_by_tg_id(self, tg_id: int) -> Optional[Dict]:
        """Отримання користувача по tg_id"""
        query = "SELECT * FROM users WHERE tg_id = $1" if self.use_postgres else "SELECT * FROM users WHERE tg_id = ?"

        async with self.get_connection() as conn:
            if self.use_postgres:
                row = await conn.fetchrow(query, tg_id)
                return dict(row) if row else None
            else:
                cursor = await conn.execute(query, (tg_id,))
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def set_user_blocked(self, tg_id: int, is_blocked: bool = True, reason: str = 'blocked'):
        """Блокування користувача"""
        now = datetime.utcnow().isoformat() if not self.use_postgres else None

        if self.use_postgres:
            update_query = """
            UPDATE users SET is_blocked = $1, is_active = $2, updated_at = now()
            WHERE tg_id = $3
            """
            unsub_query = """
            INSERT INTO unsubscriptions (user_id, reason)
            SELECT id, $1 FROM users WHERE tg_id = $2
            ON CONFLICT DO NOTHING
            """
        else:
            update_query = """
            UPDATE users SET is_blocked = ?, is_active = ?, updated_at = ?
            WHERE tg_id = ?
            """
            unsub_query = """
            INSERT OR IGNORE INTO unsubscriptions (user_id, reason, created_at)
            SELECT id, ?, ? FROM users WHERE tg_id = ?
            """

        async with self.get_connection() as conn:
            if self.use_postgres:
                async with conn.transaction():
                    await conn.execute(update_query, is_blocked, not is_blocked, tg_id)
                    if is_blocked:
                        await conn.execute(unsub_query, reason, tg_id)
            else:
                await conn.execute(update_query, (is_blocked, not is_blocked, now, tg_id))
                if is_blocked:
                    await conn.execute(unsub_query, (reason, now, tg_id))
                await conn.commit()

    async def get_users_count(self, active_only: bool = True) -> int:
        """Кількість користувачів"""
        if active_only:
            query = "SELECT COUNT(*) FROM users WHERE is_active = TRUE AND is_blocked = FALSE"
        else:
            query = "SELECT COUNT(*) FROM users"

        async with self.get_connection() as conn:
            if self.use_postgres:
                return await conn.fetchval(query)
            else:
                cursor = await conn.execute(query.replace('TRUE', '1').replace('FALSE', '0'))
                row = await cursor.fetchone()
                return row[0] if row else 0

    async def get_all_users(self) -> List[Dict]:
        """Всі користувачі для розсилки"""
        query = "SELECT id, tg_id FROM users WHERE is_active = TRUE AND is_blocked = FALSE"

        async with self.get_connection() as conn:
            if self.use_postgres:
                rows = await conn.fetch(query)
                return [{'user_id': row['id'], 'user_id': row['tg_id']} for row in rows]
            else:
                cursor = await conn.execute(query.replace('TRUE', '1').replace('FALSE', '0'))
                rows = await cursor.fetchall()
                return [{'user_id': row[0], 'user_id': row[1]} for row in rows]

    # ===== МІСТА =====
    async def find_city_by_alias(self, user_input: str) -> Optional[Dict]:
        """Пошук міста по alias"""
        if self.use_postgres:
            query = """
            SELECT c.code, c.name_uk, c.channel_url
            FROM city_aliases a
            JOIN cities c ON c.code = a.city_code
            WHERE lower(a.alias) = lower($1) AND c.is_active = TRUE
            LIMIT 1
            """
        else:
            query = """
            SELECT c.code, c.name_uk, c.channel_url
            FROM city_aliases a
            JOIN cities c ON c.code = a.city_code
            WHERE lower(a.alias) = lower(?) AND c.is_active = 1
            LIMIT 1
            """

        async with self.get_connection() as conn:
            if self.use_postgres:
                row = await conn.fetchrow(query, user_input.strip())
            else:
                cursor = await conn.execute(query, (user_input.strip(),))
                row = await cursor.fetchone()

            return dict(row) if row else None

    async def find_cities_by_prefix(self, prefix: str, limit: int = 5) -> List[Dict]:
        """Пошук міст по префіксу"""
        if self.use_postgres:
            query = """
            SELECT c.code, c.name_uk, c.channel_url
            FROM city_aliases a
            JOIN cities c ON c.code = a.city_code
            WHERE lower(a.alias) LIKE lower($1) || '%' AND c.is_active = TRUE
            ORDER BY length(a.alias) ASC
            LIMIT $2
            """
        else:
            query = """
            SELECT c.code, c.name_uk, c.channel_url
            FROM city_aliases a
            JOIN cities c ON c.code = a.city_code
            WHERE lower(a.alias) LIKE lower(?) || '%' AND c.is_active = 1
            ORDER BY length(a.alias) ASC
            LIMIT ?
            """

        async with self.get_connection() as conn:
            if self.use_postgres:
                rows = await conn.fetch(query, prefix.strip(), limit)
            else:
                cursor = await conn.execute(query, (prefix.strip(), limit))
                rows = await cursor.fetchall()

            return [dict(row) for row in rows]

    async def update_user_city(self, tg_id: int, city_code: str, city_name_uk: str):
        """Оновлення міста користувача"""
        now = datetime.utcnow().isoformat() if not self.use_postgres else None

        if self.use_postgres:
            update_query = """
            UPDATE users SET last_city = $1, updated_at = now()
            WHERE tg_id = $2
            """
            history_query = """
            INSERT INTO user_city_history (user_id, city_code, city_name_uk)
            SELECT id, $1, $2 FROM users WHERE tg_id = $3
            """
        else:
            update_query = """
            UPDATE users SET last_city = ?, updated_at = ?
            WHERE tg_id = ?
            """
            history_query = """
            INSERT INTO user_city_history (user_id, city_code, city_name_uk, selected_at)
            SELECT id, ?, ?, ? FROM users WHERE tg_id = ?
            """

        async with self.get_connection() as conn:
            if self.use_postgres:
                async with conn.transaction():
                    await conn.execute(update_query, city_name_uk, tg_id)
                    await conn.execute(history_query, city_code, city_name_uk, tg_id)
            else:
                await conn.execute(update_query, (city_name_uk, now, tg_id))
                await conn.execute(history_query, (city_code, city_name_uk, now, tg_id))
                await conn.commit()

    async def get_available_cities(self) -> List[Dict]:
        """Міста з доступними каналами"""
        if self.use_postgres:
            query = """
            SELECT code, name_uk, channel_url FROM cities
            WHERE is_active = TRUE AND channel_url IS NOT NULL AND channel_url != ''
            ORDER BY name_uk
            """
        else:
            query = """
            SELECT code, name_uk, channel_url FROM cities
            WHERE is_active = 1 AND channel_url IS NOT NULL AND channel_url != ''
            ORDER BY name_uk
            """

        async with self.get_connection() as conn:
            if self.use_postgres:
                rows = await conn.fetch(query)
            else:
                cursor = await conn.execute(query)
                rows = await cursor.fetchall()

            return [dict(row) for row in rows]

    # ===== РОЗСИЛКА =====
    async def create_broadcast(self, title: str, body_markdown: str, created_by_tg_id: int,
                             segment_query: Dict = None) -> int:
        """Створення розсилки"""
        now = datetime.utcnow().isoformat() if not self.use_postgres else None

        if self.use_postgres:
            query = """
            INSERT INTO broadcasts (title, body_markdown, created_by, segment_query)
            SELECT $1, $2, id, $3
            FROM users WHERE tg_id = $4
            RETURNING id
            """
            async with self.get_connection() as conn:
                return await conn.fetchval(query, title, body_markdown, 
                                         json.dumps(segment_query or {}), created_by_tg_id)
        else:
            query = """
            INSERT INTO broadcasts (title, body_markdown, created_by, segment_query, created_at)
            SELECT ?, ?, id, ?, ?
            FROM users WHERE tg_id = ?
            """
            async with self.get_connection() as conn:
                await conn.execute(query, (title, body_markdown, 
                                         json.dumps(segment_query or {}), now, created_by_tg_id))
                await conn.commit()
                return conn.lastrowid

    async def create_deliveries_for_broadcast(self, broadcast_id: int) -> int:
        """Створення записів доставки для всіх активних користувачів"""
        if self.use_postgres:
            query = """
            INSERT INTO deliveries (broadcast_id, user_id)
            SELECT $1, id FROM users 
            WHERE is_active = TRUE AND is_blocked = FALSE
            ON CONFLICT (broadcast_id, user_id) DO NOTHING
            """
        else:
            query = """
            INSERT OR IGNORE INTO deliveries (broadcast_id, user_id)
            SELECT ?, id FROM users 
            WHERE is_active = 1 AND is_blocked = 0
            """

        async with self.get_connection() as conn:
            if self.use_postgres:
                result = await conn.execute(query, broadcast_id)
                return len(result)
            else:
                cursor = await conn.execute(query, (broadcast_id,))
                await conn.commit()
                return cursor.rowcount

    async def get_queued_deliveries(self, broadcast_id: int, limit: int = 100) -> List[Dict]:
        """Отримання чергових доставок"""
        if self.use_postgres:
            query = """
            SELECT d.id, d.user_id, u.tg_id
            FROM deliveries d
            JOIN users u ON u.id = d.user_id
            WHERE d.broadcast_id = $1 AND d.status = 'queued'
            ORDER BY d.id
            LIMIT $2
            """
        else:
            query = """
            SELECT d.id, d.user_id, u.tg_id
            FROM deliveries d
            JOIN users u ON u.id = d.user_id
            WHERE d.broadcast_id = ? AND d.status = 'queued'
            ORDER BY d.id
            LIMIT ?
            """

        async with self.get_connection() as conn:
            if self.use_postgres:
                rows = await conn.fetch(query, broadcast_id, limit)
            else:
                cursor = await conn.execute(query, (broadcast_id, limit))
                rows = await cursor.fetchall()

            return [dict(row) for row in rows]

    async def update_delivery_status(self, delivery_id: int, status: str, 
                                   error_code: str = None):
        """Оновлення статусу доставки"""
        now = datetime.utcnow().isoformat() if not self.use_postgres else None

        if self.use_postgres:
            query = """
            UPDATE deliveries SET 
                status = $1, 
                attempts = attempts + 1,
                error_code = $2,
                sent_at = CASE WHEN $1 = 'sent' THEN now() ELSE sent_at END
            WHERE id = $3
            """
        else:
            query = """
            UPDATE deliveries SET 
                status = ?, 
                attempts = attempts + 1,
                error_code = ?,
                sent_at = CASE WHEN ? = 'sent' THEN ? ELSE sent_at END
            WHERE id = ?
            """

        async with self.get_connection() as conn:
            if self.use_postgres:
                await conn.execute(query, status, error_code, delivery_id)
            else:
                await conn.execute(query, (status, error_code, status, now, delivery_id))
                await conn.commit()

    # ===== СТАТИСТИКА =====
    async def get_admin_stats(self) -> Dict:
        """Статистика для адмін-панелі"""
        stats = {}

        async with self.get_connection() as conn:
            # Загальна кількість користувачів
            if self.use_postgres:
                stats['total_users'] = await conn.fetchval("SELECT COUNT(*) FROM users")
                stats['active_users'] = await conn.fetchval(
                    "SELECT COUNT(*) FROM users WHERE is_active = TRUE AND is_blocked = FALSE"
                )
                stats['blocked_users'] = await conn.fetchval(
                    "SELECT COUNT(*) FROM users WHERE is_blocked = TRUE"
                )
                stats['total_unsubscriptions'] = await conn.fetchval("SELECT COUNT(*) FROM unsubscriptions")

                # За останній тиждень
                stats['new_users_7d'] = await conn.fetchval(
                    "SELECT COUNT(*) FROM users WHERE created_at >= now() - interval '7 days'"
                )
                stats['unsubscribed_7d'] = await conn.fetchval(
                    "SELECT COUNT(*) FROM unsubscriptions WHERE created_at >= now() - interval '7 days'"
                )

                # Топ-5 міст
                top_cities = await conn.fetch("""
                    SELECT city_name_uk, COUNT(*) as count
                    FROM user_city_history
                    WHERE selected_at >= now() - interval '30 days'
                    GROUP BY city_name_uk
                    ORDER BY count DESC
                    LIMIT 5
                """)
            else:
                cursor = await conn.execute("SELECT COUNT(*) FROM users")
                row = await cursor.fetchone()
                stats['total_users'] = row[0] if row else 0

                cursor = await conn.execute("SELECT COUNT(*) FROM users WHERE is_active = 1 AND is_blocked = 0")
                row = await cursor.fetchone()
                stats['active_users'] = row[0] if row else 0

                cursor = await conn.execute("SELECT COUNT(*) FROM users WHERE is_blocked = 1")
                row = await cursor.fetchone()
                stats['blocked_users'] = row[0] if row else 0

                cursor = await conn.execute("SELECT COUNT(*) FROM unsubscriptions")
                row = await cursor.fetchone()
                stats['total_unsubscriptions'] = row[0] if row else 0

                # За тиждень (SQLite)
                cursor = await conn.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE created_at >= datetime('now', '-7 days')
                """)
                row = await cursor.fetchone()
                stats['new_users_7d'] = row[0] if row else 0

                cursor = await conn.execute("""
                    SELECT COUNT(*) FROM unsubscriptions 
                    WHERE created_at >= datetime('now', '-7 days')
                """)
                row = await cursor.fetchone()
                stats['unsubscribed_7d'] = row[0] if row else 0

    