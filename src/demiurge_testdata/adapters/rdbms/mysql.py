"""MySQLAdapter — aiomysql + SQLAlchemy 기반 RDBMS 어댑터"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import aiomysql
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from demiurge_testdata.adapters.base import BaseRDBMSAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import RDBMSAdapterConfig


@adapter_registry.register("mysql")
class MySQLAdapter(BaseRDBMSAdapter):
    """MySQL 어댑터.

    aiomysql을 드라이버로, SQLAlchemy async를 ORM/커넥션 풀로 사용한다.
    """

    def __init__(self, config: RDBMSAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = RDBMSAdapterConfig(port=3306, **kwargs)
        self._config = config
        self._engine = None
        self._pool: aiomysql.Pool | None = None

    @property
    def dsn(self) -> str:
        c = self._config
        return f"mysql+aiomysql://{c.user}:{c.password}@{c.host}:{c.port}/{c.database}"

    async def connect(self) -> None:
        self._engine = create_async_engine(
            self.dsn,
            pool_size=self._config.pool_size,
            max_overflow=self._config.pool_overflow,
        )
        self._pool = await aiomysql.create_pool(
            host=self._config.host,
            port=self._config.port,
            user=self._config.user,
            password=self._config.password,
            db=self._config.database,
            minsize=1,
            maxsize=self._config.pool_size,
        )

    async def disconnect(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
        if self._engine:
            await self._engine.dispose()
            self._engine = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        table = metadata.get("table", "raw_data")
        fmt = metadata.get("format", "unknown")
        compression = metadata.get("compression", "none")
        record_count = metadata.get("record_count", 0)

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                data LONGBLOB NOT NULL,
                format VARCHAR(32),
                compression VARCHAR(32),
                record_count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        insert_sql = f"""
            INSERT INTO {table} (data, format, compression, record_count)
            VALUES (%s, %s, %s, %s)
        """
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(create_sql)
                await cur.execute(insert_sql, (data, fmt, compression, record_count))
            await conn.commit()

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        table = query.get("table", "raw_data")
        sql = f"SELECT data FROM {table}"
        if limit:
            sql += f" LIMIT {limit}"

        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(sql)
            rows = await cur.fetchall()
            for row in rows:
                yield row[0]

    async def health_check(self) -> bool:
        if not self._pool:
            return False
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute("SELECT 1")
                result = await cur.fetchone()
                return result[0] == 1
        except Exception:
            return False

    async def execute_sql(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        async with AsyncSession(self._engine) as session:
            result = await session.execute(text(query), params or {})
            if result.returns_rows:
                return [dict(row._mapping) for row in result.fetchall()]
            await session.commit()
            return []

    async def create_table(self, name: str, columns: dict[str, str]) -> None:
        col_defs = ", ".join(f"`{col}` {dtype}" for col, dtype in columns.items())
        sql = f"CREATE TABLE IF NOT EXISTS `{name}` ({col_defs})"
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
            await conn.commit()

    async def bulk_insert(self, table: str, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        columns = list(records[0].keys())
        col_names = ", ".join(f"`{c}`" for c in columns)
        values = [tuple(r.get(c) for c in columns) for r in records]
        placeholders = ", ".join(["%s"] * len(columns))

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                col_defs = ", ".join(f"`{c}` TEXT" for c in columns)
                await cur.execute(
                    f"CREATE TABLE IF NOT EXISTS `{table}` "
                    f"(id INT AUTO_INCREMENT PRIMARY KEY, {col_defs})"
                )
                sql = f"INSERT INTO `{table}` ({col_names}) VALUES ({placeholders})"
                await cur.executemany(sql, values)
            await conn.commit()
        return len(records)
