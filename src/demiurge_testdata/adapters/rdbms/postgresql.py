"""PostgreSQLAdapter — asyncpg + SQLAlchemy 기반 RDBMS 어댑터"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from demiurge_testdata.adapters.base import BaseRDBMSAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import RDBMSAdapterConfig


@adapter_registry.register("postgresql")
class PostgreSQLAdapter(BaseRDBMSAdapter):
    """PostgreSQL 어댑터.

    asyncpg를 드라이버로, SQLAlchemy async를 ORM/커넥션 풀로 사용한다.
    push()는 metadata의 table 키에 지정된 테이블에 bytes를 저장한다.
    """

    def __init__(self, config: RDBMSAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = RDBMSAdapterConfig(**kwargs)
        self._config = config
        self._engine = None
        self._pool: asyncpg.Pool | None = None

    @property
    def dsn(self) -> str:
        c = self._config
        return f"postgresql+asyncpg://{c.user}:{c.password}@{c.host}:{c.port}/{c.database}"

    @property
    def raw_dsn(self) -> str:
        c = self._config
        return f"postgresql://{c.user}:{c.password}@{c.host}:{c.port}/{c.database}"

    async def connect(self) -> None:
        self._engine = create_async_engine(
            self.dsn,
            pool_size=self._config.pool_size,
            max_overflow=self._config.pool_overflow,
        )
        self._pool = await asyncpg.create_pool(
            self.raw_dsn,
            min_size=1,
            max_size=self._config.pool_size,
        )

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
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
                id SERIAL PRIMARY KEY,
                data BYTEA NOT NULL,
                format VARCHAR(32),
                compression VARCHAR(32),
                record_count INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """
        insert_sql = f"""
            INSERT INTO {table} (data, format, compression, record_count)
            VALUES ($1, $2, $3, $4)
        """
        async with self._pool.acquire() as conn:
            await conn.execute(create_sql)
            await conn.execute(insert_sql, data, fmt, compression, record_count)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        table = query.get("table", "raw_data")
        sql = f"SELECT data FROM {table}"
        if limit:
            sql += f" LIMIT {limit}"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql)
            for row in rows:
                yield row["data"]

    async def health_check(self) -> bool:
        if not self._pool:
            return False
        try:
            async with self._pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
                return result == 1
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
        col_defs = ", ".join(f"{col} {dtype}" for col, dtype in columns.items())
        sql = f"CREATE TABLE IF NOT EXISTS {name} ({col_defs})"
        async with self._pool.acquire() as conn:
            await conn.execute(sql)

    async def bulk_insert(self, table: str, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        columns = list(records[0].keys())
        col_names = ", ".join(columns)
        values = [tuple(r.get(c) for c in columns) for r in records]

        async with self._pool.acquire() as conn:
            # Ensure table exists with text columns as fallback
            col_defs = ", ".join(f"{c} TEXT" for c in columns)
            await conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table} (id SERIAL PRIMARY KEY, {col_defs})"
            )
            placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
            sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
            await conn.executemany(sql, values)
        return len(records)
