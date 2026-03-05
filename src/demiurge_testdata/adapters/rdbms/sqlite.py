"""SQLiteAdapter — aiosqlite 기반 RDBMS 어댑터"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import aiosqlite

from demiurge_testdata.adapters.base import BaseRDBMSAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import RDBMSAdapterConfig


@adapter_registry.register("sqlite")
class SQLiteAdapter(BaseRDBMSAdapter):
    """SQLite 어댑터.

    aiosqlite를 사용하여 파일 기반 비동기 연결을 제공한다.
    Docker 없이 로컬 파일로 동작한다.
    """

    def __init__(self, config: RDBMSAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = RDBMSAdapterConfig(**kwargs)
        self._config = config
        self._db_path = config.database if config.database != "testdata" else ":memory:"
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self._db_path)
        self._conn.row_factory = aiosqlite.Row

    async def disconnect(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        table = metadata.get("table", "raw_data")
        fmt = metadata.get("format", "unknown")
        compression = metadata.get("compression", "none")
        record_count = metadata.get("record_count", 0)

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data BLOB NOT NULL,
                format TEXT,
                compression TEXT,
                record_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        insert_sql = (
            f"INSERT INTO {table} (data, format, compression, record_count) VALUES (?, ?, ?, ?)"
        )
        await self._conn.execute(create_sql)
        await self._conn.execute(insert_sql, (data, fmt, compression, record_count))
        await self._conn.commit()

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        table = query.get("table", "raw_data")
        sql = f"SELECT data FROM {table}"
        if limit:
            sql += f" LIMIT {limit}"

        async with self._conn.execute(sql) as cursor:
            async for row in cursor:
                yield row[0]

    async def health_check(self) -> bool:
        if not self._conn:
            return False
        try:
            async with self._conn.execute("SELECT 1") as cursor:
                row = await cursor.fetchone()
                return row[0] == 1
        except Exception:
            return False

    async def execute_sql(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        param_tuple = tuple(params.values()) if params else ()
        async with self._conn.execute(query, param_tuple) as cursor:
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = await cursor.fetchall()
                return [dict(zip(columns, row, strict=True)) for row in rows]
        await self._conn.commit()
        return []

    async def create_table(self, name: str, columns: dict[str, str]) -> None:
        col_defs = ", ".join(f"{col} {dtype}" for col, dtype in columns.items())
        sql = f"CREATE TABLE IF NOT EXISTS {name} ({col_defs})"
        await self._conn.execute(sql)
        await self._conn.commit()

    async def bulk_insert(self, table: str, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        columns = list(records[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))
        values = [tuple(r.get(c) for c in columns) for r in records]

        col_defs = ", ".join(f"{c} TEXT" for c in columns)
        await self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, {col_defs})"
        )
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
        await self._conn.executemany(sql, values)
        await self._conn.commit()
        return len(records)
