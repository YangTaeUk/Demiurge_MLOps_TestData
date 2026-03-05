"""OracleAdapter — oracledb 기반 RDBMS 어댑터"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import oracledb

from demiurge_testdata.adapters.base import BaseRDBMSAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import RDBMSAdapterConfig


@adapter_registry.register("oracle")
class OracleAdapter(BaseRDBMSAdapter):
    """Oracle Database 어댑터.

    python-oracledb (thin mode)를 사용하여 비동기 연결을 제공한다.
    """

    def __init__(self, config: RDBMSAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = RDBMSAdapterConfig(port=1521, **kwargs)
        self._config = config
        self._pool: oracledb.AsyncConnectionPool | None = None

    @property
    def dsn(self) -> str:
        c = self._config
        return f"{c.host}:{c.port}/{c.database}"

    async def connect(self) -> None:
        self._pool = oracledb.create_pool_async(
            user=self._config.user,
            password=self._config.password,
            dsn=self.dsn,
            min=1,
            max=self._config.pool_size,
        )

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        table = metadata.get("table", "raw_data")
        fmt = metadata.get("format", "unknown")
        compression = metadata.get("compression", "none")
        record_count = metadata.get("record_count", 0)

        create_sql = (
            f"BEGIN EXECUTE IMMEDIATE "
            f"'CREATE TABLE {table} ("
            f"id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
            f"data BLOB NOT NULL, "
            f"format VARCHAR2(32), "
            f"compression VARCHAR2(32), "
            f"record_count NUMBER, "
            f"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            f")'; EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
        insert_sql = (
            f"INSERT INTO {table} (data, format, compression, record_count) VALUES (:1, :2, :3, :4)"
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(create_sql)
                await cur.execute(insert_sql, [data, fmt, compression, record_count])
            await conn.commit()

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        table = query.get("table", "raw_data")
        if limit:
            sql = f"SELECT data FROM {table} WHERE ROWNUM <= {limit}"
        else:
            sql = f"SELECT data FROM {table}"

        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(sql)
            rows = await cur.fetchall()
            for row in rows:
                data = row[0]
                if hasattr(data, "read"):
                    yield data.read()
                else:
                    yield data

    async def health_check(self) -> bool:
        if not self._pool:
            return False
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute("SELECT 1 FROM DUAL")
                result = await cur.fetchone()
                return result[0] == 1
        except Exception:
            return False

    async def execute_sql(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(query, params or {})
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                rows = await cur.fetchall()
                return [dict(zip(columns, row, strict=True)) for row in rows]
            await conn.commit()
            return []

    async def create_table(self, name: str, columns: dict[str, str]) -> None:
        col_defs = ", ".join(f"{col} {dtype}" for col, dtype in columns.items())
        sql = (
            f"BEGIN EXECUTE IMMEDIATE "
            f"'CREATE TABLE {name} ({col_defs})'; "
            f"EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
            await conn.commit()

    async def bulk_insert(self, table: str, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        columns = list(records[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join(f":{i + 1}" for i in range(len(columns)))
        values = [tuple(r.get(c) for c in columns) for r in records]

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                col_defs = ", ".join(f"{c} VARCHAR2(4000)" for c in columns)
                await cur.execute(
                    f"BEGIN EXECUTE IMMEDIATE "
                    f"'CREATE TABLE {table} ({col_defs})'; "
                    f"EXCEPTION WHEN OTHERS THEN NULL; END;"
                )
                sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
                await cur.executemany(sql, values)
            await conn.commit()
        return len(records)
