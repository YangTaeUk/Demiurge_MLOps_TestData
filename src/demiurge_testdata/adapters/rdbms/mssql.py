"""MSSQLAdapter — aioodbc 기반 RDBMS 어댑터"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import aioodbc

from demiurge_testdata.adapters.base import BaseRDBMSAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import RDBMSAdapterConfig


@adapter_registry.register("mssql")
class MSSQLAdapter(BaseRDBMSAdapter):
    """Microsoft SQL Server 어댑터.

    aioodbc를 사용하여 ODBC 기반 비동기 연결을 제공한다.
    """

    def __init__(self, config: RDBMSAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = RDBMSAdapterConfig(port=1433, **kwargs)
        self._config = config
        self._pool: aioodbc.Pool | None = None

    @property
    def connection_string(self) -> str:
        c = self._config
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={c.host},{c.port};"
            f"DATABASE={c.database};"
            f"UID={c.user};PWD={c.password};"
            f"TrustServerCertificate=yes"
        )

    async def connect(self) -> None:
        self._pool = await aioodbc.create_pool(
            dsn=self.connection_string,
            minsize=1,
            maxsize=self._config.pool_size,
        )

    async def disconnect(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        table = metadata.get("table", "raw_data")
        fmt = metadata.get("format", "unknown")
        compression = metadata.get("compression", "none")
        record_count = metadata.get("record_count", 0)

        create_sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table}')
            CREATE TABLE [{table}] (
                id INT IDENTITY(1,1) PRIMARY KEY,
                data VARBINARY(MAX) NOT NULL,
                format VARCHAR(32),
                compression VARCHAR(32),
                record_count INT,
                created_at DATETIME2 DEFAULT GETDATE()
            )
        """
        insert_sql = (
            f"INSERT INTO [{table}] (data, format, compression, record_count) VALUES (?, ?, ?, ?)"
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(create_sql)
                await cur.execute(insert_sql, data, fmt, compression, record_count)
            await conn.commit()

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        table = query.get("table", "raw_data")
        sql = f"SELECT TOP {limit} data FROM [{table}]" if limit else f"SELECT data FROM [{table}]"

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
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(query)
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                rows = await cur.fetchall()
                return [dict(zip(columns, row, strict=True)) for row in rows]
            await conn.commit()
            return []

    async def create_table(self, name: str, columns: dict[str, str]) -> None:
        col_defs = ", ".join(f"[{col}] {dtype}" for col, dtype in columns.items())
        sql = (
            f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{name}') "
            f"CREATE TABLE [{name}] ({col_defs})"
        )
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql)
            await conn.commit()

    async def bulk_insert(self, table: str, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        columns = list(records[0].keys())
        col_names = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join(["?"] * len(columns))

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                col_defs = ", ".join(f"[{c}] NVARCHAR(MAX)" for c in columns)
                await cur.execute(
                    f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table}') "
                    f"CREATE TABLE [{table}] "
                    f"(id INT IDENTITY(1,1) PRIMARY KEY, {col_defs})"
                )
                sql = f"INSERT INTO [{table}] ({col_names}) VALUES ({placeholders})"
                for r in records:
                    vals = tuple(r.get(c) for c in columns)
                    await cur.execute(sql, *vals)
            await conn.commit()
        return len(records)
