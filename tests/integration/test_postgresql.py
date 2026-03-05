"""PostgreSQL 통합 테스트 — Docker 필요"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

asyncpg = pytest.importorskip("asyncpg")

from demiurge_testdata.adapters.rdbms.postgresql import PostgreSQLAdapter
from demiurge_testdata.schemas.config import RDBMSAdapterConfig


@pytest.fixture
async def pg_adapter():
    config = RDBMSAdapterConfig(
        host="localhost",
        port=5434,
        user="testdata",
        password="testdata_dev",
        database="testdata",
    )
    adapter = PostgreSQLAdapter(config=config)
    await adapter.connect()
    yield adapter
    # Cleanup
    try:
        async with adapter._pool.acquire() as conn:
            await conn.execute("DROP TABLE IF EXISTS test_push")
            await conn.execute("DROP TABLE IF EXISTS test_bulk")
    except Exception:
        pass
    await adapter.disconnect()


class TestPostgreSQLIntegration:
    async def test_connect_and_health(self, pg_adapter):
        assert await pg_adapter.health_check() is True

    async def test_push_and_fetch(self, pg_adapter):
        data = b"hello world"
        metadata = {
            "table": "test_push",
            "format": "json",
            "compression": "none",
            "record_count": 1,
        }
        await pg_adapter.push(data, metadata)

        results = []
        async for chunk in pg_adapter.fetch({"table": "test_push"}):
            results.append(chunk)

        assert len(results) == 1
        assert results[0] == data

    async def test_execute_sql(self, pg_adapter):
        result = await pg_adapter.execute_sql("SELECT 1 AS val")
        assert result[0]["val"] == 1

    async def test_create_table(self, pg_adapter):
        await pg_adapter.create_table("test_bulk", {"name": "VARCHAR(100)", "age": "INTEGER"})
        result = await pg_adapter.execute_sql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'test_bulk' ORDER BY ordinal_position"
        )
        col_names = [r["column_name"] for r in result]
        assert "name" in col_names
        assert "age" in col_names

    async def test_bulk_insert(self, pg_adapter):
        await pg_adapter.create_table("test_bulk", {"name": "VARCHAR(100)", "age": "INTEGER"})
        records = [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]
        count = await pg_adapter.bulk_insert("test_bulk", records)
        assert count == 2

    async def test_disconnect_and_reconnect(self, pg_adapter):
        assert await pg_adapter.health_check() is True
        await pg_adapter.disconnect()
        assert await pg_adapter.health_check() is False
