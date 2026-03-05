"""MongoDB 통합 테스트 — Docker 필요"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

motor = pytest.importorskip("motor")

from demiurge_testdata.adapters.nosql.mongodb import MongoDBAdapter
from demiurge_testdata.schemas.config import NoSQLAdapterConfig


@pytest.fixture
async def mongo_adapter():
    config = NoSQLAdapterConfig(
        host="localhost",
        port=27017,
        username="testdata",
        password="testdata_dev",
        database="testdata_test",
    )
    adapter = MongoDBAdapter(config=config)
    await adapter.connect()
    yield adapter
    # Cleanup
    try:
        await adapter._db.drop_collection("test_push")
        await adapter._db.drop_collection("test_docs")
    except Exception:
        pass
    await adapter.disconnect()


class TestMongoDBIntegration:
    async def test_connect_and_health(self, mongo_adapter):
        assert await mongo_adapter.health_check() is True

    async def test_push_and_fetch(self, mongo_adapter):
        data = b"binary payload"
        metadata = {
            "collection": "test_push",
            "format": "msgpack",
            "compression": "lz4",
            "record_count": 5,
        }
        await mongo_adapter.push(data, metadata)

        results = []
        async for chunk in mongo_adapter.fetch({"collection": "test_push"}):
            results.append(chunk)

        assert len(results) == 1
        assert results[0] == data

    async def test_insert_and_query_documents(self, mongo_adapter):
        docs = [
            {"name": "alice", "score": 95},
            {"name": "bob", "score": 87},
            {"name": "charlie", "score": 92},
        ]
        count = await mongo_adapter.insert_documents("test_docs", docs)
        assert count == 3

        results = await mongo_adapter.query_documents("test_docs", {"score": {"$gt": 90}})
        assert len(results) == 2

    async def test_query_with_limit(self, mongo_adapter):
        docs = [{"i": i} for i in range(10)]
        await mongo_adapter.insert_documents("test_docs", docs)
        results = await mongo_adapter.query_documents("test_docs", limit=3)
        assert len(results) == 3

    async def test_disconnect_and_reconnect(self, mongo_adapter):
        assert await mongo_adapter.health_check() is True
        await mongo_adapter.disconnect()
        assert await mongo_adapter.health_check() is False
