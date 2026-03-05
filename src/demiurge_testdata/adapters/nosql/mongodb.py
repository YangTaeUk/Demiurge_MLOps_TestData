"""MongoDBAdapter — motor 기반 NoSQL 어댑터"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient

from demiurge_testdata.adapters.base import BaseNoSQLAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import NoSQLAdapterConfig


@adapter_registry.register("mongodb")
class MongoDBAdapter(BaseNoSQLAdapter):
    """MongoDB 어댑터.

    motor를 사용하여 비동기 MongoDB 연결을 제공한다.
    push()는 metadata의 collection 키에 지정된 컬렉션에 bytes를 저장한다.
    """

    def __init__(self, config: NoSQLAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = NoSQLAdapterConfig(**kwargs)
        self._config = config
        self._client: AsyncIOMotorClient | None = None
        self._db = None

    @property
    def connection_uri(self) -> str:
        c = self._config
        if c.username and c.password:
            return f"mongodb://{c.username}:{c.password}@{c.host}:{c.port}"
        return f"mongodb://{c.host}:{c.port}"

    async def connect(self) -> None:
        self._client = AsyncIOMotorClient(self.connection_uri)
        self._db = self._client[self._config.database]

    async def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
            self._db = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        collection_name = metadata.get("collection", self._config.collection)
        collection = self._db[collection_name]

        doc = {
            "data": data,
            "format": metadata.get("format", "unknown"),
            "compression": metadata.get("compression", "none"),
            "record_count": metadata.get("record_count", 0),
        }
        await collection.insert_one(doc)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        collection_name = query.pop("collection", self._config.collection)
        collection = self._db[collection_name]

        cursor = collection.find(query)
        if limit:
            cursor = cursor.limit(limit)

        async for doc in cursor:
            if "data" in doc and isinstance(doc["data"], bytes):
                yield doc["data"]
            else:
                # Return document as JSON bytes (excluding _id)
                doc.pop("_id", None)
                yield json.dumps(doc, default=str).encode("utf-8")

    async def health_check(self) -> bool:
        if not self._client:
            return False
        try:
            result = await self._client.admin.command("ping")
            return result.get("ok") == 1.0
        except Exception:
            return False

    async def insert_documents(self, collection: str, documents: list[dict[str, Any]]) -> int:
        if not documents:
            return 0
        coll = self._db[collection]
        result = await coll.insert_many(documents)
        return len(result.inserted_ids)

    async def query_documents(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        coll = self._db[collection]
        cursor = coll.find(filter or {}, projection)
        if limit:
            cursor = cursor.limit(limit)

        results = []
        async for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)
        return results
