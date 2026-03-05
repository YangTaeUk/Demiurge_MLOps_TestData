"""RedisAdapter — redis[hiredis] 기반 NoSQL 어댑터"""

from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as aioredis

from demiurge_testdata.adapters.base import BaseNoSQLAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import NoSQLAdapterConfig


@adapter_registry.register("redis")
class RedisAdapter(BaseNoSQLAdapter):
    """Redis 어댑터.

    redis[hiredis]를 사용하여 비동기 연결을 제공한다.
    Hash/List 구조를 사용하여 문서를 저장한다.
    """

    def __init__(self, config: NoSQLAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = NoSQLAdapterConfig(port=6379, **kwargs)
        self._config = config
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        kwargs: dict[str, Any] = {
            "host": self._config.host,
            "port": self._config.port,
            "db": 0,
            "decode_responses": False,
        }
        if self._config.password:
            kwargs["password"] = self._config.password
        self._client = aioredis.Redis(**kwargs)

    async def disconnect(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        collection = metadata.get("collection", self._config.collection)
        key = f"{collection}:{int(time.time() * 1000)}"
        await self._client.set(key, data)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        collection = query.get("collection", self._config.collection)
        pattern = f"{collection}:*"
        count = 0
        async for key in self._client.scan_iter(match=pattern):
            if limit and count >= limit:
                break
            data = await self._client.get(key)
            if data:
                yield data
            count += 1

    async def health_check(self) -> bool:
        if not self._client:
            return False
        try:
            return await self._client.ping()
        except Exception:
            return False

    async def insert_documents(self, collection: str, documents: list[dict[str, Any]]) -> int:
        if not documents:
            return 0
        pipe = self._client.pipeline()
        for i, doc in enumerate(documents):
            key = f"{collection}:{int(time.time() * 1000)}:{i}"
            pipe.set(key, json.dumps(doc, default=str).encode("utf-8"))
        await pipe.execute()
        return len(documents)

    async def query_documents(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        pattern = f"{collection}:*"
        results: list[dict[str, Any]] = []
        async for key in self._client.scan_iter(match=pattern):
            if limit and len(results) >= limit:
                break
            data = await self._client.get(key)
            if data:
                doc = json.loads(data)
                if filter:
                    if all(doc.get(k) == v for k, v in filter.items()):
                        results.append(doc)
                else:
                    results.append(doc)
        return results
