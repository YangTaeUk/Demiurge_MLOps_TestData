"""ElasticsearchAdapter — elasticsearch[async] 기반 NoSQL 어댑터"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from elasticsearch import AsyncElasticsearch

from demiurge_testdata.adapters.base import BaseNoSQLAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import NoSQLAdapterConfig


@adapter_registry.register("elasticsearch")
class ElasticsearchAdapter(BaseNoSQLAdapter):
    """Elasticsearch 어댑터.

    elasticsearch[async]를 사용하여 비동기 연결을 제공한다.
    """

    def __init__(self, config: NoSQLAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            kwargs.setdefault("port", 9200)
            config = NoSQLAdapterConfig(**kwargs)
        self._config = config
        self._client: AsyncElasticsearch | None = None

    @property
    def hosts(self) -> list[str]:
        return [f"http://{self._config.host}:{self._config.port}"]

    async def connect(self) -> None:
        kwargs: dict[str, Any] = {"hosts": self.hosts}
        if self._config.username and self._config.password:
            kwargs["basic_auth"] = (self._config.username, self._config.password)
        self._client = AsyncElasticsearch(**kwargs)

    async def disconnect(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        index = metadata.get("collection", self._config.collection)
        doc = {
            "data": data.decode("utf-8", errors="replace"),
            "format": metadata.get("format", "unknown"),
            "compression": metadata.get("compression", "none"),
            "record_count": metadata.get("record_count", 0),
        }
        await self._client.index(index=index, document=doc)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        query = dict(query)  # caller의 dict 변경 방지
        index = query.pop("collection", self._config.collection)
        body: dict[str, Any] = {"query": query} if query else {"query": {"match_all": {}}}
        if limit:
            body["size"] = limit

        result = await self._client.search(index=index, body=body)
        for hit in result["hits"]["hits"]:
            yield json.dumps(hit["_source"], default=str).encode("utf-8")

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
        operations = []
        for doc in documents:
            operations.append({"index": {"_index": collection}})
            operations.append(doc)
        result = await self._client.bulk(operations=operations)
        if result.get("errors"):
            failed = sum(
                1 for item in result.get("items", [])
                if "error" in item.get("index", {})
            )
            return len(documents) - failed
        return len(documents)

    async def query_documents(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        body: dict[str, Any] = {}
        if filter:
            body["query"] = filter
        else:
            body["query"] = {"match_all": {}}
        if limit:
            body["size"] = limit
        if projection:
            body["_source"] = list(projection.keys())

        result = await self._client.search(index=collection, body=body)
        return [hit["_source"] for hit in result["hits"]["hits"]]
