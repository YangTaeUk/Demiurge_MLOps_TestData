"""CassandraAdapter — cassandra-driver 기반 NoSQL 어댑터"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from typing import Any

from cassandra.cluster import Cluster, Session
from cassandra.query import SimpleStatement

from demiurge_testdata.adapters.base import BaseNoSQLAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import NoSQLAdapterConfig


@adapter_registry.register("cassandra")
class CassandraAdapter(BaseNoSQLAdapter):
    """Cassandra 어댑터.

    cassandra-driver를 사용한다. 드라이버가 동기 전용이므로
    asyncio.to_thread()로 비동기 래핑한다.
    """

    def __init__(self, config: NoSQLAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = NoSQLAdapterConfig(port=9042, **kwargs)
        self._config = config
        self._cluster: Cluster | None = None
        self._session: Session | None = None

    async def connect(self) -> None:
        auth = None
        if self._config.username and self._config.password:
            from cassandra.auth import PlainTextAuthProvider

            auth = PlainTextAuthProvider(
                username=self._config.username,
                password=self._config.password,
            )
        self._cluster = Cluster(
            [self._config.host],
            port=self._config.port,
            auth_provider=auth,
        )
        self._session = await asyncio.to_thread(self._cluster.connect)
        # Create keyspace if not exists
        ks = self._config.database
        await asyncio.to_thread(
            self._session.execute,
            f"CREATE KEYSPACE IF NOT EXISTS {ks} "
            f"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}",
        )
        await asyncio.to_thread(self._session.set_keyspace, ks)

    async def disconnect(self) -> None:
        if self._cluster:
            await asyncio.to_thread(self._cluster.shutdown)
            self._cluster = None
            self._session = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        table = metadata.get("collection", self._config.collection)
        fmt = metadata.get("format", "unknown")
        compression = metadata.get("compression", "none")

        create_cql = (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            f"id UUID PRIMARY KEY, data blob, format text, compression text)"
        )
        insert_cql = f"INSERT INTO {table} (id, data, format, compression) VALUES (%s, %s, %s, %s)"
        await asyncio.to_thread(self._session.execute, create_cql)
        await asyncio.to_thread(
            self._session.execute,
            insert_cql,
            (uuid.uuid4(), data, fmt, compression),
        )

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        table = query.get("collection", self._config.collection)
        cql = f"SELECT data FROM {table}"
        if limit:
            cql += f" LIMIT {limit}"

        stmt = SimpleStatement(cql)
        rows = await asyncio.to_thread(self._session.execute, stmt)
        for row in rows:
            yield row.data

    async def health_check(self) -> bool:
        if not self._session:
            return False
        try:
            result = await asyncio.to_thread(
                self._session.execute, "SELECT now() FROM system.local"
            )
            return len(list(result)) > 0
        except Exception:
            return False

    async def insert_documents(self, collection: str, documents: list[dict[str, Any]]) -> int:
        if not documents:
            return 0
        create_cql = f"CREATE TABLE IF NOT EXISTS {collection} (id UUID PRIMARY KEY, doc text)"
        await asyncio.to_thread(self._session.execute, create_cql)

        for doc in documents:
            cql = f"INSERT INTO {collection} (id, doc) VALUES (%s, %s)"
            await asyncio.to_thread(
                self._session.execute,
                cql,
                (uuid.uuid4(), json.dumps(doc, default=str)),
            )
        return len(documents)

    async def query_documents(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        cql = f"SELECT doc FROM {collection}"
        if limit:
            cql += f" LIMIT {limit}"

        rows = await asyncio.to_thread(self._session.execute, SimpleStatement(cql))
        results = []
        for row in rows:
            doc = json.loads(row.doc)
            if filter and not all(doc.get(k) == v for k, v in filter.items()):
                continue
            results.append(doc)
        return results
