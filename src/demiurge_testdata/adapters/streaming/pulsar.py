"""PulsarAdapter — pulsar-client 기반 Streaming 어댑터"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pulsar

from demiurge_testdata.adapters.base import BaseStreamAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import StreamAdapterConfig


@adapter_registry.register("pulsar")
class PulsarAdapter(BaseStreamAdapter):
    """Apache Pulsar 어댑터.

    pulsar-client를 사용한다. 동기 드라이버이므로
    asyncio.to_thread()로 비동기 래핑한다.
    """

    def __init__(self, config: StreamAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = StreamAdapterConfig(port=6650, **kwargs)
        self._config = config
        self._client: pulsar.Client | None = None
        self._producer: pulsar.Producer | None = None
        self._connected = False

    @property
    def service_url(self) -> str:
        return f"pulsar://{self._config.host}:{self._config.port}"

    async def connect(self) -> None:
        self._client = pulsar.Client(self.service_url)
        self._producer = await asyncio.to_thread(self._client.create_producer, self._config.topic)
        self._connected = True

    async def disconnect(self) -> None:
        if self._producer:
            await asyncio.to_thread(self._producer.close)
            self._producer = None
        if self._client:
            await asyncio.to_thread(self._client.close)
            self._client = None
        self._connected = False

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        topic = metadata.get("topic", self._config.topic)
        if topic != self._config.topic and self._client:
            producer = await asyncio.to_thread(self._client.create_producer, topic)
            await asyncio.to_thread(producer.send, data)
            await asyncio.to_thread(producer.close)
        elif self._producer:
            await asyncio.to_thread(self._producer.send, data)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        return
        yield  # pragma: no cover

    async def health_check(self) -> bool:
        return self._connected

    async def publish(self, topic: str, message: bytes) -> None:
        if self._client:
            producer = await asyncio.to_thread(self._client.create_producer, topic)
            await asyncio.to_thread(producer.send, message)
            await asyncio.to_thread(producer.close)

    async def publish_batch(self, topic: str, messages: list[bytes]) -> None:
        if self._client:
            producer = await asyncio.to_thread(self._client.create_producer, topic)
            for msg in messages:
                await asyncio.to_thread(producer.send, msg)
            await asyncio.to_thread(producer.close)

    async def subscribe(self, topic: str, callback: Any) -> None:
        if self._client:
            consumer = await asyncio.to_thread(
                self._client.subscribe,
                topic,
                subscription_name=self._config.group_id,
            )
            # Store consumer for later use
            self._consumer = consumer
