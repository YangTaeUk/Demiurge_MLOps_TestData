"""KafkaAdapter — aiokafka 기반 Streaming 어댑터"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from aiokafka import AIOKafkaProducer

from demiurge_testdata.adapters.base import BaseStreamAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import StreamAdapterConfig


@adapter_registry.register("kafka")
class KafkaAdapter(BaseStreamAdapter):
    """aiokafka 기반 Kafka 어댑터.

    publish-only 용도로 AIOKafkaProducer를 직접 사용한다.
    FastStream broker.start()의 subscriber 초기화 hang 문제를 회피한다.
    """

    def __init__(self, config: StreamAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = StreamAdapterConfig(**kwargs)
        self._config = config
        self._producer: AIOKafkaProducer | None = None
        self._connected = False

    async def connect(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=f"{self._config.host}:{self._config.port}",
            request_timeout_ms=10000,
            metadata_max_age_ms=5000,
        )
        await self._producer.start()
        self._connected = True

    async def disconnect(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None
            self._connected = False

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        topic = metadata.get("topic", self._config.topic)
        await self._producer.send_and_wait(topic, data)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        return
        yield  # pragma: no cover — makes this an async generator

    async def health_check(self) -> bool:
        return self._connected

    async def publish(self, topic: str, message: bytes) -> None:
        await self._producer.send_and_wait(topic, message)

    async def publish_batch(self, topic: str, messages: list[bytes]) -> None:
        for msg in messages:
            await self._producer.send_and_wait(topic, msg)

    async def subscribe(self, topic: str, callback: Any) -> None:
        raise NotImplementedError("KafkaAdapter는 publish-only입니다. Consumer는 별도 구현 필요.")
