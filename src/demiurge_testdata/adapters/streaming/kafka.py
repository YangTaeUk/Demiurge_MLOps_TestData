"""KafkaAdapter — FastStream 기반 Streaming 어댑터"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from faststream.kafka import KafkaBroker

from demiurge_testdata.adapters.base import BaseStreamAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import StreamAdapterConfig


@adapter_registry.register("kafka")
class KafkaAdapter(BaseStreamAdapter):
    """FastStream 기반 Kafka 어댑터.

    FastStream[kafka]를 사용하여 Kafka 브로커에 연결한다.
    push()는 metadata의 topic 키에 지정된 토픽에 bytes를 발행한다.
    """

    def __init__(self, config: StreamAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = StreamAdapterConfig(**kwargs)
        self._config = config
        self._broker = KafkaBroker(f"{config.host}:{config.port}")
        self._connected = False

    async def connect(self) -> None:
        await self._broker.start()
        self._connected = True

    async def disconnect(self) -> None:
        if self._connected:
            await self._broker.stop()
            self._connected = False

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        topic = metadata.get("topic", self._config.topic)
        await self._broker.publish(data, topic=topic)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        # Kafka is primarily a producer in this context.
        # Consumer-based fetch requires a subscriber pattern.
        # Yield nothing for basic interface compliance.
        return
        yield  # pragma: no cover — makes this an async generator

    async def health_check(self) -> bool:
        return self._connected

    async def publish(self, topic: str, message: bytes) -> None:
        await self._broker.publish(message, topic=topic)

    async def publish_batch(self, topic: str, messages: list[bytes]) -> None:
        for msg in messages:
            await self._broker.publish(msg, topic=topic)

    async def subscribe(self, topic: str, callback: Any) -> None:
        @self._broker.subscriber(topic)
        async def handler(msg: bytes) -> None:
            await callback(msg)
