"""RabbitMQAdapter — FastStream[rabbit] 기반 Streaming 어댑터"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from faststream.rabbit import RabbitBroker

from demiurge_testdata.adapters.base import BaseStreamAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import StreamAdapterConfig


@adapter_registry.register("rabbitmq")
class RabbitMQAdapter(BaseStreamAdapter):
    """FastStream 기반 RabbitMQ 어댑터.

    FastStream[rabbit]를 사용하여 AMQP 브로커에 연결한다.
    """

    def __init__(self, config: StreamAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = StreamAdapterConfig(port=5672, **kwargs)
        self._config = config
        url = self._build_url()
        self._broker = RabbitBroker(url)
        self._connected = False

    def _build_url(self) -> str:
        c = self._config
        if c.username and c.password:
            return f"amqp://{c.username}:{c.password}@{c.host}:{c.port}/"
        return f"amqp://{c.host}:{c.port}/"

    async def connect(self) -> None:
        await self._broker.start()
        self._connected = True

    async def disconnect(self) -> None:
        if self._connected:
            await self._broker.close()
            self._connected = False

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        queue = metadata.get("topic", self._config.topic)
        await self._broker.publish(data, queue=queue)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        return
        yield  # pragma: no cover

    async def health_check(self) -> bool:
        return self._connected

    async def publish(self, topic: str, message: bytes) -> None:
        await self._broker.publish(message, queue=topic)

    async def publish_batch(self, topic: str, messages: list[bytes]) -> None:
        for msg in messages:
            await self._broker.publish(msg, queue=topic)

    async def subscribe(self, topic: str, callback: Any) -> None:
        @self._broker.subscriber(topic)
        async def handler(msg: bytes) -> None:
            await callback(msg)
