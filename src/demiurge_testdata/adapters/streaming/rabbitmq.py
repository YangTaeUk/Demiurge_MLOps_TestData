"""RabbitMQAdapter — aio_pika 기반 Streaming 어댑터"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import aio_pika

from demiurge_testdata.adapters.base import BaseStreamAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import StreamAdapterConfig


@adapter_registry.register("rabbitmq")
class RabbitMQAdapter(BaseStreamAdapter):
    """aio_pika 기반 RabbitMQ 어댑터.

    durable 큐를 사용하여 메시지가 소비 전까지 보존된다.
    FastStream의 큐 미선언·비영속 문제를 회피한다.
    """

    def __init__(self, config: StreamAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            kwargs.setdefault("port", 5672)
            config = StreamAdapterConfig(**kwargs)
        self._config = config
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None
        self._connected = False
        self._declared_queues: set[str] = set()

    def _build_url(self) -> str:
        c = self._config
        if c.username and c.password:
            return f"amqp://{c.username}:{c.password}@{c.host}:{c.port}/"
        return f"amqp://{c.host}:{c.port}/"

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(self._build_url())
        self._channel = await self._connection.channel()
        self._connected = True

    async def disconnect(self) -> None:
        if self._channel:
            await self._channel.close()
            self._channel = None
        if self._connection:
            await self._connection.close()
            self._connection = None
        self._connected = False
        self._declared_queues.clear()

    async def _ensure_queue(self, queue_name: str) -> None:
        """durable 큐를 선언한다 (이미 선언된 경우 스킵)."""
        if queue_name in self._declared_queues:
            return
        await self._channel.declare_queue(queue_name, durable=True)
        self._declared_queues.add(queue_name)

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        queue = metadata.get("topic", self._config.topic)
        await self.publish(queue, data)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        return
        yield  # pragma: no cover

    async def health_check(self) -> bool:
        return self._connected

    async def publish(self, topic: str, message: bytes) -> None:
        await self._ensure_queue(topic)
        await self._channel.default_exchange.publish(
            aio_pika.Message(
                body=message,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=topic,
        )

    async def publish_batch(self, topic: str, messages: list[bytes]) -> None:
        await self._ensure_queue(topic)
        for msg in messages:
            await self._channel.default_exchange.publish(
                aio_pika.Message(
                    body=msg,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=topic,
            )

    async def subscribe(self, topic: str, callback: Any) -> None:
        await self._ensure_queue(topic)
        queue = await self._channel.get_queue(topic)

        async def _on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            async with message.process():
                await callback(message.body)

        await queue.consume(_on_message)
