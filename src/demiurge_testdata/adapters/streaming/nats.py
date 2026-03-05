"""NATSAdapter — FastStream[nats] 기반 Streaming 어댑터 (JetStream)"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from faststream.nats import NatsBroker

from demiurge_testdata.adapters.base import BaseStreamAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import NATSAdapterConfig


@adapter_registry.register("nats")
class NATSAdapter(BaseStreamAdapter):
    """NATS JetStream 어댑터.

    FastStream[nats]를 사용하여 NATS 서버에 연결한다.
    부모 Demiurge NATS 서비스와 호환된다.
    """

    def __init__(self, config: NATSAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = NATSAdapterConfig(**kwargs)
        self._config = config
        url = f"nats://{config.host}:{config.port}"
        self._broker = NatsBroker(url)
        self._connected = False

    async def connect(self) -> None:
        await self._broker.start()
        self._connected = True

    async def disconnect(self) -> None:
        if self._connected:
            await self._broker.close()
            self._connected = False

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        subject = metadata.get("topic", self._config.subject)
        await self._broker.publish(data, subject=subject)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        return
        yield  # pragma: no cover

    async def health_check(self) -> bool:
        return self._connected

    async def publish(self, topic: str, message: bytes) -> None:
        await self._broker.publish(message, subject=topic)

    async def publish_batch(self, topic: str, messages: list[bytes]) -> None:
        for msg in messages:
            await self._broker.publish(msg, subject=topic)

    async def subscribe(self, topic: str, callback: Any) -> None:
        @self._broker.subscriber(topic)
        async def handler(msg: bytes) -> None:
            await callback(msg)
