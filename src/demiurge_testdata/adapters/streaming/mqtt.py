"""MQTTAdapter — aiomqtt 기반 Streaming 어댑터"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import aiomqtt

from demiurge_testdata.adapters.base import BaseStreamAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import StreamAdapterConfig


@adapter_registry.register("mqtt")
class MQTTAdapter(BaseStreamAdapter):
    """MQTT 어댑터.

    aiomqtt를 사용하여 MQTT 브로커에 연결한다.
    """

    def __init__(self, config: StreamAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            kwargs.setdefault("port", 1883)
            config = StreamAdapterConfig(**kwargs)
        self._config = config
        self._client: aiomqtt.Client | None = None
        self._connected = False

    async def connect(self) -> None:
        self._client = aiomqtt.Client(
            hostname=self._config.host,
            port=self._config.port,
            username=self._config.username,
            password=self._config.password,
        )
        await self._client.__aenter__()
        self._connected = True

    async def disconnect(self) -> None:
        if self._client and self._connected:
            await self._client.__aexit__(None, None, None)
            self._connected = False
            self._client = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        topic = metadata.get("topic", self._config.topic)
        await self._client.publish(topic, payload=data)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        return
        yield  # pragma: no cover

    async def health_check(self) -> bool:
        return self._connected

    async def publish(self, topic: str, message: bytes) -> None:
        await self._client.publish(topic, payload=message)

    async def publish_batch(self, topic: str, messages: list[bytes]) -> None:
        for msg in messages:
            await self._client.publish(topic, payload=msg)

    async def subscribe(self, topic: str, callback: Any) -> None:
        await self._client.subscribe(topic)
