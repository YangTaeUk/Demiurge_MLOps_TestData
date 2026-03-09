"""NATSAdapter — nats-py JetStream 기반 Streaming 어댑터"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

import nats
from nats.js.api import StreamConfig

from demiurge_testdata.adapters.base import BaseStreamAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import NATSAdapterConfig

logger = logging.getLogger(__name__)


@adapter_registry.register("nats")
class NATSAdapter(BaseStreamAdapter):
    """nats-py JetStream 기반 NATS 어댑터.

    JetStream을 사용하여 메시지를 영속 저장한다.
    Core NATS publish는 fire-and-forget이라 구독자가 없으면 메시지가 유실되므로,
    DL 어댑터 테스트처럼 적재 후 조회하는 패턴에는 JetStream이 필수이다.
    """

    def __init__(self, config: NATSAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = NATSAdapterConfig(**kwargs)
        self._config = config
        self._nc: nats.NATS | None = None
        self._js: nats.js.JetStreamContext | None = None
        self._connected = False
        self._ensured_streams: set[str] = set()

    async def connect(self) -> None:
        url = f"nats://{self._config.host}:{self._config.port}"
        self._nc = await nats.connect(url, connect_timeout=10)
        self._js = self._nc.jetstream()
        self._connected = True

    async def disconnect(self) -> None:
        if self._nc:
            await self._nc.drain()
            self._nc = None
            self._js = None
            self._connected = False
            self._ensured_streams.clear()

    async def _ensure_stream(self, subject: str) -> None:
        """subject를 포함하는 JetStream 스트림이 존재하도록 보장한다."""
        if subject in self._ensured_streams:
            return

        # subject → 스트림명 변환 (예: "test.adapter.sample" → "TEST_ADAPTER_SAMPLE")
        stream_name = subject.replace(".", "_").replace("/", "_").upper()

        try:
            # 기존 스트림 확인
            info = await self._js.find_stream_name_by_subject(subject)
            logger.debug("NATS stream '%s' already covers subject '%s'", info, subject)
        except nats.js.errors.NotFoundError:
            # 새 스트림 생성
            await self._js.add_stream(
                StreamConfig(name=stream_name, subjects=[subject]),
            )
            logger.debug("NATS stream '%s' created for subject '%s'", stream_name, subject)

        self._ensured_streams.add(subject)

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        subject = metadata.get("topic", self._config.subject)
        await self.publish(subject, data)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        return
        yield  # pragma: no cover

    async def health_check(self) -> bool:
        return self._connected and self._nc is not None and self._nc.is_connected

    async def publish(self, topic: str, message: bytes) -> None:
        await self._ensure_stream(topic)
        await self._js.publish(topic, message)

    async def publish_batch(self, topic: str, messages: list[bytes]) -> None:
        await self._ensure_stream(topic)
        for msg in messages:
            await self._js.publish(topic, msg)

    async def subscribe(self, topic: str, callback: Any) -> None:
        """JetStream push consumer로 구독한다."""
        await self._ensure_stream(topic)
        sub = await self._js.subscribe(topic)

        async def _handler() -> None:
            async for msg in sub.messages:
                await callback(msg.data)
                await msg.ack()

        import asyncio

        asyncio.create_task(_handler())
