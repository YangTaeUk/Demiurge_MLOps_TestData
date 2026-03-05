"""Kafka 통합 테스트 — Docker 필요"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

faststream = pytest.importorskip("faststream")

from demiurge_testdata.adapters.streaming.kafka import KafkaAdapter
from demiurge_testdata.schemas.config import StreamAdapterConfig


@pytest.fixture
async def kafka_adapter():
    config = StreamAdapterConfig(
        host="localhost",
        port=9092,
        topic="test-topic",
    )
    adapter = KafkaAdapter(config=config)
    await adapter.connect()
    yield adapter
    await adapter.disconnect()


class TestKafkaIntegration:
    async def test_connect_and_health(self, kafka_adapter):
        assert await kafka_adapter.health_check() is True

    async def test_publish(self, kafka_adapter):
        await kafka_adapter.publish("test-topic", b"hello kafka")

    async def test_publish_batch(self, kafka_adapter):
        messages = [f"msg-{i}".encode() for i in range(5)]
        await kafka_adapter.publish_batch("test-topic", messages)

    async def test_push_via_metadata(self, kafka_adapter):
        data = b"push data"
        metadata = {"topic": "test-push-topic", "format": "json"}
        await kafka_adapter.push(data, metadata)

    async def test_disconnect(self, kafka_adapter):
        assert await kafka_adapter.health_check() is True
        await kafka_adapter.disconnect()
        assert await kafka_adapter.health_check() is False
