"""Pipeline 단위 테스트 — mock 기반 오케스트레이션 검증"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock

import pytest

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.exceptions import PipelineError
from demiurge_testdata.core.pipeline import DataPipeline, PipelineMetrics
from demiurge_testdata.generators.base import BaseGenerator
from demiurge_testdata.handlers.base import BaseFormatHandler
from demiurge_testdata.handlers.chain import HandlerChain

# ── Stub implementations ──


class StubFormatHandler(BaseFormatHandler):
    @property
    def format_name(self) -> str:
        return "stub"

    @property
    def content_type(self) -> str:
        return "application/stub"

    @property
    def file_extension(self) -> str:
        return ".stub"

    async def encode(self, records: list[dict]) -> bytes:
        return b"encoded:" + str(len(records)).encode()

    async def decode(self, data: bytes) -> list[dict]:
        return [{"decoded": True}]


class StubGenerator(BaseGenerator):
    def __init__(self, records: list[dict] | None = None):
        self._records = records or [{"id": i} for i in range(10)]

    @property
    def dataset_name(self) -> str:
        return "stub_dataset"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.RELATIONAL

    async def batch(self, batch_size: int = 1000) -> list[dict]:
        return self._records[:batch_size]

    async def stream(self) -> AsyncIterator[dict]:
        for record in self._records:
            yield record


class StubAdapter:
    def __init__(self):
        self.pushed: list[tuple[bytes, dict]] = []
        self.connected = False

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        self.pushed.append((data, metadata))

    async def fetch(self, query: dict, limit=None) -> AsyncIterator[bytes]:
        yield b""

    async def health_check(self) -> bool:
        return self.connected


# ── Tests ──


class TestPipelineMetrics:
    def test_defaults(self):
        m = PipelineMetrics()
        assert m.total_records == 0
        assert m.records_per_second == 0.0

    def test_records_per_second(self):
        m = PipelineMetrics(total_records=100, elapsed_seconds=2.0)
        assert m.records_per_second == 50.0


class TestDataPipelineBatch:
    @pytest.fixture
    def pipeline(self):
        generator = StubGenerator()
        chain = HandlerChain(StubFormatHandler())
        adapter = StubAdapter()
        return DataPipeline(generator, chain, adapter, batch_size=5), adapter

    async def test_batch_basic(self, pipeline):
        pipe, adapter = pipeline
        metrics = await pipe.run_batch()

        assert metrics.total_records == 5
        assert metrics.error_count == 0
        assert metrics.total_bytes > 0
        assert metrics.elapsed_seconds > 0
        assert metrics.generation_time_ms > 0
        assert metrics.encoding_time_ms >= 0
        assert metrics.push_time_ms >= 0
        assert len(adapter.pushed) == 1

    async def test_batch_metadata(self, pipeline):
        pipe, adapter = pipeline
        await pipe.run_batch()

        _, metadata = adapter.pushed[0]
        assert metadata["format"] == "stub"
        assert metadata["compression"] == "none"
        assert metadata["record_count"] == 5

    async def test_batch_error_handling(self):
        generator = StubGenerator()
        chain = HandlerChain(StubFormatHandler())
        adapter = StubAdapter()
        adapter.push = AsyncMock(side_effect=RuntimeError("push failed"))

        pipe = DataPipeline(generator, chain, adapter, batch_size=3)

        with pytest.raises(PipelineError, match="push failed"):
            await pipe.run_batch()


class TestDataPipelineStream:
    async def test_stream_basic(self):
        records = [{"id": i} for i in range(3)]
        generator = StubGenerator(records)
        chain = HandlerChain(StubFormatHandler())
        adapter = StubAdapter()

        pipe = DataPipeline(generator, chain, adapter)
        metrics = await pipe.run_stream()

        assert metrics.total_records == 3
        assert metrics.error_count == 0
        assert len(adapter.pushed) == 3

    async def test_stream_error_handling(self):
        generator = StubGenerator([{"id": 1}])
        chain = HandlerChain(StubFormatHandler())
        adapter = StubAdapter()
        adapter.push = AsyncMock(side_effect=RuntimeError("stream push failed"))

        pipe = DataPipeline(generator, chain, adapter)

        with pytest.raises(PipelineError, match="stream push failed"):
            await pipe.run_stream()
