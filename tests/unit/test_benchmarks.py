"""벤치마크 테스트 — Generator + Handler 처리량 측정

Docker 없이 실행 가능. Adapter는 mock으로 대체하여
Generator → HandlerChain → push 파이프라인의 처리량만 측정.
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock

import pytest

from demiurge_testdata.core.config import GeneratorConfig

pytestmark = pytest.mark.benchmark


def _make_config(**overrides):
    defaults = {"type": "test", "shuffle": False, "stream_interval_ms": 1}
    defaults.update(overrides)
    return GeneratorConfig(**defaults)


def _make_records(n: int, template: dict | None = None) -> list[dict]:
    """N개 합성 레코드 생성"""
    if template is None:
        template = {
            "id": "1",
            "name": "Alice",
            "amount": "100.5",
            "category": "A",
            "active": "true",
        }
    return [
        {k: f"{v}_{i}" if isinstance(v, str) and k != "amount" else v for k, v in template.items()}
        for i in range(n)
    ]


class TestGeneratorBenchmark:
    """제너레이터별 배치 생성 성능 측정"""

    @pytest.fixture(autouse=True)
    def _import_generators(self):
        from demiurge_testdata.generators import (
            document,  # noqa: F401
            event,  # noqa: F401
            geospatial,  # noqa: F401
            iot,  # noqa: F401
            relational,  # noqa: F401
            text,  # noqa: F401
        )

    @pytest.mark.parametrize(
        "generator_key",
        [
            "home_credit",
            "instacart",
            "ieee_fraud",
            "hm",
            "twitter_sentiment",
            "nyc_taxi",
            "smart_mfg",
            "amazon_reviews",
        ],
    )
    async def test_generator_batch_throughput(self, generator_key):
        """각 제너레이터가 1000개 레코드를 5초 이내에 생성하는지 확인"""
        from demiurge_testdata.core.registry import generator_registry

        config = _make_config()
        records = _make_records(2000)
        generator = generator_registry.get_class(generator_key)(config, records=records)
        batch_size = 1000

        start = time.monotonic()
        result = await generator.batch(batch_size=batch_size)
        elapsed = time.monotonic() - start

        assert len(result) == batch_size
        assert elapsed < 5.0, f"{generator_key}: {batch_size} records took {elapsed:.2f}s"

    @pytest.mark.parametrize(
        "generator_key",
        [
            "home_credit",
            "instacart",
            "ieee_fraud",
            "nyc_taxi",
        ],
    )
    async def test_generator_10k_throughput(self, generator_key):
        """10K 레코드 배치 — 목표 throughput: 10K+ records/sec"""
        from demiurge_testdata.core.registry import generator_registry

        config = _make_config()
        records = _make_records(15_000)
        generator = generator_registry.get_class(generator_key)(config, records=records)
        batch_size = 10_000

        start = time.monotonic()
        result = await generator.batch(batch_size=batch_size)
        elapsed = time.monotonic() - start

        throughput = len(result) / elapsed if elapsed > 0 else float("inf")
        assert len(result) == batch_size
        # 10K records/sec target
        assert throughput > 10_000, (
            f"{generator_key}: throughput {throughput:.0f} rec/s < 10K target"
        )


class TestHandlerBenchmark:
    """핸들러 체인 인코딩 성능 측정"""

    @pytest.fixture
    def sample_records(self):
        return [
            {"id": i, "name": f"user_{i}", "amount": i * 1.5, "active": i % 2 == 0}
            for i in range(1000)
        ]

    @pytest.mark.parametrize("format_key", ["json", "csv", "jsonl", "yaml", "xml"])
    async def test_format_encode_throughput(self, format_key, sample_records):
        """1000개 레코드 포맷 인코딩 성능"""
        import demiurge_testdata.handlers.formats  # noqa: F401
        from demiurge_testdata.core.registry import format_registry

        handler = format_registry.create(format_key)

        start = time.monotonic()
        encoded = await handler.encode(sample_records)
        elapsed = time.monotonic() - start

        assert len(encoded) > 0
        assert elapsed < 2.0, f"{format_key}: encoding 1000 records took {elapsed:.2f}s"

    async def test_json_roundtrip(self, sample_records):
        """JSON encode → decode 왕복 성능"""
        import demiurge_testdata.handlers.formats.json_handler  # noqa: F401
        from demiurge_testdata.core.registry import format_registry

        handler = format_registry.create("json")

        start = time.monotonic()
        encoded = await handler.encode(sample_records)
        decoded = await handler.decode(encoded)
        elapsed = time.monotonic() - start

        assert len(decoded) == len(sample_records)
        assert elapsed < 2.0, f"JSON roundtrip took {elapsed:.2f}s"


class TestPipelineBenchmark:
    """파이프라인 E2E 처리량 (mock adapter)"""

    async def test_batch_pipeline_throughput(self):
        """Generator → Handler → MockAdapter 파이프라인 10K 레코드"""
        import demiurge_testdata.generators.relational.home_credit
        import demiurge_testdata.handlers.formats.json_handler  # noqa: F401
        from demiurge_testdata.core.pipeline import DataPipeline
        from demiurge_testdata.core.registry import format_registry, generator_registry
        from demiurge_testdata.handlers.chain import HandlerChain

        config = _make_config()
        records = _make_records(15_000)
        generator = generator_registry.get_class("home_credit")(config, records=records)
        format_handler = format_registry.create("json")
        handler_chain = HandlerChain(format_handler=format_handler)

        mock_adapter = AsyncMock()
        mock_adapter.push = AsyncMock()

        pipeline = DataPipeline(
            generator=generator,
            handler_chain=handler_chain,
            adapter=mock_adapter,
            batch_size=10_000,
        )

        metrics = await pipeline.run_batch()

        assert metrics.total_records == 10_000
        assert metrics.total_bytes > 0
        assert metrics.errors == []
        assert metrics.records_per_second > 0

        throughput = metrics.records_per_second
        # Log throughput for visibility
        print(
            f"\n  Pipeline throughput: {throughput:.0f} records/sec"
            f"\n  Generation: {metrics.generation_time_ms:.1f}ms"
            f"\n  Encoding: {metrics.encoding_time_ms:.1f}ms"
            f"\n  Push: {metrics.push_time_ms:.1f}ms"
        )
