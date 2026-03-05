"""E2E 파이프라인 통합 테스트 — Generator → HandlerChain → Adapter

Docker 서비스가 필요한 엔드투엔드 테스트.
`docker compose up -d` 이후 `pytest -m integration` 으로 실행.
"""

from __future__ import annotations

import contextlib

import pytest

from demiurge_testdata.core.config import GeneratorConfig

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def _cfg():
    return GeneratorConfig(type="e2e_test", shuffle=False, stream_interval_ms=1)


def _home_credit_records(n: int) -> list[dict]:
    return [
        {
            "SK_ID_CURR": str(100_000 + i),
            "AMT_INCOME_TOTAL": str(50_000.0 + i * 100),
            "AMT_CREDIT": str(200_000.0 + i * 500),
            "TARGET": str(i % 2),
            "CNT_CHILDREN": str(i % 4),
            "NAME_CONTRACT_TYPE": "Cash loans",
        }
        for i in range(n)
    ]


def _instacart_records(n: int) -> list[dict]:
    return [
        {
            "order_id": str(1000 + i),
            "user_id": str(100 + i % 50),
            "product_id": str(200 + i % 100),
            "add_to_cart_order": str((i % 10) + 1),
            "reordered": str(i % 2),
            "order_number": str((i % 20) + 1),
            "order_dow": str(i % 7),
            "order_hour_of_day": str(i % 24),
            "aisle_id": str((i % 15) + 1),
            "department_id": str((i % 10) + 1),
            "days_since_prior_order": str(float(i % 30)),
        }
        for i in range(n)
    ]


def _ieee_fraud_records(n: int) -> list[dict]:
    return [
        {
            "TransactionID": str(3000000 + i),
            "TransactionDT": str(86400 + i * 60),
            "isFraud": str(i % 2),
            "card1": str(1000 + i % 500),
            "TransactionAmt": str(round(10.0 + i * 2.5, 2)),
        }
        for i in range(n)
    ]


def _hm_records(n: int) -> list[dict]:
    return [
        {
            "article_id": str(100000 + i),
            "customer_id": str(200000 + i % 500),
            "price": str(round(5.0 + i * 0.5, 2)),
            "sales_channel_id": str((i % 2) + 1),
            "product_type_name": "Trousers",
        }
        for i in range(n)
    ]


# ── E2E-A1: Home Credit → PostgreSQL ──


class TestE2EHomeCreditToPostgres:
    """Home Credit 제너레이터 → JSON 포맷 → PostgreSQL 적재"""

    @pytest.fixture
    async def pipeline_components(self):
        asyncpg = pytest.importorskip("asyncpg")  # noqa: F841

        import demiurge_testdata.generators.relational.home_credit
        import demiurge_testdata.handlers.formats.json_handler  # noqa: F401
        from demiurge_testdata.adapters.rdbms.postgresql import PostgreSQLAdapter
        from demiurge_testdata.core.registry import format_registry, generator_registry
        from demiurge_testdata.handlers.chain import HandlerChain

        gen_cls = generator_registry.get_class("home_credit")
        generator = gen_cls(_cfg(), records=_home_credit_records(200))
        format_handler = format_registry.create("json")
        handler_chain = HandlerChain(format_handler=format_handler)

        from demiurge_testdata.schemas.config import RDBMSAdapterConfig

        config = RDBMSAdapterConfig(
            host="localhost",
            port=5434,
            user="testdata",
            password="testdata_dev",
            database="testdata",
        )
        adapter = PostgreSQLAdapter(config=config)

        await adapter.connect()
        yield generator, handler_chain, adapter
        try:
            async with adapter._pool.acquire() as conn:
                await conn.execute("DROP TABLE IF EXISTS e2e_home_credit")
        except Exception:
            pass
        await adapter.disconnect()

    async def test_batch_pipeline(self, pipeline_components):
        from demiurge_testdata.core.pipeline import DataPipeline

        generator, handler_chain, adapter = pipeline_components
        pipeline = DataPipeline(
            generator=generator,
            handler_chain=handler_chain,
            adapter=adapter,
            batch_size=100,
        )

        metrics = await pipeline.run_batch()
        assert metrics.total_records == 100
        assert metrics.total_bytes > 0
        assert metrics.elapsed_seconds > 0
        assert metrics.errors == []


# ── E2E-B1: Instacart → MongoDB ──


class TestE2EInstacartToMongoDB:
    """Instacart 제너레이터 → JSON 포맷 → MongoDB 적재"""

    @pytest.fixture
    async def pipeline_components(self):
        motor = pytest.importorskip("motor")  # noqa: F841

        import demiurge_testdata.generators.document.instacart
        import demiurge_testdata.handlers.formats.json_handler  # noqa: F401
        from demiurge_testdata.adapters.nosql.mongodb import MongoDBAdapter
        from demiurge_testdata.core.registry import format_registry, generator_registry
        from demiurge_testdata.handlers.chain import HandlerChain

        gen_cls = generator_registry.get_class("instacart")
        generator = gen_cls(_cfg(), records=_instacart_records(100))
        format_handler = format_registry.create("json")
        handler_chain = HandlerChain(format_handler=format_handler)

        from demiurge_testdata.schemas.config import NoSQLAdapterConfig

        config = NoSQLAdapterConfig(
            host="localhost",
            port=27017,
            username="testdata",
            password="testdata_dev",
            database="testdata_e2e",
        )
        adapter = MongoDBAdapter(config=config)

        await adapter.connect()
        yield generator, handler_chain, adapter
        with contextlib.suppress(Exception):
            await adapter._db.drop_collection("e2e_instacart")
        await adapter.disconnect()

    async def test_batch_pipeline(self, pipeline_components):
        from demiurge_testdata.core.pipeline import DataPipeline

        generator, handler_chain, adapter = pipeline_components
        pipeline = DataPipeline(
            generator=generator,
            handler_chain=handler_chain,
            adapter=adapter,
            batch_size=50,
        )

        metrics = await pipeline.run_batch()
        assert metrics.total_records == 50
        assert metrics.total_bytes > 0
        assert metrics.errors == []


# ── E2E-C2: IEEE Fraud → Kafka ──


class TestE2EIEEEFraudToKafka:
    """IEEE Fraud 제너레이터 → JSON 포맷 → Kafka 적재"""

    @pytest.fixture
    async def pipeline_components(self):
        faststream = pytest.importorskip("faststream")  # noqa: F841

        import demiurge_testdata.generators.event.ieee_fraud
        import demiurge_testdata.handlers.formats.json_handler  # noqa: F401
        from demiurge_testdata.adapters.streaming.kafka import KafkaAdapter
        from demiurge_testdata.core.registry import format_registry, generator_registry
        from demiurge_testdata.handlers.chain import HandlerChain

        gen_cls = generator_registry.get_class("ieee_fraud")
        generator = gen_cls(_cfg(), records=_ieee_fraud_records(100))
        format_handler = format_registry.create("json")
        handler_chain = HandlerChain(format_handler=format_handler)

        from demiurge_testdata.schemas.config import StreamAdapterConfig

        config = StreamAdapterConfig(
            host="localhost",
            port=9092,
            topic="e2e-ieee-fraud",
        )
        adapter = KafkaAdapter(config=config)

        await adapter.connect()
        yield generator, handler_chain, adapter
        await adapter.disconnect()

    async def test_batch_pipeline(self, pipeline_components):
        from demiurge_testdata.core.pipeline import DataPipeline

        generator, handler_chain, adapter = pipeline_components
        pipeline = DataPipeline(
            generator=generator,
            handler_chain=handler_chain,
            adapter=adapter,
            batch_size=50,
        )

        metrics = await pipeline.run_batch()
        assert metrics.total_records == 50
        assert metrics.total_bytes > 0
        assert metrics.errors == []


# ── E2E-A3: H&M → MinIO (S3) ──


class TestE2EHMToMinIO:
    """H&M 제너레이터 → JSON 포맷 → MinIO (S3) 적재"""

    @pytest.fixture
    async def pipeline_components(self):
        s3fs = pytest.importorskip("s3fs")  # noqa: F841

        import demiurge_testdata.generators.relational.hm
        import demiurge_testdata.handlers.formats.json_handler  # noqa: F401
        from demiurge_testdata.adapters.storage.s3 import S3Adapter
        from demiurge_testdata.core.registry import format_registry, generator_registry
        from demiurge_testdata.handlers.chain import HandlerChain

        gen_cls = generator_registry.get_class("hm")
        generator = gen_cls(_cfg(), records=_hm_records(100))
        format_handler = format_registry.create("json")
        handler_chain = HandlerChain(format_handler=format_handler)

        from demiurge_testdata.schemas.config import StorageAdapterConfig

        config = StorageAdapterConfig(
            endpoint="http://localhost:9000",
            bucket="testdata",
            access_key="testdata",
            secret_key="testdata_dev_password",
        )
        adapter = S3Adapter(config=config)

        await adapter.connect()
        yield generator, handler_chain, adapter
        with contextlib.suppress(Exception):
            await adapter.delete("e2e/hm_data.json")
        await adapter.disconnect()

    async def test_batch_pipeline(self, pipeline_components):
        from demiurge_testdata.core.pipeline import DataPipeline

        generator, handler_chain, adapter = pipeline_components
        pipeline = DataPipeline(
            generator=generator,
            handler_chain=handler_chain,
            adapter=adapter,
            batch_size=50,
        )

        metrics = await pipeline.run_batch()
        assert metrics.total_records == 50
        assert metrics.total_bytes > 0
        assert metrics.errors == []
