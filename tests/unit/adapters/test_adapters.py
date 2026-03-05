"""Adapter 단위 테스트 — mock 기반 구조 검증 (Docker 불필요)"""

from __future__ import annotations

import pytest

# ── PostgreSQLAdapter ──


class TestPostgreSQLAdapter:
    @pytest.fixture
    def adapter(self):
        asyncpg = pytest.importorskip("asyncpg")  # noqa: F841
        from demiurge_testdata.adapters.rdbms.postgresql import PostgreSQLAdapter
        from demiurge_testdata.schemas.config import RDBMSAdapterConfig

        config = RDBMSAdapterConfig(
            host="localhost",
            port=5434,
            user="testdata",
            password="testdata_dev",
            database="testdata",
        )
        return PostgreSQLAdapter(config=config)

    def test_dsn(self, adapter):
        assert "asyncpg" in adapter.dsn
        assert "5434" in adapter.dsn
        assert "testdata" in adapter.dsn

    def test_raw_dsn(self, adapter):
        assert adapter.raw_dsn.startswith("postgresql://")

    def test_default_config(self):
        pytest.importorskip("asyncpg")
        from demiurge_testdata.adapters.rdbms.postgresql import PostgreSQLAdapter

        adapter = PostgreSQLAdapter(host="db", port=5432, user="u", password="p", database="d")
        assert adapter._config.host == "db"
        assert adapter._config.port == 5432

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry_registration(self):
        pytest.importorskip("asyncpg")
        from demiurge_testdata.core.registry import adapter_registry

        assert "postgresql" in adapter_registry


# ── MongoDBAdapter ──


class TestMongoDBAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("motor")
        from demiurge_testdata.adapters.nosql.mongodb import MongoDBAdapter
        from demiurge_testdata.schemas.config import NoSQLAdapterConfig

        config = NoSQLAdapterConfig(
            host="localhost",
            port=27017,
            username="testdata",
            password="testdata_dev",
            database="testdata",
        )
        return MongoDBAdapter(config=config)

    def test_connection_uri_with_auth(self, adapter):
        assert "testdata:testdata_dev" in adapter.connection_uri

    def test_connection_uri_no_auth(self):
        pytest.importorskip("motor")
        from demiurge_testdata.adapters.nosql.mongodb import MongoDBAdapter
        from demiurge_testdata.schemas.config import NoSQLAdapterConfig

        config = NoSQLAdapterConfig(database="test")
        adapter = MongoDBAdapter(config=config)
        assert "@" not in adapter.connection_uri

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry_registration(self):
        pytest.importorskip("motor")
        from demiurge_testdata.core.registry import adapter_registry

        assert "mongodb" in adapter_registry


# ── KafkaAdapter ──


class TestKafkaAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("faststream")
        from demiurge_testdata.adapters.streaming.kafka import KafkaAdapter
        from demiurge_testdata.schemas.config import StreamAdapterConfig

        config = StreamAdapterConfig(
            host="localhost",
            port=9092,
            topic="test-topic",
        )
        return KafkaAdapter(config=config)

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_default_not_connected(self, adapter):
        assert adapter._connected is False

    def test_registry_registration(self):
        pytest.importorskip("faststream")
        from demiurge_testdata.core.registry import adapter_registry

        assert "kafka" in adapter_registry


# ── S3Adapter ──


class TestS3Adapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("s3fs")
        from demiurge_testdata.adapters.storage.s3 import S3Adapter
        from demiurge_testdata.schemas.config import StorageAdapterConfig

        config = StorageAdapterConfig(
            endpoint="http://localhost:9000",
            bucket="testdata",
            access_key="testdata",
            secret_key="testdata_dev_password",
        )
        return S3Adapter(config=config)

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_default_config(self):
        pytest.importorskip("s3fs")
        from demiurge_testdata.adapters.storage.s3 import S3Adapter

        adapter = S3Adapter(bucket="mybucket", endpoint="http://minio:9000")
        assert adapter._config.bucket == "mybucket"

    def test_registry_registration(self):
        pytest.importorskip("s3fs")
        from demiurge_testdata.core.registry import adapter_registry

        assert "s3" in adapter_registry


# ── Registry completeness ──


class TestAdapterRegistry:
    def test_all_phase3_adapters_registered(self):
        pytest.importorskip("asyncpg")
        pytest.importorskip("motor")
        pytest.importorskip("faststream")
        pytest.importorskip("s3fs")

        # Force imports to trigger registration
        import demiurge_testdata.adapters.nosql.mongodb
        import demiurge_testdata.adapters.rdbms.postgresql
        import demiurge_testdata.adapters.storage.s3
        import demiurge_testdata.adapters.streaming.kafka  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        expected = ["postgresql", "mongodb", "kafka", "s3"]
        for key in expected:
            assert key in adapter_registry, f"'{key}' not in adapter_registry"
