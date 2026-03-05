"""Phase 5 어댑터 18종 단위 테스트 — mock 기반 구조 검증 (Docker 불필요)"""

from __future__ import annotations

import pytest

# ── RDBMS: MySQL ──


class TestMySQLAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("aiomysql")
        from demiurge_testdata.adapters.rdbms.mysql import MySQLAdapter
        from demiurge_testdata.schemas.config import RDBMSAdapterConfig

        config = RDBMSAdapterConfig(
            host="localhost",
            port=3306,
            user="testdata",
            password="testdata_dev",
            database="testdata",
        )
        return MySQLAdapter(config=config)

    def test_dsn(self, adapter):
        assert "aiomysql" in adapter.dsn
        assert "3306" in adapter.dsn

    def test_default_config(self):
        pytest.importorskip("aiomysql")
        from demiurge_testdata.adapters.rdbms.mysql import MySQLAdapter

        a = MySQLAdapter(host="db", port=3306, user="u", password="p", database="d")
        assert a._config.host == "db"

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("aiomysql")
        import demiurge_testdata.adapters.rdbms.mysql  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "mysql" in adapter_registry


# ── RDBMS: MariaDB ──


class TestMariaDBAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("aiomysql")
        from demiurge_testdata.adapters.rdbms.mariadb import MariaDBAdapter
        from demiurge_testdata.schemas.config import RDBMSAdapterConfig

        config = RDBMSAdapterConfig(
            host="localhost",
            port=3307,
            user="testdata",
            password="testdata_dev",
            database="testdata",
        )
        return MariaDBAdapter(config=config)

    def test_dsn(self, adapter):
        assert "aiomysql" in adapter.dsn
        assert "3307" in adapter.dsn

    def test_registry(self):
        pytest.importorskip("aiomysql")
        import demiurge_testdata.adapters.rdbms.mariadb  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "mariadb" in adapter_registry


# ── RDBMS: MSSQL ──


class TestMSSQLAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("aioodbc")
        from demiurge_testdata.adapters.rdbms.mssql import MSSQLAdapter
        from demiurge_testdata.schemas.config import RDBMSAdapterConfig

        config = RDBMSAdapterConfig(
            host="localhost",
            port=1433,
            user="sa",
            password="testdata_dev",
            database="testdata",
        )
        return MSSQLAdapter(config=config)

    def test_connection_string(self, adapter):
        cs = adapter.connection_string
        assert "1433" in cs
        assert "ODBC" in cs

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("aioodbc")
        import demiurge_testdata.adapters.rdbms.mssql  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "mssql" in adapter_registry


# ── RDBMS: Oracle ──


class TestOracleAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("oracledb")
        from demiurge_testdata.adapters.rdbms.oracle import OracleAdapter
        from demiurge_testdata.schemas.config import RDBMSAdapterConfig

        config = RDBMSAdapterConfig(
            host="localhost",
            port=1521,
            user="testdata",
            password="testdata_dev",
            database="XEPDB1",
        )
        return OracleAdapter(config=config)

    def test_dsn(self, adapter):
        assert "1521" in adapter.dsn
        assert "XEPDB1" in adapter.dsn

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("oracledb")
        import demiurge_testdata.adapters.rdbms.oracle  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "oracle" in adapter_registry


# ── RDBMS: SQLite ──


class TestSQLiteAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("aiosqlite")
        from demiurge_testdata.adapters.rdbms.sqlite import SQLiteAdapter
        from demiurge_testdata.schemas.config import RDBMSAdapterConfig

        config = RDBMSAdapterConfig(database=":memory:")
        return SQLiteAdapter(config=config)

    def test_db_path_memory(self, adapter):
        assert adapter._db_path == ":memory:"

    async def test_connect_and_health(self, adapter):
        await adapter.connect()
        assert await adapter.health_check() is True
        await adapter.disconnect()

    async def test_push_and_fetch(self, adapter):
        await adapter.connect()
        data = b"test-data-bytes"
        await adapter.push(data, {"table": "test_raw"})

        results = []
        async for chunk in adapter.fetch({"table": "test_raw"}):
            results.append(chunk)
        assert len(results) == 1
        assert results[0] == data
        await adapter.disconnect()

    async def test_bulk_insert(self, adapter):
        await adapter.connect()
        records = [{"name": "a", "value": "1"}, {"name": "b", "value": "2"}]
        count = await adapter.bulk_insert("test_bulk", records)
        assert count == 2
        await adapter.disconnect()

    async def test_execute_sql(self, adapter):
        await adapter.connect()
        await adapter.create_table("t1", {"id": "INTEGER", "name": "TEXT"})
        await adapter.execute_sql("INSERT INTO t1 (id, name) VALUES (?, ?)", {"id": 1, "name": "x"})
        rows = await adapter.execute_sql("SELECT * FROM t1")
        assert len(rows) == 1
        await adapter.disconnect()

    def test_registry(self):
        pytest.importorskip("aiosqlite")
        import demiurge_testdata.adapters.rdbms.sqlite  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "sqlite" in adapter_registry


# ── RDBMS: CockroachDB ──


class TestCockroachDBAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("asyncpg")
        from demiurge_testdata.adapters.rdbms.cockroachdb import CockroachDBAdapter
        from demiurge_testdata.schemas.config import RDBMSAdapterConfig

        config = RDBMSAdapterConfig(
            host="localhost",
            port=26257,
            user="root",
            password="",
            database="testdata",
        )
        return CockroachDBAdapter(config=config)

    def test_dsn(self, adapter):
        assert "cockroachdb" in adapter.dsn
        assert "26257" in adapter.dsn

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("asyncpg")
        import demiurge_testdata.adapters.rdbms.cockroachdb  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "cockroachdb" in adapter_registry


# ── RDBMS: BigQuery ──


class TestBigQueryAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("google.cloud.bigquery")
        from demiurge_testdata.adapters.rdbms.bigquery import BigQueryAdapter
        from demiurge_testdata.schemas.config import BigQueryAdapterConfig

        config = BigQueryAdapterConfig(
            project_id="test-project",
            dataset_id="testdata",
        )
        return BigQueryAdapter(config=config)

    def test_config(self, adapter):
        assert adapter._config.project_id == "test-project"
        assert adapter._config.dataset_id == "testdata"

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("google.cloud.bigquery")
        import demiurge_testdata.adapters.rdbms.bigquery  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "bigquery" in adapter_registry


# ── NoSQL: Elasticsearch ──


class TestElasticsearchAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("elasticsearch")
        from demiurge_testdata.adapters.nosql.elasticsearch import (
            ElasticsearchAdapter,
        )
        from demiurge_testdata.schemas.config import NoSQLAdapterConfig

        config = NoSQLAdapterConfig(
            host="localhost",
            port=9200,
            database="testdata",
        )
        return ElasticsearchAdapter(config=config)

    def test_hosts(self, adapter):
        assert adapter.hosts == ["http://localhost:9200"]

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("elasticsearch")
        import demiurge_testdata.adapters.nosql.elasticsearch  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "elasticsearch" in adapter_registry


# ── NoSQL: Redis ──


class TestRedisAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("redis")
        from demiurge_testdata.adapters.nosql.redis import RedisAdapter
        from demiurge_testdata.schemas.config import NoSQLAdapterConfig

        config = NoSQLAdapterConfig(
            host="localhost",
            port=6379,
            database="testdata",
        )
        return RedisAdapter(config=config)

    def test_config(self, adapter):
        assert adapter._config.port == 6379

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("redis")
        import demiurge_testdata.adapters.nosql.redis  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "redis" in adapter_registry


# ── NoSQL: Cassandra ──


class TestCassandraAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("cassandra")
        from demiurge_testdata.adapters.nosql.cassandra import CassandraAdapter
        from demiurge_testdata.schemas.config import NoSQLAdapterConfig

        config = NoSQLAdapterConfig(
            host="localhost",
            port=9042,
            database="testdata",
        )
        return CassandraAdapter(config=config)

    def test_config(self, adapter):
        assert adapter._config.port == 9042

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("cassandra")
        import demiurge_testdata.adapters.nosql.cassandra  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "cassandra" in adapter_registry


# ── Streaming: RabbitMQ ──


class TestRabbitMQAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("aio_pika")
        from demiurge_testdata.adapters.streaming.rabbitmq import RabbitMQAdapter
        from demiurge_testdata.schemas.config import StreamAdapterConfig

        config = StreamAdapterConfig(
            host="localhost",
            port=5672,
            topic="test-queue",
        )
        return RabbitMQAdapter(config=config)

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_default_not_connected(self, adapter):
        assert adapter._connected is False

    def test_registry(self):
        pytest.importorskip("aio_pika")
        import demiurge_testdata.adapters.streaming.rabbitmq  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "rabbitmq" in adapter_registry


# ── Streaming: MQTT ──


class TestMQTTAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("aiomqtt")
        from demiurge_testdata.adapters.streaming.mqtt import MQTTAdapter
        from demiurge_testdata.schemas.config import StreamAdapterConfig

        config = StreamAdapterConfig(
            host="localhost",
            port=1883,
            topic="test-topic",
        )
        return MQTTAdapter(config=config)

    def test_config(self, adapter):
        assert adapter._config.port == 1883

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("aiomqtt")
        import demiurge_testdata.adapters.streaming.mqtt  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "mqtt" in adapter_registry


# ── Streaming: Pulsar ──


class TestPulsarAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("pulsar")
        from demiurge_testdata.adapters.streaming.pulsar import PulsarAdapter
        from demiurge_testdata.schemas.config import StreamAdapterConfig

        config = StreamAdapterConfig(
            host="localhost",
            port=6650,
            topic="test-topic",
        )
        return PulsarAdapter(config=config)

    def test_service_url(self, adapter):
        assert adapter.service_url == "pulsar://localhost:6650"

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("pulsar")
        import demiurge_testdata.adapters.streaming.pulsar  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "pulsar" in adapter_registry


# ── Streaming: NATS ──


class TestNATSAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("nats")
        from demiurge_testdata.adapters.streaming.nats import NATSAdapter
        from demiurge_testdata.schemas.config import NATSAdapterConfig

        config = NATSAdapterConfig(
            host="localhost",
            port=4222,
            subject="testdata.events",
        )
        return NATSAdapter(config=config)

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_default_not_connected(self, adapter):
        assert adapter._connected is False

    def test_registry(self):
        pytest.importorskip("nats")
        import demiurge_testdata.adapters.streaming.nats  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "nats" in adapter_registry


# ── Storage: LocalFS ──


class TestLocalFSAdapter:
    @pytest.fixture
    def adapter(self, tmp_path):
        pytest.importorskip("aiofiles")
        from demiurge_testdata.adapters.storage.local_fs import LocalFSAdapter
        from demiurge_testdata.schemas.config import StorageAdapterConfig

        config = StorageAdapterConfig(base_path=str(tmp_path / "testdata"))
        return LocalFSAdapter(config=config)

    async def test_connect_creates_dir(self, adapter):
        await adapter.connect()
        assert await adapter.health_check() is True

    async def test_write_and_read(self, adapter):
        await adapter.connect()
        await adapter.write("test/file.bin", b"hello")
        data = await adapter.read("test/file.bin")
        assert data == b"hello"

    async def test_list_keys(self, adapter):
        await adapter.connect()
        await adapter.write("a.txt", b"1")
        await adapter.write("b.txt", b"2")
        keys = await adapter.list_keys()
        assert len(keys) == 2

    async def test_delete(self, adapter):
        await adapter.connect()
        await adapter.write("del.txt", b"x")
        await adapter.delete("del.txt")
        keys = await adapter.list_keys()
        assert "del.txt" not in keys

    async def test_push_and_fetch(self, adapter):
        await adapter.connect()
        await adapter.push(b"data", {"key": "pushed.bin"})
        results = []
        async for chunk in adapter.fetch({"key": "pushed.bin"}):
            results.append(chunk)
        assert results == [b"data"]

    def test_registry(self):
        pytest.importorskip("aiofiles")
        import demiurge_testdata.adapters.storage.local_fs  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "local_fs" in adapter_registry


# ── Storage: HDFS ──


class TestHDFSAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("fsspec")
        from demiurge_testdata.adapters.storage.hdfs import HDFSAdapter
        from demiurge_testdata.schemas.config import StorageAdapterConfig

        config = StorageAdapterConfig(
            endpoint="http://localhost:9870",
            base_path="/demiurge_testdata",
        )
        return HDFSAdapter(config=config)

    def test_config(self, adapter):
        assert adapter._config.endpoint == "http://localhost:9870"

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("fsspec")
        import demiurge_testdata.adapters.storage.hdfs  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "hdfs" in adapter_registry


# ── FileTransfer: FTP ──


class TestFTPAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("aioftp")
        from demiurge_testdata.adapters.filetransfer.ftp import FTPAdapter
        from demiurge_testdata.schemas.config import FileTransferAdapterConfig

        config = FileTransferAdapterConfig(
            host="localhost",
            port=21,
            username="testdata",
            password="testdata_dev",
        )
        return FTPAdapter(config=config)

    def test_config(self, adapter):
        assert adapter._config.port == 21

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("aioftp")
        import demiurge_testdata.adapters.filetransfer.ftp  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "ftp" in adapter_registry


# ── FileTransfer: SFTP ──


class TestSFTPAdapter:
    @pytest.fixture
    def adapter(self):
        pytest.importorskip("paramiko")
        from demiurge_testdata.adapters.filetransfer.sftp import SFTPAdapter
        from demiurge_testdata.schemas.config import FileTransferAdapterConfig

        config = FileTransferAdapterConfig(
            host="localhost",
            port=22,
            username="testdata",
            password="testdata_dev",
        )
        return SFTPAdapter(config=config)

    def test_config(self, adapter):
        assert adapter._config.port == 22

    async def test_health_check_not_connected(self, adapter):
        assert await adapter.health_check() is False

    def test_registry(self):
        pytest.importorskip("paramiko")
        import demiurge_testdata.adapters.filetransfer.sftp  # noqa: F401
        from demiurge_testdata.core.registry import adapter_registry

        assert "sftp" in adapter_registry
