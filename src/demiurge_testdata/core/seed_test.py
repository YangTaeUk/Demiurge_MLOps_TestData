"""SeedTestPipeline -- DL 어댑터 통합 테스트 데이터를 인프라에 적재한다.

요청서 스펙 기반:
- 공통 스키마 6컬럼 (id, name, value, category, created_at, is_active)
- 정확한 DDL 타입 (INTEGER, VARCHAR, DOUBLE PRECISION, TIMESTAMP, BOOLEAN)
- 지정된 테이블명/토픽명/파일명
- 어댑터별 특수 요구사항 (zero-date, large table, 다중 파일 형식 등)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from demiurge_testdata.adapters.base import (
    BaseFileTransferAdapter,
    BaseNoSQLAdapter,
    BaseRDBMSAdapter,
    BaseStorageAdapter,
    BaseStreamAdapter,
)
from demiurge_testdata.core.registry import adapter_registry, format_registry
from demiurge_testdata.generators.adapter_test import (
    ADAPTER_TEST_COLUMNS,
    generate_records,
)
from demiurge_testdata.handlers.chain import HandlerChain

logger = logging.getLogger(__name__)


# ── 어댑터별 DDL 타입 변환 ──

_TYPE_MAP_MYSQL: dict[str, str] = {
    "INTEGER": "INT",
    "VARCHAR(100)": "VARCHAR(100)",
    "VARCHAR(50)": "VARCHAR(50)",
    "DOUBLE PRECISION": "DOUBLE",
    "TIMESTAMP": "DATETIME",
    "BOOLEAN": "TINYINT(1)",
}

_TYPE_MAP_MARIADB = _TYPE_MAP_MYSQL  # 동일

_TYPE_MAP_PG: dict[str, str] = {
    "INTEGER": "INTEGER",
    "VARCHAR(100)": "VARCHAR(100)",
    "VARCHAR(50)": "VARCHAR(50)",
    "DOUBLE PRECISION": "DOUBLE PRECISION",
    "TIMESTAMP": "TIMESTAMP",
    "BOOLEAN": "BOOLEAN",
}


def _map_columns(
    columns: dict[str, str], adapter_type: str
) -> dict[str, str]:
    """어댑터 타입에 따라 SQL 타입을 변환한다."""
    if adapter_type in ("mysql", "mariadb"):
        return {col: _TYPE_MAP_MYSQL.get(t, t) for col, t in columns.items()}
    return {col: _TYPE_MAP_PG.get(t, t) for col, t in columns.items()}


def _cast_record(record: dict[str, Any]) -> dict[str, Any]:
    """생성된 레코드의 Python 타입을 보장한다."""
    from datetime import datetime

    r = dict(record)
    r["id"] = int(r["id"])
    r["value"] = float(r["value"])
    r["is_active"] = bool(r["is_active"])
    if r["created_at"] and r["created_at"] != "0000-00-00 00:00:00":
        r["created_at"] = datetime.fromisoformat(r["created_at"])
    return r


# ── 적재 스펙 정의 ──

# 각 어댑터별 적재 설정
SEED_TEST_SPEC: dict[str, dict[str, Any]] = {
    "postgresql": {
        "tables": [
            {"name": "test_adapter_sample", "count": 500},
            {"name": "test_large_table", "count": 10_000},
        ],
    },
    "mysql": {
        "tables": [
            {"name": "test_adapter_sample", "count": 500, "include_zero_date": True},
        ],
    },
    "mariadb": {
        "tables": [
            {"name": "test_adapter_sample", "count": 500},
        ],
    },
    "mongodb": {
        "collections": [
            {"name": "test_adapter_sample", "count": 500},
        ],
    },
    "elasticsearch": {
        "indices": [
            {"name": "test_adapter_sample", "count": 500},
        ],
    },
    "redis": {
        "collections": [
            {"name": "test_adapter_sample", "count": 100},
        ],
    },
    "kafka": {
        "topics": [
            {"name": "test_adapter_sample", "count": 100},
        ],
    },
    "rabbitmq": {
        "topics": [
            {"name": "test_adapter_sample", "count": 100},
        ],
    },
    "nats": {
        "topics": [
            {"name": "test.adapter.sample", "count": 100},
        ],
    },
    "mqtt": {
        "topics": [
            {"name": "test/adapter/sample", "count": 50},
        ],
    },
    "s3": {
        "files": [
            {"key": "csv/sample.csv", "format": "csv", "count": 500},
            {"key": "parquet/sample.parquet", "format": "parquet", "count": 500},
            {"key": "json/sample.jsonl", "format": "jsonl", "count": 500},
        ],
        "bucket": "test-adapter-data",
    },
    "ftp": {
        "files": [
            {"key": "/test-data/sample.csv", "format": "csv", "count": 500},
        ],
    },
    "sftp": {
        "files": [
            {"key": "/data/test-data/sample.csv", "format": "csv", "count": 500},
        ],
    },
    "local_fs": {
        "files": [
            {"key": "sample.csv", "format": "csv", "count": 500},
            {"key": "sample.parquet", "format": "parquet", "count": 500},
            {"key": "sample.json", "format": "json", "count": 100},
            {"key": "sample.xlsx", "format": "xlsx", "count": 100},
        ],
    },
}


class SeedTestPipeline:
    """DL 어댑터 통합 테스트 데이터 적재 파이프라인."""

    def __init__(self, adapter_configs: dict[str, dict[str, Any]]):
        self._adapter_configs = adapter_configs

    async def seed_target(
        self, target: str, *, drop_existing: bool = False
    ) -> dict[str, int]:
        """단일 어댑터 타겟에 테스트 데이터를 적재한다.

        Returns:
            {"table_or_topic_name": record_count, ...}
        """
        spec = SEED_TEST_SPEC.get(target)
        if not spec:
            logger.warning("seed-test 스펙에 %s가 정의되어 있지 않습니다", target)
            return {}

        config = self._adapter_configs.get(target, {})
        results: dict[str, int] = {}

        # RDBMS
        if "tables" in spec:
            results.update(
                await self._seed_rdbms(target, config, spec["tables"], drop_existing)
            )

        # NoSQL (MongoDB, ES, Redis)
        if "collections" in spec or "indices" in spec:
            items = spec.get("collections") or spec.get("indices", [])
            results.update(await self._seed_nosql(target, config, items))

        # Streaming
        if "topics" in spec:
            results.update(
                await self._seed_streaming(target, config, spec["topics"], drop_existing)
            )

        # Storage / FileTransfer
        if "files" in spec:
            # S3는 버킷 오버라이드 가능
            extra_config = dict(config)
            if "bucket" in spec:
                extra_config["bucket"] = spec["bucket"]
            results.update(
                await self._seed_files(target, extra_config, spec["files"])
            )

        return results

    async def seed_all(
        self, *, targets: list[str] | None = None, drop_existing: bool = False
    ) -> dict[str, dict[str, int]]:
        """모든 (또는 지정된) 타겟에 테스트 데이터를 적재한다."""
        all_targets = targets or list(SEED_TEST_SPEC.keys())
        all_results: dict[str, dict[str, int]] = {}

        for target in all_targets:
            if target not in SEED_TEST_SPEC:
                print(f"  [SKIP] {target}: seed-test 스펙 없음")
                continue
            if target not in self._adapter_configs:
                print(f"  [SKIP] {target}: 어댑터 설정 없음")
                continue

            print(f"  Seeding test data → {target}...")
            try:
                result = await asyncio.wait_for(
                    self.seed_target(target, drop_existing=drop_existing),
                    timeout=60,
                )
                all_results[target] = result
                for name, count in result.items():
                    print(f"    [{target}] {name}: {count} records")
            except asyncio.TimeoutError:
                print(f"    [{target}] FAILED: timeout (60s)")
                logger.warning("seed-test %s 타임아웃", target)
            except Exception as exc:
                print(f"    [{target}] FAILED: {exc}")
                logger.exception("seed-test %s 실패", target)

        return all_results

    # ── Private Methods ──

    async def _seed_rdbms(
        self,
        adapter_type: str,
        config: dict[str, Any],
        tables: list[dict[str, Any]],
        drop_existing: bool,
    ) -> dict[str, int]:
        results: dict[str, int] = {}
        adapter = adapter_registry.create(adapter_type, **config)
        columns = _map_columns(ADAPTER_TEST_COLUMNS, adapter_type)

        async with adapter:
            # MySQL zero-date 허용을 위해 strict mode 비활성화
            if adapter_type == "mysql":
                await adapter.execute_sql("SET SESSION sql_mode = ''")

            for table_spec in tables:
                table_name = table_spec["name"]
                count = table_spec["count"]
                include_zero_date = table_spec.get("include_zero_date", False)

                if drop_existing:
                    await adapter.execute_sql(f"DROP TABLE IF EXISTS {table_name}")

                await adapter.create_table(table_name, columns)

                records = generate_records(
                    count, seed=42, include_zero_date=include_zero_date
                )
                # MySQL zero-date는 문자열로 유지, 나머지는 Python 타입 캐스팅
                casted = []
                for rec in records:
                    if rec["created_at"] == "0000-00-00 00:00:00":
                        r = dict(rec)
                        r["id"] = int(r["id"])
                        r["value"] = float(r["value"])
                        r["is_active"] = bool(r["is_active"])
                        casted.append(r)
                    else:
                        casted.append(_cast_record(rec))

                total = 0
                batch_size = 5000
                for i in range(0, len(casted), batch_size):
                    batch = casted[i : i + batch_size]
                    inserted = await adapter.bulk_insert(table_name, batch)
                    total += inserted

                results[table_name] = total

        return results

    async def _seed_nosql(
        self,
        adapter_type: str,
        config: dict[str, Any],
        items: list[dict[str, Any]],
    ) -> dict[str, int]:
        results: dict[str, int] = {}
        adapter = adapter_registry.create(adapter_type, **config)

        async with adapter:
            for item in items:
                name = item["name"]
                count = item["count"]
                records = generate_records(count, seed=42)
                inserted = await adapter.insert_documents(name, records)
                results[name] = inserted

        return results

    async def _seed_streaming(
        self,
        adapter_type: str,
        config: dict[str, Any],
        topics: list[dict[str, Any]],
        drop_existing: bool = False,
    ) -> dict[str, int]:
        results: dict[str, int] = {}
        fmt = format_registry.create("json")
        chain = HandlerChain(format_handler=fmt, compression_handler=None)

        # --drop 시 기존 토픽/큐/스트림 정리
        if drop_existing:
            await self._cleanup_streaming(adapter_type, config, topics)

        adapter = adapter_registry.create(adapter_type, **config)

        async with adapter:
            for topic_spec in topics:
                topic = topic_spec["name"]
                count = topic_spec["count"]
                records = generate_records(count, seed=42)

                # 개별 레코드를 각각 메시지로 인코딩
                messages: list[bytes] = []
                for rec in records:
                    encoded = await chain.encode([rec])
                    messages.append(encoded)

                await adapter.publish_batch(topic, messages)
                results[topic] = len(messages)

        return results

    async def _cleanup_streaming(
        self,
        adapter_type: str,
        config: dict[str, Any],
        topics: list[dict[str, Any]],
    ) -> None:
        """스트리밍 어댑터의 기존 토픽/큐/스트림을 정리한다."""
        topic_names = [t["name"] for t in topics]

        if adapter_type == "kafka":
            await self._cleanup_kafka(config, topic_names)
        elif adapter_type == "rabbitmq":
            await self._cleanup_rabbitmq(config, topic_names)
        elif adapter_type == "nats":
            await self._cleanup_nats(config, topic_names)
        # MQTT는 pub/sub이므로 정리 불필요

    async def _cleanup_kafka(
        self, config: dict[str, Any], topic_names: list[str]
    ) -> None:
        """Kafka 토픽을 삭제한다."""
        try:
            from aiokafka.admin import AIOKafkaAdminClient

            admin = AIOKafkaAdminClient(
                bootstrap_servers=f"{config.get('host', '127.0.0.1')}:{config.get('port', 9092)}",
            )
            await admin.start()
            try:
                await admin.delete_topics(topic_names)
                # 토픽 삭제 후 재생성 대기
                import asyncio

                await asyncio.sleep(2)
            except Exception:
                pass  # 토픽이 없으면 무시
            finally:
                await admin.close()
        except ImportError:
            logger.warning("aiokafka admin client를 import할 수 없습니다")

    async def _cleanup_rabbitmq(
        self, config: dict[str, Any], queue_names: list[str]
    ) -> None:
        """RabbitMQ 큐를 삭제한다."""
        try:
            import aio_pika

            host = config.get("host", "127.0.0.1")
            port = config.get("port", 5672)
            username = config.get("username", "guest")
            password = config.get("password", "guest")
            url = f"amqp://{username}:{password}@{host}:{port}/"

            connection = await aio_pika.connect_robust(url)
            channel = await connection.channel()
            for name in queue_names:
                try:
                    await channel.queue_delete(name)
                except Exception:
                    pass  # 큐가 없으면 무시
            await channel.close()
            await connection.close()
        except ImportError:
            logger.warning("aio_pika를 import할 수 없습니다")

    async def _cleanup_nats(
        self, config: dict[str, Any], subjects: list[str]
    ) -> None:
        """NATS JetStream 스트림을 삭제한다."""
        try:
            import nats as nats_client

            host = config.get("host", "127.0.0.1")
            port = config.get("port", 4222)
            nc = await nats_client.connect(f"nats://{host}:{port}", connect_timeout=10)
            js = nc.jetstream()
            for subject in subjects:
                stream_name = subject.replace(".", "_").replace("/", "_").upper()
                try:
                    await js.delete_stream(stream_name)
                except Exception:
                    pass  # 스트림이 없으면 무시
            await nc.drain()
        except ImportError:
            logger.warning("nats-py를 import할 수 없습니다")

    async def _seed_files(
        self,
        adapter_type: str,
        config: dict[str, Any],
        files: list[dict[str, Any]],
    ) -> dict[str, int]:
        results: dict[str, int] = {}
        adapter = adapter_registry.create(adapter_type, **config)

        async with adapter:
            for file_spec in files:
                key = file_spec["key"]
                fmt_name = file_spec["format"]
                count = file_spec["count"]
                records = generate_records(count, seed=42)

                fmt = format_registry.create(fmt_name)
                chain = HandlerChain(format_handler=fmt, compression_handler=None)
                encoded = await chain.encode(records)

                # Storage adapter → write(), FileTransfer adapter → push()
                if isinstance(adapter, BaseStorageAdapter):
                    await adapter.write(key, encoded)
                else:
                    await adapter.push(encoded, {"key": key})

                results[key] = count

        return results
