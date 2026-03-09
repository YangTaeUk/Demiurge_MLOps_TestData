"""CLI 진입점 — python -m demiurge_testdata [command]"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

import yaml


def _import_generators() -> None:
    """모든 제너레이터를 import하여 레지스트리에 등록"""
    from demiurge_testdata.generators import document, event, geospatial, iot, relational, text  # noqa: F401  # isort: skip
    from demiurge_testdata.generators import adapter_test  # noqa: F401


def _import_handlers() -> None:
    """포맷/압축 핸들러를 import하여 레지스트리에 등록"""
    import importlib

    handler_modules = [
        "demiurge_testdata.handlers.formats.json_handler",
        "demiurge_testdata.handlers.formats.jsonl_handler",
        "demiurge_testdata.handlers.formats.csv_handler",
        "demiurge_testdata.handlers.formats.parquet_handler",
        "demiurge_testdata.handlers.formats.avro_handler",
        "demiurge_testdata.handlers.formats.orc_handler",
        "demiurge_testdata.handlers.formats.msgpack_handler",
        "demiurge_testdata.handlers.formats.arrow_handler",
        "demiurge_testdata.handlers.formats.xml_handler",
        "demiurge_testdata.handlers.formats.yaml_handler",
        "demiurge_testdata.handlers.formats.excel_handler",
        "demiurge_testdata.handlers.compression.cramjam_handler",
    ]
    for mod in handler_modules:
        with contextlib.suppress(ImportError):
            importlib.import_module(mod)


def _import_adapters() -> None:
    """사용 가능한 어댑터를 import하여 레지스트리에 등록"""
    import importlib

    adapter_modules = [
        # RDBMS
        "demiurge_testdata.adapters.rdbms.postgresql",
        "demiurge_testdata.adapters.rdbms.mysql",
        "demiurge_testdata.adapters.rdbms.mariadb",
        "demiurge_testdata.adapters.rdbms.sqlite",
        # NoSQL
        "demiurge_testdata.adapters.nosql.mongodb",
        "demiurge_testdata.adapters.nosql.elasticsearch",
        "demiurge_testdata.adapters.nosql.redis",
        # Streaming
        "demiurge_testdata.adapters.streaming.kafka",
        "demiurge_testdata.adapters.streaming.rabbitmq",
        "demiurge_testdata.adapters.streaming.nats",
        "demiurge_testdata.adapters.streaming.mqtt",
        # Storage
        "demiurge_testdata.adapters.storage.s3",
        "demiurge_testdata.adapters.storage.local_fs",
        "demiurge_testdata.adapters.storage.hdfs",
        # FileTransfer
        "demiurge_testdata.adapters.filetransfer.ftp",
        "demiurge_testdata.adapters.filetransfer.sftp",
    ]
    for mod in adapter_modules:
        with contextlib.suppress(ImportError):
            importlib.import_module(mod)


def cmd_run(args: argparse.Namespace) -> None:
    """파이프라인 실행"""
    from demiurge_testdata.core.config import load_config

    config = load_config(args.config)
    _import_generators()
    _import_adapters()

    from demiurge_testdata.core.pipeline import DataPipeline
    from demiurge_testdata.core.registry import (
        adapter_registry,
        compression_registry,
        format_registry,
        generator_registry,
    )
    from demiurge_testdata.handlers.chain import HandlerChain

    async def _run() -> None:
        generator = generator_registry.create(config.generator.type, config=config.generator)
        format_handler = format_registry.create(config.handler.format.value)
        compression_handler = (
            compression_registry.create(config.handler.compression.value)
            if config.handler.compression.value != "none"
            else None
        )
        handler_chain = HandlerChain(
            format_handler=format_handler,
            compression_handler=compression_handler,
        )
        adapter = adapter_registry.create(config.adapter.type, **config.adapter.extra)

        pipeline = DataPipeline(
            generator=generator,
            handler_chain=handler_chain,
            adapter=adapter,
            batch_size=config.generator.batch_size,
        )

        async with adapter:
            if config.mode.value == "batch":
                metrics = await pipeline.run_batch()
            else:
                metrics = await pipeline.run_stream()

        print(f"Pipeline '{config.name}' completed:")
        print(f"  Records: {metrics.total_records}")
        print(f"  Bytes: {metrics.total_bytes}")
        print(f"  Elapsed: {metrics.elapsed_seconds:.2f}s")
        print(f"  Throughput: {metrics.records_per_second:.0f} records/sec")
        if metrics.errors:
            print(f"  Errors: {metrics.errors}")

    asyncio.run(_run())


def cmd_list(args: argparse.Namespace) -> None:
    """등록된 컴포넌트 목록 출력"""
    _import_generators()
    _import_adapters()

    from demiurge_testdata.core.registry import (
        adapter_registry,
        compression_registry,
        format_registry,
        generator_registry,
    )

    target = args.target
    registries = {
        "generators": generator_registry,
        "adapters": adapter_registry,
        "formats": format_registry,
        "compressions": compression_registry,
    }

    if target == "all":
        for name, reg in registries.items():
            print(f"\n{name} ({len(reg)} registered):")
            for key in sorted(reg.list_registered()):
                print(f"  - {key}")
    elif target in registries:
        reg = registries[target]
        print(f"{target} ({len(reg)} registered):")
        for key in sorted(reg.list_registered()):
            print(f"  - {key}")
    else:
        print(f"Unknown target: {target}. Use: all, generators, adapters, formats, compressions")
        sys.exit(1)


def cmd_serve(args: argparse.Namespace) -> None:
    """REST API 서버 시작"""
    _import_generators()
    _import_adapters()

    import uvicorn

    from demiurge_testdata.api.rest.app import app

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


logger = logging.getLogger(__name__)

_MANIFEST_PATH = Path("configs/datasets/manifest.yaml")
_DATA_DIR = Path("data/raw")

# ── 핵심 필수 데이터셋 (빠른 테스트용) ──
_ESSENTIAL_DATASETS = [
    "chinook", "northwind", "tmdb", "airbnb", "cc_fraud",
    "appliances_energy", "smart_mfg", "weather",
]

# ── 스트리밍 가능 seed_target 목록 ──
_STREAMING_TARGETS = {"kafka", "nats", "rabbitmq", "mqtt"}


def _load_dotenv(path: Path | None = None) -> None:
    """프로젝트 루트의 .env 파일을 읽어 os.environ에 반영한다.

    이미 셸에서 설정된 환경변수는 덮어쓰지 않는다 (setdefault 방식).
    python-dotenv 의존성 없이 stdlib만 사용한다.
    """
    env_path = path or Path(".env")
    if not env_path.exists():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def _build_default_adapter_config() -> dict[str, Any]:
    """어댑터별 기본 연결 설정을 반환한다 (.env 환경변수 지원)."""
    _load_dotenv()
    return {
        "postgresql": {
            "host": "127.0.0.1", "port": int(os.environ.get("PG_PORT", 5434)),
            "user": "testdata", "password": "testdata_dev", "database": "testdata",
        },
        "mongodb": {
            "host": "127.0.0.1", "port": int(os.environ.get("MONGO_PORT", 27017)),
            "username": "testdata", "password": "testdata_dev", "database": "testdata",
        },
        "mysql": {
            "host": "127.0.0.1", "port": int(os.environ.get("MYSQL_PORT", 3306)),
            "user": "testdata", "password": "testdata_dev",
            "database": "testdata",
        },
        "elasticsearch": {
            "host": "127.0.0.1", "port": int(os.environ.get("ES_PORT", 9200)),
        },
        "kafka": {
            "host": "127.0.0.1", "port": int(os.environ.get("KAFKA_PORT", 9092)),
        },
        "nats": {
            "host": "127.0.0.1", "port": int(os.environ.get("NATS_PORT", 4222)),
        },
        "mqtt": {
            "host": "127.0.0.1", "port": int(os.environ.get("MQTT_PORT", 1883)),
        },
        "s3": {
            "endpoint": f"http://127.0.0.1:{os.environ.get('MINIO_API_PORT', 9000)}",
            "access_key": os.environ.get("MINIO_ROOT_USER", "testdata"),
            "secret_key": os.environ.get("MINIO_ROOT_PASSWORD", "testdata_dev_password"),
            "bucket": "testdata",
        },
        "ftp": {
            "host": "127.0.0.1", "port": int(os.environ.get("FTP_PORT", 21)),
            "username": "testdata", "password": "testdata_dev",
            "remote_base_path": "/data",
        },
        "sftp": {
            "host": "127.0.0.1", "port": int(os.environ.get("SFTP_PORT", 2222)),
            "username": "testdata", "password": "testdata_dev",
            "remote_base_path": "/data",
        },
        "mariadb": {
            "host": "127.0.0.1", "port": int(os.environ.get("MARIADB_PORT", 3307)),
            "user": "testdata", "password": "testdata_dev",
            "database": "testdata",
        },
        "rabbitmq": {
            "host": "127.0.0.1", "port": int(os.environ.get("RABBITMQ_PORT", 5672)),
            "username": "testdata", "password": "testdata_dev",
        },
        "redis": {
            "host": "127.0.0.1", "port": int(os.environ.get("REDIS_PORT", 6379)),
        },
        "local_fs": {
            "base_path": os.environ.get("LOCAL_FS_PATH", "/tmp/demiurge_testdata"),
        },
    }


def _load_manifest(path: Path | None = None) -> dict[str, Any]:
    """매니페스트 YAML 로드."""
    manifest_path = path or _MANIFEST_PATH
    if not manifest_path.exists():
        print(f"Manifest not found: {manifest_path}")
        sys.exit(1)
    with open(manifest_path) as f:
        return yaml.safe_load(f)


def cmd_download(args: argparse.Namespace) -> None:
    """Kaggle 데이터셋 다운로드"""
    from demiurge_testdata.data.downloader import DataValidator, KaggleDownloader

    manifest = _load_manifest(args.manifest)
    downloader = KaggleDownloader(data_dir=_DATA_DIR)
    validator = DataValidator(data_dir=_DATA_DIR)

    targets = args.datasets or list(manifest.keys())
    if args.essential:
        targets = [d for d in _ESSENTIAL_DATASETS if d in manifest]
    if args.category:
        targets = [d for d in targets if manifest[d].get("category") == args.category]

    for name in targets:
        if name not in manifest:
            print(f"  [SKIP] {name}: not in manifest")
            continue

        entry = manifest[name]
        print(f"  Downloading {name} ({entry['kaggle_id']})...")
        try:
            downloader.download_from_manifest(entry, name)
            result = validator.validate(name, entry)
            status = "OK" if result.ok else f"WARN: {result.errors}"
            print(f"  [{name}] {status}")
        except Exception as exc:
            print(f"  [{name}] FAILED: {exc}")
            if not args.skip_errors:
                sys.exit(1)


def cmd_seed(args: argparse.Namespace) -> None:
    """다운로드된 데이터를 인프라에 시딩"""
    _import_adapters()
    _import_handlers()

    from demiurge_testdata.core.registry import (
        compression_registry,
        format_registry,
    )
    from demiurge_testdata.core.seed import SeedPipeline, load_csv_records, load_sqlite_records
    from demiurge_testdata.handlers.chain import HandlerChain

    manifest = _load_manifest(args.manifest)

    targets = args.datasets or list(manifest.keys())
    if args.essential:
        targets = [d for d in _ESSENTIAL_DATASETS if d in manifest]
    if args.category:
        targets = [d for d in targets if manifest[d].get("category") == args.category]

    # 어댑터 설정 로드 (.env 기본값 → 파일 오버라이드)
    adapter_config: dict[str, Any] = _build_default_adapter_config()
    if args.adapter_config:
        with open(args.adapter_config) as f:
            overrides = yaml.safe_load(f) or {}
            adapter_config.update(overrides)

    async def _seed_all() -> None:
        for name in targets:
            if name not in manifest:
                print(f"  [SKIP] {name}: not in manifest")
                continue

            entry = manifest[name]
            seed_target = entry.get("seed_target", "postgresql")
            primary = entry.get("primary_file")

            if not primary:
                print(f"  [SKIP] {name}: no primary_file defined")
                continue

            csv_path = _DATA_DIR / name / primary
            if not csv_path.exists():
                print(f"  [SKIP] {name}: {csv_path} not found (run download first)")
                continue

            # 시딩 대상별 어댑터 설정 결정
            target_config = adapter_config.get(seed_target, {})

            print(f"  Seeding {name} → {seed_target}...")
            try:
                handler_chain = None

                # Storage/Streaming 타겟은 HandlerChain 필요
                if seed_target in ("s3", "hdfs", "local_fs"):
                    fmt = format_registry.create("parquet")
                    handler_chain = HandlerChain(format_handler=fmt, compression_handler=None)
                elif seed_target in ("kafka", "nats", "rabbitmq", "mqtt"):
                    fmt = format_registry.create("json")
                    handler_chain = HandlerChain(format_handler=fmt, compression_handler=None)

                pipeline = SeedPipeline(
                    adapter_type=seed_target,
                    adapter_config=target_config,
                    handler_chain=handler_chain,
                )
                if csv_path.suffix == ".sqlite":
                    records = load_sqlite_records(csv_path, limit=args.limit)
                else:
                    records = load_csv_records(csv_path, limit=args.limit)
                category = entry.get("category", "relational")

                if seed_target in ("postgresql", "mysql", "mariadb", "sqlite", "bigquery"):
                    count = await pipeline.seed_rdbms(
                        table=name, records=records, drop_existing=args.drop,
                    )
                elif seed_target in ("mongodb", "elasticsearch", "redis"):
                    count = await pipeline.seed_nosql(
                        collection=name, documents=records,
                    )
                elif seed_target in ("s3", "hdfs", "local_fs"):
                    count = await pipeline.seed_storage(
                        key=f"{category}/{name}.parquet", records=records,
                    )
                elif seed_target in ("kafka", "nats", "rabbitmq", "mqtt"):
                    count = await pipeline.seed_streaming(
                        topic=f"testdata.{name}", records=records,
                    )
                else:
                    print(f"  [SKIP] {name}: unsupported seed_target '{seed_target}'")
                    continue

                print(f"  [{name}] {count} records seeded to {seed_target}")
            except Exception as exc:
                print(f"  [{name}] FAILED: {exc}")
                if not args.skip_errors:
                    raise

    asyncio.run(_seed_all())


def cmd_stream(args: argparse.Namespace) -> None:
    """실시간 스트리밍 시뮬레이션 — 레코드를 개별 JSON 메시지로 Kafka/NATS/MQTT에 전송"""
    _import_adapters()
    _import_handlers()

    from datetime import datetime

    from demiurge_testdata.core.registry import adapter_registry, format_registry
    from demiurge_testdata.core.seed import load_csv_records, load_sqlite_records
    from demiurge_testdata.handlers.chain import HandlerChain

    manifest = _load_manifest(args.manifest)
    adapter_config = _build_default_adapter_config()

    targets = args.datasets or list(manifest.keys())
    if args.essential:
        targets = [d for d in _ESSENTIAL_DATASETS if d in manifest]

    # 스트리밍 가능한 데이터셋만 필터링
    stream_targets: list[tuple[str, dict]] = []
    for name in targets:
        if name not in manifest:
            continue
        entry = manifest[name]
        seed_target = entry.get("seed_target", "postgresql")
        if seed_target not in _STREAMING_TARGETS:
            print(f"  [SKIP] {name}: seed_target '{seed_target}' is not a streaming target")
            continue
        stream_targets.append((name, entry))

    if not stream_targets:
        print("No streaming-eligible datasets found.")
        return

    interval = args.interval
    batch_size = args.batch_size
    loop = args.loop
    limit = args.limit

    async def _stream_one(
        name: str, entry: dict, shutdown_event: asyncio.Event,
    ) -> None:
        """단일 데이터셋 스트리밍 코루틴 (재연결 + 라운드 로깅)"""
        seed_target = entry["seed_target"]
        primary = entry.get("primary_file")
        if not primary:
            print(f"  [SKIP] {name}: no primary_file defined")
            return

        data_path = _DATA_DIR / name / primary
        if not data_path.exists():
            print(f"  [SKIP] {name}: {data_path} not found (run download first)")
            return

        # 레코드 로드
        if data_path.suffix == ".sqlite":
            records = load_sqlite_records(data_path)
        else:
            records = load_csv_records(data_path)

        if not records:
            print(f"  [SKIP] {name}: no records loaded")
            return

        target_config = adapter_config.get(seed_target, {})
        topic = f"testdata.{name}"

        print(f"  Streaming {name} → {seed_target}:{topic} "
              f"(interval={interval}s, batch={batch_size}, loop={loop})")

        total_sent = 0
        round_num = 0
        idx = 0
        start_time = datetime.now()
        retry_delay = 1.0

        while not shutdown_event.is_set():
            # 어댑터·핸들러를 매 연결마다 새로 생성 (연결 상태 초기화)
            fmt = format_registry.create("json")
            chain = HandlerChain(format_handler=fmt, compression_handler=None)
            adapter = adapter_registry.create(seed_target, **target_config)

            try:
                async with adapter:
                    retry_delay = 1.0  # 연결 성공 → 백오프 리셋

                    while not shutdown_event.is_set():
                        batch = records[idx : idx + batch_size]
                        if not batch:
                            if loop:
                                round_num += 1
                                print(f"\n  [{name}] round {round_num} complete, "
                                      f"total={total_sent}")
                                idx = 0
                                continue
                            else:
                                return

                        for rec in batch:
                            encoded = await chain.encode([rec])
                            await adapter.publish(topic, encoded)

                        total_sent += len(batch)
                        idx += len(batch)

                        elapsed = (datetime.now() - start_time).total_seconds()
                        rate = total_sent / elapsed if elapsed > 0 else 0
                        print(f"\r  [{name}] sent={total_sent}  "
                              f"rate={rate:.1f} rec/s  round={round_num}",
                              end="", flush=True)

                        if limit and total_sent >= limit:
                            return

                        await asyncio.sleep(interval)

            except Exception as exc:
                if shutdown_event.is_set():
                    break
                print(f"\n  [{name}] connection error: {exc}, "
                      f"retry in {retry_delay:.0f}s")
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=retry_delay,
                    )
                    break  # shutdown 시그널 수신
                except asyncio.TimeoutError:
                    pass  # 타임아웃 → 재연결 시도
                retry_delay = min(retry_delay * 2, 60.0)

            if not loop:
                break

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n  [{name}] Done: {total_sent} records in {elapsed:.1f}s "
              f"({round_num} rounds)")

    async def _stream_all() -> None:
        shutdown_event = asyncio.Event()

        def _signal_handler() -> None:
            print("\nShutdown signal received, finishing current batch...")
            shutdown_event.set()

        ev_loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                ev_loop.add_signal_handler(sig, _signal_handler)
            except NotImplementedError:
                pass  # Windows — KeyboardInterrupt로 폴백

        tasks = [
            _stream_one(name, entry, shutdown_event)
            for name, entry in stream_targets
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for (name, _), result in zip(stream_targets, results):
            if isinstance(result, Exception):
                print(f"\n  [{name}] FAILED: {result}")
                if not args.skip_errors:
                    raise result

    try:
        asyncio.run(_stream_all())
    except KeyboardInterrupt:
        print("\nStreaming stopped by user.")


def cmd_filedrop(args: argparse.Namespace) -> None:
    """파일 드롭 시뮬레이션 — 레코드를 파일로 묶어 FTP/SFTP에 업로드"""
    _import_adapters()
    _import_handlers()

    from datetime import datetime

    from demiurge_testdata.core.registry import adapter_registry, format_registry
    from demiurge_testdata.core.seed import load_csv_records, load_sqlite_records
    from demiurge_testdata.handlers.chain import HandlerChain

    manifest = _load_manifest(args.manifest)
    adapter_config = _build_default_adapter_config()

    targets = args.datasets or list(manifest.keys())
    if args.essential:
        targets = [d for d in _ESSENTIAL_DATASETS if d in manifest]

    # 파일 드롭은 모든 데이터셋에 적용 가능
    drop_targets: list[tuple[str, dict]] = []
    for name in targets:
        if name not in manifest:
            continue
        drop_targets.append((name, manifest[name]))

    if not drop_targets:
        print("No datasets found for filedrop.")
        return

    interval = args.interval
    records_per_file = args.records_per_file
    fmt_name = args.format
    target_type = args.target  # ftp or sftp
    loop = args.loop
    limit = args.limit

    # 포맷 → 확장자 매핑
    _ext_map = {"csv": ".csv", "json": ".json", "parquet": ".parquet"}
    ext = _ext_map.get(fmt_name, f".{fmt_name}")

    async def _filedrop_one(
        name: str, entry: dict, shutdown_event: asyncio.Event,
    ) -> None:
        """단일 데이터셋 파일드롭 코루틴 (재연결 + 라운드 로깅)"""
        primary = entry.get("primary_file")
        if not primary:
            print(f"  [SKIP] {name}: no primary_file defined")
            return

        data_path = _DATA_DIR / name / primary
        if not data_path.exists():
            print(f"  [SKIP] {name}: {data_path} not found (run download first)")
            return

        # 레코드 로드
        if data_path.suffix == ".sqlite":
            records = load_sqlite_records(data_path)
        else:
            records = load_csv_records(data_path)

        if not records:
            print(f"  [SKIP] {name}: no records loaded")
            return

        target_config = adapter_config.get(target_type, {})

        print(f"  FileDrop {name} → {target_type} "
              f"(interval={interval}s, records/file={records_per_file}, "
              f"format={fmt_name}, loop={loop})")

        total_sent = 0
        files_dropped = 0
        round_num = 0
        idx = 0
        start_time = datetime.now()
        retry_delay = 1.0

        while not shutdown_event.is_set():
            # 어댑터·핸들러를 매 연결마다 새로 생성 (연결 상태 초기화)
            fmt = format_registry.create(fmt_name)
            chain = HandlerChain(format_handler=fmt, compression_handler=None)
            adapter = adapter_registry.create(target_type, **target_config)

            try:
                async with adapter:
                    retry_delay = 1.0  # 연결 성공 → 백오프 리셋

                    while not shutdown_event.is_set():
                        batch = records[idx : idx + records_per_file]
                        if not batch:
                            if loop:
                                round_num += 1
                                print(f"\n  [{name}] round {round_num} complete, "
                                      f"files={files_dropped} total={total_sent}")
                                idx = 0
                                continue
                            else:
                                return

                        encoded = await chain.encode(batch)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        remote_path = (
                            f"/data/{name}/{name}_{timestamp}_{files_dropped:04d}{ext}"
                        )
                        await adapter.push(encoded, {"key": remote_path})

                        files_dropped += 1
                        total_sent += len(batch)
                        idx += len(batch)

                        print(f"\r  [{name}] files={files_dropped}  "
                              f"records={total_sent}  round={round_num}",
                              end="", flush=True)

                        if limit and total_sent >= limit:
                            return

                        await asyncio.sleep(interval)

            except Exception as exc:
                if shutdown_event.is_set():
                    break
                print(f"\n  [{name}] connection error: {exc}, "
                      f"retry in {retry_delay:.0f}s")
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=retry_delay,
                    )
                    break  # shutdown 시그널 수신
                except asyncio.TimeoutError:
                    pass  # 타임아웃 → 재연결 시도
                retry_delay = min(retry_delay * 2, 60.0)

            if not loop:
                break

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n  [{name}] Done: {files_dropped} files, "
              f"{total_sent} records in {elapsed:.1f}s ({round_num} rounds)")

    async def _filedrop_all() -> None:
        shutdown_event = asyncio.Event()

        def _signal_handler() -> None:
            print("\nShutdown signal received, finishing current batch...")
            shutdown_event.set()

        ev_loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                ev_loop.add_signal_handler(sig, _signal_handler)
            except NotImplementedError:
                pass  # Windows — KeyboardInterrupt로 폴백

        tasks = [
            _filedrop_one(name, entry, shutdown_event)
            for name, entry in drop_targets
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for (name, _), result in zip(drop_targets, results):
            if isinstance(result, Exception):
                print(f"\n  [{name}] FAILED: {result}")
                if not args.skip_errors:
                    raise result

    try:
        asyncio.run(_filedrop_all())
    except KeyboardInterrupt:
        print("\nFileDrop stopped by user.")


def cmd_seed_test(args: argparse.Namespace) -> None:
    """DL 어댑터 통합 테스트 데이터 적재 — 공통 스키마 기반"""
    _import_adapters()
    _import_handlers()
    _import_generators()

    from demiurge_testdata.core.seed_test import SeedTestPipeline

    adapter_config: dict[str, Any] = _build_default_adapter_config()
    if args.adapter_config:
        with open(args.adapter_config) as f:
            overrides = yaml.safe_load(f) or {}
            adapter_config.update(overrides)

    targets = args.targets if args.targets else None

    async def _run() -> None:
        pipeline = SeedTestPipeline(adapter_configs=adapter_config)
        results = await pipeline.seed_all(
            targets=targets, drop_existing=args.drop
        )
        # 요약 출력
        total_records = sum(
            sum(counts.values()) for counts in results.values()
        )
        print(f"\n  seed-test 완료: {len(results)} targets, {total_records} total records")

    asyncio.run(_run())


def cmd_setup(args: argparse.Namespace) -> None:
    """download + seed 원스톱 실행"""
    # download
    args.skip_errors = True
    cmd_download(args)
    # seed
    cmd_seed(args)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="demiurge-testdata",
        description="Demiurge MLOps TestData — 생성·변환·적재 CLI",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # run
    run_parser = subparsers.add_parser("run", help="Run a pipeline from YAML config")
    run_parser.add_argument(
        "--config", "-c", required=True, type=Path, help="Pipeline YAML config path"
    )

    # list
    list_parser = subparsers.add_parser("list", help="List registered components")
    list_parser.add_argument(
        "target",
        nargs="?",
        default="all",
        choices=["all", "generators", "adapters", "formats", "compressions"],
        help="Component type to list",
    )

    # serve
    serve_parser = subparsers.add_parser("serve", help="Start REST API server")
    serve_parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    serve_parser.add_argument("--port", "-p", type=int, default=8000, help="Bind port")

    # ── 데이터 수집·시딩 명령 ──

    # download
    dl_parser = subparsers.add_parser("download", help="Download datasets from Kaggle")
    dl_parser.add_argument("datasets", nargs="*", help="Dataset names (default: all)")
    dl_parser.add_argument("--manifest", type=Path, default=None, help="Manifest YAML path")
    dl_parser.add_argument("--essential", action="store_true", help="Download essential datasets only")
    dl_parser.add_argument("--category", choices=["relational", "document", "event", "iot", "text", "geospatial"])
    dl_parser.add_argument("--skip-errors", action="store_true", help="Continue on download failure")

    # seed
    seed_parser = subparsers.add_parser("seed", help="Seed downloaded data into infrastructure")
    seed_parser.add_argument("datasets", nargs="*", help="Dataset names (default: all)")
    seed_parser.add_argument("--manifest", type=Path, default=None, help="Manifest YAML path")
    seed_parser.add_argument("--essential", action="store_true", help="Seed essential datasets only")
    seed_parser.add_argument("--category", choices=["relational", "document", "event", "iot", "text", "geospatial"])
    seed_parser.add_argument("--adapter-config", type=Path, default=None, help="Adapter config YAML")
    seed_parser.add_argument("--limit", type=int, default=None, help="Max records per dataset")
    seed_parser.add_argument("--drop", action="store_true", help="Drop existing tables before seeding")
    seed_parser.add_argument("--skip-errors", action="store_true", help="Continue on seed failure")

    # setup (download + seed)
    setup_parser = subparsers.add_parser("setup", help="Download + seed (one-stop)")
    setup_parser.add_argument("datasets", nargs="*", help="Dataset names (default: all)")
    setup_parser.add_argument("--manifest", type=Path, default=None, help="Manifest YAML path")
    setup_parser.add_argument("--essential", action="store_true", help="Essential datasets only")
    setup_parser.add_argument("--category", choices=["relational", "document", "event", "iot", "text", "geospatial"])
    setup_parser.add_argument("--adapter-config", type=Path, default=None, help="Adapter config YAML")
    setup_parser.add_argument("--limit", type=int, default=None, help="Max records per dataset")
    setup_parser.add_argument("--drop", action="store_true", help="Drop existing tables before seeding")

    # seed-test (DL 어댑터 통합 테스트 데이터)
    seed_test_parser = subparsers.add_parser(
        "seed-test", help="Seed adapter integration test data (common schema)")
    seed_test_parser.add_argument(
        "targets", nargs="*",
        help="Adapter targets (default: all). e.g. postgresql mysql kafka s3")
    seed_test_parser.add_argument(
        "--adapter-config", type=Path, default=None,
        help="Adapter config YAML override")
    seed_test_parser.add_argument(
        "--drop", action="store_true",
        help="Drop existing tables/indices before seeding")

    # ── 스트리밍·파일드롭 명령 ──

    # stream
    stream_parser = subparsers.add_parser(
        "stream", help="Real-time streaming simulation to Kafka/NATS/MQTT")
    stream_parser.add_argument("datasets", nargs="*", help="Dataset names (default: all streaming-eligible)")
    stream_parser.add_argument("--manifest", type=Path, default=None, help="Manifest YAML path")
    stream_parser.add_argument("--essential", action="store_true", help="Essential datasets only")
    stream_parser.add_argument("--interval", type=float, default=0.1, help="Send interval in seconds (default: 0.1)")
    stream_parser.add_argument("--batch-size", type=int, default=1, help="Records per send (default: 1)")
    stream_parser.add_argument("--loop", action="store_true", help="Loop infinitely over data")
    stream_parser.add_argument("--limit", type=int, default=None, help="Max total records to send")
    stream_parser.add_argument("--skip-errors", action="store_true", help="Continue on send failure")

    # filedrop
    filedrop_parser = subparsers.add_parser(
        "filedrop", help="File drop simulation to FTP/SFTP")
    filedrop_parser.add_argument("datasets", nargs="*", help="Dataset names (default: all)")
    filedrop_parser.add_argument("--manifest", type=Path, default=None, help="Manifest YAML path")
    filedrop_parser.add_argument("--essential", action="store_true", help="Essential datasets only")
    filedrop_parser.add_argument("--interval", type=float, default=60.0, help="Drop interval in seconds (default: 60)")
    filedrop_parser.add_argument("--records-per-file", type=int, default=1000, help="Records per file (default: 1000)")
    filedrop_parser.add_argument("--format", choices=["csv", "json", "parquet"], default="csv", help="File format (default: csv)")
    filedrop_parser.add_argument("--target", choices=["ftp", "sftp"], default="ftp", help="Upload target (default: ftp)")
    filedrop_parser.add_argument("--loop", action="store_true", help="Loop infinitely over data")
    filedrop_parser.add_argument("--limit", type=int, default=None, help="Max total records to send")
    filedrop_parser.add_argument("--skip-errors", action="store_true", help="Continue on upload failure")

    args = parser.parse_args()

    if args.command == "run":
        cmd_run(args)
    elif args.command == "list":
        cmd_list(args)
    elif args.command == "serve":
        cmd_serve(args)
    elif args.command == "download":
        cmd_download(args)
    elif args.command == "seed":
        cmd_seed(args)
    elif args.command == "setup":
        cmd_setup(args)
    elif args.command == "seed-test":
        cmd_seed_test(args)
    elif args.command == "stream":
        cmd_stream(args)
    elif args.command == "filedrop":
        cmd_filedrop(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
