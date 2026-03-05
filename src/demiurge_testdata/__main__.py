"""CLI 진입점 — python -m demiurge_testdata [command]"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import sys
from pathlib import Path
from typing import Any

import yaml


def _import_generators() -> None:
    """모든 제너레이터를 import하여 레지스트리에 등록"""
    from demiurge_testdata.generators import document, event, geospatial, iot, relational, text  # noqa: F401  # isort: skip


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
        "demiurge_testdata.handlers.compression.cramjam_handler",
    ]
    for mod in handler_modules:
        with contextlib.suppress(ImportError):
            importlib.import_module(mod)


def _import_adapters() -> None:
    """사용 가능한 어댑터를 import하여 레지스트리에 등록"""
    import importlib

    adapter_modules = [
        "demiurge_testdata.adapters.rdbms.postgresql",
        "demiurge_testdata.adapters.rdbms.mysql",
        "demiurge_testdata.adapters.rdbms.sqlite",
        "demiurge_testdata.adapters.nosql.mongodb",
        "demiurge_testdata.adapters.nosql.elasticsearch",
        "demiurge_testdata.adapters.nosql.redis",
        "demiurge_testdata.adapters.streaming.kafka",
        "demiurge_testdata.adapters.streaming.rabbitmq",
        "demiurge_testdata.adapters.streaming.nats",
        "demiurge_testdata.adapters.storage.s3",
        "demiurge_testdata.adapters.storage.local_fs",
        "demiurge_testdata.adapters.storage.hdfs",
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
    from demiurge_testdata.core.seed import SeedPipeline, load_csv_records
    from demiurge_testdata.handlers.chain import HandlerChain

    manifest = _load_manifest(args.manifest)

    targets = args.datasets or list(manifest.keys())
    if args.essential:
        targets = [d for d in _ESSENTIAL_DATASETS if d in manifest]
    if args.category:
        targets = [d for d in targets if manifest[d].get("category") == args.category]

    # 어댑터 설정 로드 (.env 기본값 → 파일 오버라이드)
    _default_config: dict[str, Any] = {
        "postgresql": {"host": "localhost", "port": int(os.environ.get("PG_PORT", 5434)),
                       "user": "testdata", "password": "testdata_dev", "database": "testdata"},
        "mongodb": {"host": "localhost", "port": int(os.environ.get("MONGO_PORT", 27017)),
                    "username": "testdata", "password": "testdata_dev", "database": "testdata"},
        "kafka": {"bootstrap_servers": f"localhost:{os.environ.get('KAFKA_PORT', 9092)}"},
        "s3": {"endpoint_url": f"http://localhost:{os.environ.get('MINIO_API_PORT', 9002)}",
               "access_key": os.environ.get("MINIO_ROOT_USER", "testdata"),
               "secret_key": os.environ.get("MINIO_ROOT_PASSWORD", "testdata_dev_password"),
               "bucket": "testdata"},
    }
    adapter_config: dict[str, Any] = dict(_default_config)
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
                records = load_csv_records(csv_path, limit=args.limit)
                category = entry.get("category", "relational")

                if seed_target in ("postgresql", "mysql", "sqlite", "bigquery"):
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
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
