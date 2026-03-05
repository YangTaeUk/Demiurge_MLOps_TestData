"""CLI 진입점 — python -m demiurge_testdata [command]"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import sys
from pathlib import Path


def _import_generators() -> None:
    """모든 제너레이터를 import하여 레지스트리에 등록"""
    from demiurge_testdata.generators import document, event, geospatial, iot, relational, text  # noqa: F401  # isort: skip


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
    from demiurge_testdata.core.registry import adapter_registry, generator_registry
    from demiurge_testdata.handlers.chain import HandlerChain

    async def _run() -> None:
        generator = generator_registry.create(config.generator.type, config=config.generator)
        handler_chain = HandlerChain(config.handler)
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

    args = parser.parse_args()

    if args.command == "run":
        cmd_run(args)
    elif args.command == "list":
        cmd_list(args)
    elif args.command == "serve":
        cmd_serve(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
