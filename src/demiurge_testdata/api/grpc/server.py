"""gRPC 서버 구현 — TestDataService

protobuf 코드 생성 없이 grpc reflection으로 동작하는 경량 서버.
실제 사용 시에는 `grpc_tools.protoc`로 stub을 생성해야 한다.

이 모듈은 gRPC 서버의 진입점 역할을 하며,
protobuf 코드가 생성된 후에 완전히 동작한다.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import grpc
from grpc import aio as grpc_aio

from demiurge_testdata.core.registry import adapter_registry, generator_registry


class TestDataServicer:
    """gRPC TestData 서비스 구현.

    protobuf 생성 코드 없이도 동작하도록
    JSON 직렬화 기반 간이 구현을 제공한다.
    """

    async def GenerateBatch(self, request: Any, context: Any) -> dict[str, Any]:
        """배치 데이터 생성"""
        from demiurge_testdata.core.config import GeneratorConfig

        generator_key = request.generator_key
        batch_size = request.batch_size or 100

        if generator_key not in generator_registry:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Generator '{generator_key}' not found")
            return {}

        config = GeneratorConfig(type=generator_key, batch_size=batch_size)
        gen = generator_registry.create(generator_key, config=config)
        records = await gen.batch(batch_size)

        data = json.dumps(records, default=str).encode("utf-8")
        return {
            "data": data,
            "record_count": len(records),
            "format": request.format or "json",
            "compression": "none",
        }

    async def ListGenerators(self, request: Any, context: Any) -> dict[str, Any]:
        """등록된 제너레이터 목록"""
        generators = []
        for key in generator_registry.list_registered():
            cls = generator_registry.get_class(key)
            generators.append({"key": key, "class_name": cls.__name__, "module": cls.__module__})
        return {"generators": generators}

    async def ListAdapters(self, request: Any, context: Any) -> dict[str, Any]:
        """등록된 어댑터 목록"""
        adapters = []
        for key in adapter_registry.list_registered():
            cls = adapter_registry.get_class(key)
            adapters.append({"key": key, "class_name": cls.__name__, "module": cls.__module__})
        return {"adapters": adapters}

    async def HealthCheck(self, request: Any, context: Any) -> dict[str, Any]:
        """헬스 체크"""
        return {"healthy": True, "version": "0.1.0"}


async def serve(port: int = 50051) -> None:
    """gRPC 서버 시작"""
    server = grpc_aio.server()
    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
