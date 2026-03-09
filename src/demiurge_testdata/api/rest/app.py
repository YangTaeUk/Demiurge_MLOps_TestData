"""FastAPI REST 서버 — 파이프라인 실행 및 제너레이터/어댑터 조회 API"""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from demiurge_testdata.core.registry import (
    adapter_registry,
    compression_registry,
    format_registry,
    generator_registry,
)

app = FastAPI(
    title="Demiurge TestData API",
    version="0.1.0",
    description="MLOps 테스트 데이터 생성·변환·적재 REST API",
)


# ── Request/Response Models ──


class PipelineRequest(BaseModel):
    """파이프라인 실행 요청"""

    generator: str
    adapter: str
    mode: str = Field(default="batch", pattern=r"^(batch|stream)$")
    batch_size: int = Field(default=1000, ge=1, le=1_000_000)
    format: str = "json"
    compression: str = "none"
    generator_config: dict[str, Any] = Field(default_factory=dict)
    adapter_config: dict[str, Any] = Field(default_factory=dict)


class PipelineResponse(BaseModel):
    """파이프라인 실행 결과"""

    status: str
    total_records: int = 0
    total_bytes: int = 0
    elapsed_seconds: float = 0.0
    records_per_second: float = 0.0
    errors: list[str] = Field(default_factory=list)


class GeneratorInfo(BaseModel):
    """제너레이터 정보"""

    key: str
    class_name: str


class AdapterInfo(BaseModel):
    """어댑터 정보"""

    key: str
    class_name: str


# ── Health ──


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


# ── Generators ──


@app.get("/generators", response_model=list[GeneratorInfo])
async def list_generators() -> list[dict[str, str]]:
    """등록된 제너레이터 목록 조회"""
    return [
        {"key": k, "class_name": generator_registry.get_class(k).__name__}
        for k in generator_registry.list_registered()
    ]


@app.get("/generators/{key}")
async def get_generator(key: str) -> dict[str, str]:
    """특정 제너레이터 정보 조회"""
    if key not in generator_registry:
        raise HTTPException(status_code=404, detail=f"Generator '{key}' not found")
    cls = generator_registry.get_class(key)
    return {"key": key, "class_name": cls.__name__, "module": cls.__module__}


# ── Adapters ──


@app.get("/adapters", response_model=list[AdapterInfo])
async def list_adapters() -> list[dict[str, str]]:
    """등록된 어댑터 목록 조회"""
    return [
        {"key": k, "class_name": adapter_registry.get_class(k).__name__}
        for k in adapter_registry.list_registered()
    ]


@app.get("/adapters/{key}")
async def get_adapter(key: str) -> dict[str, str]:
    """특정 어댑터 정보 조회"""
    if key not in adapter_registry:
        raise HTTPException(status_code=404, detail=f"Adapter '{key}' not found")
    cls = adapter_registry.get_class(key)
    return {"key": key, "class_name": cls.__name__, "module": cls.__module__}


# ── Pipeline ──


@app.post("/pipeline/run", response_model=PipelineResponse)
async def run_pipeline(request: PipelineRequest) -> PipelineResponse:
    """파이프라인 실행 트리거"""
    from demiurge_testdata.core.config import GeneratorConfig, HandlerConfig
    from demiurge_testdata.core.pipeline import DataPipeline
    from demiurge_testdata.handlers.chain import HandlerChain

    # Validate generator/adapter keys
    if request.generator not in generator_registry:
        raise HTTPException(status_code=404, detail=f"Generator '{request.generator}' not found")
    if request.adapter not in adapter_registry:
        raise HTTPException(status_code=404, detail=f"Adapter '{request.adapter}' not found")

    try:
        # Build generator
        gen_config = GeneratorConfig(
            type=request.generator,
            batch_size=request.batch_size,
            **request.generator_config,
        )
        generator = generator_registry.create(request.generator, config=gen_config)

        # Build handler chain
        handler_config = HandlerConfig(
            format=request.format,
            compression=request.compression,
        )
        format_handler = format_registry.create(handler_config.format)
        compression_handler = (
            compression_registry.create(handler_config.compression)
            if handler_config.compression != "none"
            else None
        )
        handler_chain = HandlerChain(
            format_handler=format_handler,
            compression_handler=compression_handler,
        )

        # Build adapter
        adapter = adapter_registry.create(request.adapter, **request.adapter_config)

        # Run pipeline
        pipeline = DataPipeline(
            generator=generator,
            handler_chain=handler_chain,
            adapter=adapter,
            batch_size=request.batch_size,
        )

        async with adapter:
            if request.mode == "batch":
                metrics = await pipeline.run_batch()
            else:
                metrics = await pipeline.run_stream()

        return PipelineResponse(
            status="success",
            total_records=metrics.total_records,
            total_bytes=metrics.total_bytes,
            elapsed_seconds=metrics.elapsed_seconds,
            records_per_second=metrics.records_per_second,
            errors=metrics.errors,
        )

    except Exception as e:
        return PipelineResponse(
            status="error",
            errors=[str(e)],
        )


# ── Test Data Endpoint (DL 어댑터 통합 테스트용) ──

_bearer = HTTPBearer()
_TEST_DATA_TOKEN = os.environ.get("TEST_DATA_TOKEN", "test-bearer-token")


class TestDataResponse(BaseModel):
    """공통 스키마 테스트 데이터 응답"""

    total: int
    offset: int
    limit: int
    data: list[dict[str, Any]]


def _verify_token(credentials: HTTPAuthorizationCredentials = Security(_bearer)) -> str:
    if credentials.credentials != _TEST_DATA_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid bearer token")
    return credentials.credentials


@app.get("/api/test/data", response_model=TestDataResponse)
async def get_test_data(
    offset: int = Query(default=0, ge=0, description="시작 위치"),
    limit: int = Query(default=50, ge=1, le=500, description="반환 건수 (최대 500)"),
    _token: str = Security(_verify_token),
) -> TestDataResponse:
    """공통 스키마(6컬럼) 테스트 데이터를 페이지네이션으로 반환한다.

    Bearer 토큰 인증 필요 (기본값: test-bearer-token, 환경변수 TEST_DATA_TOKEN으로 변경 가능).
    """
    from demiurge_testdata.generators.adapter_test import generate_records

    total = 200
    records = generate_records(total, seed=42)
    page = records[offset : offset + limit]

    return TestDataResponse(
        total=total,
        offset=offset,
        limit=limit,
        data=page,
    )
