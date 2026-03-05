"""YAML → Pydantic 설정 로더"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from demiurge_testdata.core.enums import (
    CompressionType,
    FormatType,
    GenerationMode,
)
from demiurge_testdata.core.exceptions import ConfigError


class GeneratorConfig(BaseModel):
    """제너레이터 설정"""

    type: str
    batch_size: int = Field(default=1000, ge=1, le=1_000_000)
    max_records: int | None = None
    seed: int | None = None
    data_path: str | None = None
    shuffle: bool = True
    stream_interval_ms: int = Field(default=100, ge=1)
    validation_mode: str = Field(default="sample", pattern=r"^(strict|sample|skip)$")
    sample_size: int = Field(default=10, ge=1)


class HandlerConfig(BaseModel):
    """핸들러 설정"""

    format: FormatType = FormatType.JSON
    compression: CompressionType = CompressionType.NONE


class AdapterConfig(BaseModel):
    """어댑터 공통 설정"""

    type: str
    host: str = "localhost"
    port: int = 5432
    extra: dict[str, Any] = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    """파이프라인 전체 설정"""

    name: str = "default"
    mode: GenerationMode = GenerationMode.BATCH
    generator: GeneratorConfig
    handler: HandlerConfig = Field(default_factory=HandlerConfig)
    adapter: AdapterConfig


def load_config(path: str | Path) -> PipelineConfig:
    """YAML 파일에서 PipelineConfig를 로드한다.

    Args:
        path: YAML 설정 파일 경로

    Returns:
        검증된 PipelineConfig 인스턴스

    Raises:
        ConfigError: 파일 읽기 실패 또는 검증 실패
    """
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    try:
        with open(path) as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"YAML parse error in {path}: {e}") from e

    if not isinstance(raw, dict):
        raise ConfigError(f"Config must be a YAML mapping, got {type(raw).__name__}")

    try:
        return PipelineConfig.model_validate(raw)
    except Exception as e:
        raise ConfigError(f"Config validation error: {e}") from e
