"""DataRecord — 파이프라인 내부에서 사용되는 범용 데이터 레코드"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DataRecord(BaseModel):
    """Generator가 생성하고 HandlerChain이 인코딩하는 단위 레코드.

    Generator → list[DataRecord] → HandlerChain.encode() → bytes
    """

    data: dict[str, Any]
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": True}
