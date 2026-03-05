"""BaseGenerator ABC — 32종 데이터셋 제너레이터의 기본 인터페이스"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from demiurge_testdata.core.enums import DatasetCategory, GenerationMode


class BaseGenerator(ABC):
    """데이터셋 제너레이터 기본 인터페이스.

    모든 제너레이터는 3가지 모드(Batch, Stream, API)를 지원한다.
    """

    @property
    @abstractmethod
    def dataset_name(self) -> str:
        """데이터셋 식별자 (예: 'home_credit', 'olist')"""

    @property
    @abstractmethod
    def category(self) -> DatasetCategory:
        """데이터셋 카테고리 (relational, document, event, iot, text, geospatial)"""

    @abstractmethod
    async def batch(self, batch_size: int = 1000) -> list[dict]:
        """Batch 모드: 지정된 크기의 레코드 배치를 반환한다.

        Args:
            batch_size: 반환할 레코드 수

        Returns:
            dict 리스트 (각 dict는 하나의 레코드)
        """

    @abstractmethod
    async def stream(self) -> AsyncIterator[dict]:
        """Stream 모드: 레코드를 비동기 이터레이터로 반환한다.

        Yields:
            개별 레코드 dict
        """
        yield {}  # pragma: no cover — abstract

    async def fetch(self, offset: int = 0, limit: int = 100) -> list[dict]:
        """API 모드: offset/limit 기반 페이지네이션.

        Args:
            offset: 시작 위치
            limit: 반환할 최대 레코드 수

        Returns:
            dict 리스트
        """
        records = await self.batch(batch_size=offset + limit)
        return records[offset : offset + limit]

    @property
    def supported_modes(self) -> list[GenerationMode]:
        """지원하는 생성 모드 목록"""
        return [GenerationMode.BATCH, GenerationMode.STREAM, GenerationMode.API]
