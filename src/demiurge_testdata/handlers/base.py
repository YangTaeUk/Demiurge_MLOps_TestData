"""BaseFormatHandler, BaseCompressionHandler ABC — 포맷/압축 핸들러 기본 인터페이스"""

from __future__ import annotations

from abc import ABC, abstractmethod


class BaseFormatHandler(ABC):
    """포맷 핸들러 기본 인터페이스.

    dict ↔ bytes 변환만 담당한다.
    """

    @property
    @abstractmethod
    def format_name(self) -> str:
        """포맷 식별자 (예: 'json', 'parquet')"""

    @property
    @abstractmethod
    def content_type(self) -> str:
        """MIME Content-Type (예: 'application/json')"""

    @property
    @abstractmethod
    def file_extension(self) -> str:
        """파일 확장자 (예: '.json')"""

    @abstractmethod
    async def encode(self, records: list[dict]) -> bytes:
        """list[dict] → bytes 직렬화"""

    @abstractmethod
    async def decode(self, data: bytes) -> list[dict]:
        """bytes → list[dict] 역직렬화"""


class BaseCompressionHandler(ABC):
    """압축 핸들러 기본 인터페이스.

    bytes ↔ bytes 변환만 담당한다.
    """

    @property
    @abstractmethod
    def algorithm_name(self) -> str:
        """알고리즘 식별자 (예: 'gzip', 'lz4')"""

    @property
    @abstractmethod
    def file_extension(self) -> str:
        """파일 확장자 (예: '.gz')"""

    @abstractmethod
    async def compress(self, data: bytes) -> bytes:
        """bytes → compressed bytes"""

    @abstractmethod
    async def decompress(self, data: bytes) -> bytes:
        """compressed bytes → bytes"""
