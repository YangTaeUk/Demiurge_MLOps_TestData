"""HandlerChain — Format + Compression 합성기"""

from __future__ import annotations

from demiurge_testdata.handlers.base import BaseCompressionHandler, BaseFormatHandler


class HandlerChain:
    """Format + Compression 합성.

    10종 포맷 x 7종 압축(none 포함) = 70가지 조합을 지원한다.
    HandlerChain은 Format → bytes → Compression 순서로 처리하므로,
    포맷과 압축 간 의존성이 없다.
    """

    def __init__(
        self,
        format_handler: BaseFormatHandler,
        compression_handler: BaseCompressionHandler | None = None,
    ):
        self._format = format_handler
        self._compression = compression_handler

    @property
    def format_name(self) -> str:
        return self._format.format_name

    @property
    def compression_name(self) -> str:
        if self._compression is None:
            return "none"
        return self._compression.algorithm_name

    @property
    def content_type(self) -> str:
        return self._format.content_type

    @property
    def file_extension(self) -> str:
        ext = self._format.file_extension
        if self._compression is not None:
            ext += self._compression.file_extension
        return ext

    async def encode(self, records: list[dict]) -> bytes:
        """list[dict] → 직렬화 → 압축 → bytes"""
        raw = await self._format.encode(records)
        if self._compression:
            return await self._compression.compress(raw)
        return raw

    async def decode(self, data: bytes) -> list[dict]:
        """bytes → 해제 → 역직렬화 → list[dict]"""
        if self._compression:
            data = await self._compression.decompress(data)
        return await self._format.decode(data)
