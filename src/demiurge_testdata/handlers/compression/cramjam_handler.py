"""CramjamCompressionHandler — cramjam 기반 통합 압축 핸들러

6개 압축 알고리즘을 1개 파라미터화 클래스로 통합한다:
gzip, brotli, snappy, lz4, zstd, lzma
"""

from __future__ import annotations

import asyncio
from functools import partial

import cramjam

from demiurge_testdata.core.registry import compression_registry
from demiurge_testdata.handlers.base import BaseCompressionHandler

_ALGORITHMS: dict[str, tuple[object, str]] = {
    "gzip": (cramjam.gzip, ".gz"),
    "brotli": (cramjam.brotli, ".br"),
    "snappy": (cramjam.snappy, ".snappy"),
    "lz4": (cramjam.lz4, ".lz4"),
    "zstd": (cramjam.zstd, ".zst"),
    "lzma": (cramjam.xz, ".lzma"),
}


class CramjamCompressionHandler(BaseCompressionHandler):
    """cramjam 기반 통합 압축 핸들러.

    6종 압축 알고리즘을 단일 클래스로 지원한다.
    Rust 네이티브 구현으로 시스템 C 의존성이 불필요하다.
    """

    def __init__(self, algorithm: str):
        if algorithm not in _ALGORITHMS:
            raise ValueError(
                f"Unknown compression algorithm: '{algorithm}'. "
                f"Available: {list(_ALGORITHMS.keys())}"
            )
        self._algorithm = algorithm
        self._module, self._ext = _ALGORITHMS[algorithm]

    @property
    def algorithm_name(self) -> str:
        return self._algorithm

    @property
    def file_extension(self) -> str:
        return self._ext

    async def compress(self, data: bytes) -> bytes:
        return await asyncio.to_thread(lambda: bytes(self._module.compress(data)))

    async def decompress(self, data: bytes) -> bytes:
        return await asyncio.to_thread(lambda: bytes(self._module.decompress(data)))


# Registry 등록: 6종 알고리즘을 for-loop으로 일괄 등록
for _algo in _ALGORITHMS:
    compression_registry.register_class(_algo, partial(CramjamCompressionHandler, _algo))
