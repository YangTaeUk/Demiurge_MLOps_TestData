"""CramjamCompressionHandler 단위 테스트 — 6종 알고리즘 검증"""

from __future__ import annotations

import pytest

cramjam = pytest.importorskip("cramjam")

from demiurge_testdata.handlers.compression.cramjam_handler import (  # noqa: E402
    CramjamCompressionHandler,
)

ALGORITHMS = ["gzip", "brotli", "snappy", "lz4", "zstd", "lzma"]
SAMPLE_DATA = b"Hello, World! " * 100  # 반복 데이터로 압축 효과 확인


class TestCramjamCompressionHandler:
    @pytest.mark.parametrize("algo", ALGORITHMS)
    async def test_compress_returns_bytes(self, algo: str):
        handler = CramjamCompressionHandler(algo)
        compressed = await handler.compress(SAMPLE_DATA)
        assert isinstance(compressed, bytes)
        assert len(compressed) > 0

    @pytest.mark.parametrize("algo", ALGORITHMS)
    async def test_roundtrip(self, algo: str):
        handler = CramjamCompressionHandler(algo)
        compressed = await handler.compress(SAMPLE_DATA)
        decompressed = await handler.decompress(compressed)
        assert decompressed == SAMPLE_DATA

    @pytest.mark.parametrize("algo", ALGORITHMS)
    async def test_compression_reduces_size(self, algo: str):
        handler = CramjamCompressionHandler(algo)
        compressed = await handler.compress(SAMPLE_DATA)
        # 반복 데이터이므로 압축 후 크기가 줄어야 한다
        assert len(compressed) < len(SAMPLE_DATA)

    @pytest.mark.parametrize("algo", ALGORITHMS)
    async def test_empty_data(self, algo: str):
        handler = CramjamCompressionHandler(algo)
        compressed = await handler.compress(b"")
        decompressed = await handler.decompress(compressed)
        assert decompressed == b""

    @pytest.mark.parametrize(
        "algo,ext",
        [
            ("gzip", ".gz"),
            ("brotli", ".br"),
            ("snappy", ".snappy"),
            ("lz4", ".lz4"),
            ("zstd", ".zst"),
            ("lzma", ".lzma"),
        ],
    )
    def test_properties(self, algo: str, ext: str):
        handler = CramjamCompressionHandler(algo)
        assert handler.algorithm_name == algo
        assert handler.file_extension == ext

    def test_unknown_algorithm_raises(self):
        with pytest.raises(ValueError, match="Unknown compression algorithm"):
            CramjamCompressionHandler("unknown")


class TestCompressionRegistry:
    def test_all_algorithms_registered(self):
        from demiurge_testdata.core.registry import compression_registry

        for algo in ALGORITHMS:
            assert algo in compression_registry

    def test_registry_create(self):
        from demiurge_testdata.core.registry import compression_registry

        handler = compression_registry.create("gzip")
        assert isinstance(handler, CramjamCompressionHandler)
        assert handler.algorithm_name == "gzip"

    @pytest.mark.parametrize("algo", ALGORITHMS)
    async def test_registry_roundtrip(self, algo: str):
        from demiurge_testdata.core.registry import compression_registry

        handler = compression_registry.create(algo)
        compressed = await handler.compress(SAMPLE_DATA)
        decompressed = await handler.decompress(compressed)
        assert decompressed == SAMPLE_DATA
