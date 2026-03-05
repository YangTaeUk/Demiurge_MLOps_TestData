"""HandlerChain 단위 테스트 — Format + Compression 합성"""

from demiurge_testdata.handlers.base import BaseCompressionHandler, BaseFormatHandler
from demiurge_testdata.handlers.chain import HandlerChain


class StubFormat(BaseFormatHandler):
    @property
    def format_name(self) -> str:
        return "stub_fmt"

    @property
    def content_type(self) -> str:
        return "application/stub"

    @property
    def file_extension(self) -> str:
        return ".stub"

    async def encode(self, records: list[dict]) -> bytes:
        return b"|".join(str(r).encode() for r in records)

    async def decode(self, data: bytes) -> list[dict]:
        return [{"raw": chunk.decode()} for chunk in data.split(b"|")]


class StubCompression(BaseCompressionHandler):
    @property
    def algorithm_name(self) -> str:
        return "stub_comp"

    @property
    def file_extension(self) -> str:
        return ".sc"

    async def compress(self, data: bytes) -> bytes:
        return b"C:" + data

    async def decompress(self, data: bytes) -> bytes:
        assert data.startswith(b"C:")
        return data[2:]


class TestHandlerChain:
    async def test_format_only(self):
        chain = HandlerChain(StubFormat())
        records = [{"a": 1}, {"b": 2}]

        encoded = await chain.encode(records)
        assert isinstance(encoded, bytes)
        assert len(encoded) > 0

    async def test_format_with_compression(self):
        chain = HandlerChain(StubFormat(), StubCompression())
        records = [{"x": 1}]

        encoded = await chain.encode(records)
        assert encoded.startswith(b"C:")

    async def test_encode_decode_roundtrip_no_compression(self):
        chain = HandlerChain(StubFormat())
        records = [{"a": 1}]

        encoded = await chain.encode(records)
        decoded = await chain.decode(encoded)
        assert len(decoded) == 1

    async def test_encode_decode_roundtrip_with_compression(self):
        chain = HandlerChain(StubFormat(), StubCompression())
        records = [{"a": 1}, {"b": 2}]

        encoded = await chain.encode(records)
        decoded = await chain.decode(encoded)
        assert len(decoded) == 2

    def test_properties_no_compression(self):
        chain = HandlerChain(StubFormat())
        assert chain.format_name == "stub_fmt"
        assert chain.compression_name == "none"
        assert chain.content_type == "application/stub"
        assert chain.file_extension == ".stub"

    def test_properties_with_compression(self):
        chain = HandlerChain(StubFormat(), StubCompression())
        assert chain.compression_name == "stub_comp"
        assert chain.file_extension == ".stub.sc"
