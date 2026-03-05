"""Arrow IPC FormatHandler — pyarrow 기반"""

from __future__ import annotations

import asyncio

import pyarrow as pa

from demiurge_testdata.core.registry import format_registry
from demiurge_testdata.handlers.base import BaseFormatHandler


@format_registry.register("arrow")
class ArrowFormatHandler(BaseFormatHandler):
    """Arrow IPC 포맷 핸들러.

    pyarrow IPC Streaming 포맷으로 직렬화한다.
    Arrow 네이티브 포맷으로 제로카피 역직렬화 가능.
    """

    @property
    def format_name(self) -> str:
        return "arrow"

    @property
    def content_type(self) -> str:
        return "application/vnd.apache.arrow.stream"

    @property
    def file_extension(self) -> str:
        return ".arrow"

    async def encode(self, records: list[dict]) -> bytes:
        def _encode() -> bytes:
            table = pa.Table.from_pylist(records)
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, table.schema)
            writer.write_table(table)
            writer.close()
            return sink.getvalue().to_pybytes()

        return await asyncio.to_thread(_encode)

    async def decode(self, data: bytes) -> list[dict]:
        def _decode() -> list[dict]:
            reader = pa.ipc.open_stream(data)
            table = reader.read_all()
            return table.to_pylist()

        return await asyncio.to_thread(_decode)
