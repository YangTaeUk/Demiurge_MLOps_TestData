"""Parquet FormatHandler — pyarrow 기반"""

from __future__ import annotations

import asyncio
import io

import pyarrow as pa
import pyarrow.parquet as pq

from demiurge_testdata.core.registry import format_registry
from demiurge_testdata.handlers.base import BaseFormatHandler


@format_registry.register("parquet")
class ParquetFormatHandler(BaseFormatHandler):
    """Parquet 포맷 핸들러.

    pyarrow를 사용하여 컬럼 기반 직렬화를 수행한다.
    대용량 분석 워크로드에 최적화.
    """

    @property
    def format_name(self) -> str:
        return "parquet"

    @property
    def content_type(self) -> str:
        return "application/vnd.apache.parquet"

    @property
    def file_extension(self) -> str:
        return ".parquet"

    async def encode(self, records: list[dict]) -> bytes:
        def _encode() -> bytes:
            table = pa.Table.from_pylist(records)
            buf = io.BytesIO()
            pq.write_table(table, buf)
            return buf.getvalue()

        return await asyncio.to_thread(_encode)

    async def decode(self, data: bytes) -> list[dict]:
        def _decode() -> list[dict]:
            buf = io.BytesIO(data)
            table = pq.read_table(buf)
            return table.to_pylist()

        return await asyncio.to_thread(_decode)
