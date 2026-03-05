"""ORC FormatHandler — pyarrow.orc 기반"""

from __future__ import annotations

import asyncio
import io

import pyarrow as pa
import pyarrow.orc as orc

from demiurge_testdata.core.registry import format_registry
from demiurge_testdata.handlers.base import BaseFormatHandler


@format_registry.register("orc")
class OrcFormatHandler(BaseFormatHandler):
    """ORC 포맷 핸들러.

    pyarrow.orc를 사용하여 ORC 컬럼 기반 직렬화를 수행한다.
    Hive/Spark 에코시스템과 호환.
    """

    @property
    def format_name(self) -> str:
        return "orc"

    @property
    def content_type(self) -> str:
        return "application/x-orc"

    @property
    def file_extension(self) -> str:
        return ".orc"

    async def encode(self, records: list[dict]) -> bytes:
        def _encode() -> bytes:
            table = pa.Table.from_pylist(records)
            buf = io.BytesIO()
            orc.write_table(table, buf)
            return buf.getvalue()

        return await asyncio.to_thread(_encode)

    async def decode(self, data: bytes) -> list[dict]:
        def _decode() -> list[dict]:
            buf = io.BytesIO(data)
            table = orc.read_table(buf)
            return table.to_pylist()

        return await asyncio.to_thread(_decode)
