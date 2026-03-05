"""CSV FormatHandler — stdlib csv 기반"""

from __future__ import annotations

import asyncio
import csv
import io

from demiurge_testdata.core.registry import format_registry
from demiurge_testdata.handlers.base import BaseFormatHandler


@format_registry.register("csv")
class CsvFormatHandler(BaseFormatHandler):
    """CSV 포맷 핸들러.

    stdlib csv를 사용한다. 모든 값은 문자열로 직렬화된다.
    """

    @property
    def format_name(self) -> str:
        return "csv"

    @property
    def content_type(self) -> str:
        return "text/csv"

    @property
    def file_extension(self) -> str:
        return ".csv"

    async def encode(self, records: list[dict]) -> bytes:
        def _encode() -> bytes:
            if not records:
                return b""
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=list(records[0].keys()))
            writer.writeheader()
            writer.writerows(records)
            return output.getvalue().encode("utf-8")

        return await asyncio.to_thread(_encode)

    async def decode(self, data: bytes) -> list[dict]:
        def _decode() -> list[dict]:
            reader = csv.DictReader(io.StringIO(data.decode("utf-8")))
            return list(reader)

        return await asyncio.to_thread(_decode)
