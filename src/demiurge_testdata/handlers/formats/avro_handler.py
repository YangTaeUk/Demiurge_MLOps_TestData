"""Avro FormatHandler — fastavro 기반"""

from __future__ import annotations

import asyncio
import io

import fastavro

from demiurge_testdata.core.registry import format_registry
from demiurge_testdata.handlers.base import BaseFormatHandler


@format_registry.register("avro")
class AvroFormatHandler(BaseFormatHandler):
    """Avro 포맷 핸들러.

    fastavro를 사용하여 Avro 바이너리 직렬화를 수행한다.
    스키마를 지정하지 않으면 레코드에서 자동 추론한다.
    """

    def __init__(self, schema: dict | None = None):
        self._schema = schema

    @property
    def format_name(self) -> str:
        return "avro"

    @property
    def content_type(self) -> str:
        return "application/avro"

    @property
    def file_extension(self) -> str:
        return ".avro"

    async def encode(self, records: list[dict]) -> bytes:
        def _encode() -> bytes:
            schema = self._schema or self._infer_schema(records)
            parsed = fastavro.parse_schema(schema)
            buf = io.BytesIO()
            fastavro.writer(buf, parsed, records)
            return buf.getvalue()

        return await asyncio.to_thread(_encode)

    async def decode(self, data: bytes) -> list[dict]:
        def _decode() -> list[dict]:
            buf = io.BytesIO(data)
            reader = fastavro.reader(buf)
            return list(reader)

        return await asyncio.to_thread(_decode)

    @staticmethod
    def _infer_schema(records: list[dict]) -> dict:
        """레코드에서 Avro 스키마를 추론한다."""
        if not records:
            return {"type": "record", "name": "Record", "fields": []}
        fields = []
        for key, value in records[0].items():
            avro_type = "string"
            if isinstance(value, bool):
                avro_type = "boolean"
            elif isinstance(value, int):
                avro_type = "long"
            elif isinstance(value, float):
                avro_type = "double"
            fields.append({"name": key, "type": ["null", avro_type], "default": None})
        return {"type": "record", "name": "Record", "fields": fields}
