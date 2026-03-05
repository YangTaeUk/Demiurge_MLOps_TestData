"""JSON FormatHandler — orjson 기반 고성능 JSON 직렬화"""

from __future__ import annotations

import asyncio

import orjson

from demiurge_testdata.core.registry import format_registry
from demiurge_testdata.handlers.base import BaseFormatHandler


@format_registry.register("json")
class JsonFormatHandler(BaseFormatHandler):
    """JSON 포맷 핸들러.

    orjson을 사용하여 stdlib json 대비 6-10x 성능을 제공한다.
    datetime, numpy 네이티브 지원.
    """

    @property
    def format_name(self) -> str:
        return "json"

    @property
    def content_type(self) -> str:
        return "application/json"

    @property
    def file_extension(self) -> str:
        return ".json"

    async def encode(self, records: list[dict]) -> bytes:
        return await asyncio.to_thread(orjson.dumps, records)

    async def decode(self, data: bytes) -> list[dict]:
        return await asyncio.to_thread(orjson.loads, data)
