"""JSONL (JSON Lines) FormatHandler — orjson 기반"""

from __future__ import annotations

import asyncio

import orjson

from demiurge_testdata.core.registry import format_registry
from demiurge_testdata.handlers.base import BaseFormatHandler


@format_registry.register("jsonl")
class JsonlFormatHandler(BaseFormatHandler):
    """JSONL 포맷 핸들러.

    각 레코드를 개별 JSON 라인으로 직렬화한다.
    스트리밍 처리에 최적화된 포맷.
    """

    @property
    def format_name(self) -> str:
        return "jsonl"

    @property
    def content_type(self) -> str:
        return "application/x-ndjson"

    @property
    def file_extension(self) -> str:
        return ".jsonl"

    async def encode(self, records: list[dict]) -> bytes:
        def _encode() -> bytes:
            return b"\n".join(orjson.dumps(r) for r in records)

        return await asyncio.to_thread(_encode)

    async def decode(self, data: bytes) -> list[dict]:
        def _decode() -> list[dict]:
            return [orjson.loads(line) for line in data.split(b"\n") if line.strip()]

        return await asyncio.to_thread(_decode)
