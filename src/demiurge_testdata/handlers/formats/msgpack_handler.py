"""MessagePack FormatHandler — msgspec 기반"""

from __future__ import annotations

import asyncio

import msgspec

from demiurge_testdata.core.registry import format_registry
from demiurge_testdata.handlers.base import BaseFormatHandler


@format_registry.register("msgpack")
class MsgpackFormatHandler(BaseFormatHandler):
    """MessagePack 포맷 핸들러.

    msgspec를 사용하여 stdlib msgpack 대비 5-10x 성능을 제공한다.
    바이너리 직렬화로 JSON 대비 작은 페이로드.
    """

    def __init__(self):
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder(type=list)

    @property
    def format_name(self) -> str:
        return "msgpack"

    @property
    def content_type(self) -> str:
        return "application/x-msgpack"

    @property
    def file_extension(self) -> str:
        return ".msgpack"

    async def encode(self, records: list[dict]) -> bytes:
        return await asyncio.to_thread(self._encoder.encode, records)

    async def decode(self, data: bytes) -> list[dict]:
        return await asyncio.to_thread(self._decoder.decode, data)
