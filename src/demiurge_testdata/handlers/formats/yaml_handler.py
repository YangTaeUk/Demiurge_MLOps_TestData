"""YAML FormatHandler — pyyaml 기반"""

from __future__ import annotations

import asyncio

import yaml

from demiurge_testdata.core.registry import format_registry
from demiurge_testdata.handlers.base import BaseFormatHandler


@format_registry.register("yaml")
class YamlFormatHandler(BaseFormatHandler):
    """YAML 포맷 핸들러.

    pyyaml을 사용한다. decode 시 safe_load로 안전하게 역직렬화한다.
    """

    @property
    def format_name(self) -> str:
        return "yaml"

    @property
    def content_type(self) -> str:
        return "text/yaml"

    @property
    def file_extension(self) -> str:
        return ".yaml"

    async def encode(self, records: list[dict]) -> bytes:
        def _encode() -> bytes:
            return yaml.dump(records, allow_unicode=True, default_flow_style=False).encode("utf-8")

        return await asyncio.to_thread(_encode)

    async def decode(self, data: bytes) -> list[dict]:
        return await asyncio.to_thread(yaml.safe_load, data.decode("utf-8"))
