"""XML FormatHandler — lxml 기반"""

from __future__ import annotations

import asyncio

from lxml import etree

from demiurge_testdata.core.registry import format_registry
from demiurge_testdata.handlers.base import BaseFormatHandler


@format_registry.register("xml")
class XmlFormatHandler(BaseFormatHandler):
    """XML 포맷 핸들러.

    lxml을 사용하여 고성능 XML 직렬화를 수행한다.
    root_tag, item_tag를 커스터마이징 가능.
    """

    def __init__(self, root_tag: str = "records", item_tag: str = "record"):
        self._root_tag = root_tag
        self._item_tag = item_tag

    @property
    def format_name(self) -> str:
        return "xml"

    @property
    def content_type(self) -> str:
        return "application/xml"

    @property
    def file_extension(self) -> str:
        return ".xml"

    async def encode(self, records: list[dict]) -> bytes:
        def _encode() -> bytes:
            root = etree.Element(self._root_tag)
            for record in records:
                item = etree.SubElement(root, self._item_tag)
                for key, value in record.items():
                    field = etree.SubElement(item, key)
                    field.text = str(value) if value is not None else ""
            return etree.tostring(root, xml_declaration=True, encoding="utf-8", pretty_print=True)

        return await asyncio.to_thread(_encode)

    async def decode(self, data: bytes) -> list[dict]:
        def _decode() -> list[dict]:
            root = etree.fromstring(data)
            records = []
            for item in root:
                record = {}
                for field in item:
                    record[field.tag] = field.text or ""
                records.append(record)
            return records

        return await asyncio.to_thread(_decode)
