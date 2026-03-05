"""EnronEmailGenerator (E2) — Enron Email Dataset 제너레이터

이메일 파싱, 헤더/본문 분리, Elasticsearch 인덱싱.
"""

from __future__ import annotations

import re
from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator


@generator_registry.register("enron_email")
class EnronEmailGenerator(CsvGenerator):
    """Enron Email Dataset 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "enron_email"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.TEXT

    @property
    def _csv_files(self) -> list[str]:
        return ["emails.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        # 이메일 메시지에서 헤더 파싱
        message = transformed.get("message", "")
        if isinstance(message, str) and message:
            # From 추출
            m = re.search(r"^From:\s*(.+)$", message, re.MULTILINE)
            if m:
                transformed["from_addr"] = m.group(1).strip()

            # To 추출
            m = re.search(r"^To:\s*(.+)$", message, re.MULTILINE)
            if m:
                transformed["to_addr"] = m.group(1).strip()

            # Subject 추출
            m = re.search(r"^Subject:\s*(.*)$", message, re.MULTILINE)
            if m:
                transformed["subject"] = m.group(1).strip()

            # Date 추출
            m = re.search(r"^Date:\s*(.+)$", message, re.MULTILINE)
            if m:
                transformed["date"] = m.group(1).strip()

            # 본문 분리 (빈 줄 이후)
            parts = re.split(r"\n\n", message, maxsplit=1)
            if len(parts) > 1:
                transformed["body"] = parts[1].strip()

        return transformed
