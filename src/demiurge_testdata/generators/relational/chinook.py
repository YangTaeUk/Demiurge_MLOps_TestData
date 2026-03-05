"""ChinookGenerator (A6) — Chinook Database 제너레이터

11테이블 ER, 경량 테스트, SQLite 원본 지원.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("chinook")
class ChinookGenerator(CsvGenerator):
    """Chinook Database 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "chinook"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.RELATIONAL

    @property
    def _csv_files(self) -> list[str]:
        return ["invoices.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        int_keys = ("InvoiceId", "CustomerId")
        for key in int_keys:
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        val = safe_float(transformed.get("Total"))
        if val is not None:
            transformed["Total"] = val

        return transformed
