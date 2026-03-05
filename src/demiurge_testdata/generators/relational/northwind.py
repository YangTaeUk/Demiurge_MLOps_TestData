"""NorthwindGenerator (A8) — Northwind Traders 제너레이터

14테이블 M:N 관계, 토폴로지 정렬 적재, 멀티 RDBMS.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("northwind")
class NorthwindGenerator(CsvGenerator):
    """Northwind Traders 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "northwind"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.RELATIONAL

    @property
    def _csv_files(self) -> list[str]:
        return ["orders.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        for key in ("OrderID", "CustomerID", "EmployeeID"):
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        val = safe_float(transformed.get("Freight"))
        if val is not None:
            transformed["Freight"] = val

        return transformed
