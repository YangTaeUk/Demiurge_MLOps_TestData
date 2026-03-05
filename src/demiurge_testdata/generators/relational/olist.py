"""OlistGenerator (A2) — Brazilian E-Commerce 제너레이터

스타 스키마: Dimension 선적재 → Fact 후적재, UTF-8 다국어 보존.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("olist")
class OlistGenerator(CsvGenerator):
    """Brazilian E-Commerce (Olist) 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "olist"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.RELATIONAL

    @property
    def _csv_files(self) -> list[str]:
        return ["olist_orders_dataset.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)
        float_keys = ("price", "freight_value", "payment_value")
        int_keys = ("order_item_id", "payment_sequential", "payment_installments")
        for key, value in transformed.items():
            if value in ("", None):
                transformed[key] = None
            elif key in float_keys:
                transformed[key] = safe_float(value) or value
            elif key in int_keys:
                transformed[key] = safe_int(value) or value
        return transformed
