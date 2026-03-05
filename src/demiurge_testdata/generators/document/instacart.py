"""InstacartGenerator (B1) — Instacart Market Basket Analysis 제너레이터

관계형 CSV → 중첩 JSON 문서 변환, 주문-상품 Denormalization.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("instacart")
class InstacartGenerator(CsvGenerator):
    """Instacart Market Basket Analysis 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "instacart"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.DOCUMENT

    @property
    def _csv_files(self) -> list[str]:
        return ["order_products__prior.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)
        int_fields = (
            "order_id",
            "user_id",
            "product_id",
            "add_to_cart_order",
            "reordered",
            "order_number",
            "order_dow",
            "order_hour_of_day",
            "aisle_id",
            "department_id",
        )
        for key in int_fields:
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        for key in ("days_since_prior_order",):
            if key in transformed:
                val = safe_float(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
