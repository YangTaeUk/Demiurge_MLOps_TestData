"""HmGenerator (A3) — H&M Personalized Fashion Recommendations 제너레이터

3천만 트랜잭션, 날짜 파티셔닝, 대규모 배치 처리.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("hm")
class HmGenerator(CsvGenerator):
    """H&M Personalized Fashion Recommendations 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "hm"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.RELATIONAL

    @property
    def _csv_files(self) -> list[str]:
        return ["transactions_train.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        val = safe_float(transformed.get("price"))
        if val is not None:
            transformed["price"] = val

        val = safe_int(transformed.get("sales_channel_id"))
        if val is not None:
            transformed["sales_channel_id"] = val

        return transformed
