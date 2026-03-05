"""ClickstreamGenerator (C5) — eCommerce Behavior Data 제너레이터

최대 규모 이벤트 스트림 (285M), 이벤트 타입 분류, 멀티 파티션.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("clickstream")
class ClickstreamGenerator(CsvGenerator):
    """eCommerce Behavior Data 데이터셋 제너레이터."""

    def __init__(self, config, *, records=None):
        super().__init__(config, records=records)
        if records is None:
            self._config = config.model_copy(update={"shuffle": False})

    @property
    def dataset_name(self) -> str:
        return "clickstream"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.EVENT

    @property
    def _csv_files(self) -> list[str]:
        return ["2019-Oct.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        val = safe_float(transformed.get("price"))
        if val is not None:
            transformed["price"] = val

        for key in ("product_id", "category_id", "user_id"):
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
