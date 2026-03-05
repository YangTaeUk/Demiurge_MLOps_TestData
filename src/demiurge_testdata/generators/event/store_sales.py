"""StoreSalesGenerator (C1) — Store Item Demand Forecasting 제너레이터

일별 시계열, 시간 순서 보존, 시간 압축 재생.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_int


@generator_registry.register("store_sales")
class StoreSalesGenerator(CsvGenerator):
    """Store Item Demand Forecasting 데이터셋 제너레이터."""

    def __init__(self, config, *, records=None):
        super().__init__(config, records=records)
        if records is None:
            self._config = config.model_copy(update={"shuffle": False})

    @property
    def dataset_name(self) -> str:
        return "store_sales"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.EVENT

    @property
    def _csv_files(self) -> list[str]:
        return ["train.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)
        for key in ("store", "item", "sales"):
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val
        return transformed
