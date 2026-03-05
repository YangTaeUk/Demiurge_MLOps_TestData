"""BoschGenerator (D1) — Bosch Production Line Performance 제너레이터

4000+ 피처, 고차원 IoT 시뮬레이션, 90%+ NULL 처리.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("bosch")
class BoschGenerator(CsvGenerator):
    """Bosch Production Line Performance 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "bosch"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.IOT

    @property
    def _csv_files(self) -> list[str]:
        return ["train_numeric.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        # sparse 표현: NULL/빈값 생략
        transformed: dict[str, Any] = {}
        for key, value in record.items():
            if value in ("", None, "nan", "NaN"):
                continue
            if key in ("Id", "Response"):
                val = safe_int(value)
                transformed[key] = val if val is not None else value
            else:
                val = safe_float(value)
                transformed[key] = val if val is not None else value
        return transformed
