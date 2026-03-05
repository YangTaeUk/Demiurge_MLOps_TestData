"""ElectricPowerGenerator (D3) — Individual Household Electric Power Consumption 제너레이터

1분 간격 연속 측정, 가정용 전력 계측기 IoT 시뮬레이션.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float


@generator_registry.register("electric_power")
class ElectricPowerGenerator(CsvGenerator):
    """Individual Household Electric Power Consumption 데이터셋 제너레이터."""

    def __init__(self, config, *, records=None):
        super().__init__(config, records=records)
        if records is None:
            self._config = config.model_copy(update={"shuffle": False})

    @property
    def dataset_name(self) -> str:
        return "electric_power"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.IOT

    @property
    def _csv_files(self) -> list[str]:
        return ["household_power_consumption.txt"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed: dict[str, Any] = {}
        for key, value in record.items():
            if value in ("?", "", None):
                transformed[key] = None
            elif key in ("Date", "Time"):
                transformed[key] = value
            else:
                val = safe_float(value)
                transformed[key] = val if val is not None else value
        return transformed
