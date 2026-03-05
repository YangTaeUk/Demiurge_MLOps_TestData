"""AppliancesEnergyGenerator (D4) — Appliances Energy Prediction 제너레이터

다실 센서, 10분 간격, RabbitMQ 라우팅.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float


@generator_registry.register("appliances_energy")
class AppliancesEnergyGenerator(CsvGenerator):
    """Appliances Energy Prediction 데이터셋 제너레이터."""

    def __init__(self, config, *, records=None):
        super().__init__(config, records=records)
        if records is None:
            self._config = config.model_copy(update={"shuffle": False})

    @property
    def dataset_name(self) -> str:
        return "appliances_energy"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.IOT

    @property
    def _csv_files(self) -> list[str]:
        return ["energydata_complete.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        # 모든 숫자 컬럼 float 변환 (date 제외)
        for key, value in transformed.items():
            if key in ("date",):
                continue
            val = safe_float(value)
            if val is not None:
                transformed[key] = val

        return transformed
