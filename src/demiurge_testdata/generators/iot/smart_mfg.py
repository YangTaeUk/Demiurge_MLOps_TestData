"""SmartMfgGenerator (D5) — Smart Manufacturing IoT 제너레이터

50머신 센서, MQTT→Kafka 브릿지, 이상 탐지 라벨.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float


@generator_registry.register("smart_mfg")
class SmartMfgGenerator(CsvGenerator):
    """Smart Manufacturing IoT 데이터셋 제너레이터."""

    def __init__(self, config, *, records=None):
        super().__init__(config, records=records)
        if records is None:
            self._config = config.model_copy(update={"shuffle": False})

    @property
    def dataset_name(self) -> str:
        return "smart_mfg"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.IOT

    @property
    def _csv_files(self) -> list[str]:
        return ["manufacturing_data.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        float_keys = (
            "temperature",
            "humidity",
            "vibration",
            "power_consumption",
            "pressure",
            "rpm",
        )
        for key in float_keys:
            if key in transformed:
                val = safe_float(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
