"""WeatherGenerator (D2) — Weather Dataset 제너레이터

시계열 기상 관측, MQTT → S3 파이프라인, 센서 타입별 토픽.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float


@generator_registry.register("weather")
class WeatherGenerator(CsvGenerator):
    """Weather Dataset 제너레이터."""

    def __init__(self, config, *, records=None):
        super().__init__(config, records=records)
        if records is None:
            self._config = config.model_copy(update={"shuffle": False})

    @property
    def dataset_name(self) -> str:
        return "weather"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.IOT

    @property
    def _csv_files(self) -> list[str]:
        return ["weatherHistory.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        float_keys = (
            "Temp_C",
            "Temperature (C)",
            "Dew Point Temp_C",
            "Apparent Temperature (C)",
            "Rel Hum_%",
            "Humidity",
            "Wind Speed_km/h",
            "Wind Speed (km/h)",
            "Wind Bearing (degrees)",
            "Visibility_km",
            "Visibility (km)",
            "Press_kPa",
            "Pressure (millibars)",
        )
        for key in float_keys:
            if key in transformed:
                val = safe_float(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
