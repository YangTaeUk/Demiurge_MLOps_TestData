"""NycTaxiGenerator (F1) — NYC Taxi Fare Prediction 제너레이터

5500만 건 대량 로그, GeoJSON 변환, 시간대 분포 보존.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("nyc_taxi")
class NycTaxiGenerator(CsvGenerator):
    """NYC Taxi Fare Prediction 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "nyc_taxi"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.GEOSPATIAL

    @property
    def _csv_files(self) -> list[str]:
        return ["train.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        # 좌표 float 변환
        for key in ("pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"):
            if key in transformed:
                val = safe_float(transformed[key])
                if val is not None:
                    transformed[key] = val

        # GeoJSON Point 생성
        plng = safe_float(transformed.get("pickup_longitude"))
        plat = safe_float(transformed.get("pickup_latitude"))
        if plng is not None and plat is not None:
            transformed["pickup_location"] = {
                "type": "Point",
                "coordinates": [plng, plat],
            }

        dlng = safe_float(transformed.get("dropoff_longitude"))
        dlat = safe_float(transformed.get("dropoff_latitude"))
        if dlng is not None and dlat is not None:
            transformed["dropoff_location"] = {
                "type": "Point",
                "coordinates": [dlng, dlat],
            }

        # 기타 숫자 필드
        val = safe_float(transformed.get("fare_amount"))
        if val is not None:
            transformed["fare_amount"] = val

        val = safe_int(transformed.get("passenger_count"))
        if val is not None:
            transformed["passenger_count"] = val

        return transformed
