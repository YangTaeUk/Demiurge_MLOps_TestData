"""AirbnbGenerator (B3) — Airbnb Seattle 제너레이터

리뷰 텍스트 + 지리 좌표 복합 문서, Elasticsearch 매핑.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("airbnb")
class AirbnbGenerator(CsvGenerator):
    """Airbnb Seattle 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "airbnb"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.DOCUMENT

    @property
    def _csv_files(self) -> list[str]:
        return ["listings.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        for key in ("latitude", "longitude", "price"):
            if key in transformed:
                val = safe_float(transformed[key])
                if val is not None:
                    transformed[key] = val

        for key in ("id", "host_id", "number_of_reviews", "availability_365"):
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        # GeoJSON Point 생성
        lat = safe_float(transformed.get("latitude"))
        lng = safe_float(transformed.get("longitude"))
        if lat is not None and lng is not None:
            transformed["location"] = {
                "type": "Point",
                "coordinates": [lng, lat],
            }

        return transformed
