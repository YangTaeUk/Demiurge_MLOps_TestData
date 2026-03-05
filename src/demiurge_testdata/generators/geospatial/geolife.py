"""GeoLifeGenerator (F2) — Microsoft GeoLife GPS Trajectory 제너레이터

GPS 궤적, 17K 파일, 시간순 경로, SFTP 시뮬레이션.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float


@generator_registry.register("geolife")
class GeoLifeGenerator(CsvGenerator):
    """Microsoft GeoLife GPS Trajectory 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "geolife"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.GEOSPATIAL

    @property
    def _csv_files(self) -> list[str]:
        return ["geolife_trajectories.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        for key in ("latitude", "longitude", "altitude"):
            if key in transformed:
                val = safe_float(transformed[key])
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
