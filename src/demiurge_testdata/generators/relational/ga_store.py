"""GaStoreGenerator (A4) — Google Analytics Store 제너레이터

JSON 컬럼 파싱, DW 패턴 시뮬레이션, BigQuery 호환.
"""

from __future__ import annotations

import json
from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_int


@generator_registry.register("ga_store")
class GaStoreGenerator(CsvGenerator):
    """Google Analytics Store 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "ga_store"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.RELATIONAL

    @property
    def _csv_files(self) -> list[str]:
        return ["train_v2.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        # JSON 문자열 컬럼 → dict 파싱
        json_fields = ("totals", "trafficSource", "device", "geoNetwork")
        for key in json_fields:
            if key in transformed and isinstance(transformed[key], str):
                try:
                    transformed[key] = json.loads(transformed[key])
                except (json.JSONDecodeError, TypeError):
                    transformed[key] = {}

        for key in ("visitNumber", "visitStartTime"):
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
