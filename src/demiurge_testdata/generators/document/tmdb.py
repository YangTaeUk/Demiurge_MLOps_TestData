"""TmdbGenerator (B2) — TMDB Movie Metadata 제너레이터

JSON 문자열 컬럼 → 실제 배열/객체 파싱.
"""

from __future__ import annotations

import json
from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("tmdb")
class TmdbGenerator(CsvGenerator):
    """TMDB Movie Metadata 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "tmdb"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.DOCUMENT

    @property
    def _csv_files(self) -> list[str]:
        return ["tmdb_5000_movies.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        # JSON 문자열 → 파이썬 객체
        json_fields = (
            "genres",
            "keywords",
            "cast",
            "crew",
            "production_companies",
            "production_countries",
            "spoken_languages",
        )
        for key in json_fields:
            if key in transformed and isinstance(transformed[key], str):
                try:
                    transformed[key] = json.loads(transformed[key])
                except (json.JSONDecodeError, TypeError):
                    transformed[key] = []

        for key in ("id", "budget", "revenue", "vote_count", "runtime"):
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        for key in ("popularity", "vote_average"):
            if key in transformed:
                val = safe_float(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
