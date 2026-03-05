"""EuroSoccerGenerator (A7) — European Soccer Database 제너레이터

SQLite → RDBMS 마이그레이션 패턴, 7테이블, 날짜 처리.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_int


@generator_registry.register("euro_soccer")
class EuroSoccerGenerator(CsvGenerator):
    """European Soccer Database 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "euro_soccer"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.RELATIONAL

    @property
    def _csv_files(self) -> list[str]:
        return ["Match.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        int_keys = (
            "match_api_id",
            "home_team_api_id",
            "away_team_api_id",
            "home_team_goal",
            "away_team_goal",
            "league_id",
            "country_id",
        )
        for key in int_keys:
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
