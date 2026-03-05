"""StackOverflowGenerator (E1) — Stack Overflow Q&A 제너레이터

장문 텍스트 처리, 태그 시스템 → 배열 변환, TEXT/CLOB 처리.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_int


@generator_registry.register("stackoverflow")
class StackOverflowGenerator(CsvGenerator):
    """Stack Overflow Q&A 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "stackoverflow"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.TEXT

    @property
    def _csv_files(self) -> list[str]:
        return ["Questions.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        # 태그 파싱: "<python><pandas>" → ["python", "pandas"]
        tags_raw = transformed.get("Tags") or transformed.get("tags") or ""
        if isinstance(tags_raw, str) and tags_raw:
            tags = [t for t in tags_raw.replace("<", " ").replace(">", " ").split() if t]
            transformed["tags_list"] = tags

        # 숫자 필드 변환
        for key in ("Id", "Score", "OwnerUserId", "AnswerCount", "FavoriteCount"):
            for k in (key, key.lower()):
                if k in transformed:
                    val = safe_int(transformed[k])
                    if val is not None:
                        transformed[k] = val

        return transformed
