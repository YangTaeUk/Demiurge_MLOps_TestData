"""AmazonReviewsGenerator (B4) — Amazon Reviews 제너레이터

대량 리뷰 텍스트, 감성 라벨, Elasticsearch 벌크.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("amazon_reviews")
class AmazonReviewsGenerator(CsvGenerator):
    """Amazon Reviews 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "amazon_reviews"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.DOCUMENT

    @property
    def _csv_files(self) -> list[str]:
        return ["Reviews.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        val = safe_float(transformed.get("overall") or transformed.get("Score"))
        if val is not None:
            key = "overall" if "overall" in transformed else "Score"
            transformed[key] = val

        for key in ("unixReviewTime", "Time"):
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
