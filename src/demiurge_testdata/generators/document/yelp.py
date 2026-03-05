"""YelpGenerator (B5) — Yelp Dataset 제너레이터

최대 규모 문서셋, 멀티 컬렉션, 8.65GB 청크 읽기.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("yelp")
class YelpGenerator(CsvGenerator):
    """Yelp Dataset 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "yelp"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.DOCUMENT

    @property
    def _csv_files(self) -> list[str]:
        return ["yelp_business.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        for key in ("latitude", "longitude", "stars"):
            if key in transformed:
                val = safe_float(transformed[key])
                if val is not None:
                    transformed[key] = val

        val = safe_int(transformed.get("review_count"))
        if val is not None:
            transformed["review_count"] = val

        # categories 문자열 → 리스트
        cats = transformed.get("categories")
        if isinstance(cats, str) and cats:
            transformed["categories_list"] = [c.strip() for c in cats.split(",") if c.strip()]

        return transformed
