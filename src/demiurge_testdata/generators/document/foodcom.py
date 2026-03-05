"""FoodComGenerator (B6) — Food.com Recipes 제너레이터

가변 배열 (재료, 조리 단계), 사용자 인터랙션.
"""

from __future__ import annotations

import contextlib
import json
from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_int


@generator_registry.register("foodcom")
class FoodComGenerator(CsvGenerator):
    """Food.com Recipes 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "foodcom"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.DOCUMENT

    @property
    def _csv_files(self) -> list[str]:
        return ["RAW_recipes.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        # JSON 배열 문자열 → 파이썬 리스트
        for key in ("ingredients", "steps", "tags"):
            if key in transformed and isinstance(transformed[key], str):
                with contextlib.suppress(json.JSONDecodeError, TypeError):
                    transformed[key] = json.loads(transformed[key].replace("'", '"'))

        for key in ("id", "minutes", "n_steps", "n_ingredients"):
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
