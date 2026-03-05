"""HomeCreditGenerator (A1) — Home Credit Default Risk 제너레이터

FK 관계 보존, 122열 금융 데이터, 소수점 정밀도 보존.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("home_credit")
class HomeCreditGenerator(CsvGenerator):
    """Home Credit Default Risk 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "home_credit"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.RELATIONAL

    @property
    def _csv_files(self) -> list[str]:
        return ["application_train.csv"]

    _INT_PREFIXES = ("SK_ID_", "sk_id_", "CNT_", "cnt_")
    _INT_KEYS = ("TARGET", "target")

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)
        for key, value in transformed.items():
            if value in ("", None):
                transformed[key] = None
            elif key.startswith(("AMT_", "amt_")):
                val = safe_float(value)
                transformed[key] = val if val is not None else value
            elif key.startswith(self._INT_PREFIXES) or key in self._INT_KEYS:
                val = safe_int(value)
                transformed[key] = val if val is not None else value
        return transformed
