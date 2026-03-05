"""CcFraudGenerator (C4) — Credit Card Fraud Detection 제너레이터

PCA 변환 데이터, 희소 라벨 (0.17% Fraud), Redis Streams.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("cc_fraud")
class CcFraudGenerator(CsvGenerator):
    """Credit Card Fraud Detection 데이터셋 제너레이터."""

    def __init__(self, config, *, records=None):
        super().__init__(config, records=records)
        if records is None:
            self._config = config.model_copy(update={"shuffle": False})

    @property
    def dataset_name(self) -> str:
        return "cc_fraud"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.EVENT

    @property
    def _csv_files(self) -> list[str]:
        return ["creditcard.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        # PCA 피처 V1~V28 + Time, Amount → float
        for key, value in transformed.items():
            if key.startswith("V") or key in ("Time", "Amount"):
                val = safe_float(value)
                if val is not None:
                    transformed[key] = val

        # Class → int
        val = safe_int(transformed.get("Class"))
        if val is not None:
            transformed["Class"] = val

        return transformed
