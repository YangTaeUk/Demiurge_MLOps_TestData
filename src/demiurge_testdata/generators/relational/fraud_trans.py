"""FraudTransGenerator (A5) — Fraudulent Transactions Prediction 제너레이터

금융원장 패턴, 트랜잭션 무결성, 금액 정밀도 보존.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("fraud_trans")
class FraudTransGenerator(CsvGenerator):
    """Fraudulent Transactions Prediction 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "fraud_trans"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.RELATIONAL

    @property
    def _csv_files(self) -> list[str]:
        return ["Fraud.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        float_keys = (
            "amount",
            "oldbalanceOrg",
            "newbalanceOrig",
            "oldbalanceDest",
            "newbalanceDest",
        )
        for key in float_keys:
            if key in transformed:
                val = safe_float(transformed[key])
                if val is not None:
                    transformed[key] = val

        int_keys = ("step", "isFraud", "isFlaggedFraud")
        for key in int_keys:
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
