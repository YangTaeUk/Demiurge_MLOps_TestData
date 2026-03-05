"""IeeFraudGenerator (C2) — IEEE-CIS Fraud Detection 제너레이터

434열 이벤트, TransactionDT 순서 발행, 정상/비정상 혼합 스트리밍.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("ieee_fraud")
class IeeFraudGenerator(CsvGenerator):
    """IEEE-CIS Fraud Detection 데이터셋 제너레이터."""

    def __init__(self, config, *, records=None):
        super().__init__(config, records=records)
        if records is None:
            self._config = config.model_copy(update={"shuffle": False})

    @property
    def dataset_name(self) -> str:
        return "ieee_fraud"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.EVENT

    @property
    def _csv_files(self) -> list[str]:
        return ["train_transaction.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        # NULL/빈값/NaN 제거 (sparse data 최적화)
        transformed = {k: v for k, v in record.items() if v not in ("", "nan", "NaN")}

        for key in ("TransactionID", "TransactionDT", "isFraud", "card1"):
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        if "TransactionAmt" in transformed:
            val = safe_float(transformed["TransactionAmt"])
            if val is not None:
                transformed["TransactionAmt"] = val

        return transformed
