"""BitcoinGenerator (C7) — Bitcoin Historical Data 제너레이터

1분봉 OHLCV 금융 시계열, NATS JetStream 발행.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("bitcoin")
class BitcoinGenerator(CsvGenerator):
    """Bitcoin Historical Data 데이터셋 제너레이터."""

    def __init__(self, config, *, records=None):
        super().__init__(config, records=records)
        if records is None:
            self._config = config.model_copy(update={"shuffle": False})

    @property
    def dataset_name(self) -> str:
        return "bitcoin"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.EVENT

    @property
    def _csv_files(self) -> list[str]:
        return ["bitstampUSD_1-min_data.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        val = safe_int(transformed.get("Timestamp"))
        if val is not None:
            transformed["Timestamp"] = val

        float_keys = (
            "Open",
            "High",
            "Low",
            "Close",
            "Volume_(BTC)",
            "Volume_(Currency)",
            "Weighted_Price",
        )
        for key in float_keys:
            if key in transformed:
                val = safe_float(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
