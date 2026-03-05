"""NetworkTrafficGenerator (C6) — Labeled Network Traffic Flows 제너레이터

네트워크 플로우 라벨링, 141개 앱 분류, Pulsar 멀티토픽.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("network_traffic")
class NetworkTrafficGenerator(CsvGenerator):
    """Labeled Network Traffic Flows 데이터셋 제너레이터."""

    def __init__(self, config, *, records=None):
        super().__init__(config, records=records)
        if records is None:
            self._config = config.model_copy(update={"shuffle": False})

    @property
    def dataset_name(self) -> str:
        return "network_traffic"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.EVENT

    @property
    def _csv_files(self) -> list[str]:
        return ["network_traffic.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        for key in ("Source Port", "Destination Port", "source_port", "destination_port"):
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        for key in ("Flow Duration", "flow_duration"):
            if key in transformed:
                val = safe_float(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
