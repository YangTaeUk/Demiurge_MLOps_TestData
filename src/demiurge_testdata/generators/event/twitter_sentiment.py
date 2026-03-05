"""TwitterSentimentGenerator (C3) — Twitter Entity Sentiment Analysis 제너레이터

텍스트 이벤트 스트림, 감성 라벨, NATS 발행.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator


@generator_registry.register("twitter_sentiment")
class TwitterSentimentGenerator(CsvGenerator):
    """Twitter Entity Sentiment Analysis 데이터셋 제너레이터."""

    def __init__(self, config, *, records=None):
        super().__init__(config, records=records)
        if records is None:
            self._config = config.model_copy(update={"shuffle": False})

    @property
    def dataset_name(self) -> str:
        return "twitter_sentiment"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.EVENT

    @property
    def _csv_files(self) -> list[str]:
        return ["twitter_training.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        return dict(record)
