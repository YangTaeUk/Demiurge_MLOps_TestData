"""GitHubMetadataGenerator (E3) — GitHub Repository Metadata 제너레이터

300만 레포 메타데이터 JSON, MongoDB 벌크 인서트.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_int


@generator_registry.register("github_metadata")
class GitHubMetadataGenerator(CsvGenerator):
    """GitHub Repository Metadata 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "github_metadata"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.TEXT

    @property
    def _csv_files(self) -> list[str]:
        return ["github_repos.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        int_keys = (
            "repo_id",
            "stargazers_count",
            "forks_count",
            "watchers_count",
            "open_issues_count",
            "size",
        )
        for key in int_keys:
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        # topics 문자열 → 리스트
        topics = transformed.get("topics")
        if isinstance(topics, str) and topics:
            transformed["topics_list"] = [t.strip() for t in topics.split(",") if t.strip()]

        return transformed
