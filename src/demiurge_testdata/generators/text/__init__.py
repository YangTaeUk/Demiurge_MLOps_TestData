"""Text 제너레이터 — import 시 모든 제너레이터를 레지스트리에 등록"""

from demiurge_testdata.generators.text import (
    enron_email,
    github_metadata,
    stackoverflow,
)

__all__ = [
    "enron_email",
    "github_metadata",
    "stackoverflow",
]
