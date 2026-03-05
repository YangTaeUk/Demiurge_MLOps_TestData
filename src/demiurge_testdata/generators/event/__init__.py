"""Event 제너레이터 — import 시 모든 제너레이터를 레지스트리에 등록"""

from demiurge_testdata.generators.event import (
    bitcoin,
    cc_fraud,
    clickstream,
    ieee_fraud,
    network_traffic,
    store_sales,
    twitter_sentiment,
)

__all__ = [
    "bitcoin",
    "cc_fraud",
    "clickstream",
    "ieee_fraud",
    "network_traffic",
    "store_sales",
    "twitter_sentiment",
]
