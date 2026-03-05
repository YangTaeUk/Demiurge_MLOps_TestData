"""Document 제너레이터 — import 시 모든 제너레이터를 레지스트리에 등록"""

from demiurge_testdata.generators.document import (
    airbnb,
    amazon_reviews,
    foodcom,
    instacart,
    tmdb,
    yelp,
)

__all__ = [
    "airbnb",
    "amazon_reviews",
    "foodcom",
    "instacart",
    "tmdb",
    "yelp",
]
