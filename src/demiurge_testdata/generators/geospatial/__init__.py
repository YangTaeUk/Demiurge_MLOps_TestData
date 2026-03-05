"""Geospatial 제너레이터 — import 시 모든 제너레이터를 레지스트리에 등록"""

from demiurge_testdata.generators.geospatial import (
    dataco,
    geolife,
    nyc_taxi,
)

__all__ = [
    "dataco",
    "geolife",
    "nyc_taxi",
]
