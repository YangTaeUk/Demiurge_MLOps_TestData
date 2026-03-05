"""IoT 제너레이터 — import 시 모든 제너레이터를 레지스트리에 등록"""

from demiurge_testdata.generators.iot import (
    appliances_energy,
    bosch,
    electric_power,
    smart_mfg,
    weather,
)

__all__ = [
    "appliances_energy",
    "bosch",
    "electric_power",
    "smart_mfg",
    "weather",
]
