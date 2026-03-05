"""DataCoGenerator (F3) — DataCo Smart Supply Chain 제너레이터

공급망 배송 좌표, RDBMS+지리, 출발지/도착지 좌표 쌍.
"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.csv_generator import CsvGenerator, safe_float, safe_int


@generator_registry.register("dataco")
class DataCoGenerator(CsvGenerator):
    """DataCo Smart Supply Chain 데이터셋 제너레이터."""

    @property
    def dataset_name(self) -> str:
        return "dataco"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.GEOSPATIAL

    @property
    def _csv_files(self) -> list[str]:
        return ["DataCoSupplyChainDataset.csv"]

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        transformed = dict(record)

        float_keys = (
            "Latitude",
            "Longitude",
            "latitude",
            "longitude",
            "Product Price",
            "Sales",
            "Order Profit Per Order",
            "Order Item Discount",
            "Order Item Total",
            "product_price",
            "sales",
            "order_profit_per_order",
        )
        for key in float_keys:
            if key in transformed:
                val = safe_float(transformed[key])
                if val is not None:
                    transformed[key] = val

        int_keys = (
            "Order Id",
            "Customer Id",
            "Order Item Quantity",
            "order_id",
            "customer_id",
            "order_item_quantity",
        )
        for key in int_keys:
            if key in transformed:
                val = safe_int(transformed[key])
                if val is not None:
                    transformed[key] = val

        return transformed
