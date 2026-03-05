"""Category F: Geospatial 데이터셋 스키마 — NycTaxiTrip, DataCoOrder"""

from __future__ import annotations

from pydantic import BaseModel, Field


class NycTaxiTrip(BaseModel):
    """NYC Taxi Fare Prediction — 택시 승차 레코드 + GeoJSON 변환"""

    key: str | None = None
    fare_amount: float | None = None
    pickup_datetime: str | None = None
    pickup_longitude: float | None = Field(None, ge=-180, le=180)
    pickup_latitude: float | None = Field(None, ge=-90, le=90)
    dropoff_longitude: float | None = Field(None, ge=-180, le=180)
    dropoff_latitude: float | None = Field(None, ge=-90, le=90)
    passenger_count: int | None = Field(None, ge=0)

    model_config = {"extra": "allow"}


class DataCoOrder(BaseModel):
    """DataCo Smart Supply Chain — 공급망 주문 + 배송 좌표"""

    order_id: int | None = Field(None, alias="Order Id")
    order_date: str | None = Field(None, alias="order date (DateOrders)")
    shipping_date: str | None = Field(None, alias="shipping date (DateOrders)")
    customer_id: int | None = Field(None, alias="Customer Id")
    customer_city: str | None = Field(None, alias="Customer City")
    customer_state: str | None = Field(None, alias="Customer State")
    order_region: str | None = Field(None, alias="Order Region")
    latitude: float | None = Field(None, alias="Latitude", ge=-90, le=90)
    longitude: float | None = Field(None, alias="Longitude", ge=-180, le=180)
    product_name: str | None = Field(None, alias="Product Name")
    product_price: float | None = Field(None, alias="Product Price", ge=0)
    order_item_quantity: int | None = Field(None, alias="Order Item Quantity", ge=0)
    sales: float | None = Field(None, alias="Sales", ge=0)
    order_profit_per_order: float | None = Field(None, alias="Order Profit Per Order")
    shipping_mode: str | None = Field(None, alias="Shipping Mode")
    delivery_status: str | None = Field(None, alias="Delivery Status")

    model_config = {"extra": "allow", "populate_by_name": True}


class GeoLifeTrajectory(BaseModel):
    """Microsoft GeoLife GPS Trajectory — GPS 궤적 포인트"""

    latitude: float | None = Field(None, ge=-90, le=90)
    longitude: float | None = Field(None, ge=-180, le=180)
    altitude: float | None = None
    date: str | None = None
    time: str | None = None
    user_id: str | None = None

    model_config = {"extra": "allow"}
