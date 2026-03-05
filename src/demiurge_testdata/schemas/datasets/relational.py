"""Category A: Relational 데이터셋 스키마 — HomeCreditApplication, OlistOrder"""

from __future__ import annotations

from pydantic import BaseModel, Field


class HomeCreditApplication(BaseModel):
    """Home Credit Default Risk — 대출 신청 레코드 (122열 중 핵심 필드)"""

    sk_id_curr: int = Field(..., description="Client ID")
    target: int | None = Field(None, ge=0, le=1)
    name_contract_type: str | None = None
    code_gender: str | None = None
    flag_own_car: str | None = None
    flag_own_realty: str | None = None
    cnt_children: int | None = Field(None, ge=0)
    amt_income_total: float | None = Field(None, ge=0)
    amt_credit: float | None = Field(None, ge=0)
    amt_annuity: float | None = None
    amt_goods_price: float | None = None

    model_config = {"extra": "allow"}


class OlistOrder(BaseModel):
    """Brazilian E-Commerce (Olist) — 주문 레코드"""

    order_id: str
    customer_id: str | None = None
    order_status: str | None = None
    order_purchase_timestamp: str | None = None
    order_approved_at: str | None = None
    order_delivered_carrier_date: str | None = None
    order_delivered_customer_date: str | None = None
    order_estimated_delivery_date: str | None = None

    model_config = {"extra": "allow"}


class OlistOrderItem(BaseModel):
    """Olist 주문 상품 레코드"""

    order_id: str
    order_item_id: int | None = None
    product_id: str | None = None
    seller_id: str | None = None
    price: float | None = Field(None, ge=0)
    freight_value: float | None = Field(None, ge=0)

    model_config = {"extra": "allow"}


class HmTransaction(BaseModel):
    """H&M Personalized Fashion — 구매 트랜잭션"""

    t_dat: str | None = None
    customer_id: str | None = None
    article_id: str | None = None
    price: float | None = Field(None, ge=0)
    sales_channel_id: int | None = None

    model_config = {"extra": "allow"}


class GaStoreSession(BaseModel):
    """Google Analytics Store — 세션 레코드 + JSON 컬럼"""

    fullVisitorId: str | None = None
    visitId: str | None = None
    visitNumber: int | None = None
    visitStartTime: int | None = None
    date: str | None = None
    channelGrouping: str | None = None
    totals: dict | None = None
    trafficSource: dict | None = None
    device: dict | None = None
    geoNetwork: dict | None = None

    model_config = {"extra": "allow"}


class FraudTransaction(BaseModel):
    """Fraudulent Transactions Prediction — 금융 트랜잭션"""

    step: int | None = None
    type: str | None = None
    amount: float | None = Field(None, ge=0)
    nameOrig: str | None = None
    oldbalanceOrg: float | None = None
    newbalanceOrig: float | None = None
    nameDest: str | None = None
    oldbalanceDest: float | None = None
    newbalanceDest: float | None = None
    isFraud: int | None = Field(None, ge=0, le=1)
    isFlaggedFraud: int | None = Field(None, ge=0, le=1)

    model_config = {"extra": "allow"}


class ChinookInvoice(BaseModel):
    """Chinook DB — 인보이스 레코드"""

    InvoiceId: int | None = None
    CustomerId: int | None = None
    InvoiceDate: str | None = None
    BillingAddress: str | None = None
    BillingCity: str | None = None
    BillingState: str | None = None
    BillingCountry: str | None = None
    BillingPostalCode: str | None = None
    Total: float | None = Field(None, ge=0)

    model_config = {"extra": "allow"}


class EuroSoccerMatch(BaseModel):
    """European Soccer Database — 경기 레코드"""

    match_api_id: int | None = None
    home_team_api_id: int | None = None
    away_team_api_id: int | None = None
    home_team_goal: int | None = None
    away_team_goal: int | None = None
    season: str | None = None
    date: str | None = None

    model_config = {"extra": "allow"}


class NorthwindOrder(BaseModel):
    """Northwind Traders — 주문 레코드"""

    OrderID: int | None = None
    CustomerID: str | None = None
    EmployeeID: int | None = None
    OrderDate: str | None = None
    ShipName: str | None = None
    ShipCity: str | None = None
    ShipCountry: str | None = None
    Freight: float | None = Field(None, ge=0)

    model_config = {"extra": "allow"}
