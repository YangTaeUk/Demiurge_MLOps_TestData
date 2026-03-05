"""Category C: Event/Log 데이터셋 스키마 — StoreSale, IeeFraudTransaction"""

from __future__ import annotations

from pydantic import BaseModel, Field


class StoreSale(BaseModel):
    """Store Item Demand Forecasting — 일별 매출 시계열"""

    date: str
    store: int | None = None
    item: int | None = None
    sales: int | None = Field(None, ge=0)

    model_config = {"extra": "allow"}


class IeeFraudTransaction(BaseModel):
    """IEEE-CIS Fraud Detection — 트랜잭션 이벤트 (434열 중 핵심 필드)"""

    transaction_id: str | None = Field(None, alias="TransactionID")
    is_fraud: int | None = Field(None, alias="isFraud", ge=0, le=1)
    transaction_dt: int | None = Field(None, alias="TransactionDT")
    transaction_amt: float | None = Field(None, alias="TransactionAmt")
    product_cd: str | None = Field(None, alias="ProductCD")
    card1: int | None = None
    card2: float | None = None
    card3: float | None = None
    card4: str | None = None
    card5: float | None = None
    card6: str | None = None
    addr1: float | None = None
    addr2: float | None = None
    p_emaildomain: str | None = Field(None, alias="P_emaildomain")
    r_emaildomain: str | None = Field(None, alias="R_emaildomain")

    model_config = {"extra": "allow", "populate_by_name": True}


class TwitterSentiment(BaseModel):
    """Twitter Entity Sentiment — 트윗 감성 이벤트"""

    tweet_id: str | None = None
    entity: str | None = None
    sentiment: str | None = None
    content: str | None = None

    model_config = {"extra": "allow"}


class CcFraudTransaction(BaseModel):
    """Credit Card Fraud Detection — PCA 변환 트랜잭션"""

    time: float | None = Field(None, alias="Time")
    amount: float | None = Field(None, alias="Amount", ge=0)
    fraud_class: int | None = Field(None, alias="Class", ge=0, le=1)

    model_config = {"extra": "allow", "populate_by_name": True}


class ClickstreamEvent(BaseModel):
    """eCommerce Behavior — 클릭스트림 이벤트"""

    event_time: str | None = None
    event_type: str | None = None
    product_id: int | None = None
    category_id: int | None = None
    category_code: str | None = None
    brand: str | None = None
    price: float | None = Field(None, ge=0)
    user_id: int | None = None
    user_session: str | None = None

    model_config = {"extra": "allow"}


class NetworkTrafficFlow(BaseModel):
    """Labeled Network Traffic Flows — 네트워크 플로우"""

    source_ip: str | None = Field(None, alias="Source IP")
    destination_ip: str | None = Field(None, alias="Destination IP")
    source_port: int | None = Field(None, alias="Source Port")
    destination_port: int | None = Field(None, alias="Destination Port")
    protocol: str | None = Field(None, alias="Protocol")
    application: str | None = Field(None, alias="Application")

    model_config = {"extra": "allow", "populate_by_name": True}


class BitcoinOHLCV(BaseModel):
    """Bitcoin Historical Data — 1분봉 OHLCV"""

    timestamp: int | None = Field(None, alias="Timestamp")
    open: float | None = Field(None, alias="Open", ge=0)
    high: float | None = Field(None, alias="High", ge=0)
    low: float | None = Field(None, alias="Low", ge=0)
    close: float | None = Field(None, alias="Close", ge=0)
    volume_btc: float | None = Field(None, alias="Volume_(BTC)")
    volume_currency: float | None = Field(None, alias="Volume_(Currency)")

    model_config = {"extra": "allow", "populate_by_name": True}
