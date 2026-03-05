"""Wave 2 제너레이터 12종 단위 테스트 — 레지스트리, 변환, 모드별 동작"""

from __future__ import annotations

import pytest

# Force imports to trigger registration
import demiurge_testdata.generators.document.airbnb
import demiurge_testdata.generators.document.amazon_reviews
import demiurge_testdata.generators.event.cc_fraud
import demiurge_testdata.generators.event.clickstream
import demiurge_testdata.generators.event.twitter_sentiment
import demiurge_testdata.generators.iot.smart_mfg
import demiurge_testdata.generators.iot.weather
import demiurge_testdata.generators.relational.chinook
import demiurge_testdata.generators.relational.fraud_trans
import demiurge_testdata.generators.relational.ga_store
import demiurge_testdata.generators.relational.hm
import demiurge_testdata.generators.text.enron_email  # noqa: F401
from demiurge_testdata.core.config import GeneratorConfig
from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.document.airbnb import AirbnbGenerator
from demiurge_testdata.generators.document.amazon_reviews import (
    AmazonReviewsGenerator,
)
from demiurge_testdata.generators.event.cc_fraud import CcFraudGenerator
from demiurge_testdata.generators.event.clickstream import ClickstreamGenerator
from demiurge_testdata.generators.event.twitter_sentiment import (
    TwitterSentimentGenerator,
)
from demiurge_testdata.generators.iot.smart_mfg import SmartMfgGenerator
from demiurge_testdata.generators.iot.weather import WeatherGenerator
from demiurge_testdata.generators.relational.chinook import ChinookGenerator
from demiurge_testdata.generators.relational.fraud_trans import (
    FraudTransGenerator,
)
from demiurge_testdata.generators.relational.ga_store import GaStoreGenerator
from demiurge_testdata.generators.relational.hm import HmGenerator
from demiurge_testdata.generators.text.enron_email import EnronEmailGenerator

# ── 샘플 데이터 ──

HM_RECORDS = [
    {
        "t_dat": "2020-09-01",
        "customer_id": "abc123",
        "article_id": "0108775015",
        "price": "0.0338983",
        "sales_channel_id": "2",
    },
    {
        "t_dat": "2020-09-02",
        "customer_id": "def456",
        "article_id": "0108775044",
        "price": "0.0169491",
        "sales_channel_id": "1",
    },
]

GA_STORE_RECORDS = [
    {
        "fullVisitorId": "123456",
        "visitId": "1001",
        "visitNumber": "1",
        "visitStartTime": "1470000000",
        "date": "20180101",
        "channelGrouping": "Organic Search",
        "totals": '{"visits": "1", "hits": "5", "pageviews": "5"}',
        "trafficSource": '{"source": "google"}',
        "device": '{"browser": "Chrome", "operatingSystem": "Windows"}',
        "geoNetwork": '{"country": "United States"}',
    },
]

FRAUD_TRANS_RECORDS = [
    {
        "step": "1",
        "type": "PAYMENT",
        "amount": "9839.64",
        "nameOrig": "C1231006815",
        "oldbalanceOrg": "170136.0",
        "newbalanceOrig": "160296.36",
        "nameDest": "M1979787155",
        "oldbalanceDest": "0.0",
        "newbalanceDest": "0.0",
        "isFraud": "0",
        "isFlaggedFraud": "0",
    },
]

CHINOOK_RECORDS = [
    {
        "InvoiceId": "1",
        "CustomerId": "2",
        "InvoiceDate": "2009-01-01 00:00:00",
        "BillingAddress": "Theodor-Heuss-Strasse 34",
        "BillingCity": "Stuttgart",
        "BillingCountry": "Germany",
        "Total": "1.98",
    },
]

AIRBNB_RECORDS = [
    {
        "id": "1001",
        "name": "Cozy Room",
        "host_id": "5001",
        "host_name": "Alice",
        "neighbourhood": "Capitol Hill",
        "latitude": "47.6205",
        "longitude": "-122.3201",
        "room_type": "Private room",
        "price": "75.0",
        "number_of_reviews": "15",
        "availability_365": "200",
    },
]

AMAZON_REVIEW_RECORDS = [
    {
        "reviewerID": "A2SUAM1J3GNN3B",
        "asin": "B000FA64PK",
        "reviewerName": "J. McDonald",
        "overall": "5.0",
        "reviewText": "Great product!",
        "summary": "Excellent",
        "unixReviewTime": "1252800000",
        "reviewTime": "09 13, 2009",
    },
]

TWITTER_RECORDS = [
    {
        "tweet_id": "2401",
        "entity": "Google",
        "sentiment": "Positive",
        "content": "I love Google products",
    },
    {
        "tweet_id": "2402",
        "entity": "Apple",
        "sentiment": "Negative",
        "content": "Apple prices are too high",
    },
]

CC_FRAUD_RECORDS = [
    {
        "Time": "0",
        "V1": "-1.3598",
        "V2": "-0.0728",
        "V3": "2.5363",
        "Amount": "149.62",
        "Class": "0",
    },
    {
        "Time": "1",
        "V1": "1.1919",
        "V2": "0.2662",
        "V3": "0.1665",
        "Amount": "2.69",
        "Class": "0",
    },
]

CLICKSTREAM_RECORDS = [
    {
        "event_time": "2019-10-01 00:00:00 UTC",
        "event_type": "view",
        "product_id": "44600062",
        "category_id": "2103807459595387724",
        "category_code": "electronics.smartphone",
        "brand": "samsung",
        "price": "317.39",
        "user_id": "541312140",
        "user_session": "72d76fde-8bb3-4e00-8c23-a032dfed738c",
    },
]

WEATHER_RECORDS = [
    {
        "Date/Time": "2006-04-01 00:00:00",
        "Temp_C": "4.6",
        "Dew Point Temp_C": "-6.4",
        "Rel Hum_%": "43",
        "Wind Speed_km/h": "24",
        "Visibility_km": "25.0",
        "Press_kPa": "101.26",
        "Weather": "Mainly Clear",
    },
]

SMART_MFG_RECORDS = [
    {
        "machine_id": "M001",
        "timestamp": "2023-01-01 00:00:00",
        "temperature": "72.5",
        "humidity": "45.2",
        "vibration": "0.05",
        "power_consumption": "120.8",
        "status": "normal",
    },
]

ENRON_EMAIL_RECORDS = [
    {
        "file": "allen-p/_sent_mail/1.",
        "message": (
            "Message-ID: <123@mail>\n"
            "Date: Mon, 14 May 2001\n"
            "From: allen@enron.com\n"
            "To: bob@enron.com\n"
            "Subject: Test Email\n"
            "\n"
            "This is the email body.\n"
            "With multiple lines."
        ),
    },
]


def _cfg(**overrides):
    defaults = {"type": "test", "shuffle": False, "stream_interval_ms": 1}
    defaults.update(overrides)
    return GeneratorConfig(**defaults)


# ── Registry ──


class TestW2Registry:
    """Wave 2 제너레이터 레지스트리 등록 테스트."""

    @pytest.mark.parametrize(
        "key",
        [
            "hm",
            "ga_store",
            "fraud_trans",
            "chinook",
            "airbnb",
            "amazon_reviews",
            "twitter_sentiment",
            "cc_fraud",
            "clickstream",
            "weather",
            "smart_mfg",
            "enron_email",
        ],
    )
    def test_registered(self, key):
        assert key in generator_registry

    def test_registry_count_at_least_23(self):
        assert len(generator_registry) >= 23


# ── HmGenerator ──


class TestHmGenerator:
    @pytest.fixture
    def gen(self):
        return HmGenerator(_cfg(), records=HM_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "hm"
        assert gen.category == DatasetCategory.RELATIONAL

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert len(result) == 2

    def test_transform_price(self, gen):
        r = gen._transform(HM_RECORDS[0])
        assert isinstance(r["price"], float)
        assert r["price"] == pytest.approx(0.0338983)

    def test_transform_sales_channel(self, gen):
        r = gen._transform(HM_RECORDS[0])
        assert isinstance(r["sales_channel_id"], int)
        assert r["sales_channel_id"] == 2


# ── GaStoreGenerator ──


class TestGaStoreGenerator:
    @pytest.fixture
    def gen(self):
        return GaStoreGenerator(_cfg(), records=GA_STORE_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "ga_store"
        assert gen.category == DatasetCategory.RELATIONAL

    def test_json_parsing(self, gen):
        r = gen._transform(GA_STORE_RECORDS[0])
        assert isinstance(r["totals"], dict)
        assert r["totals"]["visits"] == "1"

    def test_device_parsed(self, gen):
        r = gen._transform(GA_STORE_RECORDS[0])
        assert isinstance(r["device"], dict)
        assert r["device"]["browser"] == "Chrome"

    def test_visit_number_int(self, gen):
        r = gen._transform(GA_STORE_RECORDS[0])
        assert isinstance(r["visitNumber"], int)
        assert r["visitNumber"] == 1


# ── FraudTransGenerator ──


class TestFraudTransGenerator:
    @pytest.fixture
    def gen(self):
        return FraudTransGenerator(_cfg(), records=FRAUD_TRANS_RECORDS)

    def test_amount_float(self, gen):
        r = gen._transform(FRAUD_TRANS_RECORDS[0])
        assert isinstance(r["amount"], float)
        assert r["amount"] == pytest.approx(9839.64)

    def test_is_fraud_int(self, gen):
        r = gen._transform(FRAUD_TRANS_RECORDS[0])
        assert isinstance(r["isFraud"], int)
        assert r["isFraud"] == 0

    def test_balance_float(self, gen):
        r = gen._transform(FRAUD_TRANS_RECORDS[0])
        assert isinstance(r["oldbalanceOrg"], float)
        assert r["oldbalanceOrg"] == pytest.approx(170136.0)


# ── ChinookGenerator ──


class TestChinookGenerator:
    @pytest.fixture
    def gen(self):
        return ChinookGenerator(_cfg(), records=CHINOOK_RECORDS)

    def test_invoice_id_int(self, gen):
        r = gen._transform(CHINOOK_RECORDS[0])
        assert isinstance(r["InvoiceId"], int)
        assert r["InvoiceId"] == 1

    def test_total_float(self, gen):
        r = gen._transform(CHINOOK_RECORDS[0])
        assert isinstance(r["Total"], float)
        assert r["Total"] == pytest.approx(1.98)

    def test_properties(self, gen):
        assert gen.dataset_name == "chinook"
        assert gen.category == DatasetCategory.RELATIONAL


# ── AirbnbGenerator ──


class TestAirbnbGenerator:
    @pytest.fixture
    def gen(self):
        return AirbnbGenerator(_cfg(), records=AIRBNB_RECORDS)

    def test_coordinates_float(self, gen):
        r = gen._transform(AIRBNB_RECORDS[0])
        assert isinstance(r["latitude"], float)
        assert isinstance(r["longitude"], float)

    def test_geojson_point(self, gen):
        r = gen._transform(AIRBNB_RECORDS[0])
        loc = r["location"]
        assert loc["type"] == "Point"
        assert len(loc["coordinates"]) == 2

    def test_id_int(self, gen):
        r = gen._transform(AIRBNB_RECORDS[0])
        assert isinstance(r["id"], int)
        assert r["id"] == 1001

    def test_properties(self, gen):
        assert gen.dataset_name == "airbnb"
        assert gen.category == DatasetCategory.DOCUMENT


# ── AmazonReviewsGenerator ──


class TestAmazonReviewsGenerator:
    @pytest.fixture
    def gen(self):
        return AmazonReviewsGenerator(_cfg(), records=AMAZON_REVIEW_RECORDS)

    def test_overall_float(self, gen):
        r = gen._transform(AMAZON_REVIEW_RECORDS[0])
        assert isinstance(r["overall"], float)
        assert r["overall"] == 5.0

    def test_unix_time_int(self, gen):
        r = gen._transform(AMAZON_REVIEW_RECORDS[0])
        assert isinstance(r["unixReviewTime"], int)

    def test_properties(self, gen):
        assert gen.dataset_name == "amazon_reviews"
        assert gen.category == DatasetCategory.DOCUMENT


# ── TwitterSentimentGenerator ──


class TestTwitterSentimentGenerator:
    @pytest.fixture
    def gen(self):
        return TwitterSentimentGenerator(_cfg(), records=TWITTER_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "twitter_sentiment"
        assert gen.category == DatasetCategory.EVENT

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert result[0]["sentiment"] == "Positive"
        assert result[1]["entity"] == "Apple"


# ── CcFraudGenerator ──


class TestCcFraudGenerator:
    @pytest.fixture
    def gen(self):
        return CcFraudGenerator(_cfg(), records=CC_FRAUD_RECORDS)

    def test_pca_float(self, gen):
        r = gen._transform(CC_FRAUD_RECORDS[0])
        assert isinstance(r["V1"], float)
        assert r["V1"] == pytest.approx(-1.3598)

    def test_amount_float(self, gen):
        r = gen._transform(CC_FRAUD_RECORDS[0])
        assert isinstance(r["Amount"], float)

    def test_class_int(self, gen):
        r = gen._transform(CC_FRAUD_RECORDS[0])
        assert isinstance(r["Class"], int)
        assert r["Class"] == 0

    def test_time_float(self, gen):
        r = gen._transform(CC_FRAUD_RECORDS[0])
        assert isinstance(r["Time"], float)


# ── ClickstreamGenerator ──


class TestClickstreamGenerator:
    @pytest.fixture
    def gen(self):
        return ClickstreamGenerator(_cfg(), records=CLICKSTREAM_RECORDS)

    def test_price_float(self, gen):
        r = gen._transform(CLICKSTREAM_RECORDS[0])
        assert isinstance(r["price"], float)
        assert r["price"] == pytest.approx(317.39)

    def test_ids_int(self, gen):
        r = gen._transform(CLICKSTREAM_RECORDS[0])
        assert isinstance(r["product_id"], int)
        assert isinstance(r["user_id"], int)

    def test_event_type_preserved(self, gen):
        r = gen._transform(CLICKSTREAM_RECORDS[0])
        assert r["event_type"] == "view"


# ── WeatherGenerator ──


class TestWeatherGenerator:
    @pytest.fixture
    def gen(self):
        return WeatherGenerator(_cfg(), records=WEATHER_RECORDS)

    def test_temp_float(self, gen):
        r = gen._transform(WEATHER_RECORDS[0])
        assert isinstance(r["Temp_C"], float)
        assert r["Temp_C"] == pytest.approx(4.6)

    def test_pressure_float(self, gen):
        r = gen._transform(WEATHER_RECORDS[0])
        assert isinstance(r["Press_kPa"], float)

    def test_weather_string(self, gen):
        r = gen._transform(WEATHER_RECORDS[0])
        assert r["Weather"] == "Mainly Clear"


# ── SmartMfgGenerator ──


class TestSmartMfgGenerator:
    @pytest.fixture
    def gen(self):
        return SmartMfgGenerator(_cfg(), records=SMART_MFG_RECORDS)

    def test_temperature_float(self, gen):
        r = gen._transform(SMART_MFG_RECORDS[0])
        assert isinstance(r["temperature"], float)
        assert r["temperature"] == pytest.approx(72.5)

    def test_status_preserved(self, gen):
        r = gen._transform(SMART_MFG_RECORDS[0])
        assert r["status"] == "normal"

    def test_machine_id_string(self, gen):
        r = gen._transform(SMART_MFG_RECORDS[0])
        assert r["machine_id"] == "M001"


# ── EnronEmailGenerator ──


class TestEnronEmailGenerator:
    @pytest.fixture
    def gen(self):
        return EnronEmailGenerator(_cfg(), records=ENRON_EMAIL_RECORDS)

    def test_email_parsing(self, gen):
        r = gen._transform(ENRON_EMAIL_RECORDS[0])
        assert r["from_addr"] == "allen@enron.com"
        assert r["to_addr"] == "bob@enron.com"
        assert r["subject"] == "Test Email"

    def test_body_extraction(self, gen):
        r = gen._transform(ENRON_EMAIL_RECORDS[0])
        assert "email body" in r["body"]

    def test_date_extraction(self, gen):
        r = gen._transform(ENRON_EMAIL_RECORDS[0])
        assert "May 2001" in r["date"]

    def test_original_fields_preserved(self, gen):
        r = gen._transform(ENRON_EMAIL_RECORDS[0])
        assert r["file"] == "allen-p/_sent_mail/1."
