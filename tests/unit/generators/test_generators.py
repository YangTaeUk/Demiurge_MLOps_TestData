"""Wave 1 제너레이터 11종 단위 테스트 — 레지스트리, 변환, 모드별 동작"""

from __future__ import annotations

import pytest

# Force imports to trigger registration
import demiurge_testdata.generators.document.instacart
import demiurge_testdata.generators.document.tmdb
import demiurge_testdata.generators.event.ieee_fraud
import demiurge_testdata.generators.event.store_sales
import demiurge_testdata.generators.geospatial.dataco
import demiurge_testdata.generators.geospatial.nyc_taxi
import demiurge_testdata.generators.iot.bosch
import demiurge_testdata.generators.iot.electric_power
import demiurge_testdata.generators.relational.home_credit
import demiurge_testdata.generators.relational.olist
import demiurge_testdata.generators.text.stackoverflow  # noqa: F401
from demiurge_testdata.core.config import GeneratorConfig
from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry

# ── 샘플 데이터 ──

HOME_CREDIT_RECORDS = [
    {
        "SK_ID_CURR": "100001",
        "TARGET": "0",
        "AMT_INCOME_TOTAL": "135000.0",
        "AMT_CREDIT": "513000.0",
        "NAME_CONTRACT_TYPE": "Cash loans",
        "CODE_GENDER": "M",
        "FLAG_OWN_CAR": "Y",
    },
    {
        "SK_ID_CURR": "100002",
        "TARGET": "1",
        "AMT_INCOME_TOTAL": "99000.0",
        "AMT_CREDIT": "270000.0",
        "NAME_CONTRACT_TYPE": "Revolving loans",
        "CODE_GENDER": "F",
        "FLAG_OWN_CAR": "N",
    },
]

OLIST_RECORDS = [
    {
        "order_id": "abc123",
        "customer_id": "cust001",
        "order_status": "delivered",
        "order_purchase_timestamp": "2017-10-02 10:56:33",
        "price": "29.99",
        "freight_value": "8.72",
    },
    {
        "order_id": "def456",
        "customer_id": "cust002",
        "order_status": "shipped",
        "order_purchase_timestamp": "2018-03-15 14:22:11",
        "price": "149.90",
        "freight_value": "15.50",
    },
]

INSTACART_RECORDS = [
    {
        "order_id": "1",
        "user_id": "112108",
        "product_id": "49302",
        "add_to_cart_order": "1",
        "reordered": "1",
        "order_dow": "4",
        "days_since_prior_order": "9.0",
    },
    {
        "order_id": "1",
        "user_id": "112108",
        "product_id": "11109",
        "add_to_cart_order": "2",
        "reordered": "0",
        "order_dow": "4",
        "days_since_prior_order": "9.0",
    },
]

TMDB_RECORDS = [
    {
        "id": "19995",
        "title": "Avatar",
        "budget": "237000000",
        "revenue": "2787965087",
        "popularity": "150.437577",
        "vote_average": "7.2",
        "vote_count": "11800",
        "genres": '[{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}]',
        "cast": '[{"name": "Sam Worthington"}]',
        "keywords": "[]",
        "runtime": "162",
    },
    {
        "id": "285",
        "title": "Pirates",
        "budget": "300000000",
        "revenue": "961000000",
        "popularity": "139.0",
        "vote_average": "6.9",
        "vote_count": "4500",
        "genres": '[{"id": 12, "name": "Adventure"}]',
        "cast": "[]",
        "keywords": "[]",
        "runtime": "169",
    },
]

STORE_SALES_RECORDS = [
    {"date": "2013-01-01", "store": "1", "item": "1", "sales": "13"},
    {"date": "2013-01-02", "store": "1", "item": "1", "sales": "11"},
    {"date": "2013-01-03", "store": "2", "item": "3", "sales": "14"},
]

IEEE_FRAUD_RECORDS = [
    {
        "TransactionID": "2987000",
        "isFraud": "0",
        "TransactionDT": "86400",
        "TransactionAmt": "68.5",
        "ProductCD": "W",
        "card1": "13926",
    },
    {
        "TransactionID": "2987001",
        "isFraud": "1",
        "TransactionDT": "86401",
        "TransactionAmt": "29.0",
        "ProductCD": "H",
        "card1": "2755",
        "V1": "",
        "V2": "nan",
    },
]

BOSCH_RECORDS = [
    {"Id": "4", "L0_S0_F0": "0.03", "L0_S0_F2": "", "L0_S0_F4": "nan", "Response": "0"},
    {"Id": "6", "L0_S0_F0": "", "L0_S0_F2": "0.155", "Response": "1"},
]

ELECTRIC_POWER_RECORDS = [
    {
        "Date": "16/12/2006",
        "Time": "17:24:00",
        "Global_active_power": "4.216",
        "Voltage": "234.840",
        "Sub_metering_1": "0.000",
        "Sub_metering_2": "1.000",
        "Sub_metering_3": "17.000",
    },
    {
        "Date": "16/12/2006",
        "Time": "17:25:00",
        "Global_active_power": "?",
        "Voltage": "?",
        "Sub_metering_1": "?",
    },
]

STACKOVERFLOW_RECORDS = [
    {
        "Id": "80",
        "Title": "SQLStatement parameters",
        "Score": "26",
        "Tags": "<sql><sql-server><parameters>",
        "OwnerUserId": "31",
        "Body": "<p>How do I use parameters in SQL?</p>",
    },
    {
        "Id": "90",
        "Title": "Python list comprehension",
        "Score": "156",
        "Tags": "<python><list>",
        "OwnerUserId": "42",
        "Body": "<p>How do list comprehensions work?</p>",
    },
]

NYC_TAXI_RECORDS = [
    {
        "key": "2009-06-15 17:26:21",
        "fare_amount": "4.5",
        "pickup_longitude": "-73.844311",
        "pickup_latitude": "40.721319",
        "dropoff_longitude": "-73.841610",
        "dropoff_latitude": "40.712278",
        "passenger_count": "1",
    },
    {
        "key": "2010-01-05 16:52:16",
        "fare_amount": "16.9",
        "pickup_longitude": "-74.016048",
        "pickup_latitude": "40.711303",
        "dropoff_longitude": "-73.979268",
        "dropoff_latitude": "40.782004",
        "passenger_count": "2",
    },
]

DATACO_RECORDS = [
    {
        "Order Id": "1",
        "Customer Id": "100",
        "Latitude": "41.7251",
        "Longitude": "-86.2539",
        "Sales": "204.99",
        "Product Price": "59.99",
        "Order Item Quantity": "3",
        "Shipping Mode": "Standard Class",
        "Delivery Status": "Late delivery",
    },
    {
        "Order Id": "2",
        "Customer Id": "200",
        "Latitude": "33.9425",
        "Longitude": "-118.2551",
        "Sales": "120.50",
        "Product Price": "40.0",
        "Order Item Quantity": "2",
        "Shipping Mode": "Same Day",
        "Delivery Status": "Shipping on time",
    },
]


def _make_config(**overrides):
    defaults = {"type": "test", "shuffle": False, "stream_interval_ms": 1}
    defaults.update(overrides)
    return GeneratorConfig(**defaults)


# ── Registry ──


class TestGeneratorRegistry:
    def test_all_wave1_generators_registered(self):
        expected = [
            "home_credit",
            "olist",
            "instacart",
            "tmdb",
            "store_sales",
            "ieee_fraud",
            "bosch",
            "electric_power",
            "stackoverflow",
            "nyc_taxi",
            "dataco",
        ]
        for key in expected:
            assert key in generator_registry, f"'{key}' not in generator_registry"

    def test_registry_count_at_least_11(self):
        assert len(generator_registry) >= 11


# ── HomeCreditGenerator ──


class TestHomeCreditGenerator:
    @pytest.fixture
    def gen(self):
        from demiurge_testdata.generators.relational.home_credit import HomeCreditGenerator

        return HomeCreditGenerator(_make_config(), records=HOME_CREDIT_RECORDS)

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert len(result) == 2

    def test_transform_amt_fields(self, gen):
        record = gen._transform(HOME_CREDIT_RECORDS[0])
        assert isinstance(record["AMT_INCOME_TOTAL"], float)
        assert record["AMT_INCOME_TOTAL"] == 135000.0

    def test_transform_sk_id_fields(self, gen):
        record = gen._transform(HOME_CREDIT_RECORDS[0])
        assert isinstance(record["SK_ID_CURR"], int)
        assert record["SK_ID_CURR"] == 100001

    def test_transform_target(self, gen):
        record = gen._transform(HOME_CREDIT_RECORDS[1])
        assert record["TARGET"] == 1

    def test_category(self, gen):
        assert gen.category == DatasetCategory.RELATIONAL

    def test_dataset_name(self, gen):
        assert gen.dataset_name == "home_credit"


# ── OlistGenerator ──


class TestOlistGenerator:
    @pytest.fixture
    def gen(self):
        from demiurge_testdata.generators.relational.olist import OlistGenerator

        return OlistGenerator(_make_config(), records=OLIST_RECORDS)

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert len(result) == 2

    def test_transform_price(self, gen):
        record = gen._transform(OLIST_RECORDS[0])
        assert isinstance(record["price"], float)
        assert record["price"] == 29.99

    def test_category(self, gen):
        assert gen.category == DatasetCategory.RELATIONAL


# ── InstacartGenerator ──


class TestInstacartGenerator:
    @pytest.fixture
    def gen(self):
        from demiurge_testdata.generators.document.instacart import InstacartGenerator

        return InstacartGenerator(_make_config(), records=INSTACART_RECORDS)

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert len(result) == 2

    def test_transform_int_fields(self, gen):
        record = gen._transform(INSTACART_RECORDS[0])
        assert isinstance(record["order_id"], int)
        assert isinstance(record["product_id"], int)

    def test_transform_float_fields(self, gen):
        record = gen._transform(INSTACART_RECORDS[0])
        assert isinstance(record["days_since_prior_order"], float)

    def test_category(self, gen):
        assert gen.category == DatasetCategory.DOCUMENT


# ── TmdbGenerator ──


class TestTmdbGenerator:
    @pytest.fixture
    def gen(self):
        from demiurge_testdata.generators.document.tmdb import TmdbGenerator

        return TmdbGenerator(_make_config(), records=TMDB_RECORDS)

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert len(result) == 2

    def test_transform_json_parsing(self, gen):
        record = gen._transform(TMDB_RECORDS[0])
        assert isinstance(record["genres"], list)
        assert len(record["genres"]) == 2
        assert record["genres"][0]["name"] == "Action"

    def test_transform_int_fields(self, gen):
        record = gen._transform(TMDB_RECORDS[0])
        assert isinstance(record["id"], int)
        assert isinstance(record["budget"], int)
        assert record["budget"] == 237000000

    def test_transform_float_fields(self, gen):
        record = gen._transform(TMDB_RECORDS[0])
        assert isinstance(record["popularity"], float)

    def test_category(self, gen):
        assert gen.category == DatasetCategory.DOCUMENT


# ── StoreSalesGenerator ──


class TestStoreSalesGenerator:
    @pytest.fixture
    def gen(self):
        from demiurge_testdata.generators.event.store_sales import StoreSalesGenerator

        return StoreSalesGenerator(_make_config(), records=STORE_SALES_RECORDS)

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=3)
        assert len(result) == 3

    def test_transform_int_fields(self, gen):
        record = gen._transform(STORE_SALES_RECORDS[0])
        assert isinstance(record["store"], int)
        assert isinstance(record["sales"], int)
        assert record["sales"] == 13

    def test_category(self, gen):
        assert gen.category == DatasetCategory.EVENT

    async def test_stream_preserves_order(self, gen):
        results = [r async for r in gen.stream()]
        assert results[0]["date"] == "2013-01-01"
        assert results[-1]["date"] == "2013-01-03"


# ── IeeFraudGenerator ──


class TestIeeFraudGenerator:
    @pytest.fixture
    def gen(self):
        from demiurge_testdata.generators.event.ieee_fraud import IeeFraudGenerator

        return IeeFraudGenerator(_make_config(), records=IEEE_FRAUD_RECORDS)

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert len(result) == 2

    def test_transform_removes_null(self, gen):
        record = gen._transform(IEEE_FRAUD_RECORDS[1])
        assert "V1" not in record  # empty string removed
        assert "V2" not in record  # "nan" removed

    def test_transform_core_fields(self, gen):
        record = gen._transform(IEEE_FRAUD_RECORDS[0])
        assert isinstance(record["TransactionID"], int)
        assert isinstance(record["TransactionAmt"], float)
        assert record["isFraud"] == 0

    def test_category(self, gen):
        assert gen.category == DatasetCategory.EVENT


# ── BoschGenerator ──


class TestBoschGenerator:
    @pytest.fixture
    def gen(self):
        from demiurge_testdata.generators.iot.bosch import BoschGenerator

        return BoschGenerator(_make_config(), records=BOSCH_RECORDS)

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert len(result) == 2

    def test_transform_sparse(self, gen):
        record = gen._transform(BOSCH_RECORDS[0])
        # empty/nan values should be excluded
        assert "L0_S0_F2" not in record
        assert "L0_S0_F4" not in record
        # non-null values present
        assert isinstance(record["L0_S0_F0"], float)
        assert record["L0_S0_F0"] == 0.03

    def test_transform_id_response(self, gen):
        record = gen._transform(BOSCH_RECORDS[0])
        assert isinstance(record["Id"], int)
        assert isinstance(record["Response"], int)

    def test_category(self, gen):
        assert gen.category == DatasetCategory.IOT


# ── ElectricPowerGenerator ──


class TestElectricPowerGenerator:
    @pytest.fixture
    def gen(self):
        from demiurge_testdata.generators.iot.electric_power import ElectricPowerGenerator

        return ElectricPowerGenerator(_make_config(), records=ELECTRIC_POWER_RECORDS)

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert len(result) == 2

    def test_transform_missing_values(self, gen):
        record = gen._transform(ELECTRIC_POWER_RECORDS[1])
        assert record["Global_active_power"] is None  # "?" → None
        assert record["Voltage"] is None

    def test_transform_numeric(self, gen):
        record = gen._transform(ELECTRIC_POWER_RECORDS[0])
        assert isinstance(record["Global_active_power"], float)
        assert record["Global_active_power"] == 4.216

    def test_transform_preserves_datetime(self, gen):
        record = gen._transform(ELECTRIC_POWER_RECORDS[0])
        assert record["Date"] == "16/12/2006"
        assert record["Time"] == "17:24:00"

    def test_category(self, gen):
        assert gen.category == DatasetCategory.IOT


# ── StackOverflowGenerator ──


class TestStackOverflowGenerator:
    @pytest.fixture
    def gen(self):
        from demiurge_testdata.generators.text.stackoverflow import StackOverflowGenerator

        return StackOverflowGenerator(_make_config(), records=STACKOVERFLOW_RECORDS)

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert len(result) == 2

    def test_transform_tags_parsing(self, gen):
        record = gen._transform(STACKOVERFLOW_RECORDS[0])
        assert "tags_list" in record
        assert record["tags_list"] == ["sql", "sql-server", "parameters"]

    def test_transform_int_fields(self, gen):
        record = gen._transform(STACKOVERFLOW_RECORDS[0])
        assert isinstance(record["Id"], int)
        assert isinstance(record["Score"], int)
        assert record["Score"] == 26

    def test_category(self, gen):
        assert gen.category == DatasetCategory.TEXT


# ── NycTaxiGenerator ──


class TestNycTaxiGenerator:
    @pytest.fixture
    def gen(self):
        from demiurge_testdata.generators.geospatial.nyc_taxi import NycTaxiGenerator

        return NycTaxiGenerator(_make_config(), records=NYC_TAXI_RECORDS)

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert len(result) == 2

    def test_transform_geojson(self, gen):
        record = gen._transform(NYC_TAXI_RECORDS[0])
        assert "pickup_location" in record
        assert record["pickup_location"]["type"] == "Point"
        coords = record["pickup_location"]["coordinates"]
        assert len(coords) == 2
        assert abs(coords[0] - (-73.844311)) < 0.0001

    def test_transform_numeric(self, gen):
        record = gen._transform(NYC_TAXI_RECORDS[0])
        assert isinstance(record["fare_amount"], float)
        assert isinstance(record["passenger_count"], int)

    def test_category(self, gen):
        assert gen.category == DatasetCategory.GEOSPATIAL


# ── DataCoGenerator ──


class TestDataCoGenerator:
    @pytest.fixture
    def gen(self):
        from demiurge_testdata.generators.geospatial.dataco import DataCoGenerator

        return DataCoGenerator(_make_config(), records=DATACO_RECORDS)

    async def test_batch(self, gen):
        result = await gen.batch(batch_size=2)
        assert len(result) == 2

    def test_transform_coords(self, gen):
        record = gen._transform(DATACO_RECORDS[0])
        assert isinstance(record["Latitude"], float)
        assert isinstance(record["Longitude"], float)

    def test_transform_sales(self, gen):
        record = gen._transform(DATACO_RECORDS[0])
        assert isinstance(record["Sales"], float)
        assert record["Sales"] == 204.99

    def test_transform_int_fields(self, gen):
        record = gen._transform(DATACO_RECORDS[0])
        assert isinstance(record["Order Id"], int)
        assert isinstance(record["Order Item Quantity"], int)

    def test_category(self, gen):
        assert gen.category == DatasetCategory.GEOSPATIAL
