"""Wave 3 제너레이터 9종 단위 테스트 — 레지스트리, 변환, 모드별 동작"""

from __future__ import annotations

import pytest

# Force imports to trigger registration
import demiurge_testdata.generators.document.foodcom
import demiurge_testdata.generators.document.yelp
import demiurge_testdata.generators.event.bitcoin
import demiurge_testdata.generators.event.network_traffic
import demiurge_testdata.generators.geospatial.geolife
import demiurge_testdata.generators.iot.appliances_energy
import demiurge_testdata.generators.relational.euro_soccer
import demiurge_testdata.generators.relational.northwind
import demiurge_testdata.generators.text.github_metadata  # noqa: F401
from demiurge_testdata.core.config import GeneratorConfig
from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.document.foodcom import FoodComGenerator
from demiurge_testdata.generators.document.yelp import YelpGenerator
from demiurge_testdata.generators.event.bitcoin import BitcoinGenerator
from demiurge_testdata.generators.event.network_traffic import (
    NetworkTrafficGenerator,
)
from demiurge_testdata.generators.geospatial.geolife import GeoLifeGenerator
from demiurge_testdata.generators.iot.appliances_energy import (
    AppliancesEnergyGenerator,
)
from demiurge_testdata.generators.relational.euro_soccer import (
    EuroSoccerGenerator,
)
from demiurge_testdata.generators.relational.northwind import (
    NorthwindGenerator,
)
from demiurge_testdata.generators.text.github_metadata import (
    GitHubMetadataGenerator,
)

# ── 샘플 데이터 ──

EURO_SOCCER_RECORDS = [
    {
        "match_api_id": "483086",
        "home_team_api_id": "9987",
        "away_team_api_id": "9993",
        "home_team_goal": "1",
        "away_team_goal": "0",
        "season": "2008/2009",
        "date": "2008-08-17",
    },
]

NORTHWIND_RECORDS = [
    {
        "OrderID": "10248",
        "CustomerID": "VINET",
        "EmployeeID": "5",
        "OrderDate": "1996-07-04",
        "Freight": "32.38",
        "ShipName": "Vins et alcools Chevalier",
        "ShipCity": "Reims",
        "ShipCountry": "France",
    },
]

YELP_RECORDS = [
    {
        "business_id": "Pns2l4eNsfO8kk83dixA6A",
        "name": "Abby Rappoport",
        "city": "Santa Clara",
        "state": "CA",
        "latitude": "37.3520",
        "longitude": "-121.9548",
        "stars": "4.0",
        "review_count": "42",
        "categories": "Doctors, Health & Medical",
    },
]

FOODCOM_RECORDS = [
    {
        "id": "137739",
        "name": "arriba baked winter squash",
        "minutes": "55",
        "n_steps": "11",
        "n_ingredients": "7",
        "description": "A delicious squash recipe",
        "ingredients": "['winter squash', 'olive oil', 'salt']",
        "steps": "['preheat oven', 'cut squash', 'bake']",
    },
]

NETWORK_TRAFFIC_RECORDS = [
    {
        "Source IP": "192.168.1.1",
        "Destination IP": "10.0.0.1",
        "Source Port": "44322",
        "Destination Port": "443",
        "Protocol": "TCP",
        "Application": "HTTPS",
        "Flow Duration": "1234567.0",
    },
]

BITCOIN_RECORDS = [
    {
        "Timestamp": "1325317920",
        "Open": "4.39",
        "High": "4.39",
        "Low": "4.39",
        "Close": "4.39",
        "Volume_(BTC)": "0.455581",
        "Volume_(Currency)": "2.0",
        "Weighted_Price": "4.39",
    },
]

APPLIANCES_ENERGY_RECORDS = [
    {
        "date": "2016-01-11 17:00:00",
        "Appliances": "60",
        "lights": "30",
        "T1": "19.89",
        "RH_1": "47.596667",
        "T2": "19.2",
        "RH_2": "44.79",
    },
]

GITHUB_METADATA_RECORDS = [
    {
        "repo_id": "12345",
        "name": "awesome-project",
        "full_name": "user/awesome-project",
        "owner": "user",
        "description": "An awesome project",
        "language": "Python",
        "stargazers_count": "150",
        "forks_count": "30",
        "topics": "python,ml,data-science",
        "created_at": "2020-01-15T00:00:00Z",
    },
]

GEOLIFE_RECORDS = [
    {
        "latitude": "39.984702",
        "longitude": "116.318417",
        "altitude": "492",
        "date": "2008-10-23",
        "time": "02:53:04",
        "user_id": "000",
    },
]


def _cfg(**overrides):
    defaults = {"type": "test", "shuffle": False, "stream_interval_ms": 1}
    defaults.update(overrides)
    return GeneratorConfig(**defaults)


# ── Registry ──


class TestW3Registry:
    @pytest.mark.parametrize(
        "key",
        [
            "euro_soccer",
            "northwind",
            "yelp",
            "foodcom",
            "network_traffic",
            "bitcoin",
            "appliances_energy",
            "github_metadata",
            "geolife",
        ],
    )
    def test_registered(self, key):
        assert key in generator_registry

    def test_registry_count_at_least_32(self):
        assert len(generator_registry) >= 32


# ── EuroSoccerGenerator ──


class TestEuroSoccerGenerator:
    @pytest.fixture
    def gen(self):
        return EuroSoccerGenerator(_cfg(), records=EURO_SOCCER_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "euro_soccer"
        assert gen.category == DatasetCategory.RELATIONAL

    def test_match_api_id_int(self, gen):
        r = gen._transform(EURO_SOCCER_RECORDS[0])
        assert isinstance(r["match_api_id"], int)
        assert r["match_api_id"] == 483086

    def test_goals_int(self, gen):
        r = gen._transform(EURO_SOCCER_RECORDS[0])
        assert isinstance(r["home_team_goal"], int)
        assert r["home_team_goal"] == 1


# ── NorthwindGenerator ──


class TestNorthwindGenerator:
    @pytest.fixture
    def gen(self):
        return NorthwindGenerator(_cfg(), records=NORTHWIND_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "northwind"
        assert gen.category == DatasetCategory.RELATIONAL

    def test_order_id_int(self, gen):
        r = gen._transform(NORTHWIND_RECORDS[0])
        assert isinstance(r["OrderID"], int)
        assert r["OrderID"] == 10248

    def test_freight_float(self, gen):
        r = gen._transform(NORTHWIND_RECORDS[0])
        assert isinstance(r["Freight"], float)
        assert r["Freight"] == pytest.approx(32.38)


# ── YelpGenerator ──


class TestYelpGenerator:
    @pytest.fixture
    def gen(self):
        return YelpGenerator(_cfg(), records=YELP_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "yelp"
        assert gen.category == DatasetCategory.DOCUMENT

    def test_stars_float(self, gen):
        r = gen._transform(YELP_RECORDS[0])
        assert isinstance(r["stars"], float)
        assert r["stars"] == 4.0

    def test_categories_list(self, gen):
        r = gen._transform(YELP_RECORDS[0])
        assert isinstance(r["categories_list"], list)
        assert "Doctors" in r["categories_list"]

    def test_review_count_int(self, gen):
        r = gen._transform(YELP_RECORDS[0])
        assert isinstance(r["review_count"], int)
        assert r["review_count"] == 42


# ── FoodComGenerator ──


class TestFoodComGenerator:
    @pytest.fixture
    def gen(self):
        return FoodComGenerator(_cfg(), records=FOODCOM_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "foodcom"
        assert gen.category == DatasetCategory.DOCUMENT

    def test_ingredients_list(self, gen):
        r = gen._transform(FOODCOM_RECORDS[0])
        assert isinstance(r["ingredients"], list)
        assert "winter squash" in r["ingredients"]

    def test_id_int(self, gen):
        r = gen._transform(FOODCOM_RECORDS[0])
        assert isinstance(r["id"], int)
        assert r["id"] == 137739

    def test_minutes_int(self, gen):
        r = gen._transform(FOODCOM_RECORDS[0])
        assert isinstance(r["minutes"], int)


# ── NetworkTrafficGenerator ──


class TestNetworkTrafficGenerator:
    @pytest.fixture
    def gen(self):
        return NetworkTrafficGenerator(_cfg(), records=NETWORK_TRAFFIC_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "network_traffic"
        assert gen.category == DatasetCategory.EVENT

    def test_ports_int(self, gen):
        r = gen._transform(NETWORK_TRAFFIC_RECORDS[0])
        assert isinstance(r["Source Port"], int)
        assert isinstance(r["Destination Port"], int)

    def test_flow_duration_float(self, gen):
        r = gen._transform(NETWORK_TRAFFIC_RECORDS[0])
        assert isinstance(r["Flow Duration"], float)


# ── BitcoinGenerator ──


class TestBitcoinGenerator:
    @pytest.fixture
    def gen(self):
        return BitcoinGenerator(_cfg(), records=BITCOIN_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "bitcoin"
        assert gen.category == DatasetCategory.EVENT

    def test_timestamp_int(self, gen):
        r = gen._transform(BITCOIN_RECORDS[0])
        assert isinstance(r["Timestamp"], int)

    def test_ohlcv_float(self, gen):
        r = gen._transform(BITCOIN_RECORDS[0])
        assert isinstance(r["Open"], float)
        assert isinstance(r["High"], float)
        assert isinstance(r["Close"], float)
        assert isinstance(r["Volume_(BTC)"], float)

    def test_weighted_price_float(self, gen):
        r = gen._transform(BITCOIN_RECORDS[0])
        assert isinstance(r["Weighted_Price"], float)
        assert r["Weighted_Price"] == pytest.approx(4.39)


# ── AppliancesEnergyGenerator ──


class TestAppliancesEnergyGenerator:
    @pytest.fixture
    def gen(self):
        return AppliancesEnergyGenerator(_cfg(), records=APPLIANCES_ENERGY_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "appliances_energy"
        assert gen.category == DatasetCategory.IOT

    def test_appliances_float(self, gen):
        r = gen._transform(APPLIANCES_ENERGY_RECORDS[0])
        assert isinstance(r["Appliances"], float)
        assert r["Appliances"] == 60.0

    def test_temperature_float(self, gen):
        r = gen._transform(APPLIANCES_ENERGY_RECORDS[0])
        assert isinstance(r["T1"], float)
        assert r["T1"] == pytest.approx(19.89)

    def test_date_preserved(self, gen):
        r = gen._transform(APPLIANCES_ENERGY_RECORDS[0])
        assert r["date"] == "2016-01-11 17:00:00"


# ── GitHubMetadataGenerator ──


class TestGitHubMetadataGenerator:
    @pytest.fixture
    def gen(self):
        return GitHubMetadataGenerator(_cfg(), records=GITHUB_METADATA_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "github_metadata"
        assert gen.category == DatasetCategory.TEXT

    def test_stargazers_int(self, gen):
        r = gen._transform(GITHUB_METADATA_RECORDS[0])
        assert isinstance(r["stargazers_count"], int)
        assert r["stargazers_count"] == 150

    def test_topics_list(self, gen):
        r = gen._transform(GITHUB_METADATA_RECORDS[0])
        assert isinstance(r["topics_list"], list)
        assert "python" in r["topics_list"]

    def test_language_string(self, gen):
        r = gen._transform(GITHUB_METADATA_RECORDS[0])
        assert r["language"] == "Python"


# ── GeoLifeGenerator ──


class TestGeoLifeGenerator:
    @pytest.fixture
    def gen(self):
        return GeoLifeGenerator(_cfg(), records=GEOLIFE_RECORDS)

    def test_properties(self, gen):
        assert gen.dataset_name == "geolife"
        assert gen.category == DatasetCategory.GEOSPATIAL

    def test_coordinates_float(self, gen):
        r = gen._transform(GEOLIFE_RECORDS[0])
        assert isinstance(r["latitude"], float)
        assert isinstance(r["longitude"], float)

    def test_geojson_point(self, gen):
        r = gen._transform(GEOLIFE_RECORDS[0])
        loc = r["location"]
        assert loc["type"] == "Point"
        assert len(loc["coordinates"]) == 2

    def test_altitude_float(self, gen):
        r = gen._transform(GEOLIFE_RECORDS[0])
        assert isinstance(r["altitude"], float)
