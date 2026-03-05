"""Category D: IoT/Time-Series 데이터셋 스키마 — BoschMeasurement, ElectricPowerConsumption"""

from __future__ import annotations

from pydantic import BaseModel, Field


class BoschMeasurement(BaseModel):
    """Bosch Production Line — 생산 라인 측정값 (4000+ 피처 중 핵심)

    대부분의 피처가 NULL이므로 extra="allow"로 모든 열을 수용한다.
    """

    id: int = Field(..., alias="Id")
    response: int | None = Field(None, alias="Response", ge=0, le=1)

    model_config = {"extra": "allow", "populate_by_name": True}


class ElectricPowerConsumption(BaseModel):
    """Individual Household Electric Power Consumption — 1분 간격 가정용 전력"""

    date: str | None = Field(None, alias="Date")
    time: str | None = Field(None, alias="Time")
    global_active_power: float | None = Field(None, alias="Global_active_power")
    global_reactive_power: float | None = Field(None, alias="Global_reactive_power")
    voltage: float | None = Field(None, alias="Voltage")
    global_intensity: float | None = Field(None, alias="Global_intensity")
    sub_metering_1: float | None = Field(None, alias="Sub_metering_1")
    sub_metering_2: float | None = Field(None, alias="Sub_metering_2")
    sub_metering_3: float | None = Field(None, alias="Sub_metering_3")

    model_config = {"extra": "allow", "populate_by_name": True}


class WeatherObservation(BaseModel):
    """Weather Dataset — 기상 관측값"""

    date_time: str | None = Field(None, alias="Date/Time")
    temp_c: float | None = Field(None, alias="Temp_C")
    dew_point_temp_c: float | None = Field(None, alias="Dew Point Temp_C")
    rel_hum: float | None = Field(None, alias="Rel Hum_%")
    wind_speed_km_h: float | None = Field(None, alias="Wind Speed_km/h")
    visibility_km: float | None = Field(None, alias="Visibility_km")
    press_kpa: float | None = Field(None, alias="Press_kPa")
    weather: str | None = Field(None, alias="Weather")

    model_config = {"extra": "allow", "populate_by_name": True}


class SmartMfgReading(BaseModel):
    """Smart Manufacturing IoT — 머신 센서 데이터"""

    machine_id: str | None = None
    timestamp: str | None = None
    temperature: float | None = None
    humidity: float | None = None
    vibration: float | None = None
    power_consumption: float | None = None
    status: str | None = None

    model_config = {"extra": "allow"}


class AppliancesEnergy(BaseModel):
    """Appliances Energy Prediction — 다실 센서 + 에너지"""

    date: str | None = None
    appliances: float | None = Field(None, alias="Appliances")
    lights: float | None = Field(None, alias="lights")
    t1: float | None = Field(None, alias="T1")
    rh_1: float | None = Field(None, alias="RH_1")
    t2: float | None = Field(None, alias="T2")
    rh_2: float | None = Field(None, alias="RH_2")

    model_config = {"extra": "allow", "populate_by_name": True}
