from src.quality import DataQualityValidator
from src.transform import WeatherTransformer


def _raw_payload(relative_humidity: float = 70.0):
    return {
        "location_city": "Sao Paulo",
        "location_state": "SP",
        "latitude": -23.5505,
        "longitude": -46.6333,
        "extraction_id": "abc123def4567890",
        "request_time_utc": "2026-02-16T14:00:00+00:00",
        "current": {
            "time": "2026-02-16T14:00:00Z",
            "temperature_2m": 25.2,
            "precipitation": 0.0,
            "weather_code": 1,
            "wind_speed_10m": 10.0,
            "relative_humidity_2m": relative_humidity,
        },
    }


def _quality_config():
    return {
        "temperature_min_c": -90,
        "temperature_max_c": 60,
        "humidity_min_pct": 0,
        "humidity_max_pct": 100,
        "precipitation_min_mm": 0,
        "wind_speed_min_kmh": 0,
    }


def test_transform_and_quality_success():
    transformer = WeatherTransformer()
    validator = DataQualityValidator(_quality_config())

    df = transformer.transform(_raw_payload())

    assert df is not None
    assert df.height == 1

    is_valid, errors = validator.validate(df)
    assert is_valid is True
    assert errors == []


def test_quality_fails_for_out_of_range_humidity():
    transformer = WeatherTransformer()
    validator = DataQualityValidator(_quality_config())

    df = transformer.transform(_raw_payload(relative_humidity=140.0))

    is_valid, errors = validator.validate(df)
    assert is_valid is False
    assert any("Humidity out of range" in msg for msg in errors)


def test_transform_includes_location_fields():
    transformer = WeatherTransformer()

    df = transformer.transform(_raw_payload())

    assert df is not None
    assert df["location_city"][0] == "Sao Paulo"
    assert df["location_state"][0] == "SP"
