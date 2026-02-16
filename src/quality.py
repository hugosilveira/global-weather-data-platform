import logging
from typing import Dict, List, Tuple

import polars as pl

logger = logging.getLogger(__name__)


class DataQualityValidator:
    REQUIRED_COLUMNS = [
        "extraction_id",
        "recorded_at_utc",
        "event_time_utc",
        "event_date",
        "location_city",
        "location_state",
        "latitude",
        "longitude",
        "temperature_celsius",
        "precipitation_mm",
        "weather_code",
        "wind_speed_kmh",
        "relative_humidity",
    ]

    def __init__(self, quality_config: Dict[str, float]):
        self.quality_config = quality_config

    def validate(self, df: pl.DataFrame) -> Tuple[bool, List[str]]:
        errors: List[str] = []

        if df is None or df.is_empty():
            return False, ["Processed dataset is empty"]

        missing_columns = [col for col in self.REQUIRED_COLUMNS if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {', '.join(missing_columns)}")
            return False, errors

        null_critical = {
            "extraction_id": int(df["extraction_id"].null_count()),
            "event_time_utc": int(df["event_time_utc"].null_count()),
            "location_city": int(df["location_city"].null_count()),
            "location_state": int(df["location_state"].null_count()),
            "latitude": int(df["latitude"].null_count()),
            "longitude": int(df["longitude"].null_count()),
            "weather_code": int(df["weather_code"].null_count()),
        }
        for column_name, null_count in null_critical.items():
            if null_count > 0:
                errors.append(f"Column {column_name} has {null_count} null values")

        temp_min = float(self.quality_config["temperature_min_c"])
        temp_max = float(self.quality_config["temperature_max_c"])
        humidity_min = float(self.quality_config["humidity_min_pct"])
        humidity_max = float(self.quality_config["humidity_max_pct"])
        precipitation_min = float(self.quality_config["precipitation_min_mm"])
        wind_min = float(self.quality_config["wind_speed_min_kmh"])

        invalid_temp = df.filter(
            pl.col("temperature_celsius").is_not_null()
            & ((pl.col("temperature_celsius") < temp_min) | (pl.col("temperature_celsius") > temp_max))
        ).height
        if invalid_temp > 0:
            errors.append(f"Temperature out of range in {invalid_temp} rows")

        invalid_humidity = df.filter(
            pl.col("relative_humidity").is_not_null()
            & ((pl.col("relative_humidity") < humidity_min) | (pl.col("relative_humidity") > humidity_max))
        ).height
        if invalid_humidity > 0:
            errors.append(f"Humidity out of range in {invalid_humidity} rows")

        invalid_precipitation = df.filter(
            pl.col("precipitation_mm").is_not_null() & (pl.col("precipitation_mm") < precipitation_min)
        ).height
        if invalid_precipitation > 0:
            errors.append(f"Precipitation below minimum in {invalid_precipitation} rows")

        invalid_wind = df.filter(pl.col("wind_speed_kmh").is_not_null() & (pl.col("wind_speed_kmh") < wind_min)).height
        if invalid_wind > 0:
            errors.append(f"Wind speed below minimum in {invalid_wind} rows")

        duplicated_ids = int(df["extraction_id"].is_duplicated().sum())
        if duplicated_ids > 0:
            errors.append(f"Found {duplicated_ids} duplicated extraction_id values")

        if errors:
            logger.error("Data quality validation failed: %s", errors)
            return False, errors

        logger.info("Data quality validation passed")
        return True, []

    def validate_or_raise(self, df: pl.DataFrame) -> None:
        is_valid, errors = self.validate(df)
        if not is_valid:
            raise ValueError("; ".join(errors))
