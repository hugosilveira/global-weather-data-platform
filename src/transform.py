import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import polars as pl

logger = logging.getLogger(__name__)


class WeatherTransformer:
    def __init__(self):
        self.weather_codes = {
            0: "Clear sky",
            1: "Mainly clear",
            2: "Partly cloudy",
            3: "Overcast",
            45: "Fog",
            48: "Depositing rime fog",
            51: "Light drizzle",
            53: "Moderate drizzle",
            55: "Dense drizzle",
            61: "Slight rain",
            63: "Moderate rain",
            65: "Heavy rain",
            71: "Slight snow",
            73: "Moderate snow",
            75: "Heavy snow",
            95: "Thunderstorm",
        }

    def transform(self, raw_data: Dict[str, Any]) -> Optional[pl.DataFrame]:
        try:
            logger.info("Starting weather data transformation")

            if not raw_data or "current" not in raw_data:
                logger.error("Invalid payload: missing current weather object")
                return None

            current = raw_data["current"]
            event_time_raw = current.get("time")
            if event_time_raw:
                event_time = datetime.fromisoformat(event_time_raw.replace("Z", "+00:00"))
            else:
                event_time = datetime.now(timezone.utc)

            transformed_data = {
                "extraction_id": raw_data.get("extraction_id"),
                "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
                "event_time_utc": event_time.isoformat(),
                "event_date": event_time.date().isoformat(),
                "location_city": raw_data.get("location_city", ""),
                "location_state": raw_data.get("location_state", ""),
                "latitude": raw_data.get("latitude"),
                "longitude": raw_data.get("longitude"),
                "temperature_celsius": current.get("temperature_2m"),
                "precipitation_mm": current.get("precipitation"),
                "weather_code": current.get("weather_code"),
                "weather_description": self.weather_codes.get(current.get("weather_code"), "Unknown"),
                "wind_speed_kmh": current.get("wind_speed_10m"),
                "relative_humidity": current.get("relative_humidity_2m"),
                "request_time_utc": raw_data.get("request_time_utc"),
            }

            df = pl.DataFrame([transformed_data]).with_columns(
                [
                    pl.col("latitude").cast(pl.Float64),
                    pl.col("longitude").cast(pl.Float64),
                    pl.col("temperature_celsius").cast(pl.Float64),
                    pl.col("precipitation_mm").cast(pl.Float64),
                    pl.col("weather_code").cast(pl.Int64),
                    pl.col("wind_speed_kmh").cast(pl.Float64),
                    pl.col("relative_humidity").cast(pl.Float64),
                ]
            )

            logger.info("Transformation finished. rows=%s", df.height)
            return df

        except Exception as exc:
            logger.error("Transformation failed: %s", exc, exc_info=True)
            return None
