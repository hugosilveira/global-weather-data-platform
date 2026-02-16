import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class WeatherExtractor:
    def __init__(
        self,
        base_url: str,
        latitude: float,
        longitude: float,
        params: List[str],
        timeout_seconds: int = 15,
        max_retries: int = 3,
        retry_backoff_seconds: int = 1,
    ):
        self.base_url = base_url
        self.latitude = latitude
        self.longitude = longitude
        self.params = params
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

    def _request_weather(self, query_params: Dict[str, Any]) -> Dict[str, Any]:
        retrying = Retrying(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=self.retry_backoff_seconds, min=1, max=30),
            retry=retry_if_exception_type(requests.exceptions.RequestException),
            reraise=True,
        )

        for attempt in retrying:
            with attempt:
                response = requests.get(self.base_url, params=query_params, timeout=self.timeout_seconds)
                response.raise_for_status()
                return response.json()

        raise RuntimeError("Retry loop finished unexpectedly")

    def extract(self) -> Optional[Dict[str, Any]]:
        try:
            logger.info(
                "Starting extraction for coordinates lat=%s lon=%s",
                self.latitude,
                self.longitude,
            )

            query_params = {
                "latitude": self.latitude,
                "longitude": self.longitude,
                "current": ",".join(self.params),
                "timezone": "UTC",
            }

            data = self._request_weather(query_params)

            request_time_utc = datetime.now(timezone.utc)
            source_event_time = data.get("current", {}).get("time") or request_time_utc.isoformat()

            idempotency_seed = f"{self.latitude}:{self.longitude}:{source_event_time}"
            extraction_id = hashlib.sha1(idempotency_seed.encode("utf-8")).hexdigest()[:16]

            data["extraction_id"] = extraction_id
            data["request_time_utc"] = request_time_utc.isoformat()
            data["extraction_success"] = True

            logger.info("Extraction succeeded with extraction_id=%s", extraction_id)
            return data

        except requests.exceptions.RequestException as exc:
            logger.error("API request failed after retries: %s", exc)
            return None
        except Exception as exc:
            logger.error("Unexpected extraction error: %s", exc, exc_info=True)
            return None
