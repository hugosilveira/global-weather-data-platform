import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import requests
import yaml
from prefect import flow, get_run_logger, task

from src.extract import WeatherExtractor
from src.load import WeatherLoader
from src.quality import DataQualityValidator
from src.transform import WeatherTransformer


def setup_logging(log_level: str = "INFO", log_format: str = None) -> None:
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "weather_etl.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    if root_logger.handlers:
        root_logger.handlers.clear()

    file_handler = RotatingFileHandler(log_file, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    stream_handler = logging.StreamHandler()

    formatter = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)


def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def send_alert(webhook_url: str, message: str) -> None:
    if not webhook_url:
        return

    try:
        response = requests.post(webhook_url, json={"text": message}, timeout=5)
        response.raise_for_status()
    except Exception as exc:
        logging.getLogger(__name__).error("Failed to send alert webhook: %s", exc)


def extract_weather_data(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    api_cfg = config["api"]
    locations = api_cfg.get("locations")

    if not locations:
        locations = [
            {
                "city": "default",
                "state": "",
                "latitude": api_cfg["latitude"],
                "longitude": api_cfg["longitude"],
            }
        ]

    payloads: List[Dict[str, Any]] = []
    for location in locations:
        extractor = WeatherExtractor(
            base_url=api_cfg["base_url"],
            latitude=location["latitude"],
            longitude=location["longitude"],
            params=api_cfg["params"],
            timeout_seconds=api_cfg["timeout_seconds"],
            max_retries=api_cfg["max_retries"],
            retry_backoff_seconds=api_cfg["retry_backoff_seconds"],
        )

        payload = extractor.extract()
        if not payload:
            continue

        payload["location_city"] = location.get("city", "")
        payload["location_state"] = location.get("state", "")
        payloads.append(payload)

    if not payloads:
        raise RuntimeError("Extraction step returned no payloads for configured locations")

    return payloads


def transform_weather_data(raw_payloads: List[Dict[str, Any]]) -> pl.DataFrame:
    transformer = WeatherTransformer()
    transformed_frames: List[pl.DataFrame] = []

    for raw_payload in raw_payloads:
        transformed = transformer.transform(raw_payload)
        if transformed is not None and not transformed.is_empty():
            transformed_frames.append(transformed)

    if not transformed_frames:
        raise RuntimeError("Transform step returned empty dataset")

    if len(transformed_frames) == 1:
        return transformed_frames[0]

    return pl.concat(transformed_frames, how="vertical_relaxed")


def validate_weather_data(df: pl.DataFrame, config: Dict[str, Any]) -> bool:
    validator = DataQualityValidator(config["quality"])
    validator.validate_or_raise(df)
    return True


def load_weather_data(raw_payloads: List[Dict[str, Any]], df: pl.DataFrame, config: Dict[str, Any]) -> Dict[str, bool]:
    loader = WeatherLoader(
        raw_path=config["paths"]["raw_dir"],
        processed_path=config["paths"]["processed_dir"],
        duckdb_path=config["paths"]["duckdb_path"],
    )

    output_cfg = config["output"]
    processed_formats = output_cfg.get("processed_formats")
    if not processed_formats:
        processed_formats = [output_cfg.get("processed_format", "parquet")]

    historical_filenames = output_cfg.get("historical_filenames")
    if not historical_filenames:
        historical_filenames = {"parquet": output_cfg.get("historical_filename", "weather_historical.parquet")}

    raw_statuses = [loader.save_raw_data(raw_payload, output_cfg["raw_format"]) for raw_payload in raw_payloads]
    status: Dict[str, bool] = {"raw": all(raw_statuses)}

    for file_format in processed_formats:
        processed_key = f"processed_{file_format}"
        historical_key = f"historical_{file_format}"
        historical_filename = historical_filenames.get(file_format, f"weather_historical.{file_format}")

        status[processed_key] = loader.save_processed_data(df, file_format)
        status[historical_key] = loader.append_to_historical(df, historical_filename, file_format)

    status["duckdb"] = loader.load_into_duckdb(df)

    if not all(status.values()):
        raise RuntimeError(f"Load step failed with status={status}")

    return status


@task(name="extract_weather")
def extract_weather(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    return extract_weather_data(config)


@task(name="transform_weather")
def transform_weather(raw_payloads: List[Dict[str, Any]]) -> pl.DataFrame:
    return transform_weather_data(raw_payloads)


@task(name="validate_weather")
def validate_weather(df: pl.DataFrame, config: Dict[str, Any]) -> bool:
    return validate_weather_data(df, config)


@task(name="load_weather")
def load_weather(raw_payloads: List[Dict[str, Any]], df: pl.DataFrame, config: Dict[str, Any]) -> Dict[str, bool]:
    return load_weather_data(raw_payloads, df, config)


@flow(name="weather-etl-pipeline", log_prints=False)
def run_pipeline(config_path: str = "config/config.yaml") -> bool:
    config = load_config(config_path)
    logger = get_run_logger()

    retries = int(config["orchestration"]["task_retries"])
    retry_delay = int(config["orchestration"]["task_retry_delay_seconds"])

    extract_task = extract_weather.with_options(retries=retries, retry_delay_seconds=retry_delay)
    transform_task = transform_weather.with_options(retries=retries, retry_delay_seconds=retry_delay)
    validate_task = validate_weather.with_options(retries=0)
    load_task = load_weather.with_options(retries=retries, retry_delay_seconds=retry_delay)

    try:
        logger.info("Starting flow weather-etl-pipeline")
        raw_payloads = extract_task(config)
        transformed_df = transform_task(raw_payloads)
        validate_task(transformed_df, config)
        load_status = load_task(raw_payloads, transformed_df, config)
        logger.info("Pipeline finished successfully. load_status=%s", load_status)
        return True
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        send_alert(config.get("alerts", {}).get("webhook_url", ""), f"weather-etl-pipeline failed: {exc}")
        return False


def run_pipeline_local(config_path: str = "config/config.yaml") -> bool:
    config = load_config(config_path)
    logger = logging.getLogger(__name__)

    try:
        logger.info("Starting local pipeline execution")
        raw_payloads = extract_weather_data(config)
        transformed_df = transform_weather_data(raw_payloads)
        validate_weather_data(transformed_df, config)
        load_status = load_weather_data(raw_payloads, transformed_df, config)
        logger.info("Local pipeline finished successfully. load_status=%s", load_status)
        return True
    except Exception as exc:
        logger.error("Local pipeline failed: %s", exc, exc_info=True)
        send_alert(config.get("alerts", {}).get("webhook_url", ""), f"weather-etl-local failed: {exc}")
        return False


if __name__ == "__main__":
    app_config = load_config()
    setup_logging(
        log_level=app_config["logging"]["level"],
        log_format=app_config["logging"]["format"],
    )

    use_prefect = os.getenv("USE_PREFECT_FLOW", "0").lower() in {"1", "true", "yes"}
    successful = run_pipeline() if use_prefect else run_pipeline_local()
    raise SystemExit(0 if successful else 1)
