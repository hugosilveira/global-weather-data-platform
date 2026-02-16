import duckdb
import polars as pl

from src.load import WeatherLoader


def _processed_df():
    return pl.DataFrame(
        [
            {
                "extraction_id": "abc123def4567890",
                "recorded_at_utc": "2026-02-16T14:00:00+00:00",
                "event_time_utc": "2026-02-16T14:00:00+00:00",
                "event_date": "2026-02-16",
                "location_city": "Sao Paulo",
                "location_state": "SP",
                "latitude": -23.5505,
                "longitude": -46.6333,
                "temperature_celsius": 25.2,
                "precipitation_mm": 0.0,
                "weather_code": 1,
                "weather_description": "Mainly clear",
                "wind_speed_kmh": 10.0,
                "relative_humidity": 70.0,
                "request_time_utc": "2026-02-16T14:00:00+00:00",
            }
        ]
    )


def _raw_payload():
    return {
        "extraction_id": "abc123def4567890",
        "location_city": "Sao Paulo",
        "location_state": "SP",
        "latitude": -23.5505,
        "longitude": -46.6333,
        "current": {
            "time": "2026-02-16T14:00:00Z",
        },
    }


def test_loader_saves_partition_and_duckdb(tmp_path):
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    duckdb_path = tmp_path / "warehouse" / "weather.duckdb"

    loader = WeatherLoader(str(raw_dir), str(processed_dir), str(duckdb_path))
    df = _processed_df()

    assert loader.save_raw_data(_raw_payload(), "json") is True
    assert loader.save_processed_data(df, "parquet") is True
    assert loader.save_processed_data(df, "csv") is True

    partition_dir = processed_dir / "event_date=2026-02-16"
    assert partition_dir.exists()
    assert any(partition_dir.glob("*.parquet"))
    assert any(partition_dir.glob("*.csv"))

    assert loader.append_to_historical(df, "weather_historical.parquet") is True
    assert loader.append_to_historical(df, "weather_historical.parquet") is True
    assert loader.append_to_historical(df, "weather_historical.csv", "csv") is True
    assert loader.append_to_historical(df, "weather_historical.csv", "csv") is True

    historical_df = pl.read_parquet(processed_dir / "weather_historical.parquet")
    assert historical_df.height == 1
    historical_csv_df = pl.read_csv(processed_dir / "weather_historical.csv")
    assert historical_csv_df.height == 1

    assert loader.load_into_duckdb(df) is True

    conn = duckdb.connect(str(duckdb_path))
    count = conn.execute("SELECT COUNT(*) FROM analytics.weather_facts").fetchone()[0]
    conn.close()

    assert count == 1


def test_loader_handles_duckdb_schema_evolution(tmp_path):
    duckdb_path = tmp_path / "warehouse" / "weather.duckdb"
    loader = WeatherLoader(str(tmp_path / "raw"), str(tmp_path / "processed"), str(duckdb_path))

    old_df = _processed_df()
    assert loader.load_into_duckdb(old_df) is True

    new_df = old_df.with_columns([pl.lit("open-meteo").alias("source_system")])
    assert loader.load_into_duckdb(new_df) is True

    conn = duckdb.connect(str(duckdb_path))
    columns = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'analytics' AND table_name = 'weather_facts'
        """
    ).fetchall()
    conn.close()

    col_names = {row[0] for row in columns}
    assert "source_system" in col_names
