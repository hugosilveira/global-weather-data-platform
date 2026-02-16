import argparse
from typing import Iterable, List, Sequence, Tuple

import duckdb


def print_table(columns: Sequence[str], rows: Iterable[Tuple]) -> None:
    rows_list: List[Tuple] = list(rows)
    if not rows_list:
        print("No rows found.")
        return

    str_rows = [tuple("" if value is None else str(value) for value in row) for row in rows_list]
    widths = [len(col) for col in columns]
    for row in str_rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(value))

    header = " | ".join(columns[i].ljust(widths[i]) for i in range(len(columns)))
    separator = "-+-".join("-" * widths[i] for i in range(len(columns)))
    print(header)
    print(separator)
    for row in str_rows:
        print(" | ".join(row[i].ljust(widths[i]) for i in range(len(columns))))


def run_query(db_path: str, sql: str, params: Sequence = ()) -> None:
    conn = duckdb.connect(db_path)
    try:
        result = conn.execute(sql, params)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        print_table(columns, rows)
    finally:
        conn.close()


def latest_records(db_path: str, limit: int) -> None:
    sql = """
    WITH base AS (
      SELECT
        COALESCE(NULLIF(location_city, ''), 'Unknown') AS location_city,
        COALESCE(NULLIF(location_state, ''), 'NA') AS location_state,
        event_time_utc,
        temperature_celsius,
        precipitation_mm,
        weather_description
      FROM analytics.weather_facts
    )
    SELECT
      location_city,
      location_state,
      event_time_utc,
      temperature_celsius,
      precipitation_mm,
      weather_description
    FROM base
    WHERE location_city <> 'Unknown'
    ORDER BY event_time_utc DESC, location_city ASC
    LIMIT ?
    """
    run_query(db_path, sql, [limit])


def avg_temperature(db_path: str, date_from: str, date_to: str) -> None:
    sql = """
    WITH base AS (
      SELECT
        COALESCE(NULLIF(location_city, ''), 'Unknown') AS location_city,
        COALESCE(NULLIF(location_state, ''), 'NA') AS location_state,
        temperature_celsius,
        event_date
      FROM analytics.weather_facts
    )
    SELECT
      location_city,
      location_state,
      ROUND(AVG(temperature_celsius), 2) AS avg_temperature_c,
      COUNT(*) AS records
    FROM base
    WHERE event_date BETWEEN ? AND ?
      AND location_city <> 'Unknown'
    GROUP BY location_city, location_state
    ORDER BY avg_temperature_c DESC
    """
    run_query(db_path, sql, [date_from, date_to])


def precipitation_summary(db_path: str, date_from: str, date_to: str) -> None:
    sql = """
    WITH base AS (
      SELECT
        COALESCE(NULLIF(location_city, ''), 'Unknown') AS location_city,
        COALESCE(NULLIF(location_state, ''), 'NA') AS location_state,
        precipitation_mm,
        event_date
      FROM analytics.weather_facts
    )
    SELECT
      location_city,
      location_state,
      ROUND(SUM(precipitation_mm), 2) AS total_precipitation_mm,
      ROUND(AVG(precipitation_mm), 2) AS avg_precipitation_mm,
      COUNT(*) AS records
    FROM base
    WHERE event_date BETWEEN ? AND ?
      AND location_city <> 'Unknown'
    GROUP BY location_city, location_state
    ORDER BY total_precipitation_mm DESC, location_city ASC
    """
    run_query(db_path, sql, [date_from, date_to])


def city_snapshot(db_path: str, city: str, limit: int) -> None:
    sql = """
    SELECT
      location_city,
      location_state,
      event_time_utc,
      temperature_celsius,
      relative_humidity,
      wind_speed_kmh,
      precipitation_mm,
      weather_description
    FROM analytics.weather_facts
    WHERE lower(location_city) = lower(?)
    ORDER BY event_time_utc DESC
    LIMIT ?
    """
    run_query(db_path, sql, [city, limit])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query analytics.weather_facts from DuckDB.")
    parser.add_argument(
        "--db",
        default="data/warehouse/weather.duckdb",
        help="DuckDB database path (default: data/warehouse/weather.duckdb).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    latest_parser = subparsers.add_parser("latest", help="Show latest weather records.")
    latest_parser.add_argument("--limit", type=int, default=20, help="Max number of rows (default: 20).")

    avg_temp_parser = subparsers.add_parser("avg-temp", help="Average temperature by city for a date range.")
    avg_temp_parser.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD.")
    avg_temp_parser.add_argument("--to", dest="date_to", required=True, help="End date YYYY-MM-DD.")

    rain_parser = subparsers.add_parser("rain", help="Precipitation summary by city for a date range.")
    rain_parser.add_argument("--from", dest="date_from", required=True, help="Start date YYYY-MM-DD.")
    rain_parser.add_argument("--to", dest="date_to", required=True, help="End date YYYY-MM-DD.")

    city_parser = subparsers.add_parser("city", help="Latest records for a single city.")
    city_parser.add_argument("--name", dest="city_name", required=True, help="City name (example: Sao Paulo).")
    city_parser.add_argument("--limit", type=int, default=10, help="Max number of rows (default: 10).")

    sql_parser = subparsers.add_parser("sql", help="Run custom SQL query.")
    sql_parser.add_argument("--query", required=True, help="SQL statement to execute.")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "latest":
        latest_records(args.db, args.limit)
    elif args.command == "avg-temp":
        avg_temperature(args.db, args.date_from, args.date_to)
    elif args.command == "rain":
        precipitation_summary(args.db, args.date_from, args.date_to)
    elif args.command == "city":
        city_snapshot(args.db, args.city_name, args.limit)
    elif args.command == "sql":
        run_query(args.db, args.query)


if __name__ == "__main__":
    main()
