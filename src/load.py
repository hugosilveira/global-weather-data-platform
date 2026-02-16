import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import duckdb
import polars as pl

logger = logging.getLogger(__name__)


class WeatherLoader:
    DEPRECATED_COLUMNS = ("owner_name", "city_alias", "profile_style", "project_signature")

    def __init__(self, raw_path: str, processed_path: str, duckdb_path: str):
        self.raw_path = Path(raw_path)
        self.processed_path = Path(processed_path)
        self.duckdb_path = Path(duckdb_path)

        self.raw_path.mkdir(parents=True, exist_ok=True)
        self.processed_path.mkdir(parents=True, exist_ok=True)
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    def _sanitize_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        columns_to_drop = [column for column in self.DEPRECATED_COLUMNS if column in df.columns]
        if columns_to_drop:
            return df.drop(columns_to_drop)
        return df

    def save_raw_data(self, data: Dict[str, Any], file_format: str = "json") -> bool:
        try:
            if file_format != "json":
                logger.error("Unsupported raw format: %s", file_format)
                return False

            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            extraction_id = data.get("extraction_id", "unknown")
            filename = f"weather_raw_{timestamp}_{extraction_id}.json"
            filepath = self.raw_path / filename

            with open(filepath, "w", encoding="utf-8") as handle:
                json.dump(data, handle, indent=2, ensure_ascii=False)

            logger.info("Raw payload saved at %s", filepath)
            return True

        except Exception as exc:
            logger.error("Failed saving raw payload: %s", exc, exc_info=True)
            return False

    def save_processed_data(self, df: pl.DataFrame, file_format: str = "parquet") -> bool:
        try:
            if df is None or df.is_empty():
                logger.warning("No processed data to save")
                return False

            if file_format not in {"parquet", "csv"}:
                logger.error("Unsupported processed format: %s", file_format)
                return False

            clean_df = self._sanitize_columns(df)
            partition_value = str(clean_df["event_date"][0])
            partition_dir = self.processed_path / f"event_date={partition_value}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            extraction_id = str(clean_df["extraction_id"][0])
            filename = f"weather_processed_{timestamp}_{extraction_id}.{file_format}"
            filepath = partition_dir / filename

            if file_format == "parquet":
                clean_df.write_parquet(filepath, compression="zstd")
            else:
                clean_df.write_csv(filepath)

            logger.info("Processed %s saved at %s", file_format, filepath)
            return True

        except Exception as exc:
            logger.error("Failed saving processed %s: %s", file_format, exc, exc_info=True)
            return False

    def append_to_historical(self, df: pl.DataFrame, historical_filename: str, file_format: str = "parquet") -> bool:
        try:
            if df is None or df.is_empty():
                logger.warning("No processed data to append into historical dataset")
                return False

            if file_format not in {"parquet", "csv"}:
                logger.error("Unsupported historical format: %s", file_format)
                return False

            clean_df = self._sanitize_columns(df)
            historical_path = self.processed_path / historical_filename

            if historical_path.exists():
                historical_df = (
                    pl.read_parquet(historical_path)
                    if file_format == "parquet"
                    else pl.read_csv(historical_path)
                )
                historical_df = self._sanitize_columns(historical_df)
                combined_df = pl.concat([historical_df, clean_df], how="diagonal_relaxed")
                deduped_df = combined_df.unique(subset=["extraction_id"], keep="last")
            else:
                deduped_df = clean_df.unique(subset=["extraction_id"], keep="last")

            if file_format == "parquet":
                deduped_df.write_parquet(historical_path, compression="zstd")
            else:
                deduped_df.write_csv(historical_path)

            logger.info(
                "Historical %s updated at %s. rows=%s",
                file_format,
                historical_path,
                deduped_df.height,
            )
            return True

        except Exception as exc:
            logger.error("Failed updating historical dataset: %s", exc, exc_info=True)
            return False

    @staticmethod
    def _to_duckdb_type(dtype: pl.DataType) -> str:
        if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
            return "BIGINT"
        if dtype in (pl.Float32, pl.Float64):
            return "DOUBLE"
        if dtype == pl.Boolean:
            return "BOOLEAN"
        return "VARCHAR"

    @staticmethod
    def _quote_ident(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    def _ensure_duckdb_schema(self, conn: duckdb.DuckDBPyConnection, table_name: str, df: pl.DataFrame) -> None:
        schema_name, relation_name = table_name.split(".", 1)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._quote_ident(schema_name)}")

        table_exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [schema_name, relation_name],
        ).fetchone()[0]

        if not table_exists:
            return

        existing_columns = {
            row[1]: row[2]
            for row in conn.execute(
                f"PRAGMA table_info({self._quote_ident(schema_name)}.{self._quote_ident(relation_name)})"
            ).fetchall()
        }

        for column_name, column_dtype in zip(df.columns, df.dtypes):
            if column_name not in existing_columns:
                duckdb_type = self._to_duckdb_type(column_dtype)
                conn.execute(
                    f"ALTER TABLE {self._quote_ident(schema_name)}.{self._quote_ident(relation_name)} "
                    f"ADD COLUMN {self._quote_ident(column_name)} {duckdb_type}"
                )

    def _drop_deprecated_duckdb_columns(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        schema_name, relation_name = table_name.split(".", 1)
        existing_columns = {
            row[1]
            for row in conn.execute(
                f"PRAGMA table_info({self._quote_ident(schema_name)}.{self._quote_ident(relation_name)})"
            ).fetchall()
        }

        for column in self.DEPRECATED_COLUMNS:
            if column in existing_columns:
                conn.execute(
                    f"ALTER TABLE {self._quote_ident(schema_name)}.{self._quote_ident(relation_name)} "
                    f"DROP COLUMN {self._quote_ident(column)}"
                )

    def load_into_duckdb(self, df: pl.DataFrame, table_name: str = "analytics.weather_facts") -> bool:
        try:
            if df is None or df.is_empty():
                logger.warning("No processed data available for DuckDB load")
                return False

            clean_df = self._sanitize_columns(df)
            relation_name = "incoming_weather_data"
            conn = duckdb.connect(str(self.duckdb_path))
            schema_name, _ = table_name.split(".", 1)
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._quote_ident(schema_name)}")

            conn.register(relation_name, clean_df.to_arrow())
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {relation_name} WHERE 1=0")
            self._ensure_duckdb_schema(conn, table_name, clean_df)
            self._drop_deprecated_duckdb_columns(conn, table_name)

            quoted_columns = ", ".join(self._quote_ident(column) for column in clean_df.columns)
            conn.execute(
                f"DELETE FROM {table_name} USING {relation_name} "
                f"WHERE {table_name}.extraction_id = {relation_name}.extraction_id"
            )
            conn.execute(f"INSERT INTO {table_name} ({quoted_columns}) SELECT {quoted_columns} FROM {relation_name}")
            conn.unregister(relation_name)
            conn.close()

            logger.info("DuckDB table %s updated at %s", table_name, self.duckdb_path)
            return True

        except Exception as exc:
            logger.error("Failed loading into DuckDB: %s", exc, exc_info=True)
            return False
