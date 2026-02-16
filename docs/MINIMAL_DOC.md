# Minimal Documentation

## EN

## Purpose

Professional weather ETL designed for stable ingestion, validated transformation, and BI-ready outputs.

## Data flow

1. `extract.py`
- Calls Open-Meteo current weather endpoint.
- Uses retry/backoff.
- Generates deterministic `extraction_id`.

2. `transform.py`
- Converts each payload into a typed Polars row.
- Adds business fields: `location_city`, `location_state`, timestamps, metrics.

3. `quality.py`
- Validates required schema.
- Applies null/range checks.
- Blocks duplicated `extraction_id` within the batch.

4. `load.py`
- Saves raw JSON files.
- Saves partitioned Parquet and CSV (`event_date=YYYY-MM-DD`).
- Maintains deduplicated historical Parquet and CSV.
- Upserts records into DuckDB `analytics.weather_facts`.
- Handles schema evolution for new columns.

## Configured locations

- Sao Paulo (SP)
- Rio de Janeiro (RJ)
- Brasilia (DF)
- Belo Horizonte (MG)
- Salvador (BA)
- Curitiba (PR)
- Foz do Iguacu (PR)

## Commands

```bash
pip install -r requirements.txt
python main.py
pytest -q
```

## DataViz (Power BI, Tableau, others)

1. CSV (recommended)
- File: `data/processed/weather/weather_historical.csv`
- Default project-relative path:
  `<project-root>/data/processed/weather/weather_historical.csv`
- Configurable via `config/config.yaml` (`paths.processed_dir` and `output.historical_filenames.csv`).

2. DuckDB via ODBC/driver
- Database file:
  `<project-root>/data/warehouse/weather.duckdb`
- Table:
  `analytics.weather_facts`
- Configurable via `config/config.yaml` (`paths.duckdb_path`).

---

## PT-BR

## Objetivo

ETL de clima para uso profissional, com ingestao estavel, transformacao validada e saidas prontas para BI.

## Fluxo de dados

1. `extract.py`
- Chama o endpoint de clima atual da Open-Meteo.
- Usa retry/backoff.
- Gera `extraction_id` deterministico.

2. `transform.py`
- Converte cada payload em uma linha tipada com Polars.
- Adiciona campos de negocio: `location_city`, `location_state`, timestamps e metricas.

3. `quality.py`
- Valida schema obrigatorio.
- Aplica checks de nulos/faixa.
- Bloqueia `extraction_id` duplicado no lote.

4. `load.py`
- Salva arquivos JSON brutos.
- Salva Parquet e CSV particionados (`event_date=YYYY-MM-DD`).
- Mantem historicos Parquet e CSV deduplicados.
- Faz upsert no DuckDB `analytics.weather_facts`.
- Suporta evolucao de schema para novas colunas.

## Locais configurados

- Sao Paulo (SP)
- Rio de Janeiro (RJ)
- Brasilia (DF)
- Belo Horizonte (MG)
- Salvador (BA)
- Curitiba (PR)
- Foz do Iguacu (PR)

## Comandos

```bash
pip install -r requirements.txt
python main.py
pytest -q
```

## DataViz (Power BI, Tableau e outras)

1. CSV (recomendado)
- Arquivo: `data/processed/weather/weather_historical.csv`
- Caminho padrao relativo ao projeto:
  `<project-root>/data/processed/weather/weather_historical.csv`
- Configuravel via `config/config.yaml` (`paths.processed_dir` e `output.historical_filenames.csv`).

2. DuckDB via ODBC/driver
- Arquivo do banco:
  `<project-root>/data/warehouse/weather.duckdb`
- Tabela:
  `analytics.weather_facts`
- Configuravel via `config/config.yaml` (`paths.duckdb_path`).
