# Weather ETL Pipeline

## EN

Production-ready weather ETL pipeline focused on reliability, data quality, and analytics consumption.

### Overview

- Extracts current weather data from Open-Meteo API.
- Applies typed transformation with Polars.
- Runs quality checks before loading.
- Stores outputs in Parquet, CSV, and DuckDB.
- Supports schema evolution in historical datasets and warehouse table.

### Monitored locations

- Sao Paulo (SP)
- Rio de Janeiro (RJ)
- Foz do Iguacu (PR)
- Quebec (CA-QC)
- Orlando (US-FL)
- Porto (PT)
- Valencia (ES)
- Brussels (BE)

### Output datasets

- Raw JSON: `data/raw/*.json`
- Processed Parquet: `data/processed/weather/event_date=YYYY-MM-DD/*.parquet`
- Processed CSV: `data/processed/weather/event_date=YYYY-MM-DD/*.csv`
- Historical Parquet: `data/processed/weather/weather_historical.parquet`
- Historical CSV: `data/processed/weather/weather_historical.csv`
- DuckDB table: `analytics.weather_facts` in `data/warehouse/weather.duckdb`

### Stack

- Python 3.11+
- Prefect 3
- Polars
- DuckDB
- Parquet / CSV
- Tenacity

### Setup

```bash
python -m venv .venv
# Windows PowerShell
.\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
```

### Run

```bash
python main.py
```

### Test

```bash
pytest -q
```

### Core commands (extract vs analyze)

```bash
# Extract data (run ETL ingestion and load)
python main.py

# Analyze collected data (quick query)
python query.py latest --limit 20
```

### Quick analysis (CLI)

```bash
python query.py latest --limit 20
python query.py avg-temp --from 2026-02-01 --to 2026-02-28
python query.py rain --from 2026-02-01 --to 2026-02-28
python query.py city --name "Quebec" --limit 10
python query.py sql --query "SELECT COUNT(*) AS total_rows FROM analytics.weather_facts"
```

### DataViz integration

1. CSV (recommended for quick BI integration)
- File: `data/processed/weather/weather_historical.csv`
- Default project-relative path:
  `<project-root>/data/processed/weather/weather_historical.csv`
- Path can be customized via `config/config.yaml` (`paths.processed_dir` and `output.historical_filenames.csv`).
- Compatible with Power BI, Tableau, Excel, Qlik Sense, Apache Superset.

2. DuckDB via ODBC/driver (recommended for scalable querying)
- Database file:
  `<project-root>/data/warehouse/weather.duckdb`
- Table:
  `analytics.weather_facts`
- Path can be customized via `config/config.yaml` (`paths.duckdb_path`).
- Compatible with Power BI, Tableau, Metabase, DBeaver, Grafana.

---

## PT-BR

Pipeline ETL de clima pronto para uso profissional, com foco em confiabilidade, qualidade de dados e consumo analitico.

### Visao geral

- Extrai clima atual da API Open-Meteo.
- Aplica transformacao tipada com Polars.
- Executa validacoes de qualidade antes da carga.
- Armazena saidas em Parquet, CSV e DuckDB.
- Suporta evolucao de schema no historico e na tabela analitica.

### Locais monitorados

- Sao Paulo (SP)
- Rio de Janeiro (RJ)
- Foz do Iguacu (PR)
- Quebec (CA-QC)
- Orlando (US-FL)
- Porto (PT)
- Valencia (ES)
- Brussels (BE)

### Conjuntos de saida

- JSON bruto: `data/raw/*.json`
- Parquet processado: `data/processed/weather/event_date=YYYY-MM-DD/*.parquet`
- CSV processado: `data/processed/weather/event_date=YYYY-MM-DD/*.csv`
- Parquet historico: `data/processed/weather/weather_historical.parquet`
- CSV historico: `data/processed/weather/weather_historical.csv`
- Tabela DuckDB: `analytics.weather_facts` em `data/warehouse/weather.duckdb`

### Stack

- Python 3.11+
- Prefect 3
- Polars
- DuckDB
- Parquet / CSV
- Tenacity

### Configuracao

Edite `config/config.yaml`:
- `api.locations`: lista de cidades e coordenadas
- `api.params`: variaveis de clima da API
- `output`: formatos e nomes de historico
- `paths`: caminhos de armazenamento
- `quality`: regras de validacao
- `orchestration`: retries e agendamento
- `alerts.webhook_url`: alerta opcional

### Integracao com DataViz

1. CSV (recomendado para integracao rapida)
- Arquivo: `data/processed/weather/weather_historical.csv`
- Caminho padrao relativo ao projeto:
  `<project-root>/data/processed/weather/weather_historical.csv`
- O caminho pode ser alterado em `config/config.yaml` (`paths.processed_dir` e `output.historical_filenames.csv`).
- Compativel com Power BI, Tableau, Excel, Qlik Sense, Apache Superset.

2. DuckDB via ODBC/driver (recomendado para consultas escalaveis)
- Arquivo do banco:
  `<project-root>/data/warehouse/weather.duckdb`
- Tabela:
  `analytics.weather_facts`
- O caminho pode ser alterado em `config/config.yaml` (`paths.duckdb_path`).
- Compativel com Power BI, Tableau, Metabase, DBeaver, Grafana.

### Comandos principais (extrair vs analisar)

```bash
# Extrair dados (rodar ingestao ETL e carga)
python main.py

# Analisar dados coletados (consulta rapida)
python query.py latest --limit 20
```

### Analise rapida (CLI)

```bash
python query.py latest --limit 20
python query.py avg-temp --from 2026-02-01 --to 2026-02-28
python query.py rain --from 2026-02-01 --to 2026-02-28
python query.py city --name "Quebec" --limit 10
python query.py sql --query "SELECT COUNT(*) AS total_rows FROM analytics.weather_facts"
```

---

## FR

Pipeline ETL meteo pret pour la production, avec un focus sur la fiabilite, la qualite des donnees et la consommation analytique.

### Vue d'ensemble

- Extrait la meteo actuelle depuis l'API Open-Meteo.
- Applique une transformation typee avec Polars.
- Execute des controles de qualite avant le chargement.
- Stocke les sorties en Parquet, CSV et DuckDB.
- Supporte l'evolution de schema dans l'historique et la table analytique.

### Villes surveillees

- Sao Paulo (SP)
- Rio de Janeiro (RJ)
- Foz do Iguacu (PR)
- Quebec (CA-QC)
- Orlando (US-FL)
- Porto (PT)
- Valencia (ES)
- Brussels (BE)

### Jeux de donnees de sortie

- JSON brut: `data/raw/*.json`
- Parquet traite: `data/processed/weather/event_date=YYYY-MM-DD/*.parquet`
- CSV traite: `data/processed/weather/event_date=YYYY-MM-DD/*.csv`
- Parquet historique: `data/processed/weather/weather_historical.parquet`
- CSV historique: `data/processed/weather/weather_historical.csv`
- Table DuckDB: `analytics.weather_facts` dans `data/warehouse/weather.duckdb`

### Stack

- Python 3.11+
- Prefect 3
- Polars
- DuckDB
- Parquet / CSV
- Tenacity

### Configuration

Editez `config/config.yaml`:
- `api.locations`: liste des villes et coordonnees
- `api.params`: variables meteo de l'API
- `output`: formats et noms des historiques
- `paths`: chemins de stockage
- `quality`: regles de validation
- `orchestration`: retries et planification
- `alerts.webhook_url`: alerte optionnelle

### Integration DataViz

1. CSV (recommande pour integration rapide)
- Fichier: `data/processed/weather/weather_historical.csv`
- Chemin relatif standard:
  `<project-root>/data/processed/weather/weather_historical.csv`
- Le chemin est configurable via `config/config.yaml` (`paths.processed_dir` et `output.historical_filenames.csv`).
- Compatible avec Power BI, Tableau, Excel, Qlik Sense, Apache Superset.

2. DuckDB via ODBC/driver (recommande pour requetes scalables)
- Fichier base:
  `<project-root>/data/warehouse/weather.duckdb`
- Table:
  `analytics.weather_facts`
- Le chemin est configurable via `config/config.yaml` (`paths.duckdb_path`).
- Compatible avec Power BI, Tableau, Metabase, DBeaver, Grafana.

### Commandes principales (extraire vs analyser)

```bash
# Extraire les donnees (execution ETL + chargement)
python main.py

# Analyser les donnees collectees (requete rapide)
python query.py latest --limit 20
```

### Analyse rapide (CLI)

```bash
python query.py latest --limit 20
python query.py avg-temp --from 2026-02-01 --to 2026-02-28
python query.py rain --from 2026-02-01 --to 2026-02-28
python query.py city --name "Quebec" --limit 10
python query.py sql --query "SELECT COUNT(*) AS total_rows FROM analytics.weather_facts"
```

