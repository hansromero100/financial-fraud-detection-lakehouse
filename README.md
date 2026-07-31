# Financial Fraud Detection Lakehouse

High-Throughput Real-Time Fraud Detection & Financial Reporting built on **Databricks Delta Lakehouse** with Structured Streaming, Delta Live Tables (DLT), and Unity Catalog.

## Business Context

This project processes the **PaySim Synthetic Financial Transactions Dataset** (~6M records from Kagle) to detect fraudulent mobile money transfers in real time. PaySim simulates mobile money transactions including CASH_IN, CASH_OUT, TRANSFER, PAYMENT, and DEBIT operations with labeled fraud flags.

### Key Objectives

- **Real-time fraud detection** via Structured Streaming with 1-hour sliding window velocity scoring
- **Data governance** through Unity Catalog Dynamic Data Masking on customer account IDs
- **Data quality enforcement** using DLT Expectations at the Silver layer
- **Reporting** with Liquid Clustering for optimized BI queries on daily fraud metrics

## Architecture

```
PaySim CSV  -->  [Auto Loader]  -->  Bronze (Raw)  -->  [DLT]  -->  Silver (Clean)  -->  [DLT]  -->  Gold (Aggregated)
                                        |                           |                                |
                                CDF enabled               Masking (SHA-256)            Velocity Metrics + Daily Report
                                                           DLT Expectations             Liquid Clustering
                                                           CDC / MERGE INTO
```

See [docs/architecture.md](docs/architecture.md) for full Mermaid diagrams.

## Medallion Architecture

### Bronze Layer
- **Source**: PaySim CSV files ingested via `spark.readStream.format("cloudFiles")` (Auto Loader)
- **Table**: `transactions_raw` — raw ingestion with `ingestion_timestamp` metadata
- **Features**: Delta Change Data Feed (CDF) enabled for downstream CDC

### Silver Layer
- **Transformations**:
  - **Dynamic Data Masking**: SHA-256 hashing on `nameOrig` and `nameDest` (strip C/M prefix before hashing)
  - **DLT Expectations**: `amount > 0`, valid transaction types, non-null account IDs, valid fraud flags
  - **CDC**: `MERGE INTO` pattern using Delta Change Data Feed
- **Table**: `transactions_clean` — validated, anonymized records with `is_high_value` flag

### Gold Layer
- **`fraud_velocity_metrics`**: 1-hour sliding window (15-min slide) aggregations per user:
  - `tx_count`, `total_amount`, `avg_amount`, `max_amount`
  - `fraud_count`, `high_value_count` (>=$10k)
  - `fraud_score` (0.1–0.9) and `is_suspicious` flag
- **`daily_fraud_report`**: Daily rollups by transaction type for BI dashboards
- **Optimization**: Liquid Clustering via `CLUSTER BY (transaction_type, transaction_date)`

## Project Structure

```
.
├── .github/workflows/          # CI/CD: PyTest, Black formatting, Flake8 linting
├── config/
│   ├── dev.yaml                # Development environment configuration
│   ├── staging.yaml            # Staging environment configuration
│   ├── prod.yaml                # Production environment configuration
│   └── schemas/
│       └── bronze_schema.json  # PaySim source schema definition
├── data/
│   ├── sample_paysim.csv       # Local sample data slice for testing
│   └── generate_sample_data.py # Script to generate synthetic test data
├── docs/
│   └── architecture.md          # Architecture diagrams (Mermaid.js)
├── notebooks/
│   ├── 01_eda_transactions.ipynb            # EDA on transaction patterns
│   └── 02_velocity_scoring_prototype.ipynb   # Velocity scoring prototype
├── src/
│   ├── ingestion/
│   │   ├── auto_loader.py       # Auto Loader streaming/batch readers
│   │   ├── bronze_writer.py     # Bronze Delta table writer (streaming + batch)
│   │   ├── schema_loader.py     # Bronze schema JSON loader utility
│   │   └── streaming_producer.py # Simulated streaming via CSV batch files
│   ├── pipelines/
│   │   ├── bronze_pipeline.py   # DLT Bronze pipeline (auto_loader ingestion)
│   │   ├── silver_pipeline.py   # DLT Silver pipeline (masking, expectations, CDC)
│   │   ├── gold_pipeline.py     # DLT Gold pipeline (velocity metrics, daily report)
│   │   └── medallion_runner.py  # Batch runner for local dev (non-DLT path)
│   └── quality/
│       ├── expectations.py     # Python expectation loader and applier
│       └── expectations_silver.yaml # DLT data quality rules definition
├── tests/
│   ├── conftest.py              # PySpark session fixture + sample schemas
│   ├── test_ingestion.py        # Tests for Auto Loader and schema validation
│   ├── test_silver_pipeline.py  # Tests for masking, filtering, transformations
│   ├── test_streaming_producer.py # Tests for batch generation and cleanup
│   └── test_quality.py         # Tests for DLT expectations (drop, warn, fail)
├── .gitignore
├── databricks.yml               # Databricks Asset Bundles (DABs) configuration
├── LICENSE
├── Makefile                     # Local dev commands + Databricks deployment
├── README.md
└── requirements-dev.txt          # Development dependencies
```

## Getting Started

### Prerequisites

- Python 3.11+
- Apache Spark 3.5+ with Delta Lake
- Databricks CLI (`pip install databricks-cli`)
- Java 11+ (for local Spark)

### Local Development

```bash
# Install dependencies
make install

# Generate sample test data
make generate-sample

# Run tests (PySpark local mode)
make test

# Lint and format check
make lint
make format

# Run all checks
make check
```

### Databricks Deployment

1. Configure workspace URLs and credentials in `config/<env>.yaml`
2. Set `dev_cluster_id` / `staging_cluster_id` via environment variables or DABs variables
3. Deploy using Databricks Asset Bundles:

```bash
make deploy-dev
make deploy-staging
make deploy-prod
```

Or directly:
```bash
databricks bundle deploy --target dev
```

## Dataset

**PaySim Synthetic Financial Transactions** (Kaggle)
- ~6,362,620 records of mobile money transactions
- Fields: `step`, `type`, `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`, `nameDest`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`
- Fraud rate: ~0.13% of transactions
- Fraud occurs only in `CASH_OUT` and `TRANSFER` types

## Key Technical Features

| Feature | Implementation |
|---------|---------------|
| Streaming Ingestion | Auto Loader (`cloudFiles` format) with configurable batch size |
| Data Masking | SHA-256 hashing on account IDs (Unity Catalog dynamic masking pattern) |
| Data Quality | DLT Expectations with `expect`, `expect_or_drop`, `expect_or_fail` |
| CDC | MERGE INTO with Delta Change Data Feed |
| Real-Time Scoring | Sliding 1-hour window aggregations with watermarking |
| Query Optimization | Liquid Clustering on `(transaction_type, transaction_date)` |
| CI/CD | GitHub Actions: Black, Flake8, PyTest |
| Deployment | Databricks Asset Bundles (DABs) with multi-target support |

## License

See [LICENSE](LICENSE) file.
