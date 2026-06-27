# Enterprise Data Integration Pipeline Documentation

This documentation provides an end-to-end overview of the Python + SQL ETL/ELT pipeline. It integrates core SQL Server sales data and acquired PostgreSQL sales data into a unified, clean Snowflake sales fact table and logging rejects table.

---

## 1. System Architecture

The pipeline follows an **ELT (Extract, Load, Transform)** design pattern:
1. **Extract**: Concrete extractors pull raw data from sources using pandas.
2. **Load**: Data is staged in raw tables in the warehouse (`STG_CORE_SALES`, `STG_ACQUIRED_SALES`, `STG_EXCHANGE_RATES`).
3. **Transform**: Snowflake-native SQL scripts clean, normalize currencies, validate, and merge records into the final warehouse fact (`SALES_FACT`) and rejects (`SALES_REJECTS`) tables.

```mermaid
graph TD
    subgraph Raw Data Sources
        SS[SQL Server Mock CSV]
        PG[Postgres Mock CSV]
        ER[Exchange Rates CSV]
    end

    subgraph Extractor Contract
        SSE[SqlServerExtractor]
        PGE[PostgresExtractor]
    end

    SS --> SSE
    PG --> PGE

    subgraph Staging Layer RAW
        STG_C[STG_CORE_SALES]
        STG_A[STG_ACQUIRED_SALES]
        STG_E[STG_EXCHANGE_RATES]
    end

    SSE -->|Load DF| STG_C
    PGE -->|Load DF| STG_A
    ER -->|Load DF| STG_E

    subgraph Transformation Engine
        SC[staging_core.sql]
        SA[staging_acquired.sql]
        WM[warehouse_merge.sql]
    end

    STG_C --> SC
    STG_A --> SA
    STG_E --> SA

    subgraph Analytics Schema
        FACT[(SALES_FACT)]
        REJ[(SALES_REJECTS)]
    end

    SC --> WM
    SA -->|MERGE / UPSERT| WM
    WM --> FACT
    WM -->|DELETE + INSERT| REJ
```

---

## 2. Decoupled OOP Framework

The pipeline's Python modules are built on Object-Oriented Programming (OOP) contracts:

* **`BaseExtractor`** (`src/extractors/base_extractor.py`):
  * Abstract base class defining the extraction contract. Concrete classes `SqlServerExtractor` and `PostgresExtractor` inherit from it and implement the `extract()` method.
* **`DBEngine`** (`src/loaders/snowflake.py`):
  * Abstract database interface handling query execution (`execute`), dataframe fetching (`fetch_df`), and bulk data staging (`load_df`).
* **`DuckDBEngine`** (`src/loaders/snowflake.py`):
  * A local database simulator. Since live Snowflake credentials are not required for local development and testing, `DuckDBEngine` attaches an in-memory database and registers Python-implemented Snowflake functions (`TRY_PARSE_JSON`, `GET`, `TRY_TO_TIMESTAMP`) to execute Snowflake-native scripts locally.
* **`SnowflakeEngine`** (`src/loaders/snowflake.py`):
  * Direct production driver using `snowflake.connector` and `write_pandas` to load dataframes into Snowflake tables.
* **`StagingLoader`** (`src/loaders/staging.py`):
  * Manages schema instantiation (`RAW`, `ANALYTICS`) and handles columns staging/loading.
* **`SqlRunner`** (`src/transformers/sql_runner.py`):
  * Reads multi-statement SQL script files, runs queries sequentially, and captures performance and timing stats.

---

## 3. SQL Transformation & Normalization Logic

Transformations are configured native to Snowflake to optimize query execution and leverage warehouse performance:

* **Staging Core (SQL Server)** (`src/sql/staging_core.sql`):
  * Extracts nested JSON fields (`customer_id`, `customer_country`) from `customer_blob` via `GET(TRY_PARSE_JSON(...))`.
  * Standardizes timestamps using `TRY_TO_TIMESTAMP`.
  * Removes product prefixes like `PROD-` or `SKU-` using regular expressions.
  * Identifies validation rejects (invalid JSON blobs, duplicate transaction records, missing product SKUs).
* **Staging Acquired (Postgres)** (`src/sql/staging_acquired.sql`):
  * Handles mixed timestamp formats (`YYYY-MM-DD HH:MI:SS`, `DD/MM/YYYY HH:MI:SS`, `YYYY/MM/DD HH:MI:SS`) by coalescing multiple `TRY_TO_TIMESTAMP` patterns.
  * Adjusts transaction currencies using a `LEFT JOIN` on `STG_EXCHANGE_RATES` to normalize all raw amounts into `USD`.
  * Identifies rejects (unknown currencies, missing product SKUs, duplicate transaction IDs).
* **Warehouse Merge** (`src/sql/warehouse_merge.sql`):
  * Integrates staged records into `SALES_FACT` and `SALES_REJECTS`.

---

## 4. Execution Guide

This section explains how to run the pipeline from end-to-end using either the local DuckDB database or a live Snowflake instance.

### Prerequisites

1. Set up a Python virtual environment:
   ```bash
   python -m venv .venv
   ```
2. Activate the virtual environment:
   * **Windows (PowerShell)**: `.venv\Scripts\Activate.ps1`
   * **macOS / Linux**: `source .venv/bin/activate`
3. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

### Option A: Local Execution (DuckDB)

Local execution runs the pipeline using an in-memory DuckDB instance to simulate Snowflake. It requires no database credentials.

1. **Run the ETL Pipeline**:
   ```bash
   python main.py
   ```
   This will extract mock files, load them into staging schemas inside DuckDB, execute staging transformations, and compare results against the expected fixtures.
2. **Run the Automated Test Suite**:
   ```bash
   pytest
   ```

---

### Option B: Cloud Execution (Snowflake)

Cloud execution runs the pipeline against a live Snowflake database warehouse.

1. **Configure Environment Credentials**:
   Provide connection details as environment variables.
   
   * **Windows (PowerShell)**:
     ```powershell
     $env:SNOWFLAKE_ACCOUNT="your_snowflake_account"
     $env:SNOWFLAKE_USER="your_username"
     $env:SNOWFLAKE_PASSWORD="your_password"
     $env:SNOWFLAKE_ROLE="your_role"
     $env:SNOWFLAKE_WAREHOUSE="your_warehouse"
     ```
   * **macOS / Linux**:
     ```bash
     export SNOWFLAKE_ACCOUNT="your_snowflake_account"
     export SNOWFLAKE_USER="your_username"
     export SNOWFLAKE_PASSWORD="your_password"
     export SNOWFLAKE_ROLE="your_role"
     export SNOWFLAKE_WAREHOUSE="your_warehouse"
     ```
2. **Run the ETL Pipeline**:
   ```bash
   python main.py --use-snowflake
   ```
   This command directs the pipeline engine to initialize connections, establish schemas (`DE_INTEGRATION.RAW` and `DE_INTEGRATION.ANALYTICS`), stage the dataframes using bulk `write_pandas`, run transformations, and generate facts and reject files.

---

## 5. Technical Discussion


### 1. How does the design satisfy idempotency, incremental loading, and data quality checks?
* **Idempotency**: 
  * The sales fact tables are loaded using a SQL `MERGE` query matching on a generated `warehouse_transaction_id` (prefixed with `source_system` to ensure unique constraints). Repeated runs update changes in-place without adding duplicates.
  * For the rejects log, incoming transaction ids for the current batch are deleted from `SALES_REJECTS` before insertion, avoiding double logging of validation errors on subsequent runs.
* **Incremental Loading**:
  * The architecture stages raw batches, then performs `MERGE` actions downstream. 
* **Data Quality Checks**:
  * Performed native to the database during staging. Transactions with parsing issues (invalid JSON, missing SKU, unknown exchange rate, duplicate transaction IDs) are flagged with a specific `reject_reason` and loaded to the rejects log. Clean rows are written to the fact table.

### 2. How are database credentials handled?
* Credentials (like Snowflake passwords) are stored as environment variables on the OS and referenced in `config/dummy.yml` using `${VAR}` notation.
* The YAML config reader expands these environment variables at runtime.
* Secrets are never hardcoded or committed to git, and credentials are not required for local development and test runs (which use DuckDB).

### 3. How would your approach change for 100 million rows instead of these CSVs?
* **Ingestion**: We would avoid loading data in memory using Pandas dataframes. Instead, extractors would stream data in chunks or write directly to cloud storage (Amazon S3, Azure Blob, GCS).
* **Loading**: We would upload files to an external cloud stage using Snowflake's `PUT` statement, then load them in parallel using optimized `COPY INTO` instructions.
* **SQL Transformations**: View definitions would be materialized as transient or staging tables to prevent CPU spikes in staging query paths during warehouse runs.

### 4. Why use an abstract `BaseExtractor` instead of just functions?
* **Interface Guarantees**: Enforces a strict signature contract (`extract() -> pd.DataFrame`) across all extractors.
* **Extensibility (Open-Closed Principle)**: If a new source system (e.g. an API or MongoDB source) is added, we write a new concrete class subclassing `BaseExtractor` without changing any orchestrator code in `main.py`.
* **Testing Decoupling**: Simplifies swapping production database extractors for local CSV mock extractors.

### 5. Snowflake Ingestion: Would you use row inserts, PUT/Stage/COPY INTO, Snowpipe, or external stages?
* **Avoid Row-by-Row Inserts**: Highly inefficient on OLAP column-oriented databases that optimize for bulk micro-partitions.
* **For Batch Pipelines at Scale (PUT/Stage/COPY INTO)**: The industry standard. Data is written to compressed files (like Parquet/CSV), uploaded to a secure stage, and imported into Snowflake.
* **For Continuous Streaming (Snowpipe)**: Listens to S3 bucket upload notifications (via SQS/SNS) to copy files in micro-batches immediately as they arrive, optimizing compute resource scheduling.
