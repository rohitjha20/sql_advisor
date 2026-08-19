# 🚀 Azure SQL Server Recommendation Engine

A comprehensive performance, storage, code quality, and cost optimization tool for Azure SQL Databases running entirely as **Databricks Notebooks**.

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────┐
│                   Azure SQL Database                   │
│                 (Managed by Microsoft)                 │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  Notebook 1: 01_azure_sql_data_collector.ipynb         │
│  • 42 Metrics collected via JDBC pushdown              │
│  • 2 Incremental queries (watermarked)                 │
│  • 40 Snapshot queries (catalogs, DMVs, schemas)       │
│  • Reads queries & thresholds from config.py           │
│  • Writes partitioned Parquet to ADLS Gen2             │
│  • Updates _metadata/watermarks.json                   │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼  Azure Storage Account (ADLS Gen2)
┌────────────────────────────────────────────────────────┐
│  abfss://<container>@<storage>.dfs.core.windows.net    │
│  └── raw/{server}/{database}/{metric}/year=.../...     │
│  └── _metadata/watermarks.json                         │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│  Notebook 2: 02_azure_sql_recommendation_engine.ipynb  │
│  • Reads telemetry from ADLS Gen2 via PySpark          │
│  • 10 Analyzers (50+ sub-checks)                       │
│  • Priority scoring & Health Score (0–100)             │
│  • Parquet output: recommendations/... in ADLS Gen2    │
│  • Interactive HTML executive report                   │
└────────────────────────────────────────────────────────┘
```

## 📁 Repository Structure

```
azure_sql_advisor/
├── config.py                                          # Single configuration file (queries, thresholds, pricing, enums)
├── notebooks/
│   ├── 01_azure_sql_data_collector.ipynb              # Databricks Notebook 1: Data Collection (42 metrics)
│   └── 02_azure_sql_recommendation_engine.ipynb       # Databricks Notebook 2: 10-Category Recommendation Engine
├── requirements.txt                                   # Python dependencies (reference)
├── TECHNICAL_DESIGN_DOCUMENT.md                       # Comprehensive Technical Design Document (v3.0)
└── README.md                                          # This file
```

## ✨ 10 Analysis Categories

1. 🗂️ **Index Management** (`INDEXING`): Missing indexes, unused indexes, duplicates, fragmentation, foreign key coverage, columnstore candidates, write-heavy indexes.
2. 📋 **Stored Procedures** (`STORED_PROCEDURES`): CPU/IO/duration/write hotspots, recompile spikes, parameter sniffing, and 7 anti-pattern static checks (`SET NOCOUNT ON`, `SELECT *`, `CURSOR`, dynamic `EXEC`, table variables, `OPTION(RECOMPILE)`, unhandled transactions).
3. 👁️ **Views Optimization** (`VIEWS`): Deep view nesting, `SELECT *`, missing `WITH SCHEMABINDING`, high complexity, `NOLOCK` hints, indexed view candidates and maintenance overhead.
4. 📐 **Schema Design** (`SCHEMA`): Heap tables, missing primary keys, wide tables (>30 cols), `NVARCHAR` overuse, LOB/MAX column audits.
5. 💾 **Storage Analysis** (`STORAGE`): Large table audit, PAGE/ROW compression candidates, index-to-data space ratio, unused allocated space, database file growth.
6. 📊 **Activity & Workload** (`ACTIVITY`): Top resource-consuming logins/programs, operation type breakdown (SELECT/INSERT/UPDATE/DELETE/EXEC/DDL), idle connection pooling.
7. 🗄️ **Data Lifecycle & Archival** (`ARCHIVAL`): Cold table detection (>180/365/730 days idle), range partitioning candidates for time-series data.
8. 🏎️ **Performance Diagnostics** (`PERFORMANCE`): CPU/IO/Memory/Worker/Session saturation, wait category breakdown, top CPU/Read queries, active blocking chains, TempDB usage, stale statistics, long-running transactions.
9. 💰 **Cost & Capacity** (`COST`): Service tier rightsizing (scale down underutilized, scale up saturated), Serverless auto-pause opportunities, reserved capacity and Hybrid Benefit evaluation.
10. ⚙️ **Database Operations** (`OPERATIONS`): Query Store state, auto-create/auto-update statistics flags, Read Committed Snapshot Isolation (`RCSI`), Azure automatic tuning recommendations, plan cache single-use bloat, transaction log space.

## 🚀 Getting Started

### Prerequisites

- Azure Databricks workspace with access to Azure SQL Database
- Azure Storage Account (ADLS Gen2) accessible via ABFSS connector
- JDBC connectivity to Azure SQL Database from Databricks cluster

### Step 1: Upload Files to Databricks

1. Clone or upload repo to Databricks Repos (`Workspace > Repos`)
2. `config.py` acts as the single source of truth

### Step 2: Run Data Collection (Notebook 1)

Open **`notebooks/01_azure_sql_data_collector.ipynb`** and set widget parameters:

| Widget | Description | Example |
|---|---|---|
| `server_name` | Azure SQL Server FQDN | `myserver.database.windows.net` |
| `database_name` | Database name | `mydb` |
| `secret_scope` | Databricks Secret Scope | `azure-sql-credentials` |
| `username_key` | Secret key for username | `sql-username` |
| `password_key` | Secret key for password | `sql-password` |
| `storage_account` | ADLS Gen2 account name | `mystorageaccount` |
| `storage_container` | Container name | `azure-sql-telemetry` |

### Step 3: Run Recommendation Engine (Notebook 2)

Open **`notebooks/02_azure_sql_recommendation_engine.ipynb`** and configure:

| Widget | Description | Example |
|---|---|---|
| `server_name` | Azure SQL Server FQDN | `myserver.database.windows.net` |
| `database_name` | Database name | `mydb` |
| `storage_account` | ADLS Gen2 account name | `mystorageaccount` |
| `storage_container` | Container name | `azure-sql-telemetry` |
| `lookback_days` | Lookback window in days | `7` |

### Step 4: Schedule via Databricks Workflows

- **Task 1 (Collector)**: Run every 15–60 minutes
- **Task 2 (Engine)**: Run daily after Task 1 completes

## 📊 Storage Layout (ADLS Gen2)

```
azure-sql-telemetry/
├── raw/{server}/{database}/
│   ├── resource_stats/year=YYYY/month=MM/day=DD/*.parquet
│   ├── query_store_stats/year=YYYY/month=MM/day=DD/*.parquet
│   ├── sp_execution_stats/year=YYYY/.../*.parquet
│   ├── views_analysis/year=YYYY/.../*.parquet
│   ├── cold_tables/year=YYYY/.../*.parquet
│   ├── ... (42 metric directories)
│   └── _metadata/watermarks.json
└── recommendations/{server}/{database}/
    ├── year=YYYY/month=MM/day=DD/*.parquet
    └── reports/{database}_report_YYYYMMDD_HHMMSS.html
```

## 🔧 Technical Details & Formulas

For deep architecture diagrams, DMV query catalogs, priority score weights, and full threshold listings, consult the [Technical Design Document](TECHNICAL_DESIGN_DOCUMENT.md).
