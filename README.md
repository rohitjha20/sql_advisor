# 🚀 Azure SQL Server Recommendation Engine

A comprehensive performance, storage, and cost optimization tool for Azure SQL Databases running entirely as **Databricks Notebooks**.

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
│  • Incremental extraction via JDBC (watermarked)       │
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
│  • 4 Analyzers: Performance, Index, Storage, Cost      │
│  • Priority scoring & Health Score (0–100)              │
│  • Parquet output: recommendations/... in ADLS Gen2    │
│  • Interactive HTML executive report                   │
└────────────────────────────────────────────────────────┘
```

## 📁 Repository Structure

```
azure_sql_advisor/
├── config.py                                          # Single configuration file (queries, thresholds, pricing, enums)
├── notebooks/
│   ├── 01_azure_sql_data_collector.ipynb              # Databricks Notebook: Incremental Data Collection
│   └── 02_azure_sql_recommendation_engine.ipynb       # Databricks Notebook: Recommendation Engine
├── requirements.txt                                   # Python dependencies (reference)
└── README.md                                          # This file
```

## ✨ Features

- 🏎️ **Performance Optimization**: Analyzes CPU/IO/Memory, wait statistics, and expensive queries
- 🗂️ **Index Management**: Missing indexes, unused indexes, duplicates, and fragmentation
- 💾 **Storage Analysis**: Large table audit, compression opportunities, NVARCHAR→VARCHAR savings
- 💰 **Cost Optimization**: DTU/vCore rightsizing, serverless auto-pause, reserved capacity, Hybrid Benefit
- 📈 **Incremental Ingestion**: Watermark-based extraction — only fetches new data since last run
- 🔄 **Managed Service Safe**: Handles Azure SQL DMV rollover, failover resets, and catalog snapshots

## 🚀 Getting Started

### Prerequisites

- Azure Databricks workspace with access to Azure SQL Database
- Azure Storage Account (ADLS Gen2) mounted or accessible via ABFSS
- JDBC connectivity to Azure SQL Database from Databricks cluster

### Step 1: Upload Files to Databricks

1. Upload `config.py` to your Databricks Repos or workspace files
2. Import both notebooks from `notebooks/` into your workspace

### Step 2: Run Data Collection (Notebook 1)

Open **01_azure_sql_data_collector.ipynb** and configure the widgets:

| Widget | Description | Example |
|--------|-------------|---------|
| `server_name` | Azure SQL Server FQDN | `myserver.database.windows.net` |
| `database_name` | Database name | `mydb` |
| `storage_account` | ADLS Gen2 storage account | `mystorageaccount` |
| `storage_container` | Blob container name | `azure-sql-telemetry` |
| `auth_method` | Authentication method | `managed_identity` / `sql` |

Run all cells to extract telemetry and persist to storage.

### Step 3: Run Recommendation Engine (Notebook 2)

Open **02_azure_sql_recommendation_engine.ipynb** and configure:

| Widget | Description | Example |
|--------|-------------|---------|
| `server_name` | Same server name | `myserver.database.windows.net` |
| `database_name` | Same database | `mydb` |
| `storage_account` | Same storage account | `mystorageaccount` |
| `storage_container` | Same container | `azure-sql-telemetry` |
| `lookback_days` | Days of history to analyze | `7` |

Run all cells to generate prioritized recommendations, interactive charts, Parquet storage output, and HTML report.

### Step 4: Schedule (Optional)

Create a Databricks Workflow with two tasks:
1. **Task 1**: Run `01_azure_sql_data_collector` (e.g., every 15 minutes)
2. **Task 2**: Run `02_azure_sql_recommendation_engine` (e.g., daily), depends on Task 1

## 📊 Storage Layout

Telemetry is stored in date-partitioned Parquet:

```
raw/{server}/{database}/
├── resource_stats/year=2025/month=08/day=19/...parquet    # Incremental
├── query_store_stats/year=2025/month=08/day=19/...parquet # Incremental
├── wait_stats/year=2025/month=08/day=19/...parquet        # Snapshot
├── missing_indexes/year=2025/month=08/day=19/...parquet   # Snapshot
├── table_sizes/year=2025/month=08/day=19/...parquet       # Snapshot
├── ...
└── _metadata/watermarks.json                              # Watermark tracking
```

## 🔧 Configuration

All configuration lives in `config.py`:

- **`AdvisorConfig`**: Central dataclass with all connection, storage, threshold, and weight settings
- **`INCREMENTAL_QUERIES`**: Time-series DMV queries with watermark column definitions
- **`SNAPSHOT_QUERIES`**: Full-snapshot catalog/counter queries (parameterized)
- **`AZURE_SQL_PRICING`**: Complete DTU and vCore pricing matrix
- **`SEVERITY_SCORES`**: Priority scoring weights
- **`WAIT_CATEGORIES`**: Wait type → bottleneck category mapping
- **Enums**: `Category`, `Severity`, `Effort`, `Risk`, `Confidence`
- **`Recommendation`**: Dataclass for recommendation output

## 📋 Metrics Collected

| Metric | Type | DMV Source |
|--------|------|-----------|
| `resource_stats` | Incremental | `sys.dm_db_resource_stats` |
| `query_store_stats` | Incremental | `sys.query_store_runtime_stats` |
| `database_summary` | Snapshot | `sys.tables`, `sys.indexes` |
| `top_queries_cpu` | Snapshot | `sys.dm_exec_query_stats` |
| `top_queries_reads` | Snapshot | `sys.dm_exec_query_stats` |
| `wait_stats` | Snapshot | `sys.dm_os_wait_stats` |
| `missing_indexes` | Snapshot | `sys.dm_db_missing_index_*` |
| `unused_indexes` | Snapshot | `sys.indexes` + `sys.dm_db_index_usage_stats` |
| `duplicate_indexes` | Snapshot | `sys.indexes` + `sys.index_columns` |
| `index_fragmentation` | Snapshot | `sys.dm_db_index_physical_stats` |
| `table_sizes` | Snapshot | `sys.dm_db_partition_stats` |
| `database_files` | Snapshot | `sys.database_files` |
| `compression_candidates` | Snapshot | `sys.partitions` |
| `service_tier` | Snapshot | `DATABASEPROPERTYEX()` |
| `data_type_audit` | Snapshot | `INFORMATION_SCHEMA.COLUMNS` |
