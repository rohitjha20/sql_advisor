"""
Azure SQL Server Recommendation Engine - Unified Configuration
==============================================================

Single configuration file containing all settings, SQL queries, pricing catalogs,
thresholds, and shared utilities for the two-notebook Databricks architecture.

- Notebook 1 (Data Collector):  Imports QUERIES, WATERMARK_CONFIG, STORAGE_CONFIG
- Notebook 2 (Recommendation Engine):  Imports THRESHOLDS, PRICING, SEVERITY_SCORES, etc.

Categories: INDEXING | STORED_PROCEDURES | VIEWS | SCHEMA | STORAGE |
            ACTIVITY | ARCHIVAL | PERFORMANCE | COST | OPERATIONS
"""

from dataclasses import dataclass, field
from enum import Enum
import math


# =============================================================================
# 1. ENUMS & DATA CLASSES
# =============================================================================

class Category(Enum):
    PERFORMANCE = "Performance"
    STORAGE = "Storage"
    COST = "Cost"
    INDEX = "Index"
    STORED_PROCEDURES = "Stored Procedures"
    VIEWS = "Views"
    SCHEMA = "Schema"
    ACTIVITY = "Activity"
    ARCHIVAL = "Archival"
    OPERATIONS = "Operations"


class Severity(Enum):
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"
    INFO = "Info"


class Effort(Enum):
    QUICK_WIN = "Quick Win"
    LOW = "Low"
    MODERATE = "Moderate"
    SIGNIFICANT = "Significant"
    HIGH = "High"


class Risk(Enum):
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"


class Confidence(Enum):
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"


@dataclass
class Recommendation:
    """A single recommendation produced by an analyzer."""
    title: str
    category: Category
    severity: Severity
    description: str
    impact_description: str
    action_sql: str = ""
    action_steps: list = field(default_factory=list)
    effort: Effort = Effort.MODERATE
    risk: Risk = Risk.LOW
    confidence: Confidence = Confidence.HIGH
    estimated_impact_pct: float = 0.0
    details: dict = field(default_factory=dict)
    source_metric: str = ""


# =============================================================================
# 2. CONNECTION & STORAGE SETTINGS
# =============================================================================

@dataclass
class AdvisorConfig:
    """Central configuration for the Azure SQL Advisor pipeline."""

    # ── Azure SQL Connection ──
    server: str = ""
    database: str = ""
    username: str = ""
    password: str = ""
    driver: str = "ODBC Driver 18 for SQL Server"
    auth_method: str = "sql"            # 'sql', 'aad', or 'managed_identity'
    connection_timeout: int = 30
    query_timeout: int = 120

    # ── Azure Storage Account (ADLS Gen2 / Blob) ──
    storage_account_name: str = ""
    storage_container: str = "azure-sql-telemetry"
    storage_account_key: str = ""
    storage_connection_string: str = ""
    storage_base_path: str = "raw"
    use_managed_identity: bool = True
    watermark_blob_name: str = "_metadata/watermarks.json"
    storage_format: str = "parquet"      # 'parquet' or 'json'

    # ── Analysis Settings ──
    lookback_days: int = 7
    top_queries_count: int = 50
    min_execution_count: int = 10

    # ── Performance Thresholds ──
    cpu_critical_pct: float = 90.0
    cpu_high_pct: float = 75.0
    cpu_moderate_pct: float = 50.0
    io_critical_pct: float = 90.0
    io_high_pct: float = 75.0
    memory_high_pct: float = 80.0
    query_duration_high_ms: int = 30_000
    query_duration_low_ms: int = 5_000

    # ── Index Thresholds ──
    missing_index_impact_threshold: float = 50.0
    unused_index_min_days: int = 30
    fragmentation_rebuild_pct: float = 30.0
    fragmentation_reorg_pct: float = 10.0
    min_index_pages: int = 1000
    duplicate_index_check: bool = True
    top_missing_indexes: int = 30
    high_index_count_per_table: int = 10
    write_heavy_index_min_updates: int = 10000
    fk_missing_index_min_rows: int = 10000

    # ── Storage Thresholds ──
    compression_savings_min_pct: float = 20.0
    table_size_concern_gb: float = 10.0
    nvarchar_audit_enabled: bool = True
    max_column_oversized_ratio: float = 5.0
    high_index_to_data_ratio: float = 3.0
    unused_space_min_mb: float = 100.0
    unused_space_pct_threshold: float = 30.0
    top_tables_count: int = 50

    # ── Stored Procedure Thresholds ──
    sp_top_count: int = 50
    sp_min_execution_count: int = 10
    sp_high_cpu_ms: float = 5000.0
    sp_high_reads: int = 100000
    sp_high_duration_ms: float = 10000.0
    sp_recompile_threshold: int = 10
    sp_high_writes: int = 50000

    # ── Views Thresholds ──
    view_nested_threshold: int = 2
    view_complex_length: int = 8000
    indexed_view_reads_threshold: int = 1000
    indexed_view_write_ratio: int = 5

    # ── Schema Thresholds ──
    wide_table_column_threshold: int = 30
    nvarchar_column_alert_count: int = 5

    # ── Archival Thresholds ──
    archival_min_size_mb: float = 100.0
    partition_candidate_min_gb: float = 5.0
    cold_table_critical_days: int = 730
    cold_table_high_days: int = 365
    cold_table_medium_days: int = 180

    # ── Operations Thresholds ──
    stale_stats_days: int = 14
    plan_cache_single_use_pct: float = 70.0
    long_transaction_threshold_seconds: int = 300

    # ── Cost Thresholds ──
    underutilized_cpu_pct: float = 25.0
    underutilized_io_pct: float = 25.0
    idle_period_threshold_hours: float = 1.0

    # ── Priority Weights (10 categories) ──
    weight_performance: float = 0.20
    weight_indexing: float = 0.15
    weight_stored_procedures: float = 0.15
    weight_storage: float = 0.10
    weight_cost: float = 0.10
    weight_views: float = 0.05
    weight_schema: float = 0.05
    weight_activity: float = 0.05
    weight_archival: float = 0.05
    weight_operations: float = 0.10

    # ── Output ──
    output_dir: str = "./reports"
    report_filename: str = "azure_sql_advisor_report.html"
    recommendations_base_path: str = "recommendations"

    def __post_init__(self):
        total_weight = (
            self.weight_performance + self.weight_indexing +
            self.weight_stored_procedures + self.weight_storage +
            self.weight_cost + self.weight_views + self.weight_schema +
            self.weight_activity + self.weight_archival + self.weight_operations
        )
        if not math.isclose(total_weight, 1.0, abs_tol=0.01):
            raise ValueError(f"Priority weights must sum to 1.0, but got {total_weight}")


# =============================================================================
# 3. SEVERITY SCORING
# =============================================================================

SEVERITY_SCORES = {
    Severity.CRITICAL: 100,
    Severity.HIGH: 75,
    Severity.MEDIUM: 50,
    Severity.LOW: 25,
    Severity.INFO: 10,
}

# String-keyed version for Spark map lookups
SEVERITY_SCORES_STR = {
    "CRITICAL": 100,
    "HIGH": 75,
    "MEDIUM": 50,
    "LOW": 25,
    "INFO": 10,
}

# Maps category string → config weight field name
CATEGORY_WEIGHT_FIELDS = {
    "PERFORMANCE": "weight_performance",
    "INDEXING": "weight_indexing",
    "STORED_PROCEDURES": "weight_stored_procedures",
    "STORAGE": "weight_storage",
    "COST": "weight_cost",
    "VIEWS": "weight_views",
    "SCHEMA": "weight_schema",
    "ACTIVITY": "weight_activity",
    "ARCHIVAL": "weight_archival",
    "OPERATIONS": "weight_operations",
}

# All analysis categories (used for output & summaries)
ANALYSIS_CATEGORIES = [
    "INDEXING",
    "STORED_PROCEDURES",
    "VIEWS",
    "SCHEMA",
    "STORAGE",
    "ACTIVITY",
    "ARCHIVAL",
    "PERFORMANCE",
    "COST",
    "OPERATIONS",
]

# Output columns for recommendation rows
OUTPUT_COLUMNS = [
    "category", "subcategory", "severity", "object_type",
    "schema_name", "object_name", "recommendation", "recommendation_detail",
    "action_sql", "estimated_impact_pct", "estimated_savings_mb",
    "estimated_cost_monthly", "effort", "risk", "confidence",
    "source_dmv", "metric_value", "metric_threshold",
]


# =============================================================================
# 4. AZURE SQL PRICING CATALOG
# =============================================================================

AZURE_SQL_PRICING = {
    # DTU-based tiers (monthly price USD, East US)
    'B':    {'name': 'Basic',                   'price': 4.90,     'dtu': 5},
    'S0':   {'name': 'Standard S0',             'price': 14.72,    'dtu': 10},
    'S1':   {'name': 'Standard S1',             'price': 29.43,    'dtu': 20},
    'S2':   {'name': 'Standard S2',             'price': 73.58,    'dtu': 50},
    'S3':   {'name': 'Standard S3',             'price': 147.17,   'dtu': 100},
    'S4':   {'name': 'Standard S4',             'price': 294.34,   'dtu': 200},
    'S6':   {'name': 'Standard S6',             'price': 588.67,   'dtu': 400},
    'S7':   {'name': 'Standard S7',             'price': 1177.34,  'dtu': 800},
    'S9':   {'name': 'Standard S9',             'price': 2354.69,  'dtu': 1600},
    'S12':  {'name': 'Standard S12',            'price': 4415.03,  'dtu': 3000},
    'P1':   {'name': 'Premium P1',              'price': 465.00,   'dtu': 125},
    'P2':   {'name': 'Premium P2',              'price': 930.00,   'dtu': 250},
    'P4':   {'name': 'Premium P4',              'price': 1860.00,  'dtu': 500},
    'P6':   {'name': 'Premium P6',              'price': 3720.00,  'dtu': 1000},
    'P11':  {'name': 'Premium P11',             'price': 5765.63,  'dtu': 1750},
    'P15':  {'name': 'Premium P15',             'price': 11531.25, 'dtu': 4000},
    # vCore-based tiers (monthly price USD, General Purpose East US)
    'GP_Gen5_2':   {'name': 'GP Gen5 2vCores',          'price': 383.61},
    'GP_Gen5_4':   {'name': 'GP Gen5 4vCores',          'price': 767.22},
    'GP_Gen5_8':   {'name': 'GP Gen5 8vCores',          'price': 1534.45},
    'GP_Gen5_16':  {'name': 'GP Gen5 16vCores',         'price': 3068.89},
    'GP_Gen5_32':  {'name': 'GP Gen5 32vCores',         'price': 6137.78},
    'GP_S_Gen5_2': {'name': 'GP Serverless Gen5 2vCores','price': 153.62},
    'GP_S_Gen5_4': {'name': 'GP Serverless Gen5 4vCores','price': 307.24},
    'BC_Gen5_2':   {'name': 'BC Gen5 2vCores',          'price': 958.97},
    'BC_Gen5_4':   {'name': 'BC Gen5 4vCores',          'price': 1917.94},
}

# Tier ordering for scale-up/down recommendations
DTU_TIER_ORDER = ['B', 'S0', 'S1', 'S2', 'S3', 'S4', 'S6', 'S7', 'S9', 'S12',
                  'P1', 'P2', 'P4', 'P6', 'P11', 'P15']
VCORE_GP_TIER_ORDER = ['GP_Gen5_2', 'GP_Gen5_4', 'GP_Gen5_8', 'GP_Gen5_16', 'GP_Gen5_32']
VCORE_BC_TIER_ORDER = ['BC_Gen5_2', 'BC_Gen5_4']
VCORE_SERVERLESS_ORDER = ['GP_S_Gen5_2', 'GP_S_Gen5_4']


# =============================================================================
# 5. STORED PROCEDURE BEST PRACTICES
# =============================================================================

SP_BEST_PRACTICES = {
    "missing_nocount": {
        "pattern": "SET NOCOUNT ON",
        "check": "NOT LIKE",
        "severity": "LOW",
        "recommendation": "Add SET NOCOUNT ON",
        "detail": "Reduces unnecessary TDS_DONE network packets, improving throughput.",
    },
    "select_star": {
        "pattern": "SELECT *",
        "check": "LIKE",
        "severity": "LOW",
        "recommendation": "Avoid SELECT * — explicitly list columns",
        "detail": "Retrieving all columns increases IO and memory usage; explicit columns improve plan quality.",
    },
    "cursor_usage": {
        "pattern": "CURSOR",
        "check": "LIKE",
        "severity": "MEDIUM",
        "recommendation": "Replace CURSOR with set-based logic",
        "detail": "Row-by-row cursor processing is orders of magnitude slower than set-based operations.",
    },
    "dynamic_exec": {
        "pattern": "EXEC(",
        "check": "LIKE",
        "severity": "MEDIUM",
        "recommendation": "Use sp_executesql instead of EXEC()",
        "detail": "sp_executesql allows parameterized execution — prevents SQL injection and enables plan reuse.",
    },
    "table_variable": {
        "pattern": "DECLARE%@%TABLE",
        "check": "LIKE",
        "severity": "MEDIUM",
        "recommendation": "Review table variable usage for large rowsets",
        "detail": "Table variables produce 1-row cardinality estimates. Use #temp tables for medium/large intermediate results.",
    },
    "option_recompile": {
        "pattern": "OPTION (RECOMPILE)",
        "check": "LIKE",
        "severity": "LOW",
        "recommendation": "Validate OPTION(RECOMPILE) cost/benefit",
        "detail": "Fixes parameter-sensitive plans but increases compile CPU. Keep only where runtime savings exceed overhead.",
    },
    "transaction_without_try": {
        "pattern": "BEGIN TRAN",
        "check": "LIKE_WITHOUT",
        "anti_pattern": "TRY",
        "severity": "MEDIUM",
        "recommendation": "Wrap explicit transactions in TRY/CATCH",
        "detail": "Unguarded transactions can remain open after errors, causing blocking and log growth.",
    },
}

# Compression estimate factors
COMPRESSION_ESTIMATES = {
    "PAGE": 0.40,   # ~40% space savings
    "ROW": 0.15,    # ~15% space savings
}


# =============================================================================
# 6. SQL QUERIES CATALOG
#    Used by Notebook 1 (Data Collector).
#    Keys match the metric names used in storage paths & watermarks.
# =============================================================================

# Queries that support incremental (time-filtered) extraction.
INCREMENTAL_QUERIES = {
    'resource_stats': {
        'sql': """
            SELECT end_time, avg_cpu_percent, avg_data_io_percent,
                   avg_log_write_percent, avg_memory_usage_percent,
                   max_worker_percent, max_session_percent
            FROM sys.dm_db_resource_stats
            {where_clause}
            ORDER BY end_time ASC
        """,
        'watermark_column': 'end_time',
    },
    'query_store_stats': {
        'sql': """
            IF EXISTS (SELECT 1 FROM sys.database_query_store_options
                       WHERE actual_state_desc IN ('READ_WRITE', 'READ_ONLY'))
            BEGIN
                SELECT TOP 100
                    q.query_id,
                    qt.query_sql_text,
                    rs.avg_duration / 1000.0 AS avg_duration_ms,
                    rs.avg_cpu_time / 1000.0 AS avg_cpu_ms,
                    rs.avg_logical_io_reads AS avg_logical_reads,
                    rs.count_executions,
                    rs.first_execution_time,
                    rs.last_execution_time,
                    p.plan_id,
                    p.is_forced_plan
                FROM sys.query_store_runtime_stats rs
                JOIN sys.query_store_plan p ON rs.plan_id = p.plan_id
                JOIN sys.query_store_query q ON p.query_id = q.query_id
                JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
                {where_clause}
                ORDER BY rs.last_execution_time ASC
            END
        """,
        'watermark_column': 'last_execution_time',
    },
}

# Snapshot queries: full extraction each run (catalogs, counters, structure).
SNAPSHOT_QUERIES = {
    'database_summary': """
        SELECT
            DB_NAME() AS database_name,
            GETUTCDATE() AS analysis_time,
            (SELECT COUNT(*) FROM sys.tables) AS table_count,
            (SELECT COUNT(*) FROM sys.indexes WHERE type > 0) AS index_count,
            (SELECT COUNT(*) FROM sys.procedures WHERE is_ms_shipped = 0) AS procedure_count,
            (SELECT COUNT(*) FROM sys.views WHERE is_ms_shipped = 0) AS view_count,
            (SELECT SUM(reserved_page_count) * 8 / 1024.0 FROM sys.dm_db_partition_stats) AS total_size_mb
    """,
    'top_queries_cpu': """
        SELECT TOP {top_queries_count}
            qs.query_hash,
            SUBSTRING(st.text, (qs.statement_start_offset/2) + 1,
                ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
                  ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) + 1) AS query_text,
            qs.execution_count,
            qs.total_worker_time / 1000 AS total_cpu_ms,
            qs.total_worker_time / qs.execution_count / 1000 AS avg_cpu_ms,
            qs.total_elapsed_time / qs.execution_count / 1000 AS avg_duration_ms,
            qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
            qs.total_logical_writes / qs.execution_count AS avg_logical_writes,
            qs.creation_time AS plan_created,
            qs.last_execution_time
        FROM sys.dm_exec_query_stats AS qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
        WHERE qs.execution_count >= {min_execution_count}
        ORDER BY avg_cpu_ms DESC
    """,
    'top_queries_reads': """
        SELECT TOP {top_queries_count}
            qs.query_hash,
            SUBSTRING(st.text, (qs.statement_start_offset/2) + 1,
                ((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.text)
                  ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) + 1) AS query_text,
            qs.execution_count,
            qs.total_worker_time / 1000 AS total_cpu_ms,
            qs.total_worker_time / qs.execution_count / 1000 AS avg_cpu_ms,
            qs.total_elapsed_time / qs.execution_count / 1000 AS avg_duration_ms,
            qs.total_logical_reads / qs.execution_count AS avg_logical_reads,
            qs.total_logical_writes / qs.execution_count AS avg_logical_writes,
            qs.creation_time AS plan_created,
            qs.last_execution_time
        FROM sys.dm_exec_query_stats AS qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
        WHERE qs.execution_count >= {min_execution_count}
        ORDER BY avg_logical_reads DESC
    """,
    'wait_stats': """
        SELECT TOP 20
            wait_type,
            waiting_tasks_count,
            wait_time_ms,
            max_wait_time_ms,
            signal_wait_time_ms,
            wait_time_ms - signal_wait_time_ms AS resource_wait_time_ms,
            CAST(100.0 * wait_time_ms / SUM(wait_time_ms) OVER() AS DECIMAL(5,2)) AS pct_of_total
        FROM sys.dm_os_wait_stats
        WHERE wait_type NOT IN (
            'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE',
            'SLEEP_TASK', 'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH',
            'WAITFOR', 'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE',
            'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT',
            'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT',
            'CLR_AUTO_EVENT', 'DISPATCHER_QUEUE_SEMAPHORE',
            'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT',
            'XE_DISPATCHER_JOIN', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
            'ONDEMAND_TASK_QUEUE', 'BROKER_EVENTHANDLER',
            'SLEEP_BPOOL_FLUSH', 'DIRTY_PAGE_POLL',
            'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'SP_SERVER_DIAGNOSTICS_SLEEP',
            'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
            'WAIT_XTP_OFFLINE_CKPT_NEW_LOG'
        )
        AND waiting_tasks_count > 0
        ORDER BY wait_time_ms DESC
    """,
    'missing_indexes': """
        SELECT TOP 30
            CAST(migs.avg_total_user_cost * migs.avg_user_impact *
                 (migs.user_seeks + migs.user_scans) AS DECIMAL(28,1)) AS estimated_improvement,
            migs.avg_user_impact,
            migs.user_seeks,
            migs.user_scans,
            mid.statement AS table_name,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            'CREATE NONCLUSTERED INDEX [IX_' +
                REPLACE(REPLACE(REPLACE(mid.statement, '[', ''), ']', ''), '.', '_') + '_' +
                CAST(mid.index_handle AS VARCHAR) + '] ON ' + mid.statement +
                ' (' + ISNULL(mid.equality_columns, '') +
                CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL
                     THEN ', ' ELSE '' END +
                ISNULL(mid.inequality_columns, '') + ')' +
                ISNULL(' INCLUDE (' + mid.included_columns + ')', '') AS create_index_ddl
        FROM sys.dm_db_missing_index_groups mig
        JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
        JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
        WHERE migs.avg_user_impact > {missing_index_impact_threshold}
        ORDER BY estimated_improvement DESC
    """,
    'unused_indexes': """
        SELECT
            OBJECT_SCHEMA_NAME(i.object_id) AS schema_name,
            OBJECT_NAME(i.object_id) AS table_name,
            i.name AS index_name,
            i.type_desc AS index_type,
            ISNULL(s.user_seeks, 0) AS user_seeks,
            ISNULL(s.user_scans, 0) AS user_scans,
            ISNULL(s.user_lookups, 0) AS user_lookups,
            ISNULL(s.user_updates, 0) AS user_updates,
            ISNULL(s.last_user_seek, '1900-01-01') AS last_user_seek,
            ISNULL(s.last_user_scan, '1900-01-01') AS last_user_scan,
            ps.reserved_page_count * 8 / 1024.0 AS size_mb,
            'DROP INDEX [' + i.name + '] ON [' + OBJECT_SCHEMA_NAME(i.object_id) + '].[' + OBJECT_NAME(i.object_id) + ']' AS drop_index_ddl
        FROM sys.indexes AS i
        LEFT JOIN sys.dm_db_index_usage_stats AS s
            ON i.object_id = s.object_id AND i.index_id = s.index_id AND s.database_id = DB_ID()
        INNER JOIN sys.dm_db_partition_stats ps
            ON i.object_id = ps.object_id AND i.index_id = ps.index_id
        WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
            AND i.type_desc <> 'HEAP'
            AND i.is_primary_key = 0
            AND i.is_unique_constraint = 0
            AND i.is_unique = 0
            AND (ISNULL(s.user_seeks, 0) + ISNULL(s.user_scans, 0) + ISNULL(s.user_lookups, 0)) = 0
        ORDER BY s.user_updates DESC
    """,
    'duplicate_indexes': """
        WITH IndexColumns AS (
            SELECT
                i.object_id, i.index_id, i.name AS index_name, i.type_desc,
                i.is_primary_key, i.is_unique,
                STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns,
                STRING_AGG(CASE WHEN ic.is_included_column = 1 THEN c.name END, ',')
                    WITHIN GROUP (ORDER BY ic.key_ordinal) AS included_columns
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1 AND i.type_desc <> 'HEAP'
            GROUP BY i.object_id, i.index_id, i.name, i.type_desc, i.is_primary_key, i.is_unique
        )
        SELECT
            OBJECT_SCHEMA_NAME(a.object_id) AS schema_name,
            OBJECT_NAME(a.object_id) AS table_name,
            a.index_name AS index_a,
            a.type_desc AS type_a,
            b.index_name AS index_b,
            b.type_desc AS type_b,
            a.key_columns
        FROM IndexColumns a
        JOIN IndexColumns b ON a.object_id = b.object_id
            AND a.key_columns = b.key_columns
            AND a.index_id < b.index_id
        ORDER BY OBJECT_NAME(a.object_id)
    """,
    'index_fragmentation': """
        SELECT
            OBJECT_SCHEMA_NAME(ips.object_id) AS schema_name,
            OBJECT_NAME(ips.object_id) AS table_name,
            i.name AS index_name,
            i.type_desc AS index_type,
            ips.avg_fragmentation_in_percent,
            ips.page_count,
            ips.avg_page_space_used_in_percent,
            ips.record_count,
            CASE
                WHEN ips.avg_fragmentation_in_percent > 30 THEN
                    'ALTER INDEX [' + i.name + '] ON [' + OBJECT_SCHEMA_NAME(ips.object_id) + '].[' + OBJECT_NAME(ips.object_id) + '] REBUILD'
                WHEN ips.avg_fragmentation_in_percent > 10 THEN
                    'ALTER INDEX [' + i.name + '] ON [' + OBJECT_SCHEMA_NAME(ips.object_id) + '].[' + OBJECT_NAME(ips.object_id) + '] REORGANIZE'
                ELSE 'No action needed'
            END AS recommended_action
        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
        JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
        WHERE ips.page_count > {min_index_pages}
            AND i.name IS NOT NULL
            AND ips.avg_fragmentation_in_percent > {fragmentation_reorg_pct}
        ORDER BY ips.avg_fragmentation_in_percent DESC
    """,
    'index_usage_patterns': """
        SELECT
            OBJECT_SCHEMA_NAME(i.object_id) AS schema_name,
            OBJECT_NAME(i.object_id) AS table_name,
            i.name AS index_name,
            i.type_desc,
            ISNULL(s.user_seeks, 0) AS user_seeks,
            ISNULL(s.user_scans, 0) AS user_scans,
            ISNULL(s.user_lookups, 0) AS user_lookups,
            ISNULL(s.user_updates, 0) AS user_updates,
            CASE WHEN ISNULL(s.user_seeks,0)+ISNULL(s.user_scans,0) > ISNULL(s.user_updates,0) THEN 'READ_HEAVY'
                 WHEN ISNULL(s.user_updates,0) > ISNULL(s.user_seeks,0)+ISNULL(s.user_scans,0) THEN 'WRITE_HEAVY'
                 ELSE 'BALANCED' END AS usage_pattern
        FROM sys.indexes i
        LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id=s.object_id AND i.index_id=s.index_id AND s.database_id=DB_ID()
        WHERE OBJECTPROPERTY(i.object_id,'IsUserTable')=1 AND i.type>0 AND i.name IS NOT NULL
    """,
    'fk_without_index': """
        WITH FKColumnCounts AS (
            SELECT constraint_object_id, COUNT(*) AS fk_column_count
            FROM sys.foreign_key_columns GROUP BY constraint_object_id
        ),
        SingleColumnFK AS (
            SELECT fk.name AS fk_name, fk.parent_object_id,
                OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
                OBJECT_NAME(fk.parent_object_id) AS table_name,
                pc.name AS column_name,
                SUM(ps.row_count) AS row_count
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fk.object_id=fkc.constraint_object_id
            JOIN FKColumnCounts fkcc ON fk.object_id=fkcc.constraint_object_id AND fkcc.fk_column_count=1
            JOIN sys.columns pc ON fkc.parent_object_id=pc.object_id AND fkc.parent_column_id=pc.column_id
            JOIN sys.dm_db_partition_stats ps ON fk.parent_object_id=ps.object_id AND ps.index_id IN(0,1)
            WHERE fk.is_disabled=0 AND fk.is_not_trusted=0
            GROUP BY fk.name, fk.parent_object_id, pc.name
            HAVING SUM(ps.row_count) >= {fk_missing_index_min_rows}
        )
        SELECT fk.* FROM SingleColumnFK fk
        WHERE NOT EXISTS (
            SELECT 1 FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id=ic.object_id AND i.index_id=ic.index_id
            JOIN sys.columns c ON ic.object_id=c.object_id AND ic.column_id=c.column_id
            WHERE i.object_id=fk.parent_object_id AND i.is_disabled=0 AND i.type>0
                AND ic.is_included_column=0 AND ic.key_ordinal=1 AND c.name=fk.column_name
        )
        ORDER BY fk.row_count DESC
    """,
    'columnstore_candidates': """
        SELECT TOP 20
            OBJECT_SCHEMA_NAME(p.object_id) AS schema_name,
            OBJECT_NAME(p.object_id) AS table_name,
            SUM(p.row_count) AS row_count,
            SUM(p.reserved_page_count)*8/1024.0 AS size_mb,
            ISNULL(MAX(s.user_scans),0) AS scan_count,
            ISNULL(MAX(s.user_seeks),0) AS seek_count,
            ISNULL(MAX(s.user_updates),0) AS update_count
        FROM sys.dm_db_partition_stats p
        JOIN sys.objects o ON p.object_id=o.object_id
        LEFT JOIN sys.dm_db_index_usage_stats s ON p.object_id=s.object_id AND p.index_id=s.index_id AND s.database_id=DB_ID()
        LEFT JOIN sys.indexes ci ON p.object_id=ci.object_id AND ci.type=5
        WHERE o.type='U' AND ci.object_id IS NULL
        GROUP BY p.object_id
        HAVING SUM(p.row_count)>1000000 AND SUM(p.reserved_page_count)*8/1024.0>500
        ORDER BY size_mb DESC
    """,
    'table_sizes': """
        SELECT TOP 50
            OBJECT_SCHEMA_NAME(p.object_id) AS schema_name,
            OBJECT_NAME(p.object_id) AS table_name,
            SUM(p.row_count) AS row_count,
            SUM(p.reserved_page_count) * 8 / 1024.0 AS reserved_mb,
            SUM(p.in_row_data_page_count + p.lob_used_page_count + p.row_overflow_used_page_count) * 8 / 1024.0 AS data_mb,
            SUM(p.used_page_count - p.in_row_data_page_count - p.lob_used_page_count - p.row_overflow_used_page_count) * 8 / 1024.0 AS index_mb,
            SUM(p.reserved_page_count - p.used_page_count) * 8 / 1024.0 AS unused_mb
        FROM sys.dm_db_partition_stats p
        INNER JOIN sys.objects o ON p.object_id = o.object_id
        WHERE o.type = 'U'
        GROUP BY p.object_id
        ORDER BY reserved_mb DESC
    """,
    'database_files': """
        SELECT
            name AS file_name,
            type_desc AS file_type,
            physical_name,
            size * 8 / 1024.0 AS size_mb,
            FILEPROPERTY(name, 'SpaceUsed') * 8 / 1024.0 AS used_mb,
            (size - FILEPROPERTY(name, 'SpaceUsed')) * 8 / 1024.0 AS free_mb,
            CAST(FILEPROPERTY(name, 'SpaceUsed') * 100.0 / NULLIF(size, 0) AS DECIMAL(5,2)) AS used_pct,
            max_size,
            growth,
            is_percent_growth
        FROM sys.database_files
    """,
    'compression_candidates': """
        SELECT
            OBJECT_SCHEMA_NAME(t.object_id) AS schema_name,
            t.name AS table_name,
            p.data_compression_desc AS current_compression,
            SUM(ps.reserved_page_count) * 8 / 1024.0 AS size_mb,
            SUM(ps.row_count) AS row_count
        FROM sys.tables t
        JOIN sys.partitions p ON t.object_id = p.object_id
        JOIN sys.dm_db_partition_stats ps ON p.object_id = ps.object_id AND p.index_id = ps.index_id AND p.partition_number = ps.partition_number
        WHERE p.data_compression_desc = 'NONE'
            AND p.index_id IN (0, 1)
        GROUP BY t.object_id, t.name, p.data_compression_desc
        HAVING SUM(ps.reserved_page_count) * 8 / 1024.0 > 1
        ORDER BY size_mb DESC
    """,
    'service_tier': """
        SELECT
            DATABASEPROPERTYEX(DB_NAME(), 'Edition') AS edition,
            DATABASEPROPERTYEX(DB_NAME(), 'ServiceObjective') AS service_objective,
            DATABASEPROPERTYEX(DB_NAME(), 'MaxSizeInBytes') AS max_size_bytes,
            SERVERPROPERTY('EngineEdition') AS engine_edition,
            @@VERSION AS sql_version
    """,
    'data_type_audit': """
        SELECT
            TABLE_SCHEMA AS schema_name,
            TABLE_NAME AS table_name,
            COLUMN_NAME AS column_name,
            DATA_TYPE AS data_type,
            CHARACTER_MAXIMUM_LENGTH AS max_length,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE DATA_TYPE IN ('nvarchar', 'nchar', 'ntext', 'varchar', 'char')
            AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """,

    # ── NEW: Stored Procedure metrics ──
    'sp_execution_stats': """
        SELECT TOP {sp_top_count}
            OBJECT_SCHEMA_NAME(ps.object_id) AS schema_name,
            OBJECT_NAME(ps.object_id) AS procedure_name,
            ps.execution_count,
            ps.total_worker_time / 1000 AS total_cpu_ms,
            ps.total_worker_time / NULLIF(ps.execution_count,0) / 1000 AS avg_cpu_ms,
            ps.total_elapsed_time / NULLIF(ps.execution_count,0) / 1000 AS avg_duration_ms,
            ps.total_logical_reads / NULLIF(ps.execution_count,0) AS avg_logical_reads,
            ps.total_logical_writes / NULLIF(ps.execution_count,0) AS avg_logical_writes,
            ps.total_physical_reads / NULLIF(ps.execution_count,0) AS avg_physical_reads,
            ps.plan_generation_num AS recompile_count,
            ps.last_execution_time, ps.cached_time
        FROM sys.dm_exec_procedure_stats ps
        WHERE ps.database_id = DB_ID() AND ps.execution_count >= {sp_min_execution_count}
        ORDER BY avg_cpu_ms DESC
    """,
    'sp_source_code': """
        SELECT
            OBJECT_SCHEMA_NAME(p.object_id) AS schema_name,
            p.name AS procedure_name,
            LEN(m.definition) AS definition_length,
            CASE WHEN m.definition NOT LIKE '%SET NOCOUNT ON%' THEN 1 ELSE 0 END AS missing_nocount,
            CASE WHEN m.definition LIKE '%SELECT *%' THEN 1 ELSE 0 END AS has_select_star,
            CASE WHEN m.definition LIKE '%CURSOR%' THEN 1 ELSE 0 END AS uses_cursor,
            CASE WHEN m.definition LIKE '%EXEC(%' OR m.definition LIKE '%EXECUTE(%' THEN 1 ELSE 0 END AS uses_dynamic_exec,
            CASE WHEN m.definition LIKE '%DECLARE%@%' AND m.definition LIKE '%TABLE%' THEN 1 ELSE 0 END AS uses_table_variable,
            CASE WHEN m.definition LIKE '%OPTION (RECOMPILE)%' OR m.definition LIKE '%OPTION(RECOMPILE)%' THEN 1 ELSE 0 END AS uses_option_recompile,
            CASE WHEN m.definition LIKE '%BEGIN TRAN%' AND m.definition NOT LIKE '%TRY%' THEN 1 ELSE 0 END AS transaction_without_try
        FROM sys.procedures p
        JOIN sys.sql_modules m ON p.object_id = m.object_id
        WHERE p.is_ms_shipped = 0
    """,
    'sp_parameter_sniffing': """
        SELECT TOP {sp_top_count}
            OBJECT_SCHEMA_NAME(ps.object_id) AS schema_name,
            OBJECT_NAME(ps.object_id) AS procedure_name,
            ps.execution_count,
            ps.min_worker_time/1000 AS min_cpu_ms,
            ps.max_worker_time/1000 AS max_cpu_ms,
            ps.total_worker_time/NULLIF(ps.execution_count,0)/1000 AS avg_cpu_ms,
            CASE WHEN ps.max_worker_time/NULLIF(ps.min_worker_time,0) > 10 THEN 'SEVERE'
                 WHEN ps.max_worker_time/NULLIF(ps.min_worker_time,0) > 5  THEN 'MODERATE'
                 ELSE 'LOW' END AS sniffing_risk,
            ps.min_elapsed_time/1000 AS min_duration_ms,
            ps.max_elapsed_time/1000 AS max_duration_ms
        FROM sys.dm_exec_procedure_stats ps
        WHERE ps.database_id = DB_ID()
            AND ps.execution_count >= {sp_min_execution_count}
            AND ps.max_worker_time / NULLIF(ps.min_worker_time,0) > 5
        ORDER BY ps.max_worker_time / NULLIF(ps.min_worker_time,0) DESC
    """,

    # ── NEW: Views metrics ──
    'views_analysis': """
        SELECT
            OBJECT_SCHEMA_NAME(v.object_id) AS schema_name,
            v.name AS view_name,
            OBJECTPROPERTY(v.object_id, 'IsSchemaBound') AS is_schema_bound,
            OBJECTPROPERTY(v.object_id, 'IsIndexed') AS is_indexed,
            LEN(m.definition) AS definition_length,
            v.create_date, v.modify_date,
            CASE WHEN m.definition LIKE '%SELECT *%' THEN 1 ELSE 0 END AS has_select_star,
            CASE WHEN m.definition LIKE '%NOLOCK%' THEN 1 ELSE 0 END AS uses_nolock,
            CASE WHEN OBJECTPROPERTY(v.object_id, 'IsSchemaBound') = 0 THEN 1 ELSE 0 END AS missing_schemabinding,
            (SELECT COUNT(*) FROM sys.sql_expression_dependencies d
             WHERE d.referencing_id = v.object_id
             AND OBJECTPROPERTY(OBJECT_ID(ISNULL(d.referenced_schema_name,'dbo')+'.'+d.referenced_entity_name), 'IsView') = 1) AS nested_view_count
        FROM sys.views v
        LEFT JOIN sys.sql_modules m ON v.object_id = m.object_id
        WHERE v.is_ms_shipped = 0
    """,
    'indexed_view_candidates': """
        SELECT
            OBJECT_SCHEMA_NAME(v.object_id) AS schema_name,
            v.name AS view_name,
            ISNULL(s.user_seeks,0)+ISNULL(s.user_scans,0) AS total_reads,
            ISNULL(s.user_updates,0) AS total_writes
        FROM sys.views v
        LEFT JOIN sys.dm_db_index_usage_stats s ON v.object_id=s.object_id AND s.index_id<=1 AND s.database_id=DB_ID()
        WHERE v.is_ms_shipped=0 AND OBJECTPROPERTY(v.object_id,'IsIndexed')=0 AND OBJECTPROPERTY(v.object_id,'IsSchemaBound')=1
        ORDER BY total_reads DESC
    """,
    'indexed_view_usage': """
        SELECT
            OBJECT_SCHEMA_NAME(v.object_id) AS schema_name,
            v.name AS view_name,
            SUM(ISNULL(s.user_seeks,0)+ISNULL(s.user_scans,0)+ISNULL(s.user_lookups,0)) AS total_reads,
            SUM(ISNULL(s.user_updates,0)) AS total_writes,
            COUNT(i.index_id) AS indexed_view_index_count
        FROM sys.views v
        JOIN sys.indexes i ON v.object_id=i.object_id AND i.index_id>0
        LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id=s.object_id AND i.index_id=s.index_id AND s.database_id=DB_ID()
        WHERE v.is_ms_shipped=0 AND OBJECTPROPERTY(v.object_id,'IsIndexed')=1
        GROUP BY v.object_id, v.name
        ORDER BY total_writes DESC
    """,

    # ── NEW: Schema metrics ──
    'heap_tables': """
        SELECT OBJECT_SCHEMA_NAME(t.object_id) AS schema_name, t.name AS table_name,
            SUM(ps.row_count) AS row_count, SUM(ps.reserved_page_count)*8/1024.0 AS size_mb
        FROM sys.tables t
        JOIN sys.dm_db_partition_stats ps ON t.object_id=ps.object_id AND ps.index_id=0
        WHERE t.is_ms_shipped=0 GROUP BY t.object_id, t.name
    """,
    'tables_no_pk': """
        SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name,
            SUM(ps.row_count) AS row_count
        FROM sys.tables t
        LEFT JOIN sys.key_constraints kc ON t.object_id=kc.parent_object_id AND kc.type='PK'
        JOIN sys.dm_db_partition_stats ps ON t.object_id=ps.object_id AND ps.index_id IN(0,1)
        WHERE kc.object_id IS NULL AND t.is_ms_shipped=0
        GROUP BY t.schema_id, t.name
    """,
    'wide_tables': """
        SELECT TABLE_SCHEMA AS schema_name, TABLE_NAME AS table_name, COUNT(*) AS column_count
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA NOT IN ('sys','INFORMATION_SCHEMA')
        GROUP BY TABLE_SCHEMA, TABLE_NAME
        HAVING COUNT(*) > {wide_table_column_threshold}
        ORDER BY column_count DESC
    """,
    'lob_columns': """
        SELECT c.TABLE_SCHEMA AS schema_name, c.TABLE_NAME AS table_name,
            COUNT(*) AS lob_column_count,
            STRING_AGG(c.COLUMN_NAME+' '+c.DATA_TYPE, ', ') WITHIN GROUP (ORDER BY c.ORDINAL_POSITION) AS lob_columns
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA NOT IN ('sys','INFORMATION_SCHEMA')
            AND (c.DATA_TYPE IN ('text','ntext','image','xml') OR c.CHARACTER_MAXIMUM_LENGTH=-1)
        GROUP BY c.TABLE_SCHEMA, c.TABLE_NAME
        HAVING COUNT(*) > 0
        ORDER BY lob_column_count DESC
    """,

    # ── NEW: Activity metrics ──
    'current_activity': """
        SELECT s.login_name, s.host_name, s.program_name, s.status,
            s.cpu_time AS session_cpu_time, s.memory_usage*8 AS memory_kb,
            s.reads AS session_reads, s.writes AS session_writes,
            s.total_elapsed_time/1000 AS session_duration_ms,
            r.command, r.status AS request_status
        FROM sys.dm_exec_sessions s
        LEFT JOIN sys.dm_exec_requests r ON s.session_id=r.session_id
        WHERE s.is_user_process=1
    """,
    'operation_types': """
        SELECT
            CASE WHEN UPPER(LTRIM(st.text)) LIKE 'SELECT%' THEN 'SELECT'
                 WHEN UPPER(LTRIM(st.text)) LIKE 'INSERT%' THEN 'INSERT'
                 WHEN UPPER(LTRIM(st.text)) LIKE 'UPDATE%' THEN 'UPDATE'
                 WHEN UPPER(LTRIM(st.text)) LIKE 'DELETE%' THEN 'DELETE'
                 WHEN UPPER(LTRIM(st.text)) LIKE 'EXEC%'   THEN 'EXEC_SP'
                 WHEN UPPER(LTRIM(st.text)) LIKE 'CREATE%' OR UPPER(LTRIM(st.text)) LIKE 'ALTER%' OR UPPER(LTRIM(st.text)) LIKE 'DROP%' THEN 'DDL'
                 ELSE 'OTHER' END AS operation_type,
            COUNT(*) AS query_count,
            SUM(qs.execution_count) AS total_executions,
            SUM(qs.total_worker_time)/1000 AS total_cpu_ms,
            SUM(qs.total_logical_reads) AS total_reads,
            SUM(qs.total_logical_writes) AS total_writes
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        GROUP BY CASE WHEN UPPER(LTRIM(st.text)) LIKE 'SELECT%' THEN 'SELECT'
                      WHEN UPPER(LTRIM(st.text)) LIKE 'INSERT%' THEN 'INSERT'
                      WHEN UPPER(LTRIM(st.text)) LIKE 'UPDATE%' THEN 'UPDATE'
                      WHEN UPPER(LTRIM(st.text)) LIKE 'DELETE%' THEN 'DELETE'
                      WHEN UPPER(LTRIM(st.text)) LIKE 'EXEC%'   THEN 'EXEC_SP'
                      WHEN UPPER(LTRIM(st.text)) LIKE 'CREATE%' OR UPPER(LTRIM(st.text)) LIKE 'ALTER%' OR UPPER(LTRIM(st.text)) LIKE 'DROP%' THEN 'DDL'
                      ELSE 'OTHER' END
        ORDER BY total_cpu_ms DESC
    """,
    'connection_patterns': """
        SELECT s.program_name, s.host_name, COUNT(*) AS connection_count,
            SUM(CASE WHEN s.status='sleeping' THEN 1 ELSE 0 END) AS idle_connections,
            SUM(CASE WHEN s.status='running' THEN 1 ELSE 0 END) AS active_connections,
            AVG(s.cpu_time) AS avg_cpu_time, AVG(s.memory_usage)*8 AS avg_memory_kb
        FROM sys.dm_exec_sessions s
        WHERE s.is_user_process=1
        GROUP BY s.program_name, s.host_name ORDER BY connection_count DESC
    """,

    # ── NEW: Archival metrics ──
    'cold_tables': """
        SELECT OBJECT_SCHEMA_NAME(i.object_id) AS schema_name, OBJECT_NAME(i.object_id) AS table_name,
            MAX(ISNULL(s.last_user_seek,'1900-01-01')) AS last_seek,
            MAX(ISNULL(s.last_user_scan,'1900-01-01')) AS last_scan,
            MAX(ISNULL(s.last_user_lookup,'1900-01-01')) AS last_lookup,
            DATEDIFF(day, MAX(ISNULL(CASE WHEN s.last_user_seek>s.last_user_scan THEN s.last_user_seek ELSE s.last_user_scan END,'1900-01-01')), GETUTCDATE()) AS days_since_last_read,
            SUM(ps.reserved_page_count)*8/1024.0 AS size_mb
        FROM sys.indexes i
        LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id=s.object_id AND i.index_id=s.index_id AND s.database_id=DB_ID()
        JOIN sys.dm_db_partition_stats ps ON i.object_id=ps.object_id AND i.index_id=ps.index_id
        WHERE OBJECTPROPERTY(i.object_id,'IsUserTable')=1 AND i.index_id IN(0,1)
        GROUP BY i.object_id
        HAVING SUM(ps.reserved_page_count)*8/1024.0 > {archival_min_size_mb}
        ORDER BY days_since_last_read DESC
    """,
    'partition_candidates': """
        SELECT c.TABLE_SCHEMA AS schema_name, c.TABLE_NAME AS table_name,
            c.COLUMN_NAME AS datetime_column, c.DATA_TYPE AS column_type,
            ps.row_count, ps.size_mb
        FROM INFORMATION_SCHEMA.COLUMNS c
        JOIN (
            SELECT OBJECT_SCHEMA_NAME(p.object_id) AS schema_name, OBJECT_NAME(p.object_id) AS table_name,
                SUM(p.row_count) AS row_count, SUM(p.reserved_page_count)*8/1024.0 AS size_mb
            FROM sys.dm_db_partition_stats p
            JOIN sys.objects o ON p.object_id=o.object_id WHERE o.type='U'
            GROUP BY p.object_id
        ) ps ON c.TABLE_SCHEMA=ps.schema_name AND c.TABLE_NAME=ps.table_name
        WHERE c.DATA_TYPE IN ('datetime','datetime2','date','smalldatetime','datetimeoffset')
            AND c.TABLE_SCHEMA NOT IN ('sys','INFORMATION_SCHEMA')
            AND ps.size_mb > {partition_candidate_min_gb} * 1024
        ORDER BY ps.size_mb DESC
    """,

    # ── NEW: Operations metrics ──
    'blocking_chains': """
        SELECT r.session_id AS blocked_session_id, r.blocking_session_id,
            r.wait_type, r.wait_time/1000 AS wait_time_seconds, r.command,
            bs.login_name AS blocking_login, bs.host_name AS blocking_host,
            bs.program_name AS blocking_program
        FROM sys.dm_exec_requests r
        LEFT JOIN sys.dm_exec_sessions bs ON r.blocking_session_id=bs.session_id
        WHERE r.blocking_session_id > 0
        ORDER BY r.wait_time DESC
    """,
    'tempdb_usage': """
        SELECT s.session_id, s.login_name, s.host_name, s.program_name,
            t.user_objects_alloc_page_count*8/1024.0 AS user_objects_mb,
            t.internal_objects_alloc_page_count*8/1024.0 AS internal_objects_mb,
            (t.user_objects_alloc_page_count+t.internal_objects_alloc_page_count)*8/1024.0 AS total_tempdb_mb
        FROM sys.dm_db_session_space_usage t
        JOIN sys.dm_exec_sessions s ON t.session_id=s.session_id
        WHERE s.is_user_process=1 AND (t.user_objects_alloc_page_count+t.internal_objects_alloc_page_count)>0
        ORDER BY total_tempdb_mb DESC
    """,
    'log_space': """
        SELECT DB_NAME() AS database_name,
            total_log_size_in_bytes/1048576.0 AS total_log_size_mb,
            used_log_space_in_bytes/1048576.0 AS used_log_space_mb,
            used_log_space_in_percent,
            log_space_in_bytes_since_last_backup/1048576.0 AS log_since_backup_mb
        FROM sys.dm_db_log_space_usage
    """,
    'stale_statistics': """
        SELECT OBJECT_SCHEMA_NAME(s.object_id) AS schema_name,
            OBJECT_NAME(s.object_id) AS table_name,
            s.name AS stats_name, s.auto_created, s.user_created,
            STATS_DATE(s.object_id, s.stats_id) AS last_updated,
            DATEDIFF(day, STATS_DATE(s.object_id, s.stats_id), GETUTCDATE()) AS days_since_update,
            sp.rows AS table_rows, sp.modification_counter
        FROM sys.stats s
        CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
        WHERE OBJECTPROPERTY(s.object_id,'IsUserTable')=1
            AND DATEDIFF(day, STATS_DATE(s.object_id, s.stats_id), GETUTCDATE()) > {stale_stats_days}
            AND sp.rows > 1000
        ORDER BY sp.modification_counter DESC
    """,
    'plan_cache': """
        SELECT objtype AS plan_type, COUNT(*) AS plan_count,
            SUM(size_in_bytes)/1048576.0 AS total_size_mb,
            SUM(usecounts) AS total_use_count,
            SUM(CASE WHEN usecounts=1 THEN 1 ELSE 0 END) AS single_use_count,
            SUM(CASE WHEN usecounts=1 THEN size_in_bytes ELSE 0 END)/1048576.0 AS single_use_size_mb
        FROM sys.dm_exec_cached_plans
        GROUP BY objtype ORDER BY total_size_mb DESC
    """,
    'db_options': """
        SELECT name AS database_name,
            is_auto_create_stats_on, is_auto_update_stats_on,
            is_read_committed_snapshot_on, snapshot_isolation_state_desc,
            is_parameterization_forced
        FROM sys.databases WHERE database_id=DB_ID()
    """,
    'query_store_status': """
        SELECT actual_state_desc FROM sys.database_query_store_options
    """,
    'auto_tuning_recommendations': """
        SELECT name AS recommendation_name, reason, type AS recommendation_type,
            valid_since, state,
            JSON_VALUE(details, '$.implementationDetails.script') AS implementation_script,
            JSON_VALUE(score, '$.currentValue') AS current_score,
            JSON_VALUE(score, '$.expectedImprovement') AS expected_improvement
        FROM sys.dm_db_tuning_recommendations
        WHERE state = 'Active'
        ORDER BY JSON_VALUE(score, '$.expectedImprovement') DESC
    """,
    'long_running_transactions': """
        SELECT at.transaction_id, at.name AS transaction_name,
            at.transaction_begin_time,
            DATEDIFF(second, at.transaction_begin_time, GETUTCDATE()) AS duration_seconds,
            s.session_id, s.login_name, s.host_name, s.program_name,
            dt.database_transaction_log_bytes_used/1048576.0 AS log_used_mb
        FROM sys.dm_tran_active_transactions at
        JOIN sys.dm_tran_session_transactions st ON at.transaction_id=st.transaction_id
        JOIN sys.dm_exec_sessions s ON st.session_id=s.session_id
        LEFT JOIN sys.dm_tran_database_transactions dt ON at.transaction_id=dt.transaction_id
        WHERE at.transaction_type=1 AND DATEDIFF(second, at.transaction_begin_time, GETUTCDATE()) > {long_transaction_threshold_seconds}
        ORDER BY duration_seconds DESC
    """,
}

# List of all metric names (used by both notebooks)
ALL_METRICS = list(INCREMENTAL_QUERIES.keys()) + list(SNAPSHOT_QUERIES.keys())

# Wait types to filter out (background / benign waits)
BENIGN_WAIT_TYPES = [
    'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE',
    'SLEEP_TASK', 'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH',
    'WAITFOR', 'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE',
    'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT',
    'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT',
    'CLR_AUTO_EVENT', 'DISPATCHER_QUEUE_SEMAPHORE',
    'FT_IFTS_SCHEDULER_IDLE_WAIT', 'XE_DISPATCHER_WAIT',
    'XE_DISPATCHER_JOIN', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
    'ONDEMAND_TASK_QUEUE', 'BROKER_EVENTHANDLER',
    'SLEEP_BPOOL_FLUSH', 'DIRTY_PAGE_POLL',
    'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'SP_SERVER_DIAGNOSTICS_SLEEP',
    'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
    'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
    'WAIT_XTP_OFFLINE_CKPT_NEW_LOG',
]

# Wait type → bottleneck category mapping (for the recommendation engine)
WAIT_CATEGORIES = {
    'SOS_SCHEDULER_YIELD': 'CPU',
    'THREADPOOL': 'CPU',
    'PAGEIOLATCH_SH': 'IO',
    'PAGEIOLATCH_EX': 'IO',
    'WRITELOG': 'IO',
    'IO_COMPLETION': 'IO',
    'LCK_M_X': 'Locking',
    'LCK_M_S': 'Locking',
    'LCK_M_U': 'Locking',
    'LCK_M_IX': 'Locking',
    'LCK_M_IS': 'Locking',
    'RESOURCE_SEMAPHORE': 'Memory',
    'RESOURCE_SEMAPHORE_QUERY_COMPILE': 'Memory',
    'ASYNC_NETWORK_IO': 'Network',
    'CXPACKET': 'Parallelism',
    'CXCONSUMER': 'Parallelism',
    'LOG_RATE_GOVERNOR': 'LOG_RATE',
}
