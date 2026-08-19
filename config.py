"""
Azure SQL Server Recommendation Engine - Unified Configuration
==============================================================

Single configuration file containing all settings, SQL queries, pricing catalogs,
thresholds, and shared utilities for the two-notebook Databricks architecture.

- Notebook 1 (Data Collector):  Imports QUERIES, WATERMARK_CONFIG, STORAGE_CONFIG
- Notebook 2 (Recommendation Engine):  Imports THRESHOLDS, PRICING, SEVERITY_SCORES, etc.
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


class Severity(Enum):
    CRITICAL = "Critical"
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"
    INFO = "Info"


class Effort(Enum):
    QUICK_WIN = "Quick Win"
    MODERATE = "Moderate"
    SIGNIFICANT = "Significant"


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
    storage_format: str = "parquet"      # 'parquet', 'delta', or 'json'

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

    # ── Storage Thresholds ──
    compression_savings_min_pct: float = 20.0
    table_size_concern_gb: float = 10.0
    nvarchar_audit_enabled: bool = True
    max_column_oversized_ratio: float = 5.0

    # ── Cost Thresholds ──
    underutilized_cpu_pct: float = 25.0
    underutilized_io_pct: float = 25.0
    idle_period_threshold_hours: float = 1.0

    # ── Priority Weights (must sum to 1.0) ──
    weight_performance: float = 0.40
    weight_storage: float = 0.30
    weight_cost: float = 0.30

    # ── Output ──
    output_dir: str = "./reports"
    report_filename: str = "azure_sql_advisor_report.html"
    delta_table_name: str = "azure_sql_recommendations"

    def __post_init__(self):
        total_weight = self.weight_performance + self.weight_storage + self.weight_cost
        if not math.isclose(total_weight, 1.0, abs_tol=0.001):
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


# =============================================================================
# 5. SQL QUERIES CATALOG
#    Used by Notebook 1 (Data Collector).
#    Keys match the metric names used in storage paths & watermarks.
# =============================================================================

# Queries that support incremental (time-filtered) extraction.
# Each returns a tuple of (base_sql, watermark_column_name) so the notebook
# can inject the WHERE clause dynamically.
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
}
