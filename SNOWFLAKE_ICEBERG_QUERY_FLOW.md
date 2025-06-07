# Snowflake Query Execution with S3 Iceberg Tables

## Overview

When Snowflake queries S3-backed Iceberg tables, it leverages a sophisticated execution model that combines Snowflake's compute capabilities with Iceberg's metadata structure and S3 storage. This document explains the complete query execution flow.

## Architecture Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Snowflake     │    │   AWS Glue      │    │   S3 Iceberg    │
│   Query Engine  │◀──▶│   Catalog       │◀──▶│   Tables        │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Virtual        │    │  Metadata       │    │  Data Files     │
│  Warehouses     │    │  Management     │    │  (Parquet)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Query Execution Flow

### 1. Query Submission & Parsing

```sql
-- Example query against Iceberg table
SELECT customer_id, customer_name, order_total
FROM iceberg_catalog.analytics.customers 
WHERE customer_region = 'US' 
  AND registration_date >= '2024-01-01'
ORDER BY order_total DESC
LIMIT 100;
```

**What Happens:**
1. **Query Parsing**: Snowflake parses the SQL and identifies `iceberg_catalog.analytics.customers` as an Iceberg table
2. **Catalog Resolution**: Determines the query targets an external Iceberg table via the configured catalog integration
3. **Security Check**: Validates user permissions for the external catalog and S3 locations

### 2. Metadata Discovery & Planning

#### 2.1 Iceberg Metadata Retrieval
```
S3 Iceberg Table Structure:
s3://warehouse/customers/
├── metadata/
│   ├── v1.metadata.json          ← Current table metadata
│   ├── v2.metadata.json          ← Previous version
│   ├── snap-001.avro             ← Snapshot manifest
│   ├── snap-002.avro             ← Latest snapshot
│   └── manifests/
│       ├── manifest-001.avro     ← File listings
│       └── manifest-002.avro
└── data/
    ├── customer_region=US/
    │   ├── reg_date=2024-01-01/
    │   │   ├── data-001.parquet
    │   │   └── data-002.parquet
    │   └── reg_date=2024-01-02/
    └── customer_region=EU/
```

**Snowflake's Metadata Reading Process:**

1. **Read Current Metadata**:
   ```json
   // v2.metadata.json
   {
     "format-version": 2,
     "table-uuid": "12345-67890-abcdef",
     "location": "s3://warehouse/customers",
     "current-snapshot-id": 5678901234,
     "schema": {
       "type": "struct",
       "fields": [
         {"id": 1, "name": "customer_id", "type": "long", "required": true},
         {"id": 2, "name": "customer_name", "type": "string", "required": true},
         {"id": 3, "name": "customer_region", "type": "string", "required": true},
         {"id": 4, "name": "registration_date", "type": "date", "required": true},
         {"id": 5, "name": "order_total", "type": "decimal(10,2)", "required": false}
       ]
     },
     "partition-spec": [
       {"field-id": 3, "transform": "identity", "name": "customer_region"},
       {"field-id": 4, "transform": "day", "name": "reg_date"}
     ],
     "snapshots": [...],
     "snapshot-log": [...]
   }
   ```

2. **Read Current Snapshot**:
   ```json
   // snap-002.avro (current snapshot)
   {
     "snapshot-id": 5678901234,
     "timestamp-ms": 1704067200000,
     "manifest-list": "s3://warehouse/customers/metadata/snap-002-manifest-list.avro",
     "summary": {
       "operation": "append",
       "added-files-count": "15",
       "added-records-count": "1500000",
       "total-files-count": "145",
       "total-records-count": "25000000"
     }
   }
   ```

#### 2.2 Query Planning with Iceberg Metadata

**Snowflake's Query Planner Actions:**

1. **Partition Pruning**:
   ```
   Filter: customer_region = 'US' AND registration_date >= '2024-01-01'
   
   Partition Analysis:
   ✅ customer_region=US (matches filter)
   ❌ customer_region=EU (excluded by filter)
   ✅ reg_date=2024-01-01 (matches filter)  
   ✅ reg_date=2024-01-02 (matches filter)
   ❌ reg_date=2023-12-31 (excluded by filter)
   
   Result: Only scan partitions with customer_region=US AND reg_date >= 2024-01-01
   ```

2. **Manifest File Analysis**:
   ```json
   // For each relevant partition, read manifest files
   {
     "manifest-path": "s3://warehouse/customers/metadata/manifests/manifest-002.avro",
     "partitions": [
       {
         "partition": {"customer_region": "US", "reg_date": "2024-01-01"},
         "data-files": [
           {
             "file-path": "s3://warehouse/customers/data/customer_region=US/reg_date=2024-01-01/data-001.parquet",
             "file-size-bytes": 134217728,
             "record-count": 50000,
             "column-statistics": {
               "customer_id": {"min": 1000000, "max": 1050000},
               "order_total": {"min": 10.50, "max": 5000.00},
               "registration_date": {"min": "2024-01-01", "max": "2024-01-01"}
             }
           }
         ]
       }
     ]
   }
   ```

3. **File-Level Pruning**:
   ```
   Using column statistics from manifest:
   - File data-001.parquet: order_total max = 5000.00 ✅ (might contain high values)
   - File data-002.parquet: order_total max = 100.00 ❌ (won't contribute to TOP 100)
   
   Result: Further reduce files to scan based on column statistics
   ```

### 3. Execution Plan Generation

**Snowflake generates an optimized execution plan:**

```
QUERY PLAN:
┌─────────────────────────────────────────┐
│ 1. LIMIT (100)                          │
└─────────────────────────────────────────┘
                    ▲
┌─────────────────────────────────────────┐
│ 2. ORDER BY order_total DESC            │
└─────────────────────────────────────────┘
                    ▲
┌─────────────────────────────────────────┐
│ 3. PROJECT (customer_id, customer_name, │
│             order_total)                 │
└─────────────────────────────────────────┘
                    ▲
┌─────────────────────────────────────────┐
│ 4. FILTER (customer_region = 'US' AND   │
│            registration_date >= '2024-01-01') │
└─────────────────────────────────────────┘
                    ▲
┌─────────────────────────────────────────┐
│ 5. EXTERNAL_SCAN                        │
│    - Catalog: iceberg_catalog           │
│    - Table: analytics.customers         │
│    - Pruned Partitions: 2 of 50        │
│    - Pruned Files: 8 of 145            │
│    - Estimated Rows: 100,000 of 25M    │
└─────────────────────────────────────────┘
```

### 4. Distributed Execution

#### 4.1 Virtual Warehouse Allocation
```
Virtual Warehouse: ANALYTICS_WH (MEDIUM)
├── Node 1: Processes files 1-3
├── Node 2: Processes files 4-6  
└── Node 3: Processes files 7-8
```

#### 4.2 Parallel S3 Reading

**Each compute node performs:**

1. **Direct S3 Access**:
   ```python
   # Pseudocode for Snowflake's S3 reading
   for file_path in assigned_files:
       # Read Parquet file directly from S3
       parquet_file = s3_client.get_object(
           Bucket='warehouse',
           Key=file_path
       )
       
       # Apply column projection (only read needed columns)
       columns = ['customer_id', 'customer_name', 'order_total', 
                 'customer_region', 'registration_date']
       
       # Apply row-level filters during read
       dataframe = read_parquet(
           parquet_file,
           columns=columns,
           filters=[
               ('customer_region', '==', 'US'),
               ('registration_date', '>=', '2024-01-01')
           ]
       )
   ```

2. **Vectorized Processing**:
   ```
   Parquet Columnar Read:
   ┌─────────────┬─────────────┬─────────────┐
   │ customer_id │customer_name│ order_total │
   ├─────────────┼─────────────┼─────────────┤
   │  [1M rows]  │  [1M rows]  │  [1M rows]  │ ← Vectorized batches
   │     ...     │     ...     │     ...     │
   └─────────────┴─────────────┴─────────────┘
   
   Filter Application:
   ✅ customer_region = 'US': 400K rows remain
   ✅ registration_date >= '2024-01-01': 300K rows remain
   ```

#### 4.3 Compute Node Processing

**Each node processes its assigned data:**

```sql
-- Node-level processing (simplified)
WITH filtered_data AS (
  SELECT customer_id, customer_name, order_total
  FROM parquet_files_1_to_3
  WHERE customer_region = 'US' 
    AND registration_date >= '2024-01-01'
),
sorted_data AS (
  SELECT *, ROW_NUMBER() OVER (ORDER BY order_total DESC) as rn
  FROM filtered_data
)
SELECT customer_id, customer_name, order_total
FROM sorted_data 
WHERE rn <= 100;  -- Each node contributes top 100
```

### 5. Result Aggregation & Return

#### 5.1 Cross-Node Coordination
```
Final Aggregation:
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│    Node 1    │    │    Node 2    │    │    Node 3    │
│  Top 100     │    │  Top 100     │    │  Top 100     │
│  Results     │    │  Results     │    │  Results     │
└──────────────┘    └──────────────┘    └──────────────┘
         │                   │                   │
         └───────────────────┼───────────────────┘
                            ▼
                  ┌──────────────────┐
                  │  Coordinator     │
                  │  - Merge 300     │
                  │  - Sort by       │
                  │    order_total   │
                  │  - Return top    │
                  │    100 final     │
                  └──────────────────┘
```

#### 5.2 Final Result Assembly
```sql
-- Coordinator node final processing
WITH all_node_results AS (
  SELECT * FROM node_1_results
  UNION ALL
  SELECT * FROM node_2_results  
  UNION ALL
  SELECT * FROM node_3_results
)
SELECT customer_id, customer_name, order_total
FROM all_node_results
ORDER BY order_total DESC
LIMIT 100;
```

## Performance Optimizations

### 1. Metadata Caching
```
Snowflake caches:
├── Table metadata (schema, partitioning)
├── Snapshot information
├── Manifest file contents
└── Column statistics

Cache Duration: 
- Metadata: 24 hours (configurable)
- Statistics: 1 hour (auto-refresh on writes)
```

### 2. Predicate Pushdown
```sql
-- Original Query
SELECT * FROM customers WHERE order_total > 1000;

-- Pushdown to S3/Parquet level
READ_PARQUET(
  file_path,
  filters=[('order_total', '>', 1000)]  ← Applied during file read
)
```

### 3. Column Projection
```sql
-- Only read required columns from Parquet
SELECT customer_name, order_total  -- Only these columns read from S3
FROM customers 
WHERE customer_region = 'US';
```

### 4. Parallel S3 Access
```
S3 Access Pattern:
├── Multiple compute nodes read different files simultaneously
├── Each node uses multiple threads for S3 GET operations
├── Optimal file sizes (128MB-512MB) for parallel processing
└── S3 Transfer Acceleration for cross-region access
```

## Query Performance Characteristics

### Excellent Performance Scenarios
1. **Partition-Aligned Queries**:
   ```sql
   -- Fast: Uses partition pruning
   SELECT * FROM customers 
   WHERE customer_region = 'US' AND registration_date = '2024-01-01';
   ```

2. **Column-Selective Queries**:
   ```sql
   -- Fast: Only reads needed columns
   SELECT customer_id, customer_name FROM customers;
   ```

3. **Aggregation Queries**:
   ```sql
   -- Fast: Leverages column statistics
   SELECT COUNT(*), AVG(order_total) FROM customers 
   WHERE customer_region = 'US';
   ```

### Performance Considerations
1. **Full Table Scans**:
   ```sql
   -- Slower: No partition pruning possible
   SELECT * FROM customers WHERE customer_name LIKE '%John%';
   ```

2. **Cross-Partition Joins**:
   ```sql
   -- Slower: May require shuffling data
   SELECT c.*, o.* FROM customers c 
   JOIN orders o ON c.customer_id = o.customer_id;
   ```

## Monitoring & Debugging

### Query Profiling
```sql
-- Check query execution details
SELECT 
    query_id,
    execution_time,
    bytes_scanned,
    partitions_scanned,
    partitions_total
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text LIKE '%customers%'
ORDER BY start_time DESC;
```

### Iceberg Table Statistics
```sql
-- View table scan efficiency
SELECT 
    table_name,
    files_scanned,
    files_total,
    (files_scanned::float / files_total::float) * 100 AS scan_efficiency_pct
FROM TABLE(INFORMATION_SCHEMA.ICEBERG_TABLE_SCAN_METRICS('customers'));
```

### Performance Tuning
```sql
-- Analyze partition effectiveness
SELECT 
    partition_value,
    file_count,
    total_size_bytes,
    avg_file_size_mb
FROM TABLE(INFORMATION_SCHEMA.ICEBERG_PARTITION_STATISTICS('customers'))
ORDER BY total_size_bytes DESC;
```

## Best Practices for Optimal Performance

### 1. Partition Strategy
```json
// Optimal partitioning for time-series data
{
  "partition-spec": [
    {"field": "event_date", "transform": "day"},      // High cardinality, query-aligned
    {"field": "region", "transform": "identity"}      // Low cardinality, filter-aligned
  ]
}
```

### 2. File Size Optimization
```python
# Optimal file sizes for Snowflake queries
target_file_size = 128 * 1024 * 1024  # 128MB
max_file_size = 512 * 1024 * 1024     # 512MB

# Configure in Iceberg table properties
table_properties = {
    'write.target-file-size-bytes': str(target_file_size),
    'write.max-file-size-bytes': str(max_file_size)
}
```

### 3. Column Statistics Maintenance
```sql
-- Ensure fresh statistics for optimal query planning
CALL SYSTEM$ICEBERG_TABLE_OPTIMIZE('customers', 'REWRITE_DATA_FILES');
CALL SYSTEM$ICEBERG_TABLE_OPTIMIZE('customers', 'REWRITE_MANIFESTS');
```

---

## Summary

Snowflake's query execution against S3 Iceberg tables provides:

1. **Intelligent Metadata Usage**: Leverages Iceberg's rich metadata for query optimization
2. **Efficient Data Access**: Direct S3 reads with columnar processing
3. **Parallel Execution**: Distributed processing across virtual warehouse nodes
4. **Advanced Optimizations**: Partition pruning, predicate pushdown, and statistics-based planning

This architecture delivers the performance benefits of a cloud data warehouse while maintaining the flexibility and cost-effectiveness of a data lake storage model.