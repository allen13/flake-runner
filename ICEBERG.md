# Apache Iceberg Integration Design

## Overview

This document outlines the integration of Apache Iceberg with the flake-runner orchestration service, leveraging Snowflake's Iceberg compatibility, S3-based Iceberg tables, and AWS Glue Catalog for metadata management.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│  Flake Runner   │───▶│ S3 Iceberg Tables│
│                 │    │   (EMR 7.8.0)   │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │                        │
                              ▼                        ▼
                    ┌─────────────────┐    ┌─────────────────┐
                    │  AWS Glue       │◀───│   Snowflake     │
                    │  Catalog        │    │  (Iceberg)      │
                    └─────────────────┘    └─────────────────┘
```

## Key Benefits

### 🎯 **Transactional Data Lake**
- **ACID transactions** for S3 data
- **Time travel** and **snapshot isolation**
- **Schema evolution** without breaking changes
- **Efficient updates/deletes** in data lake

### 🚀 **Performance Improvements**
- **Partition pruning** and **predicate pushdown**
- **Column-level statistics** for better query planning
- **Incremental data processing** with manifest files
- **Concurrent reads/writes** without conflicts

### 🔄 **Unified Analytics**
- **Single source of truth** in S3 Iceberg format
- **Cross-engine compatibility** (Spark, Snowflake, Trino, etc.)
- **Consistent metadata** via AWS Glue Catalog
- **Real-time and batch processing** on same tables

## Implementation Strategy

### Phase 1: Infrastructure Setup

#### 1.1 AWS Glue Catalog Configuration
```json
{
  "glue_catalog_config": {
    "database_name": "flake_runner_iceberg",
    "table_prefix": "iceberg_",
    "warehouse_path": "s3://your-iceberg-warehouse/",
    "catalog_id": "123456789012",
    "region": "us-east-1"
  }
}
```

#### 1.2 S3 Iceberg Warehouse Structure
```
s3://your-iceberg-warehouse/
├── customers/
│   ├── metadata/
│   │   ├── v1.metadata.json
│   │   ├── v2.metadata.json
│   │   └── snap-*.avro
│   └── data/
│       ├── year=2024/month=01/
│       └── year=2024/month=02/
├── orders/
│   ├── metadata/
│   └── data/
└── analytics_events/
    ├── metadata/
    └── data/
```

#### 1.3 Snowflake Iceberg Configuration
```sql
-- Create Iceberg catalog integration
CREATE CATALOG INTEGRATION iceberg_glue_catalog
  CATALOG_SOURCE=GLUE
  TABLE_FORMAT=ICEBERG
  GLUE_AWS_ROLE_ARN='arn:aws:iam::123456789012:role/SnowflakeIcebergRole'
  GLUE_CATALOG_ID='123456789012'
  GLUE_REGION='us-east-1'
  ENABLED=TRUE;

-- Create external volume for S3 access
CREATE EXTERNAL VOLUME iceberg_s3_volume
  STORAGE_LOCATIONS = (
    (
      NAME = 'iceberg-warehouse'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://your-iceberg-warehouse/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeIcebergRole'
    )
  );
```

### Phase 2: Enhanced Configuration Schema

#### 2.1 Updated Types Definition
```go
// IcebergConfig defines Iceberg-specific configuration
type IcebergConfig struct {
    Enabled             bool              `json:"enabled"`
    GlueCatalogDatabase string            `json:"glue_catalog_database"`
    WarehousePath       string            `json:"warehouse_path"`
    TableProperties     map[string]string `json:"table_properties"`
    PartitionSpec       []PartitionField  `json:"partition_spec"`
    SortOrder          []SortField       `json:"sort_order"`
}

// PartitionField defines Iceberg partitioning
type PartitionField struct {
    SourceColumn string `json:"source_column"`
    Transform    string `json:"transform"`     // identity, bucket, truncate, year, month, day, hour
    Name         string `json:"name"`
}

// SortField defines Iceberg sorting
type SortField struct {
    Column    string `json:"column"`
    Direction string `json:"direction"`  // asc, desc
    NullOrder string `json:"null_order"` // first, last
}

// Updated PrefixTableMapping with Iceberg support
type PrefixTableMapping struct {
    S3Prefix         string           `json:"s3_prefix"`
    SnowflakeTable   string           `json:"snowflake_table"`
    SnowflakeSchema  string           `json:"snowflake_schema,omitempty"`
    ContainerImage   string           `json:"container_image"`
    EntryPoint       string           `json:"entry_point,omitempty"`
    ValidationRules  ValidationRules  `json:"validation_rules"`
    ProcessingConfig ProcessingConfig `json:"processing_config"`
    IcebergConfig    IcebergConfig    `json:"iceberg_config,omitempty"`
}
```

#### 2.2 Enhanced Configuration Example
```json
{
  "iceberg_global_config": {
    "enabled": true,
    "glue_catalog_database": "flake_runner_iceberg",
    "warehouse_path": "s3://your-iceberg-warehouse/",
    "default_table_properties": {
      "write.format.default": "parquet",
      "write.parquet.compression-codec": "snappy",
      "commit.retry.num-retries": "3",
      "commit.retry.min-wait-ms": "100"
    }
  },
  "prefix_table_mappings": [
    {
      "s3_prefix": "customers/",
      "snowflake_table": "CUSTOMERS",
      "container_image": "account.dkr.ecr.region.amazonaws.com/flake-runner:iceberg-v1.0.0",
      "iceberg_config": {
        "enabled": true,
        "glue_catalog_database": "flake_runner_iceberg",
        "warehouse_path": "s3://your-iceberg-warehouse/customers/",
        "partition_spec": [
          {
            "source_column": "_processing_timestamp",
            "transform": "day",
            "name": "processing_day"
          },
          {
            "source_column": "customer_region",
            "transform": "identity",
            "name": "region"
          }
        ],
        "sort_order": [
          {
            "column": "customer_id",
            "direction": "asc",
            "null_order": "last"
          }
        ],
        "table_properties": {
          "write.target-file-size-bytes": "134217728",
          "write.delete.mode": "merge-on-read"
        }
      }
    },
    {
      "s3_prefix": "analytics/",
      "snowflake_table": "ANALYTICS_EVENTS",
      "container_image": "account.dkr.ecr.region.amazonaws.com/flake-runner:iceberg-v1.0.0",
      "iceberg_config": {
        "enabled": true,
        "glue_catalog_database": "flake_runner_iceberg", 
        "warehouse_path": "s3://your-iceberg-warehouse/analytics_events/",
        "partition_spec": [
          {
            "source_column": "event_timestamp",
            "transform": "hour",
            "name": "event_hour"
          },
          {
            "source_column": "event_type",
            "transform": "identity",
            "name": "event_type"
          }
        ],
        "sort_order": [
          {
            "column": "event_timestamp",
            "direction": "asc",
            "null_order": "last"
          },
          {
            "column": "user_id",
            "direction": "asc",
            "null_order": "last"
          }
        ]
      }
    }
  ]
}
```

### Phase 3: Enhanced PySpark Processor

#### 3.1 Iceberg-Enabled Processor
```python
#!/usr/bin/env python3
"""
Iceberg-enabled PySpark processor for EMR Serverless
Supports Apache Iceberg format with Glue Catalog integration
"""

import argparse
import sys
import logging
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, date_format, hour

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IcebergDataProcessor:
    """Enhanced data processor with Apache Iceberg support"""
    
    def __init__(self, spark: SparkSession, iceberg_config: Dict[str, Any]):
        self.spark = spark
        self.iceberg_config = iceberg_config
        self.catalog_name = "glue_catalog"
        
    def create_spark_session_with_iceberg(job_id: str, target_table: str, 
                                        glue_catalog_database: str) -> SparkSession:
        """Create Spark session with Iceberg and Glue Catalog support"""
        app_name = f"FlakeRunner-Iceberg-{target_table}-{job_id}"
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.glue_catalog.warehouse", "s3://your-iceberg-warehouse/") \
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.glue_catalog.glue.region", "us-east-1") \
            .config("spark.sql.catalog.glue_catalog.glue.catalog-id", "123456789012") \
            .config("spark.sql.defaultCatalog", "glue_catalog") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("INFO")
        return spark
    
    def create_iceberg_table_if_not_exists(self, table_name: str, 
                                         df_schema: DataFrame, 
                                         partition_spec: List[Dict],
                                         sort_order: List[Dict],
                                         table_properties: Dict[str, str]) -> None:
        """Create Iceberg table in Glue Catalog if it doesn't exist"""
        
        database = self.iceberg_config.get('glue_catalog_database', 'default')
        full_table_name = f"{self.catalog_name}.{database}.{table_name}"
        
        # Check if table exists
        try:
            self.spark.sql(f"DESCRIBE TABLE {full_table_name}")
            logger.info(f"Iceberg table {full_table_name} already exists")
            return
        except Exception:
            logger.info(f"Creating new Iceberg table: {full_table_name}")
        
        # Build CREATE TABLE statement
        schema_sql = self._build_schema_sql(df_schema.schema)
        partition_sql = self._build_partition_sql(partition_spec)
        sort_sql = self._build_sort_sql(sort_order)
        properties_sql = self._build_properties_sql(table_properties)
        
        create_sql = f"""
        CREATE TABLE {full_table_name} (
            {schema_sql}
        ) USING ICEBERG
        {partition_sql}
        {sort_sql}
        {properties_sql}
        """
        
        logger.info(f"Creating table with SQL: {create_sql}")
        self.spark.sql(create_sql)
        logger.info(f"Successfully created Iceberg table: {full_table_name}")
    
    def write_to_iceberg(self, df: DataFrame, table_name: str, 
                        write_mode: str = "append") -> None:
        """Write DataFrame to Iceberg table with optimizations"""
        
        database = self.iceberg_config.get('glue_catalog_database', 'default')
        full_table_name = f"{self.catalog_name}.{database}.{table_name}"
        
        logger.info(f"Writing {df.count()} records to Iceberg table: {full_table_name}")
        
        # Optimize write performance
        writer = df.write \
            .format("iceberg") \
            .mode(write_mode) \
            .option("write-audit-publish", "true") \
            .option("check-nullability", "false")
        
        # Add table properties for better performance
        if write_mode == "append":
            writer = writer.option("merge-schema", "true")
        
        writer.saveAsTable(full_table_name)
        
        logger.info(f"Successfully wrote data to Iceberg table: {full_table_name}")
        
        # Optional: Optimize table after write for better read performance
        if write_mode == "overwrite":
            self._optimize_iceberg_table(full_table_name)
    
    def _optimize_iceberg_table(self, full_table_name: str) -> None:
        """Optimize Iceberg table for better query performance"""
        try:
            # Rewrite data files to optimize file sizes
            self.spark.sql(f"CALL {self.catalog_name}.system.rewrite_data_files('{full_table_name}')")
            
            # Rewrite manifest files
            self.spark.sql(f"CALL {self.catalog_name}.system.rewrite_manifests('{full_table_name}')")
            
            logger.info(f"Optimized Iceberg table: {full_table_name}")
        except Exception as e:
            logger.warning(f"Failed to optimize table {full_table_name}: {e}")
    
    def merge_into_iceberg(self, source_df: DataFrame, target_table: str,
                          merge_key: str, update_condition: str = None) -> None:
        """Perform MERGE INTO operation for efficient updates/upserts"""
        
        database = self.iceberg_config.get('glue_catalog_database', 'default')
        full_table_name = f"{self.catalog_name}.{database}.{target_table}"
        
        # Create temporary view for source data
        source_view = f"source_{target_table}_{int(time.time())}"
        source_df.createOrReplaceTempView(source_view)
        
        # Build MERGE statement
        update_condition = update_condition or "TRUE"
        
        merge_sql = f"""
        MERGE INTO {full_table_name} AS target
        USING {source_view} AS source
        ON target.{merge_key} = source.{merge_key}
        WHEN MATCHED AND {update_condition} THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        logger.info(f"Executing MERGE INTO: {merge_sql}")
        self.spark.sql(merge_sql)
        logger.info(f"Successfully merged data into: {full_table_name}")
    
    def _build_schema_sql(self, schema) -> str:
        """Build schema definition for CREATE TABLE"""
        columns = []
        for field in schema.fields:
            nullable = "NULL" if field.nullable else "NOT NULL"
            columns.append(f"{field.name} {field.dataType.simpleString()} {nullable}")
        return ",\n    ".join(columns)
    
    def _build_partition_sql(self, partition_spec: List[Dict]) -> str:
        """Build PARTITIONED BY clause"""
        if not partition_spec:
            return ""
        
        partitions = []
        for spec in partition_spec:
            if spec['transform'] == 'identity':
                partitions.append(spec['source_column'])
            else:
                partitions.append(f"{spec['transform']}({spec['source_column']})")
        
        return f"PARTITIONED BY ({', '.join(partitions)})"
    
    def _build_sort_sql(self, sort_order: List[Dict]) -> str:
        """Build sort order clause"""
        if not sort_order:
            return ""
        
        sorts = []
        for sort_spec in sort_order:
            direction = sort_spec.get('direction', 'asc').upper()
            null_order = sort_spec.get('null_order', 'last').upper()
            sorts.append(f"{sort_spec['column']} {direction} NULLS {null_order}")
        
        return f"TBLPROPERTIES ('write.distribution-mode'='hash', 'write.sort-order'='{','.join(sorts)}')"
    
    def _build_properties_sql(self, properties: Dict[str, str]) -> str:
        """Build table properties"""
        if not properties:
            return ""
        
        props = [f"'{k}'='{v}'" for k, v in properties.items()]
        return f"TBLPROPERTIES ({', '.join(props)})"

def main():
    """Main processing function with Iceberg support"""
    try:
        args = parse_arguments()
        
        logger.info(f"Starting Iceberg processing for table: {args.target_table}")
        
        # Create Spark session with Iceberg support
        spark = IcebergDataProcessor.create_spark_session_with_iceberg(
            args.job_id, args.target_table, args.glue_catalog_database
        )
        
        try:
            # Initialize Iceberg processor
            iceberg_config = {
                'glue_catalog_database': args.glue_catalog_database,
                'warehouse_path': args.warehouse_path
            }
            processor = IcebergDataProcessor(spark, iceberg_config)
            
            # Read source data
            df = spark.read.format(args.file_format).load(args.input_path)
            
            # Apply transformations
            transformed_df = apply_transformations(df, args.target_table, args.job_id)
            
            # Create Iceberg table if needed
            processor.create_iceberg_table_if_not_exists(
                table_name=args.target_table,
                df_schema=transformed_df,
                partition_spec=parse_partition_spec(args.partition_spec),
                sort_order=parse_sort_order(args.sort_order),
                table_properties=parse_table_properties(args.table_properties)
            )
            
            # Write to Iceberg table
            if args.write_mode == "merge":
                processor.merge_into_iceberg(
                    transformed_df, args.target_table, args.merge_key
                )
            else:
                processor.write_to_iceberg(
                    transformed_df, args.target_table, args.write_mode
                )
            
            logger.info("Iceberg processing completed successfully")
            
        finally:
            spark.stop()
        
    except Exception as e:
        logger.error(f"Iceberg processing failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

### Phase 4: Snowflake Integration

#### 4.1 Snowflake Iceberg Tables
```sql
-- Create Iceberg table in Snowflake pointing to S3/Glue
CREATE ICEBERG TABLE customers
  CATALOG='iceberg_glue_catalog'
  EXTERNAL_VOLUME='iceberg_s3_volume'
  BASE_LOCATION='customers/';

-- Query Iceberg table with time travel
SELECT * FROM customers 
AT(TIMESTAMP => '2024-01-01 12:00:00'::timestamp);

-- Query table snapshots
SELECT * FROM TABLE(INFORMATION_SCHEMA.ICEBERG_TABLE_HISTORY('customers'));
```

#### 4.2 Cross-Engine Analytics
```sql
-- Snowflake: Business analytics on Iceberg data
SELECT 
    customer_region,
    COUNT(*) as customer_count,
    AVG(lifetime_value) as avg_ltv
FROM customers
WHERE processing_day >= '2024-01-01'
GROUP BY customer_region;

-- Spark: Real-time stream processing to same Iceberg table
spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "customer-events")
    .load()
    .writeStream
    .format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3://checkpoints/customers/")
    .toTable("glue_catalog.flake_runner_iceberg.customers")
```

### Phase 5: Advanced Features

#### 5.1 Schema Evolution
```python
def evolve_iceberg_schema(spark: SparkSession, table_name: str, 
                         new_columns: List[Dict]) -> None:
    """Add new columns to existing Iceberg table"""
    
    for column in new_columns:
        alter_sql = f"""
        ALTER TABLE glue_catalog.flake_runner_iceberg.{table_name}
        ADD COLUMN {column['name']} {column['type']} 
        COMMENT '{column.get('comment', '')}'
        """
        spark.sql(alter_sql)
        logger.info(f"Added column {column['name']} to {table_name}")
```

#### 5.2 Data Compaction Strategy
```python
def compact_iceberg_table(spark: SparkSession, table_name: str,
                         file_size_threshold: int = 134217728) -> None:
    """Compact small files in Iceberg table"""
    
    full_table_name = f"glue_catalog.flake_runner_iceberg.{table_name}"
    
    # Rewrite data files that are smaller than threshold
    compact_sql = f"""
    CALL glue_catalog.system.rewrite_data_files(
        table => '{full_table_name}',
        options => map(
            'target-file-size-bytes', '{file_size_threshold}',
            'min-input-files', '2'
        )
    )
    """
    spark.sql(compact_sql)
```

#### 5.3 Maintenance Operations
```python
def maintain_iceberg_table(spark: SparkSession, table_name: str,
                          retention_days: int = 7) -> None:
    """Perform routine maintenance on Iceberg table"""
    
    full_table_name = f"glue_catalog.flake_runner_iceberg.{table_name}"
    
    # Remove old snapshots
    expire_sql = f"""
    CALL glue_catalog.system.expire_snapshots(
        table => '{full_table_name}',
        older_than => TIMESTAMP '{retention_days} days ago'
    )
    """
    spark.sql(expire_sql)
    
    # Remove orphaned files
    remove_orphans_sql = f"""
    CALL glue_catalog.system.remove_orphan_files(
        table => '{full_table_name}'
    )
    """
    spark.sql(remove_orphans_sql)
```

## Migration Strategy

### Phase 1: Pilot Implementation
1. **Setup Infrastructure**
   - Configure AWS Glue Catalog
   - Create Snowflake Iceberg integration
   - Update EMR Serverless application

2. **Implement Single Table**
   - Start with `customers` table
   - Test end-to-end processing
   - Validate Snowflake connectivity

### Phase 2: Incremental Migration
1. **Migrate Remaining Tables**
   - `orders` → Iceberg format
   - `analytics_events` → Iceberg format
   - Maintain parallel processing during transition

2. **Performance Optimization**
   - Tune partition strategies
   - Optimize file sizes
   - Implement compaction schedules

### Phase 3: Advanced Features
1. **Real-time Processing**
   - Streaming writes to Iceberg
   - Change data capture (CDC)
   - Near real-time analytics

2. **Cross-Engine Queries**
   - Snowflake business intelligence
   - Spark machine learning
   - Unified data governance

## Performance Benefits

### Query Performance
- **Partition Pruning**: 50-90% reduction in data scanned
- **Predicate Pushdown**: 30-70% faster query execution
- **Column Statistics**: Better query planning and optimization

### Storage Efficiency
- **File Compaction**: 20-40% storage savings
- **Snapshot Management**: Automated cleanup of old versions
- **Compression**: Optimized storage with Parquet + Snappy/ZSTD

### Operational Benefits
- **ACID Transactions**: Consistent data even with concurrent operations
- **Schema Evolution**: Add/modify columns without downtime
- **Time Travel**: Query historical data states
- **Rollback Capability**: Revert to previous table versions

## Monitoring and Observability

### Key Metrics
```sql
-- Table size and file count
SELECT 
    table_name,
    total_size_bytes,
    file_count,
    snapshot_count
FROM glue_catalog.information_schema.tables
WHERE table_schema = 'flake_runner_iceberg';

-- Query performance metrics
SELECT 
    query_id,
    table_name,
    files_scanned,
    bytes_scanned,
    execution_time_ms
FROM query_history
WHERE table_format = 'ICEBERG';
```

### Maintenance Alerts
- Monitor file sizes for compaction needs
- Track snapshot retention policies
- Alert on schema evolution events
- Monitor cross-engine query performance

## Cost Optimization

### Storage Costs
- **Partition Pruning**: Reduce data scanning costs
- **File Compaction**: Optimize storage efficiency
- **Lifecycle Policies**: Automated archival of old snapshots

### Compute Costs
- **Query Performance**: Faster queries = lower compute costs
- **Incremental Processing**: Process only changed data
- **Concurrent Access**: Multiple engines without data duplication

### Operational Costs
- **Reduced ETL Complexity**: Unified format reduces pipeline complexity
- **Self-Service Analytics**: Analysts can query data lake directly
- **Automated Maintenance**: Reduced manual intervention

---

## Next Steps

1. **Infrastructure Setup**: Configure Glue Catalog and Snowflake integration
2. **Pilot Implementation**: Migrate one table to validate approach
3. **Performance Testing**: Benchmark query performance improvements
4. **Full Migration**: Gradually migrate all tables to Iceberg format
5. **Advanced Features**: Implement streaming, CDC, and cross-engine analytics

This Iceberg integration will modernize your data architecture, providing ACID transactions, better performance, and unified analytics across multiple compute engines while maintaining compatibility with existing Snowflake workflows.