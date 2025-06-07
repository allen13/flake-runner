#!/usr/bin/env python3
"""
CSV to Parquet Converter for EMR Serverless
Optimized for EMR 7.8.0 with Spark 3.5.4, Python 3.11, Java 17
Demonstrates FlakeRunner framework capabilities with full S3 integration
"""

import argparse
import sys
import logging
import os
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, when, regexp_replace, coalesce
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CSVToParquetConverter:
    """High-performance CSV to Parquet converter with advanced features"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.processing_stats = {
            'input_records': 0,
            'output_records': 0,
            'processing_time_seconds': 0,
            'data_quality_issues': 0
        }
        
    def read_csv(self, input_path: str, **options) -> DataFrame:
        """Read CSV data from S3 with configurable options"""
        logger.info(f"Reading CSV data from: {input_path}")
        
        # Default CSV reading options optimized for EMR
        default_options = {
            "header": "true",
            "inferSchema": "true",
            "multiline": "true",
            "escape": '"',
            "timestampFormat": "yyyy-MM-dd HH:mm:ss",
            "dateFormat": "yyyy-MM-dd"
        }
        
        # Merge with user-provided options
        csv_options = {**default_options, **options}
        
        logger.info(f"CSV reading options: {csv_options}")
        
        reader = self.spark.read
        for key, value in csv_options.items():
            reader = reader.option(key, value)
        
        try:
            df = reader.csv(input_path)
            record_count = df.count()
            self.processing_stats['input_records'] = record_count
            logger.info(f"Successfully read {record_count} records from CSV")
            
            # Log schema information
            logger.info("Input schema:")
            for field in df.schema.fields:
                logger.info(f"  {field.name}: {field.dataType}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to read CSV from {input_path}: {str(e)}")
            raise
    
    def apply_transformations(self, df: DataFrame, job_id: str, target_name: str, 
                            script_params: Dict[str, str]) -> DataFrame:
        """Apply configurable transformations based on script parameters"""
        logger.info(f"Applying transformations for target: {target_name}")
        
        # Add framework metadata columns
        df = df.withColumn("_processing_timestamp", current_timestamp()) \
               .withColumn("_job_id", lit(job_id)) \
               .withColumn("_target_name", lit(target_name)) \
               .withColumn("_framework_version", lit("flake-runner-v2.0")) \
               .withColumn("_emr_version", lit("7.8.0")) \
               .withColumn("_spark_version", lit("3.5.4"))
        
        # Apply transformations based on script parameters
        if script_params.get("add-row-id", "false").lower() == "true":
            df = df.withColumn("_row_id", monotonically_increasing_id())
            logger.info("Added row ID column")
        
        if script_params.get("clean-strings", "false").lower() == "true":
            df = self._clean_string_columns(df)
        
        if script_params.get("validate-data", "true").lower() == "true":
            df = self._apply_data_quality_checks(df)
        
        # Apply custom transformations based on target name
        if "customer" in target_name.lower():
            df = self._transform_customer_data(df)
        elif "order" in target_name.lower():
            df = self._transform_order_data(df)
        elif "product" in target_name.lower():
            df = self._transform_product_data(df)
        
        return df
    
    def _clean_string_columns(self, df: DataFrame) -> DataFrame:
        """Clean string columns by removing extra whitespace and special characters"""
        logger.info("Cleaning string columns")
        
        for field in df.schema.fields:
            if field.dataType == StringType():
                col_name = field.name
                if not col_name.startswith("_"):  # Skip metadata columns
                    df = df.withColumn(
                        col_name,
                        regexp_replace(
                            regexp_replace(col(col_name), r'^\s+|\s+$', ''),  # Trim
                            r'\s+', ' '  # Normalize whitespace
                        )
                    )
        
        return df
    
    def _apply_data_quality_checks(self, df: DataFrame) -> DataFrame:
        """Apply comprehensive data quality checks and fixes"""
        logger.info("Applying data quality checks")
        
        initial_count = df.count()
        
        # Remove completely empty rows
        non_metadata_cols = [col_name for col_name in df.columns if not col_name.startswith("_")]
        if non_metadata_cols:
            # Create condition to check if at least one non-metadata column has a value
            condition = None
            for col_name in non_metadata_cols:
                col_condition = col(col_name).isNotNull() & (col(col_name) != "")
                if condition is None:
                    condition = col_condition
                else:
                    condition = condition | col_condition
            
            df = df.filter(condition)
        
        final_count = df.count()
        issues_found = initial_count - final_count
        self.processing_stats['data_quality_issues'] = issues_found
        
        if issues_found > 0:
            logger.warning(f"Removed {issues_found} empty records during data quality checks")
        
        return df
    
    def _transform_customer_data(self, df: DataFrame) -> DataFrame:
        """Customer-specific transformations"""
        logger.info("Applying customer-specific transformations")
        
        # Standardize email addresses
        if "email" in df.columns:
            df = df.withColumn("email", lower(col("email")))
        
        # Standardize phone numbers (remove non-numeric characters)
        if "phone" in df.columns:
            df = df.withColumn("phone", regexp_replace(col("phone"), r'[^\d]', ''))
        
        return df
    
    def _transform_order_data(self, df: DataFrame) -> DataFrame:
        """Order-specific transformations"""
        logger.info("Applying order-specific transformations")
        
        # Ensure amount is properly formatted
        if "amount" in df.columns:
            df = df.withColumn("amount", 
                              when(col("amount").isNull(), 0.0).otherwise(col("amount")))
        
        return df
    
    def _transform_product_data(self, df: DataFrame) -> DataFrame:
        """Product-specific transformations"""
        logger.info("Applying product-specific transformations")
        
        # Standardize product names
        if "product_name" in df.columns:
            df = df.withColumn("product_name", initcap(col("product_name")))
        
        return df
    
    def write_parquet(self, df: DataFrame, output_path: str, 
                     partition_columns: Optional[list] = None,
                     compression: str = "snappy",
                     script_params: Dict[str, str] = None) -> None:
        """Write DataFrame to Parquet with optimizations"""
        if script_params is None:
            script_params = {}
            
        logger.info(f"Writing Parquet data to: {output_path}")
        logger.info(f"Compression: {compression}")
        
        # Configure writer
        writer = df.write.mode("overwrite")
        
        # Set compression
        writer = writer.option("compression", compression)
        
        # Apply partitioning if specified
        partition_by = script_params.get("partition-by")
        if partition_by:
            partition_cols = [col.strip() for col in partition_by.split(",")]
            logger.info(f"Partitioning by columns: {partition_cols}")
            writer = writer.partitionBy(*partition_cols)
        
        # Configure additional Parquet options
        max_records_per_file = script_params.get("max-records-per-file")
        if max_records_per_file:
            writer = writer.option("maxRecordsPerFile", max_records_per_file)
        
        try:
            # Write the data
            writer.parquet(output_path)
            
            # Update stats
            self.processing_stats['output_records'] = df.count()
            
            logger.info(f"Successfully wrote Parquet data to: {output_path}")
            logger.info(f"Output records: {self.processing_stats['output_records']}")
            
        except Exception as e:
            logger.error(f"Failed to write Parquet to {output_path}: {str(e)}")
            raise
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Return processing statistics"""
        return self.processing_stats.copy()

def parse_arguments():
    """Parse command line arguments with extensive options"""
    parser = argparse.ArgumentParser(
        description="CSV to Parquet Converter for EMR Serverless",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic conversion
  python csv_to_parquet_converter.py --input-path s3://bucket/data.csv --output-path s3://bucket/output/

  # With partitioning and compression
  python csv_to_parquet_converter.py --input-path s3://bucket/data.csv --output-path s3://bucket/output/ \\
    --partition-by region,year --compression gzip

  # With data quality and string cleaning
  python csv_to_parquet_converter.py --input-path s3://bucket/data.csv --output-path s3://bucket/output/ \\
    --clean-strings true --validate-data true --add-row-id true
        """
    )
    
    # Required arguments
    parser.add_argument("--input-path", required=True,
                       help="S3 path to input CSV file(s)")
    parser.add_argument("--output-path", required=True,
                       help="S3 path for output Parquet files")
    
    # Optional processing arguments
    parser.add_argument("--target-name", default="CONVERTED_DATA",
                       help="Target table/dataset name (default: CONVERTED_DATA)")
    parser.add_argument("--job-id", default="csv-to-parquet",
                       help="Job identifier for tracking")
    
    # CSV reading options
    parser.add_argument("--csv-delimiter", default=",",
                       help="CSV delimiter character (default: comma)")
    parser.add_argument("--csv-quote", default='"',
                       help="CSV quote character (default: double quote)")
    parser.add_argument("--csv-escape", default='"',
                       help="CSV escape character (default: double quote)")
    parser.add_argument("--csv-header", default="true", choices=["true", "false"],
                       help="Whether CSV has header row (default: true)")
    
    # Parquet writing options
    parser.add_argument("--compression", default="snappy",
                       choices=["none", "snappy", "gzip", "lzo", "brotli", "lz4", "zstd"],
                       help="Parquet compression codec (default: snappy)")
    parser.add_argument("--partition-by", 
                       help="Comma-separated list of columns to partition by")
    parser.add_argument("--max-records-per-file",
                       help="Maximum records per output file")
    
    # Processing options
    parser.add_argument("--clean-strings", default="false", choices=["true", "false"],
                       help="Clean and normalize string columns (default: false)")
    parser.add_argument("--validate-data", default="true", choices=["true", "false"],
                       help="Apply data quality validation (default: true)")
    parser.add_argument("--add-row-id", default="false", choices=["true", "false"],
                       help="Add unique row ID column (default: false)")
    
    # Performance options
    parser.add_argument("--executor-memory", default="4g",
                       help="Spark executor memory (default: 4g)")
    parser.add_argument("--executor-cores", default="2", type=int,
                       help="Spark executor cores (default: 2)")
    
    return parser.parse_args()

def create_spark_session(job_id: str, executor_memory: str, executor_cores: int) -> SparkSession:
    """Create optimized Spark session for EMR Serverless"""
    app_name = f"CSVToParquetConverter-{job_id}"
    
    # Get environment variables
    log_level = os.getenv("LOG_LEVEL", "INFO")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.optimizeSkewedJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.executor.cores", str(executor_cores)) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()
    
    # Set log level based on environment variable
    spark.sparkContext.setLogLevel(log_level)
    
    logger.info(f"Created Spark session: {app_name}")
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Log level: {log_level}")
    
    return spark

def main():
    """Main processing function"""
    import time
    start_time = time.time()
    
    try:
        # Parse arguments
        args = parse_arguments()
        
        logger.info("=" * 60)
        logger.info("CSV to Parquet Converter - EMR Serverless")
        logger.info("=" * 60)
        logger.info(f"Job ID: {args.job_id}")
        logger.info(f"Input: {args.input_path}")
        logger.info(f"Output: {args.output_path}")
        logger.info(f"Target: {args.target_name}")
        logger.info(f"Compression: {args.compression}")
        
        # Create Spark session
        spark = create_spark_session(
            job_id=args.job_id,
            executor_memory=args.executor_memory,
            executor_cores=args.executor_cores
        )
        
        try:
            # Initialize converter
            converter = CSVToParquetConverter(spark)
            
            # Prepare CSV reading options
            csv_options = {
                "delimiter": args.csv_delimiter,
                "quote": args.csv_quote,
                "escape": args.csv_escape,
                "header": args.csv_header
            }
            
            # Prepare script parameters
            script_params = {
                "partition-by": args.partition_by,
                "max-records-per-file": args.max_records_per_file,
                "clean-strings": args.clean_strings,
                "validate-data": args.validate_data,
                "add-row-id": args.add_row_id
            }
            
            # Read CSV data
            logger.info("Step 1: Reading CSV data...")
            df = converter.read_csv(args.input_path, **csv_options)
            
            # Apply transformations
            logger.info("Step 2: Applying transformations...")
            transformed_df = converter.apply_transformations(
                df=df,
                job_id=args.job_id,
                target_name=args.target_name,
                script_params=script_params
            )
            
            # Write Parquet data
            logger.info("Step 3: Writing Parquet data...")
            converter.write_parquet(
                df=transformed_df,
                output_path=args.output_path,
                compression=args.compression,
                script_params=script_params
            )
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Get final statistics
            stats = converter.get_processing_stats()
            stats['processing_time_seconds'] = round(processing_time, 2)
            
            # Log success metrics
            logger.info("=" * 60)
            logger.info("PROCESSING COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            logger.info(f"Input records: {stats['input_records']:,}")
            logger.info(f"Output records: {stats['output_records']:,}")
            logger.info(f"Data quality issues fixed: {stats['data_quality_issues']:,}")
            logger.info(f"Processing time: {stats['processing_time_seconds']} seconds")
            logger.info(f"Records per second: {stats['output_records'] / max(stats['processing_time_seconds'], 0.1):,.0f}")
            
            # Log environment variables for debugging
            logger.info("Environment variables:")
            for key, value in os.environ.items():
                if key.startswith(('SPARK_', 'HADOOP_', 'AWS_', 'LOG_')):
                    logger.info(f"  {key}: {value}")
            
        finally:
            spark.stop()
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        logger.error("Full traceback:", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()