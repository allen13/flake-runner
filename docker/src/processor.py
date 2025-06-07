#!/usr/bin/env python3
"""
Universal PySpark data processor for EMR Serverless
Optimized for EMR 7.8.0 with Spark 3.5.4, Python 3.11, Java 17
Handles multiple file formats and processes data for Snowflake loading
"""

import argparse
import sys
import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, when, regexp_replace, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    """Universal data processor for different file formats and tables"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def read_data(self, input_path: str, file_format: str, 
                  compression: Optional[str] = None, 
                  options: Optional[Dict[str, str]] = None) -> DataFrame:
        """Read data from S3 based on file format"""
        logger.info(f"Reading {file_format} data from: {input_path}")
        
        reader = self.spark.read
        
        # Apply options if provided
        if options:
            for key, value in options.items():
                reader = reader.option(key, value)
        
        # Apply compression if specified
        if compression:
            reader = reader.option("compression", compression)
        
        # Read based on format
        if file_format.upper() == "CSV":
            df = reader.option("header", "true").option("inferSchema", "true").csv(input_path)
        elif file_format.upper() == "JSON":
            df = reader.json(input_path)
        elif file_format.upper() == "PARQUET":
            df = reader.parquet(input_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        logger.info(f"Successfully read {df.count()} records")
        return df
    
    def transform_data(self, df: DataFrame, target_table: str, 
                      job_id: str) -> DataFrame:
        """Apply transformations based on target table with Spark 3.5.4 optimizations"""
        logger.info(f"Applying transformations for table: {target_table}")
        
        # Add audit columns with improved timestamp handling
        df = df.withColumn("_processing_timestamp", current_timestamp()) \
               .withColumn("_job_id", lit(job_id)) \
               .withColumn("_source_system", lit("flake-runner")) \
               .withColumn("_emr_version", lit("7.8.0")) \
               .withColumn("_spark_version", lit("3.5.4"))
        
        # Apply table-specific transformations
        if "CUSTOMER" in target_table.upper():
            df = self._transform_customer_data(df)
        elif "ORDER" in target_table.upper():
            df = self._transform_order_data(df)
        elif "PRODUCT" in target_table.upper():
            df = self._transform_product_data(df)
        elif "ANALYTICS" in target_table.upper():
            df = self._transform_analytics_data(df)
        
        # Apply universal data quality checks
        df = self._apply_data_quality_checks(df)
        
        return df
    
    def _transform_customer_data(self, df: DataFrame) -> DataFrame:
        """Customer-specific transformations"""
        logger.info("Applying customer data transformations")
        
        # Standardize column names
        column_mapping = {
            "customer_id": "CUSTOMER_ID",
            "name": "CUSTOMER_NAME", 
            "email": "EMAIL_ADDRESS",
            "phone": "PHONE_NUMBER"
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)
        
        # Data quality checks
        df = df.filter(col("CUSTOMER_ID").isNotNull())
        df = df.filter(col("EMAIL_ADDRESS").isNotNull())
        
        return df
    
    def _transform_order_data(self, df: DataFrame) -> DataFrame:
        """Order-specific transformations"""
        logger.info("Applying order data transformations")
        
        # Standardize column names
        column_mapping = {
            "order_id": "ORDER_ID",
            "customer_id": "CUSTOMER_ID",
            "order_date": "ORDER_DATE",
            "amount": "ORDER_AMOUNT"
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)
        
        # Data quality checks
        df = df.filter(col("ORDER_ID").isNotNull())
        df = df.filter(col("ORDER_AMOUNT") > 0)
        
        return df
    
    def _transform_product_data(self, df: DataFrame) -> DataFrame:
        """Product-specific transformations"""
        logger.info("Applying product data transformations")
        
        # Standardize column names
        column_mapping = {
            "product_id": "PRODUCT_ID",
            "name": "PRODUCT_NAME",
            "category": "CATEGORY",
            "price": "PRICE"
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)
        
        # Data quality checks
        df = df.filter(col("PRODUCT_ID").isNotNull())
        
        return df
    
    def _transform_analytics_data(self, df: DataFrame) -> DataFrame:
        """Analytics-specific transformations for advanced workloads"""
        logger.info("Applying analytics data transformations")
        
        # Standardize column names for analytics
        column_mapping = {
            "event_id": "EVENT_ID",
            "user_id": "USER_ID",
            "event_type": "EVENT_TYPE",
            "timestamp": "EVENT_TIMESTAMP",
            "properties": "EVENT_PROPERTIES"
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)
        
        # Data quality checks for analytics
        df = df.filter(col("EVENT_ID").isNotNull())
        df = df.filter(col("EVENT_TYPE").isNotNull())
        
        return df
    
    def _apply_data_quality_checks(self, df: DataFrame) -> DataFrame:
        """Apply universal data quality checks with Spark 3.5.4 optimizations"""
        logger.info("Applying universal data quality checks")
        
        # Remove completely null rows
        non_null_columns = [col_name for col_name in df.columns if not col_name.startswith("_")]
        if non_null_columns:
            # Use better null handling in Spark 3.5.4
            df = df.filter(
                spark_sum([when(col(c).isNotNull(), 1).otherwise(0) for c in non_null_columns]) > 0
            )
        
        # Clean string columns - remove extra whitespace and control characters
        string_columns = [field.name for field in df.schema.fields if field.dataType == StringType()]
        for col_name in string_columns:
            if not col_name.startswith("_"):
                df = df.withColumn(col_name, 
                    regexp_replace(col(col_name), r"\\s+", " ")  # Replace multiple spaces with single space
                )
        
        return df
    
    def write_data(self, df: DataFrame, output_path: str, 
                   file_format: str = "PARQUET", 
                   compression: str = "SNAPPY") -> None:
        """Write processed data to S3 with Spark 3.5.4 optimizations"""
        logger.info(f"Writing {file_format} data to: {output_path}")
        
        # Optimize writer for Spark 3.5.4
        writer = df.write.mode("overwrite")
        
        # Use adaptive partitioning for better performance
        if df.count() > 100000:  # For large datasets
            writer = writer.option("maxRecordsPerFile", "50000")
        
        if file_format.upper() == "PARQUET":
            writer.option("compression", compression) \
                  .option("parquet.enable.dictionary", "true") \
                  .option("parquet.page.write-checksum.enabled", "true") \
                  .parquet(output_path)
        elif file_format.upper() == "JSON":
            writer.option("compression", compression) \
                  .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") \
                  .json(output_path)
        elif file_format.upper() == "CSV":
            writer.option("header", "true") \
                  .option("compression", compression) \
                  .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                  .csv(output_path)
        else:
            raise ValueError(f"Unsupported output format: {file_format}")
        
        logger.info("Data written successfully with Spark 3.5.4 optimizations")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Universal PySpark Data Processor")
    
    parser.add_argument("--input-path", required=True,
                       help="S3 path to input data")
    parser.add_argument("--output-path", required=True,
                       help="S3 path for processed output")
    parser.add_argument("--staging-path", required=True,
                       help="S3 path for staging")
    parser.add_argument("--target-table", required=True,
                       help="Target Snowflake table name")
    parser.add_argument("--file-format", default="CSV",
                       help="Input file format (CSV, JSON, PARQUET)")
    parser.add_argument("--output-format", default="PARQUET",
                       help="Output file format (PARQUET, JSON, CSV)")
    parser.add_argument("--compression", default="SNAPPY",
                       help="Compression type")
    parser.add_argument("--job-id", required=True,
                       help="Unique job identifier")
    
    return parser.parse_args()

def create_spark_session(job_id: str, target_table: str) -> SparkSession:
    """Create optimized Spark session for Spark 3.5.4"""
    app_name = f"FlakeRunner-{target_table}-{job_id}"
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.optimizeSkewedJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true") \
        .config("spark.sql.optimizer.runtime.bloomFilter.enabled", "true") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.sql.sources.bucketing.enabled", "true") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("INFO")
    
    return spark

def main():
    """Main processing function"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        logger.info(f"Starting data processing for table: {args.target_table}")
        logger.info(f"Job ID: {args.job_id}")
        logger.info(f"Input: {args.input_path}")
        logger.info(f"Output: {args.output_path}")
        
        # Create Spark session
        spark = create_spark_session(args.job_id, args.target_table)
        
        try:
            # Initialize processor
            processor = DataProcessor(spark)
            
            # Read data
            df = processor.read_data(
                input_path=args.input_path,
                file_format=args.file_format,
                compression=args.compression if args.compression != "NONE" else None
            )
            
            # Transform data
            transformed_df = processor.transform_data(
                df=df,
                target_table=args.target_table,
                job_id=args.job_id
            )
            
            # Write processed data
            processor.write_data(
                df=transformed_df,
                output_path=args.output_path,
                file_format=args.output_format,
                compression=args.compression
            )
            
            # Log success metrics
            record_count = transformed_df.count()
            logger.info(f"Processing completed successfully")
            logger.info(f"Records processed: {record_count}")
            logger.info(f"Target table: {args.target_table}")
            
        finally:
            spark.stop()
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()