#!/usr/bin/env python3
"""
Simple PySpark data processor for local testing
Reads from local file system and writes to local file system
Suitable for development and integration testing
"""

import argparse
import sys
import logging
import os
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleProcessor:
    """Simple data processor for local file system testing"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def read_data(self, input_path: str, file_format: str = "CSV") -> DataFrame:
        """Read data from local file system"""
        logger.info(f"Reading {file_format} data from: {input_path}")
        
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        reader = self.spark.read
        
        # Read based on format
        if file_format.upper() == "CSV":
            df = reader.option("header", "true").option("inferSchema", "true").csv(input_path)
        elif file_format.upper() == "JSON":
            df = reader.json(input_path)
        elif file_format.upper() == "PARQUET":
            df = reader.parquet(input_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        record_count = df.count()
        logger.info(f"Successfully read {record_count} records")
        return df
    
    def transform_data(self, df: DataFrame, job_id: str) -> DataFrame:
        """Apply basic transformations"""
        logger.info("Applying basic transformations")
        
        # Add processing metadata
        df = df.withColumn("_processing_timestamp", current_timestamp()) \
               .withColumn("_job_id", lit(job_id)) \
               .withColumn("_source_system", lit("flake-runner-local"))
        
        # Basic data quality - remove rows where all original columns are null
        original_columns = [col_name for col_name in df.columns if not col_name.startswith("_")]
        if original_columns:
            # Keep rows that have at least one non-null value in original columns
            condition = None
            for col_name in original_columns:
                if condition is None:
                    condition = col(col_name).isNotNull()
                else:
                    condition = condition | col(col_name).isNotNull()
            
            if condition is not None:
                df = df.filter(condition)
        
        return df
    
    def write_data(self, df: DataFrame, output_path: str, file_format: str = "PARQUET") -> None:
        """Write processed data to local file system"""
        logger.info(f"Writing {file_format} data to: {output_path}")
        
        # Create a data subdirectory to avoid volume mount issues
        actual_output_path = os.path.join(output_path, "data")
        
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)
        
        # Clear the data subdirectory if it exists
        if os.path.exists(actual_output_path):
            import shutil
            try:
                shutil.rmtree(actual_output_path)
                logger.info(f"Cleared existing data directory: {actual_output_path}")
            except Exception as e:
                logger.warning(f"Could not clear data directory: {e}")
        
        writer = df.write.mode("overwrite")
        
        if file_format.upper() == "PARQUET":
            writer.parquet(actual_output_path)
        elif file_format.upper() == "JSON":
            writer.json(actual_output_path)
        elif file_format.upper() == "CSV":
            writer.option("header", "true").csv(actual_output_path)
        else:
            raise ValueError(f"Unsupported output format: {file_format}")
        
        logger.info(f"Data written successfully to: {actual_output_path}")
        
        # Also create a _SUCCESS marker in the main output directory
        success_file = os.path.join(output_path, "_SUCCESS")
        with open(success_file, 'w') as f:
            f.write("Processing completed successfully")
        logger.info(f"Created success marker: {success_file}")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Simple PySpark Data Processor for Local Testing")
    
    parser.add_argument("--input-path", required=True,
                       help="Local path to input data file")
    parser.add_argument("--output-path", required=True,
                       help="Local path for processed output")
    parser.add_argument("--input-format", default="CSV",
                       help="Input file format (CSV, JSON, PARQUET)")
    parser.add_argument("--output-format", default="PARQUET",
                       help="Output file format (PARQUET, JSON, CSV)")
    parser.add_argument("--job-id", default="local-test",
                       help="Job identifier for processing metadata")
    
    return parser.parse_args()

def create_local_spark_session(job_id: str) -> SparkSession:
    """Create Spark session optimized for local testing"""
    app_name = f"SimpleProcessor-{job_id}"
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    # Set log level to reduce noise in local testing
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def main():
    """Main processing function"""
    try:
        # Parse arguments
        args = parse_arguments()
        
        logger.info(f"Starting simple data processing")
        logger.info(f"Job ID: {args.job_id}")
        logger.info(f"Input: {args.input_path}")
        logger.info(f"Output: {args.output_path}")
        logger.info(f"Input Format: {args.input_format}")
        logger.info(f"Output Format: {args.output_format}")
        
        # Create Spark session
        spark = create_local_spark_session(args.job_id)
        
        try:
            # Initialize processor
            processor = SimpleProcessor(spark)
            
            # Read data
            df = processor.read_data(
                input_path=args.input_path,
                file_format=args.input_format
            )
            
            # Transform data
            transformed_df = processor.transform_data(
                df=df,
                job_id=args.job_id
            )
            
            # Write processed data
            processor.write_data(
                df=transformed_df,
                output_path=args.output_path,
                file_format=args.output_format
            )
            
            # Log success metrics
            record_count = transformed_df.count()
            logger.info(f"Processing completed successfully")
            logger.info(f"Records processed: {record_count}")
            
        finally:
            spark.stop()
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()