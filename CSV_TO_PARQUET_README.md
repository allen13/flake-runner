# CSV to Parquet Converter for FlakeRunner

This document demonstrates how to use the new CSV to Parquet conversion script integrated into the FlakeRunner framework.

## Overview

The `csv_to_parquet_converter.py` script provides high-performance CSV to Parquet conversion with advanced features:

- ✅ **S3 Integration**: Direct reading from S3 and writing to S3
- ✅ **Configurable Compression**: Snappy, Gzip, LZ4, Zstd, Brotli
- ✅ **Partitioning Support**: Automatic partitioning by specified columns
- ✅ **Data Quality Checks**: Automatic data cleaning and validation
- ✅ **Metadata Enhancement**: Adds processing timestamps and job tracking
- ✅ **Environment Variables**: Configurable via FlakeRunner framework
- ✅ **Script Parameters**: Flexible parameter passing from configuration

## Quick Start

### 1. Build and Deploy Container

```bash
# Build the container with the new CSV converter
cd docker
./build.sh

# The container now includes:
# - /opt/spark/jobs/csv_to_parquet_converter.py (NEW)
# - /opt/spark/jobs/processor.py
# - /opt/spark/jobs/simple_processor.py
```

### 2. Configuration Example

```json
{
  "prefix_mappings": [
    {
      "s3_prefix": "products/",
      "target_name": "PRODUCTS_PARQUET",
      "container_image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/flake-runner:v3.1.0-with-pyspark",
      "entry_point": "/opt/spark/jobs/csv_to_parquet_converter.py",
      "script_params": {
        "compression": "snappy",
        "partition-by": "category,year",
        "clean-strings": "true",
        "validate-data": "true",
        "add-row-id": "true",
        "max-records-per-file": "50000"
      },
      "environment_vars": {
        "LOG_LEVEL": "INFO",
        "CUSTOMER_VALIDATION_MODE": "strict"
      },
      "processing_config": {
        "file_format": "CSV",
        "output_format": "PARQUET",
        "spark_config": {
          "spark.executor.memory": "4g",
          "spark.executor.cores": "2"
        }
      },
      "validation_rules": {
        "validate_record_count": true,
        "validate_file_size": true,
        "required_fields": ["product_id", "product_name"]
      }
    }
  ]
}
```

### 3. Process CSV Files

```bash
# Process a CSV file with the framework
./flake-runner process \
  --config csv-to-parquet-demo-config.json \
  --file s3://my-bucket/products/data.csv \
  --wait \
  --timeout 30

# With control data for validation
./flake-runner process \
  --config csv-to-parquet-demo-config.json \
  --file s3://my-bucket/products/data.csv \
  --control-data '{"file_name":"data.csv","record_count":1000,"file_size":52428}' \
  --wait
```

## Script Parameters

The CSV to Parquet converter supports these script parameters (via `script_params` in configuration):

### Core Parameters
- `compression`: Compression codec (snappy, gzip, lz4, zstd, brotli, none)
- `partition-by`: Comma-separated list of columns for partitioning
- `max-records-per-file`: Maximum records per output file
- `target-name`: Override target name (default from configuration)

### CSV Reading Options
- `csv-delimiter`: Field delimiter (default: `,`)
- `csv-quote`: Quote character (default: `"`)
- `csv-escape`: Escape character (default: `"`)
- `csv-header`: Whether CSV has header (true/false, default: true)

### Processing Options
- `clean-strings`: Clean and normalize string columns (true/false)
- `validate-data`: Apply data quality checks (true/false)
- `add-row-id`: Add unique row ID column (true/false)

### Performance Tuning
- `executor-memory`: Spark executor memory (default: 4g)
- `executor-cores`: Spark executor cores (default: 2)

## Environment Variables

Set via `environment_vars` in configuration:

- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARN, ERROR)
- `SPARK_SERIALIZER`: Spark serializer class
- Custom application variables for business logic

## Sample Data

The framework includes sample product data in `docker/test_data/sample_products.csv`:

```csv
product_id,product_name,category,price,stock_quantity,supplier,created_date,description
1,"Wireless Bluetooth Headphones",Electronics,79.99,45,"TechCorp","2024-01-15","High-quality wireless headphones"
2,"Organic Coffee Beans",Food,24.50,120,"GreenBean Co","2024-01-10","Premium organic coffee beans"
...
```

## Output Structure

After processing, the Parquet files are organized by partitions:

```
s3://output-bucket/processed/PRODUCTS_PARQUET/
├── category=Books/
│   └── part-00000-xxx.snappy.parquet
├── category=Electronics/
│   └── part-00001-xxx.snappy.parquet
├── category=Food/
│   └── part-00002-xxx.snappy.parquet
└── _SUCCESS
```

## Added Metadata Columns

The converter automatically adds these metadata columns:

- `_processing_timestamp`: When the record was processed
- `_job_id`: Unique FlakeRunner job identifier
- `_target_name`: Target table/dataset name
- `_framework_version`: FlakeRunner framework version
- `_emr_version`: EMR version (7.8.0)
- `_spark_version`: Spark version (3.5.4)
- `_row_id`: Unique row identifier (if enabled)

## Data Transformations

### Automatic Transformations
- **String Cleaning**: Removes extra whitespace and normalizes strings
- **Data Quality**: Removes completely empty rows
- **Type Inference**: Automatic schema detection from CSV

### Table-Specific Transformations
- **Customer Data**: Email normalization, phone number cleaning
- **Order Data**: Amount validation and formatting
- **Product Data**: Product name standardization

## Monitoring and Troubleshooting

### Check Job Status
```bash
# Get job status
./flake-runner emr --action status --job-run-id [job-run-id]

# View job logs
./flake-runner emr --action logs --file s3://bucket/products/data.csv

# Cancel running job
./flake-runner emr --action cancel --file s3://bucket/products/data.csv
```

### Performance Metrics
The script logs comprehensive statistics:
- Input/output record counts
- Processing time and throughput
- Data quality issues found and fixed
- Spark configuration details

### Common Issues
1. **Schema Inference**: Large files may need explicit schema
2. **Memory**: Increase executor memory for large datasets
3. **Partitioning**: Too many small partitions can hurt performance
4. **Compression**: Choose compression based on query patterns

## Demo Script

Run the interactive demo to see the complete workflow:

```bash
./demo_csv_to_parquet.sh
```

This demonstrates:
- Configuration validation
- Sample data overview
- EMR job submission parameters
- Expected output structure
- Monitoring commands

## Best Practices

### Performance
- Use Snappy compression for balanced performance
- Partition by commonly filtered columns
- Set appropriate max-records-per-file for your use case
- Monitor Spark UI for optimization opportunities

### Data Quality
- Always enable data validation for production workloads
- Use control files for critical data integrity checks
- Review validation errors in job logs
- Test transformations with sample data first

### Configuration Management
- Use environment-specific configuration files
- Version your container images with semantic tags
- Document custom script parameters
- Test configuration changes in development first

## Integration Examples

### With Apache Airflow
```python
from airflow.providers.amazon.aws.operators.emr import EMRServerlessStartJobOperator

emr_job = EMRServerlessStartJobOperator(
    task_id="csv_to_parquet",
    application_id="{{ var.value.emr_application_id }}",
    execution_role_arn="{{ var.value.emr_role_arn }}",
    job_driver={
        "sparkSubmit": {
            "entryPoint": "/opt/spark/jobs/csv_to_parquet_converter.py",
            "entryPointArguments": [
                "--input-path", "s3://bucket/input.csv",
                "--output-path", "s3://bucket/output/",
                "--compression", "snappy",
                "--partition-by", "year,month"
            ]
        }
    }
)
```

### With AWS Step Functions
```json
{
  "Comment": "CSV to Parquet conversion workflow",
  "StartAt": "ProcessCSV",
  "States": {
    "ProcessCSV": {
      "Type": "Task",
      "Resource": "arn:aws:states:::emr-serverless:startJobRun.sync",
      "Parameters": {
        "ApplicationId": "00f7u00a1p2k4k0k",
        "ExecutionRoleArn": "arn:aws:iam::account:role/EMRServerlessRole",
        "JobDriver": {
          "SparkSubmit": {
            "EntryPoint": "/opt/spark/jobs/csv_to_parquet_converter.py",
            "EntryPointArguments": [
              "--input-path.$": "$.input_path",
              "--output-path.$": "$.output_path",
              "--compression", "snappy"
            ]
          }
        }
      },
      "End": true
    }
  }
}
```

---

For more information, see the FlakeRunner documentation and example configurations in the repository.