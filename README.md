# FlakeRunner

FlakeRunner is a Go-based data orchestration framework that manages data processing workflows by coordinating S3 file operations with EMR Serverless PySpark jobs. It provides prefix-based routing to determine target Snowflake tables and processing configurations, with comprehensive state management through DynamoDB.

## Features

- **S3-EMR Serverless Integration**: Seamlessly orchestrate PySpark jobs on EMR Serverless
- **Prefix-Based Table Mapping**: Route S3 files to appropriate Snowflake tables based on prefixes
- **Control File Validation**: Ensure data integrity with metadata validation and checksums
- **State Management**: Track processing states in DynamoDB with full audit trails
- **Fluent API Design**: Chain operations for clean, readable workflows
- **Comprehensive Error Handling**: Built-in retry logic and dead letter queue support
- **AWS Resource Management**: Create and manage required AWS infrastructure

## Architecture

```
S3 Input → Validation → EMR Serverless Processing → S3 Output → Snowflake
                ↓                    ↓
            DynamoDB ←──── State Tracking ────→ CloudWatch Logs
```

## Installation

```bash
# Clone the repository
git clone https://github.com/allen13/flake-runner.git
cd flake-runner

# Build the binary
go build -o flake-runner ./cmd

# Or install directly
go install github.com/allen13/flake-runner/cmd@latest
```

## Configuration

Create a configuration file (`config.json`) with your AWS resources and prefix mappings:

```json
{
  "aws_profile": "default",
  "aws_region": "us-east-1",
  "input_bucket_name": "my-data-input",
  "output_bucket_name": "my-data-output",
  "staging_bucket_name": "my-data-staging",
  "control_table_name": "file-orchestrations-prod",
  "emr_application_id": "00abc123def456gh",
  "emr_execution_role_arn": "arn:aws:iam::123456789012:role/EMRServerlessExecutionRole",
  "job_timeout_minutes": 30,
  "max_retries": 3,
  "control_ttl_days": 7,
  "prefix_mappings": [
    {
      "s3_prefix": "customers/",
      "target_name": "CUSTOMERS",
      "entry_point": "s3://my-staging/scripts/customer_processor.py",
      "processing_config": {
        "file_format": "CSV",
        "compression_type": "GZIP",
        "max_file_size": 5368709120
      },
      "validation_rules": {
        "validate_record_count": true,
        "validate_file_size": true,
        "validate_checksum": true,
        "required_fields": ["customer_id", "email", "name"]
      }
    }
  ]
}
```

## Usage

### Initialize and Validate Configuration

```bash
# Validate configuration and AWS resources
$ flake-runner init -config production.json

✅ Flake Runner initialized successfully!
📊 Job ID: job-a1b2c3d4
🗂️  Configured with 3 prefix mappings:
   • customers/ → CUSTOMERS
   • orders/ → ORDERS
   • products/ → PRODUCTS
🚀 Ready to process files!
```

### Process Files

```bash
# Process a single file
$ flake-runner process -file s3://my-bucket/customers/customer_data.csv

Processing file: s3://my-bucket/customers/customer_data.csv
✅ File processing initiated successfully!
📊 Job ID: job-a1b2c3d4

# Process with control data validation
$ flake-runner process -file s3://my-bucket/customers/data.csv \
  -control-data '{"file_name":"data.csv","file_size":1024,"file_hash":"abc123","record_count":100}'

Processing file: s3://my-bucket/customers/data.csv
Using provided control data for validation
✅ File processing initiated successfully!
📊 Job ID: job-b2c3d4e5

# Process and wait for completion
$ flake-runner process -file s3://my-bucket/orders/orders_2024.csv -wait

Processing file: s3://my-bucket/orders/orders_2024.csv
✅ File processing initiated successfully!
📊 Job ID: job-c3d4e5f6
⏳ Waiting for job completion (timeout: 120 minutes, poll interval: 30 seconds)...
📈 Starting job polling at 14:23:45
[0s] State: PROCESSING - EMR Serverless Processing
   📊 EMR Job: RUNNING
[30s] State: PROCESSING - EMR Serverless Processing
   📊 EMR Job: SUCCESS
   ✅ EMR job completed successfully, updating orchestration state...
   📝 Orchestration state updated to PROCESSED
[31s] State: PROCESSED - Processing Completed Successfully
🎉 Job processing completed successfully in 31s!

📋 Job Summary:
   File: orders_2024.csv
   Target Table: ORDERS
   Batch ID: batch_20240107_001
   Started: 2024-01-07 14:23:45
   Completed: 2024-01-07 14:24:16
   Duration: 31s

🔍 Validation Results:
   Expected Records: 50000
   Actual Records: 50000
   Record Count Match: true
   File Size Match: true
   Checksum Match: true

⚡ EMR Job Details:
   Job Run ID: 00f1qa2lmnmnhj1n
   Status: SUCCESS
   Processed Records: 50000
```

### Check Processing Status

```bash
# Get status of a specific file
$ flake-runner status -file s3://my-bucket/customers/data.csv

File: s3://my-bucket/customers/data.csv
State: COMPLETED
Target: CUSTOMERS
Started: 2024-01-07 10:15:23
Completed: 2024-01-07 10:18:45
Duration: 3m22s

# Query files by state
$ flake-runner status -state FAILED

Found 2 files in FAILED state:
1. s3://my-bucket/orders/bad_file.csv
   Failed at: 2024-01-07 09:45:12
   Error: Record count validation failed: expected 1000, got 950
   
2. s3://my-bucket/products/corrupt.parquet
   Failed at: 2024-01-07 11:23:45
   Error: Parquet file corrupted: invalid magic bytes
```

### EMR Job Operations

```bash
# View job logs
$ flake-runner emr -action logs -file s3://my-bucket/customers/data.csv

Fetching EMR job logs for: job-abc12345
Job Run ID: 00f1qa2lmnmnhj1n

[DRIVER] 2024-01-07 10:15:45 INFO  Processing started for s3://my-bucket/customers/data.csv
[DRIVER] 2024-01-07 10:15:46 INFO  Found 50000 records in CSV file
[DRIVER] 2024-01-07 10:15:47 INFO  Applying transformations...
[DRIVER] 2024-01-07 10:16:23 INFO  Writing output to s3://my-output/processed/CUSTOMERS/
[DRIVER] 2024-01-07 10:16:45 INFO  Processing completed successfully

# Cancel a running job
$ flake-runner emr -action cancel -file s3://my-bucket/customers/large_file.csv

✅ Job cancellation requested successfully
```

### AWS Resource Management

```bash
# List all AWS resources
$ flake-runner aws --action list

🔍 Listing AWS Resources...
Region: us-east-1

📦 S3 Buckets:
   ✅ Input Bucket: my-data-input (accessible)
   ✅ Output Bucket: my-data-output (accessible)
   ✅ Staging Bucket: my-data-staging (accessible)

🗄️  DynamoDB Tables:
   ✅ Control Table: file-orchestrations-prod (accessible)

⚡ EMR Serverless Applications:
   ✅ Application: 00abc123def456gh (accessible)

✅ Resource listing complete

# Create missing AWS resources
$ flake-runner aws --action create --create --force

🏗️  Creating AWS Resources...

📦 S3 Buckets:
   ✅ Input Bucket: my-data-input (already exists)
   ✅ Created Output Bucket: my-data-output
   ✅ Created Staging Bucket: my-data-staging

🗄️  DynamoDB Tables:
   ✅ Created Control Table: file-orchestrations-prod

⚡ EMR Serverless Applications:
   🏗️  Creating EMR Application with IAM Role: flake-runner-production...
   ✅ Created EMR Application: flake-runner-production
   📋 Application ID: 00xyz789ghi012jk
   🔐 IAM Role ARN: arn:aws:iam::123456789012:role/EMRServerlessExecutionRole-flake-runner-production
   💡 Update your configuration file with these values:
      "emr_application_id": "00xyz789ghi012jk",
      "emr_execution_role_arn": "arn:aws:iam::123456789012:role/EMRServerlessExecutionRole-flake-runner-production"

✅ Resource creation complete

# Upload files to S3
$ flake-runner aws --action upload --local customer_data.csv --file customers/upload_20240107.csv

📤 Uploading file: customer_data.csv → customers/upload_20240107.csv
✅ Successfully uploaded to: s3://my-data-input/customers/upload_20240107.csv
📏 File size: 2483901 bytes
🎯 Target table: CUSTOMERS
💡 Process with: flake-runner process --file s3://my-data-input/customers/upload_20240107.csv
```

## Control File Format

Control files provide metadata for validation:

```json
{
  "file_name": "customer_data.csv",
  "file_size": 2483901,
  "file_hash": "a1b2c3d4e5f6789012345678901234567890abcd",
  "record_count": 50000,
  "column_count": 12,
  "created_at": "2024-01-07T10:00:00Z",
  "batch_id": "batch_20240107_001"
}
```

## Processing States

Files progress through the following states:

1. **INITIATED** - File processing started
2. **VALIDATING** - Validating file and control data
3. **VALIDATED** - Validation completed successfully
4. **STAGING** - Preparing files for processing
5. **STAGED** - Files staged successfully
6. **PROCESSING** - EMR Serverless job running
7. **PROCESSED** - Processing completed successfully
8. **LOADING** - Loading data to Snowflake
9. **LOADED** - Data loaded successfully
10. **COMPLETED** - All processing completed
11. **FAILED** - Processing failed (with retry support)

## Demo

Try the included demo to see FlakeRunner in action:

```bash
# Run the CSV to Parquet conversion demo
./demo_csv_to_parquet_full.sh

FlakeRunner Full End-to-End CSV to Parquet Demo
================================================
This demo showcases the complete FlakeRunner framework workflow:
• AWS resource creation (S3, DynamoDB, EMR Serverless)
• CSV file upload and validation
• Actual EMR job submission and processing
• Job monitoring and log retrieval
• Processed file download and verification
```

## Development

```bash
# Run tests
go test ./...

# Run linting
./scripts/go-lint.sh

# Build for different platforms
GOOS=linux GOARCH=amd64 go build -o flake-runner-linux ./cmd
GOOS=darwin GOARCH=amd64 go build -o flake-runner-darwin ./cmd
GOOS=windows GOARCH=amd64 go build -o flake-runner.exe ./cmd
```

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

For issues, questions, or contributions, please open an issue on GitHub.