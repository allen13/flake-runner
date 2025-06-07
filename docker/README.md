# EMR Serverless Container Images - Simplified Build

This directory contains Docker configuration and scripts for building a simplified, unified EMR Serverless container image optimized for Spark 3.5.4, Python 3.11, and Java 17.

## Quick Start

1. Set your AWS account ID and region:
```bash
export AWS_ACCOUNT_ID=123456789012
export AWS_REGION=us-east-1
```

2. Build and push the unified container image:
```bash
./build.sh
```

3. Use the built image in your EMR Serverless job submissions with different entry points.

## Simplified Architecture

### Single Unified Image
- **Purpose**: One container with all PySpark scripts included
- **Tag**: `flake-runner:v3.0.0-simplified`
- **Benefits**: 
  - Faster CI/CD (single build)
  - Simplified deployment
  - Easier maintenance
  - Consistent runtime environment

### Available Scripts
- **processor.py**: Universal data processor for production EMR Serverless workloads
- **simple_processor.py**: Lightweight processor for local testing and development

## Build Configuration

### Environment Variables
- `AWS_ACCOUNT_ID`: Your AWS account ID (default: 123456789012)
- `AWS_REGION`: AWS region for ECR (default: us-east-1)
- `VERSION_TAG`: Image version tag (default: v3.0.0-simplified)

### Build Process
The simplified build script:
1. Authenticates with Amazon ECR
2. Creates ECR repository if needed
3. Builds single unified container
4. Pushes image to ECR with version and latest tags

## EMR 7.8.0 Optimizations

### Runtime Environment
- **Spark Version**: 3.5.4 with adaptive query execution
- **Python Version**: 3.11 with performance improvements
- **Java Version**: 17 with enhanced security and performance

### Performance Features
- Adaptive partitioning for better resource utilization
- Optimized Snowflake connector (2.12.0-spark_3.5)
- Enhanced Arrow integration for faster data transfer
- Improved memory management and garbage collection

### Dependencies
- snowflake-connector-python 3.7.1
- snowflake-spark-connector 2.12.0-spark_3.5
- boto3 1.35.36
- pandas 2.2.3
- pyarrow 17.0.0
- Additional data processing libraries

## Usage in EMR Serverless

### Job Submission Examples

#### Customer Data Processing
```python
import boto3

emr = boto3.client('emr-serverless')

response = emr.start_job_run(
    applicationId='your-app-id',
    executionRoleArn='your-execution-role',
    jobDriver={
        'sparkSubmit': {
            'entryPoint': '/opt/spark/jobs/processor.py',
            'entryPointArguments': [
                '--input-path', 's3://input-bucket/customers/',
                '--output-path', 's3://output-bucket/processed/',
                '--staging-path', 's3://staging-bucket/temp/',
                '--target-table', 'CUSTOMERS',
                '--job-id', 'customer-job-001'
            ]
        }
    },
    configurationOverrides={
        'applicationConfiguration': [
            {
                'classification': 'spark-defaults',
                'properties': {
                    'spark.kubernetes.container.image': 'your-account.dkr.ecr.us-east-1.amazonaws.com/flake-runner:v3.0.0-simplified'
                }
            }
        ]
    }
)
```

#### Different Data Types
All data types use the same image but with different entry point arguments:
- **Customer data**: `--target-table CUSTOMERS`
- **Order data**: `--target-table ORDERS`  
- **Product data**: `--target-table PRODUCTS`
- **Analytics data**: `--target-table ANALYTICS`

The universal processor automatically applies appropriate transformations based on the target table.

## Local Development and Testing

### Simple Processor for Local Testing
The `simple_processor.py` script is designed for local development:

```bash
# Example local usage (requires PySpark installation)
python3 src/simple_processor.py \
  --input-path ./test_data/sample_customers.csv \
  --output-path ./output \
  --input-format CSV \
  --output-format PARQUET \
  --job-id local-test-001
```

### Integration Testing
Run the included integration tests:

```bash
# Run simple integration tests (no PySpark required)
python3 test_simple.py

# Run full integration tests (requires PySpark)
python3 test_integration.py
```

### Test Data
Sample test data is included in `test_data/`:
- `sample_customers.csv`: Customer data for testing

### Building Locally
```bash
# Build the unified image
docker build -t flake-runner:local .

# Test with Docker (requires proper Spark setup)
docker run --rm -v $(pwd)/test_data:/data flake-runner:local \
  python3 /opt/spark/jobs/simple_processor.py \
  --input-path /data/sample_customers.csv \
  --output-path /data/output \
  --job-id docker-test
```

## Configuration Examples

### Update FlakeRunner Configuration
Update your `example-config.json` to use the simplified image:

```json
{
  "prefix_table_mappings": [
    {
      "s3_prefix": "customers/",
      "snowflake_table": "CUSTOMERS",
      "container_image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/flake-runner:v3.0.0-simplified",
      "entry_point": "/opt/spark/jobs/processor.py"
    },
    {
      "s3_prefix": "orders/",
      "snowflake_table": "ORDERS", 
      "container_image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/flake-runner:v3.0.0-simplified",
      "entry_point": "/opt/spark/jobs/processor.py"
    }
  ]
}
```

## Benefits of Simplified Approach

### Development Benefits
- **Faster Builds**: Single build instead of multiple
- **Easier Testing**: One image to test and validate
- **Simplified CI/CD**: Fewer moving parts
- **Consistent Environment**: Same runtime for all workloads

### Operational Benefits
- **Reduced Storage**: One image instead of multiple
- **Easier Updates**: Update once, deploy everywhere
- **Simplified Monitoring**: Single image to track
- **Lower Complexity**: Fewer images to manage

### Cost Benefits
- **Reduced ECR Storage**: Fewer images stored
- **Faster Deployments**: Less data to transfer
- **Simplified Operations**: Reduced operational overhead

## Migration from Multi-Image Setup

If migrating from the previous multi-image setup:

1. Update configuration files to use the new unified image
2. Ensure entry points are correctly specified
3. Test with existing workloads
4. Clean up old images from ECR

## Troubleshooting

### Common Issues
1. **ECR Authentication**: Ensure AWS credentials are configured
2. **Entry Point**: Verify correct script path in job submission
3. **Image Size**: Image is ~2GB due to Spark dependencies
4. **Build Time**: Initial builds take 10-15 minutes

### Testing Without PySpark
The integration tests are designed to work without a local PySpark installation:
- Tests validate command-line interface
- Tests check error handling
- Tests verify file processing logic
- Full functionality requires proper Spark environment

### Logs and Debugging
- Container logs available in CloudWatch
- Use `docker logs` for local debugging
- EMR job logs provide detailed execution information

## Security

### Best Practices
- Images run as non-root `hadoop` user
- Minimal attack surface with only required dependencies
- Regular base image updates for security patches
- Secrets managed through EMR execution roles

### Compliance
- Images based on official AWS EMR runtime
- FIPS-compliant when using appropriate base images
- Audit trail through CloudTrail for image usage

## Support

For issues or questions:
1. Check EMR Serverless documentation
2. Review CloudWatch logs
3. Validate container configuration
4. Test with simple_processor.py first
5. Run integration tests to verify setup