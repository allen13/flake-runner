# FlakeRunner File Processor Lambda

This Lambda function processes files through the FlakeRunner workflow up to the point of EMR Serverless job submission. It's triggered by API Gateway requests and provides a REST endpoint for file processing with control data.

## Overview

The Lambda function follows the same workflow as the `flake-runner process` command but in a serverless environment:

1. **Initialize FlakeRunner** - Load configuration and create instance
2. **Validate Configuration** - Validate config and AWS resources
3. **Determine Target Table** - Map S3 prefix to Snowflake table
4. **Process File** - Create orchestration record, validate, and submit EMR job
5. **Return Response** - Detailed processing results with job IDs

## API Request Format

### Endpoint
```
POST /process
```

### Request Payload
```json
{
  "file_path": "s3://my-bucket/customers/data.csv",
  "control_data": {
    "file_name": "data.csv",
    "file_size": 1024000,
    "record_count": 5000,
    "column_count": 8,
    "file_hash": "abc123...",
    "created_at": "2024-01-01T10:00:00Z",
    "batch_id": "batch-001",
    "source_system": "external-api"
  },
  "options": {
    "validate_only": false
  }
}
```

### Request Fields

- **file_path** (required): S3 path to the file to process
- **control_data** (optional): Metadata about the file for validation
- **options.validate_only** (optional): If true, only validates without processing

## Response Format

### Success Response (200)
```json
{
  "success": true,
  "job_id": "job-abc123",
  "emr_job_run_id": "00fd12g3h4567890",
  "target_table": "CUSTOMERS",
  "orchestration_state": "EMR_SUBMITTED",
  "processing_steps": [
    {
      "step": "initialization",
      "status": "SUCCESS",
      "duration": 500000000,
      "message": "FlakeRunner initialized successfully"
    },
    {
      "step": "validation",
      "status": "SUCCESS",
      "duration": 1200000000,
      "message": "Configuration and AWS resources validated"
    },
    {
      "step": "table_mapping",
      "status": "SUCCESS", 
      "duration": 300000000,
      "message": "Mapped to table: CUSTOMERS"
    },
    {
      "step": "file_processing",
      "status": "SUCCESS",
      "duration": 5000000000,
      "message": "File processing completed, EMR job submitted"
    }
  ],
  "processing_duration": 7000000000,
  "timestamp": "2024-01-01T10:00:00Z"
}
```

### Error Response (400/500)
```json
{
  "success": false,
  "error_message": "Failed to determine target table: no mapping found for prefix 'unknown/'",
  "orchestration_state": "TABLE_MAPPING_FAILED",
  "processing_steps": [
    {
      "step": "initialization",
      "status": "SUCCESS",
      "duration": 500000000,
      "message": "FlakeRunner initialized successfully"
    },
    {
      "step": "table_mapping",
      "status": "FAILED",
      "duration": 300000000,
      "error": "no mapping found for prefix 'unknown/'"
    }
  ],
  "processing_duration": 800000000,
  "timestamp": "2024-01-01T10:00:00Z"
}
```

## Deployment

### 1. Build the Lambda Binary

```bash
# Navigate to the example directory
cd examples/lambda-file-processor

# Build for Lambda runtime
GOOS=linux GOARCH=amd64 go build -o bootstrap main.go

# Create deployment package
zip lambda-function.zip bootstrap
```

### 2. Create Lambda Function

Using AWS CLI:
```bash
aws lambda create-function \
  --function-name flake-runner-file-processor \
  --runtime provided.al2 \
  --role arn:aws:iam::123456789012:role/lambda-execution-role \
  --handler bootstrap \
  --zip-file fileb://lambda-function.zip \
  --timeout 300 \
  --memory-size 512 \
  --environment Variables='{
    "FLAKE_RUNNER_CONFIG_PATH": "/opt/flake-runner-config.json"
  }'
```

### 3. Upload Configuration

The FlakeRunner configuration file needs to be included in the Lambda deployment or accessible via environment variables.

**Option A: Bundle with deployment**
```bash
# Add config to zip
zip lambda-function.zip bootstrap flake-runner-config.json

# Update function code
aws lambda update-function-code \
  --function-name flake-runner-file-processor \
  --zip-file fileb://lambda-function.zip
```

**Option B: Use Lambda Layers**
```bash
# Create layer with config
mkdir layer && cp flake-runner-config.json layer/
zip -r config-layer.zip layer/

aws lambda publish-layer-version \
  --layer-name flake-runner-config \
  --zip-file fileb://config-layer.zip

# Attach layer to function
aws lambda update-function-configuration \
  --function-name flake-runner-file-processor \
  --layers arn:aws:lambda:us-east-1:123456789012:layer:flake-runner-config:1
```

### 4. Create API Gateway

```bash
# Create REST API
aws apigateway create-rest-api \
  --name flake-runner-api \
  --description "FlakeRunner File Processing API"

# Get API and resource IDs
API_ID=$(aws apigateway get-rest-apis --query 'items[?name==`flake-runner-api`].id' --output text)
RESOURCE_ID=$(aws apigateway get-resources --rest-api-id $API_ID --query 'items[?path==`/`].id' --output text)

# Create /process resource
aws apigateway create-resource \
  --rest-api-id $API_ID \
  --parent-id $RESOURCE_ID \
  --path-part process

PROCESS_RESOURCE_ID=$(aws apigateway get-resources --rest-api-id $API_ID --query 'items[?pathPart==`process`].id' --output text)

# Create POST method
aws apigateway put-method \
  --rest-api-id $API_ID \
  --resource-id $PROCESS_RESOURCE_ID \
  --http-method POST \
  --authorization-type NONE

# Configure Lambda integration
aws apigateway put-integration \
  --rest-api-id $API_ID \
  --resource-id $PROCESS_RESOURCE_ID \
  --http-method POST \
  --type AWS_PROXY \
  --integration-http-method POST \
  --uri arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:flake-runner-file-processor/invocations

# Deploy API
aws apigateway create-deployment \
  --rest-api-id $API_ID \
  --stage-name prod
```

## Environment Variables

- **FLAKE_RUNNER_CONFIG_PATH**: Path to FlakeRunner configuration file (default: `/opt/flake-runner-config.json`)

## IAM Permissions

The Lambda execution role needs the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-input-bucket/*",
        "arn:aws:s3:::your-output-bucket/*",
        "arn:aws:s3:::your-staging-bucket/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:Query"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/your-orchestration-table*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "emr-serverless:StartJobRun",
        "emr-serverless:GetJobRun",
        "emr-serverless:GetApplication"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::*:role/EMRServerlessRole"
    }
  ]
}
```

## Usage Examples

### Process a File
```bash
curl -X POST https://api-id.execute-api.us-east-1.amazonaws.com/prod/process \
  -H "Content-Type: application/json" \
  -d '{
    "file_path": "s3://my-bucket/customers/data.csv",
    "control_data": {
      "file_name": "data.csv",
      "file_size": 1024000,
      "record_count": 5000,
      "column_count": 8,
      "batch_id": "batch-001"
    }
  }'
```

### Validate Only
```bash
curl -X POST https://api-id.execute-api.us-east-1.amazonaws.com/prod/process \
  -H "Content-Type: application/json" \
  -d '{
    "file_path": "s3://my-bucket/customers/data.csv",
    "options": {
      "validate_only": true
    }
  }'
```

## Monitoring

- **CloudWatch Logs**: Function logs are available in `/aws/lambda/flake-runner-file-processor`
- **CloudWatch Metrics**: Standard Lambda metrics (duration, errors, invocations)
- **X-Ray Tracing**: Enable for detailed execution tracing

## Error Handling

The Lambda function includes comprehensive error handling and will return detailed error information in the response. Common error scenarios:

- **Configuration errors**: Missing or invalid FlakeRunner config
- **AWS resource errors**: EMR application not found, S3 bucket access denied
- **Validation errors**: File validation failures, control data mismatches
- **Processing errors**: EMR job submission failures

## Integration

This Lambda can be integrated with:

- **File upload systems**: Trigger processing after S3 uploads
- **Data pipelines**: Part of larger ETL workflows
- **Monitoring systems**: Webhooks for processing status
- **UI applications**: REST API for file processing requests

## Limitations

- **Timeout**: Lambda has a 15-minute maximum execution time
- **Memory**: Configure based on file sizes and processing requirements
- **Concurrent executions**: Monitor and adjust based on throughput needs
- **EMR job monitoring**: This function only submits jobs; use the EMR event handler Lambda for completion processing