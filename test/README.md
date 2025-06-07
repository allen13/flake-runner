# FlakeRunner Integration Test Suite

This directory contains comprehensive integration tests for the FlakeRunner system that test both the Go functions and the PySpark scripts they feed into.

## Test Types

### 1. Basic Integration Tests (`integration_test.go`)
- Tests FlakeRunner configuration and validation
- Tests file processing logic without external dependencies
- Tests workflow simulation and error scenarios
- **Dependencies**: None (pure Go testing)

### 2. Docker Integration Tests (`docker_integration_test.go`)
- Tests PySpark script execution using Docker
- Tests end-to-end data processing with real Spark
- Tests both simple_processor.py and processor.py
- **Dependencies**: Docker, flake-runner:local image

### 3. End-to-End Tests (`end_to_end_test.go`)
- Tests complete workflow from FlakeRunner to PySpark
- Tests performance scenarios with larger datasets
- Tests error handling and edge cases
- **Dependencies**: Docker (optional)

### 4. AWS Integration Tests (`aws_integration_test.go`)
- Tests with real AWS resources (S3, DynamoDB, EMR)
- Tests complete AWS workflow
- Tests resource validation and cleanup
- **Dependencies**: AWS credentials, AWS resources

## Running Tests

### Prerequisites

1. **Go Dependencies**:
   ```bash
   go mod tidy
   ```

2. **Docker (for Docker tests)**:
   ```bash
   # Build the flake-runner image
   cd ../docker
   docker build -t flake-runner:local .
   ```

3. **AWS Credentials (for AWS tests)**:
   ```bash
   aws configure
   # or set environment variables
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   export AWS_REGION=us-east-1
   ```

### Running Test Suites

#### 1. Basic Integration Tests (No Dependencies)
```bash
go test -v ./test -run TestFlakeRunnerIntegration
```

#### 2. Docker Integration Tests
```bash
# Ensure Docker is running and image is built
go test -v ./test -run TestDockerIntegration
```

#### 3. End-to-End Tests
```bash
go test -v ./test -run TestEndToEndFlakeRunnerPySparkIntegration
```

#### 4. AWS Integration Tests
```bash
# Enable AWS integration testing
export FLAKE_RUNNER_AWS_INTEGRATION=true

# Optional: Set test EMR application and role
export FLAKE_RUNNER_TEST_EMR_APP_ID=your_emr_app_id
export FLAKE_RUNNER_TEST_EXECUTION_ROLE=arn:aws:iam::account:role/EMRServerlessRole

go test -v ./test -run TestAWSIntegration
```

#### 5. All Tests
```bash
# Run all tests (will skip those with missing dependencies)
go test -v ./test

# Run with short mode (skips long-running tests)
go test -v -short ./test
```

### Benchmark Tests
```bash
# Run performance benchmarks
go test -v ./test -bench=.

# Run specific benchmark
go test -v ./test -bench=BenchmarkDockerIntegration
```

## Test Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLAKE_RUNNER_AWS_INTEGRATION` | Enable AWS integration tests | `false` |
| `FLAKE_RUNNER_TEST_EMR_APP_ID` | EMR Application ID for testing | `test-emr-app-id` |
| `FLAKE_RUNNER_TEST_EXECUTION_ROLE` | EMR execution role ARN | Test role |

### Test Data

Tests use generated test data with configurable sizes:
- **Small datasets**: 25-50 records (fast tests)
- **Medium datasets**: 100-500 records (performance tests)
- **Large datasets**: 1000+ records (stress tests)

## Test Scenarios

### 1. Data Processing Scenarios
- **Customer Data**: CSV with personal information
- **Order Data**: CSV with transaction information
- **Analytics Data**: JSON with event information

### 2. File Format Testing
- **CSV**: Comma-separated values with headers
- **JSON**: Structured JSON objects
- **JSONL**: Newline-delimited JSON
- **Parquet**: Columnar format (output only)

### 3. Error Scenarios
- Invalid configuration files
- Missing input files
- Malformed data
- Network failures (AWS tests)
- Resource limitations

### 4. Performance Testing
- Large file processing
- Concurrent operations
- Memory usage validation
- Processing time benchmarks

## AWS Integration Details

### Resources Created

When AWS integration tests run, they create:

1. **S3 Buckets**:
   - `flake-runner-test-{timestamp}-input`
   - `flake-runner-test-{timestamp}-output` 
   - `flake-runner-test-{timestamp}-staging`

2. **DynamoDB Table**:
   - `flake-runner-test-{timestamp}-orchestrations`
   - With GSI: `OrchestrationStateIndex`

3. **Test Data**:
   - Sample CSV files uploaded to input bucket
   - Orchestration records in DynamoDB

### Resource Cleanup

All test resources are automatically cleaned up after test completion:
- S3 buckets are emptied and deleted
- DynamoDB tables are deleted
- No persistent resources remain

### Cost Considerations

AWS integration tests incur minimal costs:
- S3: Few KB of storage and requests
- DynamoDB: Minimal read/write operations
- EMR: No actual jobs run (unless real app ID provided)

**Estimated cost per test run: < $0.01**

## Docker Integration Details

### Container Testing

Docker tests validate:
1. **PySpark Environment**: Correct Python and Spark versions
2. **Data Processing**: End-to-end CSV → Parquet/JSON
3. **Metadata Addition**: Processing timestamps and job IDs
4. **Error Handling**: Invalid inputs and configurations

### Output Validation

Tests verify:
- Output files are created
- `_SUCCESS` markers exist
- Data transformations are applied
- Metadata fields are added
- Record counts match expectations

## Troubleshooting

### Common Issues

1. **Docker Tests Failing**:
   ```bash
   # Check if Docker is running
   docker info
   
   # Check if image exists
   docker images | grep flake-runner
   
   # Rebuild image if needed
   cd ../docker && docker build -t flake-runner:local .
   ```

2. **AWS Tests Failing**:
   ```bash
   # Check AWS credentials
   aws sts get-caller-identity
   
   # Check permissions
   aws s3 ls  # Should list buckets
   aws dynamodb list-tables  # Should list tables
   ```

3. **Go Module Issues**:
   ```bash
   # Clean and re-download dependencies
   go clean -modcache
   go mod download
   go mod tidy
   ```

### Test Debugging

Enable verbose output:
```bash
go test -v ./test -run TestName
```

Run specific test cases:
```bash
go test -v ./test -run TestDockerIntegration/Customer_Data_Docker_Processing
```

Skip long-running tests:
```bash
go test -v -short ./test
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  basic-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v3
        with:
          go-version: 1.24.2
      - name: Run basic integration tests
        run: go test -v ./test -run TestFlakeRunnerIntegration

  docker-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v3
        with:
          go-version: 1.24.2
      - name: Build Docker image
        run: |
          cd docker
          docker build -t flake-runner:local .
      - name: Run Docker integration tests
        run: go test -v ./test -run TestDockerIntegration

  aws-tests:
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v3
        with:
          go-version: 1.24.2
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v2
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      - name: Run AWS integration tests
        run: |
          export FLAKE_RUNNER_AWS_INTEGRATION=true
          go test -v ./test -run TestAWSIntegration
```

## Contributing

When adding new tests:

1. **Follow naming conventions**: `Test{Component}{Feature}`
2. **Add proper cleanup**: Ensure resources are cleaned up
3. **Document dependencies**: Update this README
4. **Test multiple scenarios**: Success, failure, edge cases
5. **Add benchmarks**: For performance-critical features

### Test Structure

```go
func TestNewFeature(t *testing.T) {
    // Setup
    env, cleanup := setupTestEnvironment(t)
    defer cleanup()
    
    // Test scenarios
    testCases := []struct{
        name string
        // test case fields
    }{
        // test cases
    }
    
    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            // Test implementation
        })
    }
}
```

## Performance Expectations

### Benchmark Targets

| Test Type | Duration | Memory | Resource Usage |
|-----------|----------|--------|----------------|
| Basic Integration | < 30s | < 100MB | CPU only |
| Docker Integration | < 2min | < 500MB | Docker + CPU |
| End-to-End | < 5min | < 1GB | Docker + CPU |
| AWS Integration | < 10min | < 200MB | AWS + Network |

### Scaling Considerations

- Tests should complete within CI timeout limits
- Memory usage should be reasonable for CI environments  
- AWS costs should remain minimal
- Docker images should be cached when possible