# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a simplified Golang orchestration service that manages data processing workflows by coordinating S3 file operations with EMR Serverless PySpark jobs that upload files to Snowflake. The system uses S3 prefix-based routing to determine target Snowflake tables and processing configurations.

**REFACTORED STRUCTURE**: This project has been consolidated from a complex multi-package structure into a single, developer-friendly package with struct methods for easier understanding and maintenance.

## Module Information

- **Module**: `github.com/allen13/flake-runner`
- **Go Version**: 1.24.2
- **Architecture**: S3-EMR Serverless PySpark Orchestrator for Snowflake Data Loading

## Core Architecture

The system centers around a `FlakeRunner` struct that maintains AWS clients, configuration, and processing state using fluent API patterns. Key architectural features:

### Simplified Structure
- **Single Package**: All functionality consolidated into `flakerunner` package  
- **Struct Methods**: All operations are methods on `FlakeRunner` (e.g., `fr.ProcessFile()`, `fr.SubmitSparkJob()`)
- **Method Chaining**: Fluent API support: `fr.ValidateConfiguration().ValidateAWSResources()`
- **2 Files Total**: `flake_runner.go` (main library) + `cmd/main.go` (CLI interface)

### Prefix-Based Table Mapping
- S3 file paths determine target Snowflake tables via prefix matching
- Each prefix has specific validation rules, PySpark scripts, and processing configs
- Example: `s3://bucket/customers/file.csv` → `CUSTOMERS` table
- Mappings include format-specific configurations (CSV/JSON/PARQUET), compression settings, and performance tuning

### Control File System
- Each data file must have a corresponding `.ctl` control file with metadata
- Control files contain file hash, record count, column count, checksums for validation
- Example: `customer_data_20240101.csv` requires `customer_data_20240101.ctl`
- Validation ensures data integrity before processing begins

### State Management
- File orchestration states tracked in DynamoDB: `INITIATED` → `VALIDATING` → `VALIDATED` → `STAGING` → `STAGED` → `PROCESSING` → `PROCESSED` → `LOADING_SNOWFLAKE` → `LOADED` → `COMPLETED`
- State transitions recorded with timestamps and metadata for full audit trail
- Comprehensive error handling with retry logic and dead letter queue for failed files

### Asynchronous EMR Job Handling
- **EventBridge Integration**: EMR job completion events automatically trigger DynamoDB updates
- **Lambda Handler**: `HandleEMRJobEvent()` function for processing job state changes
- **Synchronous Option**: `MonitorJobProgress()` method for real-time monitoring
- **Job Management**: `GetJobLogs()`, `CancelJob()`, `GetJobStatus()` methods

### Fluent API Pattern  
- FlakeRunner methods return `*FlakeRunner` for method chaining
- Error handling accumulated in struct for streamlined workflows
- Example: `fr.ValidateConfiguration().ValidateAWSResources().ProcessFile(path)`

## Key Components

### Core FlakeRunner Structure
- `FlakeRunner`: Central struct with AWS clients (S3, EMR, DynamoDB), configuration, and state
- `Config`: Complete configuration including AWS settings, Snowflake config, and prefix mappings
- `PrefixTableMapping`: S3 prefix to Snowflake table configuration with processing rules
- `FileOrchestrationRecord`: DynamoDB tracking record for file processing with state history
- `ControlFileRecord`: Metadata validation and integrity checking with TTL management
- `EMRJobEventData`: EventBridge event structure for asynchronous job completion handling

### Processing Flow
1. **File Processing & Validation**: Control file validation and integrity checks
2. **Prefix Resolution**: Determine target Snowflake table from S3 prefix
3. **Data Integrity Validation**: Verify file against control file metadata
4. **Staging**: Prepare files for EMR processing with proper S3 staging
5. **EMR Processing**: Execute prefix-specific PySpark scripts with custom configurations
6. **Snowflake Loading**: Load processed data to target tables with validation
7. **Cleanup**: Resource cleanup and completion tracking with audit trails

### DynamoDB Schema
- **Primary Table**: `file-orchestrations-{environment}` with `file_path` (PK) and `job_id` (SK)
- **GSI 1**: `OrchestrationStateIndex` - partition by `orchestration_state`, sort by `processing_initiated_at`
- **GSI 2**: `BatchIndex` - partition by `batch_id`, sort by `processing_initiated_at`
- **GSI 3**: `JobOrchestrationIndex` - partition by `job_id`, sort by `orchestration_state`
- **TTL**: Automatic cleanup via `expires_at` attribute

## Infrastructure Dependencies

The service assumes pre-existing AWS infrastructure:
- S3 buckets (input, output, staging)
- DynamoDB table with specific schema and GSIs
- EMR Serverless application
- Custom container images in ECR
- IAM roles with required permissions

## Development Commands

### Go Lint Script
Use the provided script for comprehensive Go validation:

```bash
# Run all checks on entire project
./scripts/go-lint.sh

# Run checks on specific file or directory
./scripts/go-lint.sh path/to/file.go
./scripts/go-lint.sh path/to/directory
```

The script performs:
- `go mod tidy` (for full project runs)
- `go fmt` formatting
- `go vet` static analysis
- `go test` execution
- `goimports` import organization (if available)
- `golint` style checking (if available)

**IMPORTANT**: Always run `./scripts/go-lint.sh` after modifying any Go files.

## Git Workflow

**CRITICAL**: After making ANY changes to the codebase:
1. Always run `./scripts/go-lint.sh` to ensure code quality
2. **ALWAYS commit changes immediately** using git with descriptive commit messages
3. Use the commit message format with Claude Code attribution as shown below

**Commit Message Template**:
```
Brief description of changes

- Detailed bullet points of what was changed
- Include technical details and rationale
- Note any breaking changes or important updates

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>
```

**Example Git Commands**:
```bash
# After making changes and running go-lint.sh
git add .
git commit -m "$(cat <<'EOF'
Add new feature: EMR job monitoring with real-time status updates

- Implement GetJobStatus() method for real-time EMR job monitoring
- Add polling mechanism with configurable intervals
- Update CLI to support --wait flag for synchronous job execution
- Enhanced error handling for job failure scenarios

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>
EOF
)"
```

This ensures every change is tracked with proper attribution and maintains a clean git history.

### Manual Go Commands
```bash
# Build the project
go build -o flake-runner ./...

# Run tests
go test ./...

# Run with race detection
go test -race ./...

# Format code
go fmt ./...

# Vet code
go vet ./...

# Tidy dependencies
go mod tidy
```

### Optional Tools
Install additional Go tools for enhanced linting:
```bash
# Import formatting
go install golang.org/x/tools/cmd/goimports@latest

# Style linting
go install golang.org/x/lint/golint@latest
```

## Configuration

The system uses JSON configuration with:
- AWS resource identifiers (bucket names, DynamoDB table, EMR application ID)
- Snowflake connection details (account, database, schema, warehouse, role, authentication)
- Prefix-to-table mappings with validation rules and processing configs
- Processing timeouts, retry limits, and TTL settings

### Example Prefix Mapping
```json
{
  "s3_prefix": "customers/",
  "snowflake_table": "CUSTOMERS",
  "container_image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/flake-runner:customer-v1.0.0",
  "entry_point": "/opt/spark/jobs/customer_processor.py",
  "processing_config": {
    "file_format": "CSV",
    "compression_type": "GZIP",
    "max_file_size": 5368709120,
    "parallel_loads": 4
  },
  "validation_rules": {
    "validate_record_count": true,
    "validate_file_size": true,
    "validate_checksum": true,
    "required_fields": ["customer_id", "name", "email"]
  }
}
```

### Control File Structure
```json
{
  "file_name": "customer_data_20240101.csv",
  "file_size": 2147483648,
  "file_hash": "a1b2c3d4e5f6789012345678901234567890abcd",
  "record_count": 1000000,
  "column_count": 15,
  "created_at": "2024-01-01T10:00:00Z",
  "batch_id": "batch_20240101_001"
}
```

## Key Implementation Patterns

### Error Handling and Retries
- Automatic retries at validation, EMR processing, and Snowflake loading stages
- Dead letter queue for files exceeding retry limits
- State persistence ensures recovery from failures

### Validation Strategy
- Three-tier validation: file size, record count, and checksum verification
- Required field validation per data type

### Performance Optimization
- Configurable parallel loading for different data types
- Format-specific processing (CSV, JSON, PARQUET)
- Chunking for large files with configurable chunk sizes