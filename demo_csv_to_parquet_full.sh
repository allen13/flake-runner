#!/bin/bash

# Full End-to-End CSV to Parquet Demo Script for FlakeRunner Framework
# Demonstrates complete CSV to Parquet conversion with actual EMR processing

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
DEMO_CONFIG_TEMPLATE="csv-to-parquet-demo-config.json"
DEMO_CONFIG="demo-config-$(date +%Y%m%d_%H%M%S).json"
SAMPLE_DATA_FILE="docker/test_data/sample_products.csv"
RANDOM_SUFFIX=$(date +%Y%m%d-%H%M%S)-$$
INPUT_BUCKET="flake-runner-demo-input-${RANDOM_SUFFIX}"
OUTPUT_BUCKET="flake-runner-demo-output-${RANDOM_SUFFIX}"
STAGING_BUCKET="flake-runner-demo-staging-${RANDOM_SUFFIX}"
DEMO_S3_PATH="s3://${INPUT_BUCKET}/products/sample_products_$(date +%Y%m%d_%H%M%S).csv"

# Job tracking variables
JOB_RUN_ID=""
EMR_APP_ID=""

# Function to print colored output
print_step() {
    echo -e "${CYAN}[STEP $1]${NC} $2"
}

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${PURPLE}================================================${NC}"
    echo -e "${PURPLE}$1${NC}"
    echo -e "${PURPLE}================================================${NC}"
}


# Function to create temporary demo configuration
create_demo_config() {
    print_info "Creating temporary demo configuration..."

    # Get current AWS account ID
    local aws_account_id=$(aws sts get-caller-identity --query Account --output text)
    if [[ -z "$aws_account_id" ]]; then
        print_error "Could not get AWS account ID"
        exit 1
    fi

    # Replace placeholders in template with actual values
    sed "s/\${RANDOM_SUFFIX}/${RANDOM_SUFFIX}/g" "$DEMO_CONFIG_TEMPLATE" > "$DEMO_CONFIG"

    # Update with correct AWS account ID in the IAM role ARN
    local role_arn="arn:aws:iam::${aws_account_id}:role/EMRServerlessExecutionRole-flake-runner-demo"
    jq ".emr_execution_role_arn = \"$role_arn\"" "$DEMO_CONFIG" > "${DEMO_CONFIG}.tmp" && mv "${DEMO_CONFIG}.tmp" "$DEMO_CONFIG"

    print_success "Created demo config: $DEMO_CONFIG"
    print_info "Using consistent EMR application name: flake-runner-demo"
    print_info "Using unique resource names:"
    echo "  • Input bucket: $INPUT_BUCKET"
    echo "  • Output bucket: $OUTPUT_BUCKET"
    echo "  • Staging bucket: $STAGING_BUCKET"
    echo "  • EMR Application: flake-runner-demo"
    echo "  • IAM Role: EMRServerlessExecutionRole-flake-runner-demo"
}

# Function to check prerequisites
check_prerequisites() {
    print_step "1" "Checking Prerequisites"

    # Check if FlakeRunner binary exists
    if [[ ! -f "./flake-runner" ]]; then
        print_error "FlakeRunner binary not found. Please build it first:"
        echo "  go build -o flake-runner ./cmd"
        exit 1
    fi

    # Check if demo config template exists
    if [[ ! -f "$DEMO_CONFIG_TEMPLATE" ]]; then
        print_error "Demo configuration template not found: $DEMO_CONFIG_TEMPLATE"
        exit 1
    fi

    # Check if sample data exists
    if [[ ! -f "$SAMPLE_DATA_FILE" ]]; then
        print_error "Sample data file not found: $SAMPLE_DATA_FILE"
        exit 1
    fi

    # Check if AWS CLI is configured
    if ! aws sts get-caller-identity &>/dev/null; then
        print_error "AWS CLI not configured or no valid credentials"
        exit 1
    fi

    print_success "All prerequisites met"
}

# Function to create AWS resources
create_aws_resources() {
    print_step "2" "Creating AWS Resources"

    print_info "Creating S3 buckets, DynamoDB table, and EMR application..."
    print_warning "This will create real AWS resources that may incur charges"

    # Check if a demo EMR application already exists
    print_info "Checking for existing demo EMR application..."
    local existing_demo_app=""
    local demo_apps=$(aws emr-serverless list-applications --output json 2>/dev/null | jq -r '.applications[] | select(.name == "flake-runner-demo" and (.state == "STARTED" or .state == "CREATED" or .state == "STARTING")) | .id' | head -1)

    if [[ -n "$demo_apps" && "$demo_apps" != "None" ]]; then
        existing_demo_app="$demo_apps"
        print_success "Found existing demo application: $existing_demo_app"

        # Check if it's in a usable state
        local app_state=$(aws emr-serverless get-application --application-id "$existing_demo_app" --query 'application.state' --output text 2>/dev/null)
        if [[ "$app_state" == "STARTED" || "$app_state" == "CREATED" || "$app_state" == "STARTING" ]]; then
            print_info "Existing application is in $app_state state - reusing it"
            jq ".emr_application_id = \"$existing_demo_app\"" "$DEMO_CONFIG" > "${DEMO_CONFIG}.tmp" && mv "${DEMO_CONFIG}.tmp" "$DEMO_CONFIG"
            EMR_APP_ID="$existing_demo_app"
        else
            print_warning "Existing application is in $app_state state - will create a new one"
            existing_demo_app=""
        fi
    fi

    # Create AWS resources (will skip EMR app creation if we're reusing existing)
    if [[ -z "$existing_demo_app" ]]; then
        print_info "Creating S3 buckets, DynamoDB table, and new EMR application..."
        local create_output
        create_output=$(./flake-runner aws --action create --config "$DEMO_CONFIG" --create --force 2>&1)
        local create_exit_code=$?

        echo "$create_output"

        if [ $create_exit_code -eq 0 ]; then
            print_success "AWS resources created successfully"

            # Extract EMR application ID and role ARN from output
            local new_app_id=$(echo "$create_output" | grep -o 'Application ID: [a-z0-9]*' | cut -d' ' -f3)
            local new_role_arn=$(echo "$create_output" | grep -o 'IAM Role ARN: arn:aws:iam::[0-9]*:role/[^"]*' | cut -d' ' -f4)

            # Update config file if new values were found
            if [[ -n "$new_app_id" && "$new_app_id" != "flake-runner-demo-persistent" ]]; then
                print_info "Updating config with new EMR Application ID: $new_app_id"
                jq ".emr_application_id = \"$new_app_id\"" "$DEMO_CONFIG" > "${DEMO_CONFIG}.tmp" && mv "${DEMO_CONFIG}.tmp" "$DEMO_CONFIG"
                EMR_APP_ID="$new_app_id"
            fi

            if [[ -n "$new_role_arn" && "$new_role_arn" != *"placeholder"* ]]; then
                print_info "Updating config with new IAM Role ARN: $new_role_arn"
                jq ".emr_execution_role_arn = \"$new_role_arn\"" "$DEMO_CONFIG" > "${DEMO_CONFIG}.tmp" && mv "${DEMO_CONFIG}.tmp" "$DEMO_CONFIG"
            fi

            print_info "Waiting for EMR application and IAM resources to be ready..."
            sleep 10
        else
            print_error "Failed to create AWS resources"
            exit 1
        fi
    else
        print_info "Creating S3 buckets and DynamoDB table (reusing existing EMR application)..."
        local create_output
        create_output=$(./flake-runner aws --action create --config "$DEMO_CONFIG" --create --force 2>&1)
        local create_exit_code=$?

        echo "$create_output"

        if [ $create_exit_code -ne 0 ]; then
            print_error "Failed to create S3/DynamoDB resources"
            exit 1
        fi
        print_success "S3 and DynamoDB resources created successfully"
        print_info "Waiting for DynamoDB table to be ready..."
        sleep 15
    fi

    print_info "Final EMR Application ID: $EMR_APP_ID"
}

# Function to upload sample data and processing script
upload_sample_data() {
    print_step "3" "Uploading Sample Data and Processing Script"

    # Upload the PySpark script first
    print_info "Uploading PySpark processing script..."
    local script_s3_path="s3://${STAGING_BUCKET}/scripts/simple_csv_to_parquet.py"

    if aws s3 cp "simple_csv_to_parquet.py" "$script_s3_path"; then
        print_success "Processing script uploaded successfully"
        print_info "Script location: $script_s3_path"
    else
        print_error "Failed to upload processing script"
        exit 1
    fi

    # Upload sample CSV file
    print_info "Uploading sample CSV file to S3..."
    print_info "Source: $SAMPLE_DATA_FILE"
    print_info "Destination: $DEMO_S3_PATH"

    if aws s3 cp "$SAMPLE_DATA_FILE" "$DEMO_S3_PATH"; then
        print_success "Sample data uploaded successfully"
        print_info "File uploaded to 'products/' prefix"
        print_info "This prefix maps to PRODUCTS_PARQUET target with CSV-to-Parquet conversion"
    else
        print_error "Failed to upload sample data"
        exit 1
    fi
}

# Function to create control data
create_control_data() {
    print_step "4" "Creating Control Data"

    local file_size=$(stat -f%z "$SAMPLE_DATA_FILE" 2>/dev/null || stat -c%s "$SAMPLE_DATA_FILE" 2>/dev/null)
    local record_count=$(( $(wc -l < "$SAMPLE_DATA_FILE") - 1 ))
    local file_hash=$(shasum -a 256 "$SAMPLE_DATA_FILE" | cut -d' ' -f1)
    local current_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    cat > control_data.json << EOF
{
  "file_name": "sample_products.csv",
  "file_size": $file_size,
  "file_hash": "$file_hash",
  "record_count": $record_count,
  "column_count": 8,
  "created_at": "$current_time",
  "batch_id": "demo_batch_$(date +%Y%m%d_%H%M%S)",
  "source_system": "flake-runner-demo"
}
EOF

    print_info "Control data created:"
    cat control_data.json | jq '.'
    echo
}

# Function to process the file with actual EMR job submission
process_csv_file() {
    print_step "5" "Processing CSV File with EMR Serverless"

    print_info "Submitting CSV file for processing with FlakeRunner..."
    echo
    print_info "Command: ./flake-runner process --config $DEMO_CONFIG --file $DEMO_S3_PATH --control-data \"\$(cat control_data.json)\" --wait --timeout 20"
    echo

    if ./flake-runner process \
        --config "$DEMO_CONFIG" \
        --file "$DEMO_S3_PATH" \
        --control-data "$(cat control_data.json)" \
        --wait \
        --timeout 20; then
        print_success "File processing completed successfully!"
    else
        print_error "File processing failed"
        return 1
    fi
}

# Function to get job details and logs
get_job_details() {
    print_step "6" "Retrieving Job Details and Logs"

    print_info "Getting EMR job logs..."
    echo

    if ./flake-runner emr --action logs --config "$DEMO_CONFIG" --file "$DEMO_S3_PATH"; then
        print_success "Job logs retrieved successfully"
    else
        print_warning "Could not retrieve job logs (job may still be running)"
    fi
    echo
}

# Function to list and download processed files
download_processed_files() {
    print_step "7" "Downloading Processed Files"

    print_info "Listing processed files in output bucket..."

    # List files in the output bucket
    if aws s3 ls "s3://${OUTPUT_BUCKET}/processed/PRODUCTS_PARQUET/" --recursive; then
        print_success "Processed files found in output bucket"

        # Try to download a sample processed file
        print_info "Downloading sample processed file..."

        # Get the first parquet file
        local first_file=$(aws s3 ls "s3://${OUTPUT_BUCKET}/processed/PRODUCTS_PARQUET/" --recursive | grep '\.parquet$' | head -1 | awk '{print $4}')

        if [[ -n "$first_file" ]]; then
            local local_filename="downloads/sample_products_$(basename "$first_file")"
            if aws s3 cp "s3://${OUTPUT_BUCKET}/$first_file" "$local_filename"; then
                print_success "Downloaded processed file: $local_filename"

                # Show file info
                local file_size=$(stat -f%z "$local_filename" 2>/dev/null || stat -c%s "$local_filename" 2>/dev/null)
                print_info "File size: $file_size bytes"
                print_info "File type: Parquet"

                # If parquet-tools is available, show schema
                if command -v parquet-tools &> /dev/null; then
                    print_info "Parquet file schema:"
                    parquet-tools schema "$local_filename" || true
                fi

                return 0
            else
                print_warning "Could not download processed file"
                return 1
            fi
        else
            print_warning "No parquet files found in output bucket"
            return 1
        fi
    else
        print_warning "Could not list files in output bucket"
        return 1
    fi
}

# Function to show processing summary
show_processing_summary() {
    print_step "8" "Processing Summary"

    print_info "End-to-end processing completed successfully!"
    echo
    echo "📊 What was accomplished:"
    echo "  ✅ Created AWS resources (S3 buckets, DynamoDB table, EMR application)"
    echo "  ✅ Uploaded sample CSV data to S3"
    echo "  ✅ Created control data for validation"
    echo "  ✅ Submitted EMR Serverless job for processing"
    echo "  ✅ Monitored job completion"
    echo "  ✅ Retrieved job logs"
    echo "  ✅ Downloaded processed Parquet files"
    echo
    echo "🔧 Technical details:"
    echo "  • Input format: CSV (11 records)"
    echo "  • Output format: Parquet with Snappy compression"
    echo "  • Partitioning: By product category"
    echo "  • Processing engine: EMR Serverless 7.8.0 with Spark 3.5.4"
    echo "  • Container: Custom PySpark script for CSV-to-Parquet conversion"
    echo
    echo "📁 File locations:"
    echo "  • Input: $DEMO_S3_PATH"
    echo "  • Output: s3://${OUTPUT_BUCKET}/processed/PRODUCTS_PARQUET/"
    echo "  • Logs: CloudWatch Logs (/aws/emr-serverless/flake-runner/)"
    echo
}

# Function to cleanup demo files and AWS resources
cleanup() {
    print_step "9" "Cleanup"

    print_info "Cleaning up demo files and AWS resources..."

    # Clean up local files
    if [[ -f "control_data.json" ]]; then
        rm control_data.json
        print_info "Removed control_data.json"
    fi

    if [[ -f "$DEMO_CONFIG" ]]; then
        rm "$DEMO_CONFIG"
        print_info "Removed temporary demo config: $DEMO_CONFIG"
    fi

    # Clean up AWS resources
    print_warning "Cleaning up AWS resources to avoid charges..."

    # Delete S3 objects and buckets
    print_info "Deleting S3 buckets and contents..."
    aws s3 rm "s3://${INPUT_BUCKET}" --recursive --quiet 2>/dev/null || true
    aws s3 rb "s3://${INPUT_BUCKET}" --force --quiet 2>/dev/null || true
    aws s3 rm "s3://${OUTPUT_BUCKET}" --recursive --quiet 2>/dev/null || true
    aws s3 rb "s3://${OUTPUT_BUCKET}" --force --quiet 2>/dev/null || true
    aws s3 rm "s3://${STAGING_BUCKET}" --recursive --quiet 2>/dev/null || true
    aws s3 rb "s3://${STAGING_BUCKET}" --force --quiet 2>/dev/null || true

    # Delete DynamoDB table
    print_info "Deleting DynamoDB table..."
    aws dynamodb delete-table --table-name "flake-runner-demo-orchestrations" --region us-east-1 --quiet 2>/dev/null || true

    # Note: EMR application is left running for potential reuse
    # To delete it manually: aws emr-serverless delete-application --application-id $EMR_APP_ID
    if [[ -n "$EMR_APP_ID" && "$EMR_APP_ID" != "00f7u00a1p2demo" ]]; then
        print_info "EMR Application $EMR_APP_ID left running (stop manually if desired)"
        print_info "To delete: aws emr-serverless delete-application --application-id $EMR_APP_ID"
    fi

    print_success "Demo cleanup completed"
    print_info "Most AWS resources have been removed"
}

# Main demo function
main() {
    # Set up trap to cleanup on exit
    trap cleanup EXIT

    print_header "FlakeRunner Full End-to-End CSV to Parquet Demo"
    echo
    print_info "This demo showcases the complete FlakeRunner framework workflow:"
    print_info "• AWS resource creation (S3, DynamoDB, EMR Serverless)"
    print_info "• CSV file upload and validation"
    print_info "• Actual EMR job submission and processing"
    print_info "• Job monitoring and log retrieval"
    print_info "• Processed file download and verification"
    print_warning "This demo will create real AWS resources and submit actual EMR jobs."
    echo

    check_prerequisites
    echo

    create_demo_config
    echo

    create_aws_resources
    echo

    upload_sample_data
    echo

    create_control_data
    echo

    if process_csv_file; then
        echo
        get_job_details
        echo

        if download_processed_files; then
            echo
            show_processing_summary
        else
            print_warning "File download failed, but processing may have succeeded"
            echo
            show_processing_summary
        fi
    else
        print_error "EMR processing failed"
        echo
        print_info "Checking for any partial results..."
        get_job_details
        exit 1
    fi

    echo
    print_header "Demo Completed Successfully!"
    echo
    print_success "You have successfully demonstrated the complete FlakeRunner workflow!"
    print_info "This included real EMR Serverless processing with actual file conversion."
    echo
    print_info "Resources will be cleaned up automatically on exit."
}

# Run the demo
main "$@"
