#!/bin/bash
set -e

# FlakeRunner Integration Test Runner
# This script runs different types of integration tests based on available dependencies

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test configuration
RUN_BASIC_TESTS=true
RUN_DOCKER_TESTS=false
RUN_AWS_TESTS=false
RUN_BENCHMARKS=false
VERBOSE=false
SHORT_MODE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --docker)
            RUN_DOCKER_TESTS=true
            shift
            ;;
        --aws)
            RUN_AWS_TESTS=true
            shift
            ;;
        --bench)
            RUN_BENCHMARKS=true
            shift
            ;;
        --all)
            RUN_DOCKER_TESTS=true
            RUN_AWS_TESTS=true
            RUN_BENCHMARKS=true
            shift
            ;;
        --short)
            SHORT_MODE=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "FlakeRunner Integration Test Runner"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --docker      Run Docker integration tests"
            echo "  --aws         Run AWS integration tests"
            echo "  --bench       Run benchmark tests"
            echo "  --all         Run all test types"
            echo "  --short       Run in short mode (skip long-running tests)"
            echo "  --verbose, -v Verbose output"
            echo "  --help, -h    Show this help message"
            echo ""
            echo "Environment Variables:"
            echo "  FLAKE_RUNNER_AWS_INTEGRATION=true   Enable AWS integration tests"
            echo "  FLAKE_RUNNER_TEST_EMR_APP_ID        EMR Application ID for testing"
            echo "  FLAKE_RUNNER_TEST_EXECUTION_ROLE    EMR execution role ARN"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Functions
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check Go
    if ! command -v go &> /dev/null; then
        print_error "Go is not installed"
        exit 1
    fi
    print_success "Go $(go version | awk '{print $3}') found"
    
    # Check Docker (if needed)
    if [ "$RUN_DOCKER_TESTS" = true ]; then
        if ! command -v docker &> /dev/null; then
            print_error "Docker is required for Docker tests but not found"
            exit 1
        fi
        
        if ! docker info &> /dev/null; then
            print_error "Docker is not running"
            exit 1
        fi
        print_success "Docker found and running"
        
        # Check if flake-runner image exists
        if ! docker images --format "table {{.Repository}}:{{.Tag}}" | grep -q "flake-runner:local"; then
            print_warning "flake-runner:local image not found"
            print_info "Building Docker image..."
            
            cd "$PROJECT_ROOT/docker"
            if docker build -t flake-runner:local .; then
                print_success "Docker image built successfully"
            else
                print_error "Failed to build Docker image"
                exit 1
            fi
            cd "$SCRIPT_DIR"
        else
            print_success "flake-runner:local image found"
        fi
    fi
    
    # Check AWS credentials (if needed)
    if [ "$RUN_AWS_TESTS" = true ]; then
        if ! command -v aws &> /dev/null; then
            print_warning "AWS CLI not found, but proceeding with tests"
        else
            if aws sts get-caller-identity &> /dev/null; then
                ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
                print_success "AWS credentials found (Account: $ACCOUNT_ID)"
            else
                print_error "AWS credentials not configured"
                exit 1
            fi
        fi
        
        # Set AWS integration flag
        export FLAKE_RUNNER_AWS_INTEGRATION=true
        print_info "AWS integration testing enabled"
    fi
}

run_go_tests() {
    local test_pattern="$1"
    local test_description="$2"
    local additional_flags="$3"
    
    print_info "Running: $test_description"
    
    # Change to project root to run tests
    cd "$PROJECT_ROOT"
    
    # Build go test command
    local cmd="go test"
    
    if [ "$VERBOSE" = true ]; then
        cmd="$cmd -v"
    fi
    
    if [ "$SHORT_MODE" = true ]; then
        cmd="$cmd -short"
    fi
    
    cmd="$cmd ./test"
    
    if [ -n "$test_pattern" ]; then
        cmd="$cmd -run $test_pattern"
    fi
    
    if [ -n "$additional_flags" ]; then
        cmd="$cmd $additional_flags"
    fi
    
    print_info "Executing: $cmd"
    
    if eval "$cmd"; then
        print_success "$test_description completed successfully"
        cd "$SCRIPT_DIR"
        return 0
    else
        print_error "$test_description failed"
        cd "$SCRIPT_DIR"
        return 1
    fi
}

run_basic_tests() {
    print_header "Running Basic Integration Tests"
    
    if run_go_tests "TestFlakeRunnerIntegration" "Basic Integration Tests"; then
        print_success "Basic integration tests passed"
    else
        print_error "Basic integration tests failed"
        return 1
    fi
}

run_docker_tests() {
    print_header "Running Docker Integration Tests"
    
    print_info "Testing Docker integration with PySpark execution..."
    
    if run_go_tests "TestDockerIntegration" "Docker Integration Tests"; then
        print_success "Docker integration tests passed"
    else
        print_error "Docker integration tests failed"
        return 1
    fi
    
    # Also run end-to-end tests that include Docker
    if run_go_tests "TestEndToEndFlakeRunnerPySparkIntegration" "End-to-End Integration Tests"; then
        print_success "End-to-end integration tests passed"
    else
        print_warning "End-to-end integration tests failed"
    fi
}

run_aws_tests() {
    print_header "Running AWS Integration Tests"
    
    print_info "Testing with real AWS resources (S3, DynamoDB, EMR)..."
    print_warning "This will create temporary AWS resources and may incur small costs"
    
    if run_go_tests "TestAWS" "AWS Integration Tests"; then
        print_success "AWS integration tests passed"
    else
        print_error "AWS integration tests failed"
        return 1
    fi
}

run_benchmark_tests() {
    print_header "Running Benchmark Tests"
    
    print_info "Running performance benchmarks..."
    
    if run_go_tests "" "Benchmark Tests" "-bench=."; then
        print_success "Benchmark tests completed"
    else
        print_warning "Some benchmark tests failed"
    fi
}

cleanup() {
    print_info "Cleaning up test artifacts..."
    
    # Remove any temporary files created during testing
    find "$PROJECT_ROOT" -name "flake-runner-*-*" -type d -exec rm -rf {} + 2>/dev/null || true
    
    print_success "Cleanup completed"
}

main() {
    print_header "FlakeRunner Integration Test Suite"
    
    # Check prerequisites
    check_prerequisites
    
    # Initialize Go modules
    print_info "Ensuring Go dependencies are up to date..."
    cd "$PROJECT_ROOT"
    go mod tidy
    cd "$SCRIPT_DIR"
    
    # Track test results
    local failed_tests=0
    local total_tests=0
    
    # Run basic tests
    if [ "$RUN_BASIC_TESTS" = true ]; then
        total_tests=$((total_tests + 1))
        if ! run_basic_tests; then
            failed_tests=$((failed_tests + 1))
        fi
    fi
    
    # Run Docker tests
    if [ "$RUN_DOCKER_TESTS" = true ]; then
        total_tests=$((total_tests + 1))
        if ! run_docker_tests; then
            failed_tests=$((failed_tests + 1))
        fi
    fi
    
    # Run AWS tests
    if [ "$RUN_AWS_TESTS" = true ]; then
        total_tests=$((total_tests + 1))
        if ! run_aws_tests; then
            failed_tests=$((failed_tests + 1))
        fi
    fi
    
    # Run benchmarks
    if [ "$RUN_BENCHMARKS" = true ]; then
        total_tests=$((total_tests + 1))
        if ! run_benchmark_tests; then
            # Benchmarks failing is not a critical error
            print_warning "Benchmark tests had issues but continuing"
        fi
    fi
    
    # Cleanup
    cleanup
    
    # Print summary
    print_header "Test Summary"
    
    local passed_tests=$((total_tests - failed_tests))
    echo -e "Total test suites: $total_tests"
    echo -e "Passed: ${GREEN}$passed_tests${NC}"
    echo -e "Failed: ${RED}$failed_tests${NC}"
    
    if [ $failed_tests -eq 0 ]; then
        print_success "All test suites passed! 🎉"
        exit 0
    else
        print_error "$failed_tests test suite(s) failed"
        exit 1
    fi
}

# Handle interruption
trap cleanup EXIT

# Run main function
main "$@"