#!/bin/bash

# Go validation, linting, and formatting script
# Usage: ./scripts/go-lint.sh [path]
# If no path is provided, processes all Go files in the project

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default to current directory if no path provided
TARGET_PATH="${1:-.}"

echo -e "${BLUE}🔍 Running Go validation, linting, and formatting...${NC}"
echo -e "${BLUE}Target path: ${TARGET_PATH}${NC}"
echo ""

# Function to check if Go files exist in the target path
check_go_files() {
    if [ "$TARGET_PATH" = "." ]; then
        # Check entire project
        if ! find . -name "*.go" -not -path "./vendor/*" | grep -q .; then
            echo -e "${YELLOW}⚠️  No Go files found in project${NC}"
            exit 0
        fi
    else
        # Check specific path
        if [[ "$TARGET_PATH" == *.go ]]; then
            # Single file
            if [ ! -f "$TARGET_PATH" ]; then
                echo -e "${RED}❌ File not found: $TARGET_PATH${NC}"
                exit 1
            fi
        else
            # Directory
            if ! find "$TARGET_PATH" -name "*.go" 2>/dev/null | grep -q .; then
                echo -e "${YELLOW}⚠️  No Go files found in: $TARGET_PATH${NC}"
                exit 0
            fi
        fi
    fi
}

# Function to run go mod tidy
run_mod_tidy() {
    echo -e "${BLUE}📦 Running go mod tidy...${NC}"
    if go mod tidy; then
        echo -e "${GREEN}✅ go mod tidy completed${NC}"
    else
        echo -e "${RED}❌ go mod tidy failed${NC}"
        return 1
    fi
    echo ""
}

# Function to format Go code
run_gofmt() {
    echo -e "${BLUE}🎨 Running go fmt...${NC}"
    
    if [ "$TARGET_PATH" = "." ]; then
        # Format all Go files in project
        if go fmt ./...; then
            echo -e "${GREEN}✅ go fmt completed${NC}"
        else
            echo -e "${RED}❌ go fmt failed${NC}"
            return 1
        fi
    else
        # Format specific path
        if go fmt "$TARGET_PATH"; then
            echo -e "${GREEN}✅ go fmt completed for $TARGET_PATH${NC}"
        else
            echo -e "${RED}❌ go fmt failed for $TARGET_PATH${NC}"
            return 1
        fi
    fi
    echo ""
}

# Function to run go vet
run_govet() {
    echo -e "${BLUE}🔍 Running go vet...${NC}"
    
    if [ "$TARGET_PATH" = "." ]; then
        # Vet all packages
        if go vet ./...; then
            echo -e "${GREEN}✅ go vet passed${NC}"
        else
            echo -e "${RED}❌ go vet found issues${NC}"
            return 1
        fi
    else
        # Vet specific path
        if [[ "$TARGET_PATH" == *.go ]]; then
            # Single file - vet the package containing it
            PACKAGE_DIR=$(dirname "$TARGET_PATH")
            if go vet "$PACKAGE_DIR"; then
                echo -e "${GREEN}✅ go vet passed for $TARGET_PATH${NC}"
            else
                echo -e "${RED}❌ go vet found issues in $TARGET_PATH${NC}"
                return 1
            fi
        else
            # Directory
            if go vet "$TARGET_PATH"; then
                echo -e "${GREEN}✅ go vet passed for $TARGET_PATH${NC}"
            else
                echo -e "${RED}❌ go vet found issues in $TARGET_PATH${NC}"
                return 1
            fi
        fi
    fi
    echo ""
}

# Function to run tests
run_tests() {
    echo -e "${BLUE}🧪 Running tests...${NC}"
    
    if [ "$TARGET_PATH" = "." ]; then
        # Run all tests
        if go test ./...; then
            echo -e "${GREEN}✅ All tests passed${NC}"
        else
            echo -e "${RED}❌ Some tests failed${NC}"
            return 1
        fi
    else
        # Run tests for specific path
        if [[ "$TARGET_PATH" == *.go ]]; then
            # Single file - test the package containing it
            PACKAGE_DIR=$(dirname "$TARGET_PATH")
            if go test "$PACKAGE_DIR"; then
                echo -e "${GREEN}✅ Tests passed for $TARGET_PATH${NC}"
            else
                echo -e "${RED}❌ Tests failed for $TARGET_PATH${NC}"
                return 1
            fi
        else
            # Directory
            if go test "$TARGET_PATH"; then
                echo -e "${GREEN}✅ Tests passed for $TARGET_PATH${NC}"
            else
                echo -e "${RED}❌ Tests failed for $TARGET_PATH${NC}"
                return 1
            fi
        fi
    fi
    echo ""
}

# Function to check for common Go issues
run_additional_checks() {
    echo -e "${BLUE}🔎 Running additional checks...${NC}"
    
    # Check for goimports if available
    if command -v goimports &> /dev/null; then
        echo -e "${BLUE}📋 Running goimports...${NC}"
        if [ "$TARGET_PATH" = "." ]; then
            if find . -name "*.go" -not -path "./vendor/*" -exec goimports -w {} \;; then
                echo -e "${GREEN}✅ goimports completed${NC}"
            else
                echo -e "${YELLOW}⚠️  goimports had issues${NC}"
            fi
        else
            if [[ "$TARGET_PATH" == *.go ]]; then
                if goimports -w "$TARGET_PATH"; then
                    echo -e "${GREEN}✅ goimports completed for $TARGET_PATH${NC}"
                else
                    echo -e "${YELLOW}⚠️  goimports had issues with $TARGET_PATH${NC}"
                fi
            fi
        fi
    else
        echo -e "${YELLOW}⚠️  goimports not found (install with: go install golang.org/x/tools/cmd/goimports@latest)${NC}"
    fi
    
    # Check for golint if available
    if command -v golint &> /dev/null; then
        echo -e "${BLUE}📋 Running golint...${NC}"
        if [ "$TARGET_PATH" = "." ]; then
            golint ./...
        else
            golint "$TARGET_PATH"
        fi
    else
        echo -e "${YELLOW}⚠️  golint not found (install with: go install golang.org/x/lint/golint@latest)${NC}"
    fi
    
    echo ""
}

# Main execution
main() {
    # Check if we're in a Go project
    if [ ! -f "go.mod" ]; then
        echo -e "${RED}❌ No go.mod found. Please run this script from the root of a Go project.${NC}"
        exit 1
    fi
    
    # Check if Go files exist
    check_go_files
    
    # Track if any step fails
    FAILED=0
    
    # Run go mod tidy (only for whole project)
    if [ "$TARGET_PATH" = "." ]; then
        if ! run_mod_tidy; then
            FAILED=1
        fi
    fi
    
    # Run formatting
    if ! run_gofmt; then
        FAILED=1
    fi
    
    # Run go vet
    if ! run_govet; then
        FAILED=1
    fi
    
    # Run tests
    if ! run_tests; then
        FAILED=1
    fi
    
    # Run additional checks
    run_additional_checks
    
    # Final status
    echo -e "${BLUE}===========================================${NC}"
    if [ $FAILED -eq 0 ]; then
        echo -e "${GREEN}🎉 All checks passed successfully!${NC}"
        exit 0
    else
        echo -e "${RED}❌ Some checks failed. Please review the output above.${NC}"
        exit 1
    fi
}

# Run main function
main "$@"