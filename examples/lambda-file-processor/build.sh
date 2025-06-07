#!/bin/bash

# Build script for FlakeRunner File Processor Lambda
set -e

echo "🏗️  Building FlakeRunner File Processor Lambda..."

# Clean previous builds
rm -f bootstrap lambda-function.zip

# Build for Lambda runtime (Amazon Linux 2)
echo "📦 Compiling Go binary for Lambda runtime..."
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o bootstrap main.go

# Create deployment package
echo "📦 Creating deployment package..."
zip lambda-function.zip bootstrap

# Optional: Add config file if it exists
if [ -f "flake-runner-config.json" ]; then
    echo "📄 Adding configuration file..."
    zip -u lambda-function.zip flake-runner-config.json
fi

echo "✅ Build complete!"
echo "📦 Deployment package: lambda-function.zip"
echo "📏 Package size: $(du -h lambda-function.zip | cut -f1)"

# Show zip contents
echo "📋 Package contents:"
unzip -l lambda-function.zip