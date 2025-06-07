#!/bin/bash
set -e

# EMR Serverless Container Build Script - Simplified Single Build
# Optimized for EMR 7.8.0 with Spark 3.5.4, Python 3.11, Java 17
# This script builds a single container with all PySpark scripts included

# Configuration
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-123456789012}"
AWS_REGION="${AWS_REGION:-us-east-1}"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
IMAGE_REPOSITORY="flake-runner"

# Version tag (use git commit hash or semantic version)
# Default version now includes PySpark for complete runtime environment
VERSION_TAG="${VERSION_TAG:-v3.1.0-with-pyspark}"

echo "🚀 Building simplified EMR Serverless PySpark container (EMR 7.8.0)..."
echo "📦 Base Image: public.ecr.aws/emr-serverless/spark/emr-7.8.0:latest"
echo "🔧 Spark Version: 3.5.4"
echo "🐍 Python Version: 3.11"
echo "☕ Java Version: 17"
echo "📍 Registry: ${ECR_REGISTRY}"
echo "📂 Repository: ${IMAGE_REPOSITORY}"
echo "🏷️  Version: ${VERSION_TAG}"
echo "📄 Build Type: Single container with all PySpark scripts"
echo ""

# Login to ECR
echo "🔐 Logging in to Amazon ECR..."
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}

# Create ECR repository if it doesn't exist
echo "📦 Ensuring ECR repository exists..."
aws ecr describe-repositories --repository-names ${IMAGE_REPOSITORY} --region ${AWS_REGION} 2>/dev/null || \
    aws ecr create-repository --repository-name ${IMAGE_REPOSITORY} --region ${AWS_REGION}

# Build unified image with all scripts
echo "🏗️  Building unified container with all PySpark scripts..."
docker build \
    --tag ${ECR_REGISTRY}/${IMAGE_REPOSITORY}:${VERSION_TAG} \
    --tag ${ECR_REGISTRY}/${IMAGE_REPOSITORY}:latest \
    -f Dockerfile .

# Push the image
echo "⬆️  Pushing image to ECR..."
docker push ${ECR_REGISTRY}/${IMAGE_REPOSITORY}:${VERSION_TAG}
docker push ${ECR_REGISTRY}/${IMAGE_REPOSITORY}:latest

echo "✅ Simplified container built and pushed successfully!"
echo ""
echo "📋 Available EMR 7.8.0 image:"
echo "  Unified: ${ECR_REGISTRY}/${IMAGE_REPOSITORY}:${VERSION_TAG}"
echo "  Latest: ${ECR_REGISTRY}/${IMAGE_REPOSITORY}:latest"
echo ""
echo "📄 Included PySpark scripts:"
echo "  • processor.py - Universal data processor for EMR Serverless"
echo "  • simple_processor.py - Local testing processor"
echo "  • csv_to_parquet_converter.py - CSV to Parquet converter with advanced features"
echo ""
echo "🔧 Runtime Environment:"
echo "  • PySpark 3.5.4 - Complete Spark runtime"
echo "  • Python 3.11 with all required dependencies"
echo "  • Ready for immediate EMR Serverless deployment"
echo ""
echo "🎯 Key Benefits:"
echo "  • Single build process - faster CI/CD"
echo "  • All processors in one container"
echo "  • Entry point can be specified at job submission time"
echo "  • Simplified deployment and maintenance"
echo ""
echo "🔧 Usage in EMR job submission:"
echo "  Entry Point: /opt/spark/jobs/processor.py (for production)"
echo "  Entry Point: /opt/spark/jobs/simple_processor.py (for local testing)"
echo "  Entry Point: /opt/spark/jobs/csv_to_parquet_converter.py (for CSV conversion)"
echo ""
echo "💡 Update your configuration files with this image URI:"
echo "  ${ECR_REGISTRY}/${IMAGE_REPOSITORY}:${VERSION_TAG}"
echo ""
echo "✅ Container now includes complete PySpark runtime for full functionality!"