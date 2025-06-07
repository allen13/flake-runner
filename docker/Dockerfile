# Simplified single-build EMR Serverless PySpark container
# Base image: EMR 7.8.0 with Spark 3.5.4, Python 3.11, Java 17
FROM public.ecr.aws/emr-serverless/spark/emr-7.8.0:latest

USER root

# Install PySpark and additional Python packages optimized for Python 3.11 and Spark 3.5.4
# Note: snowflake-spark-connector is provided via JAR files in EMR, not pip
RUN pip install --no-cache-dir \
    pyspark==3.5.4 \
    snowflake-connector-python==3.7.1 \
    boto3==1.35.36 \
    pandas==2.2.3 \
    pyarrow==17.0.0 \
    requests==2.32.3 \
    python-dateutil==2.9.0.post0 \
    numpy==1.26.4 \
    pydantic==2.9.2 \
    typing-extensions==4.12.2

# Create directory for our PySpark scripts
RUN mkdir -p /opt/spark/jobs

# Copy all PySpark scripts to the container
COPY --chown=hadoop:hadoop docker/src/ /opt/spark/jobs/

# Make all Python scripts executable
RUN find /opt/spark/jobs -name "*.py" -exec chmod +x {} \;

# Set working directory
WORKDIR /opt/spark/jobs

# Switch back to non-root user for security
USER hadoop

# Default entry point - can be overridden in job submission
ENTRYPOINT ["python3", "/opt/spark/jobs/processor.py"]