# EMR Serverless Event Handler Lambda

This Lambda function handles asynchronous EMR Serverless job completion events from EventBridge and updates the FlakeRunner orchestration records in DynamoDB.

## Setup

1. **Build the Lambda binary:**
   ```bash
   GOOS=linux GOARCH=amd64 go build -o bootstrap main.go
   zip lambda-function.zip bootstrap
   ```

2. **Create Lambda function with:**
   - Runtime: `provided.al2`
   - Handler: `bootstrap`
   - Environment variables:
     - `DYNAMO_TABLE_NAME`: Name of your FlakeRunner DynamoDB table
     - `AWS_REGION`: AWS region (optional, defaults to us-east-1)

3. **Configure EventBridge Rule:**
   ```json
   {
     "source": ["aws.emr-serverless"],
     "detail-type": ["EMR Serverless Job Run State Change"],
     "detail": {
       "state": ["SUCCESS", "FAILED", "CANCELLED"]
     }
   }
   ```

## Event Flow

1. **File Processing**: FlakeRunner submits EMR Serverless job
2. **Job Execution**: EMR Serverless processes the file
3. **State Change**: EMR job completes (SUCCESS/FAILED/CANCELLED)  
4. **EventBridge**: Publishes job state change event
5. **Lambda Trigger**: This function receives the event
6. **Record Update**: Function updates DynamoDB orchestration record
7. **Next Stage**: Triggers next stage of pipeline (e.g., Snowflake loading)

## Benefits

- **Asynchronous Processing**: No need to poll job status
- **Automatic State Updates**: DynamoDB records updated immediately
- **Scalable**: EventBridge handles high-volume job completions
- **Reliable**: Built-in retry and error handling
- **Cost Effective**: Only runs when jobs complete

## IAM Permissions

Lambda execution role needs:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:Query"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/your-flake-runner-table*"
    }
  ]
}
```