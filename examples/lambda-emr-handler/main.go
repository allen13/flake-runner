// Lambda function example for handling EMR Serverless EventBridge events
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/allen13/flake-runner/pkg/flakerunner"
	"github.com/allen13/flake-runner/pkg/types"
)

// LambdaHandler handles EMR Serverless EventBridge events
func LambdaHandler(ctx context.Context, event events.CloudWatchEvent) error {
	log.Printf("Received EventBridge event: %s", event.DetailType)

	// Parse the EMR event data
	var emrEvent types.EMRServerlessEvent
	if err := json.Unmarshal(event.Detail, &emrEvent.Detail); err != nil {
		return fmt.Errorf("failed to unmarshal EMR event detail: %w", err)
	}

	// Get configuration from environment variables
	dynamoTableName := os.Getenv("DYNAMO_TABLE_NAME")
	if dynamoTableName == "" {
		return fmt.Errorf("DYNAMO_TABLE_NAME environment variable is required")
	}

	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = "us-east-1" // default
	}

	log.Printf("Processing EMR job event: JobRunId=%s, State=%s",
		emrEvent.Detail.JobRunId, emrEvent.Detail.State)

	// Handle the EMR job event using FlakeRunner's event handler
	err := flakerunner.HandleEMRJobEvent(dynamoTableName, awsRegion, emrEvent.Detail)
	if err != nil {
		log.Printf("Error handling EMR job event: %v", err)
		return err
	}

	log.Printf("Successfully processed EMR job event for JobRunId: %s", emrEvent.Detail.JobRunId)
	return nil
}

func main() {
	lambda.Start(LambdaHandler)
}
