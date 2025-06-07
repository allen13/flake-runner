package flakerunner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/allen13/flake-runner/pkg/aws"
	"github.com/allen13/flake-runner/pkg/types"
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamoTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	emrTypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
)

// GetJobLogs retrieves logs for an EMR Serverless job
func (fr *FlakeRunner) GetJobLogs(filePath string) (string, error) {
	jobRunID, exists := fr.EMRJobRuns[filePath]
	if !exists {
		return "", fmt.Errorf("no EMR job run ID found for file: %s", filePath)
	}

	fr.Logger.Infof("Retrieving logs for EMR job: %s", jobRunID)

	// Get job run details
	jobRun, err := aws.GetJobStatus(fr.EMRClient, fr.Config.EMRApplicationID, jobRunID)
	if err != nil {
		return "", fmt.Errorf("failed to get job run details: %w", err)
	}

	// Extract log information
	var logs strings.Builder
	logs.WriteString(fmt.Sprintf("Job Run ID: %s\n", jobRunID))
	logs.WriteString(fmt.Sprintf("State: %s\n", jobRun.State))

	if jobRun.StateDetails != nil {
		logs.WriteString(fmt.Sprintf("State Details: %s\n", *jobRun.StateDetails))
	}

	if jobRun.CreatedAt != nil {
		logs.WriteString(fmt.Sprintf("Created At: %s\n", jobRun.CreatedAt.Format(time.RFC3339)))
	}

	if jobRun.UpdatedAt != nil {
		logs.WriteString(fmt.Sprintf("Updated At: %s\n", jobRun.UpdatedAt.Format(time.RFC3339)))
	}

	logs.WriteString("\nNote: Full application logs available in CloudWatch Logs\n")
	return logs.String(), nil
}

// CancelJob cancels a running EMR Serverless job
func (fr *FlakeRunner) CancelJob(filePath string) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	jobRunID, exists := fr.EMRJobRuns[filePath]
	if !exists {
		fr.LastError = fmt.Errorf("no EMR job run ID found for file: %s", filePath)
		return fr
	}

	fr.Logger.Infof("Cancelling EMR job: %s for file: %s", jobRunID, filePath)

	err := aws.CancelJob(fr.EMRClient, fr.Config.EMRApplicationID, jobRunID)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to cancel job run %s: %w", jobRunID, err)
		return fr
	}

	fr.Logger.Infof("EMR job cancellation requested for: %s", jobRunID)

	// Update orchestration state
	fr.UpdateFileOrchestrationState(filePath, StateFailed)
	fr.RecordProcessingError(filePath, "emr_processing", "JOB_CANCELLED", "Job was cancelled by user request", false)

	return fr
}

// GetJobStatus retrieves the current status of an EMR job
func (fr *FlakeRunner) GetJobStatus(jobRunID string) (*emrTypes.JobRun, error) {
	return aws.GetJobStatus(fr.EMRClient, fr.Config.EMRApplicationID, jobRunID)
}

// MonitorJobProgress monitors EMR job progress (for synchronous workflows)
func (fr *FlakeRunner) MonitorJobProgress(filePath string, timeout time.Duration) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	jobRunID, exists := fr.EMRJobRuns[filePath]
	if !exists {
		fr.LastError = fmt.Errorf("no EMR job run ID found for file: %s", filePath)
		return fr
	}

	fr.Logger.Infof("Monitoring EMR job progress: %s", jobRunID)

	deadline := time.Now().Add(timeout)
	checkInterval := time.Minute * 2

	for time.Now().Before(deadline) {
		jobRun, err := fr.GetJobStatus(jobRunID)
		if err != nil {
			fr.Logger.Infof("Error checking job status: %v", err)
			time.Sleep(checkInterval)
			continue
		}

		fr.Logger.Infof("Job %s status: %s", jobRunID, jobRun.State)

		switch jobRun.State {
		case emrTypes.JobRunStateSuccess:
			fr.Logger.Infof("Job completed successfully: %s", jobRunID)
			fr.UpdateFileOrchestrationState(filePath, StateProcessed)
			return fr

		case emrTypes.JobRunStateFailed, emrTypes.JobRunStateCancelled:
			errorMsg := fmt.Sprintf("Job failed with state: %s", jobRun.State)
			if jobRun.StateDetails != nil {
				errorMsg += fmt.Sprintf(", details: %s", *jobRun.StateDetails)
			}
			fr.RecordProcessingError(filePath, "emr_processing", "JOB_FAILED", errorMsg, true)
			fr.LastError = fmt.Errorf("EMR job failed: %s", errorMsg)
			return fr

		case emrTypes.JobRunStateSubmitted, emrTypes.JobRunStatePending, emrTypes.JobRunStateScheduled, emrTypes.JobRunStateRunning:
			// Job still in progress, continue monitoring
			time.Sleep(checkInterval)

		default:
			fr.Logger.Infof("Unknown job state: %s", jobRun.State)
			time.Sleep(checkInterval)
		}
	}

	// Timeout reached
	fr.LastError = fmt.Errorf("job monitoring timed out for %s after %v", jobRunID, timeout)
	return fr
}

// UpdateValidationResultsFromEMR updates validation results based on EMR processing output
// This should be called after EMR job completion with actual validation results
func (fr *FlakeRunner) UpdateValidationResultsFromEMR(filePath string, actualRecordCount int64, validationErrors []string) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	// Get current record
	record, err := aws.GetFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, filePath, fr.JobID)
	if err != nil {
		fr.Logger.Infof("Warning: Could not get orchestration record for validation update: %v", err)
		return fr
	}

	// Update validation results with EMR findings
	record.Validation_results.ActualRecords = actualRecordCount

	// Update record count validation
	if record.Validation_results.ExpectedRecords > 0 {
		if actualRecordCount == record.Validation_results.ExpectedRecords {
			record.Validation_results.RecordCountMatch = true
		} else {
			record.Validation_results.RecordCountMatch = false
			validationErrors = append(validationErrors,
				fmt.Sprintf("Record count mismatch: expected %d, got %d",
					record.Validation_results.ExpectedRecords, actualRecordCount))
		}
	}

	// Add any additional validation errors from EMR processing
	if len(validationErrors) > 0 {
		record.Validation_results.ValidationErrors = append(record.Validation_results.ValidationErrors, validationErrors...)
	}

	record.Version++

	// Store updated record
	err = aws.PutFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, record)
	if err != nil {
		fr.Logger.Infof("Warning: Could not store updated validation results: %v", err)
		return fr
	}

	// Update local context
	fr.updateFileOrchestrationInContext(record)

	fr.Logger.Infof("Updated validation results from EMR processing for: %s (actual records: %d)", filePath, actualRecordCount)
	return fr
}

// HandleEMRJobEvent processes EMR job completion events asynchronously
// This function is designed to be called from a Lambda function or other event handler
func HandleEMRJobEvent(dynamoTableName, awsRegion string, eventData types.EMRJobEventData) error {
	// Create minimal AWS clients for DynamoDB operations
	dynamoClient, err := createMinimalDynamoClient(awsRegion)
	if err != nil {
		return fmt.Errorf("failed to create DynamoDB client: %w", err)
	}

	// Find the file orchestration record by EMR job run ID
	filePath, err := findFilePathByJobRunID(dynamoClient, dynamoTableName, eventData.JobRunId)
	if err != nil {
		return fmt.Errorf("failed to find file path for job run ID %s: %w", eventData.JobRunId, err)
	}

	if filePath == "" {
		return fmt.Errorf("no file path found for EMR job run ID: %s", eventData.JobRunId)
	}

	// Process the job state change
	switch emrTypes.JobRunState(eventData.State) {
	case emrTypes.JobRunStateSuccess:
		err = handleJobSuccess(dynamoClient, dynamoTableName, filePath, eventData)
		if err != nil {
			return fmt.Errorf("failed to handle job success: %w", err)
		}

	case emrTypes.JobRunStateFailed, emrTypes.JobRunStateCancelled:
		err = handleJobFailure(dynamoClient, dynamoTableName, filePath, eventData)
		if err != nil {
			return fmt.Errorf("failed to handle job failure: %w", err)
		}

	default:
		// Log intermediate states but don't take action
		fmt.Printf("Received intermediate job state: %s for job %s, no action taken", eventData.State, eventData.JobRunId)
	}

	return nil
}

// Helper functions for EMR event handling

// createMinimalDynamoClient creates a minimal DynamoDB client for event handling
func createMinimalDynamoClient(region string) (*dynamodb.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	return dynamodb.NewFromConfig(cfg), nil
}

// findFilePathByJobRunID finds the file path associated with an EMR job run ID
func findFilePathByJobRunID(client *dynamodb.Client, tableName, jobRunID string) (string, error) {
	// Query using JobOrchestrationIndex (GSI 3) to find records by EMR job run ID
	// Note: This assumes the job run ID is stored in the job_id field of the orchestration record
	input := &dynamodb.QueryInput{
		TableName:              awssdk.String(tableName),
		IndexName:              awssdk.String("JobOrchestrationIndex"),
		KeyConditionExpression: awssdk.String("job_id = :jobRunID"),
		ExpressionAttributeValues: map[string]dynamoTypes.AttributeValue{
			":jobRunID": &dynamoTypes.AttributeValueMemberS{Value: jobRunID},
		},
		ProjectionExpression: awssdk.String("file_path"),
		Limit:                awssdk.Int32(1),
	}

	result, err := client.Query(context.TODO(), input)
	if err != nil {
		return "", fmt.Errorf("failed to query DynamoDB: %w", err)
	}

	if len(result.Items) == 0 {
		return "", nil // No record found
	}

	// Extract file path from the first item
	item := result.Items[0]
	if filePathAttr, exists := item["file_path"]; exists {
		if s, ok := filePathAttr.(*dynamoTypes.AttributeValueMemberS); ok {
			return s.Value, nil
		}
	}

	return "", fmt.Errorf("file_path not found in DynamoDB record")
}

// handleJobSuccess processes successful EMR job completion
func handleJobSuccess(client *dynamodb.Client, tableName, filePath string, eventData types.EMRJobEventData) error {
	now := time.Now()

	// Update orchestration state to PROCESSED
	updateInput := &dynamodb.UpdateItemInput{
		TableName: awssdk.String(tableName),
		Key: map[string]dynamoTypes.AttributeValue{
			"file_path": &dynamoTypes.AttributeValueMemberS{Value: filePath},
			"job_id":    &dynamoTypes.AttributeValueMemberS{Value: eventData.JobRunId},
		},
		UpdateExpression: awssdk.String("SET orchestration_state = :state, processing_completed = :completed, #emr.#status = :emrStatus, #emr.end_time = :endTime"),
		ExpressionAttributeNames: map[string]string{
			"#emr":    "emr_job_details",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]dynamoTypes.AttributeValue{
			":state":     &dynamoTypes.AttributeValueMemberS{Value: StateProcessed},
			":completed": &dynamoTypes.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
			":emrStatus": &dynamoTypes.AttributeValueMemberS{Value: eventData.State},
			":endTime":   &dynamoTypes.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
		},
	}

	_, err := client.UpdateItem(context.TODO(), updateInput)
	if err != nil {
		return fmt.Errorf("failed to update orchestration record: %w", err)
	}

	fmt.Printf("Successfully updated orchestration record for file %s to PROCESSED state", filePath)
	return nil
}

// handleJobFailure processes failed EMR job completion
func handleJobFailure(client *dynamodb.Client, tableName, filePath string, eventData types.EMRJobEventData) error {
	now := time.Now()

	// Prepare error message
	errorMsg := fmt.Sprintf("EMR job failed with state: %s", eventData.State)
	if eventData.StateDetails != nil {
		errorMsg += fmt.Sprintf(", details: %s", *eventData.StateDetails)
	}

	// Update orchestration state to FAILED
	updateInput := &dynamodb.UpdateItemInput{
		TableName: awssdk.String(tableName),
		Key: map[string]dynamoTypes.AttributeValue{
			"file_path": &dynamoTypes.AttributeValueMemberS{Value: filePath},
			"job_id":    &dynamoTypes.AttributeValueMemberS{Value: eventData.JobRunId},
		},
		UpdateExpression: awssdk.String("SET orchestration_state = :state, #emr.#status = :emrStatus, #emr.end_time = :endTime ADD error_history :error"),
		ExpressionAttributeNames: map[string]string{
			"#emr":    "emr_job_details",
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]dynamoTypes.AttributeValue{
			":state":     &dynamoTypes.AttributeValueMemberS{Value: StateFailed},
			":emrStatus": &dynamoTypes.AttributeValueMemberS{Value: eventData.State},
			":endTime":   &dynamoTypes.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
			":error": &dynamoTypes.AttributeValueMemberL{
				Value: []dynamoTypes.AttributeValue{
					&dynamoTypes.AttributeValueMemberM{
						Value: map[string]dynamoTypes.AttributeValue{
							"stage":         &dynamoTypes.AttributeValueMemberS{Value: "emr_processing"},
							"error_type":    &dynamoTypes.AttributeValueMemberS{Value: "JOB_FAILED"},
							"error_message": &dynamoTypes.AttributeValueMemberS{Value: errorMsg},
							"timestamp":     &dynamoTypes.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
							"retryable":     &dynamoTypes.AttributeValueMemberBOOL{Value: true},
						},
					},
				},
			},
		},
	}

	_, err := client.UpdateItem(context.TODO(), updateInput)
	if err != nil {
		return fmt.Errorf("failed to update orchestration record: %w", err)
	}

	fmt.Printf("Successfully updated orchestration record for file %s to FAILED state: %s", filePath, errorMsg)
	return nil
}
