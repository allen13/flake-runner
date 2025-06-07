package flakerunner

import (
	"fmt"
	"strings"
	"time"

	"github.com/allen13/flake-runner/pkg/aws"
	"github.com/allen13/flake-runner/pkg/config"
	"github.com/allen13/flake-runner/pkg/types"
	"github.com/allen13/flake-runner/pkg/validation"
	"github.com/google/uuid"
)

// CreateFileOrchestrationRecord creates a new file orchestration record
func (fr *FlakeRunner) CreateFileOrchestrationRecord(filePath string, controlData *types.ControlData) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Infof("Creating file orchestration record for: %s", filePath)

	// Determine target table and mapping
	_, mapping, err := fr.DetermineTargetTable(filePath)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to determine target table for %s: %w", filePath, err)
		return fr
	}

	// Extract S3 prefix
	prefix := extractS3Prefix(filePath)

	// Create the record
	now := time.Now()
	fileName := extractFileName(filePath)
	batchID := generateBatchID()
	sourceSystem := "flake-runner"

	// Override with control data if provided
	if controlData != nil {
		if controlData.FileName != "" {
			fileName = controlData.FileName
		}
		if controlData.BatchID != "" {
			batchID = controlData.BatchID
		}
		if controlData.SourceSystem != "" {
			sourceSystem = controlData.SourceSystem
		}
	}

	record := types.FileOrchestrationRecord{
		File_path:               filePath,
		Job_id:                  fr.JobID,
		File_name:               fileName,
		S3_prefix:               prefix,
		Target_table:            mapping.TargetName,
		Target_schema:           "",
		Processing_script:       mapping.ContainerImage,
		Orchestration_state:     StateInitiated,
		State_history:           []types.StateTransition{},
		Current_stage:           "File Processing Initiated",
		Processing_initiated_at: &now,
		Validation_results:      types.ValidationResults{},
		Emr_job_details:         types.EMRJobDetails{},
		Load_results:            types.LoadResults{},
		Error_history:           []types.ProcessingError{},
		Retry_count:             0,
		Batch_id:                batchID,
		Source_system:           sourceSystem,
		Expires_at:              now.AddDate(0, 0, fr.Config.ControlTTLDays).Unix(),
		Version:                 1,
		Control_data:            controlData,
	}

	// Set file size and hash from control data if available
	if controlData != nil {
		record.File_size = controlData.FileSize
		record.File_hash = controlData.FileHash
	}

	// Add initial state transition
	initialTransition := types.StateTransition{
		From_state:  "",
		To_state:    StateInitiated,
		Timestamp:   now,
		Duration_ms: 0,
		Metadata:    map[string]interface{}{"job_id": fr.JobID},
	}
	record.State_history = append(record.State_history, initialTransition)

	// Store in DynamoDB
	err = aws.PutFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, &record)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to store file orchestration record: %w", err)
		return fr
	}

	// Add to context
	fr.FileOrchestrations = append(fr.FileOrchestrations, record)

	fr.Logger.Infof("Successfully created file orchestration record for: %s", filePath)
	return fr
}

// ProcessInputFile processes a single S3 input file
func (fr *FlakeRunner) ProcessInputFile(s3Path string) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Infof("Processing input file: %s", s3Path)

	// Parse S3 path
	bucket, key, err := aws.ParseS3Path(s3Path)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to parse S3 path %s: %w", s3Path, err)
		return fr
	}

	// Validate file exists
	s3Object, err := aws.GetS3ObjectMetadata(fr.S3Client, bucket, key)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to get metadata for %s: %w", s3Path, err)
		return fr
	}

	// Add to input files
	fr.InputFiles = append(fr.InputFiles, *s3Object)

	fr.Logger.Infof("Successfully processed input file: %s (size: %d bytes)", s3Path, s3Object.Size)
	return fr
}

// ValidateWithControlData validates a data file using provided control data
func (fr *FlakeRunner) ValidateWithControlData(filePath string, controlData *types.ControlData) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Infof("Validating file with control data: %s", filePath)

	// Get validation rules for this file
	_, mapping, err := fr.DetermineTargetTable(filePath)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to determine validation rules for %s: %w", filePath, err)
		return fr
	}

	// Validate control data content
	if err := validation.ValidateControlDataContent(controlData); err != nil {
		fr.LastError = fmt.Errorf("control data validation failed for %s: %w", filePath, err)
		return fr
	}

	// Get data file metadata for comparison
	bucket, key, err := aws.ParseS3Path(filePath)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to parse data file path %s: %w", filePath, err)
		return fr
	}

	dataFile, err := aws.GetS3ObjectMetadata(fr.S3Client, bucket, key)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to get data file metadata %s: %w", filePath, err)
		return fr
	}

	// Convert ControlData to legacy format for validation compatibility
	controlRecord := validation.ConvertControlDataToRecord(controlData, filePath, fr.JobID, fr.Config.ControlTTLDays)

	// Perform comprehensive validation using control data and validation rules
	validationResults, err := validation.PerformComprehensiveValidation(filePath, controlRecord, dataFile, &mapping.ValidationRules)
	if err != nil {
		fr.LastError = fmt.Errorf("comprehensive validation failed for %s: %w", filePath, err)
		return fr
	}

	// Check if validation passed
	if !validation.IsValidationSuccessful(validationResults) {
		fr.LastError = fmt.Errorf("data file validation failed for %s: %v", filePath, validationResults.ValidationErrors)
		return fr
	}

	// Update file orchestration record with validation results
	fr.updateFileOrchestrationValidationResults(filePath, validationResults)

	fr.Logger.Infof("Control data validation successful for: %s", filePath)
	return fr
}

// DetermineTargetTable determines the target table based on S3 prefix
func (fr *FlakeRunner) DetermineTargetTable(filePath string) (string, *config.PrefixMapping, error) {
	prefix := extractS3Prefix(filePath)

	for _, mapping := range fr.Config.PrefixMappings {
		if mapping.S3Prefix == prefix {
			return mapping.TargetName, &mapping, nil
		}
	}

	return "", nil, fmt.Errorf("no prefix mapping found for prefix: %s", prefix)
}

// SubmitSparkJob submits a PySpark job to EMR Serverless
func (fr *FlakeRunner) SubmitSparkJob(filePath string, mapping *config.PrefixMapping) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Infof("Submitting Spark job for file: %s", filePath)

	// Update orchestration state to PROCESSING
	fr.UpdateFileOrchestrationState(filePath, StateProcessing)
	if fr.LastError != nil {
		return fr
	}

	// Get control data for job parameters
	var controlDataMap map[string]interface{}
	if record, err := aws.GetFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, filePath, fr.JobID); err == nil && record.Control_data != nil {
		controlData := record.Control_data
		controlDataMap = map[string]interface{}{
			"record_count": controlData.RecordCount,
			"file_size":    controlData.FileSize,
			"file_hash":    controlData.FileHash,
		}
	}

	// Construct job parameters
	jobParams, err := aws.ConstructJobParameters(fr.Config, filePath, mapping, fr.JobID, controlDataMap)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to construct job parameters: %w", err)
		return fr
	}

	// Submit job to EMR Serverless
	jobRunID, err := aws.SubmitEMRJob(fr.EMRClient, jobParams)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to submit EMR job: %w", err)
		fr.RecordProcessingError(filePath, "emr_submission", "SUBMISSION_FAILED", err.Error(), true)
		return fr
	}

	fr.Logger.Infof("EMR job submitted successfully. Job Run ID: %s", jobRunID)

	// Store job run ID for monitoring
	fr.EMRJobRuns[filePath] = jobRunID

	// Record job submission in DynamoDB
	metadata := map[string]interface{}{
		"emr_job_run_id": jobRunID,
		"target_name":    mapping.TargetName,
	}
	fr.RecordStateTransition(filePath, StateInitiated, StateProcessing, metadata)

	return fr
}

// UpdateFileOrchestrationState updates the orchestration state of a file
func (fr *FlakeRunner) UpdateFileOrchestrationState(filePath, newState string) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Infof("Updating orchestration state for %s to %s", filePath, newState)

	// Get current record
	record, err := aws.GetFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, filePath, fr.JobID)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to get file orchestration record: %w", err)
		return fr
	}

	// Update state and add transition
	fr.RecordStateTransition(filePath, record.Orchestration_state, newState, nil)
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Infof("Successfully updated orchestration state for %s to %s", filePath, newState)
	return fr
}

// RecordStateTransition records a state transition with metadata
func (fr *FlakeRunner) RecordStateTransition(filePath, fromState, toState string, metadata map[string]interface{}) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Infof("Recording state transition for %s: %s -> %s", filePath, fromState, toState)

	// Get current record
	record, err := aws.GetFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, filePath, fr.JobID)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to get file orchestration record: %w", err)
		return fr
	}

	// Calculate duration from last transition
	now := time.Now()
	var duration int64 = 0
	if len(record.State_history) > 0 {
		lastTransition := record.State_history[len(record.State_history)-1]
		duration = now.Sub(lastTransition.Timestamp).Milliseconds()
	}

	// Create state transition
	transition := types.StateTransition{
		From_state:  fromState,
		To_state:    toState,
		Timestamp:   now,
		Duration_ms: duration,
		Metadata:    metadata,
	}

	// Update record
	record.Orchestration_state = toState
	record.State_history = append(record.State_history, transition)
	record.Current_stage = generateStageDescription(toState)
	record.Version++

	// Update specific timestamps based on state
	switch toState {
	case StateValidated:
		record.Validated_at = &now
	case StateProcessing:
		record.Processing_started = &now
	case StateProcessed:
		record.Processing_completed = &now
	case StateLoaded:
		record.Loaded_at = &now
	}

	// Store updated record
	err = aws.PutFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, record)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to update file orchestration record: %w", err)
		return fr
	}

	// Update local context
	fr.updateFileOrchestrationInContext(record)

	fr.Logger.Infof("Successfully recorded state transition for %s: %s -> %s (duration: %dms)",
		filePath, fromState, toState, duration)
	return fr
}

// RecordProcessingError records a processing error
func (fr *FlakeRunner) RecordProcessingError(filePath, stage, errorType, errorMessage string, retryable bool) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Infof("Recording processing error for %s at stage %s: %s", filePath, stage, errorMessage)

	// Get current record
	record, err := aws.GetFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, filePath, fr.JobID)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to get file orchestration record for error recording: %w", err)
		return fr
	}

	// Create error record
	processingError := types.ProcessingError{
		Stage:        stage,
		ErrorType:    errorType,
		ErrorMessage: errorMessage,
		Timestamp:    time.Now(),
		Retryable:    retryable,
	}

	// Add to error history
	record.Error_history = append(record.Error_history, processingError)
	record.Version++

	// Update state to failed if not retryable or max retries exceeded
	if !retryable || record.Retry_count >= fr.Config.MaxRetries {
		record.Orchestration_state = StateFailed
		record.Current_stage = "Processing Failed"
	}

	// Store updated record
	err = aws.PutFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, record)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to update file orchestration record with error: %w", err)
		return fr
	}

	// Update local context
	fr.updateFileOrchestrationInContext(record)

	fr.Logger.Infof("Successfully recorded processing error for: %s", filePath)
	return fr
}

// RefreshOrchestrationRecord refreshes a specific orchestration record from DynamoDB
func (fr *FlakeRunner) RefreshOrchestrationRecord(filePath string) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	record, err := aws.GetFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, filePath, fr.JobID)
	if err != nil {
		fr.LastError = fmt.Errorf("failed to refresh orchestration record: %w", err)
		return fr
	}

	// Update local context
	fr.updateFileOrchestrationInContext(record)
	return fr
}

// Helper methods

func (fr *FlakeRunner) updateFileOrchestrationInContext(updatedRecord *types.FileOrchestrationRecord) {
	for i, record := range fr.FileOrchestrations {
		if record.File_path == updatedRecord.File_path && record.Job_id == updatedRecord.Job_id {
			fr.FileOrchestrations[i] = *updatedRecord
			return
		}
	}
	fr.FileOrchestrations = append(fr.FileOrchestrations, *updatedRecord)
}

func (fr *FlakeRunner) updateFileOrchestrationValidationResults(filePath string, results *types.ValidationResults) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	// Get current record
	record, err := aws.GetFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, filePath, fr.JobID)
	if err != nil {
		fr.Logger.Infof("Warning: Could not update orchestration record with validation results: %v", err)
		return fr // Don't fail the whole process for this
	}

	// Update validation results
	record.Validation_results = *results
	record.Version++

	// Store updated record
	err = aws.PutFileOrchestrationRecord(fr.DynamoClient, fr.Config.ControlTableName, record)
	if err != nil {
		fr.Logger.Infof("Warning: Could not store updated orchestration record: %v", err)
		return fr // Don't fail the whole process for this
	}

	// Update local context
	fr.updateFileOrchestrationInContext(record)

	fr.Logger.Infof("Updated orchestration record with validation results for: %s", filePath)
	return fr
}

func extractS3Prefix(filePath string) string {
	path := strings.TrimPrefix(filePath, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return ""
	}

	key := parts[1]
	lastSlash := strings.LastIndex(key, "/")
	if lastSlash == -1 {
		return ""
	}

	return key[:lastSlash+1]
}

func extractFileName(s3Path string) string {
	_, key, err := aws.ParseS3Path(s3Path)
	if err != nil {
		return ""
	}

	parts := strings.Split(key, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}

	return key
}

func generateBatchID() string {
	now := time.Now()
	return fmt.Sprintf("batch_%s_%s",
		now.Format("20060102"),
		uuid.New().String()[:8])
}
