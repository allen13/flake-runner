package types

import (
	"time"
)

// JobStatus represents the status of a processing job
type JobStatus string

const (
	JobPending   JobStatus = "PENDING"
	JobRunning   JobStatus = "RUNNING"
	JobCompleted JobStatus = "COMPLETED"
	JobFailed    JobStatus = "FAILED"
	JobCancelled JobStatus = "CANCELLED"
)

// File orchestration states
const (
	StateInitiated  = "INITIATED"
	StateValidating = "VALIDATING"
	StateValidated  = "VALIDATED"
	StateStaging    = "STAGING"
	StateStaged     = "STAGED"
	StateProcessing = "PROCESSING"
	StateProcessed  = "PROCESSED"
	StateLoading    = "LOADING"
	StateLoaded     = "LOADED"
	StateCompleted  = "COMPLETED"
	StateFailed     = "FAILED"
	StateRetrying   = "RETRYING"
	StateSkipped    = "SKIPPED"
)

// S3Object represents an S3 object with metadata
type S3Object struct {
	Bucket  string    `json:"bucket"`
	Key     string    `json:"key"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
	ETag    string    `json:"etag"`
}

// ControlData represents metadata provided by the caller for validation
type ControlData struct {
	FileName     string            `json:"file_name"`
	FileSize     int64             `json:"file_size"`
	FileHash     string            `json:"file_hash"`
	RecordCount  int64             `json:"record_count"`
	ColumnCount  int               `json:"column_count"`
	CreatedAt    time.Time         `json:"created_at"`
	BatchID      string            `json:"batch_id,omitempty"`
	SourceSystem string            `json:"source_system,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// FileOrchestrationRecord tracks file processing through the entire workflow
type FileOrchestrationRecord struct {
	File_path               string            `dynamodb:"file_path"`
	Job_id                  string            `dynamodb:"job_id"`
	File_name               string            `dynamodb:"file_name"`
	File_size               int64             `dynamodb:"file_size"`
	File_hash               string            `dynamodb:"file_hash"`
	S3_prefix               string            `dynamodb:"s3_prefix"`
	Target_table            string            `dynamodb:"target_table"`
	Target_schema           string            `dynamodb:"target_schema"`
	Processing_script       string            `dynamodb:"processing_script"`
	Orchestration_state     string            `dynamodb:"orchestration_state"`
	State_history           []StateTransition `dynamodb:"state_history"`
	Current_stage           string            `dynamodb:"current_stage"`
	Processing_initiated_at *time.Time        `dynamodb:"processing_initiated_at"`
	Validated_at            *time.Time        `dynamodb:"validated_at"`
	Processing_started      *time.Time        `dynamodb:"processing_started"`
	Processing_completed    *time.Time        `dynamodb:"processing_completed"`
	Loaded_at               *time.Time        `dynamodb:"loaded_at"`
	Validation_results      ValidationResults `dynamodb:"validation_results"`
	Emr_job_details         EMRJobDetails     `dynamodb:"emr_job_details"`
	Load_results            LoadResults       `dynamodb:"load_results"`
	Error_history           []ProcessingError `dynamodb:"error_history"`
	Retry_count             int               `dynamodb:"retry_count"`
	Batch_id                string            `dynamodb:"batch_id"`
	Source_system           string            `dynamodb:"source_system"`
	Expires_at              int64             `dynamodb:"expires_at"`
	Version                 int               `dynamodb:"version"`
	// Control data embedded directly in the orchestration record
	Control_data *ControlData `dynamodb:"control_data"`
}

// StateTransition records state changes with metadata
type StateTransition struct {
	From_state  string
	To_state    string
	Timestamp   time.Time
	Duration_ms int64
	Metadata    map[string]interface{}
}

// ValidationResults contains validation check results
type ValidationResults struct {
	RecordCountMatch bool     `dynamodb:"record_count_match"`
	FileSizeMatch    bool     `dynamodb:"file_size_match"`
	ChecksumMatch    bool     `dynamodb:"checksum_match"`
	ExpectedRecords  int64    `dynamodb:"expected_records"`
	ActualRecords    int64    `dynamodb:"actual_records"`
	ValidationErrors []string `dynamodb:"validation_errors,omitempty"`
}

// EMRJobDetails contains EMR Serverless job information
type EMRJobDetails struct {
	ApplicationID    string     `dynamodb:"application_id"`
	JobRunID         string     `dynamodb:"job_run_id"`
	StartTime        time.Time  `dynamodb:"start_time"`
	EndTime          *time.Time `dynamodb:"end_time,omitempty"`
	Status           string     `dynamodb:"status"`
	ProcessedRecords int64      `dynamodb:"processed_records"`
}

// LoadResults contains data loading results
type LoadResults struct {
	TargetName      string `dynamodb:"target_name"`
	LoadedRecords   int64  `dynamodb:"loaded_records"`
	RejectedRecords int64  `dynamodb:"rejected_records"`
	LoadDuration    int64  `dynamodb:"load_duration_ms"`
	LoadID          string `dynamodb:"load_id"`
}

// ProcessingError represents an error during processing
type ProcessingError struct {
	Stage        string    `dynamodb:"stage"`
	ErrorType    string    `dynamodb:"error_type"`
	ErrorMessage string    `dynamodb:"error_message"`
	Timestamp    time.Time `dynamodb:"timestamp"`
	Retryable    bool      `dynamodb:"retryable"`
}

// EMRJobEventData represents the 'detail' section of an EMR Serverless EventBridge event
type EMRJobEventData struct {
	JobRunId               string  `json:"jobRunId"`
	ApplicationId          string  `json:"applicationId"`
	State                  string  `json:"state"`
	StateDetails           *string `json:"stateDetails,omitempty"`
	CreatedAt              string  `json:"createdAt"`
	UpdatedAt              string  `json:"updatedAt"`
	Name                   *string `json:"name,omitempty"`
	ExecutionRole          *string `json:"executionRole,omitempty"`
	JobDriver              *string `json:"jobDriver,omitempty"`
	ConfigurationOverrides *string `json:"configurationOverrides,omitempty"`
}

// EMRServerlessEvent represents the complete EventBridge event structure
type EMRServerlessEvent struct {
	Version    string          `json:"version"`
	ID         string          `json:"id"`
	DetailType string          `json:"detail-type"`
	Source     string          `json:"source"`
	Account    string          `json:"account"`
	Time       string          `json:"time"`
	Region     string          `json:"region"`
	Detail     EMRJobEventData `json:"detail"`
}

// Legacy ControlFileRecord type for backward compatibility with validation methods
type ControlFileRecord struct {
	FilePath         string     `json:"file_path"`
	ControlFilePath  string     `json:"control_file_path"`
	JobID            string     `json:"job_id"`
	FileName         string     `json:"file_name"`
	FileSize         int64      `json:"file_size"`
	FileHash         string     `json:"file_hash"`
	RecordCount      int64      `json:"record_count"`
	ColumnCount      int        `json:"column_count"`
	CreatedAt        time.Time  `json:"created_at"`
	ProcessedAt      *time.Time `json:"processed_at,omitempty"`
	ValidatedAt      *time.Time `json:"validated_at,omitempty"`
	ValidationStatus string     `json:"validation_status"`
	ProcessingStatus string     `json:"processing_status"`
	ErrorMessage     string     `json:"error_message,omitempty"`
	ExpiresAt        int64      `json:"expires_at"`
}
