package flakerunner

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/allen13/flake-runner/pkg/aws"
	"github.com/allen13/flake-runner/pkg/config"
	"github.com/allen13/flake-runner/pkg/types"
	"github.com/allen13/flake-runner/pkg/validation"
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Re-export constants from types package for convenience
const (
	StateInitiated  = types.StateInitiated
	StateValidating = types.StateValidating
	StateValidated  = types.StateValidated
	StateStaging    = types.StateStaging
	StateStaged     = types.StateStaged
	StateProcessing = types.StateProcessing
	StateProcessed  = types.StateProcessed
	StateLoading    = types.StateLoading
	StateLoaded     = types.StateLoaded
	StateCompleted  = types.StateCompleted
	StateFailed     = types.StateFailed
	StateRetrying   = types.StateRetrying
	StateSkipped    = types.StateSkipped
)

// FlakeRunner is the main orchestrator struct with all functionality
type FlakeRunner struct {
	// Configuration
	Config *config.Config

	// AWS Clients
	AWSConfig    *awssdk.Config
	S3Client     *s3.Client
	EMRClient    *emrserverless.Client
	DynamoClient *dynamodb.Client
	IAMClient    *iam.Client
	STSClient    *sts.Client

	// Processing State
	JobID              string
	InputFiles         []types.S3Object
	OutputFiles        []types.S3Object
	StagedFiles        []types.S3Object
	FileOrchestrations []types.FileOrchestrationRecord
	EMRJobRuns         map[string]string
	ProcessingStatus   types.JobStatus

	// Logging & Error Handling
	Logger     *zap.SugaredLogger
	LastError  error
	RetryCount int
}

// NewFlakeRunner creates a new FlakeRunner instance
func NewFlakeRunner(configPath string) (*FlakeRunner, error) {
	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	// Load AWS configuration
	awsConfig, err := config.LoadAWSConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	// Create AWS clients
	s3Client := s3.NewFromConfig(*awsConfig)
	emrClient := emrserverless.NewFromConfig(*awsConfig)
	dynamoClient := dynamodb.NewFromConfig(*awsConfig)
	iamClient := iam.NewFromConfig(*awsConfig)
	stsClient := sts.NewFromConfig(*awsConfig)

	// Create zap sugared logger
	var logger *zap.SugaredLogger
	if os.Getenv("LOG_ENV") == "prod" {
		zapLogger, err := zap.NewProduction()
		if err != nil {
			return nil, fmt.Errorf("failed to create zap logger: %w", err)
		}
		defer zapLogger.Sync()
		logger = zapLogger.Sugar()
	} else {
		zapLogger, err := zap.NewDevelopment()
		if err != nil {
			return nil, fmt.Errorf("failed to create zap logger: %w", err)
		}
		defer zapLogger.Sync()
		logger = zapLogger.Sugar()
	}

	// Initialize FlakeRunner
	fr := &FlakeRunner{
		Config:       cfg,
		AWSConfig:    awsConfig,
		S3Client:     s3Client,
		EMRClient:    emrClient,
		DynamoClient: dynamoClient,
		IAMClient:    iamClient,
		STSClient:    stsClient,
		JobID:        generateJobID(),
		EMRJobRuns:   make(map[string]string),
		Logger:       logger,
	}

	return fr, nil
}

// ValidateConfiguration validates the FlakeRunner configuration
func (fr *FlakeRunner) ValidateConfiguration() *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Info("Validating configuration...")

	// Validate AWS clients
	if fr.S3Client == nil {
		fr.LastError = fmt.Errorf("S3 client is nil")
		return fr
	}
	if fr.EMRClient == nil {
		fr.LastError = fmt.Errorf("EMR client is nil")
		return fr
	}
	if fr.DynamoClient == nil {
		fr.LastError = fmt.Errorf("DynamoDB client is nil")
		return fr
	}

	// Validate bucket names
	if fr.Config.InputBucketName == "" {
		fr.LastError = fmt.Errorf("input bucket name is empty")
		return fr
	}
	if fr.Config.OutputBucketName == "" {
		fr.LastError = fmt.Errorf("output bucket name is empty")
		return fr
	}
	if fr.Config.StagingBucketName == "" {
		fr.LastError = fmt.Errorf("staging bucket name is empty")
		return fr
	}

	// Validate DynamoDB table
	if fr.Config.ControlTableName == "" {
		fr.LastError = fmt.Errorf("control table name is empty")
		return fr
	}

	// Validate EMR configuration
	if fr.Config.EMRApplicationID == "" {
		fr.LastError = fmt.Errorf("EMR application ID is empty")
		return fr
	}
	if fr.Config.EMRExecutionRoleARN == "" {
		fr.LastError = fmt.Errorf("EMR execution role ARN is empty")
		return fr
	}

	// Validate prefix mappings
	if len(fr.Config.PrefixMappings) == 0 {
		fr.LastError = fmt.Errorf("no prefix mappings configured")
		return fr
	}

	for i, mapping := range fr.Config.PrefixMappings {
		if mapping.S3Prefix == "" {
			fr.LastError = fmt.Errorf("prefix mapping %d: S3 prefix is empty", i)
			return fr
		}
		if mapping.TargetName == "" {
			fr.LastError = fmt.Errorf("prefix mapping %d: target name is empty", i)
			return fr
		}
		// Container image is optional when using EMR built-in runtime
		// If empty, EMR will use the default runtime environment
	}

	fr.Logger.Info("Configuration validation completed successfully")
	return fr
}

// ValidateAWSResources validates that required AWS resources exist and are accessible
func (fr *FlakeRunner) ValidateAWSResources() *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Info("Validating AWS resources...")

	// Validate S3 buckets
	if err := aws.ValidateS3Bucket(fr.S3Client, fr.Config.InputBucketName); err != nil {
		fr.LastError = fmt.Errorf("input bucket validation failed: %w", err)
		return fr
	}
	if err := aws.ValidateS3Bucket(fr.S3Client, fr.Config.OutputBucketName); err != nil {
		fr.LastError = fmt.Errorf("output bucket validation failed: %w", err)
		return fr
	}
	if err := aws.ValidateS3Bucket(fr.S3Client, fr.Config.StagingBucketName); err != nil {
		fr.LastError = fmt.Errorf("staging bucket validation failed: %w", err)
		return fr
	}

	// Validate DynamoDB table
	if err := aws.ValidateDynamoDBTable(fr.DynamoClient, fr.Config.ControlTableName); err != nil {
		fr.LastError = fmt.Errorf("DynamoDB table validation failed: %w", err)
		return fr
	}

	// Validate EMR Serverless application
	if err := aws.ValidateEMRApplication(fr.EMRClient, fr.Config.EMRApplicationID); err != nil {
		fr.LastError = fmt.Errorf("EMR application validation failed: %w", err)
		return fr
	}

	fr.Logger.Info("AWS resources validation completed successfully")
	return fr
}

// ProcessFile processes a single file through the complete pipeline
func (fr *FlakeRunner) ProcessFile(filePath string, controlData *types.ControlData) *FlakeRunner {
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Infof("Processing file: %s", filePath)

	// Create orchestration record with control data
	fr.CreateFileOrchestrationRecord(filePath, controlData)
	if fr.LastError != nil {
		return fr
	}

	// Process input file
	fr.ProcessInputFile(filePath)
	if fr.LastError != nil {
		return fr
	}

	// Update state to validating
	fr.UpdateFileOrchestrationState(filePath, StateValidating)
	if fr.LastError != nil {
		return fr
	}

	// Validate using embedded control data
	if controlData != nil {
		fr.ValidateWithControlData(filePath, controlData)
		if fr.LastError != nil {
			fr.Logger.Infof("Control data validation failed: %v", fr.LastError)
			return fr
		}
		fr.UpdateFileOrchestrationState(filePath, StateValidated)
	} else {
		fr.Logger.Infof("No control data provided, skipping validation")
		fr.UpdateFileOrchestrationState(filePath, StateValidated)
	}

	// Determine target table
	targetTable, mapping, err := fr.DetermineTargetTable(filePath)
	if err != nil {
		fr.LastError = err
		return fr
	}

	fr.Logger.Infof("Mapped file to target table: %s → %s", filePath, targetTable)

	// Submit EMR job
	fr.SubmitSparkJob(filePath, mapping)
	if fr.LastError != nil {
		return fr
	}

	fr.Logger.Infof("File processing pipeline initiated for: %s", filePath)
	return fr
}

// Error handling methods
func (fr *FlakeRunner) GetError() error {
	return fr.LastError
}

func (fr *FlakeRunner) HasError() bool {
	return fr.LastError != nil
}

func (fr *FlakeRunner) ClearError() *FlakeRunner {
	fr.LastError = nil
	return fr
}

// GetFileOrchestrations returns the current file orchestrations for CLI access
func (fr *FlakeRunner) GetFileOrchestrations() []types.FileOrchestrationRecord {
	return fr.FileOrchestrations
}

// GetEMRJobRuns returns the current EMR job runs map for CLI access
func (fr *FlakeRunner) GetEMRJobRuns() map[string]string {
	return fr.EMRJobRuns
}

// CountRecordsInDataFile counts the number of records in a data file based on its format using streaming
func (fr *FlakeRunner) CountRecordsInDataFile(filePath string, fileFormat string) (int64, error) {
	bucket, key, err := aws.ParseS3Path(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to parse S3 path %s: %w", filePath, err)
	}

	fr.Logger.Infof("Counting records in data file: %s (format: %s)", filePath, fileFormat)

	// Get streaming reader for S3 object
	reader, err := aws.GetS3ObjectReader(fr.S3Client, bucket, key)
	if err != nil {
		return 0, fmt.Errorf("failed to get S3 object reader for %s: %w", filePath, err)
	}
	defer reader.Close()

	// Handle compression
	var dataReader io.Reader = reader
	if validation.IsCompressedFile(filePath) {
		dataReader, err = validation.CreateDecompressedReader(reader, filePath)
		if err != nil {
			return 0, fmt.Errorf("failed to create decompressed reader for %s: %w", filePath, err)
		}
		// Close gzip reader if it implements io.Closer
		if closer, ok := dataReader.(io.Closer); ok {
			defer closer.Close()
		}
	}

	// Count records based on file format
	switch strings.ToUpper(fileFormat) {
	case "CSV", "TSV":
		return validation.CountLinesInReader(dataReader, true) // Skip header for CSV
	case "JSON":
		return validation.CountJSONRecordsStreaming(dataReader)
	case "JSONL", "NDJSON":
		return validation.CountLinesInReader(dataReader, false) // Each line is a JSON record
	case "PARQUET":
		// PARQUET files require special handling - typically done during EMR processing
		fr.Logger.Infof("PARQUET record counting requires EMR processing, returning 0 for now")
		return 0, nil
	default:
		// Default to line counting for unknown formats
		fr.Logger.Infof("Unknown format %s, defaulting to line counting", fileFormat)
		return validation.CountLinesInReader(dataReader, false)
	}
}

func generateJobID() string {
	return fmt.Sprintf("job-%s", uuid.New().String()[:8])
}

// AWS Resource Management Methods

// ValidateS3Bucket validates that an S3 bucket exists and is accessible
func (fr *FlakeRunner) ValidateS3Bucket(bucketName string) error {
	return aws.ValidateS3Bucket(fr.S3Client, bucketName)
}

// ValidateDynamoDBTable validates that a DynamoDB table exists and is accessible
func (fr *FlakeRunner) ValidateDynamoDBTable(tableName string) error {
	return aws.ValidateDynamoDBTable(fr.DynamoClient, tableName)
}

// ValidateEMRApplication validates that an EMR Serverless application exists and is ready
func (fr *FlakeRunner) ValidateEMRApplication(applicationID string) error {
	return aws.ValidateEMRApplication(fr.EMRClient, applicationID)
}

// CreateS3Bucket creates an S3 bucket if it doesn't exist
func (fr *FlakeRunner) CreateS3Bucket(bucketName string) error {
	return aws.CreateS3Bucket(fr.S3Client, bucketName, fr.Config.AWSRegion)
}

// CreateDynamoDBTable creates a DynamoDB table with the standard FlakeRunner schema
func (fr *FlakeRunner) CreateDynamoDBTable(tableName string) error {
	return aws.CreateDynamoDBTable(fr.DynamoClient, tableName)
}

// CreateEMRApplication creates a new EMR Serverless application (legacy - without IAM)
func (fr *FlakeRunner) CreateEMRApplication(name, releaseLabel string) (string, error) {
	tags := map[string]string{
		"Project":     "flake-runner",
		"Environment": "production",
		"ManagedBy":   "flake-runner-cli",
	}
	return aws.CreateEMRApplication(fr.EMRClient, name, releaseLabel, tags)
}

// CreateEMRApplicationWithIAM creates a new EMR Serverless application with proper IAM role
func (fr *FlakeRunner) CreateEMRApplicationWithIAM(name, releaseLabel string) (string, string, error) {
	tags := map[string]string{
		"Project":     "flake-runner",
		"Environment": "production",
		"ManagedBy":   "flake-runner-cli",
	}

	// Collect all bucket names for IAM policy
	bucketNames := []string{
		fr.Config.InputBucketName,
		fr.Config.OutputBucketName,
		fr.Config.StagingBucketName,
	}

	return aws.CreateEMRApplicationWithIAM(fr.EMRClient, fr.IAMClient, fr.STSClient, name, releaseLabel, bucketNames, tags)
}

// CreateEMRExecutionRoleWithBuckets creates or updates an EMR execution role with specific bucket access
func (fr *FlakeRunner) CreateEMRExecutionRoleWithBuckets(roleName string, bucketNames []string) (string, error) {
	return aws.CreateEMRExecutionRole(fr.IAMClient, fr.STSClient, roleName, bucketNames)
}

// UploadToS3 uploads content to an S3 bucket
func (fr *FlakeRunner) UploadToS3(bucketName, key string, content []byte) error {
	return aws.UploadToS3(fr.S3Client, bucketName, key, content)
}

// DownloadFromS3 downloads content from an S3 bucket
func (fr *FlakeRunner) DownloadFromS3(bucketName, key string) ([]byte, error) {
	return aws.DownloadFromS3(fr.S3Client, bucketName, key)
}

func generateStageDescription(state string) string {
	switch state {
	case StateInitiated:
		return "File Processing Initiated"
	case StateValidating:
		return "Validating File and Control File"
	case StateValidated:
		return "Validation Completed Successfully"
	case StateStaging:
		return "Preparing Files for Processing"
	case StateStaged:
		return "Files Staged Successfully"
	case StateProcessing:
		return "EMR Serverless Processing"
	case StateProcessed:
		return "Processing Completed Successfully"
	case StateLoading:
		return "Loading Data to Target"
	case StateLoaded:
		return "Data Loaded Successfully"
	case StateCompleted:
		return "All Processing Completed"
	case StateFailed:
		return "Processing Failed"
	case StateRetrying:
		return "Retrying Failed Processing"
	case StateSkipped:
		return "File Processing Skipped"
	default:
		return fmt.Sprintf("State: %s", state)
	}
}
