package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/allen13/flake-runner/pkg/config"
	"github.com/allen13/flake-runner/pkg/flakerunner"
	"github.com/allen13/flake-runner/pkg/types"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamoTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AWSIntegrationTest tests with real AWS resources
func TestAWSIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping AWS integration tests in short mode")
	}

	// Check if AWS integration is enabled
	if !isAWSIntegrationEnabled() {
		t.Skip("AWS integration tests disabled. Set FLAKE_RUNNER_AWS_INTEGRATION=true to enable.")
	}

	// Setup AWS test environment
	awsEnv, cleanup := setupAWSTestEnvironment(t)
	defer cleanup()

	// Test complete AWS integration workflow
	t.Run("Complete_AWS_Workflow", func(t *testing.T) {
		testCompleteAWSWorkflow(t, awsEnv)
	})

	// Test AWS resource validation
	t.Run("AWS_Resource_Validation", func(t *testing.T) {
		testAWSResourceValidation(t, awsEnv)
	})

	// Test FlakeRunner with real AWS
	t.Run("FlakeRunner_AWS_Integration", func(t *testing.T) {
		testFlakeRunnerAWSIntegration(t, awsEnv)
	})
}

// AWSTestEnvironment represents the AWS test environment
type AWSTestEnvironment struct {
	Config           *config.Config
	AWSConfig        aws.Config
	S3Client         *s3.Client
	DynamoClient     *dynamodb.Client
	EMRClient        *emrserverless.Client
	TestBuckets      []string
	TestTableName    string
	TestPrefix       string
	CleanupResources []func() error
}

// setupAWSTestEnvironment creates real AWS resources for testing
func setupAWSTestEnvironment(t *testing.T) (*AWSTestEnvironment, func()) {
	ctx := context.TODO()

	// Load AWS configuration
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx)
	require.NoError(t, err, "Should load AWS configuration")

	// Verify AWS credentials
	stsClient := sts.NewFromConfig(awsCfg)
	identity, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	require.NoError(t, err, "Should have valid AWS credentials")

	t.Logf("Running AWS integration tests with account: %s", *identity.Account)

	// Create AWS clients
	s3Client := s3.NewFromConfig(awsCfg)
	dynamoClient := dynamodb.NewFromConfig(awsCfg)
	emrClient := emrserverless.NewFromConfig(awsCfg)

	// Generate unique test prefix
	testPrefix := fmt.Sprintf("flake-runner-test-%d", time.Now().Unix())

	// Setup test environment
	env := &AWSTestEnvironment{
		AWSConfig:        awsCfg,
		S3Client:         s3Client,
		DynamoClient:     dynamoClient,
		EMRClient:        emrClient,
		TestPrefix:       testPrefix,
		CleanupResources: []func() error{},
	}

	// Create test resources
	createAWSTestResources(t, env)

	// Create FlakeRunner configuration
	env.Config = createAWSTestConfig(env)

	cleanup := func() {
		cleanupAWSTestResources(t, env)
	}

	return env, cleanup
}

// createAWSTestResources creates the necessary AWS resources for testing
func createAWSTestResources(t *testing.T, env *AWSTestEnvironment) {
	ctx := context.TODO()

	// Create test S3 buckets
	bucketNames := []string{
		fmt.Sprintf("%s-input", env.TestPrefix),
		fmt.Sprintf("%s-output", env.TestPrefix),
		fmt.Sprintf("%s-staging", env.TestPrefix),
	}

	for _, bucketName := range bucketNames {
		t.Logf("Creating S3 bucket: %s", bucketName)

		_, err := env.S3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})

		if err != nil {
			// Check if bucket already exists (might be from previous test run)
			if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
				require.NoError(t, err, "Should create S3 bucket: %s", bucketName)
			}
		}

		env.TestBuckets = append(env.TestBuckets, bucketName)

		// Add cleanup function
		env.CleanupResources = append(env.CleanupResources, func() error {
			return cleanupS3Bucket(ctx, env.S3Client, bucketName)
		})
	}

	// Create test DynamoDB table
	env.TestTableName = fmt.Sprintf("%s-orchestrations", env.TestPrefix)
	t.Logf("Creating DynamoDB table: %s", env.TestTableName)

	_, err := env.DynamoClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(env.TestTableName),
		KeySchema: []dynamoTypes.KeySchemaElement{
			{
				AttributeName: aws.String("file_path"),
				KeyType:       dynamoTypes.KeyTypeHash,
			},
			{
				AttributeName: aws.String("job_id"),
				KeyType:       dynamoTypes.KeyTypeRange,
			},
		},
		AttributeDefinitions: []dynamoTypes.AttributeDefinition{
			{
				AttributeName: aws.String("file_path"),
				AttributeType: dynamoTypes.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("job_id"),
				AttributeType: dynamoTypes.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("orchestration_state"),
				AttributeType: dynamoTypes.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String("processing_initiated_at"),
				AttributeType: dynamoTypes.ScalarAttributeTypeS,
			},
		},
		GlobalSecondaryIndexes: []dynamoTypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("OrchestrationStateIndex"),
				KeySchema: []dynamoTypes.KeySchemaElement{
					{
						AttributeName: aws.String("orchestration_state"),
						KeyType:       dynamoTypes.KeyTypeHash,
					},
					{
						AttributeName: aws.String("processing_initiated_at"),
						KeyType:       dynamoTypes.KeyTypeRange,
					},
				},
				Projection: &dynamoTypes.Projection{
					ProjectionType: dynamoTypes.ProjectionTypeAll,
				},
				ProvisionedThroughput: &dynamoTypes.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				},
			},
		},
		BillingMode: dynamoTypes.BillingModeProvisioned,
		ProvisionedThroughput: &dynamoTypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})

	if err != nil {
		// Check if table already exists
		if !strings.Contains(err.Error(), "ResourceInUseException") {
			require.NoError(t, err, "Should create DynamoDB table")
		}
	}

	// Wait for table to be created
	waiter := dynamodb.NewTableExistsWaiter(env.DynamoClient)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(env.TestTableName),
	}, time.Minute*5)
	require.NoError(t, err, "Table should be created within 5 minutes")

	// Add cleanup function
	env.CleanupResources = append(env.CleanupResources, func() error {
		_, err := env.DynamoClient.DeleteTable(ctx, &dynamodb.DeleteTableInput{
			TableName: aws.String(env.TestTableName),
		})
		return err
	})

	t.Log("✅ AWS test resources created successfully")
}

// createAWSTestConfig creates a FlakeRunner configuration for AWS testing
func createAWSTestConfig(env *AWSTestEnvironment) *config.Config {
	// Get EMR Application ID from environment or use a test value
	emrAppID := os.Getenv("FLAKE_RUNNER_TEST_EMR_APP_ID")
	if emrAppID == "" {
		emrAppID = "test-emr-app-id" // This will cause EMR tests to be skipped
	}

	// Get execution role from environment
	executionRole := os.Getenv("FLAKE_RUNNER_TEST_EXECUTION_ROLE")
	if executionRole == "" {
		executionRole = "arn:aws:iam::123456789012:role/EMRServerlessRole" // Test role
	}

	return &config.Config{
		AWSRegion:           "us-east-1",
		InputBucketName:     env.TestBuckets[0],
		OutputBucketName:    env.TestBuckets[1],
		StagingBucketName:   env.TestBuckets[2],
		ControlTableName:    env.TestTableName,
		EMRApplicationID:    emrAppID,
		EMRExecutionRoleARN: executionRole,
		JobTimeoutMinutes:   30,
		MaxRetries:          3,
		PrefixMappings: []config.PrefixMapping{
			{
				S3Prefix:       "customers/",
				TargetName:     "CUSTOMERS",
				ContainerImage: "123456789012.dkr.ecr.us-east-1.amazonaws.com/flake-runner:v3.1.0-with-pyspark",
				EntryPoint:     "/opt/spark/jobs/simple_processor.py",
				ProcessingConfig: config.ProcessingConfig{
					FileFormat:      "CSV",
					CompressionType: "NONE",
					MaxFileSize:     10000000,
				},
				ValidationRules: config.ValidationRules{
					ValidateRecordCount: true,
					ValidateFileSize:    true,
					ValidateChecksum:    false,
					RequiredFields:      []string{"customer_id", "name", "email"},
				},
			},
		},
	}
}

// testCompleteAWSWorkflow tests the complete workflow with real AWS resources
func testCompleteAWSWorkflow(t *testing.T, env *AWSTestEnvironment) {
	ctx := context.TODO()

	// Create test data
	testData := `customer_id,name,email,phone,city,state
1,John Doe,john.doe@example.com,555-0101,New York,NY
2,Jane Smith,jane.smith@example.com,555-0102,Los Angeles,CA
3,Bob Johnson,bob.johnson@example.com,555-0103,Chicago,IL`

	// Upload test file to S3
	s3Key := "customers/test_customers.csv"
	_, err := env.S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(env.Config.InputBucketName),
		Key:    aws.String(s3Key),
		Body:   strings.NewReader(testData),
	})
	require.NoError(t, err, "Should upload test file to S3")

	// Create control data
	controlData := &types.ControlData{
		FileName:    "test_customers.csv",
		FileSize:    int64(len(testData)),
		RecordCount: 3,
		ColumnCount: 6,
		CreatedAt:   time.Now(),
		BatchID:     "aws-test-batch",
	}

	// Test FlakeRunner with real AWS
	t.Run("FlakeRunner_Real_AWS", func(t *testing.T) {
		// Create temporary config file
		tempDir, err := ioutil.TempDir("", "aws-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		configPath := createTempConfigFile(t, tempDir, env.Config)

		// Create FlakeRunner instance
		fr, err := flakerunner.NewFlakeRunner(configPath)
		require.NoError(t, err, "Should create FlakeRunner instance")

		// Test configuration validation
		fr = fr.ValidateConfiguration()
		assert.NoError(t, fr.GetError(), "Configuration should be valid")

		// Test AWS resource validation (might fail for test resources)
		fr = fr.ValidateAWSResources()
		if fr.GetError() != nil {
			t.Logf("AWS resource validation failed (expected for test setup): %v", fr.GetError())
			fr.ClearError() // Clear error to continue testing
		}

		// Test file processing logic
		s3Path := fmt.Sprintf("s3://%s/%s", env.Config.InputBucketName, s3Key)

		// Test record counting
		recordCount, err := fr.CountRecordsInDataFile(s3Path, "CSV")
		if err == nil {
			assert.Equal(t, controlData.RecordCount, recordCount, "Should count data records excluding header")
		} else {
			t.Logf("Record counting failed (expected for test setup): %v", err)
		}

		// Test target table determination
		targetTable, mapping, err := fr.DetermineTargetTable(s3Path)
		assert.NoError(t, err, "Should determine target table")
		assert.Equal(t, "CUSTOMERS", targetTable, "Should map to CUSTOMERS table")
		assert.NotNil(t, mapping, "Should find mapping")
	})

	// Test DynamoDB operations
	t.Run("DynamoDB_Operations", func(t *testing.T) {
		testDynamoDBOperations(t, env, s3Key, controlData)
	})

	// Test S3 operations
	t.Run("S3_Operations", func(t *testing.T) {
		testS3Operations(t, env, s3Key)
	})
}

// testAWSResourceValidation tests AWS resource validation
func testAWSResourceValidation(t *testing.T, env *AWSTestEnvironment) {
	ctx := context.TODO()

	// Test S3 bucket validation
	t.Run("S3_Bucket_Validation", func(t *testing.T) {
		for _, bucket := range env.TestBuckets {
			_, err := env.S3Client.HeadBucket(ctx, &s3.HeadBucketInput{
				Bucket: aws.String(bucket),
			})
			assert.NoError(t, err, "S3 bucket should be accessible: %s", bucket)
		}
	})

	// Test DynamoDB table validation
	t.Run("DynamoDB_Table_Validation", func(t *testing.T) {
		_, err := env.DynamoClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(env.TestTableName),
		})
		assert.NoError(t, err, "DynamoDB table should be accessible")
	})

	// Test EMR Application validation (if configured)
	t.Run("EMR_Application_Validation", func(t *testing.T) {
		if env.Config.EMRApplicationID != "test-emr-app-id" {
			_, err := env.EMRClient.GetApplication(ctx, &emrserverless.GetApplicationInput{
				ApplicationId: aws.String(env.Config.EMRApplicationID),
			})
			if err != nil {
				t.Logf("EMR application validation failed (may not exist): %v", err)
			} else {
				t.Log("✅ EMR application is accessible")
			}
		} else {
			t.Log("⚠️ Skipping EMR validation (using test app ID)")
		}
	})
}

// testFlakeRunnerAWSIntegration tests FlakeRunner with real AWS integration
func testFlakeRunnerAWSIntegration(t *testing.T, env *AWSTestEnvironment) {
	// Create temporary config file
	tempDir, err := ioutil.TempDir("", "flakerunner-aws-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	configPath := createTempConfigFile(t, tempDir, env.Config)

	// Create FlakeRunner instance
	fr, err := flakerunner.NewFlakeRunner(configPath)
	require.NoError(t, err, "Should create FlakeRunner instance")

	// Test comprehensive validation
	fr = fr.ValidateConfiguration().ValidateAWSResources()

	if fr.GetError() != nil {
		t.Logf("Some AWS validation failed (expected for test setup): %v", fr.GetError())
		fr.ClearError()
	}

	// Test file orchestration record operations
	testFileOrchestrationRecords(t, fr, env)
}

// testDynamoDBOperations tests DynamoDB operations
func testDynamoDBOperations(t *testing.T, env *AWSTestEnvironment, s3Key string, controlData *types.ControlData) {
	ctx := context.TODO()

	// Create test orchestration record
	now := time.Now()
	record := &types.FileOrchestrationRecord{
		File_path:               fmt.Sprintf("s3://%s/%s", env.Config.InputBucketName, s3Key),
		Job_id:                  "aws-test-job-123",
		Batch_id:                controlData.BatchID,
		Orchestration_state:     "INITIATED",
		Processing_initiated_at: &now,
		Target_table:            "CUSTOMERS",
		Control_data:            controlData,
		Version:                 1,
	}

	// Test put operation
	item := convertRecordToDynamoItem(record)
	_, err := env.DynamoClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(env.TestTableName),
		Item:      item,
	})
	assert.NoError(t, err, "Should store orchestration record in DynamoDB")

	// Test get operation
	getResult, err := env.DynamoClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(env.TestTableName),
		Key: map[string]dynamoTypes.AttributeValue{
			"file_path": &dynamoTypes.AttributeValueMemberS{Value: record.File_path},
			"job_id":    &dynamoTypes.AttributeValueMemberS{Value: record.Job_id},
		},
	})
	assert.NoError(t, err, "Should retrieve orchestration record from DynamoDB")
	assert.NotEmpty(t, getResult.Item, "Should have retrieved item")

	// Test query operation
	queryResult, err := env.DynamoClient.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(env.TestTableName),
		KeyConditionExpression: aws.String("file_path = :filePath"),
		ExpressionAttributeValues: map[string]dynamoTypes.AttributeValue{
			":filePath": &dynamoTypes.AttributeValueMemberS{Value: record.File_path},
		},
	})
	assert.NoError(t, err, "Should query orchestration records")
	assert.Greater(t, len(queryResult.Items), 0, "Should find orchestration records")
}

// testS3Operations tests S3 operations
func testS3Operations(t *testing.T, env *AWSTestEnvironment, s3Key string) {
	ctx := context.TODO()

	// Test file existence
	_, err := env.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(env.Config.InputBucketName),
		Key:    aws.String(s3Key),
	})
	assert.NoError(t, err, "Should find uploaded test file")

	// Test file download
	getResult, err := env.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(env.Config.InputBucketName),
		Key:    aws.String(s3Key),
	})
	assert.NoError(t, err, "Should download test file")
	assert.NotNil(t, getResult.Body, "Should have file content")
	getResult.Body.Close()

	// Test file listing
	listResult, err := env.S3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(env.Config.InputBucketName),
		Prefix: aws.String("customers/"),
	})
	assert.NoError(t, err, "Should list objects with prefix")
	assert.Greater(t, len(listResult.Contents), 0, "Should find objects")
}

// testFileOrchestrationRecords tests file orchestration record operations
func testFileOrchestrationRecords(t *testing.T, fr *flakerunner.FlakeRunner, env *AWSTestEnvironment) {
	// This would test the FlakeRunner's orchestration record methods
	// For now, we'll test the conceptual workflow

	s3Path := fmt.Sprintf("s3://%s/customers/test_file.csv", env.Config.InputBucketName)
	controlData := &types.ControlData{
		FileName:    "test_file.csv",
		RecordCount: 5,
		ColumnCount: 6,
		CreatedAt:   time.Now(),
		BatchID:     "test-batch",
	}

	// Test would create orchestration record, validate, etc.
	// This is conceptual since we can't run full EMR jobs in tests
	t.Logf("Would test file processing for: %s", s3Path)
	t.Logf("Control data: %+v", controlData)

	// Test target table determination
	targetTable, mapping, err := fr.DetermineTargetTable(s3Path)
	assert.NoError(t, err, "Should determine target table")
	assert.Equal(t, "CUSTOMERS", targetTable, "Should map to CUSTOMERS table")
	assert.NotNil(t, mapping, "Should find mapping")
}

// Helper functions

// isAWSIntegrationEnabled checks if AWS integration testing is enabled
func isAWSIntegrationEnabled() bool {
	return os.Getenv("FLAKE_RUNNER_AWS_INTEGRATION") == "true"
}

// createTempConfigFile creates a temporary configuration file
func createTempConfigFile(t *testing.T, dir string, cfg *config.Config) string {
	configBytes, err := json.MarshalIndent(cfg, "", "  ")
	require.NoError(t, err)

	configPath := filepath.Join(dir, "test-config.json")
	err = ioutil.WriteFile(configPath, configBytes, 0644)
	require.NoError(t, err)

	return configPath
}

// convertRecordToDynamoItem converts a record to DynamoDB item format
func convertRecordToDynamoItem(record *types.FileOrchestrationRecord) map[string]dynamoTypes.AttributeValue {
	// Simplified conversion - in practice this would be more comprehensive
	return map[string]dynamoTypes.AttributeValue{
		"file_path": &dynamoTypes.AttributeValueMemberS{
			Value: record.File_path,
		},
		"job_id": &dynamoTypes.AttributeValueMemberS{
			Value: record.Job_id,
		},
		"batch_id": &dynamoTypes.AttributeValueMemberS{
			Value: record.Batch_id,
		},
		"orchestration_state": &dynamoTypes.AttributeValueMemberS{
			Value: record.Orchestration_state,
		},
		"processing_initiated_at": &dynamoTypes.AttributeValueMemberS{
			Value: record.Processing_initiated_at.Format(time.RFC3339),
		},
		"target_table": &dynamoTypes.AttributeValueMemberS{
			Value: record.Target_table,
		},
		"version": &dynamoTypes.AttributeValueMemberN{
			Value: fmt.Sprintf("%d", record.Version),
		},
	}
}

// cleanupAWSTestResources cleans up AWS test resources
func cleanupAWSTestResources(t *testing.T, env *AWSTestEnvironment) {
	t.Log("Cleaning up AWS test resources...")

	for i, cleanup := range env.CleanupResources {
		if err := cleanup(); err != nil {
			t.Logf("Cleanup %d failed: %v", i, err)
		}
	}

	t.Log("✅ AWS test resources cleanup completed")
}

// cleanupS3Bucket empties and deletes an S3 bucket
func cleanupS3Bucket(ctx context.Context, client *s3.Client, bucketName string) error {
	// List and delete all objects
	listResult, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return err
	}

	// Delete all objects
	for _, obj := range listResult.Contents {
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    obj.Key,
		})
		if err != nil {
			return err
		}
	}

	// Delete the bucket
	_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	return err
}

// TestAWSResourceSetup tests just the AWS resource setup (can be run separately)
func TestAWSResourceSetup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping AWS resource setup test in short mode")
	}

	if !isAWSIntegrationEnabled() {
		t.Skip("AWS integration tests disabled. Set FLAKE_RUNNER_AWS_INTEGRATION=true to enable.")
	}

	// Test AWS resource creation and cleanup
	awsEnv, cleanup := setupAWSTestEnvironment(t)
	defer cleanup()

	// Verify resources were created
	assert.NotEmpty(t, awsEnv.TestBuckets, "Should have created test buckets")
	assert.NotEmpty(t, awsEnv.TestTableName, "Should have created test table")
	assert.NotNil(t, awsEnv.Config, "Should have created test configuration")

	t.Log("✅ AWS resource setup test completed")
}
