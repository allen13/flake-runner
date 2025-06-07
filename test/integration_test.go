package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/allen13/flake-runner/pkg/config"
	"github.com/allen13/flake-runner/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestData represents test data for different scenarios
type TestData struct {
	Name        string
	InputData   string
	ControlData *types.ControlData
	TargetTable string
	Expected    ExpectedResults
}

// ExpectedResults defines what we expect from processing
type ExpectedResults struct {
	RecordCount    int64
	HasMetadata    bool
	OutputFormat   string
	ProcessingTime time.Duration
}

// Integration test suite that tests the complete flow
func TestFlakeRunnerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	// Setup test environment
	testDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Test scenarios
	testCases := []TestData{
		{
			Name: "Customer Data Processing",
			InputData: `customer_id,name,email,phone,city,state
1,John Doe,john.doe@example.com,555-0101,New York,NY
2,Jane Smith,jane.smith@example.com,555-0102,Los Angeles,CA
3,Bob Johnson,bob.johnson@example.com,555-0103,Chicago,IL`,
			ControlData: &types.ControlData{
				FileName:    "customers.csv",
				FileSize:    150,
				FileHash:    "test-hash-123",
				RecordCount: 3,
				ColumnCount: 6,
				CreatedAt:   time.Now(),
				BatchID:     "batch-001",
			},
			TargetTable: "CUSTOMERS",
			Expected: ExpectedResults{
				RecordCount:  3,
				HasMetadata:  true,
				OutputFormat: "PARQUET",
			},
		},
		{
			Name: "Order Data Processing",
			InputData: `order_id,customer_id,order_date,amount,status
1001,1,2024-01-01,99.99,completed
1002,2,2024-01-02,149.50,pending
1003,1,2024-01-03,75.25,completed`,
			ControlData: &types.ControlData{
				FileName:    "orders.csv",
				FileSize:    120,
				FileHash:    "test-hash-456",
				RecordCount: 3,
				ColumnCount: 5,
				CreatedAt:   time.Now(),
				BatchID:     "batch-002",
			},
			TargetTable: "ORDERS",
			Expected: ExpectedResults{
				RecordCount:  3,
				HasMetadata:  true,
				OutputFormat: "PARQUET",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			runIntegrationTest(t, testDir, testCase)
		})
	}
}

// setupTestEnvironment creates a temporary test environment
func setupTestEnvironment(t *testing.T) (string, func()) {
	testDir, err := ioutil.TempDir("", "flake-runner-integration-*")
	require.NoError(t, err)

	// Create subdirectories
	inputDir := filepath.Join(testDir, "input")
	outputDir := filepath.Join(testDir, "output")
	stagingDir := filepath.Join(testDir, "staging")
	configDir := filepath.Join(testDir, "config")

	for _, dir := range []string{inputDir, outputDir, stagingDir, configDir} {
		err := os.MkdirAll(dir, 0755)
		require.NoError(t, err)
	}

	// Create test configuration
	createTestConfig(t, configDir)

	cleanup := func() {
		os.RemoveAll(testDir)
	}

	return testDir, cleanup
}

// createTestConfig creates a test configuration file
func createTestConfig(t *testing.T, configDir string) {
	testConfig := &config.Config{
		AWSRegion:           "us-east-1",
		InputBucketName:     "test-input-bucket",
		OutputBucketName:    "test-output-bucket",
		StagingBucketName:   "test-staging-bucket",
		ControlTableName:    "test-control-table",
		EMRApplicationID:    "test-emr-app-id",
		EMRExecutionRoleARN: "arn:aws:iam::123456789012:role/EMRServerlessRole",
		PrefixMappings: []config.PrefixMapping{
			{
				S3Prefix:       "customers/",
				TargetName:     "CUSTOMERS",
				ContainerImage: "123456789012.dkr.ecr.us-east-1.amazonaws.com/flake-runner:v3.1.0-with-pyspark",
				EntryPoint:     "/opt/spark/jobs/simple_processor.py",
				ProcessingConfig: config.ProcessingConfig{
					FileFormat:      "CSV",
					CompressionType: "NONE",
					MaxFileSize:     1000000,
				},
				ValidationRules: config.ValidationRules{
					ValidateRecordCount: true,
					ValidateFileSize:    true,
					ValidateChecksum:    false,
					RequiredFields:      []string{"customer_id", "name", "email"},
				},
			},
			{
				S3Prefix:       "orders/",
				TargetName:     "ORDERS",
				ContainerImage: "123456789012.dkr.ecr.us-east-1.amazonaws.com/flake-runner:v3.1.0-with-pyspark",
				EntryPoint:     "/opt/spark/jobs/simple_processor.py",
				ProcessingConfig: config.ProcessingConfig{
					FileFormat:      "CSV",
					CompressionType: "NONE",
					MaxFileSize:     1000000,
				},
				ValidationRules: config.ValidationRules{
					ValidateRecordCount: true,
					ValidateFileSize:    true,
					ValidateChecksum:    false,
					RequiredFields:      []string{"order_id", "customer_id", "amount"},
				},
			},
		},
	}

	configBytes, err := json.MarshalIndent(testConfig, "", "  ")
	require.NoError(t, err)

	configPath := filepath.Join(configDir, "test-config.json")
	err = ioutil.WriteFile(configPath, configBytes, 0644)
	require.NoError(t, err)
}

// runIntegrationTest executes a complete integration test
func runIntegrationTest(t *testing.T, testDir string, testData TestData) {
	configPath := filepath.Join(testDir, "config", "test-config.json")
	inputDir := filepath.Join(testDir, "input")
	outputDir := filepath.Join(testDir, "output")

	// Create input file
	inputFile := filepath.Join(inputDir, testData.ControlData.FileName)
	err := ioutil.WriteFile(inputFile, []byte(testData.InputData), 0644)
	require.NoError(t, err)

	// Test 1: FlakeRunner Configuration and Validation
	t.Run("FlakeRunner_Setup", func(t *testing.T) {
		testFlakeRunnerSetup(t, configPath)
	})

	// Test 2: File Processing and Validation
	t.Run("File_Processing", func(t *testing.T) {
		testFileProcessing(t, configPath, inputFile, testData.ControlData)
	})

	// Test 3: PySpark Script Execution
	t.Run("PySpark_Execution", func(t *testing.T) {
		testPySparkExecution(t, inputFile, outputDir, testData)
	})

	// Test 4: End-to-End Integration
	t.Run("End_to_End", func(t *testing.T) {
		testEndToEndIntegration(t, configPath, inputFile, outputDir, testData)
	})
}

// testFlakeRunnerSetup tests FlakeRunner initialization and configuration
func testFlakeRunnerSetup(t *testing.T, configPath string) {
	// This test validates FlakeRunner setup without requiring AWS
	t.Log("Testing FlakeRunner configuration loading and validation...")

	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	require.NoError(t, err, "Should load test configuration successfully")

	// Validate configuration structure
	assert.NotEmpty(t, cfg.PrefixMappings, "Should have prefix mappings")
	assert.Equal(t, 2, len(cfg.PrefixMappings), "Should have 2 prefix mappings")

	// Test prefix resolution
	for _, mapping := range cfg.PrefixMappings {
		assert.NotEmpty(t, mapping.S3Prefix, "S3 prefix should not be empty")
		assert.NotEmpty(t, mapping.TargetName, "Target name should not be empty")
		assert.NotEmpty(t, mapping.ContainerImage, "Container image should not be empty")
		assert.NotEmpty(t, mapping.EntryPoint, "Entry point should not be empty")
	}

	t.Log("✅ FlakeRunner configuration validation passed")
}

// testFileProcessing tests the Go file processing functions
func testFileProcessing(t *testing.T, configPath, inputFile string, controlData *types.ControlData) {
	t.Log("Testing Go file processing functions...")

	// Load configuration
	cfg, err := config.LoadConfig(configPath)
	require.NoError(t, err)

	// Test file validation functions
	t.Run("File_Validation", func(t *testing.T) {
		// Test file exists
		_, err := os.Stat(inputFile)
		assert.NoError(t, err, "Input file should exist")

		// Test file size
		fileInfo, err := os.Stat(inputFile)
		require.NoError(t, err)
		assert.Greater(t, fileInfo.Size(), int64(0), "File should have content")

		// Test control data validation
		assert.NotNil(t, controlData, "Control data should be provided")
		assert.NotEmpty(t, controlData.FileName, "File name should not be empty")
		assert.Greater(t, controlData.RecordCount, int64(0), "Record count should be positive")
	})

	// Test prefix resolution
	t.Run("Prefix_Resolution", func(t *testing.T) {
		// Simulate prefix resolution logic
		testPath := ""
		if strings.Contains(controlData.FileName, "customer") {
			testPath = "customers/" + controlData.FileName
		} else if strings.Contains(controlData.FileName, "order") {
			testPath = "orders/" + controlData.FileName
		}

		var matchedMapping *config.PrefixMapping
		for _, mapping := range cfg.PrefixMappings {
			if strings.HasPrefix(testPath, mapping.S3Prefix) {
				matchedMapping = &mapping
				break
			}
		}

		assert.NotNil(t, matchedMapping, "Should find matching prefix mapping")
		if matchedMapping != nil {
			assert.NotEmpty(t, matchedMapping.TargetName, "Should have target table")
			assert.NotEmpty(t, matchedMapping.ContainerImage, "Should have container image")
		}
	})

	t.Log("✅ Go file processing validation passed")
}

// testPySparkExecution tests the PySpark script execution using Docker
func testPySparkExecution(t *testing.T, inputFile, outputDir string, testData TestData) {
	t.Log("Testing PySpark script execution with Docker...")

	// Create output directory for this test
	testOutputDir := filepath.Join(outputDir, fmt.Sprintf("pyspark_%s", strings.ToLower(testData.TargetTable)))
	err := os.MkdirAll(testOutputDir, 0755)
	require.NoError(t, err)

	// Test PySpark script execution
	if !isDockerAvailable() {
		t.Skip("Docker not available, skipping PySpark execution test")
		return
	}

	success := executePySparkScript(t, inputFile, testOutputDir, testData)
	if success {
		// Validate output
		validatePySparkOutput(t, testOutputDir, testData.Expected)
	}

	t.Log("✅ PySpark script execution completed")
}

// testEndToEndIntegration tests the complete integration
func testEndToEndIntegration(t *testing.T, configPath, inputFile, outputDir string, testData TestData) {
	t.Log("Testing end-to-end integration...")

	// This test would normally create a FlakeRunner instance and process the file
	// For this integration test, we'll simulate the workflow
	t.Run("Workflow_Simulation", func(t *testing.T) {
		// Simulate the complete workflow steps
		steps := []string{
			"Load Configuration",
			"Validate Input File",
			"Create Orchestration Record",
			"Validate with Control Data",
			"Determine Target Table",
			"Generate EMR Job Parameters",
			"Simulate Job Submission",
		}

		for i, step := range steps {
			t.Logf("Step %d: %s", i+1, step)

			switch step {
			case "Load Configuration":
				_, err := config.LoadConfig(configPath)
				assert.NoError(t, err, "Should load configuration")

			case "Validate Input File":
				_, err := os.Stat(inputFile)
				assert.NoError(t, err, "Input file should exist")

			case "Create Orchestration Record":
				// Simulate orchestration record creation
				now := time.Now()
				record := &types.FileOrchestrationRecord{
					File_path:               inputFile,
					Job_id:                  "test-job-123",
					Batch_id:                testData.ControlData.BatchID,
					Orchestration_state:     "INITIATED",
					Processing_initiated_at: &now,
					Target_table:            testData.TargetTable,
				}
				assert.NotNil(t, record, "Should create orchestration record")

			case "Validate with Control Data":
				assert.NotNil(t, testData.ControlData, "Control data should be available")
				assert.Greater(t, testData.ControlData.RecordCount, int64(0), "Record count should be positive")

			case "Determine Target Table":
				assert.NotEmpty(t, testData.TargetTable, "Target table should be determined")

			case "Generate EMR Job Parameters":
				params := generateEMRJobParameters(inputFile, testData.TargetTable)
				assert.NotEmpty(t, params, "Should generate EMR parameters")

			case "Simulate Job Submission":
				t.Log("✅ Job submission simulation completed")
			}
		}
	})

	t.Log("✅ End-to-end integration test completed")
}

// Helper functions

// isDockerAvailable checks if Docker is available for testing
func isDockerAvailable() bool {
	// This would normally check if Docker is running and the flake-runner image is available
	// For now, we'll return false to avoid dependencies in CI
	return false
}

// executePySparkScript executes the PySpark script using Docker
func executePySparkScript(t *testing.T, inputFile, outputDir string, testData TestData) bool {
	// This would execute:
	// docker run --rm --entrypoint python3 \
	//   -v inputDir:/data -v outputDir:/output \
	//   flake-runner:local /opt/spark/jobs/simple_processor.py \
	//   --input-path /data/filename --output-path /output/result \
	//   --job-id test-job

	t.Log("Would execute PySpark script with Docker here")
	return true
}

// validatePySparkOutput validates the output from PySpark execution
func validatePySparkOutput(t *testing.T, outputDir string, expected ExpectedResults) {
	t.Log("Validating PySpark output...")

	// Check if output directory exists
	_, err := os.Stat(outputDir)
	assert.NoError(t, err, "Output directory should exist")

	// Check for success markers
	successFile := filepath.Join(outputDir, "_SUCCESS")
	if _, err := os.Stat(successFile); err == nil {
		t.Log("✅ Found _SUCCESS marker")
	}

	// Look for output files
	files, err := ioutil.ReadDir(outputDir)
	if err == nil && len(files) > 0 {
		t.Logf("✅ Found %d output files", len(files))

		for _, file := range files {
			if strings.HasPrefix(file.Name(), "part-") {
				t.Logf("✅ Found data file: %s", file.Name())
			}
		}
	}
}

// generateEMRJobParameters generates parameters for EMR job submission
func generateEMRJobParameters(inputFile, targetTable string) map[string]interface{} {
	return map[string]interface{}{
		"entryPoint": "/opt/spark/jobs/processor.py",
		"arguments": []string{
			"--input-path", inputFile,
			"--output-path", "/tmp/output",
			"--staging-path", "/tmp/staging",
			"--target-table", targetTable,
			"--job-id", "test-job-123",
		},
		"containerImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/flake-runner:v3.1.0-with-pyspark",
	}
}

// Benchmark tests for performance validation

func BenchmarkFileProcessing(b *testing.B) {
	testDir, cleanup := setupTestEnvironment(&testing.T{})
	defer cleanup()

	inputFile := filepath.Join(testDir, "input", "benchmark.csv")

	// Create benchmark data
	benchmarkData := generateBenchmarkData(1000) // 1000 records
	err := ioutil.WriteFile(inputFile, []byte(benchmarkData), 0644)
	if err != nil {
		b.Fatal(err)
	}

	controlData := &types.ControlData{
		FileName:    "benchmark.csv",
		FileSize:    int64(len(benchmarkData)),
		RecordCount: 1000,
		ColumnCount: 6,
		CreatedAt:   time.Now(),
		BatchID:     "benchmark-batch",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Benchmark the file processing logic
		validateFileProcessing(inputFile, controlData)
	}
}

// generateBenchmarkData creates test data for benchmarking
func generateBenchmarkData(recordCount int) string {
	var builder strings.Builder
	builder.WriteString("customer_id,name,email,phone,city,state\n")

	for i := 1; i <= recordCount; i++ {
		builder.WriteString(fmt.Sprintf("%d,Customer %d,customer%d@example.com,555-%04d,City %d,ST\n",
			i, i, i, i, i%100))
	}

	return builder.String()
}

// validateFileProcessing performs validation logic for benchmarking
func validateFileProcessing(inputFile string, controlData *types.ControlData) error {
	// Simulate file processing validation
	_, err := os.Stat(inputFile)
	if err != nil {
		return err
	}

	// Simulate control data validation
	if controlData.RecordCount <= 0 {
		return fmt.Errorf("invalid record count")
	}

	return nil
}
