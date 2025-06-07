package test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/allen13/flake-runner/pkg/config"
	"github.com/allen13/flake-runner/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEndToEndFlakeRunnerPySparkIntegration tests the complete flow from FlakeRunner to PySpark
func TestEndToEndFlakeRunnerPySparkIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping end-to-end integration tests in short mode")
	}

	// Setup comprehensive test environment
	testEnv, cleanup := setupEndToEndTestEnvironment(t)
	defer cleanup()

	// Test the complete workflow
	t.Run("Complete_Workflow", func(t *testing.T) {
		testCompleteWorkflow(t, testEnv)
	})

	// Test error scenarios
	t.Run("Error_Scenarios", func(t *testing.T) {
		testErrorScenarios(t, testEnv)
	})

	// Test performance and scalability
	t.Run("Performance_Test", func(t *testing.T) {
		testPerformanceScenarios(t, testEnv)
	})
}

// TestEnvironment represents the complete test environment
type TestEnvironment struct {
	BaseDir    string
	ConfigPath string
	InputDir   string
	OutputDir  string
	StagingDir string
	Config     *config.Config
}

// setupEndToEndTestEnvironment creates a comprehensive test environment
func setupEndToEndTestEnvironment(t *testing.T) (*TestEnvironment, func()) {
	baseDir, err := ioutil.TempDir("", "flake-runner-e2e-*")
	require.NoError(t, err)

	// Create directory structure
	dirs := []string{"input", "output", "staging", "config"}
	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(baseDir, dir), 0755)
		require.NoError(t, err)
	}

	// Create comprehensive test configuration
	testConfig := createEndToEndConfig(baseDir)
	configPath := filepath.Join(baseDir, "config", "e2e-config.json")

	configBytes, err := json.MarshalIndent(testConfig, "", "  ")
	require.NoError(t, err)

	err = ioutil.WriteFile(configPath, configBytes, 0644)
	require.NoError(t, err)

	testEnv := &TestEnvironment{
		BaseDir:    baseDir,
		ConfigPath: configPath,
		InputDir:   filepath.Join(baseDir, "input"),
		OutputDir:  filepath.Join(baseDir, "output"),
		StagingDir: filepath.Join(baseDir, "staging"),
		Config:     testConfig,
	}

	cleanup := func() {
		os.RemoveAll(baseDir)
	}

	return testEnv, cleanup
}

// createEndToEndConfig creates a comprehensive configuration for end-to-end testing
func createEndToEndConfig(baseDir string) *config.Config {
	return &config.Config{
		AWSRegion:           "us-east-1",
		InputBucketName:     "test-input-bucket",
		OutputBucketName:    "test-output-bucket",
		StagingBucketName:   "test-staging-bucket",
		ControlTableName:    "test-file-orchestrations",
		EMRApplicationID:    "test-emr-app-123",
		EMRExecutionRoleARN: "arn:aws:iam::123456789012:role/EMRServerlessRole",
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
			{
				S3Prefix:       "orders/",
				TargetName:     "ORDERS",
				ContainerImage: "123456789012.dkr.ecr.us-east-1.amazonaws.com/flake-runner:v3.1.0-with-pyspark",
				EntryPoint:     "/opt/spark/jobs/simple_processor.py",
				ProcessingConfig: config.ProcessingConfig{
					FileFormat:      "CSV",
					CompressionType: "GZIP",
					MaxFileSize:     20000000,
				},
				ValidationRules: config.ValidationRules{
					ValidateRecordCount: true,
					ValidateFileSize:    true,
					ValidateChecksum:    true,
					RequiredFields:      []string{"order_id", "customer_id", "amount"},
				},
			},
			{
				S3Prefix:       "analytics/",
				TargetName:     "ANALYTICS_EVENTS",
				ContainerImage: "123456789012.dkr.ecr.us-east-1.amazonaws.com/flake-runner:v3.1.0-with-pyspark",
				EntryPoint:     "/opt/spark/jobs/processor.py",
				ProcessingConfig: config.ProcessingConfig{
					FileFormat:      "JSON",
					CompressionType: "GZIP",
					MaxFileSize:     50000000,
				},
				ValidationRules: config.ValidationRules{
					ValidateRecordCount: true,
					ValidateFileSize:    true,
					ValidateChecksum:    true,
					RequiredFields:      []string{"event_id", "user_id", "event_type"},
				},
			},
		},
	}
}

// testCompleteWorkflow tests the complete end-to-end workflow
func testCompleteWorkflow(t *testing.T, env *TestEnvironment) {
	scenarios := []struct {
		name        string
		filePath    string
		data        string
		controlData *types.ControlData
		targetTable string
	}{
		{
			name:     "Customer_Processing_Workflow",
			filePath: "customers/customer_data.csv",
			data:     createDetailedCustomerData(25),
			controlData: &types.ControlData{
				FileName:    "customer_data.csv",
				FileSize:    1500,
				RecordCount: 25,
				ColumnCount: 8,
				CreatedAt:   time.Now(),
				BatchID:     "e2e-customer-batch",
			},
			targetTable: "CUSTOMERS",
		},
		{
			name:     "Order_Processing_Workflow",
			filePath: "orders/order_data.csv",
			data:     createDetailedOrderData(20),
			controlData: &types.ControlData{
				FileName:    "order_data.csv",
				FileSize:    1200,
				RecordCount: 20,
				ColumnCount: 7,
				CreatedAt:   time.Now(),
				BatchID:     "e2e-order-batch",
			},
			targetTable: "ORDERS",
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Step 1: Test FlakeRunner Configuration and Setup
			testFlakeRunnerConfiguration(t, env, scenario.filePath)

			// Step 2: Test File Processing and Validation Logic
			inputFile := createTestInputFile(t, env.InputDir, scenario.filePath, scenario.data)
			testFileProcessingLogic(t, env, inputFile, scenario.controlData)

			// Step 3: Test Target Table Determination
			testTargetTableDetermination(t, env, scenario.filePath, scenario.targetTable)

			// Step 4: Test EMR Job Parameter Generation
			testEMRJobParameterGeneration(t, env, inputFile, scenario.targetTable)

			// Step 5: Test PySpark Script Execution (if Docker available)
			if isDockerRunning() && isFlakeRunnerImageAvailable() {
				testPySparkIntegration(t, env, inputFile, scenario.controlData, scenario.targetTable)
			} else {
				t.Log("⚠️ Skipping PySpark integration (Docker not available)")
			}

			// Step 6: Test Complete Workflow Simulation
			testWorkflowSimulation(t, env, inputFile, scenario.controlData, scenario.targetTable)
		})
	}
}

// testFlakeRunnerConfiguration tests FlakeRunner configuration loading and validation
func testFlakeRunnerConfiguration(t *testing.T, env *TestEnvironment, filePath string) {
	t.Log("Testing FlakeRunner configuration...")

	// Test configuration loading
	cfg, err := config.LoadConfig(env.ConfigPath)
	require.NoError(t, err, "Should load configuration successfully")

	// Validate configuration structure
	assert.NotEmpty(t, cfg.PrefixMappings, "Should have prefix mappings")
	assert.Greater(t, len(cfg.PrefixMappings), 0, "Should have at least one mapping")

	// Test prefix matching logic
	var matchedMapping *config.PrefixMapping
	for _, mapping := range cfg.PrefixMappings {
		if strings.HasPrefix(filePath, mapping.S3Prefix) {
			matchedMapping = &mapping
			break
		}
	}

	assert.NotNil(t, matchedMapping, "Should find matching prefix for file path: %s", filePath)
	if matchedMapping != nil {
		assert.NotEmpty(t, matchedMapping.TargetName, "Matched mapping should have target table")
		assert.NotEmpty(t, matchedMapping.ContainerImage, "Matched mapping should have container image")
		assert.NotEmpty(t, matchedMapping.EntryPoint, "Matched mapping should have entry point")
	}

	t.Log("✅ FlakeRunner configuration validation passed")
}

// testFileProcessingLogic tests the file processing and validation logic
func testFileProcessingLogic(t *testing.T, env *TestEnvironment, inputFile string, controlData *types.ControlData) {
	t.Log("Testing file processing logic...")

	// Test file existence and basic properties
	fileInfo, err := os.Stat(inputFile)
	require.NoError(t, err, "Input file should exist")
	assert.Greater(t, fileInfo.Size(), int64(0), "File should have content")

	// Test control data validation
	assert.NotNil(t, controlData, "Control data should be provided")
	assert.NotEmpty(t, controlData.FileName, "File name should not be empty")
	assert.Greater(t, controlData.RecordCount, int64(0), "Record count should be positive")

	// Test record counting logic (simulating what FlakeRunner does)
	actualRecords, err := countCSVRecords(inputFile)
	require.NoError(t, err, "Should be able to count records")

	// Allow for header row
	expectedRecords := controlData.RecordCount
	assert.True(t, actualRecords == expectedRecords || actualRecords == expectedRecords+1,
		"Record count should match: expected %d, actual %d", expectedRecords, actualRecords)

	// Test file format detection
	format := detectFileFormat(inputFile)
	assert.Equal(t, "CSV", format, "Should detect CSV format")

	t.Log("✅ File processing logic validation passed")
}

// testTargetTableDetermination tests target table determination logic
func testTargetTableDetermination(t *testing.T, env *TestEnvironment, filePath, expectedTable string) {
	t.Log("Testing target table determination...")

	// Simulate the FlakeRunner logic for determining target table
	var targetTable string
	var matchedMapping *config.PrefixMapping

	for _, mapping := range env.Config.PrefixMappings {
		if strings.HasPrefix(filePath, mapping.S3Prefix) {
			targetTable = mapping.TargetName
			matchedMapping = &mapping
			break
		}
	}

	assert.Equal(t, expectedTable, targetTable, "Should determine correct target table")
	assert.NotNil(t, matchedMapping, "Should find matching mapping")

	if matchedMapping != nil {
		// Validate mapping configuration
		assert.NotEmpty(t, matchedMapping.ProcessingConfig.FileFormat, "Should have file format")
		assert.Greater(t, matchedMapping.ProcessingConfig.MaxFileSize, int64(0), "Should have max file size")
		assert.Greater(t, matchedMapping.ProcessingConfig.MaxFileSize, int64(0), "Should have max file size config")
	}

	t.Log("✅ Target table determination passed")
}

// testEMRJobParameterGeneration tests EMR job parameter generation
func testEMRJobParameterGeneration(t *testing.T, env *TestEnvironment, inputFile, targetTable string) {
	t.Log("Testing EMR job parameter generation...")

	// Find matching mapping
	var mapping *config.PrefixMapping
	for _, m := range env.Config.PrefixMappings {
		if m.TargetName == targetTable {
			mapping = &m
			break
		}
	}
	require.NotNil(t, mapping, "Should find mapping for target table")

	// Generate job parameters (simulating FlakeRunner logic)
	jobParams := map[string]interface{}{
		"entryPoint": mapping.EntryPoint,
		"arguments": []string{
			"--input-path", inputFile,
			"--output-path", filepath.Join(env.OutputDir, targetTable),
			"--staging-path", filepath.Join(env.StagingDir, targetTable),
			"--target-table", targetTable,
			"--file-format", mapping.ProcessingConfig.FileFormat,
			"--job-id", fmt.Sprintf("e2e-test-%d", time.Now().Unix()),
		},
		"containerImage": mapping.ContainerImage,
		"executionRole":  env.Config.EMRExecutionRoleARN,
		"applicationId":  env.Config.EMRApplicationID,
	}

	// Validate job parameters
	assert.NotEmpty(t, jobParams["entryPoint"], "Should have entry point")
	assert.NotEmpty(t, jobParams["arguments"], "Should have arguments")
	assert.NotEmpty(t, jobParams["containerImage"], "Should have container image")

	// Validate arguments structure
	args, ok := jobParams["arguments"].([]string)
	require.True(t, ok, "Arguments should be string slice")
	assert.Contains(t, args, "--target-table", "Should have target table argument")
	assert.Contains(t, args, targetTable, "Should contain target table value")

	t.Log("✅ EMR job parameter generation passed")
}

// testPySparkIntegration tests the actual PySpark script execution
func testPySparkIntegration(t *testing.T, env *TestEnvironment, inputFile string, controlData *types.ControlData, targetTable string) {
	t.Log("Testing PySpark integration with Docker...")

	outputDir := filepath.Join(env.OutputDir, strings.ToLower(targetTable))
	err := os.MkdirAll(outputDir, 0755)
	require.NoError(t, err)

	// Prepare Docker execution
	inputDir := filepath.Dir(inputFile)
	fileName := filepath.Base(inputFile)
	jobID := fmt.Sprintf("e2e-pyspark-%d", time.Now().Unix())

	dockerCmd := []string{
		"docker", "run", "--rm",
		"--entrypoint", "python3",
		"-v", fmt.Sprintf("%s:/data", inputDir),
		"-v", fmt.Sprintf("%s:/output", outputDir),
		"flake-runner:local",
		"/opt/spark/jobs/simple_processor.py",
		"--input-path", fmt.Sprintf("/data/%s", fileName),
		"--output-path", "/output",
		"--input-format", "CSV",
		"--output-format", "PARQUET",
		"--job-id", jobID,
	}

	// Execute PySpark script
	t.Logf("Executing PySpark: %s", strings.Join(dockerCmd, " "))
	cmd := exec.Command(dockerCmd[0], dockerCmd[1:]...)
	output, err := cmd.CombinedOutput()

	if err != nil {
		t.Errorf("PySpark execution failed: %v\nOutput: %s", err, string(output))
		return
	}

	// Validate execution output
	assert.Contains(t, string(output), "Processing completed successfully",
		"PySpark should complete successfully")
	assert.Contains(t, string(output), fmt.Sprintf("Records processed: %d", controlData.RecordCount),
		"Should process expected number of records")

	// Validate output files
	files, err := ioutil.ReadDir(outputDir)
	require.NoError(t, err, "Should read output directory")
	assert.Greater(t, len(files), 0, "Should have output files")

	// Check for success marker
	successFile := filepath.Join(outputDir, "_SUCCESS")
	_, err = os.Stat(successFile)
	assert.NoError(t, err, "Should have _SUCCESS marker")

	t.Log("✅ PySpark integration test passed")
}

// testWorkflowSimulation tests the complete workflow simulation
func testWorkflowSimulation(t *testing.T, env *TestEnvironment, inputFile string, controlData *types.ControlData, targetTable string) {
	t.Log("Testing complete workflow simulation...")

	// Simulate the complete FlakeRunner workflow steps
	workflowSteps := []struct {
		name string
		test func() error
	}{
		{
			name: "Initialize FlakeRunner",
			test: func() error {
				// We can't actually create FlakeRunner without AWS credentials,
				// but we can validate the configuration
				_, err := config.LoadConfig(env.ConfigPath)
				return err
			},
		},
		{
			name: "Process File",
			test: func() error {
				_, err := os.Stat(inputFile)
				return err
			},
		},
		{
			name: "Create Orchestration Record",
			test: func() error {
				// Simulate orchestration record creation
				now := time.Now()
				record := &types.FileOrchestrationRecord{
					File_path:               inputFile,
					Job_id:                  "workflow-sim-123",
					Batch_id:                controlData.BatchID,
					Orchestration_state:     "INITIATED",
					Processing_initiated_at: &now,
					Target_table:            targetTable,
					Control_data:            controlData,
				}
				return validateOrchestrationRecord(record)
			},
		},
		{
			name: "Validate Control Data",
			test: func() error {
				if controlData.RecordCount <= 0 {
					return fmt.Errorf("invalid record count")
				}
				if controlData.FileName == "" {
					return fmt.Errorf("missing file name")
				}
				return nil
			},
		},
		{
			name: "Determine Target Table",
			test: func() error {
				if targetTable == "" {
					return fmt.Errorf("target table not determined")
				}
				return nil
			},
		},
		{
			name: "Generate EMR Parameters",
			test: func() error {
				params := generateEMRJobParameters(inputFile, targetTable)
				if len(params) == 0 {
					return fmt.Errorf("failed to generate EMR parameters")
				}
				return nil
			},
		},
	}

	// Execute workflow steps
	for i, step := range workflowSteps {
		t.Run(fmt.Sprintf("Step_%d_%s", i+1, step.name), func(t *testing.T) {
			err := step.test()
			assert.NoError(t, err, "Workflow step should succeed: %s", step.name)
		})
	}

	t.Log("✅ Complete workflow simulation passed")
}

// testErrorScenarios tests various error scenarios
func testErrorScenarios(t *testing.T, env *TestEnvironment) {
	t.Log("Testing error scenarios...")

	errorTests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "Invalid_Configuration",
			test: func(t *testing.T) {
				// Test with invalid config path
				_, err := config.LoadConfig("/nonexistent/config.json")
				assert.Error(t, err, "Should fail with invalid config path")
			},
		},
		{
			name: "Missing_Input_File",
			test: func(t *testing.T) {
				nonexistentFile := filepath.Join(env.InputDir, "nonexistent.csv")
				_, err := os.Stat(nonexistentFile)
				assert.Error(t, err, "Should fail for nonexistent file")
			},
		},
		{
			name: "Invalid_Control_Data",
			test: func(t *testing.T) {
				invalidControlData := &types.ControlData{
					FileName:    "",
					RecordCount: -1,
					ColumnCount: 0,
				}
				err := validateControlData(invalidControlData)
				assert.Error(t, err, "Should fail with invalid control data")
			},
		},
		{
			name: "Unmapped_File_Prefix",
			test: func(t *testing.T) {
				unmappedPath := "unknown/unmapped_file.csv"
				var found bool
				for _, mapping := range env.Config.PrefixMappings {
					if strings.HasPrefix(unmappedPath, mapping.S3Prefix) {
						found = true
						break
					}
				}
				assert.False(t, found, "Should not find mapping for unmapped prefix")
			},
		},
	}

	for _, errorTest := range errorTests {
		t.Run(errorTest.name, errorTest.test)
	}

	t.Log("✅ Error scenario testing completed")
}

// testPerformanceScenarios tests performance with larger datasets
func testPerformanceScenarios(t *testing.T, env *TestEnvironment) {
	if testing.Short() {
		t.Skip("Skipping performance tests in short mode")
	}

	t.Log("Testing performance scenarios...")

	// Create larger dataset
	largeDataset := createDetailedCustomerData(500)
	inputFile := createTestInputFile(t, env.InputDir, "customers/large_dataset.csv", largeDataset)

	controlData := &types.ControlData{
		FileName:    "large_dataset.csv",
		FileSize:    int64(len(largeDataset)),
		RecordCount: 500,
		ColumnCount: 8,
		CreatedAt:   time.Now(),
		BatchID:     "perf-test-batch",
	}

	// Test file processing performance
	start := time.Now()
	recordCount, err := countCSVRecords(inputFile)
	processingTime := time.Since(start)

	assert.NoError(t, err, "Should count records successfully")
	assert.Equal(t, controlData.RecordCount+1, recordCount, "Should count correct number of records (including header)")
	assert.Less(t, processingTime, time.Second*5, "Processing should complete within 5 seconds")

	t.Logf("✅ Performance test completed: %d records processed in %v", recordCount, processingTime)
}

// Helper functions

// createTestInputFile creates a test input file
func createTestInputFile(t *testing.T, baseDir, filePath, data string) string {
	fullPath := filepath.Join(baseDir, filePath)

	// Create directory if it doesn't exist
	dir := filepath.Dir(fullPath)
	err := os.MkdirAll(dir, 0755)
	require.NoError(t, err)

	// Write data to file
	err = ioutil.WriteFile(fullPath, []byte(data), 0644)
	require.NoError(t, err)

	return fullPath
}

// createDetailedCustomerData creates detailed customer test data
func createDetailedCustomerData(recordCount int) string {
	var builder strings.Builder
	builder.WriteString("customer_id,name,email,phone,city,state,country,created_at\n")

	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"}
	states := []string{"NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA"}
	countries := []string{"USA", "USA", "USA", "USA", "USA", "USA", "USA", "USA"}

	for i := 1; i <= recordCount; i++ {
		cityIndex := (i - 1) % len(cities)
		createdAt := time.Now().AddDate(0, 0, -i).Format("2006-01-02T15:04:05Z")

		builder.WriteString(fmt.Sprintf("%d,Customer %d,customer%d@example.com,555-%04d,%s,%s,%s,%s\n",
			i, i, i, 1000+i, cities[cityIndex], states[cityIndex], countries[cityIndex], createdAt))
	}

	return builder.String()
}

// createDetailedOrderData creates detailed order test data
func createDetailedOrderData(recordCount int) string {
	var builder strings.Builder
	builder.WriteString("order_id,customer_id,order_date,amount,status,product_count,created_at\n")

	statuses := []string{"completed", "pending", "shipped", "cancelled", "processing"}

	for i := 1; i <= recordCount; i++ {
		customerID := ((i - 1) % 20) + 1 // Cycle through 20 customers
		amount := float64(25+(i%500)) + 0.99
		statusIndex := (i - 1) % len(statuses)
		productCount := (i % 5) + 1
		orderDate := time.Now().AddDate(0, 0, -i).Format("2006-01-02")
		createdAt := time.Now().AddDate(0, 0, -i).Format("2006-01-02T15:04:05Z")

		builder.WriteString(fmt.Sprintf("%d,%d,%s,%.2f,%s,%d,%s\n",
			1000+i, customerID, orderDate, amount, statuses[statusIndex], productCount, createdAt))
	}

	return builder.String()
}

// countCSVRecords counts records in a CSV file
func countCSVRecords(filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var count int64 = 0
	// Simple line counting
	buf := make([]byte, 32*1024)
	for {
		c, err := file.Read(buf)
		if err != nil {
			break
		}
		for _, b := range buf[:c] {
			if b == '\n' {
				count++
			}
		}
	}

	return count, nil
}

// detectFileFormat detects file format based on extension
func detectFileFormat(filePath string) string {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".csv":
		return "CSV"
	case ".json":
		return "JSON"
	case ".parquet":
		return "PARQUET"
	default:
		return "UNKNOWN"
	}
}

// validateOrchestrationRecord validates an orchestration record
func validateOrchestrationRecord(record *types.FileOrchestrationRecord) error {
	if record.File_path == "" {
		return fmt.Errorf("file path is required")
	}
	if record.Job_id == "" {
		return fmt.Errorf("job ID is required")
	}
	if record.Target_table == "" {
		return fmt.Errorf("target table is required")
	}
	return nil
}

// validateControlData validates control data
func validateControlData(controlData *types.ControlData) error {
	if controlData.FileName == "" {
		return fmt.Errorf("file name is required")
	}
	if controlData.RecordCount <= 0 {
		return fmt.Errorf("record count must be positive")
	}
	if controlData.ColumnCount <= 0 {
		return fmt.Errorf("column count must be positive")
	}
	return nil
}
