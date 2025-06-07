package test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/allen13/flake-runner/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DockerIntegrationTest tests the complete flow with actual Docker execution
func TestDockerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Docker integration tests in short mode")
	}

	if !isDockerRunning() {
		t.Skip("Docker is not running, skipping Docker integration tests")
	}

	if !isFlakeRunnerImageAvailable() {
		t.Skip("flake-runner:local image not available, skipping Docker integration tests")
	}

	// Setup test environment
	testDir, cleanup := setupDockerTestEnvironment(t)
	defer cleanup()

	// Test scenarios with Docker execution
	testCases := []DockerTestCase{
		{
			Name:      "Customer_Data_Docker_Processing",
			InputData: createCustomerTestData(50),
			ControlData: &types.ControlData{
				FileName:    "customers_docker.csv",
				FileSize:    2000,
				RecordCount: 50,
				ColumnCount: 6,
				CreatedAt:   time.Now(),
				BatchID:     "docker-batch-001",
			},
			TargetTable:     "CUSTOMERS",
			OutputFormat:    "PARQUET",
			ExpectedRecords: 50,
		},
		{
			Name:      "Order_Data_Docker_Processing",
			InputData: createOrderTestData(30),
			ControlData: &types.ControlData{
				FileName:    "orders_docker.csv",
				FileSize:    1500,
				RecordCount: 30,
				ColumnCount: 5,
				CreatedAt:   time.Now(),
				BatchID:     "docker-batch-002",
			},
			TargetTable:     "ORDERS",
			OutputFormat:    "JSON",
			ExpectedRecords: 30,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			runDockerIntegrationTest(t, testDir, testCase)
		})
	}
}

// DockerTestCase represents a test case for Docker integration
type DockerTestCase struct {
	Name            string
	InputData       string
	ControlData     *types.ControlData
	TargetTable     string
	OutputFormat    string
	ExpectedRecords int64
}

// setupDockerTestEnvironment creates a test environment for Docker integration
func setupDockerTestEnvironment(t *testing.T) (string, func()) {
	testDir, err := ioutil.TempDir("", "flake-runner-docker-*")
	require.NoError(t, err)

	// Create directory structure
	dirs := []string{"input", "output", "config"}
	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(testDir, dir), 0755)
		require.NoError(t, err)
	}

	cleanup := func() {
		os.RemoveAll(testDir)
	}

	return testDir, cleanup
}

// runDockerIntegrationTest executes a complete Docker-based integration test
func runDockerIntegrationTest(t *testing.T, testDir string, testCase DockerTestCase) {
	inputDir := filepath.Join(testDir, "input")
	outputDir := filepath.Join(testDir, "output")

	// Create input file
	inputFile := filepath.Join(inputDir, testCase.ControlData.FileName)
	err := ioutil.WriteFile(inputFile, []byte(testCase.InputData), 0644)
	require.NoError(t, err)

	// Create output directory for this test case
	testOutputDir := filepath.Join(outputDir, strings.ToLower(testCase.Name))
	err = os.MkdirAll(testOutputDir, 0755)
	require.NoError(t, err)

	// Step 1: Test Go-level file processing
	t.Run("Go_File_Processing", func(t *testing.T) {
		testGoFileProcessing(t, inputFile, testCase.ControlData)
	})

	// Step 2: Test PySpark script execution with Docker
	t.Run("Docker_PySpark_Execution", func(t *testing.T) {
		testDockerPySparkExecution(t, inputDir, testOutputDir, testCase)
	})

	// Step 3: Validate complete integration
	t.Run("Integration_Validation", func(t *testing.T) {
		validateDockerIntegration(t, testOutputDir, testCase)
	})
}

// testGoFileProcessing tests the Go-level file processing
func testGoFileProcessing(t *testing.T, inputFile string, controlData *types.ControlData) {
	t.Log("Testing Go file processing functions...")

	// Test file existence
	fileInfo, err := os.Stat(inputFile)
	require.NoError(t, err, "Input file should exist")
	assert.Greater(t, fileInfo.Size(), int64(0), "File should have content")

	// Test control data validation
	assert.NotNil(t, controlData, "Control data should be provided")
	assert.NotEmpty(t, controlData.FileName, "File name should not be empty")
	assert.Greater(t, controlData.RecordCount, int64(0), "Record count should be positive")
	assert.Greater(t, controlData.ColumnCount, 0, "Column count should be positive")

	// Test record counting (simulate what FlakeRunner would do)
	actualRecordCount, err := countRecordsInFile(inputFile)
	require.NoError(t, err, "Should be able to count records")

	// Allow for header row difference
	expectedCount := controlData.RecordCount
	assert.True(t, actualRecordCount == expectedCount || actualRecordCount == expectedCount+1,
		"Record count should match control data (with or without header): expected %d, got %d",
		expectedCount, actualRecordCount)

	t.Log("✅ Go file processing validation passed")
}

// testDockerPySparkExecution tests PySpark script execution using Docker
func testDockerPySparkExecution(t *testing.T, inputDir, outputDir string, testCase DockerTestCase) {
	t.Log("Testing PySpark execution with Docker...")

	// Prepare Docker command
	containerInputPath := "/data"
	containerOutputPath := "/output"
	jobID := fmt.Sprintf("docker-test-%d", time.Now().Unix())

	dockerCmd := []string{
		"docker", "run", "--rm",
		"--entrypoint", "python3",
		"-v", fmt.Sprintf("%s:%s", inputDir, containerInputPath),
		"-v", fmt.Sprintf("%s:%s", outputDir, containerOutputPath),
		"flake-runner:local",
		"/opt/spark/jobs/simple_processor.py",
		"--input-path", fmt.Sprintf("%s/%s", containerInputPath, testCase.ControlData.FileName),
		"--output-path", containerOutputPath,
		"--input-format", "CSV",
		"--output-format", testCase.OutputFormat,
		"--job-id", jobID,
	}

	// Execute Docker command
	t.Logf("Executing: %s", strings.Join(dockerCmd, " "))

	cmd := exec.Command(dockerCmd[0], dockerCmd[1:]...)
	output, err := cmd.CombinedOutput()

	t.Logf("Docker output:\n%s", string(output))

	if err != nil {
		t.Errorf("Docker execution failed: %v\nOutput: %s", err, string(output))
		return
	}

	// Verify Docker execution completed successfully
	assert.Contains(t, string(output), "Processing completed successfully",
		"Docker execution should complete successfully")
	assert.Contains(t, string(output), fmt.Sprintf("Records processed: %d", testCase.ExpectedRecords),
		"Should process expected number of records")

	t.Log("✅ Docker PySpark execution completed")
}

// validateDockerIntegration validates the complete Docker integration
func validateDockerIntegration(t *testing.T, outputDir string, testCase DockerTestCase) {
	t.Log("Validating Docker integration results...")

	// Check if output files were created
	files, err := ioutil.ReadDir(outputDir)
	require.NoError(t, err, "Should be able to read output directory")
	assert.Greater(t, len(files), 0, "Should have output files")

	// Look for success marker
	successFile := filepath.Join(outputDir, "_SUCCESS")
	_, err = os.Stat(successFile)
	assert.NoError(t, err, "Should have _SUCCESS marker file")

	// Find and validate data files in the data subdirectory
	dataDir := filepath.Join(outputDir, "data")
	var dataFiles []string

	if dataExists, err := os.Stat(dataDir); err == nil && dataExists.IsDir() {
		dataFileInfos, err := ioutil.ReadDir(dataDir)
		require.NoError(t, err, "Should read data directory")

		for _, file := range dataFileInfos {
			if strings.HasPrefix(file.Name(), "part-") {
				dataFiles = append(dataFiles, file.Name())
			}
		}
	}

	assert.Greater(t, len(dataFiles), 0, "Should have at least one data file in data subdirectory")

	// Validate first data file content
	if len(dataFiles) > 0 {
		validateDataFileContent(t, filepath.Join(dataDir, dataFiles[0]), testCase)
	}

	t.Log("✅ Docker integration validation completed")
}

// validateDataFileContent validates the content of a data file
func validateDataFileContent(t *testing.T, filePath string, testCase DockerTestCase) {
	t.Logf("Validating data file content: %s", filePath)

	switch testCase.OutputFormat {
	case "JSON":
		validateJSONOutput(t, filePath, testCase)
	case "PARQUET":
		validateParquetOutput(t, filePath, testCase)
	default:
		t.Logf("Skipping content validation for format: %s", testCase.OutputFormat)
	}
}

// validateJSONOutput validates JSON output file
func validateJSONOutput(t *testing.T, filePath string, testCase DockerTestCase) {
	file, err := os.Open(filePath)
	require.NoError(t, err, "Should be able to open JSON file")
	defer file.Close()

	scanner := bufio.NewScanner(file)
	recordCount := 0

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		// Parse JSON line
		var record map[string]interface{}
		err := json.Unmarshal([]byte(line), &record)
		require.NoError(t, err, "Should be able to parse JSON record")

		// Validate metadata fields were added
		assert.Contains(t, record, "_processing_timestamp", "Should have processing timestamp")
		assert.Contains(t, record, "_job_id", "Should have job ID")
		assert.Contains(t, record, "_source_system", "Should have source system")

		// Validate source system
		assert.Equal(t, "flake-runner-local", record["_source_system"],
			"Source system should be flake-runner-local")

		recordCount++

		// Validate original data based on target table
		switch testCase.TargetTable {
		case "CUSTOMERS":
			assert.Contains(t, record, "customer_id", "Customer record should have customer_id")
			assert.Contains(t, record, "name", "Customer record should have name")
			assert.Contains(t, record, "email", "Customer record should have email")
		case "ORDERS":
			assert.Contains(t, record, "order_id", "Order record should have order_id")
			assert.Contains(t, record, "customer_id", "Order record should have customer_id")
			assert.Contains(t, record, "amount", "Order record should have amount")
		}

		// Only validate first few records to avoid long test times
		if recordCount >= 3 {
			break
		}
	}

	require.NoError(t, scanner.Err(), "Should scan file without errors")
	assert.Greater(t, recordCount, 0, "Should have processed records")

	t.Logf("✅ Validated %d JSON records", recordCount)
}

// validateParquetOutput validates Parquet output (basic validation)
func validateParquetOutput(t *testing.T, filePath string, testCase DockerTestCase) {
	// For Parquet files, we'll just check that the file exists and has content
	fileInfo, err := os.Stat(filePath)
	require.NoError(t, err, "Parquet file should exist")
	assert.Greater(t, fileInfo.Size(), int64(0), "Parquet file should have content")

	t.Logf("✅ Validated Parquet file: %s (size: %d bytes)",
		filepath.Base(filePath), fileInfo.Size())
}

// Helper functions

// isDockerRunning checks if Docker is running
func isDockerRunning() bool {
	cmd := exec.Command("docker", "info")
	err := cmd.Run()
	return err == nil
}

// isFlakeRunnerImageAvailable checks if the flake-runner image is available
func isFlakeRunnerImageAvailable() bool {
	cmd := exec.Command("docker", "images", "--format", "{{.Repository}}:{{.Tag}}", "flake-runner:local")
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.Contains(string(output), "flake-runner:local")
}

// countRecordsInFile counts records in a CSV file
func countRecordsInFile(filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var count int64 = 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			count++
		}
	}

	return count, scanner.Err()
}

// createCustomerTestData generates test customer data
func createCustomerTestData(recordCount int) string {
	var builder strings.Builder
	builder.WriteString("customer_id,name,email,phone,city,state\n")

	cities := []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia"}
	states := []string{"NY", "CA", "IL", "TX", "AZ", "PA"}

	for i := 1; i <= recordCount; i++ {
		cityIndex := (i - 1) % len(cities)
		builder.WriteString(fmt.Sprintf("%d,Customer %d,customer%d@example.com,555-%04d,%s,%s\n",
			i, i, i, 1000+i, cities[cityIndex], states[cityIndex]))
	}

	return builder.String()
}

// createOrderTestData generates test order data
func createOrderTestData(recordCount int) string {
	var builder strings.Builder
	builder.WriteString("order_id,customer_id,order_date,amount,status\n")

	statuses := []string{"completed", "pending", "shipped", "cancelled"}

	for i := 1; i <= recordCount; i++ {
		customerID := ((i - 1) % 10) + 1     // Cycle through 10 customers
		amount := float64(50+(i%200)) + 0.99 // Amounts from $50.99 to $249.99
		statusIndex := (i - 1) % len(statuses)

		builder.WriteString(fmt.Sprintf("%d,%d,2024-01-%02d,%.2f,%s\n",
			1000+i, customerID, (i%28)+1, amount, statuses[statusIndex]))
	}

	return builder.String()
}

// Performance test for Docker integration
func BenchmarkDockerIntegration(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping Docker benchmark in short mode")
	}

	if !isDockerRunning() || !isFlakeRunnerImageAvailable() {
		b.Skip("Docker or flake-runner image not available")
	}

	testDir, cleanup := setupDockerTestEnvironment(&testing.T{})
	defer cleanup()

	// Create benchmark test case
	testCase := DockerTestCase{
		Name:      "Benchmark_Customer_Processing",
		InputData: createCustomerTestData(100),
		ControlData: &types.ControlData{
			FileName:    "benchmark_customers.csv",
			RecordCount: 100,
			ColumnCount: 6,
		},
		TargetTable:     "CUSTOMERS",
		OutputFormat:    "PARQUET",
		ExpectedRecords: 100,
	}

	inputDir := filepath.Join(testDir, "input")
	outputDir := filepath.Join(testDir, "output")

	// Create input file
	inputFile := filepath.Join(inputDir, testCase.ControlData.FileName)
	err := ioutil.WriteFile(inputFile, []byte(testCase.InputData), 0644)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		testOutputDir := filepath.Join(outputDir, fmt.Sprintf("benchmark_%d", i))
		os.MkdirAll(testOutputDir, 0755)

		// Run Docker execution
		executeDockerBenchmark(b, inputDir, testOutputDir, testCase)

		// Cleanup for next iteration
		os.RemoveAll(testOutputDir)
	}
}

// executeDockerBenchmark executes Docker command for benchmarking
func executeDockerBenchmark(b *testing.B, inputDir, outputDir string, testCase DockerTestCase) {
	dockerCmd := []string{
		"docker", "run", "--rm",
		"--entrypoint", "python3",
		"-v", fmt.Sprintf("%s:/data", inputDir),
		"-v", fmt.Sprintf("%s:/output", outputDir),
		"flake-runner:local",
		"/opt/spark/jobs/simple_processor.py",
		"--input-path", fmt.Sprintf("/data/%s", testCase.ControlData.FileName),
		"--output-path", "/output",
		"--input-format", "CSV",
		"--output-format", testCase.OutputFormat,
		"--job-id", fmt.Sprintf("benchmark-%d", time.Now().UnixNano()),
	}

	cmd := exec.Command(dockerCmd[0], dockerCmd[1:]...)
	_, err := cmd.CombinedOutput()
	if err != nil {
		b.Errorf("Docker benchmark failed: %v", err)
	}
}
