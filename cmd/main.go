package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/allen13/flake-runner/pkg/flakerunner"
	"github.com/allen13/flake-runner/pkg/types"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		return
	}

	command := os.Args[1]

	switch command {
	case "init":
		initCommand()
	case "process":
		processCommand()
	case "status":
		statusCommand()
	case "emr":
		emrCommand()
	case "workflow":
		workflowCommand()
	case "aws":
		awsCommand()
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

// initCommand handles the initialization command
func initCommand() {
	initFlags := flag.NewFlagSet("init", flag.ExitOnError)
	var configPath string
	initFlags.StringVar(&configPath, "config", "config.json", "Path to configuration file")
	initFlags.Parse(os.Args[2:])

	// Create FlakeRunner instance
	fr, err := flakerunner.NewFlakeRunner(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create FlakeRunner: %v\n", err)
		os.Exit(1)
	}

	// Validate configuration and AWS resources
	fr.ValidateConfiguration()
	if fr.HasError() {
		fmt.Fprintf(os.Stderr, "Configuration validation failed: %v\n", fr.GetError())
		os.Exit(1)
	}

	fr.ValidateAWSResources()
	if fr.HasError() {
		fmt.Fprintf(os.Stderr, "AWS resources validation failed: %v\n", fr.GetError())
		os.Exit(1)
	}

	fmt.Println("✅ Flake Runner initialized successfully!")
	fmt.Printf("📊 Job ID: %s\n", fr.JobID)
	fmt.Printf("🗂️  Configured with %d prefix mappings:\n", len(fr.Config.PrefixMappings))

	for _, mapping := range fr.Config.PrefixMappings {
		fmt.Printf("   • %s → %s\n", mapping.S3Prefix, mapping.TargetName)
	}

	fmt.Println("🚀 Ready to process files!")
}

// processCommand handles the process command
func processCommand() {
	processFlags := flag.NewFlagSet("process", flag.ExitOnError)
	var configPath, filePath, controlDataJSON string
	var validateOnly, waitForCompletion bool
	var timeoutMinutes, pollIntervalSeconds int
	processFlags.StringVar(&configPath, "config", "config.json", "Path to configuration file")
	processFlags.StringVar(&filePath, "file", "", "S3 file path to process")
	processFlags.StringVar(&controlDataJSON, "control-data", "", "Control data as JSON string")
	processFlags.BoolVar(&validateOnly, "validate-only", false, "Only validate files without processing")
	processFlags.BoolVar(&waitForCompletion, "wait", false, "Wait for job completion and poll status")
	processFlags.IntVar(&timeoutMinutes, "timeout", 120, "Maximum time to wait for completion in minutes")
	processFlags.IntVar(&pollIntervalSeconds, "poll-interval", 30, "Polling interval in seconds")
	processFlags.Parse(os.Args[2:])

	if filePath == "" {
		fmt.Fprintf(os.Stderr, "Error: File path is required for processing\n")
		os.Exit(1)
	}

	// Parse control data if provided
	var controlData *types.ControlData
	if controlDataJSON != "" {
		controlData = &types.ControlData{}
		if err := json.Unmarshal([]byte(controlDataJSON), controlData); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing control data JSON: %v\n", err)
			os.Exit(1)
		}
	}

	// Create FlakeRunner instance
	fr, err := flakerunner.NewFlakeRunner(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create FlakeRunner: %v\n", err)
		os.Exit(1)
	}

	// Validate configuration
	fr.ValidateConfiguration().ValidateAWSResources()
	if fr.HasError() {
		fmt.Fprintf(os.Stderr, "Validation failed: %v\n", fr.GetError())
		os.Exit(1)
	}

	// Process the file
	if validateOnly {
		fmt.Printf("Validating file: %s\n", filePath)
		fr.ProcessInputFile(filePath)
		if controlData != nil {
			fr.ValidateWithControlData(filePath, controlData)
		}
		if fr.HasError() {
			fmt.Printf("❌ Validation failed: %v\n", fr.GetError())
		} else {
			fmt.Println("✅ Validation completed successfully!")
		}
	} else {
		fmt.Printf("Processing file: %s\n", filePath)
		if controlData != nil {
			fmt.Printf("Using provided control data for validation\n")
		}
		fr.ProcessFile(filePath, controlData)
		if fr.HasError() {
			fmt.Printf("❌ Processing failed: %v\n", fr.GetError())
			os.Exit(1)
		} else {
			fmt.Println("✅ File processing initiated successfully!")
			fmt.Printf("📊 Job ID: %s\n", fr.JobID)

			if waitForCompletion {
				fmt.Printf("⏳ Waiting for job completion (timeout: %d minutes, poll interval: %d seconds)...\n",
					timeoutMinutes, pollIntervalSeconds)

				success := pollJobCompletion(fr, filePath, timeoutMinutes, pollIntervalSeconds)
				if !success {
					os.Exit(1)
				}
			}
		}
	}
}

// statusCommand handles the status command
func statusCommand() {
	fmt.Println("Status command not implemented yet")
}

// emrCommand handles the EMR command
func emrCommand() {
	emrFlags := flag.NewFlagSet("emr", flag.ExitOnError)
	var configPath, action, jobRunID, filePath string
	emrFlags.StringVar(&configPath, "config", "config.json", "Path to configuration file")
	emrFlags.StringVar(&action, "action", "", "Action to perform (logs, status, cancel)")
	emrFlags.StringVar(&jobRunID, "job-run-id", "", "EMR job run ID")
	emrFlags.StringVar(&filePath, "file", "", "File path associated with the EMR job")
	emrFlags.Parse(os.Args[2:])

	if action == "" {
		fmt.Fprintf(os.Stderr, "Error: Action is required\n")
		os.Exit(1)
	}

	// Create FlakeRunner instance
	fr, err := flakerunner.NewFlakeRunner(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create FlakeRunner: %v\n", err)
		os.Exit(1)
	}

	// Validate configuration
	fr.ValidateConfiguration()
	if fr.HasError() {
		fmt.Fprintf(os.Stderr, "Configuration validation failed: %v\n", fr.GetError())
		os.Exit(1)
	}

	switch action {
	case "logs":
		if filePath == "" {
			fmt.Fprintf(os.Stderr, "Error: File path is required for logs action\n")
			os.Exit(1)
		}
		logs, err := fr.GetJobLogs(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get job logs: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(logs)

	case "status":
		if jobRunID == "" {
			fmt.Fprintf(os.Stderr, "Error: Job run ID is required for status action\n")
			os.Exit(1)
		}
		jobRun, err := fr.GetJobStatus(jobRunID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to get job status: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Job Run ID: %s\n", jobRunID)
		fmt.Printf("State: %s\n", jobRun.State)
		if jobRun.StateDetails != nil {
			fmt.Printf("State Details: %s\n", *jobRun.StateDetails)
		}
		if jobRun.CreatedAt != nil {
			fmt.Printf("Created At: %s\n", jobRun.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		if jobRun.UpdatedAt != nil {
			fmt.Printf("Updated At: %s\n", jobRun.UpdatedAt.Format("2006-01-02 15:04:05"))
		}

	case "cancel":
		if filePath == "" {
			fmt.Fprintf(os.Stderr, "Error: File path is required for cancel action\n")
			os.Exit(1)
		}
		fr.CancelJob(filePath)
		if fr.HasError() {
			fmt.Fprintf(os.Stderr, "Failed to cancel job: %v\n", fr.GetError())
			os.Exit(1)
		}
		fmt.Println("✅ Job cancellation requested successfully")

	default:
		fmt.Fprintf(os.Stderr, "Unknown action: %s\n", action)
		os.Exit(1)
	}
}

// workflowCommand handles the workflow command
func workflowCommand() {
	fmt.Println("Workflow command not implemented yet")
}

// printUsage prints the usage information
func printUsage() {
	fmt.Println("Flake Runner - S3-EMR Serverless Data Pipeline Orchestrator")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  flake-runner <command> [options]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  init                 Initialize and validate configuration")
	fmt.Println("  process              Process files through the pipeline")
	fmt.Println("  status               Query file processing status")
	fmt.Println("  emr                  EMR Serverless job operations")
	fmt.Println("  workflow             Advanced workflow orchestration with fluent API")
	fmt.Println("  aws                  AWS resource management (list, create, upload, download)")
	fmt.Println("  help                 Show this help message")
	fmt.Println()
	fmt.Println("Init options:")
	fmt.Println("  -config string       Path to configuration file (default: config.json)")
	fmt.Println()
	fmt.Println("Process options:")
	fmt.Println("  -config string       Path to configuration file (default: config.json)")
	fmt.Println("  -file string         S3 file path to process (e.g., s3://bucket/prefix/file.csv)")
	fmt.Println("  -control-data string Control data as JSON string (optional)")
	fmt.Println("  -wait               Wait for job completion and poll status")
	fmt.Println("  -timeout int        Maximum time to wait for completion in minutes (default: 120)")
	fmt.Println("  -poll-interval int  Polling interval in seconds (default: 30)")
	fmt.Println("  -list               List files in input bucket with prefix mappings")
	fmt.Println("  -validate-only      Only validate files without processing")
	fmt.Println()
	fmt.Println("Status options:")
	fmt.Println("  -config string       Path to configuration file (default: config.json)")
	fmt.Println("  -file string         Check status of specific file")
	fmt.Println("  -job string          Get summary for specific job ID")
	fmt.Println("  -batch string        Get summary for specific batch ID")
	fmt.Println("  -state string        Get all files in specific state (e.g., PROCESSING, FAILED)")
	fmt.Println()
	fmt.Println("EMR options:")
	fmt.Println("  -config string       Path to configuration file (default: config.json)")
	fmt.Println("  -action string       Action to perform (logs, status, cancel)")
	fmt.Println("  -job-run-id string   EMR job run ID")
	fmt.Println("  -file string         File path associated with the EMR job")
	fmt.Println()
	fmt.Println("Workflow options:")
	fmt.Println("  -config string       Path to configuration file (default: config.json)")
	fmt.Println("  -file string         S3 file path to process")
	fmt.Println("  -max-retries int     Maximum number of retries (default: 3)")
	fmt.Println("  -timeout int         Workflow timeout in minutes (default: 120)")
	fmt.Println("  -skip-validation     Skip data validation steps")
	fmt.Println("  -continue-on-error   Continue workflow despite errors")
	fmt.Println("  -generate-report     Generate processing report (default: true)")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  flake-runner init -config production.json")
	fmt.Println("  flake-runner process -list")
	fmt.Println("  flake-runner process -file s3://my-bucket/customers/data.csv")
	fmt.Println("  flake-runner process -file s3://my-bucket/orders/orders.parquet -validate-only")
	fmt.Println("  flake-runner process -file s3://my-bucket/customers/data.csv -wait")
	fmt.Println("  flake-runner process -file s3://my-bucket/customers/data.csv -wait -timeout 60 -poll-interval 15")
	fmt.Println(`  flake-runner process -file s3://my-bucket/customers/data.csv -control-data '{"file_name":"data.csv","file_size":1024,"file_hash":"abc123","record_count":100,"column_count":5,"created_at":"2024-01-01T10:00:00Z"}' -wait`)
	fmt.Println("  flake-runner status -file s3://my-bucket/customers/data.csv")
	fmt.Println("  flake-runner status -job job-abc12345")
	fmt.Println("  flake-runner status -state FAILED")
	fmt.Println("  flake-runner emr -action status -job-run-id 00f1qa2lmnmnhj1n")
	fmt.Println("  flake-runner emr -action logs -file s3://my-bucket/customers/data.csv")
	fmt.Println("  flake-runner emr -action cancel -job-run-id 00f1qa2lmnmnhj1n")
	fmt.Println("  flake-runner workflow -file s3://my-bucket/customers/data.csv")
	fmt.Println("  flake-runner workflow -file s3://my-bucket/orders/data.parquet -max-retries 5 -timeout 180")
	fmt.Println("  flake-runner workflow -file s3://my-bucket/products/data.json -skip-validation -continue-on-error")
}

// pollJobCompletion polls the job status until completion or timeout
func pollJobCompletion(fr *flakerunner.FlakeRunner, filePath string, timeoutMinutes, pollIntervalSeconds int) bool {
	timeout := time.Duration(timeoutMinutes) * time.Minute
	pollInterval := time.Duration(pollIntervalSeconds) * time.Second
	startTime := time.Now()
	deadline := startTime.Add(timeout)

	fmt.Printf("📈 Starting job polling at %s\n", startTime.Format("15:04:05"))

	for time.Now().Before(deadline) {
		// Get current orchestration record
		record, err := getOrchestrationRecord(fr, filePath)
		if err != nil {
			fmt.Printf("⚠️  Warning: Could not get orchestration record: %v\n", err)
			time.Sleep(pollInterval)
			continue
		}

		// Display current status
		elapsed := time.Since(startTime)
		fmt.Printf("[%s] State: %s - %s\n",
			elapsed.Round(time.Second),
			record.Orchestration_state,
			record.Current_stage)

		// Check for completion states
		switch record.Orchestration_state {
		case "COMPLETED":
			fmt.Printf("🎉 Job completed successfully in %v!\n", elapsed.Round(time.Second))
			printJobSummary(record)
			return true

		case "PROCESSED":
			fmt.Printf("🎉 Job processing completed successfully in %v!\n", elapsed.Round(time.Second))
			fmt.Printf("📝 EMR processing finished, job is now ready for further steps\n")
			printJobSummary(record)
			return true

		case "FAILED":
			fmt.Printf("❌ Job failed after %v\n", elapsed.Round(time.Second))
			printJobSummary(record)
			printErrorDetails(record)
			return false

		case "CANCELLED":
			fmt.Printf("🛑 Job was cancelled after %v\n", elapsed.Round(time.Second))
			printJobSummary(record)
			return false

		case "PROCESSING":
			// Check EMR job status if we have a job run ID
			if jobRunID := getEMRJobRunID(fr, filePath); jobRunID != "" {
				if handled := checkAndHandleEMRCompletion(fr, filePath, jobRunID); handled {
					// EMR job completed, continue polling to see updated orchestration state
					continue
				}
				printEMRStatus(fr, jobRunID)
			}
		}

		// Wait before next poll
		time.Sleep(pollInterval)
	}

	// Timeout reached
	elapsed := time.Since(startTime)
	fmt.Printf("⏰ Timeout reached after %v (limit: %v)\n", elapsed.Round(time.Second), timeout)

	// Get final status
	if record, err := getOrchestrationRecord(fr, filePath); err == nil {
		fmt.Printf("Final state: %s - %s\n", record.Orchestration_state, record.Current_stage)
		printJobSummary(record)
	}

	return false
}

// getOrchestrationRecord retrieves the current orchestration record
func getOrchestrationRecord(fr *flakerunner.FlakeRunner, filePath string) (*types.FileOrchestrationRecord, error) {
	// Refresh the record from DynamoDB to get the latest state
	fr.RefreshOrchestrationRecord(filePath)
	if fr.HasError() {
		return nil, fr.GetError()
	}

	// Get the refreshed record from local context
	records := fr.GetFileOrchestrations()
	for _, record := range records {
		if record.File_path == filePath && record.Job_id == fr.JobID {
			return &record, nil
		}
	}
	return nil, fmt.Errorf("orchestration record not found for file: %s", filePath)
}

// getEMRJobRunID gets the EMR job run ID for a file
func getEMRJobRunID(fr *flakerunner.FlakeRunner, filePath string) string {
	jobRuns := fr.GetEMRJobRuns()
	if jobRunID, exists := jobRuns[filePath]; exists {
		return jobRunID
	}
	return ""
}

// printEMRStatus prints current EMR job status
func printEMRStatus(fr *flakerunner.FlakeRunner, jobRunID string) {
	jobRun, err := fr.GetJobStatus(jobRunID)
	if err != nil {
		fmt.Printf("   ⚠️  Could not get EMR status: %v\n", err)
		return
	}

	fmt.Printf("   📊 EMR Job: %s\n", jobRun.State)
	if jobRun.StateDetails != nil && *jobRun.StateDetails != "" {
		fmt.Printf("   📝 Details: %s\n", *jobRun.StateDetails)
	}
}

// checkAndHandleEMRCompletion checks if EMR job is complete and handles orchestration state updates
func checkAndHandleEMRCompletion(fr *flakerunner.FlakeRunner, filePath, jobRunID string) bool {
	jobRun, err := fr.GetJobStatus(jobRunID)
	if err != nil {
		fmt.Printf("   ⚠️  Could not get EMR status: %v\n", err)
		return false
	}

	fmt.Printf("   📊 EMR Job: %s\n", jobRun.State)
	if jobRun.StateDetails != nil && *jobRun.StateDetails != "" {
		fmt.Printf("   📝 Details: %s\n", *jobRun.StateDetails)
	}

	// Handle EMR job completion
	switch jobRun.State {
	case "SUCCESS":
		fmt.Printf("   ✅ EMR job completed successfully, updating orchestration state...\n")
		fr.UpdateFileOrchestrationState(filePath, "PROCESSED")
		if fr.HasError() {
			fmt.Printf("   ⚠️  Warning: Could not update orchestration state: %v\n", fr.GetError())
		} else {
			fmt.Printf("   📝 Orchestration state updated to PROCESSED\n")
		}
		return true

	case "FAILED", "CANCELLED":
		fmt.Printf("   ❌ EMR job failed, updating orchestration state...\n")
		errorDetails := "EMR job failed"
		if jobRun.StateDetails != nil && *jobRun.StateDetails != "" {
			errorDetails = *jobRun.StateDetails
		}
		fr.RecordProcessingError(filePath, "emr_processing", "EMR_JOB_FAILED", errorDetails, false)
		if fr.HasError() {
			fmt.Printf("   ⚠️  Warning: Could not update orchestration state: %v\n", fr.GetError())
		} else {
			fmt.Printf("   📝 Orchestration state updated to FAILED\n")
		}
		return true

	default:
		// Job still running or in other state
		return false
	}
}

// printJobSummary prints a summary of the job results
func printJobSummary(record *types.FileOrchestrationRecord) {
	fmt.Println("\n📋 Job Summary:")
	fmt.Printf("   File: %s\n", record.File_name)
	fmt.Printf("   Target Table: %s\n", record.Target_table)
	fmt.Printf("   Batch ID: %s\n", record.Batch_id)

	if record.Processing_initiated_at != nil {
		fmt.Printf("   Started: %s\n", record.Processing_initiated_at.Format("2006-01-02 15:04:05"))
	}

	if record.Processing_completed != nil {
		fmt.Printf("   Completed: %s\n", record.Processing_completed.Format("2006-01-02 15:04:05"))
		if record.Processing_initiated_at != nil {
			duration := record.Processing_completed.Sub(*record.Processing_initiated_at)
			fmt.Printf("   Duration: %v\n", duration.Round(time.Second))
		}
	}

	// Print validation results
	if record.Validation_results.ExpectedRecords > 0 || record.Validation_results.ActualRecords > 0 {
		fmt.Println("\n🔍 Validation Results:")
		if record.Validation_results.ExpectedRecords > 0 {
			fmt.Printf("   Expected Records: %d\n", record.Validation_results.ExpectedRecords)
		}
		if record.Validation_results.ActualRecords > 0 {
			fmt.Printf("   Actual Records: %d\n", record.Validation_results.ActualRecords)
		}
		fmt.Printf("   Record Count Match: %v\n", record.Validation_results.RecordCountMatch)
		fmt.Printf("   File Size Match: %v\n", record.Validation_results.FileSizeMatch)
		fmt.Printf("   Checksum Match: %v\n", record.Validation_results.ChecksumMatch)
	}

	// Print EMR job details
	if record.Emr_job_details.JobRunID != "" {
		fmt.Println("\n⚡ EMR Job Details:")
		fmt.Printf("   Job Run ID: %s\n", record.Emr_job_details.JobRunID)
		fmt.Printf("   Status: %s\n", record.Emr_job_details.Status)
		if record.Emr_job_details.ProcessedRecords > 0 {
			fmt.Printf("   Processed Records: %d\n", record.Emr_job_details.ProcessedRecords)
		}
	}

	// Print load results
	if record.Load_results.LoadedRecords > 0 {
		fmt.Println("\n📊 Load Results:")
		fmt.Printf("   Target: %s\n", record.Load_results.TargetName)
		fmt.Printf("   Loaded Records: %d\n", record.Load_results.LoadedRecords)
		fmt.Printf("   Rejected Records: %d\n", record.Load_results.RejectedRecords)
		if record.Load_results.LoadDuration > 0 {
			duration := time.Duration(record.Load_results.LoadDuration) * time.Millisecond
			fmt.Printf("   Load Duration: %v\n", duration)
		}
	}
}

// printErrorDetails prints detailed error information
func printErrorDetails(record *types.FileOrchestrationRecord) {
	if len(record.Error_history) > 0 {
		fmt.Println("\n❌ Error Details:")
		for _, err := range record.Error_history {
			fmt.Printf("   [%s] %s: %s\n",
				err.Timestamp.Format("15:04:05"),
				err.Stage,
				err.ErrorMessage)
		}
	}

	if len(record.Validation_results.ValidationErrors) > 0 {
		fmt.Println("\n🔍 Validation Errors:")
		for _, err := range record.Validation_results.ValidationErrors {
			fmt.Printf("   • %s\n", err)
		}
	}
}

// awsCommand handles the AWS resource management command
func awsCommand() {
	awsFlags := flag.NewFlagSet("aws", flag.ExitOnError)
	var configPath, action, bucketName, filePath, localPath string
	var createResources, force bool
	awsFlags.StringVar(&configPath, "config", "config.json", "Path to configuration file")
	awsFlags.StringVar(&action, "action", "", "Action to perform (list, create, upload, download)")
	awsFlags.StringVar(&bucketName, "bucket", "", "S3 bucket name (for upload/download)")
	awsFlags.StringVar(&filePath, "file", "", "S3 file path (for upload/download)")
	awsFlags.StringVar(&localPath, "local", "", "Local file path (for upload/download)")
	awsFlags.BoolVar(&createResources, "create", false, "Create missing AWS resources")
	awsFlags.BoolVar(&force, "force", false, "Force creation without confirmation")
	awsFlags.Parse(os.Args[2:])

	if action == "" {
		fmt.Fprintf(os.Stderr, "Error: Action is required\n")
		printAWSUsage()
		os.Exit(1)
	}

	// Create FlakeRunner instance
	fr, err := flakerunner.NewFlakeRunner(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create FlakeRunner: %v\n", err)
		os.Exit(1)
	}

	// Validate configuration
	fr.ValidateConfiguration()
	if fr.HasError() {
		fmt.Fprintf(os.Stderr, "Configuration validation failed: %v\n", fr.GetError())
		os.Exit(1)
	}

	switch action {
	case "list":
		listAWSResources(fr)
	case "create":
		createAWSResources(fr, createResources, force)
	case "upload":
		if localPath == "" || filePath == "" {
			fmt.Fprintf(os.Stderr, "Error: Both --local and --file are required for upload\n")
			os.Exit(1)
		}
		uploadFile(fr, localPath, filePath, bucketName)
	case "download":
		if localPath == "" || filePath == "" {
			fmt.Fprintf(os.Stderr, "Error: Both --local and --file are required for download\n")
			os.Exit(1)
		}
		downloadFile(fr, filePath, localPath, bucketName)
	default:
		fmt.Fprintf(os.Stderr, "Unknown action: %s\n", action)
		printAWSUsage()
		os.Exit(1)
	}
}

// listAWSResources lists all AWS resources used by FlakeRunner
func listAWSResources(fr *flakerunner.FlakeRunner) {
	fmt.Println("🔍 Listing AWS Resources...")
	fmt.Printf("Region: %s\n\n", fr.Config.AWSRegion)

	// List S3 buckets
	fmt.Println("📦 S3 Buckets:")
	listS3Bucket(fr, "Input", fr.Config.InputBucketName)
	listS3Bucket(fr, "Output", fr.Config.OutputBucketName)
	listS3Bucket(fr, "Staging", fr.Config.StagingBucketName)

	// List DynamoDB table
	fmt.Println("\n🗄️  DynamoDB Tables:")
	listDynamoDBTable(fr, fr.Config.ControlTableName)

	// List EMR application
	fmt.Println("\n⚡ EMR Serverless Applications:")
	listEMRApplication(fr, fr.Config.EMRApplicationID)

	fmt.Println("\n✅ Resource listing complete")
}

// createAWSResources creates missing AWS resources
func createAWSResources(fr *flakerunner.FlakeRunner, create, force bool) {
	if !create {
		fmt.Println("📋 Would create the following AWS resources (use --create to actually create):")
	} else {
		fmt.Println("🏗️  Creating AWS Resources...")
	}

	// Check and optionally create S3 buckets
	buckets := map[string]string{
		"Input":   fr.Config.InputBucketName,
		"Output":  fr.Config.OutputBucketName,
		"Staging": fr.Config.StagingBucketName,
	}

	fmt.Println("\n📦 S3 Buckets:")
	for bucketType, bucketName := range buckets {
		if create {
			createS3Bucket(fr, bucketType, bucketName, force)
		} else {
			fmt.Printf("   • %s Bucket: %s\n", bucketType, bucketName)
		}
	}

	// Check and optionally create DynamoDB table
	fmt.Println("\n🗄️  DynamoDB Tables:")
	if create {
		createDynamoDBTable(fr, fr.Config.ControlTableName, force)
	} else {
		fmt.Printf("   • Control Table: %s\n", fr.Config.ControlTableName)
	}

	// Check and optionally create EMR application
	fmt.Println("\n⚡ EMR Serverless Applications:")
	if create {
		createEMRApplication(fr, fr.Config.EMRApplicationID, force)
	} else {
		// For demo applications, always check if IAM policy needs updating
		if strings.Contains(fr.Config.EMRApplicationID, "demo") || strings.Contains(fr.Config.EMRExecutionRoleARN, "demo") {
			fmt.Printf("   • Demo Application ID: %s (checking IAM policy...)\n", fr.Config.EMRApplicationID)
			updateDemoApplicationIAMPolicy(fr, force)
		} else {
			fmt.Printf("   • Application ID: %s\n", fr.Config.EMRApplicationID)
		}
	}

	if !create {
		fmt.Println("\n💡 To create these resources, run with --create flag")
	} else {
		fmt.Println("\n✅ Resource creation complete")
	}
}

// uploadFile uploads a local file to S3
func uploadFile(fr *flakerunner.FlakeRunner, localPath, s3Path, bucketName string) {
	fmt.Printf("📤 Uploading file: %s → %s\n", localPath, s3Path)

	// Determine bucket from s3Path if not specified
	if bucketName == "" {
		if strings.HasPrefix(s3Path, "s3://") {
			parts := strings.SplitN(strings.TrimPrefix(s3Path, "s3://"), "/", 2)
			if len(parts) >= 1 {
				bucketName = parts[0]
				if len(parts) == 2 {
					s3Path = parts[1]
				}
			}
		} else {
			// Default to input bucket and use s3Path as key
			bucketName = fr.Config.InputBucketName
		}
	}

	// Check if file exists locally
	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "❌ Local file not found: %s\n", localPath)
		os.Exit(1)
	}

	// Read file content
	content, err := os.ReadFile(localPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to read file: %v\n", err)
		os.Exit(1)
	}

	// Upload to S3
	err = uploadToS3(fr, bucketName, s3Path, content)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Upload failed: %v\n", err)
		os.Exit(1)
	}

	fullS3Path := fmt.Sprintf("s3://%s/%s", bucketName, s3Path)
	fmt.Printf("✅ Successfully uploaded to: %s\n", fullS3Path)
	fmt.Printf("📏 File size: %d bytes\n", len(content))

	// Show target table mapping if it's a data file
	if targetTable, _, err := fr.DetermineTargetTable(fullS3Path); err == nil {
		fmt.Printf("🎯 Target table: %s\n", targetTable)
		fmt.Printf("💡 Process with: flake-runner process --file %s\n", fullS3Path)
	}
}

// downloadFile downloads a file from S3 to local path
func downloadFile(fr *flakerunner.FlakeRunner, s3Path, localPath, bucketName string) {
	fmt.Printf("📥 Downloading file: %s → %s\n", s3Path, localPath)

	// Determine bucket from s3Path if not specified
	if bucketName == "" {
		if strings.HasPrefix(s3Path, "s3://") {
			parts := strings.SplitN(strings.TrimPrefix(s3Path, "s3://"), "/", 2)
			if len(parts) >= 2 {
				bucketName = parts[0]
				s3Path = parts[1]
			} else {
				fmt.Fprintf(os.Stderr, "❌ Invalid S3 path format: %s\n", s3Path)
				os.Exit(1)
			}
		} else {
			// Default to output bucket
			bucketName = fr.Config.OutputBucketName
		}
	}

	// Download from S3
	content, err := downloadFromS3(fr, bucketName, s3Path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Download failed: %v\n", err)
		os.Exit(1)
	}

	// Write to local file
	err = os.WriteFile(localPath, content, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to write file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Successfully downloaded to: %s\n", localPath)
	fmt.Printf("📏 File size: %d bytes\n", len(content))
}

// Helper functions for AWS operations

// listS3Bucket checks if an S3 bucket exists and lists basic info
func listS3Bucket(fr *flakerunner.FlakeRunner, bucketType, bucketName string) {
	err := fr.ValidateS3Bucket(bucketName)
	if err != nil {
		fmt.Printf("   ❌ %s Bucket: %s (not accessible: %v)\n", bucketType, bucketName, err)
	} else {
		fmt.Printf("   ✅ %s Bucket: %s (accessible)\n", bucketType, bucketName)
	}
}

// listDynamoDBTable checks if a DynamoDB table exists and lists basic info
func listDynamoDBTable(fr *flakerunner.FlakeRunner, tableName string) {
	err := fr.ValidateDynamoDBTable(tableName)
	if err != nil {
		fmt.Printf("   ❌ Control Table: %s (not accessible: %v)\n", tableName, err)
	} else {
		fmt.Printf("   ✅ Control Table: %s (accessible)\n", tableName)
	}
}

// listEMRApplication checks if an EMR application exists and lists basic info
func listEMRApplication(fr *flakerunner.FlakeRunner, applicationID string) {
	err := fr.ValidateEMRApplication(applicationID)
	if err != nil {
		fmt.Printf("   ❌ Application: %s (not accessible: %v)\n", applicationID, err)
	} else {
		fmt.Printf("   ✅ Application: %s (accessible)\n", applicationID)
	}
}

// createS3Bucket creates an S3 bucket if it doesn't exist
func createS3Bucket(fr *flakerunner.FlakeRunner, bucketType, bucketName string, force bool) {
	// Check if bucket already exists
	err := fr.ValidateS3Bucket(bucketName)
	if err == nil {
		fmt.Printf("   ✅ %s Bucket: %s (already exists)\n", bucketType, bucketName)
		return
	}

	// Confirm creation if not forced
	if !force {
		fmt.Printf("   ❓ Create %s Bucket: %s? (y/N): ", bucketType, bucketName)
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
			fmt.Printf("   ⏭️  Skipped %s Bucket: %s\n", bucketType, bucketName)
			return
		}
	}

	err = fr.CreateS3Bucket(bucketName)
	if err != nil {
		fmt.Printf("   ❌ Failed to create %s Bucket: %s (%v)\n", bucketType, bucketName, err)
	} else {
		fmt.Printf("   ✅ Created %s Bucket: %s\n", bucketType, bucketName)
	}
}

// createDynamoDBTable creates a DynamoDB table if it doesn't exist
func createDynamoDBTable(fr *flakerunner.FlakeRunner, tableName string, force bool) {
	// Check if table already exists
	err := fr.ValidateDynamoDBTable(tableName)
	if err == nil {
		fmt.Printf("   ✅ Control Table: %s (already exists)\n", tableName)
		return
	}

	// Confirm creation if not forced
	if !force {
		fmt.Printf("   ❓ Create Control Table: %s? (y/N): ", tableName)
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
			fmt.Printf("   ⏭️  Skipped Control Table: %s\n", tableName)
			return
		}
	}

	err = fr.CreateDynamoDBTable(tableName)
	if err != nil {
		fmt.Printf("   ❌ Failed to create Control Table: %s (%v)\n", tableName, err)
	} else {
		fmt.Printf("   ✅ Created Control Table: %s\n", tableName)
	}
}

// createEMRApplication creates an EMR Serverless application if it doesn't exist
func createEMRApplication(fr *flakerunner.FlakeRunner, applicationID string, force bool) {
	// Check if application already exists (skip validation for demo apps)
	if !strings.Contains(applicationID, "demo") {
		err := fr.ValidateEMRApplication(applicationID)
		if err == nil {
			fmt.Printf("   ✅ EMR Application: %s (already exists)\n", applicationID)
			return
		}
	}

	// For demo applications, we still try to create a real one
	applicationName := "flake-runner-demo"
	if !strings.Contains(applicationID, "demo") {
		applicationName = "flake-runner-production"
	}

	// Confirm creation if not forced
	if !force {
		fmt.Printf("   ❓ Create EMR Application: %s? (y/N): ", applicationName)
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
			fmt.Printf("   ⏭️  Skipped EMR Application: %s\n", applicationName)
			return
		}
	}

	// For demo applications, always update IAM policy even if application exists
	if strings.Contains(applicationName, "demo") {
		fmt.Printf("   🔐 Updating IAM policy for demo application: %s...\n", applicationName)
		bucketNames := []string{fr.Config.InputBucketName, fr.Config.OutputBucketName, fr.Config.StagingBucketName}
		roleArn, err := fr.CreateEMRExecutionRoleWithBuckets("EMRServerlessExecutionRole-flake-runner-demo", bucketNames)
		if err != nil {
			fmt.Printf("   ❌ Failed to update IAM role policy: %v\n", err)
		} else {
			fmt.Printf("   ✅ Updated IAM role policy with wildcard S3 permissions\n")
			fmt.Printf("   🔐 IAM Role ARN: %s\n", roleArn)
		}
		return
	}

	fmt.Printf("   🏗️  Creating EMR Application with IAM Role: %s...\n", applicationName)
	createdAppID, roleArn, err := fr.CreateEMRApplicationWithIAM(applicationName, "emr-7.8.0")
	if err != nil {
		fmt.Printf("   ❌ Failed to create EMR Application: %s (%v)\n", applicationName, err)
		if createdAppID != "" {
			fmt.Printf("   💡 Partial success - Application ID: %s\n", createdAppID)
		}
		if roleArn != "" {
			fmt.Printf("   💡 IAM Role ARN: %s\n", roleArn)
		}
	} else {
		fmt.Printf("   ✅ Created EMR Application: %s\n", applicationName)
		fmt.Printf("   📋 Application ID: %s\n", createdAppID)
		fmt.Printf("   🔐 IAM Role ARN: %s\n", roleArn)
		fmt.Printf("   💡 Update your configuration file with these values:\n")
		fmt.Printf("      \"emr_application_id\": \"%s\",\n", createdAppID)
		fmt.Printf("      \"emr_execution_role_arn\": \"%s\"\n", roleArn)
	}
}

// updateDemoApplicationIAMPolicy updates IAM policy for demo applications
func updateDemoApplicationIAMPolicy(fr *flakerunner.FlakeRunner, force bool) {
	applicationName := "flake-runner-demo"

	fmt.Printf("   🔐 Updating IAM policy for demo application: %s...\n", applicationName)
	bucketNames := []string{fr.Config.InputBucketName, fr.Config.OutputBucketName, fr.Config.StagingBucketName}
	roleArn, err := fr.CreateEMRExecutionRoleWithBuckets("EMRServerlessExecutionRole-flake-runner-demo", bucketNames)
	if err != nil {
		fmt.Printf("   ❌ Failed to update IAM role policy: %v\n", err)
	} else {
		fmt.Printf("   ✅ Updated IAM role policy with wildcard S3 permissions\n")
		fmt.Printf("   🔐 IAM Role ARN: %s\n", roleArn)
		fmt.Printf("   📋 Policy allows access to: arn:aws:s3:::flake-runner-demo*\n")
	}
}

// uploadToS3 uploads content to S3
func uploadToS3(fr *flakerunner.FlakeRunner, bucketName, key string, content []byte) error {
	return fr.UploadToS3(bucketName, key, content)
}

// downloadFromS3 downloads content from S3
func downloadFromS3(fr *flakerunner.FlakeRunner, bucketName, key string) ([]byte, error) {
	return fr.DownloadFromS3(bucketName, key)
}

// printAWSUsage prints usage information for the aws command
func printAWSUsage() {
	fmt.Print(`
Usage: flake-runner aws --action <action> [options]

Actions:
  list      List all AWS resources (S3 buckets, DynamoDB tables, EMR applications)
  create    Create missing AWS resources (requires --create flag)
  upload    Upload a local file to S3
  download  Download a file from S3 to local path

Options:
  --config <path>    Path to configuration file (default: config.json)
  --create           Actually create resources (required for create action)
  --force            Force creation without confirmation prompts
  --bucket <name>    S3 bucket name (for upload/download, optional if using full S3 paths)
  --file <path>      S3 file path (for upload/download)
  --local <path>     Local file path (for upload/download)

Examples:
  # List all AWS resources
  flake-runner aws --action list

  # Check what resources would be created
  flake-runner aws --action create

  # Create missing resources with confirmation prompts
  flake-runner aws --action create --create

  # Create missing resources without prompts
  flake-runner aws --action create --create --force

  # Upload a local file to S3 (auto-detects input bucket)
  flake-runner aws --action upload --local data.csv --file customers/data.csv

  # Upload with explicit bucket
  flake-runner aws --action upload --local data.csv --file customers/data.csv --bucket my-input-bucket

  # Upload using full S3 path
  flake-runner aws --action upload --local data.csv --file s3://my-input-bucket/customers/data.csv

  # Download processed file from output bucket
  flake-runner aws --action download --file processed/customers/data.parquet --local processed_data.parquet

  # Download with explicit bucket
  flake-runner aws --action download --file processed_data.parquet --local data.parquet --bucket my-output-bucket
`)
}
