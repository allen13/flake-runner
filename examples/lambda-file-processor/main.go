// Lambda function for processing files through FlakeRunner workflow up to EMR submission
// Triggered by API Gateway requests with control data and S3 file path
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/allen13/flake-runner/pkg/flakerunner"
	"github.com/allen13/flake-runner/pkg/types"
)

// ProcessRequest represents the incoming API request payload
type ProcessRequest struct {
	FilePath    string                 `json:"file_path"`
	ControlData *types.ControlData     `json:"control_data,omitempty"`
	Options     ProcessOptions         `json:"options,omitempty"`
}

// ProcessOptions contains optional processing parameters
type ProcessOptions struct {
	ValidateOnly bool `json:"validate_only,omitempty"`
	SkipValidation bool `json:"skip_validation,omitempty"`
}

// ProcessResponse represents the Lambda response
type ProcessResponse struct {
	Success            bool                   `json:"success"`
	JobID              string                 `json:"job_id,omitempty"`
	EMRJobRunID        string                 `json:"emr_job_run_id,omitempty"`
	TargetTable        string                 `json:"target_table,omitempty"`
	OrchestrationState string                 `json:"orchestration_state"`
	ProcessingSteps    []ProcessingStep       `json:"processing_steps"`
	ValidationResults  *types.ValidationResults `json:"validation_results,omitempty"`
	ErrorMessage       string                 `json:"error_message,omitempty"`
	ProcessingDuration time.Duration          `json:"processing_duration"`
	Timestamp          time.Time              `json:"timestamp"`
}

// ProcessingStep represents a single step in the processing workflow
type ProcessingStep struct {
	Step      string        `json:"step"`
	Status    string        `json:"status"`
	Duration  time.Duration `json:"duration"`
	Message   string        `json:"message,omitempty"`
	Error     string        `json:"error,omitempty"`
}

// LambdaHandler processes API Gateway requests for FlakeRunner file processing
func LambdaHandler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	startTime := time.Now()
	
	log.Printf("Received API request: %s %s", request.HTTPMethod, request.Path)
	log.Printf("Request body: %s", request.Body)

	// Parse the request payload
	var processReq ProcessRequest
	if err := json.Unmarshal([]byte(request.Body), &processReq); err != nil {
		return createErrorResponse(400, fmt.Sprintf("Invalid request payload: %v", err)), nil
	}

	// Validate required fields
	if processReq.FilePath == "" {
		return createErrorResponse(400, "file_path is required"), nil
	}

	// Process the file
	response := processFile(ctx, processReq, startTime)
	
	// Return JSON response
	responseBody, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshaling response: %v", err)
		return createErrorResponse(500, "Internal server error"), nil
	}

	statusCode := 200
	if !response.Success {
		statusCode = 400
	}

	return events.APIGatewayProxyResponse{
		StatusCode: statusCode,
		Headers: map[string]string{
			"Content-Type": "application/json",
			"Access-Control-Allow-Origin": "*",
		},
		Body: string(responseBody),
	}, nil
}

// processFile processes a single file through the FlakeRunner workflow
func processFile(ctx context.Context, req ProcessRequest, startTime time.Time) ProcessResponse {
	response := ProcessResponse{
		ProcessingSteps: []ProcessingStep{},
		Timestamp:       startTime,
	}

	log.Printf("Processing file: %s", req.FilePath)

	// Step 1: Initialize FlakeRunner
	stepStart := time.Now()
	fr, err := initializeFlakeRunner()
	if err != nil {
		response.ErrorMessage = fmt.Sprintf("Failed to initialize FlakeRunner: %v", err)
		response.OrchestrationState = "INITIALIZATION_FAILED"
		response.ProcessingDuration = time.Since(startTime)
		addProcessingStep(&response, "initialization", "FAILED", time.Since(stepStart), "", err.Error())
		return response
	}
	response.JobID = fr.JobID
	addProcessingStep(&response, "initialization", "SUCCESS", time.Since(stepStart), "FlakeRunner initialized successfully", "")

	// Step 2: Validate configuration and AWS resources
	stepStart = time.Now()
	fr = fr.ValidateConfiguration().ValidateAWSResources()
	if fr.GetError() != nil {
		response.ErrorMessage = fmt.Sprintf("Configuration/AWS validation failed: %v", fr.GetError())
		response.OrchestrationState = "VALIDATION_FAILED"
		response.ProcessingDuration = time.Since(startTime)
		addProcessingStep(&response, "validation", "FAILED", time.Since(stepStart), "", fr.GetError().Error())
		return response
	}
	addProcessingStep(&response, "validation", "SUCCESS", time.Since(stepStart), "Configuration and AWS resources validated", "")

	// Step 3: Determine target table and mapping
	stepStart = time.Now()
	targetTable, _, err := fr.DetermineTargetTable(req.FilePath)
	if err != nil {
		response.ErrorMessage = fmt.Sprintf("Failed to determine target table: %v", err)
		response.OrchestrationState = "TABLE_MAPPING_FAILED"
		response.ProcessingDuration = time.Since(startTime)
		addProcessingStep(&response, "table_mapping", "FAILED", time.Since(stepStart), "", err.Error())
		return response
	}
	response.TargetTable = targetTable
	addProcessingStep(&response, "table_mapping", "SUCCESS", time.Since(stepStart), fmt.Sprintf("Mapped to table: %s", targetTable), "")

	// If validate-only mode, stop here after basic validation
	if req.Options.ValidateOnly {
		stepStart = time.Now()
		fr = fr.ProcessInputFile(req.FilePath)
		if fr.GetError() != nil {
			response.ErrorMessage = fmt.Sprintf("File validation failed: %v", fr.GetError())
			response.OrchestrationState = "VALIDATION_FAILED"
			response.ProcessingDuration = time.Since(startTime)
			addProcessingStep(&response, "file_validation", "FAILED", time.Since(stepStart), "", fr.GetError().Error())
			return response
		}

		// Validate with control data if provided
		if req.ControlData != nil {
			fr = fr.ValidateWithControlData(req.FilePath, req.ControlData)
			if fr.GetError() != nil {
				response.ErrorMessage = fmt.Sprintf("Control data validation failed: %v", fr.GetError())
				response.OrchestrationState = "VALIDATION_FAILED"
				response.ProcessingDuration = time.Since(startTime)
				addProcessingStep(&response, "control_validation", "FAILED", time.Since(stepStart), "", fr.GetError().Error())
				return response
			}
			addProcessingStep(&response, "control_validation", "SUCCESS", time.Since(stepStart), "Control data validation passed", "")
		}

		response.Success = true
		response.OrchestrationState = "VALIDATED"
		response.ProcessingDuration = time.Since(startTime)
		addProcessingStep(&response, "file_validation", "SUCCESS", time.Since(stepStart), "File validation completed successfully", "")
		return response
	}

	// Step 4: Full file processing (following processCommand pattern)
	stepStart = time.Now()
	fr = fr.ProcessFile(req.FilePath, req.ControlData)
	if fr.GetError() != nil {
		response.ErrorMessage = fmt.Sprintf("File processing failed: %v", fr.GetError())
		response.OrchestrationState = "PROCESSING_FAILED"
		response.ProcessingDuration = time.Since(startTime)
		addProcessingStep(&response, "file_processing", "FAILED", time.Since(stepStart), "", fr.GetError().Error())
		return response
	}
	addProcessingStep(&response, "file_processing", "SUCCESS", time.Since(stepStart), "File processing completed, EMR job submitted", "")

	// Get the EMR job run ID from FlakeRunner
	jobRuns := fr.GetEMRJobRuns()
	if jobRunID, exists := jobRuns[req.FilePath]; exists {
		response.EMRJobRunID = jobRunID
	}

	response.Success = true
	response.OrchestrationState = "EMR_SUBMITTED"
	response.ProcessingDuration = time.Since(startTime)

	log.Printf("Successfully processed file %s, submitted EMR job: %s", req.FilePath, response.EMRJobRunID)
	return response
}

// initializeFlakeRunner creates and initializes a FlakeRunner instance
func initializeFlakeRunner() (*flakerunner.FlakeRunner, error) {
	// Get configuration from environment variables
	configPath := os.Getenv("FLAKE_RUNNER_CONFIG_PATH")
	if configPath == "" {
		configPath = "/opt/flake-runner-config.json"
	}

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("configuration file not found: %s", configPath)
	}

	fr, err := flakerunner.NewFlakeRunner(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create FlakeRunner: %w", err)
	}

	return fr, nil
}

// addProcessingStep adds a step to the processing response
func addProcessingStep(response *ProcessResponse, step, status string, duration time.Duration, message, errorMsg string) {
	response.ProcessingSteps = append(response.ProcessingSteps, ProcessingStep{
		Step:     step,
		Status:   status,
		Duration: duration,
		Message:  message,
		Error:    errorMsg,
	})
}

// createErrorResponse creates a standardized error response
func createErrorResponse(statusCode int, message string) events.APIGatewayProxyResponse {
	errorResp := map[string]interface{}{
		"success":     false,
		"error":       message,
		"timestamp":   time.Now(),
		"status_code": statusCode,
	}

	body, _ := json.Marshal(errorResp)
	
	return events.APIGatewayProxyResponse{
		StatusCode: statusCode,
		Headers: map[string]string{
			"Content-Type": "application/json",
			"Access-Control-Allow-Origin": "*",
		},
		Body: string(body),
	}
}

func main() {
	lambda.Start(LambdaHandler)
}