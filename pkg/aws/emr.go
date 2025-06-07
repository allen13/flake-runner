package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/allen13/flake-runner/pkg/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrTypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamTypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// ValidateEMRApplication validates that an EMR Serverless application exists and is ready
func ValidateEMRApplication(emrClient *emrserverless.Client, applicationID string) error {
	// Skip validation for demo applications
	if strings.Contains(applicationID, "demo") || strings.HasPrefix(applicationID, "00f7u00a1p2demo") {
		return nil
	}

	input := &emrserverless.GetApplicationInput{
		ApplicationId: aws.String(applicationID),
	}

	result, err := emrClient.GetApplication(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("EMR application %s is not accessible: %w", applicationID, err)
	}

	if result.Application == nil {
		return fmt.Errorf("EMR application %s not found", applicationID)
	}

	switch result.Application.State {
	case emrTypes.ApplicationStateCreated, emrTypes.ApplicationStateStarted:
		// Application is ready
	case emrTypes.ApplicationStateCreating, emrTypes.ApplicationStateStarting:
		// Application is starting up - this is acceptable but log a warning
	case emrTypes.ApplicationStateStopped, emrTypes.ApplicationStateStopping:
		return fmt.Errorf("EMR application %s is stopped or stopping", applicationID)
	case emrTypes.ApplicationStateTerminated:
		return fmt.Errorf("EMR application %s is terminated", applicationID)
	}

	return nil
}

// UpdateEMRExecutionRolePolicy updates the IAM policy for an existing EMR execution role
func UpdateEMRExecutionRolePolicy(iamClient *iam.Client, stsClient *sts.Client, roleName string, bucketNames []string) error {
	// Get current AWS account ID
	callerIdentity, err := stsClient.GetCallerIdentity(context.TODO(), &sts.GetCallerIdentityInput{})
	if err != nil {
		return fmt.Errorf("failed to get caller identity: %w", err)
	}
	accountID := *callerIdentity.Account

	// Create updated policy for S3 and other AWS services
	customPolicy := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect": "Allow",
				"Action": []string{
					"s3:GetObject",
					"s3:PutObject",
					"s3:DeleteObject",
					"s3:ListBucket",
				},
				"Resource": generateS3ResourceArns(bucketNames),
			},
			{
				"Effect": "Allow",
				"Action": []string{
					"logs:CreateLogGroup",
					"logs:CreateLogStream",
					"logs:PutLogEvents",
					"logs:DescribeLogGroups",
					"logs:DescribeLogStreams",
				},
				"Resource": "*",
			},
			{
				"Effect": "Allow",
				"Action": []string{
					"glue:GetDatabase",
					"glue:GetDatabases",
					"glue:GetTable",
					"glue:GetTables",
					"glue:CreateTable",
					"glue:UpdateTable",
					"glue:DeleteTable",
					"glue:GetPartitions",
					"glue:CreatePartition",
					"glue:UpdatePartition",
					"glue:DeletePartition",
				},
				"Resource": "*",
			},
		},
	}

	customPolicyJSON, err := json.Marshal(customPolicy)
	if err != nil {
		return fmt.Errorf("failed to marshal custom policy: %w", err)
	}

	// Update the existing policy
	policyName := fmt.Sprintf("%s-Policy", roleName)
	policyArn := fmt.Sprintf("arn:aws:iam::%s:policy/%s", accountID, policyName)

	// Clean up old policy versions first (keep only the default)
	listVersionsInput := &iam.ListPolicyVersionsInput{
		PolicyArn: aws.String(policyArn),
	}
	versionsResult, err := iamClient.ListPolicyVersions(context.TODO(), listVersionsInput)
	if err == nil && versionsResult.Versions != nil {
		for _, version := range versionsResult.Versions {
			if version.VersionId != nil && !version.IsDefaultVersion {
				deleteVersionInput := &iam.DeletePolicyVersionInput{
					PolicyArn: aws.String(policyArn),
					VersionId: version.VersionId,
				}
				// Ignore errors when deleting old versions
				iamClient.DeletePolicyVersion(context.TODO(), deleteVersionInput)
			}
		}
	}

	// Create a new version of the policy
	_, err = iamClient.CreatePolicyVersion(context.TODO(), &iam.CreatePolicyVersionInput{
		PolicyArn:      aws.String(policyArn),
		PolicyDocument: aws.String(string(customPolicyJSON)),
		SetAsDefault:   true,
	})
	if err != nil {
		return fmt.Errorf("failed to update policy: %w", err)
	}

	fmt.Printf("Updated IAM policy for role: %s\n", roleName)
	return nil
}

// CreateEMRExecutionRole creates an IAM role for EMR Serverless execution
func CreateEMRExecutionRole(iamClient *iam.Client, stsClient *sts.Client, roleName string, bucketNames []string) (string, error) {
	// Get current AWS account ID
	callerIdentity, err := stsClient.GetCallerIdentity(context.TODO(), &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", fmt.Errorf("failed to get caller identity: %w", err)
	}
	accountID := *callerIdentity.Account

	// Check if role already exists
	roleArn := fmt.Sprintf("arn:aws:iam::%s:role/%s", accountID, roleName)
	getRoleInput := &iam.GetRoleInput{
		RoleName: aws.String(roleName),
	}
	_, err = iamClient.GetRole(context.TODO(), getRoleInput)
	if err == nil {
		fmt.Printf("IAM role %s already exists, updating policy...\n", roleName)
		err = UpdateEMRExecutionRolePolicy(iamClient, stsClient, roleName, bucketNames)
		if err != nil {
			return "", fmt.Errorf("failed to update policy for existing role: %w", err)
		}
		fmt.Printf("Successfully updated IAM role policy for: %s\n", roleName)
		return roleArn, nil
	}

	// Create trust policy for EMR Serverless
	trustPolicy := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect": "Allow",
				"Principal": map[string]interface{}{
					"Service": "emr-serverless.amazonaws.com",
				},
				"Action": "sts:AssumeRole",
			},
		},
	}

	trustPolicyJSON, err := json.Marshal(trustPolicy)
	if err != nil {
		return "", fmt.Errorf("failed to marshal trust policy: %w", err)
	}

	// Create the IAM role
	createRoleInput := &iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(string(trustPolicyJSON)),
		Description:              aws.String("IAM role for EMR Serverless execution created by FlakeRunner"),
		Tags: []iamTypes.Tag{
			{
				Key:   aws.String("CreatedBy"),
				Value: aws.String("FlakeRunner"),
			},
			{
				Key:   aws.String("Purpose"),
				Value: aws.String("EMRServerlessExecution"),
			},
		},
	}

	createRoleResult, err := iamClient.CreateRole(context.TODO(), createRoleInput)
	if err != nil {
		return "", fmt.Errorf("failed to create IAM role: %w", err)
	}

	fmt.Printf("Created IAM role: %s\n", *createRoleResult.Role.RoleName)

	// Create custom policy for S3 and other AWS services
	customPolicy := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect": "Allow",
				"Action": []string{
					"s3:GetObject",
					"s3:PutObject",
					"s3:DeleteObject",
					"s3:ListBucket",
				},
				"Resource": generateS3ResourceArns(bucketNames),
			},
			{
				"Effect": "Allow",
				"Action": []string{
					"logs:CreateLogGroup",
					"logs:CreateLogStream",
					"logs:PutLogEvents",
					"logs:DescribeLogGroups",
					"logs:DescribeLogStreams",
				},
				"Resource": "*",
			},
			{
				"Effect": "Allow",
				"Action": []string{
					"glue:GetDatabase",
					"glue:GetDatabases",
					"glue:GetTable",
					"glue:GetTables",
					"glue:CreateTable",
					"glue:UpdateTable",
					"glue:DeleteTable",
					"glue:GetPartitions",
					"glue:CreatePartition",
					"glue:UpdatePartition",
					"glue:DeletePartition",
				},
				"Resource": "*",
			},
		},
	}

	customPolicyJSON, err := json.Marshal(customPolicy)
	if err != nil {
		return "", fmt.Errorf("failed to marshal custom policy: %w", err)
	}

	// Create the custom policy
	policyName := fmt.Sprintf("%s-Policy", roleName)
	createPolicyInput := &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(string(customPolicyJSON)),
		Description:    aws.String("Custom policy for EMR Serverless execution created by FlakeRunner"),
		Tags: []iamTypes.Tag{
			{
				Key:   aws.String("CreatedBy"),
				Value: aws.String("FlakeRunner"),
			},
		},
	}

	createdPolicy, err := iamClient.CreatePolicy(context.TODO(), createPolicyInput)
	if err != nil {
		return "", fmt.Errorf("failed to create custom policy: %w", err)
	}

	fmt.Printf("Created IAM policy: %s\n", *createdPolicy.Policy.PolicyName)

	// Attach the custom policy to the role
	attachPolicyInput := &iam.AttachRolePolicyInput{
		RoleName:  aws.String(roleName),
		PolicyArn: createdPolicy.Policy.Arn,
	}

	_, err = iamClient.AttachRolePolicy(context.TODO(), attachPolicyInput)
	if err != nil {
		return "", fmt.Errorf("failed to attach custom policy: %w", err)
	}

	fmt.Printf("Attached custom policy to role\n")

	// Wait a moment for role to propagate
	time.Sleep(10 * time.Second)

	return roleArn, nil
}

// generateS3ResourceArns generates S3 resource ARNs for bucket access
func generateS3ResourceArns(bucketNames []string) []string {
	var resources []string

	// Check if any bucket names contain "demo" - if so, use wildcard for all demo buckets
	hasDemo := false
	for _, bucket := range bucketNames {
		if strings.Contains(bucket, "demo") {
			hasDemo = true
			break
		}
	}

	if hasDemo {
		// Use wildcard pattern for all demo buckets
		resources = append(resources, "arn:aws:s3:::flake-runner-demo*")
		resources = append(resources, "arn:aws:s3:::flake-runner-demo*/*")
	} else {
		// Use specific bucket names for production
		for _, bucket := range bucketNames {
			resources = append(resources, fmt.Sprintf("arn:aws:s3:::%s", bucket))
			resources = append(resources, fmt.Sprintf("arn:aws:s3:::%s/*", bucket))
		}
	}

	return resources
}

// CreateEMRApplicationWithIAM creates a new EMR Serverless application with proper IAM role
func CreateEMRApplicationWithIAM(emrClient *emrserverless.Client, iamClient *iam.Client, stsClient *sts.Client, name, releaseLabel string, bucketNames []string, tags map[string]string) (string, string, error) {
	// Create IAM role first
	roleName := fmt.Sprintf("EMRServerlessExecutionRole-%s", name)
	roleArn, err := CreateEMRExecutionRole(iamClient, stsClient, roleName, bucketNames)
	if err != nil {
		return "", "", fmt.Errorf("failed to create IAM role: %w", err)
	}

	// Create EMR application
	applicationID, err := CreateEMRApplication(emrClient, name, releaseLabel, tags)
	if err != nil {
		return "", "", fmt.Errorf("failed to create EMR application: %w", err)
	}

	return applicationID, roleArn, nil
}

// CreateEMRApplication creates a new EMR Serverless application
func CreateEMRApplication(emrClient *emrserverless.Client, name, releaseLabel string, tags map[string]string) (string, error) {
	// Default to EMR 7.8.0 with Spark if not specified
	if releaseLabel == "" {
		releaseLabel = "emr-7.8.0"
	}

	// Create the application
	input := &emrserverless.CreateApplicationInput{
		Name:         aws.String(name),
		ReleaseLabel: aws.String(releaseLabel),
		Type:         aws.String("SPARK"),
		ClientToken:  aws.String(fmt.Sprintf("flake-runner-%d", time.Now().Unix())),
		InitialCapacity: map[string]emrTypes.InitialCapacityConfig{
			"driver": {
				WorkerCount: aws.Int64(1),
				WorkerConfiguration: &emrTypes.WorkerResourceConfig{
					Cpu:    aws.String("1 vCPU"),
					Memory: aws.String("2 GB"),
				},
			},
			"executor": {
				WorkerCount: aws.Int64(1),
				WorkerConfiguration: &emrTypes.WorkerResourceConfig{
					Cpu:    aws.String("1 vCPU"),
					Memory: aws.String("2 GB"),
				},
			},
		},
		MaximumCapacity: &emrTypes.MaximumAllowedResources{
			Cpu:    aws.String("16 vCPU"),
			Memory: aws.String("32 GB"),
		},
		AutoStartConfiguration: &emrTypes.AutoStartConfig{
			Enabled: aws.Bool(true),
		},
		AutoStopConfiguration: &emrTypes.AutoStopConfig{
			Enabled:            aws.Bool(true),
			IdleTimeoutMinutes: aws.Int32(15),
		},
		Tags: tags,
	}

	result, err := emrClient.CreateApplication(context.TODO(), input)
	if err != nil {
		return "", fmt.Errorf("failed to create EMR application: %w", err)
	}

	if result.ApplicationId == nil {
		return "", fmt.Errorf("application ID is nil in response")
	}

	applicationID := *result.ApplicationId

	// Wait for the application to be created
	fmt.Printf("Waiting for EMR application %s to be created...\n", applicationID)

	maxWaitTime := 5 * time.Minute
	checkInterval := 10 * time.Second
	deadline := time.Now().Add(maxWaitTime)

	for time.Now().Before(deadline) {
		getResult, err := emrClient.GetApplication(context.TODO(), &emrserverless.GetApplicationInput{
			ApplicationId: aws.String(applicationID),
		})
		if err != nil {
			return "", fmt.Errorf("failed to check application status: %w", err)
		}

		if getResult.Application == nil {
			return "", fmt.Errorf("application not found after creation")
		}

		switch getResult.Application.State {
		case emrTypes.ApplicationStateCreated:
			fmt.Printf("EMR application %s created successfully\n", applicationID)
			return applicationID, nil
		case emrTypes.ApplicationStateCreating:
			// Still creating, continue waiting
			time.Sleep(checkInterval)
		case emrTypes.ApplicationStateTerminated:
			return "", fmt.Errorf("application creation failed - application was terminated")
		default:
			fmt.Printf("Application state: %s, continuing to wait...\n", getResult.Application.State)
			time.Sleep(checkInterval)
		}
	}

	return "", fmt.Errorf("timed out waiting for application %s to be created", applicationID)
}

// ConstructJobParameters creates EMR job parameters for a file processing job
func ConstructJobParameters(cfg *config.Config, filePath string, mapping *config.PrefixMapping, jobID string, controlData map[string]interface{}) (*emrserverless.StartJobRunInput, error) {
	bucket, key, err := ParseS3Path(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse S3 path: %w", err)
	}

	// Container image is optional - if empty, EMR uses built-in runtime
	// Custom container images would be specified at the application level

	jobArgs := []string{
		"--input-path", fmt.Sprintf("s3://%s/%s", bucket, key),
		"--output-path", fmt.Sprintf("s3://%s/processed/%s", cfg.OutputBucketName, mapping.TargetName),
		"--staging-path", fmt.Sprintf("s3://%s/staging/%s", cfg.StagingBucketName, mapping.TargetName),
		"--target-name", mapping.TargetName,
		"--file-format", mapping.ProcessingConfig.FileFormat,
		"--job-id", jobID,
	}

	// Add control data information if available
	if controlData != nil {
		if expectedRecords, ok := controlData["record_count"].(int64); ok {
			jobArgs = append(jobArgs, "--expected-records", fmt.Sprintf("%d", expectedRecords))
		}
		if expectedFileSize, ok := controlData["file_size"].(int64); ok {
			jobArgs = append(jobArgs, "--expected-file-size", fmt.Sprintf("%d", expectedFileSize))
		}
		if expectedHash, ok := controlData["file_hash"].(string); ok && expectedHash != "" {
			jobArgs = append(jobArgs, "--expected-hash", expectedHash)
		}
	}

	// Add validation rules as job arguments
	if mapping.ValidationRules.ValidateRecordCount {
		jobArgs = append(jobArgs, "--validate-record-count", "true")
	}
	if mapping.ValidationRules.ValidateChecksum {
		jobArgs = append(jobArgs, "--validate-checksum", "true")
	}
	if len(mapping.ValidationRules.RequiredFields) > 0 {
		jobArgs = append(jobArgs, "--required-fields", strings.Join(mapping.ValidationRules.RequiredFields, ","))
	}

	if mapping.ProcessingConfig.CompressionType != "" {
		jobArgs = append(jobArgs, "--compression", mapping.ProcessingConfig.CompressionType)
	}

	// Add script parameters if provided
	for key, value := range mapping.ScriptParams {
		jobArgs = append(jobArgs, fmt.Sprintf("--%s", key), value)
	}

	entryPoint := mapping.EntryPoint
	if entryPoint == "" {
		entryPoint = "/opt/spark/jobs/processor.py"
	}

	// Prepare configuration overrides with optional environment variables
	applicationConfigs := []emrTypes.Configuration{
		{
			Classification: aws.String("spark-defaults"),
			Properties: map[string]string{
				"spark.sql.adaptive.enabled":                    "true",
				"spark.sql.adaptive.coalescePartitions.enabled": "true",
				"spark.serializer":                              "org.apache.spark.serializer.KryoSerializer",
			},
		},
	}

	// NOTE: Environment variables in EMR Serverless are not supported via configuration overrides
	// They should be passed as Spark configurations or job arguments instead
	// Skipping environment variables configuration to avoid EMR validation errors

	jobParams := &emrserverless.StartJobRunInput{
		ApplicationId:    aws.String(cfg.EMRApplicationID),
		ExecutionRoleArn: aws.String(cfg.EMRExecutionRoleARN),
		Name:             aws.String(fmt.Sprintf("flake-runner-%s-%s", jobID, mapping.TargetName)),
		JobDriver: &emrTypes.JobDriverMemberSparkSubmit{
			Value: emrTypes.SparkSubmit{
				EntryPoint:            aws.String(entryPoint),
				EntryPointArguments:   jobArgs,
				SparkSubmitParameters: aws.String(getSparkSubmitParameters(mapping)),
			},
		},
		ConfigurationOverrides: &emrTypes.ConfigurationOverrides{
			ApplicationConfiguration: applicationConfigs,
			MonitoringConfiguration: &emrTypes.MonitoringConfiguration{
				CloudWatchLoggingConfiguration: &emrTypes.CloudWatchLoggingConfiguration{
					Enabled:      aws.Bool(true),
					LogGroupName: aws.String(fmt.Sprintf("/aws/emr-serverless/flake-runner/%s", jobID)),
				},
				S3MonitoringConfiguration: &emrTypes.S3MonitoringConfiguration{
					LogUri: aws.String(fmt.Sprintf("s3://%s/logs/", cfg.OutputBucketName)),
				},
			},
		},
		Tags: map[string]string{
			"Project":    "flake-runner",
			"JobId":      jobID,
			"TargetName": mapping.TargetName,
		},
	}

	if cfg.JobTimeoutMinutes > 0 {
		jobParams.ExecutionTimeoutMinutes = aws.Int64(int64(cfg.JobTimeoutMinutes))
	}

	return jobParams, nil
}

// SubmitEMRJob submits a job to EMR Serverless
func SubmitEMRJob(emrClient *emrserverless.Client, jobParams *emrserverless.StartJobRunInput) (string, error) {
	result, err := emrClient.StartJobRun(context.TODO(), jobParams)
	if err != nil {
		return "", fmt.Errorf("failed to start job run: %w", err)
	}

	if result.JobRunId == nil {
		return "", fmt.Errorf("job run ID is nil in response")
	}

	return *result.JobRunId, nil
}

// GetJobStatus retrieves the current status of an EMR job
func GetJobStatus(emrClient *emrserverless.Client, applicationID, jobRunID string) (*emrTypes.JobRun, error) {
	input := &emrserverless.GetJobRunInput{
		ApplicationId: aws.String(applicationID),
		JobRunId:      aws.String(jobRunID),
	}

	result, err := emrClient.GetJobRun(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to get job run status: %w", err)
	}

	if result.JobRun == nil {
		return nil, fmt.Errorf("job run is nil in response")
	}

	return result.JobRun, nil
}

// CancelJob cancels a running EMR Serverless job
func CancelJob(emrClient *emrserverless.Client, applicationID, jobRunID string) error {
	input := &emrserverless.CancelJobRunInput{
		ApplicationId: aws.String(applicationID),
		JobRunId:      aws.String(jobRunID),
	}

	_, err := emrClient.CancelJobRun(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to cancel job run %s: %w", jobRunID, err)
	}

	return nil
}

func getSparkSubmitParameters(mapping *config.PrefixMapping) string {
	params := []string{
		"--conf spark.sql.adaptive.enabled=true",
		"--conf spark.sql.adaptive.coalescePartitions.enabled=true",
		"--conf spark.serializer=org.apache.spark.serializer.KryoSerializer",
	}

	if mapping.ProcessingConfig.MaxFileSize > 1024*1024*1024 {
		params = append(params,
			"--conf spark.executor.memory=2g",
			"--conf spark.executor.cores=1",
			"--conf spark.executor.instances=2",
		)
	} else {
		params = append(params,
			"--conf spark.executor.memory=1g",
			"--conf spark.executor.cores=1",
			"--conf spark.executor.instances=1",
		)
	}

	return strings.Join(params, " ")
}
