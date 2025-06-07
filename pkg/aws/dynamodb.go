package aws

import (
	"context"
	"fmt"

	"github.com/allen13/flake-runner/pkg/types"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamoTypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ValidateDynamoDBTable validates that a DynamoDB table exists and is accessible
func ValidateDynamoDBTable(dynamoClient *dynamodb.Client, tableName string) error {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	_, err := dynamoClient.DescribeTable(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("table %s is not accessible: %w", tableName, err)
	}

	return nil
}

// marshalFileOrchestrationRecord marshals a FileOrchestrationRecord with proper field name mapping
func marshalFileOrchestrationRecord(record *types.FileOrchestrationRecord) (map[string]dynamoTypes.AttributeValue, error) {
	// First marshal normally
	item, err := attributevalue.MarshalMap(record)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal record: %w", err)
	}

	// Create a new map with corrected field names
	correctedItem := make(map[string]dynamoTypes.AttributeValue)

	// Define field name mappings from Go struct field names to DynamoDB attribute names
	fieldMappings := map[string]string{
		"File_path":               "file_path",
		"Job_id":                  "job_id",
		"File_name":               "file_name",
		"File_size":               "file_size",
		"File_hash":               "file_hash",
		"S3_prefix":               "s3_prefix",
		"Target_table":            "target_table",
		"Target_schema":           "target_schema",
		"Processing_script":       "processing_script",
		"Orchestration_state":     "orchestration_state",
		"State_history":           "state_history",
		"Current_stage":           "current_stage",
		"Processing_initiated_at": "processing_initiated_at",
		"Validated_at":            "validated_at",
		"Processing_started":      "processing_started",
		"Processing_completed":    "processing_completed",
		"Loaded_at":               "loaded_at",
		"Validation_results":      "validation_results",
		"Emr_job_details":         "emr_job_details",
		"Load_results":            "load_results",
		"Error_history":           "error_history",
		"Retry_count":             "retry_count",
		"Batch_id":                "batch_id",
		"Source_system":           "source_system",
		"Expires_at":              "expires_at",
		"Version":                 "version",
		"Control_data":            "control_data",
	}

	// Map each field to the correct DynamoDB attribute name
	for goFieldName, dynamoAttrName := range fieldMappings {
		if value, exists := item[goFieldName]; exists {
			correctedItem[dynamoAttrName] = value
		}
	}

	return correctedItem, nil
}

// PutFileOrchestrationRecord stores a file orchestration record in DynamoDB
func PutFileOrchestrationRecord(dynamoClient *dynamodb.Client, tableName string, record *types.FileOrchestrationRecord) error {
	item, err := marshalFileOrchestrationRecord(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	}

	_, err = dynamoClient.PutItem(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to put item in DynamoDB: %w", err)
	}

	return nil
}

// unmarshalFileOrchestrationRecord unmarshals a DynamoDB item with proper field name mapping
func unmarshalFileOrchestrationRecord(item map[string]dynamoTypes.AttributeValue) (*types.FileOrchestrationRecord, error) {
	// Create a new map with corrected field names
	correctedItem := make(map[string]dynamoTypes.AttributeValue)

	// Define reverse field name mappings from DynamoDB attribute names to Go struct field names
	reverseFieldMappings := map[string]string{
		"file_path":               "File_path",
		"job_id":                  "Job_id",
		"file_name":               "File_name",
		"file_size":               "File_size",
		"file_hash":               "File_hash",
		"s3_prefix":               "S3_prefix",
		"target_table":            "Target_table",
		"target_schema":           "Target_schema",
		"processing_script":       "Processing_script",
		"orchestration_state":     "Orchestration_state",
		"state_history":           "State_history",
		"current_stage":           "Current_stage",
		"processing_initiated_at": "Processing_initiated_at",
		"validated_at":            "Validated_at",
		"processing_started":      "Processing_started",
		"processing_completed":    "Processing_completed",
		"loaded_at":               "Loaded_at",
		"validation_results":      "Validation_results",
		"emr_job_details":         "Emr_job_details",
		"load_results":            "Load_results",
		"error_history":           "Error_history",
		"retry_count":             "Retry_count",
		"batch_id":                "Batch_id",
		"source_system":           "Source_system",
		"expires_at":              "Expires_at",
		"version":                 "Version",
		"control_data":            "Control_data",
	}

	// Map each DynamoDB attribute to the correct Go field name
	for dynamoAttrName, goFieldName := range reverseFieldMappings {
		if value, exists := item[dynamoAttrName]; exists {
			correctedItem[goFieldName] = value
		}
	}

	// Unmarshal with corrected field names
	var record types.FileOrchestrationRecord
	err := attributevalue.UnmarshalMap(correctedItem, &record)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal record: %w", err)
	}

	return &record, nil
}

// GetFileOrchestrationRecord retrieves a file orchestration record from DynamoDB
func GetFileOrchestrationRecord(dynamoClient *dynamodb.Client, tableName, filePath, jobID string) (*types.FileOrchestrationRecord, error) {
	key := map[string]dynamoTypes.AttributeValue{
		"file_path": &dynamoTypes.AttributeValueMemberS{Value: filePath},
		"job_id":    &dynamoTypes.AttributeValueMemberS{Value: jobID},
	}

	input := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       key,
	}

	result, err := dynamoClient.GetItem(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to get item from DynamoDB: %w", err)
	}

	if result.Item == nil {
		return nil, fmt.Errorf("file orchestration record not found: %s", filePath)
	}

	record, err := unmarshalFileOrchestrationRecord(result.Item)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal record: %w", err)
	}

	return record, nil
}

// CreateDynamoDBTable creates a DynamoDB table with the standard FlakeRunner schema
func CreateDynamoDBTable(dynamoClient *dynamodb.Client, tableName string) error {
	// Check if table already exists
	if err := ValidateDynamoDBTable(dynamoClient, tableName); err == nil {
		return nil // Table already exists
	}

	input := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
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
			{
				AttributeName: aws.String("batch_id"),
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
			{
				IndexName: aws.String("BatchIndex"),
				KeySchema: []dynamoTypes.KeySchemaElement{
					{
						AttributeName: aws.String("batch_id"),
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
			{
				IndexName: aws.String("JobOrchestrationIndex"),
				KeySchema: []dynamoTypes.KeySchemaElement{
					{
						AttributeName: aws.String("job_id"),
						KeyType:       dynamoTypes.KeyTypeHash,
					},
					{
						AttributeName: aws.String("orchestration_state"),
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
	}

	_, err := dynamoClient.CreateTable(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return nil
}
