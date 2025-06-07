package config

import (
	"encoding/json"
	"fmt"
	"os"

	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

// Config represents the main configuration structure for EMR Serverless orchestration
type Config struct {
	AWSProfile          string          `json:"aws_profile"`
	AWSRegion           string          `json:"aws_region"`
	InputBucketName     string          `json:"input_bucket_name"`
	OutputBucketName    string          `json:"output_bucket_name"`
	StagingBucketName   string          `json:"staging_bucket_name"`
	ControlTableName    string          `json:"control_table_name"`
	EMRApplicationID    string          `json:"emr_application_id"`
	EMRExecutionRoleARN string          `json:"emr_execution_role_arn"`
	JobTimeoutMinutes   int             `json:"job_timeout_minutes"`
	MaxRetries          int             `json:"max_retries"`
	ControlTTLDays      int             `json:"control_ttl_days"`
	PrefixMappings      []PrefixMapping `json:"prefix_mappings"`
}

// PrefixMapping defines how S3 prefixes map to EMR processing scripts and configurations
type PrefixMapping struct {
	S3Prefix         string            `json:"s3_prefix"`
	TargetName       string            `json:"target_name"`
	ContainerImage   string            `json:"container_image"`
	EntryPoint       string            `json:"entry_point"`
	ScriptParams     map[string]string `json:"script_params,omitempty"`
	EnvironmentVars  map[string]string `json:"environment_vars,omitempty"`
	ValidationRules  ValidationRules   `json:"validation_rules"`
	ProcessingConfig ProcessingConfig  `json:"processing_config"`
}

// ProcessingConfig contains format-specific processing configuration
type ProcessingConfig struct {
	SparkConfig     map[string]string `json:"spark_config,omitempty"`
	MaxFileSize     int64             `json:"max_file_size"`
	ChunkSize       int64             `json:"chunk_size"`
	FileFormat      string            `json:"file_format"`
	CompressionType string            `json:"compression_type"`
	OutputFormat    string            `json:"output_format,omitempty"`
	OutputOptions   map[string]string `json:"output_options,omitempty"`
}

// ValidationRules defines validation requirements for data files
type ValidationRules struct {
	ValidateRecordCount bool     `json:"validate_record_count"`
	ValidateFileSize    bool     `json:"validate_file_size"`
	ValidateChecksum    bool     `json:"validate_checksum"`
	RequiredFields      []string `json:"required_fields"`
}

// LoadConfig loads configuration from a file path
func LoadConfig(configPath string) (*Config, error) {
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("configuration file not found: %s", configPath)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read configuration file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse configuration JSON: %w", err)
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	setDefaults(&cfg)
	return &cfg, nil
}

// LoadAWSConfig creates AWS configuration from the config
func LoadAWSConfig(cfg *Config) (*aws.Config, error) {
	ctx := context.TODO()
	var options []func(*config.LoadOptions) error

	options = append(options, config.WithRegion(cfg.AWSRegion))

	if cfg.AWSProfile != "" {
		options = append(options, config.WithSharedConfigProfile(cfg.AWSProfile))
	}

	awsConfig, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &awsConfig, nil
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	if cfg.AWSRegion == "" {
		return fmt.Errorf("aws_region is required")
	}
	if cfg.InputBucketName == "" {
		return fmt.Errorf("input_bucket_name is required")
	}
	if cfg.OutputBucketName == "" {
		return fmt.Errorf("output_bucket_name is required")
	}
	if cfg.StagingBucketName == "" {
		return fmt.Errorf("staging_bucket_name is required")
	}
	if cfg.ControlTableName == "" {
		return fmt.Errorf("control_table_name is required")
	}
	if cfg.EMRApplicationID == "" {
		return fmt.Errorf("emr_application_id is required")
	}
	if cfg.EMRExecutionRoleARN == "" {
		return fmt.Errorf("emr_execution_role_arn is required")
	}
	if len(cfg.PrefixMappings) == 0 {
		return fmt.Errorf("at least one prefix mapping is required")
	}

	return nil
}

func setDefaults(cfg *Config) {
	if cfg.JobTimeoutMinutes <= 0 {
		cfg.JobTimeoutMinutes = 60
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.ControlTTLDays <= 0 {
		cfg.ControlTTLDays = 30
	}

	for i := range cfg.PrefixMappings {
		mapping := &cfg.PrefixMappings[i]
		if mapping.ProcessingConfig.FileFormat == "" {
			mapping.ProcessingConfig.FileFormat = "CSV"
		}
		if mapping.ProcessingConfig.MaxFileSize <= 0 {
			mapping.ProcessingConfig.MaxFileSize = 5 * 1024 * 1024 * 1024
		}
		if mapping.ProcessingConfig.OutputFormat == "" {
			mapping.ProcessingConfig.OutputFormat = "PARQUET"
		}
		if mapping.EntryPoint == "" {
			mapping.EntryPoint = "/opt/spark/jobs/processor.py"
		}
	}
}
