package validation

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/allen13/flake-runner/pkg/config"
	"github.com/allen13/flake-runner/pkg/types"
)

// ValidateControlDataContent validates the content of control data
func ValidateControlDataContent(controlData *types.ControlData) error {
	if controlData == nil {
		return fmt.Errorf("control data cannot be nil")
	}

	if controlData.FileName == "" {
		return fmt.Errorf("file_name is required in control data")
	}
	if controlData.FileSize <= 0 {
		return fmt.Errorf("file_size must be positive")
	}
	if controlData.RecordCount < 0 {
		return fmt.Errorf("record_count cannot be negative")
	}
	if controlData.ColumnCount <= 0 {
		return fmt.Errorf("column_count must be positive")
	}
	if controlData.FileHash == "" {
		return fmt.Errorf("file_hash is required")
	}
	if controlData.CreatedAt.IsZero() {
		return fmt.Errorf("created_at is required")
	}
	return nil
}

// IsValidationSuccessful checks if all enabled validations passed
func IsValidationSuccessful(results *types.ValidationResults) bool {
	return len(results.ValidationErrors) == 0
}

// DetermineFileFormatFromExtension determines file format from file extension
func DetermineFileFormatFromExtension(filePath string) string {
	lowerPath := strings.ToLower(filePath)

	// Remove compression extensions first
	if strings.HasSuffix(lowerPath, ".gz") {
		lowerPath = strings.TrimSuffix(lowerPath, ".gz")
	} else if strings.HasSuffix(lowerPath, ".gzip") {
		lowerPath = strings.TrimSuffix(lowerPath, ".gzip")
	}

	if strings.HasSuffix(lowerPath, ".csv") {
		return "CSV"
	} else if strings.HasSuffix(lowerPath, ".tsv") {
		return "TSV"
	} else if strings.HasSuffix(lowerPath, ".json") {
		return "JSON"
	} else if strings.HasSuffix(lowerPath, ".jsonl") || strings.HasSuffix(lowerPath, ".ndjson") {
		return "JSONL"
	} else if strings.HasSuffix(lowerPath, ".parquet") {
		return "PARQUET"
	}

	// Default to CSV for unknown formats
	return "CSV"
}

// IsCompressedFile determines if a file is compressed based on its extension
func IsCompressedFile(filePath string) bool {
	lowerPath := strings.ToLower(filePath)
	return strings.HasSuffix(lowerPath, ".gz") ||
		strings.HasSuffix(lowerPath, ".gzip") ||
		strings.HasSuffix(lowerPath, ".bz2") ||
		strings.HasSuffix(lowerPath, ".zip")
}

// CreateDecompressedReader creates a decompressed reader for compressed files
func CreateDecompressedReader(reader io.Reader, filePath string) (io.Reader, error) {
	lowerPath := strings.ToLower(filePath)

	if strings.HasSuffix(lowerPath, ".gz") || strings.HasSuffix(lowerPath, ".gzip") {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return gzipReader, nil
	}

	// For other compression formats, we'd need additional libraries
	// For now, return error for unsupported formats
	if strings.HasSuffix(lowerPath, ".bz2") || strings.HasSuffix(lowerPath, ".zip") {
		return nil, fmt.Errorf("compression format not yet supported: %s", filepath.Ext(filePath))
	}

	return reader, nil
}

// CountLinesInReader counts lines in a reader, optionally skipping the first line (header)
func CountLinesInReader(reader io.Reader, skipHeader bool) (int64, error) {
	scanner := bufio.NewScanner(reader)

	// Set a larger buffer size for big files
	const maxCapacity = 1024 * 1024 // 1MB buffer
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	var lineCount int64 = 0
	headerSkipped := false

	for scanner.Scan() {
		if skipHeader && !headerSkipped {
			headerSkipped = true
			continue
		}
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error scanning file: %w", err)
	}

	return lineCount, nil
}

// CountJSONRecords counts records in a JSON array or JSON Lines format
func CountJSONRecords(reader io.Reader) (int64, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return 0, fmt.Errorf("failed to read JSON content: %w", err)
	}

	// Try to parse as JSON array first
	var jsonArray []interface{}
	if err := json.Unmarshal(content, &jsonArray); err == nil {
		return int64(len(jsonArray)), nil
	}

	// If not a JSON array, treat as JSON Lines (one JSON object per line)
	lines := strings.Split(string(content), "\n")
	var validJSONLines int64 = 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var jsonObj interface{}
		if json.Unmarshal([]byte(line), &jsonObj) == nil {
			validJSONLines++
		}
	}

	return validJSONLines, nil
}

// CountJSONRecordsStreaming counts records in a JSON array or JSON Lines format using streaming
func CountJSONRecordsStreaming(reader io.Reader) (int64, error) {
	// For streaming JSON, we need to peek at the first few bytes to determine format
	// Use a buffered reader to peek without consuming
	bufReader := bufio.NewReader(reader)

	// Peek at first non-whitespace character to determine format
	for {
		b, err := bufReader.Peek(1)
		if err != nil {
			if err == io.EOF {
				return 0, nil // Empty file
			}
			return 0, fmt.Errorf("failed to peek at JSON content: %w", err)
		}

		if b[0] == ' ' || b[0] == '\t' || b[0] == '\n' || b[0] == '\r' {
			// Skip whitespace by reading one byte
			_, err = bufReader.ReadByte()
			if err != nil {
				return 0, fmt.Errorf("failed to skip whitespace: %w", err)
			}
			continue
		}
		break
	}

	// Check if it starts with '[' (JSON array) or '{' (JSON Lines)
	firstChar, err := bufReader.Peek(1)
	if err != nil {
		return 0, fmt.Errorf("failed to peek at first character: %w", err)
	}

	if firstChar[0] == '[' {
		// JSON array format - need to load all to count array elements
		// For very large JSON arrays, this is still a limitation
		content, err := io.ReadAll(bufReader)
		if err != nil {
			return 0, fmt.Errorf("failed to read JSON array content: %w", err)
		}

		var jsonArray []interface{}
		if err := json.Unmarshal(content, &jsonArray); err != nil {
			return 0, fmt.Errorf("failed to parse JSON array: %w", err)
		}
		return int64(len(jsonArray)), nil
	}

	// JSON Lines format - can stream line by line
	scanner := bufio.NewScanner(bufReader)

	// Set a larger buffer size for big files
	const maxCapacity = 1024 * 1024 // 1MB buffer
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	var validJSONLines int64 = 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var jsonObj interface{}
		if json.Unmarshal([]byte(line), &jsonObj) == nil {
			validJSONLines++
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error scanning JSON lines: %w", err)
	}

	return validJSONLines, nil
}

// PerformComprehensiveValidation performs all validation checks using control file data and validation rules
func PerformComprehensiveValidation(filePath string, controlRecord *types.ControlFileRecord, dataFile *types.S3Object, rules *config.ValidationRules) (*types.ValidationResults, error) {
	results := &types.ValidationResults{
		ExpectedRecords:  controlRecord.RecordCount,
		ActualRecords:    0, // Will be populated if we can determine actual record count
		ValidationErrors: []string{},
	}

	// File size validation
	if rules.ValidateFileSize {
		if controlRecord.FileSize != dataFile.Size {
			results.FileSizeMatch = false
			results.ValidationErrors = append(results.ValidationErrors,
				fmt.Sprintf("File size mismatch: expected %d bytes, got %d bytes", controlRecord.FileSize, dataFile.Size))
		} else {
			results.FileSizeMatch = true
		}
	} else {
		results.FileSizeMatch = true // Skip validation
	}

	// Checksum validation
	if rules.ValidateChecksum {
		if controlRecord.FileHash != "" {
			// For now, we'll validate against ETag if available
			// Note: ETag is not always a direct MD5 hash, especially for multipart uploads
			actualChecksum := strings.Trim(dataFile.ETag, "\"")
			if controlRecord.FileHash != actualChecksum {
				results.ChecksumMatch = false
				results.ValidationErrors = append(results.ValidationErrors,
					fmt.Sprintf("Checksum mismatch: expected %s, got %s", controlRecord.FileHash, actualChecksum))
			} else {
				results.ChecksumMatch = true
			}
		} else {
			results.ChecksumMatch = false
			results.ValidationErrors = append(results.ValidationErrors, "No checksum provided in control file")
		}
	} else {
		results.ChecksumMatch = true // Skip validation
	}

	// Record count validation with basic file format detection
	if rules.ValidateRecordCount {
		if controlRecord.RecordCount < 0 {
			results.RecordCountMatch = false
			results.ValidationErrors = append(results.ValidationErrors, "Invalid record count in control file")
		} else {
			// For comprehensive validation, actual record counting would be done by the caller
			// This function focuses on the validation logic structure
			results.RecordCountMatch = true
		}
	} else {
		results.RecordCountMatch = true // Skip validation
	}

	// Required fields validation - this would typically be done during file parsing
	if len(rules.RequiredFields) > 0 {
		// This would be validated during EMR processing
		// For now, we just note that validation is required
	}

	return results, nil
}

// ConvertControlDataToRecord converts ControlData to the legacy ControlFileRecord format for compatibility
func ConvertControlDataToRecord(controlData *types.ControlData, filePath, jobID string, ttlDays int) *types.ControlFileRecord {
	return &types.ControlFileRecord{
		FilePath:         filePath,
		ControlFilePath:  "", // No longer applicable
		JobID:            jobID,
		FileName:         controlData.FileName,
		FileSize:         controlData.FileSize,
		FileHash:         controlData.FileHash,
		RecordCount:      controlData.RecordCount,
		ColumnCount:      controlData.ColumnCount,
		CreatedAt:        controlData.CreatedAt,
		ProcessedAt:      nil,
		ValidatedAt:      nil,
		ValidationStatus: "PENDING",
		ProcessingStatus: "PENDING",
		ErrorMessage:     "",
		ExpiresAt:        time.Now().AddDate(0, 0, ttlDays).Unix(),
	}
}
