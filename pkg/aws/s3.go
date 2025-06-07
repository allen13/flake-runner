package aws

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/allen13/flake-runner/pkg/types"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3Types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ParseS3Path parses S3 paths into bucket and key components
func ParseS3Path(s3Path string) (bucket, key string, err error) {
	path := strings.TrimPrefix(s3Path, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid S3 path format: %s", s3Path)
	}
	return parts[0], parts[1], nil
}

// GetS3ObjectMetadata retrieves metadata for an S3 object
func GetS3ObjectMetadata(s3Client *s3.Client, bucket, key string) (*types.S3Object, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := s3Client.HeadObject(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("object not found: %w", err)
	}

	s3Object := &types.S3Object{
		Bucket: bucket,
		Key:    key,
	}

	if result.ContentLength != nil {
		s3Object.Size = *result.ContentLength
	}

	if result.LastModified != nil {
		s3Object.ModTime = *result.LastModified
	}

	if result.ETag != nil {
		s3Object.ETag = *result.ETag
	}

	return s3Object, nil
}

// DownloadS3ObjectAsBytes downloads an S3 object and returns its content as bytes
func DownloadS3ObjectAsBytes(s3Client *s3.Client, bucket, key string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := s3Client.GetObject(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s/%s: %w", bucket, key, err)
	}
	defer result.Body.Close()

	content, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object content: %w", err)
	}

	return content, nil
}

// GetS3ObjectReader returns an io.ReadCloser for streaming S3 object content
func GetS3ObjectReader(s3Client *s3.Client, bucket, key string) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := s3Client.GetObject(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s/%s: %w", bucket, key, err)
	}

	return result.Body, nil
}

// ValidateS3Bucket validates that an S3 bucket exists and is accessible
func ValidateS3Bucket(s3Client *s3.Client, bucketName string) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}

	_, err := s3Client.HeadBucket(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("bucket %s is not accessible: %w", bucketName, err)
	}

	return nil
}

// CreateS3Bucket creates an S3 bucket if it doesn't exist
func CreateS3Bucket(s3Client *s3.Client, bucketName, region string) error {
	// Check if bucket already exists
	if err := ValidateS3Bucket(s3Client, bucketName); err == nil {
		return nil // Bucket already exists
	}

	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}

	// For regions other than us-east-1, need to specify location constraint
	if region != "us-east-1" {
		input.CreateBucketConfiguration = &s3Types.CreateBucketConfiguration{
			LocationConstraint: s3Types.BucketLocationConstraint(region),
		}
	}

	_, err := s3Client.CreateBucket(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to create bucket %s: %w", bucketName, err)
	}

	return nil
}

// UploadToS3 uploads content to an S3 bucket
func UploadToS3(s3Client *s3.Client, bucketName, key string, content []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   strings.NewReader(string(content)),
	}

	_, err := s3Client.PutObject(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to upload to s3://%s/%s: %w", bucketName, key, err)
	}

	return nil
}

// DownloadFromS3 downloads content from an S3 bucket
func DownloadFromS3(s3Client *s3.Client, bucketName, key string) ([]byte, error) {
	return DownloadS3ObjectAsBytes(s3Client, bucketName, key)
}
