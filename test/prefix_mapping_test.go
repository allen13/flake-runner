package test

import (
	"testing"

	"github.com/allen13/flake-runner/pkg/config"
	"github.com/allen13/flake-runner/pkg/flakerunner"
	"github.com/stretchr/testify/assert"
)

func TestDetermineTargetTableNestedPrefix(t *testing.T) {
	fr := &flakerunner.FlakeRunner{
		Config: &config.Config{
			PrefixMappings: []config.PrefixMapping{
				{S3Prefix: "customers/", TargetName: "CUSTOMERS"},
				{S3Prefix: "orders/", TargetName: "ORDERS"},
			},
		},
	}

	filePath := "s3://bucket/customers/2024/01/data.csv"
	target, mapping, err := fr.DetermineTargetTable(filePath)
	assert.NoError(t, err)
	assert.Equal(t, "CUSTOMERS", target)
	assert.NotNil(t, mapping)
	assert.Equal(t, "customers/", mapping.S3Prefix)
}
