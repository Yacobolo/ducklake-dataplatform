package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseS3Path(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{
			name:       "standard",
			input:      "s3://my-bucket/path/to/file.parquet",
			wantBucket: "my-bucket",
			wantKey:    "path/to/file.parquet",
		},
		{
			name:       "nested",
			input:      "s3://bucket/a/b/c/d.parquet",
			wantBucket: "bucket",
			wantKey:    "a/b/c/d.parquet",
		},
		{
			name:    "wrong_scheme",
			input:   "https://bucket/key",
			wantErr: true,
		},
		{
			name:    "empty_key",
			input:   "s3://bucket/",
			wantErr: true,
		},
		{
			name:    "no_scheme",
			input:   "bucket/key",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := ParseS3Path(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantBucket, bucket)
			assert.Equal(t, tt.wantKey, key)
		})
	}
}

func TestParseAzurePath(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantContainer string
		wantKey       string
		wantErr       bool
	}{
		{
			name:          "abfss",
			input:         "abfss://mycontainer@myaccount.dfs.core.windows.net/path/to/file.parquet",
			wantContainer: "mycontainer",
			wantKey:       "path/to/file.parquet",
		},
		{
			name:          "az_scheme",
			input:         "az://mycontainer/path/to/file.parquet",
			wantContainer: "mycontainer",
			wantKey:       "path/to/file.parquet",
		},
		{
			name:          "https_blob",
			input:         "https://myaccount.blob.core.windows.net/mycontainer/path/to/file.parquet",
			wantContainer: "mycontainer",
			wantKey:       "path/to/file.parquet",
		},
		{
			name:    "s3_scheme_error",
			input:   "s3://bucket/key",
			wantErr: true,
		},
		{
			name:    "garbage",
			input:   "not-a-url",
			wantErr: true,
		},
		{
			name:    "empty_key_abfss",
			input:   "abfss://container@account.dfs.core.windows.net/",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			container, key, err := parseAzurePath(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantContainer, container)
			assert.Equal(t, tt.wantKey, key)
		})
	}
}

func TestParseGCSPath(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{
			name:       "standard",
			input:      "gs://my-bucket/path/to/file.parquet",
			wantBucket: "my-bucket",
			wantKey:    "path/to/file.parquet",
		},
		{
			name:    "wrong_scheme",
			input:   "s3://bucket/key",
			wantErr: true,
		},
		{
			name:    "empty_key",
			input:   "gs://bucket/",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, key, err := parseGCSPath(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantBucket, bucket)
			assert.Equal(t, tt.wantKey, key)
		})
	}
}
