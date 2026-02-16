package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
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

// === Factory dispatch: NewPresignerFromCredential ===

func TestNewPresignerFromCredential(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cred        *domain.StorageCredential
		storagePath string
		wantType    string // "S3", "Azure", "GCS", or "" for error
		wantErr     bool
	}{
		{
			name: "S3 credential returns S3Presigner",
			cred: &domain.StorageCredential{
				CredentialType: domain.CredentialTypeS3,
				KeyID:          "AKID",
				Secret:         "secret",
				Endpoint:       "s3.example.com",
				Region:         "us-east-1",
			},
			storagePath: "s3://my-bucket/prefix/file.parquet",
			wantType:    "S3",
		},
		{
			name: "Azure credential returns AzurePresigner",
			cred: &domain.StorageCredential{
				CredentialType:   domain.CredentialTypeAzure,
				AzureAccountName: "myaccount",
				AzureAccountKey:  "bXlrZXk=", // base64 "mykey"
			},
			storagePath: "az://mycontainer/path/to/file.parquet",
			wantType:    "Azure",
		},
		{
			name: "unknown credential type returns error",
			cred: &domain.StorageCredential{
				CredentialType: "UNKNOWN",
			},
			storagePath: "s3://bucket/key.parquet",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			presigner, err := NewPresignerFromCredential(tt.cred, tt.storagePath)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, presigner)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, presigner)

			switch tt.wantType {
			case "S3":
				_, ok := presigner.(*S3Presigner)
				assert.True(t, ok, "expected *S3Presigner, got %T", presigner)
			case "Azure":
				_, ok := presigner.(*AzurePresigner)
				assert.True(t, ok, "expected *AzurePresigner, got %T", presigner)
			case "GCS":
				_, ok := presigner.(*GCSPresigner)
				assert.True(t, ok, "expected *GCSPresigner, got %T", presigner)
			}
		})
	}
}

// === Factory dispatch: NewUploadPresignerFromCredential ===

func TestNewUploadPresignerFromCredential(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cred        *domain.StorageCredential
		storagePath string
		wantType    string
		wantErr     bool
	}{
		{
			name: "S3 credential returns S3 upload presigner",
			cred: &domain.StorageCredential{
				CredentialType: domain.CredentialTypeS3,
				KeyID:          "AKID",
				Secret:         "secret",
				Endpoint:       "s3.example.com",
				Region:         "us-east-1",
			},
			storagePath: "s3://my-bucket/prefix/file.parquet",
			wantType:    "S3",
		},
		{
			name: "Azure credential returns Azure upload presigner",
			cred: &domain.StorageCredential{
				CredentialType:   domain.CredentialTypeAzure,
				AzureAccountName: "myaccount",
				AzureAccountKey:  "bXlrZXk=",
			},
			storagePath: "az://mycontainer/path/to/file.parquet",
			wantType:    "Azure",
		},
		{
			name: "unknown credential type returns error",
			cred: &domain.StorageCredential{
				CredentialType: "INVALID",
			},
			storagePath: "s3://bucket/key.parquet",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			presigner, err := NewUploadPresignerFromCredential(tt.cred, tt.storagePath)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, presigner)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, presigner)

			// All returned types should implement FileUploadPresigner.
			assert.NotEmpty(t, presigner.Bucket(), "Bucket() should return a non-empty string")

			switch tt.wantType {
			case "S3":
				_, ok := presigner.(*S3Presigner)
				assert.True(t, ok, "expected *S3Presigner, got %T", presigner)
			case "Azure":
				_, ok := presigner.(*AzurePresigner)
				assert.True(t, ok, "expected *AzurePresigner, got %T", presigner)
			case "GCS":
				_, ok := presigner.(*GCSPresigner)
				assert.True(t, ok, "expected *GCSPresigner, got %T", presigner)
			}
		})
	}
}
