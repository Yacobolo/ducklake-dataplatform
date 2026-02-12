package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateStorageCredentialRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     CreateStorageCredentialRequest
		wantErr bool
	}{
		{
			name:    "valid request",
			req:     CreateStorageCredentialRequest{Name: "my_cred", CredentialType: CredentialTypeS3, KeyID: "k", Secret: "s", Endpoint: "e", Region: "r"},
			wantErr: false,
		},
		{
			name:    "missing name",
			req:     CreateStorageCredentialRequest{CredentialType: CredentialTypeS3, KeyID: "k", Secret: "s", Endpoint: "e", Region: "r"},
			wantErr: true,
		},
		{
			name:    "missing key_id",
			req:     CreateStorageCredentialRequest{Name: "c", CredentialType: CredentialTypeS3, Secret: "s", Endpoint: "e", Region: "r"},
			wantErr: true,
		},
		{
			name:    "missing secret",
			req:     CreateStorageCredentialRequest{Name: "c", CredentialType: CredentialTypeS3, KeyID: "k", Endpoint: "e", Region: "r"},
			wantErr: true,
		},
		{
			name:    "missing endpoint",
			req:     CreateStorageCredentialRequest{Name: "c", CredentialType: CredentialTypeS3, KeyID: "k", Secret: "s", Region: "r"},
			wantErr: true,
		},
		{
			name:    "missing region",
			req:     CreateStorageCredentialRequest{Name: "c", CredentialType: CredentialTypeS3, KeyID: "k", Secret: "s", Endpoint: "e"},
			wantErr: true,
		},
		{
			name:    "unsupported type",
			req:     CreateStorageCredentialRequest{Name: "c", CredentialType: "INVALID", KeyID: "k", Secret: "s", Endpoint: "e", Region: "r"},
			wantErr: true,
		},
		// Azure credential tests
		{
			name:    "valid azure with account key",
			req:     CreateStorageCredentialRequest{Name: "az_cred", CredentialType: CredentialTypeAzure, AzureAccountName: "myaccount", AzureAccountKey: "mykey=="},
			wantErr: false,
		},
		{
			name:    "valid azure with service principal",
			req:     CreateStorageCredentialRequest{Name: "az_cred", CredentialType: CredentialTypeAzure, AzureAccountName: "myaccount", AzureClientID: "cid", AzureTenantID: "tid", AzureClientSecret: "csecret"},
			wantErr: false,
		},
		{
			name:    "azure missing account name",
			req:     CreateStorageCredentialRequest{Name: "az_cred", CredentialType: CredentialTypeAzure, AzureAccountKey: "mykey=="},
			wantErr: true,
		},
		{
			name:    "azure missing both key and service principal",
			req:     CreateStorageCredentialRequest{Name: "az_cred", CredentialType: CredentialTypeAzure, AzureAccountName: "myaccount"},
			wantErr: true,
		},
		{
			name:    "azure incomplete service principal",
			req:     CreateStorageCredentialRequest{Name: "az_cred", CredentialType: CredentialTypeAzure, AzureAccountName: "myaccount", AzureClientID: "cid"},
			wantErr: true,
		},
		// GCS credential tests
		{
			name:    "valid gcs",
			req:     CreateStorageCredentialRequest{Name: "gcs_cred", CredentialType: CredentialTypeGCS, GCSKeyFilePath: "/path/to/key.json"},
			wantErr: false,
		},
		{
			name:    "gcs missing key file path",
			req:     CreateStorageCredentialRequest{Name: "gcs_cred", CredentialType: CredentialTypeGCS},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateStorageCredentialRequest(tt.req)
			if tt.wantErr {
				require.Error(t, err)
				var validationErr *ValidationError
				assert.ErrorAs(t, err, &validationErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateExternalLocationRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     CreateExternalLocationRequest
		wantErr bool
	}{
		{
			name:    "valid request",
			req:     CreateExternalLocationRequest{Name: "my_loc", URL: "s3://bucket/prefix/", CredentialName: "my_cred", StorageType: StorageTypeS3},
			wantErr: false,
		},
		{
			name:    "valid without storage type",
			req:     CreateExternalLocationRequest{Name: "my_loc", URL: "s3://bucket/prefix/", CredentialName: "my_cred"},
			wantErr: false,
		},
		{
			name:    "missing name",
			req:     CreateExternalLocationRequest{URL: "s3://bucket/", CredentialName: "my_cred"},
			wantErr: true,
		},
		{
			name:    "missing url",
			req:     CreateExternalLocationRequest{Name: "my_loc", CredentialName: "my_cred"},
			wantErr: true,
		},
		{
			name:    "missing credential_name",
			req:     CreateExternalLocationRequest{Name: "my_loc", URL: "s3://b/p/"},
			wantErr: true,
		},
		{
			name:    "unsupported storage type",
			req:     CreateExternalLocationRequest{Name: "my_loc", URL: "s3://b/", CredentialName: "c", StorageType: "INVALID"},
			wantErr: true,
		},
		{
			name:    "valid azure storage type",
			req:     CreateExternalLocationRequest{Name: "az_loc", URL: "az://container/path/", CredentialName: "az_cred", StorageType: StorageTypeAzure},
			wantErr: false,
		},
		{
			name:    "valid gcs storage type",
			req:     CreateExternalLocationRequest{Name: "gcs_loc", URL: "gs://bucket/path/", CredentialName: "gcs_cred", StorageType: StorageTypeGCS},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateExternalLocationRequest(tt.req)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
