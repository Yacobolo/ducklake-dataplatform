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
			req:     CreateStorageCredentialRequest{Name: "c", CredentialType: "GCS", KeyID: "k", Secret: "s", Endpoint: "e", Region: "r"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateStorageCredentialRequest(tt.req)
			if tt.wantErr {
				require.Error(t, err)
				assert.IsType(t, &ValidationError{}, err)
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
			req:     CreateExternalLocationRequest{Name: "my_loc", URL: "s3://b/", CredentialName: "c", StorageType: "GCS"},
			wantErr: true,
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
