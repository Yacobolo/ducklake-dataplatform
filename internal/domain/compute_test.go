package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateCreateComputeEndpointRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     CreateComputeEndpointRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_remote",
			req: CreateComputeEndpointRequest{
				Name:      "analytics-xl",
				URL:       "grpc://compute-1.example.com:9444",
				Type:      "REMOTE",
				AuthToken: "secret",
			},
			wantErr: false,
		},
		{
			name: "valid_local",
			req: CreateComputeEndpointRequest{
				Name: "local-dev",
				URL:  "http://localhost:9443",
				Type: "LOCAL",
			},
			wantErr: false,
		},
		{
			name: "valid_with_size",
			req: CreateComputeEndpointRequest{
				Name: "sized",
				URL:  "grpc://example.com:9444",
				Type: "REMOTE",
				Size: "LARGE",
			},
			wantErr: false,
		},
		{
			name: "missing_name",
			req: CreateComputeEndpointRequest{
				URL:  "grpc://example.com:9444",
				Type: "REMOTE",
			},
			wantErr: true,
			errMsg:  "name is required",
		},
		{
			name: "missing_url",
			req: CreateComputeEndpointRequest{
				Name: "test",
				Type: "REMOTE",
			},
			wantErr: true,
			errMsg:  "url is required",
		},
		{
			name: "missing_type",
			req: CreateComputeEndpointRequest{
				Name: "test",
				URL:  "grpc://example.com:9444",
			},
			wantErr: true,
			errMsg:  "type is required",
		},
		{
			name: "invalid_type",
			req: CreateComputeEndpointRequest{
				Name: "test",
				URL:  "grpc://example.com:9444",
				Type: "CLOUD",
			},
			wantErr: true,
			errMsg:  "type must be LOCAL or REMOTE",
		},
		{
			name: "invalid_size",
			req: CreateComputeEndpointRequest{
				Name: "test",
				URL:  "grpc://example.com:9444",
				Type: "REMOTE",
				Size: "HUGE",
			},
			wantErr: true,
			errMsg:  "size must be SMALL, MEDIUM, or LARGE",
		},
		{
			name: "valid_size_small",
			req: CreateComputeEndpointRequest{
				Name: "test",
				URL:  "grpc://example.com:9444",
				Type: "REMOTE",
				Size: "SMALL",
			},
			wantErr: false,
		},
		{
			name: "valid_size_medium",
			req: CreateComputeEndpointRequest{
				Name: "test",
				URL:  "grpc://example.com:9444",
				Type: "REMOTE",
				Size: "MEDIUM",
			},
			wantErr: false,
		},
		{
			name: "invalid_remote_scheme",
			req: CreateComputeEndpointRequest{
				Name: "test",
				URL:  "https://example.com",
				Type: "REMOTE",
			},
			wantErr: true,
			errMsg:  "remote url must use grpc:// or grpcs://",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCreateComputeEndpointRequest(tt.req)
			if tt.wantErr {
				require.Error(t, err)
				var valErr *ValidationError
				require.ErrorAs(t, err, &valErr)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateCreateComputeAssignmentRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     CreateComputeAssignmentRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid_user",
			req: CreateComputeAssignmentRequest{
				PrincipalID:   "1",
				PrincipalType: "user",
				IsDefault:     true,
			},
			wantErr: false,
		},
		{
			name: "valid_group",
			req: CreateComputeAssignmentRequest{
				PrincipalID:   "5",
				PrincipalType: "group",
				FallbackLocal: true,
			},
			wantErr: false,
		},
		{
			name: "missing_principal_id",
			req: CreateComputeAssignmentRequest{
				PrincipalType: "user",
			},
			wantErr: true,
			errMsg:  "principal_id is required",
		},
		{
			name: "zero_principal_id",
			req: CreateComputeAssignmentRequest{
				PrincipalID:   "",
				PrincipalType: "user",
			},
			wantErr: true,
			errMsg:  "principal_id is required",
		},
		{
			name: "negative_principal_id",
			req: CreateComputeAssignmentRequest{
				PrincipalID:   "",
				PrincipalType: "user",
			},
			wantErr: true,
			errMsg:  "principal_id is required",
		},
		{
			name: "missing_principal_type",
			req: CreateComputeAssignmentRequest{
				PrincipalID: "1",
			},
			wantErr: true,
			errMsg:  "principal_type is required",
		},
		{
			name: "invalid_principal_type",
			req: CreateComputeAssignmentRequest{
				PrincipalID:   "1",
				PrincipalType: "role",
			},
			wantErr: true,
			errMsg:  "principal_type must be user or group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCreateComputeAssignmentRequest(tt.req)
			if tt.wantErr {
				require.Error(t, err)
				var valErr *ValidationError
				require.ErrorAs(t, err, &valErr)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
