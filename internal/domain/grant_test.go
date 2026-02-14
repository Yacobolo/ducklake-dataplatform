package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateGrantRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		req     CreateGrantRequest
		wantErr string
	}{
		{
			name: "valid request",
			req: CreateGrantRequest{
				PrincipalID:   "user-1",
				PrincipalType: "user",
				SecurableType: "table",
				SecurableID:   "table-1",
				Privilege:     "SELECT",
			},
		},
		{
			name: "empty principal_id",
			req: CreateGrantRequest{
				PrincipalType: "user",
				SecurableType: "table",
				SecurableID:   "table-1",
				Privilege:     "SELECT",
			},
			wantErr: "principal_id is required",
		},
		{
			name: "invalid principal_type",
			req: CreateGrantRequest{
				PrincipalID:   "user-1",
				PrincipalType: "robot",
				SecurableType: "table",
				SecurableID:   "table-1",
				Privilege:     "SELECT",
			},
			wantErr: "principal_type must be 'user' or 'group'",
		},
		{
			name: "empty securable_type",
			req: CreateGrantRequest{
				PrincipalID:   "user-1",
				PrincipalType: "user",
				SecurableID:   "table-1",
				Privilege:     "SELECT",
			},
			wantErr: "securable_type is required",
		},
		{
			name: "empty securable_id",
			req: CreateGrantRequest{
				PrincipalID:   "user-1",
				PrincipalType: "user",
				SecurableType: "table",
				Privilege:     "SELECT",
			},
			wantErr: "securable_id is required",
		},
		{
			name: "empty privilege",
			req: CreateGrantRequest{
				PrincipalID:   "user-1",
				PrincipalType: "user",
				SecurableType: "table",
				SecurableID:   "table-1",
			},
			wantErr: "privilege is required",
		},
		{
			name: "valid group request",
			req: CreateGrantRequest{
				PrincipalID:   "group-1",
				PrincipalType: "group",
				SecurableType: "schema",
				SecurableID:   "schema-1",
				Privilege:     "USAGE",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				var validationErr *ValidationError
				assert.ErrorAs(t, err, &validationErr)
			}
		})
	}
}
