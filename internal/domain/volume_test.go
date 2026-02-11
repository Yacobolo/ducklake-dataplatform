package domain

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateCreateVolumeRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     CreateVolumeRequest
		wantErr string
	}{
		{
			name: "valid managed volume",
			req: CreateVolumeRequest{
				Name:       "my_volume",
				VolumeType: VolumeTypeManaged,
			},
		},
		{
			name: "valid external volume",
			req: CreateVolumeRequest{
				Name:            "ext_volume",
				VolumeType:      VolumeTypeExternal,
				StorageLocation: "s3://bucket/path",
			},
		},
		{
			name: "empty name",
			req: CreateVolumeRequest{
				VolumeType: VolumeTypeManaged,
			},
			wantErr: "volume name is required",
		},
		{
			name: "name too long",
			req: CreateVolumeRequest{
				Name:       strings.Repeat("a", 129),
				VolumeType: VolumeTypeManaged,
			},
			wantErr: "at most 128 characters",
		},
		{
			name: "missing volume_type",
			req: CreateVolumeRequest{
				Name: "vol",
			},
			wantErr: "volume_type is required",
		},
		{
			name: "unsupported volume_type",
			req: CreateVolumeRequest{
				Name:       "vol",
				VolumeType: "UNKNOWN",
			},
			wantErr: "unsupported volume type",
		},
		{
			name: "external without storage_location",
			req: CreateVolumeRequest{
				Name:       "vol",
				VolumeType: VolumeTypeExternal,
			},
			wantErr: "storage_location is required for EXTERNAL volumes",
		},
		{
			name: "managed with comment",
			req: CreateVolumeRequest{
				Name:       "vol",
				VolumeType: VolumeTypeManaged,
				Comment:    "some comment",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCreateVolumeRequest(tt.req)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.IsType(t, &ValidationError{}, err)
			}
		})
	}
}
