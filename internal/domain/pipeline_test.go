package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreatePipelineRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		req     CreatePipelineRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid with all fields",
			req: CreatePipelineRequest{
				Name:             "etl-daily",
				Description:      "Daily ETL pipeline",
				ScheduleCron:     strPtr("0 0 * * *"),
				IsPaused:         false,
				ConcurrencyLimit: 5,
			},
			wantErr: false,
		},
		{
			name: "valid minimal",
			req: CreatePipelineRequest{
				Name: "simple-pipeline",
			},
			wantErr: false,
		},
		{
			name: "valid zero concurrency limit",
			req: CreatePipelineRequest{
				Name:             "pipeline-zero-concurrency",
				ConcurrencyLimit: 0,
			},
			wantErr: false,
		},
		{
			name: "empty name",
			req: CreatePipelineRequest{
				Description:      "Missing name",
				ConcurrencyLimit: 1,
			},
			wantErr: true,
			errMsg:  "name is required",
		},
		{
			name: "negative concurrency limit",
			req: CreatePipelineRequest{
				Name:             "bad-concurrency",
				ConcurrencyLimit: -1,
			},
			wantErr: true,
			errMsg:  "concurrency_limit must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
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

func TestCreatePipelineJobRequest_Validate(t *testing.T) {
	tests := []struct {
		name    string
		req     CreatePipelineJobRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid with all fields",
			req: CreatePipelineJobRequest{
				Name:              "transform-step",
				ComputeEndpointID: strPtr("ep-123"),
				DependsOn:         []string{"extract-step"},
				NotebookID:        "nb-456",
				TimeoutSeconds:    int64Ptr(3600),
				RetryCount:        3,
				JobOrder:          1,
			},
			wantErr: false,
		},
		{
			name: "valid minimal",
			req: CreatePipelineJobRequest{
				Name:       "simple-job",
				NotebookID: "nb-001",
			},
			wantErr: false,
		},
		{
			name: "valid zero retry count",
			req: CreatePipelineJobRequest{
				Name:       "no-retry-job",
				NotebookID: "nb-002",
				RetryCount: 0,
			},
			wantErr: false,
		},
		{
			name: "empty name",
			req: CreatePipelineJobRequest{
				NotebookID: "nb-001",
				RetryCount: 1,
			},
			wantErr: true,
			errMsg:  "name is required",
		},
		{
			name: "empty notebook_id",
			req: CreatePipelineJobRequest{
				Name:       "missing-notebook",
				RetryCount: 0,
			},
			wantErr: true,
			errMsg:  "notebook_id is required",
		},
		{
			name: "negative retry count",
			req: CreatePipelineJobRequest{
				Name:       "bad-retry",
				NotebookID: "nb-001",
				RetryCount: -1,
			},
			wantErr: true,
			errMsg:  "retry_count must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
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

func strPtr(s string) *string { return &s }
func int64Ptr(n int64) *int64 { return &n }
