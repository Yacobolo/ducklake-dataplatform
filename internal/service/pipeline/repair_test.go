package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/testutil"
)

func TestRepairSelection_FromJobRequiresPipelineJob(t *testing.T) {
	svc := NewService(
		&testutil.MockPipelineRepo{},
		&testutil.MockPipelineRunRepo{
			ListJobRunsByRunFn: func(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
				return []domain.PipelineJobRun{{RunID: runID, JobID: "job-1", Status: domain.PipelineJobRunStatusFailed}}, nil
			},
		},
		&testutil.MockAuditRepo{},
		&testutil.MockNotebookProvider{},
		nil,
		nil,
		nil,
	)

	_, err := svc.repairSelection(context.Background(), "run-1", []domain.PipelineJob{
		{ID: "job-1", Name: "extract"},
		{ID: "job-2", Name: "load", DependsOn: []string{"extract"}},
	}, domain.RepairPipelineRunRequest{
		Mode:      domain.PipelineRepairModeFromJob,
		FromJobID: pipelineMessagePtr("missing-job"),
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "from_job_id")
}
