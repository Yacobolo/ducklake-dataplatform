package api

import (
	"context"

	"duck-demo/internal/domain"
)

// pipelineService is retained for internal wiring while orchestration APIs
// cut over to the asset surface.
type pipelineService interface {
	CreatePipeline(ctx context.Context, principal string, req domain.CreatePipelineRequest) (*domain.Pipeline, error)
	GetPipeline(ctx context.Context, name string) (*domain.Pipeline, error)
	ListPipelines(ctx context.Context, page domain.PageRequest) ([]domain.Pipeline, int64, error)
	UpdatePipeline(ctx context.Context, principal string, name string, req domain.UpdatePipelineRequest) (*domain.Pipeline, error)
	DeletePipeline(ctx context.Context, principal string, name string) error
	CreateJob(ctx context.Context, principal string, pipelineName string, req domain.CreatePipelineJobRequest) (*domain.PipelineJob, error)
	ListJobs(ctx context.Context, pipelineName string) ([]domain.PipelineJob, error)
	DeleteJob(ctx context.Context, principal string, pipelineName string, jobID string) error
}
