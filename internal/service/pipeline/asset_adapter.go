//nolint:revive // adapter types are exported for orchestration integration tests.
package pipeline

import (
	"fmt"

	"duck-demo/internal/domain"
)

type AdaptedPipelineAssets struct {
	Assets       []domain.DataAsset
	Dependencies []domain.AssetDependency
}

func BuildPipelineAssetGraph(pipeline *domain.Pipeline, jobs []domain.PipelineJob) (*AdaptedPipelineAssets, error) {
	if pipeline == nil {
		return nil, domain.ErrValidation("pipeline is required")
	}
	if len(jobs) == 0 {
		return nil, domain.ErrValidation("at least one pipeline job is required")
	}

	jobByName := make(map[string]domain.PipelineJob, len(jobs))
	assets := make([]domain.DataAsset, 0, len(jobs))
	for _, job := range jobs {
		jobByName[job.Name] = job
		assetType := domain.AssetTypeNotebook
		if job.JobType == domain.PipelineJobTypeModelRun {
			assetType = domain.AssetTypeModel
		}
		asset := domain.DataAsset{
			ID:        job.ID,
			AssetKey:  fmt.Sprintf("pipeline.%s.%s", pipeline.Name, job.Name),
			AssetType: assetType,
			Owner:     pipeline.CreatedBy,
			CreatedBy: pipeline.CreatedBy,
			IsActive:  !pipeline.IsPaused,
			Tags:      []string{"pipeline", pipeline.Name},
		}
		assets = append(assets, asset)
	}

	deps := make([]domain.AssetDependency, 0)
	for _, job := range jobs {
		for _, upstreamName := range job.DependsOn {
			upstream, ok := jobByName[upstreamName]
			if !ok {
				return nil, domain.ErrValidation("pipeline job %q depends on unknown job %q", job.Name, upstreamName)
			}
			deps = append(deps, domain.AssetDependency{
				ID:              domain.NewID(),
				AssetID:         job.ID,
				UpstreamAssetID: upstream.ID,
				DependencyType:  domain.DependencyTypeHard,
			})
		}
	}

	return &AdaptedPipelineAssets{Assets: assets, Dependencies: deps}, nil
}
