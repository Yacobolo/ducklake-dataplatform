//nolint:revive // adapter types are exported for orchestration integration tests.
package pipeline

import (
	"context"
	"errors"
	"fmt"

	"duck-demo/internal/domain"
)

type AdaptedPipelineAssets struct {
	Assets       []domain.DataAsset
	Dependencies []domain.AssetDependency
}

type AdaptedModelAssets struct {
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

func BuildModelAssetGraph(models []domain.Model) (*AdaptedModelAssets, error) {
	if len(models) == 0 {
		return &AdaptedModelAssets{Assets: []domain.DataAsset{}, Dependencies: []domain.AssetDependency{}}, nil
	}

	modelsByQualifiedName := make(map[string]domain.Model, len(models))
	assets := make([]domain.DataAsset, 0, len(models))
	for _, model := range models {
		if model.ID == "" {
			return nil, domain.ErrValidation("model id is required")
		}

		modelsByQualifiedName[model.QualifiedName()] = model

		owner := model.Owner
		if owner == "" {
			owner = model.CreatedBy
		}

		tags := append([]string{"model", model.ProjectName}, model.Tags...)
		assets = append(assets, domain.DataAsset{
			ID:          model.ID,
			AssetKey:    fmt.Sprintf("model.%s", model.QualifiedName()),
			AssetType:   domain.AssetTypeModel,
			Owner:       owner,
			Description: model.Description,
			Tags:        tags,
			CreatedBy:   model.CreatedBy,
			IsActive:    true,
		})
	}

	deps := make([]domain.AssetDependency, 0)
	seen := make(map[string]struct{})
	for _, model := range models {
		for _, upstreamRef := range model.DependsOn {
			if upstreamRef == "" {
				continue
			}

			upstreamModel, ok := modelsByQualifiedName[upstreamRef]
			if !ok || upstreamModel.ID == model.ID {
				continue
			}

			key := model.ID + "->" + upstreamModel.ID
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}

			deps = append(deps, domain.AssetDependency{
				ID:              domain.NewID(),
				AssetID:         model.ID,
				UpstreamAssetID: upstreamModel.ID,
				DependencyType:  domain.DependencyTypeHard,
			})
		}
	}

	return &AdaptedModelAssets{Assets: assets, Dependencies: deps}, nil
}

func SyncModelsToAssets(
	ctx context.Context,
	modelRepo domain.ModelRepository,
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
) error {
	if modelRepo == nil || assetRepo == nil || assetDepRepo == nil {
		return nil
	}

	models, err := modelRepo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list models: %w", err)
	}

	adapted, err := BuildModelAssetGraph(models)
	if err != nil {
		return fmt.Errorf("build model asset graph: %w", err)
	}

	for i := range adapted.Assets {
		asset := adapted.Assets[i]
		if _, getErr := assetRepo.GetByID(ctx, asset.ID); getErr == nil {
			if _, updateErr := assetRepo.Update(ctx, asset.ID, &asset); updateErr != nil {
				return fmt.Errorf("update asset %s: %w", asset.AssetKey, updateErr)
			}
		} else {
			if _, createErr := assetRepo.Create(ctx, &asset); createErr != nil {
				return fmt.Errorf("create asset %s: %w", asset.AssetKey, createErr)
			}
		}

		if depErr := assetDepRepo.DeleteByAsset(ctx, asset.ID); depErr != nil {
			return fmt.Errorf("clear dependencies for asset %s: %w", asset.AssetKey, depErr)
		}
	}

	for i := range adapted.Dependencies {
		dep := adapted.Dependencies[i]
		if _, depErr := assetDepRepo.Create(ctx, &dep); depErr != nil {
			var conflict *domain.ConflictError
			if !errors.As(depErr, &conflict) {
				return fmt.Errorf("create dependency %s->%s: %w", dep.UpstreamAssetID, dep.AssetID, depErr)
			}
		}
	}

	return nil
}
