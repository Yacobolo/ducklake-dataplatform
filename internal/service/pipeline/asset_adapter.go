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

type AdaptedNotebookAssets struct {
	Assets       []domain.DataAsset
	Dependencies []domain.AssetDependency
}

type AdaptedSemanticAssets struct {
	Assets       []domain.DataAsset
	Dependencies []domain.AssetDependency
}

type AdaptedDashboardAssets struct {
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

func BuildNotebookAssetGraph(notebooks []domain.Notebook) (*AdaptedNotebookAssets, error) {
	if len(notebooks) == 0 {
		return &AdaptedNotebookAssets{Assets: []domain.DataAsset{}, Dependencies: []domain.AssetDependency{}}, nil
	}

	assets := make([]domain.DataAsset, 0, len(notebooks))
	for _, notebook := range notebooks {
		if notebook.ID == "" {
			return nil, domain.ErrValidation("notebook id is required")
		}

		description := ""
		if notebook.Description != nil {
			description = *notebook.Description
		}

		assets = append(assets, domain.DataAsset{
			ID:          notebook.ID,
			AssetKey:    fmt.Sprintf("notebook.%s", notebook.ID),
			AssetType:   domain.AssetTypeNotebook,
			Owner:       notebook.Owner,
			Description: description,
			Tags:        []string{"notebook"},
			IsActive:    true,
		})
	}

	return &AdaptedNotebookAssets{Assets: assets, Dependencies: []domain.AssetDependency{}}, nil
}

func BuildSemanticAssetGraph(
	semanticModels []domain.SemanticModel,
	metricsByModel map[string][]domain.SemanticMetric,
	preAggsByModel map[string][]domain.SemanticPreAggregation,
	models []domain.Model,
) (*AdaptedSemanticAssets, error) {
	if len(semanticModels) == 0 {
		return &AdaptedSemanticAssets{Assets: []domain.DataAsset{}, Dependencies: []domain.AssetDependency{}}, nil
	}

	modelIDsByQualifiedName := make(map[string]string, len(models))
	for _, model := range models {
		modelIDsByQualifiedName[model.QualifiedName()] = model.ID
	}

	assets := make([]domain.DataAsset, 0)
	deps := make([]domain.AssetDependency, 0)
	metricIDsByNaturalKey := make(map[string]string)

	for _, semanticModel := range semanticModels {
		if semanticModel.ID == "" {
			return nil, domain.ErrValidation("semantic model id is required")
		}

		owner := semanticModel.Owner
		if owner == "" {
			owner = semanticModel.CreatedBy
		}

		assets = append(assets, domain.DataAsset{
			ID:          semanticModel.ID,
			AssetKey:    fmt.Sprintf("semantic_model.%s.%s", semanticModel.ProjectName, semanticModel.Name),
			AssetType:   domain.AssetTypeSemanticModel,
			Owner:       owner,
			Description: semanticModel.Description,
			Tags:        []string{"semantic_model", semanticModel.ProjectName},
			CreatedBy:   semanticModel.CreatedBy,
			IsActive:    true,
		})

		if upstreamID, ok := modelIDsByQualifiedName[semanticModel.BaseModelRef]; ok {
			deps = append(deps, domain.AssetDependency{
				ID:              domain.NewID(),
				AssetID:         semanticModel.ID,
				UpstreamAssetID: upstreamID,
				DependencyType:  domain.DependencyTypeHard,
			})
		}

		for _, metric := range metricsByModel[semanticModel.ID] {
			if metric.ID == "" {
				return nil, domain.ErrValidation("semantic metric id is required")
			}
			metricOwner := metric.Owner
			if metricOwner == "" {
				metricOwner = owner
			}
			label := metric.Description
			if label == "" {
				label = metric.Label
			}
			assets = append(assets, domain.DataAsset{
				ID:          metric.ID,
				AssetKey:    fmt.Sprintf("metric.%s.%s.%s", semanticModel.ProjectName, semanticModel.Name, metric.Name),
				AssetType:   domain.AssetTypeMetric,
				Owner:       metricOwner,
				Description: label,
				Tags:        []string{"metric", semanticModel.ProjectName, semanticModel.Name},
				CreatedBy:   metric.CreatedBy,
				IsActive:    true,
			})
			metricIDsByNaturalKey[metricNaturalKey(semanticModel.ProjectName, semanticModel.Name, metric.Name)] = metric.ID
			deps = append(deps, domain.AssetDependency{
				ID:              domain.NewID(),
				AssetID:         metric.ID,
				UpstreamAssetID: semanticModel.ID,
				DependencyType:  domain.DependencyTypeHard,
			})
		}

		for _, preAgg := range preAggsByModel[semanticModel.ID] {
			if preAgg.ID == "" {
				return nil, domain.ErrValidation("semantic pre-aggregation id is required")
			}
			description := preAgg.TargetRelation
			if description != "" {
				description = fmt.Sprintf("target_relation=%s", preAgg.TargetRelation)
			}
			assets = append(assets, domain.DataAsset{
				ID:          preAgg.ID,
				AssetKey:    fmt.Sprintf("semantic_pre_aggregation.%s.%s.%s", semanticModel.ProjectName, semanticModel.Name, preAgg.Name),
				AssetType:   domain.AssetTypeSemanticPreAggregation,
				Owner:       owner,
				Description: description,
				Tags:        []string{"semantic_pre_aggregation", semanticModel.ProjectName, semanticModel.Name},
				CreatedBy:   preAgg.CreatedBy,
				IsActive:    true,
			})
			deps = append(deps, domain.AssetDependency{
				ID:              domain.NewID(),
				AssetID:         preAgg.ID,
				UpstreamAssetID: semanticModel.ID,
				DependencyType:  domain.DependencyTypeHard,
			})
		}
	}

	_ = metricIDsByNaturalKey

	return &AdaptedSemanticAssets{Assets: assets, Dependencies: dedupeDependencies(deps)}, nil
}

func BuildDashboardAssetGraph(
	dashboards []domain.Dashboard,
	widgetsByDashboard map[string][]domain.DashboardWidget,
	notebooks []domain.Notebook,
	semanticModels []domain.SemanticModel,
	metricsByModel map[string][]domain.SemanticMetric,
) (*AdaptedDashboardAssets, error) {
	if len(dashboards) == 0 {
		return &AdaptedDashboardAssets{Assets: []domain.DataAsset{}, Dependencies: []domain.AssetDependency{}}, nil
	}

	notebookIDs := make(map[string]struct{}, len(notebooks))
	for _, notebook := range notebooks {
		notebookIDs[notebook.ID] = struct{}{}
	}

	semanticModelIDsByNaturalKey := make(map[string]string, len(semanticModels))
	for _, semanticModel := range semanticModels {
		semanticModelIDsByNaturalKey[semanticModelNaturalKey(semanticModel.ProjectName, semanticModel.Name)] = semanticModel.ID
	}

	metricIDsByNaturalKey := make(map[string]string)
	for _, semanticModel := range semanticModels {
		for _, metric := range metricsByModel[semanticModel.ID] {
			metricIDsByNaturalKey[metricNaturalKey(semanticModel.ProjectName, semanticModel.Name, metric.Name)] = metric.ID
		}
	}

	assets := make([]domain.DataAsset, 0, len(dashboards))
	deps := make([]domain.AssetDependency, 0)
	for _, dashboard := range dashboards {
		if dashboard.ID == "" {
			return nil, domain.ErrValidation("dashboard id is required")
		}

		assets = append(assets, domain.DataAsset{
			ID:          dashboard.ID,
			AssetKey:    fmt.Sprintf("dashboard.%s", dashboard.ID),
			AssetType:   domain.AssetTypeDashboard,
			Owner:       dashboard.Owner,
			Description: dashboard.Description,
			Tags:        []string{"dashboard"},
			IsActive:    true,
		})

		for _, widget := range widgetsByDashboard[dashboard.ID] {
			switch widget.Source.Kind {
			case domain.DashboardWidgetSourceNotebookCell:
				if widget.Source.NotebookCell == nil {
					continue
				}
				if _, ok := notebookIDs[widget.Source.NotebookCell.NotebookID]; ok {
					deps = append(deps, domain.AssetDependency{
						ID:              domain.NewID(),
						AssetID:         dashboard.ID,
						UpstreamAssetID: widget.Source.NotebookCell.NotebookID,
						DependencyType:  domain.DependencyTypeHard,
					})
				}
			case domain.DashboardWidgetSourceSemanticQuery:
				if widget.Source.SemanticQuery == nil {
					continue
				}
				query := widget.Source.SemanticQuery
				depAdded := false
				for _, metricName := range query.Metrics {
					if upstreamID, ok := metricIDsByNaturalKey[metricNaturalKey(query.ProjectName, query.SemanticModelName, metricName)]; ok {
						deps = append(deps, domain.AssetDependency{
							ID:              domain.NewID(),
							AssetID:         dashboard.ID,
							UpstreamAssetID: upstreamID,
							DependencyType:  domain.DependencyTypeHard,
						})
						depAdded = true
					}
				}
				if depAdded {
					continue
				}
				if upstreamID, ok := semanticModelIDsByNaturalKey[semanticModelNaturalKey(query.ProjectName, query.SemanticModelName)]; ok {
					deps = append(deps, domain.AssetDependency{
						ID:              domain.NewID(),
						AssetID:         dashboard.ID,
						UpstreamAssetID: upstreamID,
						DependencyType:  domain.DependencyTypeHard,
					})
				}
			}
		}
	}

	return &AdaptedDashboardAssets{Assets: assets, Dependencies: dedupeDependencies(deps)}, nil
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

	return syncAdaptedAssets(ctx, adapted.Assets, adapted.Dependencies, assetRepo, assetDepRepo)
}

func SyncNotebooksToAssets(
	ctx context.Context,
	notebookRepo domain.NotebookRepository,
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
) error {
	if notebookRepo == nil || assetRepo == nil || assetDepRepo == nil {
		return nil
	}

	notebooks, err := listAllNotebooks(ctx, notebookRepo)
	if err != nil {
		return fmt.Errorf("list notebooks: %w", err)
	}

	adapted, err := BuildNotebookAssetGraph(notebooks)
	if err != nil {
		return fmt.Errorf("build notebook asset graph: %w", err)
	}

	return syncAdaptedAssets(ctx, adapted.Assets, adapted.Dependencies, assetRepo, assetDepRepo)
}

func SyncSemanticResourcesToAssets(
	ctx context.Context,
	semanticModelRepo domain.SemanticModelRepository,
	semanticMetricRepo domain.SemanticMetricRepository,
	semanticPreAggRepo domain.SemanticPreAggregationRepository,
	modelRepo domain.ModelRepository,
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
) error {
	if semanticModelRepo == nil || semanticMetricRepo == nil || semanticPreAggRepo == nil || modelRepo == nil || assetRepo == nil || assetDepRepo == nil {
		return nil
	}

	semanticModels, err := semanticModelRepo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list semantic models: %w", err)
	}
	models, err := modelRepo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list models: %w", err)
	}
	metricsByModel := make(map[string][]domain.SemanticMetric, len(semanticModels))
	preAggsByModel := make(map[string][]domain.SemanticPreAggregation, len(semanticModels))
	for _, semanticModel := range semanticModels {
		metrics, listErr := semanticMetricRepo.ListByModel(ctx, semanticModel.ID)
		if listErr != nil {
			return fmt.Errorf("list semantic metrics for %s: %w", semanticModel.ID, listErr)
		}
		preAggs, listErr := semanticPreAggRepo.ListByModel(ctx, semanticModel.ID)
		if listErr != nil {
			return fmt.Errorf("list semantic pre-aggregations for %s: %w", semanticModel.ID, listErr)
		}
		metricsByModel[semanticModel.ID] = metrics
		preAggsByModel[semanticModel.ID] = preAggs
	}

	adapted, err := BuildSemanticAssetGraph(semanticModels, metricsByModel, preAggsByModel, models)
	if err != nil {
		return fmt.Errorf("build semantic asset graph: %w", err)
	}

	return syncAdaptedAssets(ctx, adapted.Assets, adapted.Dependencies, assetRepo, assetDepRepo)
}

func SyncDashboardsToAssets(
	ctx context.Context,
	dashboardRepo domain.DashboardRepository,
	widgetRepo domain.DashboardWidgetRepository,
	notebookRepo domain.NotebookRepository,
	semanticModelRepo domain.SemanticModelRepository,
	semanticMetricRepo domain.SemanticMetricRepository,
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
) error {
	if dashboardRepo == nil || widgetRepo == nil || notebookRepo == nil || semanticModelRepo == nil || semanticMetricRepo == nil || assetRepo == nil || assetDepRepo == nil {
		return nil
	}

	dashboards, err := listAllDashboards(ctx, dashboardRepo)
	if err != nil {
		return fmt.Errorf("list dashboards: %w", err)
	}
	notebooks, err := listAllNotebooks(ctx, notebookRepo)
	if err != nil {
		return fmt.Errorf("list notebooks: %w", err)
	}
	semanticModels, err := semanticModelRepo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list semantic models: %w", err)
	}
	metricsByModel := make(map[string][]domain.SemanticMetric, len(semanticModels))
	for _, semanticModel := range semanticModels {
		metrics, listErr := semanticMetricRepo.ListByModel(ctx, semanticModel.ID)
		if listErr != nil {
			return fmt.Errorf("list semantic metrics for %s: %w", semanticModel.ID, listErr)
		}
		metricsByModel[semanticModel.ID] = metrics
	}

	widgetsByDashboard := make(map[string][]domain.DashboardWidget, len(dashboards))
	for _, dashboard := range dashboards {
		widgets, listErr := widgetRepo.ListByDashboard(ctx, dashboard.ID)
		if listErr != nil {
			return fmt.Errorf("list widgets for dashboard %s: %w", dashboard.ID, listErr)
		}
		widgetsByDashboard[dashboard.ID] = widgets
	}

	adapted, err := BuildDashboardAssetGraph(dashboards, widgetsByDashboard, notebooks, semanticModels, metricsByModel)
	if err != nil {
		return fmt.Errorf("build dashboard asset graph: %w", err)
	}

	return syncAdaptedAssets(ctx, adapted.Assets, adapted.Dependencies, assetRepo, assetDepRepo)
}

func syncAdaptedAssets(
	ctx context.Context,
	assets []domain.DataAsset,
	deps []domain.AssetDependency,
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
) error {
	for i := range assets {
		asset := assets[i]
		if _, getErr := assetRepo.GetByID(ctx, asset.ID); getErr == nil {
			if _, updateErr := assetRepo.Update(ctx, asset.ID, &asset); updateErr != nil {
				return fmt.Errorf("update asset %s: %w", asset.AssetKey, updateErr)
			}
		} else {
			var notFoundErr *domain.NotFoundError
			if !errors.As(getErr, &notFoundErr) {
				return fmt.Errorf("get asset %s: %w", asset.AssetKey, getErr)
			}
			if _, createErr := assetRepo.Create(ctx, &asset); createErr != nil {
				return fmt.Errorf("create asset %s: %w", asset.AssetKey, createErr)
			}
		}

		if depErr := assetDepRepo.DeleteByAsset(ctx, asset.ID); depErr != nil {
			return fmt.Errorf("clear dependencies for asset %s: %w", asset.AssetKey, depErr)
		}
	}

	for i := range deps {
		dep := deps[i]
		if _, depErr := assetDepRepo.Create(ctx, &dep); depErr != nil {
			var conflict *domain.ConflictError
			if !errors.As(depErr, &conflict) {
				return fmt.Errorf("create dependency %s->%s: %w", dep.UpstreamAssetID, dep.AssetID, depErr)
			}
		}
	}

	return nil
}

func listAllNotebooks(ctx context.Context, notebookRepo domain.NotebookRepository) ([]domain.Notebook, error) {
	page := domain.PageRequest{MaxResults: domain.MaxMaxResults}
	out := make([]domain.Notebook, 0)
	for {
		items, total, err := notebookRepo.ListNotebooks(ctx, nil, page)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
		nextOffset := page.Offset() + page.Limit()
		if int64(nextOffset) >= total || len(items) == 0 {
			return out, nil
		}
		page.PageToken = domain.EncodePageToken(nextOffset)
	}
}

func listAllDashboards(ctx context.Context, dashboardRepo domain.DashboardRepository) ([]domain.Dashboard, error) {
	page := domain.PageRequest{MaxResults: domain.MaxMaxResults}
	out := make([]domain.Dashboard, 0)
	for {
		items, total, err := dashboardRepo.List(ctx, nil, page)
		if err != nil {
			return nil, err
		}
		out = append(out, items...)
		nextOffset := page.Offset() + page.Limit()
		if int64(nextOffset) >= total || len(items) == 0 {
			return out, nil
		}
		page.PageToken = domain.EncodePageToken(nextOffset)
	}
}

func dedupeDependencies(in []domain.AssetDependency) []domain.AssetDependency {
	out := make([]domain.AssetDependency, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, dep := range in {
		key := dep.AssetID + "->" + dep.UpstreamAssetID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, dep)
	}
	return out
}

func semanticModelNaturalKey(projectName, modelName string) string {
	return projectName + "." + modelName
}

func metricNaturalKey(projectName, semanticModelName, metricName string) string {
	return projectName + "." + semanticModelName + "." + metricName
}
