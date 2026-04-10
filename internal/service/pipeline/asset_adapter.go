//nolint:revive // adapter types are exported for orchestration integration tests.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/sqlrewrite"
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

type AdaptedNotebookOutputAssets struct {
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

func BuildModelAssetGraph(models []domain.Model, linksByModelID map[string]domain.NotebookModelLink) (*AdaptedModelAssets, error) {
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

		if link, ok := linksByModelID[model.ID]; ok && strings.TrimSpace(link.OutputCellID) != "" {
			key := model.ID + "->" + link.OutputCellID
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				deps = append(deps, domain.AssetDependency{
					ID:              domain.NewID(),
					AssetID:         model.ID,
					UpstreamAssetID: link.OutputCellID,
					DependencyType:  domain.DependencyTypeHard,
				})
			}
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

func BuildNotebookOutputAssetGraph(notebooks []domain.Notebook, links []domain.NotebookModelLink) (*AdaptedNotebookOutputAssets, error) {
	if len(links) == 0 {
		return &AdaptedNotebookOutputAssets{Assets: []domain.DataAsset{}, Dependencies: []domain.AssetDependency{}}, nil
	}

	notebooksByID := make(map[string]domain.Notebook, len(notebooks))
	for _, notebook := range notebooks {
		notebooksByID[notebook.ID] = notebook
	}

	assets := make([]domain.DataAsset, 0, len(links))
	deps := make([]domain.AssetDependency, 0, len(links))
	for _, link := range links {
		if strings.TrimSpace(link.NotebookID) == "" {
			return nil, domain.ErrValidation("notebook link notebook_id is required")
		}
		if strings.TrimSpace(link.OutputCellID) == "" {
			return nil, domain.ErrValidation("notebook link output_cell_id is required")
		}

		notebook, ok := notebooksByID[link.NotebookID]
		if !ok {
			return nil, domain.ErrValidation("notebook output link references unknown notebook %q", link.NotebookID)
		}

		assets = append(assets, domain.DataAsset{
			ID:          link.OutputCellID,
			AssetKey:    notebookOutputAssetKey(link.NotebookID, link.OutputCellID),
			AssetType:   domain.AssetTypeNotebookOutput,
			Owner:       notebook.Owner,
			Description: fmt.Sprintf("Published output for notebook %s", notebook.Name),
			Tags:        []string{"notebook_output", "notebook"},
			IsActive:    true,
		})
		deps = append(deps, domain.AssetDependency{
			ID:              domain.NewID(),
			AssetID:         link.OutputCellID,
			UpstreamAssetID: link.NotebookID,
			DependencyType:  domain.DependencyTypeHard,
		})
	}

	return &AdaptedNotebookOutputAssets{Assets: assets, Dependencies: dedupeDependencies(deps)}, nil
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
			AssetKey:    fmt.Sprintf("semantic_model.%s", semanticModel.Name),
			AssetType:   domain.AssetTypeSemanticModel,
			Owner:       owner,
			Description: semanticModel.Description,
			Tags:        []string{"semantic_model", semanticModel.Name},
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
				AssetKey:    fmt.Sprintf("metric.%s.%s", semanticModel.Name, metric.Name),
				AssetType:   domain.AssetTypeMetric,
				Owner:       metricOwner,
				Description: label,
				Tags:        []string{"metric", semanticModel.Name},
				CreatedBy:   metric.CreatedBy,
				IsActive:    true,
			})
			metricIDsByNaturalKey[metricNaturalKey(semanticModel.Name, metric.Name)] = metric.ID
			deps = append(deps, domain.AssetDependency{
				ID:              domain.NewID(),
				AssetID:         metric.ID,
				UpstreamAssetID: semanticModel.ID,
				DependencyType:  domain.DependencyTypeHard,
			})
			for _, preAgg := range preAggsByModel[semanticModel.ID] {
				if !preAggregationSupportsMetric(preAgg, metric.Name) {
					continue
				}
				deps = append(deps, domain.AssetDependency{
					ID:              domain.NewID(),
					AssetID:         metric.ID,
					UpstreamAssetID: preAgg.ID,
					DependencyType:  domain.DependencyTypeSoft,
				})
			}
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
				AssetKey:    fmt.Sprintf("semantic_pre_aggregation.%s.%s", semanticModel.Name, preAgg.Name),
				AssetType:   domain.AssetTypeSemanticPreAggregation,
				Owner:       owner,
				Description: description,
				Tags:        []string{"semantic_pre_aggregation", semanticModel.Name},
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
	models []domain.Model,
	notebooks []domain.Notebook,
	linksByNotebookID map[string]domain.NotebookModelLink,
	semanticModels []domain.SemanticModel,
	metricsByModel map[string][]domain.SemanticMetric,
	preAggsByModel map[string][]domain.SemanticPreAggregation,
) (*AdaptedDashboardAssets, error) {
	if len(dashboards) == 0 {
		return &AdaptedDashboardAssets{Assets: []domain.DataAsset{}, Dependencies: []domain.AssetDependency{}}, nil
	}

	notebookIDs := make(map[string]struct{}, len(notebooks))
	for _, notebook := range notebooks {
		notebookIDs[notebook.ID] = struct{}{}
	}

	semanticModelsByID := make(map[string]domain.SemanticModel, len(semanticModels))
	for _, semanticModel := range semanticModels {
		semanticModelsByID[semanticModel.ID] = semanticModel
	}
	modelIDsByQualifiedName := make(map[string]string, len(models))
	for _, model := range models {
		modelIDsByQualifiedName[model.QualifiedName()] = model.ID
	}

	metricIDsByNaturalKey := make(map[string]string)
	for _, semanticModel := range semanticModels {
		for _, metric := range metricsByModel[semanticModel.ID] {
			metricIDsByNaturalKey[metricNaturalKey(semanticModel.Name, metric.Name)] = metric.ID
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
			case domain.DashboardWidgetSourceSQLQuery:
				if widget.Source.SQLQuery == nil {
					continue
				}
				for _, upstreamID := range matchingModelAssetIDsForSQLWidget(widget.Source.SQLQuery, modelIDsByQualifiedName) {
					deps = append(deps, domain.AssetDependency{
						ID:              domain.NewID(),
						AssetID:         dashboard.ID,
						UpstreamAssetID: upstreamID,
						DependencyType:  domain.DependencyTypeHard,
					})
				}
			case domain.DashboardWidgetSourceNotebookCell:
				if widget.Source.NotebookCell == nil {
					continue
				}
				if link, ok := linksByNotebookID[widget.Source.NotebookCell.NotebookID]; ok && link.OutputCellID == widget.Source.NotebookCell.CellID {
					deps = append(deps, domain.AssetDependency{
						ID:              domain.NewID(),
						AssetID:         dashboard.ID,
						UpstreamAssetID: link.OutputCellID,
						DependencyType:  domain.DependencyTypeHard,
					})
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
				if preAggID := matchingPreAggregationAssetID(preAggsByModel[query.SemanticModelID], query); preAggID != "" {
						deps = append(deps, domain.AssetDependency{
							ID:              domain.NewID(),
							AssetID:         dashboard.ID,
							UpstreamAssetID: preAggID,
							DependencyType:  domain.DependencyTypeHard,
						})
						continue
				}
				depAdded := false
				for _, metricName := range query.Metrics {
					if model, hasModel := semanticModelsByID[query.SemanticModelID]; hasModel {
						if upstreamID, ok := metricIDsByNaturalKey[metricNaturalKey(model.Name, metricName)]; ok {
							deps = append(deps, domain.AssetDependency{
								ID:              domain.NewID(),
								AssetID:         dashboard.ID,
								UpstreamAssetID: upstreamID,
								DependencyType:  domain.DependencyTypeHard,
							})
							depAdded = true
						}
					}
				}
				if depAdded {
					continue
				}
				if _, ok := semanticModelsByID[query.SemanticModelID]; ok {
					deps = append(deps, domain.AssetDependency{
						ID:              domain.NewID(),
						AssetID:         dashboard.ID,
						UpstreamAssetID: query.SemanticModelID,
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
	notebookLinkRepo domain.NotebookModelLinkRepository,
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
	productID string,
) error {
	if modelRepo == nil || assetRepo == nil || assetDepRepo == nil {
		return nil
	}

	models, err := modelRepo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list models: %w", err)
	}

	linksByModelID := make(map[string]domain.NotebookModelLink)
	if notebookLinkRepo != nil {
		for _, model := range models {
			link, linkErr := notebookLinkRepo.GetByModelID(ctx, model.ID)
			if linkErr != nil {
				var notFoundErr *domain.NotFoundError
				if errors.As(linkErr, &notFoundErr) {
					continue
				}
				return fmt.Errorf("get notebook output link for model %s: %w", model.ID, linkErr)
			}
			linksByModelID[model.ID] = *link
		}
	}

	adapted, err := BuildModelAssetGraph(models, linksByModelID)
	if err != nil {
		return fmt.Errorf("build model asset graph: %w", err)
	}

	return syncAdaptedAssets(ctx, adapted.Assets, adapted.Dependencies, assetRepo, assetDepRepo, productID)
}

func SyncNotebookOutputsToAssets(
	ctx context.Context,
	notebookRepo domain.NotebookRepository,
	notebookLinkRepo domain.NotebookModelLinkRepository,
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
	productID string,
) error {
	if notebookRepo == nil || notebookLinkRepo == nil || assetRepo == nil || assetDepRepo == nil {
		return nil
	}

	notebooks, err := listAllNotebooks(ctx, notebookRepo)
	if err != nil {
		return fmt.Errorf("list notebooks: %w", err)
	}

	links := make([]domain.NotebookModelLink, 0)
	for _, notebook := range notebooks {
		link, linkErr := notebookLinkRepo.GetByNotebookID(ctx, notebook.ID)
		if linkErr != nil {
			var notFoundErr *domain.NotFoundError
			if errors.As(linkErr, &notFoundErr) {
				continue
			}
			return fmt.Errorf("get notebook output link for notebook %s: %w", notebook.ID, linkErr)
		}
		links = append(links, *link)
	}

	adapted, err := BuildNotebookOutputAssetGraph(notebooks, links)
	if err != nil {
		return fmt.Errorf("build notebook output asset graph: %w", err)
	}

	return syncAdaptedAssets(ctx, adapted.Assets, adapted.Dependencies, assetRepo, assetDepRepo, productID)
}

func SyncNotebooksToAssets(
	ctx context.Context,
	notebookRepo domain.NotebookRepository,
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
	productID string,
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

	return syncAdaptedAssets(ctx, adapted.Assets, adapted.Dependencies, assetRepo, assetDepRepo, productID)
}

func SyncSemanticResourcesToAssets(
	ctx context.Context,
	semanticModelRepo domain.SemanticModelRepository,
	semanticMetricRepo domain.SemanticMetricRepository,
	semanticPreAggRepo domain.SemanticPreAggregationRepository,
	modelRepo domain.ModelRepository,
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
	productID string,
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

	return syncAdaptedAssets(ctx, adapted.Assets, adapted.Dependencies, assetRepo, assetDepRepo, productID)
}

func SyncDashboardsToAssets(
	ctx context.Context,
	dashboardRepo domain.DashboardRepository,
	widgetRepo domain.DashboardWidgetRepository,
	modelRepo domain.ModelRepository,
	notebookRepo domain.NotebookRepository,
	notebookLinkRepo domain.NotebookModelLinkRepository,
	semanticModelRepo domain.SemanticModelRepository,
	semanticMetricRepo domain.SemanticMetricRepository,
	semanticPreAggRepo domain.SemanticPreAggregationRepository,
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
	productID string,
) error {
	if dashboardRepo == nil || widgetRepo == nil || notebookRepo == nil || semanticModelRepo == nil || semanticMetricRepo == nil || semanticPreAggRepo == nil || assetRepo == nil || assetDepRepo == nil {
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
	models, err := modelRepo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list models: %w", err)
	}
	linksByNotebookID := make(map[string]domain.NotebookModelLink)
	if notebookLinkRepo != nil {
		for _, notebook := range notebooks {
			link, linkErr := notebookLinkRepo.GetByNotebookID(ctx, notebook.ID)
			if linkErr != nil {
				var notFoundErr *domain.NotFoundError
				if errors.As(linkErr, &notFoundErr) {
					continue
				}
				return fmt.Errorf("get notebook output link for notebook %s: %w", notebook.ID, linkErr)
			}
			linksByNotebookID[notebook.ID] = *link
		}
	}
	semanticModels, err := semanticModelRepo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("list semantic models: %w", err)
	}
	metricsByModel := make(map[string][]domain.SemanticMetric, len(semanticModels))
	preAggsByModel := make(map[string][]domain.SemanticPreAggregation, len(semanticModels))
	for _, semanticModel := range semanticModels {
		metrics, listErr := semanticMetricRepo.ListByModel(ctx, semanticModel.ID)
		if listErr != nil {
			return fmt.Errorf("list semantic metrics for %s: %w", semanticModel.ID, listErr)
		}
		metricsByModel[semanticModel.ID] = metrics
		preAggs, listErr := semanticPreAggRepo.ListByModel(ctx, semanticModel.ID)
		if listErr != nil {
			return fmt.Errorf("list semantic pre-aggregations for %s: %w", semanticModel.ID, listErr)
		}
		preAggsByModel[semanticModel.ID] = preAggs
	}

	widgetsByDashboard := make(map[string][]domain.DashboardWidget, len(dashboards))
	for _, dashboard := range dashboards {
		widgets, listErr := widgetRepo.ListByDashboard(ctx, dashboard.ID)
		if listErr != nil {
			return fmt.Errorf("list widgets for dashboard %s: %w", dashboard.ID, listErr)
		}
		widgetsByDashboard[dashboard.ID] = widgets
	}

	adapted, err := BuildDashboardAssetGraph(dashboards, widgetsByDashboard, models, notebooks, linksByNotebookID, semanticModels, metricsByModel, preAggsByModel)
	if err != nil {
		return fmt.Errorf("build dashboard asset graph: %w", err)
	}

	return syncAdaptedAssets(ctx, adapted.Assets, adapted.Dependencies, assetRepo, assetDepRepo, productID)
}

func syncAdaptedAssets(
	ctx context.Context,
	assets []domain.DataAsset,
	deps []domain.AssetDependency,
	assetRepo domain.DataAssetRepository,
	assetDepRepo domain.AssetDependencyRepository,
	productID string,
) error {
	for i := range assets {
		asset := assets[i]
		if strings.TrimSpace(asset.ProductID) == "" {
			asset.ProductID = strings.TrimSpace(productID)
		}
		if existing, getErr := assetRepo.GetByID(ctx, asset.ID); getErr == nil {
			merged := mergeAdaptedAsset(existing, asset)
			if _, updateErr := assetRepo.Update(ctx, asset.ID, &merged); updateErr != nil {
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

func mergeAdaptedAsset(existing *domain.DataAsset, adapted domain.DataAsset) domain.DataAsset {
	if existing == nil {
		return adapted
	}

	// Preserve operator-managed asset policies and execution metadata while
	// refreshing graph-owned identity and dependency-facing fields.
	adapted.SchemaJSON = cloneJSONMapOrFallback(adapted.SchemaJSON, existing.SchemaJSON)
	if adapted.PartitionDefinition == nil {
		adapted.PartitionDefinition = clonePartitionDefinition(existing.PartitionDefinition)
	}
	if strings.TrimSpace(adapted.IOProfile) == "" {
		adapted.IOProfile = existing.IOProfile
	}
	adapted.FreshnessPolicy = cloneFreshnessPolicy(existing.FreshnessPolicy)
	adapted.MaterializationPolicy = cloneMaterializationPolicy(existing.MaterializationPolicy)
	adapted.AutoMaterializePolicy = cloneAutoMaterializePolicy(existing.AutoMaterializePolicy)
	adapted.CreatedAt = existing.CreatedAt
	adapted.UpdatedAt = existing.UpdatedAt
	if strings.TrimSpace(adapted.CreatedBy) == "" {
		adapted.CreatedBy = existing.CreatedBy
	}
	if strings.TrimSpace(adapted.ProductID) == "" {
		adapted.ProductID = existing.ProductID
	}
	return adapted
}

func cloneJSONMapOrFallback(value, fallback map[string]any) map[string]any {
	if len(value) > 0 {
		return cloneJSONMap(value)
	}
	if len(fallback) > 0 {
		return cloneJSONMap(fallback)
	}
	return value
}

func cloneJSONMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func clonePartitionDefinition(in *domain.PartitionDefinition) *domain.PartitionDefinition {
	if in == nil {
		return nil
	}
	out := &domain.PartitionDefinition{
		Type:       in.Type,
		Timezone:   in.Timezone,
		StaticKeys: append([]string(nil), in.StaticKeys...),
	}
	if in.DynamicGroup != nil {
		v := *in.DynamicGroup
		out.DynamicGroup = &v
	}
	return out
}

func cloneFreshnessPolicy(in *domain.AssetFreshnessPolicy) *domain.AssetFreshnessPolicy {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneMaterializationPolicy(in *domain.AssetMaterializationPolicy) *domain.AssetMaterializationPolicy {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneAutoMaterializePolicy(in *domain.AssetAutoMaterializePolicy) *domain.AssetAutoMaterializePolicy {
	if in == nil {
		return nil
	}
	out := *in
	out.DowntimeWindowsCronExpr = append([]string(nil), in.DowntimeWindowsCronExpr...)
	return &out
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

func metricNaturalKey(semanticModelName, metricName string) string {
	return semanticModelName + "." + metricName
}

func notebookOutputAssetKey(notebookID, outputCellID string) string {
	return "notebook_output." + notebookID + "." + outputCellID
}

func matchingModelAssetIDsForSQLWidget(source *domain.DashboardSQLQuerySource, modelIDsByQualifiedName map[string]string) []string {
	if source == nil || strings.TrimSpace(source.SQL) == "" || len(modelIDsByQualifiedName) == 0 {
		return nil
	}

	refs, err := sqlrewrite.ExtractTableRefs(source.SQL)
	if err != nil {
		return nil
	}

	defaultSchema := ""
	if source.Schema != nil {
		defaultSchema = strings.TrimSpace(*source.Schema)
	}

	seen := make(map[string]struct{})
	matches := make([]string, 0)
	for _, ref := range refs {
		schemaName := strings.TrimSpace(ref.Schema)
		if schemaName == "" {
			schemaName = defaultSchema
		}
		if schemaName == "" || strings.TrimSpace(ref.Name) == "" {
			continue
		}
		if modelID, ok := modelIDsByQualifiedName[schemaName+"."+strings.TrimSpace(ref.Name)]; ok {
			if _, exists := seen[modelID]; exists {
				continue
			}
			seen[modelID] = struct{}{}
			matches = append(matches, modelID)
		}
	}
	sort.Strings(matches)
	return matches
}

func preAggregationSupportsMetric(preAgg domain.SemanticPreAggregation, metricName string) bool {
	metricName = strings.TrimSpace(metricName)
	if metricName == "" {
		return false
	}
	for _, candidate := range preAgg.MetricSet {
		if strings.TrimSpace(candidate) == metricName {
			return true
		}
	}
	return false
}

func matchingPreAggregationAssetID(preAggs []domain.SemanticPreAggregation, query *domain.DashboardSemanticQuerySource) string {
	if query == nil {
		return ""
	}

	wantMetrics := append([]string(nil), query.Metrics...)
	wantDims := append([]string(nil), query.Dimensions...)
	sort.Strings(wantMetrics)
	sort.Strings(wantDims)

	for _, preAgg := range preAggs {
		mset := append([]string(nil), preAgg.MetricSet...)
		dset := append([]string(nil), preAgg.DimensionSet...)
		sort.Strings(mset)
		sort.Strings(dset)
		if strings.Join(mset, "|") != strings.Join(wantMetrics, "|") {
			continue
		}
		if strings.Join(dset, "|") != strings.Join(wantDims, "|") {
			continue
		}
		return preAgg.ID
	}

	return ""
}
