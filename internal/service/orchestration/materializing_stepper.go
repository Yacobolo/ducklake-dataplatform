//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"fmt"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
)

const defaultAssetExecutionPrincipal = "system"

type notebookSessionRunner interface {
	CreateSession(ctx context.Context, notebookID, principal string) (*domain.NotebookSession, error)
	RunAll(ctx context.Context, sessionID string, principalName ...string) (*domain.RunAllResult, error)
	CloseSession(ctx context.Context, sessionID string, principalName ...string) error
}

type semanticPreAggregationMaterializer interface {
	MaterializePreAggregation(ctx context.Context, principal, preAggregationID string) (*domain.SemanticPreAggregation, map[string]any, error)
}

type MaterializingAssetStepper struct {
	assets    domain.DataAssetRepository
	deps      domain.AssetDependencyRepository
	models    domain.ModelRepository
	runner    domain.ModelRunner
	notebooks domain.NotebookRepository
	sessions  notebookSessionRunner
	semantic  semanticPreAggregationMaterializer
}

func NewMaterializingAssetStepper(
	assets domain.DataAssetRepository,
	deps domain.AssetDependencyRepository,
	models domain.ModelRepository,
	runner domain.ModelRunner,
	notebooks domain.NotebookRepository,
	sessions notebookSessionRunner,
	semantic semanticPreAggregationMaterializer,
) *MaterializingAssetStepper {
	return &MaterializingAssetStepper{
		assets:    assets,
		deps:      deps,
		models:    models,
		runner:    runner,
		notebooks: notebooks,
		sessions:  sessions,
		semantic:  semantic,
	}
}

func (s *MaterializingAssetStepper) Execute(ctx context.Context, assetID string, _ IOManager) (map[string]any, error) {
	asset, err := s.assets.GetByID(ctx, assetID)
	if err != nil {
		return nil, fmt.Errorf("get asset %s: %w", assetID, err)
	}

	switch asset.AssetType {
	case domain.AssetTypeModel:
		return s.executeModel(ctx, asset)
	case domain.AssetTypeNotebookOutput:
		return s.executeNotebookOutput(ctx, asset)
	case domain.AssetTypeSemanticPreAggregation:
		return s.executeSemanticPreAggregation(ctx, asset)
	case domain.AssetTypeDashboard,
		domain.AssetTypeMetric,
		domain.AssetTypeSemanticModel,
		domain.AssetTypeNotebook:
		return skipMaterializationResult(asset), nil
	default:
		return nil, domain.ErrValidation("asset type %s does not support orchestration execution", asset.AssetType)
	}
}

func (s *MaterializingAssetStepper) executeModel(ctx context.Context, asset *domain.DataAsset) (map[string]any, error) {
	if s.models == nil || s.runner == nil {
		return nil, domain.ErrValidation("model execution is not configured")
	}

	model, err := s.models.GetByID(ctx, asset.ID)
	if err != nil {
		return nil, fmt.Errorf("get model for asset %s: %w", asset.AssetKey, err)
	}
	principal := executionPrincipal(asset)
	if err := s.runner.TriggerRunSync(ctx, principal, domain.TriggerModelRunRequest{
		Selector:      model.QualifiedName(),
		TargetCatalog: "memory",
		TargetSchema:  model.ProjectName,
		TriggerType:   domain.ModelTriggerTypeScheduled,
	}); err != nil {
		return nil, fmt.Errorf("trigger model run for %s: %w", model.QualifiedName(), err)
	}

	return map[string]any{
		"asset_type":     domain.AssetTypeModel,
		"model":          model.QualifiedName(),
		"principal":      principal,
		"target_catalog": "memory",
		"target_schema":  model.ProjectName,
		"status":         "success",
	}, nil
}

func (s *MaterializingAssetStepper) executeNotebookOutput(ctx context.Context, asset *domain.DataAsset) (map[string]any, error) {
	if s.deps == nil || s.notebooks == nil || s.sessions == nil {
		return nil, domain.ErrValidation("notebook output execution is not configured")
	}

	upstream, err := s.deps.ListUpstream(ctx, asset.ID)
	if err != nil {
		return nil, fmt.Errorf("list upstream notebook dependencies: %w", err)
	}

	var notebookID string
	for _, dep := range upstream {
		if dep.DependencyType != domain.DependencyTypeHard {
			continue
		}
		notebook, notebookErr := s.notebooks.GetNotebook(ctx, dep.UpstreamAssetID)
		if notebookErr == nil && notebook != nil {
			notebookID = notebook.ID
			break
		}
	}
	if notebookID == "" {
		return nil, domain.ErrValidation("notebook output asset %s is missing an upstream notebook dependency", asset.AssetKey)
	}

	principal := executionPrincipal(asset)
	session, err := s.sessions.CreateSession(ctx, notebookID, principal)
	if err != nil {
		return nil, fmt.Errorf("create notebook session for %s: %w", notebookID, err)
	}
	defer func() {
		_ = s.sessions.CloseSession(context.Background(), session.ID, principal)
	}()

	result, err := s.sessions.RunAll(ctx, session.ID, principal)
	if err != nil {
		return nil, fmt.Errorf("run notebook %s: %w", notebookID, err)
	}
	if err := firstNotebookRunError(result); err != nil {
		return nil, err
	}

	return map[string]any{
		"asset_type":   domain.AssetTypeNotebookOutput,
		"notebook_id":  notebookID,
		"output_cell":  asset.ID,
		"principal":    principal,
		"result_count": len(result.Results),
		"status":       "success",
	}, nil
}

func (s *MaterializingAssetStepper) executeSemanticPreAggregation(ctx context.Context, asset *domain.DataAsset) (map[string]any, error) {
	if s.semantic == nil {
		return nil, domain.ErrValidation("semantic pre-aggregation execution is not configured")
	}
	principal := executionPrincipal(asset)
	preAgg, metadata, err := s.semantic.MaterializePreAggregation(ctx, principal, asset.ID)
	if err != nil {
		return nil, err
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadata["asset_type"] = domain.AssetTypeSemanticPreAggregation
	metadata["principal"] = principal
	metadata["pre_aggregation_id"] = preAgg.ID
	metadata["status"] = "success"
	return metadata, nil
}

func executionPrincipal(asset *domain.DataAsset) string {
	if asset == nil {
		return defaultAssetExecutionPrincipal
	}
	for _, candidate := range []string{asset.Owner, asset.CreatedBy} {
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return defaultAssetExecutionPrincipal
}

func skipMaterializationResult(asset *domain.DataAsset) map[string]any {
	result := map[string]any{
		assetExecutionSkipMaterializationKey: true,
		"status":                             "skipped",
	}
	if asset != nil {
		result["asset_type"] = asset.AssetType
		result["asset_key"] = asset.AssetKey
	}
	return result
}

func firstNotebookRunError(result *domain.RunAllResult) error {
	if result == nil {
		return nil
	}
	for _, cell := range result.Results {
		if cell.Error != nil && strings.TrimSpace(*cell.Error) != "" {
			return domain.ErrValidation("notebook execution failed for cell %s: %s", cell.CellID, *cell.Error)
		}
	}
	return nil
}
