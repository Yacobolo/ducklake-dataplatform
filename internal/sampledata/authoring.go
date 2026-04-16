package sampledata

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"

	"github.com/Yacobolo/quackstack/internal/db/repository"
	"github.com/Yacobolo/quackstack/internal/domain"
)

const (
	sampleAuthoringProjectName         = "sample_transforms"
	sampleAuthoringLibraryProjectName  = "sample_transform_lib"
	sampleAuthoringDevEnvironmentName  = "dev"
	sampleAuthoringProdEnvironmentName = "prod"

	sampleRevenueBandMacroName = "utils.revenue_band"
	sampleSafeDivideMacroName  = "metrics.safe_divide"
)

func ensureSampleAuthoringResources(ctx context.Context, controlDB *sql.DB, assetsPath string) error {
	workspaceRepo := repository.NewWorkspaceRepo(controlDB)
	projectRepo := repository.NewProjectRepo(controlDB)
	environmentRepo := repository.NewEnvironmentRepo(controlDB)
	dependencyRepo := repository.NewProjectDependencyRepo(controlDB)
	sourceRepo := repository.NewSourceDefinitionRepo(controlDB)
	seedRepo := repository.NewSeedRepo(controlDB)
	modelRepo := repository.NewModelRepo(controlDB)
	macroRepo := repository.NewMacroRepo(controlDB)

	workspace, err := ensureSampleAuthoringWorkspace(ctx, workspaceRepo)
	if err != nil {
		return err
	}

	libraryProject, err := ensureSampleProject(ctx, projectRepo, sampleProjectSpec{
		workspaceID: workspace.ID,
		name:        sampleAuthoringLibraryProjectName,
		kind:        domain.ProjectKindLibrary,
		description: "Shared library macros and helper assets for the built-in transformation demo project.",
		createdBy:   sampleDashboardOwner,
	})
	if err != nil {
		return err
	}

	project, err := ensureSampleProject(ctx, projectRepo, sampleProjectSpec{
		workspaceID:    workspace.ID,
		name:           sampleAuthoringProjectName,
		kind:           domain.ProjectKindPersonal,
		description:    "Sample transformation project that demonstrates sources, seeds, macros, refs, and environments against the built-in NYC taxi data.",
		ownerPrincipal: strPtr(sampleDashboardOwner),
		createdBy:      sampleDashboardOwner,
	})
	if err != nil {
		return err
	}

	devEnvironment, err := ensureSampleEnvironment(ctx, environmentRepo, project.ID, domain.Environment{
		ProjectID:     project.ID,
		Name:          sampleAuthoringDevEnvironmentName,
		Kind:          domain.EnvironmentKindDevelopment,
		Description:   "Default development target for the built-in sample transformation project.",
		TargetCatalog: "memory",
		TargetSchema:  "analytics",
		Variables: map[string]string{
			"window_days": "30",
		},
		CreatedBy: sampleDashboardOwner,
	})
	if err != nil {
		return err
	}

	if _, err := ensureSampleEnvironment(ctx, environmentRepo, project.ID, domain.Environment{
		ProjectID:          project.ID,
		Name:               sampleAuthoringProdEnvironmentName,
		Kind:               domain.EnvironmentKindProduction,
		Description:        "Production-style environment that inherits from dev and widens the rolling analysis window.",
		TargetSchema:       "analytics_prod",
		DeferToEnvironment: strPtr(sampleAuthoringDevEnvironmentName),
		Variables: map[string]string{
			"window_days": "90",
		},
		CreatedBy: sampleDashboardOwner,
	}); err != nil {
		return err
	}

	if err := ensureSampleWorkspaceDefaults(ctx, workspaceRepo, workspace, project.ID, devEnvironment.ID); err != nil {
		return err
	}

	if err := ensureSampleDependency(ctx, dependencyRepo, project.ID, sampleAuthoringLibraryProjectName); err != nil {
		return err
	}

	for _, source := range []domain.SourceDefinition{
		{
			ProjectName: sampleAuthoringProjectName,
			SourceName:  "raw",
			TableName:   "trips",
			RelationRef: "sample_data.nyc_taxi.trips",
			Description: "Curated NYC taxi trip records exposed through the built-in sample catalog.",
			CreatedBy:   sampleDashboardOwner,
			Freshness:   &domain.SourceFreshnessPolicy{TimestampColumn: "pickup_at", MaxLagSeconds: 86400},
		},
		{
			ProjectName: sampleAuthoringProjectName,
			SourceName:  "raw",
			TableName:   "zones",
			RelationRef: "sample_data.nyc_taxi.zones",
			Description: "Official TLC taxi zone lookup data from the built-in sample catalog.",
			CreatedBy:   sampleDashboardOwner,
		},
	} {
		if err := ensureSampleSourceDefinition(ctx, sourceRepo, source); err != nil {
			return err
		}
	}

	if err := ensureSampleSeed(ctx, seedRepo, domain.Seed{
		ProjectName: sampleAuthoringProjectName,
		Name:        "zone_priority_overrides",
		Description: "Small seed file used by the demo project to show project-owned seed resources joining into marts.",
		InputRef:    filepathToSlash(assetsPath + "/zone_priority_overrides.csv"),
		Format:      "csv",
		Delimiter:   ",",
		HasHeader:   true,
		ColumnTypes: map[string]string{
			"zone":          "VARCHAR",
			"priority_tier": "VARCHAR",
		},
		Tags:      []string{"sample", "seed"},
		CreatedBy: sampleDashboardOwner,
	}); err != nil {
		return err
	}

	for _, macro := range []domain.Macro{
		{
			Name:        sampleRevenueBandMacroName,
			MacroType:   domain.MacroTypeScalar,
			Parameters:  []string{"amount_col"},
			Body:        "return \"case when \" + amount_col + \" >= 75 then 'high' when \" + amount_col + \" >= 35 then 'medium' else 'low' end\"",
			Description: "Buckets fare totals into simple high/medium/low revenue bands for the sample marts.",
			ProjectName: sampleAuthoringLibraryProjectName,
			Visibility:  domain.MacroVisibilityProject,
			Owner:       sampleDashboardOwner,
			Tags:        []string{"sample", "library"},
			Status:      domain.MacroStatusActive,
			CreatedBy:   sampleDashboardOwner,
		},
		{
			Name:        sampleSafeDivideMacroName,
			MacroType:   domain.MacroTypeScalar,
			Parameters:  []string{"numerator", "denominator"},
			Body:        "return \"case when \" + denominator + \" = 0 then 0 else \" + numerator + \" / \" + denominator + \" end\"",
			Description: "Protects aggregate ratios from divide-by-zero errors in the sample mart model.",
			ProjectName: sampleAuthoringProjectName,
			Visibility:  domain.MacroVisibilityProject,
			Owner:       sampleDashboardOwner,
			Tags:        []string{"sample", "project"},
			Status:      domain.MacroStatusActive,
			CreatedBy:   sampleDashboardOwner,
		},
	} {
		if err := ensureSampleMacro(ctx, macroRepo, macro); err != nil {
			return err
		}
	}

	for _, model := range []domain.Model{
		{
			ProjectName:     sampleAuthoringProjectName,
			Name:            "stg_trips",
			Materialization: domain.MaterializationView,
			Description:     "Staging model that trims the trip source to a configurable rolling window and applies library macros.",
			Owner:           sampleDashboardOwner,
			Tags:            []string{"sample", "staging"},
			Config: domain.ModelConfig{
				Materialized: "view",
				Schema:       "analytics",
				Tags:         []string{"sample", "staging"},
			},
			SQL: `{{ config(materialized='view', schema='analytics', tags=['sample', 'staging']) }}
select
  vendor_id,
  cast(pickup_at as date) as pickup_date,
  pickup_location_id,
  dropoff_location_id,
  {{ utils.revenue_band('total_amount') }} as revenue_band,
  round(total_amount, 2) as total_amount,
  round(tip_amount, 2) as tip_amount
from {{ source('raw', 'trips') }}
where pickup_at >= current_timestamp - interval '{{ var('window_days', '30') }} day'`,
			CreatedBy: sampleDashboardOwner,
		},
		{
			ProjectName:     sampleAuthoringProjectName,
			Name:            "dim_zones",
			Materialization: domain.MaterializationView,
			Description:     "Zone dimension sourced from the built-in sample taxi lookup table.",
			Owner:           sampleDashboardOwner,
			Tags:            []string{"sample", "dimension"},
			Config: domain.ModelConfig{
				Materialized: "view",
				Schema:       "analytics",
				Tags:         []string{"sample", "dimension"},
			},
			SQL: `{{ config(materialized='view', schema='analytics', tags=['sample', 'dimension']) }}
select
  location_id,
  borough,
  zone,
  service_zone
from {{ source('raw', 'zones') }}`,
			CreatedBy: sampleDashboardOwner,
		},
		{
			ProjectName:     sampleAuthoringProjectName,
			Name:            "fct_zone_revenue",
			Materialization: domain.MaterializationTable,
			Description:     "Example mart joining source-backed staging models and a seed-backed dimension override.",
			Owner:           sampleDashboardOwner,
			Tags:            []string{"sample", "mart"},
			Config: domain.ModelConfig{
				Materialized: "table",
				Schema:       "analytics",
				Tags:         []string{"sample", "mart"},
			},
			SQL: `{{ config(materialized='table', schema='analytics', tags=['sample', 'mart']) }}
select
  z.borough,
  z.zone,
  t.revenue_band,
  coalesce(p.priority_tier, 'standard') as priority_tier,
  count(*) as trip_count,
  round(sum(t.total_amount), 2) as gross_revenue,
  round({{ metrics.safe_divide('sum(t.total_amount)', 'count(*)') }}, 2) as avg_ticket_amount
from {{ ref('stg_trips') }} t
join {{ ref('dim_zones') }} z
  on z.location_id = t.pickup_location_id
left join {{ ref('zone_priority_overrides') }} p
  on lower(p.zone) = lower(z.zone)
group by 1, 2, 3, 4`,
			CreatedBy: sampleDashboardOwner,
		},
	} {
		if err := ensureSampleModel(ctx, modelRepo, model); err != nil {
			return err
		}
	}

	_ = libraryProject
	return nil
}

type sampleProjectSpec struct {
	workspaceID    string
	name           string
	kind           string
	description    string
	ownerPrincipal *string
	createdBy      string
}

func ensureSampleAuthoringWorkspace(ctx context.Context, repo *repository.WorkspaceRepo) (*domain.Workspace, error) {
	workspace, err := repo.GetPersonalByPrincipal(ctx, sampleDashboardOwner)
	if err != nil {
		var notFoundErr *domain.NotFoundError
		if !errors.As(err, &notFoundErr) {
			return nil, err
		}
		workspace, err = repo.Create(ctx, &domain.Workspace{
			Name:           sampleDashboardOwner + " workspace",
			Kind:           domain.WorkspaceKindPersonal,
			OwnerPrincipal: strPtr(sampleDashboardOwner),
			CreatedBy:      sampleDashboardOwner,
		})
		if err != nil {
			return nil, err
		}
	}

	if _, err := repo.UpsertMember(ctx, &domain.WorkspaceMember{
		WorkspaceID:   workspace.ID,
		PrincipalName: sampleDashboardOwner,
		Role:          domain.FolderShareRoleManager,
	}); err != nil {
		return nil, err
	}
	return workspace, nil
}

func ensureSampleWorkspaceDefaults(ctx context.Context, repo *repository.WorkspaceRepo, workspace *domain.Workspace, projectID, environmentID string) error {
	if workspace == nil {
		return domain.ErrValidation("workspace is required")
	}
	if workspace.DefaultProjectID != nil && *workspace.DefaultProjectID == projectID &&
		workspace.DefaultEnvironmentID != nil && *workspace.DefaultEnvironmentID == environmentID {
		return nil
	}

	updated, err := repo.Update(ctx, workspace.ID, domain.UpdateWorkspaceRequest{
		DefaultProjectID:     strPtr(projectID),
		DefaultEnvironmentID: strPtr(environmentID),
	})
	if err != nil {
		return err
	}
	*workspace = *updated
	return nil
}

func ensureSampleProject(ctx context.Context, repo *repository.ProjectRepo, spec sampleProjectSpec) (*domain.Project, error) {
	project, err := repo.GetByName(ctx, spec.name)
	if err != nil {
		var notFoundErr *domain.NotFoundError
		if !errors.As(err, &notFoundErr) {
			return nil, err
		}
		return repo.Create(ctx, &domain.Project{
			WorkspaceID:    spec.workspaceID,
			Name:           spec.name,
			Kind:           spec.kind,
			Description:    spec.description,
			OwnerPrincipal: spec.ownerPrincipal,
			DefaultBranch:  "main",
			CreatedBy:      spec.createdBy,
		})
	}

	if project.WorkspaceID == spec.workspaceID &&
		project.Kind == spec.kind &&
		project.Description == spec.description &&
		strings.TrimSpace(project.DefaultBranch) == "main" &&
		equalStringPtr(project.OwnerPrincipal, spec.ownerPrincipal) {
		return project, nil
	}

	return repo.Update(ctx, project.ID, domain.UpdateProjectRequest{
		Description:   strPtr(spec.description),
		DefaultBranch: strPtr("main"),
	})
}

func ensureSampleEnvironment(ctx context.Context, repo *repository.EnvironmentRepo, projectID string, desired domain.Environment) (*domain.Environment, error) {
	current, err := repo.GetByName(ctx, projectID, desired.Name)
	if err != nil {
		var notFoundErr *domain.NotFoundError
		if !errors.As(err, &notFoundErr) {
			return nil, err
		}
		return repo.Create(ctx, &domain.Environment{
			ProjectID:          projectID,
			Name:               desired.Name,
			Kind:               desired.Kind,
			Description:        desired.Description,
			TargetCatalog:      desired.TargetCatalog,
			TargetSchema:       desired.TargetSchema,
			ComputeEndpoint:    desired.ComputeEndpoint,
			DeferToEnvironment: desired.DeferToEnvironment,
			Variables:          cloneStringMap(desired.Variables),
			SourceOverrides:    cloneStringMap(desired.SourceOverrides),
			CreatedBy:          desired.CreatedBy,
		})
	}

	if current.Kind == desired.Kind &&
		current.Description == desired.Description &&
		current.TargetCatalog == desired.TargetCatalog &&
		current.TargetSchema == desired.TargetSchema &&
		equalStringPtr(current.ComputeEndpoint, desired.ComputeEndpoint) &&
		equalStringPtr(current.DeferToEnvironment, desired.DeferToEnvironment) &&
		reflect.DeepEqual(current.Variables, desired.Variables) &&
		reflect.DeepEqual(current.SourceOverrides, desired.SourceOverrides) {
		return current, nil
	}

	return repo.Update(ctx, current.ID, domain.UpdateEnvironmentRequest{
		Description:        strPtr(desired.Description),
		TargetCatalog:      strPtr(desired.TargetCatalog),
		TargetSchema:       strPtr(desired.TargetSchema),
		ComputeEndpoint:    desired.ComputeEndpoint,
		DeferToEnvironment: desired.DeferToEnvironment,
		Variables:          ptrToStringMap(cloneStringMap(desired.Variables)),
		SourceOverrides:    ptrToStringMap(cloneStringMap(desired.SourceOverrides)),
	})
}

func ensureSampleDependency(ctx context.Context, repo *repository.ProjectDependencyRepo, projectID, dependencyProject string) error {
	current, err := repo.ListByProject(ctx, projectID)
	if err != nil {
		return err
	}
	for _, item := range current {
		if item.DependencyProject == dependencyProject && item.DependencyKind == "project" && item.Position == 1 {
			return nil
		}
	}
	for _, item := range current {
		if item.DependencyProject == dependencyProject {
			if err := repo.Delete(ctx, projectID, dependencyProject); err != nil {
				return err
			}
			break
		}
	}
	_, err = repo.Create(ctx, &domain.ProjectDependency{
		ProjectID:         projectID,
		DependencyProject: dependencyProject,
		DependencyKind:    "project",
		Position:          1,
		CreatedBy:         sampleDashboardOwner,
	})
	return err
}

func ensureSampleSourceDefinition(ctx context.Context, repo *repository.SourceDefinitionRepo, desired domain.SourceDefinition) error {
	current, err := repo.GetByName(ctx, desired.ProjectName, desired.SourceName, desired.TableName)
	if err != nil {
		var notFoundErr *domain.NotFoundError
		if !errors.As(err, &notFoundErr) {
			return err
		}
		_, err = repo.Create(ctx, &desired)
		return err
	}
	if current.RelationRef == desired.RelationRef &&
		current.Description == desired.Description &&
		reflect.DeepEqual(current.Freshness, desired.Freshness) {
		return nil
	}
	desired.ID = current.ID
	desired.CreatedBy = current.CreatedBy
	_, err = repo.Update(ctx, current.ID, &desired)
	return err
}

func ensureSampleSeed(ctx context.Context, repo *repository.SeedRepo, desired domain.Seed) error {
	current, err := repo.GetByName(ctx, desired.ProjectName, desired.Name)
	if err != nil {
		var notFoundErr *domain.NotFoundError
		if !errors.As(err, &notFoundErr) {
			return err
		}
		_, err = repo.Create(ctx, &desired)
		return err
	}
	if current.Description == desired.Description &&
		current.InputRef == desired.InputRef &&
		current.Format == desired.Format &&
		current.Delimiter == desired.Delimiter &&
		current.HasHeader == desired.HasHeader &&
		reflect.DeepEqual(current.ColumnTypes, desired.ColumnTypes) &&
		reflect.DeepEqual(current.Tags, desired.Tags) {
		return nil
	}
	desired.ID = current.ID
	desired.CreatedBy = current.CreatedBy
	_, err = repo.Update(ctx, current.ID, &desired)
	return err
}

func ensureSampleMacro(ctx context.Context, repo *repository.MacroRepo, desired domain.Macro) error {
	current, err := repo.GetByName(ctx, desired.Name)
	if err != nil {
		var notFoundErr *domain.NotFoundError
		if !errors.As(err, &notFoundErr) {
			return err
		}
		_, err = repo.Create(ctx, &desired)
		return err
	}
	if current.Body == desired.Body &&
		current.Description == desired.Description &&
		current.MacroType == desired.MacroType &&
		reflect.DeepEqual(current.Parameters, desired.Parameters) &&
		current.ProjectName == desired.ProjectName &&
		current.Visibility == desired.Visibility &&
		current.Owner == desired.Owner &&
		reflect.DeepEqual(current.Tags, desired.Tags) &&
		current.Status == desired.Status {
		return nil
	}
	_, err = repo.Update(ctx, desired.Name, domain.UpdateMacroRequest{
		Body:        strPtr(desired.Body),
		Description: strPtr(desired.Description),
		Parameters:  desired.Parameters,
		ProjectName: strPtr(desired.ProjectName),
		Visibility:  strPtr(desired.Visibility),
		Owner:       strPtr(desired.Owner),
		Tags:        desired.Tags,
		Status:      strPtr(desired.Status),
	})
	return err
}

func ensureSampleModel(ctx context.Context, repo *repository.ModelRepo, desired domain.Model) error {
	current, err := repo.GetByName(ctx, desired.ProjectName, desired.Name)
	if err != nil {
		var notFoundErr *domain.NotFoundError
		if !errors.As(err, &notFoundErr) {
			return err
		}
		_, err = repo.Create(ctx, &desired)
		return err
	}
	if current.SQL == desired.SQL &&
		current.Materialization == desired.Materialization &&
		current.Description == desired.Description &&
		reflect.DeepEqual(current.Tags, desired.Tags) &&
		reflect.DeepEqual(current.Config, desired.Config) {
		return nil
	}
	materialization := desired.Materialization
	config := desired.Config
	_, err = repo.Update(ctx, current.ID, domain.UpdateModelRequest{
		SQL:             strPtr(desired.SQL),
		Materialization: &materialization,
		Description:     strPtr(desired.Description),
		Tags:            desired.Tags,
		Config:          &config,
	})
	return err
}

func ptrToStringMap(value map[string]string) *map[string]string {
	out := cloneStringMap(value)
	return &out
}

func cloneStringMap(value map[string]string) map[string]string {
	if len(value) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
}

func equalStringPtr(a, b *string) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

func filepathToSlash(path string) string {
	return strings.ReplaceAll(path, "\\", "/")
}
