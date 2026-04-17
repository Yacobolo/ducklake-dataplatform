package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
)

var _ domain.ProjectRepository = (*ProjectRepo)(nil)
var _ domain.EnvironmentRepository = (*EnvironmentRepo)(nil)
var _ domain.BuildRepository = (*BuildRepo)(nil)
var _ domain.CompilationRepository = (*CompilationRepo)(nil)
var _ domain.ProjectReleaseRepository = (*ProjectReleaseRepo)(nil)

// ProjectRepo persists internal authoring projects in SQLite.
type ProjectRepo struct {
	db *sql.DB
}

// NewProjectRepo constructs a project repository backed by SQLite.
func NewProjectRepo(db *sql.DB) *ProjectRepo {
	return &ProjectRepo{db: db}
}

// Create inserts a new project.
func (r *ProjectRepo) Create(ctx context.Context, p *domain.Project) (*domain.Project, error) {
	now := time.Now().UTC()
	id := newID()
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO projects (
			id, workspace_id, name, kind, description, owner_team_id, owner_principal, product_id,
			default_branch, created_by, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, p.WorkspaceID, p.Name, p.Kind, p.Description, nullableStringValue(p.OwnerTeamID), nullableStringValue(p.OwnerPrincipal),
		nullableStringValue(p.ProductID), p.DefaultBranch, p.CreatedBy, now.Format(time.RFC3339), now.Format(time.RFC3339),
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// GetByID returns a project by ID.
func (r *ProjectRepo) GetByID(ctx context.Context, id string) (*domain.Project, error) {
	row := r.db.QueryRowContext(ctx, projectSelectSQL+` WHERE p.id = ?`, id)
	return scanProject(row)
}

// GetByName returns a project by name.
func (r *ProjectRepo) GetByName(ctx context.Context, name string) (*domain.Project, error) {
	row := r.db.QueryRowContext(ctx, projectSelectSQL+` WHERE p.name = ?`, name)
	return scanProject(row)
}

// List returns all projects ordered by name.
func (r *ProjectRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.Project, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM projects`).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, projectSelectSQL+`
		ORDER BY p.name
		LIMIT ? OFFSET ?`, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()
	items := make([]domain.Project, 0)
	for rows.Next() {
		item, err := scanProject(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// ListByWorkspace returns projects within a workspace ordered by name.
func (r *ProjectRepo) ListByWorkspace(ctx context.Context, workspaceID string, page domain.PageRequest) ([]domain.Project, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM projects WHERE workspace_id = ?`, workspaceID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, projectSelectSQL+`
		WHERE p.workspace_id = ?
		ORDER BY p.name
		LIMIT ? OFFSET ?`, workspaceID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()
	items := make([]domain.Project, 0)
	for rows.Next() {
		item, scanErr := scanProject(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// ListByProduct returns projects attached to a product.
func (r *ProjectRepo) ListByProduct(ctx context.Context, productID string, page domain.PageRequest) ([]domain.Project, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM projects WHERE product_id = ?`, productID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, projectSelectSQL+`
		WHERE p.product_id = ?
		ORDER BY p.name
		LIMIT ? OFFSET ?`, productID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()
	items := make([]domain.Project, 0)
	for rows.Next() {
		item, err := scanProject(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// Update applies mutable project fields and returns the updated row.
func (r *ProjectRepo) Update(ctx context.Context, id string, req domain.UpdateProjectRequest) (*domain.Project, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	description := current.Description
	if req.Description != nil {
		description = *req.Description
	}
	defaultBranch := current.DefaultBranch
	if req.DefaultBranch != nil {
		defaultBranch = *req.DefaultBranch
	}
	productID := current.ProductID
	if req.ProductID != nil {
		productID = req.ProductID
	}
	_, err = r.db.ExecContext(ctx, `
		UPDATE projects
		SET description = ?, default_branch = ?, product_id = ?, updated_at = ?
		WHERE id = ?`,
		description, defaultBranch, nullableStringValue(productID), time.Now().UTC().Format(time.RFC3339), id,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// Delete removes a project by ID.
func (r *ProjectRepo) Delete(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM projects WHERE id = ?`, id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("project %q not found", id)
	}
	return nil
}

const projectSelectSQL = `
	SELECT
		p.id, p.workspace_id, p.name, p.kind, p.description, p.owner_team_id, p.owner_principal, p.product_id,
		p.default_branch, p.created_by, p.created_at, p.updated_at
	FROM projects p`

// EnvironmentRepo persists internal execution environments in SQLite.
type EnvironmentRepo struct {
	db *sql.DB
}

// NewEnvironmentRepo constructs an environment repository backed by SQLite.
func NewEnvironmentRepo(db *sql.DB) *EnvironmentRepo {
	return &EnvironmentRepo{db: db}
}

// Create inserts a new environment.
func (r *EnvironmentRepo) Create(ctx context.Context, e *domain.Environment) (*domain.Environment, error) {
	now := time.Now().UTC()
	id := newID()
	variablesJSON, err := marshalStringMap(e.Variables)
	if err != nil {
		return nil, err
	}
	sourceOverridesJSON, err := marshalStringMap(e.SourceOverrides)
	if err != nil {
		return nil, err
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO environments (
			id, project_id, name, kind, description, target_catalog, target_schema,
			compute_endpoint, defer_to_environment, variables_json, source_overrides_json,
			created_by, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, e.ProjectID, e.Name, e.Kind, e.Description, e.TargetCatalog, e.TargetSchema,
		nullableStringValue(e.ComputeEndpoint), nullableStringValue(e.DeferToEnvironment),
		variablesJSON, sourceOverridesJSON, e.CreatedBy, now.Format(time.RFC3339), now.Format(time.RFC3339),
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// GetByID returns an environment by ID.
func (r *EnvironmentRepo) GetByID(ctx context.Context, id string) (*domain.Environment, error) {
	row := r.db.QueryRowContext(ctx, environmentSelectSQL+` WHERE e.id = ?`, id)
	return scanEnvironment(row)
}

// GetByName returns an environment by project/name.
func (r *EnvironmentRepo) GetByName(ctx context.Context, projectID, name string) (*domain.Environment, error) {
	row := r.db.QueryRowContext(ctx, environmentSelectSQL+` WHERE e.project_id = ? AND e.name = ?`, projectID, name)
	return scanEnvironment(row)
}

// ListByProject returns environments for a project.
func (r *EnvironmentRepo) ListByProject(ctx context.Context, projectID string, page domain.PageRequest) ([]domain.Environment, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM environments WHERE project_id = ?`, projectID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, environmentSelectSQL+`
		WHERE e.project_id = ?
		ORDER BY e.name
		LIMIT ? OFFSET ?`, projectID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()
	items := make([]domain.Environment, 0)
	for rows.Next() {
		item, err := scanEnvironment(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// Update applies mutable environment fields and returns the updated row.
func (r *EnvironmentRepo) Update(ctx context.Context, id string, req domain.UpdateEnvironmentRequest) (*domain.Environment, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	description := current.Description
	if req.Description != nil {
		description = *req.Description
	}
	targetCatalog := current.TargetCatalog
	if req.TargetCatalog != nil {
		targetCatalog = *req.TargetCatalog
	}
	targetSchema := current.TargetSchema
	if req.TargetSchema != nil {
		targetSchema = *req.TargetSchema
	}
	computeEndpoint := current.ComputeEndpoint
	if req.ComputeEndpoint != nil {
		computeEndpoint = req.ComputeEndpoint
	}
	deferToEnvironment := current.DeferToEnvironment
	if req.DeferToEnvironment != nil {
		deferToEnvironment = req.DeferToEnvironment
	}
	variables := current.Variables
	if req.Variables != nil {
		variables = *req.Variables
	}
	sourceOverrides := current.SourceOverrides
	if req.SourceOverrides != nil {
		sourceOverrides = *req.SourceOverrides
	}
	variablesJSON, err := marshalStringMap(variables)
	if err != nil {
		return nil, err
	}
	sourceOverridesJSON, err := marshalStringMap(sourceOverrides)
	if err != nil {
		return nil, err
	}
	_, err = r.db.ExecContext(ctx, `
		UPDATE environments
		SET description = ?, target_catalog = ?, target_schema = ?, compute_endpoint = ?,
		    defer_to_environment = ?, variables_json = ?, source_overrides_json = ?, updated_at = ?
		WHERE id = ?`,
		description, targetCatalog, targetSchema, nullableStringValue(computeEndpoint),
		nullableStringValue(deferToEnvironment), variablesJSON, sourceOverridesJSON, time.Now().UTC().Format(time.RFC3339), id,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// Delete removes an environment by ID.
func (r *EnvironmentRepo) Delete(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM environments WHERE id = ?`, id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("environment %q not found", id)
	}
	return nil
}

const environmentSelectSQL = `
	SELECT
		e.id, e.project_id, p.name, e.name, e.kind, e.description, e.target_catalog, e.target_schema,
		e.compute_endpoint, e.defer_to_environment, e.variables_json, e.source_overrides_json,
		e.created_by, e.created_at, e.updated_at
	FROM environments e
	JOIN projects p ON p.id = e.project_id`

type projectRowScanner interface {
	Scan(dest ...any) error
}

func scanProject(scanner projectRowScanner) (*domain.Project, error) {
	var item domain.Project
	var ownerTeamID sql.NullString
	var ownerPrincipal sql.NullString
	var productID sql.NullString
	var createdAt time.Time
	var updatedAt time.Time
	if err := scanner.Scan(
		&item.ID,
		&item.WorkspaceID,
		&item.Name,
		&item.Kind,
		&item.Description,
		&ownerTeamID,
		&ownerPrincipal,
		&productID,
		&item.DefaultBranch,
		&item.CreatedBy,
		&createdAt,
		&updatedAt,
	); err != nil {
		return nil, mapDBError(err)
	}
	if ownerTeamID.Valid {
		item.OwnerTeamID = &ownerTeamID.String
	}
	if ownerPrincipal.Valid {
		item.OwnerPrincipal = &ownerPrincipal.String
	}
	if productID.Valid {
		item.ProductID = &productID.String
	}
	item.CreatedAt = createdAt
	item.UpdatedAt = updatedAt
	return &item, nil
}

func scanEnvironment(scanner projectRowScanner) (*domain.Environment, error) {
	var item domain.Environment
	var computeEndpoint sql.NullString
	var deferToEnvironment sql.NullString
	var variablesJSON string
	var sourceOverridesJSON string
	if err := scanner.Scan(
		&item.ID,
		&item.ProjectID,
		&item.ProjectName,
		&item.Name,
		&item.Kind,
		&item.Description,
		&item.TargetCatalog,
		&item.TargetSchema,
		&computeEndpoint,
		&deferToEnvironment,
		&variablesJSON,
		&sourceOverridesJSON,
		&item.CreatedBy,
		&item.CreatedAt,
		&item.UpdatedAt,
	); err != nil {
		return nil, mapDBError(err)
	}
	if computeEndpoint.Valid {
		item.ComputeEndpoint = &computeEndpoint.String
	}
	if deferToEnvironment.Valid {
		item.DeferToEnvironment = &deferToEnvironment.String
	}
	var err error
	item.Variables, err = unmarshalStringMap(variablesJSON)
	if err != nil {
		return nil, err
	}
	item.SourceOverrides, err = unmarshalStringMap(sourceOverridesJSON)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func marshalStringMap(value map[string]string) (string, error) {
	if len(value) == 0 {
		return "{}", nil
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal string map: %w", err)
	}
	return string(payload), nil
}

func unmarshalStringMap(value string) (map[string]string, error) {
	if value == "" {
		return map[string]string{}, nil
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(value), &out); err != nil {
		return nil, fmt.Errorf("unmarshal string map: %w", err)
	}
	if out == nil {
		out = map[string]string{}
	}
	return out, nil
}

func nullableStringValue(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

// BuildRepo persists immutable project builds in SQLite.
type BuildRepo struct {
	db *sql.DB
}

// NewBuildRepo constructs a build repository backed by SQLite.
func NewBuildRepo(db *sql.DB) *BuildRepo {
	return &BuildRepo{db: db}
}

// Create inserts a new build.
func (r *BuildRepo) Create(ctx context.Context, b *domain.Build) (*domain.Build, error) {
	now := time.Now().UTC()
	id := newID()
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO builds (
			id, project_id, product_id, environment_id, state, git_ref, commit_sha, selector,
			target_catalog, target_schema, source_model_run_id, resolved_release_id, compile_manifest,
			compile_diagnostics, state_snapshot, created_by, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		b.ProjectID,
		nullableStringValue(b.ProductID),
		b.EnvironmentID,
		defaultBuildState(b.State),
		b.GitRef,
		nullableStringValue(b.CommitSHA),
		b.Selector,
		b.TargetCatalog,
		b.TargetSchema,
		nullableStringValue(b.SourceModelRunID),
		nullableStringValue(b.ResolvedReleaseID),
		b.CompileManifest,
		nullableStringValue(b.CompileDiagnostics),
		nullableStringValue(b.StateSnapshot),
		b.CreatedBy,
		now.Format(time.RFC3339),
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// GetByID returns a build by ID.
func (r *BuildRepo) GetByID(ctx context.Context, id string) (*domain.Build, error) {
	row := r.db.QueryRowContext(ctx, buildSelectSQL+` WHERE b.id = ?`, id)
	return scanBuild(row)
}

// ListByProject returns builds for a project.
func (r *BuildRepo) ListByProject(ctx context.Context, projectID string, page domain.PageRequest) ([]domain.Build, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM builds WHERE project_id = ?`, projectID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, buildSelectSQL+`
		WHERE b.project_id = ?
		ORDER BY b.created_at DESC
		LIMIT ? OFFSET ?`, projectID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()
	items := make([]domain.Build, 0)
	for rows.Next() {
		item, err := scanBuild(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// ListByEnvironment returns builds for a project/environment pair.
func (r *BuildRepo) ListByEnvironment(ctx context.Context, projectID, environmentID string, page domain.PageRequest) ([]domain.Build, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM builds WHERE project_id = ? AND environment_id = ?`, projectID, environmentID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, buildSelectSQL+`
		WHERE b.project_id = ? AND b.environment_id = ?
		ORDER BY b.created_at DESC
		LIMIT ? OFFSET ?`, projectID, environmentID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()
	items := make([]domain.Build, 0)
	for rows.Next() {
		item, err := scanBuild(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

// UpdateState updates the lifecycle state for a build.
func (r *BuildRepo) UpdateState(ctx context.Context, id string, state string) error {
	result, err := r.db.ExecContext(ctx, `UPDATE builds SET state = ? WHERE id = ?`, defaultBuildState(state), id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("build %q not found", id)
	}
	return nil
}

const buildSelectSQL = `
	SELECT
		b.id, b.project_id, p.name, b.product_id, b.environment_id, e.name, b.state, b.git_ref,
		b.commit_sha, b.selector, b.target_catalog, b.target_schema, b.source_model_run_id,
		b.resolved_release_id, b.compile_manifest, b.compile_diagnostics, b.state_snapshot,
		b.created_by, b.created_at
	FROM builds b
	JOIN projects p ON p.id = b.project_id
	JOIN environments e ON e.id = b.environment_id`

func scanBuild(scanner projectRowScanner) (*domain.Build, error) {
	var item domain.Build
	var productID sql.NullString
	var commitSHA sql.NullString
	var sourceModelRunID sql.NullString
	var resolvedReleaseID sql.NullString
	var compileDiagnostics sql.NullString
	var stateSnapshot sql.NullString
	if err := scanner.Scan(
		&item.ID,
		&item.ProjectID,
		&item.ProjectName,
		&productID,
		&item.EnvironmentID,
		&item.EnvironmentName,
		&item.State,
		&item.GitRef,
		&commitSHA,
		&item.Selector,
		&item.TargetCatalog,
		&item.TargetSchema,
		&sourceModelRunID,
		&resolvedReleaseID,
		&item.CompileManifest,
		&compileDiagnostics,
		&stateSnapshot,
		&item.CreatedBy,
		&item.CreatedAt,
	); err != nil {
		return nil, mapDBError(err)
	}
	if productID.Valid {
		item.ProductID = &productID.String
	}
	if commitSHA.Valid {
		item.CommitSHA = &commitSHA.String
	}
	if sourceModelRunID.Valid {
		item.SourceModelRunID = &sourceModelRunID.String
	}
	if resolvedReleaseID.Valid {
		item.ResolvedReleaseID = &resolvedReleaseID.String
	}
	if compileDiagnostics.Valid {
		item.CompileDiagnostics = &compileDiagnostics.String
	}
	if stateSnapshot.Valid {
		item.StateSnapshot = &stateSnapshot.String
	}
	return &item, nil
}

func defaultBuildState(state string) string {
	switch state {
	case domain.BuildStateDraft, domain.BuildStateReleased, domain.BuildStateSuperseded:
		return state
	default:
		return domain.BuildStateReady
	}
}

// CompilationRepo persists immutable compilation artifacts in SQLite.
type CompilationRepo struct {
	db *sql.DB
}

// NewCompilationRepo constructs a compilation repository backed by SQLite.
func NewCompilationRepo(db *sql.DB) *CompilationRepo {
	return &CompilationRepo{db: db}
}

// Create inserts a new compilation.
func (r *CompilationRepo) Create(ctx context.Context, c *domain.Compilation) (*domain.Compilation, error) {
	now := time.Now().UTC()
	id := newID()
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO compilations (
			id, project_id, environment_id, git_ref, commit_sha, selector, target_catalog, target_schema,
			resolved_release_id, compile_manifest, compile_diagnostics, state_snapshot, created_by, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		c.ProjectID,
		c.EnvironmentID,
		c.GitRef,
		nullableStringValue(c.CommitSHA),
		c.Selector,
		c.TargetCatalog,
		c.TargetSchema,
		nullableStringValue(c.ResolvedReleaseID),
		c.CompileManifest,
		nullableStringValue(c.CompileDiagnostics),
		nullableStringValue(c.StateSnapshot),
		c.CreatedBy,
		now.Format(time.RFC3339),
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// GetByID returns a compilation by ID.
func (r *CompilationRepo) GetByID(ctx context.Context, id string) (*domain.Compilation, error) {
	row := r.db.QueryRowContext(ctx, compilationSelectSQL+` WHERE c.id = ?`, id)
	return scanCompilation(row)
}

// ListByEnvironment returns compilations for a project/environment pair.
func (r *CompilationRepo) ListByEnvironment(ctx context.Context, projectID, environmentID string, page domain.PageRequest) ([]domain.Compilation, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM compilations WHERE project_id = ? AND environment_id = ?`, projectID, environmentID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, compilationSelectSQL+`
		WHERE c.project_id = ? AND c.environment_id = ?
		ORDER BY c.created_at DESC
		LIMIT ? OFFSET ?`, projectID, environmentID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()
	items := make([]domain.Compilation, 0)
	for rows.Next() {
		item, err := scanCompilation(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

const compilationSelectSQL = `
	SELECT
		c.id, c.project_id, p.name, c.environment_id, e.name, c.git_ref, c.commit_sha, c.selector,
		c.target_catalog, c.target_schema, c.resolved_release_id, c.compile_manifest, c.compile_diagnostics,
		c.state_snapshot, c.created_by, c.created_at
	FROM compilations c
	JOIN projects p ON p.id = c.project_id
	JOIN environments e ON e.id = c.environment_id`

func scanCompilation(scanner projectRowScanner) (*domain.Compilation, error) {
	var item domain.Compilation
	var commitSHA sql.NullString
	var resolvedReleaseID sql.NullString
	var compileDiagnostics sql.NullString
	var stateSnapshot sql.NullString
	if err := scanner.Scan(
		&item.ID,
		&item.ProjectID,
		&item.ProjectName,
		&item.EnvironmentID,
		&item.EnvironmentName,
		&item.GitRef,
		&commitSHA,
		&item.Selector,
		&item.TargetCatalog,
		&item.TargetSchema,
		&resolvedReleaseID,
		&item.CompileManifest,
		&compileDiagnostics,
		&stateSnapshot,
		&item.CreatedBy,
		&item.CreatedAt,
	); err != nil {
		return nil, mapDBError(err)
	}
	if commitSHA.Valid {
		item.CommitSHA = &commitSHA.String
	}
	if resolvedReleaseID.Valid {
		item.ResolvedReleaseID = &resolvedReleaseID.String
	}
	if compileDiagnostics.Valid {
		item.CompileDiagnostics = &compileDiagnostics.String
	}
	if stateSnapshot.Valid {
		item.StateSnapshot = &stateSnapshot.String
	}
	return &item, nil
}

// ProjectReleaseRepo persists immutable project releases in SQLite.
type ProjectReleaseRepo struct {
	db *sql.DB
}

// NewProjectReleaseRepo constructs a project release repository backed by SQLite.
func NewProjectReleaseRepo(db *sql.DB) *ProjectReleaseRepo {
	return &ProjectReleaseRepo{db: db}
}

// Create inserts a new project release.
func (r *ProjectReleaseRepo) Create(ctx context.Context, release *domain.ProjectRelease) (*domain.ProjectRelease, error) {
	now := time.Now().UTC()
	id := newID()
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO project_releases (
			id, project_id, version, resolved_build_id, resolved_compilation_id, created_by, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id,
		release.ProjectID,
		release.Version,
		nullableStringValue(release.ResolvedBuildID),
		nullableStringValue(release.ResolvedCompileID),
		release.CreatedBy,
		now.Format(time.RFC3339),
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// GetByID returns a project release by ID.
func (r *ProjectReleaseRepo) GetByID(ctx context.Context, id string) (*domain.ProjectRelease, error) {
	row := r.db.QueryRowContext(ctx, projectReleaseSelectSQL+` WHERE pr.id = ?`, id)
	return scanProjectRelease(row)
}

// ListByProject returns releases for a project.
func (r *ProjectReleaseRepo) ListByProject(ctx context.Context, projectID string, page domain.PageRequest) ([]domain.ProjectRelease, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM project_releases WHERE project_id = ?`, projectID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, projectReleaseSelectSQL+`
		WHERE pr.project_id = ?
		ORDER BY pr.created_at DESC
		LIMIT ? OFFSET ?`, projectID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()
	items := make([]domain.ProjectRelease, 0)
	for rows.Next() {
		item, err := scanProjectRelease(rows)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

const projectReleaseSelectSQL = `
	SELECT
		pr.id, pr.project_id, p.name, pr.version, pr.resolved_build_id, pr.resolved_compilation_id,
		pr.created_by, pr.created_at
	FROM project_releases pr
	JOIN projects p ON p.id = pr.project_id`

func scanProjectRelease(scanner projectRowScanner) (*domain.ProjectRelease, error) {
	var item domain.ProjectRelease
	var resolvedBuildID sql.NullString
	var resolvedCompilationID sql.NullString
	if err := scanner.Scan(
		&item.ID,
		&item.ProjectID,
		&item.ProjectName,
		&item.Version,
		&resolvedBuildID,
		&resolvedCompilationID,
		&item.CreatedBy,
		&item.CreatedAt,
	); err != nil {
		return nil, mapDBError(err)
	}
	if resolvedBuildID.Valid {
		item.ResolvedBuildID = &resolvedBuildID.String
	}
	if resolvedCompilationID.Valid {
		item.ResolvedCompileID = &resolvedCompilationID.String
	}
	return &item, nil
}
