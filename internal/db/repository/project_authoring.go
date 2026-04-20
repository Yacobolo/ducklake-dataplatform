package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Yacobolo/quackstack/internal/domain"
)

var _ domain.ProjectDependencyRepository = (*ProjectDependencyRepo)(nil)
var _ domain.SourceDefinitionRepository = (*SourceDefinitionRepo)(nil)
var _ domain.SeedRepository = (*SeedRepo)(nil)

// ProjectDependencyRepo persists ordered project dependency declarations.
type ProjectDependencyRepo struct {
	db *sql.DB
}

// NewProjectDependencyRepo creates a repository for project dependency records.
func NewProjectDependencyRepo(db *sql.DB) *ProjectDependencyRepo {
	return &ProjectDependencyRepo{db: db}
}

// Create inserts a project dependency and returns the stored record.
func (r *ProjectDependencyRepo) Create(ctx context.Context, dep *domain.ProjectDependency) (*domain.ProjectDependency, error) {
	now := time.Now().UTC()
	id := newID()
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO project_dependencies (
			id, project_id, dependency_project_id, dependency_project, dependency_kind, version_constraint,
			resolved_release_id, position, created_by, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		dep.ProjectID,
		dep.DependencyProjectID,
		dep.DependencyProject,
		defaultProjectDependencyKind(dep.DependencyKind),
		dep.VersionConstraint,
		nullableStringValue(dep.ResolvedReleaseID),
		dep.Position,
		dep.CreatedBy,
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	row := r.db.QueryRowContext(ctx, projectDependencySelectSQL+` WHERE d.id = ?`, id)
	return scanProjectDependency(row)
}

// GetByID returns one project dependency by id.
func (r *ProjectDependencyRepo) GetByID(ctx context.Context, id string) (*domain.ProjectDependency, error) {
	row := r.db.QueryRowContext(ctx, projectDependencySelectSQL+` WHERE d.id = ?`, id)
	return scanProjectDependency(row)
}

// ListByProject returns project dependencies in declaration order.
func (r *ProjectDependencyRepo) ListByProject(ctx context.Context, projectID string) ([]domain.ProjectDependency, error) {
	rows, err := r.db.QueryContext(ctx, projectDependencySelectSQL+`
		WHERE d.project_id = ?
		ORDER BY d.position, d.dependency_project`, projectID)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]domain.ProjectDependency, 0)
	for rows.Next() {
		item, err := scanProjectDependency(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// Delete removes a project dependency by project and dependency name.
func (r *ProjectDependencyRepo) Delete(ctx context.Context, projectID string, dependencyID string) error {
	result, err := r.db.ExecContext(ctx, `
		DELETE FROM project_dependencies WHERE project_id = ? AND id = ?`,
		projectID, dependencyID)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("dependency %q not found for project %q", dependencyID, projectID)
	}
	return nil
}

const projectDependencySelectSQL = `
	SELECT
		d.id, d.project_id, p.name, d.dependency_project_id, d.dependency_project, d.dependency_kind,
		d.version_constraint, d.resolved_release_id, d.position, d.created_by, d.created_at, d.updated_at
	FROM project_dependencies d
	JOIN projects p ON p.id = d.project_id`

func scanProjectDependency(scanner projectRowScanner) (*domain.ProjectDependency, error) {
	var item domain.ProjectDependency
	var resolvedReleaseID sql.NullString
	var createdAtRaw string
	var updatedAtRaw string
	if err := scanner.Scan(
		&item.ID,
		&item.ProjectID,
		&item.ProjectName,
		&item.DependencyProjectID,
		&item.DependencyProject,
		&item.DependencyKind,
		&item.VersionConstraint,
		&resolvedReleaseID,
		&item.Position,
		&item.CreatedBy,
		&createdAtRaw,
		&updatedAtRaw,
	); err != nil {
		return nil, mapDBError(err)
	}
	if resolvedReleaseID.Valid {
		item.ResolvedReleaseID = &resolvedReleaseID.String
	}
	item.CreatedAt = parseSQLiteTimestamp(createdAtRaw)
	item.UpdatedAt = parseSQLiteTimestamp(updatedAtRaw)
	return &item, nil
}

func defaultProjectDependencyKind(value string) string {
	if value == "" {
		return "project"
	}
	return value
}

// SourceDefinitionRepo persists project-owned source definitions.
type SourceDefinitionRepo struct {
	db *sql.DB
}

// NewSourceDefinitionRepo creates a repository for source definitions.
func NewSourceDefinitionRepo(db *sql.DB) *SourceDefinitionRepo {
	return &SourceDefinitionRepo{db: db}
}

// Create inserts a source definition and returns the stored record.
func (r *SourceDefinitionRepo) Create(ctx context.Context, source *domain.SourceDefinition) (*domain.SourceDefinition, error) {
	now := time.Now().UTC()
	id := newID()
	freshnessJSON, err := marshalSourceFreshness(source.Freshness)
	if err != nil {
		return nil, err
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO source_definitions (
			id, project_name, source_name, table_name, relation_ref, description,
			freshness_json, created_by, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		source.ProjectName,
		source.SourceName,
		source.TableName,
		source.RelationRef,
		source.Description,
		freshnessJSON,
		source.CreatedBy,
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	row := r.db.QueryRowContext(ctx, sourceDefinitionSelectSQL+` WHERE s.id = ?`, id)
	return scanSourceDefinition(row)
}

// GetByName returns a source definition by project, source name, and table name.
func (r *SourceDefinitionRepo) GetByName(ctx context.Context, projectName, sourceName, tableName string) (*domain.SourceDefinition, error) {
	row := r.db.QueryRowContext(ctx, sourceDefinitionSelectSQL+`
		WHERE s.project_name = ? AND s.source_name = ? AND s.table_name = ?`,
		projectName, sourceName, tableName)
	return scanSourceDefinition(row)
}

// ListByProject returns all source definitions for a project.
func (r *SourceDefinitionRepo) ListByProject(ctx context.Context, projectName string) ([]domain.SourceDefinition, error) {
	rows, err := r.db.QueryContext(ctx, sourceDefinitionSelectSQL+`
		WHERE s.project_name = ?
		ORDER BY s.source_name, s.table_name`, projectName)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]domain.SourceDefinition, 0)
	for rows.Next() {
		item, err := scanSourceDefinition(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// Update replaces a source definition and returns the stored record.
func (r *SourceDefinitionRepo) Update(ctx context.Context, id string, source *domain.SourceDefinition) (*domain.SourceDefinition, error) {
	currentRow := r.db.QueryRowContext(ctx, sourceDefinitionSelectSQL+` WHERE s.id = ?`, id)
	current, err := scanSourceDefinition(currentRow)
	if err != nil {
		return nil, err
	}
	freshness := source.Freshness
	if freshness == nil {
		freshness = current.Freshness
	}
	freshnessJSON, err := marshalSourceFreshness(freshness)
	if err != nil {
		return nil, err
	}
	_, err = r.db.ExecContext(ctx, `
		UPDATE source_definitions
		SET project_name = ?, source_name = ?, table_name = ?, relation_ref = ?, description = ?,
		    freshness_json = ?, updated_at = ?
		WHERE id = ?`,
		source.ProjectName,
		source.SourceName,
		source.TableName,
		source.RelationRef,
		source.Description,
		freshnessJSON,
		time.Now().UTC().Format(time.RFC3339),
		id,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	row := r.db.QueryRowContext(ctx, sourceDefinitionSelectSQL+` WHERE s.id = ?`, id)
	return scanSourceDefinition(row)
}

// Delete removes a source definition by ID.
func (r *SourceDefinitionRepo) Delete(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM source_definitions WHERE id = ?`, id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("source definition %q not found", id)
	}
	return nil
}

const sourceDefinitionSelectSQL = `
	SELECT
		s.id, s.project_name, s.source_name, s.table_name, s.relation_ref, s.description,
		s.freshness_json, s.created_by, s.created_at, s.updated_at
	FROM source_definitions s`

func scanSourceDefinition(scanner projectRowScanner) (*domain.SourceDefinition, error) {
	var item domain.SourceDefinition
	var freshnessJSON string
	var createdAtRaw string
	var updatedAtRaw string
	if err := scanner.Scan(
		&item.ID,
		&item.ProjectName,
		&item.SourceName,
		&item.TableName,
		&item.RelationRef,
		&item.Description,
		&freshnessJSON,
		&item.CreatedBy,
		&createdAtRaw,
		&updatedAtRaw,
	); err != nil {
		return nil, mapDBError(err)
	}
	item.CreatedAt = parseSQLiteTimestamp(createdAtRaw)
	item.UpdatedAt = parseSQLiteTimestamp(updatedAtRaw)
	freshness, err := unmarshalSourceFreshness(freshnessJSON)
	if err != nil {
		return nil, err
	}
	item.Freshness = freshness
	return &item, nil
}

func marshalSourceFreshness(value *domain.SourceFreshnessPolicy) (string, error) {
	if value == nil {
		return "{}", nil
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal source freshness: %w", err)
	}
	return string(payload), nil
}

func unmarshalSourceFreshness(value string) (*domain.SourceFreshnessPolicy, error) {
	if value == "" || value == "{}" {
		return nil, nil
	}
	var out domain.SourceFreshnessPolicy
	if err := json.Unmarshal([]byte(value), &out); err != nil {
		return nil, fmt.Errorf("unmarshal source freshness: %w", err)
	}
	if out.TimestampColumn == "" && out.MaxLagSeconds == 0 {
		return nil, nil
	}
	return &out, nil
}

// SeedRepo persists project-owned seed resources.
type SeedRepo struct {
	db *sql.DB
}

// NewSeedRepo creates a repository for project seed resources.
func NewSeedRepo(db *sql.DB) *SeedRepo {
	return &SeedRepo{db: db}
}

// Create inserts a seed resource and returns the stored record.
func (r *SeedRepo) Create(ctx context.Context, seed *domain.Seed) (*domain.Seed, error) {
	now := time.Now().UTC()
	id := newID()
	columnTypesJSON, err := marshalStringMap(seed.ColumnTypes)
	if err != nil {
		return nil, err
	}
	tagsJSON, err := json.Marshal(seed.Tags)
	if err != nil {
		return nil, fmt.Errorf("marshal seed tags: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO seeds (
			id, project_name, name, description, input_ref, format, delimiter, has_header,
			column_types_json, tags_json, created_by, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id,
		seed.ProjectName,
		seed.Name,
		seed.Description,
		seed.InputRef,
		seed.Format,
		seed.Delimiter,
		boolToInt(seed.HasHeader),
		columnTypesJSON,
		string(tagsJSON),
		seed.CreatedBy,
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	row := r.db.QueryRowContext(ctx, seedSelectSQL+` WHERE s.id = ?`, id)
	return scanSeed(row)
}

// GetByName returns a seed by project and seed name.
func (r *SeedRepo) GetByName(ctx context.Context, projectName, name string) (*domain.Seed, error) {
	row := r.db.QueryRowContext(ctx, seedSelectSQL+` WHERE s.project_name = ? AND s.name = ?`, projectName, name)
	return scanSeed(row)
}

// ListByProject returns all seeds for a project.
func (r *SeedRepo) ListByProject(ctx context.Context, projectName string) ([]domain.Seed, error) {
	rows, err := r.db.QueryContext(ctx, seedSelectSQL+` WHERE s.project_name = ? ORDER BY s.name`, projectName)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	items := make([]domain.Seed, 0)
	for rows.Next() {
		item, err := scanSeed(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// Update replaces a seed resource and returns the stored record.
func (r *SeedRepo) Update(ctx context.Context, id string, seed *domain.Seed) (*domain.Seed, error) {
	columnTypesJSON, err := marshalStringMap(seed.ColumnTypes)
	if err != nil {
		return nil, err
	}
	tagsJSON, err := json.Marshal(seed.Tags)
	if err != nil {
		return nil, fmt.Errorf("marshal seed tags: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		UPDATE seeds
		SET project_name = ?, name = ?, description = ?, input_ref = ?, format = ?, delimiter = ?,
		    has_header = ?, column_types_json = ?, tags_json = ?, updated_at = ?
		WHERE id = ?`,
		seed.ProjectName,
		seed.Name,
		seed.Description,
		seed.InputRef,
		seed.Format,
		seed.Delimiter,
		boolToInt(seed.HasHeader),
		columnTypesJSON,
		string(tagsJSON),
		time.Now().UTC().Format(time.RFC3339),
		id,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	row := r.db.QueryRowContext(ctx, seedSelectSQL+` WHERE s.id = ?`, id)
	return scanSeed(row)
}

// Delete removes a seed by ID.
func (r *SeedRepo) Delete(ctx context.Context, id string) error {
	result, err := r.db.ExecContext(ctx, `DELETE FROM seeds WHERE id = ?`, id)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("seed %q not found", id)
	}
	return nil
}

const seedSelectSQL = `
	SELECT
		s.id, s.project_name, s.name, s.description, s.input_ref, s.format, s.delimiter, s.has_header,
		s.column_types_json, s.tags_json, s.created_by, s.created_at, s.updated_at
	FROM seeds s`

func scanSeed(scanner projectRowScanner) (*domain.Seed, error) {
	var item domain.Seed
	var hasHeader int
	var columnTypesJSON string
	var tagsJSON string
	var createdAtRaw string
	var updatedAtRaw string
	if err := scanner.Scan(
		&item.ID,
		&item.ProjectName,
		&item.Name,
		&item.Description,
		&item.InputRef,
		&item.Format,
		&item.Delimiter,
		&hasHeader,
		&columnTypesJSON,
		&tagsJSON,
		&item.CreatedBy,
		&createdAtRaw,
		&updatedAtRaw,
	); err != nil {
		return nil, mapDBError(err)
	}
	item.CreatedAt = parseSQLiteTimestamp(createdAtRaw)
	item.UpdatedAt = parseSQLiteTimestamp(updatedAtRaw)
	item.HasHeader = hasHeader != 0
	columnTypes, err := unmarshalStringMap(columnTypesJSON)
	if err != nil {
		return nil, err
	}
	item.ColumnTypes = columnTypes
	if err := json.Unmarshal([]byte(tagsJSON), &item.Tags); err != nil {
		return nil, fmt.Errorf("unmarshal seed tags: %w", err)
	}
	if item.Tags == nil {
		item.Tags = []string{}
	}
	return &item, nil
}
