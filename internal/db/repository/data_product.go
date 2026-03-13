package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"duck-demo/internal/domain"
)

var (
	_ domain.DomainRepository      = (*DomainRepo)(nil)
	_ domain.TeamRepository        = (*TeamRepo)(nil)
	_ domain.DataProductRepository = (*DataProductRepo)(nil)
)

// DomainRepo persists product domains.
type DomainRepo struct {
	db *sql.DB
}

// NewDomainRepo constructs a domain repository.
func NewDomainRepo(db *sql.DB) *DomainRepo {
	return &DomainRepo{db: db}
}

// Create inserts a product domain.
func (r *DomainRepo) Create(ctx context.Context, d *domain.Domain) (*domain.Domain, error) {
	if d == nil {
		return nil, domain.ErrValidation("domain is required")
	}
	if strings.TrimSpace(d.Name) == "" {
		return nil, domain.ErrValidation("domain name is required")
	}
	id := d.ID
	if id == "" {
		id = newID()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO domains (id, name, description)
		VALUES (?, ?, ?)
	`, id, strings.TrimSpace(d.Name), strings.TrimSpace(d.Description))
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// GetByID returns a domain by ID.
func (r *DomainRepo) GetByID(ctx context.Context, id string) (*domain.Domain, error) {
	return r.getOne(ctx, `SELECT id, name, description, created_at, updated_at FROM domains WHERE id = ?`, id)
}

// GetByName returns a domain by name.
func (r *DomainRepo) GetByName(ctx context.Context, name string) (*domain.Domain, error) {
	return r.getOne(ctx, `SELECT id, name, description, created_at, updated_at FROM domains WHERE name = ?`, strings.TrimSpace(name))
}

// List returns domains ordered by name.
func (r *DomainRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.Domain, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM domains`).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, description, created_at, updated_at
		FROM domains
		ORDER BY name ASC
		LIMIT ? OFFSET ?
	`, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.Domain, 0)
	for rows.Next() {
		item, scanErr := scanDomain(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

// Update modifies a domain by name.
func (r *DomainRepo) Update(ctx context.Context, name string, d *domain.Domain) (*domain.Domain, error) {
	if d == nil {
		return nil, domain.ErrValidation("domain is required")
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE domains
		SET description = ?, updated_at = CURRENT_TIMESTAMP
		WHERE name = ?
	`, strings.TrimSpace(d.Description), strings.TrimSpace(name))
	if err != nil {
		return nil, mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		return nil, domain.ErrNotFound("domain %q not found", name)
	}
	return r.GetByName(ctx, name)
}

// Delete removes a domain by name.
func (r *DomainRepo) Delete(ctx context.Context, name string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM domains WHERE name = ?`, strings.TrimSpace(name))
	if err != nil {
		return mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("domain %q not found", name)
	}
	return nil
}

func (r *DomainRepo) getOne(ctx context.Context, query string, args ...any) (*domain.Domain, error) {
	row := r.db.QueryRowContext(ctx, query, args...)
	item, err := scanDomain(row)
	if err != nil {
		return nil, mapDBError(err)
	}
	return item, nil
}

// TeamRepo persists owner teams.
type TeamRepo struct {
	db *sql.DB
}

// NewTeamRepo constructs a team repository.
func NewTeamRepo(db *sql.DB) *TeamRepo {
	return &TeamRepo{db: db}
}

// Create inserts an owner team.
func (r *TeamRepo) Create(ctx context.Context, t *domain.Team) (*domain.Team, error) {
	if t == nil {
		return nil, domain.ErrValidation("team is required")
	}
	if strings.TrimSpace(t.DomainID) == "" {
		return nil, domain.ErrValidation("domain_id is required")
	}
	if strings.TrimSpace(t.Name) == "" {
		return nil, domain.ErrValidation("team name is required")
	}
	id := t.ID
	if id == "" {
		id = newID()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO teams (id, domain_id, name, contact_channel)
		VALUES (?, ?, ?, ?)
	`, id, t.DomainID, strings.TrimSpace(t.Name), strings.TrimSpace(t.ContactChannel))
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// GetByID returns a team by ID.
func (r *TeamRepo) GetByID(ctx context.Context, id string) (*domain.Team, error) {
	return r.getOne(ctx, `
		SELECT id, domain_id, name, contact_channel, created_at, updated_at
		FROM teams
		WHERE id = ?
	`, id)
}

// GetByDomainAndName returns a team by domain and name.
func (r *TeamRepo) GetByDomainAndName(ctx context.Context, domainID, name string) (*domain.Team, error) {
	return r.getOne(ctx, `
		SELECT id, domain_id, name, contact_channel, created_at, updated_at
		FROM teams
		WHERE domain_id = ? AND name = ?
	`, domainID, strings.TrimSpace(name))
}

// List returns teams ordered by name.
func (r *TeamRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.Team, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM teams`).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, domain_id, name, contact_channel, created_at, updated_at
		FROM teams
		ORDER BY name ASC
		LIMIT ? OFFSET ?
	`, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.Team, 0)
	for rows.Next() {
		item, scanErr := scanTeam(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

// Update modifies an owner team identified by domain and name.
func (r *TeamRepo) Update(ctx context.Context, domainID, name string, t *domain.Team) (*domain.Team, error) {
	if t == nil {
		return nil, domain.ErrValidation("team is required")
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE teams
		SET contact_channel = ?, updated_at = CURRENT_TIMESTAMP
		WHERE domain_id = ? AND name = ?
	`, strings.TrimSpace(t.ContactChannel), domainID, strings.TrimSpace(name))
	if err != nil {
		return nil, mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		return nil, domain.ErrNotFound("team %q not found", name)
	}
	return r.GetByDomainAndName(ctx, domainID, name)
}

// Delete removes an owner team identified by domain and name.
func (r *TeamRepo) Delete(ctx context.Context, domainID, name string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM teams WHERE domain_id = ? AND name = ?`, domainID, strings.TrimSpace(name))
	if err != nil {
		return mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("team %q not found", name)
	}
	return nil
}

func (r *TeamRepo) getOne(ctx context.Context, query string, args ...any) (*domain.Team, error) {
	row := r.db.QueryRowContext(ctx, query, args...)
	item, err := scanTeam(row)
	if err != nil {
		return nil, mapDBError(err)
	}
	return item, nil
}

// DataProductRepo persists product control-plane records.
type DataProductRepo struct {
	db *sql.DB
}

// NewDataProductRepo constructs a data product repository.
func NewDataProductRepo(db *sql.DB) *DataProductRepo {
	return &DataProductRepo{db: db}
}

// Create inserts a product spec.
func (r *DataProductRepo) Create(ctx context.Context, p *domain.DataProduct) (*domain.DataProduct, error) {
	if p == nil {
		return nil, domain.ErrValidation("product is required")
	}
	id := p.ID
	if id == "" {
		id = newID()
	}

	definitionsJSON, err := json.Marshal(p.BusinessDefinitions)
	if err != nil {
		return nil, fmt.Errorf("marshal business_definitions_json: %w", err)
	}
	contractJSON, err := json.Marshal(p.Contract)
	if err != nil {
		return nil, fmt.Errorf("marshal contract_json: %w", err)
	}
	sloJSON, err := json.Marshal(p.SLO)
	if err != nil {
		return nil, fmt.Errorf("marshal slo_json: %w", err)
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO data_products (
			id, slug, name, description, domain_id, owner_team_id, steward_principal,
			contact_channel, visibility, consumer_audience, docs_url, access_request_path,
			business_definitions_json, contract_json, slo_json, publication_intent, created_by
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		id,
		strings.TrimSpace(p.Slug),
		strings.TrimSpace(p.Name),
		strings.TrimSpace(p.Description),
		p.DomainID,
		p.OwnerTeamID,
		strings.TrimSpace(p.StewardPrincipal),
		strings.TrimSpace(p.ContactChannel),
		strings.TrimSpace(p.Visibility),
		strings.TrimSpace(p.ConsumerAudience),
		strings.TrimSpace(p.DocsURL),
		strings.TrimSpace(p.AccessRequestPath),
		string(definitionsJSON),
		string(contractJSON),
		string(sloJSON),
		defaultProductString(p.PublicationIntent, domain.ProductPublicationIntentDraft),
		strings.TrimSpace(p.CreatedBy),
	)
	if err != nil {
		return nil, mapDBError(err)
	}

	return r.getProductByID(ctx, id)
}

// GetBySlug returns the full product detail for a slug.
func (r *DataProductRepo) GetBySlug(ctx context.Context, slug string) (*domain.DataProductDetail, error) {
	item, err := r.getProductWithJoins(ctx, `
		SELECT
			p.id, p.slug, p.name, p.description, p.domain_id, p.owner_team_id, p.steward_principal,
			p.contact_channel, p.visibility, p.consumer_audience, p.docs_url, p.access_request_path,
			p.business_definitions_json, p.contract_json, p.slo_json, p.publication_intent,
			p.created_by, p.created_at, p.updated_at,
			d.id, d.name, d.description, d.created_at, d.updated_at,
			t.id, t.domain_id, t.name, t.contact_channel, t.created_at, t.updated_at,
			s.publication_state, s.certification_state, s.freshness_status, s.quality_status,
			s.last_successful_update_at, s.failing_checks_count, s.lineage_coverage,
			s.adoption_metrics_json, s.open_warnings_json, s.replacement_product_id, s.updated_at
		FROM data_products p
		INNER JOIN domains d ON d.id = p.domain_id
		INNER JOIN teams t ON t.id = p.owner_team_id
		LEFT JOIN data_product_status s ON s.product_id = p.id
		WHERE p.slug = ?
	`, strings.TrimSpace(slug))
	if err != nil {
		return nil, err
	}

	versions, err := r.ListVersions(ctx, item.Product.ID)
	if err != nil {
		return nil, err
	}
	item.Versions = versions
	if len(versions) > 0 {
		outputs, err := r.ListOutputs(ctx, versions[0].ID)
		if err != nil {
			return nil, err
		}
		item.Outputs = outputs
		entrypoints, err := r.ListSemanticEntrypoints(ctx, versions[0].ID)
		if err != nil {
			return nil, err
		}
		item.SemanticEntrypoints = entrypoints
	}
	dependencies, err := r.ListDependencies(ctx, item.Product.ID)
	if err != nil {
		return nil, err
	}
	item.Dependencies = dependencies
	subscriptions, err := r.ListSubscriptions(ctx, item.Product.ID)
	if err != nil {
		return nil, err
	}
	item.Subscriptions = subscriptions
	events, _, err := r.ListEvents(ctx, item.Product.ID, domain.PageRequest{MaxResults: 20})
	if err != nil {
		return nil, err
	}
	item.Events = events

	return item, nil
}

// GetByID returns the authored product row for an internal control-plane identifier.
func (r *DataProductRepo) GetByID(ctx context.Context, productID string) (*domain.DataProduct, error) {
	return r.getProductByID(ctx, strings.TrimSpace(productID))
}

// List returns ranked product discovery rows.
func (r *DataProductRepo) List(ctx context.Context, filter domain.DataProductFilter) ([]domain.DataProductListItem, int64, error) {
	whereSQL, args := buildDataProductListWhere(filter)
	//nolint:gosec // whereSQL is assembled from fixed clauses and parameter placeholders.
	countQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM data_products p
		INNER JOIN domains d ON d.id = p.domain_id
		INNER JOIN teams t ON t.id = p.owner_team_id
		LEFT JOIN data_product_status s ON s.product_id = p.id
		%s`, whereSQL)

	var total int64
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}

	args = append(args, filter.Page.Limit(), filter.Page.Offset())
	//nolint:gosec // whereSQL is assembled from fixed clauses and parameter placeholders.
	listQuery := fmt.Sprintf(`
		SELECT
			p.id, p.slug, p.name, p.description, p.domain_id, p.owner_team_id, p.steward_principal,
			p.contact_channel, p.visibility, p.consumer_audience, p.docs_url, p.access_request_path,
			p.business_definitions_json, p.contract_json, p.slo_json, p.publication_intent,
			p.created_by, p.created_at, p.updated_at,
			d.id, d.name, d.description, d.created_at, d.updated_at,
			t.id, t.domain_id, t.name, t.contact_channel, t.created_at, t.updated_at,
			s.publication_state, s.certification_state, s.freshness_status, s.quality_status,
			s.last_successful_update_at, s.failing_checks_count, s.lineage_coverage,
			s.adoption_metrics_json, s.open_warnings_json, s.replacement_product_id, s.updated_at,
			v.id, v.product_id, v.producing_build_id, v.version, v.release_state, v.compatibility_level,
			v.contract_json, v.slo_json, v.docs_url, v.access_request_path, v.created_by, v.created_at,
			o.id, o.product_version_id, o.asset_id, a.asset_key, a.asset_type, o.is_primary, o.created_at
		FROM data_products p
		INNER JOIN domains d ON d.id = p.domain_id
		INNER JOIN teams t ON t.id = p.owner_team_id
		LEFT JOIN data_product_status s ON s.product_id = p.id
		LEFT JOIN data_product_versions v ON v.id = (
			SELECT id FROM data_product_versions WHERE product_id = p.id ORDER BY version DESC LIMIT 1
		)
		LEFT JOIN product_outputs o ON o.product_version_id = v.id AND o.is_primary = 1
		LEFT JOIN data_assets a ON a.id = o.asset_id
		%s
		ORDER BY
			CASE COALESCE(s.publication_state, p.publication_intent)
				WHEN 'PUBLISHED' THEN 0
				WHEN 'DEPRECATED' THEN 1
				WHEN 'DRAFT' THEN 2
				ELSE 3
			END,
			CASE COALESCE(s.certification_state, 'DRAFT')
				WHEN 'CERTIFIED' THEN 0
				WHEN 'DRAFT' THEN 1
				ELSE 2
			END,
			CASE COALESCE(s.quality_status, 'UNKNOWN')
				WHEN 'GOOD' THEN 0
				WHEN 'HEALTHY' THEN 0
				WHEN 'UNKNOWN' THEN 1
				WHEN 'STALE' THEN 2
				ELSE 3
			END,
			p.created_at DESC
		LIMIT ? OFFSET ?
	`, whereSQL)
	rows, err := r.db.QueryContext(ctx, listQuery, args...)
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.DataProductListItem, 0)
	for rows.Next() {
		item, scanErr := scanProductListItem(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

// Update modifies a product spec.
func (r *DataProductRepo) Update(ctx context.Context, p *domain.DataProduct) (*domain.DataProduct, error) {
	if p == nil {
		return nil, domain.ErrValidation("product is required")
	}
	definitionsJSON, err := json.Marshal(p.BusinessDefinitions)
	if err != nil {
		return nil, fmt.Errorf("marshal business_definitions_json: %w", err)
	}
	contractJSON, err := json.Marshal(p.Contract)
	if err != nil {
		return nil, fmt.Errorf("marshal contract_json: %w", err)
	}
	sloJSON, err := json.Marshal(p.SLO)
	if err != nil {
		return nil, fmt.Errorf("marshal slo_json: %w", err)
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE data_products
		SET name = ?, description = ?, domain_id = ?, owner_team_id = ?, steward_principal = ?,
		    contact_channel = ?, visibility = ?, consumer_audience = ?, docs_url = ?, access_request_path = ?,
		    business_definitions_json = ?, contract_json = ?, slo_json = ?, publication_intent = ?,
		    updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`,
		strings.TrimSpace(p.Name),
		strings.TrimSpace(p.Description),
		p.DomainID,
		p.OwnerTeamID,
		strings.TrimSpace(p.StewardPrincipal),
		strings.TrimSpace(p.ContactChannel),
		strings.TrimSpace(p.Visibility),
		strings.TrimSpace(p.ConsumerAudience),
		strings.TrimSpace(p.DocsURL),
		strings.TrimSpace(p.AccessRequestPath),
		string(definitionsJSON),
		string(contractJSON),
		string(sloJSON),
		defaultProductString(p.PublicationIntent, domain.ProductPublicationIntentDraft),
		p.ID,
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rows == 0 {
		return nil, domain.ErrNotFound("product %q not found", p.ID)
	}
	return r.getProductByID(ctx, p.ID)
}

// Delete removes a product and clears runtime asset ownership bindings first.
func (r *DataProductRepo) Delete(ctx context.Context, productID string) error {
	if _, err := r.db.ExecContext(ctx, `UPDATE data_assets SET product_id = NULL WHERE product_id = ?`, productID); err != nil {
		return mapDBError(err)
	}
	res, err := r.db.ExecContext(ctx, `DELETE FROM data_products WHERE id = ?`, productID)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("product %q not found", productID)
	}
	return nil
}

// CreateVersion inserts a product version snapshot.
func (r *DataProductRepo) CreateVersion(ctx context.Context, version *domain.DataProductVersion) (*domain.DataProductVersion, error) {
	if version == nil {
		return nil, domain.ErrValidation("product version is required")
	}
	id := version.ID
	if id == "" {
		id = newID()
	}
	contractJSON, err := json.Marshal(version.Contract)
	if err != nil {
		return nil, fmt.Errorf("marshal contract_json: %w", err)
	}
	sloJSON, err := json.Marshal(version.SLO)
	if err != nil {
		return nil, fmt.Errorf("marshal slo_json: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO data_product_versions (
			id, product_id, producing_build_id, version, release_state, compatibility_level,
			contract_json, slo_json, docs_url, access_request_path, created_by
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		id,
		version.ProductID,
		nullableStringValue(version.ProducingBuildID),
		version.Version,
		defaultProductString(version.ReleaseState, domain.ProductReleaseStateDraft),
		defaultProductString(version.CompatibilityLevel, domain.ProductCompatibilityBackwardCompatible),
		string(contractJSON),
		string(sloJSON),
		strings.TrimSpace(version.DocsURL),
		strings.TrimSpace(version.AccessRequestPath),
		strings.TrimSpace(version.CreatedBy),
	)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.getVersionByID(ctx, id)
}

// ListVersions returns all versions for a product.
func (r *DataProductRepo) ListVersions(ctx context.Context, productID string) ([]domain.DataProductVersion, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, product_id, producing_build_id, version, release_state, compatibility_level,
		       contract_json, slo_json, docs_url, access_request_path, created_by, created_at
		FROM data_product_versions
		WHERE product_id = ?
		ORDER BY version DESC
	`, productID)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.DataProductVersion, 0)
	for rows.Next() {
		item, scanErr := scanProductVersion(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// GetVersionByNumber returns a product version by ordinal.
func (r *DataProductRepo) GetVersionByNumber(ctx context.Context, productID string, version int) (*domain.DataProductVersion, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, product_id, producing_build_id, version, release_state, compatibility_level,
		       contract_json, slo_json, docs_url, access_request_path, created_by, created_at
		FROM data_product_versions
		WHERE product_id = ? AND version = ?
	`, productID, version)
	item, err := scanProductVersion(row)
	if err != nil {
		return nil, mapDBError(err)
	}
	return item, nil
}

// UpdateVersionReleaseState updates the release state for a version.
func (r *DataProductRepo) UpdateVersionReleaseState(ctx context.Context, versionID string, releaseState string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE data_product_versions
		SET release_state = ?
		WHERE id = ?
	`, strings.TrimSpace(releaseState), versionID)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("product version %q not found", versionID)
	}
	return nil
}

// UpdatePublicationIntent updates the authored publication intent for a product.
func (r *DataProductRepo) UpdatePublicationIntent(ctx context.Context, productID string, publicationIntent string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE data_products
		SET publication_intent = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, strings.TrimSpace(publicationIntent), productID)
	if err != nil {
		return mapDBError(err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return domain.ErrNotFound("product %q not found", productID)
	}
	return nil
}

// UpsertStatus stores the computed runtime status for a product.
func (r *DataProductRepo) UpsertStatus(ctx context.Context, status *domain.DataProductStatus) error {
	if status == nil {
		return domain.ErrValidation("status is required")
	}
	adoptionJSON, err := json.Marshal(status.AdoptionMetrics)
	if err != nil {
		return fmt.Errorf("marshal adoption_metrics_json: %w", err)
	}
	warningsJSON, err := json.Marshal(status.OpenWarnings)
	if err != nil {
		return fmt.Errorf("marshal open_warnings_json: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO data_product_status (
			product_id, publication_state, certification_state, freshness_status, quality_status,
			last_successful_update_at, failing_checks_count, lineage_coverage, adoption_metrics_json,
			open_warnings_json, replacement_product_id, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(product_id) DO UPDATE SET
			publication_state = excluded.publication_state,
			certification_state = excluded.certification_state,
			freshness_status = excluded.freshness_status,
			quality_status = excluded.quality_status,
			last_successful_update_at = excluded.last_successful_update_at,
			failing_checks_count = excluded.failing_checks_count,
			lineage_coverage = excluded.lineage_coverage,
			adoption_metrics_json = excluded.adoption_metrics_json,
			open_warnings_json = excluded.open_warnings_json,
			replacement_product_id = excluded.replacement_product_id,
			updated_at = CURRENT_TIMESTAMP
	`,
		status.ProductID,
		defaultProductString(status.PublicationState, domain.ProductReleaseStateDraft),
		defaultProductString(status.CertificationState, domain.CertificationDraft),
		defaultProductString(status.FreshnessStatus, "UNKNOWN"),
		defaultProductString(status.QualityStatus, "UNKNOWN"),
		status.LastSuccessfulUpdateAt,
		status.FailingChecksCount,
		status.LineageCoverage,
		string(adoptionJSON),
		string(warningsJSON),
		status.ReplacementProductID,
	)
	if err != nil {
		return mapDBError(err)
	}
	return nil
}

// GetStatus returns the computed runtime status for a product.
func (r *DataProductRepo) GetStatus(ctx context.Context, productID string) (*domain.DataProductStatus, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT publication_state, certification_state, freshness_status, quality_status,
		       last_successful_update_at, failing_checks_count, lineage_coverage,
		       adoption_metrics_json, open_warnings_json, replacement_product_id, updated_at
		FROM data_product_status
		WHERE product_id = ?
	`, productID)
	item, err := scanProductStatus(productID, row)
	if err != nil {
		return nil, mapDBError(err)
	}
	return item, nil
}

// AddOutput links a version to a runtime asset output.
func (r *DataProductRepo) AddOutput(ctx context.Context, output *domain.ProductOutput) error {
	if output == nil {
		return domain.ErrValidation("output is required")
	}
	id := output.ID
	if id == "" {
		id = newID()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO product_outputs (id, product_version_id, asset_id, is_primary)
		VALUES (?, ?, ?, ?)
	`, id, output.ProductVersionID, output.AssetID, boolToInt(output.IsPrimary))
	if err != nil {
		return mapDBError(err)
	}
	return nil
}

// ListOutputs returns version-to-asset output links.
func (r *DataProductRepo) ListOutputs(ctx context.Context, productVersionID string) ([]domain.ProductOutput, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT o.id, o.product_version_id, o.asset_id, a.asset_key, a.asset_type, o.is_primary, o.created_at
		FROM product_outputs o
		INNER JOIN data_assets a ON a.id = o.asset_id
		WHERE o.product_version_id = ?
		ORDER BY o.is_primary DESC, a.asset_key ASC
	`, productVersionID)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.ProductOutput, 0)
	for rows.Next() {
		item, scanErr := scanProductOutput(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ReplaceOutputs replaces all output links for a version.
func (r *DataProductRepo) ReplaceOutputs(ctx context.Context, productVersionID string, outputs []domain.ProductOutput) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return mapDBError(err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM product_outputs WHERE product_version_id = ?`, productVersionID); err != nil {
		return mapDBError(err)
	}
	for i := range outputs {
		outputID := outputs[i].ID
		if outputID == "" {
			outputID = newID()
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO product_outputs (id, product_version_id, asset_id, is_primary)
			VALUES (?, ?, ?, ?)
		`, outputID, productVersionID, outputs[i].AssetID, boolToInt(outputs[i].IsPrimary)); err != nil {
			return mapDBError(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return mapDBError(err)
	}
	return nil
}

// AddSemanticEntrypoint links a version to a semantic model entrypoint.
func (r *DataProductRepo) AddSemanticEntrypoint(ctx context.Context, entrypoint *domain.ProductSemanticEntrypoint) error {
	if entrypoint == nil {
		return domain.ErrValidation("semantic entrypoint is required")
	}
	id := entrypoint.ID
	if id == "" {
		id = newID()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO product_semantic_entrypoints (id, product_version_id, semantic_model_id)
		VALUES (?, ?, ?)
	`, id, entrypoint.ProductVersionID, entrypoint.SemanticModelID)
	if err != nil {
		return mapDBError(err)
	}
	return nil
}

// ListSemanticEntrypoints returns semantic entrypoints for a version.
func (r *DataProductRepo) ListSemanticEntrypoints(ctx context.Context, productVersionID string) ([]domain.ProductSemanticEntrypoint, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT pse.id, pse.product_version_id, pse.semantic_model_id, sm.project_name, sm.name, pse.created_at
		FROM product_semantic_entrypoints pse
		INNER JOIN semantic_models sm ON sm.id = pse.semantic_model_id
		WHERE pse.product_version_id = ?
		ORDER BY sm.project_name ASC, sm.name ASC
	`, productVersionID)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]domain.ProductSemanticEntrypoint, 0)
	for rows.Next() {
		item, scanErr := scanProductSemanticEntrypoint(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ReplaceSemanticEntrypoints replaces semantic entrypoints for a version.
func (r *DataProductRepo) ReplaceSemanticEntrypoints(ctx context.Context, productVersionID string, entrypoints []domain.ProductSemanticEntrypoint) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return mapDBError(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM product_semantic_entrypoints WHERE product_version_id = ?`, productVersionID); err != nil {
		return mapDBError(err)
	}
	for i := range entrypoints {
		entrypointID := entrypoints[i].ID
		if entrypointID == "" {
			entrypointID = newID()
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO product_semantic_entrypoints (id, product_version_id, semantic_model_id)
			VALUES (?, ?, ?)
		`, entrypointID, productVersionID, entrypoints[i].SemanticModelID); err != nil {
			return mapDBError(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return mapDBError(err)
	}
	return nil
}

// AddDependency creates a product dependency edge.
func (r *DataProductRepo) AddDependency(ctx context.Context, dependency *domain.ProductDependency) error {
	if dependency == nil {
		return domain.ErrValidation("dependency is required")
	}
	id := dependency.ID
	if id == "" {
		id = newID()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO product_dependencies (id, product_id, depends_on_product_id)
		VALUES (?, ?, ?)
	`, id, dependency.ProductID, dependency.DependsOnProductID)
	if err != nil {
		return mapDBError(err)
	}
	return nil
}

// ListDependencies returns products referenced by a product dependency edge.
func (r *DataProductRepo) ListDependencies(ctx context.Context, productID string) ([]domain.DataProductListItem, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			p.id, p.slug, p.name, p.description, p.domain_id, p.owner_team_id, p.steward_principal,
			p.contact_channel, p.visibility, p.consumer_audience, p.docs_url, p.access_request_path,
			p.business_definitions_json, p.contract_json, p.slo_json, p.publication_intent,
			p.created_by, p.created_at, p.updated_at,
			d.id, d.name, d.description, d.created_at, d.updated_at,
			t.id, t.domain_id, t.name, t.contact_channel, t.created_at, t.updated_at,
			s.publication_state, s.certification_state, s.freshness_status, s.quality_status,
			s.last_successful_update_at, s.failing_checks_count, s.lineage_coverage,
			s.adoption_metrics_json, s.open_warnings_json, s.replacement_product_id, s.updated_at,
			v.id, v.product_id, v.producing_build_id, v.version, v.release_state, v.compatibility_level,
			v.contract_json, v.slo_json, v.docs_url, v.access_request_path, v.created_by, v.created_at,
			o.id, o.product_version_id, o.asset_id, a.asset_key, a.asset_type, o.is_primary, o.created_at
		FROM product_dependencies pd
		INNER JOIN data_products p ON p.id = pd.depends_on_product_id
		INNER JOIN domains d ON d.id = p.domain_id
		INNER JOIN teams t ON t.id = p.owner_team_id
		LEFT JOIN data_product_status s ON s.product_id = p.id
		LEFT JOIN data_product_versions v ON v.id = (
			SELECT id FROM data_product_versions WHERE product_id = p.id ORDER BY version DESC LIMIT 1
		)
		LEFT JOIN product_outputs o ON o.product_version_id = v.id AND o.is_primary = 1
		LEFT JOIN data_assets a ON a.id = o.asset_id
		WHERE pd.product_id = ?
		ORDER BY p.name ASC
	`, productID)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.DataProductListItem, 0)
	for rows.Next() {
		item, scanErr := scanProductListItem(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// AddSubscription creates a product event subscription.
func (r *DataProductRepo) AddSubscription(ctx context.Context, subscription *domain.ProductSubscription) (*domain.ProductSubscription, error) {
	if subscription == nil {
		return nil, domain.ErrValidation("subscription is required")
	}
	id := subscription.ID
	if id == "" {
		id = newID()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO product_subscriptions (id, product_id, principal_name, event_type, channel)
		VALUES (?, ?, ?, ?, ?)
	`, id, subscription.ProductID, strings.TrimSpace(subscription.PrincipalName), strings.TrimSpace(subscription.EventType), defaultProductString(subscription.Channel, "inbox"))
	if err != nil {
		return nil, mapDBError(err)
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT id, product_id, principal_name, event_type, channel, created_at
		FROM product_subscriptions
		WHERE id = ?
	`, id)
	item, err := scanProductSubscription(row)
	if err != nil {
		return nil, mapDBError(err)
	}
	return item, nil
}

// ListSubscriptions returns subscriptions for a product.
func (r *DataProductRepo) ListSubscriptions(ctx context.Context, productID string) ([]domain.ProductSubscription, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, product_id, principal_name, event_type, channel, created_at
		FROM product_subscriptions
		WHERE product_id = ?
		ORDER BY created_at DESC
	`, productID)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.ProductSubscription, 0)
	for rows.Next() {
		item, scanErr := scanProductSubscription(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// AddEvent persists a durable product event.
func (r *DataProductRepo) AddEvent(ctx context.Context, event *domain.ProductEvent) (*domain.ProductEvent, error) {
	if event == nil {
		return nil, domain.ErrValidation("event is required")
	}
	id := event.ID
	if id == "" {
		id = newID()
	}
	metadataJSON, err := json.Marshal(event.Metadata)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata_json: %w", err)
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO product_events (id, product_id, event_type, title, description, metadata_json)
		VALUES (?, ?, ?, ?, ?, ?)
	`, id, event.ProductID, strings.TrimSpace(event.EventType), strings.TrimSpace(event.Title), strings.TrimSpace(event.Description), string(metadataJSON))
	if err != nil {
		return nil, mapDBError(err)
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT id, product_id, event_type, title, description, metadata_json, created_at
		FROM product_events
		WHERE id = ?
	`, id)
	item, err := scanProductEvent(row)
	if err != nil {
		return nil, mapDBError(err)
	}
	return item, nil
}

// ListEvents returns product events ordered newest-first.
func (r *DataProductRepo) ListEvents(ctx context.Context, productID string, page domain.PageRequest) ([]domain.ProductEvent, int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM product_events WHERE product_id = ?`, productID).Scan(&total); err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, product_id, event_type, title, description, metadata_json, created_at
		FROM product_events
		WHERE product_id = ?
		ORDER BY created_at DESC
		LIMIT ? OFFSET ?
	`, productID, page.Limit(), page.Offset())
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.ProductEvent, 0)
	for rows.Next() {
		item, scanErr := scanProductEvent(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		out = append(out, *item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

// CountDependents returns how many downstream products depend on the given product.
func (r *DataProductRepo) CountDependents(ctx context.Context, productID string) (int64, error) {
	var total int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM product_dependencies WHERE depends_on_product_id = ?`, productID).Scan(&total); err != nil {
		return 0, mapDBError(err)
	}
	return total, nil
}

// ListOrphanAssets returns runtime assets that are not attached to any product.
func (r *DataProductRepo) ListOrphanAssets(ctx context.Context) ([]domain.OrphanResource, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT 'asset', id, asset_key
		FROM data_assets
		WHERE product_id IS NULL
		ORDER BY asset_key ASC
	`)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.OrphanResource, 0)
	for rows.Next() {
		var item domain.OrphanResource
		if scanErr := rows.Scan(&item.ResourceType, &item.ResourceID, &item.ResourceName); scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ListOrphanSemanticModels returns semantic models not linked to any product entrypoint.
func (r *DataProductRepo) ListOrphanSemanticModels(ctx context.Context) ([]domain.OrphanResource, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT 'semantic_model', sm.id, sm.project_name || '.' || sm.name
		FROM semantic_models sm
		LEFT JOIN product_semantic_entrypoints pse ON pse.semantic_model_id = sm.id
		WHERE pse.id IS NULL
		ORDER BY sm.project_name ASC, sm.name ASC
	`)
	if err != nil {
		return nil, mapDBError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]domain.OrphanResource, 0)
	for rows.Next() {
		var item domain.OrphanResource
		if scanErr := rows.Scan(&item.ResourceType, &item.ResourceID, &item.ResourceName); scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// GetByAssetID resolves the owning product for an asset.
func (r *DataProductRepo) GetByAssetID(ctx context.Context, assetID string) (*domain.DataProductListItem, error) {
	item, err := r.getProductListItem(ctx, `
		SELECT
			p.id, p.slug, p.name, p.description, p.domain_id, p.owner_team_id, p.steward_principal,
			p.contact_channel, p.visibility, p.consumer_audience, p.docs_url, p.access_request_path,
			p.business_definitions_json, p.contract_json, p.slo_json, p.publication_intent,
			p.created_by, p.created_at, p.updated_at,
			d.id, d.name, d.description, d.created_at, d.updated_at,
			t.id, t.domain_id, t.name, t.contact_channel, t.created_at, t.updated_at,
			s.publication_state, s.certification_state, s.freshness_status, s.quality_status,
			s.last_successful_update_at, s.failing_checks_count, s.lineage_coverage,
			s.adoption_metrics_json, s.open_warnings_json, s.replacement_product_id, s.updated_at,
			v.id, v.product_id, v.version, v.release_state, v.compatibility_level,
			v.contract_json, v.slo_json, v.docs_url, v.access_request_path, v.created_by, v.created_at,
			o.id, o.product_version_id, o.asset_id, a.asset_key, a.asset_type, o.is_primary, o.created_at
		FROM product_outputs o
		INNER JOIN data_product_versions v ON v.id = o.product_version_id
		INNER JOIN data_products p ON p.id = v.product_id
		INNER JOIN domains d ON d.id = p.domain_id
		INNER JOIN teams t ON t.id = p.owner_team_id
		INNER JOIN data_assets a ON a.id = o.asset_id
		LEFT JOIN data_product_status s ON s.product_id = p.id
		WHERE o.asset_id = ?
		ORDER BY v.version DESC, o.is_primary DESC
		LIMIT 1
	`, assetID)
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (r *DataProductRepo) getProductByID(ctx context.Context, id string) (*domain.DataProduct, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, slug, name, description, domain_id, owner_team_id, steward_principal,
		       contact_channel, visibility, consumer_audience, docs_url, access_request_path,
		       business_definitions_json, contract_json, slo_json, publication_intent,
		       created_by, created_at, updated_at
		FROM data_products
		WHERE id = ?
	`, id)
	item, err := scanProduct(row)
	if err != nil {
		return nil, mapDBError(err)
	}
	return item, nil
}

func (r *DataProductRepo) getVersionByID(ctx context.Context, id string) (*domain.DataProductVersion, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, product_id, producing_build_id, version, release_state, compatibility_level,
		       contract_json, slo_json, docs_url, access_request_path, created_by, created_at
		FROM data_product_versions
		WHERE id = ?
	`, id)
	item, err := scanProductVersion(row)
	if err != nil {
		return nil, mapDBError(err)
	}
	return item, nil
}

func (r *DataProductRepo) getProductWithJoins(ctx context.Context, query string, args ...any) (*domain.DataProductDetail, error) {
	row := r.db.QueryRowContext(ctx, query, args...)
	item, err := scanProductDetail(row)
	if err != nil {
		return nil, mapDBError(err)
	}
	return item, nil
}

func (r *DataProductRepo) getProductListItem(ctx context.Context, query string, args ...any) (*domain.DataProductListItem, error) {
	row := r.db.QueryRowContext(ctx, query, args...)
	item, err := scanProductListItem(row)
	if err != nil {
		return nil, mapDBError(err)
	}
	return item, nil
}

func scanDomain(scanner interface{ Scan(dest ...any) error }) (*domain.Domain, error) {
	var item domain.Domain
	if err := scanner.Scan(&item.ID, &item.Name, &item.Description, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return nil, err
	}
	return &item, nil
}

func scanTeam(scanner interface{ Scan(dest ...any) error }) (*domain.Team, error) {
	var item domain.Team
	if err := scanner.Scan(&item.ID, &item.DomainID, &item.Name, &item.ContactChannel, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return nil, err
	}
	return &item, nil
}

func scanProduct(scanner interface{ Scan(dest ...any) error }) (*domain.DataProduct, error) {
	var (
		item            domain.DataProduct
		definitionsJSON string
		contractJSON    string
		sloJSON         string
	)
	if err := scanner.Scan(
		&item.ID, &item.Slug, &item.Name, &item.Description, &item.DomainID, &item.OwnerTeamID, &item.StewardPrincipal,
		&item.ContactChannel, &item.Visibility, &item.ConsumerAudience, &item.DocsURL, &item.AccessRequestPath,
		&definitionsJSON, &contractJSON, &sloJSON, &item.PublicationIntent,
		&item.CreatedBy, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(definitionsJSON), &item.BusinessDefinitions); err != nil {
		return nil, fmt.Errorf("unmarshal business_definitions_json: %w", err)
	}
	if err := json.Unmarshal([]byte(contractJSON), &item.Contract); err != nil {
		return nil, fmt.Errorf("unmarshal contract_json: %w", err)
	}
	if err := json.Unmarshal([]byte(sloJSON), &item.SLO); err != nil {
		return nil, fmt.Errorf("unmarshal slo_json: %w", err)
	}
	return &item, nil
}

func scanProductVersion(scanner interface{ Scan(dest ...any) error }) (*domain.DataProductVersion, error) {
	var (
		item             domain.DataProductVersion
		producingBuildID sql.NullString
		contractJSON     string
		sloJSON          string
	)
	if err := scanner.Scan(
		&item.ID, &item.ProductID, &producingBuildID, &item.Version, &item.ReleaseState, &item.CompatibilityLevel,
		&contractJSON, &sloJSON, &item.DocsURL, &item.AccessRequestPath, &item.CreatedBy, &item.CreatedAt,
	); err != nil {
		return nil, err
	}
	if producingBuildID.Valid {
		item.ProducingBuildID = &producingBuildID.String
	}
	if err := json.Unmarshal([]byte(contractJSON), &item.Contract); err != nil {
		return nil, fmt.Errorf("unmarshal product version contract_json: %w", err)
	}
	if err := json.Unmarshal([]byte(sloJSON), &item.SLO); err != nil {
		return nil, fmt.Errorf("unmarshal product version slo_json: %w", err)
	}
	return &item, nil
}

func scanProductStatus(productID string, scanner interface{ Scan(dest ...any) error }) (*domain.DataProductStatus, error) {
	var (
		item               = domain.DataProductStatus{ProductID: productID}
		lastSuccess        sql.NullTime
		lineageCoverage    sql.NullFloat64
		adoptionJSON       string
		warningsJSON       string
		replacementProduct sql.NullString
	)
	if err := scanner.Scan(
		&item.PublicationState, &item.CertificationState, &item.FreshnessStatus, &item.QualityStatus,
		&lastSuccess, &item.FailingChecksCount, &lineageCoverage,
		&adoptionJSON, &warningsJSON, &replacementProduct, &item.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if lastSuccess.Valid {
		item.LastSuccessfulUpdateAt = &lastSuccess.Time
	}
	if lineageCoverage.Valid {
		item.LineageCoverage = &lineageCoverage.Float64
	}
	if replacementProduct.Valid {
		item.ReplacementProductID = &replacementProduct.String
	}
	if err := json.Unmarshal([]byte(adoptionJSON), &item.AdoptionMetrics); err != nil {
		return nil, fmt.Errorf("unmarshal adoption_metrics_json: %w", err)
	}
	if err := json.Unmarshal([]byte(warningsJSON), &item.OpenWarnings); err != nil {
		return nil, fmt.Errorf("unmarshal open_warnings_json: %w", err)
	}
	return &item, nil
}

func scanProductOutput(scanner interface{ Scan(dest ...any) error }) (*domain.ProductOutput, error) {
	var (
		item      domain.ProductOutput
		isPrimary int64
	)
	if err := scanner.Scan(&item.ID, &item.ProductVersionID, &item.AssetID, &item.AssetKey, &item.AssetType, &isPrimary, &item.CreatedAt); err != nil {
		return nil, err
	}
	item.IsPrimary = isPrimary != 0
	return &item, nil
}

func scanProductSubscription(scanner interface{ Scan(dest ...any) error }) (*domain.ProductSubscription, error) {
	var item domain.ProductSubscription
	if err := scanner.Scan(&item.ID, &item.ProductID, &item.PrincipalName, &item.EventType, &item.Channel, &item.CreatedAt); err != nil {
		return nil, err
	}
	return &item, nil
}

func scanProductEvent(scanner interface{ Scan(dest ...any) error }) (*domain.ProductEvent, error) {
	var (
		item         domain.ProductEvent
		metadataJSON string
	)
	if err := scanner.Scan(&item.ID, &item.ProductID, &item.EventType, &item.Title, &item.Description, &metadataJSON, &item.CreatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(metadataJSON), &item.Metadata); err != nil {
		return nil, fmt.Errorf("unmarshal product event metadata_json: %w", err)
	}
	return &item, nil
}

func scanProductSemanticEntrypoint(scanner interface{ Scan(dest ...any) error }) (*domain.ProductSemanticEntrypoint, error) {
	var item domain.ProductSemanticEntrypoint
	if err := scanner.Scan(&item.ID, &item.ProductVersionID, &item.SemanticModelID, &item.ProjectName, &item.ModelName, &item.CreatedAt); err != nil {
		return nil, err
	}
	return &item, nil
}

func scanProductDetail(scanner interface{ Scan(dest ...any) error }) (*domain.DataProductDetail, error) {
	var (
		product         = domain.DataProduct{}
		d               = domain.Domain{}
		t               = domain.Team{}
		definitionsJSON string
		contractJSON    string
		sloJSON         string
		statusPub       sql.NullString
		statusCert      sql.NullString
		statusFresh     sql.NullString
		statusQuality   sql.NullString
		lastSuccess     sql.NullTime
		failingChecks   sql.NullInt64
		lineageCoverage sql.NullFloat64
		adoptionJSON    sql.NullString
		warningsJSON    sql.NullString
		replacementID   sql.NullString
		statusUpdatedAt sql.NullTime
	)
	if err := scanner.Scan(
		&product.ID, &product.Slug, &product.Name, &product.Description, &product.DomainID, &product.OwnerTeamID, &product.StewardPrincipal,
		&product.ContactChannel, &product.Visibility, &product.ConsumerAudience, &product.DocsURL, &product.AccessRequestPath,
		&definitionsJSON, &contractJSON, &sloJSON, &product.PublicationIntent,
		&product.CreatedBy, &product.CreatedAt, &product.UpdatedAt,
		&d.ID, &d.Name, &d.Description, &d.CreatedAt, &d.UpdatedAt,
		&t.ID, &t.DomainID, &t.Name, &t.ContactChannel, &t.CreatedAt, &t.UpdatedAt,
		&statusPub, &statusCert, &statusFresh, &statusQuality,
		&lastSuccess, &failingChecks, &lineageCoverage,
		&adoptionJSON, &warningsJSON, &replacementID, &statusUpdatedAt,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(definitionsJSON), &product.BusinessDefinitions); err != nil {
		return nil, fmt.Errorf("unmarshal business_definitions_json: %w", err)
	}
	if err := json.Unmarshal([]byte(contractJSON), &product.Contract); err != nil {
		return nil, fmt.Errorf("unmarshal contract_json: %w", err)
	}
	if err := json.Unmarshal([]byte(sloJSON), &product.SLO); err != nil {
		return nil, fmt.Errorf("unmarshal slo_json: %w", err)
	}
	item := &domain.DataProductDetail{
		Product:             product,
		Domain:              d,
		OwnerTeam:           t,
		Versions:            []domain.DataProductVersion{},
		Outputs:             []domain.ProductOutput{},
		SemanticEntrypoints: []domain.ProductSemanticEntrypoint{},
		Events:              []domain.ProductEvent{},
	}
	if statusPub.Valid || statusCert.Valid || statusFresh.Valid || statusQuality.Valid {
		status := &domain.DataProductStatus{
			ProductID:          product.ID,
			PublicationState:   statusPub.String,
			CertificationState: statusCert.String,
			FreshnessStatus:    statusFresh.String,
			QualityStatus:      statusQuality.String,
			AdoptionMetrics:    map[string]any{},
			OpenWarnings:       []string{},
		}
		if lastSuccess.Valid {
			status.LastSuccessfulUpdateAt = &lastSuccess.Time
		}
		if failingChecks.Valid {
			status.FailingChecksCount = failingChecks.Int64
		}
		if lineageCoverage.Valid {
			status.LineageCoverage = &lineageCoverage.Float64
		}
		if replacementID.Valid {
			status.ReplacementProductID = &replacementID.String
		}
		if statusUpdatedAt.Valid {
			status.UpdatedAt = statusUpdatedAt.Time
		}
		if adoptionJSON.Valid && strings.TrimSpace(adoptionJSON.String) != "" {
			if err := json.Unmarshal([]byte(adoptionJSON.String), &status.AdoptionMetrics); err != nil {
				return nil, fmt.Errorf("unmarshal adoption_metrics_json: %w", err)
			}
		}
		if warningsJSON.Valid && strings.TrimSpace(warningsJSON.String) != "" {
			if err := json.Unmarshal([]byte(warningsJSON.String), &status.OpenWarnings); err != nil {
				return nil, fmt.Errorf("unmarshal open_warnings_json: %w", err)
			}
		}
		item.Status = status
	}
	return item, nil
}

func scanProductListItem(scanner interface{ Scan(dest ...any) error }) (*domain.DataProductListItem, error) {
	var (
		product           = domain.DataProduct{}
		d                 = domain.Domain{}
		t                 = domain.Team{}
		definitionsJSON   string
		contractJSON      string
		sloJSON           string
		statusPub         sql.NullString
		statusCert        sql.NullString
		statusFresh       sql.NullString
		statusQuality     sql.NullString
		lastSuccess       sql.NullTime
		failingChecks     sql.NullInt64
		lineageCoverage   sql.NullFloat64
		adoptionJSON      sql.NullString
		warningsJSON      sql.NullString
		replacementID     sql.NullString
		statusUpdatedAt   sql.NullTime
		versionID         sql.NullString
		versionProductID  sql.NullString
		versionBuildID    sql.NullString
		versionNumber     sql.NullInt64
		releaseState      sql.NullString
		compatibility     sql.NullString
		versionContract   sql.NullString
		versionSLO        sql.NullString
		versionDocsURL    sql.NullString
		versionAccessPath sql.NullString
		versionCreatedBy  sql.NullString
		versionCreatedAt  sql.NullTime
		outputID          sql.NullString
		outputVersionID   sql.NullString
		outputAssetID     sql.NullString
		outputAssetKey    sql.NullString
		outputAssetType   sql.NullString
		outputIsPrimary   sql.NullInt64
		outputCreatedAt   sql.NullTime
	)
	if err := scanner.Scan(
		&product.ID, &product.Slug, &product.Name, &product.Description, &product.DomainID, &product.OwnerTeamID, &product.StewardPrincipal,
		&product.ContactChannel, &product.Visibility, &product.ConsumerAudience, &product.DocsURL, &product.AccessRequestPath,
		&definitionsJSON, &contractJSON, &sloJSON, &product.PublicationIntent,
		&product.CreatedBy, &product.CreatedAt, &product.UpdatedAt,
		&d.ID, &d.Name, &d.Description, &d.CreatedAt, &d.UpdatedAt,
		&t.ID, &t.DomainID, &t.Name, &t.ContactChannel, &t.CreatedAt, &t.UpdatedAt,
		&statusPub, &statusCert, &statusFresh, &statusQuality,
		&lastSuccess, &failingChecks, &lineageCoverage,
		&adoptionJSON, &warningsJSON, &replacementID, &statusUpdatedAt,
		&versionID, &versionProductID, &versionBuildID, &versionNumber, &releaseState, &compatibility,
		&versionContract, &versionSLO, &versionDocsURL, &versionAccessPath, &versionCreatedBy, &versionCreatedAt,
		&outputID, &outputVersionID, &outputAssetID, &outputAssetKey, &outputAssetType, &outputIsPrimary, &outputCreatedAt,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(definitionsJSON), &product.BusinessDefinitions); err != nil {
		return nil, fmt.Errorf("unmarshal business_definitions_json: %w", err)
	}
	if err := json.Unmarshal([]byte(contractJSON), &product.Contract); err != nil {
		return nil, fmt.Errorf("unmarshal contract_json: %w", err)
	}
	if err := json.Unmarshal([]byte(sloJSON), &product.SLO); err != nil {
		return nil, fmt.Errorf("unmarshal slo_json: %w", err)
	}
	item := &domain.DataProductListItem{
		Product:   product,
		Domain:    d,
		OwnerTeam: t,
	}
	if versionID.Valid {
		version := &domain.DataProductVersion{
			ID:                 versionID.String,
			ProductID:          versionProductID.String,
			Version:            int(versionNumber.Int64),
			ReleaseState:       releaseState.String,
			CompatibilityLevel: compatibility.String,
			DocsURL:            versionDocsURL.String,
			AccessRequestPath:  versionAccessPath.String,
			CreatedBy:          versionCreatedBy.String,
		}
		if versionBuildID.Valid {
			version.ProducingBuildID = &versionBuildID.String
		}
		if versionCreatedAt.Valid {
			version.CreatedAt = versionCreatedAt.Time
		}
		if versionContract.Valid && strings.TrimSpace(versionContract.String) != "" {
			if err := json.Unmarshal([]byte(versionContract.String), &version.Contract); err != nil {
				return nil, fmt.Errorf("unmarshal latest version contract_json: %w", err)
			}
		}
		if versionSLO.Valid && strings.TrimSpace(versionSLO.String) != "" {
			if err := json.Unmarshal([]byte(versionSLO.String), &version.SLO); err != nil {
				return nil, fmt.Errorf("unmarshal latest version slo_json: %w", err)
			}
		}
		item.LatestVersion = version
	}
	if statusPub.Valid || statusCert.Valid || statusFresh.Valid || statusQuality.Valid {
		status := &domain.DataProductStatus{
			ProductID:          product.ID,
			PublicationState:   statusPub.String,
			CertificationState: statusCert.String,
			FreshnessStatus:    statusFresh.String,
			QualityStatus:      statusQuality.String,
			AdoptionMetrics:    map[string]any{},
			OpenWarnings:       []string{},
		}
		if lastSuccess.Valid {
			status.LastSuccessfulUpdateAt = &lastSuccess.Time
		}
		if failingChecks.Valid {
			status.FailingChecksCount = failingChecks.Int64
		}
		if lineageCoverage.Valid {
			status.LineageCoverage = &lineageCoverage.Float64
		}
		if replacementID.Valid {
			status.ReplacementProductID = &replacementID.String
		}
		if statusUpdatedAt.Valid {
			status.UpdatedAt = statusUpdatedAt.Time
		}
		if adoptionJSON.Valid && strings.TrimSpace(adoptionJSON.String) != "" {
			if err := json.Unmarshal([]byte(adoptionJSON.String), &status.AdoptionMetrics); err != nil {
				return nil, fmt.Errorf("unmarshal adoption_metrics_json: %w", err)
			}
		}
		if warningsJSON.Valid && strings.TrimSpace(warningsJSON.String) != "" {
			if err := json.Unmarshal([]byte(warningsJSON.String), &status.OpenWarnings); err != nil {
				return nil, fmt.Errorf("unmarshal open_warnings_json: %w", err)
			}
		}
		item.Status = status
	}
	if outputID.Valid {
		item.PrimaryOutput = &domain.ProductOutput{
			ID:               outputID.String,
			ProductVersionID: outputVersionID.String,
			AssetID:          outputAssetID.String,
			AssetKey:         outputAssetKey.String,
			AssetType:        outputAssetType.String,
			IsPrimary:        outputIsPrimary.Int64 != 0,
		}
		if outputCreatedAt.Valid {
			item.PrimaryOutput.CreatedAt = outputCreatedAt.Time
		}
	}
	return item, nil
}

func defaultProductString(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func buildDataProductListWhere(filter domain.DataProductFilter) (string, []any) {
	clauses := []string{"WHERE 1=1"}
	args := make([]any, 0)
	if filter.Query != nil && strings.TrimSpace(*filter.Query) != "" {
		query := "%" + strings.ToLower(strings.TrimSpace(*filter.Query)) + "%"
		clauses = append(clauses, `AND (
			LOWER(p.slug) LIKE ? OR
			LOWER(p.name) LIKE ? OR
			LOWER(p.description) LIKE ? OR
			LOWER(d.name) LIKE ? OR
			LOWER(t.name) LIKE ? OR
			LOWER(p.steward_principal) LIKE ?
		)`)
		for i := 0; i < 6; i++ {
			args = append(args, query)
		}
	}
	if filter.DomainName != nil && strings.TrimSpace(*filter.DomainName) != "" {
		clauses = append(clauses, "AND d.name = ?")
		args = append(args, strings.TrimSpace(*filter.DomainName))
	}
	if filter.TeamName != nil && strings.TrimSpace(*filter.TeamName) != "" {
		clauses = append(clauses, "AND t.name = ?")
		args = append(args, strings.TrimSpace(*filter.TeamName))
	}
	if filter.PublicationState != nil && strings.TrimSpace(*filter.PublicationState) != "" {
		clauses = append(clauses, "AND COALESCE(s.publication_state, p.publication_intent) = ?")
		args = append(args, strings.TrimSpace(*filter.PublicationState))
	}
	if filter.CertificationState != nil && strings.TrimSpace(*filter.CertificationState) != "" {
		clauses = append(clauses, "AND COALESCE(s.certification_state, 'DRAFT') = ?")
		args = append(args, strings.TrimSpace(*filter.CertificationState))
	}
	if filter.FreshnessState != nil && strings.TrimSpace(*filter.FreshnessState) != "" {
		clauses = append(clauses, "AND COALESCE(s.freshness_status, 'UNKNOWN') = ?")
		args = append(args, strings.TrimSpace(*filter.FreshnessState))
	}
	return "\n" + strings.Join(clauses, "\n"), args
}
