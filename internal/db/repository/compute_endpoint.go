package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"

	"duck-demo/internal/db/crypto"
	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.ComputeEndpointRepository = (*ComputeEndpointRepo)(nil)

// ComputeEndpointRepo implements ComputeEndpointRepository with encrypted auth_token storage.
type ComputeEndpointRepo struct {
	q   *dbstore.Queries
	db  *sql.DB
	enc *crypto.Encryptor
}

// NewComputeEndpointRepo creates a new ComputeEndpointRepo.
func NewComputeEndpointRepo(db *sql.DB, enc *crypto.Encryptor) *ComputeEndpointRepo {
	return &ComputeEndpointRepo{q: dbstore.New(db), db: db, enc: enc}
}

// Create inserts a new compute endpoint with an encrypted auth_token.
func (r *ComputeEndpointRepo) Create(ctx context.Context, ep *domain.ComputeEndpoint) (*domain.ComputeEndpoint, error) {
	encToken, err := r.enc.Encrypt(ep.AuthToken)
	if err != nil {
		return nil, fmt.Errorf("encrypt auth_token: %w", err)
	}

	row, err := r.q.CreateComputeEndpoint(ctx, dbstore.CreateComputeEndpointParams{
		ID:          newID(),
		ExternalID:  uuid.New().String(),
		Name:        ep.Name,
		Url:         ep.URL,
		Type:        ep.Type,
		Size:        ep.Size,
		MaxMemoryGb: nullInt64Ptr(ep.MaxMemoryGB),
		AuthToken:   encToken,
		Owner:       ep.Owner,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.endpointFromDB(row)
}

// GetByID returns a compute endpoint by its ID, decrypting the auth_token.
func (r *ComputeEndpointRepo) GetByID(ctx context.Context, id string) (*domain.ComputeEndpoint, error) {
	row, err := r.q.GetComputeEndpoint(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.endpointFromDB(row)
}

// GetByName returns a compute endpoint by its name, decrypting the auth_token.
func (r *ComputeEndpointRepo) GetByName(ctx context.Context, name string) (*domain.ComputeEndpoint, error) {
	row, err := r.q.GetComputeEndpointByName(ctx, name)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.endpointFromDB(row)
}

// List returns a paginated list of compute endpoints.
func (r *ComputeEndpointRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.ComputeEndpoint, int64, error) {
	total, err := r.q.CountComputeEndpoints(ctx)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListComputeEndpoints(ctx, dbstore.ListComputeEndpointsParams{
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	eps := make([]domain.ComputeEndpoint, 0, len(rows))
	for _, row := range rows {
		ep, err := r.endpointFromDB(row)
		if err != nil {
			return nil, 0, err
		}
		eps = append(eps, *ep)
	}
	return eps, total, nil
}

// Update applies partial updates to a compute endpoint.
func (r *ComputeEndpointRepo) Update(ctx context.Context, id string, req domain.UpdateComputeEndpointRequest) (*domain.ComputeEndpoint, error) {
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	url := current.URL
	if req.URL != nil {
		url = *req.URL
	}
	size := current.Size
	if req.Size != nil {
		size = *req.Size
	}
	maxMem := current.MaxMemoryGB
	if req.MaxMemoryGB != nil {
		maxMem = req.MaxMemoryGB
	}
	authToken := current.AuthToken
	if req.AuthToken != nil {
		authToken = *req.AuthToken
	}

	encToken, err := r.enc.Encrypt(authToken)
	if err != nil {
		return nil, fmt.Errorf("encrypt auth_token: %w", err)
	}

	err = r.q.UpdateComputeEndpoint(ctx, dbstore.UpdateComputeEndpointParams{
		Url:         url,
		Size:        size,
		MaxMemoryGb: nullInt64Ptr(maxMem),
		AuthToken:   encToken,
		ID:          id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// Delete removes a compute endpoint by ID.
func (r *ComputeEndpointRepo) Delete(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteComputeEndpoint(ctx, id))
}

// UpdateStatus changes the status of a compute endpoint.
func (r *ComputeEndpointRepo) UpdateStatus(ctx context.Context, id string, status string) error {
	return mapDBError(r.q.UpdateComputeEndpointStatus(ctx, dbstore.UpdateComputeEndpointStatusParams{
		Status: status,
		ID:     id,
	}))
}

// Assign creates a compute assignment binding a principal to an endpoint.
func (r *ComputeEndpointRepo) Assign(ctx context.Context, a *domain.ComputeAssignment) (*domain.ComputeAssignment, error) {
	row, err := r.q.CreateComputeAssignment(ctx, dbstore.CreateComputeAssignmentParams{
		ID:            newID(),
		PrincipalID:   a.PrincipalID,
		PrincipalType: a.PrincipalType,
		EndpointID:    a.EndpointID,
		IsDefault:     boolToInt(a.IsDefault),
		FallbackLocal: boolToInt(a.FallbackLocal),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return assignmentFromDB(row), nil
}

// Unassign removes a compute assignment by ID.
func (r *ComputeEndpointRepo) Unassign(ctx context.Context, id string) error {
	return mapDBError(r.q.DeleteComputeAssignment(ctx, id))
}

// ListAssignments returns a paginated list of assignments for an endpoint.
func (r *ComputeEndpointRepo) ListAssignments(ctx context.Context, endpointID string, page domain.PageRequest) ([]domain.ComputeAssignment, int64, error) {
	total, err := r.q.CountAssignmentsForEndpoint(ctx, endpointID)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListAssignmentsForEndpoint(ctx, dbstore.ListAssignmentsForEndpointParams{
		EndpointID: endpointID,
		Limit:      int64(page.Limit()),
		Offset:     int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	assignments := make([]domain.ComputeAssignment, 0, len(rows))
	for _, row := range rows {
		assignments = append(assignments, *assignmentFromDB(row))
	}
	return assignments, total, nil
}

// GetDefaultForPrincipal returns the default active compute endpoint for a principal.
func (r *ComputeEndpointRepo) GetDefaultForPrincipal(ctx context.Context, principalID string, principalType string) (*domain.ComputeEndpoint, error) {
	row, err := r.q.GetDefaultEndpointForPrincipal(ctx, dbstore.GetDefaultEndpointForPrincipalParams{
		PrincipalID:   principalID,
		PrincipalType: principalType,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.endpointFromDB(row)
}

// GetAssignmentsForPrincipal returns all compute endpoints assigned to a principal.
func (r *ComputeEndpointRepo) GetAssignmentsForPrincipal(ctx context.Context, principalID string, principalType string) ([]domain.ComputeEndpoint, error) {
	rows, err := r.q.GetAssignmentsForPrincipal(ctx, dbstore.GetAssignmentsForPrincipalParams{
		PrincipalID:   principalID,
		PrincipalType: principalType,
	})
	if err != nil {
		return nil, mapDBError(err)
	}

	eps := make([]domain.ComputeEndpoint, 0, len(rows))
	for _, row := range rows {
		ep, err := r.endpointFromDB(row)
		if err != nil {
			return nil, err
		}
		eps = append(eps, *ep)
	}
	return eps, nil
}

// endpointFromDB decrypts the DB row into a domain ComputeEndpoint.
func (r *ComputeEndpointRepo) endpointFromDB(row dbstore.ComputeEndpoint) (*domain.ComputeEndpoint, error) {
	authToken, err := r.enc.Decrypt(row.AuthToken)
	if err != nil {
		return nil, fmt.Errorf("decrypt auth_token: %w", err)
	}

	var maxMem *int64
	if row.MaxMemoryGb.Valid {
		maxMem = &row.MaxMemoryGb.Int64
	}

	return &domain.ComputeEndpoint{
		ID:          row.ID,
		ExternalID:  row.ExternalID,
		Name:        row.Name,
		URL:         row.Url,
		Type:        row.Type,
		Status:      row.Status,
		Size:        row.Size,
		MaxMemoryGB: maxMem,
		AuthToken:   authToken,
		Owner:       row.Owner,
		CreatedAt:   row.CreatedAt,
		UpdatedAt:   row.UpdatedAt,
	}, nil
}

// assignmentFromDB converts a dbstore ComputeAssignment to a domain ComputeAssignment.
func assignmentFromDB(row dbstore.ComputeAssignment) *domain.ComputeAssignment {
	return &domain.ComputeAssignment{
		ID:            row.ID,
		PrincipalID:   row.PrincipalID,
		PrincipalType: row.PrincipalType,
		EndpointID:    row.EndpointID,
		IsDefault:     row.IsDefault == 1,
		FallbackLocal: row.FallbackLocal == 1,
		CreatedAt:     row.CreatedAt,
	}
}

// nullInt64Ptr converts a *int64 to sql.NullInt64.
func nullInt64Ptr(p *int64) sql.NullInt64 {
	if p == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: *p, Valid: true}
}
