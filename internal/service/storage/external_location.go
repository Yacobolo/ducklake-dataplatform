package storage

import (
	"context"
	"fmt"
	"log/slog"

	"duck-demo/internal/domain"
)

// ExternalLocationService provides CRUD operations for external locations
// with RBAC enforcement and DuckDB secret management.
// Catalog attachment is now handled by CatalogRegistrationService.
type ExternalLocationService struct {
	locRepo  domain.ExternalLocationRepository
	credRepo domain.StorageCredentialRepository
	auth     domain.AuthorizationService
	audit    domain.AuditRepository
	secrets  domain.SecretManager
	logger   *slog.Logger
}

// NewExternalLocationService creates a new ExternalLocationService.
func NewExternalLocationService(
	locRepo domain.ExternalLocationRepository,
	credRepo domain.StorageCredentialRepository,
	auth domain.AuthorizationService,
	audit domain.AuditRepository,
	secrets domain.SecretManager,
	logger *slog.Logger,
) *ExternalLocationService {
	return &ExternalLocationService{
		locRepo:  locRepo,
		credRepo: credRepo,
		auth:     auth,
		audit:    audit,
		secrets:  secrets,
		logger:   logger,
	}
}

// Create validates and persists a new external location, creates a DuckDB
// secret for the associated credential, and attaches the DuckLake catalog
// if this is the first location.
// Requires CREATE_EXTERNAL_LOCATION on catalog.
func (s *ExternalLocationService) Create(ctx context.Context, principal string, req domain.CreateExternalLocationRequest) (*domain.ExternalLocation, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivCreateExternalLocation); err != nil {
		return nil, err
	}

	if err := domain.ValidateExternalLocationRequest(req); err != nil {
		return nil, err
	}

	// Verify the referenced credential exists
	cred, err := s.credRepo.GetByName(ctx, req.CredentialName)
	if err != nil {
		return nil, fmt.Errorf("lookup credential %q: %w", req.CredentialName, err)
	}

	// Default storage type
	if req.StorageType == "" {
		req.StorageType = domain.StorageTypeS3
	}

	loc := &domain.ExternalLocation{
		Name:           req.Name,
		URL:            req.URL,
		CredentialName: req.CredentialName,
		StorageType:    req.StorageType,
		Comment:        req.Comment,
		Owner:          principal,
		ReadOnly:       req.ReadOnly,
	}

	// Persist to SQLite
	result, err := s.locRepo.Create(ctx, loc)
	if err != nil {
		return nil, fmt.Errorf("create external location: %w", err)
	}

	// Create DuckDB secret for the credential
	secretName := "cred_" + cred.Name
	if err := s.createDuckDBSecret(ctx, secretName, cred); err != nil {
		// Rollback: delete the location we just persisted
		_ = s.locRepo.Delete(ctx, result.ID)
		return nil, fmt.Errorf("create DuckDB secret for credential %q: %w", cred.Name, err)
	}

	s.logAudit(ctx, principal, "CREATE_EXTERNAL_LOCATION", fmt.Sprintf("Created location %q -> %s", req.Name, req.URL))
	return result, nil
}

// GetByName returns an external location by name.
func (s *ExternalLocationService) GetByName(ctx context.Context, name string) (*domain.ExternalLocation, error) {
	return s.locRepo.GetByName(ctx, name)
}

// List returns a paginated list of external locations.
func (s *ExternalLocationService) List(ctx context.Context, page domain.PageRequest) ([]domain.ExternalLocation, int64, error) {
	return s.locRepo.List(ctx, page)
}

// Update updates an external location by name.
// Requires CREATE_EXTERNAL_LOCATION on catalog.
func (s *ExternalLocationService) Update(ctx context.Context, principal string, name string, req domain.UpdateExternalLocationRequest) (*domain.ExternalLocation, error) {
	if err := s.requirePrivilege(ctx, principal, domain.PrivCreateExternalLocation); err != nil {
		return nil, err
	}

	existing, err := s.locRepo.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableExternalLocation, existing.ID, domain.PrivCreateExternalLocation)
	if err != nil {
		return nil, fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "UPDATE_EXTERNAL_LOCATION", fmt.Sprintf("Denied update location %q", name))
		return nil, domain.ErrAccessDenied("%q lacks %s on external location %q", principal, domain.PrivCreateExternalLocation, name)
	}

	result, err := s.locRepo.Update(ctx, existing.ID, req)
	if err != nil {
		return nil, fmt.Errorf("update external location: %w", err)
	}

	s.logAudit(ctx, principal, "UPDATE_EXTERNAL_LOCATION", fmt.Sprintf("Updated location %q", name))
	return result, nil
}

// Delete removes an external location and its associated DuckDB secret.
// Requires CREATE_EXTERNAL_LOCATION on catalog.
func (s *ExternalLocationService) Delete(ctx context.Context, principal string, name string) error {
	if err := s.requirePrivilege(ctx, principal, domain.PrivCreateExternalLocation); err != nil {
		return err
	}

	existing, err := s.locRepo.GetByName(ctx, name)
	if err != nil {
		return err
	}

	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableExternalLocation, existing.ID, domain.PrivCreateExternalLocation)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		s.logAuditDenied(ctx, principal, "DELETE_EXTERNAL_LOCATION", fmt.Sprintf("Denied delete location %q", name))
		return domain.ErrAccessDenied("%q lacks %s on external location %q", principal, domain.PrivCreateExternalLocation, name)
	}

	// Drop the DuckDB secret for this location's credential
	secretName := "cred_" + existing.CredentialName
	if err := s.secrets.DropSecret(ctx, secretName); err != nil {
		s.logger.Warn("failed to drop secret", "secret", secretName, "error", err)
	}

	if err := s.locRepo.Delete(ctx, existing.ID); err != nil {
		return fmt.Errorf("delete external location: %w", err)
	}

	s.logAudit(ctx, principal, "DELETE_EXTERNAL_LOCATION", fmt.Sprintf("Deleted location %q", name))
	return nil
}

// RestoreSecrets recreates DuckDB secrets for all persisted storage credentials.
// Called at startup. Catalog attachment is now handled by CatalogRegistrationService.
func (s *ExternalLocationService) RestoreSecrets(ctx context.Context) error {
	// Recreate DuckDB secrets for all credentials
	creds, _, err := s.credRepo.List(ctx, domain.PageRequest{MaxResults: 1000})
	if err != nil {
		return fmt.Errorf("list storage credentials: %w", err)
	}

	if len(creds) == 0 {
		return nil
	}

	for _, cred := range creds {
		secretName := "cred_" + cred.Name
		if err := s.createDuckDBSecret(ctx, secretName, &cred); err != nil {
			s.logger.Warn("failed to restore secret", "secret", secretName, "error", err)
		}
	}

	s.logger.Info("restored credential secrets", "count", len(creds))
	return nil
}

// requirePrivilege checks that the principal has the given privilege on the catalog.
func (s *ExternalLocationService) requirePrivilege(ctx context.Context, principal string, privilege string) error {
	allowed, err := s.auth.CheckPrivilege(ctx, principal, domain.SecurableCatalog, domain.CatalogID, privilege)
	if err != nil {
		return fmt.Errorf("check privilege: %w", err)
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks %s on catalog", principal, privilege)
	}
	return nil
}

// createDuckDBSecret dispatches to the correct engine secret creator based on
// the credential's type (S3, Azure, or GCS).
func (s *ExternalLocationService) createDuckDBSecret(ctx context.Context, secretName string, cred *domain.StorageCredential) error {
	switch cred.CredentialType {
	case domain.CredentialTypeS3:
		return s.secrets.CreateS3Secret(ctx, secretName,
			cred.KeyID, cred.Secret, cred.Endpoint, cred.Region, cred.URLStyle)
	case domain.CredentialTypeAzure:
		// Build connection string if using account key (no service principal secret support in DuckDB yet)
		connectionString := ""
		if cred.AzureAccountKey != "" {
			connectionString = fmt.Sprintf("AccountName=%s;AccountKey=%s", cred.AzureAccountName, cred.AzureAccountKey)
		}
		return s.secrets.CreateAzureSecret(ctx, secretName,
			cred.AzureAccountName, cred.AzureAccountKey, connectionString)
	case domain.CredentialTypeGCS:
		return s.secrets.CreateGCSSecret(ctx, secretName, cred.GCSKeyFilePath)
	default:
		return fmt.Errorf("unsupported credential type %q", cred.CredentialType)
	}
}

func (s *ExternalLocationService) logAudit(ctx context.Context, principal, action, detail string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
		OriginalSQL:   &detail,
	})
}

func (s *ExternalLocationService) logAuditDenied(ctx context.Context, principal, action, detail string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "DENIED",
		OriginalSQL:   &detail,
	})
}
