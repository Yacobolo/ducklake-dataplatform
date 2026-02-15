package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"golang.org/x/sync/errgroup"

	"duck-demo/internal/ddl"
	"duck-demo/internal/domain"
	"duck-demo/internal/engine"
)

// CatalogRegistrationService manages the lifecycle of DuckLake catalog registrations:
// register, attach, detach, delete, and startup AttachAll.
//
//nolint:revive // Name chosen for clarity across package boundaries
type CatalogRegistrationService struct {
	repo               domain.CatalogRegistrationRepository
	attacher           domain.CatalogAttacher
	controlPlaneDBPath string // to enforce SQLite separation
	duckDB             *sql.DB
	logger             *slog.Logger

	// Factories to notify on catalog deletion (pool cleanup).
	metastoreFactory    domain.MetastoreQuerierFactory
	introspectionCloser func(catalogName string) error
	catalogRepoEvict    func(catalogName string)
}

// RegistrationServiceDeps holds dependencies for CatalogRegistrationService.
type RegistrationServiceDeps struct {
	Repo               domain.CatalogRegistrationRepository
	Attacher           domain.CatalogAttacher
	ControlPlaneDBPath string
	DuckDB             *sql.DB
	Logger             *slog.Logger
	MetastoreFactory   domain.MetastoreQuerierFactory
	IntrospectionClose func(catalogName string) error
	CatalogRepoEvict   func(catalogName string)
}

// NewCatalogRegistrationService creates a new CatalogRegistrationService.
func NewCatalogRegistrationService(deps RegistrationServiceDeps) *CatalogRegistrationService {
	return &CatalogRegistrationService{
		repo:                deps.Repo,
		attacher:            deps.Attacher,
		controlPlaneDBPath:  deps.ControlPlaneDBPath,
		duckDB:              deps.DuckDB,
		logger:              deps.Logger,
		metastoreFactory:    deps.MetastoreFactory,
		introspectionCloser: deps.IntrospectionClose,
		catalogRepoEvict:    deps.CatalogRepoEvict,
	}
}

// Register validates and persists a new catalog, then attempts to ATTACH it.
func (s *CatalogRegistrationService) Register(ctx context.Context, req domain.CreateCatalogRequest) (*domain.CatalogRegistration, error) {
	// Block reserved DuckDB catalog names that would conflict with internal catalogs.
	reserved := map[string]bool{"main": true, "memory": true, "system": true, "temp": true}
	if reserved[strings.ToLower(req.Name)] {
		return nil, domain.ErrValidation("catalog name %q is reserved by DuckDB", req.Name)
	}

	// Validate catalog name as a safe SQL identifier
	if err := ddl.ValidateIdentifier(req.Name); err != nil {
		return nil, domain.ErrValidation("invalid catalog name: %s", err.Error())
	}

	// Validate metastore type
	msType := domain.MetastoreType(req.MetastoreType)
	switch msType {
	case domain.MetastoreTypeSQLite, domain.MetastoreTypePostgres:
		// ok
	default:
		return nil, domain.ErrValidation("unsupported metastore type: %q (must be 'sqlite' or 'postgres')", req.MetastoreType)
	}

	// Enforce SQLite separation: DSN must not match control plane path
	if msType == domain.MetastoreTypeSQLite {
		if err := s.enforceSQLiteSeparation(req.DSN); err != nil {
			return nil, err
		}
	}

	if req.DataPath == "" {
		return nil, domain.ErrValidation("data_path is required")
	}

	// Check for duplicate name
	if _, err := s.repo.GetByName(ctx, req.Name); err == nil {
		return nil, domain.ErrConflict("catalog %q already exists", req.Name)
	}

	// Persist with status DETACHED
	reg := &domain.CatalogRegistration{
		Name:          req.Name,
		MetastoreType: msType,
		DSN:           req.DSN,
		DataPath:      req.DataPath,
		Status:        domain.CatalogStatusDetached,
		Comment:       req.Comment,
	}

	created, err := s.repo.Create(ctx, reg)
	if err != nil {
		return nil, fmt.Errorf("create catalog registration: %w", err)
	}

	// Attempt ATTACH
	if err := s.attacher.Attach(ctx, *created); err != nil {
		_ = s.repo.UpdateStatus(ctx, created.ID, domain.CatalogStatusError, err.Error())
		created.Status = domain.CatalogStatusError
		created.StatusMessage = err.Error()
		s.logger.Warn("catalog attach failed", "catalog", req.Name, "error", err)
		return created, nil // return the created registration with ERROR status
	}

	// Update status to ACTIVE
	_ = s.repo.UpdateStatus(ctx, created.ID, domain.CatalogStatusActive, "")
	created.Status = domain.CatalogStatusActive
	created.StatusMessage = ""

	s.logger.Info("catalog registered and attached", "catalog", req.Name)
	return created, nil
}

// List returns all catalog registrations.
func (s *CatalogRegistrationService) List(ctx context.Context, page domain.PageRequest) ([]domain.CatalogRegistration, int64, error) {
	return s.repo.List(ctx, page)
}

// Get returns a catalog registration by name.
func (s *CatalogRegistrationService) Get(ctx context.Context, name string) (*domain.CatalogRegistration, error) {
	return s.repo.GetByName(ctx, name)
}

// Update updates a catalog registration's metadata.
func (s *CatalogRegistrationService) Update(ctx context.Context, name string, req domain.UpdateCatalogRegistrationRequest) (*domain.CatalogRegistration, error) {
	existing, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}
	return s.repo.Update(ctx, existing.ID, req)
}

// Delete detaches and removes a catalog registration.
// Blocks deletion of the default catalog.
func (s *CatalogRegistrationService) Delete(ctx context.Context, name string) error {
	existing, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return err
	}

	if existing.IsDefault {
		return domain.ErrValidation("cannot delete the default catalog %q — set another catalog as default first", name)
	}

	// DETACH from DuckDB (best-effort if not attached)
	if existing.Status == domain.CatalogStatusActive {
		if err := s.attacher.Detach(ctx, name); err != nil {
			s.logger.Warn("detach failed during delete", "catalog", name, "error", err)
		}
	}

	// Close pooled metastore connections via factories
	if s.metastoreFactory != nil {
		_ = s.metastoreFactory.Close(name)
	}
	if s.introspectionCloser != nil {
		_ = s.introspectionCloser(name)
	}
	if s.catalogRepoEvict != nil {
		s.catalogRepoEvict(name)
	}

	// Delete from DB
	if err := s.repo.Delete(ctx, existing.ID); err != nil {
		return fmt.Errorf("delete catalog: %w", err)
	}

	s.logger.Info("catalog deleted", "catalog", name)
	return nil
}

// SetDefault sets the given catalog as the default and executes USE on DuckDB.
func (s *CatalogRegistrationService) SetDefault(ctx context.Context, name string) (*domain.CatalogRegistration, error) {
	existing, err := s.repo.GetByName(ctx, name)
	if err != nil {
		return nil, err
	}

	if existing.Status != domain.CatalogStatusActive {
		return nil, domain.ErrValidation("catalog %q must be ACTIVE to set as default (current status: %s)", name, existing.Status)
	}

	if err := s.repo.SetDefault(ctx, existing.ID); err != nil {
		return nil, fmt.Errorf("set default catalog: %w", err)
	}

	// Execute USE <catalog> on DuckDB
	if err := engine.SetDefaultCatalog(ctx, s.duckDB, name); err != nil {
		s.logger.Warn("USE catalog failed", "catalog", name, "error", err)
	}

	return s.repo.GetByID(ctx, existing.ID)
}

// AttachAll loads all registered catalogs and attaches them concurrently at startup.
func (s *CatalogRegistrationService) AttachAll(ctx context.Context) error {
	catalogs, _, err := s.repo.List(ctx, domain.PageRequest{MaxResults: 10000})
	if err != nil {
		return fmt.Errorf("list catalogs: %w", err)
	}

	if len(catalogs) == 0 {
		s.logger.Info("no catalogs registered")
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8) // bounded parallelism

	for i := range catalogs {
		cat := catalogs[i]
		g.Go(func() error {
			if err := s.attacher.Attach(gctx, cat); err != nil {
				_ = s.repo.UpdateStatus(gctx, cat.ID, domain.CatalogStatusError, err.Error())
				s.logger.Warn("attach failed at startup", "catalog", cat.Name, "error", err)
				return nil // don't fail all catalogs
			}
			_ = s.repo.UpdateStatus(gctx, cat.ID, domain.CatalogStatusActive, "")
			s.logger.Info("catalog attached at startup", "catalog", cat.Name)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("attach catalogs: %w", err)
	}

	// USE the default catalog
	defCat, err := s.repo.GetDefault(ctx)
	if err == nil && defCat.Status == domain.CatalogStatusActive {
		if err := engine.SetDefaultCatalog(ctx, s.duckDB, defCat.Name); err != nil {
			s.logger.Warn("USE default catalog failed", "catalog", defCat.Name, "error", err)
		} else {
			s.logger.Info("default catalog set", "catalog", defCat.Name)
		}
	}

	s.logger.Info("catalog startup complete", "total", len(catalogs))
	return nil
}

// enforceSQLiteSeparation ensures the given DSN doesn't point to the control plane DB.
func (s *CatalogRegistrationService) enforceSQLiteSeparation(dsn string) error {
	if s.controlPlaneDBPath == "" {
		return nil
	}

	// Normalize paths for comparison
	absControl, err1 := filepath.Abs(s.controlPlaneDBPath)
	absDSN, err2 := filepath.Abs(dsn)

	if err1 == nil && err2 == nil && absControl == absDSN {
		return domain.ErrValidation("catalog DSN cannot be the same as the control plane database path (%s)", s.controlPlaneDBPath)
	}

	// Also check string equality for non-absolute paths
	if strings.TrimSpace(dsn) == strings.TrimSpace(s.controlPlaneDBPath) {
		return domain.ErrValidation("catalog DSN cannot be the same as the control plane database path (%s)", s.controlPlaneDBPath)
	}

	return nil
}
