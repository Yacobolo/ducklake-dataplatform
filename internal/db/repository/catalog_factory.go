package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

type catalogEntry struct {
	metaDB *sql.DB
	repo   *CatalogRepo
}

// CatalogRepoFactory creates CatalogRepo instances scoped to a catalog name.
// It opens a per-catalog metastore connection and caches instances.
type CatalogRepoFactory struct {
	catalogRegRepo domain.CatalogRegistrationRepository
	controlDB      *sql.DB // control-plane DB (for catalog_metadata, tags, etc.)
	duckDB         *sql.DB
	extRepo        *ExternalTableRepo
	logger         *slog.Logger

	mu    sync.RWMutex
	cache map[string]*catalogEntry
}

// NewCatalogRepoFactory creates a new CatalogRepoFactory.
func NewCatalogRepoFactory(
	catalogRegRepo domain.CatalogRegistrationRepository,
	controlDB, duckDB *sql.DB,
	extRepo *ExternalTableRepo,
	logger *slog.Logger,
) *CatalogRepoFactory {
	return &CatalogRepoFactory{
		catalogRegRepo: catalogRegRepo,
		controlDB:      controlDB,
		duckDB:         duckDB,
		extRepo:        extRepo,
		logger:         logger,
		cache:          make(map[string]*catalogEntry),
	}
}

// ForCatalog returns a CatalogRepository for the given catalog name.
func (f *CatalogRepoFactory) ForCatalog(ctx context.Context, catalogName string) (domain.CatalogRepository, error) {
	f.mu.RLock()
	if entry, ok := f.cache[catalogName]; ok {
		f.mu.RUnlock()
		return entry.repo, nil
	}
	f.mu.RUnlock()

	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check after acquiring write lock.
	if entry, ok := f.cache[catalogName]; ok {
		return entry.repo, nil
	}

	reg, err := f.catalogRegRepo.GetByName(ctx, catalogName)
	if err != nil {
		return nil, fmt.Errorf("lookup catalog %q: %w", catalogName, err)
	}

	metaDB, err := f.openMetastore(reg)
	if err != nil {
		return nil, fmt.Errorf("open metastore for catalog %q: %w", catalogName, err)
	}

	controlQ := dbstore.New(f.controlDB)
	repo := NewCatalogRepo(metaDB, f.controlDB, controlQ, f.duckDB, catalogName, f.extRepo, f.logger.With("catalog", catalogName))
	f.cache[catalogName] = &catalogEntry{metaDB: metaDB, repo: repo}
	return repo, nil
}

// Evict removes and closes a cached CatalogRepo when a catalog is deleted.
func (f *CatalogRepoFactory) Evict(catalogName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if entry, ok := f.cache[catalogName]; ok {
		_ = entry.metaDB.Close()
		delete(f.cache, catalogName)
	}
}

// Close releases pooled connections for a catalog.
func (f *CatalogRepoFactory) Close(catalogName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, ok := f.cache[catalogName]
	if !ok {
		return nil
	}
	delete(f.cache, catalogName)
	return entry.metaDB.Close()
}

// CloseAll releases all pooled connections (for shutdown).
func (f *CatalogRepoFactory) CloseAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, entry := range f.cache {
		_ = entry.metaDB.Close()
		delete(f.cache, name)
	}
}

func (f *CatalogRepoFactory) openMetastore(reg *domain.CatalogRegistration) (*sql.DB, error) {
	switch reg.MetastoreType {
	case domain.MetastoreTypeSQLite:
		return internaldb.OpenSQLite(reg.DSN, "write", 1)
	case domain.MetastoreTypePostgres:
		db, err := sql.Open("postgres", reg.DSN)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(4)
		return db, nil
	default:
		return nil, fmt.Errorf("unsupported metastore type: %q", reg.MetastoreType)
	}
}
