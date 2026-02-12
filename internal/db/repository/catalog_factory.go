package repository

import (
	"database/sql"
	"log/slog"
	"sync"
)

// CatalogRepoFactory creates CatalogRepo instances scoped to a catalog name.
// It caches instances to avoid re-creating them on every request.
type CatalogRepoFactory struct {
	controlDB *sql.DB
	duckDB    *sql.DB
	extRepo   *ExternalTableRepo
	logger    *slog.Logger

	mu    sync.RWMutex
	cache map[string]*CatalogRepo
}

// NewCatalogRepoFactory creates a new CatalogRepoFactory.
func NewCatalogRepoFactory(controlDB, duckDB *sql.DB, extRepo *ExternalTableRepo, logger *slog.Logger) *CatalogRepoFactory {
	return &CatalogRepoFactory{
		controlDB: controlDB,
		duckDB:    duckDB,
		extRepo:   extRepo,
		logger:    logger,
		cache:     make(map[string]*CatalogRepo),
	}
}

// ForCatalog returns a CatalogRepo for the given catalog name.
func (f *CatalogRepoFactory) ForCatalog(catalogName string) *CatalogRepo {
	f.mu.RLock()
	if repo, ok := f.cache[catalogName]; ok {
		f.mu.RUnlock()
		return repo
	}
	f.mu.RUnlock()

	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check after acquiring write lock.
	if repo, ok := f.cache[catalogName]; ok {
		return repo
	}

	repo := NewCatalogRepo(f.controlDB, f.duckDB, catalogName, f.extRepo, f.logger.With("catalog", catalogName))
	f.cache[catalogName] = repo
	return repo
}

// Evict removes a cached CatalogRepo when a catalog is deleted.
func (f *CatalogRepoFactory) Evict(catalogName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.cache, catalogName)
}
