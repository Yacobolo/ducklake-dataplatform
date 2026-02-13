package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

// MetastoreRepoFactory creates MetastoreRepo instances per catalog
// by opening read-only connections to each catalog's metastore.
// Implements domain.MetastoreQuerierFactory.
type MetastoreRepoFactory struct {
	catalogRegRepo domain.CatalogRegistrationRepository

	mu    sync.RWMutex
	cache map[string]*metastoreEntry
}

type metastoreEntry struct {
	db   *sql.DB
	repo *MetastoreRepo
}

// NewMetastoreRepoFactory creates a new MetastoreRepoFactory.
func NewMetastoreRepoFactory(catalogRegRepo domain.CatalogRegistrationRepository) *MetastoreRepoFactory {
	return &MetastoreRepoFactory{
		catalogRegRepo: catalogRegRepo,
		cache:          make(map[string]*metastoreEntry),
	}
}

// Compile-time interface check.
var _ domain.MetastoreQuerierFactory = (*MetastoreRepoFactory)(nil)

// ForCatalog returns a MetastoreQuerier for the given catalog name.
func (f *MetastoreRepoFactory) ForCatalog(ctx context.Context, catalogName string) (domain.MetastoreQuerier, error) {
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

	db, err := f.openMetastore(reg)
	if err != nil {
		return nil, fmt.Errorf("open metastore for catalog %q: %w", catalogName, err)
	}

	repo := NewMetastoreRepo(db)
	f.cache[catalogName] = &metastoreEntry{db: db, repo: repo}
	return repo, nil
}

// Close releases pooled connections for a catalog.
func (f *MetastoreRepoFactory) Close(catalogName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	entry, ok := f.cache[catalogName]
	if !ok {
		return nil
	}
	delete(f.cache, catalogName)
	return entry.db.Close()
}

// CloseAll releases all pooled connections (for shutdown).
func (f *MetastoreRepoFactory) CloseAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, entry := range f.cache {
		_ = entry.db.Close()
		delete(f.cache, name)
	}
}

func (f *MetastoreRepoFactory) openMetastore(reg *domain.CatalogRegistration) (*sql.DB, error) {
	switch reg.MetastoreType {
	case domain.MetastoreTypeSQLite:
		return internaldb.OpenSQLite(reg.DSN, "read", 4)
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
