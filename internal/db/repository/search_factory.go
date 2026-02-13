package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

// SearchRepoFactory creates SearchRepo instances per catalog by opening
// read-only connections to each catalog's metastore. The control plane DB
// is shared across all catalogs.
type SearchRepoFactory struct {
	controlDB      *sql.DB
	catalogRegRepo domain.CatalogRegistrationRepository

	mu    sync.RWMutex
	cache map[string]*searchEntry
}

type searchEntry struct {
	db   *sql.DB
	repo *SearchRepo
}

// NewSearchRepoFactory creates a new SearchRepoFactory.
func NewSearchRepoFactory(controlDB *sql.DB, catalogRegRepo domain.CatalogRegistrationRepository) *SearchRepoFactory {
	return &SearchRepoFactory{
		controlDB:      controlDB,
		catalogRegRepo: catalogRegRepo,
		cache:          make(map[string]*searchEntry),
	}
}

// ForCatalog returns a SearchRepo for the given catalog name.
// The returned repo queries that catalog's metastore for ducklake_* tables
// and the shared control plane DB for governance metadata.
func (f *SearchRepoFactory) ForCatalog(ctx context.Context, catalogName string) (domain.SearchRepository, error) {
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

	repo := NewSearchRepo(db, f.controlDB)
	f.cache[catalogName] = &searchEntry{db: db, repo: repo}
	return repo, nil
}

// Close releases pooled connections for a catalog.
func (f *SearchRepoFactory) Close(catalogName string) error {
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
func (f *SearchRepoFactory) CloseAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, entry := range f.cache {
		_ = entry.db.Close()
		delete(f.cache, name)
	}
}

func (f *SearchRepoFactory) openMetastore(reg *domain.CatalogRegistration) (*sql.DB, error) {
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
