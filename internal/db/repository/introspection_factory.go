package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"
)

// IntrospectionRepoFactory creates IntrospectionRepo instances per catalog
// by opening read-only connections to each catalog's metastore.
type IntrospectionRepoFactory struct {
	catalogRegRepo domain.CatalogRegistrationRepository

	mu    sync.RWMutex
	cache map[string]*introspectionEntry
}

type introspectionEntry struct {
	db   *sql.DB
	repo *IntrospectionRepo
}

// NewIntrospectionRepoFactory creates a new IntrospectionRepoFactory.
func NewIntrospectionRepoFactory(catalogRegRepo domain.CatalogRegistrationRepository) *IntrospectionRepoFactory {
	return &IntrospectionRepoFactory{
		catalogRegRepo: catalogRegRepo,
		cache:          make(map[string]*introspectionEntry),
	}
}

// ForDefault returns an IntrospectionRepo for the current default catalog.
func (f *IntrospectionRepoFactory) ForDefault(ctx context.Context) (domain.IntrospectionRepository, error) {
	reg, err := f.catalogRegRepo.GetDefault(ctx)
	if err != nil {
		return nil, fmt.Errorf("no default catalog configured: %w", err)
	}
	return f.ForCatalog(ctx, reg.Name)
}

// DefaultCatalogName returns the current default catalog name.
func (f *IntrospectionRepoFactory) DefaultCatalogName(ctx context.Context) (string, error) {
	reg, err := f.catalogRegRepo.GetDefault(ctx)
	if err != nil {
		return "", fmt.Errorf("no default catalog configured: %w", err)
	}
	return reg.Name, nil
}

// ForCatalog returns an IntrospectionRepo for the given catalog name.
func (f *IntrospectionRepoFactory) ForCatalog(ctx context.Context, catalogName string) (domain.IntrospectionRepository, error) {
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

	repo := NewIntrospectionRepo(db)
	f.cache[catalogName] = &introspectionEntry{db: db, repo: repo}
	return repo, nil
}

// Close releases pooled connections for a catalog.
func (f *IntrospectionRepoFactory) Close(catalogName string) error {
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
func (f *IntrospectionRepoFactory) CloseAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for name, entry := range f.cache {
		_ = entry.db.Close()
		delete(f.cache, name)
	}
}

func (f *IntrospectionRepoFactory) openMetastore(reg *domain.CatalogRegistration) (*sql.DB, error) {
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
