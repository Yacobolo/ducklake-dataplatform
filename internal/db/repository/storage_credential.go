package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"duck-demo/internal/db/crypto"
	"duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.StorageCredentialRepository = (*StorageCredentialRepo)(nil)

// StorageCredentialRepo implements StorageCredentialRepository with encrypted storage.
type StorageCredentialRepo struct {
	q   *dbstore.Queries
	db  *sql.DB
	enc *crypto.Encryptor
}

// NewStorageCredentialRepo creates a new StorageCredentialRepo.
func NewStorageCredentialRepo(db *sql.DB, enc *crypto.Encryptor) *StorageCredentialRepo {
	return &StorageCredentialRepo{q: dbstore.New(db), db: db, enc: enc}
}

func (r *StorageCredentialRepo) Create(ctx context.Context, cred *domain.StorageCredential) (*domain.StorageCredential, error) {
	encKeyID, err := r.enc.Encrypt(cred.KeyID)
	if err != nil {
		return nil, fmt.Errorf("encrypt key_id: %w", err)
	}
	encSecret, err := r.enc.Encrypt(cred.Secret)
	if err != nil {
		return nil, fmt.Errorf("encrypt secret: %w", err)
	}

	row, err := r.q.CreateStorageCredential(ctx, dbstore.CreateStorageCredentialParams{
		Name:            cred.Name,
		CredentialType:  string(cred.CredentialType),
		KeyIDEncrypted:  encKeyID,
		SecretEncrypted: encSecret,
		Endpoint:        cred.Endpoint,
		Region:          cred.Region,
		UrlStyle:        cred.URLStyle,
		Comment:         cred.Comment,
		Owner:           cred.Owner,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.fromDB(row)
}

func (r *StorageCredentialRepo) GetByID(ctx context.Context, id int64) (*domain.StorageCredential, error) {
	row, err := r.q.GetStorageCredential(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.fromDB(row)
}

func (r *StorageCredentialRepo) GetByName(ctx context.Context, name string) (*domain.StorageCredential, error) {
	row, err := r.q.GetStorageCredentialByName(ctx, name)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.fromDB(row)
}

func (r *StorageCredentialRepo) List(ctx context.Context, page domain.PageRequest) ([]domain.StorageCredential, int64, error) {
	total, err := r.q.CountStorageCredentials(ctx)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListStorageCredentials(ctx, dbstore.ListStorageCredentialsParams{
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	creds := make([]domain.StorageCredential, 0, len(rows))
	for _, row := range rows {
		cred, err := r.fromDB(row)
		if err != nil {
			return nil, 0, err
		}
		creds = append(creds, *cred)
	}
	return creds, total, nil
}

func (r *StorageCredentialRepo) Update(ctx context.Context, id int64, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error) {
	// Fetch current to fill in defaults for COALESCE
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	keyIDEnc, err := r.enc.Encrypt(current.KeyID)
	if err != nil {
		return nil, fmt.Errorf("encrypt key_id: %w", err)
	}
	secretEnc, err := r.enc.Encrypt(current.Secret)
	if err != nil {
		return nil, fmt.Errorf("encrypt secret: %w", err)
	}

	if req.KeyID != nil {
		keyIDEnc, err = r.enc.Encrypt(*req.KeyID)
		if err != nil {
			return nil, fmt.Errorf("encrypt key_id: %w", err)
		}
	}
	if req.Secret != nil {
		secretEnc, err = r.enc.Encrypt(*req.Secret)
		if err != nil {
			return nil, fmt.Errorf("encrypt secret: %w", err)
		}
	}

	endpoint := current.Endpoint
	if req.Endpoint != nil {
		endpoint = *req.Endpoint
	}
	region := current.Region
	if req.Region != nil {
		region = *req.Region
	}
	urlStyle := current.URLStyle
	if req.URLStyle != nil {
		urlStyle = *req.URLStyle
	}
	comment := current.Comment
	if req.Comment != nil {
		comment = *req.Comment
	}

	err = r.q.UpdateStorageCredential(ctx, dbstore.UpdateStorageCredentialParams{
		KeyIDEncrypted:  keyIDEnc,
		SecretEncrypted: secretEnc,
		Endpoint:        endpoint,
		Region:          region,
		UrlStyle:        urlStyle,
		Comment:         comment,
		ID:              id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

func (r *StorageCredentialRepo) Delete(ctx context.Context, id int64) error {
	return mapDBError(r.q.DeleteStorageCredential(ctx, id))
}

// fromDB decrypts the DB row into a domain StorageCredential.
func (r *StorageCredentialRepo) fromDB(row dbstore.StorageCredential) (*domain.StorageCredential, error) {
	keyID, err := r.enc.Decrypt(row.KeyIDEncrypted)
	if err != nil {
		return nil, fmt.Errorf("decrypt key_id: %w", err)
	}
	secret, err := r.enc.Decrypt(row.SecretEncrypted)
	if err != nil {
		return nil, fmt.Errorf("decrypt secret: %w", err)
	}
	createdAt, _ := time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	updatedAt, _ := time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	return &domain.StorageCredential{
		ID:             row.ID,
		Name:           row.Name,
		CredentialType: domain.CredentialType(row.CredentialType),
		KeyID:          keyID,
		Secret:         secret,
		Endpoint:       row.Endpoint,
		Region:         row.Region,
		URLStyle:       row.UrlStyle,
		Comment:        row.Comment,
		Owner:          row.Owner,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
	}, nil
}
