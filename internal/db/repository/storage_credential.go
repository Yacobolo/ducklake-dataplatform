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

// Create inserts a new storage credential with encrypted secrets.
func (r *StorageCredentialRepo) Create(ctx context.Context, cred *domain.StorageCredential) (*domain.StorageCredential, error) {
	encKeyID, err := r.enc.Encrypt(cred.KeyID)
	if err != nil {
		return nil, fmt.Errorf("encrypt key_id: %w", err)
	}
	encSecret, err := r.enc.Encrypt(cred.Secret)
	if err != nil {
		return nil, fmt.Errorf("encrypt secret: %w", err)
	}
	encAzureAccountKey, err := r.enc.Encrypt(cred.AzureAccountKey)
	if err != nil {
		return nil, fmt.Errorf("encrypt azure_account_key: %w", err)
	}
	encAzureClientSecret, err := r.enc.Encrypt(cred.AzureClientSecret)
	if err != nil {
		return nil, fmt.Errorf("encrypt azure_client_secret: %w", err)
	}

	row, err := r.q.CreateStorageCredential(ctx, dbstore.CreateStorageCredentialParams{
		ID:                         newID(),
		Name:                       cred.Name,
		CredentialType:             string(cred.CredentialType),
		KeyIDEncrypted:             encKeyID,
		SecretEncrypted:            encSecret,
		Endpoint:                   cred.Endpoint,
		Region:                     cred.Region,
		UrlStyle:                   cred.URLStyle,
		AzureAccountName:           cred.AzureAccountName,
		AzureAccountKeyEncrypted:   encAzureAccountKey,
		AzureClientID:              cred.AzureClientID,
		AzureTenantID:              cred.AzureTenantID,
		AzureClientSecretEncrypted: encAzureClientSecret,
		GcsKeyFilePath:             cred.GCSKeyFilePath,
		Comment:                    cred.Comment,
		Owner:                      cred.Owner,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.fromDB(row)
}

// GetByID returns a storage credential by its ID, decrypting secrets.
func (r *StorageCredentialRepo) GetByID(ctx context.Context, id string) (*domain.StorageCredential, error) {
	row, err := r.q.GetStorageCredential(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.fromDB(row)
}

// GetByName returns a storage credential by its name, decrypting secrets.
func (r *StorageCredentialRepo) GetByName(ctx context.Context, name string) (*domain.StorageCredential, error) {
	row, err := r.q.GetStorageCredentialByName(ctx, name)
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.fromDB(row)
}

// List returns a paginated list of storage credentials, decrypting secrets.
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

// Update applies partial updates to a storage credential by ID.
func (r *StorageCredentialRepo) Update(ctx context.Context, id string, req domain.UpdateStorageCredentialRequest) (*domain.StorageCredential, error) {
	// Fetch current to fill in defaults for COALESCE
	current, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// S3 fields
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

	// Azure fields
	azureAccountName := current.AzureAccountName
	if req.AzureAccountName != nil {
		azureAccountName = *req.AzureAccountName
	}
	azureAccountKeyEnc, err := r.enc.Encrypt(current.AzureAccountKey)
	if err != nil {
		return nil, fmt.Errorf("encrypt azure_account_key: %w", err)
	}
	if req.AzureAccountKey != nil {
		azureAccountKeyEnc, err = r.enc.Encrypt(*req.AzureAccountKey)
		if err != nil {
			return nil, fmt.Errorf("encrypt azure_account_key: %w", err)
		}
	}
	azureClientID := current.AzureClientID
	if req.AzureClientID != nil {
		azureClientID = *req.AzureClientID
	}
	azureTenantID := current.AzureTenantID
	if req.AzureTenantID != nil {
		azureTenantID = *req.AzureTenantID
	}
	azureClientSecretEnc, err := r.enc.Encrypt(current.AzureClientSecret)
	if err != nil {
		return nil, fmt.Errorf("encrypt azure_client_secret: %w", err)
	}
	if req.AzureClientSecret != nil {
		azureClientSecretEnc, err = r.enc.Encrypt(*req.AzureClientSecret)
		if err != nil {
			return nil, fmt.Errorf("encrypt azure_client_secret: %w", err)
		}
	}

	// GCS fields
	gcsKeyFilePath := current.GCSKeyFilePath
	if req.GCSKeyFilePath != nil {
		gcsKeyFilePath = *req.GCSKeyFilePath
	}

	comment := current.Comment
	if req.Comment != nil {
		comment = *req.Comment
	}

	err = r.q.UpdateStorageCredential(ctx, dbstore.UpdateStorageCredentialParams{
		KeyIDEncrypted:             keyIDEnc,
		SecretEncrypted:            secretEnc,
		Endpoint:                   endpoint,
		Region:                     region,
		UrlStyle:                   urlStyle,
		AzureAccountName:           azureAccountName,
		AzureAccountKeyEncrypted:   azureAccountKeyEnc,
		AzureClientID:              azureClientID,
		AzureTenantID:              azureTenantID,
		AzureClientSecretEncrypted: azureClientSecretEnc,
		GcsKeyFilePath:             gcsKeyFilePath,
		Comment:                    comment,
		ID:                         id,
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return r.GetByID(ctx, id)
}

// Delete removes a storage credential by ID.
func (r *StorageCredentialRepo) Delete(ctx context.Context, id string) error {
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
	azureAccountKey, err := r.enc.Decrypt(row.AzureAccountKeyEncrypted)
	if err != nil {
		return nil, fmt.Errorf("decrypt azure_account_key: %w", err)
	}
	azureClientSecret, err := r.enc.Decrypt(row.AzureClientSecretEncrypted)
	if err != nil {
		return nil, fmt.Errorf("decrypt azure_client_secret: %w", err)
	}

	createdAt, _ := time.Parse("2006-01-02 15:04:05", row.CreatedAt)
	updatedAt, _ := time.Parse("2006-01-02 15:04:05", row.UpdatedAt)
	return &domain.StorageCredential{
		ID:                row.ID,
		Name:              row.Name,
		CredentialType:    domain.CredentialType(row.CredentialType),
		KeyID:             keyID,
		Secret:            secret,
		Endpoint:          row.Endpoint,
		Region:            row.Region,
		URLStyle:          row.UrlStyle,
		AzureAccountName:  row.AzureAccountName,
		AzureAccountKey:   azureAccountKey,
		AzureClientID:     row.AzureClientID,
		AzureTenantID:     row.AzureTenantID,
		AzureClientSecret: azureClientSecret,
		GCSKeyFilePath:    row.GcsKeyFilePath,
		Comment:           row.Comment,
		Owner:             row.Owner,
		CreatedAt:         createdAt,
		UpdatedAt:         updatedAt,
	}, nil
}
