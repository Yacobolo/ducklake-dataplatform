package query

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"

	"duck-demo/internal/domain"
)

// Compile-time checks: AzurePresigner implements both presigner interfaces.
var _ FilePresigner = (*AzurePresigner)(nil)
var _ FileUploadPresigner = (*AzurePresigner)(nil)

// AzurePresigner generates presigned (SAS) URLs for Azure Blob Storage objects.
// It uses shared-key credentials to produce time-limited SAS tokens.
type AzurePresigner struct {
	client      *azblob.Client
	container   string
	accountName string
}

// NewAzurePresignerFromCredential creates an AzurePresigner from a stored StorageCredential.
// Only account-key authentication is supported; service principal presigning is not yet implemented.
func NewAzurePresignerFromCredential(cred *domain.StorageCredential, storagePath string) (*AzurePresigner, error) {
	if cred == nil {
		return nil, fmt.Errorf("credential is nil")
	}

	if cred.AzureAccountKey == "" {
		return nil, fmt.Errorf("Azure account key authentication required; service principal presigning not yet supported")
	}

	sharedKeyCred, err := azblob.NewSharedKeyCredential(cred.AzureAccountName, cred.AzureAccountKey)
	if err != nil {
		return nil, fmt.Errorf("create shared key credential: %w", err)
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net", cred.AzureAccountName)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, sharedKeyCred, nil)
	if err != nil {
		return nil, fmt.Errorf("create Azure blob client: %w", err)
	}

	container, _, err := parseAzurePath(storagePath)
	if err != nil {
		// If storagePath has no key component, that's acceptable for a constructor —
		// the container is all we need here.
		if container == "" {
			return nil, fmt.Errorf("parse storage path %q: %w", storagePath, err)
		}
	}

	return &AzurePresigner{
		client:      client,
		container:   container,
		accountName: cred.AzureAccountName,
	}, nil
}

// PresignGetObject generates a presigned (SAS) GET URL for an Azure Blob Storage object.
// path is a full Azure storage URI (abfss://, az://, or https://).
func (p *AzurePresigner) PresignGetObject(ctx context.Context, path string, expiry time.Duration) (string, error) {
	container, key, err := parseAzurePath(path)
	if err != nil {
		return "", fmt.Errorf("parse Azure path %q: %w", path, err)
	}

	blobClient := p.client.ServiceClient().NewContainerClient(container).NewBlobClient(key)
	sasURL, err := blobClient.GetSASURL(sas.BlobPermissions{Read: true}, time.Now().Add(expiry), nil)
	if err != nil {
		return "", fmt.Errorf("generate SAS URL for %q: %w", path, err)
	}
	return sasURL, nil
}

// PresignPutObject generates a presigned (SAS) PUT URL for uploading to Azure Blob Storage.
func (p *AzurePresigner) PresignPutObject(ctx context.Context, bucket, key string, expiry time.Duration) (string, error) {
	blobClient := p.client.ServiceClient().NewContainerClient(bucket).NewBlobClient(key)
	sasURL, err := blobClient.GetSASURL(sas.BlobPermissions{Write: true, Create: true}, time.Now().Add(expiry), nil)
	if err != nil {
		return "", fmt.Errorf("generate SAS upload URL for %q/%q: %w", bucket, key, err)
	}
	return sasURL, nil
}

// Bucket returns the default container name (equivalent of S3 bucket).
func (p *AzurePresigner) Bucket() string {
	return p.container
}

// parseAzurePath extracts container and key from an Azure storage URI.
//
// Supported formats:
//
//	abfss://container@account.dfs.core.windows.net/path/to/file
//	az://container/path/to/file
//	https://account.blob.core.windows.net/container/path/to/file
func parseAzurePath(path string) (container, key string, err error) {
	u, err := url.Parse(path)
	if err != nil {
		return "", "", fmt.Errorf("parse Azure path %q: %w", path, err)
	}

	switch u.Scheme {
	case "abfss":
		// abfss://container@account.dfs.core.windows.net/path/to/file
		// Go's url.Parse treats "container" as userinfo (before @) and
		// "account.dfs.core.windows.net" as host.
		if u.User == nil {
			return "", "", fmt.Errorf("abfss path %q missing container@account component", path)
		}
		container = u.User.Username()
		key = strings.TrimPrefix(u.Path, "/")

	case "az":
		// az://container/path/to/file
		container = u.Host
		key = strings.TrimPrefix(u.Path, "/")

	case "https":
		// https://account.blob.core.windows.net/container/path/to/file
		if !strings.Contains(u.Host, ".blob.core.windows.net") {
			return "", "", fmt.Errorf("unrecognized Azure HTTPS host %q in path %q", u.Host, path)
		}
		trimmed := strings.TrimPrefix(u.Path, "/")
		parts := strings.SplitN(trimmed, "/", 2)
		container = parts[0]
		if len(parts) > 1 {
			key = parts[1]
		}

	default:
		return "", "", fmt.Errorf("unrecognized Azure path scheme %q in %q", u.Scheme, path)
	}

	if container == "" {
		return "", "", fmt.Errorf("empty container in Azure path %q", path)
	}
	if key == "" {
		return container, "", fmt.Errorf("empty key in Azure path %q", path)
	}

	return container, key, nil
}
