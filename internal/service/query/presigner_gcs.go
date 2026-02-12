package query

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"duck-demo/internal/domain"
)

// Compile-time checks: GCSPresigner implements both presigner interfaces.
var _ FilePresigner = (*GCSPresigner)(nil)
var _ FileUploadPresigner = (*GCSPresigner)(nil)

// GCSPresigner generates signed URLs for Google Cloud Storage objects.
type GCSPresigner struct {
	client *storage.Client
	bucket string
}

// NewGCSPresignerFromCredential creates a GCS presigner from a stored StorageCredential.
// The storagePath is used to extract the default bucket (e.g. "gs://my-bucket/prefix").
func NewGCSPresignerFromCredential(cred *domain.StorageCredential, storagePath string) (*GCSPresigner, error) {
	if cred == nil {
		return nil, fmt.Errorf("credential is nil")
	}
	if cred.GCSKeyFilePath == "" {
		return nil, fmt.Errorf("gcs_key_file_path is required")
	}

	client, err := storage.NewClient(context.Background(), option.WithAuthCredentialsFile(option.ServiceAccount, cred.GCSKeyFilePath))
	if err != nil {
		return nil, fmt.Errorf("create GCS client: %w", err)
	}

	bucket, _, err := parseGCSPath(storagePath)
	if err != nil {
		return nil, fmt.Errorf("parse storage path %q: %w", storagePath, err)
	}

	return &GCSPresigner{
		client: client,
		bucket: bucket,
	}, nil
}

// PresignGetObject generates a signed GET URL for a GCS object.
// path is a full gs:// URI like "gs://bucket/path/to/file.parquet".
func (p *GCSPresigner) PresignGetObject(_ context.Context, path string, expiry time.Duration) (string, error) {
	bucket, key, err := parseGCSPath(path)
	if err != nil {
		return "", err
	}

	signedURL, err := p.client.Bucket(bucket).SignedURL(key, &storage.SignedURLOptions{
		Method:  "GET",
		Expires: time.Now().Add(expiry),
	})
	if err != nil {
		return "", fmt.Errorf("sign GetObject for %q: %w", path, err)
	}
	return signedURL, nil
}

// PresignPutObject generates a signed PUT URL for uploading a GCS object.
func (p *GCSPresigner) PresignPutObject(_ context.Context, bucket, key string, expiry time.Duration) (string, error) {
	signedURL, err := p.client.Bucket(bucket).SignedURL(key, &storage.SignedURLOptions{
		Method:      "PUT",
		Expires:     time.Now().Add(expiry),
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return "", fmt.Errorf("sign PutObject for %q/%q: %w", bucket, key, err)
	}
	return signedURL, nil
}

// Bucket returns the configured GCS bucket name.
func (p *GCSPresigner) Bucket() string {
	return p.bucket
}

// parseGCSPath extracts bucket and key from a "gs://bucket/path/to/file" URI.
func parseGCSPath(path string) (bucket, key string, err error) {
	u, err := url.Parse(path)
	if err != nil {
		return "", "", fmt.Errorf("parse GCS path %q: %w", path, err)
	}
	if u.Scheme != "gs" {
		return "", "", fmt.Errorf("expected gs:// scheme, got %q in %q", u.Scheme, path)
	}
	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return "", "", fmt.Errorf("empty key in GCS path %q", path)
	}
	return bucket, key, nil
}
