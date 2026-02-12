package query

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"duck-demo/internal/config"
	"duck-demo/internal/domain"
)

// Compile-time checks: S3Presigner implements both presigner interfaces.
var _ FilePresigner = (*S3Presigner)(nil)
var _ FileUploadPresigner = (*S3Presigner)(nil)

// FileUploadPresigner extends FilePresigner with upload (PUT) support and
// bucket discovery. Implementations: S3Presigner, AzurePresigner, GCSPresigner.
type FileUploadPresigner interface {
	FilePresigner
	PresignPutObject(ctx context.Context, bucket, key string, expiry time.Duration) (string, error)
	Bucket() string
}

// S3Presigner generates presigned S3 URLs for Hetzner-compatible object storage.
// It uses the AWS SDK v2, configured with path-style addressing for Hetzner.
type S3Presigner struct {
	presignClient *s3.PresignClient
	bucket        string
}

// NewS3Presigner creates a presigner configured for Hetzner S3-compatible storage.
func NewS3Presigner(cfg *config.Config) (*S3Presigner, error) {
	if !cfg.HasS3Config() {
		return nil, fmt.Errorf("S3 config is incomplete")
	}

	endpoint := fmt.Sprintf("https://%s", *cfg.S3Endpoint)

	s3Client := s3.New(s3.Options{
		Region: *cfg.S3Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			*cfg.S3KeyID, *cfg.S3Secret, "",
		),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true, // Hetzner requires path-style URLs
	})

	bucket := "duck-demo"
	if cfg.S3Bucket != nil {
		bucket = *cfg.S3Bucket
	}

	return &S3Presigner{
		presignClient: s3.NewPresignClient(s3Client),
		bucket:        bucket,
	}, nil
}

// PresignGetObject generates a presigned GET URL for an S3 object.
// s3Path is a full s3:// URI like "s3://bucket/lake_data/xxx.parquet".
func (p *S3Presigner) PresignGetObject(ctx context.Context, s3Path string, expiry time.Duration) (string, error) {
	bucket, key, err := ParseS3Path(s3Path)
	if err != nil {
		return "", err
	}

	result, err := p.presignClient.PresignGetObject(ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		},
		s3.WithPresignExpires(expiry),
	)
	if err != nil {
		return "", fmt.Errorf("presign GetObject for %q: %w", s3Path, err)
	}
	return result.URL, nil
}

// PresignPutObject generates a presigned PUT URL for uploading an S3 object.
func (p *S3Presigner) PresignPutObject(ctx context.Context, bucket, key string, expiry time.Duration) (string, error) {
	result, err := p.presignClient.PresignPutObject(ctx,
		&s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			ContentType: aws.String("application/octet-stream"),
		},
		s3.WithPresignExpires(expiry),
	)
	if err != nil {
		return "", fmt.Errorf("presign PutObject for %q/%q: %w", bucket, key, err)
	}
	return result.URL, nil
}

// Bucket returns the configured S3 bucket name.
func (p *S3Presigner) Bucket() string {
	return p.bucket
}

// NewS3PresignerFromCredential creates a presigner from a stored StorageCredential.
// This is used when schemas have per-schema external locations with their own credentials.
func NewS3PresignerFromCredential(cred *domain.StorageCredential, bucket string) (*S3Presigner, error) {
	if cred == nil {
		return nil, fmt.Errorf("credential is nil")
	}

	endpoint := fmt.Sprintf("https://%s", cred.Endpoint)
	usePathStyle := cred.URLStyle != "vhost"

	s3Client := s3.New(s3.Options{
		Region: cred.Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			cred.KeyID, cred.Secret, "",
		),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: usePathStyle,
	})

	return &S3Presigner{
		presignClient: s3.NewPresignClient(s3Client),
		bucket:        bucket,
	}, nil
}

// ParseS3Path extracts bucket and key from an "s3://bucket/path/to/file" URI.
func ParseS3Path(s3Path string) (bucket, key string, err error) {
	u, err := url.Parse(s3Path)
	if err != nil {
		return "", "", fmt.Errorf("parse S3 path %q: %w", s3Path, err)
	}
	if u.Scheme != "s3" {
		return "", "", fmt.Errorf("expected s3:// scheme, got %q in %q", u.Scheme, s3Path)
	}
	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return "", "", fmt.Errorf("empty key in S3 path %q", s3Path)
	}
	return bucket, key, nil
}

// NewPresignerFromCredential creates a FilePresigner based on credential type.
// It dispatches to the appropriate cloud-specific constructor.
func NewPresignerFromCredential(cred *domain.StorageCredential, storagePath string) (FilePresigner, error) {
	switch cred.CredentialType {
	case domain.CredentialTypeS3:
		bucket, _, _ := ParseS3Path(storagePath)
		return NewS3PresignerFromCredential(cred, bucket)
	case domain.CredentialTypeAzure:
		return NewAzurePresignerFromCredential(cred, storagePath)
	case domain.CredentialTypeGCS:
		return NewGCSPresignerFromCredential(cred, storagePath)
	default:
		return nil, fmt.Errorf("unsupported credential type %q", cred.CredentialType)
	}
}

// NewUploadPresignerFromCredential creates a FileUploadPresigner based on credential type.
// All three implementations (S3, Azure, GCS) implement FileUploadPresigner.
func NewUploadPresignerFromCredential(cred *domain.StorageCredential, storagePath string) (FileUploadPresigner, error) {
	switch cred.CredentialType {
	case domain.CredentialTypeS3:
		bucket, _, _ := ParseS3Path(storagePath)
		return NewS3PresignerFromCredential(cred, bucket)
	case domain.CredentialTypeAzure:
		return NewAzurePresignerFromCredential(cred, storagePath)
	case domain.CredentialTypeGCS:
		return NewGCSPresignerFromCredential(cred, storagePath)
	default:
		return nil, fmt.Errorf("unsupported credential type %q", cred.CredentialType)
	}
}
