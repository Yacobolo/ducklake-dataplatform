package service

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
)

// S3Presigner generates presigned S3 URLs for Hetzner-compatible object storage.
// It uses the AWS SDK v2, configured with path-style addressing for Hetzner.
type S3Presigner struct {
	presignClient *s3.PresignClient
	bucket        string
}

// NewS3Presigner creates a presigner configured for Hetzner S3-compatible storage.
func NewS3Presigner(cfg *config.Config) (*S3Presigner, error) {
	endpoint := fmt.Sprintf("https://%s", cfg.S3Endpoint)

	s3Client := s3.New(s3.Options{
		Region: cfg.S3Region,
		Credentials: credentials.NewStaticCredentialsProvider(
			cfg.S3KeyID, cfg.S3Secret, "",
		),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true, // Hetzner requires path-style URLs
	})

	return &S3Presigner{
		presignClient: s3.NewPresignClient(s3Client),
		bucket:        cfg.S3Bucket,
	}, nil
}

// PresignGetObject generates a presigned GET URL for an S3 object.
// s3Path is a full s3:// URI like "s3://bucket/lake_data/xxx.parquet".
func (p *S3Presigner) PresignGetObject(ctx context.Context, s3Path string, expiry time.Duration) (string, error) {
	bucket, key, err := parseS3Path(s3Path)
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

// parseS3Path extracts bucket and key from an "s3://bucket/path/to/file" URI.
func parseS3Path(s3Path string) (bucket, key string, err error) {
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
