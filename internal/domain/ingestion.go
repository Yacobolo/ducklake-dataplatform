package domain

import "time"

// IngestionOptions controls how ducklake_add_data_files handles schema differences.
type IngestionOptions struct {
	AllowMissingColumns bool
	IgnoreExtraColumns  bool
}

// UploadURLResult holds the presigned URL and S3 key for a client upload.
type UploadURLResult struct {
	UploadURL string
	S3Key     string
	ExpiresAt time.Time
}

// IngestionResult describes the outcome of a file ingestion operation.
type IngestionResult struct {
	FilesRegistered int
	FilesSkipped    int
	Table           string
	Schema          string
}
