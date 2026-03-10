package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInferStorageBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dataPath string
		want     string
	}{
		{name: "empty", dataPath: "", want: "UNKNOWN"},
		{name: "file url", dataPath: "file:///tmp/lake", want: "FILE"},
		{name: "plain path", dataPath: "/tmp/lake", want: "FILE"},
		{name: "s3 path", dataPath: "s3://bucket/lake", want: "S3"},
		{name: "gcs path", dataPath: "gs://bucket/lake", want: "S3"},
		{name: "azure path", dataPath: "abfss://container@account.dfs.core.windows.net/lake", want: "AZURE"},
		{name: "unknown scheme", dataPath: "minio://bucket/lake", want: "MINIO"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, inferStorageBackend(tt.dataPath))
		})
	}
}
