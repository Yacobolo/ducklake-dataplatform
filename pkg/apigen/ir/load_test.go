package ir

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoad_Valid(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ir.json")

	require.NoError(t, os.WriteFile(path, []byte(`{
  "schema_version": "v1",
  "api": {"base_path": "/v1"},
  "info": {"title": "Duck", "version": "0.1.0"},
  "endpoints": [
    {"method": "post", "path": "/v1/query", "operation_id": "executeQuery", "responses": [{"status_code": 200, "description": "ok"}]},
    {"method": "get", "path": "/healthz", "operation_id": "getHealth", "responses": [{"status_code": 200, "description": "ok"}]}
  ]
}`), 0o644))

	doc, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "getHealth", doc.Endpoints[0].OperationID)
}

func TestLoad_InvalidVersion(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ir.json")

	require.NoError(t, os.WriteFile(path, []byte(`{
  "schema_version": "v0",
  "api": {"base_path": "/v1"},
  "info": {"title": "Duck", "version": "0.1.0"},
  "endpoints": [{"method": "get", "path": "/healthz", "operation_id": "getHealth", "responses": [{"status_code": 200, "description": "ok"}]}]
}`), 0o644))

	_, err := Load(path)
	require.Error(t, err)
}

func TestLoad_DuplicateOperation(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ir.json")

	require.NoError(t, os.WriteFile(path, []byte(`{
  "schema_version": "v1",
  "api": {"base_path": "/v1"},
  "info": {"title": "Duck", "version": "0.1.0"},
  "endpoints": [
    {"method": "get", "path": "/healthz", "operation_id": "dup", "responses": [{"status_code": 200, "description": "ok"}]},
    {"method": "post", "path": "/v1/query", "operation_id": "dup", "responses": [{"status_code": 200, "description": "ok"}]}
  ]
}`), 0o644))

	_, err := Load(path)
	require.Error(t, err)
}

func TestLoad_NormalizesResponseHeaders(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ir.json")

	require.NoError(t, os.WriteFile(path, []byte(`{
  "schema_version": "v1",
  "api": {"base_path": "/v1"},
  "info": {"title": "Duck", "version": "0.1.0"},
  "endpoints": [{
    "method": "get",
    "path": "/widgets/{id}",
    "operation_id": "getWidget",
    "responses": [{
      "status_code": 429,
      "description": "rate limited",
      "headers": [
        {"name": "X-RateLimit-Reset", "schema": {"type": "integer", "format": "int64"}},
        {"name": "Retry-After", "schema": {"type": "integer", "format": "int32"}}
      ]
    }, {
      "status_code": 200,
      "description": "ok",
      "headers": [
        {"name": "X-RateLimit-Remaining", "schema": {"type": "integer", "format": "int32"}},
        {"name": "X-RateLimit-Limit", "schema": {"type": "integer", "format": "int32"}}
      ]
    }]
  }]
}`), 0o644))

	doc, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, 200, doc.Endpoints[0].Responses[0].StatusCode)
	require.Equal(t, "X-RateLimit-Limit", doc.Endpoints[0].Responses[0].Headers[0].Name)
	require.Equal(t, "X-RateLimit-Remaining", doc.Endpoints[0].Responses[0].Headers[1].Name)
	require.Equal(t, "Retry-After", doc.Endpoints[0].Responses[1].Headers[0].Name)
}

func TestLoad_RejectsDuplicateResponseHeaders(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ir.json")

	require.NoError(t, os.WriteFile(path, []byte(`{
  "schema_version": "v1",
  "api": {"base_path": "/v1"},
  "info": {"title": "Duck", "version": "0.1.0"},
  "endpoints": [{
    "method": "get",
    "path": "/widgets/{id}",
    "operation_id": "getWidget",
    "responses": [{
      "status_code": 200,
      "description": "ok",
      "headers": [
        {"name": "X-Test", "schema": {"type": "string"}},
        {"name": "x-test", "schema": {"type": "string"}}
      ]
    }]
  }]
}`), 0o644))

	_, err := Load(path)
	require.Error(t, err)
	require.ErrorContains(t, err, "duplicate header")
}

func TestLoad_ValidatesResponseShapeMetadata(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ir.json")

	require.NoError(t, os.WriteFile(path, []byte(`{
  "schema_version": "v1",
  "api": {"base_path": "/v1"},
  "info": {"title": "Duck", "version": "0.1.0"},
  "endpoints": [{
    "method": "post",
    "path": "/widgets",
    "operation_id": "createWidget",
    "responses": [{
      "status_code": 201,
      "description": "created",
      "extensions": {
        "x-apigen-response-shape": {
          "kind": "wrapped_json"
        }
      }
    }]
  }]
}`), 0o644))

	_, err := Load(path)
	require.Error(t, err)
	require.ErrorContains(t, err, "wrapped_json body_type is required")
}

func TestLoad_RejectsMissingBasePath(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "ir.json")

	require.NoError(t, os.WriteFile(path, []byte(`{
  "schema_version": "v1",
  "info": {"title": "Duck", "version": "0.1.0"},
  "endpoints": [{"method": "get", "path": "/healthz", "operation_id": "getHealth", "responses": [{"status_code": 200, "description": "ok"}]}]
}`), 0o644))

	_, err := Load(path)
	require.Error(t, err)
	require.ErrorContains(t, err, "api.base_path is required")
}

func TestLoad_RejectsUnknownSchemaRef(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "ir.json")

	require.NoError(t, os.WriteFile(path, []byte(`{
  "schema_version": "v1",
  "api": {"base_path": "/v1"},
  "info": {"title": "Duck", "version": "0.1.0"},
  "endpoints": [{
    "method": "post",
    "path": "/widgets",
    "operation_id": "createWidget",
    "request_body": {"schema": {"ref": "MissingRequest"}},
    "responses": [{"status_code": 201, "description": "created"}]
  }]
}`), 0o644))

	_, err := Load(path)
	require.Error(t, err)
	require.ErrorContains(t, err, `references unknown schema "MissingRequest"`)
}

func TestLoad_RejectsUnsupportedPathArrayParameter(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "ir.json")

	require.NoError(t, os.WriteFile(path, []byte(`{
  "schema_version": "v1",
  "api": {"base_path": "/v1"},
  "info": {"title": "Duck", "version": "0.1.0"},
  "endpoints": [{
    "method": "get",
    "path": "/widgets/{ids}",
    "operation_id": "listWidgetsByIDs",
    "parameters": [{
      "name": "ids",
      "in": "path",
      "required": true,
      "schema": {"type": "array", "items": {"type": "string"}}
    }],
    "responses": [{"status_code": 200, "description": "ok"}]
  }]
}`), 0o644))

	_, err := Load(path)
	require.Error(t, err)
	require.ErrorContains(t, err, "arrays are only supported in query parameters")
}
