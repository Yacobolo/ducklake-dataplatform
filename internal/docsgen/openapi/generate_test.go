package openapi

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupEndpointsByResource_CatalogsPaths(t *testing.T) {
	endpoints := []endpointDoc{
		{Path: "/catalogs", Method: "GET"},
		{Path: "/catalogs/{catalogName}/schemas", Method: "GET"},
		{Path: "/catalogs/{catalogName}/schemas/{schemaName}/tables", Method: "GET"},
		{Path: "/catalogs/{catalogName}/schemas/{schemaName}/views", Method: "GET"},
		{Path: "/manifest", Method: "POST"},
	}

	pages := groupEndpointsByResource("Catalogs", endpoints)

	require.Len(t, pages, 4)
	assert.Equal(t, "Catalogs", pages[0].Title)
	assert.Equal(t, "catalogs", pages[0].Slug)
	assert.Equal(t, "Schemas", pages[1].Title)
	assert.Equal(t, "Tables", pages[2].Title)
	assert.Equal(t, "Views", pages[3].Title)
}

func TestSplitEndpointPages_SplitsLargeTagByResource(t *testing.T) {
	endpoints := make([]endpointDoc, 0, 18)
	for i := 0; i < 6; i++ {
		endpoints = append(endpoints, endpointDoc{Path: "/catalogs", Method: "GET"})
		endpoints = append(endpoints, endpointDoc{Path: "/catalogs/{catalogName}/schemas", Method: "GET"})
		endpoints = append(endpoints, endpointDoc{Path: "/catalogs/{catalogName}/schemas/{schemaName}/tables", Method: "GET"})
	}

	pages := splitEndpointPages("Catalogs", "Catalog operations.", endpoints, apiTagGrouping{})

	require.Len(t, pages, 3)
	assert.Equal(t, "Catalogs", pages[0].Title)
	assert.Equal(t, "Schemas", pages[1].Title)
	assert.Equal(t, "Tables", pages[2].Title)
}

func TestSplitConfiguredEndpointPages_PrefersLongestPrefix(t *testing.T) {
	endpoints := []endpointDoc{
		{Path: "/data-products"},
		{Path: "/data-products/{productSlug}/publish"},
	}

	pages := splitEndpointPages("Products", "Product operations.", endpoints, apiTagGrouping{
		Pages: []apiPageGrouping{
			{Title: "Data Products", Slug: "data-products", MatchPrefixes: []string{"/data-products"}},
			{Title: "Versions and Releases", Slug: "versions", MatchPrefixes: []string{"/data-products/{productSlug}/publish"}},
		},
	})

	require.Len(t, pages, 2)
	assert.Equal(t, "Data Products", pages[0].Title)
	assert.Len(t, pages[0].Endpoints, 1)
	assert.Equal(t, "/data-products", pages[0].Endpoints[0].Path)
	assert.Equal(t, "Versions and Releases", pages[1].Title)
	assert.Len(t, pages[1].Endpoints, 1)
	assert.Equal(t, "/data-products/{productSlug}/publish", pages[1].Endpoints[0].Path)
}

func TestGenerate_RendersExamples(t *testing.T) {
	specDir := t.TempDir()
	specPath := filepath.Join(specDir, "openapi.yaml")
	outDir := filepath.Join(specDir, "docs")
	require.NoError(t, os.WriteFile(specPath, []byte(`openapi: 3.0.0
info:
  title: Example API
  version: 1.0.0
tags:
  - name: Widgets
paths:
  /widgets:
    post:
      tags: [Widgets]
      operationId: createWidget
      summary: Create widget
      parameters:
        - name: dry_run
          in: query
          required: false
          schema:
            type: boolean
          example: true
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Widget'
            example:
              id: widget_123
              name: Example widget
      responses:
        '201':
          description: Created
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Widget'
              example:
                id: widget_123
                name: Example widget
components:
  schemas:
    Widget:
      type: object
      example:
        id: widget_123
        name: Example widget
      required: [id, name]
      properties:
        id:
          type: string
          example: widget_123
        name:
          type: string
          example: Example widget
`), 0o600))

	require.NoError(t, Generate(specPath, outDir, ""))

	endpointPage, err := os.ReadFile(filepath.Join(outDir, "endpoints", "widgets.md"))
	require.NoError(t, err)
	assert.Contains(t, string(endpointPage), "### Request Examples")
	assert.Contains(t, string(endpointPage), "\"id\": \"widget_123\"")
	assert.Contains(t, string(endpointPage), "| `dry_run` | `boolean` | `false` | - | true |")

	schemaPage, err := os.ReadFile(filepath.Join(outDir, "schemas", "widget.md"))
	require.NoError(t, err)
	assert.Contains(t, string(schemaPage), "## Example")
	assert.Contains(t, string(schemaPage), "| `id` | `string` | `true` | - | \"widget_123\" |")
}
