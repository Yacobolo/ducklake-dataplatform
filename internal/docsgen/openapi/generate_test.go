package openapi

import (
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
