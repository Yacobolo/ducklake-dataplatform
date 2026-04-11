package apiruntime

import (
	"net/url"

	cobraruntime "duck-demo/pkg/apigen/runtime/cobra"
)

// PaginatedResponse is the minimal envelope used by FetchAllPages.
type PaginatedResponse = cobraruntime.PaginatedResponse

// FetchAllPages follows next_page_token until the resource is exhausted.
func FetchAllPages(client *Client, method, path string, baseQuery url.Values) ([]interface{}, error) {
	return cobraruntime.FetchAllPages(client, method, path, baseQuery)
}
