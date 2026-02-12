package domain

import (
	"encoding/base64"
	"fmt"
	"strconv"
)

// DefaultMaxResults is the default page size when none is specified.
const DefaultMaxResults = 100

// MaxMaxResults is the maximum allowed page size.
const MaxMaxResults = 1000

// PageRequest holds pagination parameters for list operations.
type PageRequest struct {
	MaxResults int
	PageToken  string // opaque token (base64-encoded offset)
}

// Offset decodes the page token into an integer offset.
// Returns 0 if the token is empty or invalid.
func (p PageRequest) Offset() int {
	if p.PageToken == "" {
		return 0
	}
	decoded, err := base64.StdEncoding.DecodeString(p.PageToken)
	if err != nil {
		return 0
	}
	offset, err := strconv.Atoi(string(decoded))
	if err != nil {
		return 0
	}
	return offset
}

// Limit returns the effective page size, clamped to [1, MaxMaxResults].
func (p PageRequest) Limit() int {
	if p.MaxResults <= 0 {
		return DefaultMaxResults
	}
	if p.MaxResults > MaxMaxResults {
		return MaxMaxResults
	}
	return p.MaxResults
}

// EncodePageToken creates an opaque page token from an offset.
// Returns empty string if offset is 0 or negative.
func EncodePageToken(offset int) string {
	if offset <= 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%d", offset)))
}

// NextPageToken calculates the next page token based on current offset, limit, and total count.
// Returns empty string if there are no more pages.
func NextPageToken(offset, limit int, total int64) string {
	next := offset + limit
	if int64(next) >= total {
		return ""
	}
	return EncodePageToken(next)
}
