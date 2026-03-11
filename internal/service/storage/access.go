package storage

import (
	"context"

	"duck-demo/internal/domain"
	servicepolicy "duck-demo/internal/service/policy"
)

func isAdmin(ctx context.Context) bool {
	return servicepolicy.IsAdmin(ctx)
}

func canReadOwnedResource(ctx context.Context, principal, owner string) bool {
	return servicepolicy.CanReadOwnedResource(ctx, principal, owner)
}

func paginateSlice[T any](items []T, page domain.PageRequest) ([]T, int64) {
	start := page.Offset()
	if start > len(items) {
		start = len(items)
	}
	end := start + page.Limit()
	if end > len(items) {
		end = len(items)
	}
	return items[start:end], int64(len(items))
}
