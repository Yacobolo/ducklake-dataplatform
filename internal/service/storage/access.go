package storage

import (
	"context"

	"duck-demo/internal/domain"
)

func isAdmin(ctx context.Context) bool {
	principal, ok := domain.PrincipalFromContext(ctx)
	return ok && principal.IsAdmin
}

func canReadOwnedResource(ctx context.Context, principal, owner string) bool {
	if isAdmin(ctx) {
		return true
	}
	return owner != "" && owner == principal
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
