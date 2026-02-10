package service

import (
	"context"

	"duck-demo/internal/domain"
)

type IntrospectionService struct {
	repo domain.IntrospectionRepository
}

func NewIntrospectionService(repo domain.IntrospectionRepository) *IntrospectionService {
	return &IntrospectionService{repo: repo}
}

func (s *IntrospectionService) ListSchemas(ctx context.Context) ([]domain.Schema, error) {
	return s.repo.ListSchemas(ctx)
}

func (s *IntrospectionService) ListTables(ctx context.Context, schemaID int64) ([]domain.Table, error) {
	return s.repo.ListTables(ctx, schemaID)
}

func (s *IntrospectionService) ListColumns(ctx context.Context, tableID int64) ([]domain.Column, error) {
	return s.repo.ListColumns(ctx, tableID)
}
