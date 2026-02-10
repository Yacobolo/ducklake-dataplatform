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

func (s *IntrospectionService) ListSchemas(ctx context.Context, page domain.PageRequest) ([]domain.Schema, int64, error) {
	return s.repo.ListSchemas(ctx, page)
}

func (s *IntrospectionService) ListTables(ctx context.Context, schemaID int64, page domain.PageRequest) ([]domain.Table, int64, error) {
	return s.repo.ListTables(ctx, schemaID, page)
}

func (s *IntrospectionService) ListColumns(ctx context.Context, tableID int64, page domain.PageRequest) ([]domain.Column, int64, error) {
	return s.repo.ListColumns(ctx, tableID, page)
}
