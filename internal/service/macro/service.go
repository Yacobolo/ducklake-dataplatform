// Package macro provides business logic for SQL macro management.
package macro

import (
	"context"

	"duck-demo/internal/domain"
)

// Service provides business logic for macro management.
type Service struct {
	macros domain.MacroRepository
	audit  domain.AuditRepository
}

// NewService creates a new macro Service.
func NewService(macros domain.MacroRepository, audit domain.AuditRepository) *Service {
	return &Service{macros: macros, audit: audit}
}

// Create creates a new SQL macro.
func (s *Service) Create(ctx context.Context, principal string, req domain.CreateMacroRequest) (*domain.Macro, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	m := &domain.Macro{
		Name:        req.Name,
		MacroType:   req.MacroType,
		Parameters:  req.Parameters,
		Body:        req.Body,
		Description: req.Description,
		CreatedBy:   principal,
	}
	if m.Parameters == nil {
		m.Parameters = []string{}
	}

	result, err := s.macros.Create(ctx, m)
	if err != nil {
		return nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "create_macro",
		Status:        "SUCCESS",
	})

	return result, nil
}

// Get retrieves a macro by name.
func (s *Service) Get(ctx context.Context, name string) (*domain.Macro, error) {
	return s.macros.GetByName(ctx, name)
}

// List returns a paginated list of macros.
func (s *Service) List(ctx context.Context, page domain.PageRequest) ([]domain.Macro, int64, error) {
	return s.macros.List(ctx, page)
}

// Update updates a macro.
func (s *Service) Update(ctx context.Context, principal, name string, req domain.UpdateMacroRequest) (*domain.Macro, error) {
	result, err := s.macros.Update(ctx, name, req)
	if err != nil {
		return nil, err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "update_macro",
		Status:        "SUCCESS",
	})

	return result, nil
}

// Delete deletes a macro.
func (s *Service) Delete(ctx context.Context, principal, name string) error {
	if err := s.macros.Delete(ctx, name); err != nil {
		return err
	}

	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "delete_macro",
		Status:        "SUCCESS",
	})

	return nil
}
