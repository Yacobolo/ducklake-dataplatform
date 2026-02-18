// Package macro provides business logic for SQL macro management.
package macro

import (
	"context"
	"sort"

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
		CatalogName: req.CatalogName,
		ProjectName: req.ProjectName,
		Visibility:  req.Visibility,
		Owner:       req.Owner,
		Properties:  req.Properties,
		Tags:        req.Tags,
		Status:      req.Status,
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
	if req.Status != nil {
		if *req.Status != domain.MacroStatusActive && *req.Status != domain.MacroStatusDeprecated {
			return nil, domain.ErrValidation("status must be ACTIVE or DEPRECATED")
		}
	}
	if req.Visibility != nil {
		switch *req.Visibility {
		case domain.MacroVisibilityProject, domain.MacroVisibilityCatalogGlobal, domain.MacroVisibilitySystem:
		default:
			return nil, domain.ErrValidation("visibility must be project, catalog_global, or system")
		}
	}
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

// ListRevisions returns revision history for a macro.
func (s *Service) ListRevisions(ctx context.Context, macroName string) ([]domain.MacroRevision, error) {
	if _, err := s.macros.GetByName(ctx, macroName); err != nil {
		return nil, err
	}
	return s.macros.ListRevisions(ctx, macroName)
}

// DiffRevisions computes a semantic diff between two macro revisions.
func (s *Service) DiffRevisions(ctx context.Context, macroName string, fromVersion, toVersion int) (*domain.MacroRevisionDiff, error) {
	if fromVersion <= 0 || toVersion <= 0 {
		return nil, domain.ErrValidation("from_version and to_version must be greater than zero")
	}
	if _, err := s.macros.GetByName(ctx, macroName); err != nil {
		return nil, err
	}
	fromRev, err := s.macros.GetRevisionByVersion(ctx, macroName, fromVersion)
	if err != nil {
		return nil, err
	}
	toRev, err := s.macros.GetRevisionByVersion(ctx, macroName, toVersion)
	if err != nil {
		return nil, err
	}

	fromParams := append([]string(nil), fromRev.Parameters...)
	toParams := append([]string(nil), toRev.Parameters...)
	sort.Strings(fromParams)
	sort.Strings(toParams)

	d := &domain.MacroRevisionDiff{
		MacroName:          macroName,
		FromVersion:        fromVersion,
		ToVersion:          toVersion,
		FromContentHash:    fromRev.ContentHash,
		ToContentHash:      toRev.ContentHash,
		ParametersChanged:  !equalStringSlices(fromParams, toParams),
		BodyChanged:        fromRev.Body != toRev.Body,
		DescriptionChanged: fromRev.Description != toRev.Description,
		StatusChanged:      fromRev.Status != toRev.Status,
		FromParameters:     fromRev.Parameters,
		ToParameters:       toRev.Parameters,
		FromBody:           fromRev.Body,
		ToBody:             toRev.Body,
		FromDescription:    fromRev.Description,
		ToDescription:      toRev.Description,
		FromStatus:         fromRev.Status,
		ToStatus:           toRev.Status,
	}
	d.Changed = d.ParametersChanged || d.BodyChanged || d.DescriptionChanged || d.StatusChanged
	return d, nil
}

// GetRevisionByVersion returns a specific revision version for a macro.
func (s *Service) GetRevisionByVersion(ctx context.Context, macroName string, version int) (*domain.MacroRevision, error) {
	if version <= 0 {
		return nil, domain.ErrValidation("version must be greater than zero")
	}
	if _, err := s.macros.GetByName(ctx, macroName); err != nil {
		return nil, err
	}
	return s.macros.GetRevisionByVersion(ctx, macroName, version)
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
