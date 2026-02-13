package governance

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
)

// TagService provides tag management operations.
type TagService struct {
	repo  domain.TagRepository
	audit domain.AuditRepository
}

// NewTagService creates a new TagService.
func NewTagService(repo domain.TagRepository, audit domain.AuditRepository) *TagService {
	return &TagService{repo: repo, audit: audit}
}

// CreateTag creates a new tag definition.
func (s *TagService) CreateTag(ctx context.Context, principal string, req domain.CreateTagRequest) (*domain.Tag, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	tag := &domain.Tag{
		Key:       req.Key,
		Value:     req.Value,
		CreatedBy: principal,
	}

	// Validate classification tag values
	if err := validateClassificationTag(tag); err != nil {
		return nil, err
	}

	result, err := s.repo.CreateTag(ctx, tag)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "CREATE_TAG", fmt.Sprintf("Created tag %q", req.Key))
	return result, nil
}

// GetTag returns a tag by ID.
func (s *TagService) GetTag(ctx context.Context, id string) (*domain.Tag, error) {
	return s.repo.GetTag(ctx, id)
}

// ListTags returns a paginated list of tags.
func (s *TagService) ListTags(ctx context.Context, page domain.PageRequest) ([]domain.Tag, int64, error) {
	return s.repo.ListTags(ctx, page)
}

// DeleteTag deletes a tag by ID.
func (s *TagService) DeleteTag(ctx context.Context, principal string, id string) error {

	if err := s.repo.DeleteTag(ctx, id); err != nil {
		return err
	}

	s.logAudit(ctx, principal, "DELETE_TAG", fmt.Sprintf("Deleted tag %s", id))
	return nil
}

// AssignTag assigns a tag to a securable object.
func (s *TagService) AssignTag(ctx context.Context, principal string, req domain.AssignTagRequest) (*domain.TagAssignment, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	assignment := &domain.TagAssignment{
		TagID:         req.TagID,
		SecurableType: req.SecurableType,
		SecurableID:   req.SecurableID,
		ColumnName:    req.ColumnName,
		AssignedBy:    principal,
	}

	result, err := s.repo.AssignTag(ctx, assignment)
	if err != nil {
		return nil, err
	}

	s.logAudit(ctx, principal, "ASSIGN_TAG", fmt.Sprintf("Assigned tag %s to %s %s", req.TagID, req.SecurableType, req.SecurableID))
	return result, nil
}

// UnassignTag removes a tag assignment.
func (s *TagService) UnassignTag(ctx context.Context, principal string, id string) error {

	if err := s.repo.UnassignTag(ctx, id); err != nil {
		return err
	}

	s.logAudit(ctx, principal, "UNASSIGN_TAG", fmt.Sprintf("Removed tag assignment %s", id))
	return nil
}

// ListTagsForSecurable returns all tags assigned to a securable object.
func (s *TagService) ListTagsForSecurable(ctx context.Context, securableType string, securableID string, columnName *string) ([]domain.Tag, error) {
	return s.repo.ListTagsForSecurable(ctx, securableType, securableID, columnName)
}

// ListAssignmentsForTag returns all assignments for a given tag.
func (s *TagService) ListAssignmentsForTag(ctx context.Context, tagID string) ([]domain.TagAssignment, error) {
	return s.repo.ListAssignmentsForTag(ctx, tagID)
}

func validateClassificationTag(tag *domain.Tag) error {
	for prefix, validValues := range domain.ValidClassifications {
		if tag.Key == prefix {
			if tag.Value == nil {
				return domain.ErrValidation("tag key %q requires a value", prefix)
			}
			for _, v := range validValues {
				if *tag.Value == v {
					return nil
				}
			}
			return domain.ErrValidation("invalid value %q for tag key %q; valid values: %v", *tag.Value, prefix, validValues)
		}
	}
	return nil
}

func (s *TagService) logAudit(ctx context.Context, principal, action, detail string) {
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        action,
		Status:        "ALLOWED",
		OriginalSQL:   &detail,
	})
}
