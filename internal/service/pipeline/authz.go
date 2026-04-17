package pipeline

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	servicepolicy "github.com/Yacobolo/quackstack/internal/service/policy"
)

func (s *Service) canViewPipeline(ctx context.Context, principal string, pipeline *domain.Pipeline) (bool, error) {
	if pipeline == nil {
		return false, domain.ErrNotFound("pipeline not found")
	}
	if servicepolicy.IsAdmin(ctx) || strings.TrimSpace(principal) == strings.TrimSpace(pipeline.CreatedBy) {
		return true, nil
	}
	if s.auth == nil {
		return true, nil
	}
	return servicepolicy.HasAnySecurablePrivilege(
		ctx,
		s.auth,
		principal,
		domain.SecurablePipeline,
		pipeline.ID,
		domain.PrivViewPipeline,
		domain.PrivRunPipeline,
		domain.PrivManagePipeline,
	)
}

func (s *Service) requirePipelineView(ctx context.Context, principal string, pipeline *domain.Pipeline) error {
	allowed, err := s.canViewPipeline(ctx, principal, pipeline)
	if err != nil {
		return err
	}
	if !allowed {
		return domain.ErrAccessDenied("%q lacks VIEW on pipeline %q", principal, pipeline.Name)
	}
	return nil
}

func (s *Service) requirePipelineRun(ctx context.Context, principal string, pipeline *domain.Pipeline) error {
	if pipeline == nil {
		return domain.ErrNotFound("pipeline not found")
	}
	if servicepolicy.IsAdmin(ctx) || strings.TrimSpace(principal) == strings.TrimSpace(pipeline.CreatedBy) {
		return nil
	}
	if s.auth == nil {
		return nil
	}
	return servicepolicy.RequireSecurablePrivilege(ctx, s.auth, principal, domain.SecurablePipeline, pipeline.ID, domain.PrivRunPipeline)
}

func (s *Service) requirePipelineManage(ctx context.Context, principal string, pipeline *domain.Pipeline) error {
	if pipeline == nil {
		return domain.ErrNotFound("pipeline not found")
	}
	if servicepolicy.IsAdmin(ctx) || strings.TrimSpace(principal) == strings.TrimSpace(pipeline.CreatedBy) {
		return nil
	}
	if s.auth == nil {
		return nil
	}
	return servicepolicy.RequireSecurablePrivilege(ctx, s.auth, principal, domain.SecurablePipeline, pipeline.ID, domain.PrivManagePipeline)
}

func normalizeAdmissionMode(mode string) string {
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "", domain.PipelineAdmissionModeReject:
		return domain.PipelineAdmissionModeReject
	case domain.PipelineAdmissionModeQueue:
		return domain.PipelineAdmissionModeQueue
	default:
		return mode
	}
}

func validateNotificationWebhooks(hooks []domain.PipelineNotificationWebhook) error {
	for _, hook := range hooks {
		if strings.TrimSpace(hook.URL) == "" {
			return domain.ErrValidation("notification webhook url is required")
		}
		parsed, err := url.Parse(strings.TrimSpace(hook.URL))
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return domain.ErrValidation("notification webhook url %q is invalid", hook.URL)
		}
		for _, eventType := range hook.Events {
			switch strings.TrimSpace(eventType) {
			case "", domain.PipelineRunEventFailed, domain.PipelineRunEventCancelled, domain.PipelineRunEventSucceeded, domain.PipelineRunEventSLABreach, domain.PipelineRunEventRepaired, domain.PipelineRunEventRepairFailed:
			default:
				return domain.ErrValidation("notification webhook event %q is not supported", eventType)
			}
		}
	}
	return nil
}

func (s *Service) validateRunAsPrincipal(ctx context.Context, value *string, action string) error {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil
	}
	if err := servicepolicy.RequireAdminIfPresentForAction(ctx, action); err != nil {
		return err
	}
	if s.principals == nil {
		return nil
	}
	if _, err := s.principals.GetByName(ctx, strings.TrimSpace(*value)); err != nil {
		return fmt.Errorf("validate run_as_principal: %w", err)
	}
	return nil
}

func (s *Service) validatePipelineDefaults(ctx context.Context, computeEndpointID *string, notificationWebhooks []domain.PipelineNotificationWebhook) error {
	if err := s.validateComputeEndpoint(ctx, computeEndpointID); err != nil {
		return err
	}
	if err := validateNotificationWebhooks(notificationWebhooks); err != nil {
		return err
	}
	return nil
}

func (s *Service) grantCreatorPipelinePrivileges(ctx context.Context, pipelineID, principalName string) error {
	if s.grants == nil || s.principals == nil || strings.TrimSpace(principalName) == "" {
		return nil
	}
	principal, err := s.principals.GetByName(ctx, principalName)
	if err != nil {
		return fmt.Errorf("lookup creator principal: %w", err)
	}
	for _, privilege := range []string{domain.PrivViewPipeline, domain.PrivRunPipeline, domain.PrivManagePipeline} {
		_, grantErr := s.grants.Grant(ctx, &domain.PrivilegeGrant{
			ID:            domain.NewID(),
			PrincipalID:   principal.ID,
			PrincipalType: "user",
			SecurableType: domain.SecurablePipeline,
			SecurableID:   pipelineID,
			Privilege:     privilege,
			GrantedBy:     pipelineMessagePtr(principalName),
		})
		if grantErr != nil {
			var conflict *domain.ConflictError
			if !errors.As(grantErr, &conflict) {
				return fmt.Errorf("grant %s on pipeline: %w", privilege, grantErr)
			}
		}
	}
	return nil
}

func pipelineMessagePtr(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	v := strings.TrimSpace(value)
	return &v
}

func servicePrincipalName(ctx context.Context) string {
	if principal, ok := domain.PrincipalFromContext(ctx); ok {
		return principal.Name
	}
	return ""
}
