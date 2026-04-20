package cli

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/Yacobolo/quackstack/internal/declarative"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

func httpStatusFromError(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	msg := strings.ToLower(err.Error())
	for _, code := range []int{404, 405, 501} {
		needle := fmt.Sprintf("http %d", code)
		if strings.Contains(msg, needle) {
			return code, true
		}
	}
	return 0, false
}

func isOptionalEndpointStatus(status int) bool {
	switch status {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

func (c *APIStateClient) isOptionalReadError(err error) bool {
	if err == nil {
		return false
	}
	if c.compatibilityMode == CapabilityCompatibilityLegacy {
		return true
	}
	if status, ok := httpStatusFromError(err); ok {
		return isOptionalEndpointStatus(status)
	}
	return false
}

// OptionalReadWarnings returns optional-endpoint warnings captured during ReadState.
func (c *APIStateClient) OptionalReadWarnings() []string {
	if len(c.optionalReadWarnings) == 0 {
		return nil
	}
	out := make([]string, len(c.optionalReadWarnings))
	copy(out, c.optionalReadWarnings)
	return out
}

func endpointRequiredByPlan(actions []declarative.Action, kind declarative.ResourceKind) bool {
	for _, action := range actions {
		if action.ResourceKind == kind {
			return true
		}
	}
	return false
}

func (c *APIStateClient) canonicalProjectProbePath(action declarative.Action) (string, error) {
	resolveByName := func(projectName string) (string, error) {
		if c.index == nil {
			return "", fmt.Errorf("resource index not populated; call ReadState first")
		}
		projectID, err := c.resolveProjectIDByName(projectName)
		if err != nil {
			return "", err
		}
		return "/projects/" + projectID, nil
	}

	switch action.ResourceKind {
	case declarative.KindModel:
		if desired, ok := action.Desired.(declarative.ModelResource); ok {
			return resolveByName(desired.ProjectName)
		}
		if actual, ok := action.Actual.(declarative.ModelResource); ok {
			return resolveByName(actual.ProjectName)
		}
		parts := strings.SplitN(action.ResourceName, ".", 2)
		if len(parts) == 2 {
			return resolveByName(parts[0])
		}
	case declarative.KindMacro:
		if desired, ok := action.Desired.(declarative.MacroResource); ok {
			return resolveByName(desired.Spec.ProjectName)
		}
		if actual, ok := action.Actual.(declarative.MacroResource); ok {
			return resolveByName(actual.Spec.ProjectName)
		}
	}

	return "", fmt.Errorf("cannot resolve project scope for %s %q", action.ResourceKind, action.ResourceName)
}

func (c *APIStateClient) probeEndpoint(ctx context.Context, path string) error {
	q := url.Values{}
	q.Set("max_results", "1")
	resp, err := c.client.Do(http.MethodGet, path, q, nil)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	body, err := apiruntime.ReadBody(resp)
	if err != nil {
		return fmt.Errorf("read GET %s: %w", path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s: HTTP %d: %s", path, resp.StatusCode, string(body))
	}
	_ = ctx
	return nil
}

// ValidateApplyCapabilities validates that optional model/macro endpoints
// required by the current plan are available before execution starts.
func (c *APIStateClient) ValidateApplyCapabilities(ctx context.Context, actions []declarative.Action) error {
	if endpointRequiredByPlan(actions, declarative.KindModel) {
		probed := false
		for _, action := range actions {
			if action.ResourceKind != declarative.KindModel {
				continue
			}
			path, err := c.canonicalProjectProbePath(action)
			if err != nil {
				if c.index == nil {
					break
				}
				return fmt.Errorf("model actions present but project scope could not be resolved: %w", err)
			}
			probed = true
			if err := c.probeEndpoint(ctx, path+"/models"); err != nil {
				return fmt.Errorf("model actions present but %s/models endpoint is unavailable: %w", path, err)
			}
			break
		}
		if !probed {
			if err := c.probeEndpoint(ctx, "/workspaces"); err != nil {
				return fmt.Errorf("model actions present but canonical project endpoints could not be probed: %w", err)
			}
		}
	}
	if endpointRequiredByPlan(actions, declarative.KindMacro) {
		probed := false
		for _, action := range actions {
			if action.ResourceKind != declarative.KindMacro {
				continue
			}
			path, err := c.canonicalProjectProbePath(action)
			if err != nil {
				if c.index == nil {
					break
				}
				return fmt.Errorf("macro actions present but project scope could not be resolved: %w", err)
			}
			probed = true
			if err := c.probeEndpoint(ctx, path+"/macros"); err != nil {
				return fmt.Errorf("macro actions present but %s/macros endpoint is unavailable: %w", path, err)
			}
			break
		}
		if !probed {
			if err := c.probeEndpoint(ctx, "/workspaces"); err != nil {
				return fmt.Errorf("macro actions present but canonical project endpoints could not be probed: %w", err)
			}
		}
	}
	if endpointRequiredByPlan(actions, declarative.KindSemanticModel) {
		if err := c.probeEndpoint(ctx, "/workspaces"); err != nil {
			return fmt.Errorf("semantic model actions present but /workspaces endpoint is unavailable: %w", err)
		}
	}
	if endpointRequiredByPlan(actions, declarative.KindAsset) {
		if err := c.probeEndpoint(ctx, "/assets"); err != nil {
			if c.isOptionalReadError(err) {
				return fmt.Errorf("asset actions present but /assets endpoint is unavailable: %w", err)
			}
			return fmt.Errorf("cannot probe /assets endpoint: %w", err)
		}
	}
	return nil
}
