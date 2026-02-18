package cli

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"duck-demo/internal/declarative"
	"duck-demo/pkg/cli/gen"
)

// CapabilityCompatibilityMode controls optional-endpoint compatibility behavior.
type CapabilityCompatibilityMode string

const (
	// CapabilityCompatibilityStrict only treats explicit endpoint absence HTTP
	// statuses as optional.
	CapabilityCompatibilityStrict CapabilityCompatibilityMode = "strict"
	// CapabilityCompatibilityLegacy preserves legacy behavior and also tolerates
	// a set of transport-level errors as optional.
	CapabilityCompatibilityLegacy CapabilityCompatibilityMode = "legacy"
)

// APIStateClientOptions configures APIStateClient behavior.
type APIStateClientOptions struct {
	CompatibilityMode CapabilityCompatibilityMode
}

func normalizeCompatibilityMode(mode CapabilityCompatibilityMode) CapabilityCompatibilityMode {
	if mode == "" {
		return CapabilityCompatibilityStrict
	}
	if mode != CapabilityCompatibilityLegacy {
		return CapabilityCompatibilityStrict
	}
	return mode
}

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
	if status, ok := httpStatusFromError(err); ok {
		return isOptionalEndpointStatus(status)
	}
	if c.compatibilityMode == CapabilityCompatibilityLegacy {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "eof") || strings.Contains(msg, "connection reset by peer") || strings.Contains(msg, "broken pipe") {
			return true
		}
	}
	return false
}

// OptionalReadWarnings returns model/macro compatibility warnings captured during
// ReadState.
func (c *APIStateClient) OptionalReadWarnings() []string {
	if len(c.optionalReadWarnings) == 0 {
		return nil
	}
	out := make([]string, len(c.optionalReadWarnings))
	copy(out, c.optionalReadWarnings)
	return out
}

func (c *APIStateClient) addOptionalReadWarning(resource string, err error) {
	if err == nil {
		return
	}
	if status, ok := httpStatusFromError(err); ok {
		c.optionalReadWarnings = append(c.optionalReadWarnings,
			fmt.Sprintf("%s endpoint unavailable (HTTP %d); continuing without %s state", resource, status, resource))
		return
	}
	c.optionalReadWarnings = append(c.optionalReadWarnings,
		fmt.Sprintf("%s endpoint read failed in compatibility mode: %v", resource, err))
}

func endpointRequiredByPlan(actions []declarative.Action, kind declarative.ResourceKind) bool {
	for _, action := range actions {
		if action.ResourceKind == kind {
			return true
		}
	}
	return false
}

func (c *APIStateClient) probeEndpoint(ctx context.Context, path string) error {
	q := url.Values{}
	q.Set("max_results", "1")
	resp, err := c.client.Do(http.MethodGet, path, q, nil)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	body, err := gen.ReadBody(resp)
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
		if err := c.probeEndpoint(ctx, "/models"); err != nil {
			if c.isOptionalReadError(err) {
				return fmt.Errorf("model actions present but /models endpoint is unavailable: %w", err)
			}
			return fmt.Errorf("cannot probe /models endpoint: %w", err)
		}
	}
	if endpointRequiredByPlan(actions, declarative.KindMacro) {
		if err := c.probeEndpoint(ctx, "/macros"); err != nil {
			if c.isOptionalReadError(err) {
				return fmt.Errorf("macro actions present but /macros endpoint is unavailable: %w", err)
			}
			return fmt.Errorf("cannot probe /macros endpoint: %w", err)
		}
	}
	return nil
}
