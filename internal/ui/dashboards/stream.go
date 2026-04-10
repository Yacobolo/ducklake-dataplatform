package dashboards

import (
	"encoding/json"
	"fmt"

	. "maragu.dev/gomponents"

	"github.com/starfederation/datastar-go/datastar"
)

const (
	dashboardWidgetPayloadEventName = "dashboard-widget-payload"
	dashboardSurfaceSelector        = "#dashboard-view-content"
)

type dashboardStream struct {
	sse *datastar.ServerSentEventGenerator
}

func newDashboardStream(sse *datastar.ServerSentEventGenerator) *dashboardStream {
	return &dashboardStream{sse: sse}
}

func (s *dashboardStream) patchSurface(surface Node) error {
	if s == nil || s.sse == nil {
		return fmt.Errorf("dashboard stream is not initialized")
	}
	return s.sse.PatchElementGostar(
		surface,
		datastar.WithSelectorID("dashboard-view-content"),
	)
}

func (s *dashboardStream) dispatchWidgetPayload(payload dashboardWidgetPayloadEvent) error {
	if s == nil || s.sse == nil {
		return fmt.Errorf("dashboard stream is not initialized")
	}
	script, err := dashboardWidgetPayloadScript(payload)
	if err != nil {
		return err
	}
	return s.sse.ExecuteScript(
		script,
		datastar.WithExecuteScriptAutoRemove(true),
	)
}

func dashboardWidgetPayloadScript(payload dashboardWidgetPayloadEvent) (string, error) {
	detailJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal dashboard widget payload: %w", err)
	}
	return fmt.Sprintf(
		`(() => {
  const detail = %s;
  const bus = window.__dashboardWidgetPayloadBus || (window.__dashboardWidgetPayloadBus = {});
  if (detail && detail.version && detail.widget_id) {
    bus[detail.version + ":" + detail.widget_id] = detail;
  }
  window.dispatchEvent(new CustomEvent(%q, { detail }));
})();`,
		string(detailJSON),
		dashboardWidgetPayloadEventName,
	), nil
}
