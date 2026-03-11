package ui

import (
	"encoding/json"
	"fmt"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

type chartRenderPayload struct {
	Columns []string           `json:"columns"`
	Rows    [][]interface{}    `json:"rows"`
	Visual  *domain.VisualSpec `json:"visual"`
}

func chartPayload(columns []string, rows [][]interface{}, visual *domain.VisualSpec) string {
	payload, err := json.Marshal(chartRenderPayload{
		Columns: columns,
		Rows:    rows,
		Visual:  visual,
	})
	if err != nil {
		return "{}"
	}
	return string(payload)
}

func chartHost(columns []string, rows [][]interface{}, visual *domain.VisualSpec) Node {
	return El(
		"duck-chart",
		Class("chart-host"),
		Attr("data-chart-payload", chartPayload(columns, rows, visual)),
	)
}

func visualMetricCard(title string, value interface{}, secondary string) Node {
	return Div(
		Class("metric-card metric-card-accent"),
		P(Class("metric-label"), Text(title)),
		P(Class("metric-value"), Text(fmt.Sprint(value))),
		P(Class("color-fg-muted text-small mb-0"), Text(secondary)),
	)
}
