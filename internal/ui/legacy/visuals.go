package legacy

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
		Class("block min-h-[20rem]"),
		Attr("data-chart-payload", chartPayload(columns, rows, visual)),
	)
}

func visualMetricCard(title string, value interface{}, secondary string) Node {
	return Div(
		Class("relative overflow-hidden rounded-xl border border-[var(--borderColor-default)] bg-[linear-gradient(135deg,var(--bgColor-accent-muted)_0%,var(--bgColor-default)_45%)] p-4 shadow-[var(--shadow-resting-small)] before:absolute before:inset-y-0 before:left-0 before:w-1 before:bg-[var(--borderColor-accent-emphasis)] before:content-['']"),
		P(Class("m-0 text-xs font-semibold text-[var(--fgColor-default)]"), Text(title)),
		P(Class("my-1 text-3xl font-semibold leading-[var(--text-title-lineHeight-medium)] text-[var(--fgColor-default)]"), Text(fmt.Sprint(value))),
		P(Class("m-0 text-xs text-[var(--fgColor-muted)]"), Text(secondary)),
	)
}
