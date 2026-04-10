package dashboards

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"

	"duck-demo/internal/domain"
	dashboardsvc "duck-demo/internal/service/dashboard"
)

type dashboardUpdateHub struct {
	mu      sync.Mutex
	streams map[string]map[chan dashboardUpdateMessage]struct{}
}

func newDashboardUpdateHub() *dashboardUpdateHub {
	return &dashboardUpdateHub{
		streams: make(map[string]map[chan dashboardUpdateMessage]struct{}),
	}
}

type dashboardUpdateMessage struct {
	FilterKey string
	Version   string
	RawOriginFilters []string
	Filters   []dashboardsvc.InteractiveFilter
	TablePage *dashboardTablePageRequest
}

type dashboardTablePageRequest struct {
	WidgetID         string
	Offset           int
	Limit            int
	Append           bool
	SortColumn       string
	SortDirection    string
	FilterKey        string
	Version          string
	RawOriginFilters []string
	Filters          []dashboardsvc.InteractiveFilter
}

func (h *dashboardUpdateHub) subscribe(streamID string) (<-chan dashboardUpdateMessage, func()) {
	ch := make(chan dashboardUpdateMessage, 8)

	h.mu.Lock()
	if _, ok := h.streams[streamID]; !ok {
		h.streams[streamID] = make(map[chan dashboardUpdateMessage]struct{})
	}
	h.streams[streamID][ch] = struct{}{}
	h.mu.Unlock()

	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()

		subscribers, ok := h.streams[streamID]
		if !ok {
			return
		}
		delete(subscribers, ch)
		close(ch)
		if len(subscribers) == 0 {
			delete(h.streams, streamID)
		}
	}
}

func (h *dashboardUpdateHub) publishFilters(streamID, filterKey, version string, rawOriginFilters []string, filters []dashboardsvc.InteractiveFilter) {
	h.mu.Lock()
	subscribers := h.streams[streamID]
	channels := make([]chan dashboardUpdateMessage, 0, len(subscribers))
	for ch := range subscribers {
		channels = append(channels, ch)
	}
	h.mu.Unlock()

	for _, ch := range channels {
		select {
		case ch <- dashboardUpdateMessage{
			FilterKey:        filterKey,
			Version:          version,
			RawOriginFilters: append([]string(nil), rawOriginFilters...),
			Filters:          cloneInteractiveFilters(filters),
		}:
		default:
		}
	}
}

func (h *dashboardUpdateHub) publishTablePage(streamID string, req dashboardTablePageRequest) {
	h.mu.Lock()
	subscribers := h.streams[streamID]
	channels := make([]chan dashboardUpdateMessage, 0, len(subscribers))
	for ch := range subscribers {
		channels = append(channels, ch)
	}
	h.mu.Unlock()

	for _, ch := range channels {
		select {
		case ch <- dashboardUpdateMessage{
			Filters: cloneInteractiveFilters(req.Filters),
			TablePage: &dashboardTablePageRequest{
				WidgetID:         req.WidgetID,
				Offset:           req.Offset,
				Limit:            req.Limit,
				Append:           req.Append,
				SortColumn:       req.SortColumn,
				SortDirection:    req.SortDirection,
				FilterKey:        req.FilterKey,
				Version:          req.Version,
				RawOriginFilters: append([]string(nil), req.RawOriginFilters...),
				Filters:          cloneInteractiveFilters(req.Filters),
			},
		}:
		default:
		}
	}
}

type dashboardUpdateRequest struct {
	CSRFToken     string     `json:"csrfToken"`
	OriginFilters []string   `json:"originFilters"`
	Version       string     `json:"version"`
	URLParams     filterData `json:"urlParams"`
}

type dashboardTablePageUpdateRequest struct {
	CSRFToken     string     `json:"csrfToken"`
	OriginFilters []string   `json:"originFilters"`
	Version       string     `json:"version"`
	URLParams     filterData `json:"urlParams"`
	WidgetID      string     `json:"widgetId"`
	Offset        int        `json:"offset"`
	Limit         int        `json:"limit"`
	Append        bool       `json:"append"`
	SortColumn    string     `json:"sortColumn"`
	SortDirection string     `json:"sortDirection"`
}

type filterData struct {
	OriginFilters []string `json:"fo"`
}

func decodeDashboardUpdateRequest(r *http.Request) ([]string, string, error) {
	var payload dashboardUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		return nil, "", err
	}

	rawOriginFilters := payload.OriginFilters
	if len(rawOriginFilters) == 0 {
		rawOriginFilters = payload.URLParams.OriginFilters
	}
	return sanitizeOriginFilterRaw(rawOriginFilters), strings.TrimSpace(payload.Version), nil
}

func decodeDashboardTablePageRequest(r *http.Request) (*dashboardTablePageRequest, error) {
	var payload dashboardTablePageUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		return nil, err
	}

	rawOriginFilters := payload.OriginFilters
	if len(rawOriginFilters) == 0 {
		rawOriginFilters = payload.URLParams.OriginFilters
	}
	limit := payload.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	offset := payload.Offset
	if offset < 0 {
		offset = 0
	}

	return &dashboardTablePageRequest{
		WidgetID:         strings.TrimSpace(payload.WidgetID),
		Offset:           offset,
		Limit:            limit,
		Append:           payload.Append,
		SortColumn:       strings.TrimSpace(payload.SortColumn),
		SortDirection:    strings.ToLower(strings.TrimSpace(payload.SortDirection)),
		Version:          strings.TrimSpace(payload.Version),
		RawOriginFilters: sanitizeOriginFilterRaw(rawOriginFilters),
	}, nil
}

func interactiveFiltersFromOriginRaw(rawOriginFilters []string, widgets []domain.DashboardWidget) []dashboardsvc.InteractiveFilter {
	if len(rawOriginFilters) == 0 {
		return nil
	}
	originKeyToID := make(map[string]string, len(widgets))
	for _, widget := range widgets {
		key := strings.TrimSpace(widget.FilterOriginKey)
		if key == "" {
			continue
		}
		originKeyToID[key] = strings.TrimSpace(widget.ID)
	}
	return interactiveFiltersFromGroupedRaw(rawOriginFilters, func(raw string) (dashboardsvc.InteractiveFilter, bool) {
		widgetKey, remainder, ok := strings.Cut(raw, "|")
		if !ok {
			return dashboardsvc.InteractiveFilter{}, false
		}
		dimension, value, ok := strings.Cut(remainder, ":")
		widgetKey = strings.TrimSpace(widgetKey)
		dimension = strings.TrimSpace(dimension)
		value = strings.TrimSpace(value)
		widgetID := originKeyToID[widgetKey]
		if widgetID == "" || !ok || dimension == "" || value == "" {
			return dashboardsvc.InteractiveFilter{}, false
		}
		return dashboardsvc.InteractiveFilter{
			WidgetID:  widgetID,
			Dimension: dimension,
			Values:    []string{value},
		}, true
	})
}

func sanitizeOriginFilterRaw(rawOriginFilters []string) []string {
	if len(rawOriginFilters) == 0 {
		return nil
	}

	out := make([]string, 0, len(rawOriginFilters))
	seen := make(map[string]struct{}, len(rawOriginFilters))
	for _, raw := range rawOriginFilters {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if _, ok := seen[raw]; ok {
			continue
		}
		seen[raw] = struct{}{}
		out = append(out, raw)
	}
	return out
}

func dashboardFilterKeyFromOriginRaw(rawOriginFilters []string) string {
	rawOriginFilters = sanitizeOriginFilterRaw(rawOriginFilters)
	if len(rawOriginFilters) == 0 {
		return ""
	}
	parts := append([]string(nil), rawOriginFilters...)
	sort.Strings(parts)
	return strings.Join(parts, "|")
}

func interactiveFiltersFromGroupedRaw[T ~string](rawFilters []T, parser func(string) (dashboardsvc.InteractiveFilter, bool)) []dashboardsvc.InteractiveFilter {
	if len(rawFilters) == 0 {
		return nil
	}

	order := make([]string, 0, len(rawFilters))
	grouped := make(map[string][]string)
	widgetIDs := make(map[string]string)
	for _, raw := range rawFilters {
		filter, ok := parser(string(raw))
		if !ok || filter.Dimension == "" || len(filter.Values) == 0 {
			continue
		}
		groupKey := strings.TrimSpace(filter.WidgetID) + "\x00" + strings.TrimSpace(filter.Dimension)
		if _, seen := grouped[groupKey]; !seen {
			order = append(order, groupKey)
			widgetIDs[groupKey] = strings.TrimSpace(filter.WidgetID)
		}
		grouped[groupKey] = append(grouped[groupKey], filter.Values...)
	}

	out := make([]dashboardsvc.InteractiveFilter, 0, len(order))
	for _, groupKey := range order {
		dimension := strings.TrimPrefix(groupKey, widgetIDs[groupKey]+"\x00")
		out = append(out, dashboardsvc.InteractiveFilter{
			WidgetID:  widgetIDs[groupKey],
			Dimension: dimension,
			Values:    append([]string(nil), grouped[groupKey]...),
		})
	}
	return out
}

func cloneInteractiveFilters(filters []dashboardsvc.InteractiveFilter) []dashboardsvc.InteractiveFilter {
	if len(filters) == 0 {
		return nil
	}

	out := make([]dashboardsvc.InteractiveFilter, 0, len(filters))
	for _, filter := range filters {
		out = append(out, dashboardsvc.InteractiveFilter{
			WidgetID:  filter.WidgetID,
			Dimension: filter.Dimension,
			Values:    append([]string(nil), filter.Values...),
		})
	}
	return out
}
