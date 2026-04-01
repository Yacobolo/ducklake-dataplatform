package dashboards

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"

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
	Filters   []dashboardsvc.InteractiveFilter
	TablePage *dashboardTablePageRequest
}

type dashboardTablePageRequest struct {
	WidgetID string
	Offset   int
	Limit    int
	Filters  []dashboardsvc.InteractiveFilter
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

func (h *dashboardUpdateHub) publishFilters(streamID string, filters []dashboardsvc.InteractiveFilter) {
	h.mu.Lock()
	subscribers := h.streams[streamID]
	channels := make([]chan dashboardUpdateMessage, 0, len(subscribers))
	for ch := range subscribers {
		channels = append(channels, ch)
	}
	h.mu.Unlock()

	for _, ch := range channels {
		select {
		case ch <- dashboardUpdateMessage{Filters: cloneInteractiveFilters(filters)}:
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
				WidgetID: req.WidgetID,
				Offset:   req.Offset,
				Limit:    req.Limit,
				Filters:  cloneInteractiveFilters(req.Filters),
			},
		}:
		default:
		}
	}
}

type dashboardUpdateRequest struct {
	CSRFToken     string     `json:"csrfToken"`
	OriginFilters []string   `json:"originFilters"`
	URLParams     filterData `json:"urlParams"`
}

type dashboardTablePageUpdateRequest struct {
	CSRFToken     string     `json:"csrfToken"`
	OriginFilters []string   `json:"originFilters"`
	URLParams     filterData `json:"urlParams"`
	WidgetID      string     `json:"widgetId"`
	Offset        int        `json:"offset"`
	Limit         int        `json:"limit"`
}

type filterData struct {
	OriginFilters []string `json:"fo"`
}

func decodeDashboardUpdateRequest(r *http.Request) ([]dashboardsvc.InteractiveFilter, error) {
	var payload dashboardUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		return nil, err
	}

	rawOriginFilters := payload.OriginFilters
	if len(rawOriginFilters) == 0 {
		rawOriginFilters = payload.URLParams.OriginFilters
	}
	return interactiveFiltersFromOriginRaw(rawOriginFilters), nil
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
		WidgetID: strings.TrimSpace(payload.WidgetID),
		Offset:   offset,
		Limit:    limit,
		Filters:  interactiveFiltersFromOriginRaw(rawOriginFilters),
	}, nil
}

func interactiveFiltersFromOriginRaw(rawOriginFilters []string) []dashboardsvc.InteractiveFilter {
	if len(rawOriginFilters) == 0 {
		return nil
	}
	return interactiveFiltersFromGroupedRaw(rawOriginFilters, func(raw string) (dashboardsvc.InteractiveFilter, bool) {
		widgetID, remainder, ok := strings.Cut(raw, "|")
		if !ok {
			return dashboardsvc.InteractiveFilter{}, false
		}
		dimension, value, ok := strings.Cut(remainder, ":")
		widgetID = strings.TrimSpace(widgetID)
		dimension = strings.TrimSpace(dimension)
		value = strings.TrimSpace(value)
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
