package notebooks

import "sync"

type exploreUpdateHub struct {
	mu      sync.Mutex
	streams map[string]map[chan exploreUpdateParams]struct{}
}

func newExploreUpdateHub() *exploreUpdateHub {
	return &exploreUpdateHub{streams: make(map[string]map[chan exploreUpdateParams]struct{})}
}

func (h *exploreUpdateHub) subscribe(streamID string) (<-chan exploreUpdateParams, func()) {
	ch := make(chan exploreUpdateParams, 8)

	h.mu.Lock()
	if _, ok := h.streams[streamID]; !ok {
		h.streams[streamID] = make(map[chan exploreUpdateParams]struct{})
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

func (h *exploreUpdateHub) publish(streamID string, update exploreUpdateParams) {
	h.mu.Lock()
	subscribers := h.streams[streamID]
	channels := make([]chan exploreUpdateParams, 0, len(subscribers))
	for ch := range subscribers {
		channels = append(channels, ch)
	}
	h.mu.Unlock()

	for _, ch := range channels {
		select {
		case ch <- update:
		default:
		}
	}
}
