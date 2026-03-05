package api

import "net/http"

// GenStrictAdapter dispatches APIGen routes directly to strict handler methods.
type GenStrictAdapter struct {
	handler StrictServerInterface
}

// NewAPIGenStrictAdapter creates an adapter backed by strict handler methods.
func NewAPIGenStrictAdapter(handler StrictServerInterface) *GenStrictAdapter {
	return &GenStrictAdapter{handler: handler}
}

// HandleAPIGen dispatches using APIGen-owned transport parsing/response handling.
func (a *GenStrictAdapter) HandleAPIGen(operationID string, w http.ResponseWriter, r *http.Request) {
	if ok := DispatchAPIGenStrictOperation(operationID, a.handler, w, r); !ok {
		http.NotFound(w, r)
	}
}

var _ GenServerInterface = (*GenStrictAdapter)(nil)
