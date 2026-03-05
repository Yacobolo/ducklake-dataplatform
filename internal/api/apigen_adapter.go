package api

import "net/http"

// GenLegacyAdapter forwards APIGen routes to the runtime server interface.
type GenLegacyAdapter struct {
	dispatcher GenOperationDispatcher
}

// NewAPIGenLegacyAdapter creates an adapter backed by server transport methods.
func NewAPIGenLegacyAdapter(server ServerInterface) *GenLegacyAdapter {
	return &GenLegacyAdapter{
		dispatcher: server,
	}
}

// HandleAPIGen dispatches to generated transport handling.
func (a *GenLegacyAdapter) HandleAPIGen(operationID string, w http.ResponseWriter, r *http.Request) {
	if ok := DispatchAPIGenOperation(operationID, a.dispatcher, w, r); !ok {
		http.NotFound(w, r)
	}
}

var _ GenServerInterface = (*GenLegacyAdapter)(nil)

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
