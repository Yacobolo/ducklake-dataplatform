package api

import (
	"net/http"
)

// GenLegacyAdapter forwards APIGen route handlers to the strict handler stack.
type GenLegacyAdapter struct {
	dispatcher GenOperationDispatcher
}

// NewAPIGenLegacyAdapter creates an adapter backed by legacy strict routes.
func NewAPIGenLegacyAdapter(server ServerInterface) *GenLegacyAdapter {
	wrapper := &ServerInterfaceWrapper{
		Handler: server,
		ErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		},
	}

	return &GenLegacyAdapter{
		dispatcher: wrapper,
	}
}

// HandleAPIGen dispatches to the strict legacy route implementation.
func (a *GenLegacyAdapter) HandleAPIGen(operationID string, w http.ResponseWriter, r *http.Request) {
	if ok := DispatchAPIGenOperation(operationID, a.dispatcher, w, r); !ok {
		http.NotFound(w, r)
	}
}

var _ GenServerInterface = (*GenLegacyAdapter)(nil)
