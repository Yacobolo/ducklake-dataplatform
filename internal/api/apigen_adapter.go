package api

import (
	"net/http"
)

// GenLegacyAdapter forwards APIGen route handlers to the strict handler stack.
type GenLegacyAdapter struct {
	wrapper ServerInterfaceWrapper
}

// NewAPIGenLegacyAdapter creates an adapter backed by legacy strict routes.
func NewAPIGenLegacyAdapter(server ServerInterface) *GenLegacyAdapter {
	return &GenLegacyAdapter{
		wrapper: ServerInterfaceWrapper{
			Handler: server,
			ErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
				http.Error(w, err.Error(), http.StatusBadRequest)
			},
		},
	}
}

// HandleAPIGen dispatches to the strict legacy route implementation.
func (a *GenLegacyAdapter) HandleAPIGen(operationID string, w http.ResponseWriter, r *http.Request) {
	if ok := DispatchAPIGenOperation(operationID, &a.wrapper, w, r); !ok {
		http.NotFound(w, r)
	}
}

var _ GenServerInterface = (*GenLegacyAdapter)(nil)
