package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/example/apigen-consumer/internal/api"
)

func main() {
	router := chi.NewRouter()
	api.RegisterAPIGenRoutes(router, server{})

	if err := http.ListenAndServe(":8081", router); err != nil {
		log.Fatal(err)
	}
}

type server struct{}

func (server) HandleAPIGen(operationID string, w http.ResponseWriter, r *http.Request) {
	if ok := api.DispatchAPIGenOperation(operationID, server{}, w, r); !ok {
		http.NotFound(w, r)
	}
}

func (server) ListWidgets(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"data": []map[string]string{
			{"id": "widget-1", "name": "first"},
		},
	})
}

func (server) CreateWidget(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"id":   "widget-2",
		"name": "created",
	})
}

func (server) DeleteWidget(w http.ResponseWriter, _ *http.Request, _ string) {
	w.WriteHeader(http.StatusNoContent)
}
