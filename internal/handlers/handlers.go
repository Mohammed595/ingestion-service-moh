package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"ingestion-service/internal/models"
	"ingestion-service/internal/storage"
	"ingestion-service/internal/validation"
)

// Pre-allocated responses
var (
	okResp     = []byte(`{"status":"ok"}`)
	healthResp = []byte(`{"status":"healthy"}`)
)

// DeliveryEventsHandler - main handler
func DeliveryEventsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		handlePost(w, r)
	case "GET":
		handleGet(w, r)
	default:
		w.WriteHeader(405)
	}
}

// handlePost - ultra-fast POST handler
func handlePost(w http.ResponseWriter, r *http.Request) {
	// 1. Get token (fast header read)
	token := r.Header.Get("X-Platform-Token")
	
	// 2. Validate token - O(1), never blocks
	if !validation.ValidateToken(token) {
		w.WriteHeader(403)
		return
	}

	// 3. Read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(400)
		return
	}

	// 4. Parse event
	var event models.DeliveryEvent
	if err := json.Unmarshal(body, &event); err != nil {
		w.WriteHeader(400)
		return
	}

	// 5. Queue async (non-blocking)
	storage.QueueEvent(event, token, "valid")

	// 6. Immediate success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(okResp)
}

// handleGet - GET events
func handleGet(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if l, _ := strconv.Atoi(v); l > 0 && l <= 1000 {
			limit = l
		}
	}

	filters := make(map[string]interface{})
	for _, k := range []string{"order_id", "event_type", "customer_id", "restaurant_id"} {
		if v := r.URL.Query().Get(k); v != "" {
			filters[k] = v
		}
	}

	events, err := storage.QueryEvents(limit, filters)
	if err != nil {
		w.WriteHeader(500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

// HealthHandler - health check
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(healthResp)
}
