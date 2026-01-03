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

// Pre-allocated responses (zero allocation in hot path)
var okResp = []byte(`{"status":"ok"}`)
var healthResp = []byte(`{"status":"healthy"}`)

// DeliveryEventsHandler - main entry point
func DeliveryEventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		handlePost(w, r)
	} else if r.Method == http.MethodGet {
		handleGet(w, r)
	} else {
		w.WriteHeader(405)
	}
}

// handlePost - ultra-optimized POST handler
func handlePost(w http.ResponseWriter, r *http.Request) {
	// 1. Validate token FIRST (fastest check, no I/O)
	token := r.Header.Get("X-Platform-Token")
	if !validation.ValidateToken(token) {
		w.WriteHeader(403)
		return
	}

	// 2. Read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(400)
		return
	}

	// 3. Parse JSON
	var event models.DeliveryEvent
	if err := json.Unmarshal(body, &event); err != nil {
		w.WriteHeader(400)
		return
	}

	// 4. Queue for async write (non-blocking)
	storage.QueueEvent(event, token, "valid")

	// 5. Immediate response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(okResp)
}

// handleGet - query events
func handleGet(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 {
		limit = l
	}

	filters := map[string]interface{}{}
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
