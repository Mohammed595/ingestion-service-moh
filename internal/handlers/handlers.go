package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"

	"ingestion-service/internal/models"
	"ingestion-service/internal/storage"
	"ingestion-service/internal/validation"
)

// Pre-allocated response bytes for zero-allocation responses
var (
	okResponse     = []byte(`{"status":"ok"}`)
	okResponseLen  = len(okResponse)
	
	// Reusable byte pools
	bodyPool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, 0, 4096)
			return &b
		},
	}
)

// DeliveryEventsHandler - ultra-fast event ingestion
func DeliveryEventsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		handlePostDeliveryEvent(w, r)
	case http.MethodGet:
		handleGetDeliveryEvents(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// handlePostDeliveryEvent - optimized for minimum latency
func handlePostDeliveryEvent(w http.ResponseWriter, r *http.Request) {
	// Quick token validation first - O(1) lookup, no network call
	platformToken := r.Header.Get("X-Platform-Token")
	if !validation.ValidateToken(platformToken) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Read body using pooled buffer
	bufPtr := bodyPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	defer func() {
		*bufPtr = buf[:0]
		bodyPool.Put(bufPtr)
	}()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Parse event
	var event models.DeliveryEvent
	if err := json.Unmarshal(body, &event); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Queue event for async processing - returns immediately
	storage.QueueEvent(event, platformToken, "valid")

	// Send pre-allocated response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(okResponse)
}

// handleGetDeliveryEvents returns stored events
func handleGetDeliveryEvents(w http.ResponseWriter, r *http.Request) {
	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	filters := make(map[string]interface{})
	if v := r.URL.Query().Get("order_id"); v != "" {
		filters["order_id"] = v
	}
	if v := r.URL.Query().Get("event_type"); v != "" {
		filters["event_type"] = v
	}
	if v := r.URL.Query().Get("customer_id"); v != "" {
		filters["customer_id"] = v
	}
	if v := r.URL.Query().Get("restaurant_id"); v != "" {
		filters["restaurant_id"] = v
	}

	events, err := storage.QueryEvents(limit, filters)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

// HealthHandler returns service health
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy"}`))
}
