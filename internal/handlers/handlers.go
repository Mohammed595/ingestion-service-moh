package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"

	"ingestion-service/internal/models"
	"ingestion-service/internal/storage"
	"ingestion-service/internal/validation"
)

// Pre-allocated responses (zero allocation)
var (
	okResp     = []byte(`{"status":"ok"}`)
	healthResp = []byte(`{"status":"healthy"}`)
)

// sync.Pool for request body buffers
var bufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// sync.Pool for DeliveryEvent objects
var eventObjPool = sync.Pool{
	New: func() interface{} {
		return &models.DeliveryEvent{}
	},
}

// DeliveryEventsHandler - entry point
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

// handlePost - ultra-optimized with sync.Pool
func handlePost(w http.ResponseWriter, r *http.Request) {
	// 1. Fast token validation (O(1), no blocking)
	token := r.Header.Get("X-Platform-Token")
	if !validation.ValidateToken(token) {
		w.WriteHeader(403)
		return
	}

	// 2. Read body using pooled buffer
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	_, err := io.Copy(buf, r.Body)
	if err != nil {
		w.WriteHeader(400)
		return
	}

	// 3. Parse JSON using pooled event object
	event := eventObjPool.Get().(*models.DeliveryEvent)
	defer eventObjPool.Put(event)
	
	if err := json.Unmarshal(buf.Bytes(), event); err != nil {
		w.WriteHeader(400)
		return
	}

	// 4. Queue async (non-blocking, returns immediately)
	storage.QueueEvent(*event, token, "valid")

	// 5. Pre-allocated response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(okResp)
}

// handleGet - query with timeout
func handleGet(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if l, _ := strconv.Atoi(v); l > 0 && l <= 1000 {
			limit = l
		}
	}

	filters := make(map[string]interface{}, 4)
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

// HealthHandler - instant response
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(healthResp)
}
