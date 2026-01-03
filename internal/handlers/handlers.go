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

// Pre-allocated responses - zero allocation
var (
	okResponse     = []byte(`{"status":"ok"}`)
	healthResponse = []byte(`{"status":"healthy"}`)
)

// sync.Pool for body buffers - eliminates GC pressure
var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 8192))
	},
}

// sync.Pool for event objects
var eventPool = sync.Pool{
	New: func() interface{} {
		return new(models.DeliveryEvent)
	},
}

// DeliveryEventsHandler - routing
func DeliveryEventsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		handlePostZeroAlloc(w, r)
	case "GET":
		handleGet(w, r)
	default:
		w.WriteHeader(405)
	}
}

// handlePostZeroAlloc - maximum performance POST handler
func handlePostZeroAlloc(w http.ResponseWriter, r *http.Request) {
	// 1. Token validation FIRST - O(1), no allocation, no I/O
	token := r.Header.Get("X-Platform-Token")
	if !validation.ValidateToken(token) {
		w.WriteHeader(403)
		return
	}

	// 2. Read body into pooled buffer
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()

	_, err := io.Copy(buf, r.Body)
	if err != nil {
		bufPool.Put(buf)
		w.WriteHeader(400)
		return
	}

	// 3. Parse into pooled event object
	event := eventPool.Get().(*models.DeliveryEvent)
	err = json.Unmarshal(buf.Bytes(), event)
	
	// Return buffer immediately
	bufPool.Put(buf)

	if err != nil {
		eventPool.Put(event)
		w.WriteHeader(400)
		return
	}

	// 4. Queue for async write - non-blocking
	storage.QueueEvent(*event, token, "valid")
	
	// Return event to pool
	eventPool.Put(event)

	// 5. Pre-allocated response - zero allocation
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(okResponse)
}

// handleGet - query endpoint
func handleGet(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if l, _ := strconv.Atoi(v); l > 0 && l <= 1000 {
			limit = l
		}
	}

	filters := make(map[string]interface{}, 4)
	q := r.URL.Query()
	if v := q.Get("order_id"); v != "" {
		filters["order_id"] = v
	}
	if v := q.Get("event_type"); v != "" {
		filters["event_type"] = v
	}
	if v := q.Get("customer_id"); v != "" {
		filters["customer_id"] = v
	}
	if v := q.Get("restaurant_id"); v != "" {
		filters["restaurant_id"] = v
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
	w.Write(healthResponse)
}
