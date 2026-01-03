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

// sync.Pools for zero-allocation handling
var (
	bufPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4096))
		},
	}
	eventObjPool = sync.Pool{
		New: func() interface{} {
			return &models.DeliveryEvent{}
		},
	}
)

var (
	okResponse     = []byte(`{"status":"ok"}`)
	healthResponse = []byte(`{"status":"healthy"}`)
)

// DeliveryEventsHandler - Refactored for ultra-high performance
func DeliveryEventsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		handlePostFast(w, r)
	case http.MethodGet:
		handleGet(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func handlePostFast(w http.ResponseWriter, r *http.Request) {
	// 1. Fast token check (O(1), no I/O)
	token := r.Header.Get("X-Platform-Token")
	if !validation.ValidateToken(token) {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// 2. Body reading using pooled buffer
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if _, err := io.Copy(buf, r.Body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 3. JSON unmarshal using pooled object
	event := eventObjPool.Get().(*models.DeliveryEvent)
	defer eventObjPool.Put(event)

	if err := json.Unmarshal(buf.Bytes(), event); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 4. Async queueing (non-blocking)
	storage.QueueEvent(*event, token, "valid")

	// 5. Direct response write (pre-allocated)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(okResponse)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if l, err := strconv.Atoi(v); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	filters := make(map[string]interface{})
	q := r.URL.Query()
	for _, k := range []string{"order_id", "event_type", "customer_id", "restaurant_id"} {
		if v := q.Get(k); v != "" {
			filters[k] = v
		}
	}

	events, err := storage.QueryEvents(limit, filters)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(healthResponse)
}
