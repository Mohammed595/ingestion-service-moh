package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"ingestion-service/internal/logger"
	"ingestion-service/internal/models"
	"ingestion-service/internal/storage"
	"ingestion-service/internal/validation"
)

// DeliveryEventsHandler routes between POST (receive events), POST batch, and GET (query events)
func DeliveryEventsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		// Check if it's a batch request
		if r.URL.Query().Get("batch") == "true" {
			handlePostDeliveryEventsBatch(w, r)
		} else {
			handlePostDeliveryEvent(w, r)
		}
	case http.MethodGet:
		handleGetDeliveryEvents(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePostDeliveryEvent processes incoming delivery events with optimized performance
func handlePostDeliveryEvent(w http.ResponseWriter, r *http.Request) {
	// Limit request body size for security and performance
	r.Body = http.MaxBytesReader(w, r.Body, 10*1024) // 10KB limit

	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("failed to read request body <>", map[string]interface{}{
			"error": err.Error(),
		})
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var event models.DeliveryEvent
	if err := json.Unmarshal(body, &event); err != nil {
		logger.Error("failed to unmarshal event", map[string]interface{}{
			"error": err.Error(),
		})
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	platformToken := r.Header.Get("X-Platform-Token")
	if platformToken == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing X-Platform-Token header"})
		return
	}

	validTokens, err := validation.FetchPlatformTokens()
	if err != nil {
		logger.Error("failed to fetch platform tokens", map[string]interface{}{
			"error": err.Error(),
		})
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if !validation.ValidatePlatformToken(platformToken, validTokens) {
		logger.Warn("invalid platform token", map[string]interface{}{
			"token_provided": platformToken != "",
		})
		w.WriteHeader(http.StatusForbidden)
		return
	}

	err = storage.StoreEvent(event, platformToken, "valid")
	if err != nil {
		logger.Error("failed to store event", map[string]interface{}{
			"error":    err.Error(),
			"order_id": event.OrderID,
		})
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handlePostDeliveryEventsBatch processes multiple delivery events in a single request
func handlePostDeliveryEventsBatch(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("failed to read batch request body", map[string]interface{}{
			"error": err.Error(),
		})
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var events []models.DeliveryEvent
	if err := json.Unmarshal(body, &events); err != nil {
		logger.Error("failed to unmarshal batch events", map[string]interface{}{
			"error": err.Error(),
		})
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Limit batch size for performance and memory protection
	if len(events) > 1000 {
		logger.Warn("batch size too large", map[string]interface{}{
			"batch_size":  len(events),
			"max_allowed": 1000,
		})
		http.Error(w, "batch size exceeds maximum of 1000 events", http.StatusBadRequest)
		return
	}

	if len(events) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "no events provided"})
		return
	}

	platformToken := r.Header.Get("X-Platform-Token")
	if platformToken == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing X-Platform-Token header"})
		return
	}

	validTokens, err := validation.FetchPlatformTokens()
	if err != nil {
		logger.Error("failed to fetch platform tokens for batch", map[string]interface{}{
			"error": err.Error(),
		})
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if !validation.ValidatePlatformToken(platformToken, validTokens) {
		logger.Warn("invalid platform token for batch", map[string]interface{}{
			"token_provided": platformToken != "",
		})
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Store batch of events
	err = storage.StoreEventsBatch(events, platformToken, "valid")
	if err != nil {
		logger.Error("failed to store batch events", map[string]interface{}{
			"error":      err.Error(),
			"batch_size": len(events),
		})
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	logger.Info("batch events stored successfully", map[string]interface{}{
		"batch_size":     len(events),
		"platform_token": platformToken,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":           "ok",
		"events_processed": len(events),
	})
}

// handleGetDeliveryEvents returns stored events for debugging/inspection
func handleGetDeliveryEvents(w http.ResponseWriter, r *http.Request) {
	logger.Info("GET /delivery-events requested", nil)

	// Get optional limit parameter (default 100)
	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	// Build filters map safely
	filters := make(map[string]interface{})

	if orderID := r.URL.Query().Get("order_id"); orderID != "" {
		filters["order_id"] = orderID
	}
	if eventType := r.URL.Query().Get("event_type"); eventType != "" {
		filters["event_type"] = eventType
	}
	if customerID := r.URL.Query().Get("customer_id"); customerID != "" {
		filters["customer_id"] = customerID
	}
	if restaurantID := r.URL.Query().Get("restaurant_id"); restaurantID != "" {
		filters["restaurant_id"] = restaurantID
	}

	events, err := storage.QueryEvents(limit, filters)
	if err != nil {
		logger.Error("failed to query events", map[string]interface{}{"error": err.Error()})
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	logger.Info("returning events", map[string]interface{}{"count": len(events)})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

// HealthHandler returns service health status
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}
