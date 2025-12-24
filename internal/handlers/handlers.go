package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"ingestion-service/internal/logger"
	"ingestion-service/internal/metrics"
	"ingestion-service/internal/models"
	"ingestion-service/internal/storage"
	"ingestion-service/internal/validation"
)

// DeliveryEventsHandler routes between POST (receive events) and GET (query events)
func DeliveryEventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		handlePostDeliveryEvent(w, r)
	} else if r.Method == http.MethodGet {
		handleGetDeliveryEvents(w, r)
	} else {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handlePostDeliveryEvent processes incoming delivery events
func handlePostDeliveryEvent(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	defer func() {
		metrics.DeliveryEventProcessingDuration.Observe(time.Since(startTime).Seconds())
	}()

	metrics.DeliveryEventsTotal.Inc()

	logger.Info("delivery event received", map[string]interface{}{
		"remote_addr": r.RemoteAddr,
	})

	body, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("failed to read request body", map[string]interface{}{"error": err.Error()})
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var event models.DeliveryEvent
	if err := json.Unmarshal(body, &event); err != nil {
		logger.Error("failed to parse JSON", map[string]interface{}{"error": err.Error()})
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	logger.Info("event parsed", map[string]interface{}{
		"order_id":        event.OrderID,
		"event_type":      event.EventType,
		"event_timestamp": event.EventTimestamp,
	})

	platformToken := r.Header.Get("X-Platform-Token")
	if platformToken == "" {
		logger.Warn("missing X-Platform-Token header", map[string]interface{}{
			"order_id": event.OrderID,
		})
		storage.StoreEvent(event, platformToken, "missing_token", "X-Platform-Token header not provided")
		metrics.DeliveryEventsInvalidTotal.Inc()
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "accepted", "validation": "failed"})
		return
	}

	logger.Info("platform token extracted", map[string]interface{}{
		"token_prefix": platformToken[:min(20, len(platformToken))],
	})

	validTokens, err := validation.FetchPlatformTokens()
	if err != nil {
		logger.Error("failed to fetch platform tokens", map[string]interface{}{
			"error":    err.Error(),
			"order_id": event.OrderID,
		})
		storage.StoreEvent(event, platformToken, "validation_service_error", err.Error())
		metrics.DeliveryEventsInvalidTotal.Inc()
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "accepted", "validation": "error"})
		return
	}

	logger.Info("platform tokens fetched", map[string]interface{}{
		"token_count": len(validTokens),
	})

	if !validation.ValidatePlatformToken(platformToken, validTokens) {
		logger.Warn("invalid platform token", map[string]interface{}{
			"order_id": event.OrderID,
		})
		storage.StoreEvent(event, platformToken, "invalid_token", "token not found in valid platform tokens")
		metrics.DeliveryEventsInvalidTotal.Inc()
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"status": "accepted", "validation": "failed"})
		return
	}

	logger.Info("platform token validated successfully", map[string]interface{}{
		"order_id": event.OrderID,
	})

	storage.StoreEvent(event, platformToken, "valid", "")
	metrics.DeliveryEventsValidTotal.Inc()

	logger.Info("event processed successfully", map[string]interface{}{
		"order_id":       event.OrderID,
		"total_duration": time.Since(startTime).Milliseconds(),
	})

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleGetDeliveryEvents returns stored events for debugging/inspection
func handleGetDeliveryEvents(w http.ResponseWriter, r *http.Request) {
	logger.Info("GET /delivery-events requested", nil)

	// Get optional limit parameter (default 100)
	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}

	events, err := storage.QueryEvents(limit)
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
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
