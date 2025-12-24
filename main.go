package main

import (
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"ingestion-service/internal/handlers"
	"ingestion-service/internal/logger"
	"ingestion-service/internal/metrics"
	"ingestion-service/internal/storage"
	"ingestion-service/internal/validation"
)

func main() {
	// Configuration from environment variables
	validation.ControlServerURL = getEnv("CONTROL_SERVER_URL", "http://control-server:9000")
	databaseURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/ingestion?sslmode=disable")
	port := getEnv("PORT", "8080")

	logger.Info("ingestion service starting", map[string]interface{}{
		"port":               port,
		"control_server_url": validation.ControlServerURL,
	})

	// Register Prometheus metrics
	metrics.Register()
	logger.Info("prometheus metrics registered", nil)

	// Initialize database
	if err := storage.InitDatabase(databaseURL); err != nil {
		logger.Fatal("failed to initialize database", map[string]interface{}{
			"error": err.Error(),
		})
	}
	defer storage.Close()

	// HTTP routes
	http.HandleFunc("/delivery-events", handlers.DeliveryEventsHandler)
	http.HandleFunc("/health", handlers.HealthHandler)
	http.Handle("/metrics", promhttp.Handler())

	logger.Info("ingestion service listening", map[string]interface{}{
		"port": port,
	})

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		logger.Fatal("server failed", map[string]interface{}{
			"error": err.Error(),
		})
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
