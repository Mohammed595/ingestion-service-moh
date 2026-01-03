package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"ingestion-service/internal/handlers"
	"ingestion-service/internal/logger"
	"ingestion-service/internal/storage"
	"ingestion-service/internal/validation"
)

var (
	// Standard HTTP metrics - can't be manipulated as they're recorded by middleware
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"handler", "method", "status"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"handler", "method"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
}

func main() {
	// Configuration from environment variables
	validation.ControlServerURL = getEnv("CONTROL_SERVER_URL", "http://control-server:9000")
	databaseURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/ingestion?sslmode=disable")
	port := getEnv("PORT", "8080")

	// Initialize database
	if err := storage.InitDatabase(databaseURL); err != nil {
		logger.Fatal("failed to initialize database", map[string]interface{}{
			"error": err.Error(),
		})
	}
	defer storage.Close()

	// HTTP routes with instrumentation middleware
	http.HandleFunc("/delivery-events", instrumentHandler("delivery-events", handlers.DeliveryEventsHandler))
	http.HandleFunc("/health", instrumentHandler("health", handlers.HealthHandler))
	http.Handle("/metrics", promhttp.Handler())

	// Configure optimized HTTP server for high performance
	server := &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  10 * time.Second,  // Max time to read request
		WriteTimeout: 10 * time.Second,  // Max time to write response
		IdleTimeout:  120 * time.Second, // Keep-alive timeout
	}

	logger.Info("starting optimized HTTP server", map[string]interface{}{
		"port":          port,
		"read_timeout":  "10s",
		"write_timeout": "10s",
		"idle_timeout":  "120s",
	})

	// Start server with graceful shutdown handling
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Fatal("server failed", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	logger.Info("shutting down server", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("server shutdown failed", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	logger.Info("server shutdown complete", nil)
}

// instrumentHandler wraps an HTTP handler with Prometheus instrumentation
func instrumentHandler(handlerName string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()

		// Wrap ResponseWriter to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Call the actual handler
		handler(wrapped, r)

		// Record metrics
		duration := time.Since(startTime).Seconds()
		httpRequestDuration.WithLabelValues(handlerName, r.Method).Observe(duration)
		httpRequestsTotal.WithLabelValues(handlerName, r.Method, strconv.Itoa(wrapped.statusCode)).Inc()
	}
}

// responseWriter wraps http.ResponseWriter to capture the status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
