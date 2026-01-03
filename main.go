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
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1},
		},
		[]string{"handler", "method"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
}

func main() {
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

	// Start background token refresh - tokens always available, never blocks
	validation.StartBackgroundTokenRefresh()

	// Start async event writers - 4 workers for parallel DB writes
	storage.StartAsyncWriter(4)

	// HTTP routes
	http.HandleFunc("/delivery-events", instrumentHandler("delivery-events", handlers.DeliveryEventsHandler))
	http.HandleFunc("/health", instrumentHandler("health", handlers.HealthHandler))
	http.Handle("/metrics", promhttp.Handler())

	// Ultra-low latency server config
	server := &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	logger.Info("starting high-performance server", map[string]interface{}{
		"port": port,
	})

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server failed", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	logger.Info("shutting down", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

func instrumentHandler(name string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		
		handler(wrapped, r)
		
		duration := time.Since(start).Seconds()
		httpRequestDuration.WithLabelValues(name, r.Method).Observe(duration)
		httpRequestsTotal.WithLabelValues(name, r.Method, strconv.Itoa(wrapped.statusCode)).Inc()
	}
}

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
