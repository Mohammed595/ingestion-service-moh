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
			Buckets: []float64{.0005, .001, .005, .01, .025, .05, .1, .5, 1},
		},
		[]string{"handler", "method"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
}

func main() {
	// Config
	validation.ControlServerURL = getEnv("CONTROL_SERVER_URL", "http://control-server:9000")
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/ingestion?sslmode=disable")
	port := getEnv("PORT", "8080")

	// 1. Init database FIRST
	if err := storage.InitDatabase(dbURL); err != nil {
		logger.Fatal("db init failed", map[string]interface{}{"error": err.Error()})
	}
	defer storage.Close()

	// 2. Load tokens SYNCHRONOUSLY (blocks until ready)
	logger.Info("loading tokens...", nil)
	if err := validation.InitTokens(); err != nil {
		logger.Warn("token init warning", map[string]interface{}{"error": err.Error()})
	}
	logger.Info("tokens loaded", nil)

	// 3. Start background systems
	validation.StartBackgroundRefresh()
	storage.StartWriters(4) // 4 parallel DB writers

	// 4. Routes
	http.HandleFunc("/delivery-events", instrument("events", handlers.DeliveryEventsHandler))
	http.HandleFunc("/health", instrument("health", handlers.HealthHandler))
	http.Handle("/metrics", promhttp.Handler())

	// 5. Server with optimized settings
	server := &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	logger.Info("server starting", map[string]interface{}{"port": port})

	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal("server error", map[string]interface{}{"error": err.Error()})
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

func instrument(name string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &respWriter{ResponseWriter: w, status: 200}
		
		h(rw, r)
		
		d := time.Since(start).Seconds()
		httpRequestDuration.WithLabelValues(name, r.Method).Observe(d)
		httpRequestsTotal.WithLabelValues(name, r.Method, strconv.Itoa(rw.status)).Inc()
	}
}

type respWriter struct {
	http.ResponseWriter
	status int
}

func (w *respWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
