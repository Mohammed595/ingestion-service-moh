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
			Help: "Total HTTP requests",
		},
		[]string{"handler", "method", "status"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration",
			Buckets: []float64{.0001, .0005, .001, .005, .01, .05, .1, .5, 1},
		},
		[]string{"handler", "method"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration)
}

func main() {
	// Config
	validation.ControlServerURL = getEnv("CONTROL_SERVER_URL", "http://control-server:9000")
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/ingestion?sslmode=disable")
	port := getEnv("PORT", "8080")

	// 1. Initialize database
	logger.Info("connecting to database", nil)
	if err := storage.InitDatabase(dbURL); err != nil {
		logger.Fatal("db error", map[string]interface{}{"error": err.Error()})
	}
	defer storage.Close()
	logger.Info("database connected", nil)

	// 2. Load tokens BEFORE accepting traffic
	logger.Info("loading tokens", nil)
	validation.InitTokens()
	logger.Info("tokens ready", nil)

	// 3. Start background workers
	validation.StartBackgroundRefresh()
	storage.StartWriters(8) // 8 parallel DB writers

	// 4. Routes
	http.HandleFunc("/delivery-events", instrument("events", handlers.DeliveryEventsHandler))
	http.HandleFunc("/health", instrument("health", handlers.HealthHandler))
	http.Handle("/metrics", promhttp.Handler())

	// 5. High-performance server
	server := &http.Server{
		Addr:              ":" + port,
		ReadTimeout:       3 * time.Second,
		ReadHeaderTimeout: 1 * time.Second,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    1 << 16, // 64KB
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
	logger.Info("shutdown complete", nil)
}

func instrument(name string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: 200}
		
		h(rw, r)
		
		d := time.Since(start).Seconds()
		httpRequestDuration.WithLabelValues(name, r.Method).Observe(d)
		httpRequestsTotal.WithLabelValues(name, r.Method, strconv.Itoa(rw.status)).Inc()
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func getEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
