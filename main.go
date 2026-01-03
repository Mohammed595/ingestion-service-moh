package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"runtime"
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
			Help:    "Request duration",
			Buckets: []float64{.00005, .0001, .0005, .001, .005, .01, .05, .1, .5},
		},
		[]string{"handler", "method"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration)
}

func main() {
	// Maximize CPU usage
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Config
	validation.ControlServerURL = getEnv("CONTROL_SERVER_URL", "http://control-server:9000")
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/ingestion?sslmode=disable")
	port := getEnv("PORT", "8080")

	// 1. Initialize database with global connection pool
	logger.Info("initializing database", nil)
	if err := storage.InitDatabase(dbURL); err != nil {
		logger.Fatal("db error", map[string]interface{}{"error": err.Error()})
	}
	defer storage.Close()

	// 2. Load tokens synchronously with timeout
	logger.Info("loading tokens", nil)
	validation.InitTokens()

	// 3. Start background goroutines
	validation.StartBackgroundRefresh()
	storage.StartWriters(16) // 16 parallel DB writers for maximum throughput

	// 4. Routes
	mux := http.NewServeMux()
	mux.HandleFunc("/delivery-events", instrument("events", handlers.DeliveryEventsHandler))
	mux.HandleFunc("/health", instrument("health", handlers.HealthHandler))
	mux.Handle("/metrics", promhttp.Handler())

	// 5. High-performance server with aggressive timeouts
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadTimeout:       2 * time.Second,
		ReadHeaderTimeout: 500 * time.Millisecond,
		WriteTimeout:      2 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    1 << 15, // 32KB
	}

	logger.Info("server starting", map[string]interface{}{
		"port":    port,
		"workers": 16,
		"cpus":    runtime.NumCPU(),
	})

	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal("server error", map[string]interface{}{"error": err.Error()})
		}
	}()

	// Graceful shutdown with timeout
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

func instrument(name string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: 200}
		
		h(rw, r)
		
		httpRequestDuration.WithLabelValues(name, r.Method).Observe(time.Since(start).Seconds())
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
