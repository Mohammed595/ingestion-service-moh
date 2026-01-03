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
			Help:    "Request latency",
			Buckets: []float64{.00001, .00005, .0001, .0005, .001, .005, .01, .05, .1, .5},
		},
		[]string{"handler", "method"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal, httpRequestDuration)
}

func main() {
	// Maximize parallelism
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Config
	validation.ControlServerURL = getEnv("CONTROL_SERVER_URL", "http://control-server:9000")
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/ingestion?sslmode=disable")
	port := getEnv("PORT", "8080")

	// 1. Initialize DB with massive connection pool
	logger.Info("init db", nil)
	if err := storage.InitDatabase(dbURL); err != nil {
		logger.Fatal("db fail", map[string]interface{}{"e": err.Error()})
	}
	defer storage.Close()

	// 2. Load tokens synchronously (fast)
	logger.Info("init tokens", nil)
	validation.InitTokens()

	// 3. Start background systems
	validation.StartBackgroundRefresh()
	storage.StartWriters(32) // 32 parallel DB writers!

	// 4. Routes with minimal overhead
	mux := http.NewServeMux()
	mux.HandleFunc("/delivery-events", instrument("ev", handlers.DeliveryEventsHandler))
	mux.HandleFunc("/health", instrument("hp", handlers.HealthHandler))
	mux.Handle("/metrics", promhttp.Handler())

	// 5. Ultra-low latency server config
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadTimeout:       1500 * time.Millisecond,
		ReadHeaderTimeout: 200 * time.Millisecond,
		WriteTimeout:      1500 * time.Millisecond,
		IdleTimeout:       15 * time.Second,
		MaxHeaderBytes:    1 << 14, // 16KB
	}

	logger.Info("starting", map[string]interface{}{
		"port":    port,
		"workers": 32,
		"cpus":    runtime.NumCPU(),
	})

	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal("server fail", map[string]interface{}{"e": err.Error()})
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

func instrument(name string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &sw{ResponseWriter: w, s: 200}
		h(rw, r)
		httpRequestDuration.WithLabelValues(name, r.Method).Observe(time.Since(start).Seconds())
		httpRequestsTotal.WithLabelValues(name, r.Method, strconv.Itoa(rw.s)).Inc()
	}
}

type sw struct {
	http.ResponseWriter
	s int
}

func (w *sw) WriteHeader(c int) {
	w.s = c
	w.ResponseWriter.WriteHeader(c)
}

func getEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
