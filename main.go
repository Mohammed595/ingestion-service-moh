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
	"golang.org/x/sync/errgroup"

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
			Buckets: []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5},
		},
		[]string{"handler", "method"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestDuration)
}

func main() {
	// 1. Runtime tuning
	runtime.GOMAXPROCS(runtime.NumCPU())

	// 2. Load configuration
	validation.ControlServerURL = getEnv("CONTROL_SERVER_URL", "http://control-server:9000")
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/ingestion?sslmode=disable")
	port := getEnv("PORT", "8080")

	// 3. Global resources init
	if err := storage.InitDatabase(dbURL); err != nil {
		logger.Fatal("db init failed", map[string]interface{}{"error": err.Error()})
	}
	defer storage.Close()

	if err := validation.InitTokens(); err != nil {
		logger.Warn("token init warning", map[string]interface{}{"error": err.Error()})
	}

	// 4. Start background systems via errgroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	g, ctx := errgroup.WithContext(ctx)

	// Background refresh
	validation.StartBackgroundRefresh()

	// Parallel writers (32 parallel DB writers for maximum RPS)
	storage.StartWriters(ctx, g, 32)

	// 5. Optimized server setup
	mux := http.NewServeMux()
	mux.HandleFunc("/delivery-events", instrumentHandler("ev", handlers.DeliveryEventsHandler))
	mux.HandleFunc("/health", instrumentHandler("hp", handlers.HealthHandler))
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadTimeout:       1 * time.Second,
		ReadHeaderTimeout: 500 * time.Millisecond,
		WriteTimeout:      1 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	logger.Info("ingestion service starting", map[string]interface{}{
		"port": port,
		"cpus": runtime.NumCPU(),
	})

	// Start server
	g.Go(func() error {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	// 6. Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	select {
	case sig := <-sigChan:
		logger.Info("received signal, shutting down", map[string]interface{}{"signal": sig.String()})
	case <-ctx.Done():
		logger.Info("context cancelled, shutting down", nil)
	}

	cancel() // Trigger background system shutdown
	
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown failed", map[string]interface{}{"error": err.Error()})
	}

	// Wait for background writers to finish flushing
	if err := g.Wait(); err != nil && err != context.Canceled {
		logger.Error("background system failure", map[string]interface{}{"error": err.Error()})
	}

	logger.Info("shutdown complete", nil)
}

func instrumentHandler(name string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: 200}
		h(rw, r)
		duration := time.Since(start).Seconds()
		httpRequestDuration.WithLabelValues(name, r.Method).Observe(duration)
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
