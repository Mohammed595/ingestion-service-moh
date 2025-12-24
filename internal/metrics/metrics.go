package metrics

import "github.com/prometheus/client_golang/prometheus"

// Prometheus metrics for monitoring service health and performance
var (
	DeliveryEventsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "delivery_events_total",
			Help: "Total number of delivery events received",
		},
	)

	DeliveryEventsValidTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "delivery_events_valid_total",
			Help: "Total number of valid delivery events",
		},
	)

	DeliveryEventsInvalidTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "delivery_events_invalid_total",
			Help: "Total number of invalid delivery events",
		},
	)

	PlatformTokensRequestsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "platform_tokens_requests_total",
			Help: "Total number of requests to platform tokens endpoint",
		},
	)

	PlatformTokensRequestDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "platform_tokens_request_duration_seconds",
			Help:    "Duration of platform tokens requests",
			Buckets: prometheus.DefBuckets,
		},
	)

	DeliveryEventProcessingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "delivery_event_processing_duration_seconds",
			Help:    "Duration of delivery event processing",
			Buckets: prometheus.DefBuckets,
		},
	)

	PlatformTokensCacheHitsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "platform_tokens_cache_hits_total",
			Help: "Total number of platform tokens cache hits (if implemented)",
		},
	)
)

// Register registers all Prometheus metrics
func Register() {
	prometheus.MustRegister(DeliveryEventsTotal)
	prometheus.MustRegister(DeliveryEventsValidTotal)
	prometheus.MustRegister(DeliveryEventsInvalidTotal)
	prometheus.MustRegister(PlatformTokensRequestsTotal)
	prometheus.MustRegister(PlatformTokensRequestDuration)
	prometheus.MustRegister(DeliveryEventProcessingDuration)
	prometheus.MustRegister(PlatformTokensCacheHitsTotal)
}
