package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"ingestion-service/internal/models"

	"github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

var DB *sql.DB

// Global Connection Pool & Tuning Constants
const (
	MaxConns       = 100
	IdleConns      = 100
	BatchSize      = 512              // Increased batch size for COPY
	FlushInterval  = 15 * time.Millisecond
	WorkerCount    = 32               // Parallel writers
	ChannelSize    = 500000           // Half a million events buffer
)

type eventEntry struct {
	OrderID        string
	EventType      string
	EventTimestamp int64
	ReceivedAt     int64
	CustomerID     string
	RestaurantID   string
	DriverID       string
	Lat            float64
	Lng            float64
	Token          string
	Status         string
}

// sync.Pool for zero-allocation entries
var eventPool = sync.Pool{
	New: func() interface{} {
		return &eventEntry{}
	},
}

// Channel for ultra-fast ingestion
var eventChan = make(chan *eventEntry, ChannelSize)

// QueueEvent - Non-blocking, zero-allocation entry path
func QueueEvent(event models.DeliveryEvent, token, status string) {
	e := eventPool.Get().(*eventEntry)
	e.OrderID = event.OrderID
	e.EventType = event.EventType
	e.EventTimestamp = event.EventTimestamp.Unix()
	e.ReceivedAt = time.Now().Unix()
	e.CustomerID = event.CustomerID
	e.RestaurantID = event.RestaurantID
	e.DriverID = event.DriverID
	e.Lat = event.Location.Lat
	e.Lng = event.Location.Lng
	e.Token = token
	e.Status = status

	select {
	case eventChan <- e:
	default:
		// Drop to protect latency if buffer is saturated
		eventPool.Put(e)
	}
}

// StartWriters - Uses PostgreSQL COPY protocol for maximum throughput
func StartWriters(ctx context.Context, g *errgroup.Group) {
	for i := 0; i < WorkerCount; i++ {
		g.Go(func() error {
			return copyWorker(ctx)
		})
	}
}

// copyWorker uses the PostgreSQL COPY command (fastest possible ingestion)
func copyWorker(ctx context.Context) error {
	batch := make([]*eventEntry, 0, BatchSize)
	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				flushCopy(batch)
			}
			return ctx.Err()
		case e := <-eventChan:
			batch = append(batch, e)
			if len(batch) >= BatchSize {
				flushCopy(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushCopy(batch)
				batch = batch[:0]
			}
		}
	}
}

// flushCopy implements the PostgreSQL COPY protocol - No placeholders, no SQL injection
func flushCopy(events []*eventEntry) {
	if len(events) == 0 || DB == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start transaction for COPY
	tx, err := DB.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer tx.Rollback()

	// Use COPY protocol - No string concatenation or placeholders
	stmt, err := tx.Prepare(pq.CopyIn("events",
		"order_id", "event_type", "event_timestamp", "received_at",
		"customer_id", "restaurant_id", "driver_id", "location_lat", "location_lng",
		"platform_token", "validation_status", "validation_error"))
	if err != nil {
		return
	}

	for _, e := range events {
		_, err = stmt.Exec(
			e.OrderID, e.EventType, e.EventTimestamp, e.ReceivedAt,
			e.CustomerID, e.RestaurantID, e.DriverID, e.Lat, e.Lng,
			e.Token, e.Status, "",
		)
		if err != nil {
			stmt.Close()
			return
		}
	}

	err = stmt.Close()
	if err != nil {
		return
	}

	err = tx.Commit()
	if err != nil {
		return
	}

	// Return objects to pool
	for _, e := range events {
		eventPool.Put(e)
	}
}

// QueryEvents - Secure whitelisted filtering
func QueryEvents(limit int, filters map[string]interface{}) ([]models.StoredEvent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Columns whitelisting to prevent SQL injection in column names
	allowedFilters := map[string]string{
		"order_id":      "order_id",
		"event_type":    "event_type",
		"customer_id":   "customer_id",
		"restaurant_id": "restaurant_id",
		"driver_id":     "driver_id",
	}

	var sb strings.Builder
	sb.WriteString("SELECT id, order_id, event_type, event_timestamp, received_at, customer_id, restaurant_id, driver_id, location_lat, location_lng, platform_token, validation_status, COALESCE(validation_error, '') FROM events")

	args := make([]interface{}, 0, len(filters)+1)
	conds := make([]string, 0, len(filters))
	n := 1

	for k, v := range filters {
		col, ok := allowedFilters[k]
		if !ok {
			continue // Skip unallowed or suspicious filters
		}
		if s, ok := v.(string); ok && s != "" {
			conds = append(conds, fmt.Sprintf("%s=$%d", col, n))
			args = append(args, s)
			n++
		}
	}

	if len(conds) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(conds, " AND "))
	}
	
	fmt.Fprintf(&sb, " ORDER BY id DESC LIMIT $%d", n)
	args = append(args, limit)

	rows, err := DB.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]models.StoredEvent, 0, limit)
	for rows.Next() {
		var e models.StoredEvent
		err := rows.Scan(&e.ID, &e.OrderID, &e.EventType, &e.EventTimestamp, &e.ReceivedAt,
			&e.CustomerID, &e.RestaurantID, &e.DriverID, &e.LocationLat, &e.LocationLng,
			&e.PlatformToken, &e.ValidationStatus, &e.ValidationError)
		if err == nil {
			results = append(results, e)
		}
	}
	return results, nil
}

func InitDatabase(url string) error {
	var err error
	DB, err = sql.Open("postgres", url)
	if err != nil {
		return err
	}

	DB.SetMaxOpenConns(MaxConns)
	DB.SetMaxIdleConns(IdleConns)
	DB.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return DB.PingContext(ctx)
}

func Close() {
	if DB != nil {
		DB.Close()
	}
}

// StoreEvent & StoreEventsBatch - Kept for compatibility
func StoreEvent(event models.DeliveryEvent, token, status string) error {
	QueueEvent(event, token, status)
	return nil
}

func StoreEventsBatch(events []models.DeliveryEvent, token, status string) error {
	for i := range events {
		QueueEvent(events[i], token, status)
	}
	return nil
}
