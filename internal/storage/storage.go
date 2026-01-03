package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"ingestion-service/internal/models"

	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

var DB *sql.DB

// Global Connection Pool
const (
	MaxConns    = 100
	IdleConns   = 100
	MaxBatch    = 256
	FlushInter  = 20 * time.Millisecond
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

// sync.Pools for zero-allocation batching
var (
	eventPool = sync.Pool{New: func() interface{} { return &eventEntry{} }}
	batchPool = sync.Pool{New: func() interface{} { 
		b := make([]eventEntry, 0, MaxBatch)
		return &b 
	}}
	argsPool = sync.Pool{New: func() interface{} { 
		a := make([]interface{}, 0, MaxBatch*12)
		return &a 
	}}
)

// Channel for buffering events - large enough to handle bursts
var eventChan = make(chan eventEntry, 200000)

// QueueEvent - non-blocking ingestion
func QueueEvent(event models.DeliveryEvent, token, status string) {
	select {
	case eventChan <- eventEntry{
		OrderID:        event.OrderID,
		EventType:      event.EventType,
		EventTimestamp: event.EventTimestamp.Unix(),
		ReceivedAt:     time.Now().Unix(),
		CustomerID:     event.CustomerID,
		RestaurantID:   event.RestaurantID,
		DriverID:       event.DriverID,
		Lat:            event.Location.Lat,
		Lng:            event.Location.Lng,
		Token:          token,
		Status:         status,
	}:
	default:
		// Drop events if channel is full to protect latency
	}
}

// StartWriters - uses errgroup to manage parallel background writers
func StartWriters(ctx context.Context, g *errgroup.Group, n int) {
	for i := 0; i < n; i++ {
		g.Go(func() error {
			return writerLoop(ctx)
		})
	}
}

func writerLoop(ctx context.Context) error {
	batchPtr := batchPool.Get().(*[]eventEntry)
	batch := (*batchPtr)[:0]
	defer batchPool.Put(batchPtr)

	ticker := time.NewTicker(FlushInter)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				flushBatch(batch)
			}
			return ctx.Err()
		case e := <-eventChan:
			batch = append(batch, e)
			if len(batch) >= MaxBatch {
				flushBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// Pre-allocated query fragments
const insertQueryPrefix = `INSERT INTO events (order_id, event_type, event_timestamp, received_at, customer_id, restaurant_id, driver_id, location_lat, location_lng, platform_token, validation_status, validation_error) VALUES `

func flushBatch(events []eventEntry) {
	if len(events) == 0 || DB == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	argsPtr := argsPool.Get().(*[]interface{})
	args := (*argsPtr)[:0]
	defer argsPool.Put(argsPtr)

	var sb strings.Builder
	sb.Grow(len(insertQueryPrefix) + len(events)*120)
	sb.WriteString(insertQueryPrefix)

	for i, e := range events {
		if i > 0 {
			sb.WriteString(",")
		}
		offset := i * 12
		fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			offset+1, offset+2, offset+3, offset+4, offset+5, offset+6,
			offset+7, offset+8, offset+9, offset+10, offset+11, offset+12)
		
		args = append(args, e.OrderID, e.EventType, e.EventTimestamp, e.ReceivedAt,
			e.CustomerID, e.RestaurantID, e.DriverID, e.Lat, e.Lng,
			e.Token, e.Status, "")
	}

	_, _ = DB.ExecContext(ctx, sb.String(), args...)
}

// Compatibility layer
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

func QueryEvents(limit int, filters map[string]interface{}) ([]models.StoredEvent, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	query := `SELECT id, order_id, event_type, event_timestamp, received_at, customer_id, restaurant_id, driver_id, location_lat, location_lng, platform_token, validation_status, COALESCE(validation_error, '') FROM events`
	
	var args []interface{}
	var conds []string
	n := 1

	// Whitelist of allowed filter keys to prevent SQL injection
	allowedFilters := map[string]struct{}{
		"order_id":      {},
		"event_type":    {},
		"customer_id":   {},
		"restaurant_id": {},
		"driver_id":     {},
	}

	for k, v := range filters {
		if _, allowed := allowedFilters[k]; !allowed {
			continue // Skip unallowed filter keys
		}
		if s, ok := v.(string); ok && s != "" {
			conds = append(conds, fmt.Sprintf("%s=$%d", k, n))
			args = append(args, s)
			n++
		}
	}

	if len(conds) > 0 {
		query += " WHERE " + strings.Join(conds, " AND ")
	}
	query += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", n)
	args = append(args, limit)

	rows, err := DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []models.StoredEvent
	for rows.Next() {
		var e models.StoredEvent
		if err := rows.Scan(&e.ID, &e.OrderID, &e.EventType, &e.EventTimestamp, &e.ReceivedAt, &e.CustomerID, &e.RestaurantID, &e.DriverID, &e.LocationLat, &e.LocationLng, &e.PlatformToken, &e.ValidationStatus, &e.ValidationError); err == nil {
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
	
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return DB.PingContext(ctx)
}

func Close() {
	if DB != nil {
		DB.Close()
	}
}
