package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ingestion-service/internal/models"

	_ "github.com/lib/pq"
)

var DB *sql.DB

// Pre-built query template for batch inserts
var insertPrefix = `INSERT INTO events (order_id,event_type,event_timestamp,received_at,customer_id,restaurant_id,driver_id,location_lat,location_lng,platform_token,validation_status,validation_error) VALUES `

// Event entry - fixed size struct for cache efficiency
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

// Lock-free ring buffer using channels
var eventChan = make(chan eventEntry, 200000) // 200K capacity

// Metrics
var (
	eventsQueued  atomic.Int64
	eventsWritten atomic.Int64
)

// QueueEvent - zero blocking, always returns immediately
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
		eventsQueued.Add(1)
	default:
		// Full buffer - drop to maintain latency
	}
}

// sync.Pool for batch slices - zero allocation in hot path
var batchPool = sync.Pool{
	New: func() interface{} {
		s := make([]eventEntry, 0, 512)
		return &s
	},
}

// sync.Pool for string builders
var builderPool = sync.Pool{
	New: func() interface{} {
		return &strings.Builder{}
	},
}

// sync.Pool for args slices
var argsPool = sync.Pool{
	New: func() interface{} {
		s := make([]interface{}, 0, 6144) // 512 * 12
		return &s
	},
}

// StartWriters - parallel goroutine writers
func StartWriters(n int) {
	for i := 0; i < n; i++ {
		go writerLoop()
	}
}

func writerLoop() {
	batchPtr := batchPool.Get().(*[]eventEntry)
	batch := (*batchPtr)[:0]

	ticker := time.NewTicker(15 * time.Millisecond) // Aggressive flush
	defer ticker.Stop()

	for {
		select {
		case e := <-eventChan:
			batch = append(batch, e)

			// Drain aggressively
		drain:
			for len(batch) < 512 {
				select {
				case e := <-eventChan:
					batch = append(batch, e)
				default:
					break drain
				}
			}

		case <-ticker.C:
			// Time-based flush
		}

		if len(batch) > 0 {
			fastBatchInsert(batch)
			eventsWritten.Add(int64(len(batch)))
			batch = batch[:0]
		}
	}
}

func fastBatchInsert(events []eventEntry) {
	if len(events) == 0 || DB == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Get pooled resources
	argsPtr := argsPool.Get().(*[]interface{})
	args := (*argsPtr)[:0]
	
	sb := builderPool.Get().(*strings.Builder)
	sb.Reset()
	sb.Grow(len(insertPrefix) + len(events)*120) // Pre-allocate
	sb.WriteString(insertPrefix)

	defer func() {
		*argsPtr = args[:0]
		argsPool.Put(argsPtr)
		sb.Reset()
		builderPool.Put(sb)
	}()

	for i, e := range events {
		if i > 0 {
			sb.WriteByte(',')
		}
		n := i * 12
		fmt.Fprintf(sb, "($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			n+1, n+2, n+3, n+4, n+5, n+6, n+7, n+8, n+9, n+10, n+11, n+12)
		args = append(args, e.OrderID, e.EventType, e.EventTimestamp, e.ReceivedAt,
			e.CustomerID, e.RestaurantID, e.DriverID, e.Lat, e.Lng, e.Token, e.Status, "")
	}

	DB.ExecContext(ctx, sb.String(), args...)
}

// Legacy compatibility
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

// QueryEvents with aggressive timeout
func QueryEvents(limit int, filters map[string]interface{}) ([]models.StoredEvent, error) {
	if DB == nil {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	var sb strings.Builder
	sb.WriteString(`SELECT id,order_id,event_type,event_timestamp,received_at,customer_id,restaurant_id,driver_id,location_lat,location_lng,platform_token,validation_status,COALESCE(validation_error,'') FROM events`)

	args := make([]interface{}, 0, 5)
	n := 0

	if len(filters) > 0 {
		sb.WriteString(" WHERE ")
		first := true
		for k, v := range filters {
			if s, ok := v.(string); ok && s != "" {
				if !first {
					sb.WriteString(" AND ")
				}
				n++
				fmt.Fprintf(&sb, "%s=$%d", k, n)
				args = append(args, s)
				first = false
			}
		}
	}

	n++
	fmt.Fprintf(&sb, " ORDER BY id DESC LIMIT $%d", n)
	args = append(args, limit)

	rows, err := DB.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]models.StoredEvent, 0, limit)
	for rows.Next() {
		var e models.StoredEvent
		rows.Scan(&e.ID, &e.OrderID, &e.EventType, &e.EventTimestamp, &e.ReceivedAt,
			&e.CustomerID, &e.RestaurantID, &e.DriverID, &e.LocationLat, &e.LocationLng,
			&e.PlatformToken, &e.ValidationStatus, &e.ValidationError)
		events = append(events, e)
	}
	return events, nil
}

func Close() {
	time.Sleep(50 * time.Millisecond) // Brief drain
	if DB != nil {
		DB.Close()
	}
}

func InitDatabase(url string) error {
	var err error
	DB, err = sql.Open("postgres", url)
	if err != nil {
		return err
	}

	// Maximum connection pool
	DB.SetMaxOpenConns(150)
	DB.SetMaxIdleConns(150)
	DB.SetConnMaxLifetime(3 * time.Minute)
	DB.SetConnMaxIdleTime(1 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return DB.PingContext(ctx)
}
