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
)

var DB *sql.DB

// Event structure for buffering
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

// sync.Pool for zero-allocation event handling
var eventPool = sync.Pool{
	New: func() interface{} {
		return &eventEntry{}
	},
}

// sync.Pool for batch slices
var batchPool = sync.Pool{
	New: func() interface{} {
		s := make([]eventEntry, 0, 256)
		return &s
	},
}

// High-capacity channel
var eventChan = make(chan eventEntry, 100000)

// QueueEvent - zero-allocation fast path
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
		// Drop if full - availability over durability
	}
}

// StartWriters with optimized goroutine pool
func StartWriters(n int) {
	for i := 0; i < n; i++ {
		go writerLoop()
	}
}

func writerLoop() {
	// Get batch from pool
	batchPtr := batchPool.Get().(*[]eventEntry)
	batch := (*batchPtr)[:0]
	
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case e := <-eventChan:
			batch = append(batch, e)
			
			// Aggressive drain
			for len(batch) < 256 {
				select {
				case e := <-eventChan:
					batch = append(batch, e)
				default:
					goto flush
				}
			}
			
		case <-ticker.C:
			// Time-triggered flush
		}
		
	flush:
		if len(batch) > 0 {
			batchInsertWithContext(batch)
			batch = batch[:0]
		}
	}
}

// sync.Pool for query args
var argsPool = sync.Pool{
	New: func() interface{} {
		s := make([]interface{}, 0, 3072) // 256 * 12
		return &s
	},
}

// sync.Pool for placeholders
var placeholderPool = sync.Pool{
	New: func() interface{} {
		s := make([]string, 0, 256)
		return &s
	},
}

func batchInsertWithContext(events []eventEntry) {
	if len(events) == 0 || DB == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Get from pools
	argsPtr := argsPool.Get().(*[]interface{})
	args := (*argsPtr)[:0]
	
	phPtr := placeholderPool.Get().(*[]string)
	placeholders := (*phPtr)[:0]
	
	defer func() {
		*argsPtr = args[:0]
		argsPool.Put(argsPtr)
		*phPtr = placeholders[:0]
		placeholderPool.Put(phPtr)
	}()

	for i, e := range events {
		n := i * 12
		placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			n+1, n+2, n+3, n+4, n+5, n+6, n+7, n+8, n+9, n+10, n+11, n+12))
		args = append(args,
			e.OrderID, e.EventType, e.EventTimestamp, e.ReceivedAt,
			e.CustomerID, e.RestaurantID, e.DriverID, e.Lat, e.Lng,
			e.Token, e.Status, "")
	}

	query := `INSERT INTO events (order_id,event_type,event_timestamp,received_at,
		customer_id,restaurant_id,driver_id,location_lat,location_lng,
		platform_token,validation_status,validation_error) VALUES ` + 
		strings.Join(placeholders, ",")

	DB.ExecContext(ctx, query, args...)
}

// Legacy
func StoreEvent(event models.DeliveryEvent, token, status string) error {
	QueueEvent(event, token, status)
	return nil
}

func StoreEventsBatch(events []models.DeliveryEvent, token, status string) error {
	for _, e := range events {
		QueueEvent(e, token, status)
	}
	return nil
}

// QueryEvents with context timeout
func QueryEvents(limit int, filters map[string]interface{}) ([]models.StoredEvent, error) {
	if DB == nil {
		return nil, fmt.Errorf("db nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	query := `SELECT id,order_id,event_type,event_timestamp,received_at,
		customer_id,restaurant_id,driver_id,location_lat,location_lng,
		platform_token,validation_status,COALESCE(validation_error,'') FROM events`

	var args []interface{}
	var conds []string
	n := 0

	for k, v := range filters {
		if s, ok := v.(string); ok && s != "" {
			n++
			conds = append(conds, fmt.Sprintf("%s=$%d", k, n))
			args = append(args, s)
		}
	}

	if len(conds) > 0 {
		query += " WHERE " + strings.Join(conds, " AND ")
	}
	n++
	query += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", n)
	args = append(args, limit)

	rows, err := DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []models.StoredEvent
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
	time.Sleep(100 * time.Millisecond)
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
	
	// Aggressive global connection pool
	DB.SetMaxOpenConns(100)
	DB.SetMaxIdleConns(100)
	DB.SetConnMaxLifetime(5 * time.Minute)
	DB.SetConnMaxIdleTime(2 * time.Minute)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	return DB.PingContext(ctx)
}
