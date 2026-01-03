package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"ingestion-service/internal/models"

	_ "github.com/lib/pq"
)

var DB *sql.DB

// High-performance event buffer
type eventEntry struct {
	OrderID          string
	EventType        string
	EventTimestamp   int64
	ReceivedAt       int64
	CustomerID       string
	RestaurantID     string
	DriverID         string
	Lat              float64
	Lng              float64
	PlatformToken    string
	ValidationStatus string
}

var (
	eventChan   = make(chan eventEntry, 10000) // Large buffer
	flushSignal = make(chan struct{}, 1)
)

// QueueEvent - non-blocking, always succeeds
func QueueEvent(event models.DeliveryEvent, platformToken, status string) {
	select {
	case eventChan <- eventEntry{
		OrderID:          event.OrderID,
		EventType:        event.EventType,
		EventTimestamp:   event.EventTimestamp.Unix(),
		ReceivedAt:       time.Now().Unix(),
		CustomerID:       event.CustomerID,
		RestaurantID:     event.RestaurantID,
		DriverID:         event.DriverID,
		Lat:              event.Location.Lat,
		Lng:              event.Location.Lng,
		PlatformToken:    platformToken,
		ValidationStatus: status,
	}:
	default:
		// Buffer full - drop event (better than blocking)
	}
}

// StartWriters starts background DB writers
func StartWriters(count int) {
	for i := 0; i < count; i++ {
		go writer()
	}
	
	// Periodic flush trigger
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		for range ticker.C {
			select {
			case flushSignal <- struct{}{}:
			default:
			}
		}
	}()
}

func writer() {
	batch := make([]eventEntry, 0, 100)
	
	for {
		// Collect events
		select {
		case e := <-eventChan:
			batch = append(batch, e)
			
			// Drain more if available (up to batch size)
			for len(batch) < 100 {
				select {
				case e := <-eventChan:
					batch = append(batch, e)
				default:
					goto flush
				}
			}
			
		case <-flushSignal:
			// Drain what's available
			for len(batch) < 100 {
				select {
				case e := <-eventChan:
					batch = append(batch, e)
				default:
					goto flush
				}
			}
		}
		
	flush:
		if len(batch) > 0 {
			insertBatch(batch)
			batch = batch[:0]
		}
	}
}

func insertBatch(events []eventEntry) {
	if len(events) == 0 {
		return
	}

	// Build multi-row INSERT
	values := make([]string, len(events))
	args := make([]interface{}, 0, len(events)*12)
	
	for i, e := range events {
		n := i * 12
		values[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			n+1, n+2, n+3, n+4, n+5, n+6, n+7, n+8, n+9, n+10, n+11, n+12)
		args = append(args, e.OrderID, e.EventType, e.EventTimestamp, e.ReceivedAt,
			e.CustomerID, e.RestaurantID, e.DriverID, e.Lat, e.Lng,
			e.PlatformToken, e.ValidationStatus, "")
	}

	query := `INSERT INTO events (order_id,event_type,event_timestamp,received_at,
		customer_id,restaurant_id,driver_id,location_lat,location_lng,
		platform_token,validation_status,validation_error) VALUES ` + strings.Join(values, ",")

	DB.Exec(query, args...)
}

// Legacy compatibility
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

// QueryEvents - for GET requests
func QueryEvents(limit int, filters map[string]interface{}) ([]models.StoredEvent, error) {
	query := `SELECT id,order_id,event_type,event_timestamp,received_at,
		customer_id,restaurant_id,driver_id,location_lat,location_lng,
		platform_token,validation_status,validation_error FROM events`

	args := []interface{}{}
	conditions := []string{}
	n := 0

	for k, v := range filters {
		if s, ok := v.(string); ok && s != "" {
			n++
			conditions = append(conditions, fmt.Sprintf("%s=$%d", k, n))
			args = append(args, s)
		}
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	
	n++
	query += fmt.Sprintf(" ORDER BY received_at DESC LIMIT $%d", n)
	args = append(args, limit)

	rows, err := DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []models.StoredEvent
	for rows.Next() {
		var e models.StoredEvent
		var verr sql.NullString
		rows.Scan(&e.ID, &e.OrderID, &e.EventType, &e.EventTimestamp, &e.ReceivedAt,
			&e.CustomerID, &e.RestaurantID, &e.DriverID, &e.LocationLat, &e.LocationLng,
			&e.PlatformToken, &e.ValidationStatus, &verr)
		if verr.Valid {
			e.ValidationError = verr.String
		}
		events = append(events, e)
	}
	return events, nil
}

var closeOnce sync.Once

func Close() {
	closeOnce.Do(func() {
		// Drain remaining events
		time.Sleep(200 * time.Millisecond)
		if DB != nil {
			DB.Close()
		}
	})
}

func InitDatabase(url string) error {
	var err error
	DB, err = sql.Open("postgres", url)
	if err != nil {
		return err
	}
	
	DB.SetMaxOpenConns(30)
	DB.SetMaxIdleConns(30)
	DB.SetConnMaxLifetime(5 * time.Minute)
	
	return DB.Ping()
}
