package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"ingestion-service/internal/models"

	_ "github.com/lib/pq"
)

var DB *sql.DB

// Event entry for buffering
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

// Large buffered channel - never blocks
var eventChan = make(chan eventEntry, 50000)

// QueueEvent adds event to buffer - NEVER BLOCKS
func QueueEvent(event models.DeliveryEvent, token, status string) {
	e := eventEntry{
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
	}
	
	select {
	case eventChan <- e:
	default:
		// Buffer full - drop (availability over durability)
	}
}

// StartWriters starts parallel DB writers
func StartWriters(n int) {
	for i := 0; i < n; i++ {
		go writerLoop()
	}
}

func writerLoop() {
	batch := make([]eventEntry, 0, 200)
	ticker := time.NewTicker(25 * time.Millisecond) // Flush every 25ms
	
	for {
		select {
		case e := <-eventChan:
			batch = append(batch, e)
			
			// Drain up to batch size
			for len(batch) < 200 {
				select {
				case e := <-eventChan:
					batch = append(batch, e)
				default:
					goto flush
				}
			}
			
		case <-ticker.C:
			// Time-based flush
		}
		
	flush:
		if len(batch) > 0 {
			batchInsert(batch)
			batch = batch[:0]
		}
	}
}

func batchInsert(events []eventEntry) {
	if len(events) == 0 || DB == nil {
		return
	}

	// Build multi-row INSERT for maximum throughput
	placeholders := make([]string, len(events))
	args := make([]interface{}, 0, len(events)*12)
	
	for i, e := range events {
		n := i * 12
		placeholders[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			n+1, n+2, n+3, n+4, n+5, n+6, n+7, n+8, n+9, n+10, n+11, n+12)
		args = append(args,
			e.OrderID, e.EventType, e.EventTimestamp, e.ReceivedAt,
			e.CustomerID, e.RestaurantID, e.DriverID, e.Lat, e.Lng,
			e.Token, e.Status, "")
	}

	query := `INSERT INTO events (order_id,event_type,event_timestamp,received_at,
		customer_id,restaurant_id,driver_id,location_lat,location_lng,
		platform_token,validation_status,validation_error) VALUES ` + 
		strings.Join(placeholders, ",")

	DB.Exec(query, args...)
}

// Legacy functions
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

// QueryEvents for GET requests
func QueryEvents(limit int, filters map[string]interface{}) ([]models.StoredEvent, error) {
	if DB == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	
	query := `SELECT id,order_id,event_type,event_timestamp,received_at,
		customer_id,restaurant_id,driver_id,location_lat,location_lng,
		platform_token,validation_status,COALESCE(validation_error,'') FROM events`

	var args []interface{}
	var conditions []string
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
	query += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", n)
	args = append(args, limit)

	rows, err := DB.Query(query, args...)
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
	// Drain buffer
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
	
	// Aggressive connection pooling
	DB.SetMaxOpenConns(50)
	DB.SetMaxIdleConns(50)
	DB.SetConnMaxLifetime(3 * time.Minute)
	DB.SetConnMaxIdleTime(1 * time.Minute)
	
	// Test connection
	for i := 0; i < 10; i++ {
		if err = DB.Ping(); err == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return err
}
