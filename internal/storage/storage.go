package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"ingestion-service/internal/logger"
	"ingestion-service/internal/models"

	_ "github.com/lib/pq"
)

// DB is the database handle
var DB *sql.DB

// Event buffer for async processing
type eventBuffer struct {
	events    []bufferedEvent
	mutex     sync.Mutex
	maxSize   int
	flushChan chan struct{}
}

type bufferedEvent struct {
	Event           models.DeliveryEvent
	PlatformToken   string
	ValidationStatus string
	ReceivedAt      int64
}

var buffer = &eventBuffer{
	events:    make([]bufferedEvent, 0, 100),
	maxSize:   50, // Flush every 50 events
	flushChan: make(chan struct{}, 1),
}

// QueueEvent adds event to buffer for async processing - returns immediately
func QueueEvent(event models.DeliveryEvent, platformToken, validationStatus string) {
	buffer.mutex.Lock()
	buffer.events = append(buffer.events, bufferedEvent{
		Event:           event,
		PlatformToken:   platformToken,
		ValidationStatus: validationStatus,
		ReceivedAt:      time.Now().Unix(),
	})
	shouldFlush := len(buffer.events) >= buffer.maxSize
	buffer.mutex.Unlock()

	if shouldFlush {
		select {
		case buffer.flushChan <- struct{}{}:
		default:
		}
	}
}

// StartAsyncWriter starts background goroutines for writing events
func StartAsyncWriter(workers int) {
	// Start worker pool
	for i := 0; i < workers; i++ {
		go flushWorker()
	}
	
	// Start periodic flusher
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		
		for range ticker.C {
			select {
			case buffer.flushChan <- struct{}{}:
			default:
			}
		}
	}()
}

func flushWorker() {
	for range buffer.flushChan {
		flushBuffer()
	}
}

func flushBuffer() {
	buffer.mutex.Lock()
	if len(buffer.events) == 0 {
		buffer.mutex.Unlock()
		return
	}
	
	// Take all events
	events := buffer.events
	buffer.events = make([]bufferedEvent, 0, 100)
	buffer.mutex.Unlock()

	// Batch insert
	if err := insertBatch(events); err != nil {
		logger.Error("batch insert failed", map[string]interface{}{
			"error": err.Error(),
			"count": len(events),
		})
	}
}

func insertBatch(events []bufferedEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Build multi-value INSERT
	valueStrings := make([]string, 0, len(events))
	valueArgs := make([]interface{}, 0, len(events)*12)
	
	for i, e := range events {
		base := i * 12
		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base+1, base+2, base+3, base+4, base+5, base+6,
			base+7, base+8, base+9, base+10, base+11, base+12,
		))
		valueArgs = append(valueArgs,
			e.Event.OrderID,
			e.Event.EventType,
			e.Event.EventTimestamp.Unix(),
			e.ReceivedAt,
			e.Event.CustomerID,
			e.Event.RestaurantID,
			e.Event.DriverID,
			e.Event.Location.Lat,
			e.Event.Location.Lng,
			e.PlatformToken,
			e.ValidationStatus,
			"",
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO events (order_id, event_type, event_timestamp, received_at,
		                    customer_id, restaurant_id, driver_id, location_lat, location_lng,
		                    platform_token, validation_status, validation_error)
		VALUES %s`, strings.Join(valueStrings, ","))

	_, err := DB.Exec(query, valueArgs...)
	return err
}

// StoreEvent - synchronous fallback (not used in hot path)
func StoreEvent(event models.DeliveryEvent, platformToken, validationStatus string) error {
	QueueEvent(event, platformToken, validationStatus)
	return nil
}

// StoreEventsBatch - kept for compatibility
func StoreEventsBatch(events []models.DeliveryEvent, platformToken, validationStatus string) error {
	for _, e := range events {
		QueueEvent(e, platformToken, validationStatus)
	}
	return nil
}

// QueryEvents retrieves events from the database
func QueryEvents(limit int, filters map[string]interface{}) ([]models.StoredEvent, error) {
	query := `
		SELECT id, order_id, event_type, event_timestamp, received_at,
		       customer_id, restaurant_id, driver_id, location_lat, location_lng,
		       platform_token, validation_status, validation_error
		FROM events`

	var args []interface{}
	argCount := 0

	if len(filters) > 0 {
		query += " WHERE"
		conditions := []string{}

		if orderID, ok := filters["order_id"].(string); ok && orderID != "" {
			argCount++
			conditions = append(conditions, fmt.Sprintf(" order_id = $%d", argCount))
			args = append(args, orderID)
		}

		if eventType, ok := filters["event_type"].(string); ok && eventType != "" {
			argCount++
			conditions = append(conditions, fmt.Sprintf(" event_type = $%d", argCount))
			args = append(args, eventType)
		}

		if customerID, ok := filters["customer_id"].(string); ok && customerID != "" {
			argCount++
			conditions = append(conditions, fmt.Sprintf(" customer_id = $%d", argCount))
			args = append(args, customerID)
		}

		if restaurantID, ok := filters["restaurant_id"].(string); ok && restaurantID != "" {
			argCount++
			conditions = append(conditions, fmt.Sprintf(" restaurant_id = $%d", argCount))
			args = append(args, restaurantID)
		}

		if len(conditions) > 0 {
			query += strings.Join(conditions, " AND")
		}
	}

	argCount++
	query += fmt.Sprintf(" ORDER BY received_at DESC LIMIT $%d", argCount)
	args = append(args, limit)

	rows, err := DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []models.StoredEvent
	for rows.Next() {
		var e models.StoredEvent
		var validationError sql.NullString
		if err := rows.Scan(&e.ID, &e.OrderID, &e.EventType, &e.EventTimestamp, &e.ReceivedAt,
			&e.CustomerID, &e.RestaurantID, &e.DriverID, &e.LocationLat, &e.LocationLng,
			&e.PlatformToken, &e.ValidationStatus, &validationError); err != nil {
			continue
		}
		if validationError.Valid {
			e.ValidationError = validationError.String
		}
		events = append(events, e)
	}

	return events, nil
}

// Close closes the database connection
func Close() {
	// Flush remaining events
	flushBuffer()
	
	if DB != nil {
		DB.Close()
	}
}

// InitDatabase opens the database connection
func InitDatabase(databaseURL string) error {
	var err error
	DB, err = sql.Open("postgres", databaseURL)
	if err != nil {
		return err
	}

	// Optimized connection pool
	DB.SetMaxOpenConns(50)
	DB.SetMaxIdleConns(50)
	DB.SetConnMaxLifetime(5 * time.Minute)

	if err = DB.Ping(); err != nil {
		return err
	}

	logger.Info("database initialized", nil)
	return nil
}
