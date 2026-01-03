package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"ingestion-service/internal/logger"
	"ingestion-service/internal/models"

	_ "github.com/lib/pq"
)

// DB is the database handle - initialized by InitDatabase
var DB *sql.DB

// Prepared statements for performance
var insertEventStmt *sql.Stmt

// StoreEvent persists an event to the database using prepared statement
func StoreEvent(event models.DeliveryEvent, platformToken, validationStatus string) error {
	receivedAt := time.Now().Unix()
	eventTimestamp := event.EventTimestamp.Unix()

	_, err := insertEventStmt.Exec(
		event.OrderID, event.EventType, eventTimestamp, receivedAt,
		event.CustomerID, event.RestaurantID, event.DriverID,
		event.Location.Lat, event.Location.Lng,
		platformToken, validationStatus, "")

	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	return nil
}

// StoreEventsBatch persists multiple events in a single transaction for high performance
func StoreEventsBatch(events []models.DeliveryEvent, platformToken, validationStatus string) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Prepare statement within transaction
	stmt, err := tx.Prepare(`
		INSERT INTO events (order_id, event_type, event_timestamp, received_at,
		                    customer_id, restaurant_id, driver_id, location_lat, location_lng,
		                    platform_token, validation_status, validation_error)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch insert statement: %w", err)
	}
	defer stmt.Close()

	receivedAt := time.Now().Unix()
	for _, event := range events {
		eventTimestamp := event.EventTimestamp.Unix()
		_, err = stmt.Exec(
			event.OrderID, event.EventType, eventTimestamp, receivedAt,
			event.CustomerID, event.RestaurantID, event.DriverID,
			event.Location.Lat, event.Location.Lng,
			platformToken, validationStatus, "")
		if err != nil {
			return fmt.Errorf("failed to insert event in batch: %w", err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// QueryEvents retrieves events from the database with safe filtering
func QueryEvents(limit int, filters map[string]interface{}) ([]models.StoredEvent, error) {
	query := `
		SELECT id, order_id, event_type, event_timestamp, received_at,
		       customer_id, restaurant_id, driver_id, location_lat, location_lng,
		       platform_token, validation_status, validation_error
		FROM events`

	var args []interface{}
	argCount := 0

	// Build WHERE clause safely
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

	// Add ORDER BY and LIMIT
	argCount++
	query += fmt.Sprintf(" ORDER BY received_at DESC LIMIT $%d", argCount)
	args = append(args, limit)

	rows, err := DB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var events []models.StoredEvent
	for rows.Next() {
		var e models.StoredEvent
		var validationError sql.NullString
		if err := rows.Scan(&e.ID, &e.OrderID, &e.EventType, &e.EventTimestamp, &e.ReceivedAt,
			&e.CustomerID, &e.RestaurantID, &e.DriverID, &e.LocationLat, &e.LocationLng,
			&e.PlatformToken, &e.ValidationStatus, &validationError); err != nil {
			logger.Error("failed to scan row", map[string]interface{}{"error": err.Error()})
			continue
		}
		if validationError.Valid {
			e.ValidationError = validationError.String
		}
		events = append(events, e)
	}

	return events, nil
}

// Close closes the database connection and prepared statements
func Close() {
	if insertEventStmt != nil {
		insertEventStmt.Close()
	}
	if DB != nil {
		DB.Close()
	}
}

// InitDatabase opens the database connection with optimized settings
func InitDatabase(databaseURL string) error {
	var err error
	DB, err = sql.Open("postgres", databaseURL)
	if err != nil {
		return err
	}

	// Configure connection pool for high performance
	DB.SetMaxOpenConns(25)                 // Maximum open connections
	DB.SetMaxIdleConns(25)                 // Maximum idle connections
	DB.SetConnMaxLifetime(5 * time.Minute) // Maximum connection lifetime

	// Test connection
	if err = DB.Ping(); err != nil {
		return err
	}

	// Prepare statement for high-performance inserts
	insertEventStmt, err = DB.Prepare(`
		INSERT INTO events (order_id, event_type, event_timestamp, received_at,
		                    customer_id, restaurant_id, driver_id, location_lat, location_lng,
		                    platform_token, validation_status, validation_error)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}

	logger.Info("database connection established", map[string]interface{}{
		"max_open_conns": 25,
		"max_idle_conns": 25,
		"conn_max_lifetime": "5m",
		"prepared_statements": true,
	})
	return nil
}
