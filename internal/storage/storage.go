package storage

import (
	"database/sql"
	"time"

	"ingestion-service/internal/logger"
	"ingestion-service/internal/models"

	_ "github.com/lib/pq"
)

// DB is the database handle - initialized by InitDatabase
var DB *sql.DB

// InitDatabase opens the database connection and creates tables
func InitDatabase(databaseURL string) error {
	var err error
	DB, err = sql.Open("postgres", databaseURL)
	if err != nil {
		return err
	}

	// Test connection
	if err = DB.Ping(); err != nil {
		return err
	}

	// Create schema
	return createTable()
}

// createTable creates the events table if it doesn't exist
func createTable() error {
	// Create table
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS events (
		id SERIAL PRIMARY KEY,
		order_id TEXT NOT NULL,
		event_type TEXT NOT NULL,
		event_timestamp BIGINT NOT NULL,
		received_at BIGINT NOT NULL,
		event_signature TEXT NOT NULL,
		platform_token TEXT NOT NULL,
		validation_status TEXT NOT NULL,
		validation_error TEXT
	)`

	if _, err := DB.Exec(createTableSQL); err != nil {
		return err
	}

	// Create indexes
	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_order_id ON events(order_id)`,
		`CREATE INDEX IF NOT EXISTS idx_event_timestamp ON events(event_timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_received_at ON events(received_at)`,
		`CREATE INDEX IF NOT EXISTS idx_validation_status ON events(validation_status)`,
	}

	for _, indexSQL := range indexes {
		if _, err := DB.Exec(indexSQL); err != nil {
			return err
		}
	}

	logger.Info("database schema initialized", nil)
	return nil
}

// StoreEvent persists an event to the database
func StoreEvent(event models.DeliveryEvent, platformToken, validationStatus, validationError string) error {
	receivedAt := time.Now().Unix()

	_, err := DB.Exec(`
		INSERT INTO events (order_id, event_type, event_timestamp, received_at,
		                    event_signature, platform_token, validation_status, validation_error)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, event.OrderID, event.EventType, event.EventTimestamp, receivedAt,
		event.EventSignature, platformToken, validationStatus, validationError)

	if err != nil {
		logger.Error("failed to store event", map[string]interface{}{
			"error":    err.Error(),
			"order_id": event.OrderID,
		})
		return err
	}

	logger.Info("event stored", map[string]interface{}{
		"order_id":          event.OrderID,
		"validation_status": validationStatus,
	})

	return nil
}

// QueryEvents retrieves events from the database
func QueryEvents(limit int) ([]models.StoredEvent, error) {
	rows, err := DB.Query(`
		SELECT id, order_id, event_type, event_timestamp, received_at,
		       event_signature, platform_token, validation_status, validation_error
		FROM events
		ORDER BY received_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []models.StoredEvent
	for rows.Next() {
		var e models.StoredEvent
		var validationError sql.NullString
		if err := rows.Scan(&e.ID, &e.OrderID, &e.EventType, &e.EventTimestamp, &e.ReceivedAt,
			&e.EventSignature, &e.PlatformToken, &e.ValidationStatus, &validationError); err != nil {
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

// Close closes the database connection
func Close() {
	if DB != nil {
		DB.Close()
	}
}
