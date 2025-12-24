package models

// DeliveryEvent represents an incoming order lifecycle event
type DeliveryEvent struct {
	OrderID        string `json:"order_id"`
	EventType      string `json:"event_type"`
	EventTimestamp int64  `json:"event_timestamp"`
	EventSignature string `json:"event_signature"`
}

// StoredEvent represents what we store in the database
type StoredEvent struct {
	ID               int    `json:"id"`
	OrderID          string `json:"order_id"`
	EventType        string `json:"event_type"`
	EventTimestamp   int64  `json:"event_timestamp"`
	ReceivedAt       int64  `json:"received_at"`
	EventSignature   string `json:"event_signature"`
	PlatformToken    string `json:"platform_token"`
	ValidationStatus string `json:"validation_status"`
	ValidationError  string `json:"validation_error,omitempty"`
}
