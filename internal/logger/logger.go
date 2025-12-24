package logger

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

// Simple structured JSON logging
// Students can improve this with proper logging libraries (zap, logrus, etc.)

func Info(message string, fields map[string]interface{}) {
	logJSON("info", message, fields)
}

func Warn(message string, fields map[string]interface{}) {
	logJSON("warn", message, fields)
}

func Error(message string, fields map[string]interface{}) {
	logJSON("error", message, fields)
}

func Fatal(message string, fields map[string]interface{}) {
	logJSON("fatal", message, fields)
	os.Exit(1)
}

func logJSON(level string, message string, fields map[string]interface{}) {
	logEntry := map[string]interface{}{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"level":     level,
		"message":   message,
		"service":   "ingestion",
	}

	if fields != nil {
		for k, v := range fields {
			logEntry[k] = v
		}
	}

	jsonBytes, _ := json.Marshal(logEntry)
	log.Println(string(jsonBytes))
}
