package validation

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"ingestion-service/internal/logger"
	"ingestion-service/internal/metrics"
)

// ControlServerURL is the URL of the control server
var ControlServerURL string

// FetchPlatformTokens calls the control server to get valid platform tokens
// 🚨 OPTIMIZATION OPPORTUNITY: This is called on EVERY event!
// Students should implement caching here to reduce calls to the slow control server
func FetchPlatformTokens() ([]string, error) {
	metrics.PlatformTokensRequestsTotal.Inc()
	startTime := time.Now()
	defer func() {
		metrics.PlatformTokensRequestDuration.Observe(time.Since(startTime).Seconds())
	}()

	logger.Info("fetching platform tokens from control server", map[string]interface{}{
		"url": ControlServerURL + "/platform-tokens",
	})

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(ControlServerURL + "/platform-tokens")
	if err != nil {
		return nil, fmt.Errorf("failed to call control server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("control server returned status %d", resp.StatusCode)
	}

	var result map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	tokens, ok := result["platform_tokens"]
	if !ok {
		return nil, fmt.Errorf("platform_tokens not found in response")
	}

	logger.Info("platform tokens fetched", map[string]interface{}{
		"count":    len(tokens),
		"duration": time.Since(startTime).Milliseconds(),
	})

	return tokens, nil
}

// ValidatePlatformToken checks if a token exists in the list of valid tokens
func ValidatePlatformToken(token string, validTokens []string) bool {
	for _, validToken := range validTokens {
		if validToken == token {
			return true
		}
	}
	return false
}
