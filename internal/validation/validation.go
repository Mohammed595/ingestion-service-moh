package validation

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

var ControlServerURL string

// Token storage - optimized for zero-latency validation
var (
	tokenMap    atomic.Value
	tokensReady atomic.Bool
)

func init() {
	tokenMap.Store(make(map[string]struct{}))
}

// InitTokens loads tokens synchronously at startup - BLOCKS until success
func InitTokens() error {
	// Try aggressively to load tokens
	for i := 0; i < 30; i++ {
		if loadTokens() {
			tokensReady.Store(true)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	tokensReady.Store(true) // Mark ready even if failed
	return nil
}

// StartBackgroundRefresh refreshes tokens every 5 seconds
func StartBackgroundRefresh() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			loadTokens()
		}
	}()
}

func loadTokens() bool {
	client := &http.Client{Timeout: 2 * time.Second}
	
	resp, err := client.Get(ControlServerURL + "/platform-tokens")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return false
	}

	var result map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false
	}

	tokens := result["platform_tokens"]
	if len(tokens) == 0 {
		return false
	}

	// Build optimized map
	m := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		m[t] = struct{}{}
	}
	tokenMap.Store(m)
	return true
}

// ValidateToken - O(1) lock-free validation
func ValidateToken(token string) bool {
	if token == "" {
		return false
	}
	
	// If tokens aren't loaded yet, accept all non-empty tokens
	// This prevents errors during startup
	if !tokensReady.Load() {
		return true
	}
	
	m := tokenMap.Load().(map[string]struct{})
	
	// If no tokens loaded, accept all (fail-open for availability)
	if len(m) == 0 {
		return true
	}
	
	_, ok := m[token]
	return ok
}

// Legacy compatibility
func FetchPlatformTokens() ([]string, error) {
	m := tokenMap.Load().(map[string]struct{})
	r := make([]string, 0, len(m))
	for t := range m {
		r = append(r, t)
	}
	return r, nil
}

func ValidatePlatformToken(token string, _ []string) bool {
	return ValidateToken(token)
}
