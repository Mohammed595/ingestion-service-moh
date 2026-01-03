package validation

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// ControlServerURL is the URL of the control server
var ControlServerURL string

// tokenSet holds platform tokens in a map for O(1) lookup
type tokenSet struct {
	tokens  map[string]struct{}
	mutex   sync.RWMutex
	ready   bool
}

var validTokens = &tokenSet{
	tokens: make(map[string]struct{}),
}

// StartBackgroundTokenRefresh starts a goroutine that refreshes tokens periodically
// This ensures tokens are always available and requests never block
func StartBackgroundTokenRefresh() {
	// Initial load - blocking
	refreshTokensNow()
	
	// Background refresh every 30 seconds
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		
		for range ticker.C {
			refreshTokensNow()
		}
	}()
}

// refreshTokensNow fetches tokens from control server
func refreshTokensNow() {
	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	resp, err := client.Get(ControlServerURL + "/platform-tokens")
	if err != nil {
		return // Keep existing tokens on error
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	var result map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return
	}

	tokens, ok := result["platform_tokens"]
	if !ok {
		return
	}

	// Build new token set
	newSet := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		newSet[t] = struct{}{}
	}

	// Atomic swap
	validTokens.mutex.Lock()
	validTokens.tokens = newSet
	validTokens.ready = true
	validTokens.mutex.Unlock()
}

// ValidateToken checks if token is valid - O(1) lookup, never blocks
func ValidateToken(token string) bool {
	validTokens.mutex.RLock()
	defer validTokens.mutex.RUnlock()
	
	if !validTokens.ready {
		return false
	}
	
	_, exists := validTokens.tokens[token]
	return exists
}

// FetchPlatformTokens - kept for compatibility but now just returns current tokens
func FetchPlatformTokens() ([]string, error) {
	validTokens.mutex.RLock()
	defer validTokens.mutex.RUnlock()
	
	result := make([]string, 0, len(validTokens.tokens))
	for t := range validTokens.tokens {
		result = append(result, t)
	}
	return result, nil
}

// ValidatePlatformToken - kept for compatibility
func ValidatePlatformToken(token string, _ []string) bool {
	return ValidateToken(token)
}
