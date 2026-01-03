package validation

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ControlServerURL is the URL of the control server
var ControlServerURL string

// TokenCache holds cached platform tokens with TTL and circuit breaker
type TokenCache struct {
	tokens        []string
	expires       time.Time
	mutex         sync.RWMutex
	ttl           time.Duration
	lastErr       error
	lastErrAt     time.Time
	failureCount  int
	lastFailureAt time.Time
	openTimeout   time.Duration
}

// Circuit breaker states
const (
	maxFailures     = 5
	circuitOpenTime = 30 * time.Second
)

// Global token cache instance
var tokenCache = &TokenCache{
	ttl:         5 * time.Minute, // Cache tokens for 5 minutes
	openTimeout: circuitOpenTime, // Circuit breaker timeout
}

// FetchPlatformTokens returns cached platform tokens or fetches new ones if expired
func FetchPlatformTokens() ([]string, error) {
	tokenCache.mutex.RLock()
	// Check if cache is still valid
	if time.Now().Before(tokenCache.expires) && len(tokenCache.tokens) > 0 {
		tokens := make([]string, len(tokenCache.tokens))
		copy(tokens, tokenCache.tokens)
		tokenCache.mutex.RUnlock()
		return tokens, nil
	}
	tokenCache.mutex.RUnlock()

	// Cache expired or empty, need to refresh
	return tokenCache.refreshTokens()
}

// refreshTokens fetches new tokens from control server with thread safety and circuit breaker
func (tc *TokenCache) refreshTokens() ([]string, error) {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	// Double-check after acquiring write lock
	if time.Now().Before(tc.expires) && len(tc.tokens) > 0 {
		tokens := make([]string, len(tc.tokens))
		copy(tokens, tc.tokens)
		return tokens, nil
	}

	// Circuit breaker: if we've had too many failures recently, fail fast
	if tc.failureCount >= maxFailures {
		if time.Since(tc.lastFailureAt) < tc.openTimeout {
			// Circuit is open, return stale data if available
			if len(tc.tokens) > 0 {
				tokens := make([]string, len(tc.tokens))
				copy(tokens, tc.tokens)
				return tokens, nil
			}
			return nil, fmt.Errorf("circuit breaker open: too many failures")
		}
		// Reset circuit breaker after timeout
		tc.failureCount = 0
	}

	// Fetch new tokens
	client := &http.Client{
		Timeout: 5 * time.Second, // Add timeout for resilience
	}

	resp, err := client.Get(ControlServerURL + "/platform-tokens")
	if err != nil {
		tc.recordFailure(err)
		// If we have stale tokens, return them as fallback
		if len(tc.tokens) > 0 {
			tokens := make([]string, len(tc.tokens))
			copy(tokens, tc.tokens)
			return tokens, nil
		}
		return nil, fmt.Errorf("failed to fetch tokens: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("control server returned status %d", resp.StatusCode)
		tc.recordFailure(err)
		// Return stale data if available
		if len(tc.tokens) > 0 {
			tokens := make([]string, len(tc.tokens))
			copy(tokens, tc.tokens)
			return tokens, nil
		}
		return nil, err
	}

	var result map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		tc.recordFailure(err)
		// Return stale data if available
		if len(tc.tokens) > 0 {
			tokens := make([]string, len(tc.tokens))
			copy(tokens, tc.tokens)
			return tokens, nil
		}
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	tokens, ok := result["platform_tokens"]
	if !ok {
		err := fmt.Errorf("platform_tokens not found in response")
		tc.recordFailure(err)
		// Return stale data if available
		if len(tc.tokens) > 0 {
			tokens := make([]string, len(tc.tokens))
			copy(tokens, tc.tokens)
			return tokens, nil
		}
		return nil, err
	}

	// Success: update cache and reset circuit breaker
	tc.tokens = make([]string, len(tokens))
	copy(tc.tokens, tokens)
	tc.expires = time.Now().Add(tc.ttl)
	tc.lastErr = nil
	tc.failureCount = 0 // Reset circuit breaker on success

	return tokens, nil
}

// recordFailure updates the circuit breaker state on failure
func (tc *TokenCache) recordFailure(err error) {
	tc.lastErr = err
	tc.lastErrAt = time.Now()
	tc.failureCount++
	tc.lastFailureAt = time.Now()
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
