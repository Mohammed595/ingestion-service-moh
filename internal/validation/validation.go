package validation

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

var ControlServerURL string

// Global HTTP client with optimized connection pool
var httpClient = &http.Client{
	Timeout: 2 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DialContext: (&net.Dialer{
			Timeout:   1 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	},
}

// Token storage with atomic operations
var (
	tokenMap    atomic.Value
	tokensReady atomic.Bool
)

func init() {
	tokenMap.Store(make(map[string]struct{}))
}

// InitTokens loads tokens with context timeout
func InitTokens() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	for i := 0; i < 20; i++ {
		if loadTokensWithContext(ctx) {
			tokensReady.Store(true)
			return nil
		}
		
		select {
		case <-ctx.Done():
			tokensReady.Store(true)
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
	tokensReady.Store(true)
	return nil
}

// StartBackgroundRefresh with context
func StartBackgroundRefresh() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			loadTokensWithContext(ctx)
			cancel()
		}
	}()
}

func loadTokensWithContext(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, "GET", ControlServerURL+"/platform-tokens", nil)
	if err != nil {
		return false
	}

	resp, err := httpClient.Do(req)
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

	m := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		m[t] = struct{}{}
	}
	tokenMap.Store(m)
	return true
}

// ValidateToken - O(1) lock-free
func ValidateToken(token string) bool {
	if token == "" {
		return false
	}
	if !tokensReady.Load() {
		return true // Accept during startup
	}
	m := tokenMap.Load().(map[string]struct{})
	if len(m) == 0 {
		return true // Fail-open
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

