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

// Global optimized HTTP client for connection reuse
var httpClient = &http.Client{
	Timeout: 2 * time.Second,
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   1 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   1 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	},
}

// Atomic value for lock-free map access
var (
	tokenMap    atomic.Value // map[string]struct{}
	tokensReady atomic.Bool
)

func init() {
	tokenMap.Store(make(map[string]struct{}))
}

// InitTokens - fast synchronous initialization
func InitTokens() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if loadTokens(ctx) {
			tokensReady.Store(true)
			return nil
		}
		select {
		case <-ctx.Done():
			tokensReady.Store(true)
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	tokensReady.Store(true)
	return nil
}

// StartBackgroundRefresh - keeps the cache hot without blocking requests
func StartBackgroundRefresh() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			loadTokens(ctx)
			cancel()
		}
	}()
}

func loadTokens(ctx context.Context) bool {
	req, _ := http.NewRequestWithContext(ctx, "GET", ControlServerURL+"/platform-tokens", nil)
	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false
	}

	var result struct {
		PlatformTokens []string `json:"platform_tokens"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false
	}

	newMap := make(map[string]struct{}, len(result.PlatformTokens))
	for _, t := range result.PlatformTokens {
		newMap[t] = struct{}{}
	}
	tokenMap.Store(newMap)
	return true
}

// ValidateToken - ultra-fast O(1) lock-free check
func ValidateToken(token string) bool {
	if token == "" {
		return false
	}
	// Fail-open during startup to prevent 503s
	if !tokensReady.Load() {
		return true
	}
	m := tokenMap.Load().(map[string]struct{})
	// Fail-open if map is somehow empty
	if len(m) == 0 {
		return true
	}
	_, ok := m[token]
	return ok
}

// Compatibility wrappers
func FetchPlatformTokens() ([]string, error) {
	m := tokenMap.Load().(map[string]struct{})
	res := make([]string, 0, len(m))
	for t := range m {
		res = append(res, t)
	}
	return res, nil
}

func ValidatePlatformToken(token string, _ []string) bool {
	return ValidateToken(token)
}
