package validation

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var ControlServerURL string

// Ultra-fast token storage with atomic operations
var (
	tokenMap     atomic.Value // map[string]struct{}
	tokenMutex   sync.Mutex
	initialized  atomic.Bool
)

func init() {
	tokenMap.Store(make(map[string]struct{}))
}

// InitTokens - MUST be called at startup, blocks until tokens loaded
func InitTokens() error {
	for i := 0; i < 10; i++ {
		if err := refreshTokens(); err == nil {
			initialized.Store(true)
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	// Even if refresh fails, mark as initialized to not block
	initialized.Store(true)
	return nil
}

// StartBackgroundRefresh starts periodic token refresh
func StartBackgroundRefresh() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			refreshTokens()
		}
	}()
}

func refreshTokens() error {
	client := &http.Client{Timeout: 3 * time.Second}
	
	resp, err := client.Get(ControlServerURL + "/platform-tokens")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return err
	}

	var result map[string][]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}

	tokens, ok := result["platform_tokens"]
	if !ok || len(tokens) == 0 {
		return err
	}

	// Build new map
	newMap := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		newMap[t] = struct{}{}
	}

	// Atomic swap - lock-free reads after this
	tokenMap.Store(newMap)
	return nil
}

// ValidateToken - O(1) lock-free lookup
func ValidateToken(token string) bool {
	if token == "" {
		return false
	}
	m := tokenMap.Load().(map[string]struct{})
	_, ok := m[token]
	return ok
}

// Legacy compatibility
func FetchPlatformTokens() ([]string, error) {
	m := tokenMap.Load().(map[string]struct{})
	result := make([]string, 0, len(m))
	for t := range m {
		result = append(result, t)
	}
	return result, nil
}

func ValidatePlatformToken(token string, _ []string) bool {
	return ValidateToken(token)
}
