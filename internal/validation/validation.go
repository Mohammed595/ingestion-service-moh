package validation

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var ControlServerURL string

// Global optimized transport - connection reuse, no allocation per request
var globalTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   500 * time.Millisecond,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}).DialContext,
	ForceAttemptHTTP2:     false, // HTTP/1.1 is faster for small requests
	MaxIdleConns:          200,
	MaxIdleConnsPerHost:   200,
	MaxConnsPerHost:       200,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   500 * time.Millisecond,
	ExpectContinueTimeout: 100 * time.Millisecond,
	DisableCompression:    true,
	DisableKeepAlives:     false,
}

var httpClient = &http.Client{
	Transport: globalTransport,
	Timeout:   1500 * time.Millisecond,
}

// Token storage - lock-free atomic operations
var (
	tokenMap    atomic.Value // map[string]struct{}
	tokensReady atomic.Bool
	tokenMu     sync.Mutex
)

// sync.Pool for response body reading
var bodyPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 8192)
		return &b
	},
}

func init() {
	tokenMap.Store(make(map[string]struct{}))
}

// InitTokens - blocking init with fast timeout
func InitTokens() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 0; i < 15; i++ {
		if loadTokensFast(ctx) {
			tokensReady.Store(true)
			return
		}
		select {
		case <-ctx.Done():
			tokensReady.Store(true)
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	tokensReady.Store(true)
}

// StartBackgroundRefresh - non-blocking background refresh
func StartBackgroundRefresh() {
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			loadTokensFast(ctx)
			cancel()
		}
	}()
}

func loadTokensFast(ctx context.Context) bool {
	req, _ := http.NewRequestWithContext(ctx, "GET", ControlServerURL+"/platform-tokens", nil)
	req.Header.Set("Connection", "keep-alive")

	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}

	// Read body with pooled buffer
	bufPtr := bodyPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	defer func() {
		*bufPtr = buf[:0]
		bodyPool.Put(bufPtr)
	}()

	buf, err = io.ReadAll(resp.Body)
	resp.Body.Close()

	if err != nil || resp.StatusCode != 200 {
		return false
	}

	var result struct {
		PlatformTokens []string `json:"platform_tokens"`
	}
	if json.Unmarshal(buf, &result) != nil || len(result.PlatformTokens) == 0 {
		return false
	}

	// Build map atomically
	m := make(map[string]struct{}, len(result.PlatformTokens))
	for _, t := range result.PlatformTokens {
		m[t] = struct{}{}
	}

	tokenMu.Lock()
	tokenMap.Store(m)
	tokenMu.Unlock()
	return true
}

// ValidateToken - O(1) lock-free, zero allocation
func ValidateToken(token string) bool {
	if len(token) == 0 {
		return false
	}
	if !tokensReady.Load() {
		return true
	}
	m := tokenMap.Load().(map[string]struct{})
	if len(m) == 0 {
		return true
	}
	_, ok := m[token]
	return ok
}

// Legacy
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
