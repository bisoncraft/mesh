package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
)

const (
	defaultMinPeriod = 30 * time.Second
	defaultWeight    = 1.0

	// httpErrBodySnippetLimit is the max bytes of response body to include in a
	// non-2xx HTTP error.
	httpErrBodySnippetLimit = 4 << 10 // 4 KiB

	// maxJSONBytes is a safety cap for JSON decoding from HTTP responses.
	// Note: callers generally decode a small subset of fields, so responses
	// should be modest in size.
	maxJSONBytes = 10 << 20 // 10 MiB
)

// HTTPClient defines the requirements for implementing an http client.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// DoGet performs an HTTP GET request, returning the response or an error for
// non-2xx status codes.
func DoGet(ctx context.Context, client HTTPClient, url string, headers []http.Header) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("error generating request %q: %w", url, err)
	}

	for _, header := range headers {
		for k, vs := range header {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error fetching %q: %w", url, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		snippetBytes, _ := io.ReadAll(io.LimitReader(resp.Body, httpErrBodySnippetLimit))
		_ = resp.Body.Close()
		snippet := strings.TrimSpace(string(snippetBytes))
		if snippet != "" {
			return nil, fmt.Errorf("http %d fetching %q: %s", resp.StatusCode, url, snippet)
		}
		return nil, fmt.Errorf("http %d fetching %q", resp.StatusCode, url)
	}

	return resp, nil
}

// UnlimitedQuotaStatus returns a quota status indicating unlimited fetches.
func UnlimitedQuotaStatus() *sources.QuotaStatus {
	now := time.Now().UTC()
	return &sources.QuotaStatus{
		FetchesRemaining: math.MaxInt64,
		FetchesLimit:     math.MaxInt64,
		ResetTime:        now.Add(24 * time.Hour),
	}
}

// StreamDecodeJSON decodes JSON from a stream.
func StreamDecodeJSON(stream io.Reader, thing any) error {
	dec := json.NewDecoder(io.LimitReader(stream, maxJSONBytes))
	if err := dec.Decode(thing); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON")
		}
		return err
	}
	return nil
}
