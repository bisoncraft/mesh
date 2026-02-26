package utils_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/utils"
	"github.com/decred/slog"
)

const testAsyncWriteDelay = 100 * time.Millisecond

func TestFileTrackedSourceLoadFromFile(t *testing.T) {
	tmpFile := t.TempDir() + "/quota.json"

	fetchCount := 0
	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		fetchCount++
		return &sources.RateInfo{Prices: []*sources.PriceUpdate{}}, nil
	}

	resetTime := time.Now().UTC().Add(24 * time.Hour)
	qfd := map[string]interface{}{
		"fetches_remaining": int64(9450),
		"reset_time":        resetTime.Format(time.RFC3339),
	}
	data, _ := json.Marshal(qfd)
	os.WriteFile(tmpFile, data, 0600)

	src, err := utils.NewFileTrackedSource(utils.FileTrackedSourceConfig{
		Name:         "test",
		MinPeriod:    30 * time.Second,
		FetchRates:   fetchRates,
		FetchesLimit: 10000,
		QuotaFile:    tmpFile,
		ResetTime:    func() time.Time { return time.Now().UTC().Add(24 * time.Hour) },
		Log:          slog.Disabled,
	})
	if err != nil {
		t.Fatalf("NewFileTrackedSource failed: %v", err)
	}

	status := src.QuotaStatus()
	if status.FetchesRemaining != 9450 {
		t.Errorf("expected 9450 remaining, got %d", status.FetchesRemaining)
	}
	if status.FetchesLimit != 10000 {
		t.Errorf("expected 10000 limit, got %d", status.FetchesLimit)
	}
}

func TestFileTrackedSourceMonthlyReset(t *testing.T) {
	tmpFile := t.TempDir() + "/quota.json"

	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		return &sources.RateInfo{Prices: []*sources.PriceUpdate{}}, nil
	}

	pastResetTime := time.Now().UTC().Add(-24 * time.Hour)
	qfd := map[string]interface{}{
		"fetches_remaining": int64(100),
		"reset_time":        pastResetTime.Format(time.RFC3339),
	}
	data, _ := json.Marshal(qfd)
	os.WriteFile(tmpFile, data, 0600)

	src, err := utils.NewFileTrackedSource(utils.FileTrackedSourceConfig{
		Name:         "test",
		MinPeriod:    30 * time.Second,
		FetchRates:   fetchRates,
		FetchesLimit: 10000,
		QuotaFile:    tmpFile,
		ResetTime:    func() time.Time { return time.Now().UTC().Add(24 * time.Hour) },
		Log:          slog.Disabled,
	})
	if err != nil {
		t.Fatalf("NewFileTrackedSource failed: %v", err)
	}

	status := src.QuotaStatus()
	if status.FetchesRemaining != 10000 {
		t.Errorf("expected 10000 remaining after reset, got %d", status.FetchesRemaining)
	}
}

func TestFileTrackedSourceConsumesCredits(t *testing.T) {
	tmpFile := t.TempDir() + "/quota.json"

	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		return &sources.RateInfo{Prices: []*sources.PriceUpdate{}}, nil
	}

	src, err := utils.NewFileTrackedSource(utils.FileTrackedSourceConfig{
		Name:         "test",
		MinPeriod:    30 * time.Second,
		FetchRates:   fetchRates,
		FetchesLimit: 100,
		QuotaFile:    tmpFile,
		ResetTime:    func() time.Time { return time.Now().UTC().Add(24 * time.Hour) },
		Log:          slog.Disabled,
	})
	if err != nil {
		t.Fatalf("NewFileTrackedSource failed: %v", err)
	}

	src.FetchRates(context.Background())
	time.Sleep(testAsyncWriteDelay)
	status := src.QuotaStatus()
	if status.FetchesRemaining != 99 {
		t.Errorf("expected 99 remaining after fetch, got %d", status.FetchesRemaining)
	}

	src.FetchRates(context.Background())
	time.Sleep(testAsyncWriteDelay)
	status = src.QuotaStatus()
	if status.FetchesRemaining != 98 {
		t.Errorf("expected 98 remaining after second fetch, got %d", status.FetchesRemaining)
	}
}

func TestFileTrackedSourceFloorsAtZero(t *testing.T) {
	tmpFile := t.TempDir() + "/quota.json"

	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		return &sources.RateInfo{Prices: []*sources.PriceUpdate{}}, nil
	}

	src, err := utils.NewFileTrackedSource(utils.FileTrackedSourceConfig{
		Name:         "test",
		MinPeriod:    30 * time.Second,
		FetchRates:   fetchRates,
		FetchesLimit: 2,
		QuotaFile:    tmpFile,
		ResetTime:    func() time.Time { return time.Now().UTC().Add(24 * time.Hour) },
		Log:          slog.Disabled,
	})
	if err != nil {
		t.Fatalf("NewFileTrackedSource failed: %v", err)
	}

	src.FetchRates(context.Background())
	time.Sleep(testAsyncWriteDelay)
	status := src.QuotaStatus()
	if status.FetchesRemaining != 1 {
		t.Errorf("expected 1 remaining, got %d", status.FetchesRemaining)
	}

	src.FetchRates(context.Background())
	time.Sleep(testAsyncWriteDelay)
	status = src.QuotaStatus()
	if status.FetchesRemaining != 0 {
		t.Errorf("expected 0 remaining after overdraw, got %d", status.FetchesRemaining)
	}
}

func TestFileTrackedSourceWritesFile(t *testing.T) {
	tmpFile := t.TempDir() + "/quota.json"

	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		return &sources.RateInfo{Prices: []*sources.PriceUpdate{}}, nil
	}

	src, err := utils.NewFileTrackedSource(utils.FileTrackedSourceConfig{
		Name:         "test",
		MinPeriod:    30 * time.Second,
		FetchRates:   fetchRates,
		FetchesLimit: 100,
		QuotaFile:    tmpFile,
		ResetTime:    func() time.Time { return time.Now().UTC().Add(24 * time.Hour) },
		Log:          slog.Disabled,
	})
	if err != nil {
		t.Fatalf("NewFileTrackedSource failed: %v", err)
	}

	src.FetchRates(context.Background())
	time.Sleep(testAsyncWriteDelay)
	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("failed to read quota file: %v", err)
	}

	var qfd map[string]interface{}
	json.Unmarshal(data, &qfd)

	if remaining, ok := qfd["fetches_remaining"].(float64); ok {
		if int64(remaining) != 99 {
			t.Errorf("expected 99 fetches remaining in file, got %v", remaining)
		}
	}
}

func TestFileTrackedSourceFieldAccessors(t *testing.T) {
	tmpFile := t.TempDir() + "/quota.json"

	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		return &sources.RateInfo{Prices: []*sources.PriceUpdate{}}, nil
	}

	src, err := utils.NewFileTrackedSource(utils.FileTrackedSourceConfig{
		Name:         "testname",
		Weight:       0.5,
		MinPeriod:    60 * time.Second,
		FetchRates:   fetchRates,
		FetchesLimit: 100,
		QuotaFile:    tmpFile,
		ResetTime:    func() time.Time { return time.Now().UTC().Add(24 * time.Hour) },
		Log:          slog.Disabled,
	})
	if err != nil {
		t.Fatalf("NewFileTrackedSource failed: %v", err)
	}

	if src.Name() != "testname" {
		t.Errorf("expected name testname, got %s", src.Name())
	}
	if src.Weight() != 0.5 {
		t.Errorf("expected weight 0.5, got %f", src.Weight())
	}
	if src.MinPeriod() != 60*time.Second {
		t.Errorf("expected min period 60s, got %v", src.MinPeriod())
	}
}

func TestFileTrackedSourceQuotaStatus(t *testing.T) {
	tmpFile := t.TempDir() + "/quota.json"

	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		return &sources.RateInfo{Prices: []*sources.PriceUpdate{}}, nil
	}

	src, err := utils.NewFileTrackedSource(utils.FileTrackedSourceConfig{
		Name:         "test",
		MinPeriod:    30 * time.Second,
		FetchRates:   fetchRates,
		FetchesLimit: 100,
		QuotaFile:    tmpFile,
		ResetTime:    func() time.Time { return time.Now().UTC().Add(24 * time.Hour) },
		Log:          slog.Disabled,
	})
	if err != nil {
		t.Fatalf("NewFileTrackedSource failed: %v", err)
	}

	status := src.QuotaStatus()
	if status.FetchesRemaining != 100 {
		t.Errorf("expected 100 remaining, got %d", status.FetchesRemaining)
	}
	if status.FetchesLimit != 100 {
		t.Errorf("expected 100 limit, got %d", status.FetchesLimit)
	}
	if status.ResetTime.IsZero() {
		t.Error("reset time should not be zero")
	}
}
