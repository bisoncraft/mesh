package oracle

import (
	"testing"
	"time"
)

func TestFetchTracker_RecordAndCounts(t *testing.T) {
	ft := newFetchTracker()
	now := time.Now()

	ft.recordFetch("source1", "node-a", now)
	ft.recordFetch("source1", "node-a", now.Add(-time.Hour))
	ft.recordFetch("source1", "node-b", now)
	ft.recordFetch("source2", "node-a", now)

	counts := ft.fetchCounts()
	if counts["source1"]["node-a"] != 2 {
		t.Errorf("expected 2, got %d", counts["source1"]["node-a"])
	}
	if counts["source1"]["node-b"] != 1 {
		t.Errorf("expected 1, got %d", counts["source1"]["node-b"])
	}
	if counts["source2"]["node-a"] != 1 {
		t.Errorf("expected 1, got %d", counts["source2"]["node-a"])
	}
}

func TestFetchTracker_LatestPerSource(t *testing.T) {
	ft := newFetchTracker()
	now := time.Now()

	ft.recordFetch("source1", "node-a", now.Add(-time.Hour))
	ft.recordFetch("source1", "node-b", now)
	ft.recordFetch("source2", "node-a", now.Add(-2*time.Hour))

	latest := ft.latestPerSource()

	if !latest["source1"].Equal(now) {
		t.Errorf("expected latest stamp for source1 to be %v, got %v", now, latest["source1"])
	}
	if !latest["source2"].Equal(now.Add(-2 * time.Hour)) {
		t.Errorf("expected latest stamp for source2 to be %v, got %v", now.Add(-2*time.Hour), latest["source2"])
	}
}

func TestFetchTracker_CountsExcludes24hOld(t *testing.T) {
	ft := newFetchTracker()
	now := time.Now()

	ft.recordFetch("source1", "node-a", now.Add(-25*time.Hour))
	ft.recordFetch("source1", "node-a", now)

	counts := ft.fetchCounts()
	if counts["source1"]["node-a"] != 1 {
		t.Errorf("expected count to be 1 (excluding old record), got %d", counts["source1"]["node-a"])
	}
}

func TestFetchTracker_OutOfOrderExpiry(t *testing.T) {
	ft := newFetchTracker()
	now := time.Now()

	// Insert a recent record followed by an expired one (out of order).
	ft.recordFetch("source1", "node-a", now)
	ft.recordFetch("source1", "node-a", now.Add(-25*time.Hour))

	counts := ft.fetchCounts()
	if counts["source1"]["node-a"] != 1 {
		t.Errorf("expected count to be 1 (out-of-order expired record should be dropped), got %d", counts["source1"]["node-a"])
	}
}

func TestFetchTracker_Empty(t *testing.T) {
	ft := newFetchTracker()

	counts := ft.fetchCounts()
	if len(counts) != 0 {
		t.Errorf("expected empty counts, got %d entries", len(counts))
	}

	latest := ft.latestPerSource()
	if len(latest) != 0 {
		t.Errorf("expected empty latest, got %d entries", len(latest))
	}
}
