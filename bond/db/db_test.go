package db

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/bond"
	"github.com/decred/slog"
)

func TestDB(t *testing.T) {
	t.Run("StoreBond", testStoreBond)
	t.Run("BondsByAccount", testBondsByAccount)
	t.Run("BondStrength", testBondStrength)
	t.Run("PruneExpiredBonds", testPruneExpiredBonds)
	t.Run("BackgroundPruning", testBackgroundPruning)
}

func testStoreBond(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	accountID := "test-account"
	bp := &bond.BondParams{
		ID:       "0:abc123:0",
		Strength: 100,
		Expiry:   time.Now().Add(time.Hour),
	}

	if err := db.StoreBond(accountID, bp); err != nil {
		t.Fatalf("StoreBond failed: %v", err)
	}

	// Verify bond was stored
	bonds, err := db.BondsByAccount(accountID)
	if err != nil {
		t.Fatalf("BondsByAccount failed: %v", err)
	}

	if len(bonds) != 1 {
		t.Fatalf("expected 1 bond, got %d", len(bonds))
	}

	if bonds[0].ID != bp.ID || bonds[0].Strength != bp.Strength {
		t.Fatalf("stored bond doesn't match: %+v vs %+v", bonds[0], bp)
	}
}

func testBondsByAccount(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	accountID := "account-1"
	bonds := []*bond.BondParams{
		{ID: "0:tx1:0", Strength: 50, Expiry: time.Now().Add(time.Hour)},
		{ID: "42:tx2:0", Strength: 75, Expiry: time.Now().Add(2 * time.Hour)},
		{ID: "0:tx3:1", Strength: 100, Expiry: time.Now().Add(3 * time.Hour)},
	}

	for _, bp := range bonds {
		if err := db.StoreBond(accountID, bp); err != nil {
			t.Fatalf("StoreBond failed: %v", err)
		}
	}

	// Retrieve bonds for account
	retrieved, err := db.BondsByAccount(accountID)
	if err != nil {
		t.Fatalf("BondsByAccount failed: %v", err)
	}

	if len(retrieved) != len(bonds) {
		t.Fatalf("expected %d bonds, got %d", len(bonds), len(retrieved))
	}

	// Check all bonds are present
	bondMap := make(map[string]*bond.BondParams)
	for _, b := range retrieved {
		bondMap[b.ID] = b
	}

	for _, bp := range bonds {
		if b, ok := bondMap[bp.ID]; !ok || b.Strength != bp.Strength {
			t.Fatalf("bond %s not found or doesn't match", bp.ID)
		}
	}

	// Verify different accounts are isolated
	otherAccountID := "account-2"
	otherBonds := []*bond.BondParams{
		{ID: "0:tx4:0", Strength: 200, Expiry: time.Now().Add(time.Hour)},
	}
	for _, bp := range otherBonds {
		if err := db.StoreBond(otherAccountID, bp); err != nil {
			t.Fatalf("StoreBond failed: %v", err)
		}
	}

	retrieved2, err := db.BondsByAccount(accountID)
	if err != nil {
		t.Fatalf("BondsByAccount failed: %v", err)
	}

	if len(retrieved2) != len(bonds) {
		t.Fatalf("account isolation failed: expected %d bonds for account-1, got %d", len(bonds), len(retrieved2))
	}
}

func testBondStrength(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	accountID := "strength-test"
	bonds := []*bond.BondParams{
		{ID: "0:tx1:0", Strength: 100, Expiry: time.Now().Add(time.Hour)},
		{ID: "42:tx2:0", Strength: 200, Expiry: time.Now().Add(time.Hour)},
		{ID: "0:tx3:0", Strength: 150, Expiry: time.Now().Add(time.Hour)},
	}

	for _, bp := range bonds {
		if err := db.StoreBond(accountID, bp); err != nil {
			t.Fatalf("StoreBond failed: %v", err)
		}
	}

	strength, err := db.BondStrength(accountID)
	if err != nil {
		t.Fatalf("BondStrength failed: %v", err)
	}

	expected := uint32(450)
	if strength != expected {
		t.Fatalf("expected strength %d, got %d", expected, strength)
	}
}

func testPruneExpiredBonds(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	accountID := "prune-test"
	now := time.Now()

	// Store bonds with different expiry times
	expiredBond := &bond.BondParams{
		ID:       "0:expired:0",
		Strength: 50,
		Expiry:   now.Add(-time.Hour), // Already expired
	}

	activeBond := &bond.BondParams{
		ID:       "0:active:0",
		Strength: 100,
		Expiry:   now.Add(time.Hour), // Not expired
	}

	if err := db.StoreBond(accountID, expiredBond); err != nil {
		t.Fatalf("StoreBond failed: %v", err)
	}

	if err := db.StoreBond(accountID, activeBond); err != nil {
		t.Fatalf("StoreBond failed: %v", err)
	}

	// Prune expired bonds
	if err := db.PruneExpiredBonds(now); err != nil {
		t.Fatalf("PruneExpiredBonds failed: %v", err)
	}

	// Verify expired bond is gone, active remains
	bonds, err := db.BondsByAccount(accountID)
	if err != nil {
		t.Fatalf("BondsByAccount failed: %v", err)
	}

	if len(bonds) != 1 || bonds[0].ID != activeBond.ID {
		t.Fatalf("expired bond not pruned: got %+v", bonds)
	}

	strength, err := db.BondStrength(accountID)
	if err != nil {
		t.Fatalf("BondStrength failed: %v", err)
	}

	if strength != activeBond.Strength {
		t.Fatalf("expected strength %d after prune, got %d", activeBond.Strength, strength)
	}
}

func testBackgroundPruning(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	accountID := "background-prune"
	now := time.Now()

	expiredBond := &bond.BondParams{
		ID:       "0:expired:0",
		Strength: 50,
		Expiry:   now.Add(-time.Hour),
	}

	activeBond := &bond.BondParams{
		ID:       "0:active:0",
		Strength: 100,
		Expiry:   now.Add(time.Hour),
	}

	if err := db.StoreBond(accountID, expiredBond); err != nil {
		t.Fatalf("StoreBond failed: %v", err)
	}

	if err := db.StoreBond(accountID, activeBond); err != nil {
		t.Fatalf("StoreBond failed: %v", err)
	}

	// Start background pruning
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := db.Run(ctx); err != nil && err != context.Canceled {
			t.Errorf("Run failed: %v", err)
		}
	}()

	// Manually trigger pruning since background goroutine runs hourly
	time.Sleep(100 * time.Millisecond)

	if err := db.PruneExpiredBonds(now); err != nil {
		t.Fatalf("PruneExpiredBonds failed: %v", err)
	}

	bonds, err := db.BondsByAccount(accountID)
	if err != nil {
		t.Fatalf("BondsByAccount failed: %v", err)
	}

	if len(bonds) != 1 || bonds[0].ID != activeBond.ID {
		t.Fatalf("background pruning failed: expected 1 active bond, got %d", len(bonds))
	}

	cancel()
}

func setupTestDB(t *testing.T) (*DB, func()) {
	tmpDir, err := os.MkdirTemp("", "bond-db-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	logger := slog.NewBackend(os.Stderr).Logger("db")
	db, err := New(tmpDir, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create test database: %v", err)
	}

	teardown := func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close database: %v", err)
		}
		os.RemoveAll(tmpDir)
	}

	return db, teardown
}
