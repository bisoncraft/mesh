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
		ID:       "bip122:000000000019d6689c085ae165831e93/slip44:0:abc123:0",
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
		{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:tx1:0", Strength: 50, Expiry: time.Now().Add(time.Hour)},
		{ID: "bip122:298e5cc3d985bcc8d3d8ec0a6c0d5755eb8d8374eb5aa635d37c2ab26370498a/slip44:42:tx2:0", Strength: 75, Expiry: time.Now().Add(2 * time.Hour)},
		{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:tx3:1", Strength: 100, Expiry: time.Now().Add(3 * time.Hour)},
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
		{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:tx4:0", Strength: 200, Expiry: time.Now().Add(time.Hour)},
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

	t.Run("empty account", func(t *testing.T) {
		bonds, err := db.BondsByAccount("never-stored")
		if err != nil {
			t.Fatalf("BondsByAccount failed: %v", err)
		}

		if bonds != nil || len(bonds) != 0 {
			t.Fatalf("expected nil/empty slice for non-existent account, got %+v", bonds)
		}
	})
}

func testBondStrength(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	accountID := "strength-test"
	now := time.Now()
	bonds := []*bond.BondParams{
		{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:tx1:0", Strength: 100, Expiry: now.Add(time.Hour)},
		{ID: "bip122:298e5cc3d985bcc8d3d8ec0a6c0d5755eb8d8374eb5aa635d37c2ab26370498a/slip44:42:tx2:0", Strength: 200, Expiry: now.Add(time.Hour)},
		{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:tx3:0", Strength: 150, Expiry: now.Add(time.Hour)},
	}

	for _, bp := range bonds {
		if err := db.StoreBond(accountID, bp); err != nil {
			t.Fatalf("StoreBond failed: %v", err)
		}
	}

	strength, err := db.BondStrength(accountID, now)
	if err != nil {
		t.Fatalf("BondStrength failed: %v", err)
	}

	expected := uint32(450)
	if strength != expected {
		t.Fatalf("expected strength %d, got %d", expected, strength)
	}

	// Test that expired bonds are not counted
	t.Run("expired bonds excluded", func(t *testing.T) {
		strength, err := db.BondStrength(accountID, now.Add(2*time.Hour))
		if err != nil {
			t.Fatalf("BondStrength failed: %v", err)
		}

		if strength != 0 {
			t.Fatalf("expected strength 0 for all expired bonds, got %d", strength)
		}
	})

	t.Run("mixed expiry", func(t *testing.T) {
		accountID := "mixed-expiry"
		now := time.Now()
		bonds := []*bond.BondParams{
			{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:bond1:0", Strength: 100, Expiry: now.Add(time.Hour)},
			{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:bond2:0", Strength: 200, Expiry: now.Add(2 * time.Hour)},
			{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:bond3:0", Strength: 150, Expiry: now.Add(3 * time.Hour)},
		}

		for _, bp := range bonds {
			if err := db.StoreBond(accountID, bp); err != nil {
				t.Fatalf("StoreBond failed: %v", err)
			}
		}

		strength, err := db.BondStrength(accountID, now.Add(time.Hour + 30*time.Minute))
		if err != nil {
			t.Fatalf("BondStrength failed: %v", err)
		}

		expected := uint32(350)
		if strength != expected {
			t.Fatalf("expected strength %d for non-expired bonds, got %d", expected, strength)
		}
	})
}

func testPruneExpiredBonds(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	accountID := "prune-test"
	now := time.Now()

	// Store bonds with different expiry times
	expiredBond := &bond.BondParams{
		ID:       "bip122:000000000019d6689c085ae165831e93/slip44:0:expired:0",
		Strength: 50,
		Expiry:   now.Add(-time.Hour), // Already expired
	}

	activeBond := &bond.BondParams{
		ID:       "bip122:000000000019d6689c085ae165831e93/slip44:0:active:0",
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

	strength, err := db.BondStrength(accountID, now)
	if err != nil {
		t.Fatalf("BondStrength failed: %v", err)
	}

	if strength != activeBond.Strength {
		t.Fatalf("expected strength %d after prune, got %d", activeBond.Strength, strength)
	}

	t.Run("prune none (all active)", func(t *testing.T) {
		db, teardown := setupTestDB(t)
		defer teardown()

		now := time.Now()
		bonds := []*bond.BondParams{
			{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:active1:0", Strength: 100, Expiry: now.Add(time.Hour)},
			{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:active2:0", Strength: 200, Expiry: now.Add(2 * time.Hour)},
		}

		accountID := "prune-none"
		for _, bp := range bonds {
			if err := db.StoreBond(accountID, bp); err != nil {
				t.Fatalf("StoreBond failed: %v", err)
			}
		}

		if err := db.PruneExpiredBonds(now); err != nil {
			t.Fatalf("PruneExpiredBonds failed: %v", err)
		}

		retrieved, err := db.BondsByAccount(accountID)
		if err != nil {
			t.Fatalf("BondsByAccount failed: %v", err)
		}

		if len(retrieved) != 2 {
			t.Fatalf("expected 2 bonds after prune, got %d", len(retrieved))
		}

		strength, err := db.BondStrength(accountID, now)
		if err != nil {
			t.Fatalf("BondStrength failed: %v", err)
		}

		expected := uint32(300)
		if strength != expected {
			t.Fatalf("expected strength %d, got %d", expected, strength)
		}
	})

	t.Run("prune all (all expired)", func(t *testing.T) {
		db, teardown := setupTestDB(t)
		defer teardown()

		now := time.Now()
		bonds := []*bond.BondParams{
			{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:exp1:0", Strength: 50, Expiry: now.Add(-2 * time.Hour)},
			{ID: "bip122:000000000019d6689c085ae165831e93/slip44:0:exp2:0", Strength: 75, Expiry: now.Add(-time.Hour)},
		}

		accountID := "prune-all"
		for _, bp := range bonds {
			if err := db.StoreBond(accountID, bp); err != nil {
				t.Fatalf("StoreBond failed: %v", err)
			}
		}

		if err := db.PruneExpiredBonds(now); err != nil {
			t.Fatalf("PruneExpiredBonds failed: %v", err)
		}

		retrieved, err := db.BondsByAccount(accountID)
		if err != nil {
			t.Fatalf("BondsByAccount failed: %v", err)
		}

		if len(retrieved) != 0 {
			t.Fatalf("expected 0 bonds after prune, got %d", len(retrieved))
		}

		strength, err := db.BondStrength(accountID, now)
		if err != nil {
			t.Fatalf("BondStrength failed: %v", err)
		}

		if strength != 0 {
			t.Fatalf("expected strength 0, got %d", strength)
		}
	})

	t.Run("multiple accounts", func(t *testing.T) {
		db, teardown := setupTestDB(t)
		defer teardown()

		now := time.Now()

		account1ExpiredBond := &bond.BondParams{
			ID:       "bip122:000000000019d6689c085ae165831e93/slip44:0:acc1-exp:0",
			Strength: 50,
			Expiry:   now.Add(-time.Hour),
		}
		account1ActiveBond := &bond.BondParams{
			ID:       "bip122:000000000019d6689c085ae165831e93/slip44:0:acc1-active:0",
			Strength: 100,
			Expiry:   now.Add(time.Hour),
		}
		account2ActiveBond := &bond.BondParams{
			ID:       "bip122:000000000019d6689c085ae165831e93/slip44:0:acc2-active:0",
			Strength: 150,
			Expiry:   now.Add(time.Hour),
		}

		if err := db.StoreBond("account-1", account1ExpiredBond); err != nil {
			t.Fatalf("StoreBond failed: %v", err)
		}
		if err := db.StoreBond("account-1", account1ActiveBond); err != nil {
			t.Fatalf("StoreBond failed: %v", err)
		}
		if err := db.StoreBond("account-2", account2ActiveBond); err != nil {
			t.Fatalf("StoreBond failed: %v", err)
		}

		if err := db.PruneExpiredBonds(now); err != nil {
			t.Fatalf("PruneExpiredBonds failed: %v", err)
		}

		bonds1, err := db.BondsByAccount("account-1")
		if err != nil {
			t.Fatalf("BondsByAccount failed: %v", err)
		}

		if len(bonds1) != 1 || bonds1[0].ID != account1ActiveBond.ID {
			t.Fatalf("account-1: expected 1 active bond, got %+v", bonds1)
		}

		bonds2, err := db.BondsByAccount("account-2")
		if err != nil {
			t.Fatalf("BondsByAccount failed: %v", err)
		}

		if len(bonds2) != 1 || bonds2[0].ID != account2ActiveBond.ID {
			t.Fatalf("account-2: expected 1 active bond, got %+v", bonds2)
		}

		strength1, err := db.BondStrength("account-1", now)
		if err != nil {
			t.Fatalf("BondStrength failed: %v", err)
		}

		if strength1 != account1ActiveBond.Strength {
			t.Fatalf("account-1: expected strength %d, got %d", account1ActiveBond.Strength, strength1)
		}

		strength2, err := db.BondStrength("account-2", now)
		if err != nil {
			t.Fatalf("BondStrength failed: %v", err)
		}

		if strength2 != account2ActiveBond.Strength {
			t.Fatalf("account-2: expected strength %d, got %d", account2ActiveBond.Strength, strength2)
		}
	})
}

func testBackgroundPruning(t *testing.T) {
	db, teardown := setupTestDB(t)
	defer teardown()

	accountID := "background-prune"
	now := time.Now()

	expiredBond := &bond.BondParams{
		ID:       "bip122:000000000019d6689c085ae165831e93/slip44:0:expired:0",
		Strength: 50,
		Expiry:   now.Add(-time.Hour),
	}

	activeBond := &bond.BondParams{
		ID:       "bip122:000000000019d6689c085ae165831e93/slip44:0:active:0",
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

	strength, err := db.BondStrength(accountID, now)
	if err != nil {
		t.Fatalf("BondStrength failed: %v", err)
	}

	if strength != activeBond.Strength {
		t.Fatalf("expected strength %d after prune, got %d", activeBond.Strength, strength)
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
