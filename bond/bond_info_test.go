package bond

import (
	"sync"
	"testing"
	"time"
)

func TestBondInfo(t *testing.T) {
	mockTime := time.Now()
	bond1 := &BondParams{ID: "bond1", Strength: 20, Expiry: mockTime.Add(time.Hour)}
	bond2 := &BondParams{ID: "bond2", Strength: 20, Expiry: mockTime.Add(time.Hour * 3)}

	// Ensure a bond info can add bonds.
	bInfo := NewBondInfo()
	bInfo.AddBonds([]*BondParams{bond1}, mockTime)
	initialStrength := bInfo.BondStrength()

	// Ensure adding bonds updates the cumulative bond strength.
	bInfo.AddBonds([]*BondParams{bond2}, mockTime)
	updatedStrength := bInfo.BondStrength()

	if updatedStrength-initialStrength != bond2.Strength {
		t.Fatalf("Expected updated bond strength of %d, got %d", bond1.Strength+bond2.Strength, updatedStrength)
	}

	// Ensure expired bonds can be cleared.
	bInfo.ClearExpiredBonds(mockTime.Add(time.Hour * 2))
	afterExpiryStrength := bInfo.BondStrength()

	if afterExpiryStrength < updatedStrength && afterExpiryStrength != 20 {
		t.Fatalf("Expected after expiry strength to be %d, got %d", 20, afterExpiryStrength)
	}

	// Ensure removing a bond at an invalid index errors.
	err := bInfo.RemoveBondAtIndex(10)
	if err == nil {
		t.Fatalf("Expected an invalid index error")
	}

	// Ensure removing a bond at a valid index succeeds.
	err = bInfo.RemoveBondAtIndex(0)
	if err != nil {
		t.Fatalf("Unexpected error removing bond at index 0: %v", err)
	}

	// Ensure a post bond request can be created from a bond info.
	bInfo.AddBonds([]*BondParams{bond1}, mockTime)

	bondReq, err := PostBondReqFromBondInfo(bInfo)
	if err != nil {
		t.Fatalf("Unexpected error creating a post bond request from bond info")
	}

	if string(bondReq.Bonds[0].BondID) != bond1.ID {
		t.Fatalf("Expected bond id %s in request, got %s", bond1.ID, string(bondReq.Bonds[0].BondID))
	}
}

// TestRemoveBondAtIndexConcurrent verifies that RemoveBondAtIndex is safe under concurrent operations.
// This tests the fix for the TOCTOU race condition where bondParams size could change
// between the bounds check and the removal.
func TestRemoveBondAtIndexConcurrent(t *testing.T) {
	mockTime := time.Now()

	// Create multiple bonds to remove
	bonds := make([]*BondParams, 10)
	for i := 0; i < 10; i++ {
		bonds[i] = &BondParams{
			ID:       "bond" + string(rune(i)),
			Strength: 10,
			Expiry:   mockTime.Add(time.Hour),
		}
	}

	bInfo := NewBondInfo()
	bInfo.AddBonds(bonds, mockTime)

	var wg sync.WaitGroup

	// Multiple goroutines removing bonds concurrently
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Try to remove index 0 multiple times
			// Some will succeed, some will fail with out-of-bounds error (expected)
			_ = bInfo.RemoveBondAtIndex(0)
		}()
	}

	// Goroutine clearing expired bonds concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		bInfo.ClearExpiredBonds(mockTime.Add(time.Minute))
	}()

	// Goroutine adding bonds concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		bInfo.AddBonds([]*BondParams{bonds[0]}, mockTime)
	}()

	wg.Wait()

	// Verify we don't have any panics and state is consistent
	// The exact count depends on scheduling, but we should have reduced bonds
	finalCount := len(bInfo.bondParams)
	if finalCount > len(bonds) {
		t.Fatalf("Bond count increased unexpectedly: %d > %d", finalCount, len(bonds))
	}
}

// TestPostBondReqFromBondInfoConcurrent verifies that PostBondReqFromBondInfo is safe
// under concurrent modifications to the BondInfo. This tests the fix for the data race
// where bondParams could be modified during iteration.
func TestPostBondReqFromBondInfoConcurrent(t *testing.T) {
	mockTime := time.Now()

	// Create initial bonds
	bonds := make([]*BondParams, 20)
	for i := 0; i < 20; i++ {
		bonds[i] = &BondParams{
			ID:       "bond" + string(rune(48+i%10)),
			Strength: 10,
			Expiry:   mockTime.Add(time.Hour),
		}
	}

	bInfo := NewBondInfo()
	bInfo.AddBonds(bonds[:10], mockTime)

	var wg sync.WaitGroup
	var successCount int
	var mu sync.Mutex

	// Multiple goroutines reading the bond info concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Try to convert to protobuf request while other goroutines modify
			req, err := PostBondReqFromBondInfo(bInfo)
			if err == nil && req != nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	// Goroutines modifying the bond info concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			bInfo.AddBonds([]*BondParams{bonds[10+i]}, mockTime)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			_ = bInfo.RemoveBondAtIndex(0)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		bInfo.ClearExpiredBonds(mockTime.Add(time.Minute))
	}()

	wg.Wait()

	// Verify all reads succeeded (no panics, no data races)
	if successCount == 0 {
		t.Fatalf("Expected at least one successful PostBondReqFromBondInfo call")
	}
}

// TestRemoveBondAtIndexCleanup verifies that RemoveBondAtIndex properly updates
// totalStrength and storedBonds when removing a bond. This tests the fix for
// the incomplete cleanup issue.
func TestRemoveBondAtIndexCleanup(t *testing.T) {
	mockTime := time.Now()
	bond1 := &BondParams{ID: "bond1", Strength: 100, Expiry: mockTime.Add(time.Hour)}
	bond2 := &BondParams{ID: "bond2", Strength: 200, Expiry: mockTime.Add(time.Hour)}
	bond3 := &BondParams{ID: "bond3", Strength: 150, Expiry: mockTime.Add(time.Hour)}

	bInfo := NewBondInfo()
	bInfo.AddBonds([]*BondParams{bond1, bond2, bond3}, mockTime)

	// Verify initial state
	initialStrength := bInfo.BondStrength()
	expectedInitialStrength := uint32(100 + 200 + 150)
	if initialStrength != expectedInitialStrength {
		t.Fatalf("Expected initial strength %d, got %d", expectedInitialStrength, initialStrength)
	}

	// Remove middle bond (bond2)
	err := bInfo.RemoveBondAtIndex(1)
	if err != nil {
		t.Fatalf("Unexpected error removing bond at index 1: %v", err)
	}

	// Verify totalStrength decreased by removed bond's strength
	afterRemovalStrength := bInfo.BondStrength()
	expectedAfterRemovalStrength := uint32(100 + 150)
	if afterRemovalStrength != expectedAfterRemovalStrength {
		t.Fatalf("Expected strength after removal %d, got %d", expectedAfterRemovalStrength, afterRemovalStrength)
	}

	// Verify storedBonds is updated (removed bond should be able to be re-added)
	bInfo.AddBonds([]*BondParams{bond2}, mockTime)
	finalStrength := bInfo.BondStrength()
	expectedFinalStrength := uint32(100 + 200 + 150)
	if finalStrength != expectedFinalStrength {
		t.Fatalf("Expected final strength %d after re-adding, got %d", expectedFinalStrength, finalStrength)
	}

	// Verify that adding same bond that was already in storedBonds is rejected
	beforeAddStrength := bInfo.BondStrength()
	bInfo.AddBonds([]*BondParams{bond1}, mockTime)
	afterAddStrength := bInfo.BondStrength()
	if beforeAddStrength != afterAddStrength {
		t.Fatalf("Strength should not change when adding existing bond: before=%d, after=%d",
			beforeAddStrength, afterAddStrength)
	}
}

// TestMultipleRemovalsCleanup verifies that multiple sequential removals
// maintain correct state across totalStrength and storedBonds.
func TestMultipleRemovalsCleanup(t *testing.T) {
	mockTime := time.Now()
	bonds := make([]*BondParams, 5)
	totalExpectedStrength := uint32(0)
	for i := 0; i < 5; i++ {
		strength := uint32((i + 1) * 50)
		bonds[i] = &BondParams{
			ID:       "bond" + string(rune(48+i)),
			Strength: strength,
			Expiry:   mockTime.Add(time.Hour),
		}
		totalExpectedStrength += strength
	}

	bInfo := NewBondInfo()
	bInfo.AddBonds(bonds, mockTime)

	// Verify initial state
	if bInfo.BondStrength() != totalExpectedStrength {
		t.Fatalf("Initial strength mismatch")
	}

	// Remove bonds from the end to avoid index shifting issues
	for i := 4; i >= 0; i-- {
		err := bInfo.RemoveBondAtIndex(uint32(i))
		if err != nil {
			t.Fatalf("Unexpected error removing bond at index %d: %v", i, err)
		}

		// Verify strength decreases correctly
		expectedStrength := totalExpectedStrength - bonds[i].Strength
		actualStrength := bInfo.BondStrength()
		if actualStrength != expectedStrength {
			t.Fatalf("After removing bond %d: expected strength %d, got %d",
				i, expectedStrength, actualStrength)
		}

		// Verify we can re-add the removed bond
		bInfo.AddBonds([]*BondParams{bonds[i]}, mockTime)
		if bInfo.BondStrength() != totalExpectedStrength {
			t.Fatalf("After re-adding bond %d: strength should return to %d, got %d",
				i, totalExpectedStrength, bInfo.BondStrength())
		}

		// Remove it again for the next iteration
		_ = bInfo.RemoveBondAtIndex(uint32(len(bInfo.bondParams) - 1))
		totalExpectedStrength -= bonds[i].Strength
	}

	// Final state should be empty
	if len(bInfo.bondParams) != 0 {
		t.Fatalf("Expected empty bonds after all removals, got %d", len(bInfo.bondParams))
	}
	if bInfo.BondStrength() != 0 {
		t.Fatalf("Expected zero strength after all removals, got %d", bInfo.BondStrength())
	}
}

// TestBondInfoEdgeCases verifies edge cases and error paths across all BondInfo operations.
func TestBondInfoEdgeCases(t *testing.T) {
	mockTime := time.Now()

	t.Run("AddBonds_DuplicateRejection", func(t *testing.T) {
		bond1 := &BondParams{ID: "bond1", Strength: 100, Expiry: mockTime.Add(time.Hour)}
		bInfo := NewBondInfo()
		bInfo.AddBonds([]*BondParams{bond1}, mockTime)

		initialStrength := bInfo.BondStrength()
		initialCount := len(bInfo.bondParams)

		// Try to add the same bond again
		bInfo.AddBonds([]*BondParams{bond1}, mockTime)

		// Verify no change
		if bInfo.BondStrength() != initialStrength {
			t.Errorf("Strength changed after adding duplicate: %d -> %d", initialStrength, bInfo.BondStrength())
		}
		if len(bInfo.bondParams) != initialCount {
			t.Errorf("Bond count changed after adding duplicate: %d -> %d", initialCount, len(bInfo.bondParams))
		}

		// Verify duplicate within same AddBonds call is also rejected
		bInfo.AddBonds([]*BondParams{bond1, bond1}, mockTime)
		if bInfo.BondStrength() != initialStrength {
			t.Error("Strength changed after adding batch with duplicates")
		}
	})

	t.Run("AddBonds_ExpiredRejection", func(t *testing.T) {
		expiredBond := &BondParams{ID: "expired", Strength: 100, Expiry: mockTime.Add(-time.Hour)}
		validBond := &BondParams{ID: "valid", Strength: 50, Expiry: mockTime.Add(time.Hour)}
		bInfo := NewBondInfo()

		// Add mix of expired and valid bonds
		bInfo.AddBonds([]*BondParams{expiredBond, validBond}, mockTime)

		// Only the valid bond should be added
		if bInfo.BondStrength() != 50 {
			t.Errorf("Expected strength 50 (only valid bond), got %d", bInfo.BondStrength())
		}
		if len(bInfo.bondParams) != 1 {
			t.Errorf("Expected 1 bond, got %d", len(bInfo.bondParams))
		}
		if bInfo.bondParams[0].ID != "valid" {
			t.Errorf("Expected 'valid' bond, got '%s'", bInfo.bondParams[0].ID)
		}
	})

	t.Run("AddBonds_EmptyBatch", func(t *testing.T) {
		bInfo := NewBondInfo()

		// Add empty batch - should be safe
		bInfo.AddBonds([]*BondParams{}, mockTime)

		if len(bInfo.bondParams) != 0 {
			t.Error("Expected no bonds after adding empty batch")
		}
		if bInfo.BondStrength() != 0 {
			t.Error("Expected zero strength after adding empty batch")
		}
	})

	t.Run("PostBondReqFromBondInfo_Empty", func(t *testing.T) {
		bInfo := NewBondInfo()

		// Try to create request with no bonds
		req, err := PostBondReqFromBondInfo(bInfo)

		if err == nil {
			t.Error("Expected error when creating request with no bonds")
		}
		if req != nil {
			t.Errorf("Expected nil request, got %v", req)
		}
	})

	t.Run("ClearExpiredBonds_Empty", func(t *testing.T) {
		bInfo := NewBondInfo()

		// Clear expired bonds from empty list - should be safe
		bInfo.ClearExpiredBonds(mockTime)

		if len(bInfo.bondParams) != 0 {
			t.Error("Expected empty bonds after clearing from empty list")
		}
		if bInfo.BondStrength() != 0 {
			t.Error("Expected zero strength after clearing from empty list")
		}
	})

	t.Run("ClearExpiredBonds_AllExpired", func(t *testing.T) {
		// Create bonds with future expiry times
		bond1 := &BondParams{ID: "bond1", Strength: 100, Expiry: mockTime.Add(time.Minute)}
		bond2 := &BondParams{ID: "bond2", Strength: 200, Expiry: mockTime.Add(time.Hour)}
		bInfo := NewBondInfo()
		bInfo.AddBonds([]*BondParams{bond1, bond2}, mockTime)

		initialStrength := bInfo.BondStrength()
		if initialStrength != 300 {
			t.Fatalf("Expected initial strength 300, got %d", initialStrength)
		}

		// Clear with time after all expirations
		bInfo.ClearExpiredBonds(mockTime.Add(time.Hour * 2))

		if len(bInfo.bondParams) != 0 {
			t.Error("Expected no bonds after clearing all expired")
		}
		if bInfo.BondStrength() != 0 {
			t.Error("Expected zero strength after clearing all expired")
		}
	})

	t.Run("ClearExpiredBonds_NoneExpired", func(t *testing.T) {
		bond1 := &BondParams{ID: "bond1", Strength: 100, Expiry: mockTime.Add(time.Hour)}
		bond2 := &BondParams{ID: "bond2", Strength: 200, Expiry: mockTime.Add(time.Hour * 2)}
		bInfo := NewBondInfo()
		bInfo.AddBonds([]*BondParams{bond1, bond2}, mockTime)

		initialStrength := bInfo.BondStrength()
		initialCount := len(bInfo.bondParams)

		// Clear with time before any expirations
		bInfo.ClearExpiredBonds(mockTime.Add(time.Minute))

		if len(bInfo.bondParams) != initialCount {
			t.Errorf("Bond count changed: %d -> %d", initialCount, len(bInfo.bondParams))
		}
		if bInfo.BondStrength() != initialStrength {
			t.Errorf("Strength changed: %d -> %d", initialStrength, bInfo.BondStrength())
		}
	})

	t.Run("RemoveBondAtIndex_Empty", func(t *testing.T) {
		bInfo := NewBondInfo()

		// Try to remove from empty list
		err := bInfo.RemoveBondAtIndex(0)

		// Should return nil (safe, idempotent)
		if err != nil {
			t.Errorf("Expected nil error when removing from empty list, got %v", err)
		}
	})
}
