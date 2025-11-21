package bond

import (
	"testing"
	"time"
)

func TestBondInfo(t *testing.T) {
	mockTime := time.Now()
	bond1 := &BondParams{ID: "bond1", Strength: 20, Expiry: mockTime.Add(time.Hour)}
	bond2 := &BondParams{ID: "bond2", Strength: 20, Expiry: mockTime.Add(time.Hour * 2)}

	// Ensure a bond info can add bonds.
	bInfo := NewBondInfo()
	bInfo.AddBonds([]*BondParams{bond1}, mockTime)
	initialStrength := bInfo.BondStrength()

	// Ensure adding bonds updates the cummulative bond strength.
	bInfo.AddBonds([]*BondParams{bond2}, mockTime)
	updatedStrength := bInfo.BondStrength()

	if updatedStrength-initialStrength != bond2.Strength {
		t.Fatalf("expected updated bond strength of %d, got %d", bond1.Strength+bond2.Strength, updatedStrength)
	}
}
