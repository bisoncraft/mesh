package bond

import (
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
