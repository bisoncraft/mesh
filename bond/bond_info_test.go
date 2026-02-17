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

func TestBondParamsMarshalUnmarshal(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name         string
		bond         *BondParams
		marshalData  []byte
		expectError  bool
		verifyFields func(*testing.T, *BondParams, *BondParams)
	}{
		{
			name: "standard bond params",
			bond: &BondParams{
				ID:       "test:bond:id",
				Strength: 42,
				Expiry:   now.Add(time.Hour * 24),
			},
			expectError: false,
			verifyFields: func(t *testing.T, original, unmarshalled *BondParams) {
				if unmarshalled.ID != original.ID {
					t.Errorf("ID mismatch: expected %s, got %s", original.ID, unmarshalled.ID)
				}
				if unmarshalled.Strength != original.Strength {
					t.Errorf("Strength mismatch: expected %d, got %d", original.Strength, unmarshalled.Strength)
				}
				if unmarshalled.Expiry.Unix() != original.Expiry.Unix() {
					t.Errorf("Expiry mismatch: expected %d, got %d", original.Expiry.Unix(), unmarshalled.Expiry.Unix())
				}
			},
		},
		{
			name: "empty ID",
			bond: &BondParams{
				ID:       "",
				Strength: 100,
				Expiry:   now.Add(time.Hour),
			},
			expectError: false,
			verifyFields: func(t *testing.T, original, unmarshalled *BondParams) {
				if unmarshalled.ID != original.ID {
					t.Errorf("ID mismatch: expected %q, got %q", original.ID, unmarshalled.ID)
				}
				if unmarshalled.Strength != original.Strength {
					t.Errorf("Strength mismatch: expected %d, got %d", original.Strength, unmarshalled.Strength)
				}
			},
		},
		{
			name: "large ID",
			bond: &BondParams{
				ID:       "this:is:a:very:long:bond:identifier:with:many:components:12345",
				Strength: 9999,
				Expiry:   now,
			},
			expectError: false,
			verifyFields: func(t *testing.T, original, unmarshalled *BondParams) {
				if unmarshalled.ID != original.ID {
					t.Errorf("ID mismatch: expected %s, got %s", original.ID, unmarshalled.ID)
				}
				if unmarshalled.Strength != original.Strength {
					t.Errorf("Strength mismatch: expected %d, got %d", original.Strength, unmarshalled.Strength)
				}
			},
		},
		{
			name:        "empty data",
			marshalData: []byte{},
			expectError: true,
		},
		{
			name:        "bad version",
			marshalData: []byte{1, 0, 0, 0, 0},
			expectError: true,
		},
		{
			name:        "truncated ID length",
			marshalData: []byte{0, 0, 0},
			expectError: true,
		},
		{
			name:        "truncated data",
			marshalData: []byte{0, 0, 0, 0, 5, 't', 'e', 's', 't'},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var data []byte
			var originalBond *BondParams

			// If bond is provided, marshal it
			if test.bond != nil {
				originalBond = test.bond
				var err error
				data, err = test.bond.MarshalBinary()
				if err != nil {
					t.Fatalf("Failed to marshal bond params: %v", err)
				}
				if len(data) == 0 {
					t.Fatalf("Expected non-empty marshalled data")
				}
			} else {
				// Use provided marshal data
				data = test.marshalData
			}

			// Unmarshal the data
			unmarshalled := &BondParams{}
			err := unmarshalled.UnmarshalBinary(data)

			if test.expectError {
				if err == nil {
					t.Errorf("Expected unmarshal error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Failed to unmarshal bond params: %v", err)
			}

			// Run verification checks if provided
			if test.verifyFields != nil {
				test.verifyFields(t, originalBond, unmarshalled)
			}
		})
	}
}
