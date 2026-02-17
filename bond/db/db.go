package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bisoncraft/mesh/bond"
	"github.com/decred/slog"
	"github.com/dgraph-io/badger/v4"
)

// DB is a persistent bond storage layer using Badger.
type DB struct {
	db  *badger.DB
	log slog.Logger
}

// New initializes a new bond database at the specified directory.
func New(dir string, log slog.Logger) (*DB, error) {
	opts := badger.DefaultOptions(dir)
	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger database: %w", err)
	}

	return &DB{
		db:  badgerDB,
		log: log,
	}, nil
}

func bondKey(id string) []byte {
	return append([]byte("bond:"), []byte(id)...)
}

func accountIndexKey(accountID, bondID string) []byte {
	prefix := "byAccount:" + accountID + ":"
	return append([]byte(prefix), []byte(bondID)...)
}

func expiryIndexKey(expiry time.Time, accountID, bondID string) []byte {
	// Prefix has a zero-padded timestamp for sorting purposes.
	prefix := fmt.Sprintf("byExpiry:%020d:%s:", expiry.Unix(), accountID)
	key := []byte(prefix)
	return append(key, []byte(bondID)...)
}

func scanIndexPrefix(txn *badger.Txn, prefix []byte, fn func(key []byte) error) error {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.Valid(); it.Next() {
		key := it.Item().Key()
		if err := fn(key); err != nil {
			return err
		}
	}
	return nil
}

func setBondWithIndices(txn *badger.Txn, accountID string, bp *bond.BondParams) error {
	data, err := bp.MarshalBinary()
	if err != nil {
		return fmt.Errorf("failed to marshal bond: %w", err)
	}

	if err := txn.Set(bondKey(bp.ID), data); err != nil {
		return fmt.Errorf("failed to store bond: %w", err)
	}

	accountKey := accountIndexKey(accountID, bp.ID)
	if err := txn.Set(accountKey, []byte{}); err != nil {
		return fmt.Errorf("failed to add account index: %w", err)
	}

	expiryKey := expiryIndexKey(bp.Expiry, accountID, bp.ID)
	if err := txn.Set(expiryKey, []byte{}); err != nil {
		return fmt.Errorf("failed to add expiry index: %w", err)
	}

	return nil
}

func deleteBondWithIndices(txn *badger.Txn, accountID string, bp *bond.BondParams) error {
	if err := txn.Delete(bondKey(bp.ID)); err != nil {
		return fmt.Errorf("failed to delete bond: %w", err)
	}

	if err := txn.Delete(accountIndexKey(accountID, bp.ID)); err != nil {
		return fmt.Errorf("failed to delete account index: %w", err)
	}

	if err := txn.Delete(expiryIndexKey(bp.Expiry, accountID, bp.ID)); err != nil {
		return fmt.Errorf("failed to delete expiry index: %w", err)
	}

	return nil
}

// StoreBond inserts or updates a bond with index maintenance.
func (d *DB) StoreBond(accountID string, bp *bond.BondParams) error {
	return d.db.Update(func(txn *badger.Txn) error {
		return setBondWithIndices(txn, accountID, bp)
	})
}

// BondsByAccount retrieves all bonds for a given account ID.
func (d *DB) BondsByAccount(accountID string) ([]*bond.BondParams, error) {
	var bonds []*bond.BondParams

	err := d.db.View(func(txn *badger.Txn) error {
		prefix := []byte("byAccount:" + accountID + ":")

		return scanIndexPrefix(txn, prefix, func(key []byte) error {
			prefixLen := len(prefix)
			bondID := string(key[prefixLen:])

			item, err := txn.Get(bondKey(bondID))
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return nil
				}
				return err
			}

			var bp bond.BondParams
			err = item.Value(func(val []byte) error {
				return bp.UnmarshalBinary(val)
			})
			if err != nil {
				return err
			}

			bonds = append(bonds, &bp)
			return nil
		})
	})

	return bonds, err
}

// BondStrength computes the total strength for an account by summing all non-expired bonds.
func (d *DB) BondStrength(accountID string) (uint32, error) {
	bonds, err := d.BondsByAccount(accountID)
	if err != nil {
		return 0, err
	}

	var totalStrength uint32
	for _, b := range bonds {
		totalStrength += b.Strength
	}
	return totalStrength, nil
}

// PruneExpiredBonds deletes all expired bonds as of the given time.
func (d *DB) PruneExpiredBonds(now time.Time) error {
	return d.db.Update(func(txn *badger.Txn) error {
		prefix := []byte("byExpiry:")
		nowThreshold := fmt.Sprintf("byExpiry:%020d:", now.Unix())

		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		var bondsToDelete []*struct {
			accountID string
			bp        *bond.BondParams
		}

		for it.Seek(prefix); it.Valid(); it.Next() {
			key := string(it.Item().Key())
			if key >= nowThreshold {
				break
			}

			parts := strings.SplitN(key, ":", 4)
			if len(parts) < 4 {
				continue
			}
			accountID := parts[2]
			bondID := parts[3]

			item, err := txn.Get(bondKey(bondID))
			if err != nil {
				if err == badger.ErrKeyNotFound {
					continue
				}
				return err
			}

			var bp bond.BondParams
			err = item.Value(func(val []byte) error {
				return bp.UnmarshalBinary(val)
			})
			if err != nil {
				continue
			}

			bondsToDelete = append(bondsToDelete, &struct {
				accountID string
				bp        *bond.BondParams
			}{accountID, &bp})
		}

		for _, item := range bondsToDelete {
			if err := deleteBondWithIndices(txn, item.accountID, item.bp); err != nil {
				return err
			}
		}

		return nil
	})
}

// Run starts a process that prunes expired bonds periodically.
func (d *DB) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := d.PruneExpiredBonds(time.Now()); err != nil {
				d.log.Errorf("error pruning expired bonds: %v", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close gracefully flushes and closes the Badger database.
func (d *DB) Close() error {
	return d.db.Close()
}
