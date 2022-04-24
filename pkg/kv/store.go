package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrUnknownDriver       = errors.New("unknown driver")
	ErrDriverConfiguration = errors.New("driver configuration")
	ErrOperationFailed     = errors.New("operation failed")
	ErrConnectFailed       = errors.New("connect failed")
	ErrSetupFailed         = errors.New("setup failed")
	ErrMissingKey          = errors.New("missing key")
	ErrMissingValue        = errors.New("missing value")
	ErrNotFound            = errors.New("not found")
	ErrClosedEntries       = errors.New("closed entries")
)

// Driver define the root interface to access the kv database.
// Each kv provider implements a driver which provides Store instance.
type Driver interface {
	// Open access to the database store. migrate kv if needed.
	Open(ctx context.Context, dsn string) (Store, error)
}

type Store interface {
	// Get returns a value for the given key, or ErrNotFound if key doesn't exist
	Get(ctx context.Context, key []byte) ([]byte, error)

	// Set stores the given value, overwriting an existing value if one exists
	Set(ctx context.Context, key, value []byte) error

	// SetIf returns an ErrNotFound error if the key with valuePredicate passed
	//  doesn't match the currently stored value. SetIf is a simple compare-and-swap operator:
	//  valuePredicate is either the existing value, or nil for no key.
	//  this is intentionally simplistic: we can model a better abstraction on top, keeping this interface simple for implementors
	SetIf(ctx context.Context, key, value, valuePredicate []byte) error

	// Delete will delete the key, or ErrNotFound if key doesn't exist
	Delete(ctx context.Context, key []byte) error

	// Scan returns entries that can be read by key order, starting at or after the `start` position
	Scan(ctx context.Context, start []byte) (Entries, error)

	// Close access to the database store
	Close()
}

// Entries used to enumerate over Scan results
type Entries interface {
	// Next should be called first before access Entry.
	// it will process the next entry and return true if it was successful, and false when none or error
	Next() bool

	// Entry current entry read after calling Next, set to nil in case of an error or no more entries
	Entry() *Entry

	// Err set to last error by reading or parse the next entry
	Err() error

	// Close should be called at the end of processing entries, required to release resources used to scan entries
	Close()
}

// Entry holds a pair of key/value
type Entry struct {
	Key   []byte
	Value []byte
}

// map drivers implementation
var (
	drivers   = make(map[string]Driver)
	driversMu sync.RWMutex
)

func Register(name string, driver Driver) {
	if name == "" {
		panic("kv store register name is missing")
	}
	if driver == nil {
		panic("kv store Register driver is nil")
	}
	driversMu.Lock()
	defer driversMu.Unlock()
	if _, found := drivers[name]; found {
		panic("kv store Register driver already registered " + name)
	}
	drivers[name] = driver
}

func Open(ctx context.Context, name string, dsn string) (Store, error) {
	driversMu.RLock()
	d, ok := drivers[name]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownDriver, name)
	}
	return d.Open(ctx, dsn)
}

func (e *Entry) String() string {
	if e == nil {
		return "Entry{nil}"
	}
	return fmt.Sprintf("Entry{%s, %s}", e.Key, e.Value)
}
