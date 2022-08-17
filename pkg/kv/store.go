package kv

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

const (
	InitialMigrateVersion = 1
	PathDelimiter         = "/"
	MetadataPartitionKey  = "kv-internal-metadata"
)

var (
	ErrClosedEntries        = errors.New("closed entries")
	ErrConnectFailed        = errors.New("connect failed")
	ErrDriverConfiguration  = errors.New("driver configuration")
	ErrMissingPartitionKey  = errors.New("missing partition key")
	ErrMissingKey           = errors.New("missing key")
	ErrMissingValue         = errors.New("missing value")
	ErrNotFound             = errors.New("not found")
	ErrOperationFailed      = errors.New("operation failed")
	ErrPredicateFailed      = errors.New("predicate failed")
	ErrSetupFailed          = errors.New("setup failed")
	ErrUnknownDriver        = errors.New("unknown driver")
	ErrTableNotActive       = errors.New("table not active")
	ErrInvalidSchemaVersion = errors.New("invalid database schema version")
)

func FormatPath(p ...string) string {
	return strings.Join(p, PathDelimiter)
}

// Driver is the interface to access a kv database as a Store.
// Each kv provider implements a Driver.
type Driver interface {
	// Open opens access to the database store. Implementations give access to the same storage based on the dsn.
	// Implementation can return the same Storage instance based on dsn or new one as long as it provides access to
	// the same storage.
	Open(ctx context.Context, params kvparams.KV) (Store, error)
}

// Predicate value used to update a key base on a previous fetched value.
//   Store's Get used to pull the key's value with the associated predicate.
//   Store's SetIf used to set the key's value based on the predicate.
type Predicate interface{}

// ValueWithPredicate value with predicate - Value holds the data and Predicate a value used for conditional set.
//   Get operation will return this struct, holding the key's information
//   SetIf operation will use the Predicate for conditional set
type ValueWithPredicate struct {
	Value     []byte
	Predicate Predicate
}

type Store interface {
	// Get returns a result containing the Value and Predicate for the given key, or ErrNotFound if key doesn't exist
	//   Predicate can be used for SetIf operation
	Get(ctx context.Context, partitionKey, key []byte) (*ValueWithPredicate, error)

	// Set stores the given value, overwriting an existing value if one exists
	Set(ctx context.Context, partitionKey, key, value []byte) error

	// SetIf returns an ErrPredicateFailed error if the key with valuePredicate passed
	//  doesn't match the currently stored value. SetIf is a simple compare-and-swap operator:
	//  valuePredicate is either the existing value, or nil for no previous key exists.
	//  this is intentionally simplistic: we can model a better abstraction on top, keeping this interface simple for implementors
	SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate Predicate) error

	// Delete will delete the key, no error in if key doesn't exist
	Delete(ctx context.Context, partitionKey, key []byte) error

	// Scan returns entries that can be read by key order, starting at or after the `start` position
	// partitionKey is optional, passing it might increase performance.
	Scan(ctx context.Context, partitionKey, start []byte) (EntriesIterator, error)

	// Close access to the database store. After calling Close the instance is unusable.
	Close()
}

// EntriesIterator used to enumerate over Scan results
type EntriesIterator interface {
	// Next should be called first before access Entry.
	// it will process the next entry and return true if it was successful, and false when none or error.
	Next() bool

	// Entry current entry read after calling Next, set to nil in case of an error or no more entries.
	Entry() *Entry

	// Err set to last error by reading or parse the next entry.
	Err() error

	// Close should be called at the end of processing entries, required to release resources used to scan entries.
	// After calling 'Close' the instance should not be used as the behaviour will not be defined.
	Close()
}

// Entry holds a pair of key/value
type Entry struct {
	PartitionKey []byte
	Key          []byte
	Value        []byte
}

func (e *Entry) String() string {
	if e == nil {
		return "Entry{nil}"
	}
	return fmt.Sprintf("Entry{%v, %v}", e.Key, e.Value)
}

// map drivers implementation
var (
	drivers   = make(map[string]Driver)
	driversMu sync.RWMutex
)

// Register 'driver' implementation under 'name'. Panic in case of empty name, nil driver or name already registered.
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

// UnregisterAllDrivers remove all loaded drivers, used for test code.
func UnregisterAllDrivers() {
	driversMu.Lock()
	defer driversMu.Unlock()
	for k := range drivers {
		delete(drivers, k)
	}
}

// Open lookup driver with 'name' and return Store based on 'dsn' (data source name).
// Failed with ErrUnknownDriver in case 'name' is not registered
func Open(ctx context.Context, params kvparams.KV) (Store, error) {
	driversMu.RLock()
	d, ok := drivers[params.Type]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownDriver, params.Type)
	}
	return d.Open(ctx, params)
}

// Drivers returns a list of registered drive names
func Drivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	names := make([]string, 0, len(drivers))
	for name := range drivers {
		names = append(names, name)
	}
	return names
}

// GetDBSchemaVersion returns the current KV DB schema version
func GetDBSchemaVersion(ctx context.Context, store Store) (int, error) {
	res, err := store.Get(ctx, []byte(MetadataPartitionKey), dbSchemaPath())
	if err != nil {
		return -1, err
	}
	version, err := strconv.Atoi(string(res.Value))
	if err != nil {
		return -1, err
	}
	return version, nil
}

// SetDBSchemaVersion sets KV DB schema version
func SetDBSchemaVersion(ctx context.Context, store Store, version uint) error {
	return store.Set(ctx, []byte(MetadataPartitionKey), dbSchemaPath(), []byte(fmt.Sprintf("%d", version)))
}

func dbSchemaPath() []byte {
	return []byte(FormatPath("kv", "schema", "version"))
}
