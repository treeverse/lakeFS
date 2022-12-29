package kv_test

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

type MockDriver struct {
	Name string
	Err  error
}

type MockStore struct {
	Driver string
	Params kvparams.KV
}

var errNotImplemented = errors.New("not implemented")

func (m *MockStore) Get(_ context.Context, _, _ []byte) (*kv.ValueWithPredicate, error) {
	return nil, errNotImplemented
}

func (m *MockStore) Set(_ context.Context, _, _, _ []byte) error {
	return errNotImplemented
}

func (m *MockStore) SetIf(_ context.Context, _, _, _ []byte, _ kv.Predicate) error {
	return errNotImplemented
}

func (m *MockStore) Delete(_ context.Context, _, _ []byte) error {
	return errNotImplemented
}

func (m *MockStore) Scan(_ context.Context, _ []byte, _ kv.ScanOptions) (kv.EntriesIterator, error) {
	return nil, errNotImplemented
}

func (m *MockStore) Close() {}

func (m *MockDriver) Open(_ context.Context, params kvparams.KV) (kv.Store, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	return &MockStore{
		Driver: m.Name,
		Params: params,
	}, nil
}

func TestRegister(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("open", func(t *testing.T) {
		md := &MockDriver{Name: "md"}
		kv.Register("md", md)
		// open registered 'md'
		params := kvparams.KV{Type: "md", Postgres: &kvparams.Postgres{ConnectionString: "dsn1"}}
		s1, err := kv.Open(ctx, params)
		if err != nil {
			t.Fatal("expected Store 'md'", err)
		}
		store1, ok := s1.(*kv.StoreMetricsWrapper)
		if !ok {
			t.Fatal("expected StoreMetricsWrapper")
		}
		if store, ok := store1.Store.(*MockStore); !ok {
			t.Fatal("expected mock Store")
		} else if store.Driver != "md" {
			t.Fatal("expected Store from 'md' driver")
		} else if store.Params.Postgres.ConnectionString != params.Postgres.ConnectionString {
			t.Fatalf("Store open with dsn '%s', expected 'dsn1'", store.Params.Postgres.ConnectionString)
		}
		// open missing driver
		params2 := kvparams.KV{Type: "missing", Postgres: &kvparams.Postgres{ConnectionString: "dsn2"}}
		_, err = kv.Open(ctx, params2)
		if !errors.Is(err, kv.ErrUnknownDriver) {
			t.Fatalf("Open unknown driver err=%v, expected=%s", err, kv.ErrUnknownDriver)
		}
	})

	t.Run("no_driver", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected to panic on nil dirver")
			}
		}()
		kv.Register("nil", nil)
	})

	t.Run("no_name", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected to panic on no name")
			}
		}()
		kv.Register("", &MockDriver{Name: "empty"})
	})
}

func TestDrivers(t *testing.T) {
	kv.UnregisterAllDrivers()
	kv.Register("driver1", &MockDriver{Name: "driver1"})
	kv.Register("driver2", &MockDriver{Name: "driver2"})
	all := kv.Drivers()
	sort.Strings(all)
	expectedDrivers := []string{"driver1", "driver2"}
	if diff := deep.Equal(all, expectedDrivers); diff != nil {
		t.Fatalf("Drivers diff = %s", diff)
	}
}
