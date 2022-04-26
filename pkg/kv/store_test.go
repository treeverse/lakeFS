package kv_test

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/pkg/kv"
)

type MockDriver struct {
	Name string
	Err  error
}

type MockStore struct {
	Driver string
	DSN    string
}

var errNotImplemented = errors.New("not implemented")

func (m *MockStore) Get(_ context.Context, _ []byte) ([]byte, error) {
	return nil, errNotImplemented
}

func (m *MockStore) Set(_ context.Context, _, _ []byte) error {
	return errNotImplemented
}

func (m *MockStore) SetIf(_ context.Context, _, _, _ []byte) error {
	return errNotImplemented
}

func (m *MockStore) Delete(_ context.Context, _ []byte) error {
	return errNotImplemented
}

func (m *MockStore) Scan(_ context.Context, _ []byte) (kv.Entries, error) {
	return nil, errNotImplemented
}

func (m *MockStore) Close() {}

func (m *MockDriver) Open(_ context.Context, dsn string) (kv.Store, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	return &MockStore{
		Driver: m.Name,
		DSN:    dsn,
	}, nil
}

func TestRegister(t *testing.T) {
	ctx := context.Background()

	t.Run("open", func(t *testing.T) {
		md := &MockDriver{Name: "md"}
		kv.Register("md", md)
		// open registered 'md'
		s1, err := kv.Open(ctx, "md", "dsn1")
		if err != nil {
			t.Fatal("expected store 'md'", err)
		}
		if store, ok := s1.(*MockStore); !ok {
			t.Fatal("expected mock store")
		} else if store.Driver != "md" {
			t.Fatal("expected store from 'md' driver")
		} else if store.DSN != "dsn1" {
			t.Fatalf("store open with dsn '%s', expected 'dsn1'", store.DSN)
		}
		// open missing driver
		_, err = kv.Open(ctx, "missing", "dsn2")
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
