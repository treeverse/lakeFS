package kvtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/kv"
)

type makeStore func(t *testing.T, ctx context.Context) kv.Store

func TestDriver(t *testing.T, name string, dsn string) {
	ctx := context.Background()
	ms := makeStoreByName(name, dsn)
	t.Run("Driver_Open", func(t *testing.T) { testDriverOpen(t, ctx, ms) })
	t.Run("Store_SetGet", func(t *testing.T) { testStoreSetGet(t, ctx, ms) })
	t.Run("Store_SetIf", func(t *testing.T) { testStoreSetIf(t, ctx, ms) })
	t.Run("Store_Delete", func(t *testing.T) { testStoreDelete(t, ctx, ms) })
	t.Run("Store_Scan", func(t *testing.T) { testStoreScan(t, ctx, ms) })
}

func testDriverOpen(t *testing.T, ctx context.Context, ms makeStore) {
	store1 := ms(t, ctx)
	store1.Close()

	store2 := ms(t, ctx)
	store2.Close()
}

func testStoreSetGet(t *testing.T, ctx context.Context, ms makeStore) {
	store := ms(t, ctx)
	defer store.Close()

	testKey := []byte("key")
	testValue1 := []byte("value1")
	testValue2 := []byte("value2")

	// set test key with value1
	err := store.Set(ctx, testKey, testValue1)
	if err != nil {
		t.Fatalf("failed to set key '%s', to value '%s': %s", testKey, testValue1, err)
	}

	// get test key with value1
	val, err := store.Get(ctx, testKey)
	if err != nil {
		t.Fatalf("failed to get key '%s': %s", testKey, err)
	}
	if !bytes.Equal(testValue1, val) {
		t.Fatalf("key='%s' value='%s' doesn't match, expected='%s'", testKey, val, testValue1)
	}

	// override key with value2
	err = store.Set(ctx, testKey, testValue2)
	if err != nil {
		t.Fatalf("failed to set key '%s', to value '%s': %s", testKey, testValue2, err)
	}

	// get test key with value2
	val2, err := store.Get(ctx, testKey)
	if err != nil {
		t.Fatalf("failed to get key '%s': %s", testKey, err)
	}
	if !bytes.Equal(testValue2, val2) {
		t.Fatalf("key='%s' value='%s' doesn't match, expected='%s'", testKey, val2, testValue2)
	}

	// get a missing key
	const keyNotExists = "key-not-exists"
	val3, err := store.Get(ctx, []byte(keyNotExists))
	if !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("get key='%s' err=%s, expected not found", keyNotExists, err)
	}
	if val3 != nil {
		t.Fatalf("get key='%s' value='%s', expected nil", keyNotExists, val3)
	}
}

func testStoreDelete(t *testing.T, ctx context.Context, ms makeStore) {
	store := ms(t, ctx)
	defer store.Close()

	t.Run("exists", func(t *testing.T) {
		keyToDel := []byte("key-to-delete")
		err := store.Set(ctx, keyToDel, []byte("value to delete"))
		if err != nil {
			t.Fatalf("failed to set key='%s': %s", keyToDel, err)
		}
		err = store.Delete(ctx, keyToDel)
		if err != nil {
			t.Fatalf("failed to delete key='%s': %s", keyToDel, err)
		}
	})

	t.Run("non_exists", func(t *testing.T) {
		keyToDel := []byte("missing-key-to-delete")
		err := store.Delete(ctx, keyToDel)
		if !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("delete missing key '%s', err=%v expected=%s", keyToDel, err, kv.ErrNotFound)
		}
	})
}

func testStoreSetIf(t *testing.T, ctx context.Context, ms makeStore) {
	store := ms(t, ctx)
	defer store.Close()

	t.Run("previous_value", func(t *testing.T) {
		// setup initial value
		key := []byte("setIfKey1")
		value := []byte("val1")
		err := store.Set(ctx, key, value)
		if err != nil {
			t.Fatalf("failed to set key=%s, value=%s: %s", key, value, err)
		}

		// should set new value if old value is kept
		newValue := []byte("val2")
		err = store.SetIf(ctx, key, newValue, value)
		if err != nil {
			t.Fatalf("failed to setIf key=%s value=%s pred=%s: %s", key, newValue, value, err)
		}

		// should NOT set new value if old value is not kept
		newValue = []byte("val3")
		err = store.SetIf(ctx, key, newValue, value)
		if !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("setIf err=%s, key=%s, value=%s, pred=%s, expected err=%s", err, key, newValue, value, kv.ErrNotFound)
		}

		// should NOT set new value if try conditional nil
		newValue = []byte("val4")
		err = store.SetIf(ctx, key, newValue, nil)
		if !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("setIf err=%s, key=%s, value=%s, pred=%s, expected err=%s", err, key, newValue, value, kv.ErrNotFound)
		}
	})

	t.Run("no_previous_key", func(t *testing.T) {
		// setIf without previous value
		key := []byte("setIfKey2")
		value := []byte("val1")
		err := store.SetIf(ctx, key, value, nil)
		if err != nil {
			t.Fatalf("setIf without previous key - key=%s, value=%s: %s", key, value, err)
		}

		// should fail if we try to do the same again - as the value updated to 'val1'
		err = store.SetIf(ctx, key, value, nil)
		if !errors.Is(err, kv.ErrNotFound) {
			t.Fatalf("setIf with nil while key/value exists - err=%v, key=%s value=%s, expected err=%s",
				err, key, value, kv.ErrNotFound)
		}
	})
}

func testStoreScan(t *testing.T, ctx context.Context, ms makeStore) {
	store := ms(t, ctx)
	defer store.Close()

	// setup sample data
	const samplePrefix = "scan"
	const sampleItems = 10
	sampleData := setupSampleData(t, ctx, store, samplePrefix, sampleItems)

	t.Run("full", func(t *testing.T) {
		scanKeyPrefix := []byte("scan-key")
		scan, err := store.Scan(ctx, scanKeyPrefix)
		if err != nil {
			t.Fatal("failed to scan", err)
		}
		defer scan.Close()

		var entries []kv.Entry
		for scan.Next() {
			ent := scan.Entry()
			if ent == nil {
				t.Fatal("scan got nil entry")
			}
			if !bytes.HasPrefix(ent.Key, scanKeyPrefix) {
				break
			}
			entries = append(entries, *ent)
		}
		if err := scan.Err(); err != nil {
			t.Fatal("scan ended with an error", err)
		}
		if diff := deep.Equal(entries, sampleData); diff != nil {
			t.Fatal("scan data didn't match:", diff)
		}
	})

	t.Run("part", func(t *testing.T) {
		scan, err := store.Scan(ctx, []byte("scan-key-5"))
		if err != nil {
			t.Fatal("failed to scan", err)
		}
		defer scan.Close()

		var entries []kv.Entry
		for scan.Next() {
			ent := scan.Entry()
			if ent == nil {
				t.Fatal("scan got nil entry")
			}
			if !bytes.HasPrefix(ent.Key, []byte("scan-key-")) {
				break
			}
			entries = append(entries, *ent)
		}
		if err := scan.Err(); err != nil {
			t.Fatal("scan ended with an error", err)
		}
		if diff := deep.Equal(entries, sampleData[5:]); diff != nil {
			t.Fatal("scan data didn't match:", diff)
		}
	})
}

func makeStoreByName(name string, dsn string) makeStore {
	return func(t *testing.T, ctx context.Context) kv.Store {
		t.Helper()
		store, err := kv.Open(ctx, name, dsn)
		if err != nil {
			t.Fatalf("failed to open kv '%s' (%s) store: %s", name, dsn, err)
		}
		return store
	}
}

func setupSampleData(t *testing.T, ctx context.Context, store kv.Store, prefix string, items int) []kv.Entry {
	t.Helper()
	entries := make([]kv.Entry, 0, items)
	for i := 0; i < items; i++ {
		entry := sampleEntry(prefix, i)
		err := store.Set(ctx, entry.Key, entry.Value)
		if err != nil {
			t.Fatalf("failed to setup data with '%s': %s", entry, err)
		}
		entries = append(entries, entry)
	}
	return entries
}

func sampleEntry(prefix string, n int) kv.Entry {
	k := fmt.Sprintf("%s-key-%d", prefix, n)
	v := fmt.Sprintf("%s-value-%d", prefix, n)
	return kv.Entry{Key: []byte(k), Value: []byte(v)}
}
