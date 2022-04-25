package kvtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-test/deep"
	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/treeverse/lakefs/pkg/kv"
)

type makeStore func(t *testing.T, ctx context.Context) kv.Store

var runTestID = nanoid.MustGenerate("abcdef1234567890", 8)

func uniqueKey(k string) []byte {
	return []byte(k + "-" + runTestID)
}

func TestDriver(t *testing.T, name string, dsn string) {
	ms := makeStoreByName(name, dsn)
	t.Run("Driver_Open", func(t *testing.T) { testDriverOpen(t, ms) })
	t.Run("Store_SetGet", func(t *testing.T) { testStoreSetGet(t, ms) })
	t.Run("Store_SetIf", func(t *testing.T) { testStoreSetIf(t, ms) })
	t.Run("Store_Delete", func(t *testing.T) { testStoreDelete(t, ms) })
	t.Run("Store_Scan", func(t *testing.T) { testStoreScan(t, ms) })
}

func testDriverOpen(t *testing.T, ms makeStore) {
	ctx := context.Background()
	store1 := ms(t, ctx)
	store2 := ms(t, ctx)
	store1.Close()
	store2.Close()
}

func testStoreSetGet(t *testing.T, ms makeStore) {
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	testKey := uniqueKey("key")
	testValue1 := []byte("value")
	testValue2 := []byte("a different kind of value")

	// set test key with value1
	err := store.Set(ctx, testKey, testValue1)
	if err != nil {
		t.Fatalf("failed to set key '%s', to value '%s': %s", testKey, testValue1, err)
	}

	// get test key with value1
	val, err := store.Get(ctx, testKey)
	switch {
	case err != nil:
		t.Fatalf("failed to get key '%s': %s", testKey, err)
	case val == nil:
		t.Fatalf("got value with nil")
	case !bytes.Equal(testValue1, val):
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
	keyNotExists := uniqueKey("key-not-exists")
	val3, err := store.Get(ctx, keyNotExists)
	if !errors.Is(err, kv.ErrNotFound) {
		t.Fatalf("get key='%s' err=%s, expected not found", keyNotExists, err)
	}
	if val3 != nil {
		t.Fatalf("get key='%s' value='%s', expected nil", keyNotExists, val3)
	}
}

func testStoreDelete(t *testing.T, ms makeStore) {
	t.Parallel()
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	t.Run("exists", func(t *testing.T) {
		keyToDel := uniqueKey("key-to-delete")
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
		keyToDel := uniqueKey("missing-key-to-delete")
		err := store.Delete(ctx, keyToDel)
		if err != nil {
			t.Fatalf("delete missing key '%s', err=%v expected nil", keyToDel, err)
		}
	})
}

func testStoreSetIf(t *testing.T, ms makeStore) {
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	t.Run("previous_value", func(t *testing.T) {
		// setup initial value
		key := uniqueKey("setIfKey1")
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
		key := uniqueKey("setIfKey2")
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

func testStoreScan(t *testing.T, ms makeStore) {
	ctx := context.Background()
	store := ms(t, ctx)
	defer store.Close()

	// setup sample data
	samplePrefix := uniqueKey("scan")
	const sampleItems = 10
	sampleData := setupSampleData(t, ctx, store, string(samplePrefix), sampleItems)

	t.Run("full", func(t *testing.T) {
		scan, err := store.Scan(ctx, samplePrefix)
		if err != nil {
			t.Fatal("failed to scan", err)
		}
		defer scan.Close()

		var entries []kv.Entry
		for scan.Next() {
			ent := scan.Entry()
			switch {
			case ent == nil:
				t.Fatal("scan got nil entry")
			case ent.Key == nil:
				t.Fatal("Key is nil while scan item", len(entries))
			case ent.Value == nil:
				t.Fatal("Value is nil while scan item", len(entries))
			}
			if !bytes.HasPrefix(ent.Key, samplePrefix) {
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
		fromKey := append(samplePrefix, []byte("-key-5")...)
		scan, err := store.Scan(ctx, fromKey)
		if err != nil {
			t.Fatal("failed to scan", err)
		}
		defer scan.Close()

		var entries []kv.Entry
		for scan.Next() {
			ent := scan.Entry()
			switch {
			case ent == nil:
				t.Fatal("scan got nil entry")
			case ent.Key == nil:
				t.Fatal("scan got entry with nil Key")
			case ent.Value == nil:
				t.Fatal("scan got entry with nil Value")
			}
			if !bytes.HasPrefix(ent.Key, samplePrefix) {
				break
			}
			entries = append(entries, *ent)
			t.Fail()
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
