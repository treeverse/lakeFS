// +build !race

package db_test

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/treeverse/lakefs/db"

	"github.com/dgraph-io/badger"

	log "github.com/sirupsen/logrus"
)

func TestBadgerDeleteRange(t *testing.T) {
	dir, err := ioutil.TempDir("", "treeverse-tests-badger")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := os.RemoveAll(dir)
		if err != nil {
			t.Fatal(err)
		}
	}()
	opts := badger.DefaultOptions(dir)
	opts.Logger = db.NewBadgerLoggingAdapter(log.WithField("subsystem", "badger"))

	kv, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	// create a bunch of keys:
	prefix := []byte("hello")
	keyBuffer := make([]byte, 50)
	keys := make([][]byte, 800)
	for i := 0; i < 800; i++ {
		_, err = rand.Read(keyBuffer)
		if err != nil {
			t.Fatal(err)
		}
		k := append(prefix, keyBuffer...)
		keys[i] = k
	}

	// now also a bunch of keys that have another prefix
	otherPrefix := []byte("hell\x00foo")
	otherKeys := make([][]byte, 200)
	for i := 0; i < 200; i++ {
		_, err = rand.Read(keyBuffer)
		if err != nil {
			t.Fatal(err)
		}
		k := append(otherPrefix, keyBuffer...)
		otherKeys[i] = k
	}

	var gwg sync.WaitGroup
	gwg.Add(2)

	// write them to kv
	go func() {
		var wg sync.WaitGroup
		wg.Add(200)
		for i := 0; i < 200; i++ {
			go func(i int) {
				err = kv.Update(func(txn *badger.Txn) error {
					err = txn.Set(otherKeys[i], []byte(fmt.Sprintf("%d", i)))
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
		gwg.Done()
	}()

	// write them to kv
	go func() {
		var wg sync.WaitGroup
		wg.Add(800)
		for i := 0; i < 800; i++ {
			go func(i int) {
				err = kv.Update(func(txn *badger.Txn) error {
					err = txn.Set(keys[i], []byte(fmt.Sprintf("%d", i)))
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				wg.Done()
			}(i)
		}

		wg.Wait()
		gwg.Done()
	}()

	gwg.Wait()

	err = kv.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		it := txn.NewIterator(opts)
		readKeys := make([][]byte, 0)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			readKeys = append(readKeys, it.Item().Key())
		}
		it.Close()
		if len(readKeys) != len(keys) {
			t.Fatalf("expected %d keys, read back %d keys", len(keys), len(readKeys))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
