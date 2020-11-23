package sstable

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/treeverse/lakefs/rocks3"
)

type PebbleSSTableManager struct {
	// maximum size in bytes for the in-memory cache for SSTables
	cacheMaxSize int64

	disk diskSSTableManager

	tableReaders map[SSTableID]*sstable.Reader

	loadingReaders sync.Map
}

func NewPebbleSSTableManager() *PebbleSSTableManager {
	return &PebbleSSTableManager{
		loadingReaders: sync.Map{},
	}
}

// GetEntry returns the entry matching the path in the SSTable referenced by the id.
// If path not found, (nil, ErrPathNotFound) is returned.
func (m *PebbleSSTableManager) GetEntry(path rocks3.Path, tid SSTableID) (*rocks3.Entry, error) {
	reader, err := m.getReader(tid)
	if err != nil {
		return nil, err
	}

	it, err := reader.NewIter([]byte(path), nil)
	defer it.Close()
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}

	// actual reading
	key, val := it.Next()
	if key == nil {
		// checking if an error occurred or key simply not found
		if it.Error() != nil {
			return nil, fmt.Errorf("reading key: %w", it.Error())
		}

		// lookup path is bigger than the last path in the SSTable
		return nil, ErrPathNotFound
	}

	if rocks3.Path(key.UserKey) != path {
		// lookup path in range but key not found
		return nil, ErrPathNotFound
	}

	return deserializeEntry(val)
}

func (m *PebbleSSTableManager) getReader(tid SSTableID) (*sstable.Reader, error) {
	if !m.isReaderLoaded(tid) {
		err := m.loadReader(tid)
		if err != nil {
			return nil, fmt.Errorf("load sstable: %w", err)
		}
	}

	return m.tableReaders[tid], nil
}

func deserializeEntry(val []byte) (*rocks3.Entry, error) {
	// TODO: pending serialization
	return nil, nil
}

func serializeEntry(entry rocks3.Entry) ([]byte, error) {
	// TODO: serialize in the best compact way
	return nil, nil
}

// SSTableIterator takes a given SSTable and returns an EntryIterator seeked to >= "from" path
func (m *PebbleSSTableManager) SSTableIterator(tid SSTableID, from rocks3.Path) (EntryIterator, error) {
	reader, err := m.getReader(tid)
	if err != nil {
		return nil, err
	}

	iter, err := reader.NewIter([]byte(from), nil)
	if err != nil {
		return nil, fmt.Errorf("creating sstable iterator: %w", err)
	}

	return &Iterator{it: iter}, nil
}

// GetWriter returns a new SSTable writer instance
func (m *PebbleSSTableManager) GetWriter() (Writer, error) {
	return m.disk.newSSTableWriter()
}

type dispatcher struct {
	sync.Mutex
	chs        []chan bool
	dispatched bool
}

func (d *dispatcher) register() (<-chan bool, bool) {
	d.Lock()
	defer d.Unlock()
	if d.dispatched {
		return nil, false
	}

	ch := make(chan bool)
	d.chs = append(d.chs, ch)

	return ch, true
}

func (d *dispatcher) dispatch() {
	d.Lock()
	defer d.Unlock()

	for _, ch := range d.chs {
		ch <- true
	}
	d.dispatched = true
}

func (m *PebbleSSTableManager) loadReader(tid SSTableID) error {
	// the goroutine that stored the dispatcher is the one in-charge of loading the reader
	val, loaded := m.loadingReaders.LoadOrStore(tid, &dispatcher{})
	dispatcher, ok := val.(*dispatcher)
	if !ok {
		return fmt.Errorf("unkown value")
	}

	if loaded {
		return m.waitUntilReaderLoaded(tid, dispatcher)
	}

	// we're in-charge of initiating the reader
	defer dispatcher.dispatch()
	defer m.loadingReaders.Delete(tid)

	// checking again before actual loading
	if m.isReaderLoaded(tid) {
		return nil
	}

	reader, err := m.disk.loadReader(tid)
	if err != nil {
		return fmt.Errorf("load reader: %w", err)
	}
	m.tableReaders[tid] = reader
	return nil
}

func (m *PebbleSSTableManager) waitUntilReaderLoaded(tid SSTableID, dispatcher *dispatcher) error {
	// need to wait until someone else loads the reader
	ch, ok := dispatcher.register()
	if !ok {
		// dispatcher was invalidated, need to retry
		return m.loadReader(tid)
	}

	select {
	case <-ch:
		if m.isReaderLoaded(tid) {
			return nil
		}
		// Loading reader failed in primary flow.
		// We should start initiating reader again.
		return m.loadReader(tid)
	}
}

func (m *PebbleSSTableManager) isReaderLoaded(tid SSTableID) bool {
	_, ok := m.tableReaders[tid]
	return ok
}
