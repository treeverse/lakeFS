package sstable

import (
	"github.com/cockroachdb/pebble/sstable"
)

// staticCollector is a sstable.TablePropertyCollector that adds a map's values to the user
// property map.
type staticCollector struct {
	m map[string]string
}

func (*staticCollector) Add(_ sstable.InternalKey, _ []byte) error {
	return nil
}

func (*staticCollector) Name() string {
	return "static"
}

func (s *staticCollector) Finish(userProps map[string]string) error {
	for k, v := range s.m {
		userProps[k] = v
	}
	return nil
}

// NewStaticCollector returns an SSTable collector that will add the properties in m when
// writing ends.
func NewStaticCollector(m map[string]string) func() sstable.TablePropertyCollector {
	return func() sstable.TablePropertyCollector { return &staticCollector{m} }
}
