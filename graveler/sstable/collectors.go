package sstable

import (
	"fmt"

	"github.com/cockroachdb/pebble/sstable"
)

// staticCollector is an sstable.TablePropertyCollector that adds a map's values to the user
// property map.
type staticCollector struct {
	m map[string]interface{}
}

func (*staticCollector) Add(key sstable.InternalKey, value []byte) error {
	return nil
}

func (*staticCollector) Name() string {
	return "static"
}

func (s *staticCollector) Finish(userProps map[string]string) error {
	for k, v := range s.m {
		userProps[k] = fmt.Sprint(v)
	}
	return nil
}

// NewStaticCollector returns an SSTable collector that will add the properties in m when
// writing ends.
func NewStaticCollector(m map[string]interface{}) func() sstable.TablePropertyCollector {
	return func() sstable.TablePropertyCollector { return &staticCollector{m} }
}
