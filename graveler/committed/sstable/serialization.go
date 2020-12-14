package sstable

import "github.com/treeverse/lakefs/graveler"

// serializer is a placeholder interface for committed way of
// serializing the graveler.Value
type serializer interface {
	serializeValue(inVal graveler.Value) ([]byte, error)
	deserializeValue(bytes []byte) (*graveler.Value, error)
}
