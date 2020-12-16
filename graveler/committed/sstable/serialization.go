package sstable

import "github.com/treeverse/lakefs/graveler"

//go:generate mockgen -source=serialization.go -destination=mock/serialization.go -package=mock

// serializer is a placeholder interface for committed way of
// serializing the graveler.Value
type serializer interface {
	SerializeValue(inVal graveler.Value) ([]byte, error)
	DeserializeValue(bytes []byte) (*graveler.Value, error)
}
