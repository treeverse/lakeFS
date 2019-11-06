package block

type Adapter interface {
	Put(block []byte, identifier string) error
	Get(identifier string) (block []byte, err error)
	GetOffset(identifier string, from, to int64) (block []byte, err error)
}
