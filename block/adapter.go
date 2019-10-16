package block

type Adapter interface {
	Put(block []byte, identifier string) error
	Get(identifier string) (block []byte, err error)
}
