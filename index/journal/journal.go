package journal

import (
	"github.com/treeverse/lakefs/ident"
)

type OpCode int

type Event struct {
	Time        int64             // logical timestamp derived from a transaction ID
	RepoId      string            // requesting repo
	ClientId    string            // requesting Client
	Operation   OpCode            // what did we perform?
	Path        string            // file system path
	Metadata    map[string]string // as exist in the object or tree entry
	LogicalId   string            // say, object ID
	PhysicalIds []string          // actual blocks on the backing block store
}

func (e Event) Identity() []byte {
	return []byte(ident.MultiHash(e.ClientId, e.RepoId, e.Path))
}

type Journal interface {
	Log(Event) error
}
