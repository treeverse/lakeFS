package catalog

import (
	"strings"
)

// prefixIterator use the underlying iterator to go over a specific prefix.
// will start by seek to the first item inside the prefix and end when it gets to the first item without the prefix.
type prefixIterator struct {
	prefix string
	it     EntryIterator
	ended  bool
}

func NewPrefixIterator(it EntryIterator, prefix Path) EntryIterator {
	it.SeekGE(prefix)
	return &prefixIterator{
		prefix: prefix.String(),
		it:     it,
	}
}

func (p *prefixIterator) Next() bool {
	if p.ended {
		return false
	}
	// prefix it ends when there is no more data, or the next value doesn't match the prefix
	if !p.it.Next() || !strings.HasPrefix(p.it.Value().Path.String(), p.prefix) {
		p.ended = true
		return false
	}
	return true
}

func (p *prefixIterator) SeekGE(id Path) {
	var from Path
	if id.String() <= p.prefix {
		from = Path(p.prefix)
	} else {
		from = id
	}
	p.it.SeekGE(from)
	p.ended = false
}

func (p *prefixIterator) Value() *EntryRecord {
	if p.ended {
		return nil
	}
	return p.it.Value()
}

func (p *prefixIterator) Err() error {
	return p.it.Err()
}

func (p *prefixIterator) Close() {
	p.it.Close()
}
