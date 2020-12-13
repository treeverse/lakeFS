package graveler

import "bytes"

// prefixIterator holds a ValueIterator and iterates only over values the their Key starts with the prefix
type prefixIterator struct {
	prefix   Key
	iterator ValueIterator
	ended    bool
}

func NewPrefixIterator(iterator ValueIterator, prefix Key) ValueIterator {
	iterator.SeekGE(prefix)
	return &prefixIterator{
		prefix:   prefix,
		iterator: iterator,
	}
}

func (p *prefixIterator) Next() bool {
	if p.ended {
		return false
	}
	// prefix iterator ends when there is no more data, or the next value doesn't match the prefix
	if !p.iterator.Next() || !bytes.HasPrefix(p.iterator.Value().Key, p.prefix) {
		p.ended = true
		return false
	}
	return true
}

func (p *prefixIterator) SeekGE(id Key) {
	from := id
	if bytes.Compare(id, p.prefix) <= 0 {
		from = p.prefix
	}
	p.iterator.SeekGE(from)
	p.ended = false
}

func (p *prefixIterator) Value() *ValueRecord {
	if p.ended {
		return nil
	}
	return p.iterator.Value()
}

func (p *prefixIterator) Err() error {
	return p.iterator.Err()
}

func (p *prefixIterator) Close() {
	p.iterator.Close()
}
