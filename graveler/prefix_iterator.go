package graveler

import "bytes"

// PrefixIterator holds a ValueIterator and iterates only over values the their Key starts with the prefix
type PrefixIterator struct {
	prefix        Key
	valueIterator ValueIterator
	current       *ValueRecord
}

func NewPrefixIterator(iterator ValueIterator, prefix Key) *PrefixIterator {
	iterator.SeekGE(prefix)
	return &PrefixIterator{
		prefix:        prefix,
		valueIterator: iterator,
		current:       nil,
	}
}

func (p *PrefixIterator) Next() bool {
	if !p.valueIterator.Next() {
		p.current = nil
		return false
	}
	val := p.valueIterator.Value()
	if !bytes.HasPrefix(val.Key, p.prefix) {
		p.current = nil
		return false
	}
	p.current = val
	return true
}

func (p *PrefixIterator) SeekGE(id Key) {
	if bytes.Compare(id, p.prefix) <= 0 {
		p.valueIterator.SeekGE(p.prefix)
		return
	}
	p.valueIterator.SeekGE(id)
}

func (p *PrefixIterator) Value() *ValueRecord {
	return p.current
}

func (p *PrefixIterator) Err() error {
	return p.valueIterator.Err()
}

func (p *PrefixIterator) Close() {
	p.valueIterator.Close()
}
