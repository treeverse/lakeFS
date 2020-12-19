package onboard

import (
	"sort"
	"strings"
)

type PrefixIterator struct {
	Iterator
	prefixes   []string
	currentIdx int
}

// NewPrefixIterator returns an iterator to filter the given iterator.
// The returned iterator will return only the objects whose keys start with one of the given prefixes.
func NewPrefixIterator(it Iterator, prefixes []string) *PrefixIterator {
	sort.Strings(prefixes)
	return &PrefixIterator{Iterator: it, prefixes: prefixes}
}

func (p *PrefixIterator) Next() bool {
	for p.Iterator.Next() && p.currentIdx < len(p.prefixes) {
		val := p.Iterator.Get()
		key := val.Obj.Key
		for p.currentIdx < len(p.prefixes) {
			if key <= p.prefixes[p.currentIdx] {
				// current key before prefix - get next from iterator
				break
			}
			if strings.HasPrefix(key, p.prefixes[p.currentIdx]) {
				return true
			}
			// current key after prefix - go to next prefix
			p.currentIdx++
		}
	}
	return false
}
