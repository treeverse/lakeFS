package tree

import "github.com/treeverse/lakefs/graveler"

type partsAndValuesIterator struct {
	started bool
	repo    Repo
	parts   []Part
	it      graveler.ValueIterator
	err     error
}

func NewPartsAndValuesIterator(repo Repo, parts []Part) PartsAndValuesIterator {
	return &partsAndValuesIterator{repo: repo, parts: parts}
}

func (pvi *partsAndValuesIterator) NextPart() bool {
	if len(pvi.parts) <= 1 {
		return false
	}
	pvi.parts = pvi.parts[1:]
	pvi.it = nil
	return true
}

func (pvi *partsAndValuesIterator) Next() bool {
	if !pvi.started {
		pvi.started = true
		return len(pvi.parts) > 0
	}
	if pvi.it != nil {
		ok := pvi.it.Next()
		if ok {
			return true
		}
		// At end of part
		return pvi.NextPart()
	}
	// Start iterating inside part
	if len(pvi.parts) == 0 {
		return false // Iteration was already finished.
	}
	var err error
	pvi.it, err = pvi.repo.NewPartIterator(pvi.parts[0].Name, nil)
	if err != nil {
		pvi.err = err
		return false
	}
	ok := pvi.it.Next()
	if ok {
		return true
	}
	// Already at end of empty part
	return pvi.NextPart()
}

func (pvi *partsAndValuesIterator) Value() (*graveler.ValueRecord, *Part) {
	if len(pvi.parts) == 0 {
		return nil, nil
	}
	part := &pvi.parts[0]
	if pvi.it == nil {
		return nil, part // start new part
	}
	return pvi.it.Value(), part
}

func (pvi *partsAndValuesIterator) Err() error {
	if pvi.err != nil {
		return pvi.err
	}
	if pvi.it == nil {
		return nil
	}
	return pvi.it.Err()
}

func (pvi *partsAndValuesIterator) Close() {
	if pvi.it == nil {
		return
	}
	pvi.it.Close()
}
