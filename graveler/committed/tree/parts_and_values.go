package tree

import "github.com/treeverse/lakefs/graveler"

type iterator struct {
	started bool
	repo    Repo
	parts   []Part
	it      graveler.ValueIterator
	err     error
}

func (pvi *iterator) SeekGE(id graveler.Key) {
	panic("implement me")
}

func NewIterator(repo Repo, parts []Part) Iterator {
	return &iterator{repo: repo, parts: parts}
}

func (pvi *iterator) NextPart() bool {
	if len(pvi.parts) <= 1 {
		return false
	}
	pvi.parts = pvi.parts[1:]
	pvi.it.Close()
	pvi.it = nil
	return true
}

func (pvi *iterator) Next() bool {
	if !pvi.started {
		pvi.started = true
		return len(pvi.parts) > 0
	}
	if pvi.it != nil {
		if pvi.it.Next() {
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
	if pvi.it.Next() {
		return true
	}
	// Already at end of empty part
	return pvi.NextPart()
}

func (pvi *iterator) Value() (*graveler.ValueRecord, *Part) {
	if len(pvi.parts) == 0 {
		return nil, nil
	}
	part := &pvi.parts[0]
	if pvi.it == nil {
		return nil, part // start new part
	}
	return pvi.it.Value(), part
}

func (pvi *iterator) Err() error {
	if pvi.err != nil {
		return pvi.err
	}
	if pvi.it == nil {
		return nil
	}
	return pvi.it.Err()
}

func (pvi *iterator) Close() {
	if pvi.it == nil {
		return
	}
	pvi.it.Close()
}
