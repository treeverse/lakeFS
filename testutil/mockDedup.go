package testutil

type MockDedup struct {
	DedupIndex map[string]string
}

func NewMockDedup() *MockDedup {
	m := make(map[string]string)
	return &MockDedup{DedupIndex: m}
}

func (d *MockDedup) CreateDedupEntryIfNone(repoId string, dedupId string, objName string) (string, error) {
	existingObj, ok := d.DedupIndex[dedupId]
	if ok {
		return existingObj, nil
	} else {
		d.DedupIndex[dedupId] = objName
		return objName, nil
	}
}
