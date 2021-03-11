package testutil

import (
	"sync"
	"testing"
)

type UploadIDTranslator struct {
	T          *testing.T
	TransMap   map[string]string
	ExpectedID string
	mux        sync.Mutex
}

func (d *UploadIDTranslator) SetUploadID(uploadID string) string {
	d.mux.Lock()
	defer d.mux.Unlock()
	d.TransMap[d.ExpectedID] = uploadID
	return d.ExpectedID
}

func (d *UploadIDTranslator) TranslateUploadID(simulationID string) string {
	id, ok := d.TransMap[simulationID]
	if !ok {
		d.T.Error("upload id " + simulationID + " not in map")
		return simulationID
	} else {
		return id
	}
}
func (d *UploadIDTranslator) RemoveUploadID(inputUploadID string) {
	var keyToRemove string
	d.mux.Lock()
	defer d.mux.Unlock()
	for k, v := range d.TransMap {
		if v == inputUploadID {
			keyToRemove = k
			break
		}
	}
	delete(d.TransMap, keyToRemove)
}
