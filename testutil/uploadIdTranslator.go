package testutil

import (
	"sync"
	"testing"
)

type UploadIdTranslator struct {
	TransMap   map[string]string
	ExpectedId string
	T          *testing.T
	mux        sync.Mutex
}

func (d *UploadIdTranslator) SetUploadId(uploadId string) string {
	d.mux.Lock()
	defer d.mux.Unlock()
	d.TransMap[d.ExpectedId] = uploadId
	return d.ExpectedId
}
func (d *UploadIdTranslator) TranslateUploadId(simulationId string) string {
	id, ok := d.TransMap[simulationId]
	if !ok {
		d.T.Error("upload id " + simulationId + " not in map")
		return simulationId
	} else {
		return id
	}
}
func (d *UploadIdTranslator) RemoveUploadId(inputUploadId string) {
	var keyToRemove string
	d.mux.Lock()
	defer d.mux.Unlock()
	for k, v := range d.TransMap {
		if v == inputUploadId {
			keyToRemove = k
			break
		}
	}
	delete(d.TransMap, keyToRemove)
}
