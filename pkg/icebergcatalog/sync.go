package icebergcatalog

import (
	"errors"
	"net/http"

	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/httputil"
)

type SyncManager interface {
	Pull(w http.ResponseWriter, r *http.Request, body apigen.PullIcebergTableJSONRequestBody, catalogID string)
	Push(w http.ResponseWriter, r *http.Request, body apigen.PushIcebergTableJSONRequestBody, catalogID string)
}

var ErrNotImplemented = errors.New("not implemented")

// NopSyncManager is a No-Operation implementation of sync manager
type NopSyncManager struct {
}

func (n *NopSyncManager) Pull(w http.ResponseWriter, r *http.Request, _ apigen.PullIcebergTableJSONRequestBody, _ string) {
	httputil.WriteError(w, r, http.StatusNotImplemented, ErrNotImplemented)
}

func (n *NopSyncManager) Push(w http.ResponseWriter, r *http.Request, _ apigen.PushIcebergTableJSONRequestBody, _ string) {
	httputil.WriteError(w, r, http.StatusNotImplemented, ErrNotImplemented)
}
