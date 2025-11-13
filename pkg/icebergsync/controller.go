package icebergsync

import (
	"errors"
	"net/http"

	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/httputil"
)

type Controller interface {
	Pull(w http.ResponseWriter, r *http.Request, body apigen.PullIcebergTableJSONRequestBody, catalogID string)
	Push(w http.ResponseWriter, r *http.Request, body apigen.PushIcebergTableJSONRequestBody, catalogID string)
}

var ErrNotImplemented = errors.New("not implemented")

type NopController struct {
}

func (n *NopController) Pull(w http.ResponseWriter, r *http.Request, _ apigen.PullIcebergTableJSONRequestBody, _ string) {
	httputil.WriteAPIError(w, r, http.StatusNotImplemented, ErrNotImplemented)
}

func (n *NopController) Push(w http.ResponseWriter, r *http.Request, _ apigen.PushIcebergTableJSONRequestBody, _ string) {
	httputil.WriteAPIError(w, r, http.StatusNotImplemented, ErrNotImplemented)
}
