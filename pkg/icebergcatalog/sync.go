package icebergcatalog

import (
	"errors"

	"github.com/treeverse/lakefs/pkg/api/apigen"
)

type SyncManager interface {
	Pull(catalogID string, req apigen.IcebergPullRequest) error
	Push(catalogID string, req apigen.IcebergPushRequest) error
}

var ErrNotImplemented = errors.New("not implemented")

// NopSyncManager is a No-Operation implementation of sync manager
type NopSyncManager struct {
}

func (n *NopSyncManager) Pull(_ string, _ apigen.IcebergPullRequest) error {
	return ErrNotImplemented
}

func (n *NopSyncManager) Push(_ string, _ apigen.IcebergPushRequest) error {
	return ErrNotImplemented
}
