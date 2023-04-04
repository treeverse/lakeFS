package api

import "github.com/treeverse/lakefs/pkg/version"

type AuditChecker interface {
	LastCheck() (*version.AuditResponse, error)
	CheckLatestVersion() (*version.LatestVersionResponse, error)
}
