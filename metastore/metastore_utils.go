package metastore

import (
	"net/url"

	"github.com/treeverse/lakefs/gateway/path"
)

const SymlinkInputFormat = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"

func ReplaceBranchName(location, branch string) string {
	u, err := url.Parse(location)
	if err != nil {
		return location
	}
	p, err := path.ResolvePath(u.Path)
	if err != nil {
		return location
	}

	return u.Scheme + "://" + u.Host + "/" + branch + "/" + p.Path
}

func GetSymlinkLocation(location, locationPrefix string) string {
	u, err := url.Parse(location)
	if err != nil {
		return location
	}
	p, err := path.ResolvePath(u.Path)
	if err != nil {
		return location
	}
	return locationPrefix + "/" + u.Host + "/" + p.Ref + "/" + p.Path
}
