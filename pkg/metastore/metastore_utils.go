package metastore

import (
	"errors"
	"net/url"

	"github.com/treeverse/lakefs/pkg/gateway/path"
)

const SymlinkInputFormat = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"

var ErrInvalidLocation = errors.New("got empty schema or host wile parsing location url, location should be schema://host/path")

func ReplaceBranchName(location, branch string) (string, error) {
	u, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", ErrInvalidLocation
	}
	p, err := path.ResolvePath(u.Path)
	if err != nil {
		return "", err
	}

	return u.Scheme + "://" + u.Host + "/" + branch + "/" + p.Path, nil
}

func GetSymlinkLocation(location, locationPrefix string) (string, error) {
	u, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", ErrInvalidLocation
	}
	p, err := path.ResolvePath(u.Path)
	if err != nil {
		return "", err
	}
	return locationPrefix + "/" + u.Host + "/" + p.Ref + "/" + p.Path, nil
}
