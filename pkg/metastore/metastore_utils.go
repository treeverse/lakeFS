package metastore

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/treeverse/lakefs/pkg/gateway/path"
)

const symlinkInputFormat = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"

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

func ReplaceExternalToLakeFSImported(location, repo, branch string) (string, error) {
	u, err := url.Parse(location)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("%w: %s", ErrInvalidLocation, location)
	}
	return u.Scheme + "://" + repo + "/" + branch + "/" + u.Path, nil
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

func ExtractRepoAndBranch(metastoreLocationURI string) (string, string, error) {
	u, err := url.Parse(metastoreLocationURI)
	if err != nil {
		return "", "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", "", ErrInvalidLocation
	}
	p, err := path.ResolvePath(u.Path)
	if err != nil {
		return "", "", err
	}

	return u.Host, p.Ref, nil
}
