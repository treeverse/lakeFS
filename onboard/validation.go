package onboard

import (
	"encoding/json"
	"errors"
	"github.com/treeverse/lakefs/block"
	"net/url"
	"strings"
)

var (
	ErrManifestParse = errors.New("failed to parse manifest url")
	ErrManifestRead  = errors.New("failed to read manifest")
)

func ValidateManifest(adapter block.Adapter, manifestURL string) error {
	u, err := url.Parse(manifestURL)
	if err != nil {
		return ErrManifestParse
	}
	data, err := adapter.Get(block.ObjectPointer{StorageNamespace: u.Scheme + "://" + u.Host, Identifier: u.Path})
	if err != nil {
		return ErrManifestRead
	}
	decoder := json.NewDecoder(data)
	buf := new(strings.Builder)
	err = decoder.Decode(buf)
	if err != nil {
		return ErrManifestRead
	}
	return nil
}
