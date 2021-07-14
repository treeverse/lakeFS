package ref_test

import (
	"encoding/hex"

	"github.com/treeverse/lakefs/pkg/ident"
)

type fakeAddressProvider struct {
	identityToFakeIdentity map[string]string
}

func (f *fakeAddressProvider) ContentAddress(identifiable ident.Identifiable) string {
	return f.identityToFakeIdentity[hex.EncodeToString(identifiable.Identity())]
}
