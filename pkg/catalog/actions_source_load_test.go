package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/graveler"
)

type actionsLoadStore struct {
	*FakeGraveler
	namespace graveler.StorageNamespace
}

func (s *actionsLoadStore) GetRepository(_ context.Context, id graveler.RepositoryID) (*graveler.RepositoryRecord, error) {
	return &graveler.RepositoryRecord{
		RepositoryID: id,
		Repository:   &graveler.Repository{StorageNamespace: s.namespace},
	}, nil
}

func TestActionsSourceLoadByAddressType(t *testing.T) {
	const (
		repositoryID = graveler.RepositoryID("repo")
		ref          = graveler.Ref("main")
		name         = "_lakefs_actions/check.yaml"
		namespace    = "mem://namespace"
		content      = "name: check\n"
	)
	tests := []struct {
		name           string
		addressType    Entry_AddressType
		identifierType block.IdentifierType
		address        string
	}{
		{name: "relative", addressType: Entry_RELATIVE, identifierType: block.IdentifierTypeRelative, address: "data/check"},
		{name: "full", addressType: Entry_FULL, identifierType: block.IdentifierTypeFull, address: "mem://external/_lakefs_actions/check.yaml"},
		{name: "by prefix", addressType: Entry_BY_PREFIX_DEPRECATED, identifierType: block.IdentifierTypeRelative, address: "data/check"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			adapter := mem.New(ctx)
			_, err := adapter.Put(ctx, block.ObjectPointer{
				StorageNamespace: namespace,
				IdentifierType:   tt.identifierType,
				Identifier:       tt.address,
			}, int64(len(content)), strings.NewReader(content), block.PutOpts{})
			require.NoError(t, err)

			store := &actionsLoadStore{
				FakeGraveler: &FakeGraveler{KeyValue: map[string]*graveler.Value{
					fakeGravelerBuildKey(repositoryID, ref, graveler.Key(name)): MustEntryToValue(&Entry{
						Address:     tt.address,
						AddressType: tt.addressType,
					}),
				}},
				namespace: namespace,
			}
			source := NewActionsSource(&Catalog{Store: store, BlockAdapter: adapter})

			got, err := source.Load(ctx, graveler.HookRecord{
				Repository: &graveler.RepositoryRecord{RepositoryID: repositoryID},
				SourceRef:  ref,
			}, name)
			require.NoError(t, err)
			require.Equal(t, content, string(got))
		})
	}
}
