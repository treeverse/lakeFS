package catalog_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/catalog/testutils"
	"github.com/treeverse/lakefs/pkg/ingest/store"
)

const (
	fromSourceURI           = "https://valid.uri"
	uriPrefix               = "take/from/here"
	fromSourceURIWithPrefix = fromSourceURI + "/" + uriPrefix
	after                   = "some/key/to/start/after"
	continuationToken       = "opaque"
	prepend                 = "some/logical/prefix"
	count                   = 1000
)

func TestWalkEntryIterator(t *testing.T) {
	tests := []struct {
		name string
		max  int
	}{
		{
			name: "walker exhausted",
			max:  count * 3,
		},
		{
			name: "walker not exhausted",
			max:  count / 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := testutils.NewFakeWalker(count, tt.max, uriPrefix, after, continuationToken, fromSourceURIWithPrefix, nil)
			parsedURL, _ := url.Parse(fromSourceURIWithPrefix)
			sut, err := catalog.NewWalkEntryIterator(context.Background(), store.NewWrapper(w, parsedURL), prepend, after, continuationToken)
			require.NoError(t, err, "creating walk entry iterator")
			require.NotNil(t, sut)

			i := 0
			for ; sut.Next() && i < w.Max; i++ {
				require.NoError(t, sut.Err())
				require.Equal(t, prepend+"/"+w.Entries[i].RelativeKey, sut.Value().Path.String())
				if i < count-1 {
					// Last entry since race condition can give inconsistent HasMore.
					// After it's closed than HasMore must be set to false
					require.Equal(t, catalog.Mark{LastKey: w.Entries[i].FullKey, HasMore: true, ContinuationToken: testutils.ContinuationTokenOpaque}, sut.Marker())
				}
			}
			sut.Close()
			require.NoError(t, sut.Err())

			if i == count {
				require.Equal(t, catalog.Mark{LastKey: "", HasMore: false, ContinuationToken: ""}, sut.Marker())
			} else {
				require.Equal(t, catalog.Mark{LastKey: w.Entries[i].FullKey, HasMore: true, ContinuationToken: testutils.ContinuationTokenOpaque}, sut.Marker())
			}
			require.NoError(t, sut.Err())
		})
	}
}
