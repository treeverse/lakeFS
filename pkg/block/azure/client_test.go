package azure_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/azure"
)

func TestExtraction(t *testing.T) {
	parse := func(p string) *url.URL {
		u, err := url.Parse(p)
		require.NoError(t, err)
		return u
	}
	tests := []struct {
		name                   string
		url                    *url.URL
		expectedErr            error
		expectedStorageAccount string
	}{
		{
			name:                   "simple",
			url:                    parse("https://somestorageaccount.blob.core.windows.net/newcontainer/2023/"),
			expectedStorageAccount: "somestorageaccount",
		},
		{
			name:                   "no container",
			url:                    parse("https://somestorageaccount.blob.core.windows.net/"),
			expectedStorageAccount: "somestorageaccount",
		},
		{
			name:                   "long prefix",
			url:                    parse("https://somestorageaccount.blob.core.windows.net/container/somestorageaccount.blob.core.windows.net"),
			expectedStorageAccount: "somestorageaccount",
		},
		{
			name:        "No subdomains",
			url:         parse("https://Rgeaccountblobcorewindowsnet/newcontainer/2023/"),
			expectedErr: block.ErrInvalidAddress,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualStorageAccount, err := azure.ExtractStorageAccount(tt.url)
			if tt.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, tt.expectedStorageAccount, actualStorageAccount)
			} else {
				require.ErrorIs(t, err, tt.expectedErr)
			}
		})
	}
}
