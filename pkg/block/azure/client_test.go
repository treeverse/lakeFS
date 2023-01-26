package azure_test

import (
	"errors"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/azure"
)

func TestExtraction(t *testing.T) {
	parse := func(u string) *url.URL {
		url, err := url.Parse(u)
		require.NoError(t, err)
		return url
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
			expectedErr: block.ErrInvalidNamespace,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualStorageAccount, err := azure.ExtractStorageAccount(tt.url)
			if tt.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, tt.expectedStorageAccount, actualStorageAccount)
			} else {
				require.True(t, errors.Is(err, tt.expectedErr))
			}
		})
	}

}
