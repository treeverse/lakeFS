package azure_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block/azure"
)

func TestExtraction(t *testing.T) {
	tests := []struct {
		name                   string
		url                    string
		expectedErr            error
		expectedStorageAccount string
	}{
		{
			name:                   "simple",
			url:                    "https://somestorageaccount.blob.core.windows.net/newcontainer/2023/",
			expectedStorageAccount: "somestorageaccount",
		},
		{
			name:                   "no container",
			url:                    "https://somestorageaccount.blob.core.windows.net/",
			expectedStorageAccount: "somestorageaccount",
		},
		{
			name:                   "long prefix",
			url:                    "https://somestorageaccount.blob.core.windows.net/container/somestorageaccount.blob.core.windows.net",
			expectedStorageAccount: "somestorageaccount",
		},
		{
			name:        "capital letters",
			url:         "https://Rgeaccount.blob.core.windows.net/newcontainer/2023/",
			expectedErr: azure.ErrUnknownAzureURL,
		},
		{
			name:        "different host",
			url:         "https://somestorgeaccount.dark.web/newcontainer/2023/",
			expectedErr: azure.ErrUnknownAzureURL,
		},
		{
			name:        "different host",
			url:         "https://somestorgeaccount.dark.web/newcontainer/2023/",
			expectedErr: azure.ErrUnknownAzureURL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualStorageAccount, err := azure.ExtractStorageAccount(tt.url)
			if tt.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, tt.expectedStorageAccount, actualStorageAccount)
			} else {
				require.Equal(t, tt.expectedErr, err)
			}
		})
	}

}
