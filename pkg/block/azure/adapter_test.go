package azure_test

import (
	"context"
	"net/url"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/params"
)

func TestAzureAdapter(t *testing.T) {
	basePath, err := url.JoinPath(blockURL, containerName)
	require.NoError(t, err)
	localPath, err := url.JoinPath(basePath, "lakefs")
	require.NoError(t, err)
	externalPath, err := url.JoinPath(basePath, "external")
	require.NoError(t, err)

	adapter, err := azure.NewAdapter(context.Background(), params.Azure{
		StorageAccount:   accountName,
		StorageAccessKey: accountKey,
		TestEndpointURL:  blockURL,
		Domain:           domain,
	})
	require.NoError(t, err, "create new adapter")
	blocktest.AdapterTest(t, adapter, localPath, externalPath)
}

func TestAdapterNamespace(t *testing.T) {
	tests := []struct {
		Name          string
		Namespace     string
		Success       bool
		Domain        string
		InvalidDomain bool
	}{
		{
			Name:      "valid_https",
			Namespace: "https://test.blob.core.windows.net/container1/repo1",
			Success:   true,
		},
		{
			Name: "valid_https_presign",
			Namespace: "https://esti.blob.core.windows.net/esti-system-testing/11484/29538/coddo7tpocvujsvjhhjg/repo-coddo7tpocvujsvjhhj0/data/ggq2f8tpocvs73ftd1qg/" +
				"coddo7tpocvs73ftd7u0?se=2024-04-13T19%3A40%3A19Z&sig=r%2BV3wR16PUG74U7wxZ1KknhRT%2BO2JqVJM0EVTuJbz8U%3D&ske=2024-04-13T20%3A40%3A19Z&" +
				"skoid=d8193925-4108-40a0-8d76-0c6d3ceb1f61&sks=b&skt=2024-04-13T19%3A25%3A09Z&sktid=***&skv=2023-08-03&sp=raw&spr=https&sr=b&sv=2021-12-02",
			Success: true,
		},
		{
			Name:      "valid_https",
			Namespace: "https://test.blob.core.windows.net/container1/repo1",
			Success:   false,
			Domain:    "blob.core.chinacloudapi.cn",
		},
		{
			Name:      "valid_https_china",
			Namespace: "https://test.blob.core.chinacloudapi.cn/container1/repo1",
			Success:   false,
		},
		{
			Name:      "valid_https_china",
			Namespace: "https://test.blob.core.chinacloudapi.cn/container1/repo1",
			Success:   true,
			Domain:    "blob.core.chinacloudapi.cn",
		},
		{
			Name:      "valid_http",
			Namespace: "http://test.blob.core.windows.net/container1/repo1",
			Success:   true,
		},
		{
			Name:      "invalid_subdomain",
			Namespace: "https://test.adls.core.windows.net/container1/repo1",
			Success:   false,
		},
		{
			Name:      "partial",
			Namespace: "https://test.adls.core.windows.n",
			Success:   false,
		},
		{
			Name:      "s3",
			Namespace: "s3://test/adls/core/windows/net",
			Success:   false,
		},
		{
			Name:      "invalid_string",
			Namespace: "this is a bad string",
			Success:   false,
		},
		{
			Name:      "invalid_https_china_mix_1",
			Namespace: "https://test.blob.core.chinacloudapi.net/container1/repo1",
			Success:   false,
		},
		{
			Name:      "invalid_https_china_mix_2",
			Namespace: "https://test.blob.core.windows.cn/container1/repo1",
			Success:   false,
		},
		{
			Name:      "valid_gov_cloud",
			Namespace: "https://test.blob.core.usgovcloudapi.net/container1/repo1",
			Success:   true,
			Domain:    "blob.core.usgovcloudapi.net",
		},
		{
			Name:      "valid_gov_cloud_no_domain",
			Namespace: "https://test.blob.core.usgovcloudapi.net/container1/repo1",
			Success:   false,
		},
		{
			Name:          "invalid_domain",
			Namespace:     "https://test.blob.core.usgovcloudapi.net/container1/repo1",
			Success:       false,
			Domain:        "invalid_domain",
			InvalidDomain: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			adapter, err := azure.NewAdapter(context.Background(), params.Azure{
				StorageAccount:   accountName,
				StorageAccessKey: accountKey,
				TestEndpointURL:  blockURL,
				Domain:           tt.Domain,
			})
			if tt.InvalidDomain {
				require.ErrorIs(t, err, azure.ErrInvalidDomain)
				return
			}
			require.NoError(t, err, "create new adapter")

			namespaceInfo := adapter.GetStorageNamespaceInfo()
			expr, err := regexp.Compile(namespaceInfo.ValidityRegex)
			require.NoError(t, err)

			require.Equal(t, tt.Success, expr.MatchString(tt.Namespace))
		})
	}
}

func TestAzureParseURL(t *testing.T) {
	tests := []struct {
		Name          string
		Url           string
		Account       string
		Domain        string
		InvalidDomain bool
	}{
		{
			Name:    "valid_https",
			Url:     "https://account1.blob.core.windows.net/container1/repo1",
			Account: "account1",
			Domain:  azure.BlobEndpointDefaultDomain,
		},
		{
			Name: "valid_https_presign",
			Url: "https://test.blob.core.windows.net/esti-system-testing/11484/29538/coddo7tpocvujsvjhhjg/repo-coddo7tpocvujsvjhhj0/data/ggq2f8tpocvs73ftd1qg/coddo7tpocvs73ftd7u0?" +
				"se=2024-04-13T19%3A40%3A19Z&sig=r%2BV3wR16PUG74U7wxZ1KknhRT%2BO2JqVJM0EVTuJbz8U%3D&ske=2024-04-13T20%3A40%3A19Z&skoid=d8193925-4108-40a0-8d76-0c6d3ceb1f61&" +
				"sks=b&skt=2024-04-13T19%3A25%3A09Z&sktid=***&skv=2023-08-03&sp=raw&spr=https&sr=b&sv=2021-12-02",
			Account: "test",
			Domain:  azure.BlobEndpointDefaultDomain,
		},
		{
			Name:    "valid_china_https",
			Url:     "https://china.blob.core.chinacloudapi.cn/container1/repo1",
			Account: "china",
			Domain:  azure.BlobEndpointChinaDomain,
		},
		{
			Name:    "valid_usgov_https",
			Url:     "https://mygov.blob.core.usgovcloudapi.net/container1/repo1",
			Account: "mygov",
			Domain:  azure.BlobEndpointUSGovDomain,
		},
		{
			Name:    "valid_other_https",
			Url:     "https://koala.blob.core.australiacloudapi.net/container1/repo1",
			Account: "koala",
			Domain:  "blob.core.australiacloudapi.net",
		},
		{
			Name:          "invalid_http",
			Url:           "http://mygov.blob.core.usgovcloudapi.net/container1/repo1",
			InvalidDomain: true,
		},
		{
			Name:          "invalid_url_s3",
			Url:           "s3://example/bar/baz",
			InvalidDomain: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			uri, err := url.Parse(tt.Url)
			account, dom, err := azure.ParseURL(uri)
			if tt.InvalidDomain {
				require.ErrorIs(t, err, azure.ErrAzureInvalidURL)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.Account, account)
				require.Equal(t, tt.Domain, dom)
			}
		})
	}
}
