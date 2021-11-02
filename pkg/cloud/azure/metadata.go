package azure

import (
	"crypto/md5" //nolint:gosec
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/logging"
)

const azureMetadataIP = "169.254.169.254"
const metadataRequestTimeout = 5 * time.Second

type MetadataProvider struct {
	logger logging.Logger
}

func NewMetadataProvider(logger logging.Logger) *MetadataProvider {
	return &MetadataProvider{logger: logger}
}

type instanceMetadataResponse struct {
	Compute struct {
		SubscriptionID string `json:"subscriptionId"`
	} `json:"compute"`
}

func (m *MetadataProvider) GetMetadata() map[string]string {
	client := http.Client{Timeout: metadataRequestTimeout}
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/metadata/instance", azureMetadataIP), nil)
	if err != nil {
		m.logger.WithError(err).Warn("Failed to create request for Azure instance metadata")
		return nil
	}
	req.Header.Add("Metadata", "True")
	q := url.Values{"format": {"json"}, "api-version": {"2019-03-11"}}
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil {
		m.logger.WithError(err).Warn("Failed to get Azure subscription ID from instance metadata", err)
		return nil
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		m.logger.WithError(err).Warn("Failed to get Azure subscription ID from instance metadata", err)
		return nil
	}
	responseObj := &instanceMetadataResponse{}
	err = json.Unmarshal(responseBody, responseObj)
	if err != nil {
		m.logger.WithError(err).Warn("Failed to get Azure subscription ID from instance metadata", err)
		return nil
	}
	if responseObj.Compute.SubscriptionID == "" {
		m.logger.Info("Got empty subscription id from azure")
		return nil
	}
	return map[string]string{
		cloud.IDKey:     fmt.Sprintf("%x", md5.Sum([]byte(responseObj.Compute.SubscriptionID))), //nolint:gosec
		cloud.IDTypeKey: "azure_subscription_id",
	}
}
