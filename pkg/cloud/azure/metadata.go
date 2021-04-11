package azure

import (
	"crypto/md5" //nolint:gosec
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/logging"
)

type MetadataProvider struct {
	logger logging.Logger
}

func NewMetadataProvider(logger logging.Logger) *MetadataProvider {
	return &MetadataProvider{logger: logger}
}

type instanceMetadataResponse struct {
	Compute struct {
		SubscriptionId string `json:"subscriptionId"`
	} `json:"compute"`
}

func (m *MetadataProvider) GetMetadata() map[string]string {
	var PTransport = &http.Transport{Proxy: nil}
	client := http.Client{Transport: PTransport}
	req, _ := http.NewRequest("GET", "http://169.254.169.254/metadata/instance", nil)
	req.Header.Add("Metadata", "True")
	q := req.URL.Query()
	q.Add("format", "json")
	q.Add("api-version", "2019-03-11")
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil {
		m.logger.Warnf("%v: failed to get Azure subscription ID from instance metadata", err)
		return nil
	}
	defer resp.Body.Close()
	responseBody, _ := ioutil.ReadAll(resp.Body)
	responseObj := &instanceMetadataResponse{}
	err = json.Unmarshal(responseBody, responseObj)
	if err != nil {
		m.logger.Warnf("%v: failed to get Azure subscription ID from instance metadata", err)
		return nil
	}
	if responseObj.Compute.SubscriptionId == "" {
		m.logger.Warn("got empty subscription id from azure")
		return nil
	}
	return map[string]string{
		cloud.IDKey:     fmt.Sprintf("%x", md5.Sum([]byte(responseObj.Compute.SubscriptionId))), //nolint:gosec
		cloud.IDTypeKey: "azure_subscription_id",
	}
}
