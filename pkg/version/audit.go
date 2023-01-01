package version

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	auditCheckTimeout = 5 * time.Minute
)

var (
	ErrAuditCheckFailed = errors.New("audit check request failed")
	ErrMissingCheckURL  = errors.New("missing audit check URL")
)

type Alert struct {
	ID               string `json:"id"`
	AffectedVersions string `json:"affected_versions"`
	PatchedVersions  string `json:"patched_versions"`
	Description      string `json:"description"`
}

type AuditResponse struct {
	UpgradeURL string  `json:"upgrade_url,omitempty"`
	Alerts     []Alert `json:"alerts"`
}

type auditPeriodicResponse struct {
	AuditResponse *AuditResponse
	Err           error
}

type AuditChecker struct {
	CheckURL         string
	Client           http.Client
	Version          string
	InstallationID   string
	periodicResponse atomic.Value
	ticker           *time.Ticker
}

func NewDefaultAuditChecker(checkURL, installationID string) *AuditChecker {
	return NewAuditChecker(checkURL, Version, installationID)
}

func NewAuditChecker(checkURL, version, installationID string) *AuditChecker {
	ac := &AuditChecker{
		CheckURL: checkURL,
		Client: http.Client{
			Timeout: auditCheckTimeout,
		},
		Version:        version,
		InstallationID: installationID,
	}
	// initial value for last check - empty value
	ac.periodicResponse.Store(auditPeriodicResponse{})
	return ac
}

func (a *AuditChecker) Check(ctx context.Context) (*AuditResponse, error) {
	if a.CheckURL == "" {
		return nil, ErrMissingCheckURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.CheckURL, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Add("version", a.Version)
	q.Add("installation_id", a.InstallationID)
	req.URL.RawQuery = q.Encode()

	resp, err := a.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s (Status code: %d)", ErrAuditCheckFailed, resp.Status, resp.StatusCode)
	}
	var auditResponse AuditResponse
	if err := json.NewDecoder(resp.Body).Decode(&auditResponse); err != nil {
		return nil, err
	}
	return &auditResponse, nil
}

// CheckAndLog performs an audit check, logs and keeps the last response
func (a *AuditChecker) CheckAndLog(ctx context.Context, log logging.Logger) {
	resp, err := a.Check(ctx)
	a.periodicResponse.Store(auditPeriodicResponse{
		AuditResponse: resp,
		Err:           err,
	})
	if err != nil {
		log.WithFields(logging.Fields{
			"version":   a.Version,
			"check_url": a.CheckURL,
		}).WithError(err).Error("Audit check failed")
		return
	}
	if len(resp.Alerts) == 0 {
		log.WithFields(logging.Fields{
			"version":   a.Version,
			"check_url": a.CheckURL,
		}).Debug("No alerts found on audit check")
		return
	}
	for _, alert := range resp.Alerts {
		log.WithFields(logging.Fields{
			"id":                alert.ID,
			"description":       alert.Description,
			"affected_versions": alert.AffectedVersions,
			"patched_versions":  alert.PatchedVersions,
		}).Warn("Audit security alert")
	}
	log.WithFields(logging.Fields{
		"alerts_len": len(resp.Alerts),
		"check_url":  a.CheckURL,
		"version":    a.Version,
	}).Warnf("Audit security - upgrade recommended: %s", resp.UpgradeURL)
}

func (a *AuditChecker) LastCheck() (*AuditResponse, error) {
	resp := a.periodicResponse.Load().(auditPeriodicResponse)
	return resp.AuditResponse, resp.Err
}

// StartPeriodicCheck performs one check and continues every 'interval' in the background
// check results will be found in the log and updated for 'LastCheck'
// Return false if periodic check already ran
func (a *AuditChecker) StartPeriodicCheck(ctx context.Context, interval time.Duration, log logging.Logger) bool {
	if a.ticker != nil {
		return false
	}
	a.CheckAndLog(ctx, log)
	a.ticker = time.NewTicker(interval)
	go func() {
		for range a.ticker.C {
			a.CheckAndLog(ctx, log)
		}
	}()
	return true
}

func (a *AuditChecker) StopPeriodicCheck() {
	if a.ticker == nil {
		return
	}
	a.ticker.Stop()
	a.ticker = nil
}

// Close release resources used by audit checker - ex: periodic check
func (a *AuditChecker) Close() {
	a.StopPeriodicCheck()
}

func (a *AuditResponse) UpgradeRecommendedURL() string {
	if a == nil {
		return ""
	}
	if len(a.Alerts) == 0 {
		return ""
	}
	return a.UpgradeURL
}
