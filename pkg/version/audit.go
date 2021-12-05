package version

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	auditCheckURL     = "http://localhost:4000/audit"
	auditCheckTimeout = time.Minute
)

var ErrAuditCheckFailed = errors.New("audit check request failed")

type Alert struct {
	ID              string `json:"id" db:"id"`
	AffectedVersion string `json:"affected_version" db:"affected_version"`
	PatchedVersion  string `json:"patched_version" db:"patched_version"`
	Description     string `json:"description" db:"description"`
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
	periodicResponse atomic.Value
}

func NewAuditChecker(version string) *AuditChecker {
	return &AuditChecker{
		CheckURL: auditCheckURL,
		Client: http.Client{
			Timeout: auditCheckTimeout,
		},
		Version: version,
	}
}

func (a *AuditChecker) Check(ctx context.Context) (*AuditResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.CheckURL, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	q.Add("version", a.Version)
	req.URL.RawQuery = q.Encode()

	resp, err := a.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, ErrAuditCheckFailed
	}
	var auditResponse AuditResponse
	if err := json.NewDecoder(resp.Body).Decode(&auditResponse); err != nil {
		return nil, err
	}
	return &auditResponse, nil
}

// CheckAndLog performs an audit check, log and keep the last response
func (a *AuditChecker) CheckAndLog(ctx context.Context, log logging.Logger) {
	resp, err := a.Check(ctx)
	a.periodicResponse.Store(auditPeriodicResponse{
		AuditResponse: resp,
		Err:           err,
	})
	if err != nil {
		log.WithField("version", a.Version).WithError(err).Error("Audit check failed")
		return
	}
	if len(resp.Alerts) == 0 {
		log.WithField("version", a.Version).Debug("No alerts found on audit check")
		return
	}
	for _, alert := range resp.Alerts {
		log.WithFields(logging.Fields{
			"id":               alert.ID,
			"description":      alert.Description,
			"affected_version": alert.AffectedVersion,
			"patched_version":  alert.PatchedVersion,
		}).Warn("Audit security alert")
	}
	log.WithFields(logging.Fields{
		"alerts_len": len(resp.Alerts),
		"version":    a.Version,
	}).Warnf("Audit security - upgrade recommended: %s", resp.UpgradeURL)
}

func (a *AuditChecker) LastCheck() (*AuditResponse, error) {
	resp := a.periodicResponse.Load().(auditPeriodicResponse)
	return resp.AuditResponse, resp.Err
}

// PeriodicCheck perform one check and continue every 'interval' the check results found in the 'log'
func (a *AuditChecker) PeriodicCheck(ctx context.Context, interval time.Duration, log logging.Logger) {
	a.CheckAndLog(ctx, log)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		a.CheckAndLog(ctx, log)
	}
}
