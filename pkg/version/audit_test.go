package version

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-test/deep"
	"github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/pkg/logging"
)

type LogLine struct {
	Fields logging.Fields
	Level  string
	Msg    string
}
type MemLogger struct {
	log    []*LogLine
	fields logging.Fields
}

func (m *MemLogger) WithContext(context.Context) logging.Logger {
	return m
}

func (m *MemLogger) WithField(key string, value interface{}) logging.Logger {
	if m.fields == nil {
		m.fields = make(logging.Fields)
	}
	m.fields[key] = value
	return m
}

func (m *MemLogger) WithFields(fields logging.Fields) logging.Logger {
	if m.fields == nil {
		m.fields = make(logging.Fields)
	}
	for k, v := range fields {
		m.fields[k] = v
	}
	return m
}

func (m *MemLogger) WithError(err error) logging.Logger {
	return m.WithField("err", err)
}

func (m *MemLogger) Trace(args ...interface{}) {
	m.logLine("TRACE", args...)
}

func (m *MemLogger) Debug(args ...interface{}) {
	m.logLine("DEBUG", args...)
}

func (m *MemLogger) Info(args ...interface{}) {
	m.logLine("INFO", args...)
}

func (m *MemLogger) Warn(args ...interface{}) {
	m.logLine("WARN", args...)
}

func (m *MemLogger) Warning(args ...interface{}) {
	m.logLine("WARN", args...)
}

func (m *MemLogger) Error(args ...interface{}) {
	m.logLine("ERROR", args...)
}

func (m *MemLogger) Fatal(args ...interface{}) {
	m.logLine("FATAL", args...)
}

func (m *MemLogger) Panic(args ...interface{}) {
	m.logLine("PANIC", args...)
}

func (m *MemLogger) Log(level logrus.Level, args ...interface{}) {
	m.logLine(level.String(), args...)
}

func (m *MemLogger) Tracef(format string, args ...interface{}) {
	m.logLine("TRACE", fmt.Sprintf(format, args...))
}

func (m *MemLogger) Debugf(format string, args ...interface{}) {
	m.logLine("DEBUG", fmt.Sprintf(format, args...))
}

func (m *MemLogger) Infof(format string, args ...interface{}) {
	m.logLine("INFO", fmt.Sprintf(format, args...))
}

func (m *MemLogger) Warnf(format string, args ...interface{}) {
	m.logLine("WARN", fmt.Sprintf(format, args...))
}

func (m *MemLogger) Warningf(format string, args ...interface{}) {
	m.logLine("WARN", fmt.Sprintf(format, args...))
}

func (m *MemLogger) Errorf(format string, args ...interface{}) {
	m.logLine("ERROR", fmt.Sprintf(format, args...))
}

func (m *MemLogger) Fatalf(format string, args ...interface{}) {
	m.logLine("FATAL", fmt.Sprintf(format, args...))
}

func (m *MemLogger) Panicf(format string, args ...interface{}) {
	m.logLine("PANIC", fmt.Sprintf(format, args...))
}

func (m *MemLogger) Logf(level logrus.Level, format string, args ...interface{}) {
	m.logLine(level.String(), fmt.Sprintf(format, args...))
}

func (m *MemLogger) IsTracing() bool {
	return true
}

func (m *MemLogger) logLine(level string, args ...interface{}) {
	m.log = append(m.log, &LogLine{
		Fields: m.fields,
		Level:  level,
		Msg:    fmt.Sprint(args...),
	})
	m.fields = nil
}

func TestAuditChecker_PassVersionOnRequest(t *testing.T) {
	var requestedVersion string
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedVersion = r.FormValue("version")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer svr.Close()

	ctx := context.Background()
	installationID := "a-sample-installation-id"
	for _, version := range []string{"v1", "v1.2", "v2.0.1"} {
		t.Run(version, func(t *testing.T) {
			auditChecker := NewAuditChecker(svr.URL, version, installationID)
			_, err := auditChecker.Check(ctx)
			if err != nil {
				t.Errorf("Check() error = %v", err)
				return
			}
			if requestedVersion != version {
				t.Errorf("Check() callhome got '%s', expected '%s' as version", requestedVersion, version)
			}
		})
	}
}

func TestAuditChecker_Check(t *testing.T) {
	const upgradeURL = "https://no.place.like/home"
	var (
		responseAlerts     []Alert
		responseStatusCode int
	)
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := AuditResponse{
			UpgradeURL: upgradeURL,
			Alerts:     responseAlerts,
		}
		if responseStatusCode != 0 {
			w.WriteHeader(responseStatusCode)
			return
		}
		err := json.NewEncoder(w).Encode(response)
		if err != nil {
			t.Fatal("Failed to encode response from audit checker service", err)
		}
	}))
	defer svr.Close()

	tests := []struct {
		name       string
		alerts     []Alert
		statusCode int
		wantErr    bool
	}{
		{name: "none", alerts: []Alert{}},
		{name: "alerts", alerts: []Alert{
			{ID: "1", AffectedVersions: ">v1,<v2", PatchedVersions: "v2", Description: "bad1"},
			{ID: "2", AffectedVersions: ">v1,<v1.0.5||>v2<v2.2.0", PatchedVersions: "v1.0.5,v.2.1.1", Description: "bad2"},
		}},
		{name: "failed", alerts: []Alert{}, statusCode: http.StatusInternalServerError, wantErr: true},
	}
	ctx := context.Background()
	installationID := "a-sample-installation-id"
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAuditChecker(svr.URL, "v1", installationID)
			responseAlerts = tt.alerts
			responseStatusCode = tt.statusCode
			got, err := a.Check(ctx)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Check() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			if got.UpgradeURL != upgradeURL {
				t.Errorf("Check() upgrade URL: %s, expected %s", got.UpgradeURL, upgradeURL)
			}
			if diff := deep.Equal(got.Alerts, tt.alerts); diff != nil {
				t.Error("Check() found difference in expected alerts", diff)
			}
		})
	}
}

func TestAuditChecker_InstallationID(t *testing.T) {
	const upgradeURL = "https://no.place.like/home"

	tests := []struct {
		name               string
		installationID     string
		respInstallationID string
	}{
		{name: "none", installationID: "", respInstallationID: ""},
		{name: "installation_id", installationID: "a-sample-installation-id", respInstallationID: "a-sample-installation-id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAuditChecker(upgradeURL, "v1", tt.installationID)

			if tt.respInstallationID != a.InstallationID {
				t.Errorf("Check() installation ID: %s, expected: %s", a.InstallationID, tt.respInstallationID)
			}
		})
	}
}

func TestAuditChecker_CheckAndLog(t *testing.T) {
	const upgradeURL = "https://no.place.like/home"
	responseAlert := Alert{
		ID:               "1",
		AffectedVersions: "v2",
		PatchedVersions:  "v2.1",
		Description:      "alert",
	}
	responseAlerts := []Alert{
		responseAlert,
	}
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := AuditResponse{
			UpgradeURL: upgradeURL,
			Alerts:     responseAlerts,
		}
		err := json.NewEncoder(w).Encode(response)
		if err != nil {
			t.Fatal("Failed to encode response from audit checker service", err)
		}
	}))
	defer svr.Close()

	installationID := "a-sample-installation-id"
	checker := NewAuditChecker(svr.URL, "v1.0", installationID)
	ctx := context.Background()
	memLog := &MemLogger{}
	checker.CheckAndLog(ctx, memLog)

	// verify we logged the right information
	if diff := deep.Equal(memLog.log[0], &LogLine{
		Fields: logging.Fields{
			"id":                responseAlert.ID,
			"affected_versions": responseAlert.AffectedVersions,
			"patched_versions":  responseAlert.PatchedVersions,
			"description":       responseAlert.Description,
		},
		Level: "WARN",
		Msg:   "Audit security alert",
	}); diff != nil {
		t.Fatal("CheckAndLog first line diff", diff)
	}

	// verify the last check information
	lastResponse, lastErr := checker.LastCheck()
	if lastErr != nil {
		t.Fatal("LastCheck() should not return an error, got=", lastErr)
	}
	if lastResponse.UpgradeURL != upgradeURL {
		t.Errorf("LastCheck() upgrade URL=%s, expected %s", lastResponse.UpgradeURL, upgradeURL)
	}
	if diff := deep.Equal(lastResponse.Alerts, responseAlerts); diff != nil {
		t.Error("LastCheck() alerts diff found", diff)
	}
}
