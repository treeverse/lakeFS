package actions_test

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/httputil"
)

func TestWebhook_AllowedHosts(t *testing.T) {
	tests := []struct {
		name         string
		allowedHosts []string
		viaRedirect  bool  // request the redirect server as "localhost"; it redirects to the target at 127.0.0.1
		expectedHits int   // requests reaching either test server
		expectedErr  error // nil when the request is allowed
	}{
		{name: "internal_blocked_by_default", expectedErr: httputil.ErrHostNotAllowed},
		{name: "internal_allowed_ip", allowedHosts: []string{"127.0.0.1"}, expectedHits: 1},
		{name: "internal_allowed_cidr", allowedHosts: []string{"127.0.0.0/8"}, expectedHits: 1},
		{name: "other_internal_blocked", allowedHosts: []string{"10.0.0.0/8"}, expectedErr: httputil.ErrHostNotAllowed},
		{name: "host_name_blocked", viaRedirect: true, expectedErr: httputil.ErrHostNotAllowed},
		{name: "redirect_allowed", viaRedirect: true, allowedHosts: []string{"localhost", "127.0.0.1"}, expectedHits: 2},
		{name: "redirect_blocked", viaRedirect: true, allowedHosts: []string{"localhost"}, expectedHits: 1, expectedErr: httputil.ErrHostNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hits := 0
			target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				hits++
			}))
			defer target.Close()
			redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits++
				http.Redirect(w, r, target.URL, http.StatusFound)
			}))
			defer redirect.Close()

			url := target.URL
			if tt.viaRedirect {
				url = strings.Replace(redirect.URL, "127.0.0.1", "localhost", 1)
			}
			cfg := actions.Config{Enabled: true}
			cfg.Network.AllowedHosts = tt.allowedHosts
			h, err := actions.NewWebhook(actions.ActionHook{
				ID:         "webhook",
				Type:       actions.HookTypeWebhook,
				Properties: map[string]any{"url": url},
			}, &actions.Action{Name: "action"}, cfg, nil, "", nil)
			if err != nil {
				t.Fatalf("NewWebhook: %s", err)
			}

			var buf bytes.Buffer
			err = h.Run(t.Context(), graveler.HookRecord{
				EventType:  graveler.EventTypePreCommit,
				Repository: &graveler.RepositoryRecord{Repository: &graveler.Repository{}},
			}, &buf)
			if !errors.Is(err, tt.expectedErr) {
				t.Errorf("Run() error = %v, expected %v", err, tt.expectedErr)
			}
			if hits != tt.expectedHits {
				t.Errorf("got %d hits, expected %d", hits, tt.expectedHits)
			}
		})
	}
}
