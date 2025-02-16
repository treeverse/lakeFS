package actions_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestNewLuaHook(t *testing.T) {
	mockStatsCollector := NewActionStatsMockCollector()
	_, err := actions.NewLuaHook(
		actions.ActionHook{
			ID:          "myHook",
			Type:        actions.HookTypeLua,
			Description: "na",
			Properties: map[string]interface{}{
				"script": "print(tostring(1 + 2))",
			},
		},
		&actions.Action{
			Name:        "",
			Description: "",
			On:          nil,
			Hooks:       nil,
		},
		actions.Config{
			Enabled: true,
			Lua: struct {
				NetHTTPEnabled bool
			}{
				NetHTTPEnabled: true,
			},
		},
		nil, "", &mockStatsCollector)
	if err != nil {
		t.Errorf("unexpedcted error: %v", err)
	}
}

func TestLuaRun(t *testing.T) {
	mockStatsCollector := NewActionStatsMockCollector()
	h, err := actions.NewLuaHook(
		actions.ActionHook{
			ID:          "myHook",
			Type:        actions.HookTypeLua,
			Description: "na",
			Properties: map[string]interface{}{
				"script": "print(tostring(350 * 239))",
			},
		},
		&actions.Action{
			Name:        "",
			Description: "",
			On:          nil,
			Hooks:       nil,
		},
		actions.Config{
			Enabled: true,
			Lua: struct {
				NetHTTPEnabled bool
			}{
				NetHTTPEnabled: true,
			},
		},
		nil, "", &mockStatsCollector)
	if err != nil {
		t.Errorf("unexpedcted error: %v", err)
	}
	out := &bytes.Buffer{}
	// load a user
	ctx := context.Background()
	ctx = auth.WithUser(ctx, &model.User{
		CreatedAt: time.Time{},
		Username:  "user1",
	})
	err = h.Run(ctx, graveler.HookRecord{
		RunID:     "abc123",
		EventType: graveler.EventTypePreCreateBranch,
		Repository: &graveler.RepositoryRecord{
			RepositoryID: "example123",
			Repository: &graveler.Repository{
				StorageNamespace: "local://foo/bar",
				CreationDate:     time.Time{},
			},
		},
		SourceRef: "abc123",
		BranchID:  "my-branch",
		Commit: graveler.Commit{
			Version: 1,
		},
		CommitID: "123456789",
		PreRunID: "3498032432",
		TagID:    "",
	}, out)
	if err != nil {
		t.Errorf("unexpected error running hook: %v", err)
	}
	output := out.String()
	expected := "83650"
	if !strings.Contains(output, expected) {
		t.Errorf("expected output\n%s\n------- got\n%s-------", expected, output)
	}
}

func TestLuaRun_NetHttpDisabled(t *testing.T) {
	mockStatsCollector := NewActionStatsMockCollector()
	h, err := actions.NewLuaHook(
		actions.ActionHook{
			ID:          "myHook",
			Type:        actions.HookTypeLua,
			Description: "na",
			Properties: map[string]interface{}{
				"script": `local http = require("net/http")`,
			},
		},
		&actions.Action{
			Name:        "",
			Description: "",
			On:          nil,
			Hooks:       nil,
		},
		actions.Config{Enabled: true},
		nil, "", &mockStatsCollector)
	if err != nil {
		t.Errorf("unexpedcted error: %v", err)
	}
	out := &bytes.Buffer{}
	ctx := context.Background()
	ctx = auth.WithUser(ctx, &model.User{
		CreatedAt: time.Time{},
		Username:  "user1",
	})
	err = h.Run(ctx, graveler.HookRecord{
		RunID:     "abc123",
		EventType: graveler.EventTypePreCreateBranch,
		Repository: &graveler.RepositoryRecord{
			RepositoryID: "example123",
			Repository: &graveler.Repository{
				StorageNamespace: "local://foo/bar",
				CreationDate:     time.Time{},
			},
		},
		SourceRef: "abc123",
		BranchID:  "my-branch",
		Commit: graveler.Commit{
			Version: 1,
		},
		CommitID: "123456789",
		PreRunID: "3498032432",
		TagID:    "",
	}, out)
	const expectedErr = "module 'net/http' not found"
	if err == nil || !strings.Contains(err.Error(), expectedErr) {
		t.Fatalf("Error=%v, expected: '%s'", err, expectedErr)
	}
}

func TestLuaRun_NetHttp(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		codeVal := r.URL.Query().Get("code")
		statusCode, _ := strconv.Atoi(codeVal)
		if statusCode != 0 {
			w.WriteHeader(statusCode)
		}
		_, _ = fmt.Fprint(w, "hello-"+r.Method)
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			_, _ = w.Write([]byte{' '})
			_, _ = w.Write(body)
		}
	}))
	defer ts.Close()

	tests := []struct {
		Name        string
		Script      string
		ExpectedErr bool
		Expected    string
	}{
		{
			Name: "simple_get",
			Script: `local http = require("net/http")
local code, body, headers, status = http.request("` + ts.URL + `")
print(code .. " " .. body .. " " .. status)
`,
			Expected: "200 hello-GET 200 OK",
		},
		{
			Name: "invalid_address",
			Script: `local http = require("net/http")
local code, body, headers, status = http.request("https://invalid.place.com")
print(code .. " " .. body .. " " .. status)
`,
			ExpectedErr: true,
			Expected:    "no such host",
		},
		{
			Name: "simple_post",
			Script: `local http = require("net/http")
local code, body, headers, status = http.request("` + ts.URL + `", "name=value")
print(code .. " " .. body .. " " .. status)
`,
			Expected: "200 hello-POST name=value 200 OK",
		},
		{
			Name: "simple_get_404",
			Script: `local http = require("net/http")
local code, body, headers, status = http.request("` + ts.URL + `/?code=404")
print(code .. " " .. body .. " " .. status)
`,
			Expected: "404 hello-GET 404 Not Found",
		},
		{
			Name: "table_get",
			Script: `local http = require("net/http")
local code, body, headers, status = http.request{
	url="` + ts.URL + `",
	method="GET",
}
print(code .. " " .. body .. " " .. status)
`,
			Expected: "200 hello-GET 200 OK",
		},
		{
			Name: "table_post",
			Script: `local http = require("net/http")
local code, body, headers, status = http.request{
	url="` + ts.URL + `",
	body="name=value",
}
print(code .. " " .. body .. " " .. status)
`,
			Expected: "200 hello-POST name=value 200 OK",
		},
		{
			Name: "table_post_method",
			Script: `local http = require("net/http")
local code, body, headers, status = http.request{
	url="` + ts.URL + `",
	method="POST",
	body="name=value",
}
print(code .. " " .. body .. " " .. status)
`,
			Expected: "200 hello-POST name=value 200 OK",
		},
		{
			Name: "table_get_404",
			Script: `local http = require("net/http")
local code, body, headers, status = http.request{
	url="` + ts.URL + `/?code=404",
}
print(code .. " " .. body .. " " .. status)
`,
			Expected: "404 hello-GET 404 Not Found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			mockStatsCollector := NewActionStatsMockCollector()
			h, err := actions.NewLuaHook(
				actions.ActionHook{
					ID:   "myLuaHook",
					Type: actions.HookTypeLua,
					Properties: map[string]interface{}{
						"script": tt.Script,
					},
				},
				&actions.Action{},
				actions.Config{
					Enabled: true,
					Lua: struct {
						NetHTTPEnabled bool
					}{
						NetHTTPEnabled: true,
					},
				},
				nil, "", &mockStatsCollector)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			out := &bytes.Buffer{}
			ctx := auth.WithUser(context.Background(), &model.User{
				Username: "user1",
			})
			runID := nanoid.Must(20)
			err = h.Run(ctx, graveler.HookRecord{
				RunID:     runID,
				EventType: graveler.EventTypePreCreateBranch,
				Repository: &graveler.RepositoryRecord{
					RepositoryID: "example123",
					Repository: &graveler.Repository{
						StorageNamespace: "local://foo/bar",
						CreationDate:     time.Time{},
					},
				},
				SourceRef: "abc123",
				BranchID:  "my-branch",
				Commit: graveler.Commit{
					Version: 1,
				},
				CommitID: "123456789",
				PreRunID: "3498032432",
				TagID:    "",
			}, out)
			if tt.ExpectedErr {
				if err == nil {
					t.Fatal("Expected error - got none.")
				}
				if !strings.Contains(err.Error(), tt.Expected) {
					t.Fatalf("Error '%s' expected to contain '%s'", err.Error(), tt.Expected)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error running hook: %v", err)
			}
			output := out.String()
			if !strings.Contains(output, tt.Expected) {
				t.Fatalf("expected output\n%s\n------- got\n%s-------", tt.Expected, output)
			}
		})
	}
}

func TestLuaRunTable(t *testing.T) {
	tests := []struct {
		Name   string
		Input  string
		Output string
		Error  string
	}{
		{
			Name:   "simple_math",
			Input:  "testdata/lua/simple_math.lua",
			Output: "testdata/lua/simple_math.output",
		},
		{
			Name:  "ensure_no_io",
			Input: "testdata/lua/no_io.lua",
			Error: "module 'io' not found",
		},
		{
			Name:  "ensure_no_loadfile",
			Input: "testdata/lua/loadfile.lua",
			Error: "attempt to call a nil value",
		},
		{
			Name:   "user_injected",
			Input:  "testdata/lua/print_user.lua",
			Output: "testdata/lua/print_user.output",
		},
		{
			Name:   "json_dump_action",
			Input:  "testdata/lua/json_marshal_action.lua",
			Output: "testdata/lua/json_marshal_action.output",
		},
		{
			Name:   "strings_partition",
			Input:  "testdata/lua/strings_partition.lua",
			Output: "testdata/lua/strings_partition.output",
		},
		{
			Name:   "catalogexport_hive_partition_pager",
			Input:  "testdata/lua/catalogexport_hive_partition_pager.lua",
			Output: "testdata/lua/catalogexport_hive_partition_pager.output",
		},
		{
			Name:  "catalogexport_delta",
			Input: "testdata/lua/catalogexport_delta.lua",
		},
		{
			Name:  "catalogexport_unity",
			Input: "testdata/lua/catalogexport_unity.lua",
		},
	}

	for _, testCase := range tests {
		// read input
		data, err := os.ReadFile(testCase.Input)
		if err != nil {
			t.Errorf("could not load fixture %s: %v", testCase.Input, err)
		}
		script := string(data)

		t.Run(testCase.Name, func(t *testing.T) {
			mockStatsCollector := NewActionStatsMockCollector()
			h, err := actions.NewLuaHook(
				actions.ActionHook{
					ID:   "myHook",
					Type: actions.HookTypeLua,
					Properties: map[string]interface{}{
						"script": script,
					},
				},
				&actions.Action{
					Name:        "",
					Description: "",
					On:          nil,
					Hooks:       nil,
				},
				actions.Config{
					Enabled: true,
					Lua: struct {
						NetHTTPEnabled bool
					}{
						NetHTTPEnabled: true,
					},
				},
				nil, "", &mockStatsCollector)
			if err != nil {
				t.Errorf("unexpedcted error: %v", err)
			}
			out := &bytes.Buffer{}
			// load a user
			ctx := context.Background()
			ctx = auth.WithUser(ctx, &model.User{
				CreatedAt: time.Time{},
				Username:  "user1",
			})
			err = h.Run(ctx, graveler.HookRecord{
				RunID:     "abc123",
				EventType: graveler.EventTypePreCreateBranch,
				Repository: &graveler.RepositoryRecord{
					RepositoryID: "example123",
					Repository: &graveler.Repository{
						StorageNamespace: "local://foo/bar",
						CreationDate:     time.Time{},
					},
				},
				SourceRef: "abc123",
				BranchID:  "my-branch",
				Commit: graveler.Commit{
					Version: 1,
				},
				CommitID: "123456789",
				PreRunID: "3498032432",
				TagID:    "",
			}, out)
			if testCase.Error != "" {
				if !strings.Contains(err.Error(), testCase.Error) {
					t.Errorf("expected error to contain: '%v', got: %v", testCase.Error, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error running hook: %v", err)
			}
			if testCase.Output != "" {
				output := out.String()
				expectedOutBytes, err := os.ReadFile(testCase.Output)
				if err != nil {
					t.Errorf("could not load fixture %s: %v", testCase.Output, err)
				}
				expectedOut := string(expectedOutBytes)
				if !strings.Contains(output, expectedOut) {
					t.Errorf("expected output\n%s\n------- got\n%s-------", expectedOut, output)
				}
			}
		})
	}
}

func TestDescendArgs(t *testing.T) {
	t.Run("valid secrets", func(t *testing.T) {
		testutil.WithEnvironmentVariable(t, "magic_environ123123", "magic_environ_value")
		v := map[string]interface{}{
			"key":              "value",
			"secure_key":       "value with {{ ENV.magic_environ123123 }}",
			"slice_of_strings": []string{"a", "{{ENV.magic_environ123123}}", "c"},
			"map_of_things": map[string]interface{}{
				"a":        1,
				"b":        false,
				"c":        "hello",
				"secure_d": "{{ ENV.magic_environ123123 }}",
				"e":        []interface{}{"a", 1, false, "{{ ENV.magic_environ123123 }}"},
			},
		}
		envGetter := actions.NewEnvironmentVariableGetter(true, "")
		out, err := actions.DescendArgs(v, envGetter)
		if err != nil {
			t.Fatalf("unexpected err: %s", err)
		}
		outParsed, ok := out.(map[string]interface{})
		if !ok {
			t.Fatalf("expected map[string]interface{}, got a %T", outParsed)
		}
		m, ok := outParsed["map_of_things"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected map[string]interface{}, got a %T", m)
		}
		secureString, ok := m["secure_d"].(string)
		if !ok {
			t.Fatalf("expected a string, got a %T", m["secure_d"])
		}
		if secureString != "magic_environ_value" {
			t.Fatalf("expected %s got %s", "magic_environ_value", v)
		}
		_, isBool := m["b"].(bool)
		if !isBool {
			t.Fatalf("expected  a bool, got a %T", m["b"])
		}
	})

	t.Run("invalid param", func(t *testing.T) {
		testutil.WithEnvironmentVariable(t, "magic_environ123123", "magic_environ_value")
		v := map[string]interface{}{
			"key":              "value",
			"secure_key":       "value with {{ ENV.magic_environ123123 }}",
			"slice_of_strings": []string{"a", "{{ENV.magic_environ123123}}", "c"},
			"map_of_things": map[string]interface{}{
				"a":        1,
				"b":        false,
				"c":        "hello",
				"secure_d": "{{ ENV.magic_environ123123 }}",
				"e":        []interface{}{"a", 1, false, "{{ ENV.magic_environ123123456 }}"}, // <- shouldn't exist?
			},
		}

		envGetter := actions.NewEnvironmentVariableGetter(true, "")
		_, err := actions.DescendArgs(v, envGetter)
		if err == nil {
			t.Fatalf("expected error!")
		}
	})

	t.Run("env_disabled", func(t *testing.T) {
		testutil.WithEnvironmentVariable(t, "magic_environ123123", "magic_environ_value")
		v := map[string]interface{}{
			"key":        "value",
			"secure_key": "value with {{ ENV.magic_environ123123 }}",
		}
		envGetter := actions.NewEnvironmentVariableGetter(false, "")
		args, err := actions.DescendArgs(v, envGetter)
		if err != nil {
			t.Fatalf("DescendArgs failed: %s", err)
		}
		argsMap, ok := args.(map[string]interface{})
		if !ok {
			t.Fatalf("expected map[string]interface{}, got a %T", argsMap)
		}
		secureString, ok := argsMap["secure_key"].(string)
		if !ok {
			t.Fatalf("expected a string, got a %T", argsMap["secure_key"])
		}
		const expectedValue = "value with "
		if secureString != expectedValue {
			t.Fatalf("expected '%s' as value from env when env is disabled, got '%s'", expectedValue, secureString)
		}
	})

	t.Run("env_prefix", func(t *testing.T) {
		testutil.WithEnvironmentVariable(t, "magic_environ123123", "magic_environ_value")
		v := map[string]interface{}{
			"key":        "value{{ ENV.no_magic_environ123123 }}",
			"secure_key": "value with {{ ENV.magic_environ123123 }}",
		}
		envGetter := actions.NewEnvironmentVariableGetter(true, "magic_")
		args, err := actions.DescendArgs(v, envGetter)
		if err != nil {
			t.Fatalf("DescendArgs failed: %s", err)
		}
		argsMap, ok := args.(map[string]interface{})
		if !ok {
			t.Fatalf("expected map[string]interface{}, got a %T", argsMap)
		}

		// verify that we have value access to keys with the prefix
		secureString, ok := argsMap["secure_key"].(string)
		if !ok {
			t.Fatalf("expected a string, got a %T", argsMap["secure_key"])
		}
		if secureString != "value with magic_environ_value" {
			t.Fatalf("expected magic environ value, got '%s'", secureString)
		}

		// verify that we don't have value access to keys without the prefix
		secureString, ok = argsMap["key"].(string)
		if !ok {
			t.Fatalf("expected a string, got a %T", argsMap["key"])
		}
		if secureString != "value" {
			t.Fatalf("expected just value for 'key', got '%s'", secureString)
		}
	})
}
