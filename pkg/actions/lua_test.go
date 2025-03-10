package actions_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/go-chi/chi/v5"
	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func newLuaActionHook(t *testing.T, server *http.Server, address string, netHTTPEnabled bool, script string) actions.Hook {
	t.Helper()

	mockStatsCollector := NewActionStatsMockCollector()

	actionConfig := actions.Config{
		Enabled: true,
		Lua:     struct{ NetHTTPEnabled bool }{NetHTTPEnabled: netHTTPEnabled},
	}

	h, err := actions.NewLuaHook(
		actions.ActionHook{
			ID:          "hook-" + t.Name(),
			Type:        actions.HookTypeLua,
			Description: t.Name() + " hook description",
			Properties: map[string]interface{}{
				"script": script,
			},
		},
		&actions.Action{},
		actionConfig,
		server,
		address,
		&mockStatsCollector)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	return h
}

func runHook(h actions.Hook) (string, error) {
	var out bytes.Buffer

	// load a user on context
	ctx := context.Background()
	ctx = auth.WithUser(ctx, &model.User{
		CreatedAt: time.Time{},
		Username:  "user1",
	})

	// run the hook
	err := h.Run(ctx, graveler.HookRecord{
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
		CommitID:    "123456789",
		PreRunID:    "3498032432",
		TagID:       "tag1",
		MergeSource: "merge-source",
	}, &out)
	if err != nil {
		return "", err
	}
	return out.String(), nil
}

func TestNewLuaHook(t *testing.T) {
	const script = "print(tostring(1 + 2))"
	_ = newLuaActionHook(t, nil, "", true, script)
}

func TestLuaRun(t *testing.T) {
	const script = "print(tostring(350 * 239))"
	h := newLuaActionHook(t, nil, "", true, script)
	output, err := runHook(h)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	const expected = "83650"
	if !strings.Contains(output, expected) {
		t.Errorf("expected output\n%s\n------- got\n%s-------", expected, output)
	}
}

func TestLuaRun_NetHttpDisabled(t *testing.T) {
	const script = `local http = require("net/http")`
	h := newLuaActionHook(t, nil, "", false, script)
	_, err := runHook(h)
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
			h := newLuaActionHook(t, nil, "", true, tt.Script)
			output, err := runHook(h)
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
			t.Fatalf("could not load fixture %s: %v", testCase.Input, err)
		}
		script := string(data)

		t.Run(testCase.Name, func(t *testing.T) {
			h := newLuaActionHook(t, nil, "", true, script)
			output, err := runHook(h)
			if testCase.Error != "" {
				if !strings.Contains(err.Error(), testCase.Error) {
					t.Errorf("expected error to contain: '%v', got: %v", testCase.Error, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error running hook: %v", err)
			}
			if testCase.Output != "" {
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

// TestLuaRun_LakeFS tests the lakefs Lua module.
func TestLuaRun_LakeFS(t *testing.T) {
	// testing server for lakefs api, we use the request validator middleware to validate the requests
	lakeFSServer := &testLakeFSServer{}
	swagger, err := apigen.GetSwagger()
	if err != nil {
		t.Fatalf("failed to load swagger spec: %s", err)
	}
	router := chi.NewRouter()
	router.Use(api.OapiRequestValidatorWithOptions(swagger, &openapi3filter.Options{
		AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
	}))

	handler := apigen.HandlerFromMuxWithBaseURL(lakeFSServer, router, "/api/v1")
	ts := httptest.NewServer(handler)
	defer ts.Close()

	tests := []struct {
		Name            string
		Script          string
		ShouldFail      bool
		ExpectedErr     bool
		ExpectedOutput  string
		ExpectedRequest map[string]any
	}{
		{
			Name: "update_object_user_metadata",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.update_object_user_metadata("repo", "branch", "path/to/object", {key="value", key2="value2"})
print(code, resp)
`,
			ExpectedOutput: "201",
			ExpectedRequest: map[string]any{
				"repository": "repo",
				"branch":     "branch",
				"params": apigen.UpdateObjectUserMetadataParams{
					Path: "path/to/object",
				},
				"body": apigen.UpdateObjectUserMetadataJSONRequestBody{
					Set: apigen.ObjectUserMetadata{
						AdditionalProperties: map[string]string{
							"key":  "value",
							"key2": "value2",
						},
					},
				},
			},
		},
		{
			Name: "update_object_user_metadata-no_meta",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.update_object_user_metadata("repo", "branch", "object", nil)
print(code, resp)
`,
			ExpectedErr: true,
		},
		{
			Name: "update_object_user_metadata-fail",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.update_object_user_metadata("repo", "branch", "object", {key="value"})
print(code, resp)
`,
			ShouldFail:     true,
			ExpectedOutput: "400",
		},
		{
			Name: "create_tag",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.create_tag("repo", "main", "tag1")
print(code, resp.id, resp.commit_id)
`,
			ExpectedOutput: "201\ttag1\tmain",
			ExpectedRequest: map[string]any{
				"repository": "repo",
				"body":       apigen.CreateTagJSONRequestBody{Id: "tag1", Ref: "main"},
			},
		},
		{
			Name: "create_tag-fail",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.create_tag("repo", "main", "tag1")
print(code, resp)
`,
			ShouldFail:     true,
			ExpectedOutput: "400",
		},
		{
			Name: "diff_refs",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.diff_refs("repo", "main", "branch1", "after", "prefix", "delim", 100)
print(code, resp)
`,
			ExpectedOutput: "200\ttable:",
			ExpectedRequest: map[string]any{
				"repository": "repo",
				"leftRef":    "main",
				"rightRef":   "branch1",
				"params": apigen.DiffRefsParams{
					After:     apiutil.Ptr[apigen.PaginationAfter]("after"),
					Amount:    apiutil.Ptr[apigen.PaginationAmount](100),
					Prefix:    apiutil.Ptr[apigen.PaginationPrefix]("prefix"),
					Delimiter: apiutil.Ptr[apigen.PaginationDelimiter]("delim"),
				},
			},
		},
		{
			Name: "diff_refs-fail",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.diff_refs("repo", "main", "branch1")
print(code, resp)
`,
			ShouldFail:     true,
			ExpectedOutput: "400",
		},
		{
			Name: "list_objects",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.list_objects("repo", "main", "after", "prefix", "delim", 100, true)
print(code, resp)
`,
			ExpectedOutput: "200\ttable:",
			ExpectedRequest: map[string]any{
				"repository": "repo",
				"ref":        "main",
				"params": apigen.ListObjectsParams{
					After:        apiutil.Ptr[apigen.PaginationAfter]("after"),
					Amount:       apiutil.Ptr[apigen.PaginationAmount](100),
					Prefix:       apiutil.Ptr[apigen.PaginationPrefix]("prefix"),
					Delimiter:    apiutil.Ptr[apigen.PaginationDelimiter]("delim"),
					UserMetadata: apiutil.Ptr(true),
				},
			},
		},
		{
			Name: "list_objects-fail",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.list_objects("repo", "main")
print(code, resp)
`,
			ShouldFail:     true,
			ExpectedOutput: "400",
		},
		{
			Name: "get_object",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.get_object("repo", "main", "path/to/object")
print(code, resp)
`,
			ExpectedOutput: "200\tobject content",
			ExpectedRequest: map[string]any{
				"repository": "repo",
				"ref":        "main",
				"params":     apigen.GetObjectParams{Path: "path/to/object"},
			},
		},
		{
			Name: "get_object-fail",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.get_object("repo", "main", "path/to/object")
print(code, resp)
`,
			ShouldFail:     true,
			ExpectedOutput: "400",
		},
		{
			Name: "stat_object",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.stat_object("repo", "main", "path/to/object")
print(code, resp)
`,
			ExpectedOutput: "200\t" + `{"checksum":"cksum1","metadata":{"key1":"value1"},"mtime":0,"path":"path/to/object","path_type":"","physical_address":"addr1","size_bytes":100}`,
			ExpectedRequest: map[string]any{
				"repository": "repo",
				"ref":        "main",
				"params":     apigen.StatObjectParams{Path: "path/to/object"},
			},
		},
		{
			Name: "stat_object-fail",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.stat_object("repo", "main", "path/to/object")
print(code, resp)
`,
			ShouldFail:     true,
			ExpectedOutput: "400",
		},
		{
			Name: "diff_branch",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.diff_branch("repo", "branch1", "after", 100, "prefix", "delim")
print(code, resp)
`,
			ExpectedOutput: "200\ttable:",
			ExpectedRequest: map[string]any{
				"repository": "repo",
				"branch":     "branch1",
				"params": apigen.DiffBranchParams{
					After:     apiutil.Ptr[apigen.PaginationAfter]("after"),
					Amount:    apiutil.Ptr[apigen.PaginationAmount](100),
					Prefix:    apiutil.Ptr[apigen.PaginationPrefix]("prefix"),
					Delimiter: apiutil.Ptr[apigen.PaginationDelimiter]("delim"),
				},
			},
		},
		{
			Name: "diff_branch-fail",
			Script: `local lakefs = require("lakefs")
local code, resp = lakefs.diff_branch("repo", "branch1")
print(code, resp)
`,
			ShouldFail:     true,
			ExpectedOutput: "400",
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			lakeFSServer.shouldFail = tt.ShouldFail
			h := newLuaActionHook(t, ts.Config, ts.URL, true, tt.Script)
			output, err := runHook(h)

			if tt.ExpectedErr {
				if err == nil {
					t.Fatal("Expected error - got none.")
				}
				if !strings.Contains(err.Error(), tt.ExpectedOutput) {
					t.Fatalf("Error '%s' expected to contain '%s'", err.Error(), tt.ExpectedOutput)
				}
				return
			}
			if err != nil {
				t.Fatalf("Error running hook: %s", err)
			}

			// match the code and response
			if !strings.Contains(output, tt.ExpectedOutput) {
				t.Fatalf("Expected output\n%s\n------- got\n%s\n-------", tt.ExpectedOutput, output)
			}
			if diff := deep.Equal(tt.ExpectedRequest, lakeFSServer.lastRequest); diff != nil {
				t.Fatalf("Unexpected request (diff): %s", diff)
			}
		})
	}
}

// testLakeFSServer is a test server for the lakefs api.
// capture the last request and return a predefined response.
type testLakeFSServer struct {
	apigen.ServerInterface
	shouldFail  bool
	lastRequest map[string]any
}

func (s *testLakeFSServer) UpdateObjectUserMetadata(w http.ResponseWriter, _ *http.Request, body apigen.UpdateObjectUserMetadataJSONRequestBody, repository, branch string, params apigen.UpdateObjectUserMetadataParams) {
	if s.shouldFail {
		s.lastRequest = nil
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.lastRequest = map[string]any{
		"repository": repository,
		"branch":     branch,
		"params":     params,
		"body":       body,
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *testLakeFSServer) CreateTag(w http.ResponseWriter, _ *http.Request, body apigen.CreateTagJSONRequestBody, repository string) {
	if s.shouldFail {
		s.lastRequest = nil
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"test error"}`))
		return
	}
	s.lastRequest = map[string]any{
		"repository": repository,
		"body":       body,
	}
	resp := apigen.Ref{
		Id:       body.Id,
		CommitId: body.Ref,
	}
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *testLakeFSServer) DiffRefs(w http.ResponseWriter, _ *http.Request, repository, leftRef, rightRef string, params apigen.DiffRefsParams) {
	if s.shouldFail {
		s.lastRequest = nil
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"test error"}`))
		return
	}
	s.lastRequest = map[string]any{
		"repository": repository,
		"leftRef":    leftRef,
		"rightRef":   rightRef,
		"params":     params,
	}
	resp := apigen.DiffList{
		Pagination: apigen.Pagination{
			HasMore:    false,
			MaxPerPage: 100,
			Results:    1,
		},
		Results: []apigen.Diff{
			{
				Path: "file1",
				Type: "changed",
			},
		},
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *testLakeFSServer) ListObjects(w http.ResponseWriter, _ *http.Request, repository, ref string, params apigen.ListObjectsParams) {
	if s.shouldFail {
		s.lastRequest = nil
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"test error"}`))
		return
	}
	s.lastRequest = map[string]any{
		"repository": repository,
		"ref":        ref,
		"params":     params,
	}
	resp := apigen.ObjectStatsList{
		Pagination: apigen.Pagination{
			HasMore:    false,
			MaxPerPage: 100,
			Results:    1,
		},
		Results: []apigen.ObjectStats{
			{
				Path:            "file1",
				PhysicalAddress: "addr1",
				Checksum:        "cksum1",
				SizeBytes:       apiutil.Ptr(int64(100)),
				Metadata: &apigen.ObjectUserMetadata{
					AdditionalProperties: map[string]string{
						"key1": "value1",
					},
				},
			},
		},
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *testLakeFSServer) GetObject(w http.ResponseWriter, _ *http.Request, repository, ref string, params apigen.GetObjectParams) {
	if s.shouldFail {
		s.lastRequest = nil
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.lastRequest = map[string]any{
		"repository": repository,
		"ref":        ref,
		"params":     params,
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("object content"))
}

func (s *testLakeFSServer) StatObject(w http.ResponseWriter, _ *http.Request, repository, ref string, params apigen.StatObjectParams) {
	if s.shouldFail {
		s.lastRequest = nil
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.lastRequest = map[string]any{
		"repository": repository,
		"ref":        ref,
		"params":     params,
	}
	resp := apigen.ObjectStats{
		Path:            params.Path,
		PhysicalAddress: "addr1",
		Checksum:        "cksum1",
		SizeBytes:       apiutil.Ptr(int64(100)),
		Metadata: &apigen.ObjectUserMetadata{
			AdditionalProperties: map[string]string{
				"key1": "value1",
			},
		},
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *testLakeFSServer) DiffBranch(w http.ResponseWriter, _ *http.Request, repository, branch string, params apigen.DiffBranchParams) {
	if s.shouldFail {
		s.lastRequest = nil
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"test error"}`))
		return
	}
	s.lastRequest = map[string]any{
		"repository": repository,
		"branch":     branch,
		"params":     params,
	}
	resp := apigen.DiffList{
		Pagination: apigen.Pagination{
			HasMore:    false,
			MaxPerPage: 100,
			NextOffset: "",
			Results:    1,
		},
		Results: []apigen.Diff{
			{
				Path:      "file1",
				PathType:  "object",
				Type:      "changed",
				SizeBytes: apiutil.Ptr(int64(200)),
			},
		},
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
