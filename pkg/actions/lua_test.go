package actions_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/testutil"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"

	"github.com/treeverse/lakefs/pkg/graveler"

	"github.com/treeverse/lakefs/pkg/actions"
)

func TestNewLuaHook(t *testing.T) {
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
		nil)
	if err != nil {
		t.Errorf("unexpedcted error: %v", err)
	}
}

func TestLuaRun(t *testing.T) {
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
		nil)
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
		RunID:            "abc123",
		EventType:        graveler.EventTypePreCreateBranch,
		RepositoryID:     "example123",
		StorageNamespace: "local://foo/bar",
		SourceRef:        "abc123",
		BranchID:         "my-branch",
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
	}

	for _, testCase := range tests {
		// read input
		data, err := os.ReadFile(testCase.Input)
		if err != nil {
			t.Errorf("could not load fixture %s: %v", testCase.Input, err)
		}
		script := string(data)

		t.Run(testCase.Name, func(t *testing.T) {
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
				nil)
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
				RunID:            "abc123",
				EventType:        graveler.EventTypePreCreateBranch,
				RepositoryID:     "example123",
				StorageNamespace: "local://foo/bar",
				SourceRef:        "abc123",
				BranchID:         "my-branch",
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
		out, err := actions.DescendArgs(v)
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
		_, err := actions.DescendArgs(v)
		if err == nil {
			t.Fatalf("expected error!")
		}
	})
}
