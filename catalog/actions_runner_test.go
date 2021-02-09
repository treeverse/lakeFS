package catalog_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-test/deep"

	"github.com/treeverse/lakefs/block/mem"
	"gopkg.in/yaml.v3"

	"github.com/treeverse/lakefs/catalog"
)

func TestActionsRunner(t *testing.T) {
	const actionsTmpl = `
name: action1
on:
  pre-commit:
hooks:
  - id: hook1
    type: webhook
    properties:
      url: @hook_base_url@/action1/hook1
  - id: hook2
    type: webhook
    properties:
      url: @hook_base_url@/action1/hook2
====
name: action2
on:
  pre-merge:
hooks:
  - id: hook3
    type: webhook
    properties:
      url: @hook_base_url@/action2/hook3
  - id: hook4
    type: webhook
    properties:
      url: @hook_base_url@/action2/hook4
`
	var postedData []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error("Failed to read posted data", err)
		} else {
			postedData = append(postedData, string(data))
		}
		_, _ = fmt.Fprintln(w, r.URL.Path)
	}))
	actionsData := strings.Split(strings.ReplaceAll(actionsTmpl, "@hook_base_url@", ts.URL), "====\n")
	var actions []catalog.Action
	for _, data := range actionsData {
		var action catalog.Action
		err := yaml.Unmarshal([]byte(data), &action)
		if err != nil {
			t.Fatalf("Failed to parse Action: %s", err)
		}
		actions = append(actions, action)
	}
	defer ts.Close()
	const expectedActionsLen = 2
	if len(actions) != expectedActionsLen {
		t.Fatalf("Actions len=%d, expected=%d", len(actions), expectedActionsLen)
	}

	memAdapter := mem.New()
	eventData := catalog.ActionEventData{
		RunID:        "run_id",
		EventType:    "event_type",
		EventTime:    "event_time",
		ActionName:   "action_name",
		HookID:       "hook_id",
		RepositoryID: "repo_id",
		BranchID:     "branch_id",
		SourceRef:    "source_ref",
		Commit: catalog.ActionCommitData{
			Message:   "commit_message",
			Committer: "commit_committer",
			Metadata:  map[string]string{"key1": "value1"},
		},
	}
	const storageLocation = "_lakefs/actions/output"
	runner := catalog.NewActionsRunner(memAdapter, "mem://", storageLocation, actions, eventData)
	err := runner.Run(context.Background())
	if err != nil {
		t.Fatal("Failed to Run actions", err)
	}

	// verify results
	const expectedResultsLen = 4
	if len(runner.Results) != expectedResultsLen {
		t.Fatalf("Run results len=%d, expected=%d", len(runner.Results), expectedResultsLen)
	}
	for i, result := range runner.Results {
		if result.StatusCode != http.StatusOK {
			t.Errorf("Result status code=%d, expected OK", result.StatusCode)
		}
		if result.Error != nil {
			t.Errorf("Result error=%v expected nil", result.Error)
		}
		var act *catalog.Action
		if len(actions) == 2 {
			if i < 2 {
				act = &actions[0]
			} else {
				act = &actions[1]
			}
		}
		if diff := deep.Equal(result.Action, act); diff != nil {
			t.Error("Result action diff", diff)
		}
	}
	// verify posted data to webhook
	if len(postedData) != expectedResultsLen {
		t.Fatalf("Posted data len=%d, expected=%d", len(postedData), expectedResultsLen)
	}
	var expectedPostedData string
	if data, err := json.Marshal(eventData); err != nil {
		t.Fatal("Marshal action event data", err)
	} else {
		expectedPostedData = string(data)
	}
	for i, data := range postedData {
		if data != expectedPostedData {
			t.Errorf("Posted data[%d] '%s', expected '%s'", i, data, expectedPostedData)
		}
	}
}
