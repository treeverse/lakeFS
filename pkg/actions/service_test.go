package actions_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/actions/mock"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	"github.com/treeverse/lakefs/pkg/stats"
)

const (
	testActionName          = "test action"
	testWebhookID           = "webhook_id"
	testAirflowHookID       = "airflow_hook_id"
	testAirflowHookIDNoConf = "airflow_hook_id_no_conf"
)

type ActionStatsMockCollector struct {
	Hits map[string]uint64
}

func NewActionStatsMockCollector() ActionStatsMockCollector {
	return ActionStatsMockCollector{
		Hits: make(map[string]uint64),
	}
}

func (c *ActionStatsMockCollector) CollectEvent(ev stats.Event) {
	c.CollectEvents(ev, 1)
}

func (c *ActionStatsMockCollector) CollectEvents(ev stats.Event, count uint64) {
	c.Hits[ev.Name] += count
}

func (c *ActionStatsMockCollector) CollectMetadata(_ *stats.Metadata)  {}
func (c *ActionStatsMockCollector) SetInstallationID(_ string)         {}
func (c *ActionStatsMockCollector) CollectCommPrefs(_ stats.CommPrefs) {}
func (c *ActionStatsMockCollector) Close()                             {}

type getService func(t *testing.T, ctx context.Context, source actions.Source, writer actions.OutputWriter, stats stats.Collector, runHooks bool) actions.Service

func GetKVService(t *testing.T, ctx context.Context, source actions.Source, writer actions.OutputWriter, stats stats.Collector, runHooks bool) actions.Service {
	t.Helper()
	kvStore := kvtest.GetStore(ctx, t)
	cfg := actions.Config{Enabled: runHooks}
	cfg.Lua.NetHTTPEnabled = true
	cfg.Env.Enabled = true
	return actions.NewService(ctx, actions.NewActionsKVStore(kvStore), source, writer, &actions.DecreasingIDGenerator{}, stats, cfg, "")
}

func TestServiceRun(t *testing.T) {
	ctx := context.Background()

	var lastManifest *actions.RunManifest
	var writerBytes []byte

	expectedWebhookRunID := actions.NewHookRunID(0, 0)
	expectedAirflowHookRunIDWithConf := actions.NewHookRunID(0, 1)
	expectedAirflowHookRunIDWithoutConf := actions.NewHookRunID(0, 2)

	testOutputWriter, ctrl, ts, record := setupTest(t)
	defer ts.Close()
	actionContent := `name: ` + testActionName + `
on:
  pre-commit: {}
hooks:
  - id: ` + testWebhookID + `
    type: webhook
    properties:
      url: "` + ts.URL + `/webhook"
      timeout: 2m30s
      query_params:
        prefix: public/
        disallow: ["user_", "{{ ENV.PRIVATE }}"]
      headers:
        user: admin
  - id: ` + testAirflowHookID + `
    type: airflow
    properties:
      url: "` + ts.URL + `/airflow?conf=true"
      dag_id: "some_dag_id"
      username: "some_username" 
      password: "{{ ENV.AIRFLOW_PASSWORD }}"
      dag_conf:
        some: "additional_conf"
  - id: ` + testAirflowHookIDNoConf + `
    type: airflow
    properties:
      url: "` + ts.URL + `/airflow?conf=false"
      dag_id: "some_dag_id"
      username: "some_username" 
      password: "{{ ENV.AIRFLOW_PASSWORD }}"
`
	tests := []struct {
		name           string
		actionsService getService
	}{
		{name: "KV service test", actionsService: GetKVService},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testOutputWriter.EXPECT().
				OutputWrite(ctx, record.Repository, actions.FormatHookOutputPath(record.RunID, expectedWebhookRunID), gomock.Any(), gomock.Any()).
				Return(nil).
				DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, name string, reader io.Reader, size int64) error {
					var err error
					writerBytes, err = io.ReadAll(reader)
					return err
				})
			testOutputWriter.EXPECT().
				OutputWrite(ctx, record.Repository, actions.FormatHookOutputPath(record.RunID, expectedAirflowHookRunIDWithConf), gomock.Any(), gomock.Any()).
				Return(nil).
				DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, name string, reader io.Reader, size int64) error {
					var err error
					writerBytes, err = io.ReadAll(reader)
					return err
				})
			testOutputWriter.EXPECT().
				OutputWrite(ctx, record.Repository, actions.FormatHookOutputPath(record.RunID, expectedAirflowHookRunIDWithoutConf), gomock.Any(), gomock.Any()).
				Return(nil).
				DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, name string, reader io.Reader, size int64) error {
					var err error
					writerBytes, err = io.ReadAll(reader)
					return err
				})
			testOutputWriter.EXPECT().
				OutputWrite(ctx, record.Repository, actions.FormatRunManifestOutputPath(record.RunID), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx context.Context, repository *graveler.RepositoryRecord, name string, reader io.Reader, size int64) error {
					data, err := io.ReadAll(reader)
					if err != nil {
						return err
					}
					var manifest actions.RunManifest
					err = json.Unmarshal(data, &manifest)
					if err != nil {
						return err
					}
					lastManifest = &manifest
					return nil
				}).
				Times(2)

			testSource := mock.NewMockSource(ctrl)
			testSource.EXPECT().
				List(ctx, record).
				Return([]string{"act.yaml"}, nil)
			testSource.EXPECT().
				Load(ctx, record, "act.yaml").
				Return([]byte(actionContent), nil)

			mockStatsCollector := NewActionStatsMockCollector()

			// run actions
			now := time.Now()
			actionsService := tt.actionsService(t, ctx, testSource, testOutputWriter, &mockStatsCollector, true)
			defer actionsService.Stop()

			err := actionsService.Run(ctx, record)
			if err != nil {
				t.Fatalf("Run() failed with err=%s", err)
			}
			if lastManifest == nil {
				t.Fatalf("Run() should store manifest")
			}
			if lastManifest.Run.RunID != record.RunID {
				t.Errorf("Run() manifest RunID %s, expected %s", lastManifest.Run.RunID, record.RunID)
			}
			if lastManifest.Run.CommitID != "" {
				t.Errorf("Run() manifest MergedCommitID %s, expected empty", lastManifest.Run.CommitID)
			}
			lastManifest = nil

			// update commit using post event record
			err = actionsService.UpdateCommitID(ctx, record.Repository, record.RunID, "commit1")
			if err != nil {
				t.Fatalf("UpdateCommitID() failed with err=%s", err)
			}
			if lastManifest == nil {
				t.Fatalf("UpdateCommitID() should store updated manifest")
			}
			if lastManifest.Run.RunID != record.RunID {
				t.Errorf("UpdateCommitID() manifest RunID %s, expected %s", lastManifest.Run.RunID, record.RunID)
			}
			if lastManifest.Run.CommitID != "commit1" {
				t.Errorf("UpdateCommitID() manifest MergedCommitID %s, expected 'commit1'", lastManifest.Run.CommitID)
			}

			// get run result
			runResult, err := actionsService.GetRunResult(ctx, record.Repository.RepositoryID.String(), record.RunID)
			if err != nil {
				t.Fatal("GetRunResult() get run result", err)
			}
			if runResult.RunID != record.RunID {
				t.Errorf("GetRunResult() result RunID=%s, expect=%s", runResult.RunID, record.RunID)
			}
			if runResult.BranchID != record.BranchID.String() {
				t.Errorf("GetRunResult() result BranchID=%s, expect=%s", runResult.BranchID, record.BranchID)
			}
			if runResult.EventType != string(record.EventType) {
				t.Errorf("GetRunResult() result Type=%s, expect=%s", runResult.EventType, record.EventType)
			}
			startTime := runResult.StartTime
			if startTime.Before(now) {
				t.Errorf("GetRunResult() result StartTime should be after we run the actions, %v > %v", startTime, now)
			}
			endTime := runResult.EndTime
			if endTime.Before(startTime) {
				t.Errorf("GetRunResult() result EndTime should be same or after StartTime %v >= %v", endTime, startTime)
			}

			const expectedPassed = true
			if runResult.Passed != expectedPassed {
				t.Errorf("GetRunResult() result Passed=%t, expect=%t", runResult.Passed, expectedPassed)
			}
			const expectedCommitID = "commit1"
			if runResult.CommitID != expectedCommitID {
				t.Errorf("GetRunResult() result MergedCommitID=%s, expect=%s", runResult.CommitID, expectedCommitID)
			}

			require.Equal(t, uint64(3), mockStatsCollector.Hits["pre-commit"])

			// get run - not found
			runResult, err = actionsService.GetRunResult(ctx, record.Repository.RepositoryID.String(), "not-run-id")
			expectedErr := actions.ErrNotFound
			if !errors.Is(err, expectedErr) {
				t.Errorf("GetRunResult() err=%v, expected=%v", err, expectedErr)
			}
			if runResult != nil {
				t.Errorf("GetRunResult() result=%v, expected nil", runResult)
			}

			require.Greater(t, bytes.Count(writerBytes, []byte("\n")), 10)
		})
	}
}

func TestDisableHooksRun(t *testing.T) {
	ctx := context.Background()
	testOutputWriter, ctrl, ts, record := setupTest(t)
	defer ts.Close()

	testSource := mock.NewMockSource(ctrl)

	tests := []struct {
		name           string
		actionsService getService
	}{
		{
			name:           "KV service test",
			actionsService: GetKVService,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// run actions
			mockStatsCollector := NewActionStatsMockCollector()
			actionsService := tt.actionsService(t, ctx, testSource, testOutputWriter, &mockStatsCollector, false)
			defer actionsService.Stop()

			err := actionsService.Run(ctx, record)
			if err != nil {
				t.Fatalf("Run() failed with err=%s", err)
			}

			// get run result
			runResult, err := actionsService.GetRunResult(ctx, record.Repository.RepositoryID.String(), record.RunID)
			if !errors.Is(err, actions.ErrNotFound) || runResult != nil {
				t.Fatal("GetRunResult() shouldn't get run result", err, runResult)
			}
			require.Equal(t, uint64(0), mockStatsCollector.Hits["pre-commit"])
		})
	}
}

func TestMissingEnvVar(t *testing.T) {
	ctx := context.Background()
	testOutputWriter, ctrl, _, record := setupTest(t)

	actionContent := `name: test action
on:
  pre-commit: {}
hooks:
  - id: airflow_hook_id
    type: airflow
    properties:
      url: "http://wontsendrequesthere/airflow"
      dag_id: "some_dag_id"
      username: "some_username"
      password: "{{ ENV.NEW_AIRFLOW_PASSWORD }}"
      dag_conf:
        some: "additional_conf"
`

	tests := []struct {
		name           string
		actionsService getService
	}{
		{
			name:           "KV service test",
			actionsService: GetKVService,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testSource := mock.NewMockSource(ctrl)
			testSource.EXPECT().
				List(ctx, record).
				Return([]string{"act.yaml"}, nil)
			testSource.EXPECT().
				Load(ctx, record, "act.yaml").
				Return([]byte(actionContent), nil)

			// run actions
			mockStatsCollector := NewActionStatsMockCollector()
			actionsService := tt.actionsService(t, ctx, testSource, testOutputWriter, &mockStatsCollector, true)
			defer actionsService.Stop()

			require.Error(t, actionsService.Run(ctx, record))
			require.Equal(t, uint64(0), mockStatsCollector.Hits["pre-commit"])
		})
	}
}

func TestHookIf(t *testing.T) {
	ctx := context.Background()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code := r.URL.Query().Get("code")
		if statusCode, err := strconv.Atoi(code); err == nil {
			w.WriteHeader(statusCode)
		}
		_, _ = fmt.Fprintln(w, "Hi, hook")
	}))
	defer ts.Close()

	tests := []struct {
		Name                string
		Action              string
		ExpectedErr         bool
		ExpectedHookIndexes []int
	}{
		{
			Name: "run",
			Action: `---
name: test action
on:
  pre-commit: {}
hooks:
  - id: id1
    type: webhook
    properties:
      url: ` + ts.URL + `
`,
			ExpectedHookIndexes: []int{0},
		},
		{
			Name: "skip_using_false",
			Action: `---
name: test action
on:
  pre-commit: {}
hooks:
  - id: id1
    type: webhook
    if: false
    properties:
      url: ` + ts.URL + `
  - id: id2
    type: webhook
    properties:
      url: ` + ts.URL,
			ExpectedHookIndexes: []int{1},
		},
		{
			Name: "fail_run_always",
			Action: `---
name: test action
on:
  pre-commit: {}
hooks:
  - id: id1
    type: webhook
    properties:
      url: ` + ts.URL + `?code=500
  - id: id2
    if: failure()
    type: webhook
    properties:
      url: ` + ts.URL + `
  - id: id3
    if: true
    type: webhook
    properties:
      url: ` + ts.URL,
			ExpectedHookIndexes: []int{0, 1, 2},
			ExpectedErr:         true,
		},
		{
			Name: "fail_skip_always",
			Action: `---
name: test action
on:
  pre-commit: {}
hooks:
  - id: id1
    type: webhook
    properties:
      url: ` + ts.URL + `?code=500
  - id: id2
    type: webhook
    properties:
      url: ` + ts.URL + `
  - id: id3
    if: true
    type: webhook
    properties:
      url: ` + ts.URL,
			ExpectedHookIndexes: []int{0, 2},
			ExpectedErr:         true,
		},
		{
			Name: "invalid_if_expression",
			Action: `---
name: test action
on:
  pre-commit: {}
hooks:
  - id: id1
    if: looks_bad
    type: webhook
    properties:
      url: ` + ts.URL,
			ExpectedHookIndexes: []int{0},
			ExpectedErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			testOutputWriter, ctrl, _, record := setupTest(t)
			defer ctrl.Finish()
			outputWriteReturn := func(ctx context.Context, repository *graveler.RepositoryRecord, name string, reader io.Reader, size int64) error {
				_, err := io.ReadAll(reader)
				return err
			}
			for _, idx := range tt.ExpectedHookIndexes {
				hookRunID := actions.NewHookRunID(0, idx)
				testOutputWriter.EXPECT().
					OutputWrite(gomock.Any(), record.Repository, actions.FormatHookOutputPath(record.RunID, hookRunID), gomock.Any(), gomock.Any()).
					Return(nil).
					DoAndReturn(outputWriteReturn)
			}
			testOutputWriter.EXPECT().
				OutputWrite(gomock.Any(), record.Repository, actions.FormatRunManifestOutputPath(record.RunID), gomock.Any(), gomock.Any()).
				Return(nil).
				DoAndReturn(outputWriteReturn)

			testSource := mock.NewMockSource(ctrl)
			testSource.EXPECT().List(ctx, record).Return([]string{"act.yaml"}, nil)
			testSource.EXPECT().Load(ctx, record, "act.yaml").Return([]byte(tt.Action), nil)

			mockStatsCollector := NewActionStatsMockCollector()
			actionsService := GetKVService(t, ctx, testSource, testOutputWriter, &mockStatsCollector, true)
			defer actionsService.Stop()

			err := actionsService.Run(ctx, record)
			if tt.ExpectedErr {
				if err == nil {
					t.Fatal("Expected run to fail with an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Run failed with error: %s", err)
			}
		})
	}
}

func checkEvent(t *testing.T, record graveler.HookRecord, event actions.EventInfo, actionName string, hookID string) {
	t.Helper()
	if event.EventType != string(record.EventType) {
		t.Errorf("Webhook post EventType=%s, expected=%s", event.EventType, record.EventType)
	}
	if event.ActionName != actionName {
		t.Errorf("Webhook post ActionName=%s, expected=%s", event.ActionName, actionName)
	}
	if event.HookID != hookID {
		t.Errorf("Webhook post HookID=%s, expected=%s", event.HookID, hookID)
	}
	if event.RepositoryID != record.Repository.RepositoryID.String() {
		t.Errorf("Webhook post RepositoryID=%s, expected=%s", event.RepositoryID, record.Repository.RepositoryID)
	}
	if event.BranchID != record.BranchID.String() {
		t.Errorf("Webhook post BranchID=%s, expected=%s", event.BranchID, record.BranchID)
	}
	if event.SourceRef != record.SourceRef.String() {
		t.Errorf("Webhook post SourceRef=%s, expected=%s", event.SourceRef, record.SourceRef)
	}
	if event.MergeSource != record.MergeSource.String() {
		t.Errorf("Webhook post MergeSource=%s, expected=%s", event.MergeSource, record.MergeSource)
	}
	if event.CommitMessage != record.Commit.Message {
		t.Errorf("Webhook post CommitMessage=%s, expected=%s", event.CommitMessage, record.Commit.Message)
	}
	if event.Committer != record.Commit.Committer {
		t.Errorf("Webhook post Committer=%s, expected=%s", event.Committer, record.Commit.Committer)
	}
	if diff := deep.Equal(event.CommitMetadata, map[string]string(record.Commit.Metadata)); diff != nil {
		t.Errorf("Webhook post Metadata diff=%s", diff)
	}
}

func setupTest(t *testing.T) (*mock.MockOutputWriter, *gomock.Controller, *httptest.Server, graveler.HookRecord) {
	t.Helper()
	hooks := graveler.HooksNoOp{}
	record := graveler.HookRecord{
		RunID:     hooks.NewRunID(),
		EventType: graveler.EventTypePreCommit,
		Repository: &graveler.RepositoryRecord{
			RepositoryID: "repoID",
			Repository: &graveler.Repository{
				StorageNamespace: "storageNamespace",
			},
		},
		BranchID:  "branchID",
		SourceRef: "sourceRef",
		Commit: graveler.Commit{
			Message:   "commitMessage",
			Committer: "committer",
			Metadata:  map[string]string{"key": "value"},
		},
	}
	hookResponse := "OK"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error("Failed to read webhook post data", err)
			return
		}

		switch r.URL.Path {
		case "/webhook":
			queryParams := map[string][]string(r.URL.Query())
			require.Len(t, queryParams["prefix"], 1)
			require.Equal(t, "public/", queryParams["prefix"][0])
			require.Len(t, queryParams["disallow"], 2)
			require.Equal(t, "user_", queryParams["disallow"][0])
			require.Equal(t, "private_", queryParams["disallow"][1])

			var eventInfo actions.EventInfo
			err = json.Unmarshal(data, &eventInfo)
			if err != nil {
				t.Error("Failed to unmarshal webhook data", err)
				return
			}

			checkEvent(t, record, eventInfo, testActionName, testWebhookID)
		case "/airflow/api/v1/dags/some_dag_id/dagRuns":
			var req actions.DagRunReq

			withConf := r.URL.Query().Get("conf") == "true"
			expectedID := testAirflowHookID
			if !withConf {
				expectedID = testAirflowHookIDNoConf
			}

			require.NoError(t, json.Unmarshal(data, &req))
			require.True(t, strings.HasPrefix(req.DagRunID, "lakeFS_hook_"+expectedID))

			if withConf {
				require.Equal(t, req.Conf["some"], "additional_conf")
			}

			username, pass, ok := r.BasicAuth()
			require.True(t, ok)
			require.Equal(t, "some_username", username)
			require.Equal(t, "some_password", pass)

			rawEvent, ok := req.Conf["lakeFS_event"]
			require.True(t, ok, "missing lakeFS event")
			b, err := json.Marshal(rawEvent)
			require.NoError(t, err)

			var event actions.EventInfo
			require.NoError(t, json.Unmarshal(b, &event))

			checkEvent(t, record, event, testActionName, expectedID)
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}

		_, _ = io.WriteString(w, hookResponse)
	}))

	ctrl := gomock.NewController(t)

	require.NoError(t, os.Setenv("PRIVATE", "private_"))
	require.NoError(t, os.Setenv("AIRFLOW_PASSWORD", "some_password"))

	return mock.NewMockOutputWriter(ctrl), ctrl, ts, record
}

func TestNewRunID(t *testing.T) {
	ctx := context.Background()
	testOutputWriter, ctrl, _, _ := setupTest(t)
	testSource := mock.NewMockSource(ctrl)
	mockStatsCollector := NewActionStatsMockCollector()
	actionsService := GetKVService(t, ctx, testSource, testOutputWriter, &mockStatsCollector, false)

	id1 := actionsService.NewRunID()
	time.Sleep(2 * time.Second)
	id2 := actionsService.NewRunID()

	require.Greater(t, id1, id2)
}
