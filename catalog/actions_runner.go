package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/treeverse/lakefs/block"
)

// ActionsRunner used to execute hooks associated to actions and store the output under storage namespace
type ActionsRunner struct {
	BlockAdapter     block.Adapter
	StorageNamespace string
	StorageLocation  string
	Actions          []Action
	EventData        ActionEventData
	Results          []HookResult
}

type HookResult struct {
	Action     *Action
	StartTime  time.Time
	EndTime    time.Time
	StatusCode int
	OutputFile string
	Error      error
}

func NewActionsRunner(blockAdapter block.Adapter, storageNamespace, storageLocation string, actions []Action, actionEventData ActionEventData) *ActionsRunner {
	return &ActionsRunner{
		BlockAdapter:     blockAdapter,
		StorageNamespace: storageNamespace,
		StorageLocation:  storageLocation,
		Actions:          actions,
		EventData:        actionEventData,
	}
}

func (a *ActionsRunner) Run(ctx context.Context) error {
	for _, action := range a.Actions {
		act := action
		for _, hook := range action.Hooks {
			err := a.runHook(ctx, &act, hook)
			if err != nil {
				return fmt.Errorf("failed action '%s' hook '%s': %w", action.Name, hook.ID, err)
			}
		}
	}
	return nil
}

func (a *ActionsRunner) runHook(ctx context.Context, action *Action, hook ActionHook) error {
	if hook.Type != HookTypeWebhook {
		return fmt.Errorf("%w, hook type: %s", ErrInvalidAction, hook.Type)
	}
	// marshal data
	eventData, err := json.Marshal(a.EventData)
	if err != nil {
		return err
	}
	// http post request
	startTime := time.Now()
	url := hook.Properties[WebhookPropertyKeyURL]
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(eventData))
	if err != nil {
		return err
	}
	client := &http.Client{
		Timeout: time.Minute,
	}
	resp, err := client.Do(req)
	// keep record of what just happened
	result := HookResult{
		Action:    action,
		StartTime: startTime,
		EndTime:   time.Now(),
		Error:     err,
	}
	defer func() {
		a.Results = append(a.Results, result)
	}()
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// collect output and status code
	outputFile, err := a.writeResponseBody(ctx, action.Name, hook.ID, resp)
	if err != nil {
		result.Error = fmt.Errorf("failed to write webhook output: %w", err)
		return err
	}
	result.OutputFile = outputFile
	result.StatusCode = resp.StatusCode
	return nil
}

// writeResponseBody stream response output into an object and return the path
func (a *ActionsRunner) writeResponseBody(ctx context.Context, actionName string, hookID string, resp *http.Response) (string, error) {
	outputName := fmt.Sprintf("%s-%s.log", actionName, hookID)
	outputPath := path.Join(a.StorageLocation, a.EventData.RunID, outputName)
	objectPointer := block.ObjectPointer{
		StorageNamespace: a.StorageNamespace,
		Identifier:       outputPath,
	}
	err := a.BlockAdapter.
		WithContext(ctx).
		Put(objectPointer, resp.ContentLength, resp.Body, block.PutOpts{})
	if err != nil {
		return "", fmt.Errorf("write hook output '%s': %w", outputPath, err)
	}
	return outputPath, nil
}
