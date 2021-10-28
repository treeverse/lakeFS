package actions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Airflow struct {
	HookBase
	URL      string
	DagID    string
	Username string
	Password SecureString
	DAGConf  map[string]interface{}
}

const (
	airflowClientDefaultTimeout = 5 * time.Second
	airflowURLPropertyKey       = "url"
	airflowDagIDPropertyKey     = "dag_id"
	airflowUsernamePropertyKey  = "username"
	airflowPasswordPropertyKey  = "password"
	airflowConf                 = "dag_conf"
)

var (
	errAirflowHookRequestFailed = errors.New("airflow hook request failed")
)

func NewAirflowHook(h ActionHook, action *Action) (Hook, error) {
	airflowHook := Airflow{
		HookBase: HookBase{
			ID:         h.ID,
			ActionName: action.Name,
		},
		DAGConf: map[string]interface{}{},
	}
	var err error
	airflowHook.URL, err = h.Properties.getRequiredProperty(airflowURLPropertyKey)
	if err != nil {
		return nil, fmt.Errorf("airflow hook url property: %w", err)
	}

	airflowHook.DagID, err = h.Properties.getRequiredProperty(airflowDagIDPropertyKey)
	if err != nil {
		return nil, fmt.Errorf("airflow hook DAG ID property: %w", err)
	}
	airflowHook.Username, err = h.Properties.getRequiredProperty(airflowUsernamePropertyKey)
	if err != nil {
		return nil, fmt.Errorf("airflow hook username property: %w", err)
	}
	rawPass, err := h.Properties.getRequiredProperty(airflowPasswordPropertyKey)
	if err != nil {
		return nil, fmt.Errorf("airflow hook password property: %w", err)
	}
	airflowHook.Password, err = NewSecureString(rawPass)
	if err != nil {
		return nil, fmt.Errorf("airflow hook password property: %w", err)
	}

	conf, ok := h.Properties[airflowConf]
	if ok {
		airflowHook.DAGConf, ok = conf.(Properties)
		if !ok {
			return nil, fmt.Errorf("airflow hook conf is not of type Properties: %w", errWrongValueType)
		}
	}

	return &airflowHook, nil
}

type DagRunReq struct {
	// Run ID. This together with DAG_ID are a unique key.
	DagRunID string `json:"dag_run_id,omitempty"`

	// JSON object describing additional configuration parameters.
	Conf map[string]interface{} `json:"conf,omitempty"`
}

func (a *Airflow) Run(ctx context.Context, record graveler.HookRecord, writer *HookOutputWriter) error {
	logging.FromContext(ctx).
		WithField("hook_type", "airflow").
		WithField("event_type", record.EventType).
		Debug("hook action executing")

	eventData, err := marshalEventInformation(a.ActionName, a.ID, record)
	if err != nil {
		return err
	}
	a.DAGConf["lakeFS_event"] = json.RawMessage(eventData)

	dagRunID := fmt.Sprintf("lakeFS_hook_%s_%s", a.ID, time.Now().Format(time.RFC3339))
	bod, err := json.Marshal(DagRunReq{
		DagRunID: dagRunID,
		Conf:     a.DAGConf,
	})
	if err != nil {
		return fmt.Errorf("request serialization error: %w", err)
	}
	reqReader := bytes.NewReader(bod)

	dagRunURL, err := a.buildDagRunURL()
	if err != nil {
		return fmt.Errorf("building dag run path: %w", err)
	}
	buf := bytes.NewBufferString(fmt.Sprintf("Request:\nPOST %s\n", dagRunURL))

	req, err := http.NewRequest(http.MethodPost, dagRunURL, reqReader)
	if err != nil {
		return fmt.Errorf("request serialization error: %w", err)
	}
	req.SetBasicAuth(a.Username, a.Password.val)
	buf.WriteString(fmt.Sprintf("Username: %s, Password: %s\n", a.Username, a.Password.String()))

	req.Header.Set("Content-Type", "application/json")

	buf.WriteString(fmt.Sprintf("Body: %s\n\n", bod))
	statusCode, err := executeAndLogResponse(ctx, req, buf, writer, airflowClientDefaultTimeout)
	if err != nil {
		return fmt.Errorf("failed executing airflow request: %w", err)
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("status code (%d): %w", statusCode, errAirflowHookRequestFailed)
	}

	return nil
}

func (a *Airflow) buildDagRunURL() (string, error) {
	u, err := url.Parse(a.URL)
	if err != nil {
		return "", err
	}
	u.Path = path.Join(u.Path, "/api/v1/dags/", a.DagID, "/dagRuns")
	return u.String(), nil
}
