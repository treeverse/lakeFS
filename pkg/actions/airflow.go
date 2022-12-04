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
	URL        string
	DagID      string
	Username   string
	Password   SecureString
	DAGConf    map[string]interface{}
	Timeout    time.Duration
	WaitForDAG bool
}

type airflowGetDagResponse struct {
	DagRunID      string    `json:"dag_run_id"`
	DagID         string    `json:"dag_id"`
	LogicalDate   time.Time `json:"logical_date"`
	ExecutionDate time.Time `json:"execution_date"`
	StartDate     time.Time `json:"start_date"`
	EndDate       time.Time `json:"end_date"`
	State         string    `json:"state"`
}

const (
	airflowDefaultTimeout       = 1 * time.Minute
	airflowClientDefaultTimeout = 5 * time.Second
	airflowCheckStatusInterval  = 5 * time.Second

	airflowURLPropertyKey        = "url"
	airflowDagIDPropertyKey      = "dag_id"
	airflowUsernamePropertyKey   = "username"
	airflowPasswordPropertyKey   = "password"
	airflowConf                  = "dag_conf"
	airflowTimeoutPropertyKey    = "timeout"
	airflowWaitForDAGPropertyKey = "wait_for_dag"
)

var (
	errAirflowHookRequestFailed = errors.New("airflow hook request failed")
	errAirflowHookDAGFailed     = errors.New("airflow hook DAG failed")
)

func NewAirflowHook(h ActionHook, action *Action, endpoint *http.Server) (Hook, error) {
	airflowHook := Airflow{
		HookBase: HookBase{
			ID:         h.ID,
			ActionName: action.Name,
			Endpoint:   endpoint,
		},
		DAGConf: map[string]interface{}{},
		Timeout: airflowDefaultTimeout,
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

	if v, ok := h.Properties[airflowTimeoutPropertyKey].(string); ok {
		duration, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("airflow hook timeout property: %w", err)
		}
		airflowHook.Timeout = duration
	}

	if v, ok := h.Properties[airflowWaitForDAGPropertyKey].(bool); ok {
		airflowHook.WaitForDAG = v
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
	// DagRunID Run ID. This together with DAG_ID are a unique key.
	DagRunID string `json:"dag_run_id,omitempty"`
	// Conf JSON object describing additional configuration parameters.
	Conf map[string]interface{} `json:"conf,omitempty"`
}

func (a *Airflow) Run(ctx context.Context, record graveler.HookRecord, buf *bytes.Buffer) error {
	logging.FromContext(ctx).
		WithField("hook_type", "airflow").
		WithField("event_type", record.EventType).
		Debug("hook action executing")

	eventData, err := marshalEventInformation(a.ActionName, a.ID, record)
	if err != nil {
		return err
	}
	a.DAGConf["lakeFS_event"] = json.RawMessage(eventData)

	dagRunID := fmt.Sprintf("lakeFS_hook_%s_%s", a.ID, record.RunID)
	body, err := json.Marshal(DagRunReq{
		DagRunID: dagRunID,
		Conf:     a.DAGConf,
	})
	if err != nil {
		return fmt.Errorf("request serialization error: %w", err)
	}
	reqReader := bytes.NewReader(body)

	dagRunURL, err := a.buildDagRunURL()
	if err != nil {
		return fmt.Errorf("building dag run path: %w", err)
	}
	_, _ = fmt.Fprintf(buf, "Request:\nPOST %s\n", dagRunURL)

	req, err := http.NewRequest(http.MethodPost, dagRunURL, reqReader)
	if err != nil {
		return fmt.Errorf("request serialization error: %w", err)
	}
	req.SetBasicAuth(a.Username, a.Password.val)
	_, _ = fmt.Fprintf(buf, "Username: %s, Password: %s\n", a.Username, a.Password.String())

	req.Header.Set("Content-Type", "application/json")

	_, _ = fmt.Fprintf(buf, "Body: %s\n\n", body)

	statusCode, err := doHTTPRequestWithLog(ctx, req, buf, airflowClientDefaultTimeout)
	if err != nil {
		return fmt.Errorf("failed executing airflow request: %w", err)
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("status code (%d): %w", statusCode, errAirflowHookRequestFailed)
	}

	if a.WaitForDAG {
		return a.waitForDAGComplete(ctx, dagRunID, buf)
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

func (a *Airflow) waitForDAGComplete(ctx context.Context, dagRunID string, buf *bytes.Buffer) error {
	_, _ = fmt.Fprintf(buf, "\nWaiting for DAG to complete...\n")

	// get dag request url
	u, err := url.Parse(a.URL)
	if err != nil {
		return fmt.Errorf("failed parse airflow URL for DAG status: %w", err)
	}
	u.Path = path.Join(u.Path, "/api/v1/dags/", url.PathEscape(a.DagID), "/dagRuns/", url.PathEscape(dagRunID))
	dagRunURL := u.String()

	ctx, cancel := context.WithTimeout(ctx, a.Timeout)
	defer cancel()

	t := time.NewTicker(airflowCheckStatusInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			state, err := a.getAirflowDAGStatus(ctx, dagRunURL, buf)
			if err != nil {
				return err
			}
			switch state {
			case "failed":
				return errAirflowHookDAGFailed
			case "success":
				return nil
			}
		}
	}
}

func (a *Airflow) getAirflowDAGStatus(ctx context.Context, dagRunURL string, buf *bytes.Buffer) (string, error) {
	_, _ = fmt.Fprintf(buf, "Request:\n%s %s\n", http.MethodGet, dagRunURL)

	req, err := http.NewRequest(http.MethodGet, dagRunURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed airflow new request for DAG status: %w", err)
	}
	req.SetBasicAuth(a.Username, a.Password.val)
	req.Header.Set("Content-Type", "application/json")

	var dagResponse airflowGetDagResponse
	statusCode, err := doHTTPRequestResponseWithLog(ctx, req, &dagResponse, buf, airflowClientDefaultTimeout)
	if err != nil {
		return "", err
	}
	if statusCode != http.StatusOK {
		return "", fmt.Errorf("status code (%d): %w", statusCode, errAirflowHookRequestFailed)
	}
	_, _ = fmt.Fprintf(buf, "\nDAG completed with state: %s\n", dagResponse.State)
	return dagResponse.State, nil
}
