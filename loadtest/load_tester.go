package loadtest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jedib0t/go-pretty/text"
	log "github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/auth/model"
	vegeta "github.com/tsenart/vegeta/v12/lib"
	"net/http"
	"os"
	"time"
)

type LoadTest struct {
	History      []Request
	Buffer       SafeBuffer
	Config       Config
	NewRepoName  string
	Metrics      map[string]*vegeta.Metrics
	TotalMetrics *vegeta.Metrics
}

type Config struct {
	FreqPerSecond int
	Duration      time.Duration
	RepoName      string
	KeepRepo      bool
	Credentials   model.Credential
	ServerAddress string
}

func NewLoadTest(config Config) LoadTest {
	res := LoadTest{
		Config: config,
	}
	if config.RepoName == "" {
		res.NewRepoName = uuid.New().String()
	}
	return res
}

func (t *LoadTest) Run() error {
	apiClient, err := t.getClient()
	if err != nil {
		return err
	}
	repoName, err := t.createRepo(apiClient)
	if err != nil {
		return err
	}
	stopCh := make(chan struct{})
	out := new(SimpleScenario).Play(t.Config.ServerAddress, repoName, stopCh)
	errs := t.streamRequests(out)
	hasErrors := t.doAttack()
	close(stopCh)

	if t.Config.RepoName == "" && !t.Config.KeepRepo {
		err = apiClient.DeleteRepository(context.Background(), t.NewRepoName)
		if err != nil {
			return err
		}
	}
	for err := range errs {
		log.Errorf("error during request pipeline: %v", err)
		return err
	}
	err = printResults(t.Metrics, t.TotalMetrics)
	if err != nil {
		return err
	}
	if hasErrors {
		return errors.New("got errors during load tests, see output for details")
	}
	return nil
}

func (t *LoadTest) createRepo(apiClient api.Client) (string, error) {
	if t.Config.RepoName != "" {
		// using an existing repo, no need to create one
		return t.Config.RepoName, nil
	}
	err := apiClient.CreateRepository(context.Background(), &models.RepositoryCreation{
		DefaultBranch: "master",
		ID:            &t.NewRepoName,
		BucketName:    &t.NewRepoName,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create lakeFS repository: %w", err)
	}
	return t.NewRepoName, nil
}

func (t *LoadTest) getClient() (apiClient api.Client, err error) {
	if t.Config.RepoName != "" {
		// using an existing repo, no need to create a client
		return nil, nil
	}
	apiClient, err = api.NewClient(t.Config.ServerAddress, t.Config.Credentials.AccessKeyId, t.Config.Credentials.AccessSecretKey)
	if err != nil {
		return nil, errors.New("failed to create lakeFS client")
	}
	return apiClient, nil
}

func (t *LoadTest) doAttack() (hasErrors bool) {
	targeter := vegeta.NewJSONTargeter(&t.Buffer, nil,
		http.Header{http.CanonicalHeaderKey("Authorization"): []string{"Basic " + getAuth(&t.Config.Credentials)}})
	attacker := vegeta.NewAttacker()
	t.Metrics = make(map[string]*vegeta.Metrics)
	t.TotalMetrics = new(vegeta.Metrics)
	rate := vegeta.Rate{Freq: t.Config.FreqPerSecond, Per: time.Second}
	for res := range attacker.Attack(targeter, rate, t.Config.Duration, "lakeFS load test") {
		if len(res.Error) > 0 {
			log.Debugf("Error in request type %s, error: %s, status: %d", t.History[res.Seq].Type, res.Error, res.Code)
			hasErrors = true
		}
		typeMetrics := t.Metrics[t.History[res.Seq].Type]
		if typeMetrics == nil {
			typeMetrics = new(vegeta.Metrics)
			t.Metrics[t.History[res.Seq].Type] = typeMetrics
		}
		typeMetrics.Add(res)
		t.TotalMetrics.Add(res)
	}
	return
}

func printResults(metrics map[string]*vegeta.Metrics, metricsTotal *vegeta.Metrics) error {
	for requestType, typeMetrics := range metrics {
		typeMetrics.Close()
		fmt.Println(text.FgYellow.Sprintf("Results for request type: %s", requestType))

		err := vegeta.NewTextReporter(typeMetrics).Report(os.Stdout)
		if err != nil {
			fmt.Println("Error trying to write report")
			return err
		}
		fmt.Println()
	}
	fmt.Println(text.FgYellow.Sprintf("Results for ALL requests combined:"))
	metricsTotal.Close()
	err := vegeta.NewTextReporter(metricsTotal).Report(os.Stdout)
	if err != nil {
		fmt.Println("Error trying to write report")
		return err
	}
	return nil
}

func (t *LoadTest) streamRequests(in <-chan Request) <-chan error {
	errs := make(chan error, 1)
	go func() {
		defer close(errs)
		for tgt := range in {
			t.History = append(t.History, tgt)
			target, err := json.Marshal(tgt.Target)
			if err != nil {
				errs <- err
				return
			}
			_, err = t.Buffer.Write(target)
			if err != nil {
				errs <- err
				return
			}
		}
	}()
	return errs
}

func getAuth(credentials *model.Credential) string {
	return base64.StdEncoding.EncodeToString([]byte(credentials.AccessKeyId + ":" + credentials.AccessSecretKey))
}
