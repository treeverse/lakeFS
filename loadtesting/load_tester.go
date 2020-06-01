package loadtesting

import (
	"bytes"
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
	"sync"
	"time"
)

type WriteSafeBuffer struct {
	bytes.Buffer
	sync.Mutex
}

func (s *WriteSafeBuffer) Write(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	n, err = s.Buffer.Write(p)
	if err != nil {
		return n, err
	}
	return s.WriteString("\n")
}

func (s *WriteSafeBuffer) Read(p []byte) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	return s.Buffer.Read(p)
}

type LoadTester struct {
	RequestHistory []Request
	Buffer         WriteSafeBuffer
	Config         LoadTesterConfig
}

type LoadTesterConfig struct {
	FreqPerSecond int
	Duration      time.Duration
	RepoName      string
	DeleteRepo    bool
	Credentials   model.Credential
	ServerAddress string
}

func LoadTest(config LoadTesterConfig) error {
	var apiClient api.Client
	var err error
	if config.RepoName == "" || config.DeleteRepo {
		apiClient, err = getClient(config)
		if err != nil {
			return err
		}
	}
	if config.RepoName == "" {
		config.RepoName, err = createRepo(config, apiClient)
		if err != nil {
			return err
		}
	}
	loadTester := LoadTester{
		Config: config,
	}
	stopCh := make(chan struct{})

	out := new(SimpleScenario).Play(config.ServerAddress, config.RepoName, stopCh)
	errs := loadTester.streamRequests(out)
	hasErrors, metrics, metricsTotal := loadTester.doAttack()
	close(stopCh)

	if config.DeleteRepo {
		err = apiClient.DeleteRepository(context.Background(), config.RepoName)
		if err != nil {
			return err
		}
	}
	for err := range errs {
		log.Errorf("error during request pipeline: %v", err)
		return err
	}
	err = printResults(metrics, metricsTotal)
	if err != nil {
		return err
	}
	if hasErrors {
		return errors.New("got errors during load tests, see output for details")
	}
	return nil
}

func createRepo(config LoadTesterConfig, apiClient api.Client) (string, error) {
	if config.RepoName == "" {
		config.RepoName = uuid.New().String()
		err := apiClient.CreateRepository(context.Background(), &models.RepositoryCreation{
			DefaultBranch: "master",
			ID:            &config.RepoName,
			BucketName:    &config.RepoName,
		})
		if err != nil {
			return "", errors.New(fmt.Sprintf("failed to create lakeFS repository: %v", err))
		}
	}
	return config.RepoName, nil
}

func getClient(config LoadTesterConfig) (apiClient api.Client, err error) {

	apiClient, err = api.NewClient(config.ServerAddress, config.Credentials.AccessKeyId, config.Credentials.AccessSecretKey)
	if err != nil {
		return nil, errors.New("failed to create lakeFS client")
	}

	return apiClient, nil
}

func (t *LoadTester) doAttack() (hasErrors bool, metrics map[string]*vegeta.Metrics, metricsTotal *vegeta.Metrics) {
	targeter := vegeta.NewJSONTargeter(&t.Buffer, nil,
		http.Header{http.CanonicalHeaderKey("Authorization"): []string{"Basic " + getAuth(&t.Config.Credentials)}})
	attacker := vegeta.NewAttacker()
	metrics = make(map[string]*vegeta.Metrics)
	metricsTotal = new(vegeta.Metrics)
	rate := vegeta.Rate{Freq: t.Config.FreqPerSecond, Per: time.Second}
	for res := range attacker.Attack(targeter, rate, t.Config.Duration, "lakeFS load test") {
		if len(res.Error) > 0 {
			log.Debugf("Error in request type %s, error: %s, status: %d", t.RequestHistory[res.Seq].RequestType, res.Error, res.Code)
			hasErrors = true
		}
		typeMetrics := metrics[t.RequestHistory[res.Seq].RequestType]
		if typeMetrics == nil {
			metrics[t.RequestHistory[res.Seq].RequestType] = new(vegeta.Metrics)
			typeMetrics = metrics[t.RequestHistory[res.Seq].RequestType]
		}
		typeMetrics.Add(res)
		metricsTotal.Add(res)
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

func (t *LoadTester) streamRequests(in <-chan Request) <-chan error {
	errs := make(chan error, 1)
	go func() {
		defer close(errs)
		for tgt := range in {
			t.RequestHistory = append(t.RequestHistory, tgt)
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
