package esti

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/logging"
)

func getSparkSubmitArgs(entryPoint string) []string {
	return []string{
		"--master", "spark://localhost:7077",
		"--conf", "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp",
		"--conf", "spark.hadoop.lakefs.api.url=http://lakefs:8000" + apiutil.BaseURL,
		"--conf", "spark.hadoop.lakefs.api.access_key=AKIAIOSFDNN7EXAMPLEQ",
		"--conf", "spark.hadoop.lakefs.api.secret_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"--class", entryPoint,
	}
}

func getDockerArgs(workingDirectory, localJar string) []string {
	jarHostPath := filepath.Join(workingDirectory, localJar)
	return []string{
		"run",
		"--network", "host",
		"--add-host", "lakefs:127.0.0.1",
		"-v", fmt.Sprintf("%s/ivy:/opt/bitnami/spark/.ivy2", workingDirectory),
		"-v", fmt.Sprintf("%s:/opt/metaclient/client.jar:ro", jarHostPath),
		"--rm",
		"-e", "AWS_ACCESS_KEY_ID",
		"-e", "AWS_SECRET_ACCESS_KEY",
	}
}

// handlePipe calls log on each line of pipe, and writes nil or an error to
// ch when done.
func handlePipe(pipe io.ReadCloser, log func(messages ...interface{}), ch chan<- error) {
	reader := bufio.NewReader(pipe)
	go func() {
		var err error
		for {
			var line string
			line, err = reader.ReadString('\n')
			if err != nil {
				break
			}
			log(strings.TrimSuffix(line, "\n"))
		}
		if err == io.EOF {
			err = nil
		}
		ch <- err
	}()
}

// runCommand runs cmd. It logs the outputs of cmd with an appropriate field
// to distinguish stdout from stderr.
func runCommand(cmdName string, cmd *exec.Cmd) error {
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	errs := make(chan error, 2)
	handlePipe(stdout, logger.WithFields(logging.Fields{"source": cmdName, "std": "out"}).Info, errs)
	handlePipe(stderr, logger.WithFields(logging.Fields{"source": cmdName, "std": "err"}).Info, errs)

	if err := cmd.Start(); err != nil {
		return err
	}
	if e := <-errs; e != nil {
		logger.WithFields(logging.Fields{"source": cmdName}).WithError(e).Error("pipe read error")
	}
	if e := <-errs; e != nil {
		logger.WithFields(logging.Fields{"source": cmdName}).WithError(e).Error("pipe read error")
	}
	return cmd.Wait()
}

type SparkSubmitConfig struct {
	SparkVersion string
	// LocalJar is a local path to a jar that contains the main class.
	LocalJar string
	// EntryPoint is the class name to run
	EntryPoint      string
	ExtraSubmitArgs []string
	ProgramArgs     []string
	LogSource       string
}

func RunSparkSubmit(config *SparkSubmitConfig) error {
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getting working directory: %w", err)
	}
	wd = strings.TrimSuffix(wd, "/")

	dockerArgs := getDockerArgs(wd, config.LocalJar)

	version := config.SparkVersion
	if version == "" || strings.HasPrefix(version, "$") {
		version = os.Getenv("SPARK_IMAGE_TAG")
	}
	if version == "" || strings.HasPrefix(version, "$") {
		version = "3.2.1"
	}

	image := fmt.Sprintf("docker.io/bitnami/spark:%s", version)
	dockerArgs = append(dockerArgs, image, "spark-submit")
	submitArgs := getSparkSubmitArgs(config.EntryPoint)
	submitArgs = append(submitArgs, config.ExtraSubmitArgs...)
	submitArgs = append(submitArgs, "/opt/metaclient/client.jar")
	submitArgs = append(submitArgs, config.ProgramArgs...)
	cmd := exec.Command("docker", append(dockerArgs, submitArgs...)...)
	logger.Infof("Running command: %s", cmd.String())

	return runCommand(config.LogSource, cmd)
}
