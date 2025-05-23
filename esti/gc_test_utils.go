package esti

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
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

func getDockerArgs(workingDirectory string, localJar string) []string {
	return []string{
		"run", "--network", "host", "--add-host", "lakefs:127.0.0.1",
		"-v", fmt.Sprintf("%s/ivy:/opt/bitnami/spark/.ivy2", workingDirectory),
		"-v", fmt.Sprintf("%s:/opt/metaclient/client.jar", localJar),
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
			str, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			log(strings.TrimSuffix(str, "\n"))
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
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout from command: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr from command: %w", err)
	}

	const channelSize = 2
	cmdErrs := make(chan error, channelSize)
	handlePipe(stdoutPipe, logger.WithFields(logging.Fields{
		"source": cmdName,
		"std":    "out",
	}).Info, cmdErrs)
	handlePipe(stderrPipe, logger.WithFields(logging.Fields{
		"source": cmdName,
		"std":    "err",
	}).Info, cmdErrs)

	err = cmd.Start()
	if err != nil {
		return err
	}

	err = <-cmdErrs
	if err != nil {
		logger.WithFields(logging.Fields{"source": cmdName, "component": "handlePipe"}).WithError(err).Error("Error reading command pipe")
	}
	err = <-cmdErrs
	if err != nil {
		logger.WithFields(logging.Fields{"source": cmdName, "component": "handlePipe"}).WithError(err).Error("Error reading command pipe")
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
	accessKey := os.Getenv("LAKEFS_ACCESS_KEY_ID")
	secretKey := os.Getenv("LAKEFS_SECRET_ACCESS_KEY")
	if accessKey == "" || secretKey == "" {
		return fmt.Errorf("missing lakeFS credentials in environment variables")
	}

	cmdArgs := []string{
		"exec",
		"-e", fmt.Sprintf("LAKEFS_ACCESS_KEY_ID=%s", accessKey),
		"-e", fmt.Sprintf("LAKEFS_SECRET_ACCESS_KEY=%s", secretKey),
		"lakefs-spark",
		"spark-submit",
		"--master", "spark://spark:7077",
		"--conf", "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp",
		"--conf", "spark.hadoop.lakefs.api.url=http://lakefs:8000/api/v1",
		"--conf", fmt.Sprintf("spark.hadoop.lakefs.access.key=%s", accessKey),
		"--conf", fmt.Sprintf("spark.hadoop.lakefs.secret.key=%s", secretKey),
		"--class", config.EntryPoint,
		"/opt/metaclient/spark-assembly.jar",
	}
	cmdArgs = append(cmdArgs, config.ProgramArgs...)

	cmd := exec.Command("docker", cmdArgs...)
	cmd.Env = os.Environ()

	return runCommand(config.LogSource, cmd)
}
