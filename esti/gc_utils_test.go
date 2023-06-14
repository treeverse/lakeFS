package esti

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/treeverse/lakefs/pkg/logging"
)

func getSparkSubmitArgs(entryPoint string) []string {
	return []string{
		"--master", "spark://localhost:7077",
		"--conf", "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp",
		"--conf", "spark.hadoop.lakefs.api.url=http://lakefs:8000/api/v1",
		"--conf", "spark.hadoop.lakefs.api.access_key=AKIAIOSFDNN7EXAMPLEQ",
		"--conf", "spark.hadoop.lakefs.api.secret_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"--class", entryPoint,
	}
}

func getDockerArgs(workingDirectory string, localJar string) []string {
	return []string{"run", "--network", "host", "--add-host", "lakefs:127.0.0.1",
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

	cmdErrs := make(chan error, 2)
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

type sparkSubmitConfig struct {
	sparkVersion string
	// localJar is a local path to a jar that contains the main class.
	localJar string
	// entryPoint is the class name to run
	entryPoint      string
	extraSubmitArgs []string
	programArgs     []string
	logSource       string
}

func runSparkSubmit(config *sparkSubmitConfig) error {
	workingDirectory, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getting working directory: %w", err)
	}
	workingDirectory = strings.TrimSuffix(workingDirectory, "/")
	dockerArgs := getDockerArgs(workingDirectory, config.localJar)
	dockerArgs = append(dockerArgs, fmt.Sprintf("docker.io/bitnami/spark:%s", config.sparkVersion), "spark-submit")
	sparkSubmitArgs := getSparkSubmitArgs(config.entryPoint)
	sparkSubmitArgs = append(sparkSubmitArgs, config.extraSubmitArgs...)
	args := append(dockerArgs, sparkSubmitArgs...)
	args = append(args, "/opt/metaclient/client.jar")
	args = append(args, config.programArgs...)
	cmd := exec.Command("docker", args...)
	logger.Infof("Running command: %s", cmd.String())
	return runCommand(config.logSource, cmd)
}
