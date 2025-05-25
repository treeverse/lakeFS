package esti

import (
	"bufio"
	"io"
	"os/exec"
	"strings"

	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/logging"
)

func getSparkSubmitArgs(entryPoint string) []string {
	return []string{
		"--master", "spark://spark-master:7077",
		"--conf", "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp",
		"--conf", "spark.hadoop.lakefs.api.url=http://lakefs:8000" + apiutil.BaseURL,
		"--conf", "spark.hadoop.lakefs.api.access_key=AKIAIOSFDNN7EXAMPLEQ",
		"--conf", "spark.hadoop.lakefs.api.secret_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"--class", entryPoint,
	}
}

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
	args := []string{
		"exec", "spark-master", "spark-submit",
	}
	args = append(args, getSparkSubmitArgs(config.EntryPoint)...)
	args = append(args, config.ExtraSubmitArgs...)
	args = append(args, "/opt/metaclient/client.jar")
	args = append(args, config.ProgramArgs...)

	cmd := exec.Command("docker", args...)
	logger.Infof("Running command: %s", strings.Join(cmd.Args, " "))
	return runCommand(config.LogSource, cmd)
}
