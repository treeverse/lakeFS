package esti

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"

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

func streamLog(r io.Reader, src, std string, ch chan<- error) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		logger.WithFields(logging.Fields{"source": src, "std": std}).Info(scanner.Text())
	}
	ch <- scanner.Err()
}

func runCommand(cmdName string, args []string) error {
	cmd := exec.Command(
		filepath.Join(os.Getenv("SPARK_HOME"), "bin", "spark-submit"),
		args...,
	)
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	errs := make(chan error, 2)

	go streamLog(stdout, cmdName, "out", errs)
	go streamLog(stderr, cmdName, "err", errs)

	if err := cmd.Start(); err != nil {
		return err
	}
	<-errs
	<-errs
	return cmd.Wait()
}

type SparkSubmitConfig struct {
	SparkVersion    string
	LocalJar        string
	EntryPoint      string
	ExtraSubmitArgs []string
	ProgramArgs     []string
	LogSource       string
}

func RunSparkSubmit(config *SparkSubmitConfig) error {
	if sparkHome := os.Getenv("SPARK_HOME"); sparkHome != "" {
		args := getSparkSubmitArgs(config.EntryPoint)
		args = append(args, config.ExtraSubmitArgs...)
		args = append(args, config.LocalJar)
		args = append(args, config.ProgramArgs...)
		return runCommand(config.LogSource, args)
	}
	dockerArgs := []string{
		"run", "--network", "host", "--add-host", "lakefs:127.0.0.1",
		"-v", filepath.Dir(config.LocalJar) + ":/lakefs/test/spark/metaclient:ro",
		"--rm",
		"-e", "AWS_ACCESS_KEY_ID",
		"-e", "AWS_SECRET_ACCESS_KEY",
		"docker.io/bitnami/spark:" + config.SparkVersion,
		"spark-submit",
	}
	sparkArgs := getSparkSubmitArgs(config.EntryPoint)
	sparkArgs = append(sparkArgs, config.ExtraSubmitArgs...)
	sparkArgs = append(sparkArgs, "/lakefs/test/spark/metaclient/"+filepath.Base(config.LocalJar))
	sparkArgs = append(sparkArgs, config.ProgramArgs...)
	dockerArgs = append(dockerArgs, sparkArgs...)
	cmd := exec.Command("docker", dockerArgs...)
	logger.Infof("Running command: %s", cmd.String())
	return runCommand(config.LogSource, dockerArgs)
}
