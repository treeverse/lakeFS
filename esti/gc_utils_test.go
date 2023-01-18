package esti

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
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

// runCommand runs the given command. It redirects the output of the command:
// 1. stdout is redirected to the logger.
// 2. stderr is simply printed out.
func runCommand(cmdName string, cmd *exec.Cmd) error {
	handlePipe := func(pipe io.ReadCloser, log func(format string, args ...interface{})) {
		reader := bufio.NewReader(pipe)
		go func() {
			defer func() {
				_ = pipe.Close()
			}()
			for {
				str, err := reader.ReadString('\n')
				if err != nil {
					break
				}
				log(strings.TrimSuffix(str, "\n"))
			}
		}()
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout from command: %w", err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr from command: %w", err)
	}
	handlePipe(stdoutPipe, logger.WithField("source", cmdName).Infof)
	handlePipe(stderrPipe, func(format string, args ...interface{}) {
		println(format, args)
	})
	err = cmd.Start()
	if err != nil {
		return err
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
