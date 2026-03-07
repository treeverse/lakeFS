package esti

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

// hadoopAzureVersion returns a hadoop-azure Maven coordinate version that
// matches the Hadoop runtime bundled in the given Spark image.  Using a
// mismatched version pulls conflicting transitive dependencies (e.g.
// hadoop-common 3.3.6 vs the 3.3.1 already on the Spark 3.2.1 classpath).
func hadoopAzureVersion(sparkVersion string) string {
	switch {
	case strings.HasPrefix(sparkVersion, "3."):
		return "3.3.1" // Spark 3.2.1 ships Hadoop 3.3.1
	case strings.HasPrefix(sparkVersion, "4.0"):
		return "3.4.0"
	case strings.HasPrefix(sparkVersion, "4.1"):
		return "3.4.1"
	default:
		return "3.3.6"
	}
}

func getSparkSubmitArgs(entryPoint string, blockstoreType string, sparkVersion string) []string {
	args := []string{
		"--master", "spark://localhost:7077",
		"--conf", "spark.jars.ivy=/tmp/ivy", // Use a writable path for ivy; the bitnami image runs as non-root user 1001
		"--conf", "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp -Divy.home=/tmp",
		"--conf", "spark.hadoop.lakefs.api.url=http://lakefs:8000" + apiutil.BaseURL,
		"--conf", "spark.hadoop.lakefs.api.access_key=AKIAIOSFDNN7EXAMPLEQ",
		"--conf", "spark.hadoop.lakefs.api.secret_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"--class", entryPoint,
	}
	switch blockstoreType {
	case block.BlockstoreTypeGS:
		args = append(args,
			"--conf", "spark.hadoop.google.cloud.auth.service.account.enable=true",
			"--conf", "spark.hadoop.google.cloud.auth.service.account.json.keyfile=/tmp/gc-creds.json",
			"--conf", "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
			"--conf", "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
		)
	case block.BlockstoreTypeAzure:
		azureStorageAccount := os.Getenv("ESTI_AZURE_STORAGE_ACCOUNT")
		azureStorageAccessKey := os.Getenv("ESTI_AZURE_STORAGE_ACCESS_KEY")
		args = append(args,
			// hadoop-azure is "provided" in the assembly jar and not bundled in the
			// bitnami-spark image; pull it at runtime so the ABFS driver is available.
			// The version must match the Hadoop runtime in the Spark image to avoid
			// classpath conflicts.
			"--packages", fmt.Sprintf("org.apache.hadoop:hadoop-azure:%s", hadoopAzureVersion(sparkVersion)),
			"--conf", fmt.Sprintf("spark.hadoop.fs.azure.account.key.%s.dfs.core.windows.net=%s", azureStorageAccount, azureStorageAccessKey),
		)
	}
	return args
}

func getDockerArgs(t testing.TB, localJar string, blockstoreType string) []string {
	t.Helper()
	args := []string{
		"run", "--network", "host", "--add-host", "lakefs:127.0.0.1",
		"-v", fmt.Sprintf("%s:/opt/metaclient/client.jar", localJar),
		"--rm",
		"-e", "AWS_ACCESS_KEY_ID",
		"-e", "AWS_SECRET_ACCESS_KEY",
	}
	switch blockstoreType {
	case block.BlockstoreTypeGS:
		credsFile := writeGCSCredentialsFile(t)
		args = append(args, "-v", fmt.Sprintf("%s:/tmp/gc-creds.json:ro", credsFile))
	case block.BlockstoreTypeAzure:
		args = append(args,
			"-e", "AZURE_CLIENT_ID",
			"-e", "AZURE_CLIENT_SECRET",
			"-e", "AZURE_TENANT_ID",
		)
	}
	return args
}

// handlePipe calls log on each line of pipe, and writes nil or an error to
// ch when done.
func handlePipe(pipe io.ReadCloser, log func(messages ...any), ch chan<- error) {
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
	BlockstoreType  string
	ExtraSubmitArgs []string
	ProgramArgs     []string
	LogSource       string
}

func writeGCSCredentialsFile(t testing.TB) string {
	t.Helper()
	credsJSON := os.Getenv("LAKEFS_BLOCKSTORE_GS_CREDENTIALS_JSON")
	f, err := os.CreateTemp("", "gc-gcs-creds-*.json")
	if err != nil {
		t.Fatalf("failed to create GCS credentials temp file: %v", err)
	}
	if _, err := f.WriteString(credsJSON); err != nil {
		t.Fatalf("failed to write GCS credentials file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close GCS credentials file: %v", err)
	}
	return f.Name()
}

func RunSparkSubmit(t testing.TB, config *SparkSubmitConfig) error {
	t.Helper()
	dockerArgs := getDockerArgs(t, config.LocalJar, config.BlockstoreType)
	dockerArgs = append(dockerArgs, fmt.Sprintf("docker.io/treeverse/bitnami-spark:%s", config.SparkVersion), "spark-submit")
	sparkSubmitArgs := getSparkSubmitArgs(config.EntryPoint, config.BlockstoreType, config.SparkVersion)
	sparkSubmitArgs = append(sparkSubmitArgs, config.ExtraSubmitArgs...)
	args := dockerArgs
	args = append(args, sparkSubmitArgs...)
	args = append(args, "/opt/metaclient/client.jar")
	args = append(args, config.ProgramArgs...)
	cmd := exec.Command("docker", args...)
	logger.Infof("Running command: %s", cmd.String())
	return runCommand(config.LogSource, cmd)
}
