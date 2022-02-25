package dbtutil

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
)

const (
	resourceJsonKeys = "alias,schema"
)

type MissingSchemaIdentifierError struct {
	schemaIdentifier       string
	generateSchemaFileName string
}

func (mie *MissingSchemaIdentifierError) Error() string {
	return fmt.Sprintf("generate_schema_name does not contain %s addition. Handle lakefs support to %s or use skip-views flag", mie.schemaIdentifier, mie.generateSchemaFileName)
}

type MissingSchemaInDebugError struct{}

func (mie *MissingSchemaInDebugError) Error() string {
	return "failed extracting schema from dbt debug message"
}

type DbtResource struct {
	Schema string `json:"schema"`
	Alias  string `json:"alias"`
}

type CommandExecutor interface {
	ExecuteCommand(*exec.Cmd) ([]byte, error)
}

func ValidateGenerateSchemaMacro(projectRoot, macrosDirName, generateSchemaFileName, schemaIdentifier string) error {
	p := path.Join(projectRoot, macrosDirName, generateSchemaFileName)
	data, err := ioutil.ReadFile(p)
	if err != nil {
		return err
	}
	if !strings.Contains(string(data), schemaIdentifier) {
		return &MissingSchemaIdentifierError{schemaIdentifier: schemaIdentifier, generateSchemaFileName: generateSchemaFileName}
	}
	return nil
}

func DbtDebug(projectRoot string, schemaRegex *regexp.Regexp, executor CommandExecutor) (string, error) {
	dbtCmd := exec.Command("dbt", "debug")
	dbtCmd.Dir = projectRoot
	output, err := executor.ExecuteCommand(dbtCmd)
	if err != nil {
		return string(output), err
	}
	submatch := schemaRegex.FindSubmatch(output)
	if submatch == nil || len(submatch) < 2 {
		return "", &MissingSchemaInDebugError{}
	}
	schema := submatch[1]
	return string(schema), nil
}

func DbtRun(projectRoot, schema, schemaEnvVarIdentifier string, selectValues []string, executor CommandExecutor) (string, error) {
	dbtCmd := exec.Command("dbt", "run", "--select", strings.Join(selectValues, " "))
	if strings.TrimSpace(schemaEnvVarIdentifier) != "" && strings.TrimSpace(schema) != "" {
		dbtCmd.Env = append(os.Environ(), schemaEnvVarIdentifier+"="+schema)
	}
	dbtCmd.Dir = projectRoot
	output, err := executor.ExecuteCommand(dbtCmd)
	return string(output), err
}

func DbtLsToJson(projectRoot, resourceType string, selectValues []string, executor CommandExecutor) ([]DbtResource, error) {
	dbtCmd := exec.Command("dbt", "ls", "--resource-type", resourceType, "--select", strings.Join(selectValues, " "), "--output", "json", "--output-keys", resourceJsonKeys)
	dbtCmd.Dir = projectRoot
	output, err := executor.ExecuteCommand(dbtCmd)
	if err != nil {
		fmt.Println(string(output))
		return nil, err
	}
	models := make([]DbtResource, 0)
	scan := bufio.NewScanner(bytes.NewReader(output))
	for scan.Scan() {
		line := scan.Bytes()
		var m DbtResource
		err = json.Unmarshal(line, &m)
		if err != nil {
			return nil, err
		}
		models = append(models, m)

	}
	err = scan.Err()
	return models, err
}
