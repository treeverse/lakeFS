package cmd

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
	resourceJSONKeys = "alias,schema"
)

type DBTResource struct {
	Schema string `json:"schema"`
	Alias  string `json:"alias"`
}

type CommandExecutor interface {
	ExecuteCommand(*exec.Cmd) ([]byte, error)
}

var errSchemaExtraction = fmt.Errorf("failed extracting schema from dbt debug message")

func ValidateGenerateSchemaMacro(projectRoot, macrosDirName, generateSchemaFileName, schemaIdentifier string) error {
	p := path.Join(projectRoot, macrosDirName, generateSchemaFileName)
	data, err := ioutil.ReadFile(p)
	if err != nil {
		return err
	}
	if !strings.Contains(string(data), schemaIdentifier) {
		return fmt.Errorf("generate_schema_name does not contain %s addition. Handle lakefs support to %s or use skip-views flag: %w", schemaIdentifier, generateSchemaFileName, err)
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
		return "", errSchemaExtraction
	}
	schema := submatch[1]
	return string(schema), nil
}

func DbtRun(projectRoot, schema, schemaEnvVarIdentifier string, selectValues []string, executor CommandExecutor) (string, error) {
	selectedValuesList := strings.Join(selectValues, " ")
	dbtCmd := exec.Command("dbt", "run", "--select", selectedValuesList)
	if strings.TrimSpace(schemaEnvVarIdentifier) != "" && strings.TrimSpace(schema) != "" {
		dbtCmd.Env = append(os.Environ(), schemaEnvVarIdentifier+"="+schema)
	}
	dbtCmd.Dir = projectRoot
	output, err := executor.ExecuteCommand(dbtCmd)
	return string(output), err
}

func DbtLsToJSON(projectRoot, resourceType string, selectValues []string, executor CommandExecutor) ([]DBTResource, error) {
	dbtCmd := exec.Command("dbt", "ls", "--resource-type", resourceType, "--select", strings.Join(selectValues, " "), "--output", "json", "--output-keys", resourceJSONKeys)
	dbtCmd.Dir = projectRoot
	output, err := executor.ExecuteCommand(dbtCmd)
	if err != nil {
		fmt.Println(string(output))
		return nil, err
	}
	dbtResources := make([]DBTResource, 0)
	scan := bufio.NewScanner(bytes.NewReader(output))
	for scan.Scan() {
		line := scan.Bytes()
		var m DBTResource
		err = json.Unmarshal(line, &m)
		if err != nil {
			return nil, err
		}
		dbtResources = append(dbtResources, m)
	}
	err = scan.Err()
	return dbtResources, err
}
