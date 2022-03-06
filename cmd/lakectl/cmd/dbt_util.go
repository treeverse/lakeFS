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

type Executor func(*exec.Cmd) ([]byte, error)

var errSchemaExtraction = fmt.Errorf("failed extracting schema from dbt debug message")

func ValidateGenerateSchemaMacro(projectRoot, macrosDirName, generateSchemaFileName, schemaIdentifier string) error {
	p := path.Join(projectRoot, macrosDirName, generateSchemaFileName)
	data, err := ioutil.ReadFile(p)
	if err != nil {
		return err
	}
	if !strings.Contains(string(data), schemaIdentifier) {
		return fmt.Errorf(`'%s' doesn't include "%s" as the environment variable for schema name identifier: %w`, generateSchemaFileName, schemaIdentifier, err)
	}
	return nil
}

func DBTDebug(projectRoot string, schemaRegex *regexp.Regexp, executor Executor) (string, error) {
	DBTCmd := exec.Command("dbt", "debug")
	DBTCmd.Dir = projectRoot
	output, err := executor(DBTCmd)
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

func DBTRun(projectRoot, schema, schemaEnvVarIdentifier string, selectValues []string, executor Executor) (string, error) {
	selectedValuesList := strings.Join(selectValues, " ")
	DBTCmd := exec.Command("dbt", "run", "--select", selectedValuesList)
	if strings.TrimSpace(schemaEnvVarIdentifier) != "" && strings.TrimSpace(schema) != "" {
		DBTCmd.Env = append(os.Environ(), schemaEnvVarIdentifier+"="+schema)
	}
	DBTCmd.Dir = projectRoot
	output, err := executor(DBTCmd)
	return string(output), err
}

func DBTLsToJSON(projectRoot, resourceType string, selectValues []string, executor Executor) ([]DBTResource, error) {
	DBTCmd := exec.Command("dbt", "ls", "--resource-type", resourceType, "--select", strings.Join(selectValues, " "), "--output", "json", "--output-keys", resourceJSONKeys)
	DBTCmd.Dir = projectRoot
	output, err := executor(DBTCmd)
	if err != nil {
		fmt.Println(string(output))
		return nil, err
	}
	resources := make([]DBTResource, 0)
	scan := bufio.NewScanner(bytes.NewReader(output))
	for scan.Scan() {
		line := scan.Bytes()
		var m DBTResource
		err = json.Unmarshal(line, &m)
		if err != nil {
			return nil, err
		}
		resources = append(resources, m)
	}
	err = scan.Err()
	return resources, err
}
