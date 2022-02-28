package dbtutil

import (
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

type DummyCommandExecutor struct {
	err       error
	output    string
	envReturn bool
}

type dummyCommandExecutorError struct{}

func (dcee dummyCommandExecutorError) Error() string {
	return "BOOM"
}

func (dce DummyCommandExecutor) ExecuteCommand(cmd *exec.Cmd) ([]byte, error) {
	sb := strings.Builder{}
	sb.WriteString(dce.output)
	if len(cmd.Env) > 0 && dce.envReturn {
		sb.WriteString(fmt.Sprintf(" %s", cmd.Env[len(cmd.Env)-1]))
	}
	return []byte(sb.String()), dce.err
}

var schemaRegex = regexp.MustCompile(`schema: (.+)`)

type dbtDebugTestCase struct {
	name string
	args struct {
		projectRoot string
		schemaRegex *regexp.Regexp
		executor    CommandExecutor
	}
	schemaName string
	errType    error
}

func TestDbtDebug(t *testing.T) {
	type args struct {
		projectRoot string
		schemaRegex *regexp.Regexp
		executor    CommandExecutor
	}
	tests := []dbtDebugTestCase{
		{
			name: "Sunny day Flow",
			args: args{
				projectRoot: "nevermind",
				schemaRegex: schemaRegex,
				executor:    DummyCommandExecutor{output: "schema: success", err: nil},
			},
			schemaName: "success",
			errType:    nil,
		},
		{
			name: "Command failed with error and output string",
			args: args{
				projectRoot: "nevermind",
				schemaRegex: nil,
				executor:    DummyCommandExecutor{output: "some error output", err: dummyCommandExecutorError{}},
			},
			schemaName: "some error output",
			errType:    dummyCommandExecutorError{},
		},
		{
			name: "Submatch is nil (output is not in the regex)",
			args: args{
				projectRoot: "nevermind",
				schemaRegex: schemaRegex,
				executor:    DummyCommandExecutor{output: "no schema definition", err: nil},
			},
			schemaName: "",
			errType:    MissingSchemaInDebugError{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DbtDebug(tt.args.projectRoot, tt.args.schemaRegex, tt.args.executor)
			if (err != nil) && tt.errType == nil {
				t.Errorf("DbtDebug() error = %v, but no error was expected", err)
				return
			}
			if (err != nil) && !errors.Is(err, tt.errType) {
				t.Errorf("DbtDebug() error = %v, expected error type = %v", err, tt.errType)
				return
			}
			if got != tt.schemaName {
				t.Errorf("DbtDebug() got = %v, schemaName %v", got, tt.schemaName)
			}
		})
	}
}

type dbtLsToJsonTestCase struct {
	name string
	args struct {
		projectRoot  string
		resourceType string
		selectValues []string
		executor     CommandExecutor
	}
	dbtResources []DBTResource
	errType      error
}

func TestDbtLsToJson(t *testing.T) {
	DBTResources := make([]DBTResource, 0)
	DBTResources = append(DBTResources, DBTResource{Schema: "schema1", Alias: "alias1"}, DBTResource{Schema: "schema2", Alias: "alias2"})
	type args struct {
		projectRoot  string
		resourceType string
		selectValues []string
		executor     CommandExecutor
	}
	tests := []dbtLsToJsonTestCase{
		{
			name: "Sunny day Flow",
			args: args{
				projectRoot:  "nevermind",
				resourceType: "nevermind",
				selectValues: []string{"nevermind"},
				executor:     DummyCommandExecutor{err: nil, output: generateJsonStringFromResources(DBTResources...)},
			},
			dbtResources: DBTResources,
			errType:      nil,
		},
		{
			name: "command returns an error",
			args: args{
				projectRoot:  "nevermind",
				resourceType: "nevermind",
				selectValues: []string{"nevermind"},
				executor:     DummyCommandExecutor{err: dummyCommandExecutorError{}, output: ""},
			},
			dbtResources: nil,
			errType:      dummyCommandExecutorError{},
		},
		{
			name: "command returns invalid json",
			args: args{
				projectRoot:  "nevermind",
				resourceType: "nevermind",
				selectValues: []string{"nevermind"},
				executor:     DummyCommandExecutor{err: nil, output: "invalid json"},
			},
			dbtResources: nil,
			errType:      &json.SyntaxError{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DbtLsToJson(tt.args.projectRoot, tt.args.resourceType, tt.args.selectValues, tt.args.executor)
			if (err != nil) && !errors.As(err, &tt.errType) {
				t.Errorf("DbtLsToJson() error = %v, expected errType: %v", err, tt.errType)
				return
			}
			if !reflect.DeepEqual(got, tt.dbtResources) {
				t.Errorf("DbtLsToJson() got = %v, schemaName %v", got, tt.dbtResources)
			}
		})
	}
}

type dbtRunTestCase struct {
	name string
	args struct {
		projectRoot            string
		schema                 string
		schemaEnvVarIdentifier string
		selectValues           []string
		executor               CommandExecutor
	}
	runOutput string
	errType   error
}

func TestDbtRun(t *testing.T) {
	schemaKey := "theschema"
	schemaValue := "myschema"
	type args struct {
		projectRoot            string
		schema                 string
		schemaEnvVarIdentifier string
		selectValues           []string
		executor               CommandExecutor
	}
	tests := []dbtRunTestCase{
		{
			name: "Sunny day flow",
			args: args{
				projectRoot:            "nevermind",
				schema:                 schemaValue,
				schemaEnvVarIdentifier: schemaKey,
				selectValues:           nil,
				executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: true,
				},
			},
			runOutput: fmt.Sprintf("hello %s=%s", schemaKey, schemaValue),
			errType:   nil,
		},
		{
			name: "schemaEnvVarIdentifier empty",
			args: args{
				projectRoot:            "nevermind",
				schema:                 schemaValue,
				schemaEnvVarIdentifier: "",
				selectValues:           nil,
				executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: false,
				},
			},
			runOutput: "hello",
			errType:   nil,
		},
		{
			name: "schema value empty",
			args: args{
				projectRoot:            "nevermind",
				schema:                 "",
				schemaEnvVarIdentifier: schemaKey,
				selectValues:           nil,
				executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: false,
				},
			},
			runOutput: "hello",
			errType:   nil,
		},
		{
			name: "command returns an error",
			args: args{
				projectRoot:            "nevermind",
				schema:                 schemaValue,
				schemaEnvVarIdentifier: schemaKey,
				selectValues:           nil,
				executor: DummyCommandExecutor{
					err:       dummyCommandExecutorError{},
					output:    "hello",
					envReturn: true,
				},
			},
			runOutput: fmt.Sprintf("hello %s=%s", schemaKey, schemaValue),
			errType:   dummyCommandExecutorError{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DbtRun(tt.args.projectRoot, tt.args.schema, tt.args.schemaEnvVarIdentifier, tt.args.selectValues, tt.args.executor)
			if (err != nil) && !errors.Is(err, tt.errType) {
				t.Errorf("DbtRun() error = %v, errType %v", err, tt.errType)
				return
			}
			if got != tt.runOutput {
				t.Errorf("DbtRun() got = %v, schemaName %v", got, tt.runOutput)
			}
		})
	}
}

func generateJsonStringFromResources(resources ...DBTResource) string {
	sb := strings.Builder{}
	for _, resource := range resources {
		j, _ := json.Marshal(resource)
		sb.WriteString(string(j))
		sb.WriteString("\n")
	}
	return sb.String()
}
