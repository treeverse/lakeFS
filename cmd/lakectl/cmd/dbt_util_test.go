package cmd_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd"
)

type DummyCommandExecutor struct {
	err       error
	output    string
	envReturn bool
}

var errDummyCommandFailed = errors.New("boom")

func (dce DummyCommandExecutor) ExecuteCommand(cmd *exec.Cmd) ([]byte, error) {
	sb := strings.Builder{}
	sb.WriteString(dce.output)
	if len(cmd.Env) > 0 && dce.envReturn {
		sb.WriteString(fmt.Sprintf(" %s", cmd.Env[len(cmd.Env)-1]))
	}
	return []byte(sb.String()), dce.err
}

func TestDbtDebug(t *testing.T) {
	type args struct {
		projectRoot string
		schemaRegex *regexp.Regexp
		executor    DummyCommandExecutor
	}
	schemaRegex := regexp.MustCompile(`schema: (.+)`)

	type dbtDebugTestCase struct {
		Name       string
		Args       args
		SchemaName string
		WantErr    bool
	}
	tests := []dbtDebugTestCase{
		{
			Name: "Sunny day Flow",
			Args: args{
				projectRoot: "nevermind",
				schemaRegex: schemaRegex,
				executor:    DummyCommandExecutor{output: "schema: success", err: nil},
			},
			SchemaName: "success",
			WantErr:    false,
		},
		{
			Name: "Command failed with error and output string",
			Args: args{
				projectRoot: "nevermind",
				schemaRegex: nil,
				executor:    DummyCommandExecutor{output: "some error output", err: fmt.Errorf("BOOM")},
			},
			SchemaName: "some error output",
			WantErr:    true,
		},
		{
			Name: "Submatch is nil (output is not in the regex)",
			Args: args{
				projectRoot: "nevermind",
				schemaRegex: schemaRegex,
				executor:    DummyCommandExecutor{output: "no schema definition", err: nil},
			},
			SchemaName: "",
			WantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			schema, err := cmd.DBTDebug(tt.Args.projectRoot, tt.Args.schemaRegex, tt.Args.executor.ExecuteCommand)
			if err != nil && tt.WantErr == false {
				t.Errorf("DBTDebug() error = %v, but no error was expected", err)
				return
			}
			if schema != tt.SchemaName {
				t.Errorf("DBTDebug() schema = %v, schemaName %v", schema, tt.SchemaName)
			}
		})
	}
}

func TestDbtLsToJson(t *testing.T) {
	DBTResources := make([]cmd.DBTResource, 0)
	DBTResources = append(DBTResources, cmd.DBTResource{Schema: "schema1", Alias: "alias1"}, cmd.DBTResource{Schema: "schema2", Alias: "alias2"})
	type args struct {
		ProjectRoot  string
		ResourceType string
		SelectValues []string
		Executor     DummyCommandExecutor
	}
	type dbtLsToJsonTestCase struct {
		Name         string
		Args         args
		DbtResources []cmd.DBTResource
		ErrType      error
	}
	tests := []dbtLsToJsonTestCase{
		{
			Name: "Sunny day Flow",
			Args: args{
				ProjectRoot:  "nevermind",
				ResourceType: "nevermind",
				SelectValues: []string{"nevermind"},
				Executor:     DummyCommandExecutor{err: nil, output: toJSON(DBTResources...)},
			},
			DbtResources: DBTResources,
			ErrType:      nil,
		},
		{
			Name: "command returns an error",
			Args: args{
				ProjectRoot:  "nevermind",
				ResourceType: "nevermind",
				SelectValues: []string{"nevermind"},
				Executor:     DummyCommandExecutor{err: errDummyCommandFailed, output: ""},
			},
			DbtResources: nil,
			ErrType:      errDummyCommandFailed,
		},
		{
			Name: "command returns invalid json",
			Args: args{
				ProjectRoot:  "nevermind",
				ResourceType: "nevermind",
				SelectValues: []string{"nevermind"},
				Executor:     DummyCommandExecutor{err: nil, output: "invalid json"},
			},
			DbtResources: nil,
			ErrType:      &json.SyntaxError{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			dbtResources, err := cmd.DBTLsToJSON(tt.Args.ProjectRoot, tt.Args.ResourceType, tt.Args.SelectValues, tt.Args.Executor.ExecuteCommand)
			if err != nil && reflect.TypeOf(err) != reflect.TypeOf(tt.ErrType) {
				t.Errorf("DBTLsToJSON() error type=%T, expected type=%T", err, tt.ErrType)
				return
			}
			if !reflect.DeepEqual(dbtResources, tt.DbtResources) {
				t.Errorf("DBTLsToJSON() dbtResources = %v, schemaName %v", dbtResources, tt.DbtResources)
			}
		})
	}
}

func TestDbtRun(t *testing.T) {
	schemaKey := "theschema"
	schemaValue := "myschema"
	type args struct {
		Schema                 string
		SchemaEnvVarIdentifier string
		SelectValues           []string
		Executor               DummyCommandExecutor
	}
	type dbtRunTestCase struct {
		Name      string
		Args      args
		RunOutput string
		Err       error
	}
	tests := []dbtRunTestCase{
		{
			Name: "Sunny day flow",
			Args: args{
				Schema:                 schemaValue,
				SchemaEnvVarIdentifier: schemaKey,
				SelectValues:           nil,
				Executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: true,
				},
			},
			RunOutput: fmt.Sprintf("hello %s=%s", schemaKey, schemaValue),
			Err:       nil,
		},
		{
			Name: "schemaEnvVarIdentifier empty",
			Args: args{
				Schema:                 schemaValue,
				SchemaEnvVarIdentifier: "",
				SelectValues:           nil,
				Executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: false,
				},
			},
			RunOutput: "hello",
			Err:       nil,
		},
		{
			Name: "schema value empty",
			Args: args{
				Schema:                 "",
				SchemaEnvVarIdentifier: schemaKey,
				SelectValues:           nil,
				Executor: DummyCommandExecutor{
					err:       nil,
					output:    "hello",
					envReturn: false,
				},
			},
			RunOutput: "hello",
			Err:       nil,
		},
		{
			Name: "command returns an error",
			Args: args{
				Schema:                 schemaValue,
				SchemaEnvVarIdentifier: schemaKey,
				SelectValues:           nil,
				Executor: DummyCommandExecutor{
					err:       errDummyCommandFailed,
					output:    "hello",
					envReturn: true,
				},
			},
			RunOutput: fmt.Sprintf("hello %s=%s", schemaKey, schemaValue),
			Err:       errDummyCommandFailed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			runOutput, err := cmd.DBTRun("nevermind", tt.Args.Schema, tt.Args.SchemaEnvVarIdentifier, tt.Args.SelectValues, tt.Args.Executor.ExecuteCommand)
			if (err != nil) && !errors.Is(err, tt.Err) {
				t.Errorf("DBTRun() error = %v, expected error = %v", err, tt.Err)
				return
			}
			if runOutput != tt.RunOutput {
				t.Errorf("DBTRun() runOutput = %v, expected = %v", runOutput, tt.RunOutput)
			}
		})
	}
}

func toJSON(resources ...cmd.DBTResource) string {
	sb := strings.Builder{}
	for _, resource := range resources {
		j, _ := json.Marshal(resource)
		sb.WriteString(string(j))
		sb.WriteString("\n")
	}
	return sb.String()
}
