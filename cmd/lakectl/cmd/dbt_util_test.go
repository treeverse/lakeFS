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
	const projectRoot = "nevermind"
	const resourceType = "nevermind"
	selectValues := []string{"nevermind"}

	t.Run("simple", func(t *testing.T) {
		expectedResources := []cmd.DBTResource{
			{Schema: "schema1", Alias: "alias1"},
			{Schema: "schema2", Alias: "alias2"},
		}
		executor := DummyCommandExecutor{output: toJSON(expectedResources...)}
		dbtResources, err := cmd.DBTLsToJSON(projectRoot, resourceType, selectValues, executor.ExecuteCommand)
		if err != nil {
			t.Fatalf("DBTLsToJSON() error = %v, expected no error", err)
		}
		if !reflect.DeepEqual(dbtResources, expectedResources) {
			t.Errorf("DBTLsToJSON() dbtResources = %v, schemaName %v", dbtResources, expectedResources)
		}
	})

	t.Run("error", func(t *testing.T) {
		executor := DummyCommandExecutor{err: errDummyCommandFailed}
		_, err := cmd.DBTLsToJSON(projectRoot, resourceType, selectValues, executor.ExecuteCommand)
		if !errors.Is(err, errDummyCommandFailed) {
			t.Fatalf("DBTLsToJSON() error = %v, expected = %s", err, errDummyCommandFailed)
		}
	})

	t.Run("syntax", func(t *testing.T) {
		executor := DummyCommandExecutor{output: "invalid json"}
		_, err := cmd.DBTLsToJSON(projectRoot, resourceType, selectValues, executor.ExecuteCommand)
		var cmdErr *json.SyntaxError
		if !errors.As(err, &cmdErr) {
			t.Fatalf("DBTLsToJSON() error = %v, expected json.SyntaxErr", err)
		}
	})
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
