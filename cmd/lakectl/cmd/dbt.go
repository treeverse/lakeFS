package cmd

import (
	"os/exec"
	"regexp"

	"github.com/spf13/cobra"
)

var (
	schemaRegex           = regexp.MustCompile(`schema: (.+)`)
	materializedSelection = []string{"config.materialized:table", "config.materialized:incremental"}
)

const (
	schemaEnvVarName     = "LAKEFS_SCHEMA"
	macrosDirName        = "macros"
	generateSchemaName   = "generate_schema_name.sql"
	continueOnSchemaFlag = "continue-on-schema-exists"
	createBranchFlag     = "create-branch"
	resourceType         = "model"
)

func ExecuteCommand(cmd *exec.Cmd) ([]byte, error) {
	return cmd.Output()
}

var dbtCmd = &cobra.Command{
	Use:   "dbt",
	Short: "Integration with dbt commands",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(dbtCmd)
}
