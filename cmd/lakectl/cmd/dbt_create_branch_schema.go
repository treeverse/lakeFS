package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/pkg/metastore"
	mserrors "github.com/treeverse/lakefs/pkg/metastore/errors"

	"github.com/spf13/cobra"
)

var dbtCreateBranchSchema = &cobra.Command{
	Use:        "create-branch-schema",
	Short:      "Creates a new schema dedicated for branch and clones all dbt models to new schema",
	Example:    "lakectl dbt create-branch-schema --branch <branch-name>",
	Deprecated: "Upcoming releases of lakectl will no longer support this command.",
	Run: func(cmd *cobra.Command, args []string) {
		clientType := Must(cmd.Flags().GetString("from-client-type"))
		branchName := Must(cmd.Flags().GetString("branch"))
		shouldCreateBranch := Must(cmd.Flags().GetBool(createBranchFlag))
		toSchema := Must(cmd.Flags().GetString("to-schema"))
		projectRoot := Must(cmd.Flags().GetString("project-root"))
		skipViews := Must(cmd.Flags().GetBool("skip-views"))
		dbfsLocation := Must(cmd.Flags().GetString("dbfs-location"))
		continueOnError := Must(cmd.Flags().GetBool("continue-on-error"))
		continueOnSchemaExists := Must(cmd.Flags().GetBool(continueOnSchemaFlag))

		if strings.TrimSpace(toSchema) == "" {
			toSchema = branchName
		}

		metastoreClient, closeClient := getMetastoreClient(clientType, "")
		defer closeClient()

		// in order to generate views in new schema dbt should contain a macro script allowing lakectl to dynamically set the schema
		// this could be skipped by using the skip-views flag
		if !skipViews {
			err := ValidateGenerateSchemaMacro(projectRoot, macrosDirName, generateSchemaName, schemaEnvVarName)
			if err != nil {
				DieErr(err)
			}
		}

		schema := retrieveSchema(projectRoot)
		ctx := cmd.Context()

		if shouldCreateBranch {
			handleBranchCreation(ctx, schema, branchName, metastoreClient)
		}

		copySchemaWithDbtTables(ctx, continueOnError, continueOnSchemaExists, schema, toSchema, branchName, dbfsLocation, projectRoot, metastoreClient)

		if skipViews {
			fmt.Println("skipping views")
		} else {
			createViews(projectRoot, toSchema)
		}

		fmt.Printf("create-branch-schema done!\nyour new schema is %s\nyou can now edit your dbt profile and run dbt on branch %s\n", toSchema, branchName)
	},
}

func createViews(projectRoot, toSchema string) {
	output, err := DBTRun(projectRoot, toSchema, schemaEnvVarName, []string{"config.materialized:view"}, ExecuteCommand)
	if err != nil {
		DieFmt("Failed creating views with err: %v\nOutput:\n%s", err, output)
	}
	fmt.Println("views created")
}

// copySchemaWithDbtTables create a copy of fromSchema and copies all dbt models materialized as table or incremental
func copySchemaWithDbtTables(ctx context.Context, continueOnError, continueOnSchemaExists bool, fromSchema, toSchema, branchName, dbfsLocation, projectRoot string, client metastore.Client) {
	err := metastore.CopyDB(ctx, client, client, fromSchema, toSchema, branchName, dbfsLocation)
	switch {
	case err == nil:
		fmt.Printf("schema %s created\n", toSchema)
	case !errors.Is(err, mserrors.ErrSchemaExists):
		DieErr(err)
	case continueOnSchemaExists:
		fmt.Printf("schema %s already exists, proceeding with existing schema\n", toSchema)
	default:
		DieFmt("Schema %s exists, change schema or use %s flag", toSchema, continueOnSchemaFlag)
	}

	models := getDBTTables(projectRoot)
	if err := copyModels(ctx, models, continueOnError, fromSchema, toSchema, branchName, dbfsLocation, client); err != nil {
		DieErr(err)
	}
}

func copyModels(ctx context.Context, models []DBTResource, continueOnError bool, fromSchema, toSchema, branchName, dbfsLocation string, client metastore.Client) error {
	for _, m := range models {
		if fromSchema != m.Schema {
			fmt.Printf("skipping %s.%s, not in schema: %s", m.Schema, m.Alias, fromSchema)
			continue
		}
		err := metastore.CopyOrMerge(ctx, client, client, m.Schema, m.Alias, toSchema, m.Alias, branchName, "", nil, false, dbfsLocation)
		if err != nil {
			if !continueOnError {
				return err
			}
			fmt.Printf("copy %s.%s -> %s.%s  failed: %s\n", m.Schema, m.Alias, toSchema, m.Alias, err)
		} else {
			fmt.Printf("copied %s.%s -> %s.%s\n", m.Schema, m.Alias, toSchema, m.Alias)
		}
	}
	return nil
}

func retrieveSchema(projectRoot string) string {
	schemaOutput, err := DBTDebug(projectRoot, schemaRegex, ExecuteCommand)
	if err != nil {
		fmt.Println(schemaOutput)
		DieErr(err)
	}
	fmt.Println("dbt debug succeeded with schemaOutput:", schemaOutput)
	return schemaOutput
}

func handleBranchCreation(ctx context.Context, schema, branchName string, metastoreClient metastore.Client) {
	sourceRepo, sourceBranch, err := ExtractRepoAndBranchFromDBName(ctx, schema, metastoreClient)
	if err != nil {
		DieFmt(`Given schema has no source branch associated with it %s`, err)
	}
	CreateBranch(ctx, sourceRepo, sourceBranch, branchName)
}

//nolint:gochecknoinits
func init() {
	_ = dbtCreateBranchSchema.Flags().String("branch", "", "requested branch")
	_ = dbtCreateBranchSchema.MarkFlagRequired("branch")
	_ = dbtCreateBranchSchema.Flags().String("from-client-type", "", "metastore type [hive, glue]")
	_ = dbtCreateBranchSchema.Flags().String("to-schema", "", "destination schema name [default is branch]")
	_ = dbtCreateBranchSchema.Flags().String("project-root", ".", "location of dbt project")
	_ = dbtCreateBranchSchema.Flags().String("dbfs-location", "", "")
	_ = dbtCreateBranchSchema.Flags().Bool("skip-views", false, "")
	_ = dbtCreateBranchSchema.Flags().Bool("continue-on-error", false, "prevent command from failing when a single table fails")
	_ = dbtCreateBranchSchema.Flags().Bool(continueOnSchemaFlag, false, "allow running on existing schema")
	_ = dbtCreateBranchSchema.Flags().Bool(createBranchFlag, false, "create a new branch for the schema")

	dbtCmd.AddCommand(dbtCreateBranchSchema)
}
