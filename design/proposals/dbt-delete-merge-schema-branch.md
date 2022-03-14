# Add DBT delete and merge branch-schema operations

## Background

When using dbt alongside lakeFS, the first step is to create a branch to work on and then create a dbt schema dedicated for that branch (that might have the same name of the branch [by default] or a custom one). #2988 added an option to use `lakectl dbt create-branch-schema --create-branch` to create the dedicated branch and schema at once.

After running dbt over that branch and conducting tests and validations we would want to either: 
- delete the branch and the schema associated with it
- merge them into master/main/production/other branch
- both.

## Requirements

The users that use the dbt command are, well, dbt users. They will interact with the `lakectl` tool in the context of the dbt command (and some basic commands such as `branch`, `commit` and `merge`).
Adding the `delete-branch-schema` and `merge-branch-schema` subcommands will make it easier for them to integrate lakeFS into their framework.

### Goals 
- Delete schema, tables, partitions and associated metadata from the metastore using a single command.
- Merge schema, tables, partitions and associated metadata from the metastore using a single command.

### Non-goals

- Deleting a branch before/after deleting an associated schema.
- Merging a branch before/after deleting an associated schema.

## Suggestions

- `delete-branch-schema` will allow deleting a schema based on a branch name.
- `merge-branch-schema` will allow merging a schema and all dbt model underneath it to a destination schema by branch names.

### `delete-branch-schema` Usage

`lakectl dbt delete-branch-schema --branch <branch name>`: Delete the schema associated with the input branch name, e.g. `lakectl dbt delete-branch-schema --branch dbt_jon` will delete the schema associated with (mapped to) the `dbt_jon` branch.

### `merge-branch-schema` Usage

`lakectl dbt merge-branch-schema --source-branch <source branch name> --destination-branch <destination branch name>`: Merge the source schema (associated with the source branch name) to the destination schema (associated with the destination branch name), e.g. `lakectl dbt merge-branch-schema --source-branch dbt_jon --destination-branch master` will merge the schema and models associated with `dbt_jon` branch to `main` branch.

## Required change

When creating a new schema for a branch using the `lakectl dbt create-branch-schema` command, there will be added a mapping of that branch to that schema in a file (probably under the dbt project directory).  
This is necessary because the branch and the schema might have different names (when using the `--to-schema` flag) and not use the default behavior (which is branch and schema having the same name).

### Suggestion
When the users start working with lakectl and dbt together they'll also need to map the main/master/production branch they are working with to a schema. If not, the target schema will get mapped to it by default. We can do that by adding a `lakectl dbt map-branch-schema` command.
