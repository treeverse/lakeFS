# Add DBT delete and merge branch-schema operations

## Background

When using dbt alongside lakeFS, the first step is to create a branch to work on and then create a dbt schema dedicated for that branch (that might have the same name of the branch [by default] or a custom one). #2988 added an option to use `lakectl dbt create-branch-schema` to create the dedicated branch and schema at once.

After running dbt over that branch and conducting tests and validations we would want to either: 
- delete the branch and the schema associated with it
- merge them into master/main/production/other branch
- both.

`lakectl dbt delete-branch-schema` will allow deleting a branch and the schema associated with it, or just the schema.  
`lakectl dbt merge-branch-schema` will allow merging a branch to a destination branch and the schema associated with it.

### Requirements

The users that use the dbt command are, well, dbt users. They will interact with the `lakectl` tool in the context of the dbt command (and some basic commands such as `commit` and `merge`).
Adding the `delete-branch-schema` and `merge-branch-schema` subcommands will make it easier for them to integrate lakeFS into their framework.

## `delete-branch-schema` Usage

1. `lakectl dbt delete-branch-schema --branch <branch name> --schema <schema name>`: Delete branch and schema with the given names.
2. `lakectl dbt delete-branch-schema --branch <branch name>`: Delete the branch from the repository that the dbt project's target schema resides at. The command will also try to delete a schema with the same name as the branch (when creating a branch using the `lakectl dbt create-branch-schema` command, the default schema name is the branch name).
3. `lakectl dbt delete-branch-schema --schema <schema name>`: Delete the given schema.
4. `lakectl dbt delete-branch-schema`: Failure. Must specify at least `--branch` or `--schema`

## `merge-branch-schema` Usage

1. `lakectl dbt merge-branch-schema <source branch name> <destination branch name>`: Merge the source branch to the destination branch and use the default schemas of the source and destination (branches names).
2. `lakectl dbt merge-branch-schema <source branch name> <destination branch name> --source-schema <source schema name>`: Merge source branch to destination branch in the repository that the dbt project's target schema resides at. Merge the custom source schema to the default destination schema (branch name).
3. `lakectl dbt merge-branch-schema <source branch name> <destination branch name> --destination-schema <destination schema name>`: Merge source branch to destination branch in the repository that the dbt project's target schema resides at. Merge the default source schema (branch name) to a custom destination schema.
4. `lakectl dbt merge-branch-schema <source branch name> <destination branch name> --source-schema <source schema name> --destination-schema <destination schema name>`: Merge source branch to destination branch in the repository that the dbt project's target schema resides at. Merge the source schema to the destination schema.
5. `lakectl dbt merge-branch-schema <source branch name> <destination branch name> --target-schema`:  Merge the source branch to the destination branch and use the default source schema (branch name) and the dbt target schema specified in the dbt project.
7. `lakectl dbt merge-branch-schema`: Failure. Must specify at least `<source branch name> <destination branch name>`

