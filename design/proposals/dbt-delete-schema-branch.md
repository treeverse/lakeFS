# `lakectl dbt delete-branch-schema`

## Description
When using dbt alongside lakeFS, the first thing we would want to do is to create a branch to work on.
After running dbt over that branch and conducting tests and validations we would like to delete the branch and the schema that was associated with it (merge is for another proposal).  
`lakectl dbt delete-branch-schema` will allow the deletion of a branch and the schema associated with it.  
### Why do we need it?
The users that use the dbt command are, well, dbt users. They will interact with the `lakectl` tool in the context of the dbt command (and some basic commands such as `commit` and `merge`).
Adding the `delete-branch-schema` subcommand will make it easier for dbt users to integrate lakeFS into their framework.

## Usage
1. `lakectl dbt delete-branch-schema --branch <branch name>`: Delete the branch from the repository that the dbt project's target schema resides at. The command will also try to delete a schema with the same name as the branch (when creating a branch using the `lakectl dbt create-branch-schema` command, the default schema name is the branch name).
2. `lakectl dbt delete-branch-schema --schema <schema name>`: Delete the given schema.
3. `lakectl dbt delete-branch-schema --branch <branch name> --schema <schema name>`: Delete branch and schema with the given names.
4. `lakectl dbt delete-branch-schema`: Failure. Must specify at least `--branch` or `--schema`

