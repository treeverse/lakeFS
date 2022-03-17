# Add DBT delete and merge branch-schema operations

---

## Background

To use dbt, users must have an already initialized schema under their dbt profile's target.    
When using dbt alongside lakeFS, the schema above should have a branch's URI as its location.
When the users want to branch out and run dbt over a different branch, they first need to create a branch to work on and then create a dbt schema dedicated for that branch (that might have the same name of the branch [by default] or a custom one). #2988 added an option to use `lakectl dbt create-branch-schema --create-branch` to create the dedicated branch and schema at once.

After running dbt over that branch and conducting tests and validations we would want to either: 
- delete the branch and the schema associated with it
- merge them into master/main/production/other branch
- both.

## Goals 
- Enable deletion of schemas created for dbt branches using lakectl.
- Enable merge of schemas created for dbt branches using lakectl.

## Non-goals

- Couple branch delete and schema delete operations.
- Couple branch merge and schema merge operations.

## Suggestions

- `delete-branch-schema` will allow deleting a schema based on a branch name.
- `merge-branch-schema` will allow merging a schema to a destination schema by branch names.

### `delete-branch-schema` Usage

`lakectl dbt delete-branch-schema --branch <branch name>`: Delete the schema associated with the input branch name.
* Example: `lakectl dbt delete-branch-schema --branch dbt_jon` will delete the schema associated with the `dbt_jon` branch.

### `merge-branch-schema` Usage

`lakectl dbt merge-branch-schema --source-branch <source branch name> --destination-branch <destination branch name>`: Merge the source schema (associated with the source branch name) to the destination schema (associated with the destination branch name)
* Example: `lakectl dbt merge-branch-schema --source-branch dbt_jon --destination-branch master` will merge the schema associated with the `dbt_jon` branch to the schema associated with the `master` branch.

## Required change

In order to implement the new commands, there's a need to map branches to schemas.
This is necessary because the branch and the schema might have different names (when adding the `--to-schema <custom name>` flag when creating the schema using the `lakectl dbt create-branch-schema` command) and not use the default behavior (which is the same name for the branch and schema).

### Suggested implementation

The mapping of branches to schemas will be facilitated under the branch's (or more accurately, the commit that this branch is pointing to) metadata property.
The `schema` key will be used to point to the name of the schema that is configured with the URI of the containing branch: `schema -> <schema name>`.  
When users start working with lakectl and dbt together they'll also need to map the main/master/production branch they are working with to a schema, otherwise they will be blocked from working with `lakectl dbt` commands.
This is necessary because, as mentioned in the [Background section](#background), when users first initialize a schema it points to the branch's location, but they didn't take any action to **map the branch to the schema**.
It might cause a problem if, for example, a user branches out of the main branch (the branch that the target schema's location is pointing to), commits changes with the newly created schema, merges the branches and then wants to merge the schemas. The user wouldn't be able to merge the schemas because the main branch is not aware of the schema associated with it.  
We can allow users to map a branch to a schema by providing a `lakectl dbt map-branch-schema <branch> <schema>` command. 
* The functionality of this subcommand is not limited to `lakectl dbt` and can be used in the `lakectl metastore` command suite as well. It's still suggested under the `lakectl dbt` command suite to make it easier for dbt users to integrate lakeFS to their development flow.

## Future improvements

### Switch used schema by branch

Make it easier for users to switch the schema they are running their dbt commands over.
Currently, users need to manually change the schema they want to run their dbt commands over in the profile target's schema property. This means they need to be aware of changes they make to the dbt profile file and not check-in those changes to their version control system later. It also means they need to book the different branch to schema mapping.
- `lakectl dbt use-schema --branch <branch name>`: Use the provided branch's mapped schema within the terminal that the user uses to run dbt commands.

