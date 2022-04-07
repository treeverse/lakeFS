#!/bin/bash -ex
#

dbt run
lakectl commit -m 'run dbt on main' lakefs://example/main
lakectl branch create lakefs://example/test -s lakefs://example/main

# create schema for new branch
lakectl dbt generate-schema-macro
lakectl dbt create-branch-schema --branch test  --to-schema test

# change schema and test
LAKEFS_SCHEMA=test dbt run --select star_rating

DBT_BRANCH_CREATION_NAME=testbranchcreation
lakectl dbt create-branch-schema --create-branch --branch ${DBT_BRANCH_CREATION_NAME}

# change schema and test branch creation
LAKEFS_SCHEMA=${DBT_BRANCH_CREATION_NAME} dbt run --select star_rating

# todo(Guys) - uncomment and test once create-symlink supports hive, in order not to create many tables on our glue catalog
# create symlinks
#lakectl metastore create-symlink --branch test --from-schema test --from-table amazon_reviews --to-schema esti_system_testing --to-table ${TO_TABLE} --repo example --path dbt/amazon_reviews


