---
title: 6️⃣ Using Actions and Hooks in lakeFS
description: lakeFS quickstart / Use Actions and Hooks to enforce conditions when committing and merging changes
parent: ⭐ Quickstart ⭐
nav_order: 30
has_children: false
next: ["Resources for learning more about lakeFS", "./learning-more-lakefs.html"]
previous: ["Rollback the changes", "./rollback.html"]
---

# Actions and Hooks in lakeFS 🪝

When we interact with lakeFS it can be useful to have certain checks performed at stages along the way. Let's see how [actions in lakeFS](/hooks/) can be of benefit here. 

We're going to enforce a rule that when a commit is made to any branch that begins with `etl`: 

* the commit message must not be blank
* there must be `job_name` and `version` metadata
* the `version` must be numeric

To do this we'll create an _action_. In lakeFS, an action specifies one or more events that will trigger it, and references one or more _hooks_ to run when triggered. Actions are YAML files written to lakeFS under the `_lakefs_actions/` folder of the lakeFS repository.

_Hooks_ can be either a [Lua](/hooks/lua.html) script that lakeFS will execute itself, an external [web hook](/hooks/webhook.html), or an [Airflow DAG](/hooks/airflow.html). In the example here we're using a Lua hook.

## Configuring the Action

1. In lakeFS create a new branch called `add_action`. You can do this through the UI or with `lakectl`: 

    ```bash
    docker exec lakefs \
        lakectl branch create \
                lakefs://quickstart/add_action \
                        --source lakefs://quickstart/main
    ```

1. Open up your favorite text editor (or emacs), and paste the following YAML: 

    ```yaml
    name: Check Commit Message and Metadata
    on:
    pre-commit:
        branches: 
        - etl**
    hooks:
    - id: check_metadata
        type: lua
        properties:
        script: |
            commit_message=action.commit.message
            if commit_message and #commit_message>0 then
                print("✅ The commit message exists and is not empty: " .. commit_message)
            else
                error("\n\n❌ A commit message must be provided")
            end

            job_name=action.commit.metadata["job_name"]
            if job_name == nil then
                error("\n❌ Commit metadata must include job_name")
            else
                print("✅ Commit metadata includes job_name: " .. job_name)
            end

            version=action.commit.metadata["version"]
            if version == nil then
                error("\n❌ Commit metadata must include version")
            else
                print("✅ Commit metadata includes version: " .. version)
                if tonumber(version) then
                    print("✅ Commit metadata version is numeric")
                else
                    error("\n❌ Version metadata must be numeric: " .. version)
                end
            end
    ```

1. Save this file as `/tmp/check_commit_metadata.yml`

    * You can save it elsewhere, but make sure you change the path below when uploading

1. Upload the `check_commit_metadata.yml` file to the `add_action` branch under `_lakefs_actions/`. As above, you can use the UI (make sure you select the correct branch when you do), or with `lakectl`:

    ```bash
    docker exec lakefs \
        lakectl fs upload \
            lakefs://quickstart/add_action/_lakefs_actions/check_commit_metadata.yml \
            --source /tmp/check_commit_metadata.yml
    ```

1. Go to the **Uncommitted Changes** tab in the UI, and make sure that you see the new file in the path shown: 

    <img width="75%" src="/assets/img/quickstart/hooks-00.png" alt="lakeFS Uncommitted Changes view showing a file called `check_commit_metadata.yml` under the path `_lakefs_actions/`" class="quickstart"/>

    Click **Commit Changes** and enter a suitable message to commit this new file to the branch. 

1. Now we'll merge this new branch into `main`. From the **Compare** tab in the UI compare the `main` branch with `add_action` and click **Merge**

    <img width="75%" src="/assets/img/quickstart/hooks-01.png" alt="lakeFS Compare view showing the difference between `main` and `add_action` branches" class="quickstart"/>

## Testing the Action

Let's remind ourselves what the rules are that the action is going to enforce. 

> When a commit is made to any branch that begins with `etl`: 

> * the commit message must not be blank
> * there must be `job_name` and `version` metadata
> * the `version` must be numeric

We'll start by creating a branch that's going to match the `etl` pattern, and then go ahead and commit a change and see how the action works. 

1. Create a new branch (see above instructions on how to do this if necessary) called `etl_20230504`. Make sure you use `main` as the source branch. 

    In your new branch you should see the action that you created and merged above: 

    <img width="75%" src="/assets/img/quickstart/hooks-02.png" alt="lakeFS branch etl_20230504 with object /_lakefs_actions/check_commit_metadata.yml" class="quickstart"/>

1. To simulate an ETL job we'll use the built-in DuckDB editor to run some SQL and write the result back to the lakeFS branch. 

    Open the `lakes.parquet` file on the `etl_20230504` branch from the **Objects** tab. Replace the SQL statement with the following: 

    ```sql
    COPY (
        WITH src AS (
            SELECT lake_name, country, depth_m,
                RANK() OVER ( ORDER BY depth_m DESC) AS lake_rank
            FROM READ_PARQUET('lakefs://quickstart/etl_20230504/lakes.parquet'))
        SELECT * FROM SRC WHERE lake_rank <= 10
    ) TO 'lakefs://quickstart/etl_20230504/top10_lakes.parquet'    
    ```

1. Head to the **Uncommitted Changes** tab in the UI and notice that there is now a file called `top10_lakes.parquet` waiting to be committed. 

    <img width="75%" src="/assets/img/quickstart/hooks-03.png" alt="lakeFS branch etl_20230504 with uncommitted file top10_lakes.parquet" class="quickstart"/>

    Now we're ready to start trying out the commit rules, and seeing what happens if we violate them.
    
1. Click on **Commit Changes**, leave the _Commit message_ blank, and click **Commit Changes** to confirm. 

    Note that the commit fails because the hook did not succeed
    
    `pre-commit hook aborted`
    
    with the output from the hook's code displayed

    `❌ A commit message must be provided`

    <img width="75%" src="/assets/img/quickstart/hooks-04.png" alt="lakeFS blocking an attempt to commit with no commit message" class="quickstart"/>

1. Do the same as the previous step, but provide a message this time: 

    <img width="75%" src="/assets/img/quickstart/hooks-05.png" alt="A commit to lakeFS with commit message in place" class="quickstart"/>

    The commit still fails as we need to include metadata too, which is what the error tells us

    `❌ Commit metadata must include job_name`

1. Repeat the **Commit Changes** dialog and use the **Add Metadata field** to add the required metadata: 

    <img width="75%" src="/assets/img/quickstart/hooks-06.png" alt="A commit to lakeFS with commit message and metadata in place" class="quickstart"/>

    We're almost there, but this still fails (as it should), since the version is not entirely numeric but includes `v` and `ß`: 

    `❌ Version metadata must be numeric: v1.00ß`

    Repeat the commit attempt specify the version as `1.00` this time, and rejoice as the commit succeeds

    <img width="75%" src="/assets/img/quickstart/hooks-07.png" alt="Commit history in lakeFS showing that the commit met the rules set by the action and completed successfully." class="quickstart"/>

---

You can view the history of all action runs from the **Action** tab: 

<img width="75%" src="/assets/img/quickstart/hooks-08.png" alt="Action run history in lakeFS" class="quickstart"/>


## Bonus Challenge

And so with that, this quickstart for lakeFS draws to a close. If you're simply having _too much fun_ to stop then here's an exercise for you. 

Implement the requirement from the beginning of this quickstart *correctly*, such that you write `denmark-lakes.parquet` in the respective branch and successfully merge it back into main. Look up how to list the contents of the `main` branch and verify that it looks like this:

```
object          2023-03-21 17:33:51 +0000 UTC    20.9 kB         denmark-lakes.parquet
object          2023-03-21 14:45:38 +0000 UTC    916.4 kB        lakes.parquet
```

# Finishing Up

Once you've finished the quickstart, shut down your local environment with the following command: 

```bash
docker stop lakefs
```
