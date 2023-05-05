# Welcome to the Lake!

![Waving Axolotl](/images/waving-axolotl-transparent-w90.gif)

**lakeFS brings software engineering best practices and applies them to data engineering**.

lakeFS provides version control over the data lake, and uses Git-like semantics to create and access those versions. If you know git, you'll be right at home with lakeFS.

With lakeFS, you can use concepts on your data lake such as **branch** to create an isolated version of the data, **commit** to create a reproducible point in time, and **merge** in order to incorporate your changes in one atomic action.

This quickstart will introduce you to some of the core ideas in lakeFS and show what you can do by illustrating the concept of branching, merging, and rolling back changes to data. It's laid out in four short sections.

* ![Query icon](/images/quickstart-step-01-query.png) [Query](#query) the pre-populated data on the `main` branch
* ![Branch icon](/images/quickstart-step-02-branch.png) [Make changes](#branch) to the data on a new branch
* ![Merge icon](/images/quickstart-step-03-merge.png) [Merge](#commit-and-merge) the changed data back to the `main` branch
* ![Rollback icon](/images/quickstart-step-04-rollback.png) [Change our mind](#rollback) and rollback the changes

You might also be interested in this list of [additional lakeFS resources](#resources).

## Setup

If you're reading this within the sample repository on lakeFS then you've already got lakeFS running! In this quickstart we'll reference different ways to perform tasks depending on how you're running lakeFS. See below for how you need to set up your environment for these. 

<details>
  <summary>Docker</summary>

If you're running lakeFS with Docker then all the tools you need (`lakectl`, `duckdb`) are included in the image already. 

```bash
docker run --name lakefs \
           --publish 8000:8000 \
           treeverse/lakefs:latest \
           run --local-settings
```

You can optionally pre-create the user with the alternative command to above: 

```bash
docker run --name lakefs \
           --rm --publish 8000:8000 \
           --entrypoint "/bin/sh" \
           --env "LAKECTL_CREDENTIALS_ACCESS_KEY_ID=AKIA-EXAMPLE-KEY" \
           --env "LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY=EXAMPLE-SECRET" \
           --env "LAKECTL_SERVER_ENDPOINT_URL=http://localhost:8000" \
           treeverse/lakefs:latest -c \
           "lakefs setup --local-settings --user-name admin --access-key-id \"AKIA-EXAMPLE-KEY\" --secret-access-key \"EXAMPLE-SECRET\"; \ 
            lakefs run --local-settings"
```

</details>

<details>
  <summary>Local install</summary>

1. When you download [lakeFS from the GitHub repository](https://github.com/treeverse/lakeFS/releases) the distribution includes the `lakectl` tool. 

    Add this to your `$PATH`, or when invoking it reference it from the downloaded folder
2. [Configure](https://docs.lakefs.io/reference/cli.html#configuring-credentials-and-api-endpoint) `lakectl` by running

    ```bash
    lakectl config
    ```
3. Install [DuckDB](https://duckdb.org/docs/installation/)

</details>


<a name="query"></a>

# Let's get started 😺

_We'll start off by querying the sample data to orient ourselves around what it is we're working with. The lakeFS server has been loaded with a sample parquet datafile. Fittingly enough for a piece of software to help users of data lakes, the `lakes.parquet` file holds data about lakes around the world._

_You'll notice that the branch is set to `main`. This is conceptually the same as your main branch in Git against which you develop software code._

<img width="75%" src="/api/v1/repositories/quickstart/refs/main/objects?path=images%2Frepo-contents.png" alt="The lakeFS objects list with a highlight to indicate that the branch is set to main." class="quickstart"/>

_Let's have a look at the data, ahead of making some changes to it on a branch in the following steps._.

Click on [`lakes.parquet`](object?ref=main&path=lakes.parquet) from the object browser and notice that the built-it DuckDB runs a query to show a preview of the file's contents.

<img width="75%" src="/api/v1/repositories/quickstart/refs/main/objects?path=images%2Fduckdb-main-01.png" alt="The lakeFS object viewer with embedded DuckDB to query parquet files. A query has run automagically to preview the contents of the selected parquet file." class="quickstart"/>

_Now we'll run our own query on it to look at the top five countries represented in the data_.

Copy and paste the following SQL statement into the DuckDB query panel and click on Execute.

```sql
SELECT   country, COUNT(*)
FROM     READ_PARQUET(LAKEFS_OBJECT('quickstart', 'main', 'lakes.parquet'))
GROUP BY country
ORDER BY COUNT(*.
DESC LIMIT 5;
```

<img width="75%" src="/api/v1/repositories/quickstart/refs/main/objects?path=images%2Fduckdb-main-02.png" alt="An embedded DuckDB query showing a count of rows per country in the dataset." class="quickstart"/>

_Next we're going to make some changes to the data—but on a development branch so that the data in the main branch remains untouched._

<a name="branch"></a>
# Create a Branch 🪓

_lakeFS uses branches in a similar way to Git. It's a great way to isolate changes until, or if, we are ready to re-integrate them. lakeFS uses a copy-on-write technique which means that it's very efficient to create branches of your data._

_Having seen the lakes data in the previous step we're now going to create a new dataset to hold data only for lakes in 🇩🇰 Denmark. Why? Well, because 😄_

_The first thing we'll do is create a branch for us to do this development against. Choose one of the following methods depending on your preferred interface and how you're running lakeFS._


<details>
  <summary>Web UI</summary>

From the [branches](./branches) page, click on **Create Branch**. Call the new branch `denmark-lakes` and click on **Create**

![lakeFS Create Branch dialog](/images/create-lakefs-branch.png)

</details>

<details>
  <summary>CLI (Docker)</summary>
_We'll use the `lakectl` tool to create the branch._ 

In a new terminal window run the following:

```bash
docker exec lakefs \
    lakectl branch create \
	    lakefs://quickstart/denmark-lakes \
    --source lakefs://quickstart/main
```

_You should get a confirmation message like this:_

```bash
Source ref: lakefs://quickstart/main
created branch 'denmark-lakes' 3384cd7cdc4a2cd5eb6249b52f0a709b49081668bb1574ce8f1ef2d956646816
```
</details>

<details>
  <summary>CLI (local)</summary>
_We'll use the `lakectl` tool to create the branch._ 

In a new terminal window run the following:

```bash
lakectl branch create \
  lakefs://quickstart/denmark-lakes \
  --source lakefs://quickstart/main
```

_You should get a confirmation message like this:_

```bash
Source ref: lakefs://quickstart/main
created branch 'denmark-lakes' 3384cd7cdc4a2cd5eb6249b52f0a709b49081668bb1574ce8f1ef2d956646816
```
</details>

## Transforming the Data

_Now we'll make a change to the data. lakeFS has several native clients, as well as an S3-compatible endpoint. This means that anything that can use S3 will work with lakeFS. Pretty neat. We're going to use DuckDB, but unlike in the previous step where it was run within the lakeFS web page, we've got a standalone container running._

### Setting up DuckDB


<details>
  <summary>CLI (Docker)</summary>
Run the following in a terminal window to launch the DuckDB CLI:

```bash
docker exec --interactive --tty \
            lakefs duckdb
```

</details>
<details>
  <summary>CLI (local)</summary>
Run the following in a terminal window to launch the DuckDB CLI:

```bash
duckdb
```

Install the required modules by running this from the DuckDB prompt.

```sql
INSTALL httpfs;
LOAD httpfs;
```
</details>

_The first thing to do is configure the S3 connection so that DuckDB can access lakeFS, as well as tell DuckDB to report back how many rows are changed by the query we'll soon be executing._ 

Run this from the DuckDB prompt.

```sql
SET s3_url_style='path';
SET s3_region='us-east-1';
SET s3_use_ssl=false;
.changes on
```

In addition, customise the following for your environment and then run it too. 

_If you are using the lakeFS quickstart then you don't need to change anything and can run the SQL unchanged._

```sql
SET s3_endpoint='lakefs:8000';
SET s3_access_key_id='AKIA-EXAMPLE-KEY';
SET s3_secret_access_key='EXAMPLE-SECRET';
```

_Now we'll load the lakes data into a DuckDB table so that we can manipulate it._

```sql
CREATE TABLE lakes AS
    SELECT * FROM READ_PARQUET('s3://quickstart/denmark-lakes/lakes.parquet');
```

_Just to check that it's the same we saw before we're run the same query._

```sql
SELECT   country, COUNT(*)
FROM     lakes
GROUP BY country
ORDER BY COUNT(*.
DESC LIMIT 5;
```

```
┌──────────────────────────┬──────────────┐
│         Country          │ count_star() │
│         varchar          │    int64     │
├──────────────────────────┼──────────────┤
│ Canada                   │        83819 │
│ United States of America │         6175 │
│ Russia                   │         2524 │
│ Denmark                  │         1677 │
│ China                    │          966 │
└──────────────────────────┴──────────────┘
```

### Making a Change to the Data

_Now we can change our table, which was loaded from the original `lakes.parquet`, to remove all rows not for Denmark:_

```sql
DELETE FROM lakes WHERE Country != 'Denmark';
```

_You'll see that 98k rows have been deleted._

```sql
changes: 98323   total_changes: 198323
```

_We can verify that it's worked by reissuing the same query as before:_

```sql
SELECT   country, COUNT(*)
FROM     lakes
GROUP BY country
ORDER BY COUNT(*.
DESC LIMIT 5;
```

```
┌─────────┬──────────────┐
│ Country │ count_star() │
│ varchar │    int64     │
├─────────┼──────────────┤
│ Denmark │         1677 │
└─────────┴──────────────┘
```
## Write the Data back to lakeFS

_The changes so far have only been to DuckDB's copy of the data. Let's now push it back to lakeFS._ 

_Note the S3 path is different this time as we're writing it to the `denmark-lakes` branch, not `main`._

```sql
COPY lakes TO 's3://quickstart/denmark-lakes/lakes.parquet'
    (FORMAT 'PARQUET', ALLOW_OVERWRITE TRUE);
```

## Verify that the Data's Changed on the Branch

_Let's just confirm for ourselves that the parquet file itself has the new data._ 

_We'll drop the `lakes` table just to be sure, and then query the parquet file directly:_

```sql
DROP TABLE lakes;

SELECT   country, COUNT(*)
FROM     READ_PARQUET('s3://quickstart/denmark-lakes/lakes.parquet')
GROUP BY country
ORDER BY COUNT(*.
DESC LIMIT 5;
```

```
┌─────────┬──────────────┐
│ Country │ count_star() │
│ varchar │    int64     │
├─────────┼──────────────┤
│ Denmark │         1677 │
└─────────┴──────────────┘
```

## What about the data in `main`?

_So we've changed the data in our `denmark-lakes` branch, deleting swathes of the dataset. What's this done to our original data in the `main` branch? Absolutely nothing!_ 

See for yourself by returning to [the lakeFS object view](object?ref=main&path=lakes.parquet) and re-running the same query:

```sql
SELECT   country, COUNT(*)
FROM     READ_PARQUET(LAKEFS_OBJECT('quickstart', 'main', 'lakes.parquet'))
GROUP BY country
ORDER BY COUNT(*.
DESC LIMIT 5;
```
<img width="75%" src="/api/v1/repositories/quickstart/refs/main/objects?path=images%2Fduckdb-main-02.png" alt="The lakeFS object browser showing DuckDB querying lakes.parquet on the main branch. The results are the same as they were before we made the changes to the denmark-lakes branch, which is as expected." class="quickstart"/>

_In the next step we'll see how to merge our branch back into main._

<a name="commit-and-merge"></a>
# Committing Changes in lakeFS 🤝🏻

_In the previous step we branched our data from `main` into a new `denmark-lakes` branch, and overwrote the `lakes.parquet` to hold solely information about lakes in Denmark. Now we're going to commit that change (just like Git) and merge it back to main (just like Git)._

_Having make the change to the datafile in the `denmark-lakes` branch, we now want to commit it. There are various options for interacting with lakeFS' API, including the web interface, [a Python client](https://pydocs.lakefs.io/), and `lakectl`._

Choose one of the following methods depending on your preferred interface and how you're running lakeFS.

<details>
  <summary>Web UI</summary>

1. Go to the [**Uncommitted Changes**](./changes?ref=denmark-lakes) and make sure you have the `denmark-lakes` branch selected

2. Click on **Commit Changes**

    ![Screenshot of Uncommitted Changes screen in lakeFS](images/commit-change.png)

3. Enter a commit message and then click **Commits Changes**

    ![Adding a commit message in lakeFS](images/commit-change-02.png)

</details>

<details>
  <summary>CLI (Docker)</summary>
Run the following from a terminal window:

```bash
docker exec lakefs \
  lakectl commit lakefs://quickstart/denmark-lakes \
	-m "Create a dataset of just the lakes in Denmark"
```

_You will get confirmation of the commit including its hash._

```bash
Branch: lakefs://quickstart/denmark-lakes
Commit for branch "denmark-lakes" completed.

ID: ba6d71d0965fa5d97f309a17ce08ad006c0dde15f99c5ea0904d3ad3e765bd74
Message: Create a dataset of just the lakes in Denmark
Timestamp: 2023-03-15 08:09:36 +0000 UTC
Parents: 3384cd7cdc4a2cd5eb6249b52f0a709b49081668bb1574ce8f1ef2d956646816
```

</details>

<details>
  <summary>CLI (local)</summary>
Run the following from a terminal window:

```bash
lakectl commit lakefs://quickstart/denmark-lakes \
  -m "Create a dataset of just the lakes in Denmark"
```

_You will get confirmation of the commit including its hash._

```bash
Branch: lakefs://quickstart/denmark-lakes
Commit for branch "denmark-lakes" completed.

ID: ba6d71d0965fa5d97f309a17ce08ad006c0dde15f99c5ea0904d3ad3e765bd74
Message: Create a dataset of just the lakes in Denmark
Timestamp: 2023-03-15 08:09:36 +0000 UTC
Parents: 3384cd7cdc4a2cd5eb6249b52f0a709b49081668bb1574ce8f1ef2d956646816
```

</details>


_With our change committed, it's now time to merge it to back to the `main` branch._

# Merging Branches in lakeFS 🔀

_As with most operations in lakeFS, merging can be done through a variety of interfaces._

<details>
  <summary>Web UI</summary>

1. Click [here](./compare?ref=main&compare=denmark-lakes), or manually go to the **Compare** tab and set the **Compared to branch** to `denmark-lakes`.

    ![Merge dialog in lakeFS](images/merge01.png)

2. Click on **Merge**, leave the **Strategy** as `Default` and click on **Merge** confirm

    ![Merge dialog in lakeFS](images/merge02.png)

</details>

<details>
  <summary>CLI (Docker)</summary>

_The syntax for `merge` requires us to specify the source and target of the merge._ 

Run this from a terminal window.

```bash
docker exec lakefs \
  lakectl merge \
    lakefs://quickstart/denmark-lakes \
    lakefs://quickstart/main
```

</details>

<details>
  <summary>CLI (local)</summary>

_The syntax for `merge` requires us to specify the source and target of the merge._ 

Run this from a terminal window.

```bash
lakectl merge \
  lakefs://quickstart/denmark-lakes \
  lakefs://quickstart/main
```

</details>


_We can confirm that this has worked by returning to the same object view of [`lakes.parquet`](object?ref=main&path=lakes.parquet) as before and clicking on **Execute** to rerun the same query. You'll see that the country row counts have changed, and only Denmark is left in the data._

<img width="75%" src="/api/v1/repositories/quickstart/refs/main/objects?path=images%2Fduckdb-main-03.png" alt="The lakeFS object browser with a DuckDB query on lakes.parquet showing that there is only data for Denmark." class="quickstart"/>

**But…oh no!** 😬 A slow chill creeps down your spine, and the bottom drops out of your stomach. What have you done! 😱 *You were supposed to create **a separate file** of Denmark's lakes - not replace the original one* 🤦🏻🤦🏻‍♀.

_Is all lost? Will our hero overcome the obstacles? No, and yes respectively!_

_Have no fear; lakeFS can revert changes. Keep reading for the final part of the quickstart to see how._

<a name="rollback"></a>
# Rolling back Changes in lakeFS ↩️

_Our intrepid user (you) merged a change back into the `main` branch and realised that they had made a mistake 🤦🏻._

_The good news for them (you) is that lakeFS can revert changes made, similar to how you would in Git 😅._

<details>
  <summary>CLI (Docker)</summary>

From your terminal window run `lakectl` with the `revert` command:

```bash
docker exec -it lakefs \
    lakectl branch revert \
	    lakefs://quickstart/main \
	    main --parent-number 1 --yes
```

_You should see a confirmation of a successful rollback:_

```bash
Branch: lakefs://quickstart/main
commit main successfully reverted
```

</details>

<details>
  <summary>CLI (local)</summary>

From your terminal window run `lakectl` with the `revert` command:

```bash
lakectl branch revert \
  lakefs://quickstart/main \
  main --parent-number 1 --yes
```

_You should see a confirmation of a successful rollback:_

```bash
Branch: lakefs://quickstart/main
commit main successfully reverted
```

</details>

Back in the object page and the DuckDB query we can see that the original file is now back to how it was.

<img width="75%" src="/api/v1/repositories/quickstart/refs/main/objects?path=images%2Fduckdb-main-02.png" alt="The lakeFS object viewer with DuckDB query showing that the lakes dataset on main branch has been successfully returned to state prior to the merge." class="quickstart"/>

## Bonus Challenge

And so with that, this quickstart for lakeFS draws to a close. If you're simply having _too much fun_ to stop then here's an exercise for you.

Implement the requirement from above *correctly*, such that you write `denmark-lakes.parquet` in the respective branch and successfully merge it back into main. Look up how to list the contents of the `main` branch and verify that it looks like this:

```bash
object          2023-03-21 17:33:51 +0000 UTC    20.9 kB         denmark-lakes.parquet
object          2023-03-21 14:45:38 +0000 UTC    916.4 kB        lakes.parquet
```

<a name="resources"></a>
# Learn more about lakeFS

Here are some more resources to help you find out more about lakeFS.

## Connecting lakeFS to your own object storage

Enjoyed the quickstart and want to try out lakeFS against your own data? The documentation explains h[how to run lakeFS locally as a Docker container locally connecting to an object store](https://docs.lakefs.io/quickstart/learning-more-lakefs.html#connecting-lakefs-to-your-own-object-storage).

## Deploying lakeFS

Ready to do this thing for real? The deployment guides show you how to deploy lakeFS [locally](https://docs.lakefs.io/deploy/onprem.html) (including on [Kubernetes](https://docs.lakefs.io/deploy/onprem.html#k8s)) or on [AWS](https://docs.lakefs.io/deploy/aws.html), [Azure](https://docs.lakefs.io/deploy/azure.html), or [GCP](https://docs.lakefs.io/deploy/gcp.html).

Alternatively you might want to have a look at [lakeFS Cloud](https://lakefs.cloud/) which provides a fully-managed, SOC-2 compliant, lakeFS service.

## lakeFS Samples

The [lakeFS Samples](https://github.com/treeverse/lakeFS-samples) GitHub repository includes some excellent examples including.

* How to implement multi-table transaction on multiple Delta Tables
* Notebooks to show integration of lakeFS with Spark, Python, Delta Lake, Airflow and Hooks.
* Examples of using lakeFS webhooks to run automated data quality checks on different branches.
* Using lakeFS branching features to create dev/test data environments for ETL testing and experimentation.
* Reproducing ML experiments with certainty using lakeFS tags.

## lakeFS Community

lakeFS' community is important to us. Our **guiding principles** are.

* Fully open, in code and conversation
* We learn and grow together
* Compassion and respect in every interaction

We'd love for you to join [our **Slack group**](https://lakefs.io/slack) and come and introduce yourself on `#say-hello`. Or just lurk and soak up the vibes 😎

If you're interested in getting involved in lakeFS' development head over our [the **GitHub repo**](https://github.com/treeverse/lakeFS) to look at the code and peruse the issues. The comprehensive [contributing](https://docs.lakefs.io/contributing.html) document should have you covered on next steps but if you've any questions the `#dev` channel on [Slack](https://lakefs.io/slack) will be delighted to help.

We love speaking at meetups and chatting to community members at them - you can find a list of these [here](https://lakefs.io/community/).

Finally, make sure to drop by to say hi on [Twitter](https://twitter.com/lakeFS), [Mastodon](https://data-folks.masto.host/@lakeFS), and [LinkedIn](https://www.linkedin.com/company/treeverse/) 👋🏻

## lakeFS Concepts and Internals

We describe lakeFS as "_Git for data_" but what does that actually mean? Have a look at the [concepts](https://docs.lakefs.io/understand/model.html) and [architecture](https://docs.lakefs.io/understand/architecture.html) guides, as well as the explanation of [how merges are handled](https://docs.lakefs.io/understand/how/merge.html). To go deeper you might be interested in [the internals of versioning](https://docs.lakefs.io/understand/how/versioning-internals.htm) and our [internal database structure](https://docs.lakefs.io/understand/how/kv.html).
