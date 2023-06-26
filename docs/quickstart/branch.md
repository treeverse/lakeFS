---
title: 3️⃣ Create a branch
description: lakeFS quickstart / Create a branch in lakeFS without copying data on disk, make a change to the branch, see that the original version of the data is unchanged. 
parent: ⭐ Quickstart ⭐
nav_order: 15
has_children: false
next: ["Merge the branch back into main", "./commit-and-merge.html"]
previous: ["Query the pre-populated data", "./query.html"]
---

# Create a Branch 🪓

lakeFS uses branches in a similar way to Git. It's a great way to isolate changes until, or if, we are ready to re-integrate them. lakeFS uses a copy-on-write technique which means that it's very efficient to create branches of your data. 

Having seen the lakes data in the previous step we're now going to create a new dataset to hold data only for lakes in Denmark. Why? Well, because :)

The first thing we'll do is create a branch for us to do this development against. We'll use the `lakectl` tool to create the branch, which we first need to configure with our credentials.  In a new terminal window run the following:

```bash
docker exec -it lakefs lakectl config
```

Follow the prompts to enter the credentials that you got in the first step. Leave the **Server endpoint URL** as `http://127.0.0.1:8000`. 

Now that lakectl is configured, we can use it to create the branch. Run the following:

```bash
docker exec lakefs \
    lakectl branch create \
            lakefs://quickstart/denmark-lakes \
		    --source lakefs://quickstart/main
```

You should get a confirmation message like this:

```
Source ref: lakefs://quickstart/main
created branch 'denmark-lakes' 3384cd7cdc4a2cd5eb6249b52f0a709b49081668bb1574ce8f1ef2d956646816
```

## Transforming the Data

Now we'll make a change to the data. lakeFS has several native clients, as well as an [S3-compatible endpoint](https://docs.lakefs.io/understand/architecture.html#s3-gateway). This means that anything that can use S3 will work with lakeFS. Pretty neat.

We're going to use DuckDB which is embedded within the web interface of lakeFS. 

From the lakeFS **Objects** page select the `lakes.parquet` file to open the DuckDB editor: 

<img src="/assets/img/quickstart/duckdb-main-01.png" alt="The lakeFS object viewer with embedded DuckDB to query parquet files. A query has run automagically to preview the contents of the selected parquet file." class="quickstart"/>

To start with, we'll load the lakes data into a DuckDB table so that we can manipulate it. Replace the previous text in the DuckDB editor with this: 

```sql
CREATE OR REPLACE TABLE lakes AS 
    SELECT * FROM READ_PARQUET('lakefs://quickstart/denmark-lakes/lakes.parquet');
```

You'll see a row count of 100,000 to confirm that the DuckDB table has been created. 

Just to check that it's the same data that we saw before we'll run the same query. Note that we are querying a DuckDB table (`lakes`), rather than using a function to query a parquet file directly. 

```sql
SELECT   country, COUNT(*)
FROM     lakes
GROUP BY country
ORDER BY COUNT(*) 
DESC LIMIT 5;
```

<img src="/assets/img/quickstart/duckdb-editor-02.png" alt="The DuckDB editor pane querying the lakes table" class="quickstart"/>

### Making a Change to the Data

Now we can change our table, which was loaded from the original `lakes.parquet`, to remove all rows not for Denmark:

```sql
DELETE FROM lakes WHERE Country != 'Denmark';
```

<img src="/assets/img/quickstart/duckdb-editor-03.png" alt="The DuckDB editor pane deleting rows from the lakes table" class="quickstart"/>

We can verify that it's worked by reissuing the same query as before:

```sql
SELECT   country, COUNT(*)
FROM     lakes
GROUP BY country
ORDER BY COUNT(*) 
DESC LIMIT 5;
```


<img src="/assets/img/quickstart/duckdb-editor-04.png" alt="The DuckDB editor pane querying the lakes table showing only rows for Denmark remain" class="quickstart"/>

## Write the Data back to lakeFS

The changes so far have only been to DuckDB's copy of the data. Let's now push it back to lakeFS. Note the path is different this time as we're writing it to the `denmark-lakes` branch, not `main`: 

```sql
COPY lakes TO 'lakefs://quickstart/denmark-lakes/lakes.parquet';
```

<img src="/assets/img/quickstart/duckdb-editor-05.png" alt="The DuckDB editor pane writing data back to the denmark-lakes branch" class="quickstart"/>

## Verify that the Data's Changed on the Branch

Let's just confirm for ourselves that the parquet file itself has the new data. We'll drop the `lakes` table just to be sure, and then query the parquet file directly:

```sql
DROP TABLE lakes;

SELECT   country, COUNT(*)
FROM     READ_PARQUET('lakefs://quickstart/denmark-lakes/lakes.parquet')
GROUP BY country
ORDER BY COUNT(*) 
DESC LIMIT 5;
```

<img src="/assets/img/quickstart/duckdb-editor-06.png" alt="The DuckDB editor pane show the parquet file on denmark-lakes branch has been changed" class="quickstart"/>


## What about the data in `main`?

So we've changed the data in our `denmark-lakes` branch, deleting swathes of the dataset. What's this done to our original data in the `main` branch? Absolutely nothing! See for yourself by running the same query as above, but against the `main` branch:

```sql
SELECT   country, COUNT(*)
FROM     READ_PARQUET('lakefs://quickstart/main/lakes.parquet')
GROUP BY country
ORDER BY COUNT(*) 
DESC LIMIT 5;
```
<img src="/assets/img/quickstart/duckdb-main-02.png" alt="The lakeFS object browser showing DuckDB querying lakes.parquet on the main branch. The results are the same as they were before we made the changes to the denmark-lakes branch, which is as expected." class="quickstart"/>

In the next step we'll see how to merge our branch back into main. 
