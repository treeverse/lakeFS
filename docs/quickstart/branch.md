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

The first thing we'll do is create a branch for us to do this development against. We'll use the `lakectl` tool to create the branch. In a new terminal window run the following:

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

Now we'll make a change to the data. lakeFS has several native clients, as well as an S3-compatible endpoint. This means that anything that can use S3 will work with lakeFS. Pretty neat. We're going to use DuckDB, but unlike in the previous step where it was run within the lakeFS web page, we've got a standalone container running. 

### Setting up DuckDB

Run the following in a new terminal window to launch the DuckDB CLI:

```bash
docker exec --interactive --tty lakefs duckdb
```

The first thing to do is configure the S3 connection so that DuckDB can access lakeFS, as well as tell DuckDB to report back how many rows are changed by the query we'll soon be executing. Run this from the DuckDB prompt: 

```sql
SET s3_endpoint='localhost:8000';
SET s3_access_key_id='AKIA-EXAMPLE-KEY';
SET s3_secret_access_key='EXAMPLE-SECRET';
SET s3_url_style='path';
SET s3_region='us-east-1';
SET s3_use_ssl=false;
.changes on
```

Now we'll load the lakes data into a DuckDB table so that we can manipulate it:

```sql
CREATE TABLE lakes AS 
    SELECT * FROM READ_PARQUET('s3://quickstart/denmark-lakes/lakes.parquet');
```

Just to check that it's the same we saw before we're run the same query: 

```sql
SELECT   country, COUNT(*)
FROM     lakes
GROUP BY country
ORDER BY COUNT(*) 
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

Now we can change our table, which was loaded from the original `lakes.parquet`, to remove all rows not for Denmark:

```sql
DELETE FROM lakes WHERE Country != 'Denmark';
```

You'll see that 98k rows have been deleted: 

```sql
changes: 98323   total_changes: 198323
```

We can verify that it's worked by reissuing the same query as before:
```sql
SELECT   country, COUNT(*)
FROM     lakes
GROUP BY country
ORDER BY COUNT(*) 
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

The changes so far have only been to DuckDB's copy of the data. Let's now push it back to lakeFS. Note the S3 path is different this time as we're writing it to the `denmark-lakes` branch, not `main`: 

```sql
COPY lakes TO 's3://quickstart/denmark-lakes/lakes.parquet' 
    (FORMAT 'PARQUET', ALLOW_OVERWRITE TRUE);
```

## Verify that the Data's Changed on the Branch

Let's just confirm for ourselves that the parquet file itself has the new data. We'll drop the `lakes` table just to be sure, and then query the parquet file directly:

```sql
DROP TABLE lakes;

SELECT   country, COUNT(*)
FROM     READ_PARQUET('s3://quickstart/denmark-lakes/lakes.parquet')
GROUP BY country
ORDER BY COUNT(*) 
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

So we've changed the data in our `denmark-lakes` branch, deleting swathes of the dataset. What's this done to our original data in the `main` branch? Absolutely nothing! See for yourself by returning to the lakeFS object view and re-running the same query:

```sql
SELECT   country, COUNT(*)
FROM     READ_PARQUET(LAKEFS_OBJECT('quickstart', 'main', 'lakes.parquet'))
GROUP BY country
ORDER BY COUNT(*) 
DESC LIMIT 5;
```
<img src="/assets/img/quickstart/duckdb-main-02.png" alt="The lakeFS object browser showing DuckDB querying lakes.parquet on the main branch. The results are the same as they were before we made the changes to the denmark-lakes branch, which is as expected." class="quickstart"/>

In the next step we'll see how to merge our branch back into main. 