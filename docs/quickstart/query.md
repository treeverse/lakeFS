---
title: 2️⃣ Query the data
description: lakeFS quickstart / Query the pre-populated data using DuckDB browser that's built into lakeFS
parent: ⭐ Quickstart ⭐
nav_order: 10
has_children: false
next: ["Create a branch of the data", "./branch.html"]
previous: ["Launch the quickstart environment", "./launch.html"]
---

# Let's Query Something 👀 

The lakeFS server has been loaded with a sample parquet datafile. Fittingly enough for a piece of software to help users of data lakes, the `lakes.parquet` file holds data about lakes around the world. 

You'll notice that the branch is set to `main`. This is conceptually the same as your main branch in Git against which you develop software code. 

<img src="/assets/img/quickstart/repo-contents.png" alt="The lakeFS objects list with a highlight to indicate that the branch is set to main." class="quickstart"/>

Let's have a look at the data, ahead of making some changes to it on a branch in the following steps. 

Click on `lakes.parquet` and notice that the built-it DuckDB runs a query to show a preview of the file's contents. 

<img src="/assets/img/quickstart/duckdb-main-01.png" alt="The lakeFS object viewer with embedded DuckDB to query parquet files. A query has run automagically to preview the contents of the selected parquet file." class="quickstart"/>

Now we'll run our own query on it to look at the top five countries represented in the data. 

Copy and paste the following SQL statement into the DuckDB query panel and click on Execute.

```sql
SELECT   country, COUNT(*)
FROM     READ_PARQUET(LAKEFS_OBJECT('quickstart', 'main', 'lakes.parquet'))
GROUP BY country
ORDER BY COUNT(*) 
DESC LIMIT 5;
```

<img src="/assets/img/quickstart/duckdb-main-02.png" alt="An embedded DuckDB query showing a count of rows per country in the dataset." class="quickstart"/>

Next we're going to make some changes to the data—but on a development branch so that the data in the main branch remains untouched. 