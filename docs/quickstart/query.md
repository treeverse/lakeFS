---
title: 2️⃣ Query the data
description: lakeFS quickstart / Query the pre-populated data using lakeFS' built-in DuckDB browser
parent: ⭐ Quickstart ⭐
nav_order: 10
has_children: false
next: ["Create a branch of the data", "./branch.html"]
previous: ["Launch the quickstart environment", "./launch.html"]
---

# Let's Query Something 👀 

The lakeFS server has been loaded with a sample parquet datafile. Fittingly enough for a piece of software to help users of data lakes, the `lakes.parquet` file holds data about lakes around the world. 

You'll notice that the branch is set to `main`. This is conceptually the same as your main branch in git against which you develop software code. 

![](/assets/img/quickstart/repo-contents.png)

Let's have a look at the data, ahead of making some changes to it on a branch in the following steps. 

Click on `lakes.parquet` and notice that the built-it DuckDB runs a query to show a preview of the file's contents. 

![](/assets/img/quickstart/duckdb-main-01.png)

Now we'll run our own query on it to look at the top five countries represented in the data. 

Copy and paste the following SQL statement into the DuckDB query panel and click on Execute.

```sql
SELECT   country, COUNT(*)
FROM     READ_PARQUET(LAKEFS_OBJECT('quickstart', 'main', 'lakes.parquet'))
GROUP BY country
ORDER BY COUNT(*) 
DESC LIMIT 5;
```

![](/assets/img/quickstart/duckdb-main-02.png)

Next we're going to make some changes to the data—but on a development branch so that the data in the main branch remains untouched. 