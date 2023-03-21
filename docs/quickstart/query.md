---
layout: default
title: Query
description: TODO
parent: lakeFS Quickstart
nav_order: 2
has_children: false
# next: ["Create your first repository", "./repository.html"]
# redirect_from: [ "./quickstart/", "quickstart/installing.html", "quickstart/try.html"]
---

## Let's Query Something

The lakeFS server has been loaded with a sample parquet datafile. Fittingly enough for a piece of software to help users of data lakes, the `lakes.parquet` file holds data about lakes around the world. 

You'll notice that the branch is set to `main`. This is conceptually the same as your main branch in git against which you develop software code. 

Let's have a look at the data, ahead of making some changes to it on a branch in the following steps. 

Click on `lakes.parquet` and notice that the built-it DuckDB runs a query to show a preview of the file's contents. 

![](Pasted%20image%2020230321160707.png)

Now we'll run our own query on it to look at the top five countries represented in the data. 

Copy and paste the following SQL statement into the DuckDB query panel and click on Execute.

```sql
SELECT   country, COUNT(*)
FROM     READ_PARQUET(LAKEFS_OBJECT('quickstart', 'main', 'lakes.parquet'))
GROUP BY country
ORDER BY COUNT(*) 
DESC LIMIT 5;
```

![](CleanShot%202023-03-21%20at%2016.10.45.png)

Next we're going to make some changes to the data—but on a development branch so that the data in the main branch remains untouched. 