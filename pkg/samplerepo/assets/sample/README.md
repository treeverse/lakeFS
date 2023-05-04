# Welcome to the Lake!

![Waving Axolotl](/images/waving-axolotl-transparent-w90.gif)

**lakeFS brings software engineering best practices and applies them to data engineering.** 

lakeFS provides version control over the data lake, and uses Git-like semantics to create and access those versions. If you know git, you'll be right at home with lakeFS.

With lakeFS, you can use concepts on your data lake such as **branch** to create an isolated version of the data, **commit** to create a reproducible point in time, and **merge** in order to incorporate your changes in one atomic action.

This quickstart will introduce you to some of the core ideas in lakeFS and show what you can do by illustrating the concept of branching, merging, and rolling back changes to data. It's laid out in five short sections: 


* ![Query icon](/images/quickstart-step-01-query.png)
* ![Branch icon](/images/quickstart-step-02-branch.png)
* ![Merge icon](/images/quickstart-step-03-merge.png)
* ![Rollback icon](/images/quickstart-step-04-rollback.png)

<div class="quickstart-steps">
<div class="row">
<div class="col step-num">
<img src="https://docs.lakefs.io/assets/img/quickstart/quickstart-step-02.png" alt="step 2"/>
</div>
<div class="col">
<h3>
<a href="#query">Query</a>
</h3>
<p>Query the pre-populated data on the `main` branch</p>
</div>
</div>

<div class="row">
<div class="col step-num">
<img src="https://docs.lakefs.io/assets/img/quickstart/quickstart-step-03.png" alt="step 3"/>
</div>
<div class="col">
<h3>
<a href="#branch">Branch</a>
</h3>
<p>Make changes to the data on a new branch</p>
</div>
</div>

<div class="row">
<div class="col step-num">
<img src="https://docs.lakefs.io/assets/img/quickstart/quickstart-step-04.png" alt="step 4"/>
</div>
<div class="col">
<h3>
<a href="#commit-and-merge">Merge</a>
</h3>
<p>Merge the changed data back to the `main` branch</p>
</div>
</div>

<div class="row">
<div class="col step-num">
<img src="https://docs.lakefs.io/assets/img/quickstart/quickstart-step-05.png" alt="step 5"/>
</div>
<div class="col">
<h3>
<a href="#rollback">Rollback</a>
</h3>
<p>Change our mind and revert the changes</p>
</div>
</div>
</div>


<a name="query"></a>
## Let's get started 😺

We'll start off by querying the sample data to orient ourselves around what it is we're working with. The lakeFS server has been loaded with a sample parquet datafile. Fittingly enough for a piece of software to help users of data lakes, the `lakes.parquet` file holds data about lakes around the world. 

You'll notice that the branch is set to `main`. This is conceptually the same as your main branch in Git against which you develop software code. 

<img width="75%" src="https://docs.lakefs.io/assets/img/quickstart/repo-contents.png" alt="The lakeFS objects list with a highlight to indicate that the branch is set to main." class="quickstart"/>

Let's have a look at the data, ahead of making some changes to it on a branch in the following steps. 

Click on [`lakes.parquet`](/repositories/quickstart/object?ref=main&path=data%2Flakes.parquet) from the object browser and notice that the built-it DuckDB runs a query to show a preview of the file's contents. 

<img width="75%" src="https://docs.lakefs.io/assets/img/quickstart/duckdb-main-01.png" alt="The lakeFS object viewer with embedded DuckDB to query parquet files. A query has run automagically to preview the contents of the selected parquet file." class="quickstart"/>

Now we'll run our own query on it to look at the top five countries represented in the data. 

Copy and paste the following SQL statement into the DuckDB query panel and click on Execute.

```sql
SELECT   country, COUNT(*)
FROM     READ_PARQUET(LAKEFS_OBJECT('quickstart', 'main', 'lakes.parquet'))
GROUP BY country
ORDER BY COUNT(*) 
DESC LIMIT 5;
```

<img width="75%" src="https://docs.lakefs.io/assets/img/quickstart/duckdb-main-02.png" alt="An embedded DuckDB query showing a count of rows per country in the dataset." class="quickstart"/>

Next we're going to make some changes to the data—but on a development branch so that the data in the main branch remains untouched. 

