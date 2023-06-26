---
title: 4️⃣ Commit and Merge
description: lakeFS quickstart / Commit the change and merge it back into the main branhch
parent: ⭐ Quickstart ⭐
nav_order: 20
has_children: false
next: ["Rollback the changes", "./rollback.html"]
previous: ["Create a branch of the data", "./branch.html"]
---

_In the previous step we branched our data from `main` into a new `denmark-lakes` branch, and overwrote the `lakes.parquet` to hold solely information about lakes in Denmark. Now we're going to commit that change (just like Git) and merge it back to main (just like git)._

# Committing Changes in lakeFS 🤝🏻

Having make the change to the datafile in the `denmark-lakes` branch, we now want to commit it. There are various options for interacting with the lakeFS API, including the web interface, [a Python client](https://pydocs.lakefs.io/), and `lakectl` which is what we'll use here. Run the following from a terminal window:

```bash
docker exec lakefs \
    lakectl commit lakefs://quickstart/denmark-lakes \
	    -m "Create a dataset of just the lakes in Denmark"
```

You will get confirmation of the commit including its hash.
```
Branch: lakefs://quickstart/denmark-lakes
Commit for branch "denmark-lakes" completed.

ID: ba6d71d0965fa5d97f309a17ce08ad006c0dde15f99c5ea0904d3ad3e765bd74
Message: Create a dataset of just the lakes in Denmark
Timestamp: 2023-03-15 08:09:36 +0000 UTC
Parents: 3384cd7cdc4a2cd5eb6249b52f0a709b49081668bb1574ce8f1ef2d956646816
```

With our change committed, it's now time to merge it to back to the `main` branch. 

# Merging Branches in lakeFS 🔀

As above, we'll use `lakectl` to do this too. The syntax just requires us to specify the source and target of the merge. Run this from a terminal window.

```bash
docker exec lakefs \
	lakectl merge \
		lakefs://quickstart/denmark-lakes \
		lakefs://quickstart/main
```

We can confirm that this has worked by returning to the same object view of `lakes.parquet` as before and clicking on **Execute** to rerun the same query. You'll see that the country row counts have changed, and only Denmark is left in the data: 

<img src="/assets/img/quickstart/duckdb-main-03.png" alt="The lakeFS object browser with a DuckDB query on lakes.parquet showing that there is only data for Denmark." class="quickstart"/>

**But…oh no!** 😬 A slow chill creeps down your spine, and the bottom drops out of your stomach. What have you done! 😱 *You were supposed to create **a separate file** of Denmark's lakes - not replace the original one* 🤦🏻🤦🏻‍♀️ 

Is all lost? Will our hero overcome the obstacles? No, and yes respectively!

Have no fear; lakeFS can revert changes. Tune in for the final part of the quickstart to see how. 