---
title: 5️⃣ Roll back Changes
description: lakeFS quickstart / Rollback the changes made to show how lakeFS can be used to revert changes made in error. 
parent: ⭐ Quickstart ⭐
nav_order: 25
has_children: false
next: ["Resources for learning more about lakeFS", "./getting-started.html"]
previous: ["Merge the branch back into main", "./commit-and-merge.html"]
---

# Rolling back Changes in lakeFS ↩️

Our intrepid user (you) merged a change back into the `main` branch and realised that they had made a mistake 🤦🏻. 

The good news for them (you) is that lakeFS can revert changes made, similar to how you would in git 😅. 

From your terminal window run `lakectl` with the `revert` command:

```bash
docker exec -it lakefs \
    lakectl branch revert \
	    lakefs://quickstart/main \
	    main --parent-number 1 --yes
```
You should see a confirmation of a successful rollback:
```
Branch: lakefs://quickstart/main
commit main successfully reverted
```

Back in the object page and the DuckDB query we can see that the original file is now back to how it was: 
![](/assets/img/quickstart/duckdb-main-02.png)

## Bonus Challenge

And so with that, this quickstart for lakeFS draws to a close. If you're simply having _too much fun_ to stop then here's an exercise for you. 

Implement the requirement from above *correctly*, such that you write `denmark-lakes.parquet` in the respective branch and successfully merge it back into main. Look up how to list the contents of the `main` branch and verify that it looks like this:

```
object          2023-03-21 17:33:51 +0000 UTC    20.9 kB         denmark-lakes.parquet
object          2023-03-21 14:45:38 +0000 UTC    916.4 kB        lakes.parquet
```

# Finishing Up

Once you've finished the quickstart, shut down your local environment with the following command: 

```bash
docker-compose down
```
