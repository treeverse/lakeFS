---
layout: default
title: Rolling back Changes
description: TODO
parent: lakeFS Quickstart
nav_order: 5
has_children: false
# next: ["Create your first repository", "./repository.html"]
# redirect_from: [ "./quickstart/", "quickstart/installing.html", "quickstart/try.html"]
---

# Rolling back Changes in lakeFS

Our intrepid user (you) merged a change back into the `main` branch and realised that they had made a mistake. 

The good news for them (you) is that lakeFS can revert changes made, similar to how you would in git. 

From your terminal window run `lakectl` with the `revert` command:

```bash
docker exec -it lakefs \
	lakectl branch revert lakefs://quickstart/main \
						  main --parent-number 1 \
						  --yes
```
You should see a confirmation of a successful rollback:
```
Branch: lakefs://quickstart/main
commit main successfully reverted
```

Back in the object page and the DuckDB query we can see that the original file is now back to how it was: 
![](CleanShot%202023-03-21%20at%2017.23.20.png)

## Bonus Challenge

And so with that, this quickstart for lakeFS draws to a close. If you're simply having _too much fun_ to stop then here's an exercise for you. 

Implement the requirement from above *correctly*, such that you write `denmark-lakes.parquet` in the respective branch and successfully merge it back into main. Look up how to list the contents of the `main` branch and verify that it looks like this:

```bash
object          2023-03-21 17:33:51 +0000 UTC    20.9 kB         denmark-lakes.parquet
object          2023-03-21 14:45:38 +0000 UTC    916.4 kB        lakes.parquet
```

## Where Next?

_`MAYBE DO AS AN --INCLUDE-- TO INCLUDE ON INTRO PAGE TOO`_

* https://github.com/treeverse/lakeFS-samples
* Hooks
* lakeFS Cloud
* Installation / deployment docs
* Community