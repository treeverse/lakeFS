---
title: :seven: Work with lakeFS data on your local environment
description: lakeFS quickstart / Bring lakeFS data to a local environment to show how lakeFS can be used for ML experiments development. 
parent: ⭐ Quickstart
nav_order: 35
next: ["Resources for learning more about lakeFS", "./learning-more-lakefs.html"]
previous: ["Using Actions and Hooks in lakeFS", "./actions-and-hooks.html"]
---

# Work with lakeFS data locally



## Bonus Challenge

And so with that, this quickstart for lakeFS draws to a close. If you're simply having _too much fun_ to stop then here's an exercise for you.

Implement the requirement from the beginning of this quickstart *correctly*, such that you write `denmark-lakes.parquet` in the respective branch and successfully merge it back into main. Look up how to list the contents of the `main` branch and verify that it looks like this:

```
object          2023-03-21 17:33:51 +0000 UTC    20.9 kB         denmark-lakes.parquet
object          2023-03-21 14:45:38 +0000 UTC    916.4 kB        lakes.parquet
```

# Finishing Up

Once you've finished the quickstart, shut down your local environment with the following command:

```bash
docker stop lakefs
```

