---
title: 5️⃣ Roll back Changes
description: lakeFS quickstart / Rollback the changes made to show how lakeFS can be used to revert changes made in error. 
parent: ⭐ Quickstart ⭐
nav_order: 25
has_children: false
next: ["Using Actions and Hooks in lakeFS", "./actions-and-hooks.html"]
previous: ["Merge the branch back into main", "./commit-and-merge.html"]
---

# Rolling back Changes in lakeFS ↩️

Our intrepid user (you) merged a change back into the `main` branch and realised that they had made a mistake 🤦🏻. 

The good news for them (you) is that lakeFS can revert changes made, similar to how you would in Git 😅. 

From your terminal window run `lakectl` with the `revert` command:

```bash
docker exec -it lakefs lakectl branch revert lakefs://quickstart/main main --parent-number 1 --yes
```
You should see a confirmation of a successful rollback:
```
Branch: lakefs://quickstart/main
commit main successfully reverted
```

Back in the object page and the DuckDB query we can see that the original file is now back to how it was: 
<img src="/assets/img/quickstart/duckdb-main-02.png" alt="The lakeFS object viewer with DuckDB query showing that the lakes dataset on main branch has been successfully returned to state prior to the merge." class="quickstart"/>
