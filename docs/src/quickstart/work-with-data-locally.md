---
title: 7️⃣ Work with lakeFS data locally
description: lakeFS quickstart / Bring lakeFS data to a local environment to show how lakeFS can be used for ML experiments development.
---

# Work with lakeFS Data Locally

When working with lakeFS, there are scenarios where we need to access and manipulate data locally. An example use case for working
locally is machine learning model development. Machine learning model development is dynamic and iterative. To optimize this
process, experiments need to be conducted with speed, tracking ease, and reproducibility. Localizing model data during development
accelerates the process by enabling interactive and offline development and reducing data access latency.

lakeFS provides 2 ways to expose versioned data locally

## lakeFS Mount

!!! info
    lakeFS Mount is available for [lakeFS Enterprise](../enterprise/index.md) and [lakeFS Cloud](../cloud/index.md) customers. You can try it out by [signing up](https://info.lakefs.io/thanks-lakefs-mounts)


<iframe width="420" height="315" src="https://www.youtube.com/embed/BgKuoa8LAaU"></iframe>

### Getting started with lakeFS Mount

Prerequisites:

- A working lakeFS Server running either lakeFS Enterprise or lakeFS Cloud
- You’ve installed the lakectl command line utility: this is the official lakeFS command line interface, on top of which lakeFS Mount is built.
- lakectl is configured properly to access your lakeFS server as detailed in the configuration instructions

### Mounting a path to a local directory:

1. In lakeFS create a new branch called `my-experiment`. You can do this through the UI or with `lakectl`:

    ```bash
    lakectl branch create \
        lakefs://quickstart/my-experiment \
        --source lakefs://quickstart/main
    ```

2. Mount images from your quickstart repository into a local directory named `my_local_dir`

    ```bash
    everest mount lakefs://quickstart/my-experiment/images my_local_dir
    ```

    Once complete, `my_local_dir` should be mounted with the specified path.

3. Verify that `my_local_dir` is linked to the correct path in your lakeFS remote:
    ```bash
    ls -l my_local_dir
    ```
4. To unmount the directory, simply run:

    ```bash
    everest umount ./my_local_dir
    ```

    Which will unmount the path and terminate the local mount-server.


## lakectl local

Alternatively, we can use [lakectl local](../howto/local-checkouts.md#sync-a-local-directory-with-lakefs) to bring a subset of our lakeFS data to a local directory within the lakeFS
container and edit an image dataset used for ML model development. Unlike lakeFS Mount, using `lakectl local` requires copying data to/from lakeFS and your local machine.

<iframe width="420" height="315" src="https://www.youtube.com/embed/afgQnmesLZM"></iframe>

Reference Guide: [lakeFS lakectl local for machine learning](https://lakefs.io/blog/guide-lakectl-local-machine-learning/)

### Cloning a Subset of lakeFS Data into a Local Directory

1. In lakeFS create a new branch called `my-experiment`. You can do this through the UI or with `lakectl`:
    ```bash
    lakectl branch create lakefs://quickstart/my-experiment --source lakefs://quickstart/main
    ```
2. Clone images from your quickstart repository into a local directory named `my_local_dir` within your container:
    ```bash
    lakectl local clone lakefs://quickstart/my-experiment/images my_local_dir
    ```
3. Verify that `my_local_dir` is linked to the correct path in your lakeFS remote:
    ```bash
    lakectl local list
    ```
   You should see confirmation that my_local_dir is tracking the desired lakeFS path.:
   ```bash
       my_local_dir	lakefs://quickstart/my-experiment/images/8614575b5488b47a094163bd17a12ed0b82e0bcbfd22ed1856151c671f1faa53
   ```
4. Verify that your local environment is up-to-date with its remote path:

    ```bash
    lakectl local status my_local_dir
    ```

    You should get a confirmation message like this showing that there is no difference between your local environment and the lakeFS remote:

    ```text
    diff 'local:///home/lakefs/my_local_dir' <--> 'lakefs://quickstart/8614575b5488b47a094163bd17a12ed0b82e0bcbfd22ed1856151c671f1faa53/images/'...
    diff 'lakefs://quickstart/8614575b5488b47a094163bd17a12ed0b82e0bcbfd22ed1856151c671f1faa53/images/' <--> 'lakefs://quickstart/my-experiment/images/'...

    No diff found.
    ```

### Making Changes to Data Locally

1. Clean the dataset by removing images larger than 225 KB:
    ```bash
    find my_local_dir -type f -size +225k -delete
    ```
2. Check the status of your local changes compared to the lakeFS remote path:
    ```bash
    lakectl local status my_local_dir
    ```

    You should get a confirmation message like this, showing the modifications you made locally:
    ```bash
    diff 'local:///home/lakefs/my_local_dir' <--> 'lakefs://quickstart/8614575b5488b47a094163bd17a12ed0b82e0bcbfd22ed1856151c671f1faa53/images/'...
    diff 'lakefs://quickstart/8614575b5488b47a094163bd17a12ed0b82e0bcbfd22ed1856151c671f1faa53/images/' <--> 'lakefs://quickstart/my-experiment/images/'...

    ╔════════╦══════════╦═════════════════════╗
    ║ SOURCE ║ CHANGE   ║ PATH                ║
    ╠════════╬══════════╬═════════════════════╣
    ║ local  ║ modified ║ axolotl.png         ║
    ║ local  ║ removed  ║ duckdb-main-02.png  ║
    ║ local  ║ removed  ║ empty-repo-list.png ║
    ║ local  ║ removed  ║ repo-contents.png   ║
    ╚════════╩══════════╩═════════════════════╝
    ```

### Pushing Local Changes to lakeFS


Once we are done with editing the image dataset in our local environment, we will push our changes to the lakeFS remote so that
the improved dataset is shared and versioned.

1. Commit your local changes to lakeFS:

    ```bash
    lakectl local commit -m 'Deleted images larger than 225KB in size and changed the Axolotl image' my_local_dir
    ```

    In your branch, you should see the commit including your local changes:

    <img width="75%" src="../../assets/img/quickstart/lakectl-local-01.png" alt="A lakectl local commit to lakeFS" class="quickstart"/>

2. Compare `my-experiment` branch to the `main` branch to visualize your changes:

    <img width="75%" src="../../assets/img/quickstart/lakectl-local-02.png" alt="A comparison between a branch that includes local changes to the main branch" class="quickstart"/>



!!! example "Bonus Challenge"
    And so with that, this quickstart for lakeFS draws to a close. If you're simply having _too much fun_ to stop then here's an exercise for you.

    Implement the requirement from the beginning of this quickstart *correctly*, such that you write `denmark-lakes.parquet` in the respective branch and successfully merge it back into main. Look up how to list the contents of the `main` branch and verify that it looks like this:

    ```text
    object          2023-03-21 17:33:51 +0000 UTC    20.9 kB         denmark-lakes.parquet
    object          2023-03-21 14:45:38 +0000 UTC    916.4 kB        lakes.parquet
    ```

---

[← Using Actions and Hooks in lakeFS](actions-and-hooks.md){ .md-button } [Learn more about lakeFS →](learning-more-lakefs.md){ .md-button .md-button--primary }

---
